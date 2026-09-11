package upstream

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/betterme/remap-service/internal/config"
)

// ProxyEvictReporter 在 per-request 代理池发生淘汰时被调用。
//
// size 是淘汰后的池内条目数，limit 是配置的上限。它要回答的是
// 「上限是否过小、客户端是否在用大量不同代理」—— 这两个都不是任何单条
// 请求能反映的，所以以回调形式交给 obs 打点，与 H2ErrorReporter 同理，
// upstream 层因此不反向依赖 obs。允许为 nil。
type ProxyEvictReporter func(size, limit int)

// defaultProxyPoolLimit 是池容量的内置默认值。
//
// 为什么需要上限：UPSTREAM_PROXY_FROM_HEADER 打开后，代理数量不再由运维
// 决定，而是「客户端能声明多少就有多少」，而池是常驻的 —— 没有上限就意味着
// 客户端用 N 个不同代理各发一次请求，就能把 N 个 transport 永久留在内存里。
//
// 取 64：运维侧的正常取值是个位数，客户端自带代理的场景几十个已很宽裕；
// 64 个 transport 的对象开销在 MB 量级，对 2G 的容器可忽略。
//
// 注意它与 config 的 UPSTREAM_MAX_PROXY_TRANSPORTS 默认值必须一致。
// 配置加载路径一定已填该值，这里的常量只服务手工构造的 Router（测试、
// 零配置入口）—— 与 limitsFor 里那几个零值兜底是同一取向。
// 启动日志的 max_proxy_transports 可用于核对两者是否漂移。
const defaultProxyPoolLimit = 64

// proxyEntry 是池中的一个条目。
//
// lastUsed 用原子纳秒时间戳，而不是在锁内维护链表：命中池的读路径必须保持
// 无锁零分配（见 BenchmarkRouterProxyLookup），而精确 LRU 要求每次读都更新
// 顺序，那就必须加锁。这里选择近似淘汰 —— 极端情况下可能淘汰掉一个仍在被
// 使用的条目，代价只是该代理的下次请求重建 transport；而淘汰只关空闲连接，
// 不会打断任何在途请求。
type proxyEntry struct {
	rt       *http.Transport
	lastUsed atomic.Int64
}

// Router 按 per-request 声明的代理把请求分派到对应的 transport。
//
// 它实现 http.RoundTripper，因此对 Client 完全透明 —— Client.Do 的
// 看门狗逻辑不需要任何改动。
//
// 性能取向（方案 B 的核心约束）：
//   - 未声明代理时直接委派给 def，除一次 ctx.Value 外无额外开销、零分配；
//   - 声明代理时走 sync.Map 读路径，命中即无锁、零分配；
//   - 只有首次见到某个代理才加锁构建 transport，此后永久复用连接池。
//
// 代理数量由运维侧的取值集合决定（通常个位数），因此不设淘汰：
// 淘汰会关掉正在复用的连接池，代价远高于常驻几个 transport 的内存。
type Router struct {
	def http.RoundTripper
	// maxConns 与 tc 一并交给 newTransport 构建 per-request 代理的出口。
	// tc 必须穿透到这里：为某个代理新建的 transport 若用内置默认，
	// 就会出现「默认出口按配置、代理出口按硬编码」的分裂 ——
	// 而故障恰恰多发在经代理的路径上。
	maxConns int
	tc       config.Transport
	// noProxy 让 per-request 代理同样遵守豁免列表。
	// 不共享豁免会让 X-Upstream-Proxy 成为绕过 NO_PROXY 的后门 ——
	// 本机 mock 上游会被打进客户端指定的代理。
	noProxy string
	// maxProxyTransports 是池内容量上限，池满时淘汰最久未使用的条目。
	//
	// 上限存在的理由见 defaultProxyPoolLimit：「代理数量由运维侧决定」
	// 这个前提在 UPSTREAM_PROXY_FROM_HEADER 打开时并不成立。
	maxProxyTransports int
	// onProxyEvict 在淘汰发生时上报，可为 nil。
	onProxyEvict ProxyEvictReporter
	// onH2Err 同样要穿透到 per-proxy 的 transport：故障多发在经代理的路径上，
	// 只在默认出口接事件出口，会让「代理出口的 PING 失败 / GOAWAY」在指标里
	// 完全不可见 —— 而按代理碎片化的池正是最容易积压死连接的地方。
	onH2Err H2ErrorReporter

	// pool 的键是请求声明的代理 URL **原始串**，不做规范化；值是 *proxyEntry。
	//
	// 不规范化是因为规范化需要解析 URL，而命中池的读路径必须零分配。
	// 代价是 `http://h:8080` 与 `http://h:8080/` 这类等价写法会各占一个条目 ——
	// 客户端格式不统一时会加速池的填充，但不会出错（两个 transport 都能
	// 正确工作），且有上限兜底。
	//
	// 用 sync.Map 而非 RWMutex+map：这是读极多写极少的场景，
	// sync.Map 的读路径无锁，不会让高并发请求争抢同一把读锁。
	pool sync.Map

	// mu 只保护「淘汰 + 构建新 transport」这一段，不参与读路径。
	mu sync.Mutex
	// count 是 pool 的条目数，**只在持有 mu 时读写**。
	count int
}

// NewRouter 创建代理路由器。def 承载未声明代理时的默认出口，
// maxConns 与 tc 用于构建 per-request 代理的 transport（与默认出口同一套参数），
// noProxy 同时作用于默认出口之外的 per-request 代理，
// maxProxyTransports 是池内容量上限（<= 0 时取 defaultProxyPoolLimit），
// onH2Err 接收 h2 连接级事件（可为 nil），
// onProxyEvict 接收池淘汰事件（可为 nil）。
func NewRouter(
	def http.RoundTripper, maxConns int, tc config.Transport, noProxy string,
	maxProxyTransports int, onH2Err H2ErrorReporter, onProxyEvict ProxyEvictReporter,
) *Router {
	if maxProxyTransports <= 0 {
		maxProxyTransports = defaultProxyPoolLimit
	}
	return &Router{
		def:                def,
		maxConns:           maxConns,
		tc:                 tc,
		noProxy:            noProxy,
		maxProxyTransports: maxProxyTransports,
		onH2Err:            onH2Err,
		onProxyEvict:       onProxyEvict,
	}
}

// RoundTrip 实现 http.RoundTripper。
func (r *Router) RoundTrip(req *http.Request) (*http.Response, error) {
	p := ProxyFrom(req.Context())
	if p == "" {
		return r.def.RoundTrip(req) // 快路径：与不带代理时完全一致
	}
	t, err := r.transportFor(p)
	if err != nil {
		return nil, err
	}
	return t.RoundTrip(req)
}

// transportFor 取出或构建某个代理专属的 transport。
func (r *Router) transportFor(proxy string) (http.RoundTripper, error) {
	if v, ok := r.pool.Load(proxy); ok {
		e := v.(*proxyEntry)
		e.lastUsed.Store(time.Now().UnixNano())
		return e.rt, nil
	}

	u, err := url.Parse(proxy)
	if err != nil {
		return nil, fmt.Errorf("代理地址无法解析: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// 双重检查：并发首次请求同一代理时只构建一个实例，
	// 否则会凭空多出几个各自独立的连接池。
	if v, ok := r.pool.Load(proxy); ok {
		e := v.(*proxyEntry)
		e.lastUsed.Store(time.Now().UnixNano())
		return e.rt, nil
	}

	// 先腾位置再建，保证池大小不会瞬时越过上限。
	r.evictUntilRoomLocked()

	e := &proxyEntry{rt: newTransport(r.maxConns, r.tc, proxyWithBypass(u, r.noProxy), r.onH2Err)}
	e.lastUsed.Store(time.Now().UnixNano())
	r.pool.Store(proxy, e)
	r.count++
	return e.rt, nil
}

// evictUntilRoomLocked 在池已满时淘汰最久未使用的条目，直到腾出位置。
//
// 用循环而非单个 if：上限被调小之后池里可能已超出若干条目，需要一次腾够，
// 否则上限就形同虚设。
func (r *Router) evictUntilRoomLocked() {
	for r.count >= r.maxProxyTransports && r.evictOldestLocked() {
	}
}

// evictOldestLocked 淘汰 lastUsed 最小的条目，返回是否真的淘汰了一个。
// 调用方必须持有 mu。
//
// 这里只关空闲连接：CloseIdleConnections 不触碰在途请求，因此即使淘汰到的
// 是一个正在被使用的代理，它的在途请求也照常完成，只是后续请求会重建
// transport。这一步不能省 —— 不关连接的话，淘汰只是把「transport 对象常驻」
// 换成「连接 + fd 常驻」，泄漏换了个形态而已。
func (r *Router) evictOldestLocked() bool {
	var (
		oldestKey any
		oldestTS  int64
		found     bool
	)
	r.pool.Range(func(k, v any) bool {
		ts := v.(*proxyEntry).lastUsed.Load()
		if !found || ts < oldestTS {
			oldestKey, oldestTS, found = k, ts, true
		}
		return true
	})
	if !found {
		return false
	}
	v, ok := r.pool.LoadAndDelete(oldestKey)
	if !ok {
		// 增删都发生在 mu 内，走到这里说明条目已被别处移除，
		// 当作「本次没淘汰成功」处理，让调用方重新评估。
		return false
	}
	v.(*proxyEntry).rt.CloseIdleConnections()
	r.count--
	if r.onProxyEvict != nil {
		r.onProxyEvict(r.count, r.maxProxyTransports)
	}
	return true
}

// CloseIdleConnections 关闭默认出口与所有代理 transport 的空闲连接。
func (r *Router) CloseIdleConnections() {
	type closer interface{ CloseIdleConnections() }
	if c, ok := r.def.(closer); ok {
		c.CloseIdleConnections()
	}
	r.pool.Range(func(_, v any) bool {
		v.(*proxyEntry).rt.CloseIdleConnections()
		return true
	})
}
