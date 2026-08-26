package upstream

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
)

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
	def      http.RoundTripper
	maxConns int
	// noProxy 让 per-request 代理同样遵守豁免列表。
	// 不共享豁免会让 X-Upstream-Proxy 成为绕过 NO_PROXY 的后门 ——
	// 本机 mock 上游会被打进客户端指定的代理。
	noProxy string

	// pool 的键是规范化后的代理 URL，值是 *http.Transport。
	// 用 sync.Map 而非 RWMutex+map：这是读极多写极少的场景，
	// sync.Map 的读路径无锁，不会让高并发请求争抢同一把读锁。
	pool sync.Map

	// mu 只保护「构建新 transport」这一段，不参与读路径。
	mu sync.Mutex
}

// NewRouter 创建代理路由器。def 承载未声明代理时的默认出口，
// noProxy 同时作用于默认出口之外的 per-request 代理。
func NewRouter(def http.RoundTripper, maxConns int, noProxy string) *Router {
	return &Router{def: def, maxConns: maxConns, noProxy: noProxy}
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
		return v.(*http.Transport), nil
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
		return v.(*http.Transport), nil
	}
	t := newTransport(r.maxConns, proxyWithBypass(u, r.noProxy))
	r.pool.Store(proxy, t)
	return t, nil
}

// CloseIdleConnections 关闭默认出口与所有代理 transport 的空闲连接。
func (r *Router) CloseIdleConnections() {
	type closer interface{ CloseIdleConnections() }
	if c, ok := r.def.(closer); ok {
		c.CloseIdleConnections()
	}
	r.pool.Range(func(_, v any) bool {
		v.(*http.Transport).CloseIdleConnections()
		return true
	})
}
