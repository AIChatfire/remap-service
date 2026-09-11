// Package upstream 提供面向高并发的上游 HTTP 客户端。
//
// 连接池**规模**由 config.Limits.MaxConns 一个数字推导（该值即「单上游主机
// 的空闲连接上限」，全局上限按 4 倍放宽以容纳多协议端点）；而连接**寿命**
// 与 h2 健康检查没有可推导的正确值 —— 它们取决于上游与网络形态 ——
// 因此改由 config.Transport 显式配置。这几项配错会直接表现为批量故障
// （取到半关闭连接、或 h2 误杀长静默的连接），必须可调。
//
// 另外两个刻意的默认：
//   - 关闭自动解压，让响应体以原始字节透传（SSE 场景必须）；
//   - 不设 http.Client.Timeout，流式时长由 per-request context 控制。
package upstream

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"

	"github.com/betterme/remap-service/internal/config"
)

// H2ErrorReporter 接收 x/net/http2 报告的**连接级**内部事件。
//
// 事件名由 x/net/http2 封闭定义。实践中最要紧的是 conn_close_lost_ping：
// 它意味着 PING 探活没等到 PONG，客户端据此关闭**整条**连接，其上所有
// 在途流一起拿到 "http2: client connection lost"（这正是该错误串的
// 唯一产生点，见 closeForLostPing）。recv_goaway_* 与读帧错误同理。
//
// 这类事件不属于任何单个请求 —— 一条连接被多个请求复用 —— 因此不适合
// 写进 span，只能作为指标上报。允许为 nil（零配置入口与测试用）。
type H2ErrorReporter func(kind string)

// NewTransport 用**内置默认**的传输层参数构建 transport，
// 代理取自 HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。
//
// 这是零配置入口（测试与不需要调参的场景）。生产路径是 NewRoundTripper，
// 它会把 config.Transport 带进来 —— 用错这一个会静默丢掉全部传输层配置，
// 启动日志里的 idle_conn_timeout / h2_idle_timeout 就是判别依据。
func NewTransport(maxConns int) *http.Transport {
	return newTransport(maxConns, config.Transport{}, http.ProxyFromEnvironment, nil)
}

// NewRoundTripper 是给 main 用的统一入口：按配置决定默认出口与传输层参数，
// 再包一层 Router 以支持 per-request 的 X-Upstream-Proxy。
//
// 默认出口的代理来源：UPSTREAM_PROXY 非空则固定走它，否则回落到
// HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。
//
// onH2Err 接收 h2 连接级事件（见 H2ErrorReporter），可为 nil。
// onProxyEvict 接收 per-request 代理池的淘汰事件（见 ProxyEvictReporter），可为 nil。
//
// 返回值同时满足 http.RoundTripper 与 CloseIdleConnections，
// 可直接交给 NewClient 并在退出时清理。
func NewRoundTripper(
	maxConns int, tc config.Transport, u config.Upstream,
	onH2Err H2ErrorReporter, onProxyEvict ProxyEvictReporter,
) *Router {
	proxy := http.ProxyFromEnvironment
	if p := strings.TrimSpace(u.Proxy); p != "" {
		// 已在 config.Validate 校验过，这里解析失败只可能是被绕过配置层，
		// 此时宁可回落到环境变量也不要 panic。
		if pu, err := url.Parse(p); err == nil {
			proxy = proxyWithBypass(pu, u.NoProxy)
		}
	}
	return NewRouter(newTransport(maxConns, tc, proxy, onH2Err), maxConns, tc, u.NoProxy,
		u.MaxProxyTransports, onH2Err, onProxyEvict)
}

// 内置默认值：与这些参数被暴露为配置项之前的行为保持一致。
//
// 保留一组内置默认（而不是要求全部显式配置）的目的，是让零值的
// config.Transport —— 手工构造、测试用例 —— 不会静默改变传输层行为。
// 唯一的例外是 h2 健康检查：它的零值表示「关闭」，方向上是安全的，
// 原因见 config.Transport.H2IdleTimeout。
const (
	defDialTimeout         = 5 * time.Second
	defKeepAlive           = 30 * time.Second
	defIdleConnTimeout     = 90 * time.Second
	defTLSHandshakeTimeout = 10 * time.Second
	defBufSize             = 64 << 10
	defH2PingTimeout       = 20 * time.Second
)

// newTransport 构建 transport，代理策略与传输层参数由入参决定。
//
// 每个代理都要一个独立实例，不能用 Clone()：ConfigureTransports 注册的
// TLSNextProto 处理器会捕获一个共享的 http2.Transport，而后者按 authority
// 池化连接。Clone 出来的多个代理 transport 共享该池，会把「经代理 A 建立的
// h2 连接」交给声明走代理 B 的请求 —— 表现为流量静默走错代理。
func newTransport(
	maxConns int, tc config.Transport, proxy func(*http.Request) (*url.URL, error),
	onH2Err H2ErrorReporter,
) *http.Transport {
	if maxConns <= 0 {
		maxConns = 1024
	}
	d := newDialer(tc)

	t := &http.Transport{
		Proxy:               proxy,
		DialContext:         d.DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        maxConns * 4,
		MaxIdleConnsPerHost: maxConns,
		// 0 = 不限制。主要约束 HTTP/1.1 通道，h2 下连接数由流并发上限决定。
		MaxConnsPerHost:       tc.MaxConnsPerHost,
		IdleConnTimeout:       orDur(tc.IdleConnTimeout, defIdleConnTimeout),
		TLSHandshakeTimeout:   orDur(tc.TLSHandshakeTimeout, defTLSHandshakeTimeout),
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		WriteBufferSize:       orInt(tc.WriteBufferSize, defBufSize),
		ReadBufferSize:        orInt(tc.ReadBufferSize, defBufSize),
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	// h2 健康检查。取值必须排在应用层看门狗之后，理由见
	// config.Transport.H2IdleTimeout：PING 无应答时关掉的是**整条**连接，
	// 其上所有复用中的流会一起失败。默认值由 config.normalizeTransport
	// 依非流式首字节预算推导。
	applyH2HealthCheck(t, tc, onH2Err)
	return t
}

// newDialer 按配置构建拨号器。
//
// 单独成函数是为了可测：*http.Transport 不保留 Dialer，
// Timeout 与 KeepAlive 两个值配完之后读不回来，只有把构造这一步
// 暴露出来，测试才能断言它们真的被写进去了。
func newDialer(tc config.Transport) *net.Dialer {
	return &net.Dialer{
		Timeout:   orDur(tc.DialTimeout, defDialTimeout),
		KeepAlive: orDur(tc.KeepAlive, defKeepAlive),
	}
}

// applyH2HealthCheck 把 h2 健康检查参数与连接级事件出口写到 transport 上。
//
// 单独成函数是为了**可测**：这几个值无法从 *http.Transport 读回
// （ConfigureTransports 的返回值才是唯一句柄），只有把赋值这一步暴露出来，
// 测试才能断言「配置真的生效了」。否则这类参数「改了没效果」
// 在单测里完全不可见 —— 而这正是本次把它们配置化的起因。
//
// x/net/http2 的语义是 ReadIdleTimeout == 0 即关闭 PING，因此配置里的 0
// 直接落到「不启用」。返回 nil 表示 h2 未配置成功（连接按 HTTP/1.1 处理）。
//
// onH2Err 无条件接上（即使 PING 被关掉）：CountError 报的不只是 PING 失败，
// 还有 GOAWAY 与读帧错误，那几类与健康检查开关无关。
func applyH2HealthCheck(t *http.Transport, tc config.Transport, onH2Err H2ErrorReporter) *http2.Transport {
	h2, err := http2.ConfigureTransports(t)
	if err != nil || h2 == nil {
		return nil
	}
	h2.ReadIdleTimeout = tc.H2IdleTimeout
	h2.PingTimeout = orDur(tc.H2PingTimeout, defH2PingTimeout)
	// x/net/http2 在**每次建连**时读这个字段（newClientConn），所以在此赋值
	// 即可覆盖此后建立的所有连接，无需在建连路径上再做手脚。
	h2.CountError = onH2Err
	return h2
}

// Client 包装 http.Client，提供带首字节与空闲超时的请求执行。
type Client struct {
	hc *http.Client
	// firstByteTimeout 用于**流式**请求：等响应头 ≈ 等首 token。
	firstByteTimeout time.Duration
	// nonStreamFirstByte 用于**非流式**请求：上游要先跑完整段生成才发
	// 响应头，所以这个值实质是整段生成时长的上限。两者不能混用。
	nonStreamFirstByte time.Duration
	// totalTimeout 是非流式请求的总时长上限。
	totalTimeout time.Duration
	// streamTimeout 是流式请求的总时长兜底。
	streamTimeout time.Duration
}

// NewClient 创建上游客户端。
func NewClient(t http.RoundTripper, u config.Upstream) *Client {
	return &Client{
		hc: &http.Client{
			Transport: t,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		firstByteTimeout:   orDur(u.FirstByteTimeout, 30*time.Second),
		nonStreamFirstByte: orDur(u.FirstByteTimeoutNonStream, 300*time.Second),
		totalTimeout:       orDur(u.Timeout, 600*time.Second),
		streamTimeout:      orDur(u.StreamTimeout, 30*time.Minute),
	}
}

// limitsFor 返回该请求类型实际生效的（首字节上限, 总时长上限）。
//
// 两者都随 stream 切换，理由见 config.Upstream 中两个 FirstByte 字段的说明：
// 非流式的「首字节」其实是整段生成的上限，流式的则是 TTFT，量级差一个数量级，
// 共用一个值必然二选一地错。
//
// 零值回落只可能出现在手工构造的 Client 上（配置加载路径一定已填默认值），
// 保留它是为了让「只配了一个值」的形态不会退化成无限等待。
func (c *Client) limitsFor(stream bool) (firstByte, total time.Duration) {
	if stream {
		fb, t := c.firstByteTimeout, c.streamTimeout
		if fb <= 0 {
			fb = 30 * time.Second
		}
		if t <= 0 {
			t = 30 * time.Minute
		}
		return fb, t
	}
	fb, t := c.nonStreamFirstByte, c.totalTimeout
	if fb <= 0 {
		fb = c.firstByteTimeout
	}
	if t <= 0 {
		t = 10 * time.Minute
	}
	return fb, t
}

// Do 发起上游请求。返回的 cancel 必须在响应体读取完毕后调用。
//
// 返回的 Timing 记录各阶段耗时，**即便返回错误也有效** —— 超时场景下
// 它才是最有价值的信息：wrote_request 与 limit 一对照即可判定请求
// 到底有没有发出去。
//
// 首字节超时通过一个「拿到响应头即解除」的看门狗实现：http.Client.Do 在
// 收到响应头后即返回，因此 Do 返回前用首字节上限约束，返回后切换到总超时，
// 无需引入 httptrace 之外的超时机制。
//
// 看门狗与 Do 返回之间存在天然竞态（定时器已进入回调，此时 Do 恰好返回），
// 若不处理会误杀一个已经建立的流式连接。这里用 CAS 保证「解除」与「触发」
// 二者只有一个生效。
func (c *Client) Do(
	ctx context.Context, req *http.Request, stream bool,
) (*http.Response, context.CancelFunc, Timing, error) {
	firstByte, total := c.limitsFor(stream)
	reqCtx, cancel := context.WithTimeout(ctx, total)
	start := time.Now()

	// httptrace 的回调由传输层的多个 goroutine 调用（WroteRequest 在写循环、
	// GotFirstResponseByte 在读循环、GotConn 在连接获取路径），与 Do 返回
	// 所在的 goroutine 并非同一个，因此 Timing 必须加锁 ——
	// 这与下面看门狗的竞态是同源问题，都出在「跨 goroutine 共享现场」。
	var (
		tmu  sync.Mutex
		tm   Timing
		alpn string
	)

	trace := &httptrace.ClientTrace{
		DNSDone: func(httptrace.DNSDoneInfo) {
			tmu.Lock()
			if tm.DNS == 0 {
				tm.DNS = time.Since(start)
			}
			tmu.Unlock()
		},
		ConnectDone: func(_, _ string, _ error) {
			// 多地址拨号会回调多次，取首次：最后一次往往是最慢/失败的那次，
			// 用它衡量建连成本会把偶发的地址回退误报成常态。
			tmu.Lock()
			if tm.Connect == 0 {
				tm.Connect = time.Since(start)
			}
			tmu.Unlock()
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			tmu.Lock()
			if tm.TLS == 0 {
				tm.TLS = time.Since(start)
			}
			tmu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			p := protoFromConn(info.Conn)
			tmu.Lock()
			tm.GotConn = true
			tm.Reused = info.Reused
			if alpn == "" {
				alpn = p
			}
			tmu.Unlock()
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			tmu.Lock()
			if tm.WroteRequest == 0 {
				tm.WroteRequest = time.Since(start)
			}
			tmu.Unlock()
		},
		GotFirstResponseByte: func() {
			tmu.Lock()
			if tm.FirstByte == 0 {
				tm.FirstByte = time.Since(start)
			}
			tmu.Unlock()
		},
	}

	var settled atomic.Bool
	timer := time.AfterFunc(firstByte, func() {
		if settled.CompareAndSwap(false, true) {
			cancel() // 首字节超时：主动断开
		}
	})

	resp, err := c.hc.Do(req.WithContext(httptrace.WithClientTrace(reqCtx, trace)))
	timer.Stop()

	tmu.Lock()
	out := tm
	outAlpn := alpn
	tmu.Unlock()
	// 有响应时以传输层给出的版本为准，没有（超时/连接失败）才退回 ALPN 结果。
	respProto := ""
	if resp != nil {
		respProto = resp.Proto
	}
	out.Proto = protoLabel(respProto, outAlpn)

	// 抢在看门狗之前解除；抢不到说明已被判定为首字节超时。
	if !settled.CompareAndSwap(false, true) {
		if resp != nil {
			resp.Body.Close()
		}
		// 不返回裸的 context.DeadlineExceeded：它是全局单例，落到看板上
		// 只有一句 "context deadline exceeded"，看不出超的是首字节还是
		// 总时长、阈值多少、哪个上游。这些信息此刻全在手上。
		return nil, func() {}, out, &TimeoutError{
			Kind:    TimeoutKindFirstByte,
			Limit:   firstByte,
			Elapsed: time.Since(start),
			URL:     safeURL(req.URL),
			Stream:  stream,
		}
	}

	if err != nil {
		cancel()
		return nil, func() {}, out, c.describe(err, req, start, total, stream)
	}
	return resp, cancel, out, nil
}

// describe 给传输层错误补上上下文。
//
// 只对超时做加工：*url.Error 已经带上了方法与 URL，而 deadline 类错误
// 的最内层是无信息的哨兵值，必须由这里补出阈值与耗时。
//
// 区分两种 deadline 来源：本客户端设的总超时，与调用方 ctx 自带的 deadline
// （父 ctx 更早到期时）。后者的 Limit 记为实际耗时，避免上报一个
// 与现场不符的阈值把排查引向网关配置。
func (c *Client) describe(err error, req *http.Request, start time.Time, total time.Duration, stream bool) error {
	if !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	elapsed := time.Since(start)
	limit := total
	if elapsed < total {
		// 没到本客户端的上限就超时了，说明 deadline 来自父 ctx。
		limit = elapsed
	}
	return &TimeoutError{
		Kind:    TimeoutKindTotal,
		Limit:   limit,
		Elapsed: elapsed,
		URL:     safeURL(req.URL),
		Stream:  stream,
	}
}

func orDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}

// orInt 是或运算的整数版：0 视为「未配置」。
//
// 只用于缓冲这类 0 无独立语义的参数；MaxConnsPerHost 那种
// 「0 = 不限制」的取值不能过这里，否则会被替换成默认值。
func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}
