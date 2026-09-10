// Package upstream 提供面向高并发的上游 HTTP 客户端。
//
// 连接池参数不对外暴露，全部由 config.Limits.MaxConns 一个数字推导：
// 该值即「单上游主机的空闲连接上限」，全局上限按 4 倍放宽以容纳多协议端点。
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

// NewTransport 依据连接上限构建可高度复用的 transport。
//
// 代理取自 HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。显式配置与
// per-request 代理由 Router 处理，见 proxy.go。
func NewTransport(maxConns int) *http.Transport {
	return newTransport(maxConns, http.ProxyFromEnvironment)
}

// NewRoundTripper 是给 main 用的统一入口：按配置决定默认出口，
// 再包一层 Router 以支持 per-request 的 X-Upstream-Proxy。
//
// 默认出口的代理来源：UPSTREAM_PROXY 非空则固定走它，否则回落到
// HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。
//
// 返回值同时满足 http.RoundTripper 与 CloseIdleConnections，
// 可直接交给 NewClient 并在退出时清理。
func NewRoundTripper(maxConns int, u config.Upstream) *Router {
	proxy := http.ProxyFromEnvironment
	if p := strings.TrimSpace(u.Proxy); p != "" {
		// 已在 config.Validate 校验过，这里解析失败只可能是被绕过配置层，
		// 此时宁可回落到环境变量也不要 panic。
		if pu, err := url.Parse(p); err == nil {
			proxy = proxyWithBypass(pu, u.NoProxy)
		}
	}
	return NewRouter(newTransport(maxConns, proxy), maxConns, u.NoProxy)
}

// newTransport 构建 transport，代理策略由入参决定。
//
// 每个代理都要一个独立实例，不能用 Clone()：ConfigureTransports 注册的
// TLSNextProto 处理器会捕获一个共享的 http2.Transport，而后者按 authority
// 池化连接。Clone 出来的多个代理 transport 共享该池，会把「经代理 A 建立的
// h2 连接」交给声明走代理 B 的请求 —— 表现为流量静默走错代理。
func newTransport(maxConns int, proxy func(*http.Request) (*url.URL, error)) *http.Transport {
	if maxConns <= 0 {
		maxConns = 1024
	}
	d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}

	t := &http.Transport{
		Proxy:                 proxy,
		DialContext:           d.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          maxConns * 4,
		MaxIdleConnsPerHost:   maxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		WriteBufferSize:       64 << 10,
		ReadBufferSize:        64 << 10,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	if h2, err := http2.ConfigureTransports(t); err == nil && h2 != nil {
		h2.ReadIdleTimeout = 30 * time.Second
		h2.PingTimeout = 15 * time.Second
	}
	return t
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
