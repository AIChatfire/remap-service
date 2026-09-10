package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/obs"
)

func testObs(t *testing.T) *obs.Provider {
	t.Helper()
	o, err := obs.New(context.Background(), config.Obs{LogLevel: "error", LogFormat: "text"})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func testCfg() *config.Config {
	return &config.Config{
		Addr:     ":0",
		Upstream: config.Upstream{Base: "https://x.com", AllowBaseHeader: true},
	}
}

func TestHealthAndReady(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))

	// 未 Run 时 readyz 应为 503，避免实例未就绪就被打流量
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("未就绪时 readyz = %d, want 503", w.Code)
	}

	s.ready.Store(true)
	w = httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != 200 {
		t.Errorf("就绪后 readyz = %d", w.Code)
	}

	w = httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("healthz = %d %s", w.Code, w.Body.String())
	}
}

// 网关不做准入鉴权：无凭据的请求也应进入代理链路（由上游判定合法性）。
func TestNoGatewayLevelAuth(t *testing.T) {
	var reached bool
	s := New(testCfg(), nil, testObs(t))
	// 直接替换 /v1 处理器，验证中间件没有拦截
	mux := http.NewServeMux()
	mux.Handle("/v1/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(204)
	}))
	s.http.Handler = recoverMW(testObs(t))(mux)

	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if !reached || w.Code != 204 {
		t.Fatalf("无凭据请求应直达代理链路，code = %d reached = %v", w.Code, reached)
	}
}

func TestRecoverMiddleware(t *testing.T) {
	h := recoverMW(testObs(t))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "gateway_error") {
		t.Errorf("panic 响应格式不符: %s", w.Body.String())
	}
}

func TestUnknownEndpoint(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/not-exist", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unknown endpoint") {
		t.Errorf("404 响应 = %s", w.Body.String())
	}
}

// 流式响应可能持续数分钟，不能设 WriteTimeout。
func TestNoWriteTimeout(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))
	if s.http.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v，会误杀长连接流式响应", s.http.WriteTimeout)
	}
}

// panic 发生在响应头已下发之后时不能再写错误体。
//
// 污染路径很具体：writeJSON 会 WriteHeader(500) 再 Write，而响应头已经发出去
// 之后 WriteHeader 会被 net/http 记为 superfluous 直接忽略，那段 JSON 则被
// **追加**到已下发的正文里 —— 流式转发中最容易踩到（响应头与多个 chunk
// 早已 flush 出去），客户端拿到的是「合法 SSE 后面跟一坨裸 JSON」，
// 而不是一个明确的错误。
func TestPanicAfterHeadersDoesNotCorruptBody(t *testing.T) {
	const streamed = "data: {\"a\":1}\n\n"
	h := recoverMW(testObs(t))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(streamed))
		panic("mid-stream boom")
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil))

	if w.Code != http.StatusOK {
		t.Errorf("已下发的状态码不应被改写成 500，got %d", w.Code)
	}
	if got := w.Body.String(); got != streamed {
		t.Errorf("响应体被污染，got %q want %q", got, streamed)
	}
}

// 反向用例：响应头还没下发时，panic 仍必须回一个干净的 500 JSON。
// 少了它，把 panic 分支整个改成「只记日志」也能让上面那个用例通过。
func TestPanicBeforeHeadersStillWrites500(t *testing.T) {
	h := recoverMW(testObs(t))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "internal gateway error") {
		t.Errorf("响应体 = %s", w.Body.String())
	}
}

// statusWriter 必须完整代理 http.Flusher。
//
// 这不是接口洁癖：gateway.pipeStream 直接做 w.(http.Flusher) 断言，丢了它
// SSE 会静默退化成「攒满缓冲才发」—— 首字节从毫秒涨到秒级，且不报错。
// 断言到「透传到底层 writer」而不是「接口存在」，否则包装方法写成空实现
// 也能过。
func TestStatusWriterKeepsFlusher(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec}

	fl, ok := any(sw).(http.Flusher)
	if !ok {
		t.Fatal("statusWriter 丢掉了 http.Flusher，SSE 会静默退化成缓冲")
	}
	fl.Flush()
	if !rec.Flushed {
		t.Error("Flush 未透传到底层 writer")
	}
	if sw.Unwrap() != http.ResponseWriter(rec) {
		t.Error("Unwrap 未返回底层 writer，http.ResponseController 会失效")
	}
}

// Write 也要置位「已下发」：隐式 200 同样意味着状态码已经定死。
func TestStatusWriterDetectsImplicitHeader(t *testing.T) {
	sw := &statusWriter{ResponseWriter: httptest.NewRecorder()}
	if sw.wrote {
		t.Fatal("初始状态不应为已下发")
	}
	_, _ = sw.Write([]byte("x")) // 不显式调 WriteHeader
	if !sw.wrote {
		t.Error("Write 之后应视为响应头已下发")
	}
}

// 计数指标必须真的注册过。
//
// 字段忘了在 newMetrics 里初始化时它是 nil，而 obs.Add 对 nil 是静默 no-op
// （这是刻意的设计）—— 于是 panic 计数永远显示 0，与「一次都没发生过」
// 完全同形。这条断言守的就是这个「静默失效」。
func TestObservabilityCountersRegistered(t *testing.T) {
	o := testObs(t)
	if o.Metrics.Panics == nil {
		t.Error("gateway.panics 未在 newMetrics 中注册，panic 计数会静默为 0")
	}
	if o.Metrics.SanitizeSuppressed == nil {
		t.Error("gateway.sanitize.suppressed 未注册，抑制信号会静默为 0")
	}
}

// 退出预算：零值必须回落内置默认，而不是退化成「退出时立刻硬断连接」。
// 手工构造 Config（测试、零配置入口）走的就是这条路径。
func TestLifecycleZeroValueDefaults(t *testing.T) {
	var lc config.Lifecycle
	e := lc.Effective()
	if e.ShutdownTimeout != 20*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 20s", e.ShutdownTimeout)
	}
	if e.FlushTimeout != 5*time.Second {
		t.Errorf("FlushTimeout = %v, want 5s", e.FlushTimeout)
	}
	// DrainDelay 的 0 是「不等待」这一有效语义，不能被回落成默认值。
	if e.DrainDelay != 0 {
		t.Errorf("DrainDelay = %v, want 0（0 是有效取值）", e.DrainDelay)
	}
	if got, want := lc.Total(), 25*time.Second; got != want {
		t.Errorf("Total() = %v, want %v", got, want)
	}
}
