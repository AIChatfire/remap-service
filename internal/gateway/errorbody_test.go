package gateway

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/upstream"
)

const chatBody = `{"model":"deepseek-v3","messages":[]}`

// 端到端确认：上游返回的错误正文最终出现在 span 上。
// obs 包的单测只验证了记录函数本身，这里验证网关真的调了它 ——
// 「函数正确但没人调用」正是装配缺陷的典型形态。
func TestUpstreamErrorBodyReachesSpan(t *testing.T) {
	const upstreamBody = `{"error":{"message":"insufficient quota for model deepseek-v3","type":"insufficient_quota"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(upstreamBody))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("状态码应透传，实际 %d", resp.StatusCode)
	}

	span := findSpanWithAttr(t, sr, "gateway.error.body")
	body := attrString(span, "gateway.error.body")
	if body != upstreamBody {
		t.Errorf("span 上的正文与上游不一致\n want %s\n got  %s", upstreamBody, body)
	}
	if !strings.Contains(span.Status().Description, "insufficient quota") {
		t.Errorf("status 描述应含上游原因，实际 %q", span.Status().Description)
	}
	if ct := attrString(span, "gateway.error.content_type"); ct != "application/json" {
		t.Errorf("content_type 应为 application/json，实际 %q", ct)
	}
}

// 上游返回 HTML（常见于被反向代理/WAF 拦截）时同样要如实上报，
// 这类响应体是判断「不是上游应用报错」的唯一线索。
func TestUpstreamHTMLErrorBodyReported(t *testing.T) {
	const html = `<html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(html))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()

	span := findSpanWithAttr(t, sr, "gateway.error.body")
	if got := attrString(span, "gateway.error.body"); got != html {
		t.Errorf("HTML 正文应原样上报，实际 %q", got)
	}
	if ct := attrString(span, "gateway.error.content_type"); ct != "text/html" {
		t.Errorf("content_type 应保留 text/html 以识别被拦截，实际 %q", ct)
	}
}

// 成功响应不应带任何错误正文属性，否则看板会被噪声填满。
func TestSuccessCarriesNoErrorBody(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"1","model":"deepseek-v3"}`))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("期望 200，实际 %d", resp.StatusCode)
	}

	for _, s := range sr.Ended() {
		for _, kv := range s.Attributes() {
			if strings.HasPrefix(string(kv.Key), "gateway.error.") {
				t.Errorf("成功请求出现错误属性 %s=%v", kv.Key, kv.Value.AsString())
			}
		}
	}
}

// OBS_ERROR_BODY_BYTES=0 时正文既不能进属性，也不能经 status 描述泄漏。
func TestErrorBodyDisabledByConfig(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"tenant-identifiable detail"}`))
	}, func(c *config.Config) { c.Obs.ErrorBodyBytes = 0 })

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()

	for _, s := range sr.Ended() {
		for _, kv := range s.Attributes() {
			if string(kv.Key) == "gateway.error.body" {
				t.Error("关闭后仍上报了正文")
			}
		}
		if strings.Contains(s.Status().Description, "tenant-identifiable") {
			t.Errorf("正文经 status 描述泄漏: %q", s.Status().Description)
		}
	}
}

// 流式路径曾是最大的缺口：上游以 text/event-stream 声明却返回 4xx/5xx 时，
// 正文被当作数据流转发，错误原因完全不进 span。
func TestStreamingUpstreamErrorBodyReported(t *testing.T) {
	const upstreamBody = `{"error":{"message":"rate limit exceeded on stream","type":"rate_limit"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		// 关键形态：Content-Type 是 SSE，但状态码是错误。
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(upstreamBody))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"deepseek-v3","messages":[],"stream":true}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("状态码应透传，实际 %d", resp.StatusCode)
	}

	span := findSpanWithAttr(t, sr, "gateway.error.body")
	if got := attrString(span, "gateway.error.body"); got != upstreamBody {
		t.Errorf("流式错误正文应原样上报\n want %s\n got  %s", upstreamBody, got)
	}
	if !strings.Contains(span.Status().Description, "rate limit exceeded") {
		t.Errorf("status 描述应含上游原因，实际 %q", span.Status().Description)
	}
}

// 上游连不上时，看板必须能看到根因（connection refused 等），
// 而不是只有网关归一后的那句 "upstream connection failed"。
func TestUnreachableUpstreamReportsRootCause(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	// 先起再关，得到一个必定连不上的地址。
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	gs, _ := newFixtureWithRecorder(t, sr,
		func(http.ResponseWriter, *http.Request) {},
		func(c *config.Config) { c.Upstream.Base = deadURL })

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()

	span := findSpanWithAttr(t, sr, "gateway.error.detail")
	detail := attrString(span, "gateway.error.detail")
	if !strings.Contains(detail, deadURL) {
		t.Errorf("完整错误串应含目标地址，实际 %q", detail)
	}
	if cause := attrString(span, "gateway.error.cause"); cause == "" {
		t.Error("缺少根因属性，无法区分 refused / DNS / TLS")
	}
	// 对外消息与真实原因并存，才能对照客户端看到的状态码。
	if msg := attrString(span, "gateway.error.client_message"); msg == "" {
		t.Error("缺少对外消息属性")
	}
	if span.Status().Code != codes.Error {
		t.Errorf("span 应标红，实际 %v", span.Status().Code)
	}
}

func findSpanWithAttr(t *testing.T, sr *tracetest.SpanRecorder, key string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range sr.Ended() {
		for _, kv := range s.Attributes() {
			if string(kv.Key) == key {
				return s
			}
		}
	}
	var names []string
	for _, s := range sr.Ended() {
		names = append(names, s.Name())
	}
	t.Fatalf("没有 span 带属性 %s（已结束的 span: %v）—— 记录函数可能未被调用", key, names)
	return nil
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func attrBool(s sdktrace.ReadOnlySpan, key string) bool {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsBool()
		}
	}
	return false
}

// hasAttr 报告 span 上是否存在该属性，用于断言「不该出现的属性确实没出现」。
func hasAttr(s sdktrace.ReadOnlySpan, key string) bool {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}

// 错误正文超过 MAX_SANITIZE_BYTES 时走放弃脱敏的直通路径，上报的正文
// 可能含上游真实模型名。看板必须能筛出这类记录，否则会误以为所有上报
// 正文都已脱敏。
func TestUnsanitizedErrorBodyIsFlagged(t *testing.T) {
	// 正文里带上游真实模型名，正是脱敏本该抹掉、这条路径抹不掉的东西。
	upstreamBody := `{"error":{"message":"model deepseek-v3-0324 not found","padding":"` +
		strings.Repeat("x", 256) + `"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(upstreamBody))
	}, func(c *config.Config) {
		// 压到远小于正文，强制走超限直通。
		c.Limits.MaxSanitizeBytes = 64
	})

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("状态码应透传，实际 %d", resp.StatusCode)
	}

	span := findSpanWithAttr(t, sr, "gateway.error.body_sanitized")
	if attrBool(span, "gateway.error.body_sanitized") {
		t.Error("超限直通未经脱敏，body_sanitized 应为 false")
	}

	// 顺带确认：这条路径上报的正文确实是未脱敏的原文，即标记没有虚报。
	if got := attrString(span, "gateway.error.body"); !strings.Contains(got, "deepseek-v3-0324") {
		t.Errorf("超限直通应上报原文，实际 %q", got)
	}
}

// 反向守卫：正常脱敏路径不该带 body_sanitized 标记，否则标记失去筛选意义。
func TestSanitizedErrorBodyHasNoFlag(t *testing.T) {
	const upstreamBody = `{"error":{"message":"rate limited","type":"rate_limit"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(upstreamBody))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()

	span := findSpanWithAttr(t, sr, "gateway.error.body")
	if hasAttr(span, "gateway.error.body_sanitized") {
		t.Error("正常脱敏路径不应带 body_sanitized 标记")
	}
}

// newFixtureWithRecorder 与 newFixture 等价，额外把 Provider 的 tracer 换成
// 录制器。生产路径下 Provider.Tracer 由 obs.New 装配，这里只替换字段，
// 不为测试在生产代码里开注入口子。
func newFixtureWithRecorder(t *testing.T, sr *tracetest.SpanRecorder, h http.HandlerFunc, tweak func(*config.Config)) (*httptest.Server, *capture) {
	t.Helper()

	cap := &capture{}
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cap.mu.Lock()
		cap.path, cap.body, cap.header, cap.host = r.URL.Path, b, r.Header.Clone(), r.Host
		cap.mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(b))
		h(w, r)
	}))
	t.Cleanup(us.Close)

	cfg := testConfig(us.URL)
	cfg.Obs.ErrorBodyBytes = 8192
	if tweak != nil {
		tweak(cfg)
	}

	// 经 obs.New 构造，让 errBodyBytes 走真实的配置装配路径；
	// 只有 Tracer 字段换成录制器。
	o, err := obs.New(context.Background(), config.Obs{
		LogLevel: "error", LogFormat: "text", LogUpstreamModel: true,
		ErrorBodyBytes: cfg.Obs.ErrorBodyBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	o.Tracer = tp.Tracer("test")

	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	t.Cleanup(tr.CloseIdleConnections)
	gs := httptest.NewServer(New(cfg, upstream.NewClient(tr, cfg.Upstream), o))
	t.Cleanup(gs.Close)
	return gs, cap
}
