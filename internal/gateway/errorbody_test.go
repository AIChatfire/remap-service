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

	// 新增校验：gateway.upstream.status_code 应记录上游的 429
	if got := attrInt(span, "gateway.upstream.status_code"); got != http.StatusTooManyRequests {
		t.Errorf("gateway.upstream.status_code 应为 429，实际 %d", got)
	}

	// 新增校验：gateway.upstream.path 应在 span 属性中
	if path := attrString(span, "gateway.upstream.path"); path == "" {
		t.Error("gateway.upstream.path 应在 span 属性中，实际为空")
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

// 返回 0 表示属性不存在或确实为 0。需要区分两者时，先用
// findSpanWithAttr 确认属性存在。
func attrInt(s sdktrace.ReadOnlySpan, key string) int {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return int(kv.Value.AsInt64())
		}
	}
	return 0
}

func attrString(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// hasAttr 报告 span 上是否存在该属性，用于断言「不该出现的属性确实没出现」。
//
// 布尔属性一律用本函数断言，不要重新引入 attrBool：读值版对「属性不存在」
// 与「属性为 false」返回同一个 false，两种形态分不开 —— body_sanitized
// 移除后正是靠 hasAttr 才锁住「键彻底消失」而非「恒为 false」。
func hasAttr(s sdktrace.ReadOnlySpan, key string) bool {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}

// 超过 MAX_SANITIZE_BYTES 且本次需要脱敏 → **直接 400，绝不原文直通**。
//
// 这条守的是「静默泄漏」这个失效形态：旧实现放弃脱敏、把上游原文原样转发，
// 响应里带着真实上游模型名而客户端毫不知情。现在宁可给一个明确的失败 ——
// 一次可观测、可重现的失败，好过一次不可观测的泄漏。
func TestOversizeResponseIsRejectedNotLeaked(t *testing.T) {
	// 正文里带上游真实模型名，正是脱敏本该抹掉的那种东西。
	upstreamBody := `{"error":{"message":"model deepseek-v3-0324 not found","padding":"` +
		strings.Repeat("x", 256) + `"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(upstreamBody))
	}, func(c *config.Config) {
		// 压到远小于正文，强制超限。
		c.Limits.MaxSanitizeBytes = 64
	})

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()
	// 400 而不是 404：上游是正常返回的，是网关基于策略拒发。记成 5xx 会把
	// 责任推给上游，污染上游侧的错误率。
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("超限且需要脱敏时应返回 400，实际 %d", resp.StatusCode)
	}

	clientBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取客户端正文失败: %v", err)
	}
	// 核心断言：上游正文一个字节都不能漏出去。
	if strings.Contains(string(clientBody), "deepseek-v3-0324") {
		t.Errorf("客户端拿到了未脱敏的上游正文，泄漏: %q", clientBody)
	}
	if strings.Contains(string(clientBody), "padding") {
		t.Errorf("客户端拿到了上游正文片段: %q", clientBody)
	}

	span := findSpanWithAttr(t, sr, obs.AttrErrKind)
	if got := attrString(span, obs.AttrErrKind); got != "response_too_large" {
		t.Errorf("error.kind 应为 response_too_large，实际 %q", got)
	}
	// 上游真实状态码必须留下：换成 400 后若不记，看板上「上游本来返回了什么」
	// 就永久丢失，无从判断这是上游故障还是网关策略。
	if got := attrInt(span, obs.AttrUpstreamStatus); got != http.StatusNotFound {
		t.Errorf("gateway.upstream.status_code 应为上游的 404，实际 %d", got)
	}
	// 上游错误正文的原始片段仍要上报 —— 只进看板，绝不进客户端。
	if got := attrString(span, obs.AttrErrBody); !strings.Contains(got, "deepseek-v3-0324") {
		t.Errorf("看板应拿到上游错误正文原文，实际 %q", got)
	}
}

// 不需要脱敏时上限只是内存护栏，仍然透传。
//
// 不能把这类响应也打成 400：脱敏关闭（或本次请求没有 model 字段）时本来
// 就不执行任何替换，根本不存在泄漏面，报 400 只会让 SANITIZE_OFF 的部署
// 在遇到大响应时整类失败。
func TestOversizeResponseStillPassesWhenSanitizeNotNeeded(t *testing.T) {
	upstreamBody := `{"error":{"message":"boom"},"padding":"` +
		strings.Repeat("x", 256) + `"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(upstreamBody))
	}, func(c *config.Config) {
		c.Sanitize.Off = true
		c.Limits.MaxSanitizeBytes = 64
	})

	resp := post(t, gs, "/v1/chat/completions", chatBody, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("不需要脱敏时应透传上游状态，实际 %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取客户端正文失败: %v", err)
	}
	if string(got) != upstreamBody {
		t.Errorf("不需要脱敏时应原样透传，实际 %q", got)
	}
}

// Content-Length 未知（chunked）时只能在读满才发现超限，行为必须与提前判定一致。
//
// 两条判定路径是分开实现的（提前看 Content-Length / 读完发现超限），
// 只测其中一条时另一条写错照样全绿。
func TestOversizeResponseRejectedOnChunkedBody(t *testing.T) {
	const upstreamBody = `{"model":"deepseek-v3-0324","choices":[]}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// 先 flush 一次，迫使响应走 chunked、Content-Length 为 -1。
		w.(http.Flusher).Flush()
		_, _ = w.Write([]byte(upstreamBody))
	}, func(c *config.Config) {
		c.Limits.MaxSanitizeBytes = 8
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("chunked 超限同样应返回 400，实际 %d", resp.StatusCode)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取客户端正文失败: %v", err)
	}
	if strings.Contains(string(got), "deepseek-v3-0324") {
		t.Errorf("客户端拿到了未脱敏正文，泄漏: %q", got)
	}

	span := findSpanWithAttr(t, sr, obs.AttrErrKind)
	if got := attrString(span, obs.AttrErrKind); got != "response_too_large" {
		t.Errorf("error.kind 应为 response_too_large，实际 %q", got)
	}
	// 上游是 200，这条路径不读正文上报：那是一段模型生成内容。
	if hasAttr(span, obs.AttrErrBody) {
		t.Error("正常响应的正文不应被搬到看板上")
	}
}

// 核心分工守卫：客户端收到的是脱敏版，看板收到的是上游原文。
//
// 两者必须同时断言。只查上报侧，「脱敏彻底失效、两边都是原文」也能通过；
// 只查客户端侧，「上报侧也被脱敏、看板查不到真实上游模型」同样能通过。
func TestSanitizedErrorBodyHasNoFlag(t *testing.T) {
	// 正文带上游真实模型名 primary-up：脱敏会把它换成对外名 pub，
	// 于是这一个字符串就能区分两侧拿到的是哪个版本。
	const upstreamBody = `{"error":{"message":"rate limited on primary-up","type":"rate_limit"}}`

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(upstreamBody))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()

	clientBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取客户端正文失败: %v", err)
	}
	if strings.Contains(string(clientBody), "primary-up") {
		t.Errorf("客户端正文泄漏上游模型名，脱敏失效: %q", clientBody)
	}

	span := findSpanWithAttr(t, sr, "gateway.error.body")
	if got := attrString(span, "gateway.error.body"); !strings.Contains(got, "primary-up") {
		t.Errorf("看板正文应为上游原文（含 primary-up），实际 %q", got)
	}
	if hasAttr(span, "gateway.error.body_sanitized") {
		t.Error("body_sanitized 标记已移除，不应再出现")
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
