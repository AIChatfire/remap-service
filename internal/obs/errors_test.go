package obs

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// newRecordedSpan 返回一个真实录制的 span 及取回其快照的闭包。
// 用 SDK 的 SpanRecorder 而非自造 mock：属性去重、状态覆盖等语义
// 都由 SDK 决定，自己写 mock 容易验证出一个并不存在的行为。
func newRecordedSpan(t *testing.T) (trace.Span, func() sdktrace.ReadOnlySpan) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	_, span := tp.Tracer("test").Start(t.Context(), "op")
	return span, func() sdktrace.ReadOnlySpan {
		span.End()
		ended := sr.Ended()
		if len(ended) != 1 {
			t.Fatalf("期望 1 个 span，实际 %d", len(ended))
		}
		return ended[0]
	}
}

func attrOf(t *testing.T, s sdktrace.ReadOnlySpan, key string) (attribute.Value, bool) {
	t.Helper()
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

// 上游 4xx 的原始正文必须同时进属性和 status 描述：属性用于筛选，
// 描述让 trace 列表不展开就能看到失败原因。
func TestRecordUpstreamErrorCarriesRawBody(t *testing.T) {
	span, done := newRecordedSpan(t)
	body := []byte(`{"error":{"message":"model deepseek-v3 not found","code":"invalid_request"}}`)
	RecordUpstreamError(span, 404, "application/json", body, 1024)
	s := done()

	got, ok := attrOf(t, s, AttrErrBody)
	if !ok {
		t.Fatal("正文属性缺失，错误原因会在看板上丢失")
	}
	if got.AsString() != string(body) {
		t.Errorf("正文被改写\n want %s\n got  %s", body, got.AsString())
	}
	if size, ok := attrOf(t, s, AttrErrBodySize); !ok || size.AsInt64() != int64(len(body)) {
		t.Errorf("body_size 应为 %d，实际 %v", len(body), size.AsInt64())
	}
	if ct, ok := attrOf(t, s, AttrErrContentType); !ok || ct.AsString() != "application/json" {
		t.Error("Content-Type 缺失，无法区分上游应用层报错与被反代拦截")
	}
	if _, ok := attrOf(t, s, AttrErrTruncated); ok {
		t.Error("未截断时不应出现 truncated 标记")
	}
	if s.Status().Code != codes.Error {
		t.Error("上游 4xx 必须标红")
	}
	if !strings.Contains(s.Status().Description, "model deepseek-v3 not found") {
		t.Errorf("status 描述未含正文: %q", s.Status().Description)
	}
	if !strings.Contains(s.Status().Description, "404") {
		t.Errorf("status 描述未含状态码: %q", s.Status().Description)
	}
}

// 中文错误消息被按字节截断会切出半个字符，在看板上显示为乱码。
func TestRecordUpstreamErrorTruncatesOnRuneBoundary(t *testing.T) {
	span, done := newRecordedSpan(t)
	body := []byte("请求参数不合法：模型名称未注册")
	limit := 10 // 落在某个 3 字节汉字中间
	RecordUpstreamError(span, 400, "application/json", body, limit)
	s := done()

	got, _ := attrOf(t, s, AttrErrBody)
	frag := got.AsString()
	if len(frag) > limit {
		t.Errorf("片段 %d 字节，超过上限 %d", len(frag), limit)
	}
	if !utf8Valid(frag) {
		t.Errorf("截断产生了非法 UTF-8: %q", frag)
	}
	if tr, ok := attrOf(t, s, AttrErrTruncated); !ok || !tr.AsBool() {
		t.Error("截断后必须标记 truncated，否则会把片段误读成全文")
	}
	if size, _ := attrOf(t, s, AttrErrBodySize); size.AsInt64() != int64(len(body)) {
		t.Error("body_size 必须是原始长度，用于判断截断幅度")
	}
}

// limit<=0 表示运维显式关闭正文上报，此时只能留元信息，不能泄漏正文。
func TestRecordUpstreamErrorLimitZeroOmitsBody(t *testing.T) {
	span, done := newRecordedSpan(t)
	body := []byte(`{"error":"secret detail"}`)
	RecordUpstreamError(span, 500, "application/json", body, 0)
	s := done()

	if _, ok := attrOf(t, s, AttrErrBody); ok {
		t.Error("limit=0 时不应上报正文")
	}
	if strings.Contains(s.Status().Description, "secret") {
		t.Errorf("正文经 status 描述泄漏: %q", s.Status().Description)
	}
	if size, ok := attrOf(t, s, AttrErrBodySize); !ok || size.AsInt64() != int64(len(body)) {
		t.Error("即便不上报正文，也应保留 body_size 以判断上游是否返回了内容")
	}
	if s.Status().Code != codes.Error {
		t.Error("状态仍须标红")
	}
}

// RecordErrorBody 只补正文，状态留给随后的 RecordError 决定 ——
// 否则读正文失败时的 span 描述会被正文片段盖掉真正的 Go 错误。
func TestRecordErrorBodyLeavesStatusToRecordError(t *testing.T) {
	span, done := newRecordedSpan(t)
	frag := RecordErrorBody(span, "application/json", []byte(`{"error":"half"}`), 1024)
	if frag != `{"error":"half"}` {
		t.Errorf("应返回上报的片段，实际 %q", frag)
	}
	RecordError(span, "read_upstream_failed", errors.New("unexpected EOF"))
	s := done()

	if _, ok := attrOf(t, s, AttrErrBody); !ok {
		t.Error("已读前缀应保留，它常是上游错误 JSON 的开头")
	}
	if s.Status().Description != "unexpected EOF" {
		t.Errorf("status 应由 RecordError 决定，实际 %q", s.Status().Description)
	}
	if kind, _ := attrOf(t, s, AttrErrKind); kind.AsString() != "read_upstream_failed" {
		t.Errorf("kind 应为 read_upstream_failed，实际 %q", kind.AsString())
	}
	// exception 事件是 Logfire 异常面板的数据来源，只 SetStatus 会让它空着。
	var hasException bool
	for _, e := range s.Events() {
		if e.Name == "exception" {
			hasException = true
		}
	}
	if !hasException {
		t.Error("缺少 exception 事件，Logfire 异常面板会为空")
	}
}

// 空正文不应产生任何属性，避免看板上出现一堆空值字段。
func TestRecordErrorBodyEmptyIsNoop(t *testing.T) {
	span, done := newRecordedSpan(t)
	if frag := RecordErrorBody(span, "application/json", nil, 1024); frag != "" {
		t.Errorf("空正文应返回空串，实际 %q", frag)
	}
	s := done()
	if len(s.Attributes()) != 0 {
		t.Errorf("空正文不应写属性，实际 %v", s.Attributes())
	}
}

// 传输层错误是层层包裹的，最外层只说「哪个请求失败了」。
// 根因必须单独上报，否则看板上只有 "Post https://... : ..." 而看不到
// 到底是 connection refused、DNS 解析失败还是 TLS 握手失败。
func TestRecordErrorReportsRootCause(t *testing.T) {
	root := errors.New("connect: connection refused")
	wrapped := &url.Error{
		Op:  "Post",
		URL: "https://upstream.example/v1/chat/completions",
		Err: &net.OpError{Op: "dial", Net: "tcp", Err: root},
	}

	span, done := newRecordedSpan(t)
	RecordError(span, "upstream_unreachable", wrapped)
	s := done()

	detail, ok := attrOf(t, s, AttrErrDetail)
	if !ok {
		t.Fatal("缺少完整错误串属性")
	}
	if !strings.Contains(detail.AsString(), "upstream.example") {
		t.Errorf("完整串应含请求 URL，实际 %q", detail.AsString())
	}
	cause, ok := attrOf(t, s, AttrErrCause)
	if !ok {
		t.Fatal("缺少根因属性，真正的失败原因会被包装层埋掉")
	}
	if cause.AsString() != root.Error() {
		t.Errorf("根因应 unwrap 到底，want %q got %q", root.Error(), cause.AsString())
	}
	// 类型信息能区分同一句 "connection refused" 的不同来源。
	if typ, _ := attrOf(t, s, AttrErrType); typ.AsString() != "*url.Error" {
		t.Errorf("错误类型应为 *url.Error，实际 %q", typ.AsString())
	}
}

// 单层错误不该多出一个与完整串重复的根因字段。
func TestRecordErrorSkipsRedundantCause(t *testing.T) {
	span, done := newRecordedSpan(t)
	RecordError(span, "bad_request", errors.New("invalid model"))
	s := done()

	if _, ok := attrOf(t, s, AttrErrCause); ok {
		t.Error("根因与完整串相同时不应上报，避免看板出现重复字段")
	}
}

// 网关自身判定的失败（没有 Go error）也必须带可读原因，
// 否则客户端拿到 4xx/5xx，看板上只有一条无说明的红 span。
func TestRecordGatewayErrorWithoutGoError(t *testing.T) {
	span, done := newRecordedSpan(t)
	RecordGatewayError(span, "unmapped_model", "model `gpt-9` is not available", nil)
	s := done()

	if s.Status().Code != codes.Error {
		t.Errorf("状态应为 Error，实际 %v", s.Status().Code)
	}
	// 对外消息比 kind 更具体，应当成为描述。
	if s.Status().Description != "model `gpt-9` is not available" {
		t.Errorf("描述应为对外消息，实际 %q", s.Status().Description)
	}
	if msg, _ := attrOf(t, s, AttrErrClientMsg); msg.AsString() != "model `gpt-9` is not available" {
		t.Errorf("缺少对外消息属性，实际 %q", msg.AsString())
	}
	if kind, _ := attrOf(t, s, AttrErrKind); kind.AsString() != "unmapped_model" {
		t.Errorf("kind 应为 unmapped_model，实际 %q", kind.AsString())
	}
}

// 有 Go error 时，对外消息与真实原因必须并存：
// 网关把多种底层原因归一成一句 502，只留对外消息等于丢掉根因。
func TestRecordGatewayErrorKeepsBothMessages(t *testing.T) {
	span, done := newRecordedSpan(t)
	RecordGatewayError(span, "upstream_unreachable", "upstream connection failed",
		&url.Error{Op: "Post", URL: "https://up.example", Err: errors.New("no such host")})
	s := done()

	if msg, ok := attrOf(t, s, AttrErrClientMsg); !ok || msg.AsString() != "upstream connection failed" {
		t.Error("对外消息应保留，用于对照客户端看到的 502")
	}
	detail, ok := attrOf(t, s, AttrErrDetail)
	if !ok || !strings.Contains(detail.AsString(), "no such host") {
		t.Errorf("真实原因必须上报，实际 %q", detail.AsString())
	}
	if s.Status().Description == "upstream connection failed" {
		t.Error("描述应为真实错误串，而非归类后的对外消息")
	}
}

// nil span 与 nil error 必须安全：调用方不该在每个错误分支上包 if。
func TestRecordErrorNilSafe(t *testing.T) {
	RecordError(nil, "k", errors.New("x"))
	RecordGatewayError(nil, "k", "m", nil)
	RecordUpstreamError(nil, 500, "application/json", []byte("x"), 1024)

	span, done := newRecordedSpan(t)
	RecordError(span, "no_upstream", nil)
	s := done()
	// 没有 Go error 时，kind 至少要进描述。
	if s.Status().Description != "no_upstream" {
		t.Errorf("nil error 时描述应回落到 kind，实际 %q", s.Status().Description)
	}
}

func utf8Valid(s string) bool {
	for _, r := range s {
		if r == '\uFFFD' {
			return false
		}
	}
	return true
}
