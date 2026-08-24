package protocol

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/betterme/remap-service/internal/sanitize"
)

type testTransformer struct {
	spec   *Spec
	rep    *sanitize.Replacer
	public string
}

func (t testTransformer) Data(p []byte) ([]byte, bool) {
	return t.spec.Sanitize(p, t.public, t.rep)
}

func newTr(t testing.TB, path string) testTransformer {
	t.Helper()
	return testTransformer{
		spec:   Detect(path),
		rep:    sanitize.NewRules(nil, nil, nil, 0).For("up", "pub"),
		public: "pub",
	}
}

func TestPipeSSEBasic(t *testing.T) {
	in := strings.Join([]string{
		`data: {"id":"chatcmpl-up-1","model":"up","choices":[{"delta":{"content":"He"}}]}`,
		``,
		`data: {"id":"chatcmpl-up-1","model":"up","choices":[{"delta":{"content":"llo"}}]}`,
		``,
		`data: [DONE]`,
		``,
		``,
	}, "\n")

	var out bytes.Buffer
	stats, err := PipeSSE(&out, strings.NewReader(in), newTr(t, "/v1/chat/completions"), nil)
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()

	if strings.Contains(got, `"model":"up"`) {
		t.Fatalf("上游模型名泄漏:\n%s", got)
	}
	if !strings.Contains(got, `"model":"pub"`) {
		t.Fatalf("未替换为对外模型:\n%s", got)
	}
	if !strings.Contains(got, "data: [DONE]") {
		t.Fatalf("[DONE] 标记丢失:\n%s", got)
	}
	if !strings.Contains(got, `"content":"He"`) || !strings.Contains(got, `"content":"llo"`) {
		t.Fatalf("增量内容丢失或被合并:\n%s", got)
	}
	if stats.Events != 3 {
		t.Errorf("events = %d, want 3", stats.Events)
	}
}

func TestPipeSSEPreservesEventLines(t *testing.T) {
	in := "event: message_start\n" +
		`data: {"type":"message_start","message":{"id":"msg_up1","model":"up"}}` + "\n\n" +
		"event: content_block_delta\n" +
		`data: {"type":"content_block_delta","delta":{"text":"hi"}}` + "\n\n"

	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(in), newTr(t, "/v1/messages"), nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"event: message_start", "event: content_block_delta",
		`"model":"pub"`, `"text":"hi"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("缺少 %q:\n%s", want, got)
		}
	}
}

// 非 data 行原样保留：即便事件名里恰好含上游标识也不改动，
// 避免破坏 SSE 协议语义。
func TestPipeSSELeavesNonDataLinesAlone(t *testing.T) {
	in := "event: up.custom.event\n" +
		"id: up-event-1\n" +
		": comment mentioning up\n" +
		`data: {"model":"up"}` + "\n\n"

	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(in), newTr(t, "/v1/chat/completions"), nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"event: up.custom.event", "id: up-event-1", ": comment mentioning up"} {
		if !strings.Contains(got, want) {
			t.Errorf("非 data 行被改动，缺少 %q:\n%s", want, got)
		}
	}
	if !strings.Contains(got, `"model":"pub"`) {
		t.Errorf("data 行未脱敏:\n%s", got)
	}
}

func TestPipeSSEResponsesEvents(t *testing.T) {
	in := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"resp_up9","model":"up"}}` + "\n\n" +
		"event: response.output_text.delta\n" +
		`data: {"type":"response.output_text.delta","delta":"你好"}` + "\n\n"

	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(in), newTr(t, "/v1/responses"), nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if strings.Contains(got, `"model":"up"`) {
		t.Errorf("responses 事件泄漏:\n%s", got)
	}
	if !strings.Contains(got, `"delta":"你好"`) {
		t.Errorf("UTF-8 增量内容被破坏:\n%s", got)
	}
}

// 生成内容里出现上游模型名时，流式路径同样不得篡改。
func TestPipeSSENeverTouchesContent(t *testing.T) {
	answer := "我用的是 up 模型"
	in := `data: {"model":"up","choices":[{"delta":{"content":"` + answer + `"}}]}` + "\n\n"

	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(in), newTr(t, "/v1/chat/completions"), nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, answer) {
		t.Errorf("生成内容被篡改:\n%s", got)
	}
	if !strings.Contains(got, `"model":"pub"`) {
		t.Errorf("model 字段未脱敏:\n%s", got)
	}
}

func TestPipeSSENoTrailingNewline(t *testing.T) {
	// 上游可能在最后一个事件后直接 EOF，无空行。
	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(`data: {"model":"up"}`),
		newTr(t, "/v1/chat/completions"), nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"model":"pub"`) {
		t.Fatalf("末行未处理: %q", out.String())
	}
}

func TestPipeSSENilTransformer(t *testing.T) {
	in := "data: {\"model\":\"up\"}\n\n"
	var out bytes.Buffer
	if _, err := PipeSSE(&out, strings.NewReader(in), nil, nil); err != nil {
		t.Fatal(err)
	}
	if out.String() != in {
		t.Fatalf("nil transformer 应原样透传:\ngot  %q\nwant %q", out.String(), in)
	}
}

type errReader struct{ n int }

func (e *errReader) Read(p []byte) (int, error) {
	if e.n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	e.n--
	copy(p, "data: x\n")
	return 8, nil
}

func TestPipeSSEUpstreamError(t *testing.T) {
	var out bytes.Buffer
	if _, err := PipeSSE(&out, &errReader{n: 2}, nil, nil); err == nil {
		t.Fatal("上游读取错误应向上传播")
	}
}

func BenchmarkPipeSSE(b *testing.B) {
	var sb strings.Builder
	for i := 0; i < 200; i++ {
		sb.WriteString(`data: {"id":"chatcmpl-up-1","object":"chat.completion.chunk","model":"up","choices":[{"index":0,"delta":{"content":"token"},"finish_reason":null}]}` + "\n\n")
	}
	payload := sb.String()
	tr := newTr(b, "/v1/chat/completions")

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		out.Grow(len(payload) + 4096)
		if _, err := PipeSSE(&out, strings.NewReader(payload), tr, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// 稳态场景：chunk 里已是对外模型名，MayMatch 直接剪枝。
func BenchmarkPipeSSEClean(b *testing.B) {
	var sb strings.Builder
	for i := 0; i < 200; i++ {
		sb.WriteString(`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"pub","choices":[{"index":0,"delta":{"content":"token"},"finish_reason":null}]}` + "\n\n")
	}
	payload := sb.String()
	tr := newTr(b, "/v1/chat/completions")

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		out.Grow(len(payload) + 4096)
		if _, err := PipeSSE(&out, strings.NewReader(payload), tr, nil); err != nil {
			b.Fatal(err)
		}
	}
}
