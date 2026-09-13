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

func (t testTransformer) Data(p []byte) ([]byte, SanitizeReport) {
	return t.spec.SanitizeWithReport(p, t.public, t.rep)
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

// SanitizeEvent 存在的唯一理由：让**本地构造**的合成事件也走一遍与上游字节
// 相同的脱敏路径。这里直接喂一个「写坏了」的事件 —— model 填的是上游真实名。
// 生产代码不会这么写，但这条路径必须能兜住「后来有人改错了字段」。
func TestSanitizeEventNeutralizesUpstreamModelInSyntheticEvent(t *testing.T) {
	tr := newTr(t, "/v1/chat/completions") // public = "pub"，规则 up -> pub

	bad := []byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\"," +
		"\"model\":\"up\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"}}]}\n\n")

	got := string(SanitizeEvent(bad, tr))

	if strings.Contains(got, `"up"`) {
		t.Errorf("合成事件里的上游真实名没被覆盖掉：%s", got)
	}
	if !strings.Contains(got, `"model":"pub"`) {
		t.Errorf("model 应被整体覆盖为对外名：%s", got)
	}
	// 脱敏不能把帧结构或载荷弄坏 —— 那等于用一个 bug 换掉另一个 bug。
	if !strings.HasPrefix(got, "data: ") || !strings.HasSuffix(got, "\n\n") {
		t.Errorf("SSE 帧结构被破坏：%q", got)
	}
	if !strings.Contains(got, `"content":""`) || !strings.Contains(got, `"object":"chat.completion.chunk"`) {
		t.Errorf("载荷被破坏：%s", got)
	}
}

// 无需改写时必须原样返回同一个切片（不复制），否则「未启用脱敏」这条路径
// 会平白多出分配 —— 而这正是脱敏关闭时「行为与从前逐字节一致」的依据。
func TestSanitizeEventReturnsSameSliceWhenNothingToDo(t *testing.T) {
	tr := newTr(t, "/v1/chat/completions")

	clean := []byte("data: {\"model\":\"pub\",\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\n")
	if got := SanitizeEvent(clean, tr); &got[0] != &clean[0] {
		t.Error("没有需要替换的内容时不应复制")
	}
	if got := SanitizeEvent(clean, nil); &got[0] != &clean[0] {
		t.Error("transformer 为 nil（脱敏未启用）时应原样返回")
	}
	if got := SanitizeEvent(nil, tr); got != nil {
		t.Errorf("nil 输入应原样返回，实际 %q", got)
	}

	// 非 data 行原样保留：Anthropic 的 event: ping 不能被当成载荷处理。
	ping := []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")
	if got := SanitizeEvent(ping, tr); &got[0] != &ping[0] {
		t.Errorf("ping 事件不含任何标识，应原样返回：%q", got)
	}
}

// 合成事件的脱敏是「每个请求一次」（不是逐 chunk），但仍要求稳态零分配：
// 合成事件里本来就是对外名，必须走 MayMatch 剪枝直接返回原切片。
//
// tr 要**提到循环外**并显式存成接口：testTransformer 有 3 个字宽，每次调用点
// 现转接口都会装箱分配一次 —— 那是基准自己的噪声，会把 0 报成 2 allocs/op。
//
// 稳态的 1 alloc/op 不是本函数引入的，而是 Spec.SanitizeWithReport 自身对每个
// 载荷的单价（对照 BenchmarkPipeSSEClean：200 个事件 204 allocs）。
func BenchmarkSanitizeEventClean(b *testing.B) {
	var tr Transformer = newTr(b, "/v1/chat/completions")
	frame := []byte("data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\"," +
		"\"model\":\"pub\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\"},\"finish_reason\":null}]}\n\n")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if out := SanitizeEvent(frame, tr); len(out) == 0 {
			b.Fatal("合成事件不应被清空")
		}
	}
}

// 两条「不该有成本」的路径必须是零分配，否则等于给所有请求（不只是打开开关的
// 那些）都加了开销：
//   - 开关关闭 → earlyFirstEvent 返回 nil → frame 为空；
//   - 脱敏关闭 → tr 为 nil。
//
// 用 AllocsPerRun 而不是基准来守：这是**必须恒为 0** 的契约，不是「大概很低」。
func TestSanitizeEventZeroAllocWhenDisabled(t *testing.T) {
	frame := Preamble("/v1/chat/completions", "pub")
	// 必须显式存成接口变量：newTr 返回的是具体类型，每次调用点现转接口都会
	// 装箱分配一次 —— 那是**测试自己的**噪声，会把「生产路径零分配」误报成
	// 1 alloc/op。生产代码里 tr 本来就是接口变量，只转一次。
	var tr Transformer = newTr(t, "/v1/chat/completions")

	if n := testing.AllocsPerRun(1000, func() { _ = SanitizeEvent(nil, tr) }); n != 0 {
		t.Errorf("开关关闭（无合成事件）时不应有分配，实际 %.1f allocs/op", n)
	}
	if n := testing.AllocsPerRun(1000, func() { _ = SanitizeEvent(frame, nil) }); n != 0 {
		t.Errorf("脱敏关闭时不应有分配，实际 %.1f allocs/op", n)
	}
}
