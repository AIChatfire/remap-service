package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/betterme/remap-service/internal/sanitize"
)

func newRep(t testing.TB, upstream, public string, aliases ...string) *sanitize.Replacer {
	t.Helper()
	var al map[string][]string
	if len(aliases) > 0 {
		al = map[string][]string{upstream: aliases}
	}
	return sanitize.NewRules(al, nil, nil, 0).For(upstream, public)
}

func TestExtractModel(t *testing.T) {
	cases := []struct {
		body string
		want string
		err  bool
	}{
		{`{"model":"gpt-4o","messages":[]}`, "gpt-4o", false},
		{`{"messages":[],"model":"claude-3"}`, "claude-3", false},
		{`{"messages":[]}`, "", true},
		{`{"model":123}`, "", true},
		{`{"model":""}`, "", true},
		{`not json`, "", true},
	}
	for _, c := range cases {
		got, err := ExtractModel([]byte(c.body))
		if (err != nil) != c.err {
			t.Errorf("ExtractModel(%s) err=%v, wantErr=%v", c.body, err, c.err)
			continue
		}
		if got != c.want {
			t.Errorf("ExtractModel(%s) = %q, want %q", c.body, got, c.want)
		}
	}
}

func TestIsStream(t *testing.T) {
	if !IsStream([]byte(`{"stream":true}`)) {
		t.Error("stream:true 应识别为流式")
	}
	if IsStream([]byte(`{"stream":false}`)) || IsStream([]byte(`{}`)) {
		t.Error("非 true 不应为流式")
	}
}

func TestRewriteModel(t *testing.T) {
	in := []byte(`{"model":"deepseek-pro","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	out, err := RewriteModel(in, "deepseek-v3")
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(out, "model").Str; got != "deepseek-v3" {
		t.Fatalf("model = %q", got)
	}
	if gjson.GetBytes(out, "messages.0.content").Str != "hi" {
		t.Fatal("messages 被破坏")
	}
	if !gjson.GetBytes(out, "stream").Bool() {
		t.Fatal("stream 被破坏")
	}
}

// --- 字段级脱敏：按协议路径 ---

func TestSanitizePerProtocol(t *testing.T) {
	t.Run("openai", func(t *testing.T) {
		s := Detect("/v1/chat/completions")
		rep := newRep(t, "up", "pub")
		in := []byte(`{"id":"chatcmpl-up-abc","object":"chat.completion","model":"up","system_fingerprint":"fp_up_01"}`)
		out, changed := s.Sanitize(in, "pub", rep)
		if !changed {
			t.Fatal("应报告发生改动")
		}
		assertField(t, out, "model", "pub")
		assertField(t, out, "id", "chatcmpl-pub-abc")
		assertField(t, out, "system_fingerprint", "fp_pub_01")
	})

	t.Run("anthropic message_start", func(t *testing.T) {
		s := Detect("/v1/messages")
		rep := newRep(t, "up", "pub")
		in := []byte(`{"type":"message_start","message":{"id":"msg_up1","model":"up","role":"assistant"}}`)
		out, _ := s.Sanitize(in, "pub", rep)
		assertField(t, out, "message.model", "pub")
		assertField(t, out, "message.id", "msg_pub1")
	})

	t.Run("responses 流式事件", func(t *testing.T) {
		s := Detect("/v1/responses")
		rep := newRep(t, "up", "pub")
		in := []byte(`{"type":"response.created","response":{"id":"resp_up9","model":"up"}}`)
		out, _ := s.Sanitize(in, "pub", rep)
		assertField(t, out, "response.model", "pub")
		assertField(t, out, "response.id", "resp_pub9")
	})

	t.Run("responses 非流式顶层", func(t *testing.T) {
		s := Detect("/v1/responses")
		rep := newRep(t, "up", "pub")
		in := []byte(`{"id":"resp_up9","model":"up","output":[]}`)
		out, _ := s.Sanitize(in, "pub", rep)
		assertField(t, out, "model", "pub")
		assertField(t, out, "id", "resp_pub9")
	})
}

// 上游返回带版本后缀的变体时，模型字段仍要整体覆盖为对外名。
func TestSanitizeModelFieldAlwaysOverwritten(t *testing.T) {
	s := Detect("/v1/chat/completions")
	// 故意不配别名：模型字段走整体覆盖，不依赖替换器
	rep := newRep(t, "up", "pub")
	in := []byte(`{"model":"up-20260101-preview"}`)
	out, changed := s.Sanitize(in, "pub", rep)
	if !changed {
		t.Fatal("应报告发生改动")
	}
	assertField(t, out, "model", "pub")
}

// --- 核心：两步职责不重叠（对外名与上游名交叉时不产生重复片段）---

// 用户实际报告的组合：public 是 upstream 的超串。
// 步骤 ① 把 model 覆盖成 DeepSeek-V3-20260813 后，若步骤 ② 再扫一遍，
// 会命中其中的 DeepSeek-V3 并二次替换，产出 …-20260813-20260813。
func TestSanitizeModelPathExemptFromSubstringScan(t *testing.T) {
	const (
		upstream = "deepseek-v3"
		public   = "DeepSeek-V3-20260813"
	)
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			s := Detect(path)
			rep := newRep(t, upstream, public)
			for _, mp := range s.ModelPaths {
				in := []byte(`{"` + mp + `":"` + upstream + `"}`)
				if mp == "response.model" || mp == "message.model" {
					in = []byte(`{"` + strings.SplitN(mp, ".", 2)[0] + `":{"model":"` + upstream + `"}}`)
				}
				out, changed := s.Sanitize(in, public, rep)
				if !changed {
					t.Fatalf("%s: 应报告发生改动", mp)
				}
				assertField(t, out, mp, public)
			}
		})
	}
}

// 客户端已传 public 形态时，① 无需改写，② 也不能碰 —— 整体应报告未改动。
func TestSanitizeIdempotentWhenPublicContainsUpstream(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "deepseek-v3", "DeepSeek-V3-20260813")
	in := []byte(`{"model":"DeepSeek-V3-20260813"}`)

	out, changed := s.Sanitize(in, "DeepSeek-V3-20260813", rep)
	assertField(t, out, "model", "DeepSeek-V3-20260813")
	if changed {
		t.Error("值已是最终形态，不应报告改动（会造成上报假信号）")
	}
	// 二次调用必须稳定
	out2, _ := s.Sanitize(out, "DeepSeek-V3-20260813", rep)
	if !bytes.Equal(out, out2) {
		t.Errorf("非幂等:\n  1st %s\n  2nd %s", out, out2)
	}
}

// 非模型路径（id / 错误文本）不在 covered 里，仍须正常替换且只替换一次。
func TestSanitizeNonModelPathsStillReplacedOnce(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "deepseek-v3", "DeepSeek-V3-20260813")
	in := []byte(`{"model":"deepseek-v3","id":"chatcmpl-deepseek-v3-abc",` +
		`"error":{"message":"model deepseek-v3 not found"}}`)

	out, changed := s.Sanitize(in, "DeepSeek-V3-20260813", rep)
	if !changed {
		t.Fatal("应报告发生改动")
	}
	assertField(t, out, "model", "DeepSeek-V3-20260813")
	assertField(t, out, "id", "chatcmpl-DeepSeek-V3-20260813-abc")
	assertField(t, out, "error.message", "model DeepSeek-V3-20260813 not found")
	if n := bytes.Count(out, []byte("20260813")); n != 3 {
		t.Errorf("对外名出现 %d 次，期望 3 次（无重复后缀累积）: %s", n, out)
	}
}

// 反向包含：public 是 upstream 的子串。此时上游名本身以 public 开头，
// 跳过它会让上游形态泄漏，因此必须照常替换。
func TestSanitizeModelPathWhenPublicIsSubstring(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "deepseek-v3-preview", "deepseek-v3")
	in := []byte(`{"model":"deepseek-v3-preview","id":"req-deepseek-v3-preview-1"}`)

	out, _ := s.Sanitize(in, "deepseek-v3", rep)
	assertField(t, out, "model", "deepseek-v3")
	assertField(t, out, "id", "req-deepseek-v3-1")
	if bytes.Contains(out, []byte("preview")) {
		t.Errorf("上游形态泄漏: %s", out)
	}
}

// --- 核心：不篡改模型生成的内容 ---

func TestSanitizeNeverTouchesGeneratedContent(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "deepseek-v3", "deepseek-pro")

	// 用户问「你是什么模型」，回答里出现了上游模型名 —— 必须原样保留
	answer := "我底层使用的是 deepseek-v3 架构，具有强大的推理能力。"
	in := []byte(`{"model":"deepseek-v3","choices":[{"message":{"role":"assistant","content":"` + answer + `"}}]}`)

	out, _ := s.Sanitize(in, "deepseek-pro", rep)
	assertField(t, out, "model", "deepseek-pro")
	if got := gjson.GetBytes(out, "choices.0.message.content").Str; got != answer {
		t.Errorf("模型生成的内容被篡改了:\n  got  %q\n  want %q", got, answer)
	}
}

func TestSanitizeSkipsContentFieldsEvenWhenShort(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "up", "pub")
	// delta / text / content 都很短，但都属于生成内容
	in := []byte(`{"choices":[{"delta":{"content":"up"}}],"text":"up","delta":"up"}`)

	out, _ := s.Sanitize(in, "pub", rep)
	for _, p := range []string{"choices.0.delta.content", "text", "delta"} {
		if got := gjson.GetBytes(out, p).Str; got != "up" {
			t.Errorf("%s = %q，生成内容字段不应被替换", p, got)
		}
	}
}

func TestSanitizeSkipsLongValues(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "up", "pub")

	long := strings.Repeat("x", 300) + " up"
	in := []byte(`{"model":"up","note":"` + long + `"}`)
	out, _ := s.Sanitize(in, "pub", rep)

	assertField(t, out, "model", "pub")
	if got := gjson.GetBytes(out, "note").Str; got != long {
		t.Error("超过长度阈值的字符串被改写了")
	}
}

// 错误体的 message / code / param 需要脱敏，它们是短值且非生成内容。
func TestSanitizeErrorBody(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "deepseek-v3", "deepseek-pro", "deepseek-v3-250101")

	in := []byte(`{"error":{"message":"The model ` + "`deepseek-v3-250101`" + ` is overloaded","type":"rate_limit_error","code":"deepseek-v3_quota","param":"model"}}`)
	out, changed := s.Sanitize(in, "deepseek-pro", rep)
	if !changed {
		t.Fatal("错误体应被脱敏")
	}
	body := string(out)
	if strings.Contains(body, "deepseek-v3-250101") || strings.Contains(body, "deepseek-v3_quota") {
		t.Errorf("错误体仍含上游标识: %s", body)
	}
	assertField(t, out, "error.code", "deepseek-pro_quota")
	if !strings.Contains(gjson.GetBytes(out, "error.message").Str, "deepseek-pro") {
		t.Errorf("error.message 未脱敏: %s", gjson.GetBytes(out, "error.message").Str)
	}
}

// 工具调用与引用注记里的模型标识也要处理（短值、非生成内容字段）。
func TestSanitizeToolAndAnnotation(t *testing.T) {
	s := Detect("/v1/responses")
	rep := newRep(t, "up-model", "pub-model")

	in := []byte(`{"type":"response.output_text.annotation.added","annotation":{"type":"url_citation","source":"up-model-search"},"item_id":"item_up-model_7"}`)
	out, changed := s.Sanitize(in, "pub-model", rep)
	if !changed {
		t.Fatal("注记字段应被脱敏")
	}
	assertField(t, out, "annotation.source", "pub-model-search")
	assertField(t, out, "item_id", "item_pub-model_7")
}

// 数组与深层嵌套都要覆盖到。
func TestSanitizeNestedAndArrays(t *testing.T) {
	s := Detect("/v1/responses")
	rep := newRep(t, "up", "pub")

	in := []byte(`{"output":[{"id":"rs_up_1","type":"reasoning"},{"id":"msg_up_2","tags":["up-a","up-b"]}]}`)
	out, changed := s.Sanitize(in, "pub", rep)
	if !changed {
		t.Fatal("嵌套字段应被脱敏")
	}
	assertField(t, out, "output.0.id", "rs_pub_1")
	assertField(t, out, "output.1.id", "msg_pub_2")
	assertField(t, out, "output.1.tags.0", "pub-a")
	assertField(t, out, "output.1.tags.1", "pub-b")
}

// 无匹配时不应产生任何改动，也不应报告 changed。
func TestSanitizeNoChange(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "up", "pub")

	in := []byte(`{"model":"pub","choices":[{"delta":{"content":"你好"}}]}`)
	out, changed := s.Sanitize(in, "pub", rep)
	if changed {
		t.Error("无需改动时不应报告 changed")
	}
	if string(out) != string(in) {
		t.Errorf("内容被意外修改:\n  got  %s\n  want %s", out, in)
	}
}

// noop 替换器（上游名 == 对外名）时，模型字段的整体覆盖仍要生效。
func TestSanitizeWithNoopReplacer(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := sanitize.NewRules(nil, nil, nil, 0).For("m", "m")
	if !rep.Noop() {
		t.Fatal("前置条件：应为 noop 替换器")
	}
	in := []byte(`{"model":"m-20260101","id":"chatcmpl-m-1"}`)
	out, changed := s.Sanitize(in, "m", rep)
	if !changed {
		t.Fatal("模型字段应被覆盖")
	}
	assertField(t, out, "model", "m")
	assertField(t, out, "id", "chatcmpl-m-1") // noop 时其余字段不动
}

// openai spec 不该去动 message.model —— 避免跨协议误伤用户数据。
func TestSanitizeScopedToProtocol(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := sanitize.NewRules(nil, nil, nil, 0).For("x", "x")
	in := []byte(`{"model":"up","message":{"model":"user-provided-value"}}`)
	out, _ := s.Sanitize(in, "pub", rep)
	assertField(t, out, "model", "pub")
	assertField(t, out, "message.model", "user-provided-value")
}

// 含特殊字符的字段名需要正确转义，否则 sjson 会写错位置。
func TestSanitizeEscapesPathSegments(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newRep(t, "up", "pub")
	in := []byte(`{"meta.info":{"tag":"up"}}`)
	out, changed := s.Sanitize(in, "pub", rep)
	if !changed {
		t.Fatal("应发生改动")
	}
	if got := gjson.GetBytes(out, `meta\.info.tag`).Str; got != "pub" {
		t.Errorf("含点号的字段名未正确处理: %s", out)
	}
}

func TestLooksLikeJSON(t *testing.T) {
	yes := []string{`{"a":1}`, "\n\t{}", ` {"model":"x"}`}
	no := []string{`[DONE]`, `[1,2]`, `plain`, ``, `"str"`}
	for _, s := range yes {
		if !LooksLikeJSON([]byte(s)) {
			t.Errorf("%q 应识别为 JSON", s)
		}
	}
	for _, s := range no {
		if LooksLikeJSON([]byte(s)) {
			t.Errorf("%q 不应识别为 JSON", s)
		}
	}
}

func assertField(t *testing.T, body []byte, path, want string) {
	t.Helper()
	if got := gjson.GetBytes(body, path).Str; got != want {
		t.Errorf("%s = %q, want %q\n  body: %s", path, got, want, body)
	}
}

// 典型 SSE 增量 chunk：不含上游标识，应被 MayMatch 剪枝掉。
func BenchmarkSanitizeMiss(b *testing.B) {
	s := Detect("/v1/chat/completions")
	rep := newRep(b, "deepseek-v3", "deepseek-pro")
	in := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","model":"deepseek-pro","choices":[{"index":0,"delta":{"content":"你好世界"},"finish_reason":null}]}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	for i := 0; i < b.N; i++ {
		_, _ = s.Sanitize(in, "deepseek-pro", rep)
	}
}

func BenchmarkSanitizeHit(b *testing.B) {
	s := Detect("/v1/chat/completions")
	rep := newRep(b, "deepseek-v3", "deepseek-pro")
	in := []byte(`{"id":"chatcmpl-deepseek-v3-1","object":"chat.completion.chunk","model":"deepseek-v3","choices":[{"index":0,"delta":{"content":"你好世界"},"finish_reason":null}]}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(in)))
	for i := 0; i < b.N; i++ {
		_, _ = s.Sanitize(in, "deepseek-pro", rep)
	}
}

// --- 多模态与工具能力的透传边界 ---

// 网关对「模型能力」是无感的：识图、音频、视频、文档、联网、函数调用
// 都只体现在请求体的结构里，而网关只改 model 字段。
//
// 这个测试是一道护栏：任何让网关开始理解请求体结构的改动都会打破它。
// 新增能力时不需要改代码，但改脱敏逻辑时必须保证这里仍然通过。
func TestMultimodalRequestBodyUntouched(t *testing.T) {
	cases := map[string]string{
		"识图 image_url": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"text","text":"这是什么"},` +
			`{"type":"image_url","image_url":{"url":"https://x.com/a.png","detail":"high"}}]}]}`,
		"识图 base64": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgoAAAANS"}}]}]}`,
		"音频输入": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"input_audio","input_audio":{"data":"UklGRiQAAABXQVZF","format":"wav"}}]}]}`,
		"视频输入": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"video_url","video_url":{"url":"https://x.com/v.mp4","fps":2}}]}]}`,
		"文档输入": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"file","file":{"file_id":"f-123","filename":"a.pdf"}}]}]}`,
		"联网搜索工具": `{"model":"m","tools":[{"type":"web_search","max_keyword":3}],"input":[]}`,
		"函数调用": `{"model":"m","tools":[{"type":"function","function":{"name":"get_weather",` +
			`"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}]}`,
		"Anthropic 图片块": `{"model":"m","messages":[{"role":"user","content":[` +
			`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBOR"}}]}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			out, err := RewriteModel([]byte(body), "up-real")
			if err != nil {
				t.Fatal(err)
			}
			if got, _ := ExtractModel(out); got != "up-real" {
				t.Fatalf("model 未被改写: %q", got)
			}
			// 改回原名后必须与原文逐字节一致 —— 证明除 model 外无任何改动。
			back, err := RewriteModel(out, "m")
			if err != nil {
				t.Fatal(err)
			}
			if string(back) != body {
				t.Errorf("请求体被额外改动\n原文: %s\n结果: %s", body, back)
			}
		})
	}
}

// 多模态响应里的 base64 数据不能被脱敏逻辑当成短值改写。
func TestMultimodalResponseDataUntouched(t *testing.T) {
	rules := sanitize.NewRules(nil, nil, nil, 256)
	rep := rules.For("up-v3", "pub")

	// 上游模型名恰好作为子串出现在一段短 base64 里（构造的极端情况）。
	body := []byte(`{"model":"up-v3","audio":{"data":"up-v3AAAA","transcript":"up-v3 说话"},` +
		`"choices":[{"message":{"content":"我是 up-v3"}}]}`)

	out, changed := openAISpec.Sanitize(body, "pub", rep)
	if !changed {
		t.Fatal("model 字段应被改写")
	}
	s := string(out)
	if !strings.Contains(s, `"model":"pub"`) {
		t.Error("model 字段未脱敏")
	}
	// transcript 与 content 属于生成内容白名单，必须原样保留。
	if !strings.Contains(s, `"transcript":"up-v3 说话"`) {
		t.Error("transcript（生成内容）被篡改")
	}
	if !strings.Contains(s, `"content":"我是 up-v3"`) {
		t.Error("content（生成内容）被篡改")
	}
}
