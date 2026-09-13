package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// newAPICountsAsFirstEvent 复刻 new-api 流扫描器判定「首字」的那几行条件
// （relay/helper/stream_scanner.go，扫描器按行切分后逐行判断）：
//
//	if len(data) < 6 { continue }
//	if data[:5] != "data:" && data[:6] != "[DONE]" { continue }
//	data = strings.TrimSpace(data[5:])
//	if data == "" { continue }
//	if !strings.HasPrefix(data, "[DONE]") { info.SetFirstResponseTime() }
//
// 之后 frt 被写成 FirstResponseTime - StartTime，也就是 new-api 后台
// 那个「首字」。
//
// 为什么要把外部实现抄进测试：本能力是否生效**完全**取决于这段对方代码。
// 它一改（比如哪天改成「只认带 content 的载荷」），提前下发就静默失效，
// 而表现是「看板上首字没变」—— 与「上游本来就不慢」完全同形。抄进来之后，
// 对方一旦漂移，这里会红。
func newAPICountsAsFirstEvent(event []byte) bool {
	for _, raw := range strings.Split(strings.TrimRight(string(event), "\n"), "\n") {
		if len(raw) < 6 {
			continue
		}
		if raw[:5] != "data:" && raw[:6] != "[DONE]" {
			continue
		}
		d := strings.TrimSpace(raw[5:])
		if d == "" {
			continue
		}
		if !strings.HasPrefix(d, "[DONE]") {
			return true
		}
	}
	return false
}

// TestPreambleSatisfiesNewAPIFirstEventRule 是本能力成立的**前提条件**：
// 合成事件必须真的会被下游计为首字，否则这就是一堆白发的字节，
// 而且不会有任何报错指向它。
func TestPreambleSatisfiesNewAPIFirstEventRule(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/messages"} {
		ev := Preamble(route, "public-model")
		if len(ev) == 0 {
			t.Fatalf("%s：应下发首事件，实际为 nil", route)
		}
		if !newAPICountsAsFirstEvent(ev) {
			t.Errorf("%s：合成事件不会被 new-api 计为首字，等于白发：%q", route, ev)
		}
		if !bytes.HasSuffix(ev, []byte("\n\n")) {
			t.Errorf("%s：事件未以空行收尾，客户端会一直等下一个事件：%q", route, ev)
		}
	}
}

// TestEmptyOrWhitespacePayloadIsNotFirstEvent 把「用空字符串/空格当首个事件」
// 这条**看似可行实则无效**的路子钉在测试里。
//
// new-api 会先 TrimSpace 再判空，所以 `data: ` 与纯空白载荷都被静默跳过；
// 注释行（它自己的心跳就是 `: PING`）更不计入 —— 那是刻意设计。
// 能生效的只有「载荷是合法 JSON、而 JSON 里的 content 为空」。
func TestEmptyOrWhitespacePayloadIsNotFirstEvent(t *testing.T) {
	for _, ev := range []string{
		"data: \n\n",
		"data:\n\n",
		"data:    \n\n",
		": PING\n\n",
		"event: ping\n\n",
		"data: [DONE]\n\n",
	} {
		if newAPICountsAsFirstEvent([]byte(ev)) {
			t.Errorf("复刻的判定与实测不符，new-api 不该把 %q 计为首字", ev)
		}
	}
}

// TestPreambleSkippedForRoutesWithoutSafeShape 钉住「宁可不发」的边界：
// 没有协议合法且无副作用的首事件形态的路由一律返回 nil。
func TestPreambleSkippedForRoutesWithoutSafeShape(t *testing.T) {
	for _, route := range []string{
		// 事件带单调递增的 sequence_number 与完整 response 对象，
		// 伪造 response.created 会被严格客户端判成序列错乱。
		"/v1/responses",
		// 同一个 Spec 下的 legacy 端点，流形态是 text_completion 而非
		// chat.completion.chunk，套用会给出一个客户端解析不了的事件。
		"/v1/completions",
		"/v1/embeddings",
		"/v1/models",
		"/v1/*",
		"other",
	} {
		if ev := Preamble(route, "m"); ev != nil {
			t.Errorf("%s：无安全形态，应返回 nil，实际 %q", route, ev)
		}
	}
}

// TestPreambleRequiresModel 覆盖「请求体没有 model 字段」的形态：
// 无从知道对外模型名，就不该合成一个 model 为空的事件。
func TestPreambleRequiresModel(t *testing.T) {
	for _, route := range []string{"/v1/chat/completions", "/v1/messages"} {
		if ev := Preamble(route, ""); ev != nil {
			t.Errorf("%s：模型名未知时应返回 nil，实际 %q", route, ev)
		}
	}
}

// TestPreambleChatCompletionFabricatesNoContent 是本能力的红线：
// 合成事件不得携带任何内容，也不得带 role（上游通常自己会发一个，
// 多一个会让「以 delta.role 为新消息起点」的客户端多起一条消息）。
func TestPreambleChatCompletionFabricatesNoContent(t *testing.T) {
	ev := Preamble("/v1/chat/completions", "pub-model")

	payload := bytes.TrimSpace(bytes.TrimPrefix(ev, dataPrefix))
	if !json.Valid(payload) {
		t.Fatalf("载荷不是合法 JSON: %s", payload)
	}

	var got struct {
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Index        int            `json:"index"`
			Delta        map[string]any `json:"delta"`
			FinishReason *string        `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Object != "chat.completion.chunk" {
		t.Errorf("object = %q", got.Object)
	}
	// 必须是**对外**模型名：合成事件绕过了响应脱敏（它直接写 w），
	// 这里写错就是把上游真实名直接送出去。
	if got.Model != "pub-model" {
		t.Errorf("model = %q，应为对外模型名", got.Model)
	}
	if len(got.Choices) != 1 {
		t.Fatalf("choices 长度 = %d", len(got.Choices))
	}
	if c, ok := got.Choices[0].Delta["content"]; !ok || c != "" {
		t.Errorf("delta.content = %v（存在=%v），必须为空", c, ok)
	}
	if _, ok := got.Choices[0].Delta["role"]; ok {
		t.Error("delta 不应带 role：与上游自身的 role 前置事件叠加会多起一条消息")
	}
	if got.Choices[0].FinishReason != nil {
		t.Errorf("finish_reason 应为 null，实际 %q", *got.Choices[0].FinishReason)
	}
}

// TestPreambleEscapesModelName 覆盖客户端可控的模型名：它来自请求体，
// 手工拼串遇到引号/换行就会生成非法 JSON，把整条流毒掉。
func TestPreambleEscapesModelName(t *testing.T) {
	const weird = "m\"x\\y\nz\tü"
	ev := Preamble("/v1/chat/completions", weird)

	payload := bytes.TrimSpace(bytes.TrimPrefix(ev, dataPrefix))
	if !json.Valid(payload) {
		t.Fatalf("模型名未转义，载荷非法: %s", payload)
	}
	var got struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatal(err)
	}
	if got.Model != weird {
		t.Errorf("model 往返后 = %q，原值 %q", got.Model, weird)
	}
}

// TestPreambleAnthropicIsPingEvent 说明 Anthropic 侧为什么选 ping：
// 官方 API 本身就会穿插下发 ping，各家 SDK 都显式忽略，因此它既满足
// 「非空 data 载荷」，又不会与上游随后发出的真实 message_start 冲突。
func TestPreambleAnthropicIsPingEvent(t *testing.T) {
	ev := string(Preamble("/v1/messages", "pub-model"))
	if !strings.Contains(ev, "event: ping\n") {
		t.Errorf("缺少 event: ping 行，SDK 无法按事件名分派: %q", ev)
	}
	if !strings.Contains(ev, `data: {"type":"ping"}`) {
		t.Errorf("缺少 ping 载荷: %q", ev)
	}
	// 伪造 message_start 会很危险：上游还会再发一个真实 message_start。
	if strings.Contains(ev, "message_start") {
		t.Errorf("不应伪造 message_start: %q", ev)
	}
}

// 合成事件的成本要能回答，别让后人靠猜。它分两段，且都在**每个流式请求一次**
// 的量级（不是逐 chunk）：Preamble 构造 + JSON 序列化，SanitizeEvent 走正文那条
// 逐行脱敏路径。这个基准把网关实际做的这两件事连起来测，给出的就是
// 「打开开关后，每个流式请求多花多少」。
func BenchmarkPreambleAndSanitize(b *testing.B) {
	// 提到循环外并显式存成接口：否则每次调用点现转接口会装箱，把噪声算进结果。
	var tr Transformer = newTr(b, "/v1/chat/completions")

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if out := SanitizeEvent(Preamble("/v1/chat/completions", "pub"), tr); len(out) == 0 {
			b.Fatal("合成事件不应为空")
		}
	}
}
