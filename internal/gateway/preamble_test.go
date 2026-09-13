package gateway

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/betterme/remap-service/internal/config"
)

// wantPreambleCopies 是**测试侧持有的期望份数**，刻意写成字面量而不是
// protocol.PreambleCopies。
//
// 用常量做期望值会让「实现」与「期望」一起变：把 protocol.PreambleCopies
// 改成 1，断言仍然通过 —— 而那正是本功能最隐蔽的失败形态（下游延迟一块转发
// 把提前量吃掉，只有指标变快、终端用户毫无变化）。实测过，这个变异体确实逃逸。
//
// 所以这里重述一遍需求：**两份**，因为下游收到第 N 块才下发第 N-1 块。
// 改动 protocol.PreambleCopies 必须同时改这里 —— 那正是我们想要的摩擦。
const wantPreambleCopies = 2

// streamPreambleAttr 读 gateway.stream.preamble 的值；ok=false 表示属性不存在。
//
// 刻意不叫 attrBool：那个名字在本包里被明确禁用（见 errorbody_test.go 中
// hasAttr 的注释）—— 读值版对「属性不存在」与「属性为 false」返回同一个
// false，两种形态分不开。这里返回二元组把它们分开。
func streamPreambleAttr(s sdktrace.ReadOnlySpan) (val, ok bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == "gateway.stream.preamble" {
			return kv.Value.AsBool(), true
		}
	}
	return false, false
}

// cutEvents 去掉开头的前 n 个完整事件（以空行为界），返回剩余原始字节。
//
// 刻意不用 protocol.Preamble 去算前缀：那会让断言跟着被测实现一起变，
// 实现改了测试也跟着改、永远为真。这里只依赖 SSE 的分帧规则。
func cutEvents(s string, n int) string {
	for i := 0; i < n; i++ {
		j := strings.Index(s, "\n\n")
		if j < 0 {
			return ""
		}
		s = s[j+2:]
	}
	return s
}

// 本能力的全部意义：上游还在 prefill（响应头已回、首个 token 未出）时，
// 客户端就已经收到了一个事件。所以断言必须落在**时序**上 —— 上游被卡住
// 的那一刻读客户端，读到的必须是合成事件。
func TestStreamEarlyFirstEventArrivesBeforeUpstreamFirstToken(t *testing.T) {
	release := make(chan struct{})
	// 放行必须走这个函数、并挂到 Cleanup 上：失败路径上的 t.Fatal 不会走到
	// 下面的放行，而上游桩仍阻塞在 <-release，t.Cleanup 里的 gs.Close() 会
	// 一直等它 —— 于是「断言失败」表现为整个测试二进制在 10 分钟后超时，
	// 真正的原因被埋进一堆 goroutine dump 里。
	// 两个调用点都在测试 goroutine 上，不需要同步原语。
	released := false
	releaseUpstream := func() {
		if !released {
			released = true
			close(release)
		}
	}
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-release // 模拟 prefill：头已回，首个 token 迟迟不来
		_, _ = io.WriteString(w, `data: {"model":"up-m","choices":[{"delta":{"content":"hi"}}]}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
	})
	// 注册顺序是承重的，别上移：t.Cleanup 是 **LIFO**，而 fixture 内部注册了
	// gs.Close()（它会等上游桩那个 goroutine 结束）。若在 fixture 之前注册本函数，
	// 清理时 gs.Close() 先跑、上游桩还阻塞在 <-release ⇒ 死锁，且表现为
	// 「整个测试二进制超时」而不是「这条断言失败」。
	// 放在 fixture 之后，任何一条 t.Fatalf 都能正常收场。
	t.Cleanup(releaseUpstream)

	ctx, cancelReq := context.WithCancel(context.Background())
	defer cancelReq()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, gs.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"pub-m","stream":true}`))
	resp, err := gs.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 4096)
	done := make(chan string, 1)
	go func() {
		n, _ := resp.Body.Read(buf)
		done <- string(buf[:n])
	}()

	var first string
	select {
	case first = <-done:
	case <-time.After(2 * time.Second):
		// 失败路径要先解锁再断言。resp.Body 的 Read 在阻塞期间**一直持有
		// body 的互斥锁**，Close 只能在它返回后才拿得到锁；不先取消请求，
		// 这个用例的失败形态就是「defer resp.Body.Close() 挂住 → 整个测试
		// 二进制 10 分钟后超时」，真正的断言信息被埋进 goroutine dump。
		cancelReq()
		releaseUpstream()
		t.Fatalf("上游仍卡在 prefill，客户端却什么也没收到 —— 首事件没有提前下发")
	}

	if !strings.Contains(first, `"object":"chat.completion.chunk"`) {
		t.Fatalf("首块不是合成的 chunk：%q", first)
	}
	if !strings.Contains(first, `"content":""`) {
		t.Errorf("合成事件必须内容为空：%q", first)
	}
	// 合成事件**绕过**了响应脱敏（直接写 w），模型名写错就是把上游真实名
	// 直接送到客户端 —— 这是本功能唯一可能造成泄漏的地方。
	if !strings.Contains(first, `"model":"pub-m"`) {
		t.Errorf("合成事件必须用对外模型名：%q", first)
	}
	if strings.Contains(first, "up-m") {
		t.Errorf("合成事件泄漏了上游真实模型名：%q", first)
	}
	if !strings.HasSuffix(first, "\n\n") {
		t.Errorf("事件未以空行收尾，客户端会一直等下一个事件：%q", first)
	}

	releaseUpstream()
	rest, _ := io.ReadAll(resp.Body)
	all := first + string(rest)

	// 真实数据一字不能少、也不能多。
	if !strings.Contains(all, `"content":"hi"`) {
		t.Errorf("真实内容丢失：%s", all)
	}
	if !strings.Contains(all, "data: [DONE]") {
		t.Errorf("[DONE] 丢失：%s", all)
	}
	if strings.Contains(all, "up-m") {
		t.Errorf("流里出现上游真实模型名：%s", all)
	}
	// 份数必须精确等于 wantPreambleCopies：少一份，下游延迟一块的转发会把
	// 提前量吃掉（指标提前、终端不变）；多一份则纯属多发。
	if n := strings.Count(all, `"object":"chat.completion.chunk"`); n != wantPreambleCopies {
		t.Errorf("合成首事件出现 %d 次，应为 %d 次：%s", n, wantPreambleCopies, all)
	}

	sp := findSpanWithAttr(t, sr, "gateway.stream.preamble")
	if v, ok := streamPreambleAttr(sp); !ok || !v {
		t.Errorf("span 必须留下 gateway.stream.preamble=true 的痕迹，否则「首字变快」"+
			"与「上游真的变快」在看板上完全同形；实际 ok=%v val=%v", ok, v)
	}
	if n := attrInt(sp, "gateway.stream.preamble_bytes"); n <= 0 {
		t.Errorf("应上报合成事件字节数，实际 %d", n)
	}
	// sse.events 保持「上游转发的事件数」口径（本用例上游发 2 个：
	// 数据块 + [DONE]），合成事件由 gateway.stream.preamble 单独计数。
	if n := attrInt(sp, "gateway.sse.events"); n != 2 {
		t.Errorf("gateway.sse.events = %d，应为上游的 2 个事件；"+
			"把合成事件混进来会让「上游到底发了几个事件」这个问题失去答案", n)
	}
}

// 开关默认关闭：不配置时行为必须与从前逐字节一致。
func TestStreamEarlyFirstEventOffByDefault(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `data: {"model":"up-m","choices":[{"delta":{"content":"hi"}}]}`+"\n\n")
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
	})

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"pub-m","stream":true}`, nil))

	if strings.Contains(body, "chat.completion.chunk") {
		t.Errorf("未开启开关却下发了合成事件：%s", body)
	}
	if !strings.Contains(body, `"content":"hi"`) {
		t.Errorf("真实内容异常：%s", body)
	}
}

// 没有安全形态的路由一律不下发 —— 宁可少一份好看的首字数字，
// 也不能给客户端塞一个它解析不了的事件。
func TestStreamEarlyFirstEventSkippedWithoutSafeShape(t *testing.T) {
	const upResponses = "event: response.created\n" +
		`data: {"type":"response.created","response":{"model":"up-m"}}` + "\n\n"

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, upResponses)
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
	})

	body := readAll(t, post(t, gs, "/v1/responses", `{"model":"pub-m","stream":true}`, nil))

	if strings.Contains(body, "chat.completion.chunk") {
		t.Errorf("/v1/responses 不应下发 chat 形态的合成事件：%s", body)
	}
	if !strings.Contains(body, "event: response.created") {
		t.Errorf("上游事件丢失：%s", body)
	}
}

// Anthropic 侧用官方就存在的 ping 事件：SDK 都显式忽略它，
// 因此既有非空 data 载荷（能被下游计为首字），又不会与上游随后发出的
// 真实 message_start 冲突。
func TestStreamEarlyFirstEventAnthropicPing(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: message_start\n")
		_, _ = io.WriteString(w, `data: {"type":"message_start","message":{"id":"m1","model":"up-m"}}`+"\n\n")
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
	})

	body := readAll(t, post(t, gs, "/v1/messages",
		`{"model":"pub-m","stream":true,"max_tokens":16}`, nil))

	head := body[:len(body)-len(cutEvents(body, wantPreambleCopies))]
	if !strings.Contains(head, "event: ping") || !strings.Contains(head, `"type":"ping"`) {
		t.Errorf("首事件应是 ping，实际 %q", head)
	}
	if n := strings.Count(head, "event: ping"); n != wantPreambleCopies {
		t.Errorf("ping 出现 %d 次，应为 %d 次", n, wantPreambleCopies)
	}
	// 上游事件一颗不少
	if rest := cutEvents(body, wantPreambleCopies); !strings.Contains(rest, "event: message_start") {
		t.Errorf("上游 message_start 丢失：%s", rest)
	}
	// 而且 ping 不能伪造 message_start（上游还会再发一个真的）
	if strings.Contains(head, "message_start") {
		t.Errorf("不该伪造 message_start：%q", head)
	}
}

// 去掉合成事件之后，上行原始载荷必须逐字节不变。
//
// 这是「不影响上游真实数据」这条要求的机械检验：关掉脱敏后没有任何
// 改写理由，多一个字节、少一个字节都会在这里露出来。
func TestStreamEarlyFirstEventLeavesUpstreamPayloadIntact(t *testing.T) {
	const up = "data: {\"model\":\"up-m\",\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\n\n" +
		"data: [DONE]\n\n"

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, up)
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
		// 关掉脱敏：本用例只检验「有没有动过上游载荷」，脱敏会引入改写，
		// 那是另一组用例的事。
		c.Sanitize.Off = true
	})

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"pub-m","stream":true}`, nil))

	head := body[:len(body)-len(cutEvents(body, wantPreambleCopies))]
	if !strings.Contains(head, `"model":"pub-m"`) || !strings.Contains(head, `"content":""`) {
		t.Errorf("首事件形态不符：%q", head)
	}
	if rest := cutEvents(body, wantPreambleCopies); rest != up {
		t.Errorf("去掉合成事件后载荷被改动：\n得到 %q\n期望 %q", rest, up)
	}
}

// modelValues 取出 body 里所有 "model":"…" 的取值，按出现顺序。
func modelValues(body string) []string {
	const key = `"model":"`
	var out []string
	for i := 0; ; {
		j := strings.Index(body[i:], key)
		if j < 0 {
			return out
		}
		j += i + len(key)
		k := strings.IndexByte(body[j:], '"')
		if k < 0 {
			return out
		}
		out = append(out, body[j:j+k])
		i = j + k
	}
}

// 合成事件与正文必须对模型名给出**同一个答案**。
//
// 这比「首事件里必须出现对外名」更强：它把合成事件与脱敏后的正文绑在一起。
// 「两边各说各话」（客户端先看到 up-m、紧接着又看到 pub-m）这种失败形态
// 只能在这里露出来 —— 单看任意一边都是合法的。
func TestPreambleAgreesWithSanitizedBodyOnModel(t *testing.T) {
	const up = `data: {"id":"chatcmpl-1","model":"up-m","choices":[{"delta":{"content":"hi"}}]}` + "\n\n" +
		"data: [DONE]\n\n"

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, up)
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
	})

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"pub-m","stream":true}`, nil))

	models := modelValues(body)
	// 下游逐块转发会把两份合成事件合并成一块，但两处的 model 都在，
	// 加上上游那一处，至少 2 个 —— 少于 2 个说明有一处根本没出现。
	if len(models) < 2 {
		t.Fatalf("期望至少 2 处 model（合成 + 上游），实际 %d 处：%s", len(models), body)
	}
	for _, m := range models {
		if m != models[0] {
			t.Fatalf("合成事件与正文对模型名的说法不一致：%q vs %q\n%s", models[0], m, body)
		}
	}
	if models[0] != "pub-m" {
		t.Errorf("客户端看到的模型名应为对外名 pub-m，实际 %q", models[0])
	}
	if strings.Contains(body, "up-m") {
		t.Errorf("流里出现上游真实模型名：%s", body)
	}
}

// 把「合成事件到底有没有经过脱敏管道」变成**可观测**的。
//
// 探针选在 id、不选 model，是因为 model 被 Spec 的 ModelPaths 整体覆盖、
// 恒等于对外名 —— 管道在不在它都是那个值，看不出接线死活。id 是合成事件里
// 唯一还会被通用扫描碰到的字段，于是拿它当探针。
//
// 配置是刻意构造的（全局替换对的源通常写上游标识，不会写 chatcmpl），
// 唯一目的是让管道的作用可见。它的价值在于：一旦去掉 SanitizeEvent 这一层，
// 客户端收到的 id 会退回 chatcmpl- 前缀，下面两条断言必然变红。
func TestPreambleGoesThroughSanitizePipeline(t *testing.T) {
	const up = `data: {"id":"chatcmpl-1","model":"up-m","choices":[{"delta":{"content":"hi"}}]}` + "\n\n" +
		"data: [DONE]\n\n"

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, up)
		w.(http.Flusher).Flush()
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub-m": {"up-m"}}
		c.Stream.EarlyFirstEvent = true
		c.Sanitize.Replace = map[string]string{"chatcmpl": "gw"}
	})

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"pub-m","stream":true}`, nil))

	head := body[:len(body)-len(cutEvents(body, wantPreambleCopies))]
	if !strings.Contains(head, `"id":"gw-`) {
		t.Errorf("合成事件的 id 没被替换成 gw- 前缀 —— 它绕过了脱敏管道：%q", head)
	}
	if strings.Contains(head, "chatcmpl-") {
		t.Errorf("合成事件里仍有未脱敏的 chatcmpl- 前缀：%q", head)
	}
	// 正文里的 id 也应被同一把替换器改写（同一套规则，不是两套）。
	if rest := cutEvents(body, wantPreambleCopies); strings.Contains(rest, "chatcmpl-") {
		t.Errorf("正文里的 id 未脱敏，说明规则不一致：%q", rest)
	}
}
