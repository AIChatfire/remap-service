package protocol

import (
	"encoding/json"
	"strconv"
	"time"
)

// PreambleCopies 是提前下发时**重复的份数**，不是笔误。
//
// 下游（以 new-api 的 OaiStreamHandler 为例）逐块转发时是「收到第 N 块才
// 下发第 N-1 块」—— 它把上一块存在 lastStreamData 里，等下一块到了才写出去。
// 于是只发一份的话，提前量会被这一块的延迟整个吃掉：new-api 记的首字确实
// 提前了，但终端用户仍然要等到真实 token 才收到第一块字节。
//
// 用 new-api 真实代码（relay/helper.StreamScannerHandler）实测，上游 prefill
// 1.5s 时：
//
//	份数   new-api 首字   终端首字节
//	 0       1.5s          1.5s
//	 1       0s            1.5s   ← 只改了指标，体验没动
//	 2       0s            0s
//
// 取 2 而不是「可配置」：它与新一份、后一份的具体实现无关（对方哪天改成
// 立即下发，两份仍然正确），而再多发只是多两行空事件、不带来额外收益。
const PreambleCopies = 2

// Preamble 返回该路由下可以「提前下发」的**一份**首事件字节（含结尾空行）。
//
// # 为什么需要它
//
// 上游「返回响应头」与「吐出第一个 token」是两个时刻，中间隔着整段
// prefill。响应头一到，流其实已经建立，但客户端在这段时间里收不到任何
// 事件 —— 于是所有以「收到第一个 SSE 事件」为口径的首字/TTFT 统计，
// 都把这一段等待算进了模型响应时间。
//
// 提前下发一个内容为空、协议合法的首事件，把这段空白从「还在等」变成
// 「流已开始」：下游的渲染状态机（以及以首个事件为口径的统计）会在
// prefill 开始前就推进。
//
// # 它改了什么、没改什么
//
// 没改：模型生成的内容、token 数、总时长、发给上游的请求。
// 改了：下游观测到的首字时间不再等于「上游生成第一个 token 的时刻」，
// 而等于「上游返回响应头的时刻」。对把 role 前置事件当协议常态的上游
// （OpenAI 系），两者本就接近；对把首事件压在 prefill 之后的上游，
// 差距就是 prefill 的时长 —— 这个数字会被显著改善，但它衡量的东西
// 也确实变了。因此本能力由 STREAM_EARLY_FIRST_EVENT 显式开启，
// 且每次注入都单独计数（gateway.stream.preamble），可据此把合成事件
// 从首字统计里剔掉。
//
// # 形态约束（与下游解析器强耦合；改这里必须同步改测试）
//
// 事件必须是**载荷非空的 data 行**。以 new-api 的流扫描器
// （relay/helper/stream_scanner.go）为例，判定首字的条件等价于：
//
//	len(line) >= 6 && line[:5] == "data:"
//	    && TrimSpace(line[5:]) != "" && !HasPrefix(trimmed, "[DONE]")
//
// 由此推出两条反直觉的结论：
//
//  1. **空载荷不生效**。`data: ` 会被 TrimSpace 成空串后静默跳过 ——
//     「第一个事件用空字符串」达不到目的。能生效的是「载荷是合法 JSON、
//     而 JSON 里的 content 为空」。
//  2. **纯空白载荷同样不生效**，注释行（`: ping`）更不生效 ——
//     new-api 自己的心跳就是注释行，设计上刻意不计入首字。
//
// 这两条由 TestPreambleSatisfiesNewAPIFirstEventRule 钉住。
//
// publicModel 是**对外**模型名（客户端请求里那个），不是上游真实名。
//
// 这个名字会被写进事件里直接发给客户端，而合成事件不经过响应脱敏管道，
// 所以它是本文件唯一可能把上游标识送出去的地方。两道防线：
//
//  1. 参数名就叫 publicModel，调用点取错字段时一眼能看出来；
//  2. 网关在写出去之前还会把结果过一遍**与正文同一把替换器**
//     （sanitize.Replacer.ApplyBytes），因此即便这里混进了上游名、
//     别名或上游 id，也会像正文那样被擦掉。
//
// 第 2 条才是真正的保证：断言只能证明「当前这行代码写对了」，
// 挡不住后来任何改动。这里不依赖调用方永远正确，只是让它更容易正确。
//
// publicModel 为空（请求里没有 model 字段）时返回 nil：既无从知道对外名，
// 也就没有理由去合成一个带空 model 的事件。
func Preamble(route, publicModel string) []byte {
	if publicModel == "" {
		return nil
	}
	build, ok := preambleBuilders[route]
	if !ok {
		return nil
	}
	return build(publicModel)
}

// preambleBuilders 按**路由**登记首事件构造器，而不是按 Spec.Name 分派。
//
// 同一个 Spec 下不同端点的流形态并不相同：/v1/chat/completions 发的是
// chat.completion.chunk，/v1/completions 发的是 text_completion ——
// 按协议名分派会把两者混为一谈，给 legacy 端点塞一个 chat 形态的事件。
//
// 未登记的路由一律不下发。宁可少一份好看的首字数字，也不能给客户端
// 塞一个它解析不了的事件：
//
//	/v1/responses  事件带单调递增的 sequence_number 与完整 response 对象，
//	               伪造 response.created / response.in_progress 都可能被
//	               严格客户端当成「序列错乱」而中断解析。
var preambleBuilders = map[string]func(string) []byte{
	"/v1/chat/completions": preambleChatCompletion,
	"/v1/messages":         preambleAnthropicPing,
}

// preambleChatCompletion 构造一个内容为空的 chat.completion.chunk。
//
// 刻意**不带** "role":"assistant"：上游自身通常也会发一个 role 前置事件，
// 再由我们补一个，会让「以 delta.role 作为新消息起点」的客户端多起一条
// 消息。只带空 content 已经够用 —— 下游据此判定「流已开始」，而任何按
// 内容拼接的客户端从这个事件里拼不出任何字符。
func preambleChatCompletion(publicModel string) []byte {
	now := time.Now().Unix()
	v := chatPreamble{
		ID:      "chatcmpl-" + strconv.FormatInt(now, 10),
		Object:  "chat.completion.chunk",
		Created: now,
		Model:   publicModel,
		Choices: []chatPreambleChoice{{
			Index:        0,
			Delta:        chatPreambleDelta{Content: ""},
			FinishReason: nil,
		}},
	}
	// 用 encoding/json 而不是手工拼串：对外名来自请求体、完全由客户端控制，
	// 手工拼串遇到引号或控制字符就会生成非法 JSON，把整条流毒掉。
	body, err := json.Marshal(v)
	if err != nil {
		// 字段全是可序列化类型，Marshal 实际不会失败。真失败时宁可不下发，
		// 也不能把一个半截事件写进流里。
		return nil
	}
	out := make([]byte, 0, len(dataPrefix)+2+len(body)+2)
	out = append(out, dataPrefix...)
	out = append(out, ' ')
	out = append(out, body...)
	return append(out, '\n', '\n')
}

// preambleAnthropicPing 返回一个 Anthropic 的 ping 事件。
//
// 选 ping 而不是伪造 message_start：Anthropic 官方 API 本身就会穿插下发
// ping，各家 SDK 都显式忽略它（Python SDK 里就是 `elif event.type == "ping"`），
// 因此它既满足「非空 data 载荷、能被下游计为首字」，又不会与上游随后
// 发出的真实 message_start 冲突。
var anthropicPing = []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")

func preambleAnthropicPing(string) []byte { return anthropicPing }

// chatPreamble 是 chat.completion.chunk 的最小可用形态。
// 字段顺序即 JSON 输出顺序（Go 按声明顺序序列化）。
type chatPreamble struct {
	ID      string               `json:"id"`
	Object  string               `json:"object"`
	Created int64                `json:"created"`
	Model   string               `json:"model"`
	Choices []chatPreambleChoice `json:"choices"`
}

type chatPreambleChoice struct {
	Index        int               `json:"index"`
	Delta        chatPreambleDelta `json:"delta"`
	FinishReason *string           `json:"finish_reason"`
}

type chatPreambleDelta struct {
	Content string `json:"content"`
}
