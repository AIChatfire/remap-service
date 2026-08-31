package obs

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// 错误相关的 span 属性键。集中在此，避免各处手写字符串导致看板筛选条件失配。
const (
	// AttrErrKind 是错误的粗分类，与指标里的 outcome 同源，便于两侧对照。
	AttrErrKind = "gateway.error.kind"
	// AttrErrBody 是上游错误正文的原始片段（未做任何结构化解析）。
	AttrErrBody = "gateway.error.body"
	// AttrErrBodySize 是错误正文的实际字节数。它与 AttrErrBody 的长度
	// 不一致时说明发生了截断 —— 排查时能立刻知道看到的不是全文。
	AttrErrBodySize = "gateway.error.body_size"
	// AttrErrTruncated 标记正文是否被截断。
	AttrErrTruncated = "gateway.error.truncated"
	// AttrErrContentType 是错误响应的 Content-Type。上游返回 HTML 而非
	// JSON 往往意味着请求根本没到上游应用层（被反代或 WAF 拦了）。
	AttrErrContentType = "gateway.error.content_type"
	// AttrErrDetail 是错误的完整原始描述。
	//
	// span 的 status 描述在 Logfire 列表里会被截短，且 exception 事件需要
	// 展开才能看到；这个属性让「完整错误串」始终能被直接筛选与复制。
	AttrErrDetail = "gateway.error.detail"
	// AttrErrType 是 Go 侧的错误类型（如 *url.Error、*net.OpError）。
	// 同一句 "connection refused" 出现在不同类型上，含义并不相同。
	AttrErrType = "gateway.error.type"
	// AttrErrCause 是错误链 unwrap 到底的根因。
	//
	// 传输层错误的 Error() 串是层层包裹的（*url.Error 包 *net.OpError 包
	// *os.SyscallError），最外层只说"Post xxx 失败"，真正的原因
	// （connection refused / TLS 握手失败 / no such host）在最里层。
	AttrErrCause = "gateway.error.cause"
	// AttrErrClientMsg 是实际返回给客户端的归类消息。
	//
	// 网关会把各种底层原因归一成少数几句对外消息（如统一的
	// "upstream connection failed"）。把两者并列上报，才能对照
	// 「客户端报的 502」与「实际发生的 DNS 解析失败」。
	AttrErrClientMsg = "gateway.error.client_message"

	// 以下 failover 属性挂在**请求 span 上**（而非 attempt_failed 事件里）。
	//
	// 存在的理由：切换成功后 span 的 http.response.status_code 恒为最终的
	// 200，首次失败的 429 只活在事件属性里。而 Logfire 的 trace 列表、
	// Full Trace 视图、以及按属性筛选/聚合都只看 span 属性 —— 结果是
	// 「今天有多少请求是被 429 顶掉后切换的」这个问题必须逐条展开事件
	// 才能回答，等于不可查。这里把首次失败的状态码提到 span 上，
	// 让它成为一等的筛选维度；事件仍保留正文等细节，两者不重复承载。
	//
	// AttrFailover 标记本次请求发生过故障切换。
	AttrFailover = "gateway.failover"
	// AttrFailoverStage 是首次失败发生的阶段（transport / status）。
	AttrFailoverStage = "gateway.failover.stage"
	// AttrFailoverStatus 是**首次失败**的上游状态码（如 429）。
	// 与 http.response.status_code 刻意分开：后者是客户端实际收到的结果。
	AttrFailoverStatus = "gateway.failover.first_status_code"
	// AttrFailoverFrom 是切换前的上游模型名（真正出问题的那个）。
	AttrFailoverFrom = "gateway.failover.from_model"
	// AttrFailoverTo 是切换后实际承接请求的上游模型名。
	AttrFailoverTo = "gateway.failover.to_model"

	// AttrFailoverOutcome 说明切换的最终结局，三取一：
	//
	//   succeeded  切换救回了请求（客户端收到 2xx/3xx）
	//   exhausted  备用模型也返回错误，主备同时不可用 —— 最该告警的形态
	//   aborted    重试请求没发出去（改写/构造/连接失败），是网关侧问题
	//
	// succeeded 与 exhausted 的判据是**切换后的最终状态码**，不是
	// 「重试请求是否发出去了」。此前三者共用同一组属性、一律记作切换
	// 成功，于是主备双挂在看板上与正常降级完全无法区分。
	AttrFailoverOutcome = "gateway.failover.outcome"

	// AttrUpstreamPath 是**上游**请求的实际路径（如 /api/v3/chat/completions）。
	//
	// 刻意不用 OTel 语义约定的 url.path：那个键属于**入站**请求
	// （客户端打进网关的 /v1/chat/completions），被上游路径覆盖后
	// Logfire 的 HTTP Request Attributes 面板会显示上游路径，
	// 等于伪造了客户端请求的事实。两者必须并存才能看出重写前后。
	AttrUpstreamPath = "gateway.upstream.path"
)

// RecordFailover 把「发生过故障切换」这件事落到请求 span 的属性上。
//
// 与 RecordAttemptFailure 的分工：后者用事件承载单次尝试的完整现场
// （错误正文、Go 错误类型），可重复；本函数每请求最多调用一次，
// 提供少量高价值、可聚合的维度。
//
// 不改 span 状态。切换成功时对客户端确实没故障；切换失败时客户端收到的
// 错误由正常的错误上报路径标红，这里再标一次只会重复。
//
// status <= 0 表示首次失败在传输层（没有状态码），此时不写状态码属性，
// 避免看板上出现无意义的 0 值影响聚合。
//
// toModel 为空表示切换未能完成（RecordFailoverExhausted 的场景）。
// finalStatus 是切换后上游返回的状态码，决定 outcome：
//   - < 400（含传输层成功的 0）视为 succeeded，请求被救回；
//   - >= 400 视为 exhausted，备用模型同样失败，主备双挂。
//
// 判据放在这里而不是各调用点：调用点只知道「重试请求发出去了」，
// 把「发出去」误当成「救回来」曾让 exhausted 形态完全查不到。
func RecordFailover(span trace.Span, stage, fromModel, toModel string, status, finalStatus int) {
	outcome := "succeeded"
	if finalStatus >= 400 {
		outcome = "exhausted"
	}
	recordFailover(span, stage, fromModel, toModel, status, outcome)
}

// RecordFailoverAborted 记录「重试请求根本没发出去」。
//
// 与 exhausted 不同：那是备用模型也返回了错误，这是切换动作自身失败
// （改写请求体失败、构造请求失败、连不上备用上游）。两者的排查方向
// 完全相反 —— 前者要查上游容量，后者要查网关配置。
//
// 不写 to_model：没有任何模型承接过这个请求。
func RecordFailoverAborted(span trace.Span, stage, fromModel string, status int) {
	recordFailover(span, stage, fromModel, "", status, "aborted")
}

func recordFailover(span trace.Span, stage, fromModel, toModel string, status int, outcome string) {
	if span == nil || !span.IsRecording() {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 6)
	attrs = append(attrs,
		attribute.Bool(AttrFailover, true),
		attribute.String(AttrFailoverStage, stage),
		attribute.String(AttrFailoverOutcome, outcome),
	)
	if status > 0 {
		attrs = append(attrs, attribute.Int(AttrFailoverStatus, status))
	}
	if fromModel != "" {
		attrs = append(attrs, attribute.String(AttrFailoverFrom, fromModel))
	}
	if toModel != "" {
		attrs = append(attrs, attribute.String(AttrFailoverTo, toModel))
	}
	span.SetAttributes(attrs...)
}

// ErrorBodyLimit 报告允许上报的错误正文字节上限。
func (p *Provider) ErrorBodyLimit() int {
	if p == nil {
		return 0
	}
	return p.errBodyBytes
}

// RecordError 把一个 Go 错误完整落到 span 上。
//
// 同时做三件事，缺一不可：
//   - SetStatus(Error) 让 span 在看板上标红，参与错误率统计；
//   - RecordError 生成 exception 事件，带上错误类型与完整 Error() 串。
//     这是 OTel 里承载「详细错误信息」的标准位置，只 SetStatus 会让
//     Logfire 的异常面板空着；
//   - kind 属性给出粗分类，与指标的 outcome 标签对齐。
//
// 对 nil span、nil err 安全：调用方无需在每个错误分支上包 if。
func RecordError(span trace.Span, kind string, err error) {
	if span == nil || !span.IsRecording() {
		return
	}
	if kind != "" {
		span.SetAttributes(attribute.String(AttrErrKind, kind))
	}
	if err == nil {
		// 没有 Go error 的分支（配置缺失、请求被拒等）至少要让 kind 进
		// 描述，否则 Logfire 上是一条没有任何文字说明的红 span。
		span.SetStatus(codes.Error, kind)
		return
	}

	msg := err.Error()
	attrs := []attribute.KeyValue{
		// 完整错误串独立成属性：status 描述会被看板截短，属性不会。
		attribute.String(AttrErrDetail, msg),
		attribute.String(AttrErrType, fmt.Sprintf("%T", err)),
	}
	// 根因与最外层不同才上报，避免给单层错误加一个重复字段。
	if cause := rootCause(err); cause != "" && cause != msg {
		attrs = append(attrs, attribute.String(AttrErrCause, cause))
	}
	attrs = append(attrs, structuredAttrs(err)...)
	span.SetAttributes(attrs...)

	// WithStackTrace 关掉：错误来自上游 IO 或配置校验，栈指向的永远是
	// 网关内部的转发点，对定位问题没有帮助，却让每个 span 多出几 KB。
	span.RecordError(err)
	span.SetStatus(codes.Error, msg)
}

// RecordGatewayError 记录网关自身产生的错误（非上游故障）。
//
// 这类分支的特点是「往往没有 Go error」：上游未配置、模型未映射、
// 闸门拒绝等都是网关主动判定的失败。它们同样必须在看板上可见且带原因，
// 否则客户端拿到 400/502/503，而 Logfire 上只有一条无说明的红 span，
// 排查只能回去翻客户端日志。
//
// clientMsg 是返回给客户端的那句话，作为兜底的失败说明。
// err 可为 nil。
func RecordGatewayError(span trace.Span, kind, clientMsg string, err error) {
	if span == nil || !span.IsRecording() {
		return
	}
	if clientMsg != "" {
		span.SetAttributes(attribute.String(AttrErrClientMsg, clientMsg))
	}
	if err != nil {
		RecordError(span, kind, err)
		return
	}
	// 无 Go error：用对外消息当描述，它比 kind 更具体
	// （"model `x` is not available" vs "unmapped_model"）。
	if kind != "" {
		span.SetAttributes(attribute.String(AttrErrKind, kind))
	}
	if clientMsg == "" {
		clientMsg = kind
	}
	span.SetAttributes(attribute.String(AttrErrDetail, clientMsg))
	span.SetStatus(codes.Error, clientMsg)
}

// RecordAttemptFailure 记录一次「失败但会被重试」的尝试。
//
// 与 RecordError 的关键区别：**不改 span 状态**。故障切换成功时整个请求
// 是成功的，把 span 标红会让看板上出现大量假故障；但中间那次失败的原因
// 必须留痕 —— 「为什么老在走兜底模型」只能从这里看出来，否则首次失败
// 的原因（配额耗尽？该模型下线了？）被彻底丢弃。
//
// 用 span 事件而非属性承载：一次请求可能有多次尝试，属性会互相覆盖，
// 事件天然可重复且带各自的时间戳。
//
// stage 说明失败发生在重试的哪一步（rewrite / build / transport / status），
// 定位「切换本身没生效」时是第一手线索。
//
// urlPath 是**上游**请求的实际路径（如 /api/v3/chat/completions），
// 落在 AttrUpstreamPath 而非 url.path —— 后者属于入站请求，覆盖它会让
// 看板上的客户端请求路径显示成上游路径，等于伪造事实。
//
// errOrBody 可以是 Go error（transport / build 阶段）或 []byte 错误正文
// （status 阶段，此时上游返回了 4xx/5xx 响应体）。status 阶段必须传正文：
// 429/503 的具体原因（配额类型、剩余额度、建议等待时间）全在正文里。
func RecordAttemptFailure(span trace.Span, stage, model, urlPath string, status int, errOrBody interface{}) {
	if span == nil || !span.IsRecording() {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 10)
	attrs = append(attrs, attribute.String("gateway.attempt.stage", stage))
	if model != "" {
		attrs = append(attrs, attribute.String("gateway.attempt.model", model))
	}
	if urlPath != "" {
		attrs = append(attrs, attribute.String(AttrUpstreamPath, urlPath))
	}
	if status > 0 {
		attrs = append(attrs, attribute.Int("gateway.attempt.status_code", status))
	}

	switch v := errOrBody.(type) {
	case error:
		if v != nil {
			attrs = append(attrs,
				attribute.String(AttrErrDetail, v.Error()),
				attribute.String(AttrErrType, fmt.Sprintf("%T", v)),
			)
			if cause := rootCause(v); cause != "" && cause != v.Error() {
				attrs = append(attrs, attribute.String(AttrErrCause, cause))
			}
			attrs = append(attrs, structuredAttrs(v)...)
		}
	case []byte:
		if len(v) > 0 {
			// 上游错误正文，按 UTF-8 截断到合理长度（默认 2KB）。
			// 不走 RecordErrorBody：它会写 AttrErrBody 等全局属性，
			// 而事件内的属性必须用带命名空间的键以区分多次尝试。
			const limit = 2048
			frag, trunc := truncateUTF8(v, limit)
			attrs = append(attrs,
				attribute.String("gateway.attempt.error_body", frag),
				attribute.Int("gateway.attempt.error_body_size", len(v)),
			)
			if trunc {
				attrs = append(attrs, attribute.Bool("gateway.attempt.error_truncated", true))
			}
		}
	}

	span.AddEvent("gateway.attempt_failed", trace.WithAttributes(attrs...))
}

// AttrProvider 由「自身携带结构化诊断字段」的错误类型实现。
//
// 用接口而非在 obs 里按具体类型分支：obs 是被 gateway、upstream 共同依赖的
// 底层包，反向 import 会成环。任何错误类型只要实现 Attrs() 就能把自己的
// 字段带上看板，无需改动这里。
//
// 键名由错误类型自己决定 —— 它才知道自己的字段该怎么命名，
// 在这里做映射等于把命名知识分散到两个包。
type AttrProvider interface {
	Attrs() map[string]string
}

// structuredAttrs 沿错误链找第一个 AttrProvider，取其结构化字段。
//
// 只取第一个：错误链上出现多个实现时，最外层离故障现场最近。
//
// 存在的价值在于把「字符串里的信息」变成「可聚合的维度」。超时错误的
// Error() 串里虽然也有阈值和耗时，但看板无法对字符串做
// 「按 kind 分组、按 elapsed 排序」这类操作。
func structuredAttrs(err error) []attribute.KeyValue {
	var ap AttrProvider
	if !errors.As(err, &ap) {
		return nil
	}
	m := ap.Attrs()
	if len(m) == 0 {
		return nil
	}
	// 键排序后输出：属性顺序稳定，测试与看板对照时不会随 map 遍历漂移。
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := make([]attribute.KeyValue, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, attribute.String(k, m[k]))
	}
	return attrs
}

// rootCause 把错误链 unwrap 到底，返回最内层错误的描述。
//
// 传输层错误在 Go 里是层层包裹的：*url.Error → *net.OpError →
// *os.SyscallError → syscall.Errno。最外层只说明「哪个请求失败了」，
// "connection refused" / "no such host" / TLS 证书错误都在最里层。
func rootCause(err error) string {
	for {
		next := errors.Unwrap(err)
		if next == nil {
			return err.Error()
		}
		err = next
	}
}

// RecordUpstreamError 把上游返回的错误响应完整落到 span 上。
//
// 与 RecordError 的区别：这里没有 Go error —— 连接是成功的，是上游
// 应用层返回了 4xx/5xx。真正有诊断价值的信息全在响应正文里，所以正文
// 按原样上报，不做字段提取。上游的错误结构各家不同（OpenAI 的
// error.message、Anthropic 的 error.type、火山的 InvalidParameter），
// 网关猜哪个字段重要必然会漏，把原文交给看板由人判断更可靠。
//
// urlPath 是**上游**请求的实际路径（如 /api/v3/chat/completions），
// 落在 AttrUpstreamPath 而非 url.path —— 后者属于入站请求，覆盖它会让
// 看板上的客户端请求路径显示成上游路径，等于伪造事实。
//
// body 超过 limit 时截断，并通过 AttrErrBodySize / AttrErrTruncated
// 让看板上能看出「这不是全文」。limit <= 0 表示不上报正文。
func RecordUpstreamError(span trace.Span, status int, urlPath, contentType string, body []byte, limit int) {
	if span == nil || !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String(AttrErrKind, "upstream_status"))
	if urlPath != "" {
		span.SetAttributes(attribute.String(AttrUpstreamPath, urlPath))
	}
	frag := RecordErrorBody(span, contentType, body, limit)

	msg := "upstream status " + strconv.Itoa(status)
	if frag != "" {
		// 正文进 status 描述而不只进属性：Logfire 的 trace 列表直接显示
		// 描述，能不展开 span 就看到失败原因。
		msg += ": " + frag
	}
	span.SetStatus(codes.Error, msg)
}

// RecordErrorBody 只把错误正文及其元信息落到 span，不改 span 状态。
//
// 供「已有 Go error 会另行 RecordError、但手上还握着一段上游正文」的
// 场景使用（如读正文中途失败，已读前缀往往就是上游错误 JSON 的开头）。
// 返回实际上报的片段，便于调用方拼进 status 描述；未上报时返回空串。
func RecordErrorBody(span trace.Span, contentType string, body []byte, limit int) string {
	if span == nil || !span.IsRecording() || len(body) == 0 {
		return ""
	}
	attrs := make([]attribute.KeyValue, 0, 4)
	attrs = append(attrs, attribute.Int(AttrErrBodySize, len(body)))
	if contentType != "" {
		attrs = append(attrs, attribute.String(AttrErrContentType, contentType))
	}
	var frag string
	if limit > 0 {
		var truncated bool
		frag, truncated = truncateUTF8(body, limit)
		attrs = append(attrs, attribute.String(AttrErrBody, frag))
		if truncated {
			attrs = append(attrs, attribute.Bool(AttrErrTruncated, true))
		}
	}
	span.SetAttributes(attrs...)
	return frag
}

// truncateUTF8 把 b 截到不超过 limit 字节，且不切断多字节字符。
//
// 直接 string(b[:limit]) 会在中文错误消息上切出半个字符，得到的属性值
// 含替换符、在看板上显示为乱码。这里回退到最近的字符边界。
func truncateUTF8(b []byte, limit int) (string, bool) {
	if len(b) <= limit {
		return string(b), false
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(b[cut]) {
		cut--
	}
	// 整个窗口都在一个超长字符中间（异常输入）时按原长度切，
	// 宁可留一个替换符也不要返回空串。
	if cut == 0 {
		cut = limit
	}
	return string(b[:cut]), true
}
