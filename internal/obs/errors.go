package obs

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

	// AttrUpstreamStatus 是**上游**返回的 HTTP 状态码（含 failover 与非 failover 场景）。
	//
	// 与 http.response.status_code 刻意分开：后者是客户端实际收到的最终状态码，
	// failover 成功时它是 200；前者始终记录上游的真实响应，429/503 等失败状态码
	// 才能在 trace 列表直接筛选，不必逐条展开事件。
	//
	// 与 AttrFailoverStatus 的分工：那个只在 failover 触发时写入，记录首次失败；
	// 本键在所有上游响应（成功或失败）后都写入，是更通用的诊断维度。
	AttrUpstreamStatus = "gateway.upstream.status_code"

	// AttrMappingMatch 是模型映射的命中级别（exact/wildcard/fallback/none）。
	//
	// 「上游收到了错误的模型名」这类问题的第一问永远是：网关到底命中了
	// 哪条规则？此前该信息只进内部日志（match=...），看板上只有映射的
	// **结果**（gateway.upstream.model）而没有**决策过程**，一旦结果不符
	// 预期就只能去翻服务器日志。none 表示未命中任何规则、请求被原样透传
	// —— 正是「写错通配符导致上游 404」的形态，必须能在看板直接筛出来。
	AttrMappingMatch = "gateway.mapping.match"

	// AttrMappingSource 是映射表的来源（header/static）。
	//
	// Header 表与静态表叠加生效，同一个模型名在两边可能都有规则。
	// 排查「命中了但映射错了」时必须先知道规则来自哪一层。
	AttrMappingSource = "gateway.mapping.source"

	// AttrMappingInvalid 是 X-Model-Map 中解析失败的片段原文。
	//
	// 只进看板、绝不进对客户端的错误响应：片段可能含真实上游模型名，
	// 而 X-Model-Map 常由 new-api 这类中间层注入，400 会被原样透传给
	// 终端用户 —— 正是脱敏要挡掉的形态。客户端只收通用的格式说明。
	AttrMappingInvalid = "gateway.mapping.invalid"

	// AttrAttemptCount 是本次请求内失败尝试的总次数。
	//
	// 失败现场改用 span 属性承载后（见 RecordAttemptFailure 的注释），多次
	// 失败会互相覆盖，属性描述的只是**最后一次**。本键让覆盖这件事可见：
	// count > 1 表示前面还有被盖掉的失败，此时不能把属性读成「只失败过一次」。
	// 没有它，「第一次 429、第二次 503」与「只有一次 503」在看板上完全同形。
	AttrAttemptCount = "gateway.attempt.count"

	// 失败尝试的现场维度。stage 取 transport/status/rewrite/build ——
	// 「上游拒了」与「网关自己没把重试请求发出去」排查方向完全不同。
	//
	// 键名集中在此，不散在调用点：曾因 grep 不到无前缀的键名，
	// 出现过文档里怎么填都不生效且不报错的情况。
	AttrAttemptStage     = "gateway.attempt.stage"
	AttrAttemptModel     = "gateway.attempt.model"
	AttrAttemptStatus    = "gateway.attempt.status_code"
	AttrAttemptBody      = "gateway.attempt.error_body"
	AttrAttemptBodySize  = "gateway.attempt.error_body_size"
	AttrAttemptBodyTrunc = "gateway.attempt.error_truncated"

	// AttrLogfireMsg 覆盖 Logfire 记录在列表里渲染出来的文案。
	//
	// 只对 span 本身有效，**对 span event 无效**：事件行恒显示静态事件名，
	// 写了也不渲染，且 logfire.* 属性会被 Logfire 消费、不出现在 Attributes
	// 面板里 —— 于是「写了没生效」和「没写」在 UI 上完全同形，白耗一个属性。
	//
	// 这条约束正是本项目不再用 span event 记录失败尝试的原因：event 在
	// trace 列表里占一整行却改不掉文案，父 span 已写明
	// "→ 200 (upstream ... → 429)" 时，下面那行 gateway.attempt_failed
	// 是纯噪音。失败现场改由 RecordAttemptFailure 写进 span 属性。
	//
	// 只在信息确实与默认渲染不同时才写，且注意 Logfire 会在文案末尾自动
	// 追加 http.response.status_code —— 自己再写一遍最终状态码会出现
	// "→ 200 ... → 200" 的重复尾巴。
	AttrLogfireMsg = "logfire.msg"

	// AttrLogfireLevel 是 Logfire 的记录等级（OTel SeverityNumber）。
	//
	// span 没有原生 severity：Logfire 只认「span 状态为 ERROR → error，
	// 否则一律 info」。而 failover 成功的请求状态就是 OK（对客户端确实没
	// 故障，不能标红），于是被 429 顶掉这件事在等级上与一次普通成功毫无
	// 区别 —— 这才是「看板上找不到 429」的真正成因：不是没上报，是没有
	// 任何维度能把它从成功流量里筛出来。
	//
	// 写 13（warn）让它落在 level >= 'warn' 的筛选里：既不像 error 那样
	// 制造假故障（请求最终成功了），又不至于淹没在 info 里。
	AttrLogfireLevel = "logfire.level_num"

	// LevelWarn 是 OTel SeverityNumber 里的 WARN。
	//
	// 取值遵循 OTel 规范（trace=1 debug=5 info=9 notice=10 warn=13
	// error=17 fatal=21），不可自定义 —— Logfire 按这套数值映射等级名。
	LevelWarn = 13
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

// SetRequestMsg 改写请求 span 在 Logfire 列表里显示的那一行文案。
//
// 目标形态（末尾的 "→ 200" 由 Logfire 自动追加 http.response.status_code，
// 不由本函数写出）：
//
//	gateway /v1/chat/completions → 200 (upstream /api/v3/chat/completions) → 200
//	gateway /v1/chat/completions → 429 (upstream /api/v3/chat/completions → 429) → 429
//
// 两条规则：
//   - **上游路径恒进文案**。它是"这次请求实际打到哪个上游端点"的唯一现场，
//     而同一个入站路径会按模型映射打到不同上游（/api/v3/... 与
//     /compatible-mode/v1/...）。放在 span 属性里只能筛不能扫，列表上逐行
//     可见才能一眼看出流量的实际去向。
//   - **上游状态码只在上游失败（>= 400）时才进括号**。判据是"上游是否失败"，
//     不是"上游与客户端是否不同"：401/403 直通时两者相同，但那个状态码正是
//     上游给的、必须显示；而上游 200 时写它纯属重复。
//
// upstreamPath 为空且上游未失败时不写属性 —— 默认渲染已足够。
func SetRequestMsg(span trace.Span, route, upstreamPath string, clientStatus, upstreamStatus int) {
	if span == nil || !span.IsRecording() {
		return
	}
	upFailed := upstreamStatus >= 400
	if upstreamPath == "" && !upFailed {
		return
	}

	var b strings.Builder
	b.Grow(len(route) + len(upstreamPath) + 40)
	b.WriteString("gateway ")
	b.WriteString(route)
	b.WriteString(" → ")
	b.WriteString(strconv.Itoa(clientStatus))
	b.WriteString(" (upstream")
	if upstreamPath != "" {
		b.WriteByte(' ')
		b.WriteString(upstreamPath)
	}
	if upFailed {
		b.WriteString(" → ")
		b.WriteString(strconv.Itoa(upstreamStatus))
	}
	b.WriteByte(')')

	attrs := make([]attribute.KeyValue, 0, 2)
	attrs = append(attrs, attribute.String(AttrLogfireMsg, b.String()))
	// 只有「上游失败过、但客户端最终成功」才抬等级：那是 failover 救回来的
	// 请求，span 状态是 OK（标红等于伪造故障），不抬等级就永远埋在成功流量里。
	// 客户端自己也是 4xx/5xx 时无需抬 —— RecordUpstreamError 已 SetStatus
	// 为 Error，Logfire 渲染成 error 级，再写 warn 反而是降级。
	if upFailed && clientStatus < 400 {
		attrs = append(attrs, attribute.Int(AttrLogfireLevel, LevelWarn))
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
// 用 span 属性而非事件承载。事件本来更合适（可重复、带独立时间戳），但
// Logfire 把每条 event 渲染成 trace 列表里的独立一行，而 event 行的文案
// **改不掉**（logfire.msg 对 event 无效，恒显示静态事件名
// "gateway.attempt_failed"）。结果父 span 那行已经写明
// "→ 200 (upstream ... → 429)"，下面却多挂一行毫无信息量的
// gateway.attempt_failed —— 噪音换不来任何东西。
//
// 代价是**属性会互相覆盖**：一次请求内 status 失败后若紧接 rewrite/build/
// transport 失败，后者会盖掉前者。用 AttrAttemptCount 让覆盖这件事本身可见 ——
// count > 1 说明前面还有被盖掉的失败，此时属性描述的是**最后一次**。
// 不用 count 就会把「第一次 429、第二次 503」误读成只失败过一次。
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
//
// attempt 是本次请求内第几次失败（从 1 起）。由调用方持有计数而不在此处
// 累加：本函数保持无状态才能被并发请求共用，且计数落在 state 上与其余
// 请求级字段同源。
func RecordAttemptFailure(span trace.Span, stage, model, urlPath string, status, attempt int, errOrBody interface{}) {
	if span == nil || !span.IsRecording() {
		return
	}
	// 容量含末尾的 logfire.level_num 与 attempt_count，少算就会在失败路径上
	// 多一次切片扩容。
	attrs := make([]attribute.KeyValue, 0, 12)
	if attempt > 0 {
		attrs = append(attrs, attribute.Int(AttrAttemptCount, attempt))
	}
	attrs = append(attrs, attribute.String(AttrAttemptStage, stage))
	if model != "" {
		attrs = append(attrs, attribute.String(AttrAttemptModel, model))
	}
	if urlPath != "" {
		attrs = append(attrs, attribute.String(AttrUpstreamPath, urlPath))
	}
	if status > 0 {
		attrs = append(attrs, attribute.Int(AttrAttemptStatus, status))
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
			// 不走 RecordErrorBody：它写的 AttrErrBody 表示「返回给客户端
			// 的那个错误」，而这里是被重试掉的中间失败，混进同一个键会让
			// failover 成功的请求看起来也带客户端错误正文。
			const limit = 2048
			frag, trunc := truncateUTF8(v, limit)
			attrs = append(attrs,
				attribute.String(AttrAttemptBody, frag),
				attribute.Int(AttrAttemptBodySize, len(v)),
			)
			if trunc {
				attrs = append(attrs, attribute.Bool(AttrAttemptBodyTrunc, true))
			}
		}
	}

	// 抬等级到 warn，这是让失败尝试在看板上可筛的唯一手段。
	//
	// 不能靠标红 span：failover 成功时请求对客户端没故障，标红会制造假故障。
	// 于是默认下这条 429 的等级与成功流量完全同级、无从筛出 ——
	// 「看板上找不到 429」正是这么来的。
	//
	// 这里刻意不写 logfire.msg：父 span 的文案由 SetRequestMsg 统一生成
	// （已含上游路径与真实状态码），此处再写会互相覆盖，且谁最后写谁生效
	// 取决于调用顺序 —— 那是最难查的一类 bug。
	attrs = append(attrs, attribute.Int(AttrLogfireLevel, LevelWarn))

	span.SetAttributes(attrs...)
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
