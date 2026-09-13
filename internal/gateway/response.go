package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"syscall"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/http2"

	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/protocol"
	"github.com/betterme/remap-service/internal/sanitize"
	"github.com/betterme/remap-service/internal/upstream"
)

// transformer 把「协议规格 + 替换器」适配为 SSE 行处理器。
//
// 只处理 data 行里的 JSON 载荷，且交给 Spec.Sanitize 做字段级脱敏；
// 非 data 行（event: / id: / 注释）原样保留 —— 上游不会把模型名写进那里，
// 贸然替换反而可能破坏事件名。
type transformer struct {
	spec   *protocol.Spec
	rep    *sanitize.Replacer
	public string
}

func (t transformer) Data(payload []byte) ([]byte, protocol.SanitizeReport) {
	return t.spec.SanitizeWithReport(payload, t.public, t.rep)
}

// pipeStream 转发 SSE 流式响应，逐 chunk 脱敏并即时 flush。
//
// cancel 是本次上游请求的取消函数，交给空闲超时看门狗使用 ——
// 只有取消请求 context 才能真正让传输层放弃这条连接。
func (g *Gateway) pipeStream(
	ctx context.Context, w http.ResponseWriter, resp *http.Response,
	spec *protocol.Spec, rep *sanitize.Replacer, st *state, span trace.Span,
	cancel context.CancelFunc,
) int {
	h := w.Header()
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)

	// 与正文共用同一个 transformer：合成事件必须走**同一条**脱敏路径，
	// 而不是一条长得像的旁路。见下面 protocol.SanitizeEvent 处的说明。
	var tr protocol.Transformer
	if g.cfg.SanitizeEnabled() && st.publicModel != "" {
		tr = transformer{spec: spec, rep: rep, public: st.publicModel}
	}

	// 首事件提前下发。响应头已到 = 上游接受了这次请求，但真实的首个 token
	// 还要等整段 prefill；这段时间里流虽已建立，客户端却收不到任何事件，
	// 所有「以首个 SSE 事件为口径」的首字统计都会把 prefill 算进去。
	// 先合成一个内容为空、协议合法的首事件，把这段等待变成「流已开始」。
	//
	// ⚠️ 合成事件必须过 tr。它不是上游来的，天然不经过 PipeSSE，因此是整条流
	// 里唯一一段「本进程构造、却能原样写进客户端响应」的字节 —— 这是本功能
	// 唯一的泄漏面。只加一条「里面必须是对外名」的断言不够：断言只能证明当前
	// 那行代码写对了，挡不住后续任何改动（改错字段、或将来往事件里补了上游的
	// id / system_fingerprint）。走同一条路径之后，model 字段会被 Spec 的
	// ModelPaths 规则**整体覆盖**为对外名，合成事件的 model 与正文的必然一致；
	// 其余字段里的上游标识也会被同一把替换器擦掉。泄漏面由此从「约定」变成
	// 「结构上不成立」。
	//
	// 连写 PreambleCopies 份：下游逐块转发时会延迟一块（收到第 N 块才发
	// 第 N-1 块），只发一份的话提前量会被那一块的延迟吃掉。原因与实测数字
	// 见 protocol.PreambleCopies 的注释。
	//
	// 写在 flush 之前是有意的：合并成一次 flush，不多付 syscall。
	// 未启用、或该路由没有安全形态时 len(pre)==0，行为与从前逐字节一致。
	if pre := protocol.SanitizeEvent(g.earlyFirstEvent(st), tr); len(pre) > 0 {
		for i := 0; i < protocol.PreambleCopies; i++ {
			n, werr := w.Write(pre)
			st.preambleBytes += n
			if werr != nil {
				break
			}
		}
		st.preamble = st.preambleBytes > 0
	}

	fl, _ := w.(http.Flusher)
	if fl != nil {
		fl.Flush() // 立刻把响应头（以及上面那个合成首事件）推给客户端。
	}

	// 空闲超时：响应头已到、流已建立，但上游中途不再吐数据。
	//
	// 这是首字节与总超时都覆盖不到的形态：看门狗此刻已解除，而总超时
	// 要等到 StreamTimeout（默认 30 分钟）才生效，中间这段时间请求一直
	// 占着并发名额、连接和内存，客户端却什么都收不到。
	var src io.Reader = resp.Body
	stall := upstream.NewStallReader(resp.Body, g.cfg.Upstream.StallTimeout, cancel)
	if stall != nil {
		src = stall
		// 负责停掉计时器。底层响应体的关闭仍由 handle 的 defer 负责，
		// 重复 Close 是幂等的。
		defer stall.Close()
	}

	stats, err := protocol.PipeSSE(w, src, tr, fl)
	st.sseEvents = stats.Events
	st.bytesOut = stats.Bytes
	st.rewrites = stats.Rewrote
	// 合成首事件确实写到了客户端，字节要计入 out_bytes / bytes.out ——
	// 那两个口径是「下行体积」，少算就等于出口流量与日志对不上。
	// sse_events 保持「上游转发的事件数」不变，合成事件由
	// gateway.stream.preamble 单独计数：两个信号各自可数，「有多少事件
	// 是合成的」才回答得了。
	if st.preamble {
		st.bytesOut += int64(st.preambleBytes)
	}

	a := st.metricAttrs("stream", resp.StatusCode)
	obs.Add(ctx, st.mx.SSEEvents, stats.Events, a)
	obs.Add(ctx, st.mx.BytesOut, st.bytesOut, a)
	obs.Add(ctx, st.mx.Rewrites, stats.Rewrote, a)
	// 抑制信号与改写数并列上报：单看任何一个都分不清「响应里根本没出现
	// 上游标识」与「出现了但全被长度阈值放行」。后者是唯一的静默漏点。
	obs.Add(ctx, st.mx.SanitizeSuppressed, stats.Suppressed, a)
	if st.preamble {
		obs.Add(ctx, st.mx.StreamPreamble, 1, a)
	}
	span.SetAttributes(
		attribute.Int64("gateway.sse.events", stats.Events),
		attribute.Int64("gateway.sse.bytes", stats.Bytes),
		attribute.Int64("gateway.sanitize.suppressed", stats.Suppressed),
		// 合成首事件必须单独留痕、且与 sse.events/bytes 并列：缺了它，
		// 「首字变快」与「上游真的变快」在 trace 列表与曲线上完全同形，
		// 而这正是打开 STREAM_EARLY_FIRST_EVENT 后最容易被误读的地方。
		attribute.Bool("gateway.stream.preamble", st.preamble),
		attribute.Int("gateway.stream.preamble_bytes", st.preambleBytes),
	)

	if err != nil {
		st.err = err
		switch {
		// 空闲超时优先判定，顺序不能与下面互换：它通过 cancel 断开，
		// 读操作返回的是 context.Canceled，而 isClientGone 恰好看作
		// 「客户端提前断开」—— 那不是故障、不标红、不计错误率。
		// 判定顺序反了，最该告警的形态会被静默吞掉。
		case stall.Tripped():
			st.outcome = "stream_stall"
			// 用带阈值的超时错误替换裸的 context.Canceled：
			// 后者落到看板上只有一句 "context deadline exceeded"。
			st.err = stall.Err(resp.Request.URL)
			obs.RecordError(span, st.outcome, st.err)
			obs.Add(ctx, st.mx.UpstreamErr, 1, a)

		// 客户端提前断开不是故障：span 保持 Unset，也不计上游错误，
		// 否则长耗时流式请求会因用户主动取消而在看板上大面积泛红。
		case isClientGone(err):
			st.outcome = "client_gone"
			span.SetAttributes(attribute.Bool("gateway.client_gone", true))

		// 上游正常收尾（EOF 截断 / GOAWAY NO_ERROR）同样不是故障。
		// 已下发的事件全部有效，客户端拿到的内容是完整的。
		case isUpstreamGracefulEnd(err):
			st.outcome = "stream_eof"
			st.err = nil // 不进错误日志，避免正常收尾刷屏
			span.SetAttributes(attribute.Bool("gateway.upstream_eof", true))

		default:
			st.outcome = "stream_broken"
			// 中断位置由上面已上报的 gateway.sse.events / bytes 给出
			// （它们就是中断时的计数），无需重复上报。
			// RecordError 会带上完整错误串、Go 类型与 unwrap 后的根因 ——
			// 流中断的真因（h2 RST_STREAM 码、TLS 断连）都在错误链里层。
			obs.RecordError(span, st.outcome, err)
			obs.Add(ctx, st.mx.UpstreamErr, 1, a)
		}
	}
	return resp.StatusCode
}

// earlyFirstEvent 返回本次流式响应最前面要合成的首事件，未启用时返回 nil。
//
// 判据只认 st.route —— 由**入站**路径派生的低基数标签。协议形态取决于
// 入站路径，与上游实际拼出来的路径无关：上游前缀会被 buildURL 剥掉
// （base=https://ark…/api/v3 + /v1/chat/completions →
// /api/v3/chat/completions），拿上游路径去匹配只会得到「配了却不生效」，
// 而且不会有任何报错。
func (g *Gateway) earlyFirstEvent(st *state) []byte {
	if !g.cfg.Stream.EarlyFirstEvent {
		return nil
	}
	return protocol.Preamble(st.route, st.publicModel)
}

// pipeBuffered 处理非流式响应：整体读入后做字段级脱敏。
//
// # 超过 MAX_SANITIZE_BYTES 为什么直接失败，而不是原文直通
//
// 原文直通会在客户端毫不知情的情况下把上游真实标识原样送出去 ——
// 正常响应里的 model / id / system_fingerprint，错误正文里的真实模型名，
// 全都是脱敏存在的理由。**一次明确的失败是可观测、可重现的；一次静默的
// 泄漏不是。** 宁可让客户端拿到一个 400，也不要让它拿到一份未经处理的响应。
//
// 状态码取 400 而非 5xx：上游正常返回了，是网关基于策略拒绝转发。记成 5xx
// 会把责任错误地推给上游，让上游侧的错误率被这类请求污染。
//
// 判据是 needSanitize，而不是「脱敏开关是否打开」：脱敏关闭、或本次请求
// 没有 model 字段时本来就不会执行任何替换，这时上限只剩「防止大响应打爆
// 内存」一个作用，透传是对的。对这类响应报 400 会让 SANITIZE_OFF 的部署
// 整类失败，而它并不存在泄漏面。
func (g *Gateway) pipeBuffered(
	ctx context.Context, w http.ResponseWriter, resp *http.Response,
	spec *protocol.Spec, rep *sanitize.Replacer, st *state, span trace.Span,
) int {
	limit := g.cfg.Limits.MaxSanitizeBytes
	needSanitize := g.cfg.SanitizeEnabled() && st.publicModel != ""

	// 需要脱敏，且 Content-Length 已经表明超限：在读任何字节之前就拒绝。
	// 这条路径读进来只为丢掉，白占内存也白占带宽。
	//
	// ContentLength < 0 表示长度未知（chunked），不能据此判定，留给下面
	// 读完之后的检查。
	if needSanitize && resp.ContentLength > limit {
		return g.rejectOversizeResponse(w, resp, st, span, nil)
	}

	// 无需脱敏：零拷贝直通。
	//
	// 例外：4xx/5xx 不走这条路。错误正文是排查的唯一线索，必须先读进内存
	// 才能上报；且错误响应一定很小（各家上游的错误 JSON 都在几百字节内），
	// 缓冲它不构成内存风险 —— 真正需要防的是正常响应的大 body。
	if resp.StatusCode < 400 && !needSanitize {
		if resp.ContentLength >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
		}
		w.WriteHeader(resp.StatusCode)
		n, err := io.Copy(w, resp.Body)
		st.bytesOut = n
		if err != nil {
			st.err = err
			obs.RecordError(span, "write_client_failed", err)
		}
		obs.Add(ctx, st.mx.BytesOut, n, st.metricAttrs(st.outcome, resp.StatusCode))
		return resp.StatusCode
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		st.err = err
		st.outcome = "read_upstream_failed"
		// 读上游正文中断：io.ReadAll 出错时仍返回已读到的部分，据实上报
		// 字节数以区分「一个字节没来」（多为上游拒连）和「读到一半断了」
		// （多为链路中断）。已读前缀本身也常含上游的错误 JSON 开头。
		span.SetAttributes(attribute.Int("gateway.upstream.read_bytes", len(body)))
		if len(body) > 0 {
			obs.RecordErrorBody(span, resp.Header.Get("Content-Type"), body, g.o.ErrorBodyLimit())
		}
		obs.RecordError(span, st.outcome, err)
		return writeError(w, http.StatusBadGateway, "upstream_error", "failed to read upstream response")
	}
	if int64(len(body)) > limit {
		// 走到这里说明长度未知或不可信（chunked，或上游声明的长度与实际
		// 不符），只能读完才发现超限 —— 上限本身保证了最多多读一个字节。
		if needSanitize {
			return g.rejectOversizeResponse(w, resp, st, span, body)
		}
		// 不需要脱敏时超限只是内存问题：已读部分 + 剩余流原样透传。
		w.WriteHeader(resp.StatusCode)
		n1, _ := w.Write(body)
		n2, _ := io.Copy(w, resp.Body)
		st.bytesOut = int64(n1) + n2
		st.outcome = "sanitize_skipped_too_large"
		if resp.StatusCode >= 400 {
			g.recordUpstreamStatus(resp, body, st, span)
		}
		return resp.StatusCode
	}

	// needSanitize 必须重新判断：4xx/5xx 会绕过上面的直通分支走到这里，
	// 此时 rep 可能为 nil（脱敏关闭或请求无 model 字段），无条件调用
	// Sanitize 等于在关闭脱敏的部署里悄悄启用它。
	out := body
	suppressed := false
	if needSanitize && protocol.LooksLikeJSON(body) {
		nb, report := spec.SanitizeWithReport(body, st.publicModel, rep)
		suppressed = report.Suppressed
		if report.Changed {
			out = nb
			st.rewrites++
		}
	}
	// 非流式这条路径最容易踩到长度阈值：上游的长错误 message（配额超限、
	// 多行校验错误、回显了整个请求的 400）几乎都超过 SANITIZE_MAX_VALUE_LEN，
	// 因而被整体放行。落一个计数 + span 属性，让「静默漏点」可被筛出来。
	if suppressed {
		obs.Add(ctx, st.mx.SanitizeSuppressed, 1, st.metricAttrs(st.outcome, resp.StatusCode))
		span.SetAttributes(attribute.Bool("gateway.sanitize.suppressed", true))
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(out)))
	w.WriteHeader(resp.StatusCode)
	n, werr := w.Write(out)
	st.bytesOut = int64(n)
	if werr != nil {
		st.err = werr
	}

	a := st.metricAttrs(st.outcome, resp.StatusCode)
	obs.Add(ctx, st.mx.BytesOut, int64(n), a)
	obs.Add(ctx, st.mx.Rewrites, st.rewrites, a)
	if resp.StatusCode >= 400 {
		// 上报**原始** body 而非脱敏后的 out：客户端拿到的仍是 out（脱敏过，
		// 上面已写出），看板要的是上游到底说了什么。脱敏会把上游真实模型名
		// 换掉，而"model xxx not found""quota exceeded for xxx"这类错误里，
		// 那个模型名正是排查的全部信息量 —— 上报脱敏版等于把线索擦掉。
		//
		// 这是一个有意的权衡：上游形态只泄漏到自己的可观测性后端，不流向
		// 客户端。若看板属于第三方，这条需要改回 out。
		g.recordUpstreamStatus(resp, body, st, span)
	}
	return resp.StatusCode
}

// rejectOversizeResponse 拒绝一个「需要脱敏但超出上限」的响应。
//
// prefix 是已经读进内存的正文前缀（走 Content-Length 提前判定的路径时为 nil），
// 只用于上报、绝不回给客户端 —— 上游为什么返回这么大一段，是排查这类拒绝时
// 唯一的线索。**仅在上游本身是错误响应时才上报**：正常响应的那一段是模型
// 生成的内容，既没有诊断价值，也不该被搬到看板上。
func (g *Gateway) rejectOversizeResponse(
	w http.ResponseWriter, resp *http.Response, st *state, span trace.Span, prefix []byte,
) int {
	st.outcome = "response_too_large"

	// 此刻 w 上还挂着 copyResponseHeaders 复制过来的上游响应头。必须清掉：
	// 它们属于「上游正常响应」这个上下文，挂在一个网关自己产生的 400 上
	// 既不成立，也可能把上游标识（Server、各家 logid）带出去。
	// writeError 只补 Content-Type，不负责清理。
	h := w.Header()
	for k := range h {
		h.Del(k)
	}

	// 上游真实状态码与错误正文仍要留痕，否则「上游本来返回了什么」永久丢失。
	//
	// Content-Length 提前判定的路径上还没读过正文，这里按上报上限补读一个
	// 前缀：上游报错时「为什么返回这么大一段」正是排查的关键，而错误响应
	// 本就该是几百字节，超大属异常形态。正常响应不读 —— 那是一段模型生成的
	// 内容，既无诊断价值，也不该被搬到看板上。
	if resp.StatusCode >= 400 {
		if prefix == nil {
			if n := g.o.ErrorBodyLimit(); n > 0 {
				prefix, _ = io.ReadAll(io.LimitReader(resp.Body, int64(n)))
			}
		}
		if len(prefix) > 0 {
			// 顺序不能反：RecordUpstreamError 会把 gateway.error.kind 写成
			// upstream_status，盖掉本函数的 response_too_large —— 看板上就
			// 分不出「上游失败」与「网关因过大而拒发」这两种不同的成因。
			g.recordUpstreamStatus(resp, prefix, st, span)
		}
	}

	msg := "upstream response too large to sanitize (limit " +
		strconv.FormatInt(g.cfg.Limits.MaxSanitizeBytes, 10) +
		" bytes); reduce the response size or raise MAX_SANITIZE_BYTES"
	obs.RecordGatewayError(span, st.outcome, msg, nil)
	return writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
}

// recordUpstreamStatus 把上游的错误响应（状态码 + 正文 + 关键头）落到 span。
//
// 上游报错时，状态码只说明「失败了」，正文才说明「为什么」。各家上游的
// 错误结构互不相同，网关不做字段提取、原样上报，由看板侧判断。
//
// 额外单独提取 request_id 与 retry-after：前者是找上游对账的唯一凭据，
// 后者决定客户端该等多久，两者都值得成为可直接筛选的属性。
//
// body 一律传上游原文（未脱敏）。因此不再有 body_sanitized 标记 —— 恒为
// false 的属性没有信息量，只会让人以为存在"有时脱敏"的另一种情况。
func (g *Gateway) recordUpstreamStatus(resp *http.Response, body []byte, st *state, span trace.Span) {
	if id := upstreamRequestID(resp.Header); id != "" {
		span.SetAttributes(attribute.String("gateway.upstream.request_id", id))
	}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		span.SetAttributes(attribute.String("http.response.retry_after", ra))
	}
	obs.RecordUpstreamError(span, resp.StatusCode, resp.Request.URL.Path,
		resp.Header.Get("Content-Type"), body, g.o.ErrorBodyLimit())
}

// upstreamRequestID 从响应头里找上游的请求标识。
//
// 各家用的头名不同，按常见程度排列取第一个非空：出错找上游对账时，
// 这个 ID 是唯一能让对方定位到同一次调用的凭据。
var requestIDHeaders = []string{
	"X-Request-Id",     // OpenAI、多数网关
	"Request-Id",       // Anthropic
	"X-Amzn-Requestid", // Bedrock
	"X-Ms-Request-Id",  // Azure OpenAI
	"X-Tt-Logid",       // 火山方舟
}

func upstreamRequestID(h http.Header) string {
	for _, k := range requestIDHeaders {
		if v := h.Get(k); v != "" {
			return v
		}
	}
	return ""
}

// isClientGone 判断错误是否源于客户端提前离开（主动取消或连接被关闭），
// 而非上游或网关自身故障。这类错误在流式长连接里属于常态，
// 不应污染 span 状态与上游错误率。
//
// 覆盖的形态均经诊断测试实测确认（见 classify_test.go）：
//   - context.Canceled：客户端关闭请求导致 ctx 取消
//   - io.ErrClosedPipe：向已关闭的响应体写入
//   - EPIPE / ECONNRESET：TCP 层对端已断
//   - http2 RST_STREAM(CANCEL)：h2 客户端取消的等价形态
func isClientGone(err error) bool {
	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, io.ErrClosedPipe),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, http.ErrBodyReadAfterClose):
		return true
	}
	// HTTP/2 下客户端取消不映射为上面任何一个 errno，而是 RST_STREAM。
	var se http2.StreamError
	if errors.As(err, &se) {
		return se.Code == http2.ErrCodeCancel
	}
	return false
}

// isUpstreamGracefulEnd 判断错误是否为上游「正常收尾」而非故障。
//
// 大模型上游有两种常见的合法收尾方式会在 Go 侧表现为 error：
//
//   - io.ErrUnexpectedEOF：生成结束后直接关闭连接，末个事件没有终止换行。
//     SSE 规范允许流以 EOF 结束，此时已下发的事件全部有效，客户端也已拿到
//     完整内容 —— 记成故障会让看板出现与实际体验不符的错误率。
//   - GOAWAY(NO_ERROR)：上游实例优雅下线/滚动重启，通知本端不要再复用连接。
//     这是协议层的正常信号，与「上游挂了」是两件事。
//
// 刻意不覆盖的形态：GOAWAY 携带非 NO_ERROR 错误码、RST_STREAM(INTERNAL_ERROR)
// 等，它们代表上游真异常，必须计入错误率。
func isUpstreamGracefulEnd(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ga http2.GoAwayError
	if errors.As(err, &ga) {
		return ga.ErrCode == http2.ErrCodeNo
	}
	return false
}

// writeError 以 OpenAI 兼容格式返回网关自身产生的错误。
func writeError(w http.ResponseWriter, status int, typ, msg string) int {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Del("Content-Length")
	w.WriteHeader(status)
	_, _ = w.Write(errorJSON(typ, msg))
	return status
}

func errorJSON(typ, msg string) []byte {
	buf := make([]byte, 0, 96+len(typ)+len(msg))
	buf = append(buf, `{"error":{"message":`...)
	buf = appendJSONString(buf, msg)
	buf = append(buf, `,"type":`...)
	buf = appendJSONString(buf, typ)
	buf = append(buf, `,"param":null,"code":null}}`...)
	return buf
}

func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			dst = append(dst, '\\', '"')
		case '\\':
			dst = append(dst, '\\', '\\')
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			if c < 0x20 {
				const hex = "0123456789abcdef"
				dst = append(dst, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				dst = append(dst, c)
			}
		}
	}
	return append(dst, '"')
}
