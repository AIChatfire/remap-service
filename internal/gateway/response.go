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

func (t transformer) Data(payload []byte) ([]byte, bool) {
	return t.spec.Sanitize(payload, t.public, t.rep)
}

// pipeStream 转发 SSE 流式响应，逐 chunk 脱敏并即时 flush。
func (g *Gateway) pipeStream(
	ctx context.Context, w http.ResponseWriter, resp *http.Response,
	spec *protocol.Spec, rep *sanitize.Replacer, st *state, span trace.Span,
) int {
	h := w.Header()
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(resp.StatusCode)

	fl, _ := w.(http.Flusher)
	if fl != nil {
		fl.Flush() // 立刻把响应头推给客户端，缩短感知首包时间。
	}

	var tr protocol.Transformer
	if g.cfg.SanitizeEnabled() && st.publicModel != "" {
		tr = transformer{spec: spec, rep: rep, public: st.publicModel}
	}

	stats, err := protocol.PipeSSE(w, resp.Body, tr, fl)
	st.sseEvents = stats.Events
	st.bytesOut = stats.Bytes
	st.rewrites = stats.Rewrote

	a := st.metricAttrs("stream", resp.StatusCode)
	obs.Add(ctx, st.mx.SSEEvents, stats.Events, a)
	obs.Add(ctx, st.mx.BytesOut, stats.Bytes, a)
	obs.Add(ctx, st.mx.Rewrites, stats.Rewrote, a)
	span.SetAttributes(
		attribute.Int64("gateway.sse.events", stats.Events),
		attribute.Int64("gateway.sse.bytes", stats.Bytes),
	)

	if err != nil {
		st.err = err
		switch {
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

// pipeBuffered 处理非流式响应：整体读入后做字段级脱敏。
// 超过 MAX_SANITIZE_BYTES 的响应直接透传，避免大响应打爆内存。
func (g *Gateway) pipeBuffered(
	ctx context.Context, w http.ResponseWriter, resp *http.Response,
	spec *protocol.Spec, rep *sanitize.Replacer, st *state, span trace.Span,
) int {
	limit := g.cfg.Limits.MaxSanitizeBytes
	needSanitize := g.cfg.SanitizeEnabled() && st.publicModel != ""

	// 无需脱敏或响应过大：零拷贝直通。
	//
	// 例外：4xx/5xx 不走这条路。错误正文是排查的唯一线索，必须先读进内存
	// 才能上报；且错误响应一定很小（各家上游的错误 JSON 都在几百字节内），
	// 缓冲它不构成内存风险 —— 真正需要防的是正常响应的大 body。
	if resp.StatusCode < 400 && (!needSanitize || (resp.ContentLength > 0 && resp.ContentLength > limit)) {
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
		// 超限：已读部分 + 剩余流原样透传，放弃脱敏。
		w.WriteHeader(resp.StatusCode)
		n1, _ := w.Write(body)
		n2, _ := io.Copy(w, resp.Body)
		st.bytesOut = int64(n1) + n2
		st.outcome = "sanitize_skipped_too_large"
		if resp.StatusCode >= 400 {
			// 错误正文超过 MAX_SANITIZE_BYTES 属异常形态（通常是上游把
			// 整个请求回显了）。只上报已读到的前缀，且明确标注未脱敏，
			// 因为这条路径本身就是放弃脱敏的直通。
			g.recordUpstreamStatus(resp, body, st, span, false)
		}
		return resp.StatusCode
	}

	// needSanitize 必须重新判断：4xx/5xx 会绕过上面的直通分支走到这里，
	// 此时 rep 可能为 nil（脱敏关闭或请求无 model 字段），无条件调用
	// Sanitize 等于在关闭脱敏的部署里悄悄启用它。
	out := body
	if needSanitize && protocol.LooksLikeJSON(body) {
		if nb, changed := spec.Sanitize(body, st.publicModel, rep); changed {
			out = nb
			st.rewrites++
		}
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
		// 上报脱敏后的 out 而非原始 body：错误正文里同样可能出现上游真实
		// 模型名（"model xxx not found" 是最常见的一类错误），上报原文
		// 等于绕过脱敏把上游形态泄漏进看板。
		g.recordUpstreamStatus(resp, out, st, span, needSanitize)
	}
	return resp.StatusCode
}

// recordUpstreamStatus 把上游的错误响应（状态码 + 正文 + 关键头）落到 span。
//
// 上游报错时，状态码只说明「失败了」，正文才说明「为什么」。各家上游的
// 错误结构互不相同，网关不做字段提取、原样上报，由看板侧判断。
//
// 额外单独提取 request_id 与 retry-after：前者是找上游对账的唯一凭据，
// 后者决定客户端该等多久，两者都值得成为可直接筛选的属性。
// sanitized 为 false 表示正文未过脱敏（超限直通路径），此时正文可能含上游
// 真实模型名 —— 看板上必须能把这类记录筛出来，否则会误以为所有上报正文
// 都已脱敏。
func (g *Gateway) recordUpstreamStatus(resp *http.Response, body []byte, st *state, span trace.Span, sanitized bool) {
	if id := upstreamRequestID(resp.Header); id != "" {
		span.SetAttributes(attribute.String("gateway.upstream.request_id", id))
	}
	if !sanitized {
		span.SetAttributes(attribute.Bool("gateway.error.body_sanitized", false))
	}
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		span.SetAttributes(attribute.String("http.response.retry_after", ra))
	}
	obs.RecordUpstreamError(span, resp.StatusCode,
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
