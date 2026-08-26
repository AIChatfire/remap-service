package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"syscall"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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
			span.SetStatus(codes.Error, err.Error())
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
	if !needSanitize || (resp.ContentLength > 0 && resp.ContentLength > limit) {
		if resp.ContentLength >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
		}
		w.WriteHeader(resp.StatusCode)
		n, err := io.Copy(w, resp.Body)
		st.bytesOut = n
		if err != nil {
			st.err = err
		}
		obs.Add(ctx, st.mx.BytesOut, n, st.metricAttrs(st.outcome, resp.StatusCode))
		return resp.StatusCode
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		st.err = err
		st.outcome = "read_upstream_failed"
		span.SetStatus(codes.Error, err.Error())
		return writeError(w, http.StatusBadGateway, "upstream_error", "failed to read upstream response")
	}
	if int64(len(body)) > limit {
		// 超限：已读部分 + 剩余流原样透传，放弃脱敏。
		w.WriteHeader(resp.StatusCode)
		n1, _ := w.Write(body)
		n2, _ := io.Copy(w, resp.Body)
		st.bytesOut = int64(n1) + n2
		st.outcome = "sanitize_skipped_too_large"
		return resp.StatusCode
	}

	out := body
	if protocol.LooksLikeJSON(body) {
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
		span.SetStatus(codes.Error, "upstream status "+strconv.Itoa(resp.StatusCode))
	}
	return resp.StatusCode
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
