package gateway

import (
	"context"
	"io"
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

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
		st.outcome = "stream_broken"
		span.SetStatus(codes.Error, err.Error())
		obs.Add(ctx, st.mx.UpstreamErr, 1, a)
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
