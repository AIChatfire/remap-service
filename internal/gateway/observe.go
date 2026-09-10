package gateway

import (
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/upstream"
)

// recordUpstreamTiming 把传输层的分段耗时落到 span 上。
//
// 与 gateway.ttfb_ms 的分工：那个从「上游请求发起」算到「响应头处理完」，
// 只回答「总共等了多久」；这里的每个字段由传输层在对应事件上直接打点，
// 能回答「时间具体花在 DNS、建连、TLS、请求写出，还是等响应」。
//
// 其中 gateway.upstream.wrote_request_ms 是判定超时归属的关键：它接近
// 超时上限说明请求压根没发出去（本地排队），只有几毫秒说明时间全花在
// 等上游。没有这个字段，「上游超时」在架构问题与上游问题之间无法区分。
//
// 只写发生过的阶段。缺失本身是信息 —— 复用连接时不会有 DNS/建连/TLS，
// 补 0 会让人误读成「瞬间完成」，与「没发生」同形。
func recordUpstreamTiming(span trace.Span, t upstream.Timing) {
	if span == nil || !span.IsRecording() {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 7)
	// 判据是「该阶段发生过」而非「毫秒数非零」：属性存在但值为 0 表示
	// 耗时不足 1ms，属性缺失才表示没发生。把两者都跳过会让本地/内网上游
	// 的阶段全部消失。
	appendMS := func(k string, d time.Duration) {
		if d > 0 {
			attrs = append(attrs, attribute.Int64(k, d.Milliseconds()))
		}
	}
	appendMS(obs.AttrUpstreamDNS, t.DNS)
	appendMS(obs.AttrUpstreamConnect, t.Connect)
	appendMS(obs.AttrUpstreamTLS, t.TLS)
	appendMS(obs.AttrUpstreamWroteRequest, t.WroteRequest)
	appendMS(obs.AttrUpstreamFirstByte, t.FirstByte)
	if t.Proto != "" {
		attrs = append(attrs, attribute.String(obs.AttrUpstreamProto, t.Proto))
	}
	// Reused 只在确实建立过连接时才写：DNS 或建连就失败时写 false
	// 会被读成「没有复用连接」，而事实是「根本没连上」。
	if t.GotConn {
		attrs = append(attrs, attribute.Bool(obs.AttrUpstreamConnReused, t.Reused))
	}
	span.SetAttributes(attrs...)
}

// setTailAttrs 在请求收尾时统一补齐「结果与水位」类属性。
//
// 集中在一个收尾点，而不是在各条 return 之前各写一遍：这些字段此前只写在
// 成功路径上，于是失败请求的 span 里既没有最终状态码也没有体积，只能从
// 错误类型反推客户端究竟收到了什么。收尾点只有一个，漏写就是整类请求都漏。
func (g *Gateway) setTailAttrs(span trace.Span, st *state, status int) {
	if span == nil || !span.IsRecording() {
		return
	}
	// 此处仍在闸门持有期内（release 由 ServeHTTP 的 defer 执行），
	// 因此在途数包含本次请求 —— 正是「本请求处理期间的水位」这一语义。
	cur, limit := g.InFlight()

	attrs := make([]attribute.KeyValue, 0, 8)
	attrs = append(attrs,
		attribute.Int("http.response.status_code", status),
		attribute.Int64(obs.AttrRequestBytes, st.bytesIn),
		attribute.Int64(obs.AttrResponseBytes, st.bytesOut),
		attribute.Int(obs.AttrInflight, cur),
		attribute.Int(obs.AttrInflightLimit, limit),
		// 恒写失败尝试次数：缺失时「没失败过」与「这个属性没实现」
		// 在看板上完全同形，读的人只能猜。
		attribute.Int(obs.AttrAttemptCount, st.attempts),
	)
	if st.credFP != "" {
		attrs = append(attrs, attribute.String(obs.AttrCredentialFingerprint, st.credFP))
	}
	if st.requestID != "" {
		attrs = append(attrs, attribute.String(obs.AttrRequestID, st.requestID))
	}
	span.SetAttributes(attrs...)
}
