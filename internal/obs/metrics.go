package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// Metrics 是网关的核心指标集合。
//
// 标签维度刻意保持低基数：只用对外模型名、路由前缀、状态码类别，
// 绝不使用上游真实模型名（避免泄漏）或请求 ID（避免基数爆炸）。
type Metrics struct {
	Requests    metric.Int64Counter
	Duration    metric.Float64Histogram
	TTFB        metric.Float64Histogram
	InFlight    metric.Int64UpDownCounter
	UpstreamErr metric.Int64Counter
	SSEEvents   metric.Int64Counter
	BytesIn     metric.Int64Counter
	BytesOut    metric.Int64Counter
	Rewrites    metric.Int64Counter
	// Rejected 是被并发闸门拒绝的请求数。这个指标持续非零
	// 说明容量不足或上游变慢，应当扩容或调高 MAX_INFLIGHT。
	Rejected metric.Int64Counter
	// Failover 是切换到兜底模型的次数。它上升说明首选上游在报错，
	// 客户端侧可能感知不到，只能靠这个指标发现。
	Failover metric.Int64Counter
	// H2ConnErrors 是 h2 传输层的**连接级**内部事件计数，标签是 kind。
	//
	// 与 UpstreamErr 的差别是粒度：那个数「请求」，这个数「连接」。
	// 事件名由 x/net/http2 封闭定义（conn_close_lost_ping、recv_goaway_*、
	// 读帧错误等），种类有限，可安全当标签用。
	//
	// 为什么要单独一个指标：一次 conn_close_lost_ping 会让该连接上
	// **所有**在途流一起失败，于是它成了请求级错误的放大器 ——
	// 两者一起看才能分清「挂了一个请求」与「挂了一条连接、连带几十个请求」。
	H2ConnErrors metric.Int64Counter
	// ProxyTransportsEvicted 是 per-request 代理池因超出上限而淘汰 transport
	// 的次数。
	//
	// 持续非零说明两件事之一：上限过小，或客户端在大量使用不同代理。
	// 两者都需要运维介入，且都不是任何请求级指标能反映的 —— 淘汰发生在池上，
	// 是「某个代理被挤出容量」，与单条请求的结果无关。
	ProxyTransportsEvicted metric.Int64Counter
	// SanitizeSuppressed 是「上游标识确实出现在响应里、但一次都没被替换」的
	// 计数 —— 唯一的静默漏点。
	//
	// 唯一会走到这里的规则是长度阈值（SANITIZE_MAX_VALUE_LEN）：超过阈值的
	// 值被视为模型生成的内容而整体放行。上游的长错误 message（配额超限、
	// 多行校验错误、回显了整个请求的 400）正落在这个区间里，而它们恰恰是
	// 最需要脱敏的一类文本（含真实上游模型名）。
	//
	// 稳态下应恒为 0 —— model 字段是整体覆盖的、必然发生替换。一旦非零，
	// 就说明有标识**只**出现在长字段里，需要人工判断「是漏了还是本来就该放过」。
	// 这是把「静默漏点」变成「可测量的漏点」，不放松任何现有保护。
	//
	// 刻意不统计「生成内容字段被跳过」：那是设计意图（绝不篡改模型输出），
	// 把它计进来会让这条信号永远非零、从而失去意义。
	SanitizeSuppressed metric.Int64Counter
	// Panics 是 recoverMW 拦下的 panic 数。
	//
	// panic 此前只有一行日志，「这个月发生过几次」无从回答；而它每次都会
	// 让一个请求以一个结构可能已损坏的响应收场。计数为 0 与「没有实现」
	// 才是真的同形 —— 那正是要避免的。
	Panics metric.Int64Counter
}

func newMetrics(m metric.Meter) (*Metrics, error) {
	var err error
	pick := func(e error) {
		if err == nil && e != nil {
			err = e
		}
	}
	out := &Metrics{}

	var e error
	out.Requests, e = m.Int64Counter("gateway.requests",
		metric.WithDescription("网关处理的请求总数"))
	pick(e)
	out.Duration, e = m.Float64Histogram("gateway.request.duration",
		metric.WithDescription("端到端请求耗时"), metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 25, 50, 100, 200, 400, 800, 1500, 3000, 6000, 15000, 60000))
	pick(e)
	out.TTFB, e = m.Float64Histogram("gateway.upstream.ttfb",
		metric.WithDescription("上游首字节耗时"), metric.WithUnit("ms"),
		metric.WithExplicitBucketBoundaries(5, 10, 25, 50, 100, 200, 400, 800, 1500, 3000, 10000, 30000))
	pick(e)
	out.InFlight, e = m.Int64UpDownCounter("gateway.requests.inflight",
		metric.WithDescription("并发处理中的请求数"))
	pick(e)
	out.UpstreamErr, e = m.Int64Counter("gateway.upstream.errors",
		metric.WithDescription("上游连接或传输错误数"))
	pick(e)
	out.SSEEvents, e = m.Int64Counter("gateway.sse.events",
		metric.WithDescription("转发的 SSE 事件数"))
	pick(e)
	out.BytesIn, e = m.Int64Counter("gateway.bytes.in",
		metric.WithDescription("请求体字节数"), metric.WithUnit("By"))
	pick(e)
	out.BytesOut, e = m.Int64Counter("gateway.bytes.out",
		metric.WithDescription("响应体字节数"), metric.WithUnit("By"))
	pick(e)
	out.Rewrites, e = m.Int64Counter("gateway.sanitize.rewrites",
		metric.WithDescription("发生实际脱敏替换的片段数"))
	pick(e)
	out.Rejected, e = m.Int64Counter("gateway.requests.rejected",
		metric.WithDescription("因并发闸门满而被拒绝的请求数"))
	pick(e)
	out.Failover, e = m.Int64Counter("gateway.model.failover",
		metric.WithDescription("首选上游失败后切换到兜底模型的次数"))
	pick(e)
	out.H2ConnErrors, e = m.Int64Counter("gateway.upstream.h2_conn_errors",
		metric.WithDescription("h2 传输层连接级事件数（PING 探活失败、GOAWAY、读帧错误等）"))
	pick(e)
	out.ProxyTransportsEvicted, e = m.Int64Counter("gateway.upstream.proxy_transports_evicted",
		metric.WithDescription("per-request 代理池超出上限而淘汰 transport 的次数"))
	pick(e)
	out.SanitizeSuppressed, e = m.Int64Counter("gateway.sanitize.suppressed",
		metric.WithDescription("响应里出现上游标识但零替换的次数（长度阈值放行的静默漏点）"))
	pick(e)
	out.Panics, e = m.Int64Counter("gateway.panics",
		metric.WithDescription("请求处理中 panic 被 recover 的次数"))
	pick(e)

	if err != nil {
		return nil, err
	}
	return out, nil
}

func newNoopMetrics() *Metrics {
	m := metricnoop.NewMeterProvider().Meter("noop")
	out, _ := newMetrics(m)
	return out
}

// Attrs 构建低基数标签集。
func Attrs(route, model, outcome string, status int) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.String("route", route),
		attribute.String("model", model),
		attribute.String("outcome", outcome),
		attribute.Int("status", status),
	)
}

// Add 是对 counter 的 nil-safe 封装。
func Add(ctx context.Context, c metric.Int64Counter, v int64, opts ...metric.AddOption) {
	if c == nil {
		return
	}
	c.Add(ctx, v, opts...)
}

// Record 是对 histogram 的 nil-safe 封装。
func Record(ctx context.Context, h metric.Float64Histogram, v float64, opts ...metric.RecordOption) {
	if h == nil {
		return
	}
	h.Record(ctx, v, opts...)
}

// AddUpDown 是对 up-down counter 的 nil-safe 封装。
func AddUpDown(ctx context.Context, c metric.Int64UpDownCounter, v int64, opts ...metric.AddOption) {
	if c == nil {
		return
	}
	c.Add(ctx, v, opts...)
}
