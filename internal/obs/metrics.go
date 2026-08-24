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
