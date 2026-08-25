// Package obs 提供可一键开关的可观测性设施：结构化日志、分布式 trace、指标。
//
// 后端只有一个：Pydantic Logfire，走 OTLP/HTTP + 裸 token，trace 与 metrics
// 都推给它。网关不自带 Prometheus 端点 —— 单一出口意味着「配了没生效」只有
// 一条排查路径：token 对不对、进程有没有把 Enabled 打开。
//
// observability.enabled=false 时全部退化为 no-op：
// trace 使用 OTel 空实现，指标写入丢弃型 provider，热路径无额外分配。
package obs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/betterme/remap-service/internal/config"
)

// Provider 聚合本进程的可观测性组件。
type Provider struct {
	Logger  *slog.Logger
	Tracer  trace.Tracer
	Metrics *Metrics

	enabled     bool
	logUpstream bool
	shutdowns   []func(context.Context) error
}

// Enabled 报告可观测性总开关状态。
func (p *Provider) Enabled() bool { return p != nil && p.enabled }

// LogUpstreamModel 报告是否允许在日志中记录真实上游模型。
func (p *Provider) LogUpstreamModel() bool { return p != nil && p.logUpstream }

// New 依据配置初始化可观测性。返回的 Provider 永远非 nil。
func New(ctx context.Context, c config.Obs) (*Provider, error) {
	p := &Provider{
		Logger:      newLogger(c.LogLevel, c.LogFormat),
		Tracer:      noop.NewTracerProvider().Tracer("remap-gateway"),
		Metrics:     newNoopMetrics(),
		enabled:     c.Enabled,
		logUpstream: c.LogUpstreamModel,
	}

	if !c.Enabled {
		p.Logger.Info("可观测性已关闭",
			slog.String("hint", "配置 observability.enabled=true 或设置 OBS_ENABLED=1 开启"))
		return p, nil
	}

	// 用 NewSchemaless 而非 NewWithAttributes：后者会附带 semconv 包的 SchemaURL，
	// 与 resource.Default() 自带的 SchemaURL（随 SDK 版本浮动）冲突，Merge 会直接报错。
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(orStr(c.ServiceName, "remap-gateway")),
		semconv.ServiceVersion(orStr(c.Version, "dev")),
		attribute.String("deployment.environment.name", orStr(c.Env, "default")),
	))
	if err != nil {
		return p, fmt.Errorf("build otel resource: %w", err)
	}

	// Enabled 为真时 token 必定非空（config.Validate 已拦），这里只做兜底。
	if c.LogfireToken == "" {
		return p, fmt.Errorf("LOGFIRE_TOKEN 为空，无法启用可观测性")
	}
	endpoint := logfireEndpoint(c.LogfireRegion)
	headers := map[string]string{
		"Authorization": c.LogfireToken, // Logfire 要求裸 token，不带 Bearer 前缀
	}

	if err := p.initTraces(ctx, res, endpoint, headers, c.SampleRatio); err != nil {
		return p, err
	}
	if err := p.initMetrics(ctx, res, endpoint, headers); err != nil {
		return p, err
	}

	p.Logger.Info("可观测性已启用",
		slog.String("backend", "logfire"),
		slog.String("region", c.LogfireRegion),
		slog.String("endpoint", endpoint),
	)
	return p, nil
}

func (p *Provider) initTraces(
	ctx context.Context, res *resource.Resource,
	endpoint string, headers map[string]string, ratio float64,
) error {
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"),
		otlptracehttp.WithHeaders(headers),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("otlp trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxQueueSize(8192),
			sdktrace.WithMaxExportBatchSize(1024),
			sdktrace.WithBatchTimeout(2*time.Second),
		),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	p.Tracer = tp.Tracer("remap-gateway")
	p.shutdowns = append(p.shutdowns, tp.Shutdown)
	return nil
}

func (p *Provider) initMetrics(
	ctx context.Context, res *resource.Resource,
	endpoint string, headers map[string]string,
) error {
	exp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"),
		otlpmetrichttp.WithHeaders(headers),
		otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression),
		otlpmetrichttp.WithTimeout(10*time.Second),
	)
	if err != nil {
		return fmt.Errorf("otlp metric exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(15*time.Second)),
		),
	)
	otel.SetMeterProvider(mp)
	m, err := newMetrics(mp.Meter("remap-gateway"))
	if err != nil {
		return fmt.Errorf("init metrics: %w", err)
	}
	p.Metrics = m
	p.shutdowns = append(p.shutdowns, mp.Shutdown)
	return nil
}

// logfireEndpoint 按区域推导 Logfire 的 OTLP 基地址。
// region 已由 config 归一化并校验为 us|eu，这里对未知值保守回落到 us。
func logfireEndpoint(region string) string {
	if strings.EqualFold(region, "eu") {
		return "https://logfire-eu.pydantic.dev"
	}
	return "https://logfire-us.pydantic.dev"
}

// Shutdown 优雅关闭所有导出器。
func (p *Provider) Shutdown(ctx context.Context) {
	if p == nil {
		return
	}
	for i := len(p.shutdowns) - 1; i >= 0; i-- {
		if err := p.shutdowns[i](ctx); err != nil {
			p.Logger.Warn("可观测性组件关闭失败", slog.String("err", err.Error()))
		}
	}
}

func newLogger(level, format string) *slog.Logger {
	var lv slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lv = slog.LevelDebug
	case "warn", "warning":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lv}
	if strings.EqualFold(format, "text") {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}

func orStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
