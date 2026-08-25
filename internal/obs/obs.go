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
	excluded    []string
	// noopTracer 预先建好：排除路径上不能每次现场构造，否则省下的出网
	// 开销又被分配吃掉。
	noopTracer trace.Tracer
	shutdowns  []func(context.Context) error
}

// Excluded 报告某请求路径是否应跳过 trace 与指标。
//
// 匹配为 ASCII 大小写无关的子串包含，与 sanitize 侧保持同一套语义：
// 配 "healthz" 能同时命中 /healthz、/v1/healthz、/HealthZ。
// 这里刻意不做前缀或精确匹配 —— 探针路径在不同编排系统下前缀各异，
// 子串匹配让一份配置在 k8s / 云 LB / 自建探活下都成立。
func (p *Provider) Excluded(path string) bool {
	if p == nil || len(p.excluded) == 0 || path == "" {
		return false
	}
	for _, kw := range p.excluded {
		if containsFold(path, kw) {
			return true
		}
	}
	return false
}

// containsFold 是 ASCII 大小写无关的子串包含判断。
//
// 不用 strings.Contains(strings.ToLower(s), kw)：那会在每个请求上多分配
// 一次字符串。路径通常很短、关键字通常只有几个，直接扫描更划算。
func containsFold(s, substr string) bool {
	if len(substr) == 0 || len(s) < len(substr) {
		return false
	}
	c0 := lowerASCII(substr[0])
	last := len(s) - len(substr)
outer:
	for i := 0; i <= last; i++ {
		if lowerASCII(s[i]) != c0 {
			continue
		}
		for j := 1; j < len(substr); j++ {
			if lowerASCII(s[i+j]) != lowerASCII(substr[j]) {
				continue outer
			}
		}
		return true
	}
	return false
}

func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// noopMetrics 的所有字段都是 nil 接口。obs.Add / Record / AddUpDown 均已
// nil-safe，因此把它交给热路径即可让全部上报退化成一次 nil 判断 —— 不必在
// 每个上报点散布 if，排除逻辑只存在于一处。
var noopMetrics = &Metrics{}

// MetricsFor 返回该路径应使用的指标集合。命中排除名单时返回空集合，
// 上报全部退化为 no-op，请求本身照常处理。
func (p *Provider) MetricsFor(path string) *Metrics {
	if p == nil {
		return noopMetrics
	}
	if p.Excluded(path) {
		return noopMetrics
	}
	return p.Metrics
}

// TracerFor 返回该路径应使用的 tracer。命中排除名单时返回 no-op tracer，
// 其产生的 span 不记录、不采样、不出网。
func (p *Provider) TracerFor(path string) trace.Tracer {
	if p == nil {
		return noop.NewTracerProvider().Tracer("")
	}
	if p.Excluded(path) {
		return p.noopTracer
	}
	return p.Tracer
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
		excluded:    c.ExcludedURLs,
		noopTracer:  noop.NewTracerProvider().Tracer(""),
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
	if err := p.initMetrics(ctx, res, endpoint, headers, c.MetricInterval); err != nil {
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
	interval time.Duration,
) error {
	// 兜住 0 与负值：PeriodicReader 拿到非正间隔会 panic 或退化成 SDK 默认，
	// 两种都不是「配了个奇怪的值」应有的表现。下限取 1s，避免把出网打成风暴。
	if interval < time.Second {
		interval = time.Second
	}

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
			sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(interval)),
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
