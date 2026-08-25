package obs

import (
	"context"
	"strings"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/betterme/remap-service/internal/config"
)

func TestLogfireEndpoint(t *testing.T) {
	cases := map[string]string{
		"us": "https://logfire-us.pydantic.dev",
		"US": "https://logfire-us.pydantic.dev",
		"eu": "https://logfire-eu.pydantic.dev",
		"":   "https://logfire-us.pydantic.dev",
	}
	for region, want := range cases {
		if got := logfireEndpoint(region); got != want {
			t.Errorf("logfireEndpoint(%q) = %q, want %q", region, got, want)
		}
	}
}

// Enabled=true 但 token 为空时必须报错，而不是静默降级成「什么都不上报」。
func TestEnabledRequiresToken(t *testing.T) {
	_, err := New(context.Background(), config.Obs{
		Enabled: true, LogLevel: "error", LogfireRegion: "us",
	})
	if err == nil {
		t.Fatal("缺 LOGFIRE_TOKEN 应报错")
	}
	if !strings.Contains(err.Error(), "LOGFIRE_TOKEN") {
		t.Errorf("错误信息应点明缺少的配置项，实际: %v", err)
	}
}

// 关闭可观测性时必须完全退化为 no-op，且所有调用都安全。
func TestDisabledIsNoop(t *testing.T) {
	p, err := New(context.Background(),
		config.Obs{Enabled: false, LogLevel: "error", LogFormat: "text", LogUpstreamModel: true})
	if err != nil {
		t.Fatal(err)
	}
	if p.Enabled() {
		t.Error("Enabled() 应为 false")
	}
	if p.Tracer == nil || p.Metrics == nil || p.Logger == nil {
		t.Fatal("关闭时组件也必须非 nil，避免调用方判空")
	}

	// 全链路调用一遍，确认不会 panic
	ctx, span := p.Tracer.Start(context.Background(), "test")
	span.End()
	Add(ctx, p.Metrics.Requests, 1, Attrs("/v1/chat", "m", "ok", 200))
	Record(ctx, p.Metrics.Duration, 1.5, Attrs("/v1/chat", "m", "ok", 200))
	AddUpDown(ctx, p.Metrics.InFlight, 1)
	p.Shutdown(context.Background())
}

func TestNewLoggerFormats(t *testing.T) {
	for _, f := range []string{"json", "text", ""} {
		for _, lv := range []string{"debug", "info", "warn", "error", "bogus"} {
			if lg := newLogger(lv, f); lg == nil {
				t.Errorf("format=%q level=%q 返回 nil", f, lv)
			}
		}
	}
}

func TestLogUpstreamModelFlag(t *testing.T) {
	on, _ := New(context.Background(), config.Obs{LogLevel: "error", LogUpstreamModel: true})
	if !on.LogUpstreamModel() {
		t.Error("LOG_UPSTREAM_MODEL=true 时应允许内部日志记录真实模型")
	}
	off, _ := New(context.Background(), config.Obs{LogLevel: "error", LogUpstreamModel: false})
	if off.LogUpstreamModel() {
		t.Error("LOG_UPSTREAM_MODEL=false 时应禁止")
	}
}

func TestMetricsNoopUsable(t *testing.T) {
	m := newNoopMetrics()
	if m == nil || m.Requests == nil {
		t.Fatal("noop metrics 必须可用")
	}
}

// 指标标签必须保持低基数：恰好 4 个维度，且 model 只放客户端请求的名字。
func TestAttrsLowCardinality(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer mp.Shutdown(context.Background())

	m, err := newMetrics(mp.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	Add(context.Background(), m.Requests, 1, Attrs("/v1/chat/completions", "deepseek-pro", "ok", 200))

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}

	var labels []string
	for _, sm := range rm.ScopeMetrics {
		for _, mt := range sm.Metrics {
			if !strings.HasPrefix(mt.Name, "gateway.requests") {
				continue
			}
			sum, ok := mt.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("gateway.requests 类型 = %T，期望 Sum[int64]", mt.Data)
			}
			for _, dp := range sum.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					labels = append(labels, string(kv.Key)+"="+kv.Value.String())
				}
			}
		}
	}
	if len(labels) == 0 {
		t.Fatal("未采集到 gateway.requests 指标")
	}
	joined := strings.Join(labels, ",")
	for _, want := range []string{
		"route=/v1/chat/completions", "model=deepseek-pro", "outcome=ok", "status=200",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("缺少标签 %q，实际: %s", want, joined)
		}
	}
	if len(labels) != 4 {
		t.Errorf("标签数 = %d，期望恰好 4 个低基数维度: %s", len(labels), joined)
	}
}

func TestExcludedMatchesSubstringFoldCase(t *testing.T) {
	p := &Provider{excluded: []string{"health", "metrics", "poll"}}

	cases := map[string]bool{
		"/health":              true,
		"/healthz":             true,
		"/HEALTHZ":             true, // 大小写无关
		"/v1/inner/Metrics":    true, // 子串命中即可
		"/api/poll?wait=30":    true,
		"/v1/chat/completions": false,
		"/v1/models":           false,
		"":                     false, // 空路径不视为命中
	}
	for path, want := range cases {
		if got := p.Excluded(path); got != want {
			t.Errorf("Excluded(%q) = %v，期望 %v", path, got, want)
		}
	}
}

func TestExcludedPathGetsNoopExits(t *testing.T) {
	p := &Provider{excluded: []string{"health"}, Metrics: newNoopMetrics()}

	if p.MetricsFor("/healthz") != noopMetrics {
		t.Error("排除路径应拿到 noopMetrics，否则指标仍会带上排除路径的维度")
	}
	if p.MetricsFor("/v1/chat/completions") == noopMetrics {
		t.Error("正常路径不应被降级成 noopMetrics")
	}
	// nil Provider 必须也安全：未启用可观测性时热路径走同一条分支。
	var nilP *Provider
	if nilP.MetricsFor("/anything") != noopMetrics {
		t.Error("nil Provider 应返回 noopMetrics")
	}
	if nilP.Excluded("/health") {
		t.Error("nil Provider 的 Excluded 应为 false，不得 panic")
	}
}

func TestExcludedEmptyListDisablesFiltering(t *testing.T) {
	p := &Provider{Metrics: newNoopMetrics()}
	if p.Excluded("/healthz") {
		t.Error("未配置 EXCLUDED_URLS 时不应排除任何路径")
	}
}
