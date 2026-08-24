package obs

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

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

func TestResolveBackend(t *testing.T) {
	t.Run("logfire", func(t *testing.T) {
		ep, h, insecure, err := resolveBackend(config.Obs{
			Backend: "logfire", LogfireRegion: "eu", LogfireToken: "pylf_v1_xxx",
		})
		if err != nil {
			t.Fatal(err)
		}
		if ep != "https://logfire-eu.pydantic.dev" {
			t.Errorf("endpoint = %q", ep)
		}
		// Logfire 要求裸 token，不带 Bearer 前缀
		if h["Authorization"] != "pylf_v1_xxx" {
			t.Errorf("Authorization = %q，Logfire 要求裸 token", h["Authorization"])
		}
		if insecure {
			t.Error("Logfire 走 HTTPS，不应为 insecure")
		}
	})

	t.Run("logfire 缺 token", func(t *testing.T) {
		if _, _, _, err := resolveBackend(config.Obs{Backend: "logfire"}); err == nil {
			t.Fatal("缺 token 应报错")
		}
	})

	t.Run("otlp http 自动 insecure", func(t *testing.T) {
		_, _, insecure, err := resolveBackend(config.Obs{
			Backend: "otlp", OTLPEndpoint: "http://localhost:4318",
		})
		if err != nil {
			t.Fatal(err)
		}
		if !insecure {
			t.Error("http:// 端点应自动判定为 insecure")
		}
	})

	t.Run("otlp https 不 insecure", func(t *testing.T) {
		_, _, insecure, err := resolveBackend(config.Obs{
			Backend: "otlp", OTLPEndpoint: "https://otlp.example.com",
		})
		if err != nil {
			t.Fatal(err)
		}
		if insecure {
			t.Error("https:// 端点不应为 insecure")
		}
	})

	t.Run("otlp 自定义头", func(t *testing.T) {
		_, h, _, err := resolveBackend(config.Obs{
			Backend:      "otlp",
			OTLPEndpoint: "https://otlp.example.com",
			OTLPHeaders:  map[string]string{"X-Token": "abc"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if h["X-Token"] != "abc" {
			t.Errorf("自定义头未透传: %v", h)
		}
	})

	t.Run("none", func(t *testing.T) {
		ep, _, _, err := resolveBackend(config.Obs{Backend: "none"})
		if err != nil || ep != "" {
			t.Errorf("none 应返回空 endpoint，got %q %v", ep, err)
		}
	})

	t.Run("未知 backend", func(t *testing.T) {
		if _, _, _, err := resolveBackend(config.Obs{Backend: "datadog"}); err == nil {
			t.Fatal("未知 backend 应报错")
		}
	})
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
	p.StartPrometheus(":0") // registry 为 nil，应静默跳过
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

// 指标标签必须保持低基数，且绝不能包含上游真实模型名。
func TestAttrsLowCardinality(t *testing.T) {
	reg := prometheus.NewRegistry()
	pe, err := promexp.New(promexp.WithRegisterer(reg),
		promexp.WithoutScopeInfo(), promexp.WithoutTargetInfo())
	if err != nil {
		t.Fatal(err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(pe))
	defer mp.Shutdown(context.Background())

	m, err := newMetrics(mp.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	Add(context.Background(), m.Requests, 1, Attrs("/v1/chat/completions", "deepseek-pro", "ok", 200))

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var labels []string
	for _, mf := range mfs {
		if !strings.HasPrefix(mf.GetName(), "gateway_requests") {
			continue
		}
		for _, mm := range mf.GetMetric() {
			for _, l := range mm.GetLabel() {
				labels = append(labels, l.GetName()+"="+l.GetValue())
			}
		}
	}
	if len(labels) == 0 {
		t.Fatal("未采集到 gateway_requests 指标")
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
