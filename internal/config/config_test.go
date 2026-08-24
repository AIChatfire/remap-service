package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 所有可能影响断言的环境变量，测试前统一清空。
var allEnvKeys = []string{
	"ADDR", "UPSTREAM_BASE", "UPSTREAM_KEY", "UPSTREAM_BASE_FROM_HEADER",
	"UPSTREAM_TIMEOUT", "UPSTREAM_FIRST_BYTE_TIMEOUT",
	"UPSTREAM_BASE_OPENAI", "UPSTREAM_BASE_RESPONSES", "UPSTREAM_BASE_ANTHROPIC",
	"MODEL_MAP", "MODEL_MAP_STRICT",
	"SANITIZE_OFF", "SANITIZE_ALIASES", "SANITIZE_REPLACE",
	"SANITIZE_DROP_HEADERS", "SANITIZE_MAX_VALUE_LEN",
	"MAX_CONNS", "MAX_BODY_BYTES", "MAX_SANITIZE_BYTES",
	"OBS_ENABLED", "OBS_BACKEND", "OBS_SERVICE_NAME", "OBS_ENV", "OBS_SAMPLE_RATIO",
	"LOGFIRE_TOKEN", "LOGFIRE_REGION",
	"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS",
	"METRICS_ADDR", "LOG_LEVEL", "LOG_FORMAT", "LOG_UPSTREAM_MODEL",
}

func isolate(t *testing.T) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "")
	}
}

func writeEnv(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// --- .env 解析 ---

func TestParseLine(t *testing.T) {
	cases := []struct {
		in   string
		k, v string
		ok   bool
	}{
		{"KEY=value", "KEY", "value", true},
		{"  KEY = value  ", "KEY", "value", true},
		{"export KEY=value", "KEY", "value", true},
		{`KEY="quoted value"`, "KEY", "quoted value", true},
		{`KEY='single quoted'`, "KEY", "single quoted", true},
		{`KEY="with # hash"`, "KEY", "with # hash", true},
		{"KEY=bare # comment", "KEY", "bare", true},
		{`KEY="line\nbreak"`, "KEY", "line\nbreak", true},
		{"KEY=", "KEY", "", true},
		{"KEY=a=b=c", "KEY", "a=b=c", true},
		{"# comment", "", "", false},
		{"", "", "", false},
		{"   ", "", "", false},
		{"NOEQUALS", "", "", false},
	}
	for _, c := range cases {
		k, v, ok := parseLine(c.in)
		if ok != c.ok || k != c.k || v != c.v {
			t.Errorf("parseLine(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.in, k, v, ok, c.k, c.v, c.ok)
		}
	}
}

// 真实环境变量优先于 .env 文件，方便 docker -e 临时覆盖。
func TestDotEnvDoesNotOverrideRealEnv(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE", "https://from-real-env.com")
	p := writeEnv(t, "UPSTREAM_BASE=https://from-file.com\nADDR=:9999\n")

	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Upstream.Base != "https://from-real-env.com" {
		t.Errorf("base = %q，真实环境变量应优先", c.Upstream.Base)
	}
	if c.Addr != ":9999" {
		t.Errorf("addr = %q，未设置的项应从文件读取", c.Addr)
	}
}

func TestDotEnvMissingIsOK(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE", "https://x.com")
	if _, err := Load(filepath.Join(t.TempDir(), "nonexistent.env")); err != nil {
		t.Fatalf(".env 不存在应静默跳过: %v", err)
	}
}

// --- 默认值 ---

func TestDefaults(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE", "https://ark.cn-beijing.volces.com/api/v3")

	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" {
		t.Errorf("addr = %q", c.Addr)
	}
	if c.Upstream.Timeout != 120*time.Second || c.Upstream.FirstByteTimeout != 30*time.Second {
		t.Errorf("超时默认值错误: %v / %v", c.Upstream.Timeout, c.Upstream.FirstByteTimeout)
	}
	if !c.Upstream.AllowBaseHeader {
		t.Error("默认应允许 X-Upstream-Base 覆盖")
	}
	if c.Limits.MaxConns != 1024 || c.Limits.MaxBodyBytes != 64<<20 {
		t.Errorf("limits 默认值错误: %+v", c.Limits)
	}
	if c.Sanitize.MaxValueLen != 256 {
		t.Errorf("MaxValueLen = %d", c.Sanitize.MaxValueLen)
	}
	if !c.SanitizeEnabled() {
		t.Error("脱敏应默认开启")
	}
	if !c.Obs.LogUpstreamModel {
		t.Error("内部日志记录上游模型应默认开启")
	}
	if c.Obs.Backend != "none" {
		t.Errorf("无凭据时 backend 应为 none，实际 %q", c.Obs.Backend)
	}
}

// 网关不持有密钥：UPSTREAM_KEY 只是可选兜底。
func TestUpstreamKeyOptional(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE", "https://x.com")
	c, err := Load("")
	if err != nil {
		t.Fatalf("不配 UPSTREAM_KEY 应能正常启动: %v", err)
	}
	if c.Upstream.Key != "" {
		t.Errorf("Key = %q，应为空", c.Upstream.Key)
	}
}

// base 完全由请求头提供时，启动期没有默认上游是合法的。
func TestBaseCanComeFromHeaderOnly(t *testing.T) {
	isolate(t)
	if _, err := Load(""); err != nil {
		t.Fatalf("允许头覆盖时无 UPSTREAM_BASE 应可启动: %v", err)
	}
}

func TestBaseRequiredWhenHeaderDisabled(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE_FROM_HEADER", "false")
	_, err := Load("")
	if err == nil {
		t.Fatal("禁用头覆盖且无 UPSTREAM_BASE 时应报错")
	}
	if !strings.Contains(err.Error(), "UPSTREAM_BASE") {
		t.Errorf("错误信息 = %q", err.Error())
	}
}

// --- 值解析 ---

func TestEnvBytes(t *testing.T) {
	cases := map[string]int64{
		"64MB": 64 << 20, "64mb": 64 << 20, "64M": 64 << 20,
		"1GB": 1 << 30, "512KB": 512 << 10, "1024": 1024, "2048B": 2048,
		"": 999, "bogus": 999, "-5": 999,
	}
	for in, want := range cases {
		t.Setenv("TEST_BYTES", in)
		if got := envBytes("TEST_BYTES", 999); got != want {
			t.Errorf("envBytes(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestEnvDur(t *testing.T) {
	cases := map[string]time.Duration{
		"120s":  120 * time.Second,
		"2m":    2 * time.Minute,
		"1h30m": 90 * time.Minute,
		"120":   120 * time.Second, // 纯数字按秒
		"":      7 * time.Second,
		"junk":  7 * time.Second,
	}
	for in, want := range cases {
		t.Setenv("TEST_DUR", in)
		if got := envDur("TEST_DUR", 7*time.Second); got != want {
			t.Errorf("envDur(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestEnvBool(t *testing.T) {
	for _, v := range []string{"1", "true", "TRUE", "yes", "on", "y"} {
		t.Setenv("TEST_BOOL", v)
		if !envBool("TEST_BOOL", false) {
			t.Errorf("envBool(%q) 应为 true", v)
		}
	}
	for _, v := range []string{"0", "false", "no", "off", "n"} {
		t.Setenv("TEST_BOOL", v)
		if envBool("TEST_BOOL", true) {
			t.Errorf("envBool(%q) 应为 false", v)
		}
	}
	t.Setenv("TEST_BOOL", "")
	if !envBool("TEST_BOOL", true) {
		t.Error("空值应返回默认值")
	}
}

func TestEnvMapList(t *testing.T) {
	t.Setenv("TEST_ML", "pro=v3|r1; flash=lite ;bad;=x;empty=")
	got := envMapList("TEST_ML")
	want := map[string][]string{"pro": {"v3", "r1"}, "flash": {"lite"}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if strings.Join(got[k], ",") != strings.Join(v, ",") {
			t.Errorf("%s: got %v, want %v", k, got[k], v)
		}
	}
}

func TestEnvMap(t *testing.T) {
	t.Setenv("TEST_M", "a=b;c=d; e = f ;bad")
	got := envMap("TEST_M")
	want := map[string]string{"a": "b", "c": "d", "e": "f"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

func TestEnvList(t *testing.T) {
	t.Setenv("TEST_L", "a, b ,,c")
	got := envList("TEST_L")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("envList = %v", got)
	}
}

// --- 完整加载 ---

func TestFullConfigFromEnvFile(t *testing.T) {
	isolate(t)
	p := writeEnv(t, `
# 上游
UPSTREAM_BASE=https://ark.cn-beijing.volces.com/api/v3/
UPSTREAM_BASE_ANTHROPIC=https://api.anthropic.com
UPSTREAM_TIMEOUT=180s

# 映射
MODEL_MAP=pro=v3|v3|r1;flash=lite
MODEL_MAP_STRICT=true

# 脱敏
SANITIZE_ALIASES=v3=v3-250101|V3-Preview
SANITIZE_REPLACE=volces=internal
SANITIZE_DROP_HEADERS=X-Tt-Logid,X-Req-Id
SANITIZE_MAX_VALUE_LEN=128

MAX_CONNS=2048
MAX_BODY_BYTES=32MB
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}

	if c.Upstream.Base != "https://ark.cn-beijing.volces.com/api/v3" {
		t.Errorf("base 尾斜杠未去除: %q", c.Upstream.Base)
	}
	if c.BaseFor("anthropic") != "https://api.anthropic.com" {
		t.Errorf("anthropic 覆盖未生效: %q", c.BaseFor("anthropic"))
	}
	if c.BaseFor("openai") != c.Upstream.Base {
		t.Errorf("未覆盖的协议应回落默认 base: %q", c.BaseFor("openai"))
	}
	if c.Upstream.Timeout != 180*time.Second {
		t.Errorf("timeout = %v", c.Upstream.Timeout)
	}
	if !c.Mapping.Strict {
		t.Error("MODEL_MAP_STRICT 未生效")
	}
	if len(c.Mapping.Models["pro"]) != 3 {
		t.Errorf("加权映射解析错误: %v", c.Mapping.Models)
	}
	if len(c.Sanitize.Aliases["v3"]) != 2 {
		t.Errorf("别名解析错误: %v", c.Sanitize.Aliases)
	}
	if c.Sanitize.Replace["volces"] != "internal" {
		t.Errorf("替换对解析错误: %v", c.Sanitize.Replace)
	}
	if len(c.Sanitize.DropHeaders) != 2 {
		t.Errorf("drop headers = %v", c.Sanitize.DropHeaders)
	}
	if c.Sanitize.MaxValueLen != 128 {
		t.Errorf("MaxValueLen = %d", c.Sanitize.MaxValueLen)
	}
	if c.Limits.MaxConns != 2048 || c.Limits.MaxBodyBytes != 32<<20 {
		t.Errorf("limits = %+v", c.Limits)
	}
}

// 给了凭据却忘改 backend 是最常见失误，应自动推导。
func TestBackendInferred(t *testing.T) {
	t.Run("logfire", func(t *testing.T) {
		isolate(t)
		t.Setenv("UPSTREAM_BASE", "https://x.com")
		t.Setenv("OBS_ENABLED", "1")
		t.Setenv("LOGFIRE_TOKEN", "pylf_v1_xxx")
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if c.Obs.Backend != "logfire" {
			t.Errorf("backend = %q", c.Obs.Backend)
		}
	})

	t.Run("otlp", func(t *testing.T) {
		isolate(t)
		t.Setenv("UPSTREAM_BASE", "https://x.com")
		t.Setenv("OBS_ENABLED", "1")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if c.Obs.Backend != "otlp" {
			t.Errorf("backend = %q", c.Obs.Backend)
		}
	})

	t.Run("显式指定不被覆盖", func(t *testing.T) {
		isolate(t)
		t.Setenv("UPSTREAM_BASE", "https://x.com")
		t.Setenv("OBS_BACKEND", "otlp")
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
		t.Setenv("LOGFIRE_TOKEN", "pylf_xxx")
		c, err := Load("")
		if err != nil {
			t.Fatal(err)
		}
		if c.Obs.Backend != "otlp" {
			t.Errorf("backend = %q", c.Obs.Backend)
		}
	})
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"base 协议非法", map[string]string{"UPSTREAM_BASE": "ark.volces.com"}, "必须以 http"},
		{"协议覆盖非法", map[string]string{
			"UPSTREAM_BASE": "https://x.com", "UPSTREAM_BASE_ANTHROPIC": "ftp://y.com",
		}, "UPSTREAM_BASE_ANTHROPIC 必须以 http"},
		{"logfire 缺 token", map[string]string{
			"UPSTREAM_BASE": "https://x.com", "OBS_ENABLED": "1", "OBS_BACKEND": "logfire",
		}, "LOGFIRE_TOKEN"},
		{"otlp 缺 endpoint", map[string]string{
			"UPSTREAM_BASE": "https://x.com", "OBS_ENABLED": "1", "OBS_BACKEND": "otlp",
		}, "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{"未知 backend", map[string]string{
			"UPSTREAM_BASE": "https://x.com", "OBS_BACKEND": "datadog",
		}, "OBS_BACKEND 只能是"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isolate(t)
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			_, err := Load("")
			if err == nil {
				t.Fatalf("期望校验失败（%s）", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("错误信息 = %q，期望包含 %q", err.Error(), c.want)
			}
		})
	}
}

// 启动日志摘要不得含密钥。
func TestSummaryHasNoSecrets(t *testing.T) {
	isolate(t)
	t.Setenv("UPSTREAM_BASE", "https://x.com")
	t.Setenv("UPSTREAM_KEY", "sk-super-secret-value")
	t.Setenv("LOGFIRE_TOKEN", "pylf_secret")
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range c.Summary() {
		if s, ok := v.(string); ok {
			if strings.Contains(s, "sk-super-secret") || strings.Contains(s, "pylf_secret") {
				t.Errorf("启动摘要泄漏密钥: %v", c.Summary())
			}
		}
	}
	// 但应体现「是否配置了兜底 Key」
	found := false
	for i, v := range c.Summary() {
		if v == "upstream_key_fallback" && c.Summary()[i+1] == true {
			found = true
		}
	}
	if !found {
		t.Error("摘要应标明是否存在兜底 Key")
	}
}
