package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/betterme/remap-service/internal/protocol"
)

// Config 是网关的完整配置，全部来自环境变量。
//
// 设计取向：能推导的都不配。
//   - 上游协议由请求路径自动识别（见 internal/protocol）；
//   - 认证头与前缀由协议决定；
//   - 上游 Key 直接透传客户端凭据，网关不持有任何密钥；
//   - 连接池由 MAX_CONNS 一个数字推导。
//
// 最小可用部署只需一行：UPSTREAM_BASE=https://...
type Config struct {
	Addr string

	Upstream Upstream
	Mapping  Mapping
	Sanitize Sanitize
	Limits   Limits
	Obs      Obs
}

// Upstream 描述上游访问方式。
type Upstream struct {
	// Base 是默认上游地址，可被请求头 X-Upstream-Base 覆盖。
	Base string
	// Protocols 按协议名覆盖上游地址，键为 openai / responses / anthropic。
	Protocols map[string]string
	// Key 是可选的上游凭据。留空时完全透传客户端携带的凭据；
	// 设置后作为客户端未提供凭据时的兜底。
	Key string
	// AllowBaseHeader 控制是否接受 X-Upstream-Base 覆盖上游地址。
	AllowBaseHeader bool

	Timeout          time.Duration
	FirstByteTimeout time.Duration
}

// Mapping 控制模型映射。
type Mapping struct {
	// Strict 为 true 时，请求模型未命中任何规则（含兜底）则直接 400。
	Strict bool
	// Models 是静态映射，在 X-Model-Map 头缺失时生效。
	// 键可含 `*` 通配（claude-* / *-flash / gpt-*-turbo / *）。
	// 同一键多个值时按条数等权随机（["v3","v3","r1"] 即 2:1）。
	Models map[string][]string
	// Fallback 是精确与通配都未命中时使用的上游模型。
	// 多个值等权随机，可用于把未知模型导向一个通用能力模型。
	Fallback []string
	// FailoverOnError 为 true 时，首选上游返回 5xx/429 或连接失败，
	// 会用 Fallback 模型重试一次。仅在配置了 Fallback 时有意义。
	FailoverOnError bool
}

// Sanitize 控制响应脱敏。
type Sanitize struct {
	// Off 为 true 时完全关闭脱敏（调试用）。
	Off bool
	// Aliases 是上游模型 -> 变体名列表，覆盖版本化名称。
	Aliases map[string][]string
	// Replace 是无条件替换对（上游标识 -> 对外标识）。
	Replace map[string]string
	// DropHeaders 中的响应头会被删除。
	DropHeaders []string
	// MaxValueLen 是允许做子串替换的字符串长度上限，超过则视为生成内容不动。
	MaxValueLen int
}

// Limits 是容量相关的少量旋钮。
type Limits struct {
	MaxConns int
	// MaxInflight 是同时在途的请求数上限，超出即返回 503。
	// 0 表示不限制（仅在网关前已有可靠的并发控制时才建议这样配）。
	MaxInflight      int
	MaxBodyBytes     int64
	MaxSanitizeBytes int64
}

// Obs 控制可观测性，由 Enabled 一键开关。
type Obs struct {
	Enabled     bool
	ServiceName string
	Env         string
	Version     string
	SampleRatio float64

	Backend       string // logfire | otlp | none
	LogfireToken  string
	LogfireRegion string
	OTLPEndpoint  string
	OTLPHeaders   map[string]string

	MetricsAddr string

	LogLevel         string
	LogFormat        string
	LogUpstreamModel bool
}

// Load 从环境变量构建配置。envFile 为空时默认尝试 ./.env。
func Load(envFile string) (*Config, error) {
	if err := LoadDotEnv(envFile); err != nil {
		return nil, err
	}

	c := &Config{
		Addr: envStr("ADDR", ":8080"),
		Upstream: Upstream{
			Base:             strings.TrimRight(envStr("UPSTREAM_BASE", ""), "/"),
			Key:              envStr("UPSTREAM_KEY", ""),
			AllowBaseHeader:  envBool("UPSTREAM_BASE_FROM_HEADER", true),
			Timeout:          envDur("UPSTREAM_TIMEOUT", 120*time.Second),
			FirstByteTimeout: envDur("UPSTREAM_FIRST_BYTE_TIMEOUT", 30*time.Second),
			Protocols:        loadProtocolBases(),
		},
		Mapping: Mapping{
			Strict:          envBool("MODEL_MAP_STRICT", false),
			Models:          envMapList("MODEL_MAP"),
			Fallback:        envPipeList("MODEL_MAP_FALLBACK"),
			FailoverOnError: envBool("MODEL_MAP_FAILOVER", false),
		},
		Sanitize: Sanitize{
			Off:         envBool("SANITIZE_OFF", false),
			Aliases:     envMapList("SANITIZE_ALIASES"),
			Replace:     envMap("SANITIZE_REPLACE"),
			DropHeaders: envList("SANITIZE_DROP_HEADERS"),
			MaxValueLen: envInt("SANITIZE_MAX_VALUE_LEN", 256),
		},
		Limits: Limits{
			MaxConns: envInt("MAX_CONNS", 1024),
			// 默认取 MAX_CONNS 的 4 倍：LLM 请求大部分时间在等上游，
			// 在途数高于连接数是正常的，但需要一个明确的天花板。
			MaxInflight:      envIntAllowZero("MAX_INFLIGHT", 4096),
			MaxBodyBytes:     envBytes("MAX_BODY_BYTES", 64<<20),
			MaxSanitizeBytes: envBytes("MAX_SANITIZE_BYTES", 32<<20),
		},
		Obs: Obs{
			Enabled:          envBool("OBS_ENABLED", false),
			ServiceName:      envStr("OBS_SERVICE_NAME", "remap-gateway"),
			Env:              envStr("OBS_ENV", "default"),
			SampleRatio:      envFloat("OBS_SAMPLE_RATIO", 1.0),
			Backend:          strings.ToLower(envStr("OBS_BACKEND", "")),
			LogfireToken:     envStr("LOGFIRE_TOKEN", ""),
			LogfireRegion:    envStr("LOGFIRE_REGION", "us"),
			OTLPEndpoint:     strings.TrimRight(envStr("OTEL_EXPORTER_OTLP_ENDPOINT", ""), "/"),
			OTLPHeaders:      envMap("OTEL_EXPORTER_OTLP_HEADERS"),
			MetricsAddr:      envStr("METRICS_ADDR", ":9090"),
			LogLevel:         envStr("LOG_LEVEL", "info"),
			LogFormat:        envStr("LOG_FORMAT", "json"),
			LogUpstreamModel: envBool("LOG_UPSTREAM_MODEL", true),
		},
	}

	c.inferBackend()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// loadProtocolBases 读取 UPSTREAM_BASE_<PROTO> 形式的按协议覆盖。
func loadProtocolBases() map[string]string {
	out := make(map[string]string, 3)
	for _, s := range protocol.All() {
		key := "UPSTREAM_BASE_" + strings.ToUpper(s.Name)
		if v := strings.TrimRight(envStr(key, ""), "/"); v != "" {
			out[s.Name] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// inferBackend 补全可观测性后端：给了凭据却忘了指定 backend 是最常见的失误。
func (c *Config) inferBackend() {
	if c.Obs.Backend == "" {
		switch {
		case c.Obs.LogfireToken != "":
			c.Obs.Backend = "logfire"
		case c.Obs.OTLPEndpoint != "":
			c.Obs.Backend = "otlp"
		default:
			c.Obs.Backend = "none"
		}
	}
	if c.Obs.LogfireRegion == "" {
		c.Obs.LogfireRegion = "us"
	}
}

// Validate 在启动阶段拦截明显的错误配置。
func (c *Config) Validate() error {
	var errs []error

	// 允许 base 完全由请求头提供，此时启动期没有默认上游是合法的。
	if c.Upstream.Base == "" && !c.Upstream.AllowBaseHeader && len(c.Upstream.Protocols) == 0 {
		errs = append(errs, errNoUpstream)
	}
	if c.Upstream.Base != "" {
		if err := checkBase("UPSTREAM_BASE", c.Upstream.Base); err != nil {
			errs = append(errs, err)
		}
	}
	for name, base := range c.Upstream.Protocols {
		if err := checkBase("UPSTREAM_BASE_"+strings.ToUpper(name), base); err != nil {
			errs = append(errs, err)
		}
	}

	switch c.Obs.Backend {
	case "logfire":
		if c.Obs.Enabled && c.Obs.LogfireToken == "" {
			errs = append(errs, errors.New("OBS_BACKEND=logfire 时必须提供 LOGFIRE_TOKEN"))
		}
	case "otlp":
		if c.Obs.Enabled && c.Obs.OTLPEndpoint == "" {
			errs = append(errs, errors.New("OBS_BACKEND=otlp 时必须提供 OTEL_EXPORTER_OTLP_ENDPOINT"))
		}
	case "none":
	default:
		errs = append(errs, fmt.Errorf("OBS_BACKEND 只能是 logfire|otlp|none，当前 %q", c.Obs.Backend))
	}

	return errors.Join(errs...)
}

func checkBase(name, v string) error {
	if !strings.HasPrefix(v, "http://") && !strings.HasPrefix(v, "https://") {
		return fmt.Errorf("%s 必须以 http(s):// 开头，当前 %q", name, v)
	}
	return nil
}

// SanitizeEnabled 报告是否启用响应脱敏。
func (c *Config) SanitizeEnabled() bool { return !c.Sanitize.Off }

// BaseFor 返回该协议应使用的默认上游地址（不含请求头覆盖）。
func (c *Config) BaseFor(proto string) string {
	if b, ok := c.Upstream.Protocols[proto]; ok && b != "" {
		return b
	}
	return c.Upstream.Base
}

// Summary 返回用于启动日志的配置摘要，不含任何密钥。
func (c *Config) Summary() []any {
	protos := make([]string, 0, len(c.Upstream.Protocols))
	for k := range c.Upstream.Protocols {
		protos = append(protos, k)
	}
	return []any{
		"addr", c.Addr,
		"upstream_base", orNone(c.Upstream.Base),
		"upstream_overrides", protos,
		"base_from_header", c.Upstream.AllowBaseHeader,
		"upstream_key_fallback", c.Upstream.Key != "",
		"sanitize", c.SanitizeEnabled(),
		"strict_mapping", c.Mapping.Strict,
		"obs", c.Obs.Enabled,
	}
}

func orNone(s string) string {
	if s == "" {
		return "(由请求头提供)"
	}
	return s
}
