package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/betterme/remap-service/internal/protocol"
	"github.com/betterme/remap-service/internal/proxyurl"
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
	// Proxy 是默认出网代理，支持 http / https / socks5 / socks5h，须为完整 URL。
	// 留空时回落到 HTTP_PROXY / HTTPS_PROXY / NO_PROXY 环境变量。
	Proxy string
	// AllowProxyHeader 控制是否接受 X-Upstream-Proxy 覆盖本次请求的代理。
	// 默认关闭：开启后客户端可指定任意出网地址，属于把网关当跳板的能力。
	AllowProxyHeader bool
	// NoProxy 是不走代理的主机列表，语义同标准 NO_PROXY。
	//
	// 显式配 Proxy 时走 http.ProxyURL，它没有 ProxyFromEnvironment
	// 对 localhost 的豁免，因此默认值必须带上本机地址 ——
	// 否则本地 mock 上游联调会被打进代理。
	NoProxy string

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
	// Capabilities 是「能力 -> 上游模型」的静态声明，
	// 在 X-Model-Capability 头缺失时生效。
	// 键为 vision / audio / video / tools / file（含中文与常见别名）。
	// 上游因某能力报错时切到对应模型重试；file 能力另有前置路由
	// —— 请求体含 file_id 时直接改走，不等报错。
	Capabilities map[string][]string
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
//
// 后端只有 Pydantic Logfire 一个：trace 与 metrics 都走它的 OTLP/HTTP 端点。
// 网关不再自带 Prometheus 端点，也不再支持通用 OTLP —— 少一个后端选项，
// 就少一整套「配了却没生效」的排查路径。
type Obs struct {
	Enabled     bool
	ServiceName string
	Env         string
	Version     string
	SampleRatio float64

	LogfireToken  string
	LogfireRegion string

	LogLevel         string
	LogFormat        string
	LogUpstreamModel bool

	// ExcludedURLs 是不产生 trace 与指标的路径关键字（ASCII 大小写无关的子串匹配）。
	//
	// 健康检查、就绪探针、心跳这类轮询请求的频率往往比真实业务高一个数量级，
	// 却没有任何诊断价值：它们会稀释 P99、抬高上报量、污染 trace 列表。
	// 命中的请求仍然正常代理，只是不记录可观测数据。
	ExcludedURLs []string

	// TrustedProxyHops 是网关前方可信反向代理的层数，决定客户 IP 从
	// X-Forwarded-For 的哪一位取值。
	//
	// 默认 0 表示只信任 RemoteAddr、完全忽略 XFF —— 该头由客户端可写，
	// 在没有可信代理剥离的前提下采纳它等于允许任意伪造来源 IP。
	// 网关直接暴露公网时保持 0；前置 1 层 Nginx 或 CLB 时设 1，依此类推。
	TrustedProxyHops int

	// MetricInterval 是指标上报周期。
	//
	// 上报本身在独立 goroutine，不占请求路径；调大只减少出网次数，
	// 代价是看板的数据新鲜度。
	MetricInterval time.Duration
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
			Proxy:            strings.TrimSpace(envStr("UPSTREAM_PROXY", "")),
			AllowProxyHeader: envBool("UPSTREAM_PROXY_FROM_HEADER", false),
			NoProxy:          strings.TrimSpace(envStr("UPSTREAM_NO_PROXY", "localhost,127.0.0.1,::1")),
			Timeout:          envDur("UPSTREAM_TIMEOUT", 120*time.Second),
			FirstByteTimeout: envDur("UPSTREAM_FIRST_BYTE_TIMEOUT", 30*time.Second),
			Protocols:        loadProtocolBases(),
		},
		Mapping: Mapping{
			Strict:          envBool("MODEL_MAP_STRICT", false),
			Models:          envMapList("MODEL_MAP"),
			Fallback:        envPipeList("MODEL_MAP_FALLBACK"),
			FailoverOnError: envBool("MODEL_MAP_FAILOVER", false),
			Capabilities:    envMapList("MODEL_CAPABILITY"),
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
			LogfireToken:     envStr("LOGFIRE_TOKEN", ""),
			LogfireRegion:    strings.ToLower(envStr("LOGFIRE_REGION", "us")),
			LogLevel:         envStr("LOG_LEVEL", "info"),
			LogFormat:        envStr("LOG_FORMAT", "json"),
			LogUpstreamModel: envBool("LOG_UPSTREAM_MODEL", true),
			ExcludedURLs:     envList("EXCLUDED_URLS"),
			MetricInterval:   envDur("OBS_METRIC_INTERVAL", 60*time.Second),
			// 用 AllowZero：0 是「只信 RemoteAddr」的有效取值，不是未设置。
			TrustedProxyHops: envIntAllowZero("TRUSTED_PROXY_HOPS", 0),
		},
	}

	c.normalizeObs()
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

// normalizeObs 补全可观测性的隐含默认值。
//
// 只给 LOGFIRE_TOKEN、忘了 OBS_ENABLED=true 是最常见的失误，直接视为要开。
// 但显式写了 OBS_ENABLED=false 必须被尊重——临时静默上报不该要求先删掉 token。
// 判据是「值非空」而非 LookupEnv：空值等同未配置，与 envBool 的口径保持一致，
// 否则 .env 里一行 `OBS_ENABLED=` 就会静悄悄压掉自动启用。
// 反向不成立：Enabled=true 但没 token 是硬错误，交给 Validate 拦。
func (c *Config) normalizeObs() {
	if os.Getenv("OBS_ENABLED") == "" && c.Obs.LogfireToken != "" {
		c.Obs.Enabled = true
	}
	if c.Obs.LogfireRegion == "" {
		c.Obs.LogfireRegion = "us"
	}
	// 间隔过小会让空周期报错更频繁、出网次数陡增，且对看板没有实际收益。
	// 这里取 5s 作为下限而非报错：它是性能取舍，不是配置错误。
	if c.Obs.MetricInterval < 5*time.Second {
		c.Obs.MetricInterval = 5 * time.Second
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
	// 代理配错的表现是「所有请求超时且无从判断原因」，启动期拦掉比线上查便宜。
	if c.Upstream.Proxy != "" {
		norm, err := proxyurl.Validate(c.Upstream.Proxy)
		if err != nil {
			errs = append(errs, fmt.Errorf("UPSTREAM_PROXY: %w", err))
		} else {
			c.Upstream.Proxy = norm
		}
	}
	for name, base := range c.Upstream.Protocols {
		if err := checkBase("UPSTREAM_BASE_"+strings.ToUpper(name), base); err != nil {
			errs = append(errs, err)
		}
	}

	if c.Obs.Enabled {
		if c.Obs.LogfireToken == "" {
			errs = append(errs, errors.New("OBS_ENABLED=true 时必须提供 LOGFIRE_TOKEN"))
		}
		if c.Obs.LogfireRegion != "us" && c.Obs.LogfireRegion != "eu" {
			errs = append(errs, fmt.Errorf("LOGFIRE_REGION 只能是 us|eu，当前 %q", c.Obs.LogfireRegion))
		}
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
		// 仅 scheme://host：代理 URL 的 userinfo 段常带密码。
		"upstream_proxy", orNone(proxyurl.Redact(c.Upstream.Proxy)),
		"proxy_from_header", c.Upstream.AllowProxyHeader,
		"sanitize", c.SanitizeEnabled(),
		"strict_mapping", c.Mapping.Strict,
		"obs", c.Obs.Enabled,
		"obs_excluded_urls", c.Obs.ExcludedURLs,
	}
}

func orNone(s string) string {
	if s == "" {
		return "(由请求头提供)"
	}
	return s
}
