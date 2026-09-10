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
//   - 连接池规模由 MAX_CONNS 一个数字推导。
//
// 唯一的例外是 Transport 一组：连接寿命与 h2 健康检查没有可推导的
// 「正确值」，它们由上游与网络形态决定，且配错会直接表现为批量故障，
// 因此必须可配（见 Transport 的说明）。
//
// 最小可用部署只需一行：UPSTREAM_BASE=https://...
type Config struct {
	Addr string

	Upstream  Upstream
	Transport Transport
	Mapping   Mapping
	Sanitize  Sanitize
	Limits    Limits
	Lifecycle Lifecycle
	Obs       Obs
}

// Lifecycle 是进程**退出**阶段的时间预算，三段互不重叠、串行耗用。
//
// 为什么必须显式配而不是继续各写一个常量：退出路径上有三处独立的等待，
// 早期实现里它们分别硬编码在 server.Run（20s）与 main.run 的 defer（20s），
// 谁也不知道对方存在。两段一叠加就是 40s，而编排层的 stop_grace_period
// 只有 30s —— 于是 Docker 在第 30s 发 SIGKILL，**正在 flush 的那批
// trace/metrics 直接被掐掉**，而那恰好是「关停前最后一批数据」，
// 也就是排查重启问题时最想看的那批。
//
// 三段各自的用途：
//
//	DrainDelay      readyz 转 503 之后、真正关闭连接之前的等待。
//	                负载均衡器的健康检查间隔通常 5~10s，探测到 503 之前
//	                它仍会把新请求打到本实例上；这段等待买的就是这个窗口。
//	                取 0 表示不等待（直连客户端、无 LB 的场景）。
//	ShutdownTimeout 在途请求的最长等待（http.Server.Shutdown 的上限）。
//	                超过即被强制断开 —— 注意这个值同时是「部署时一条长流
//	                能活多久」的上限，它几乎必然小于 UPSTREAM_STREAM_TIMEOUT。
//	FlushTimeout    最后一次遥测 flush 的上限（obs.Provider.Shutdown）。
//
// 不变量（无法在代码里检查，因为 stop_grace_period 只有编排层知道）：
//
//	DrainDelay + ShutdownTimeout + FlushTimeout < stop_grace_period
//
// 三项之和进启动日志，运维可据此对齐 compose 里的 stop_grace_period。
type Lifecycle struct {
	DrainDelay      time.Duration
	ShutdownTimeout time.Duration
	FlushTimeout    time.Duration
}

// Effective 补齐零值，返回可直接使用的时间预算。
//
// 零值回落内置默认，与 Transport 一组保持同一取向：手工构造的 Config
// （测试、零配置入口）不会因为少写一个字段就退化成「退出时立刻硬断连接」。
// 唯一例外是 DrainDelay —— 它的 0 是「不等待」这一有效语义，原样保留。
func (l Lifecycle) Effective() Lifecycle {
	if l.ShutdownTimeout <= 0 {
		l.ShutdownTimeout = 20 * time.Second
	}
	if l.FlushTimeout <= 0 {
		l.FlushTimeout = 5 * time.Second
	}
	if l.DrainDelay < 0 {
		l.DrainDelay = 0
	}
	return l
}

// Total 返回退出阶段的总预算，供启动日志与编排侧对齐使用。
func (l Lifecycle) Total() time.Duration {
	e := l.Effective()
	return e.DrainDelay + e.ShutdownTimeout + e.FlushTimeout
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

	// Timeout 是**非流式**请求的总时长上限。
	Timeout time.Duration
	// StreamTimeout 是**流式**请求的总时长上限。
	//
	// 流式正常由客户端断开或上游 EOF 结束，这里只是一个兜底，防止连接泄漏。
	// 它必须是配置项：早期实现把 30 分钟硬编码在 Client.Do 里，导致无论
	// 怎么调 UPSTREAM_TIMEOUT 都压不动流式请求。
	StreamTimeout time.Duration
	// FirstByteTimeout 是**流式**请求等待响应头的时间上限。
	//
	// 绝大多数上游在首个 token 就绪前不发响应头，因此它实际约等于 TTFT。
	FirstByteTimeout time.Duration
	// FirstByteTimeoutNonStream 是**非流式**请求等待响应头的时间上限。
	//
	// 与流式那个的语义完全不同：非流式上游要先跑完整段生成才发响应头，
	// 所以它实质上是「整段生成时长」的上限。两者必须分开配 —— 共用一个值
	// 只能二选一地错：按 TTFT 的量级设会误杀 60s 以上的正常长生成，
	// 按生成时长的量级设则流式的异常永远检测不出来。
	FirstByteTimeoutNonStream time.Duration
	// StallTimeout 是流式响应**两个数据块之间**的最大间隔，0 表示关闭。
	//
	// 它覆盖的是首字节与总超时都管不到的形态：响应头已到、流已建立，
	// 但上游中途不再吐数据。没有它，这样的流会一直挂到 StreamTimeout
	// （默认 30 分钟），期间占着并发名额、连接与请求体内存。
	//
	// 默认关闭而非拍一个默认值：「多长算卡住」强依赖上游形态，部分上游
	// 在长时间推理期间确实一个字节都不发，设小了会误杀正常请求。
	StallTimeout time.Duration
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
	MaxInflight int
	// MaxBodyBytes 是入站请求体的内存上限，超出返回 400。
	MaxBodyBytes int64
	// MaxSanitizeBytes 是「需要脱敏的响应」能占用的内存上限。
	//
	// 超出即返回 400，**不做原文直通**：直通会让上游真实标识（响应里的
	// model / id、错误正文里的真实模型名）未经处理就落到客户端，而脱敏
	// 存在的全部理由就是挡这个。一次明确的失败可观测、可重现；
	// 一次静默的泄漏不是。
	//
	// 不需要脱敏的响应（脱敏关闭、或本次请求没有 model 字段）不受此限，
	// 对它们它只是防大响应打爆内存的护栏。
	MaxSanitizeBytes int64
}

// Transport 是传输层（连接池与 h2 健康检查）参数。
//
// 为什么单独成组，而不是继续由 MAX_CONNS 一个数字推导：早期版本把
// IdleConnTimeout、Dialer 超时、h2 的 PING 参数全部硬编码在
// upstream.newTransport 里，结果是「已知会影响故障形态的几个旋钮一个都调不了」
// —— 排查时能定位、却无法处置。这里把它们显式暴露出来。
//
// 零值语义：字段为 0 时回落到 upstream 包内的内置默认值（与暴露前的
// 行为一致），唯一例外是 H2IdleTimeout —— 它取 0 表示**关闭**该健康检查。
type Transport struct {
	// IdleConnTimeout 是连接池里**空闲**连接的存活时长。
	//
	// 必须短于上游 / LB / NAT 的空闲回收时间，否则池里会留下一条
	// 已被对端静默关闭、而本端尚不知情的连接；下一个请求取到它即失败。
	// 参考量级：nginx 默认 75s、云 CLB 常见 60s。
	IdleConnTimeout time.Duration

	// DialTimeout 是建立 TCP 连接（含到代理那一跳）的上限。
	//
	// 注意它与调用方预算可能落在同一量级：本项先触发时返回的是
	// i/o timeout，**不会**被包装成带上下文的超时错误，因而更容易
	// 被误读成「上游慢」。走代理或跨境链路时要留出余量。
	DialTimeout time.Duration

	// KeepAlive 是 TCP 层保活探测的初始间隔。
	KeepAlive time.Duration

	// TLSHandshakeTimeout 是 TLS 握手上限。
	// 经代理隧道时握手要穿过整条链路，实测出现过 25s 量级。
	TLSHandshakeTimeout time.Duration

	// MaxConnsPerHost 限制单个主机的**总**连接数（含活动与空闲），0 为不限制。
	// 它主要作用于 HTTP/1.1 通道；h2 下连接由多路复用的流并发上限决定，
	// 本项约束有限。
	MaxConnsPerHost int

	// WriteBufferSize / ReadBufferSize 是单连接的读写缓冲。
	// 调大可提升吞吐，但按「连接数 × 2」线性占用内存。
	WriteBufferSize int
	ReadBufferSize  int

	// H2IdleTimeout 是 h2 连接**无任何帧**达到该时长后发 PING 的间隔。
	//
	// ⚠️ 这项是「空闲断连」类故障的关键，而且方向极易配反：
	// h2 的 PING 是**连接级**动作 —— 无应答时关闭整条连接，
	// 该连接上所有复用中的流会一起失败（client connection lost）。
	// 而非流式请求在整段生成期间一个字节都不收，属于**合法静默**，
	// 其上限就是 UPSTREAM_FIRST_BYTE_TIMEOUT_NONSTREAM。
	//
	// 所以本项（连同 H2PingTimeout）必须排在应用层看门狗**之后**：
	//
	//	H2IdleTimeout + H2PingTimeout > UPSTREAM_FIRST_BYTE_TIMEOUT_NONSTREAM
	//
	// 未显式配置时按该式取默认值（见 normalizeTransport）。
	// 取 0 表示**关闭** PING 健康检查，完全交给应用层超时。
	H2IdleTimeout time.Duration

	// H2PingTimeout 是发出 PING 后等待应答的上限，超时即关闭连接。
	// 仅在 H2IdleTimeout > 0 时生效。
	H2PingTimeout time.Duration
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

	// ErrorBodyBytes 是上报到 span 的上游错误正文上限（字节）。
	//
	// 上游报错时真正有用的信息全在正文里（message / type / code /
	// request_id），只记状态码等于把「为什么失败」丢掉，看板上只能看到
	// 一个 502，排查还得回去翻客户端日志。这里把原始正文按原样带上。
	//
	// 有上限是因为错误正文并非总是紧凑 JSON：反代打回的 HTML 错误页、
	// 上游把整个请求回显进错误的情况都可能达到几百 KB，无节制上报会
	// 撑爆单个 span 并拖慢出网。0 表示不上报正文（只留状态码与分类）。
	ErrorBodyBytes int

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
			Timeout:          envDur("UPSTREAM_TIMEOUT", 600*time.Second),
			StreamTimeout:    envDur("UPSTREAM_STREAM_TIMEOUT", 30*time.Minute),
			FirstByteTimeout: envDur("UPSTREAM_FIRST_BYTE_TIMEOUT", 30*time.Second),
			// 非流式的响应头要等整段生成跑完，必须比流式那个宽松一个量级。
			FirstByteTimeoutNonStream: envDur("UPSTREAM_FIRST_BYTE_TIMEOUT_NONSTREAM", 300*time.Second),
			// 默认 0 = 关闭。envDur 对 0 与未设置同样返回默认值，
			// 而这里默认值就是 0，因此两种写法都落到「关闭」，符合预期。
			StallTimeout: envDur("UPSTREAM_STALL_TIMEOUT", 0),
			Protocols:    loadProtocolBases(),
		},
		Transport: Transport{
			// 这几个 0 表示「用内置默认」，与参数暴露前的行为一致。
			IdleConnTimeout:     envDur("UPSTREAM_IDLE_CONN_TIMEOUT", 0),
			DialTimeout:         envDur("UPSTREAM_DIAL_TIMEOUT", 0),
			KeepAlive:           envDur("UPSTREAM_TCP_KEEPALIVE", 0),
			TLSHandshakeTimeout: envDur("UPSTREAM_TLS_HANDSHAKE_TIMEOUT", 0),
			MaxConnsPerHost:     envIntAllowZero("UPSTREAM_MAX_CONNS_PER_HOST", 0),
			WriteBufferSize:     int(envBytes("UPSTREAM_WRITE_BUFFER", 0)),
			ReadBufferSize:      int(envBytes("UPSTREAM_READ_BUFFER", 0)),
			// 用 AllowZero：0 是「关闭 h2 PING 健康检查」的有效取值，
			// 不是未设置。未设置时的默认值由 normalizeTransport 推导。
			H2IdleTimeout: envDurAllowZero("UPSTREAM_H2_IDLE_TIMEOUT", 0),
			H2PingTimeout: envDurAllowZero("UPSTREAM_H2_PING_TIMEOUT", 0),
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
		Lifecycle: Lifecycle{
			// 用 AllowZero：0 是「不等待、立刻关停」的有效取值（无 LB 的场景）。
			DrainDelay: envDurAllowZero("SHUTDOWN_DRAIN_DELAY", 5*time.Second),
			// 这两个 0 无独立语义，用 envDur 让 0 与未设置同样回落默认值。
			ShutdownTimeout: envDur("SHUTDOWN_TIMEOUT", 20*time.Second),
			FlushTimeout:    envDur("SHUTDOWN_FLUSH_TIMEOUT", 5*time.Second),
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
			// 用 AllowZero：0 是「不上报正文」的有效取值，不是未设置。
			// 8KiB 足以容纳各家上游的错误 JSON，又不至于让偶发的 HTML
			// 错误页把 span 撑大。
			ErrorBodyBytes: envIntAllowZero("OBS_ERROR_BODY_BYTES", 8<<10),
			ExcludedURLs:   envList("EXCLUDED_URLS"),
			MetricInterval: envDur("OBS_METRIC_INTERVAL", 60*time.Second),
			// 用 AllowZero：0 是「只信 RemoteAddr」的有效取值，不是未设置。
			TrustedProxyHops: envIntAllowZero("TRUSTED_PROXY_HOPS", 0),
		},
	}

	c.normalizeTransport()
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

// h2DefaultPingTimeout 是 PING 应答的默认容忍时长。
//
// 取 60s 而非更小：PING 与应答同样要穿过代理 / 跨境链路，判得太急会把
// 「链路慢」误判成「连接已死」—— 而误判的代价是关掉整条连接，
// 连带其上所有复用中的流一起失败。
const h2DefaultPingTimeout = 60 * time.Second

// normalizeTransport 补全传输层的隐含默认值。
//
// 只推导 h2 健康检查这一对，理由是它们的「正确值」不能凭空拍：
// 取决于上游在一次生成里会静默多久，而配置里已经有一个精确刻画该上限的量
// —— UPSTREAM_FIRST_BYTE_TIMEOUT_NONSTREAM。默认值直接由它推导，
// 以便避免「改了非流式首字节预算却忘了同步 PING」这类静默错配：
// 那样 h2 会在应用层看门狗之前关掉连接，而报错形态是
// client connection lost，与「上游不可达」完全同形，排查方向会被整个带偏。
//
// 判据是「值非空」而非 LookupEnv，与 normalizeObs 口径一致：
// 显式写 0 表示关闭健康检查，必须被尊重。
func (c *Config) normalizeTransport() {
	if os.Getenv("UPSTREAM_H2_IDLE_TIMEOUT") == "" {
		c.Transport.H2IdleTimeout = c.Upstream.FirstByteTimeoutNonStream
	}
	if os.Getenv("UPSTREAM_H2_PING_TIMEOUT") == "" {
		c.Transport.H2PingTimeout = h2DefaultPingTimeout
	}
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
		//
		// 空值**不能**复用 orNone 的「由请求头提供」文案：那是 upstream_base 的
		// 语义（Base 为空时确实只能靠 X-Upstream-Base）。默认代理为空表示
		// 「没有默认出口代理，回落到 HTTP_PROXY 环境变量」，而此时
		// UPSTREAM_PROXY_FROM_HEADER 默认是 false —— 打成「由请求头提供」
		// 会让人以为客户端能决定出网路径，把排查方向整个带偏。
		"upstream_proxy", orNoProxy(c.Upstream.Proxy),
		"proxy_from_header", c.Upstream.AllowProxyHeader,
		// 超时值全部进启动日志：这几个键此前只能靠「行为不像预期」反推，
		// 而改错了键名不会有任何报错。
		"timeout", c.Upstream.Timeout.String(),
		"stream_timeout", c.Upstream.StreamTimeout.String(),
		"first_byte_timeout", c.Upstream.FirstByteTimeout.String(),
		"first_byte_timeout_nonstream", c.Upstream.FirstByteTimeoutNonStream.String(),
		"stall_timeout", c.Upstream.StallTimeout.String(),
		// 传输层参数同样全部进启动日志，理由同上：键名写错不会有任何报错，
		// 只会静默沿用内置默认，而表现是「改了半天没效果」。
		// h2_idle_timeout 尤其要看一眼 —— 它小于非流式首字节预算时
		// 会以 client connection lost 的形式误杀正常的长生成。
		"idle_conn_timeout", c.Transport.IdleConnTimeout.String(),
		"dial_timeout", c.Transport.DialTimeout.String(),
		"tls_handshake_timeout", c.Transport.TLSHandshakeTimeout.String(),
		"max_conns_per_host", c.Transport.MaxConnsPerHost,
		"h2_idle_timeout", c.Transport.H2IdleTimeout.String(),
		"h2_ping_timeout", c.Transport.H2PingTimeout.String(),
		"max_inflight", c.Limits.MaxInflight,
		"max_body_bytes", c.Limits.MaxBodyBytes,
		// 退出预算三段全部进启动日志，并给出总和：这个和必须小于编排层的
		// stop_grace_period（compose 里 45s），否则最后一段遥测 flush 会被
		// SIGKILL 掐掉 —— 而它是三段里唯一「不做就永久丢数据」的一段。
		// 不检查是因为 stop_grace_period 只存在于编排层，进程看不到。
		"shutdown_drain_delay", c.Lifecycle.Effective().DrainDelay.String(),
		"shutdown_timeout", c.Lifecycle.Effective().ShutdownTimeout.String(),
		"shutdown_flush_timeout", c.Lifecycle.Effective().FlushTimeout.String(),
		"shutdown_budget_total", c.Lifecycle.Total().String(),
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

// orNoProxy 渲染默认出网代理。
//
// 非空时只保留 scheme://host —— 代理 URL 的 userinfo 段常带密码，原文进日志
// 等于把凭据写进日志系统。空值时给的是**回落目标**而不是「由请求头提供」：
// 后者是 upstream_base 的语义，默认代理为空时 UPSTREAM_PROXY_FROM_HEADER
// 也是 false，打成「由请求头提供」会让人以为客户端能决定出网路径。
func orNoProxy(raw string) string {
	if raw == "" {
		return "(未配置，回落到 HTTP_PROXY 环境变量)"
	}
	return proxyurl.Redact(raw)
}
