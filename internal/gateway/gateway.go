// Package gateway 实现网关核心：请求改写、透传转发、响应脱敏。
//
// 三条贯穿始终的原则：
//
//	协议自动识别 —— 由请求路径判定（见 internal/protocol），认证头、
//	                必需头、响应字段路径随协议切换，配置里没有协议开关。
//	凭据纯透传   —— 网关不持有密钥。客户端的 Key 被提取后按目标协议
//	                重新注入，所以 OpenAI 风格的客户端也能调 Anthropic 端点。
//	脱敏保守     —— 只改模型字段与短值，绝不碰模型生成的内容。
package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/betterme/remap-service/internal/capability"
	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/mapping"
	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/protocol"
	"github.com/betterme/remap-service/internal/sanitize"
	"github.com/betterme/remap-service/internal/upstream"
)

// Gateway 是无状态的请求处理器，可被任意 goroutine 并发调用。
type Gateway struct {
	cfg      *config.Config
	client   *upstream.Client
	rules    *sanitize.Rules
	mapCache *mapping.Cache
	static   *mapping.Table
	// capCache 解析 X-Model-Capability 头，capStatic 是其静态兜底。
	capCache  *capability.Cache
	capStatic *capability.Map
	gate      *gate
	o         *obs.Provider
}

// New 构建网关。
func New(cfg *config.Config, client *upstream.Client, o *obs.Provider) *Gateway {
	return &Gateway{
		cfg:    cfg,
		client: client,
		rules: sanitize.NewRules(
			cfg.Sanitize.Aliases,
			cfg.Sanitize.Replace,
			cfg.Sanitize.DropHeaders,
			cfg.Sanitize.MaxValueLen,
		),
		mapCache:  mapping.NewCache(1024),
		static:    mapping.FromStatic(cfg.Mapping.Models, cfg.Mapping.Fallback),
		capCache:  capability.NewCache(256),
		capStatic: capability.FromStatic(cfg.Mapping.Capabilities),
		gate:      newGate(cfg.Limits.MaxInflight),
		o:         o,
	}
}

// InFlight 返回当前在途请求数与容量上限，供健康检查暴露。
//
// 对 nil 接收者安全：健康检查必须在任何情况下都能响应，
// 不能因为网关尚未装配就让 /readyz 变成 500。
func (g *Gateway) InFlight() (cur, limit int) {
	if g == nil {
		return 0, 0
	}
	return g.gate.inFlight(), g.gate.limit()
}

// hopHeaders 是不应转发的逐跳首部。
var hopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
	"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
}

type state struct {
	route         string
	proto         string
	start         time.Time
	publicModel   string
	upstreamModel string
	// metricModel 是 publicModel 的低基数形式，只用于指标标签。
	// 未在配置或映射表中声明的模型名一律归一为 modelOther。
	metricModel string
	// matchKind 记录模型映射命中的级别（精确 / 通配 / 兜底 / 未命中）。
	matchKind mapping.MatchKind
	// caps 是本次请求体用到的能力集合（仅统计已声明专用模型的那些）。
	caps capability.Set
	// capUsed 非 None 时，表示本次已切到该能力的专用模型。
	capUsed capability.Kind
	// proxy 是本次请求实际使用的出网代理（已脱敏，仅 scheme://host）。
	// 空串表示走默认出口。只进内部日志与 trace 属性，不进指标标签。
	proxy string
	// failedOver 标记本次请求是否发生了故障切换。
	failedOver bool
	stream     bool
	outcome    string
	ttfb       time.Duration
	bytesIn    int64
	bytesOut   int64
	sseEvents  int64
	rewrites   int64
	err        error
	// mx 是本次请求实际使用的指标集合。命中 EXCLUDED_URLS 时它是空集合，
	// 所有上报退化为 no-op —— 排除判断只在 ServeHTTP 入口做一次。
	mx *obs.Metrics
}

// modelOther 是未声明模型在指标里的归一值。
//
// model 标签的取值必须来自「运维声明过的有限集合」，否则客户端只要每次
// 填一个新的 model 名，就能让指标的时间序列数无限增长
// （实测 60 个随机名产生 60 条序列）。声明之外的一律归到这里。
const modelOther = "other"

// overloadRetryAfter 是过载时建议客户端退避的秒数。
// 取值考虑 LLM 请求的典型时长：太短会让客户端立刻回来加剧拥塞，
// 太长则在瞬时尖峰过去后浪费容量。
const overloadRetryAfter = 2

// metricAttrs 构建本次请求的指标标签，model 维度已做基数收敛。
func (st *state) metricAttrs(outcome string, status int) metric.MeasurementOption {
	return obs.Attrs(st.route, st.metricModel, outcome, status)
}

// ServeHTTP 处理一次代理请求。
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	route := protocol.RouteLabel(r.URL.Path)

	// 排除路径（健康检查、探针、心跳）在此一次性降级为 no-op tracer 与空指标
	// 集合，后续所有上报点无需再判断。请求本身照常代理。
	mx := g.o.MetricsFor(r.URL.Path)

	ctx, span := g.o.TracerFor(r.URL.Path).Start(r.Context(), "gateway "+route,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("url.path", r.URL.Path),
			// 客户 IP 只作 span 属性，不作指标标签：IP 基数无上界，
			// 一旦进 metric label 就会按客户数量线性放大时间序列。
			attribute.String("client.address", clientIP(r, g.cfg.Obs.TrustedProxyHops)),
		))
	defer span.End()

	// metricModel 默认取归一值：无 model 字段的端点（/v1/models 等）
	// 与未声明的模型名共用同一条时间序列。
	st := &state{route: route, start: start, metricModel: modelOther, mx: mx}

	// 闸门必须在读取请求体之前获取：body 一旦读入就占用了
	// 最多 MAX_BODY_BYTES 的内存，此时再拒绝已经失去保护意义。
	release, ok := g.gate.tryAcquire()
	if !ok {
		st.outcome = "overloaded"
		status := writeOverloaded(w, overloadRetryAfter)
		// 过载是容量事件而非请求错误，但看板上必须能看出「这条 503 是
		// 网关自己拒的，不是上游返回的」，并带上当时的在途水位。
		cur, limit := g.InFlight()
		span.SetAttributes(
			attribute.Int("gateway.inflight", cur),
			attribute.Int("gateway.inflight_limit", limit),
		)
		obs.RecordGatewayError(span, st.outcome,
			"gateway at capacity; retry after "+strconv.Itoa(overloadRetryAfter)+"s", nil)
		obs.Add(ctx, mx.Rejected, 1, st.metricAttrs(st.outcome, status))
		g.log(st, status, r)
		return
	}
	defer release()

	obs.AddUpDown(ctx, mx.InFlight, 1)
	defer obs.AddUpDown(ctx, mx.InFlight, -1)

	status := g.handle(ctx, w, r, st, span)

	a := st.metricAttrs(st.outcome, status)
	obs.Add(ctx, mx.Requests, 1, a)
	obs.Record(ctx, mx.Duration, msSince(start), a)
	obs.Add(ctx, mx.BytesIn, st.bytesIn, a)

	g.log(st, status, r)
}

func (g *Gateway) handle(ctx context.Context, w http.ResponseWriter, r *http.Request, st *state, span trace.Span) int {
	// ---------- 1. 识别协议与上游地址 ----------
	spec := protocol.Detect(r.URL.Path)
	st.proto = spec.Name
	span.SetAttributes(attribute.String("gateway.protocol", spec.Name))

	base, ok := g.resolveBase(r, spec.Name)
	if !ok {
		st.outcome = "no_upstream"
		msg := "upstream base not configured; set UPSTREAM_BASE or send a valid " + BaseHeader + " header"
		obs.RecordGatewayError(span, st.outcome, msg, nil)
		return writeError(w, http.StatusBadGateway, "gateway_error", msg)
	}

	proxy, ok := g.resolveProxy(r)
	if !ok {
		st.outcome = "bad_proxy"
		msg := "invalid " + ProxyHeader + " header; expected a full URL with scheme http, https, socks5 or socks5h"
		obs.RecordGatewayError(span, st.outcome, msg, nil)
		return writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
	}
	if proxy != "" {
		// 注入 ctx 而非改 Transport：buildRequest 与 client.Do 都派生自
		// 这个 ctx，故障切换重试同样继承，无需在各处重复传递。
		ctx = upstream.WithProxy(ctx, proxy)
		// 只留 scheme://host：代理 URL 的 userinfo 段常带密码，
		// 原文进日志等于把凭据写进日志系统。
		st.proxy = upstream.RedactProxy(proxy)
	}

	// ---------- 2. 读取并改写请求体 ----------
	body, err := g.readBody(r)
	if err != nil {
		st.outcome, st.err = "bad_request", err
		// 读请求体失败的原因（超限 / 客户端中途断开）只在 err 里，
		// 不上报的话看板上就是一条没有说明的 400。
		obs.RecordGatewayError(span, st.outcome, err.Error(), err)
		return writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
	}
	st.bytesIn = int64(len(body))

	outBody := body
	if len(body) > 0 && protocol.LooksLikeJSON(body) {
		if code := g.rewrite(w, r, st, &outBody, span); code != 0 {
			return code
		}
	}

	span.SetAttributes(
		attribute.String("gen_ai.request.model", st.publicModel),
		attribute.Bool("gen_ai.request.stream", st.stream),
	)
	if g.o.LogUpstreamModel() {
		span.SetAttributes(attribute.String("gateway.upstream.model", st.upstreamModel))
	}

	// ---------- 3. 构造并发起上游请求 ----------
	ureq, err := g.buildRequest(ctx, r, spec, base, outBody)
	if err != nil {
		st.outcome, st.err = "build_request_failed", err
		// 对客户端只说"构造失败"，但看板必须拿到真正的原因
		// （多为 base 拼接后 URL 非法），否则无法判断是配置问题。
		obs.RecordGatewayError(span, st.outcome, "failed to build upstream request", err)
		return writeError(w, http.StatusInternalServerError, "gateway_error", "failed to build upstream request")
	}

	plan := g.planFailover(r, st)

	sendAt := time.Now()
	resp, cancel, err := g.client.Do(ctx, ureq, st.stream)

	// 连接层失败：还没有任何字节写给客户端，可以安全地换模型重试。
	if err != nil && plan.enabled {
		if r2, c2, ok := g.retryWithFallback(ctx, r, spec, base, outBody, plan, st, nil); ok {
			// 切换成功，整个请求最终是成功的，因此不标红 span；但首次
			// 失败的原因要留痕 —— 否则「兜底一直在生效」这件事本身
			// 完全不可见，上游某个模型静默挂掉也无人发现。
			obs.RecordAttemptFailure(span, "transport", st.upstreamModel, 0, err)
			resp, cancel, err = r2, c2, nil
			st.applyPlan(plan)
		}
	}
	if err != nil {
		st.outcome, st.err = "upstream_error", err
		obs.Add(ctx, st.mx.UpstreamErr, 1, st.metricAttrs("transport", 0))
		code, msg := classifyUpstreamError(ctx, err)
		// 传输层失败没有响应正文，err 本身就是全部线索。RecordGatewayError
		// 会落下完整错误串、Go 错误类型、unwrap 到底的根因，并生成
		// exception 事件 —— 网关对客户端只说 "upstream connection failed"，
		// 真正的 DNS/TLS/超时原因只能从这里看到。
		//
		// kind 用 st.outcome 而非另写一个字面量：指标侧的 "transport" 是
		// 刻意区分传输层失败的维度，但 span 的 kind 必须与 outcome 同名，
		// 否则按 outcome 筛 span 会整类漏掉。
		obs.RecordGatewayError(span, st.outcome, msg, err)
		return writeError(w, code, "upstream_error", msg)
	}

	// 状态码层失败：响应头已到但正文一个字节都没下发，同样可以重试。
	// 必须先读正文（用于上报）再关响应体归还连接。
	if !st.failedOver && plan.shouldRetry(resp.StatusCode) {
		if r2, c2, ok := g.retryWithFallback(ctx, r, spec, base, outBody, plan, st, resp); ok {
			resp.Body.Close()
			cancel()
			resp, cancel = r2, c2
			st.applyPlan(plan)
			obs.Add(ctx, st.mx.Failover, 1, st.metricAttrs("failover", resp.StatusCode))
		}
	}
	defer cancel()
	defer resp.Body.Close()

	st.ttfb = time.Since(sendAt)
	obs.Record(ctx, st.mx.TTFB, float64(st.ttfb.Microseconds())/1000.0,
		st.metricAttrs("ok", resp.StatusCode))
	span.SetAttributes(
		attribute.Int("http.response.status_code", resp.StatusCode),
		attribute.Int64("gateway.ttfb_ms", st.ttfb.Milliseconds()),
	)

	// ---------- 4. 响应脱敏并回写 ----------
	rep := g.replacerFor(st)
	copyResponseHeaders(w.Header(), resp.Header, g.rules.DropHeaders())

	// 上游以 SSE 头返回错误状态时不能走流式转发：错误正文是普通 JSON
	// 而非事件流，且它是排查的唯一线索。交给 pipeBuffered 读入并上报。
	if isSSE(resp) && resp.StatusCode < 400 {
		st.outcome = "stream"
		return g.pipeStream(ctx, w, resp, spec, rep, st, span)
	}
	st.outcome = outcomeForStatus(resp.StatusCode)
	return g.pipeBuffered(ctx, w, resp, spec, rep, st, span)
}

// rewrite 解析请求模型并改写为上游真实模型。返回非 0 表示已写出错误响应。
func (g *Gateway) rewrite(w http.ResponseWriter, r *http.Request, st *state, body *[]byte, span trace.Span) int {
	pub, err := protocol.ExtractModel(*body)
	if err != nil {
		return 0 // 无 model 字段的请求（如部分工具类端点）直接透传
	}
	st.publicModel = pub
	st.stream = protocol.IsStream(*body)

	table := g.resolveTable(r)
	// 指标标签只接受声明过的模型名，其余归一，防止基数爆炸。
	if table.Declared(pub) || g.static.Declared(pub) {
		st.metricModel = pub
	} else {
		st.metricModel = modelOther
	}

	up, kind, ok := table.LookupKind(pub)
	// Header 表未命中时回落静态表：Header 通常只声明本渠道的少量映射，
	// 全局通配与兜底规则配在环境变量里，两者应当叠加而非互相屏蔽。
	if !ok && table != g.static {
		up, kind, ok = g.static.LookupKind(pub)
	}
	st.matchKind = kind

	switch {
	case ok:
		st.upstreamModel = up
	case g.cfg.Mapping.Strict:
		st.outcome = "unmapped_model"
		// 带上被拒的模型名：严格模式下这是最高频的 4xx，
		// 看板上没有模型名就无法判断该补哪条映射。
		msg := "model `" + pub + "` is not available"
		obs.RecordGatewayError(span, st.outcome, msg, nil)
		return writeError(w, http.StatusBadRequest, "invalid_request_error", msg)
	default:
		// 未命中任何规则时原样透传，保证网关对新模型零配置可用。
		st.upstreamModel = pub
	}

	// 能力识别与文档前置路由。放在模型选定之后：前置路由要覆盖的正是
	// 上一步选出的模型。
	g.applyCapabilities(r, st, *body)

	if st.upstreamModel == pub {
		return 0
	}
	nb, err := protocol.RewriteModel(*body, st.upstreamModel)
	if err != nil {
		st.outcome, st.err = "rewrite_failed", err
		// 改写失败意味着请求体形态超出预期（model 字段类型不对等），
		// 对客户端只回一句通用错误，具体原因必须进看板。
		obs.RecordGatewayError(span, st.outcome, "failed to rewrite request", err)
		return writeError(w, http.StatusInternalServerError, "gateway_error", "failed to rewrite request")
	}
	*body = nb
	return 0
}

// applyCapabilities 识别请求用到的能力，并处理文档理解的前置路由。
//
// 绝大多数能力走「先撞错再切」：上游明确报错才说明当前模型确实不支持，
// 这样不会为了假设中的不支持而牺牲主力模型的效果。
//
// 文档理解是唯一的例外 —— 请求体一旦携带 file_id，直接改走文档模型：
//  1. file_id 由上游的文件服务颁发，绑定在特定模型/服务上，
//     发给不支持的模型往往得到 "file not found" 这类误导性错误，
//     无法与真正的文件不存在区分开；
//  2. 文件请求体积大，撞一次错的成本（上传 + 等待 + 重发）远高于普通请求。
func (g *Gateway) applyCapabilities(r *http.Request, st *state, body []byte) {
	cm := g.resolveCapMap(r)
	want := cm.Want()
	if want == 0 {
		return
	}
	st.caps = capability.Detect(body, want)
	if st.caps.Empty() {
		return
	}
	// 文档理解前置路由：命中即改写，不等上游报错。
	if st.caps.Has(capability.File) {
		if m, ok := cm.Lookup(capability.File); ok && m != st.upstreamModel {
			st.upstreamModel = m
			st.capUsed = capability.File
		}
	}
}

// resolveCapMap 优先使用请求头下发的能力映射，缺失时回落到静态配置。
func (g *Gateway) resolveCapMap(r *http.Request) *capability.Map {
	if raw := r.Header.Get(CapHeader); raw != "" {
		if m := g.capCache.Get(raw); !m.Empty() {
			return m
		}
	}
	return g.capStatic
}

// buildRequest 组装发往上游的请求：路径拼接、头过滤、按协议注入凭据与必需头。
func (g *Gateway) buildRequest(
	ctx context.Context, r *http.Request,
	spec *protocol.Spec, base string, body []byte,
) (*http.Request, error) {
	uri, err := buildURL(base, r.URL)
	if err != nil {
		return nil, err
	}

	var rc io.Reader
	if len(body) > 0 {
		rc = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, uri, rc)
	if err != nil {
		return nil, err
	}

	copyRequestHeaders(req.Header, r.Header)
	if len(body) > 0 {
		req.ContentLength = int64(len(body))
		req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	}

	// 凭据透传：取客户端的 Key（或配置的兜底），按目标协议的方式注入。
	// 这样客户端无需关心上游是 Bearer 还是 x-api-key。
	key := extractCredential(r.Header)
	if key == "" {
		key = g.cfg.Upstream.Key
	}
	if v := spec.AuthValue(key); v != "" {
		req.Header.Set(spec.AuthHeader, v)
	}
	// 协议必需头：仅在客户端未提供时补齐，不覆盖用户的显式指定。
	for k, v := range spec.RequiredHeaders {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}
	req.Host = req.URL.Host
	return req, nil
}

// replacerFor 构建本次请求的脱敏替换器。
func (g *Gateway) replacerFor(st *state) *sanitize.Replacer {
	if !g.cfg.SanitizeEnabled() || st.publicModel == "" {
		return nil
	}
	return g.rules.For(st.upstreamModel, st.publicModel)
}

func (g *Gateway) readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	limit := g.cfg.Limits.MaxBodyBytes
	b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("request body too large")
	}
	return b, nil
}

// resolveTable 优先使用 Header 下发的映射，缺失时回落到静态配置。
func (g *Gateway) resolveTable(r *http.Request) *mapping.Table {
	if raw := r.Header.Get(MapHeader); raw != "" {
		if t := g.mapCache.Get(raw); !t.Empty() {
			return t
		}
	}
	return g.static
}

func (g *Gateway) log(st *state, status int, r *http.Request) {
	attrs := []any{
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("proto", st.proto),
		slog.Int("status", status),
		slog.String("model", st.publicModel),
		slog.Bool("stream", st.stream),
		slog.String("outcome", st.outcome),
		slog.String("match", st.matchKind.String()),
		slog.Bool("failover", st.failedOver),
		slog.Int64("ttfb_ms", st.ttfb.Milliseconds()),
		slog.Int64("dur_ms", time.Since(st.start).Milliseconds()),
		slog.Int64("in_bytes", st.bytesIn),
		slog.Int64("out_bytes", st.bytesOut),
	}
	if st.stream {
		attrs = append(attrs, slog.Int64("sse_events", st.sseEvents))
	}
	if !st.caps.Empty() {
		attrs = append(attrs, slog.String("caps", st.caps.String()))
	}
	if st.capUsed != capability.None {
		attrs = append(attrs, slog.String("cap_route", st.capUsed.String()))
	}
	// 已脱敏为 scheme://host。只在走了 per-request 代理时出现，
	// 默认出口不打这个字段以免每条日志多一列常量。
	if st.proxy != "" {
		attrs = append(attrs, slog.String("proxy", st.proxy))
	}
	// 真实上游模型仅进入内部日志，永不出现在对外响应中。
	if g.o.LogUpstreamModel() && st.upstreamModel != "" {
		attrs = append(attrs, slog.String("upstream_model", st.upstreamModel))
	}

	lg := g.o.Logger
	switch {
	case st.err != nil:
		lg.Error("请求失败", append(attrs, slog.String("err", st.err.Error()))...)
	case status >= 500:
		lg.Error("上游错误", attrs...)
	case status >= 400:
		lg.Warn("请求被拒", attrs...)
	default:
		lg.Info("请求完成", attrs...)
	}
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000.0
}

func outcomeForStatus(s int) string {
	switch {
	case s >= 500:
		return "upstream_5xx"
	case s == 429:
		return "rate_limited"
	case s >= 400:
		return "upstream_4xx"
	default:
		return "ok"
	}
}

// buildURL 把入站路径拼接到上游 base 上。
//
// 拼接规则（覆盖主流上游形态，无需逐个端点配置）：
//
//	base 无路径段                   -> 原样拼接入站路径
//	  https://api.openai.com        + /v1/chat/completions = https://api.openai.com/v1/chat/completions
//	  https://api.anthropic.com     + /v1/messages         = https://api.anthropic.com/v1/messages
//	base 已含路径段（自带版本前缀） -> 剥离入站的 /v1 段后拼接
//	  https://ark…/api/v3           + /v1/chat/completions = https://ark…/api/v3/chat/completions
//	  https://xx/v1                 + /v1/messages         = https://xx/v1/messages
func buildURL(base string, u *url.URL) (string, error) {
	if base == "" {
		return "", errors.New("empty upstream base")
	}
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	basePath := strings.TrimRight(b.EscapedPath(), "/")
	path := joinUpstreamPath(basePath, u.EscapedPath())

	b.Path, b.RawPath, b.RawQuery, b.Fragment = "", "", "", ""
	out := b.String() + path
	if u.RawQuery != "" {
		out += "?" + u.RawQuery
	}
	return out, nil
}

// joinUpstreamPath 实现 buildURL 文档中描述的拼接规则。
func joinUpstreamPath(basePath, reqPath string) string {
	if basePath == "" {
		return reqPath
	}
	// base 自带路径前缀时，入站的 /v1 属于「网关对外协议版本」，不应二次带入上游。
	if i := strings.IndexByte(reqPath[1:], '/'); i >= 0 {
		if first := reqPath[1 : i+1]; isVersionSeg(first) {
			return basePath + reqPath[i+1:]
		}
	}
	return basePath + reqPath
}

// isVersionSeg 判断路径首段是否为 v1 / v2 / v1beta 之类的版本标识。
func isVersionSeg(s string) bool {
	return len(s) >= 2 && s[0] == 'v' && s[1] >= '0' && s[1] <= '9'
}

// copyRequestHeaders 转发客户端请求头，剔除逐跳首部、凭据与网关内部协议头。
func copyRequestHeaders(dst, src http.Header) {
	for k, vv := range src {
		switch {
		case isHop(k),
			// 凭据由 buildRequest 按协议重新注入，此处先剔除。
			isCredentialHeader(k),
			// 网关内部协议头不外泄。
			isInternalHeader(k),
			strings.EqualFold(k, "Host"),
			strings.EqualFold(k, "Content-Length"),
			// 由网关统一协商，避免上游返回压缩流破坏 SSE 逐行处理。
			strings.EqualFold(k, "Accept-Encoding"):
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
	dst.Set("Accept-Encoding", "identity")
}

func copyResponseHeaders(dst, src http.Header, drop []string) {
	for k, vv := range src {
		if isHop(k) || strings.EqualFold(k, "Content-Length") || matchAny(k, drop) {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

func matchAny(k string, list []string) bool {
	for _, s := range list {
		if strings.EqualFold(k, s) {
			return true
		}
	}
	return false
}

func isHop(k string) bool { return matchAny(k, hopHeaders) }

func isSSE(resp *http.Response) bool {
	return strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
}

func classifyUpstreamError(ctx context.Context, err error) (int, string) {
	switch {
	case errors.Is(err, context.Canceled) && ctx.Err() != nil:
		return 499, "client closed request"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusGatewayTimeout, "upstream timeout"
	default:
		return http.StatusBadGateway, "upstream connection failed"
	}
}
