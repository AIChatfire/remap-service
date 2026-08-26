package gateway

import (
	"net/http"
	"strings"

	"github.com/betterme/remap-service/internal/upstream"
)

// 网关内部协议头。这些头不会转发给上游。
const (
	// MapHeader 承载模型映射声明。
	MapHeader = "X-Model-Map"
	// BaseHeader 覆盖本次请求的上游地址。
	BaseHeader = "X-Upstream-Base"
	// CapHeader 承载「能力 -> 上游模型」的声明，用于按能力做故障切换。
	CapHeader = "X-Model-Capability"
	// ProxyHeader 覆盖本次请求的出网代理，值为完整 URL。
	ProxyHeader = "X-Upstream-Proxy"
)

// credentialHeaders 是可能承载客户端凭据的请求头，按优先级排列。
//
// 网关不持有密钥，客户端的凭据被提取出来后按目标协议重新注入 ——
// 因此客户端用 OpenAI 风格调用 Anthropic 端点也能正常工作。
var credentialHeaders = []string{
	"Authorization",
	"X-Api-Key",
	"Api-Key",
}

// extractCredential 从请求中提取客户端凭据（已剥离 Bearer 前缀）。
func extractCredential(h http.Header) string {
	for _, name := range credentialHeaders {
		v := strings.TrimSpace(h.Get(name))
		if v == "" {
			continue
		}
		// 注意先判前缀再取值：`Bearer ` 经 TrimSpace 后变成 `Bearer`，
		// 若按普通取值返回会把方案名当成密钥发给上游。
		if k, ok := trimScheme(v, "bearer"); ok {
			if k == "" {
				continue // 只有方案名没有密钥，视作未提供
			}
			return k
		}
		return v
	}
	return ""
}

// trimScheme 剥离形如 "Bearer xxx" 的认证方案前缀。
// 返回 ok=true 表示确实带了该方案（此时 key 可能为空）。
func trimScheme(v, scheme string) (key string, ok bool) {
	n := len(scheme)
	if len(v) < n || !strings.EqualFold(v[:n], scheme) {
		return "", false
	}
	rest := v[n:]
	if rest == "" {
		return "", true // 纯 "Bearer"
	}
	if rest[0] != ' ' && rest[0] != '\t' {
		return "", false // 如 "BearerToken"，不是方案前缀
	}
	return strings.TrimSpace(rest), true
}

// isCredentialHeader 报告某个头名是否承载凭据（转发时需剔除后重新注入）。
func isCredentialHeader(name string) bool {
	for _, n := range credentialHeaders {
		if strings.EqualFold(name, n) {
			return true
		}
	}
	return false
}

// isInternalHeader 报告某个头名是否为网关内部协议头。
func isInternalHeader(name string) bool {
	return strings.EqualFold(name, MapHeader) ||
		strings.EqualFold(name, BaseHeader) ||
		strings.EqualFold(name, CapHeader) ||
		strings.EqualFold(name, ProxyHeader)
}

// resolveProxy 决定本次请求的出网代理。
//
// 优先级：X-Upstream-Proxy 请求头 > UPSTREAM_PROXY 配置 > 环境变量。
// 返回空串表示不做 per-request 覆盖（由 Router 落到默认出口）。
//
// 与 resolveBase 一致：头存在但非法时返回 ok=false 而非静默回落 ——
// 代理配错时静默直连可能让内网流量走到公网出口。
func (g *Gateway) resolveProxy(r *http.Request) (string, bool) {
	v := strings.TrimSpace(r.Header.Get(ProxyHeader))
	if v == "" {
		return "", true
	}
	if !g.cfg.Upstream.AllowProxyHeader {
		// 未开启时视作未声明，直接忽略。头本身仍不会转发给上游。
		return "", true
	}
	p, err := upstream.ValidateProxy(v)
	if err != nil {
		return "", false
	}
	return p, true
}

// resolveBase 决定本次请求的上游地址。
// 优先级：X-Upstream-Base 请求头 > 按协议的环境变量 > 默认环境变量。
func (g *Gateway) resolveBase(r *http.Request, proto string) (string, bool) {
	if g.cfg.Upstream.AllowBaseHeader {
		if v := strings.TrimSpace(r.Header.Get(BaseHeader)); v != "" {
			if b, ok := normalizeBase(v); ok {
				return b, true
			}
			// 头存在但非法：不静默回落，避免请求被发到预期之外的地址。
			return "", false
		}
	}
	base := g.cfg.BaseFor(proto)
	return base, base != ""
}

// normalizeBase 校验并规范化上游地址。缺少 scheme 时补 https。
//
// 只接受 http/https：其余 scheme（file、ftp、gopher…）一律拒绝，
// 避免网关被当作任意协议的跳板。
func normalizeBase(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", false
	}

	// 先切分 scheme，再处理剩余部分 —— 顺序反过来会把 "https://"
	// 的尾斜杠去掉后当成无 scheme 的主机名，拼出 "https://https:"。
	scheme := "https"
	rest := v
	if i := strings.Index(v, "://"); i >= 0 {
		scheme = strings.ToLower(v[:i])
		rest = v[i+3:]
		if scheme != "http" && scheme != "https" {
			return "", false
		}
	}

	rest = strings.TrimRight(rest, "/")
	if rest == "" || rest[0] == '/' {
		return "", false // 没有 host
	}
	host := rest
	if i := strings.IndexByte(host, '/'); i >= 0 {
		host = host[:i]
	}
	if host == "" || strings.ContainsAny(host, " \t") {
		return "", false
	}
	return scheme + "://" + rest, true
}
