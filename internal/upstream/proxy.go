package upstream

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"golang.org/x/net/http/httpproxy"

	"github.com/betterme/remap-service/internal/proxyurl"
)

// proxyKey 是代理地址在 context 中的键。
// 用私有零大小类型避免与其他包的键冲突。
type proxyKey struct{}

// WithProxy 把代理地址挂到 ctx 上，供 Router 在 RoundTrip 时取出。
//
// 传空串返回原 ctx，避免在 ctx 链上挂一个无意义的空值节点。
func WithProxy(ctx context.Context, proxy string) context.Context {
	if proxy == "" {
		return ctx
	}
	return context.WithValue(ctx, proxyKey{}, proxy)
}

// ProxyFrom 取出 ctx 上的代理地址，未设置时返回空串。
func ProxyFrom(ctx context.Context) string {
	v, _ := ctx.Value(proxyKey{}).(string)
	return v
}

// proxyWithBypass 返回固定走 fixed 的代理函数，但 noProxy 命中的目标直连。
//
// NO_PROXY 的匹配语义（CIDR、.suffix、带端口、* 全绕过）由
// golang.org/x/net/http/httpproxy 提供 —— 它正是标准库
// ProxyFromEnvironment 的实现来源，自己写一份必然随时间漂移。
//
// 这里把 fixed 填进 HTTPProxy 与 HTTPSProxy 两个字段：httpproxy 按请求
// scheme 选取，只填一个会让另一种 scheme 静默直连。
//
// 注意 httpproxy.Config 不读环境变量，此处不会被宿主机的 HTTP_PROXY 干扰。
func proxyWithBypass(fixed *url.URL, noProxy string) func(*http.Request) (*url.URL, error) {
	if strings.TrimSpace(noProxy) == "" {
		return http.ProxyURL(fixed)
	}
	s := fixed.String()
	cfg := &httpproxy.Config{HTTPProxy: s, HTTPSProxy: s, NoProxy: noProxy}
	// ProxyFunc() 内部预编译 NoProxy 规则，必须在构造期调用一次并复用，
	// 不能每请求重建 —— 那会把字符串解析搬进热路径。
	f := cfg.ProxyFunc()
	return func(r *http.Request) (*url.URL, error) { return f(r.URL) }
}

// ValidateProxy 校验并规范化代理 URL，见 proxyurl.Validate。
func ValidateProxy(v string) (string, error) { return proxyurl.Validate(v) }

// RedactProxy 剥掉代理 URL 的 userinfo 段，见 proxyurl.Redact。
func RedactProxy(v string) string { return proxyurl.Redact(v) }
