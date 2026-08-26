// Package proxyurl 提供代理 URL 的校验与脱敏。
//
// 独立成包是为了让 config 与 upstream 共用同一份实现：upstream 已导入
// config，把校验放在 config 会成环，各写一份则必然随时间漂移。
// 本包零依赖（仅标准库），不引用项目内任何包。
package proxyurl

import (
	"fmt"
	"net/url"
	"strings"
)

// 支持的代理 scheme。
//
// socks5 与 socks5h 在 net/http.Transport 中行为一致 —— 两者都由代理侧
// 解析目标域名。保留 socks5h 是为了兼容 curl 的书写习惯，不做特殊处理。
const (
	SchemeHTTP    = "http"
	SchemeHTTPS   = "https"
	SchemeSOCKS5  = "socks5"
	SchemeSOCKS5H = "socks5h"
)

// Validate 校验代理 URL 并返回规范化形式。
//
// 要求完整 URL：必须带受支持的 scheme 且有主机名。不接受裸 host:port ——
// 猜测 scheme 会让 socks5 代理被当成 http 代理用，表现是所有请求超时。
func Validate(v string) (string, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return "", fmt.Errorf("代理地址为空")
	}
	// 显式要求 "://"：url.Parse("127.0.0.1:1080") 会把 "127.0.0.1"
	// 当成 scheme 且不报错，必须先挡掉这种形态。
	if !strings.Contains(v, "://") {
		return "", fmt.Errorf("代理地址缺少 scheme，需形如 socks5://host:1080：%q", v)
	}
	u, err := url.Parse(v)
	if err != nil {
		return "", fmt.Errorf("代理地址解析失败: %w", err)
	}
	switch strings.ToLower(u.Scheme) {
	case SchemeHTTP, SchemeHTTPS, SchemeSOCKS5, SchemeSOCKS5H:
	default:
		return "", fmt.Errorf("代理 scheme 只能是 http|https|socks5|socks5h，当前 %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return "", fmt.Errorf("代理地址缺少主机名：%q", v)
	}
	// 统一小写 scheme，让相同代理的不同写法命中同一个连接池条目。
	u.Scheme = strings.ToLower(u.Scheme)
	return u.String(), nil
}

// Redact 剥掉代理 URL 的 userinfo 段，只保留 scheme://host。
// 用于日志与 trace 属性 —— 代理凭据不进任何可观测出口。
func Redact(v string) string {
	if v == "" {
		return ""
	}
	u, err := url.Parse(v)
	if err != nil {
		return "invalid"
	}
	return u.Scheme + "://" + u.Host
}
