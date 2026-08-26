package gateway

import (
	"net"
	"net/http"
	"strings"
)

// clientIP 提取客户端 IP，用于 trace 属性（不用于指标标签）。
//
// hops 是网关前方可信反向代理的层数，语义如下：
//
//	hops = 0  只信任 RemoteAddr，完全忽略 X-Forwarded-For。
//	          直接暴露在公网时必须用这个值。
//	hops = n  从 X-Forwarded-For 右端向左跳过 n 个由可信代理追加的地址，
//	          取再左一个作为客户端 IP。
//
// 之所以从右往左数：XFF 由每一跳追加，右端是离网关最近、最可信的部分，
// 左端可被客户端预先伪造。盲取最左值等于让客户端自由声明自己的 IP。
func clientIP(r *http.Request, hops int) string {
	if hops <= 0 {
		return hostOnly(r.RemoteAddr)
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return hostOnly(r.RemoteAddr)
	}

	parts := strings.Split(xff, ",")
	// RemoteAddr 本身相当于 XFF 最右侧再往右一跳，因此需要跳过的
	// 链内地址是 hops-1 个。
	idx := len(parts) - hops
	if idx < 0 {
		// 声明的跳数多于实际链长：链路被截断或配置偏大，
		// 此时最左值是链上能拿到的最早地址，仍优于回落到代理 IP。
		idx = 0
	}
	if ip := strings.TrimSpace(parts[idx]); ip != "" {
		return ip
	}
	return hostOnly(r.RemoteAddr)
}

// hostOnly 去掉 RemoteAddr 上的端口，保留 IPv6 的原始形态。
func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}
