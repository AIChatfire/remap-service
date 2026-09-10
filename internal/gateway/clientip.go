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
//	hops = n  取 X-Forwarded-For 右起第 n 位（RemoteAddr 本身算最右一跳）。
//	          前置 1 层 Nginx / CLB 取最右一位；2 层取右起第二位。
//
// 之所以从右往左数：XFF 由每一跳追加，右端是离网关最近、最可信的部分，
// 左端可被客户端预先伪造。盲取最左值等于让客户端自由声明自己的 IP。
//
// 注意这里是「右起第 n 位」而**不是**「跳过 n 个再取它左边那个」——前置 1 层
// 代理时，真实客户端 IP 就是最右那一位（代理刚追加的）。两种写法只差一位，
// 而差一位的后果是取到一个「看起来正常」的地址：不报错、不影响功能，
// 只是静静地把每次请求的来源记成另一个人。改这里务必对照 clientip_test.go。
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
