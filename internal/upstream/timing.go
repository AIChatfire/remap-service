package upstream

import (
	"crypto/tls"
	"net"
	"time"
)

// Timing 记录一次上游请求各阶段的时间偏移与连接事实。
//
// 全部用「相对请求发起时刻的偏移」而不是「相邻阶段的差值」，理由有两条：
//
//   - 偏移量可以直接与超时上限对照。wrote_request=299.8s 对上 limit=300s，
//     一眼就能判定「请求根本没发出去」，差值还得再做一次减法才能比；
//   - 阶段可能缺失。复用连接时没有 DNS / 建连 / TLS，用差值会在这些
//     位置上退化成无意义的 0，看上去像「瞬间完成」，与「没发生」同形。
//
// 零值表示该阶段没有发生（典型是连接复用）或尚未走到。
type Timing struct {
	DNS          time.Duration
	Connect      time.Duration
	TLS          time.Duration
	WroteRequest time.Duration
	FirstByte    time.Duration
	// Reused 报告本次是否复用了已有连接。仅在 GotConn 为 true 时有意义。
	Reused bool
	// GotConn 报告是否成功拿到过一条连接。
	//
	// 与 Reused 分开是必要的：DNS 或建连阶段就失败时两者都是零值，
	// 但「没复用连接」与「根本没连上」是完全不同的结论，
	// 少了这个标记就无法区分，看板上会把连不上的请求读成「每条都新建连接」。
	GotConn bool
	// Proto 是归一后的协议版本，取值 h2 / http/1.1，未知为空串。
	Proto string
}

// protoLabel 把「响应里的协议版本」与「ALPN 协商结果」归一到同一个取值集。
//
// 不归一会让同一个属性同时出现 "HTTP/2.0" 与 "h2" 两种写法，
// 看板上按它分组会得到两行，而它们其实是同一件事。
func protoLabel(respProto, alpn string) string {
	switch {
	case respProto == "HTTP/2.0" || alpn == "h2":
		return "h2"
	case respProto != "", alpn != "":
		return "http/1.1"
	default:
		return ""
	}
}

// protoFromConn 从已建立的连接上推断协议版本，供「响应还没到」的场景兜底。
//
// 超时是这里的主要用途：一个字节都没回来，就没有 resp.Proto 可读，
// 但「这次到底走的 h2 还是 h1.1」恰恰是超时排查要回答的第一个问题
// （h2 下超出对端流并发上限的请求会在客户端排队，而排队时间计入首字节超时）。
//
// 传输层强制 HTTPS + HTTP/2，因此 TLS 握手后的 ALPN 结果就是最终协议。
// 非 TLS 连接返回空串（本服务不走 h2c）。
func protoFromConn(c net.Conn) string {
	tc, ok := c.(*tls.Conn)
	if !ok {
		return ""
	}
	return tc.ConnectionState().NegotiatedProtocol
}
