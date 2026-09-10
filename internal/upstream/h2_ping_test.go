package upstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/betterme/remap-service/internal/config"
)

// h2 PING 探活失败必须真的走完整条链：连接被关 + 事件被上报。
//
// 为什么需要这条用例：既有的 TestH2HealthCheck* 只断言「两个阈值被写进去」，
// 属于赋值层面。而这类参数最危险的失效形态恰恰是「赋值对了但没人读」——
// 配置改了、单测全绿、线上照旧。这里把整条链跑通，钉住「配合生效」。
//
// 必须走 std http.Transport，不能直接调 http2.Transport：ConfigureTransports
// 返回的 h2 transport 装的是 noDialClientConnPool —— 按设计「它永不自己拨号，
// 由 HTTP/1.1 客户端拨号后把 TLS 连接交给它」。独立使用它会在取连接阶段
// 直接返回 ErrNoCachedConn（no cached connection was available），
// 连 PING 都走不到。
//
// 它同时固化了一个容易忽略的事实：closeForLostPing 关的是**整条**连接，
// 其上所有在途流一起拿到同一个错误 —— 这正是该错误串表现为「批状故障」的
// 机制，也是连接级事件必须单独计数、不能塞进某个请求 span 的原因。
func TestH2PingFailureClosesConnAndReportsError(t *testing.T) {
	// 借 httptest 生成一套可用的服务端证书与 TLS 配置，免得手写自签逻辑。
	certSrv := httptest.NewUnstartedServer(nil)
	certSrv.EnableHTTP2 = true
	certSrv.StartTLS()
	defer certSrv.Close()

	addr, stop := startPingDeadH2Server(t, certSrv.TLS)
	defer stop()

	var (
		mu    sync.Mutex
		kinds []string
	)
	report := func(kind string) {
		mu.Lock()
		kinds = append(kinds, kind)
		mu.Unlock()
	}

	// 走生产路径：CountError 由 applyH2HealthCheck 接上，测试不手工补。
	tr := newTransport(4, config.Transport{
		H2IdleTimeout: 80 * time.Millisecond,
		H2PingTimeout: 80 * time.Millisecond,
	}, nil, report)
	defer tr.CloseIdleConnections()

	// 只补 RootCAs，不整体替换这个 config —— NextProtos 由 ConfigureTransports
	// 填好（含 h2），替换掉就等于把 ALPN 抹了，请求会降级成 HTTP/1.1。
	pool := x509.NewCertPool()
	pool.AddCert(certSrv.Certificate())
	tr.TLSClientConfig.RootCAs = pool
	// 请求写 example.com（证书 SAN 就是它），拨号改指 stub。
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}

	req, err := http.NewRequest(http.MethodPost,
		"https://example.com/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}

	// stub 永不回响应，所以这个请求只能靠「PING 判定连接死亡」来结束。
	errCh := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("请求成功了：stub 从不响应，说明 PING 根本没生效")
		}
		if !strings.Contains(err.Error(), "client connection lost") {
			t.Errorf("错误 = %q，期望含 client connection lost", err.Error())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("请求既没完成也没被 PING 中断：探活机制没在工作")
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(kinds, "conn_close_lost_ping") {
		t.Errorf("CountError 未收到 conn_close_lost_ping，实际收到 %v", kinds)
	}
}

// startPingDeadH2Server 起一个「TLS + h2 握手正常、但永不回应 PING」的服务端。
//
// 不能用 httptest.Server：Go 的 h2 服务端会自动回 PONG（协议栈行为，handler
// 管不到），那样 PING 永远不会失败。这里只实现握手所需的最小帧处理，其余一律
// 忽略 —— 于是 PING 石沉大海，客户端的 healthCheck 如期判定连接死亡。
func startPingDeadH2Server(t *testing.T, srvTLS *tls.Config) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go servePingDeadH2(c, srvTLS)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func servePingDeadH2(c net.Conn, srvTLS *tls.Config) {
	defer c.Close()

	cfg := srvTLS.Clone()
	// 只宣告 h2：协商不出 h2 的话 std transport 会把这连接当 HTTP/1.1 用，
	// 那 PING 就完全不是本用例要测的那条路径了。
	cfg.NextProtos = []string{"h2"}
	tc := tls.Server(c, cfg)
	if err := tc.Handshake(); err != nil {
		return
	}
	defer tc.Close()

	pre := make([]byte, len(http2.ClientPreface))
	if _, err := io.ReadFull(tc, pre); err != nil {
		return
	}
	if string(pre) != http2.ClientPreface {
		return
	}
	// 服务端 SETTINGS：让客户端认为握手完成、连接可用。
	if err := writeH2Frame(tc, http2.FrameSettings, 0, nil); err != nil {
		return
	}

	for {
		var hdr [9]byte
		if _, err := io.ReadFull(tc, hdr[:]); err != nil {
			return
		}
		length := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
		ftype, flags := http2.FrameType(hdr[3]), http2.Flags(hdr[4])
		if length > 0 {
			if _, err := io.CopyN(io.Discard, tc, int64(length)); err != nil {
				return
			}
		}
		// SETTINGS 回 ACK；PING 故意不回 —— 这正是本 stub 存在的理由。
		if ftype == http2.FrameSettings && flags&http2.FlagSettingsAck == 0 {
			if err := writeH2Frame(tc, http2.FrameSettings, http2.FlagSettingsAck, nil); err != nil {
				return
			}
		}
	}
}

// writeH2Frame 写一个流 0 的 h2 帧（负载可空）。
func writeH2Frame(w io.Writer, ftype http2.FrameType, flags http2.Flags, payload []byte) error {
	hdr := [9]byte{
		byte(len(payload) >> 16), byte(len(payload) >> 8), byte(len(payload)),
		byte(ftype), byte(flags),
	}
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}
