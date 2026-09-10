package main

import (
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
)

// probeHostPort 必须把监听写法翻译成「可以直连的目标地址」。
// 这里最容易错的是 :8080 与 0.0.0.0:8080 —— 它们都是合法的**监听**地址，
// 但都不能直接当 URL 主机用（前者主机段为空，后者是 bind 专用地址）。
//
// 反向的一半同样重要：解析不了的地址必须报错，不能回落到默认端口。
// 回落会制造假绿灯 —— 实测本机 8080 上就有个无关服务对 /healthz 返回 200，
// 探针会把它当成自己的网关。
func TestProbeHostPort(t *testing.T) {
	ok := []struct {
		in       string
		wantHost string
		wantPort string
	}{
		{":8080", "127.0.0.1", "8080"},
		{"0.0.0.0:9000", "127.0.0.1", "9000"},
		{"[::]:9000", "127.0.0.1", "9000"},
		{"127.0.0.1:18080", "127.0.0.1", "18080"},
		{"localhost:8080", "localhost", "8080"},
		// 显式绑到某个内网地址时原样使用：探针在同一个网络命名空间里，
		// 那个地址本来就是可达的。
		{"192.168.1.5:8080", "192.168.1.5", "8080"},
		// 空值不是猜测：config 的默认值就是 ":8080"。
		{"", "127.0.0.1", "8080"},
		{"   ", "127.0.0.1", "8080"},
	}
	for _, c := range ok {
		host, port, err := probeHostPort(c.in)
		if err != nil {
			t.Errorf("probeHostPort(%q) 意外报错: %v", c.in, err)
			continue
		}
		if host != c.wantHost || port != c.wantPort {
			t.Errorf("probeHostPort(%q) = (%q, %q), want (%q, %q)",
				c.in, host, port, c.wantHost, c.wantPort)
		}
	}

	// 这些写法下进程自己就起不来（net.Listen 直接报错），探针必须同样失败。
	for _, bad := range []string{"garbage", "8080", "::1:8080", "host:port", ":-1"} {
		if _, _, err := probeHostPort(bad); err == nil {
			t.Errorf("probeHostPort(%q) 应报错，回落会制造假绿灯", bad)
		}
	}
}

// 真起一个 HTTP 服务再探它，而不是两端都用 mock：探针的价值全在
// 「地址拼对了没有、真的能连上吗」，两端都假的话拼错也测不出来。
func TestSelfHealthCheckOK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	t.Setenv("ADDR", ln.Addr().String())
	// 传一个不存在的 env 文件：显式避免读到仓库里真实的 .env，
	// 否则本用例会顺带把生产配置注入测试进程的环境。
	if err := selfHealthCheck(filepath.Join(t.TempDir(), "absent.env")); err != nil {
		t.Fatalf("健康服务应探测成功: %v", err)
	}
}

// 反向用例：监听已关闭时必须失败。没有这一条，「探针」写成恒返回 nil
// 也能让上面那个用例通过。
func TestSelfHealthCheckFailsWhenDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // 立刻释放：该端口上不再有人监听

	t.Setenv("ADDR", addr)
	if err := selfHealthCheck(filepath.Join(t.TempDir(), "absent.env")); err == nil {
		t.Fatal("无人监听时探针必须失败")
	}
}

// 状态码非 200 也算失败：/healthz 正常恒 200，
// 拿到别的码说明敲错了端点（例如探到了代理或别的服务）。
func TestSelfHealthCheckRejectsNonOK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	t.Setenv("ADDR", ln.Addr().String())
	if err := selfHealthCheck(filepath.Join(t.TempDir(), "absent.env")); err == nil {
		t.Fatal("非 200 必须判定为失败")
	}
}
