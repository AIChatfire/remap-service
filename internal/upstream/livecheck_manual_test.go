package upstream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/betterme/remap-service/internal/config"
)

func liveProxy(t *testing.T) string {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("TESTPROXY"))
	if raw == "" {
		t.Skip("未设置 TESTPROXY")
	}
	return raw
}

func egressIP(t *testing.T, hc *http.Client, url string) (string, string) {
	t.Helper()
	resp, err := hc.Get(url)
	if err != nil {
		t.Fatalf("%s 请求失败: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(b)), resp.Proto
}

// TestLiveProxySchemes 验证 socks5 与 socks5h 两种写法都能出网。
func TestLiveProxySchemes(t *testing.T) {
	raw := liveProxy(t)
	for _, scheme := range []string{"socks5", "socks5h"} {
		p := scheme + strings.TrimPrefix(raw, strings.Split(raw, "://")[0])
		norm, err := ValidateProxy(p)
		if err != nil {
			t.Fatalf("%s 校验失败: %v", scheme, err)
		}
		rt := NewRoundTripper(64, config.Upstream{Proxy: norm})
		hc := &http.Client{Transport: rt, Timeout: 30 * time.Second}
		ip, proto := egressIP(t, hc, "https://api.ipify.org")
		rt.CloseIdleConnections()
		t.Logf("%-8s 出口IP=%s proto=%s 脱敏=%s", scheme, ip, proto, RedactProxy(norm))
		if ip == "" {
			t.Errorf("%s 未取到出口 IP", scheme)
		}
	}
}

// TestLivePerRequestProxy 验证 X-Upstream-Proxy 路径：默认直连的 Router
// 在 ctx 带代理时改走代理，出口 IP 应与直连不同。
func TestLivePerRequestProxy(t *testing.T) {
	raw := liveProxy(t)
	norm, err := ValidateProxy(raw)
	if err != nil {
		t.Fatalf("校验失败: %v", err)
	}
	// 默认出口不配代理
	rt := NewRoundTripper(64, config.Upstream{})
	defer rt.CloseIdleConnections()
	hc := &http.Client{Transport: rt, Timeout: 30 * time.Second}

	req, _ := http.NewRequestWithContext(
		WithProxy(context.Background(), norm), http.MethodGet, "https://api.ipify.org", nil)
	resp, err := hc.Do(req)
	if err != nil {
		t.Fatalf("per-request 代理请求失败: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	got := strings.TrimSpace(string(b))
	t.Logf("per-request 代理出口IP=%s", got)
	if !strings.Contains(norm, got) {
		t.Errorf("出口 IP %q 不是代理 IP，per-request 代理未生效", got)
	}
}

// TestLiveNoProxyBypass 验证 NO_PROXY 命中的目标直连，同时确认远端仍走代理。
//
// 只测本地可达是假阳性：代理配置整体失效时本地请求依然成功。因此这里用
// 同一个 Router 各发一次，断言「本地直连 + 远端经代理」两个事实同时成立。
func TestLiveNoProxyBypass(t *testing.T) {
	raw := liveProxy(t)
	norm, err := ValidateProxy(raw)
	if err != nil {
		t.Fatalf("校验失败: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("local-ok"))
	}))
	defer srv.Close()

	rt := NewRoundTripper(64, config.Upstream{Proxy: norm, NoProxy: "127.0.0.1,localhost,::1"})
	defer rt.CloseIdleConnections()
	hc := &http.Client{Transport: rt, Timeout: 30 * time.Second}

	body, _ := egressIP(t, hc, srv.URL)
	if body != "local-ok" {
		t.Fatalf("本地请求未直连, 返回 %q", body)
	}
	t.Logf("NO_PROXY 绕过生效: %s -> %s", srv.URL, body)

	// 反向确认：同一 Router 访问远端时代理仍生效，排除「代理压根没配上」。
	ip, _ := egressIP(t, hc, "https://api.ipify.org")
	if !strings.Contains(norm, ip) {
		t.Errorf("远端出口 IP %q 不是代理 IP，代理未生效，上一断言无意义", ip)
	}
	t.Logf("同一 Router 远端仍走代理: 出口IP=%s", ip)
}
