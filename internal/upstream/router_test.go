package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
)

// countingRT 记录默认出口被调用的次数。
type countingRT struct {
	n    atomic.Int64
	resp *http.Response
}

func (c *countingRT) RoundTrip(*http.Request) (*http.Response, error) {
	c.n.Add(1)
	return c.resp, nil
}

func newRouterFixture() (*Router, *countingRT) {
	def := &countingRT{resp: &http.Response{StatusCode: 204, Body: http.NoBody}}
	return NewRouter(def, 0, ""), def
}

// 未声明代理时必须原样走默认出口。
func TestRouterFallsBackToDefault(t *testing.T) {
	r, def := newRouterFixture()
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, err := r.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got := def.n.Load(); got != 1 {
		t.Errorf("默认出口调用次数 = %d，应为 1", got)
	}
	if n := poolSize(r); n != 0 {
		t.Errorf("未声明代理不应建 transport，池内有 %d 个", n)
	}
}

// 同一代理重复请求必须复用同一个 transport —— 这是连接池不退化的前提。
func TestRouterReusesTransportPerProxy(t *testing.T) {
	r, _ := newRouterFixture()
	const p = "socks5://127.0.0.1:1080"

	first, err := r.transportFor(p)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		got, err := r.transportFor(p)
		if err != nil {
			t.Fatal(err)
		}
		if got != first {
			t.Fatal("同一代理返回了不同 transport，连接池被拆散")
		}
	}
	if n := poolSize(r); n != 1 {
		t.Errorf("池内应只有 1 个 transport，实际 %d", n)
	}
}

// 不同代理各自独立，互不串用连接。
func TestRouterIsolatesDistinctProxies(t *testing.T) {
	r, _ := newRouterFixture()
	a, _ := r.transportFor("socks5://127.0.0.1:1080")
	b, _ := r.transportFor("http://127.0.0.1:8080")
	if a == b {
		t.Fatal("不同代理必须使用不同 transport")
	}
	if n := poolSize(r); n != 2 {
		t.Errorf("池内应有 2 个 transport，实际 %d", n)
	}
}

// 并发首次命中同一代理时只能构建一个实例（双重检查是否生效）。
func TestRouterConcurrentFirstUseBuildsOnce(t *testing.T) {
	r, _ := newRouterFixture()
	const p = "socks5://127.0.0.1:1080"

	var wg sync.WaitGroup
	got := make([]http.RoundTripper, 64)
	for i := range got {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t, err := r.transportFor(p)
			if err == nil {
				got[i] = t
			}
		}(i)
	}
	wg.Wait()

	for i, v := range got {
		if v == nil {
			t.Fatalf("第 %d 个 goroutine 未拿到 transport", i)
		}
		if v != got[0] {
			t.Fatal("并发首次使用建出了多个 transport")
		}
	}
	if n := poolSize(r); n != 1 {
		t.Errorf("池内应只有 1 个 transport，实际 %d", n)
	}
}

// per-request 代理确实生效：请求应落到代理服务器而非目标地址。
func TestRouterRoutesThroughProxy(t *testing.T) {
	var hits atomic.Int64
	px := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(204)
	}))
	defer px.Close()

	r := NewRouter(&countingRT{resp: &http.Response{StatusCode: 204, Body: http.NoBody}}, 0, "")
	defer r.CloseIdleConnections()

	// 目标是一个不存在的地址，只有真的过了代理才可能成功。
	req, _ := http.NewRequest(http.MethodGet, "http://gateway.invalid/v1/models", nil)
	req = req.WithContext(WithProxy(context.Background(), px.URL))

	resp, err := r.RoundTrip(req)
	if err != nil {
		t.Fatalf("经代理请求失败: %v", err)
	}
	resp.Body.Close()
	if hits.Load() != 1 {
		t.Errorf("代理收到 %d 个请求，应为 1", hits.Load())
	}
}

func TestRouterRejectsUnparsableProxy(t *testing.T) {
	r, _ := newRouterFixture()
	if _, err := r.transportFor("http://[::1"); err == nil {
		t.Error("无法解析的代理地址应报错")
	}
}

// 快路径不得引入堆分配 —— 绝大多数请求不带代理头，
// 这里退化会直接抬高全局 QPS 成本。
func BenchmarkRouterDefaultPath(b *testing.B) {
	r, _ := newRouterFixture()
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.RoundTrip(req); err != nil {
			b.Fatal(err)
		}
	}
}

// 代理命中池时应无锁无分配。
func BenchmarkRouterProxyLookup(b *testing.B) {
	r, _ := newRouterFixture()
	const p = "socks5://127.0.0.1:1080"
	if _, err := r.transportFor(p); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.transportFor(p); err != nil {
			b.Fatal(err)
		}
	}
}

// per-request 代理必须遵守 NoProxy，否则 X-Upstream-Proxy 会成为
// 绕过豁免列表的后门 —— 本机 mock 上游会被打进客户端指定的代理。
func TestRouterPerRequestProxyHonorsNoProxy(t *testing.T) {
	var hits atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(204)
	}))
	defer origin.Close()

	// 代理指向一个不可达地址：只有真的绕过代理直连 origin 才可能成功。
	r := NewRouter(&countingRT{resp: &http.Response{StatusCode: 204, Body: http.NoBody}},
		0, "127.0.0.1")
	defer r.CloseIdleConnections()

	req, _ := http.NewRequest(http.MethodGet, origin.URL+"/v1/models", nil)
	req = req.WithContext(WithProxy(context.Background(), "socks5://127.0.0.1:1"))

	resp, err := r.RoundTrip(req)
	if err != nil {
		t.Fatalf("NoProxy 未生效，请求被打进代理: %v", err)
	}
	resp.Body.Close()
	if got := hits.Load(); got != 1 {
		t.Fatalf("origin 命中次数 = %d, 期望 1", got)
	}
}

func poolSize(r *Router) int {
	n := 0
	r.pool.Range(func(_, _ any) bool { n++; return true })
	return n
}
