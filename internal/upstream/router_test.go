package upstream

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/betterme/remap-service/internal/config"
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
	return NewRouter(def, 0, config.Transport{}, "", 0, nil, nil), def
}

// newPoolRouter 造一个带池上限与淘汰回调的 Router（容量类测试用）。
func newPoolRouter(limit int, onEvict ProxyEvictReporter) *Router {
	def := &countingRT{resp: &http.Response{StatusCode: 204, Body: http.NoBody}}
	return NewRouter(def, 0, config.Transport{}, "", limit, nil, onEvict)
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

	r := NewRouter(&countingRT{resp: &http.Response{StatusCode: 204, Body: http.NoBody}}, 0, config.Transport{}, "", 0, nil, nil)
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
		0, config.Transport{}, "127.0.0.1", 0, nil, nil)
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

// 池满时必须淘汰最久未使用的那个，最近用过的要留下。
//
// 这是近似 LRU 的核心契约：淘汰 lastUsed 最小的条目。
func TestRouterEvictsLeastRecentlyUsed(t *testing.T) {
	var evicted []int
	r := newPoolRouter(2, func(size, limit int) { evicted = append(evicted, size) })

	a, err := r.transportFor("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	// 时间戳精度虽是纳秒，但同一毫秒内的连续调用仍可能取到相同值，
	// 这里显式拉开间隔，让「最久未用」是确定的。
	time.Sleep(2 * time.Millisecond)
	if _, err := r.transportFor("http://127.0.0.1:2"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	// 重新使用 a，使它成为最近使用过的那个
	if _, err := r.transportFor("http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)

	// 池已满，第三个代理应挤掉最久未用的 b
	if _, err := r.transportFor("http://127.0.0.1:3"); err != nil {
		t.Fatal(err)
	}

	if n := poolSize(r); n != 2 {
		t.Fatalf("池大小 = %d，应为上限 2", n)
	}
	if len(evicted) != 1 {
		t.Fatalf("淘汰回调被调用 %d 次，应为 1", len(evicted))
	}
	again, err := r.transportFor("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if again != a {
		t.Error("最近使用过的代理被淘汰了，淘汰策略不是 LRU")
	}
}

// 并发灌入远超上限的代理时，池大小不得越过上限，且必须触发淘汰。
func TestRouterPoolRespectsLimitUnderConcurrency(t *testing.T) {
	const limit = 8
	var evictions atomic.Int64
	r := newPoolRouter(limit, func(int, int) { evictions.Add(1) })

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := r.transportFor(fmt.Sprintf("http://127.0.0.1:%d", i+1)); err != nil {
				t.Errorf("代理 %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if got := poolSize(r); got > limit {
		t.Errorf("池大小 %d 越过上限 %d", got, limit)
	}
	if evictions.Load() == 0 {
		t.Error("建了 200 个代理却没有触发任何淘汰")
	}
}

// 淘汰必须同时关掉该 transport 的空闲连接。
//
// 不关的话，淘汰只是把「transport 对象常驻」换成「连接 + fd 常驻」——
// 泄漏换了个形态，问题并没有解决。
func TestRouterEvictionClosesIdleConnections(t *testing.T) {
	var closed atomic.Int64
	// 把测试服务器当作「客户端指定的代理」：请求经由它发出，
	// 请求结束后该 transport 上就会留下一条空闲连接。
	px := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(204)
	}))
	px.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateClosed {
			closed.Add(1)
		}
	}
	px.Start()
	defer px.Close()

	r := newPoolRouter(1, nil)
	defer r.CloseIdleConnections()

	rt, err := r.transportFor(px.URL)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, "http://upstream.invalid/v1/models", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("经代理请求失败: %v", err)
	}
	resp.Body.Close()

	// 再换一个代理把池挤满 → 上面那个 transport 被淘汰，空闲连接应被关闭。
	if _, err := r.transportFor("http://127.0.0.1:9"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for closed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if closed.Load() == 0 {
		t.Error("淘汰未关闭该 transport 的空闲连接")
	}
}

func poolSize(r *Router) int {
	n := 0
	r.pool.Range(func(_, _ any) bool { n++; return true })
	return n
}
