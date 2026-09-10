package upstream

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/betterme/remap-service/internal/config"
)

func newClient(firstByte, total time.Duration) (*Client, func()) {
	tr := NewTransport(0)
	// 两个首字节上限都显式给出：非流式那个的语义是「整段生成上限」，
	// 与流式的 TTFT 不是一回事，测试里必须明确它测的是哪一个。
	c := NewClient(tr, config.Upstream{
		FirstByteTimeout:          firstByte,
		FirstByteTimeoutNonStream: firstByte,
		Timeout:                   total,
	})
	return c, tr.CloseIdleConnections
}

func TestFirstByteTimeout(t *testing.T) {
	release := make(chan struct{})
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // 迟迟不返回响应头
		w.WriteHeader(200)
	}))
	defer us.Close()
	defer close(release)

	c, closeTr := newClient(150*time.Millisecond, 10*time.Second)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	start := time.Now()
	_, cancel, _, err := c.Do(context.Background(), req, false)
	cancel()

	if err == nil {
		t.Fatal("首字节超时应返回错误")
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("超时判定耗时 %v，应接近 150ms", el)
	}
}

// 流式请求不应被 first_byte_timeout 之后的长时间传输误杀。
func TestStreamNotKilledAfterHeaders(t *testing.T) {
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		fl.Flush()
		for i := 0; i < 5; i++ {
			time.Sleep(60 * time.Millisecond) // 总计 300ms > firstByteTimeout
			_, _ = io.WriteString(w, "data: tick\n\n")
			fl.Flush()
		}
	}))
	defer us.Close()

	c, closeTr := newClient(100*time.Millisecond, 10*time.Second)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	resp, cancel, _, err := c.Do(context.Background(), req, true)
	if err != nil {
		t.Fatalf("响应头已及时返回，不应报错: %v", err)
	}
	defer cancel()
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("流式读取被中断: %v", err)
	}
	if len(b) < 50 {
		t.Errorf("只读到 %d 字节，流被提前截断", len(b))
	}
}

func TestTotalTimeoutNonStream(t *testing.T) {
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second) // 响应头很快，body 很慢
		_, _ = io.WriteString(w, "late")
	}))
	defer us.Close()

	c, closeTr := newClient(5*time.Second, 200*time.Millisecond)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	resp, cancel, _, err := c.Do(context.Background(), req, false)
	if err != nil {
		t.Fatalf("响应头正常，Do 不应报错: %v", err)
	}
	defer cancel()
	defer resp.Body.Close()

	if _, err := io.ReadAll(resp.Body); err == nil {
		t.Error("非流式请求的 body 读取应被总超时打断")
	}
}

func TestCallerContextCancel(t *testing.T) {
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer us.Close()

	c, closeTr := newClient(5*time.Second, 5*time.Second)
	defer closeTr()

	ctx, cancelCtx := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancelCtx()
	}()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	start := time.Now()
	_, cancel, _, err := c.Do(ctx, req, false)
	cancel()

	if err == nil {
		t.Fatal("调用方取消后应返回错误")
	}
	if !errors.Is(err, context.Canceled) {
		t.Logf("err = %v（只要能及时返回即可）", err)
	}
	if el := time.Since(start); el > time.Second {
		t.Errorf("取消响应耗时 %v，过慢", el)
	}
}

func TestConnectionReuse(t *testing.T) {
	var conns sync.Map
	var count atomic.Int64
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conns.Store(r.RemoteAddr, true)
		count.Add(1)
		w.WriteHeader(200)
	}))
	defer us.Close()

	c, closeTr := newClient(5*time.Second, 5*time.Second)
	defer closeTr()

	var firstTiming, lastTiming Timing
	for i := 0; i < 100; i++ {
		req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
		resp, cancel, timing, err := c.Do(context.Background(), req, false)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body) // 必须读完，否则连接无法复用
		resp.Body.Close()
		cancel()
		if i == 0 {
			firstTiming = timing
		}
		lastTiming = timing
	}

	n := 0
	conns.Range(func(_, _ any) bool { n++; return true })
	if n != 1 {
		t.Errorf("串行 100 次请求应复用 1 条连接，实际用了 %d 条", n)
	}
	if count.Load() != 100 {
		t.Errorf("上游收到 %d 个请求", count.Load())
	}

	// 埋点必须与真实连接行为一致。httptrace 的钩子挂错位置（写成别的请求、
	// 或回调根本没触发）时连接复用本身仍然正常，只有这组断言会失败 ——
	// 而 wrote_request 正是判定超时归属的唯一依据，不能靠"看着像对的"。
	if !firstTiming.GotConn {
		t.Error("首次请求应记录 GotConn")
	}
	if firstTiming.Reused {
		t.Error("首次请求不应标记为复用连接")
	}
	if firstTiming.WroteRequest <= 0 {
		t.Errorf("缺少 wrote_request 时刻，超时归属将无法判定: %+v", firstTiming)
	}
	if firstTiming.FirstByte <= 0 {
		t.Errorf("缺少 first_byte 时刻: %+v", firstTiming)
	}
	if firstTiming.Proto != "http/1.1" {
		t.Errorf("proto = %q，httptest 是明文 HTTP，应归一为 http/1.1", firstTiming.Proto)
	}
	if !lastTiming.Reused {
		t.Error("第 100 次请求必须复用连接")
	}
	if lastTiming.DNS != 0 || lastTiming.Connect != 0 {
		t.Errorf("复用连接时不应再有 DNS/建连耗时: %+v", lastTiming)
	}
}

// 连接池参数由 maxConns 一个数字推导，无需逐项配置。
func TestTransportDerivedFromMaxConns(t *testing.T) {
	tr := NewTransport(256)
	if tr.MaxIdleConnsPerHost != 256 {
		t.Errorf("MaxIdleConnsPerHost = %d, want 256", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns != 1024 {
		t.Errorf("MaxIdleConns = %d，应为 maxConns 的 4 倍", tr.MaxIdleConns)
	}

	def := NewTransport(0)
	if def.MaxIdleConnsPerHost != 1024 {
		t.Errorf("零值应回落到默认 1024，实际 %d", def.MaxIdleConnsPerHost)
	}
	if !def.DisableCompression {
		t.Error("必须关闭自动解压，否则 SSE 无法逐行处理")
	}
	if def.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Error("应强制 TLS 1.2+")
	}
	if !def.ForceAttemptHTTP2 {
		t.Error("应尝试 HTTP/2")
	}
}
