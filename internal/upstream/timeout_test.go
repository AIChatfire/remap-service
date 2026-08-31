package upstream

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// 替换哨兵值的前提：errors.Is(err, context.DeadlineExceeded) 必须继续成立。
// 上层 classifyUpstreamError 靠它给出 504，破了会静默退化成 502。
func TestTimeoutErrorKeepsSentinelSemantics(t *testing.T) {
	err := error(&TimeoutError{Kind: TimeoutKindFirstByte, Limit: time.Second})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("必须仍匹配 context.DeadlineExceeded，否则上层分类退化为 502")
	}
	if errors.Is(err, context.Canceled) {
		t.Error("不应匹配 Canceled：那会被当成客户端离开而静默吞掉")
	}
}

// 错误串必须自带排查所需的全部信息，这是本次修复的核心诉求。
func TestTimeoutErrorMessageCarriesContext(t *testing.T) {
	e := &TimeoutError{
		Kind:    TimeoutKindFirstByte,
		Limit:   30 * time.Second,
		Elapsed: 30123 * time.Millisecond,
		URL:     "https://api.example.com/v1/chat/completions",
		Stream:  true,
	}
	msg := e.Error()
	for _, want := range []string{"first_byte", "30s", "30.123s", "api.example.com", "[stream]"} {
		if !strings.Contains(msg, want) {
			t.Errorf("错误串缺少 %q: %s", want, msg)
		}
	}
	if msg == context.DeadlineExceeded.Error() {
		t.Error("与裸哨兵值等价，等于没修")
	}
}

// query 与 userinfo 不能进上报：凭据当前只走 header，但日后有人塞进 query
// 时不应静默泄漏到看板。
func TestSafeURLStripsSecrets(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet,
		"https://user:pass@api.example.com/v1/chat?api_key=SECRET123&x=1#frag", nil)
	if err != nil {
		t.Fatal(err)
	}
	got := safeURL(req.URL)
	for _, bad := range []string{"SECRET123", "pass", "api_key", "frag"} {
		if strings.Contains(got, bad) {
			t.Errorf("safeURL 泄漏 %q: %s", bad, got)
		}
	}
	if want := "https://api.example.com/v1/chat"; got != want {
		t.Errorf("safeURL = %q, want %q", got, want)
	}
}

// 端到端：首字节超时必须产出 first_byte 类型且带真实阈值。
func TestFirstByteTimeoutIsDescriptive(t *testing.T) {
	release := make(chan struct{})
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(200)
	}))
	defer us.Close()
	defer close(release)

	c, closeTr := newClient(120*time.Millisecond, 10*time.Second)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	_, cancel, err := c.Do(context.Background(), req, false)
	cancel()

	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("应返回 *TimeoutError，实际 %T: %v", err, err)
	}
	if te.Kind != TimeoutKindFirstByte {
		t.Errorf("kind = %q, want %q", te.Kind, TimeoutKindFirstByte)
	}
	if te.Limit != 120*time.Millisecond {
		t.Errorf("limit = %v，应为配置的 first_byte_timeout", te.Limit)
	}
	if te.Elapsed <= 0 {
		t.Error("elapsed 未记录，无法判断实际等了多久")
	}
	if !strings.Contains(te.URL, "127.0.0.1") {
		t.Errorf("URL 未记录: %q", te.URL)
	}
	if a := te.Attrs(); a["gateway.timeout.kind"] != TimeoutKindFirstByte {
		t.Errorf("Attrs 缺 kind: %v", a)
	}
}

// 总超时：响应头已到、正文读取中途超时，应归为 total 而非 first_byte。
// 两者处置方向不同（调 UPSTREAM_TIMEOUT vs UPSTREAM_FIRST_BYTE_TIMEOUT），
// 混淆会把排查带向错误的配置项。
func TestTotalTimeoutKind(t *testing.T) {
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		time.Sleep(2 * time.Second)
	}))
	defer us.Close()

	// 首字节宽松、总超时极短：确保只可能命中总超时。
	c, closeTr := newClient(5*time.Second, 200*time.Millisecond)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	resp, cancel, err := c.Do(context.Background(), req, false)
	if err != nil {
		t.Fatalf("响应头及时返回，Do 不应报错: %v", err)
	}
	defer cancel()
	_, rerr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if rerr == nil {
		t.Fatal("正文读取应因总超时失败")
	}
	// 读正文的错误由 net/http 产生，不经过 Do，这里只确认它仍被
	// 识别为 deadline —— 网关侧据此归类为 504。
	if !errors.Is(rerr, context.DeadlineExceeded) {
		t.Errorf("正文读取错误未被识别为 deadline: %v", rerr)
	}
}

// 父 ctx 自带 deadline 时，不应上报一个与现场不符的网关阈值。
func TestParentDeadlineReportsActualElapsed(t *testing.T) {
	release := make(chan struct{})
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(200)
	}))
	defer us.Close()
	defer close(release)

	// 首字节与总超时都远大于父 ctx 的 deadline。
	c, closeTr := newClient(10*time.Second, 20*time.Second)
	defer closeTr()

	ctx, ccl := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer ccl()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	_, cancel, err := c.Do(ctx, req, false)
	cancel()

	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("应返回 *TimeoutError，实际 %T: %v", err, err)
	}
	if te.Kind != TimeoutKindTotal {
		t.Errorf("kind = %q，父 ctx 到期应归 total", te.Kind)
	}
	if te.Limit >= 20*time.Second {
		t.Errorf("limit = %v，上报网关阈值会把排查引向错误的配置项", te.Limit)
	}
}
