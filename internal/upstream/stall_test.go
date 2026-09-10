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

	"github.com/betterme/remap-service/internal/config"
)

// 空闲超时：响应头已到、流已建立，但上游中途不再吐数据，必须被断开。
//
// 这是首字节与总超时都覆盖不到的形态 —— 看门狗此刻已解除，总超时要等到
// StreamTimeout（默认 30 分钟）才生效，中间这段时间请求占着并发名额、
// 连接和内存，客户端却什么都收不到。
func TestStallReaderBreaksSilentStream(t *testing.T) {
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: first\n\n")
		fl.Flush()
		<-r.Context().Done() // 之后一个字节都不再发
	}))
	defer us.Close()

	c, closeTr := newClient(5*time.Second, 5*time.Second)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	resp, cancel, _, err := c.Do(context.Background(), req, true)
	if err != nil {
		t.Fatalf("响应头已到，Do 不应报错: %v", err)
	}
	defer cancel()
	defer resp.Body.Close()

	sr := NewStallReader(resp.Body, 150*time.Millisecond, cancel)
	if sr == nil {
		t.Fatal("limit > 0 且 cancel 非空时必须启用")
	}
	defer sr.Close()

	if _, rerr := io.ReadAll(sr); rerr == nil {
		t.Fatal("静默的流应被空闲超时打断")
	}

	// Tripped 是网关区分「上游停摆」与「客户端离开」的唯一依据。
	// 它没置位，这次断开就会被记成 client_gone —— 不标红、不计错误率。
	if !sr.Tripped() {
		t.Fatal("Tripped 未置位，网关会把上游停摆误判为客户端离开而静默吞掉")
	}
	te := sr.Err(resp.Request.URL)
	if te == nil {
		t.Fatal("Tripped 之后 Err 必须非 nil")
	}
	if te.Kind != TimeoutKindStall {
		t.Errorf("kind = %q，want %q", te.Kind, TimeoutKindStall)
	}
	if te.Limit != 150*time.Millisecond {
		t.Errorf("limit = %v，应为配置的空闲阈值（改错了会去调错的配置项）", te.Limit)
	}
	if te.URL == "" {
		t.Error("URL 未记录，无法定位是哪个上游")
	}
	if !te.Stream {
		t.Error("空闲超时只可能发生在流式请求上")
	}
	// 必须仍匹配 deadline 哨兵：上层 classifyUpstreamError 靠它给出 504，
	// 破了会静默退化成 502。
	if !errors.Is(te, context.DeadlineExceeded) {
		t.Error("必须仍匹配 context.DeadlineExceeded")
	}
	if errors.Is(te, context.Canceled) {
		t.Error("匹配 Canceled 会被当成客户端离开而静默吞掉")
	}
}

// 持续产出数据的流不能被空闲超时误杀。
//
// 这条用例锁定「判据是间隔而不是总时长」：总时长刻意超过阈值，
// 但每个间隔都远小于它。若实现退化成按总时长判断，这里必然失败。
func TestStallReaderKeepsProducingStream(t *testing.T) {
	const (
		gap       = 100 * time.Millisecond
		ticks     = 8
		threshold = 500 * time.Millisecond // 总时长 800ms > 阈值
	)
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		for i := 0; i < ticks; i++ {
			time.Sleep(gap)
			_, _ = io.WriteString(w, "data: tick\n\n")
			fl.Flush()
		}
	}))
	defer us.Close()

	c, closeTr := newClient(5*time.Second, 5*time.Second)
	defer closeTr()

	req, _ := http.NewRequest(http.MethodGet, us.URL, nil)
	resp, cancel, _, err := c.Do(context.Background(), req, true)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	defer resp.Body.Close()

	sr := NewStallReader(resp.Body, threshold, cancel)
	defer sr.Close()

	b, err := io.ReadAll(sr)
	if err != nil {
		t.Fatalf("持续产出的流被误杀（判据退化成了总时长？）: %v", err)
	}
	if sr.Tripped() {
		t.Error("正常收尾的流不应标记为空闲超时")
	}
	if n := strings.Count(string(b), "tick"); n != ticks {
		t.Errorf("读到 %d 个事件，应为 %d 个", n, ticks)
	}
}

// 关闭与边界：limit <= 0 表示关闭；没有 cancel 就无法真正断开连接。
func TestStallReaderDisabledAndNilSafe(t *testing.T) {
	rc := io.NopCloser(strings.NewReader("x"))
	if sr := NewStallReader(rc, 0, func() {}); sr != nil {
		t.Error("limit <= 0 应表示关闭")
	}
	if sr := NewStallReader(rc, -time.Second, func() {}); sr != nil {
		t.Error("负值应表示关闭")
	}
	if sr := NewStallReader(rc, time.Second, nil); sr != nil {
		t.Error("无 cancel 时断开不了连接，应拒绝启用而不是假装启用")
	}
	if sr := NewStallReader(nil, time.Second, func() {}); sr != nil {
		t.Error("nil 响应体应拒绝启用")
	}

	// nil 接收者必须安全：网关在关闭状态下照样会调这两个方法。
	var nilSr *StallReader
	if nilSr.Tripped() {
		t.Error("nil 的 Tripped 应为 false")
	}
	if nilSr.Err(nil) != nil {
		t.Error("nil 的 Err 应为 nil")
	}
}

// 两个首字节上限各管各的，总时长也必须随请求类型分开。
//
// 混用的后果是二选一地错：把流式的 TTFT 阈值套到非流式上会误杀正常的长生成；
// 反过来，非流式的量级（几分钟）用在流式上则异常永远检测不出来。
func TestLimitsSplitByStream(t *testing.T) {
	tr := NewTransport(0)
	defer tr.CloseIdleConnections()
	c := NewClient(tr, config.Upstream{
		FirstByteTimeout:          30 * time.Second,
		FirstByteTimeoutNonStream: 5 * time.Minute,
		Timeout:                   time.Minute,
		StreamTimeout:             10 * time.Minute,
	})

	streamFB, streamTotal := c.limitsFor(true)
	if streamFB != 30*time.Second {
		t.Errorf("流式首字节上限 = %v，应取 UPSTREAM_FIRST_BYTE_TIMEOUT", streamFB)
	}
	if streamTotal != 10*time.Minute {
		t.Errorf("流式总超时 = %v，应取 UPSTREAM_STREAM_TIMEOUT 而非硬编码的 30 分钟", streamTotal)
	}
	syncFB, syncTotal := c.limitsFor(false)
	if syncFB != 5*time.Minute {
		t.Errorf("非流式首字节上限 = %v，应取 UPSTREAM_FIRST_BYTE_TIMEOUT_NONSTREAM", syncFB)
	}
	if syncTotal != time.Minute {
		t.Errorf("非流式总超时 = %v，应取 UPSTREAM_TIMEOUT", syncTotal)
	}
}

// protoLabel 必须把两处来源归一：响应里的 "HTTP/2.0" 与 ALPN 的 "h2"
// 指的是同一件事，不归一会让看板按它分组得到两行。
func TestProtoLabelNormalizes(t *testing.T) {
	cases := []struct{ respProto, alpn, want string }{
		{"HTTP/2.0", "", "h2"},
		{"", "h2", "h2"},
		{"HTTP/1.1", "", "http/1.1"},
		{"", "http/1.1", "http/1.1"},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := protoLabel(c.respProto, c.alpn); got != c.want {
			t.Errorf("protoLabel(%q, %q) = %q, want %q", c.respProto, c.alpn, got, c.want)
		}
	}
}
