package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/obs"
)

// 流建立后上游停摆：必须由空闲超时断开，且**不能被当成客户端离开**。
//
// 这是整组埋点里最容易错、后果最严重的一条。空闲超时通过 cancel 断开连接，
// 读操作返回的是 context.Canceled，而网关把 context.Canceled 一律视为
// 「客户端提前断开」—— 那条规则不标红 span、不计错误率。
//
// 判定顺序写反的后果不是「少一条信息」，而是：最该告警的形态被静默吞掉，
// span 保持 Unset，看板上连一条可筛的记录都不存在。
func TestStreamStallIsNotMistakenForClientGone(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"model\":\"up\"}\n\n")
		fl.Flush()
		<-r.Context().Done() // 之后一个字节都不再发
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"up"}}
		c.Upstream.StallTimeout = 150 * time.Millisecond
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub","stream":true}`, nil)
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body) // 一直读到网关断开为止

	sp := findSpanWithAttr(t, sr, "gateway.error.kind")
	if got := attrString(sp, "gateway.error.kind"); got != "stream_stall" {
		t.Fatalf("空闲超时应归类为 stream_stall，实际 %q —— 被当成客户端离开会静默丢失告警", got)
	}
	if hasAttr(sp, "gateway.client_gone") {
		t.Error("空闲超时不得标记 gateway.client_gone")
	}
	// 注意这两个键是**字符串**属性：它们由 structuredAttrs 从
	// TimeoutError.Attrs() 展开，走的是 AttrProvider 的 map[string]string。
	if got := attrString(sp, "gateway.timeout.limit_ms"); got != "150" {
		t.Errorf("应上报配置的空闲阈值 150ms，实际 %q", got)
	}
	if got := attrString(sp, "gateway.timeout.kind"); got != "stall" {
		t.Errorf("timeout.kind 应为 stall，实际 %q", got)
	}
	if got := attrString(sp, "gateway.timeout.url"); got == "" {
		t.Error("超时错误应带上游端点，否则不知道该找谁")
	}
	// 它确实是故障，必须标红 —— 与「用户点了停止」不是一回事。
	if sp.Status().Code != codes.Error {
		t.Errorf("空闲超时必须标红，实际状态 %v", sp.Status().Code)
	}
}

// 失败请求的 span 也必须带最终状态码与体积。
//
// 这些字段此前只写在成功路径上：于是「客户端到底收到 504 还是 502」只能从
// gateway.error.kind 反推，请求/响应字节数则完全不可见 —— 而首字节延迟与
// prefill 长度强相关，没有体积这个维度就无法区分「长上下文拖慢了上游」
// 与「上游在排队」。
func TestTailAttrsPresentOnErrorPath(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":"hi"}]}`, nil)
	defer resp.Body.Close()

	sp := findSpanWithAttr(t, sr, "http.response.status_code")
	if got := attrInt(sp, "http.response.status_code"); got != http.StatusInternalServerError {
		t.Errorf("失败请求也要记录最终状态码，实际 %d", got)
	}
	if !hasAttr(sp, obs.AttrRequestBytes) {
		t.Error("缺少请求体字节数")
	}
	if got := attrInt(sp, obs.AttrRequestBytes); got <= 0 {
		t.Errorf("请求体字节数应大于 0，实际 %d", got)
	}
	if !hasAttr(sp, obs.AttrResponseBytes) {
		t.Error("缺少响应字节数")
	}
	if !hasAttr(sp, obs.AttrInflightLimit) {
		t.Error("缺少在途水位：无法判断失败时网关是否已接近饱和")
	}
	// 没有失败尝试时也必须存在（值为 0）。属性缺失与「没失败过」在看板上
	// 完全同形，读的人只能猜是没失败还是没实现。
	if !hasAttr(sp, obs.AttrAttemptCount) {
		t.Error("attempt.count 必须恒写，即使本次请求没有失败尝试")
	}
	if got := attrInt(sp, obs.AttrAttemptCount); got != 0 {
		t.Errorf("本用例没有失败尝试，count 应为 0，实际 %d", got)
	}
}

// 客户端标识、凭据指纹与上游分段耗时都必须落到 span 上。
//
// 三者共同回答「失败发生时现场是什么样」：
//   - 客户端 ID 是跨系统串联的唯一把手（超时拿不到上游 logid）；
//   - 凭据指纹用于按渠道聚合（上游限流按账号/渠道计）；
//   - wrote_request 用于判定时间花在本地排队还是等上游。
func TestRequestIDCredentialAndTimingAttrs(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"pub","choices":[]}`))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, map[string]string{
		"X-Request-Id":  "req-abc-123",
		"Authorization": "Bearer sk-secret-value",
	})
	defer resp.Body.Close()

	sp := findSpanWithAttr(t, sr, obs.AttrRequestID)
	if got := attrString(sp, obs.AttrRequestID); got != "req-abc-123" {
		t.Errorf("入站 request id 未上报，实际 %q", got)
	}
	fp := attrString(sp, obs.AttrCredentialFingerprint)
	if fp == "" {
		t.Fatal("缺少凭据指纹：无法按渠道聚合上游错误")
	}
	// 指纹必须是短哈希，绝不能含凭据明文或其可读前缀。
	if len(fp) != 8 || strings.Contains(fp, "secret") || strings.Contains(fp, "sk-") {
		t.Errorf("指纹应为 8 位十六进制哈希，实际 %q", fp)
	}

	// 分段耗时：缺失时无法区分「请求卡在本地」与「上游不回」。
	if !hasAttr(sp, obs.AttrUpstreamWroteRequest) {
		t.Error("缺少 wrote_request 时刻，超时归属无法判定")
	}
	if !hasAttr(sp, obs.AttrUpstreamProto) {
		t.Error("缺少上游协议版本，无法验证 h2 流排队这一假设")
	}
	if got := attrString(sp, obs.AttrUpstreamProto); got != "http/1.1" {
		t.Errorf("proto = %q，fixture 上游是明文 HTTP", got)
	}
	if !hasAttr(sp, obs.AttrUpstreamConnReused) {
		t.Error("缺少连接复用标记")
	}
}

// 主备都连不上时，**首次失败**必须留痕。
//
// 此前只有「重试成功」才补记首次失败，于是主备双挂的场景下真正先挂的主模型
// 没有任何独立记录 —— 排查时会一路去查备用模型，方向完全错。
func TestTransportFailoverRecordsFirstFailure(t *testing.T) {
	// 起一个监听再立刻关掉，得到一个必然拒连的地址。
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // 不会被调用：上游地址已被改写成死端口
	}, func(c *config.Config) {
		c.Upstream.Base = deadURL
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = true
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()

	sp := findSpanWithAttr(t, sr, "gateway.attempt.count")
	if got := attrInt(sp, "gateway.attempt.count"); got != 2 {
		t.Errorf("主备两次传输失败都应留痕，实际 count=%d —— 首次被吞掉就无法定位是谁挂了", got)
	}
	if got := attrString(sp, "gateway.failover.outcome"); got != "aborted" {
		t.Errorf("重试请求没发出去时 outcome 应为 aborted，实际 %q", got)
	}
	if got := attrString(sp, "gateway.failover.from_model"); got != "primary-up" {
		t.Errorf("from_model 应为切换前的 primary-up，实际 %q", got)
	}
	// 传输层失败没有上游状态码，写 0 会污染聚合。
	if hasAttr(sp, "gateway.failover.first_status_code") {
		t.Error("传输层失败无状态码，不应写 first_status_code")
	}
}
