package gateway

import (
	"net/http"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/betterme/remap-service/internal/config"
)

// 故障切换成功时的双重要求：
//   - span 不能标红（请求最终成功了，标红会在看板上制造假故障）
//   - 但被切掉的那次失败必须留痕，否则「某个上游模型静默挂掉、
//     兜底一直在顶着」这件事完全不可见
func TestFailoverRecordsAttemptButKeepsSpanOK(t *testing.T) {
	var n int32
	sr := tracetest.NewSpanRecorder()
	// 必须有精确映射：若对外名只能落到 Fallback，首次请求就已经在
	// 兜底模型上，无从再切换（这一步配错会让测试假失败）。
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		// 首次 429（可切换状态），重试放过。
		if atomic.AddInt32(&n, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"message":"quota exceeded"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"backup-up","choices":[]}`))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = true
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("切换后应成功，实际 %d", resp.StatusCode)
	}
	if got := atomic.LoadInt32(&n); got != 2 {
		t.Fatalf("上游应被调用 2 次（首次 + 重试），实际 %d", got)
	}

	ev := findEvent(t, sr, "gateway.attempt_failed")
	if got := eventInt(ev, "gateway.attempt.status_code"); got != http.StatusTooManyRequests {
		t.Errorf("事件应记录被切掉的状态码 429，实际 %d", got)
	}
	if got := eventString(ev, "gateway.attempt.stage"); got != "status" {
		t.Errorf("stage 应为 status，实际 %q", got)
	}

	// 关键断言：整个 span 不得为 Error。
	for _, s := range sr.Ended() {
		if s.Status().Code == codes.Error {
			t.Errorf("切换成功的请求不应有 error span（%s: %s）",
				s.Name(), s.Status().Description)
		}
	}
}

// 重试本身失败时（这里让兜底模型改写失败）不能静默：
// 调用方会回落到首次错误，若不留痕就无法回答「切换为什么没生效」。
func TestFailoverAttemptFailureIsRecorded(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	// 上游直接拒连由 transport 分支覆盖；这里用一个必然 5xx 的上游，
	// 让首次与重试都失败，重试的 transport/status 失败同样要留痕。
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"message":"model offline"}}`))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = true
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("两次都失败应透传 503，实际 %d", resp.StatusCode)
	}

	// 首次 503 被切掉 → 留一条 attempt 事件；重试仍 503 → 走正常
	// 上游错误上报，正文进 span。
	ev := findEvent(t, sr, "gateway.attempt_failed")
	if got := eventString(ev, "gateway.attempt.stage"); got != "status" {
		t.Errorf("stage 应为 status，实际 %q", got)
	}
	span := findSpanWithAttr(t, sr, "gateway.error.body")
	if got := attrString(span, "gateway.error.body"); got == "" {
		t.Error("最终失败的正文仍应上报")
	}
}

func findEvent(t *testing.T, sr *tracetest.SpanRecorder, name string) sdktrace.Event {
	t.Helper()
	for _, s := range sr.Ended() {
		for _, e := range s.Events() {
			if e.Name == name {
				return e
			}
		}
	}
	var seen []string
	for _, s := range sr.Ended() {
		for _, e := range s.Events() {
			seen = append(seen, e.Name)
		}
	}
	t.Fatalf("没有 span 带事件 %s（实际事件: %v）—— 记录函数可能未被调用", name, seen)
	return sdktrace.Event{}
}

func eventString(e sdktrace.Event, key string) string {
	for _, kv := range e.Attributes {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

func eventInt(e sdktrace.Event, key string) int {
	for _, kv := range e.Attributes {
		if string(kv.Key) == key {
			return int(kv.Value.AsInt64())
		}
	}
	return 0
}
