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

	// 事件之外，429 还必须作为 span 属性存在。
	//
	// 只进事件是不够的：Logfire 的 trace 列表、Full Trace 视图和属性筛选
	// 都只读 span 属性，切换成功后 http.response.status_code 恒为 200，
	// 于是「今天有多少请求被 429 顶掉」必须逐条展开事件才能回答。
	sp := findSpanWithAttr(t, sr, "gateway.failover.first_status_code")
	if got := attrInt(sp, "gateway.failover.first_status_code"); got != http.StatusTooManyRequests {
		t.Errorf("span 属性应记录首次失败的 429，实际 %d", got)
	}
	if got := attrString(sp, "gateway.failover.stage"); got != "status" {
		t.Errorf("failover.stage 应为 status，实际 %q", got)
	}
	// from/to 必须真的分别是切换前后的模型：若两者相同，说明取值时机
	// 错在 applyPlan 之后，属性看着有值实则无法定位是谁挂了。
	from := attrString(sp, "gateway.failover.from_model")
	to := attrString(sp, "gateway.failover.to_model")
	if from != "primary-up" {
		t.Errorf("from_model 应为切换前的 primary-up，实际 %q", from)
	}
	if to != "backup-up" {
		t.Errorf("to_model 应为切换后的 backup-up，实际 %q", to)
	}
	// span 的最终状态码仍应是客户端实际收到的 200，两者不能混淆。
	if got := attrInt(sp, "http.response.status_code"); got != http.StatusOK {
		t.Errorf("最终状态码应保持 200，实际 %d", got)
	}

	// 新增校验：gateway.upstream.status_code 应与最终状态码一致（切换成功后为 200）
	if got := attrInt(sp, "gateway.upstream.status_code"); got != http.StatusOK {
		t.Errorf("gateway.upstream.status_code 应为最终的 200，实际 %d", got)
	}

	// 新增校验：gateway.upstream.path 应在 span 属性中可见
	if path := attrString(sp, "gateway.upstream.path"); path == "" {
		t.Error("gateway.upstream.path 不应为空，必须在 span 属性中")
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

	// 主备同时不可用必须在 span 属性上与「正常降级」区分开。
	//
	// 这里切换动作是成功的（请求确实发到了 backup-up），但备用模型同样
	// 返回 503，请求并没有被救回。若仍记 succeeded，看板上这条与真正
	// 救回的请求完全同形，主备双挂就无法告警。
	sp := findSpanWithAttr(t, sr, "gateway.failover.outcome")
	if got := attrString(sp, "gateway.failover.outcome"); got != "exhausted" {
		t.Errorf("备用模型也失败时 outcome 应为 exhausted，实际 %q", got)
	}
	if got := attrInt(sp, "gateway.failover.first_status_code"); got != http.StatusServiceUnavailable {
		t.Errorf("应记录首次失败的 503，实际 %d", got)
	}
	if got := attrString(sp, "gateway.failover.from_model"); got != "primary-up" {
		t.Errorf("from_model 应为 primary-up，实际 %q", got)
	}
	// 请求确实发到了备用模型，to_model 应有值 —— exhausted 不等于
	// 「没切过去」，这一条区分了 exhausted 与 aborted。
	if got := attrString(sp, "gateway.failover.to_model"); got != "backup-up" {
		t.Errorf("to_model 应为 backup-up，实际 %q", got)
	}

	// span 上的 url.path 是入站路径，必须保持客户端实际打的那个。
	if got := attrString(sp, "url.path"); got != "/v1/chat/completions" {
		t.Errorf("span 的 url.path 应为入站路径，实际 %q", got)
	}

	// 上游路径只能落在独立键上。断言分两条且都必要：
	// 只断言新键存在时，事件里同时写了 url.path 也能通过 —— 而那正是
	// 之前的 bug 形态（上游路径混进入站语义的键，两种路径无法区分）。
	ev2 := findEvent(t, sr, "gateway.attempt_failed")
	if got := eventString(ev2, "gateway.upstream.path"); got == "" {
		t.Error("事件缺少 gateway.upstream.path，无法定位上游端点")
	}
	if got := eventString(ev2, "url.path"); got != "" {
		t.Errorf("事件不得写 url.path（那是入站语义），实际 %q", got)
	}
}

// 切换成功时 outcome 必须是 succeeded，与 exhausted 形成可聚合的二分。
func TestFailoverOutcomeSucceeded(t *testing.T) {
	var n int32
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
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

	sp := findSpanWithAttr(t, sr, "gateway.failover.outcome")
	if got := attrString(sp, "gateway.failover.outcome"); got != "succeeded" {
		t.Errorf("切换成功时 outcome 应为 succeeded，实际 %q", got)
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
