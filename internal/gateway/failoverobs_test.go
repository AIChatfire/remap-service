package gateway

import (
	"net/http"
	"strings"
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

	// 关键修复：gateway.upstream.status_code 应记录首次失败的 429，而非切换后的 200。
	// 这样即使 failover 成功，看板仍能直接筛选出所有被 429 顶掉的请求。
	if got := attrInt(sp, "gateway.upstream.status_code"); got != http.StatusTooManyRequests {
		t.Errorf("gateway.upstream.status_code 应为首次失败的 429，实际 %d", got)
	}

	// 新增校验：gateway.upstream.path 应在 span 属性中可见
	if path := attrString(sp, "gateway.upstream.path"); path == "" {
		t.Error("gateway.upstream.path 不应为空，必须在 span 属性中")
	}

	// 列表文案必须让「上游 429 被救成 200」与普通成功不同形：默认渲染只有
	// span name + 客户端最终状态码，两者完全同形。
	msg := attrString(sp, "logfire.msg")
	if msg == "" {
		t.Fatal("请求 span 缺少 logfire.msg，Logfire 列表行仍只显示最终 200")
	}
	// 逐字锁定文案契约。上游路径**必须**在文案里：同一入站路径会按模型映射
	// 打到不同上游端点（/api/v3/... 与 /compatible-mode/v1/...），只进 span
	// 属性只能筛不能扫，列表上逐行可见才能看出流量实际去向。
	// 末尾的 "→ 200" 由 Logfire 自动追加 http.response.status_code，不由
	// 本函数写出，所以这里的期望串不含它。
	// 上游路径取自 span 属性而非写死 /api/v3/...：fixture 的上游是本地
	// httptest，路径与入站同名。写死真实上游路径会把测试绑到线上配置上。
	upPath := attrString(sp, "gateway.upstream.path")
	wantMsg := "gateway /v1/chat/completions → 200 (upstream " + upPath + " → 429)"
	if msg != wantMsg {
		t.Errorf("logfire.msg 文案不符\n got: %q\nwant: %q", msg, wantMsg)
	}

	// 等级抬到 warn(13) —— 这才是「看板上找不到 429」的真正成因。
	//
	// span 没有原生 severity，Logfire 只认「状态 ERROR → error，否则 info」。
	// failover 成功的 span 状态是 OK（不能标红，那是假故障），于是它与普通
	// 成功同为 info 级，没有任何维度能把它从成功流量里筛出来。
	if got := attrInt(sp, "logfire.level_num"); got != 13 {
		t.Errorf("请求 span 等级应为 warn(13)，实际 %d", got)
	}

	// 事件同理靠等级可筛。
	if got := eventInt(ev, "logfire.level_num"); got != 13 {
		t.Errorf("attempt 事件等级应为 warn(13)，实际 %d", got)
	}
	// 反向锁定：logfire.msg 对 span event 不渲染，写了是纯浪费且「写了没
	// 生效」与「没写」在 UI 上同形，不许再加回来。
	if got := eventString(ev, "logfire.msg"); got != "" {
		t.Errorf("事件不应写 logfire.msg（Logfire 不渲染事件文案），实际 %q", got)
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

// 上游 4xx 直通（401/403/429 未配 failover）时的列表文案与正文上报。
//
// 这类请求不发生切换，客户端最终状态码就等于上游状态码。它是看板上最高频的
// 一类失败，必须满足两点：
//   - 文案里要同时有上游端点与上游状态码（用户明确要求的形态）
//   - 上报的错误正文是**上游原文**，不是发给客户端的脱敏版
func TestUpstreamErrorPassthroughMsgAndRawBody(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		// 正文里带上游真实模型名：脱敏会把它换成对外名，据此即可判定
		// 上报的是原文还是脱敏版。
		_, _ = w.Write([]byte(`{"error":{"message":"no access to primary-up","type":"invalid_api_key"}}`))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("上游 401 应原样透传，实际 %d", resp.StatusCode)
	}

	sp := findSpanWithAttr(t, sr, "logfire.msg")
	msg := attrString(sp, "logfire.msg")
	if !strings.Contains(msg, "(upstream /") {
		t.Errorf("文案必须含上游端点，实际 %q", msg)
	}
	if !strings.Contains(msg, "→ 401)") {
		t.Errorf("上游失败时括号内必须带上游状态码 401，实际 %q", msg)
	}
	// 客户端自己就是 401，span 已 SetStatus(Error) → Logfire 渲染为 error 级。
	// 此时再写 warn(13) 是把 error 降级，比不写更糟。
	if got := attrInt(sp, "logfire.level_num"); got != 0 {
		t.Errorf("客户端已是 4xx 时不应写 warn 等级（会降级 error），实际 %d", got)
	}

	// 正文必须是上游原文：脱敏版里 primary-up 已被替换成 pub，
	// 这条断言同时锁住「客户端拿脱敏版、看板拿原文」的分工。
	body := attrString(findSpanWithAttr(t, sr, "gateway.error.body"), "gateway.error.body")
	if !strings.Contains(body, "primary-up") {
		t.Errorf("上报给看板的正文应为上游原文（含真实模型名），实际 %q", body)
	}
	// body_sanitized 标记已随「恒传原文」移除：恒 false 的属性没有信息量。
	if attrString(sp, "gateway.error.body_sanitized") != "" {
		t.Error("body_sanitized 应已移除")
	}
}
