package gateway

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/trace"

	"github.com/betterme/remap-service/internal/capability"
	"github.com/betterme/remap-service/internal/mapping"
	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/protocol"
)

// failoverStatuses 是值得换模型重试的上游状态码。
//
// 只收录「换一个模型有机会成功」的情形：
//
//	429 配额或速率受限 —— 换模型通常换到不同的配额池
//	500/502/503/504    —— 上游该模型的服务端故障
//
// 刻意排除 4xx 中的其余状态：400/401/403/404 是请求本身的问题
// （参数错误、凭据无效、模型不存在），换模型重试只会再失败一次，
// 白白让客户端多等一个 RTT。
func isFailoverStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// isCapabilityFailoverStatus 是能力切换专用的、放宽后的重试状态集。
//
// 与通用兜底的关键差异：这里额外收录 4xx。因为「模型不支持某能力」
// 的上游报错恰恰落在 400/415/422 —— 火山返回 400 InvalidParameter、
// OpenAI 返回 400 invalid_request_error、Anthropic 返回 400 与
// "does not support image" 之类的说明。通用兜底排除 4xx 是对的
// （换个模型同样会参数错误），但能力切换换的是「确实具备该能力的模型」，
// 重试有实质成功率。
//
// 仍然排除 401/403：凭据无效换任何模型都过不去。
func isCapabilityFailoverStatus(code int) bool {
	switch code {
	case http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType,
		http.StatusUnprocessableEntity:
		return true
	default:
		return isFailoverStatus(code)
	}
}

// failoverPlan 描述一次可能的故障切换。
type failoverPlan struct {
	// model 是切换到的上游模型。
	model string
	// cap 非 None 时表示这是一次「能力切换」，触发条件更宽。
	cap capability.Kind
	// enabled 为 false 时不做任何切换。
	enabled bool
}

// shouldRetry 判断某个上游状态码是否触发本计划。
func (p failoverPlan) shouldRetry(code int) bool {
	if !p.enabled {
		return false
	}
	if p.cap != capability.None {
		return isCapabilityFailoverStatus(code)
	}
	return isFailoverStatus(code)
}

// planFailover 判断本次请求是否具备切换条件。
//
// 两类切换按优先级排列：
//
//	① 能力切换 —— 请求确实用到了某项能力，且该能力声明了专用模型。
//	   这是精准的：知道缺什么，就换成具备什么的模型。
//	② 通用兜底 —— MODEL_MAP_FALLBACK，不区分失败原因。
//
// 能力切换优先，因为它的成功率明显更高；只有在没有可用的能力目标时
// 才退回到通用兜底。
func (g *Gateway) planFailover(r *http.Request, st *state) failoverPlan {
	if p, ok := g.planCapabilityFailover(r, st); ok {
		return p
	}
	return g.planFallback(st)
}

// planCapabilityFailover 挑选能力切换目标。
//
// 请求可能同时用到多项能力（带图 + 带工具）。按 capability.Order()
// 的优先级取第一个「用到了 且 声明了专用模型 且 与当前上游模型不同」的，
// 多模态输入排在工具调用之前 —— 前者是硬约束，模型不支持就根本无法处理。
func (g *Gateway) planCapabilityFailover(r *http.Request, st *state) (failoverPlan, bool) {
	if st.caps.Empty() {
		return failoverPlan{}, false
	}
	// 已经是能力切换命中的请求不再切，避免来回横跳。
	if st.capUsed != capability.None {
		return failoverPlan{}, false
	}
	cm := g.resolveCapMap(r)
	for _, k := range capability.Order() {
		if !st.caps.Has(k) {
			continue
		}
		m, ok := cm.Lookup(k)
		if !ok || m == st.upstreamModel {
			continue
		}
		return failoverPlan{model: m, cap: k, enabled: true}, true
	}
	return failoverPlan{}, false
}

// planFallback 是原有的通用兜底切换。
//
// 三个必要条件：
//  1. 配置显式开启（MODEL_MAP_FAILOVER=true）；
//  2. 配置了兜底模型；
//  3. 本次选中的上游模型与兜底模型不同 —— 否则重试等于原地再试一次。
func (g *Gateway) planFallback(st *state) failoverPlan {
	if !g.cfg.Mapping.FailoverOnError || !g.static.HasFallback() {
		return failoverPlan{}
	}
	// 已经是兜底命中的请求不再切换，避免「兜底失败后又切到兜底」。
	if st.matchKind == mapping.MatchFallback {
		return failoverPlan{}
	}
	fb, _, ok := g.static.LookupFallback()
	if !ok || fb == st.upstreamModel {
		return failoverPlan{}
	}
	return failoverPlan{model: fb, enabled: true}
}

// applyPlan 把切换结果落到请求状态上。
//
// 关键不变量：只改 upstreamModel（发往上游的名字），publicModel 一个字节
// 都不动。响应脱敏读的是这两者的组合，因此客户端看到的仍是它请求时用的名字。
func (st *state) applyPlan(p failoverPlan) {
	st.failedOver = true
	st.upstreamModel = p.model
	if p.cap != capability.None {
		st.capUsed = p.cap
		return
	}
	st.matchKind = mapping.MatchFallback
}

// retryWithFallback 用计划中的模型重新发起一次上游请求。
//
// 调用前提（由调用方保证）：首次响应的正文尚未写给客户端。
// 一旦有任何字节下发就不能重试 —— 客户端会收到两段拼接的响应。
func (g *Gateway) retryWithFallback(
	ctx context.Context, r *http.Request, spec *protocol.Spec,
	base string, body []byte, plan failoverPlan, st *state,
) (*http.Response, context.CancelFunc, bool) {
	// 重试自身的失败必须留痕。调用方只看到 ok == false，随后回落到首次
	// 失败的错误 —— 若这里静默返回，「切换为什么没生效」在看板上无迹可寻。
	span := trace.SpanFromContext(ctx)

	nb, err := protocol.RewriteModel(body, plan.model)
	if err != nil {
		obs.RecordAttemptFailure(span, "rewrite", plan.model, 0, err)
		return nil, nil, false
	}
	ureq, err := g.buildRequest(ctx, r, spec, base, nb)
	if err != nil {
		obs.RecordAttemptFailure(span, "build", plan.model, 0, err)
		return nil, nil, false
	}
	resp, cancel, err := g.client.Do(ctx, ureq, st.stream)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		obs.RecordAttemptFailure(span, "transport", plan.model, 0, err)
		return nil, nil, false
	}
	return resp, cancel, true
}
