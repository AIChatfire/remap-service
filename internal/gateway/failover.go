package gateway

import (
	"context"
	"net/http"

	"github.com/betterme/remap-service/internal/mapping"
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

// failoverPlan 描述一次可能的故障切换。
type failoverPlan struct {
	// model 是切换到的兜底上游模型。
	model string
	// enabled 为 false 时不做任何切换。
	enabled bool
}

// planFailover 判断本次请求是否具备切换条件。
//
// 三个必要条件：
//  1. 配置显式开启（MODEL_MAP_FAILOVER=true）；
//  2. 配置了兜底模型；
//  3. 本次选中的上游模型与兜底模型不同 —— 否则重试等于原地再试一次。
func (g *Gateway) planFailover(st *state) failoverPlan {
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

// retryWithFallback 用兜底模型重新发起一次上游请求。
//
// 调用前提（由调用方保证）：首次响应的正文尚未写给客户端。
// 一旦有任何字节下发就不能重试 —— 客户端会收到两段拼接的响应。
func (g *Gateway) retryWithFallback(
	ctx context.Context, r *http.Request, spec *protocol.Spec,
	base string, body []byte, plan failoverPlan, st *state,
) (*http.Response, context.CancelFunc, bool) {
	nb, err := protocol.RewriteModel(body, plan.model)
	if err != nil {
		return nil, nil, false
	}
	ureq, err := g.buildRequest(ctx, r, spec, base, nb)
	if err != nil {
		return nil, nil, false
	}
	resp, cancel, err := g.client.Do(ctx, ureq, st.stream)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, false
	}
	return resp, cancel, true
}
