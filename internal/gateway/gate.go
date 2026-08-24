package gateway

import (
	"net/http"
	"strconv"
	"sync/atomic"
)

// gate 是并发闸门，限制同时在途的上游请求数。
//
// # 为什么必须有
//
// LLM 请求的特点是「单请求耗时长、上游容量有限」：一次生成动辄数十秒，
// 上游又常有并发配额。一旦上游变慢，入站请求会持续堆积 —— 每个在途请求
// 至少持有一个 goroutine、一份请求体缓冲（最大 MAX_BODY_BYTES）与一条
// 上游连接。没有上限时，网关会在上游恢复之前先把自己 OOM 掉，
// 并且把压力全部传导给已经过载的上游。
//
// 闸门把「过载」从不可控的资源耗尽，变成可控的快速失败：超限直接返回
// 503 + Retry-After，让客户端与负载均衡器有明确信号可以退避。
//
// # 为什么用带缓冲 channel 而不是 semaphore.Weighted
//
// 这里只需要计数为 1 的获取与非阻塞尝试，channel 的 select-default 恰好
// 表达「拿不到就立刻放弃」，且不引入额外依赖。
type gate struct {
	tokens chan struct{}
	// rejected 累计被拒次数，用于健康检查与日志。
	rejected atomic.Int64
}

// newGate 创建容量为 limit 的闸门。limit <= 0 表示不限制。
func newGate(limit int) *gate {
	if limit <= 0 {
		return &gate{}
	}
	return &gate{tokens: make(chan struct{}, limit)}
}

// tryAcquire 尝试获取一个令牌。返回的 release 在请求结束时调用。
//
// 刻意不阻塞等待：排队等待只会把延迟累积到客户端超时，
// 对已经过载的系统没有帮助。快速失败让上游有机会恢复。
func (g *gate) tryAcquire() (release func(), ok bool) {
	if g.tokens == nil {
		return noopRelease, true
	}
	select {
	case g.tokens <- struct{}{}:
		return func() { <-g.tokens }, true
	default:
		g.rejected.Add(1)
		return noopRelease, false
	}
}

// inFlight 返回当前在途请求数。
func (g *gate) inFlight() int {
	if g.tokens == nil {
		return 0
	}
	return len(g.tokens)
}

// limit 返回闸门容量，0 表示不限制。
func (g *gate) limit() int {
	if g.tokens == nil {
		return 0
	}
	return cap(g.tokens)
}

// Rejected 返回累计被拒次数。
func (g *gate) Rejected() int64 { return g.rejected.Load() }

func noopRelease() {}

// writeOverloaded 返回 503 并给出退避提示。
//
// 用 429 会被客户端误解为「上游限流」进而触发它自己的重试策略；
// 503 + Retry-After 语义更准确：网关暂时无法处理，请稍后再试。
func writeOverloaded(w http.ResponseWriter, retryAfterSec int) int {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
	return writeError(w, http.StatusServiceUnavailable, "gateway_overloaded",
		"gateway at capacity; retry after "+strconv.Itoa(retryAfterSec)+"s")
}
