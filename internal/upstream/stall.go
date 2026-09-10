package upstream

import (
	"context"
	"io"
	"net/url"
	"sync/atomic"
	"time"
)

// StallReader 给流式响应体加一道「空闲超时」。
//
// # 为什么必须有
//
// 网关原有的两条超时都拦不住这种形态：响应头已到、流已建立，但上游中途
// 不再吐数据。此时首字节看门狗已经解除，总超时又要等到 UPSTREAM_STREAM_TIMEOUT
// （默认 30 分钟）才生效 —— 中间这段时间里，请求占着并发名额、一条上游连接、
// 一个 goroutine 和整份请求体内存，而客户端一个字节都收不到。
//
// # 判据是「间隔」而不是「总时长」
//
// 一条正常的长回答与一条死掉的流，在总时长上无法区分：前者可能跑十分钟，
// 后者跑了十秒就不再动。能区分它们的是**两个数据块之间的间隔** ——
// 持续产出 token 的流，这个间隔恒定很小，与总时长无关。
//
// # 断开方式
//
// 超时后调用请求的 cancel，让传输层真正放弃这条连接（而不是只在应用层
// 返回一个错误），与首字节看门狗用的是同一种手段。
type StallReader struct {
	rc     io.ReadCloser
	limit  time.Duration
	cancel context.CancelFunc
	timer  *time.Timer
	// tripped 标记本次断开是空闲超时造成的。
	//
	// 必须显式区分：cancel 会让读操作返回 context.Canceled，而网关把
	// context.Canceled 一律当成「客户端提前断开」处理 —— 那不是故障、
	// 不标红、不计错误率。空闲超时恰好会被那套规则静默吞掉。
	tripped atomic.Bool
}

// NewStallReader 包装响应体。limit <= 0 时返回 nil，表示不启用。
func NewStallReader(rc io.ReadCloser, limit time.Duration, cancel context.CancelFunc) *StallReader {
	if rc == nil || limit <= 0 || cancel == nil {
		return nil
	}
	s := &StallReader{rc: rc, limit: limit, cancel: cancel}
	s.timer = time.AfterFunc(limit, func() {
		s.tripped.Store(true)
		cancel()
	})
	return s
}

// Read 实现 io.Reader。每次读取前重置计时器，因此被度量的是「相邻两次
// 读到数据之间的间隔」—— 正是死流与慢流的唯一区分点。
//
// 计时器在无读取在途时触发同样会被捕获：下一次 Read 会立刻因 ctx 已取消
// 而失败，无需在触发时主动打断什么。
func (s *StallReader) Read(p []byte) (int, error) {
	s.timer.Reset(s.limit)
	n, err := s.rc.Read(p)
	if err != nil {
		// 收尾（EOF 或错误）后不再需要计时器，否则它会在请求结束之后
		// 才触发，白白多留一个引用与一次无意义的 cancel。
		s.timer.Stop()
	}
	return n, err
}

// Close 停止计时器并关闭底层响应体。
func (s *StallReader) Close() error {
	if s.timer != nil {
		s.timer.Stop()
	}
	return s.rc.Close()
}

// Tripped 报告是否因空闲超时被断开。
func (s *StallReader) Tripped() bool { return s != nil && s.tripped.Load() }

// Err 在因空闲超时断开后返回带上下文的超时错误，否则返回 nil。
//
// u 由调用方提供 —— 只有它知道这次请求打的是哪个上游端点。
func (s *StallReader) Err(u *url.URL) *TimeoutError {
	if s == nil || !s.tripped.Load() {
		return nil
	}
	// Limit 与 Elapsed 都取阈值：计时器正是到点才触发的，两者相等恰好
	// 表达了「这是本客户端的看门狗开的枪」，与 describe 的判断口径一致。
	return &TimeoutError{
		Kind:    TimeoutKindStall,
		Limit:   s.limit,
		Elapsed: s.limit,
		URL:     safeURL(u),
		Stream:  true,
	}
}
