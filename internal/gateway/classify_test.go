package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"syscall"
	"testing"

	"golang.org/x/net/http2"
)

// outcomeOf 复现 pipeStream 的分流决策，用于在不起真实连接的前提下
// 断言错误分类。三个分支与 response.go 中的 switch 一一对应。
func outcomeOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case isClientGone(err):
		return "client_gone"
	case isUpstreamGracefulEnd(err):
		return "stream_eof"
	default:
		return "stream_broken"
	}
}

// TestStreamErrorClassification 锁定流式转发中各类错误的归类。
//
// 关键不变量：只有 stream_broken 会计入上游错误率并把 span 标红。
// 大模型上游的两种合法收尾（EOF 截断、GOAWAY NO_ERROR）必须归到
// stream_eof，否则正常请求会在看板上表现为错误。
func TestStreamErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
		why  string
	}{
		{
			name: "上游生成完直接关连接",
			err:  io.ErrUnexpectedEOF,
			want: "stream_eof",
			why:  "末事件无终止换行，SSE 允许以 EOF 结束，内容是完整的",
		},
		{
			name: "包装后的 ErrUnexpectedEOF",
			err:  fmt.Errorf("read body: %w", io.ErrUnexpectedEOF),
			want: "stream_eof",
			why:  "transport 常包一层，判定需能穿透包装",
		},
		{
			name: "上游优雅下线",
			err:  http2.GoAwayError{ErrCode: http2.ErrCodeNo},
			want: "stream_eof",
			why:  "GOAWAY(NO_ERROR) 是协议层正常信号，不是上游故障",
		},
		{
			name: "h2 客户端取消",
			err:  http2.StreamError{Code: http2.ErrCodeCancel},
			want: "client_gone",
			why:  "HTTP/2 下客户端取消表现为 RST_STREAM(CANCEL)，不映射到 errno",
		},
		{
			name: "客户端关闭请求",
			err:  context.Canceled,
			want: "client_gone",
			why:  "长连接常态",
		},
		{
			name: "写入已关闭的响应体",
			err:  io.ErrClosedPipe,
			want: "client_gone",
			why:  "长连接常态",
		},
		{
			name: "对端 TCP 已断",
			err:  syscall.EPIPE,
			want: "client_gone",
			why:  "长连接常态",
		},
		{
			name: "读取已关闭的 body",
			err:  http.ErrBodyReadAfterClose,
			want: "client_gone",
			why:  "长连接常态",
		},
		{
			name: "上游内部错误",
			err:  http2.StreamError{Code: http2.ErrCodeInternal},
			want: "stream_broken",
			why:  "上游真故障，必须计入错误率",
		},
		{
			name: "GOAWAY 携带错误码",
			err:  http2.GoAwayError{ErrCode: http2.ErrCodeInternal},
			want: "stream_broken",
			why:  "非 NO_ERROR 的 GOAWAY 代表上游异常，不能与优雅下线混同",
		},
		{
			name: "超时",
			err:  context.DeadlineExceeded,
			want: "stream_broken",
			why:  "超时是真故障，与客户端主动取消必须区分",
		},
		{
			name: "未知错误",
			err:  errors.New("boom"),
			want: "stream_broken",
			why:  "默认从严，不认识的错误一律上报",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := outcomeOf(tc.err); got != tc.want {
				t.Errorf("归类错误：got %s want %s\n  错误: %v\n  理由: %s",
					got, tc.want, tc.err, tc.why)
			}
		})
	}
}

// TestDeadlineExceededNotTreatedAsClientGone 单独守住最容易搞错的一对：
// context.Canceled 是客户端离开，context.DeadlineExceeded 是超时故障。
// 若把两者一起放进 isClientGone，首字节超时会被静默吞掉。
func TestDeadlineExceededNotTreatedAsClientGone(t *testing.T) {
	if isClientGone(context.DeadlineExceeded) {
		t.Fatal("超时不能算客户端离开，否则上游超时在看板上完全不可见")
	}
	if isUpstreamGracefulEnd(context.DeadlineExceeded) {
		t.Fatal("超时不能算正常收尾")
	}
}

// TestGracefulEndExcludesRealEOF 确认 io.EOF 不会进入 stream_eof 分支。
// PipeSSE 已把 io.EOF 转成 nil error，若这里也接纳 EOF，
// 说明有一层重复处理，需要重新审视职责划分。
func TestGracefulEndExcludesRealEOF(t *testing.T) {
	if isUpstreamGracefulEnd(io.EOF) {
		t.Fatal("io.EOF 应由 PipeSSE 消化成 nil，不应到达分类逻辑")
	}
}
