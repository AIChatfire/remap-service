package protocol

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// TestPipeSSEClientDisconnectError 记录客户端中途断开时 PipeSSE 返回的错误形态。
// 这类错误不代表上游或网关故障，调用方不应据此把 span 标成 Error。
func TestPipeSSEClientDisconnectError(t *testing.T) {
	src := strings.NewReader("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n")
	_, err := PipeSSE(brokenWriter{}, src, nil, nil)
	if err == nil {
		t.Fatal("客户端断开时应返回错误")
	}
	t.Logf("写入端断开: %v (errors.Is ErrClosedPipe=%v)", err, errors.Is(err, io.ErrClosedPipe))
}

// TestPipeSSEUpstreamCanceledError 记录 ctx 取消导致读上游失败时的错误形态。
func TestPipeSSEUpstreamCanceledError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := PipeSSE(io.Discard, ctxReader{ctx}, nil, nil)
	if err == nil {
		t.Fatal("ctx 取消时应返回错误")
	}
	t.Logf("上游读取取消: %v (errors.Is Canceled=%v)", err, errors.Is(err, context.Canceled))
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type ctxReader struct{ ctx context.Context }

func (r ctxReader) Read([]byte) (int, error) { return 0, r.ctx.Err() }
