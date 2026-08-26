package protocol

import (
	"errors"
	"io"
	"strings"
	"testing"
)

// TestPipeSSETruncatedStreamIsNotError 断言上游流被截断（末行无换行、
// 无 [DONE]）时 PipeSSE 不返回错误。
//
// 大模型上游在生成完成后常直接关闭连接，末尾既没有终止标记也没有换行；
// 这在 SSE 语义下是合法的流结束，不应被记成故障。
func TestPipeSSETruncatedStreamIsNotError(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"末行无换行", "data: {\"a\":1}\n\ndata: {\"b\":2}"},
		{"无 DONE 标记", "data: {\"a\":1}\n\n"},
		{"空流", ""},
		{"仅注释行", ": ping\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sb strings.Builder
			_, err := PipeSSE(&sb, strings.NewReader(tc.in), nil, nil)
			if err != nil {
				t.Fatalf("截断流应视为正常结束，得到 err=%v", err)
			}
			if tc.in != "" && sb.Len() == 0 {
				t.Fatal("已读内容必须下发，不能丢弃")
			}
		})
	}
}

// TestPipeSSEUpstreamReadErrorSurfaces 断言上游读取真故障不会被吞掉。
//
// 与截断流相反：ErrUnexpectedEOF 之外的读错误代表连接异常中断，
// 必须上报，否则客户端会收到一个「看起来正常结束」的残缺流。
func TestPipeSSEUpstreamReadErrorSurfaces(t *testing.T) {
	want := errors.New("upstream reset")
	src := io.MultiReader(
		strings.NewReader("data: {\"a\":1}\n\n"),
		failReader{want},
	)
	var sb strings.Builder
	st, err := PipeSSE(&sb, src, nil, nil)
	if !errors.Is(err, want) {
		t.Fatalf("上游读错误必须上报，得到 %v", err)
	}
	if st.Events != 1 {
		t.Fatalf("中断前已完成的事件应计入统计，得到 %d", st.Events)
	}
	if !strings.Contains(sb.String(), `{"a":1}`) {
		t.Fatal("中断前已读内容必须已下发")
	}
}

// TestPipeSSEOversizeLineDoesNotSwallowRealError 守住 readLine 的超长行分支：
// 拼接循环退出时若 err 是真错误（非 ErrBufferFull），不能被 normalizeErr 抹平。
func TestPipeSSEOversizeLineDoesNotSwallowRealError(t *testing.T) {
	want := errors.New("boom mid-line")
	huge := strings.Repeat("x", sseReaderBuf+1024)
	src := io.MultiReader(
		strings.NewReader("data: "+huge),
		failReader{want},
	)
	_, err := PipeSSE(io.Discard, src, nil, nil)
	if !errors.Is(err, want) {
		t.Fatalf("超长行中途的真错误必须上报，得到 %v", err)
	}
}

// failReader 立即返回指定错误。与 sse_test.go 的 errReader 区别在于
// 后者固定返回 io.ErrUnexpectedEOF，无法用于断言错误的原样传播。
type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }
