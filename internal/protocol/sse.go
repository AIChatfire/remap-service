package protocol

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"sync"
)

// SSEStats 记录一次流式转发的统计信息。
type SSEStats struct {
	Events  int64
	Bytes   int64
	Chunks  int64
	Rewrote int64
}

var (
	dataPrefix = []byte("data:")
	doneMarker = []byte("[DONE]")
)

const (
	sseReaderBuf = 64 << 10
	sseWriterBuf = 32 << 10
	// sseMaxLine 是单行 SSE 的上限；超过则按分片透传（不做 JSON 级脱敏）。
	sseMaxLine = 8 << 20
)

var (
	readerPool = sync.Pool{New: func() any { return bufio.NewReaderSize(nil, sseReaderBuf) }}
	writerPool = sync.Pool{New: func() any { return bufio.NewWriterSize(nil, sseWriterBuf) }}
)

// Transformer 描述对单行 SSE data 载荷的脱敏动作。
//
// 只作用于 data 行的 JSON 载荷：event / id / retry / 注释行原样保留。
// 上游不会把模型名写进事件名，对它们做替换只会平添破坏协议的风险。
type Transformer interface {
	// Data 对 JSON 载荷做脱敏，返回新载荷与是否发生改动。
	Data(payload []byte) ([]byte, bool)
}

// PipeSSE 从 src 逐行读取 SSE，经 tr 脱敏后写入 dst，并在事件边界立即 flush。
//
// 处理策略：
//   - 保持原始行结构（含 event: / id: / retry: / 注释行 / 空行）不变；
//   - 仅对 "data:" 行的 JSON 载荷做字段级脱敏；
//   - 每个事件（空行）结束后 flush，保证首 token 与增量延迟不受缓冲影响。
func PipeSSE(dst io.Writer, src io.Reader, tr Transformer, flusher http.Flusher) (SSEStats, error) {
	var st SSEStats

	br := readerPool.Get().(*bufio.Reader)
	br.Reset(src)
	defer func() {
		br.Reset(nil)
		readerPool.Put(br)
	}()

	bw := writerPool.Get().(*bufio.Writer)
	bw.Reset(dst)
	defer func() {
		bw.Reset(nil)
		writerPool.Put(bw)
	}()

	flush := func() {
		_ = bw.Flush()
		if flusher != nil {
			flusher.Flush()
		}
	}

	for {
		line, err := readLine(br)
		if len(line) > 0 || err == nil {
			st.Bytes += int64(len(line))

			if len(line) == 0 {
				// 空行 = 事件边界，立即下发已缓冲内容。
				st.Events++
				if werr := bw.WriteByte('\n'); werr != nil {
					return st, werr
				}
				flush()
			} else {
				out, rewrote := transformLine(line, tr)
				if rewrote {
					st.Rewrote++
				}
				st.Chunks++
				if _, werr := bw.Write(out); werr != nil {
					return st, werr
				}
				if werr := bw.WriteByte('\n'); werr != nil {
					return st, werr
				}
				// 上游若已无缓冲数据，说明这是当前可得的最后一行，立即下发。
				if br.Buffered() == 0 {
					flush()
				}
			}
		}
		if err != nil {
			flush()
			if err == io.EOF {
				return st, nil
			}
			return st, err
		}
	}
}

// transformLine 只处理 data 行的 JSON 载荷，其余行原样返回。
func transformLine(line []byte, tr Transformer) (out []byte, rewrote bool) {
	if tr == nil || !bytes.HasPrefix(line, dataPrefix) {
		return line, false
	}
	payload := line[len(dataPrefix):]

	// 保留 "data:" 与载荷之间的空格，维持字节级一致。
	lead := 0
	for lead < len(payload) && payload[lead] == ' ' {
		lead++
	}
	body := payload[lead:]
	if len(body) == 0 || bytes.Equal(body, doneMarker) || !LooksLikeJSON(body) {
		return line, false
	}

	nb, changed := tr.Data(body)
	if !changed {
		return line, false
	}
	buf := make([]byte, 0, len(dataPrefix)+lead+len(nb))
	buf = append(buf, dataPrefix...)
	buf = append(buf, payload[:lead]...)
	buf = append(buf, nb...)
	return buf, true
}

// readLine 读取一整行（去掉行尾 \n 与 \r），处理超长行。
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		buf := append(make([]byte, 0, len(line)*2), line...)
		for err == bufio.ErrBufferFull && len(buf) < sseMaxLine {
			line, err = br.ReadSlice('\n')
			buf = append(buf, line...)
		}
		return trimEOL(buf), normalizeErr(err)
	}
	return trimEOL(line), err
}

func normalizeErr(err error) error {
	if err == bufio.ErrBufferFull {
		return nil
	}
	return err
}

func trimEOL(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}
