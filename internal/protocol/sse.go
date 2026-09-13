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
	// Suppressed 是「载荷里出现上游标识但零替换」的行数，见 SanitizeReport。
	// 与 Rewrote 并列上报：只有两者一起看，才能分辨「没出现标识」与
	// 「出现了但全被长度阈值放行」——后者就是那个静默漏点。
	Suppressed int64
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
	// Data 对 JSON 载荷做脱敏，返回新载荷与本次的脱敏结果。
	Data(payload []byte) ([]byte, SanitizeReport)
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
				out, rewrote, suppressed := transformLine(line, tr)
				if rewrote {
					st.Rewrote++
				}
				if suppressed {
					st.Suppressed++
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
func transformLine(line []byte, tr Transformer) (out []byte, rewrote, suppressed bool) {
	if tr == nil || !bytes.HasPrefix(line, dataPrefix) {
		return line, false, false
	}
	payload := line[len(dataPrefix):]

	// 保留 "data:" 与载荷之间的空格，维持字节级一致。
	lead := 0
	for lead < len(payload) && payload[lead] == ' ' {
		lead++
	}
	body := payload[lead:]
	if len(body) == 0 || bytes.Equal(body, doneMarker) || !LooksLikeJSON(body) {
		return line, false, false
	}

	nb, report := tr.Data(body)
	if !report.Changed {
		return line, false, report.Suppressed
	}
	buf := make([]byte, 0, len(dataPrefix)+lead+len(nb))
	buf = append(buf, dataPrefix...)
	buf = append(buf, payload[:lead]...)
	buf = append(buf, nb...)
	return buf, true, report.Suppressed
}

// SanitizeEvent 把一段**本地构造**的、已按 SSE 分帧的完整事件，走一遍与上游
// 字节**完全相同**的逐行脱敏路径。
//
// # 为什么需要它
//
// 网关会提前下发一个合成首事件（见 gateway 的 STREAM_EARLY_FIRST_EVENT）。
// 它不是上游来的，因此天然不经过 PipeSSE —— 于是成了整条流里唯一一段
// 「本进程构造、却能原样写进客户端响应」的字节。
//
// 只加一条「合成事件里必须是对外名」的断言是不够的：断言只能证明当前那行
// 代码写对了，挡不住后续任何改动（改错字段、或将来往事件里补了上游的 id /
// system_fingerprint）。这里让它走同一条路径之后，泄漏面从「约定」变成
// 「结构上不成立」—— 尤其 model 字段会被 Spec 的 ModelPaths 规则**整体覆盖**
// 为对外名，因此合成事件的 model 与正文的 model 必然一致，不可能自相矛盾。
//
// 返回原切片（而不是复制一份）当且仅当没有任何一行被改写，这样
// 「未启用脱敏」与「载荷里没有上游标识」两条路径都保持零分配、逐字节一致。
//
// # 边界（写新构造器前必须知道）
//
// 与 PipeSSE 一样，**只处理 `data:` 行**：`event:` / `id:` / 注释行原样保留
// （上游也不会把模型名写进那里，动它们只会破坏协议）。因此这条保证的边界是
// 「data 载荷里的标识一定会被擦掉」，**不等于「任何字节都安全」**。
// 今后往合成事件里加内容时，**非 data 行只能是常量**，绝不能拼接上游来的值。
func SanitizeEvent(frame []byte, tr Transformer) []byte {
	if tr == nil || len(frame) == 0 {
		return frame
	}
	var out []byte // 惰性分配：真的有行被改写时才构建
	rest := frame
	for len(rest) > 0 {
		i := bytes.IndexByte(rest, '\n')
		line := rest
		if i >= 0 {
			line = rest[:i]
		}
		nl, rewrote, _ := transformLine(line, tr)
		if rewrote && out == nil {
			out = make([]byte, 0, len(frame)+len(frame)/4)
			out = append(out, frame[:len(frame)-len(rest)]...)
		}
		if out != nil {
			out = append(out, nl...)
			if i >= 0 {
				out = append(out, '\n')
			}
		}
		if i < 0 {
			rest = nil
		} else {
			rest = rest[i+1:]
		}
	}
	if out == nil {
		return frame
	}
	return out
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
