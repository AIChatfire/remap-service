package protocol

import (
	"errors"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/betterme/remap-service/internal/sanitize"
)

// ErrNoModel 表示请求体中不含顶层 model 字段。
var ErrNoModel = errors.New("请求体缺少 model 字段")

var setOpts = &sjson.Options{Optimistic: true, ReplaceInPlace: false}

// ExtractModel 从请求体中读取对外模型名。
// OpenAI / Responses / Anthropic 三种协议的请求体都把模型放在顶层 "model"。
func ExtractModel(body []byte) (string, error) {
	r := gjson.GetBytes(body, "model")
	if !r.Exists() || r.Type != gjson.String || r.Str == "" {
		return "", ErrNoModel
	}
	return r.Str, nil
}

// IsStream 判断请求是否为流式。三种协议均使用顶层 "stream" 布尔。
func IsStream(body []byte) bool {
	return gjson.GetBytes(body, "stream").Bool()
}

// RewriteModel 把请求体中的 model 改写为上游真实模型名。
func RewriteModel(body []byte, upstream string) ([]byte, error) {
	return sjson.SetBytesOptions(body, "model", upstream, setOpts)
}

// maxScanDepth 限制递归扫描深度，防御畸形的深层嵌套 JSON。
const maxScanDepth = 12

// Sanitize 对一段 JSON 响应做脱敏，分两步：
//
//	① 模型字段（协议已知路径）整体覆盖为 public —— 精确、无副作用；
//	② 其余字符串值递归扫描，只改写「短且非生成内容」的值 ——
//	   覆盖 id、system_fingerprint、错误 message/code/param 等
//	   事先无法穷举路径的位置。
//
// 步骤 ② 刻意不碰长字符串与 content/text/delta 之类的字段，
// 避免把模型生成的正文改掉。详见 sanitize 包的文档。
func (s *Spec) Sanitize(body []byte, public string, rep *sanitize.Replacer) ([]byte, bool) {
	out := body
	changed := false

	// ① 模型字段：整体覆盖。即使 rep 为 noop（上游名 == 对外名）也要执行，
	//    因为上游可能返回带版本后缀的变体（deepseek-v3-250101）。
	for _, p := range s.ModelPaths {
		r := gjson.GetBytes(out, p)
		if !r.Exists() || r.Type != gjson.String || r.Str == public {
			continue
		}
		if v, err := sjson.SetBytesOptions(out, p, public, setOpts); err == nil {
			out, changed = v, true
		}
	}

	// ② 其余短值：仅在确实可能命中时才扫描，避免无谓的解析开销。
	if rep.MayMatch(out) {
		if v, ok := scanAndReplace(out, rep); ok {
			out, changed = v, true
		}
	}
	return out, changed
}

// scanAndReplace 递归遍历 JSON，替换符合条件的字符串值。
//
// 判定条件（全部满足才替换）：
//   - 值是字符串且包含待替换的源串；
//   - 值长度不超过 rep.MaxValueLen()；
//   - 所在字段名不在生成内容白名单里（content / text / delta …）。
func scanAndReplace(body []byte, rep *sanitize.Replacer) ([]byte, bool) {
	var paths []string
	var values []string

	collect(gjson.ParseBytes(body), "", "", rep, 0, &paths, &values)
	if len(paths) == 0 {
		return body, false
	}

	out := body
	for i, p := range paths {
		if v, err := sjson.SetBytesOptions(out, p, values[i], setOpts); err == nil {
			out = v
		}
	}
	return out, true
}

// collect 递归收集需要改写的路径与新值。
//
// path 为 sjson 路径，key 为当前值所属的字段名（数组元素继承父字段名，
// 因为 ["a","b"] 这类数组里的元素同样属于该字段的语义范畴）。
func collect(v gjson.Result, path, key string, rep *sanitize.Replacer, depth int, paths, values *[]string) {
	if depth > maxScanDepth {
		return
	}
	switch v.Type {
	case gjson.String:
		if sanitize.IsContentField(key) {
			return // 生成内容，无论长短都不动
		}
		if len(v.Str) > rep.MaxValueLen() {
			return // 过长，视为生成内容
		}
		nv := rep.Apply(v.Str)
		if nv != v.Str {
			*paths = append(*paths, path)
			*values = append(*values, nv)
		}
	case gjson.JSON:
		if v.IsArray() {
			i := 0
			v.ForEach(func(_, item gjson.Result) bool {
				collect(item, joinIndex(path, i), key, rep, depth+1, paths, values)
				i++
				return true
			})
			return
		}
		v.ForEach(func(k, item gjson.Result) bool {
			name := k.String()
			collect(item, joinKey(path, name), name, rep, depth+1, paths, values)
			return true
		})
	}
}

// joinKey 拼接对象字段路径，转义 sjson 的特殊字符。
func joinKey(base, key string) string {
	esc := escapePathSeg(key)
	if base == "" {
		return esc
	}
	return base + "." + esc
}

// joinIndex 拼接数组下标路径。
func joinIndex(base string, i int) string {
	idx := itoa(i)
	if base == "" {
		return idx
	}
	return base + "." + idx
}

// escapePathSeg 转义 sjson 路径中的 . * ? \ 等特殊字符。
func escapePathSeg(s string) string {
	if !strings.ContainsAny(s, `.*?\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '.', '*', '?', '\\':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

// LooksLikeJSON 快速判断一段 payload 是否为 JSON 对象。
//
// 刻意只接受对象而不接受数组：SSE 的 data 载荷在这三种协议下恒为对象，
// 而 "[DONE]" 哨兵恰好以 '[' 开头，放行数组会导致对它做无意义的解析。
func LooksLikeJSON(b []byte) bool {
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		case '{':
			return true
		default:
			return false
		}
	}
	return false
}
