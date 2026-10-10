package protocol

import (
	"errors"
	"math"
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

// SanitizeReport 描述一次脱敏的结果。
type SanitizeReport struct {
	// Changed 报告本次是否改写了任何字段（模型字段整体覆盖 或 子串替换），
	// 与旧版 Sanitize 的第二个返回值同义。
	Changed bool
	// Suppressed 报告「上游标识确实出现在响应里，但一次都没被改写」。
	//
	// 只有一种规则会造成它：长度阈值（见 collect 里的 MaxValueLen 判断）。
	// 生成内容字段被跳过**不算** —— 那是设计意图，计进来信号就永远非零了。
	//
	// 它值得单独一个信号，是因为这个漏点是静默的：客户端拿到的是原文，
	// 看板上既没有错误也没有计数，只能等有人肉眼发现真实上游名出现在
	// 对外响应里。而最容易踩中的正是上游的长错误 message。
	Suppressed bool
}

// Sanitize 对一段 JSON 响应做脱敏，分两步：
//
//	① 模型字段（协议已知路径）整体覆盖为 public —— 精确、无副作用；
//	② 其余字符串值递归扫描，只改写「短且非生成内容」的值 ——
//	   覆盖 id、system_fingerprint、错误 message/code/param 等
//	   事先无法穷举路径的位置。
//
// 步骤 ② 刻意不碰长字符串与 content/text/delta 之类的字段，
// 避免把模型生成的正文改掉。详见 sanitize 包的文档。
//
// # 两步的职责不重叠
//
// 步骤 ① 已整体覆盖的路径不再参与步骤 ② 的子串扫描。这两步对模型字段的
// 语义本就不同 —— ① 是「这个字段的值就是对外模型名」，② 是「这段文本里
// 可能夹着上游标识」。让 ② 再扫一遍 ① 的成果没有任何收益，却会在对外名
// 包含上游名时（public=DeepSeek-V3-20260813 含 upstream=deepseek-v3）
// 把已经正确的值再替换一次，产出 …-20260813-20260813 这样的重复后缀。
//
// 不关心抑制信号的调用方用这个包装即可。
func (s *Spec) Sanitize(body []byte, public string, rep *sanitize.Replacer) ([]byte, bool) {
	out, report := s.sanitize(body, public, rep, rep.MaxValueLen())
	return out, report.Changed
}

// SanitizeWithReport 与 Sanitize 相同，但额外回报「被跳过的漏点」。
func (s *Spec) SanitizeWithReport(
	body []byte, public string, rep *sanitize.Replacer,
) ([]byte, SanitizeReport) {
	return s.sanitize(body, public, rep, rep.MaxValueLen())
}

// SanitizeError 是错误响应（4xx/5xx）专用的脱敏入口：长度阈值不再生效。
//
// 阈值（SANITIZE_MAX_VALUE_LEN）的存在理由是「长字符串 ≈ 模型生成的
// 内容，替换它等于篡改用户的回答」。这个假设只对 2xx 成立 —— 错误正文
// 是上游程序写的，不是模型生成的；而上游的长错误 message（配额超限、
// 多行校验错误、回显了整个请求的 400）几乎必然超过阈值，其间的真实模型
// 名正是最需要脱敏的内容。对错误响应保留阈值，等于给泄漏开后门。
//
// 体积上限不受影响：整体仍被 MAX_SANITIZE_BYTES 卡住，这里只是不再对
// 单个字符串值设限。
func (s *Spec) SanitizeError(
	body []byte, public string, rep *sanitize.Replacer,
) ([]byte, SanitizeReport) {
	return s.sanitize(body, public, rep, math.MaxInt)
}

func (s *Spec) sanitize(
	body []byte, public string, rep *sanitize.Replacer, maxLen int,
) ([]byte, SanitizeReport) {
	out := body
	var report SanitizeReport

	// covered 记录已被 ① 整体覆盖的路径，② 必须跳过它们。
	// ModelPaths 恒为个位数，线性查找比建 map 更快且零分配。
	var covered []string

	// ① 模型字段：整体覆盖。即使 rep 为 noop（上游名 == 对外名）也要执行，
	//    因为上游可能返回带版本后缀的变体（deepseek-v3-250101）。
	for _, p := range s.ModelPaths {
		r := gjson.GetBytes(out, p)
		if !r.Exists() || r.Type != gjson.String {
			continue
		}
		// 字段存在即纳入 covered：值已等于 public 时虽无需改写，
		// 但它同样是「已达最终形态」，仍要挡住 ② 的子串扫描。
		covered = append(covered, p)
		// 用精确比较：仅大小写不同（DeepSeek-Pro vs deepseek-pro）时也要
		// 覆盖，否则上游形态会原样透给客户端。
		if r.Str == public {
			continue
		}
		if v, err := sjson.SetBytesOptions(out, p, public, setOpts); err == nil {
			out, report.Changed = v, true
		}
	}

	// ② 其余短值：仅在确实可能命中时才扫描，避免无谓的解析开销。
	//
	// 抑制信号只在「确实扫过」的前提下才可信：MayMatch 判负说明整段文本里
	// 根本没有上游标识，此时零替换是正确结果，不是漏点。
	if !rep.Noop() && rep.MayMatch(out) {
		v, ok, suppressed := scanAndReplace(out, rep, covered, maxLen)
		report.Suppressed = suppressed
		if ok {
			out, report.Changed = v, true
		}
	}
	return out, report
}

// scanAndReplace 递归遍历 JSON，替换符合条件的字符串值。
//
// 判定条件（全部满足才替换）：
//   - 值是字符串且包含待替换的源串；
//   - 值长度不超过 maxLen（2xx 路径为 SANITIZE_MAX_VALUE_LEN，
//     错误路径为无限 —— 见 SanitizeError）；
//   - 所在字段名不在生成内容白名单里（content / text / delta …）；
//   - 所在路径不在 covered 里（已被模型字段覆盖步骤处理过）。
//
// 第三个返回值 suppressed 表示本次遇到过「含上游标识、但被长度阈值放行」
// 的值 —— 即真正的静默漏点。它与 ok 可以同时为真：一次响应里既有被改写
// 的短值，也有被放行的长值。
func scanAndReplace(
	body []byte, rep *sanitize.Replacer, covered []string, maxLen int,
) (out []byte, ok, suppressed bool) {
	var paths []string
	var values []string

	collect(gjson.ParseBytes(body), "", "", rep, covered, maxLen, 0, &paths, &values, &suppressed)
	if len(paths) == 0 {
		return body, false, suppressed
	}

	out = body
	for i, p := range paths {
		if v, err := sjson.SetBytesOptions(out, p, values[i], setOpts); err == nil {
			out = v
		}
	}
	return out, true, suppressed
}

// collect 递归收集需要改写的路径与新值。
//
// path 为 sjson 路径，key 为当前值所属的字段名（数组元素继承父字段名，
// 因为 ["a","b"] 这类数组里的元素同样属于该字段的语义范畴）。
//
// suppressed 在「含上游标识但被长度阈值放行」时置位。判定放在长度分支内、
// 且只对非生成内容字段生效，因此代价只在「出现超长非内容字符串」这种
// 低频形态上产生 —— 热路径（SSE 增量 chunk）不会走到这里。
func collect(
	v gjson.Result, path, key string, rep *sanitize.Replacer, covered []string, maxLen int,
	depth int, paths, values *[]string, suppressed *bool,
) {
	if depth > maxScanDepth {
		return
	}
	switch v.Type {
	case gjson.String:
		if sanitize.IsContentField(key) {
			return // 生成内容，无论长短都不动
		}
		if isCovered(path, covered) {
			return // 模型字段已整体覆盖，二次子串替换只会产生重复片段
		}
		if len(v.Str) > maxLen {
			// 过长，视为生成内容而放行 —— 这是有意的，但它是**静默**的：
			// 上游的长错误 message 正落在这个区间（配额超限、多行校验错误、
			// 回显了整个请求的 400），而它们往往含真实上游模型名。
			// 错误路径（SanitizeError）的 maxLen 为无限，不会走到这里。
			// 这里顺手判一次「里面到底有没有标识」，把静默漏点变成可测量
			// 的计数；判定只在已经超长的值上发生，不影响热路径。
			if rep.MayMatchString(v.Str) {
				*suppressed = true
			}
			return
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
				collect(item, joinIndex(path, i), key, rep, covered, maxLen, depth+1, paths, values, suppressed)
				i++
				return true
			})
			return
		}
		v.ForEach(func(k, item gjson.Result) bool {
			name := k.String()
			collect(item, joinKey(path, name), name, rep, covered, maxLen, depth+1, paths, values, suppressed)
			return true
		})
	}
}

// isCovered 报告 path 是否已被模型字段覆盖步骤处理过。
//
// covered 元素恒为个位数且是 Spec.ModelPaths 的原样拷贝，
// 与 collect 拼出的路径格式一致（点分、无下标），可直接精确比较。
func isCovered(path string, covered []string) bool {
	for _, c := range covered {
		if c == path {
			return true
		}
	}
	return false
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
