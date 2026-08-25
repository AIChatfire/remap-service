package sanitize

import "strings"

// foldMatcher 是一组「源串 -> 替换串」规则的大小写无关匹配器。
//
// # 为什么不用 strings.Replacer
//
// strings.Replacer 只做精确匹配，而上游返回的标识大小写并不稳定：同一个
// 渠道可能在 model 字段给 deepseek-v3，在错误 message 里给 DeepSeek-V3，
// 在 request id 里给 DEEPSEEK-V3。精确匹配会让后两者静默漏过 —— 不报错、
// 不降级，只是没脱敏，这是最难发现的一类失效。
//
// # 折叠范围只限 ASCII
//
// 模型名、域名、请求 ID、错误码都是 ASCII。Unicode 折叠需要按 rune 解码，
// 会让热路径上的逐字节扫描失效，收益却接近于零。非 ASCII 字节按原样精确
// 比较，因此中文等内容不受影响。
type foldMatcher struct {
	// pats 按源串长度降序排列，保证 deepseek-v3-250101 先于 deepseek-v3
	// 命中，否则会留下一个 "-250101" 的尾巴造成脱敏不完整。
	pats []foldPat
	// first 是所有源串首字节（大小写两种形态）的存在位图。逐字节扫描时
	// 先查这张表，一次数组访问就能淘汰绝大多数位置。
	first [256]bool
	// minLen 是最短源串长度，短于它的输入直接判负。
	minLen int
	// offs[j] 对应 pats[j]：src 在折叠后的 dst 中首次出现的字节偏移，
	// 不含则为 -1。用于判定「此处已是目标形态」，见 alreadyTarget。
	//
	// 刻意不并入 foldPat：mayMatch 判负是最热路径，它只遍历 pats 比较 src。
	// 把这个字段塞进 foldPat 会让结构体从 32 字节涨到 40 字节，遍历时跨越
	// 更多缓存行 —— 实测 MayMatch miss 由 ~82ns 退化到 ~94ns（+14%）。
	offs []int
}

type foldPat struct {
	// src 已全部转为小写；比较时把输入侧逐字节折叠后对齐。
	src string
	dst string
}

func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

func upperASCII(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - ('a' - 'A')
	}
	return c
}

// newFoldMatcher 构建匹配器。pairs 需已按优先级排好（长串在前）。
// 返回 nil 表示没有任何有效规则。
func newFoldMatcher(pairs [][2]string) *foldMatcher {
	m := &foldMatcher{minLen: -1}
	for _, p := range pairs {
		src := strings.ToLower(p[0])
		if src == "" {
			continue
		}
		m.pats = append(m.pats, foldPat{src: src, dst: p[1]})
		m.offs = append(m.offs, foldIndex(p[1], src))
		c := src[0]
		m.first[c] = true
		m.first[upperASCII(c)] = true
		if m.minLen < 0 || len(src) < m.minLen {
			m.minLen = len(src)
		}
	}
	if len(m.pats) == 0 {
		return nil
	}
	return m
}

// alreadyTarget 报告 s 中 i 处的命中是否已处于目标形态，此时应整段跳过。
// 返回跳过的字节数；0 表示需要正常替换。
//
// # 为什么不能只判断「匹配片段恰好等于 dst」
//
// 对外模型名可能是上游名的超串（upstream=deepseek-v3，public=DeepSeek-V3-20260813）。
// 此时 dst 比 src 长，等长比较永远判负，已经正确的 public 形态会被再替换一次，
// 尾部留下重复后缀（…-20260813-20260813）。
//
// # 为什么要按 off 回看而不是只看 s[i:] 的前缀
//
// src 可能落在 dst 的中间或末尾（upstream=v3，public=pro-v3，off=4）。命中点
// i 指向的是 dst 内部的 "v3"，从 i 起比较永远判负，于是 pro-v3 被反复加前缀，
// 累积成 pro-pro-v3。正确做法是回退 off 字节，检查 [i-off, i-off+len(dst))
// 这个窗口是否恰好是 dst。
//
// off < 0（dst 不含 src）时直接判负：这类规则不存在自匹配问题。
// len(dst) >= len(src) 由 off >= 0 隐含保证 —— dst 若比 src 短则不可能包含它，
// 这一点很关键：当 dst 更短时（upstream=deepseek-v3，public=deepseek），
// 待脱敏的上游名本身就以 dst 开头，跳过它会让上游形态原样泄漏给客户端。
//
// 精确比较而非折叠比较：dst 是运维声明的对外名，只有字节完全一致才算「已达目标」。
func alreadyTarget(s string, i int, dst string, off int) int {
	if off < 0 || i < off {
		return 0
	}
	start := i - off
	if start+len(dst) > len(s) {
		return 0
	}
	if s[start:start+len(dst)] != dst {
		return 0
	}
	return start + len(dst) - i
}

// foldIndex 返回 src 在折叠后的 s 中首次出现的字节偏移，不含则返回 -1。
// src 必须已是小写。仅用于构建期，不在热路径上。
func foldIndex(s, src string) int {
	if src == "" || len(s) < len(src) {
		return -1
	}
	for i := 0; i+len(src) <= len(s); i++ {
		if hasFoldPrefix(s[i:], src) {
			return i
		}
	}
	return -1
}

// hasFoldPrefix 报告 s 是否以 src 开头（src 必须已是小写）。
func hasFoldPrefix(s, src string) bool {
	if len(s) < len(src) {
		return false
	}
	for i := 0; i < len(src); i++ {
		if lowerASCII(s[i]) != src[i] {
			return false
		}
	}
	return true
}

// findAt 报告 s 是否以某条规则的源串开头，命中则返回规则下标，否则返回 -1。
// pats 已按长度降序，因此首个命中就是最长匹配。
func (m *foldMatcher) findAt(s string) int {
	for j := range m.pats {
		if hasFoldPrefix(s, m.pats[j].src) {
			return j
		}
	}
	return -1
}

// mayMatch 报告 s 中是否存在需要替换的内容。
//
// 这是热路径上最重要的一次剪枝：SSE 增量 chunk 绝大多数只含生成文本，
// 此处判负后，后续的 JSON 解析与递归扫描全部省掉。
func (m *foldMatcher) mayMatch(s string) bool {
	if m == nil || m.minLen <= 0 || len(s) < m.minLen {
		return false
	}
	last := len(s) - m.minLen
	for i := 0; i <= last; i++ {
		if !m.first[s[i]] {
			continue
		}
		if m.findAt(s[i:]) >= 0 {
			return true
		}
	}
	return false
}

// replace 执行替换。无实际改动时返回原字符串，不产生分配。
func (m *foldMatcher) replace(s string) string {
	if m == nil || m.minLen <= 0 || len(s) < m.minLen {
		return s
	}
	var b strings.Builder
	last := len(s) - m.minLen
	prev := 0
	for i := 0; i <= last; {
		if !m.first[s[i]] {
			i++
			continue
		}
		j := m.findAt(s[i:])
		if j < 0 {
			i++
			continue
		}
		p := m.pats[j]
		end := i + len(p.src)
		// 已是目标形态：跳到该 dst 之后而不写入。两个作用：避免上报「改动过」
		// 的假信号（body.Sanitize 会把未变化的响应误判为已改写），以及在
		// dst 含有 src 时防止二次替换累积重复片段。
		if skip := alreadyTarget(s, i, p.dst, m.offs[j]); skip > 0 {
			i += skip
			continue
		}
		if b.Cap() == 0 {
			b.Grow(len(s) + len(p.dst))
		}
		b.WriteString(s[prev:i])
		b.WriteString(p.dst)
		i = end
		prev = i
	}
	if prev == 0 {
		return s
	}
	b.WriteString(s[prev:])
	return b.String()
}
