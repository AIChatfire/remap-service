package mapping

import "strings"

// pattern 是一条支持 `*` 通配的模型名规则。
//
// # 支持的形态
//
//	claude-*            前缀匹配
//	*-flash             后缀匹配
//	gpt-*-turbo         中缀匹配（前后各一段字面量）
//	*vision*            包含匹配
//	*                   全匹配（catch-all）
//
// 刻意不用正则：模型名是简单标识符，正则会带来编译开销、回溯风险，
// 以及「用户写错一个字符就静默匹配到意外的模型」这类难查的问题。
// 通配符的语义边界清晰，实现只有几十行且可穷举测试。
type pattern struct {
	raw string
	// segs 是按 `*` 切分后的字面量片段。
	segs []string
	// leading / trailing 表示首尾是否为通配（即是否允许前后有任意内容）。
	leading  bool
	trailing bool
	// literal 为 true 时该规则不含通配符，等价于精确匹配。
	literal bool
	// weight 是具体度，用于在多条规则同时命中时挑选最具体的一条。
	weight int
}

// compilePattern 解析一条规则。返回的 pattern 可并发只读使用。
func compilePattern(raw string) pattern {
	if !strings.Contains(raw, "*") {
		return pattern{raw: raw, literal: true, weight: literalWeight(raw)}
	}
	parts := strings.Split(raw, "*")
	p := pattern{
		raw:      raw,
		leading:  parts[0] == "",
		trailing: parts[len(parts)-1] == "",
	}
	for _, s := range parts {
		if s != "" {
			p.segs = append(p.segs, s)
		}
	}
	p.weight = patternWeight(p)
	return p
}

// literalWeight 让精确规则永远排在所有通配规则之前。
func literalWeight(raw string) int {
	return 1 << 20 // 远大于任何通配规则的权重
}

// patternWeight 计算通配规则的具体度。
//
// 排序依据（由强到弱）：
//  1. 字面量总长度 —— 匹配的字符越多越具体（claude-3-5-* 比 claude-* 具体）；
//  2. 锚定的边界数 —— 首尾贴边比两头都通配更具体（claude-* 比 *claude* 具体）。
//
// catch-all（"*"）没有字面量也没有锚点，权重为 0，永远最后匹配。
func patternWeight(p pattern) int {
	n := 0
	for _, s := range p.segs {
		n += len(s)
	}
	w := n * 4
	if !p.leading {
		w += 2 // 前端锚定
	}
	if !p.trailing {
		w += 1 // 后端锚定
	}
	return w
}

// match 判断 s 是否符合该规则。可并发调用：只读 p，不修改任何字段。
func (p pattern) match(s string) bool {
	if p.literal {
		return s == p.raw
	}
	if len(p.segs) == 0 {
		return true // "*" 或 "**"，全匹配
	}

	rest := s
	segs := p.segs // 局部切片头，避免误以为在改 p 的状态

	// 首段未通配时必须从头精确贴合。
	if !p.leading {
		if !strings.HasPrefix(rest, segs[0]) {
			return false
		}
		rest = rest[len(segs[0]):]
		segs = segs[1:]
	}
	// 末段未通配时必须贴到尾部；先处理它，避免与中间段的贪心匹配冲突。
	if !p.trailing && len(segs) > 0 {
		tail := segs[len(segs)-1]
		if !strings.HasSuffix(rest, tail) {
			return false
		}
		rest = rest[:len(rest)-len(tail)]
		segs = segs[:len(segs)-1]
	}
	// 剩余段按出现顺序依次查找即可 —— 每段之间都隔着至少一个 `*`。
	for _, seg := range segs {
		i := strings.Index(rest, seg)
		if i < 0 {
			return false
		}
		rest = rest[i+len(seg):]
	}
	return true
}

// hasWildcard 报告一条规则是否含通配符。
func hasWildcard(s string) bool { return strings.Contains(s, "*") }
