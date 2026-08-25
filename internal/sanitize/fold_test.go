package sanitize

import "testing"

func newFoldRules() *Rules {
	return NewRules(
		map[string][]string{"deepseek-v3": {"deepseek-v3-250101"}},
		map[string]string{"ark.cn-beijing.volces.com": "api.gateway.local"},
		nil,
		0,
	)
}

// 上游在不同字段里给出的标识形态不统一，各种大小写变体都必须命中。
func TestApplyIgnoresCase(t *testing.T) {
	p := newFoldRules().For("deepseek-v3", "deepseek-pro")

	cases := [][2]string{
		{"deepseek-v3", "deepseek-pro"},
		{"DeepSeek-V3", "deepseek-pro"},
		{"DEEPSEEK-V3", "deepseek-pro"},
		{"DeepSeek-v3-250101", "deepseek-pro"},
		{"Model DEEPSEEK-V3 is overloaded", "Model deepseek-pro is overloaded"},
		{"no match here", "no match here"},
	}
	for _, c := range cases {
		if got := p.Apply(c[0]); got != c[1] {
			t.Errorf("Apply(%q) = %q, 期望 %q", c[0], got, c[1])
		}
		if c[0] != c[1] && !p.MayMatchString(c[0]) {
			t.Errorf("MayMatchString(%q) 应为 true", c[0])
		}
	}
}

// 全局替换对同样要大小写无关：域名在 Location 头里常见大写变体。
func TestGlobalReplacementIgnoresCase(t *testing.T) {
	p := newFoldRules().For("deepseek-v3", "deepseek-pro")
	got := p.Apply("https://ARK.CN-Beijing.Volces.com/api/v3")
	const want = "https://api.gateway.local/api/v3"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// 长串必须先于其前缀命中，否则会留下 "-250101" 之类的残尾。
func TestLongestMatchWinsIgnoringCase(t *testing.T) {
	p := newFoldRules().For("deepseek-v3", "X")
	if got := p.Apply("DeepSeek-V3-250101"); got != "X" {
		t.Errorf("最长匹配失效：得到 %q，期望 %q", got, "X")
	}
}

// 上游名与对外名仅大小写不同时不能当成 noop，响应里出现的是上游形态。
func TestCaseOnlyDifferenceIsNotNoop(t *testing.T) {
	p := NewRules(nil, nil, nil, 0).For("DeepSeek-V3", "deepseek-v3")
	if got := p.Apply("DeepSeek-V3"); got != "deepseek-v3" {
		t.Errorf("仅大小写不同时应替换：得到 %q", got)
	}
}

// 已是目标形态时不应再改动，否则会给出「已改写」的假信号。
func TestApplyIsIdempotent(t *testing.T) {
	p := newFoldRules().For("deepseek-v3", "deepseek-pro")
	once := p.Apply("chatcmpl-DeepSeek-V3-abc123")
	if twice := p.Apply(once); twice != once {
		t.Errorf("重复应用不稳定：%q -> %q", once, twice)
	}
}

// 字段名大小写不可信；判负会让生成内容退回「短值即替换」的通用规则。
func TestIsContentFieldIgnoresCase(t *testing.T) {
	for _, n := range []string{"content", "Content", "CONTENT", "reasoning_content", "Reasoning_Content"} {
		if !IsContentField(n) {
			t.Errorf("IsContentField(%q) 应为 true", n)
		}
	}
	for _, n := range []string{"model", "id", "object"} {
		if IsContentField(n) {
			t.Errorf("IsContentField(%q) 应为 false", n)
		}
	}
}

// 非 ASCII 内容不参与折叠，中文 payload 必须原样返回。
func TestApplyLeavesNonASCIIIntact(t *testing.T) {
	p := newFoldRules().For("deepseek-v3", "deepseek-pro")
	const s = "这是一段中文内容，不含任何上游标识。"
	if got := p.Apply(s); got != s {
		t.Errorf("中文内容被改写：%q", got)
	}
	if p.MayMatchString(s) {
		t.Error("中文内容不应触发预检")
	}
}
