package sanitize

import "testing"

// 交叉命名：对外名与上游名互相包含时的替换行为。
//
// 这类配置在真实渠道里很常见 —— 客户按带日期后缀的全名请求
// （DeepSeek-V3-20260813），网关映射到不带后缀的上游名（deepseek-v3），
// 于是对外名成了上游名的超串。此时「已是目标形态」的判定若只做等长比较，
// 会把已经正确的值再替换一次，尾部留下重复片段。
func TestOverlappingNamesApply(t *testing.T) {
	cases := []struct {
		name     string
		upstream string
		public   string
		in       string
		want     string
	}{
		{
			name:     "public 是 upstream 的超串（带日期后缀）",
			upstream: "deepseek-v3",
			public:   "DeepSeek-V3-20260813",
			in:       "deepseek-v3",
			want:     "DeepSeek-V3-20260813",
		},
		{
			name:     "public 超串且入参已是 public 形态",
			upstream: "deepseek-v3",
			public:   "DeepSeek-V3-20260813",
			in:       "DeepSeek-V3-20260813",
			want:     "DeepSeek-V3-20260813",
		},
		{
			name:     "public 超串，夹在错误文本里",
			upstream: "deepseek-v3",
			public:   "DeepSeek-V3-20260813",
			in:       "model deepseek-v3 is overloaded",
			want:     "model DeepSeek-V3-20260813 is overloaded",
		},
		{
			name:     "public 超串，错误文本里已是 public 形态",
			upstream: "deepseek-v3",
			public:   "DeepSeek-V3-20260813",
			in:       "model DeepSeek-V3-20260813 is overloaded",
			want:     "model DeepSeek-V3-20260813 is overloaded",
		},
		{
			name:     "public 是 upstream 的子串（反向包含）",
			upstream: "deepseek-v3",
			public:   "deepseek",
			in:       "deepseek-v3",
			want:     "deepseek",
		},
		{
			name:     "public 子串，入参已是 public 形态不应被动",
			upstream: "deepseek-v3",
			public:   "deepseek",
			in:       "deepseek",
			want:     "deepseek",
		},
		{
			name:     "仅大小写不同",
			upstream: "deepseek-v3",
			public:   "DeepSeek-V3",
			in:       "deepseek-v3",
			want:     "DeepSeek-V3",
		},
		{
			name:     "public 含 upstream 但不在开头",
			upstream: "v3",
			public:   "pro-v3",
			in:       "v3",
			want:     "pro-v3",
		},
		{
			name:     "完全不相交",
			upstream: "deepseek-v3",
			public:   "deepseek-pro",
			in:       "deepseek-v3",
			want:     "deepseek-pro",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewRules(nil, nil, nil, 0).For(c.upstream, c.public)
			if got := p.Apply(c.in); got != c.want {
				t.Errorf("Apply(%q) = %q, 期望 %q", c.in, got, c.want)
			}
		})
	}
}

// 无论命名如何交叉，Apply 都必须幂等：网关可能在多个层级重复调用，
// 不幂等会让每一次经过都累积一段重复后缀。
func TestOverlappingNamesApplyIsIdempotent(t *testing.T) {
	pairs := [][2]string{
		{"deepseek-v3", "DeepSeek-V3-20260813"},
		{"deepseek-v3", "deepseek-v3-250101"},
		{"deepseek-v3", "deepseek"},
		{"deepseek-v3", "DeepSeek-V3"},
		{"v3", "pro-v3"},
		{"v3", "v3-turbo"},
		{"deepseek-v3", "deepseek-pro"},
	}
	inputs := []string{
		"deepseek-v3",
		"DeepSeek-V3",
		"chatcmpl-deepseek-v3-abc123",
		"model deepseek-v3 not found",
		"v3",
	}

	for _, pr := range pairs {
		p := NewRules(nil, nil, nil, 0).For(pr[0], pr[1])
		for _, in := range inputs {
			once := p.Apply(in)
			twice := p.Apply(once)
			if twice != once {
				t.Errorf("upstream=%q public=%q: Apply 不幂等 %q -> %q -> %q",
					pr[0], pr[1], in, once, twice)
			}
		}
	}
}

// 反向包含（public 比 upstream 短）时不得因「已以 dst 开头」而整段跳过，
// 否则上游名会原样泄漏给客户端 —— 这正是脱敏要防的事。
func TestShorterPublicStillSanitizes(t *testing.T) {
	p := NewRules(nil, nil, nil, 0).For("deepseek-v3", "deepseek")

	for _, in := range []string{"deepseek-v3", "DeepSeek-V3", "use deepseek-v3 here"} {
		got := p.Apply(in)
		if containsFold(got, "deepseek-v3") {
			t.Errorf("Apply(%q) = %q，上游名仍然残留", in, got)
		}
	}
}

// containsFold 是测试内的大小写无关子串查找，避免依赖被测实现。
func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if hasFoldPrefix(s[i:], sub) {
			return true
		}
	}
	return false
}
