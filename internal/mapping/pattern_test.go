package mapping

import "testing"

func TestPatternMatch(t *testing.T) {
	cases := []struct {
		pat  string
		yes  []string
		no   []string
		desc string
	}{
		{
			pat:  "claude-*",
			desc: "前缀",
			yes:  []string{"claude-3", "claude-3-5-sonnet", "claude-"},
			no:   []string{"claude", "my-claude-3", "gpt-4"},
		},
		{
			pat:  "*-flash",
			desc: "后缀",
			yes:  []string{"gemini-flash", "a-flash", "-flash"},
			no:   []string{"flash", "flash-lite", "gemini-flash-8b"},
		},
		{
			pat:  "gpt-*-turbo",
			desc: "中缀",
			yes:  []string{"gpt-4-turbo", "gpt-3.5-turbo", "gpt--turbo"},
			no:   []string{"gpt-4", "4-turbo", "gpt-4-turbo-preview"},
		},
		{
			pat:  "*vision*",
			desc: "包含",
			yes:  []string{"vision", "gpt-4-vision-preview", "xvisiony"},
			no:   []string{"gpt-4", "visio"},
		},
		{
			pat:  "*",
			desc: "catch-all",
			yes:  []string{"anything", "", "a-b-c"},
			no:   nil,
		},
		{
			pat:  "exact-model",
			desc: "无通配即精确",
			yes:  []string{"exact-model"},
			no:   []string{"exact-model-2", "exact", "EXACT-MODEL"},
		},
		{
			// 多段通配：每段之间按出现顺序查找。
			pat:  "a*b*c",
			desc: "多段",
			yes:  []string{"abc", "a1b2c", "aXXbYYc"},
			no:   []string{"acb", "ab", "a1b2c3"},
		},
	}

	for _, c := range cases {
		t.Run(c.desc+" "+c.pat, func(t *testing.T) {
			p := compilePattern(c.pat)
			for _, s := range c.yes {
				if !p.match(s) {
					t.Errorf("%q 应匹配 %q", c.pat, s)
				}
			}
			for _, s := range c.no {
				if p.match(s) {
					t.Errorf("%q 不应匹配 %q", c.pat, s)
				}
			}
		})
	}
}

// 匹配优先级必须由具体度决定，不受配置书写顺序影响。
func TestWildcardPriorityIsDeterministic(t *testing.T) {
	// 同一个模型名能被多条规则命中，应选最具体的那条。
	rules := map[string][]string{
		"*":                      {"catch-all"},
		"claude-*":               {"prefix"},
		"claude-3-5-*":           {"longer-prefix"},
		"*sonnet":                {"suffix"},
		"claude-3-5-sonnet-2024": {"exact"},
	}

	t.Run("精确优先于一切通配", func(t *testing.T) {
		tb := FromStatic(rules, nil)
		got, kind, ok := tb.LookupKind("claude-3-5-sonnet-2024")
		if !ok || got != "exact" {
			t.Fatalf("命中 %q (kind=%v)，期望 exact", got, kind)
		}
		if kind != MatchExact {
			t.Errorf("kind = %v，期望 MatchExact", kind)
		}
	})

	t.Run("字面量更长的通配优先", func(t *testing.T) {
		tb := FromStatic(rules, nil)
		got, kind, _ := tb.LookupKind("claude-3-5-haiku")
		if got != "longer-prefix" {
			t.Errorf("命中 %q，期望 longer-prefix（claude-3-5-* 比 claude-* 具体）", got)
		}
		if kind != MatchWildcard {
			t.Errorf("kind = %v，期望 MatchWildcard", kind)
		}
	})

	t.Run("前缀锚定优先于后缀锚定", func(t *testing.T) {
		// "claude-*"(weight=4*7+2=30) vs "*sonnet"(weight=4*6+1=25)
		tb := FromStatic(rules, nil)
		got, _, _ := tb.LookupKind("claude-2-sonnet")
		if got != "prefix" {
			t.Errorf("命中 %q，期望 prefix", got)
		}
	})

	t.Run("都不匹配时落到 catch-all", func(t *testing.T) {
		tb := FromStatic(rules, nil)
		got, _, _ := tb.LookupKind("totally-unrelated")
		if got != "catch-all" {
			t.Errorf("命中 %q，期望 catch-all", got)
		}
	})

	// 关键：换一个 map 迭代顺序（Go 的 map 顺序本就随机），结果必须一致。
	t.Run("多次构建结果稳定", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			tb := FromStatic(rules, nil)
			if got, _, _ := tb.LookupKind("claude-3-5-haiku"); got != "longer-prefix" {
				t.Fatalf("第 %d 次构建结果不同: %q", i, got)
			}
		}
	})
}

func TestFallback(t *testing.T) {
	t.Run("精确与通配都未命中时启用兜底", func(t *testing.T) {
		tb := FromStatic(map[string][]string{"known": {"up-known"}}, []string{"safe-model"})
		got, kind, ok := tb.LookupKind("never-heard-of")
		if !ok || got != "safe-model" {
			t.Fatalf("命中 %q，期望 safe-model", got)
		}
		if kind != MatchFallback {
			t.Errorf("kind = %v，期望 MatchFallback", kind)
		}
	})

	t.Run("兜底不得抢占已命中的规则", func(t *testing.T) {
		tb := FromStatic(map[string][]string{"known": {"up-known"}}, []string{"safe-model"})
		if got, kind, _ := tb.LookupKind("known"); got != "up-known" || kind != MatchExact {
			t.Errorf("命中 %q (kind=%v)，期望 up-known/exact", got, kind)
		}
	})

	t.Run("未配兜底时保持未命中", func(t *testing.T) {
		tb := FromStatic(map[string][]string{"known": {"up"}}, nil)
		if _, _, ok := tb.LookupKind("unknown"); ok {
			t.Error("未配兜底却报告命中")
		}
	})

	t.Run("兜底支持权重", func(t *testing.T) {
		tb := FromStatic(nil, []string{"a", "a", "b"})
		counts := map[string]int{}
		for i := 0; i < 3000; i++ {
			got, _, _ := tb.LookupKind("x")
			counts[got]++
		}
		ratio := float64(counts["a"]) / float64(counts["b"])
		if ratio < 1.6 || ratio > 2.5 {
			t.Errorf("a:b = %.2f:1，期望约 2:1（a=%d b=%d）", ratio, counts["a"], counts["b"])
		}
	})
}

// 通配命中不算「已声明」：否则一条 catch-all 就放开了指标基数限制。
func TestDeclaredExcludesWildcard(t *testing.T) {
	tb := FromStatic(map[string][]string{
		"exact-one": {"up"},
		"pre-*":     {"up"},
	}, []string{"fb"})

	if !tb.Declared("exact-one") {
		t.Error("精确声明的模型应报告已声明")
	}
	if tb.Declared("pre-anything") {
		t.Error("通配命中不应算已声明（会放开指标基数）")
	}
	if tb.Declared("unrelated") {
		t.Error("兜底命中不应算已声明")
	}
}

// 只有通配或只有兜底的表同样可用，不能被 Empty 误判。
func TestEmptyConsidersAllRuleKinds(t *testing.T) {
	if FromStatic(map[string][]string{"a-*": {"up"}}, nil).Empty() {
		t.Error("仅含通配规则的表被误判为空")
	}
	if FromStatic(nil, []string{"fb"}).Empty() {
		t.Error("仅含兜底的表被误判为空")
	}
	if !FromStatic(nil, nil).Empty() {
		t.Error("真正空的表未被识别")
	}
}

// Header 里也应支持通配语法。
func TestParseWildcard(t *testing.T) {
	tb := Parse("claude-*:up-claude;exact:up-exact")
	if got, kind, _ := tb.LookupKind("claude-3"); got != "up-claude" || kind != MatchWildcard {
		t.Errorf("命中 %q (kind=%v)，期望 up-claude/wildcard", got, kind)
	}
	if got, kind, _ := tb.LookupKind("exact"); got != "up-exact" || kind != MatchExact {
		t.Errorf("命中 %q (kind=%v)，期望 up-exact/exact", got, kind)
	}
}

// 通配匹配在热路径上，不应产生分配。
func BenchmarkWildcardLookupMiss(b *testing.B) {
	tb := FromStatic(map[string][]string{
		"claude-*": {"up1"}, "*-flash": {"up2"}, "gpt-*-turbo": {"up3"},
	}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Lookup("some-unrelated-model")
	}
}

func BenchmarkWildcardLookupHit(b *testing.B) {
	tb := FromStatic(map[string][]string{
		"claude-*": {"up1"}, "*-flash": {"up2"}, "gpt-*-turbo": {"up3"},
	}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Lookup("claude-3-5-sonnet")
	}
}

func BenchmarkExactLookupHit(b *testing.B) {
	tb := FromStatic(map[string][]string{
		"claude-*": {"up1"}, "exact-model": {"up-exact"},
	}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tb.Lookup("exact-model")
	}
}
