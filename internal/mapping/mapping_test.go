package mapping

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want map[string][]string
	}{
		{"空", "", map[string][]string{}},
		{"单条", "a:b", map[string][]string{"a": {"b"}}},
		{
			"标准三条",
			"deepseek-pro:deepseek-v3;deepseek-flash:deepseek-lite;deepseek-reasoner:deepseek-r1",
			map[string][]string{
				"deepseek-pro":      {"deepseek-v3"},
				"deepseek-flash":    {"deepseek-lite"},
				"deepseek-reasoner": {"deepseek-r1"},
			},
		},
		{
			"多对一",
			"a:x;b:x",
			map[string][]string{"a": {"x"}, "b": {"x"}},
		},
		{
			"一对多权重",
			"a:x;a:y;a:x",
			map[string][]string{"a": {"x", "y", "x"}},
		},
		{"带空格", " a : b ; c : d ", map[string][]string{"a": {"b"}, "c": {"d"}}},
		{"尾随分号", "a:b;", map[string][]string{"a": {"b"}}},
		{"非法片段不影响合法规则", "a:b;garbage;:x;y:;c:d", map[string][]string{"a": {"b"}, "c": {"d"}}},
		{"上游名含冒号", "a:ep:20240101", map[string][]string{"a": {"ep:20240101"}}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Parse(c.raw)
			if got.Size() != len(c.want) {
				t.Fatalf("size = %d, want %d (%v)", got.Size(), len(c.want), got.m)
			}
			for k, want := range c.want {
				gv := got.Targets(k)
				if len(gv) != len(want) {
					t.Fatalf("key %q: got %v, want %v", k, gv, want)
				}
				for i := range want {
					if gv[i] != want[i] {
						t.Fatalf("key %q[%d]: got %q, want %q", k, i, gv[i], want[i])
					}
				}
			}
		})
	}
}

func TestLookupDistribution(t *testing.T) {
	// 2 条 v3 + 1 条 r1 => 期望约 2:1
	tbl := Parse("m:v3;m:r1;m:v3")
	counts := map[string]int{}
	const n = 30000
	for i := 0; i < n; i++ {
		got, ok := tbl.Lookup("m")
		if !ok {
			t.Fatal("lookup 应该命中")
		}
		counts[got]++
	}
	ratio := float64(counts["v3"]) / float64(counts["r1"])
	if ratio < 1.7 || ratio > 2.3 {
		t.Fatalf("分流比例 %v 偏离预期 2.0（v3=%d r1=%d）", ratio, counts["v3"], counts["r1"])
	}
}

func TestLookupMiss(t *testing.T) {
	tbl := Parse("a:b")
	if _, ok := tbl.Lookup("zzz"); ok {
		t.Fatal("未声明的模型不应命中")
	}
	var nilTbl *Table
	if _, ok := nilTbl.Lookup("a"); ok {
		t.Fatal("nil 表不应命中")
	}
}

// 非法片段必须被记录而非静默丢弃：写错的声明退化成透传会让上游收到
// 对外模型名报 404，且看板上与「故意不配映射」完全同形。
func TestParseRecordsInvalidSegments(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		invalid int
	}{
		{"全部合法", "a:b;c:d", 0},
		{"缺冒号", "a:b;garbage", 1},
		{"缺上游", "a:b;y:", 1},
		{"缺对外名", "a:b;:x", 1},
		{"全角分号粘连", "glm*:g1；deepseek*:d1", 1}, // ；是 U+FF1B，整段是一条非法规则
		{"全角冒号", "glm*：g1", 1},                // ：是 U+FF1A，找不到 ASCII 冒号
		{"空片段合法", "a:b;;c:d;", 0},
		{"条数上限", "x;y;z;w;v;u", maxInvalid},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Parse(c.raw)
			if len(got.Invalid()) != c.invalid {
				t.Errorf("Invalid() = %v，期望 %d 条", got.Invalid(), c.invalid)
			}
		})
	}

	t.Run("超长片段被截断", func(t *testing.T) {
		long := strings.Repeat("x", 300)
		got := Parse(long)
		if inv := got.Invalid(); len(inv) != 1 || len(inv[0]) > maxInvalidLen+3 {
			t.Errorf("超长非法片段应截断到 %d+3，实际 %v", maxInvalidLen, inv)
		}
	})
}

// 生产事故回归：X-Model-Map 里的通配规则必须命中带点号版本名。
// 线上 glm-5.2 报 404 的排查中，此形态是首要怀疑对象，锁死为契约。
func TestProdWildcardHeaderForm(t *testing.T) {
	raw := "glm*:glm-5-2-260617;deepseek*:deepseek-v4-flash-ga-260731;" +
		"doubao-seed*:doubao-seed-2-0-pro-260215;doubao-seed*:doubao-seed-2-0-lite-260428"
	tb := Parse(raw)
	if bad := tb.Invalid(); len(bad) > 0 {
		t.Fatalf("生产 Header 应完全合法，实际 invalid=%v", bad)
	}

	for _, m := range []string{"glm-5.1", "glm-5.2", "glm-5.3", "glm-5", "glm5"} {
		up, kind, ok := tb.LookupKind(m)
		if !ok || up != "glm-5-2-260617" || kind != MatchWildcard {
			t.Errorf("%s -> (%q, %v, %v)，期望 glm-5-2-260617/wildcard", m, up, kind, ok)
		}
	}
	// doubao-seed* 声明两次 => 两个候选等权随机，且都必须在候选集内。
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		up, _, ok := tb.LookupKind("doubao-seed-evolving")
		if !ok {
			t.Fatal("doubao-seed-evolving 应命中 doubao-seed*")
		}
		seen[up] = true
	}
	for up := range seen {
		if up != "doubao-seed-2-0-pro-260215" && up != "doubao-seed-2-0-lite-260428" {
			t.Errorf("doubao-seed* 命中了候选集外的 %q", up)
		}
	}
	// 大小写：通配匹配是字节精确的，GLM-5.2 不该命中 glm*（模型名区分大小写）。
	if _, _, ok := tb.LookupKind("GLM-5.2"); ok {
		t.Error("通配匹配应保持字节精确，GLM-5.2 不应命中 glm*")
	}
}

func TestCacheConcurrent(t *testing.T) {
	c := NewCache(64)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			raw := fmt.Sprintf("m%d:u%d", i%8, i%8)
			for j := 0; j < 500; j++ {
				tbl := c.Get(raw)
				if got, ok := tbl.Lookup(fmt.Sprintf("m%d", i%8)); !ok || got != fmt.Sprintf("u%d", i%8) {
					t.Errorf("并发读取结果错误: %q %v", got, ok)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestCacheEviction(t *testing.T) {
	c := NewCache(16) // 每分片 8
	for i := 0; i < 5000; i++ {
		raw := fmt.Sprintf("k%d:v%d", i, i)
		tbl := c.Get(raw)
		if _, ok := tbl.Lookup(fmt.Sprintf("k%d", i)); !ok {
			t.Fatalf("第 %d 次未命中", i)
		}
	}
}

func BenchmarkParse(b *testing.B) {
	raw := "deepseek-pro:deepseek-v3;deepseek-flash:deepseek-lite;deepseek-reasoner:deepseek-r1"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Parse(raw)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	raw := "deepseek-pro:deepseek-v3;deepseek-flash:deepseek-lite;deepseek-reasoner:deepseek-r1"
	c := NewCache(1024)
	c.Get(raw)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := c.Get(raw)
			_, _ = t.Lookup("deepseek-pro")
		}
	})
}
