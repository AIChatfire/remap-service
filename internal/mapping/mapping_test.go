package mapping

import (
	"fmt"
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
		{"非法片段被跳过", "a:b;garbage;:x;y:;c:d", map[string][]string{"a": {"b"}, "c": {"d"}}},
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
