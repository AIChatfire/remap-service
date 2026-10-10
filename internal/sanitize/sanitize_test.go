package sanitize

import (
	"strconv"
	"strings"
	"sync"
	"testing"
)

func newTestRules() *Rules {
	return NewRules(
		map[string][]string{
			"deepseek-v3": {"deepseek-v3-250101", "deepseek-v3-volc", "DeepSeek-V3"},
			"deepseek-r1": {"deepseek-r1-250120"},
		},
		map[string]string{
			"ark.cn-beijing.volces.com": "api.gateway.local",
			"volcengine":                "gateway",
		},
		[]string{"X-Upstream-Instance"},
		0, // 用默认长度上限
	)
}

func TestReplacerBasic(t *testing.T) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")

	cases := [][2]string{
		{`deepseek-v3`, `deepseek-pro`},
		{`Model deepseek-v3-250101 is overloaded`, `Model deepseek-pro is overloaded`},
		{`chatcmpl-deepseek-v3-abc123`, `chatcmpl-deepseek-pro-abc123`},
		{`no match here`, `no match here`},
	}
	for _, c := range cases {
		if got := p.Apply(c[0]); got != c[1] {
			t.Errorf("Apply(%q) = %q, want %q", c[0], got, c[1])
		}
	}
}

// 长别名必须优先匹配，否则 deepseek-v3 会先吃掉 deepseek-v3-250101 的前缀，
// 留下一个 "-250101" 的尾巴造成脱敏不完整。
func TestLongestAliasWins(t *testing.T) {
	p := newTestRules().For("deepseek-v3", "X")
	got := p.Apply("deepseek-v3-250101")
	if got != "X" {
		t.Fatalf("长别名未优先匹配: got %q, want %q", got, "X")
	}
	if strings.Contains(got, "250101") {
		t.Fatalf("脱敏残留版本号: %q", got)
	}
}

func TestGlobalReplacement(t *testing.T) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")
	got := p.Apply("https://ark.cn-beijing.volces.com/api/v3")
	want := "https://api.gateway.local/api/v3"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// 核心防线：超长字符串一律视为模型生成的内容，绝不替换。
func TestApplyShortRejectsLongValues(t *testing.T) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")

	short := "model deepseek-v3 not found"
	if got := p.ApplyShort(short); got != "model deepseek-pro not found" {
		t.Errorf("短字符串应替换: %q", got)
	}

	// 模拟模型回答里提到了上游模型名的长文本
	long := "用户问我是什么模型。" + strings.Repeat("这是一段很长的回答内容。", 30) +
		"其实我底层用的是 deepseek-v3。"
	if len(long) <= p.MaxValueLen() {
		t.Fatalf("测试数据长度 %d 未超过阈值 %d", len(long), p.MaxValueLen())
	}
	if got := p.ApplyShort(long); got != long {
		t.Error("超长字符串被改写了 —— 这会篡改模型生成的内容")
	}
}

func TestMaxValueLenConfigurable(t *testing.T) {
	r := NewRules(nil, map[string]string{"up": "pub"}, nil, 10)
	p := r.For("up", "pub")
	if p.MaxValueLen() != 10 {
		t.Fatalf("MaxValueLen = %d, want 10", p.MaxValueLen())
	}
	if got := p.ApplyShort("up"); got != "pub" {
		t.Errorf("2 字符应替换: %q", got)
	}
	if in := "xxxxxxxxxx up"; p.ApplyShort(in) != in {
		t.Error("13 字符超过阈值 10，不应替换")
	}
}

// 生成内容字段名白名单：即便很短也不能动。
func TestIsContentField(t *testing.T) {
	yes := []string{"content", "text", "delta", "reasoning_content", "arguments",
		"refusal", "partial_json", "output_text"}
	no := []string{"model", "id", "system_fingerprint", "code", "message", "param", "type"}
	for _, f := range yes {
		if !IsContentField(f) {
			t.Errorf("%q 应识别为生成内容字段", f)
		}
	}
	for _, f := range no {
		if IsContentField(f) {
			t.Errorf("%q 不应识别为生成内容字段", f)
		}
	}
}

// MayMatch 是热路径上最重要的剪枝，必须准确。
func TestMayMatch(t *testing.T) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")

	hits := []string{
		`{"model":"deepseek-v3"}`,
		`{"model":"deepseek-v3-250101"}`,
		`{"x":"DeepSeek-V3"}`,
		`{"url":"https://ark.cn-beijing.volces.com"}`,
	}
	for _, s := range hits {
		if !p.MayMatch([]byte(s)) {
			t.Errorf("MayMatch(%q) = false，应为 true", s)
		}
		if !p.MayMatchString(s) {
			t.Errorf("MayMatchString(%q) = false", s)
		}
	}

	misses := []string{
		`{"choices":[{"delta":{"content":"你好"}}]}`,
		`{"model":"deepseek-pro"}`, // 已是对外名
		``,
	}
	for _, s := range misses {
		if p.MayMatch([]byte(s)) {
			t.Errorf("MayMatch(%q) = true，应为 false", s)
		}
	}

	if noopReplacer.MayMatch([]byte("deepseek-v3")) {
		t.Error("noop 替换器不应报告匹配")
	}
}

func TestNoopWhenIdentical(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	p := r.For("m", "m")
	if !p.Noop() {
		t.Fatal("上游与对外模型相同且无别名/全局规则时应为 noop")
	}
	if got := p.Apply("m stays"); got != "m stays" {
		t.Fatalf("noop 替换器不应改动内容: %q", got)
	}
	if got := p.ApplyShort("m stays"); got != "m stays" {
		t.Fatalf("noop ApplyShort 不应改动内容: %q", got)
	}
}

func TestReplacerCached(t *testing.T) {
	r := newTestRules()
	// 分两次取再比指针：命中缓存时必须复用同一个 *Replacer，而不是重建一个
	// 等价实例。两侧存成变量，避免 staticcheck 把它当成恒假比较。
	first := r.For("deepseek-v3", "deepseek-pro")
	second := r.For("deepseek-v3", "deepseek-pro")
	if first != second {
		t.Fatal("相同 (upstream, public) 应复用同一 Replacer 实例")
	}
}

func TestDropHeaders(t *testing.T) {
	hs := newTestRules().DropHeaders()
	if len(hs) != 1 || hs[0] != "X-Upstream-Instance" {
		t.Fatalf("DropHeaders = %v", hs)
	}
}

func TestNilSafety(t *testing.T) {
	var r *Rules
	if r.DropHeaders() != nil {
		t.Error("nil Rules 的 DropHeaders 应为 nil")
	}
	if r.MaxValueLen() != DefaultMaxValueLen {
		t.Error("nil Rules 应返回默认长度上限")
	}
	if !r.For("a", "b").Noop() {
		t.Error("nil Rules 应返回 noop 替换器")
	}

	var p *Replacer
	if !p.Noop() || p.Apply("x") != "x" || p.ApplyShort("x") != "x" || p.MayMatch([]byte("x")) {
		t.Error("nil Replacer 的所有方法都应安全退化")
	}
}

func BenchmarkMayMatchMiss(b *testing.B) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")
	// 典型的 SSE 增量 chunk：只有生成内容，无任何上游标识
	s := []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"delta":{"content":"你好世界"}}]}`)
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	for i := 0; i < b.N; i++ {
		if p.MayMatch(s) {
			b.Fatal("不应命中")
		}
	}
}

func BenchmarkApplyHit(b *testing.B) {
	p := newTestRules().For("deepseek-v3", "deepseek-pro")
	s := `chatcmpl-deepseek-v3-abc123`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = p.ApplyShort(s)
	}
}

func BenchmarkForCached(b *testing.B) {
	r := newTestRules()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = r.For("deepseek-v3", "deepseek-pro")
		}
	})
}

// --- 缓存行为 ---

// 同一组合必须恒为同一实例，否则 Replacer 会被反复重建。
func TestForReturnsSameInstance(t *testing.T) {
	r := newTestRules()
	a := r.For("up-v3", "pub")
	b := r.For("up-v3", "pub")
	if a != b {
		t.Fatal("同一组合返回了不同实例，缓存未生效")
	}
}

// 冷层命中后应提升进无锁快照，让后续请求走零锁路径。
func TestForPromotesToHotSnapshot(t *testing.T) {
	r := newTestRules()
	key := cacheKey{"up-v3", "pub"}

	r.For("up-v3", "pub") // 第 1 次：build + 写冷层
	if _, ok := (*r.cache.hot.Load())[key]; ok {
		t.Fatal("首次出现就进了快照，无法区分一次性键")
	}
	r.For("up-v3", "pub") // 第 2 次：冷层命中 -> 提升
	if _, ok := (*r.cache.hot.Load())[key]; !ok {
		t.Fatal("重复出现的组合未被提升进快照")
	}
}

// 高基数（客户端可控的 model 名）不得让缓存无限增长。
func TestForHighCardinalityBounded(t *testing.T) {
	r := newTestRules()
	for i := 0; i < repShardLimit*repShards*3; i++ {
		r.For("up-v3", "pub-"+strconv.Itoa(i))
	}
	total := 0
	for i := range r.cache.shards {
		s := &r.cache.shards[i]
		s.mu.RLock()
		n := len(s.m)
		s.mu.RUnlock()
		if n > repShardLimit {
			t.Errorf("分片 %d 超出上限：%d > %d", i, n, repShardLimit)
		}
		total += n
	}
	if hot := len(*r.cache.hot.Load()); hot > repHotLimit {
		t.Errorf("快照超出上限：%d > %d", hot, repHotLimit)
	}
	t.Logf("冷层共 %d 条，快照 %d 条（均在上限内）", total, len(*r.cache.hot.Load()))
}

// 并发下缓存不得产生数据竞争或不一致实例（配合 -race 运行）。
func TestForConcurrent(t *testing.T) {
	r := newTestRules()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				// 一半稳定键、一半随机键，混合走两层。
				if j%2 == 0 {
					r.For("up-v3", "pub")
				} else {
					r.For("up-v3", "pub-"+strconv.Itoa(i*1000+j))
				}
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// 系列名变体：上游报错按「模型系列」回显，形态与请求用的全名不同。
//
// 事故（2026-10-10）：X-Model-Map wildcard 把对外名 doubao-seed-2-0-lite-260428
// 映射为上游全名 doubao-seed-2-1-lite-260915；火山 TPM 限流按系列计，
// 429 正文回显的是不带日期后缀的 doubao-seed-2-1-lite —— 词表里只有全名，
// 匹配不上，错误正文原样透给终端用户。
// ---------------------------------------------------------------------------

// 事故形态的直接复现：系列名必须被替换，MayMatch（SanitizeWithReport 的
// 剪枝门）也必须放行 —— 判负的话 scanAndReplace 根本不会跑，照样泄漏。
func TestNumericSuffixVariantCatchesSeriesName(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	p := r.For("doubao-seed-2-1-lite-260915", "doubao-seed-2-0-lite-260428")

	in := "TPM (Tokens Per Minute) limit of the model doubao-seed-2-1-lite is exceeded."
	want := "TPM (Tokens Per Minute) limit of the model doubao-seed-2-0-lite-260428 is exceeded."
	if got := p.Apply(in); got != want {
		t.Fatalf("系列名未被替换:\n got %q\nwant %q", got, want)
	}
	if !p.MayMatchString(in) {
		t.Fatal("MayMatch 对系列名判负 —— SanitizeWithReport 会被剪枝,替换永远不执行")
	}
}

// 全名优先于变体：带日期后缀的形态仍被长规则整体吃掉,不留尾巴。
func TestNumericSuffixVariantLongNameStillWins(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	p := r.For("doubao-seed-2-1-lite-260915", "doubao-seed-2-0-lite-260428")

	if got := p.Apply("quota exhausted for doubao-seed-2-1-lite-260915 tonight"); got != "quota exhausted for doubao-seed-2-0-lite-260428 tonight" {
		t.Fatalf("全名未整体替换: %q", got)
	}
}

// 变体派生只认「日期段」形态（6-8 位纯数字尾段），避免把模型名的
// 正式组成部分误当代号摘掉。
func TestNumericSuffixVariantDoesNotOverreach(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	// 尾段 70b 非纯数字；2024 只有 4 位 —— 均不派生变体。断言对象是
	// 正文里的**派生形态**（llama-3 / gpt-4o）：全名命中是既有职责，
	// 与变体无关。
	for _, tc := range [][2]string{
		{"llama-3-70b", "llama-3"},
		{"gpt-4o-2024", "gpt-4o"},
	} {
		p := r.For(tc[0], "pub")
		if got := p.Apply("about " + tc[1]); got != "about "+tc[1] {
			t.Errorf("For(%q) 不应派生变体 %q,却替换了正文: %q", tc[0], tc[1], got)
		}
	}
	// 6 位与 8 位日期段均应派生。
	for _, tc := range [][2]string{
		{"doubao-seed-2-1-lite-260915", "doubao-seed-2-1-lite"},
		{"claude-3-5-20241120", "claude-3-5"},
	} {
		p := r.For(tc[0], "pub")
		if got := p.Apply("model " + tc[1] + " is over quota"); got != "model pub is over quota" {
			t.Errorf("For(%q): 变体 %q 未被替换: %q", tc[0], tc[1], got)
		}
	}
}

// 派生出的变体恰好等于对外名时不得入表：否则会把已是对外形态的正文
// 再「替换」一次（alreadyTarget 能兜住,但规则本身就是多余的噪声）。
func TestNumericSuffixVariantEqualToPublicIsDropped(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	p := r.For("foo-bar-123456", "foo-bar")

	if got := p.Apply("using foo-bar now"); got != "using foo-bar now" {
		t.Fatalf("对外名形态被改写: %q", got)
	}
	// 全名仍正常替换 —— 目标形态就是对外名本身。
	if got := p.Apply("using foo-bar-123456 now"); got != "using foo-bar now" {
		t.Fatalf("全名未替换: %q", got)
	}
}

// 词表只有系列名时,正文里「系列名 + 新日期段」的形态必须整体替换,不留尾巴。
// 上游随时会给出比映射登记时更新的日期后缀,逐个登记别名追不上;命中后吞掉
// 单个 6-8 位纯数字日期段,把「词表内前缀 + 日期尾巴」一次收干净。
func TestSwallowTrailingDateSegment(t *testing.T) {
	r := NewRules(nil, nil, nil, 0)
	p := r.For("doubao-seed-2-1-lite", "pub")

	if got := p.Apply("model doubao-seed-2-1-lite-260915 is over quota"); got != "model pub is over quota" {
		t.Errorf("日期段尾未吞掉: %q", got)
	}
	if got := p.Apply("doubao-seed-2-1-lite-260915-260916"); got != "pub-260916" {
		t.Errorf("只应吞掉一个日期段: %q", got)
	}
	// 语义后缀不是日期段:保持「替换前缀、保留尾巴」的既有行为,
	// 不把别的模型名也吞进来。
	if got := p.Apply("model doubao-seed-2-1-lite-pro"); got != "model pub-pro" {
		t.Errorf("语义后缀处理意外: %q", got)
	}
	// 已是对外形态时不得误吞:对外名(带自己的日期段)保持原样。
	if got := p.Apply("model pub-260428"); got != "model pub-260428" {
		t.Errorf("对外形态被误改: %q", got)
	}
}
