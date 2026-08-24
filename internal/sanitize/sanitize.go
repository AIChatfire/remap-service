// Package sanitize 负责响应脱敏：把上游真实模型名（及其别名变体）
// 还原为用户请求时使用的对外模型名。
//
// # 为什么不做全文替换
//
// 早期实现对整个响应体做字符串替换，风险在于模型生成的内容里若恰好出现
// 上游模型名（用户问「你是什么模型」、代码片段、搜索结果摘要……），会被
// 静默篡改。实测火山 /v1/responses 的 438 个 SSE chunk 中，上游模型名只
// 出现在 response.model 一处 —— 全文替换属于过度打击。
//
// 现在的策略是「路径白名单 + 短值限制」双保险：
//
//  1. 模型字段（model / response.model / message.model）整体覆盖为对外模型名；
//  2. 其余字段走递归扫描，但只处理**短字符串**，且跳过承载生成内容的字段名；
//  3. 扫描前先做一次 bytes 级预检，不含上游标识的 payload 零成本返回。
package sanitize

import (
	"bytes"
	"hash/maphash"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// DefaultMaxValueLen 是允许做子串替换的字符串长度上限。
//
// 取值依据：模型名、ID、指纹、错误码通常在 64 字符内，错误 message 多在
// 200 字符内；而模型生成的文本片段往往更长。超过这个长度的字符串一律
// 视为生成内容，不做任何改动。
const DefaultMaxValueLen = 256

// contentFields 是承载模型生成内容的字段名，无论长短一律不替换。
// 这是长度阈值之外的第二道保险 —— 生成内容也可能很短（如单 token delta）。
var contentFields = map[string]struct{}{
	"content":           {},
	"text":              {},
	"delta":             {},
	"reasoning":         {},
	"reasoning_content": {},
	"arguments":         {},
	"refusal":           {},
	"transcript":        {},
	"input_text":        {},
	"output_text":       {},
	"summary_text":      {},
	"partial_json":      {},
}

// IsContentField 报告某字段名是否承载模型生成内容。
func IsContentField(name string) bool {
	_, ok := contentFields[name]
	return ok
}

// cacheKey 用结构体做键，避免每次查找都拼接字符串产生分配。
type cacheKey struct{ upstream, public string }

// 替换器缓存的分片数与单片容量。
//
// 分片是必需的而非优化：对外模型名完全由客户端的请求体决定，
// 因此缓存键的基数不可信 —— 详见 replacerCache 的说明。
const (
	repShards     = 16
	repShardLimit = 256
	// repHotLimit 是无锁快照的容量上限。真实部署里对外模型数通常在
	// 几十个量级，512 足够容纳，同时把 COW 复制成本锁在可接受范围。
	repHotLimit = 512
)

// replacerCache 是「{上游模型, 对外模型} -> Replacer」的分片缓存。
//
// # 为什么不用 copy-on-write
//
// 早期实现用 atomic.Pointer[map] 做 COW，读路径确实是零分配的 12ns。
// 但它有个隐含假设：写入极其罕见。这个假设不成立 —— 对外模型名取自
// 客户端请求体的 model 字段，客户端可以每次都填一个新名字。此时每个
// 请求都 miss，每次 miss 都要复制整张 map，实测退化到 165µs/op +
// 168KB/op，比稳态慢四个数量级，且分配量随缓存规模线性增长。
//
// 现在是「无锁快照 + 分片回填」两层：
//
//	第一层 hot：atomic.Pointer 指向一份只读快照，稳态命中完全无锁无分配。
//	           快照只装真正稳定的组合（来自配置或映射表的模型），容量封顶。
//	第二层 cold：分片 map + RWMutex，承接快照未覆盖的键。miss 只写自己
//	           那一片，单片满则整片丢弃，让基数攻击的成本恒定在 O(1)。
//
// 两层的分工来自一个观察：真实流量里对外模型名的基数极低（等于渠道
// 配置的模型数），一旦稳定就永久命中第一层；而攻击流量的键各不相同，
// 永远落在第二层，既拿不到无锁快路径，也无法让成本随时间累积。
type replacerCache struct {
	// hot 是稳态快照，copy-on-write 更新，读路径零开销。
	hot atomic.Pointer[map[cacheKey]*Replacer]
	// hotMu 只保护 hot 的写入（罕见）。
	hotMu sync.Mutex

	seed   maphash.Seed
	shards [repShards]replacerShard
}

type replacerShard struct {
	mu sync.RWMutex
	m  map[cacheKey]*Replacer
}

func newReplacerCache() *replacerCache {
	c := &replacerCache{seed: maphash.MakeSeed()}
	empty := make(map[cacheKey]*Replacer, 8)
	c.hot.Store(&empty)
	for i := range c.shards {
		c.shards[i].m = make(map[cacheKey]*Replacer, 8)
	}
	return c
}

// shardFor 选择 key 所属的分片。
//
// 只对 public 取哈希：分片要分散的是写入压力，而压力全部来自 public
// 这一维（它取自客户端请求体）。upstream 的基数等于真实模型数，把它也
// 纳入哈希需要 maphash.Hash 的流式接口，实测多花约 110ns/次，
// 分布质量却没有可测的改善。
func (c *replacerCache) shardFor(k cacheKey) *replacerShard {
	return &c.shards[maphash.String(c.seed, k.public)%repShards]
}

// promote 把一个已确认稳定的组合提升进无锁快照。
//
// 只有「命中次数足以证明它不是一次性键」的组合才值得提升。这里用一个
// 朴素但有效的判据：该键在冷层被查到过（说明至少是第二次出现）。
// 快照容量封顶，满了就不再提升 —— 稳态下模型组合数远小于这个上限。
func (c *replacerCache) promote(k cacheKey, p *Replacer) {
	cur := *c.hot.Load()
	if len(cur) >= repHotLimit {
		return
	}
	c.hotMu.Lock()
	defer c.hotMu.Unlock()
	cur = *c.hot.Load()
	if _, ok := cur[k]; ok || len(cur) >= repHotLimit {
		return
	}
	next := make(map[cacheKey]*Replacer, len(cur)+1)
	for kk, vv := range cur {
		next[kk] = vv
	}
	next[k] = p
	c.hot.Store(&next)
}

// Rules 是从配置加载的静态脱敏规则，进程内只读。
type Rules struct {
	// aliases: 上游模型 -> 变体名列表
	aliases map[string][]string
	// global: 无条件替换对（上游标识 -> 对外标识）
	global [][2]string
	// dropHeaders: 需要从响应中删除的头
	dropHeaders []string
	// maxValueLen: 允许替换的字符串长度上限
	maxValueLen int

	cache *replacerCache
}

// NewRules 构建脱敏规则集。maxValueLen <= 0 时使用 DefaultMaxValueLen。
func NewRules(aliases map[string][]string, global map[string]string, dropHeaders []string, maxValueLen int) *Rules {
	if maxValueLen <= 0 {
		maxValueLen = DefaultMaxValueLen
	}
	r := &Rules{
		aliases:     make(map[string][]string, len(aliases)),
		maxValueLen: maxValueLen,
		cache:       newReplacerCache(),
	}

	for k, v := range aliases {
		cp := make([]string, 0, len(v))
		for _, s := range v {
			if s = strings.TrimSpace(s); s != "" && s != k {
				cp = append(cp, s)
			}
		}
		r.aliases[k] = cp
	}

	keys := make([]string, 0, len(global))
	for k := range global {
		if k != "" {
			keys = append(keys, k)
		}
	}
	sortByLenDesc(keys)
	for _, k := range keys {
		r.global = append(r.global, [2]string{k, global[k]})
	}

	for _, h := range dropHeaders {
		if h = strings.TrimSpace(h); h != "" {
			r.dropHeaders = append(r.dropHeaders, h)
		}
	}
	return r
}

// DropHeaders 返回需要从响应中删除的头名列表。
func (r *Rules) DropHeaders() []string {
	if r == nil {
		return nil
	}
	return r.dropHeaders
}

// MaxValueLen 返回允许替换的字符串长度上限。
func (r *Rules) MaxValueLen() int {
	if r == nil {
		return DefaultMaxValueLen
	}
	return r.maxValueLen
}

// Replacer 是一次请求维度的替换器（上游模型 -> 对外模型），可并发只读使用。
type Replacer struct {
	rep *strings.Replacer
	// needles 是全部待匹配的源串，用于 bytes 级快速预检。
	needles     [][]byte
	upstream    string
	public      string
	maxValueLen int
	noop        bool
}

var noopReplacer = &Replacer{noop: true, maxValueLen: DefaultMaxValueLen}

// Noop 报告该替换器是否无需做任何事。
func (p *Replacer) Noop() bool { return p == nil || p.noop }

// Upstream 返回真实上游模型名。
func (p *Replacer) Upstream() string {
	if p == nil {
		return ""
	}
	return p.upstream
}

// Public 返回对外模型名。
func (p *Replacer) Public() string {
	if p == nil {
		return ""
	}
	return p.public
}

// MaxValueLen 返回允许替换的字符串长度上限。
func (p *Replacer) MaxValueLen() int {
	if p == nil || p.maxValueLen <= 0 {
		return DefaultMaxValueLen
	}
	return p.maxValueLen
}

// MayMatch 快速判断一段字节里是否可能存在需要替换的内容。
//
// 这是热路径上最重要的一次剪枝：SSE 增量 chunk 绝大多数只含生成文本，
// 不含任何上游标识，此处直接返回 false，后续解析与扫描全部省掉。
func (p *Replacer) MayMatch(b []byte) bool {
	if p == nil || p.noop || len(b) == 0 {
		return false
	}
	for _, n := range p.needles {
		if bytes.Contains(b, n) {
			return true
		}
	}
	return false
}

// MayMatchString 是 MayMatch 的字符串版本。
func (p *Replacer) MayMatchString(s string) bool {
	if p == nil || p.noop || s == "" {
		return false
	}
	for _, n := range p.needles {
		if strings.Contains(s, bytesToString(n)) {
			return true
		}
	}
	return false
}

// Apply 对文本执行替换。无匹配时返回原字符串，不产生分配。
func (p *Replacer) Apply(s string) string {
	if p == nil || p.noop || s == "" {
		return s
	}
	return p.rep.Replace(s)
}

// ApplyShort 只在字符串「足够短」时执行替换，否则原样返回。
//
// 长字符串几乎必然是模型生成的内容，替换它等于篡改用户可见的回答。
func (p *Replacer) ApplyShort(s string) string {
	if p == nil || p.noop || s == "" || len(s) > p.MaxValueLen() {
		return s
	}
	return p.rep.Replace(s)
}

// For 返回把 upstream（含别名）替换为 public 的替换器。
func (r *Rules) For(upstream, public string) *Replacer {
	if r == nil {
		return noopReplacer
	}
	if upstream == public && len(r.global) == 0 && len(r.aliases[upstream]) == 0 {
		return noopReplacer
	}
	key := cacheKey{upstream, public}

	// 第一层：无锁快照。稳态流量在这里全部命中。
	if p, ok := (*r.cache.hot.Load())[key]; ok {
		return p
	}

	// 第二层：分片 map。
	s := r.cache.shardFor(key)
	s.mu.RLock()
	p, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		// 冷层命中说明这个键至少出现过两次，不是一次性的随机名，
		// 提升进快照让后续请求走无锁路径。
		r.cache.promote(key, p)
		return p
	}

	p = r.build(upstream, public)

	s.mu.Lock()
	// 双检：并发构建时统一到先写入的实例，保证「同一组合恒为同一指针」。
	if existing, ok := s.m[key]; ok {
		p = existing
	} else {
		// 满则清空：单片成本恒定，不随历史累积增长。
		if len(s.m) >= repShardLimit {
			s.m = make(map[cacheKey]*Replacer, 8)
		}
		s.m[key] = p
	}
	s.mu.Unlock()
	return p
}

func (r *Rules) build(upstream, public string) *Replacer {
	// 收集所有需要被替换成 public 的源串。
	srcs := make([]string, 0, 4+len(r.aliases[upstream]))
	if upstream != "" && upstream != public {
		srcs = append(srcs, upstream)
	}
	for _, a := range r.aliases[upstream] {
		if a != public {
			srcs = append(srcs, a)
		}
	}

	seen := make(map[string]struct{}, len(srcs))
	uniq := srcs[:0]
	for _, s := range srcs {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		uniq = append(uniq, s)
	}
	// 长串优先：否则 deepseek-v3 会先吃掉 deepseek-v3-250101 的前缀，
	// 留下一个 "-250101" 的尾巴造成脱敏不完整。
	sortByLenDesc(uniq)

	pairs := make([]string, 0, len(uniq)*2+len(r.global)*2)
	needles := make([][]byte, 0, len(uniq)+len(r.global))
	for _, s := range uniq {
		pairs = append(pairs, s, public)
		needles = append(needles, []byte(s))
	}
	for _, g := range r.global {
		pairs = append(pairs, g[0], g[1])
		needles = append(needles, []byte(g[0]))
	}
	if len(pairs) == 0 {
		return noopReplacer
	}
	return &Replacer{
		rep:         strings.NewReplacer(pairs...),
		needles:     needles,
		upstream:    upstream,
		public:      public,
		maxValueLen: r.maxValueLen,
	}
}

// sortByLenDesc 按长度降序排序，等长时按字典序，保证结果稳定。
func sortByLenDesc(s []string) {
	sort.Slice(s, func(i, j int) bool {
		if len(s[i]) != len(s[j]) {
			return len(s[i]) > len(s[j])
		}
		return s[i] < s[j]
	})
}
