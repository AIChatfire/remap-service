// Package mapping 负责解析 X-Model-Map 头，并按等权重随机选择上游模型。
//
// 格式：<对外模型>:<上游模型>;<对外模型>:<上游模型>;...
// 同一对外模型可出现多次，出现次数即权重（2 条 v3 + 1 条 r1 => 2:1）。
//
// 由于同一渠道的 Header 值在绝大多数请求中完全相同，这里用一个分片
// LRU 缓存把「字符串 -> 解析结果」结果复用，避免每请求重复解析与分配。
package mapping

import (
	"hash/maphash"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
)

// Table 是一份已解析的映射表，创建后只读，可被多个请求并发共享。
type Table struct {
	// m 是精确规则，查找零分配。
	m map[string][]string
	// wild 是通配规则，按具体度降序排列，仅在精确未命中时才遍历。
	wild []wildRule
	// fallback 是全部规则都未命中时的兜底上游模型。
	fallback []string
}

// wildRule 是一条通配规则及其目标上游列表。
type wildRule struct {
	pat     pattern
	targets []string
}

// Lookup 返回该对外模型对应的上游模型。
//
// 匹配顺序（先命中即返回）：
//
//	① 精确规则     deepseek-pro=deepseek-v3
//	② 通配规则     claude-*=claude-3-5-sonnet   （按具体度降序）
//	③ 兜底模型     MODEL_MAP_FALLBACK
//
// 同一规则声明多个上游时按条数等权随机（v3|v3|r1 即 2:1）。
// ok 为 false 表示三级都未命中。
func (t *Table) Lookup(public string) (upstream string, ok bool) {
	if t == nil {
		return "", false
	}
	// ① 精确：热路径，map 查找零分配。
	if cands, hit := t.m[public]; hit && len(cands) > 0 {
		return pick(cands), true
	}
	// ② 通配：规则数通常是个位数，线性扫描优于建 trie。
	//    wild 已按具体度降序排好，第一个命中的就是最具体的。
	for i := range t.wild {
		if t.wild[i].pat.match(public) {
			return pick(t.wild[i].targets), true
		}
	}
	// ③ 兜底。
	if len(t.fallback) > 0 {
		return pick(t.fallback), true
	}
	return "", false
}

// LookupKind 与 Lookup 相同，但额外返回命中的级别，供日志与指标使用。
func (t *Table) LookupKind(public string) (upstream string, kind MatchKind, ok bool) {
	if t == nil {
		return "", MatchNone, false
	}
	if cands, hit := t.m[public]; hit && len(cands) > 0 {
		return pick(cands), MatchExact, true
	}
	for i := range t.wild {
		if t.wild[i].pat.match(public) {
			return pick(t.wild[i].targets), MatchWildcard, true
		}
	}
	if len(t.fallback) > 0 {
		return pick(t.fallback), MatchFallback, true
	}
	return "", MatchNone, false
}

// MatchKind 标识 Lookup 命中的级别。
type MatchKind uint8

// 命中级别，取值刻意做成低基数字符串，可直接作指标标签。
const (
	MatchNone MatchKind = iota
	MatchExact
	MatchWildcard
	MatchFallback
)

// String 返回适合做指标标签的短名。
func (k MatchKind) String() string {
	switch k {
	case MatchExact:
		return "exact"
	case MatchWildcard:
		return "wildcard"
	case MatchFallback:
		return "fallback"
	default:
		return "none"
	}
}

// pick 从候选中等权随机取一个。
func pick(cands []string) string {
	if len(cands) == 1 {
		return cands[0]
	}
	return cands[rand.IntN(len(cands))]
}

// Targets 返回该对外模型的全部候选上游（用于脱敏时构建替换集合）。
func (t *Table) Targets(public string) []string {
	if t == nil {
		return nil
	}
	return t.m[public]
}

// Empty 报告映射表是否完全没有可用规则。
//
// 三类规则都要算进来：只声明了通配规则或只声明了兜底的表同样可用。
// 若只看精确表，Header 里的 `claude-*:xxx` 会被误判为空而整表丢弃。
func (t *Table) Empty() bool {
	return t == nil || (len(t.m) == 0 && len(t.wild) == 0 && len(t.fallback) == 0)
}

// Declared 报告某个对外模型名是否被**精确**声明过。
//
// 用于指标标签的基数控制：model 字段由客户端任意填写，直接做标签会让
// 时间序列数随请求增长。只有精确声明的名字才是「运维已知的有限集合」。
//
// 刻意不含通配命中：`*` 能匹配无穷多个名字，若把通配命中也算「已声明」，
// 配一条 catch-all 就等于放开了基数限制。
func (t *Table) Declared(public string) bool {
	if t == nil || len(t.m) == 0 {
		return false
	}
	_, ok := t.m[public]
	return ok
}

// Size 返回规则总数（精确 + 通配）。
func (t *Table) Size() int {
	if t == nil {
		return 0
	}
	return len(t.m) + len(t.wild)
}

// HasFallback 报告是否配置了兜底模型。
func (t *Table) HasFallback() bool { return t != nil && len(t.fallback) > 0 }

// LookupFallback 直接取一个兜底模型，跳过精确与通配匹配。
// 用于故障切换：此时已知首选上游失败，只需要一个替代目标。
func (t *Table) LookupFallback() (upstream string, kind MatchKind, ok bool) {
	if t == nil || len(t.fallback) == 0 {
		return "", MatchNone, false
	}
	return pick(t.fallback), MatchFallback, true
}

// Parse 解析 X-Model-Map 头的原始值。
// 非法片段（缺少冒号、任一侧为空）被静默跳过，保证单条错误不影响整体。
//
// 对外模型名可含 `*` 通配（如 `claude-*:claude-3-5-sonnet`）。
func Parse(raw string) *Table {
	if strings.TrimSpace(raw) == "" {
		return &Table{}
	}
	m := make(map[string][]string, 8)
	for len(raw) > 0 {
		var seg string
		if i := strings.IndexByte(raw, ';'); i >= 0 {
			seg, raw = raw[:i], raw[i+1:]
		} else {
			seg, raw = raw, ""
		}
		if seg = strings.TrimSpace(seg); seg == "" {
			continue
		}
		// 从右往左找冒号：对外模型名不含冒号，上游模型名理论上可能含（如 ep:xxx）。
		i := strings.IndexByte(seg, ':')
		if i <= 0 || i == len(seg)-1 {
			continue
		}
		pub := strings.TrimSpace(seg[:i])
		up := strings.TrimSpace(seg[i+1:])
		if pub == "" || up == "" {
			continue
		}
		m[pub] = append(m[pub], up)
	}
	return build(m, nil)
}

// FromStatic 由静态配置构建映射表（Header 缺失时的兜底）。
// fallback 是全部规则都未命中时使用的上游模型列表，可为空。
func FromStatic(src map[string][]string, fallback []string) *Table {
	if len(src) == 0 && len(fallback) == 0 {
		return &Table{}
	}
	m := make(map[string][]string, len(src))
	for k, v := range src {
		if k == "" || len(v) == 0 {
			continue
		}
		cp := make([]string, 0, len(v))
		for _, s := range v {
			if s = strings.TrimSpace(s); s != "" {
				cp = append(cp, s)
			}
		}
		if len(cp) > 0 {
			m[k] = cp
		}
	}
	return build(m, fallback)
}

// build 把「规则名 -> 上游列表」拆成精确表与通配表，并预排序通配规则。
//
// 通配规则在构建期一次性编译并排序，Lookup 时只做匹配，不再解析。
func build(m map[string][]string, fallback []string) *Table {
	t := &Table{}

	exact := make(map[string][]string, len(m))
	for k, v := range m {
		if hasWildcard(k) {
			t.wild = append(t.wild, wildRule{pat: compilePattern(k), targets: v})
			continue
		}
		exact[k] = v
	}
	if len(exact) > 0 {
		t.m = exact
	}

	// 按具体度降序：Lookup 取第一个命中即为最具体的规则，
	// 结果不受配置书写顺序影响。等权时按规则名排序保证确定性。
	sort.Slice(t.wild, func(i, j int) bool {
		if t.wild[i].pat.weight != t.wild[j].pat.weight {
			return t.wild[i].pat.weight > t.wild[j].pat.weight
		}
		return t.wild[i].pat.raw < t.wild[j].pat.raw
	})

	for _, s := range fallback {
		if s = strings.TrimSpace(s); s != "" {
			t.fallback = append(t.fallback, s)
		}
	}
	return t
}

const shards = 16

type shard struct {
	mu    sync.RWMutex
	m     map[string]*Table
	limit int
}

// Cache 是「Header 原始串 -> Table」的分片缓存。
// 采用简单的满则清空策略：Header 取值基数极低（等于渠道数），
// 相比 LRU 链表的每次读写指针维护，这里的零开销读路径更划算。
type Cache struct {
	seed   maphash.Seed
	shards [shards]shard
}

// NewCache 创建容量约为 size 的缓存。
func NewCache(size int) *Cache {
	if size <= 0 {
		size = 1024
	}
	per := size / shards
	if per < 8 {
		per = 8
	}
	c := &Cache{seed: maphash.MakeSeed()}
	for i := range c.shards {
		c.shards[i].m = make(map[string]*Table, per)
		c.shards[i].limit = per
	}
	return c
}

// Get 返回 raw 对应的映射表，未命中时解析并写入缓存。
func (c *Cache) Get(raw string) *Table {
	if raw == "" {
		return &Table{}
	}
	s := &c.shards[maphash.String(c.seed, raw)%shards]

	s.mu.RLock()
	t, ok := s.m[raw]
	s.mu.RUnlock()
	if ok {
		return t
	}

	t = Parse(raw)
	s.mu.Lock()
	if len(s.m) >= s.limit {
		s.m = make(map[string]*Table, s.limit)
	}
	s.m[raw] = t
	s.mu.Unlock()
	return t
}
