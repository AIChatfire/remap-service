// Package capability 按「模型能力」维度做故障切换。
//
// 解决的问题：一个渠道的主力模型往往并不全能 —— 它可能不支持识图、
// 不支持音频、不支持工具调用。此前这类请求只能整体失败，或者靠
// MODEL_MAP_FALLBACK 切到一个同样不具备该能力的兜底模型。
//
// 现在的做法是把「能力」提升为一等概念：
//
//	① 从请求体识别本次用到了哪些能力（vision / audio / video / tools / file）；
//	② 上游因该能力报错时，切到请求头声明的**能力专用模型**重试；
//	③ 文档理解另有前置规则 —— 请求体一旦含 file_id 就直接改走文档模型，
//	   不必先撞一次错（多数上游对未知 file_id 的报错并不可靠）。
//
// 对外模型名始终不变：切换只影响发往上游的 model 字段，
// 响应仍由 sanitize 还原为客户端请求时使用的名字。
package capability

import (
	"bytes"
	"hash/maphash"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
)

// Kind 是一项模型能力。
type Kind uint8

// 能力枚举。取值刻意保持低基数，可直接作日志与指标标签。
const (
	None Kind = iota
	Vision
	Audio
	Video
	Tools
	File
	kindCount
)

// String 返回适合做标签的短名。
func (k Kind) String() string {
	switch k {
	case Vision:
		return "vision"
	case Audio:
		return "audio"
	case Video:
		return "video"
	case Tools:
		return "tools"
	case File:
		return "file"
	default:
		return "none"
	}
}

// ParseKind 解析能力名。
//
// 只接受英文键，大小写不敏感、允许首尾空白。每项能力保留少量同义写法，
// 因为 vision/image、tools/function 在不同上游文档里都是常见叫法，
// 强制记住唯一拼写只会制造配置错误。
func ParseKind(s string) (Kind, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "vision", "image":
		return Vision, true
	case "audio":
		return Audio, true
	case "video":
		return Video, true
	case "tools", "tool", "function":
		return Tools, true
	case "file", "document", "doc":
		return File, true
	default:
		return None, false
	}
}

// order 是故障切换时的能力优先级。
//
// 一个请求可能同时用到多种能力（带图 + 带工具）。此时按这里的顺序
// 挑第一个「本次用到 且 已声明专用模型」的能力作为切换目标：
// 多模态输入是硬约束（模型不支持就根本无法处理），而工具调用
// 更多是软失败，因此排在多模态之后。
var order = [...]Kind{Vision, Audio, Video, Tools, File}

// Order 返回故障切换的能力优先级序列。
func Order() []Kind { return order[:] }

// Set 是能力集合，用位掩码表示，零分配传递。
type Set uint8

// Add 返回加入某能力后的集合。
func (s Set) Add(k Kind) Set { return s | (1 << k) }

// Has 报告集合是否包含某能力。
func (s Set) Has(k Kind) bool { return s&(1<<k) != 0 }

// Empty 报告集合是否为空。
func (s Set) Empty() bool { return s == 0 }

// Covers 报告 s 是否已包含 want 的全部能力（用于扫描提前退出）。
func (s Set) Covers(want Set) bool { return s&want == want }

// String 返回逗号分隔的能力名，供日志使用。
func (s Set) String() string {
	if s == 0 {
		return ""
	}
	var b strings.Builder
	for _, k := range order {
		if s.Has(k) {
			if b.Len() > 0 {
				b.WriteByte(',')
			}
			b.WriteString(k.String())
		}
	}
	return b.String()
}

// Map 是「能力 -> 上游模型列表」的声明，创建后只读，可并发共享。
type Map struct {
	m    [kindCount][]string
	want Set
}

// Lookup 返回该能力对应的上游模型。多值时等权随机。
func (m *Map) Lookup(k Kind) (string, bool) {
	if m == nil || k >= kindCount {
		return "", false
	}
	c := m.m[k]
	if len(c) == 0 {
		return "", false
	}
	if len(c) == 1 {
		return c[0], true
	}
	return c[rand.IntN(len(c))], true
}

// Want 返回已声明专用模型的能力集合。
//
// 请求体扫描只针对这个集合 —— 没声明就没有切换目标，
// 扫描它纯属浪费，因此未配置能力映射的部署零额外开销。
func (m *Map) Want() Set {
	if m == nil {
		return 0
	}
	return m.want
}

// Empty 报告是否没有任何能力声明。
func (m *Map) Empty() bool { return m == nil || m.want == 0 }

// Size 返回已声明的能力数量。
func (m *Map) Size() int {
	if m == nil {
		return 0
	}
	n := 0
	for _, k := range order {
		if len(m.m[k]) > 0 {
			n++
		}
	}
	return n
}

// Parse 解析能力映射声明。
//
// 格式：<能力>:<上游模型>;<能力>:<上游模型>;...
// 同一能力可出现多次，出现次数即权重（2 条 A + 1 条 B => 2:1）。
// 非法片段（未知能力名、缺冒号、任一侧为空）被静默跳过 ——
// 单条书写错误不应让整份声明失效。
func Parse(raw string) *Map {
	if strings.TrimSpace(raw) == "" {
		return &Map{}
	}
	out := &Map{}
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
		// 从左往右找第一个冒号：能力名不含冒号，
		// 上游模型名理论上可能含（如火山的 ep:xxx）。
		i := strings.IndexByte(seg, ':')
		if i <= 0 || i == len(seg)-1 {
			continue
		}
		k, ok := ParseKind(seg[:i])
		if !ok {
			continue
		}
		model := strings.TrimSpace(seg[i+1:])
		if model == "" {
			continue
		}
		out.m[k] = append(out.m[k], model)
		out.want = out.want.Add(k)
	}
	return out
}

// FromStatic 由静态配置构建能力映射（请求头缺失时的兜底）。
func FromStatic(src map[string][]string) *Map {
	out := &Map{}
	for name, models := range src {
		k, ok := ParseKind(name)
		if !ok {
			continue
		}
		for _, s := range models {
			if s = strings.TrimSpace(s); s != "" {
				out.m[k] = append(out.m[k], s)
				out.want = out.want.Add(k)
			}
		}
	}
	return out
}

// ---------- 请求体能力识别 ----------

// maxScanDepth 限制递归深度，防御畸形的深层嵌套 JSON。
const maxScanDepth = 12

// coarseNeedles 是粗筛用的子串。
//
// 三种协议表达多模态的方式各不相同，但无一例外都靠 content 数组里的
// `type` 字段区分，工具调用则固定在顶层 tools/functions。因此
// 不含这几个词的请求体（纯文本对话，占绝大多数）可以零成本跳过解析。
var coarseNeedles = [][]byte{
	[]byte(`"type"`),
	[]byte(`"tools"`),
	[]byte(`"functions"`),
	[]byte(`"tool_choice"`),
}

// Detect 识别请求体用到了哪些能力，只检测 want 中声明过的。
//
// 识别依据（覆盖 OpenAI / Responses / Anthropic 三种协议）：
//
//	识图  content[].type = image_url | input_image | image
//	音频  content[].type = input_audio | audio_url | audio
//	视频  content[].type = video_url | input_video | video
//	文档  content[].type = file（须带 file.file_id）| input_file | document
//	工具  顶层 tools / functions 非空数组，或 content[].type = tool_use
func Detect(body []byte, want Set) Set {
	if want == 0 || len(body) == 0 {
		return 0
	}
	if !coarseMatch(body) {
		return 0
	}

	var got Set
	root := gjson.ParseBytes(body)

	// 工具能力在顶层就能判定，先做 —— 它是最廉价的一项。
	if want.Has(Tools) && hasTools(root) {
		got = got.Add(Tools)
	}
	if got.Covers(want) {
		return got
	}
	scan(root, want, &got, 0)
	return got
}

// coarseMatch 是解析前的字节级粗筛。
func coarseMatch(b []byte) bool {
	for _, n := range coarseNeedles {
		if bytes.Contains(b, n) {
			return true
		}
	}
	return false
}

// hasTools 判断顶层是否声明了可调用工具。
//
// 只认非空数组：`"tools":[]` 是客户端 SDK 常见的默认值，
// 把它当作「用到了工具调用」会让大量普通对话被误切模型。
func hasTools(root gjson.Result) bool {
	for _, p := range [...]string{"tools", "functions"} {
		if r := root.Get(p); r.IsArray() && len(r.Array()) > 0 {
			return true
		}
	}
	// tool_choice 显式指定具体函数时同样属于工具调用；
	// "none" / "auto" 只是默认策略，不算。
	if r := root.Get("tool_choice"); r.Exists() {
		switch {
		case r.IsObject():
			return true
		case r.Type == gjson.String && r.Str != "none" && r.Str != "auto":
			return true
		}
	}
	return false
}

// kindOfType 把 content 元素的 type 值映射到能力。
func kindOfType(t string) Kind {
	switch t {
	case "image_url", "input_image", "image":
		return Vision
	case "input_audio", "audio_url", "audio":
		return Audio
	case "video_url", "input_video", "video":
		return Video
	case "file", "input_file", "document":
		return File
	case "tool_use", "tool_result", "function_call":
		return Tools
	default:
		return None
	}
}

// scan 递归查找 content 元素的 type 字段。
//
// 不预设 messages / input / content 的具体路径 —— 三种协议的嵌套结构
// 各不相同，且还在演进。递归代价可控：粗筛已挡掉纯文本请求，
// 且集合一旦覆盖 want 就立即返回。
func scan(v gjson.Result, want Set, got *Set, depth int) {
	if depth > maxScanDepth || got.Covers(want) {
		return
	}
	if !v.IsObject() && !v.IsArray() {
		return
	}

	if v.IsObject() {
		if t := v.Get("type"); t.Type == gjson.String {
			if k := kindOfType(t.Str); k != None && want.Has(k) && !got.Has(k) {
				if k != File || isFileRef(v) {
					*got = got.Add(k)
					if got.Covers(want) {
						return
					}
				}
			}
		}
	}

	v.ForEach(func(_, item gjson.Result) bool {
		scan(item, want, got, depth+1)
		return !got.Covers(want)
	})
}

// isFileRef 确认 type=file 的元素确实携带文件引用。
//
// 需求明确要求识别 {"type":"file","file":{"file_id":"…"}}；
// 同时容忍 file_data（内联文件）与 Responses 协议的顶层 file_id 写法。
// 不带任何引用的裸 {"type":"file"} 不算 —— 那多半是别的语义。
func isFileRef(v gjson.Result) bool {
	for _, p := range [...]string{
		"file.file_id", "file.file_data", "file.filename",
		"file_id", "file_data", "source.file_id",
	} {
		if v.Get(p).Exists() {
			return true
		}
	}
	return false
}

// ---------- 请求头解析缓存 ----------

const shards = 16

type shard struct {
	mu    sync.RWMutex
	m     map[string]*Map
	limit int
}

// Cache 是「请求头原始串 -> Map」的分片缓存。
//
// 与 mapping.Cache 同构：同一渠道的头值在绝大多数请求中完全相同，
// 缓存把解析成本摊薄到接近零；满则整片丢弃，让基数攻击的成本恒定。
type Cache struct {
	seed   maphash.Seed
	shards [shards]shard
}

// NewCache 创建容量约为 size 的缓存。
func NewCache(size int) *Cache {
	if size <= 0 {
		size = 256
	}
	per := size / shards
	if per < 8 {
		per = 8
	}
	c := &Cache{seed: maphash.MakeSeed()}
	for i := range c.shards {
		c.shards[i].m = make(map[string]*Map, per)
		c.shards[i].limit = per
	}
	return c
}

// Get 返回 raw 对应的能力映射，未命中时解析并写入缓存。
func (c *Cache) Get(raw string) *Map {
	if raw == "" {
		return &Map{}
	}
	s := &c.shards[maphash.String(c.seed, raw)%shards]

	s.mu.RLock()
	m, ok := s.m[raw]
	s.mu.RUnlock()
	if ok {
		return m
	}

	m = Parse(raw)
	s.mu.Lock()
	if len(s.m) >= s.limit {
		s.m = make(map[string]*Map, s.limit)
	}
	s.m[raw] = m
	s.mu.Unlock()
	return m
}
