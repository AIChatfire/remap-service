package upstream

import (
	"context"
	"net/url"
	"strconv"
	"time"
)

// 超时的两种成因。它们的处置方向完全不同，混在一起会把排查带偏：
// 首字节超时指向上游排队或模型冷启动（该调大 UPSTREAM_FIRST_BYTE_TIMEOUT），
// 总超时指向单次生成确实太长（该调大 UPSTREAM_TIMEOUT 或改用流式）。
const (
	// TimeoutKindFirstByte：响应头在 first_byte_timeout 内没到。
	TimeoutKindFirstByte = "first_byte"
	// TimeoutKindTotal：响应头已到，整体时长超过总超时上限。
	TimeoutKindTotal = "total"
)

// TimeoutError 是带上下文的超时错误。
//
// 存在的理由：裸的 context.DeadlineExceeded 是全局单例，Error() 恒为
// "context deadline exceeded" 且 Unwrap() 为 nil。它落到 Logfire 上就是
// 一条无信息量的异常 —— 看不出超的是首字节还是总时长、阈值是多少、
// 打的哪个上游。这些字段在错误产生的瞬间全都在手上，不带出去就永久丢失。
//
// 实现 Is(context.DeadlineExceeded)，因此上层所有
// errors.Is(err, context.DeadlineExceeded) 的判断保持原样生效
// （classifyUpstreamError 仍会给出 504 upstream timeout）。
type TimeoutError struct {
	// Kind 是超时成因，取 TimeoutKind* 常量。
	Kind string
	// Limit 是被突破的阈值。
	Limit time.Duration
	// Elapsed 是实际耗时。它与 Limit 明显不符时说明超时来自
	// 父 context（如客户端自带 deadline），而非本客户端的配置。
	Elapsed time.Duration
	// URL 是上游端点，已剥掉 query 与 userinfo。凭据只走 header，
	// 但仍按最小暴露原则处理，避免日后有人把 token 塞进 query 时静默泄漏。
	URL string
	// Stream 标记是否流式请求。流式的总超时上限被刻意放宽到 30 分钟，
	// 不带这个标记会让人对着 120s 的配置值困惑。
	Stream bool
}

func (e *TimeoutError) Error() string {
	var b []byte
	b = append(b, "upstream "...)
	b = append(b, e.Kind...)
	b = append(b, " timeout after "...)
	b = append(b, e.Elapsed.Round(time.Millisecond).String()...)
	b = append(b, " (limit "...)
	b = append(b, e.Limit.String()...)
	b = append(b, ')')
	if e.URL != "" {
		b = append(b, ": "...)
		b = append(b, e.URL...)
	}
	if e.Stream {
		b = append(b, " [stream]"...)
	}
	return string(b)
}

// Is 让 errors.Is(err, context.DeadlineExceeded) 继续成立。
//
// 这是本类型能安全替换哨兵值的前提：上层的分类逻辑
// （classifyUpstreamError、isClientGone）一行都不用改。
func (e *TimeoutError) Is(target error) bool {
	return target == context.DeadlineExceeded
}

// Unwrap 返回哨兵值，让 rootCause 能下潜到底。
func (e *TimeoutError) Unwrap() error { return context.DeadlineExceeded }

// Attrs 返回可直接上报的结构化字段。
//
// 与 Error() 并存的理由：字符串适合人读，键值适合看板筛选与聚合
// （「按 kind 分组统计超时」无法从字符串里做）。
func (e *TimeoutError) Attrs() map[string]string {
	m := map[string]string{
		"gateway.timeout.kind":       e.Kind,
		"gateway.timeout.limit_ms":   strconv.FormatInt(e.Limit.Milliseconds(), 10),
		"gateway.timeout.elapsed_ms": strconv.FormatInt(e.Elapsed.Milliseconds(), 10),
	}
	if e.URL != "" {
		m["gateway.timeout.url"] = e.URL
	}
	return m
}

// safeURL 去掉 query 与 userinfo，只保留 scheme://host/path。
func safeURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	c := *u
	c.RawQuery, c.ForceQuery, c.Fragment, c.RawFragment, c.User = "", false, "", "", nil
	return c.String()
}
