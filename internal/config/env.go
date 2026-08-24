// Package config 从环境变量加载网关配置。
//
// 部署形态统一为 .env —— 容器、systemd、k8s ConfigMap 都能直接消费，
// 不再需要 YAML 解析与配置文件挂载。
//
// 加载顺序：真实环境变量 > .env 文件 > 内置默认值。
// 即已存在的环境变量不会被 .env 覆盖，方便用 -e 临时改参数。
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// LoadDotEnv 读取 .env 文件并注入进程环境（不覆盖已有变量）。
// 文件不存在时静默跳过 —— 纯环境变量部署是合法形态。
func LoadDotEnv(path string) error {
	if path == "" {
		path = ".env"
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	line := 0
	for sc.Scan() {
		line++
		k, v, ok := parseLine(sc.Text())
		if !ok {
			continue
		}
		if k == "" {
			return fmt.Errorf("%s:%d 变量名为空", path, line)
		}
		// 真实环境变量优先，不被文件覆盖。
		// 空值等同未设置 —— 容器编排里常见 `FOO=` 这种占位，
		// 若把它当作有效值会让 .env 里的默认配置整体失效。
		if os.Getenv(k) == "" {
			if err := os.Setenv(k, v); err != nil {
				return fmt.Errorf("%s:%d setenv %s: %w", path, line, k, err)
			}
		}
	}
	return sc.Err()
}

// parseLine 解析一行 KEY=VALUE，支持 export 前缀、# 注释、单/双引号。
func parseLine(raw string) (key, val string, ok bool) {
	s := strings.TrimSpace(raw)
	if s == "" || strings.HasPrefix(s, "#") {
		return "", "", false
	}
	s = strings.TrimPrefix(s, "export ")

	i := strings.IndexByte(s, '=')
	if i < 0 {
		return "", "", false
	}
	key = strings.TrimSpace(s[:i])
	val = strings.TrimSpace(s[i+1:])

	// 引号内的内容原样保留（含 # 与空格）；无引号时行内 # 起注释作用。
	switch {
	case len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"':
		val = unescape(val[1 : len(val)-1])
	case len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'':
		val = val[1 : len(val)-1]
	default:
		if j := strings.Index(val, " #"); j >= 0 {
			val = strings.TrimSpace(val[:j])
		}
	}
	return key, val, true
}

// unescape 处理双引号字符串里的 \n \t \" \\ 转义。
func unescape(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i == len(s)-1 {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch s[i] {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// --- 取值助手 ---

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on", "y":
		return true
	case "0", "false", "no", "off", "n":
		return false
	default:
		return def
	}
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// envIntAllowZero 与 envInt 相同，但把 0 视为有效取值。
//
// 用于 0 有明确语义（「不限制」）而非「未设置」的开关，
// 例如 MAX_INFLIGHT=0 表示放弃并发闸门。
func envIntAllowZero(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

// envBytes 支持 64MB / 32mb / 1048576 这类写法。
func envBytes(key string, def int64) int64 {
	v := strings.TrimSpace(strings.ToUpper(os.Getenv(key)))
	if v == "" {
		return def
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(v, "GB"), strings.HasSuffix(v, "G"):
		mult, v = 1<<30, strings.TrimRight(v, "GB")
	case strings.HasSuffix(v, "MB"), strings.HasSuffix(v, "M"):
		mult, v = 1<<20, strings.TrimRight(v, "MB")
	case strings.HasSuffix(v, "KB"), strings.HasSuffix(v, "K"):
		mult, v = 1<<10, strings.TrimRight(v, "KB")
	case strings.HasSuffix(v, "B"):
		v = strings.TrimRight(v, "B")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || n <= 0 {
		return def
	}
	return n * mult
}

func envDur(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	// 纯数字按秒解释，容忍 UPSTREAM_TIMEOUT=120 这种写法。
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			return f
		}
	}
	return def
}

// envList 解析逗号分隔的列表。
func envList(key string) []string {
	v := os.Getenv(key)
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// envPipeList 解析 `|` 分隔的列表，与 envMapList 的值侧写法保持一致。
//
// 用于 MODEL_MAP_FALLBACK：`v3|v3|r1` 表示按 2:1 权重随机。
func envPipeList(key string) []string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	parts := strings.Split(v, "|")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// envMapList 解析 "k=v1|v2;k2=v3" 形式的「键 -> 列表」映射。
// 用于 SANITIZE_ALIASES 与 MODEL_MAP。
func envMapList(key string) map[string][]string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	out := make(map[string][]string)
	for _, seg := range strings.Split(v, ";") {
		if seg = strings.TrimSpace(seg); seg == "" {
			continue
		}
		i := strings.IndexByte(seg, '=')
		if i <= 0 {
			continue
		}
		k := strings.TrimSpace(seg[:i])
		if k == "" {
			continue
		}
		for _, item := range strings.Split(seg[i+1:], "|") {
			if item = strings.TrimSpace(item); item != "" {
				out[k] = append(out[k], item)
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// envMap 解析 "a=b;c=d" 形式的字符串映射。用于 SANITIZE_REPLACE。
func envMap(key string) map[string]string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return nil
	}
	out := make(map[string]string)
	for _, seg := range strings.Split(v, ";") {
		if seg = strings.TrimSpace(seg); seg == "" {
			continue
		}
		i := strings.IndexByte(seg, '=')
		if i <= 0 {
			continue
		}
		k := strings.TrimSpace(seg[:i])
		val := strings.TrimSpace(seg[i+1:])
		if k != "" && val != "" {
			out[k] = val
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var errNoUpstream = errors.New(
	"未配置上游地址：请设置 UPSTREAM_BASE，或由客户端通过 X-Upstream-Base 头指定")
