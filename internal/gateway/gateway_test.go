package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/upstream"
)

// --- 测试脚手架 ---

type capture struct {
	mu     sync.Mutex
	path   string
	body   []byte
	header http.Header
	host   string
}

func (c *capture) snapshot() (string, []byte, http.Header) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.path, c.body, c.header
}

func testObs(t testing.TB) *obs.Provider {
	t.Helper()
	o, err := obs.New(context.Background(), config.Obs{
		LogLevel: "error", LogFormat: "text", LogUpstreamModel: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func testConfig(base string) *config.Config {
	return &config.Config{
		Addr: ":0",
		Upstream: config.Upstream{
			Base:             base,
			AllowBaseHeader:  true,
			Timeout:          30 * time.Second,
			FirstByteTimeout: 10 * time.Second,
		},
		Sanitize: config.Sanitize{
			Aliases:     map[string][]string{"deepseek-v3": {"deepseek-v3-250101", "DeepSeek-V3"}},
			MaxValueLen: 256,
		},
		Limits: config.Limits{
			MaxConns: 1024, MaxBodyBytes: 64 << 20, MaxSanitizeBytes: 32 << 20,
		},
	}
}

func newFixture(t *testing.T, h http.HandlerFunc, tweak func(*config.Config)) (*httptest.Server, *capture) {
	t.Helper()

	cap := &capture{}
	us := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		cap.mu.Lock()
		cap.path, cap.body, cap.header, cap.host = r.URL.Path, b, r.Header.Clone(), r.Host
		cap.mu.Unlock()
		// 把 body 放回：桩函数常需要按请求内容分支（如判断收到哪个模型），
		// 若这里读空了，桩里再读只能拿到 EOF。
		r.Body = io.NopCloser(bytes.NewReader(b))
		h(w, r)
	}))
	t.Cleanup(us.Close)

	cfg := testConfig(us.URL)
	if tweak != nil {
		tweak(cfg)
	}

	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	t.Cleanup(tr.CloseIdleConnections)
	gs := httptest.NewServer(New(cfg, upstream.NewClient(tr, cfg.Upstream), testObs(t)))
	t.Cleanup(gs.Close)
	return gs, cap
}

func post(t *testing.T, srv *httptest.Server, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readAll(t *testing.T, r *http.Response) string {
	t.Helper()
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func okJSON(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
}

// --- 凭据透传 ---

// 客户端的 Key 被提取后按目标协议重新注入：
// 同一个 Bearer 请求打到 /v1/messages 会自动变成 x-api-key。
func TestCredentialPassthroughAcrossProtocols(t *testing.T) {
	cases := []struct {
		name        string
		path        string
		clientHdr   map[string]string
		wantHeader  string
		wantValue   string
		wantAbsent  string
		wantVersion string
	}{
		{
			name:       "OpenAI 客户端 → OpenAI 端点",
			path:       "/v1/chat/completions",
			clientHdr:  map[string]string{"Authorization": "Bearer sk-client-key"},
			wantHeader: "Authorization", wantValue: "Bearer sk-client-key",
			wantAbsent: "X-Api-Key",
		},
		{
			name:       "OpenAI 风格凭据 → Anthropic 端点（自动转换）",
			path:       "/v1/messages",
			clientHdr:  map[string]string{"Authorization": "Bearer sk-client-key"},
			wantHeader: "X-Api-Key", wantValue: "sk-client-key",
			wantAbsent: "Authorization", wantVersion: "2023-06-01",
		},
		{
			name:       "Anthropic 风格凭据 → OpenAI 端点（自动转换）",
			path:       "/v1/chat/completions",
			clientHdr:  map[string]string{"x-api-key": "sk-client-key"},
			wantHeader: "Authorization", wantValue: "Bearer sk-client-key",
			wantAbsent: "X-Api-Key",
		},
		{
			name:       "Anthropic 风格凭据 → Anthropic 端点",
			path:       "/v1/messages",
			clientHdr:  map[string]string{"x-api-key": "sk-client-key"},
			wantHeader: "X-Api-Key", wantValue: "sk-client-key",
			wantAbsent: "Authorization", wantVersion: "2023-06-01",
		},
		{
			name:       "裸 Key（无 Bearer 前缀）",
			path:       "/v1/chat/completions",
			clientHdr:  map[string]string{"Authorization": "sk-client-key"},
			wantHeader: "Authorization", wantValue: "Bearer sk-client-key",
		},
		{
			name:       "Responses 端点",
			path:       "/v1/responses",
			clientHdr:  map[string]string{"Authorization": "Bearer sk-client-key"},
			wantHeader: "Authorization", wantValue: "Bearer sk-client-key",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gs, cap := newFixture(t, okJSON(`{}`), nil)
			readAll(t, post(t, gs, c.path, `{"model":"m"}`, c.clientHdr))

			_, _, hdr := cap.snapshot()
			if got := hdr.Get(c.wantHeader); got != c.wantValue {
				t.Errorf("%s = %q, want %q", c.wantHeader, got, c.wantValue)
			}
			if c.wantAbsent != "" && hdr.Get(c.wantAbsent) != "" {
				t.Errorf("%s 不应存在，实际 %q", c.wantAbsent, hdr.Get(c.wantAbsent))
			}
			if c.wantVersion != "" && hdr.Get("anthropic-version") != c.wantVersion {
				t.Errorf("anthropic-version = %q，协议必需头未注入", hdr.Get("anthropic-version"))
			}
		})
	}
}

// 网关不持有密钥：客户端不带凭据时上游也收不到（除非配了兜底）。
func TestNoCredentialMeansNoInjection(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{}`), nil)
	readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil))

	_, _, hdr := cap.snapshot()
	if v := hdr.Get("Authorization"); v != "" {
		t.Errorf("无客户端凭据时不应注入，实际 %q", v)
	}
}

// UPSTREAM_KEY 作为兜底：客户端没带才用。
func TestUpstreamKeyFallback(t *testing.T) {
	t.Run("客户端未带凭据 → 用兜底", func(t *testing.T) {
		gs, cap := newFixture(t, okJSON(`{}`), func(c *config.Config) {
			c.Upstream.Key = "sk-fallback"
		})
		readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil))
		_, _, hdr := cap.snapshot()
		if got := hdr.Get("Authorization"); got != "Bearer sk-fallback" {
			t.Errorf("Authorization = %q, want Bearer sk-fallback", got)
		}
	})

	t.Run("客户端带了凭据 → 客户端优先", func(t *testing.T) {
		gs, cap := newFixture(t, okJSON(`{}`), func(c *config.Config) {
			c.Upstream.Key = "sk-fallback"
		})
		readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`,
			map[string]string{"Authorization": "Bearer sk-client"}))
		_, _, hdr := cap.snapshot()
		if got := hdr.Get("Authorization"); got != "Bearer sk-client" {
			t.Errorf("Authorization = %q，客户端凭据应优先", got)
		}
	})
}

// 客户端显式提供的协议头不被覆盖。
func TestRequiredHeaderNotOverridden(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{}`), nil)
	readAll(t, post(t, gs, "/v1/messages", `{"model":"m"}`,
		map[string]string{"anthropic-version": "2024-10-22"}))

	_, _, hdr := cap.snapshot()
	if got := hdr.Get("anthropic-version"); got != "2024-10-22" {
		t.Errorf("anthropic-version = %q，不应覆盖客户端的显式取值", got)
	}
}

// --- 上游地址：header > 环境变量 ---

func TestUpstreamBaseFromHeader(t *testing.T) {
	var hitAlt atomic.Bool
	alt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitAlt.Store(true)
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("备用上游收到路径 %q", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer alt.Close()

	gs, cap := newFixture(t, okJSON(`{"from":"default"}`), nil)

	// 带头 → 走 header 指定的地址
	readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`,
		map[string]string{BaseHeader: alt.URL}))
	if !hitAlt.Load() {
		t.Fatal("X-Upstream-Base 未生效")
	}
	// 内部协议头不得转发给上游
	if _, _, hdr := cap.snapshot(); hdr.Get(BaseHeader) != "" {
		t.Error("X-Upstream-Base 不应转发给上游")
	}

	// 不带头 → 回落默认地址
	hitAlt.Store(false)
	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil))
	if hitAlt.Load() {
		t.Error("无头时不应走备用上游")
	}
	if !strings.Contains(body, "default") {
		t.Errorf("未回落默认上游: %s", body)
	}
}

func TestUpstreamBaseHeaderNormalization(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"https://api.openai.com", "https://api.openai.com", true},
		{"https://api.openai.com/", "https://api.openai.com", true},
		{"http://127.0.0.1:8080/api/v3", "http://127.0.0.1:8080/api/v3", true},
		{"api.openai.com", "https://api.openai.com", true}, // 缺 scheme 补 https
		{"ftp://evil.com", "", false},
		{"file:///etc/passwd", "", false},
		{"https://", "", false},
		{"", "", false},
	}
	for _, c := range cases {
		got, ok := normalizeBase(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("normalizeBase(%q) = (%q, %v), want (%q, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

// 非法的 base 头不静默回落 —— 避免请求被发到预期之外的地址。
func TestInvalidBaseHeaderRejected(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("非法 base 头时不应到达上游")
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`,
		map[string]string{BaseHeader: "ftp://evil.com"})
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	readAll(t, resp)
}

func TestBaseHeaderCanBeDisabled(t *testing.T) {
	alt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("禁用后不应走 header 指定的上游")
	}))
	defer alt.Close()

	gs, _ := newFixture(t, okJSON(`{"from":"default"}`), func(c *config.Config) {
		c.Upstream.AllowBaseHeader = false
	})
	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`,
		map[string]string{BaseHeader: alt.URL}))
	if !strings.Contains(body, "default") {
		t.Errorf("禁用头覆盖后应走默认上游: %s", body)
	}
}

// 按协议指定不同上游。
func TestPerProtocolBase(t *testing.T) {
	var hitAlt atomic.Bool
	alt := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitAlt.Store(true)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer alt.Close()

	gs, _ := newFixture(t, okJSON(`{}`), func(c *config.Config) {
		c.Upstream.Protocols = map[string]string{"anthropic": alt.URL}
	})

	readAll(t, post(t, gs, "/v1/messages", `{"model":"m"}`, nil))
	if !hitAlt.Load() {
		t.Fatal("/v1/messages 未路由到 anthropic 专属上游")
	}

	hitAlt.Store(false)
	readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil))
	if hitAlt.Load() {
		t.Error("/v1/chat/completions 不应路由到 anthropic 上游")
	}
}

// header 优先级高于按协议的环境变量。
func TestBaseHeaderBeatsProtocolConfig(t *testing.T) {
	var hitHeader atomic.Bool
	byHeader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitHeader.Store(true)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer byHeader.Close()

	byProto := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("header 应优先于按协议配置")
	}))
	defer byProto.Close()

	gs, _ := newFixture(t, okJSON(`{}`), func(c *config.Config) {
		c.Upstream.Protocols = map[string]string{"anthropic": byProto.URL}
	})
	readAll(t, post(t, gs, "/v1/messages", `{"model":"m"}`,
		map[string]string{BaseHeader: byHeader.URL}))
	if !hitHeader.Load() {
		t.Fatal("header 未生效")
	}
}

// --- 协议自动识别 ---

func TestProtocolDetectedFromPath(t *testing.T) {
	for _, path := range []string{"/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		t.Run(path, func(t *testing.T) {
			gs, cap := newFixture(t, okJSON(`{}`), nil)
			readAll(t, post(t, gs, path, `{"model":"m"}`, nil))
			if upPath, _, _ := cap.snapshot(); upPath != path {
				t.Errorf("上游路径 = %q，动态路由应原样保留", upPath)
			}
		})
	}
}

// --- 请求改写 ---

func TestRequestModelRewritten(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{"id":"chatcmpl-1","model":"deepseek-v3","choices":[]}`), nil)

	body := readAll(t, post(t, gs, "/v1/chat/completions",
		`{"model":"deepseek-pro","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{MapHeader: "deepseek-pro:deepseek-v3"}))

	_, upBody, upHeader := cap.snapshot()
	var got map[string]any
	if err := json.Unmarshal(upBody, &got); err != nil {
		t.Fatal(err)
	}
	if got["model"] != "deepseek-v3" {
		t.Errorf("上游收到 model=%v，期望 deepseek-v3", got["model"])
	}
	if upHeader.Get(MapHeader) != "" {
		t.Error("X-Model-Map 是网关内部协议，不应转发给上游")
	}
	if !strings.Contains(body, `"model":"deepseek-pro"`) {
		t.Errorf("响应未脱敏: %s", body)
	}
	if strings.Contains(body, "deepseek-v3") {
		t.Errorf("上游模型名泄漏: %s", body)
	}
}

func TestUnmappedModelPassthrough(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{"model":"brand-new"}`), nil)
	readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"brand-new"}`,
		map[string]string{MapHeader: "other:x"}))

	if _, upBody, _ := cap.snapshot(); !strings.Contains(string(upBody), `"model":"brand-new"`) {
		t.Errorf("未声明映射的模型应原样透传，实际 %s", upBody)
	}
}

func TestStrictModeRejectsUnmapped(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("strict 模式下不应到达上游")
	}, func(c *config.Config) { c.Mapping.Strict = true })

	resp := post(t, gs, "/v1/chat/completions", `{"model":"ghost"}`,
		map[string]string{MapHeader: "real:x"})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	if body := readAll(t, resp); !strings.Contains(body, "ghost") {
		t.Errorf("错误信息应包含请求的模型名: %s", body)
	}
}

func TestStaticFallbackMapping(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{}`), func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"real-upstream"}}
	})
	readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil))

	if _, upBody, _ := cap.snapshot(); !strings.Contains(string(upBody), `"model":"real-upstream"`) {
		t.Errorf("静态兜底映射未生效: %s", upBody)
	}
}

// --- 响应脱敏 ---

func TestSanitizeAliasesAndFingerprint(t *testing.T) {
	gs, _ := newFixture(t, okJSON(
		`{"id":"chatcmpl-deepseek-v3-xyz","model":"deepseek-v3-250101","system_fingerprint":"fp_DeepSeek-V3","choices":[{"message":{"content":"ok"}}]}`), nil)

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"deepseek-pro"}`,
		map[string]string{MapHeader: "deepseek-pro:deepseek-v3"}))

	for _, leak := range []string{"deepseek-v3", "DeepSeek-V3", "250101"} {
		if strings.Contains(body, leak) {
			t.Errorf("泄漏 %q: %s", leak, body)
		}
	}
	if !strings.Contains(body, `"id":"chatcmpl-deepseek-pro-xyz"`) {
		t.Errorf("id 应替换模型名并保留随机后缀: %s", body)
	}
	if !strings.Contains(body, `"content":"ok"`) {
		t.Errorf("正文内容被破坏: %s", body)
	}
}

// 关键防线：模型生成的内容里提到上游模型名时不得篡改。
func TestGeneratedContentNotTampered(t *testing.T) {
	answer := "我底层使用的是 deepseek-v3 架构。"
	gs, _ := newFixture(t, okJSON(
		`{"model":"deepseek-v3","choices":[{"message":{"role":"assistant","content":"`+answer+`"}}]}`), nil)

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"deepseek-pro"}`,
		map[string]string{MapHeader: "deepseek-pro:deepseek-v3"}))

	if !strings.Contains(body, answer) {
		t.Errorf("模型生成的内容被篡改:\n%s", body)
	}
	if !strings.Contains(body, `"model":"deepseek-pro"`) {
		t.Errorf("model 字段未脱敏: %s", body)
	}
}

func TestErrorBodySanitizedStatusPreserved(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"model deepseek-v3-250101 is rate limited","type":"rate_limit_error","code":"deepseek-v3_quota","param":"model"}}`))
	}, nil)

	resp := post(t, gs, "/v1/chat/completions", `{"model":"deepseek-pro"}`,
		map[string]string{MapHeader: "deepseek-pro:deepseek-v3"})
	body := readAll(t, resp)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429（容灾交给上层）", resp.StatusCode)
	}
	if strings.Contains(body, "deepseek-v3") {
		t.Errorf("错误体泄漏上游模型: %s", body)
	}
	if !strings.Contains(body, "deepseek-pro is rate limited") ||
		!strings.Contains(body, `"code":"deepseek-pro_quota"`) {
		t.Errorf("message/code 未正确脱敏: %s", body)
	}
}

func TestSanitizeOff(t *testing.T) {
	gs, _ := newFixture(t, okJSON(`{"model":"deepseek-v3"}`),
		func(c *config.Config) { c.Sanitize.Off = true })

	body := readAll(t, post(t, gs, "/v1/chat/completions", `{"model":"deepseek-pro"}`,
		map[string]string{MapHeader: "deepseek-pro:deepseek-v3"}))
	if !strings.Contains(body, "deepseek-v3") {
		t.Errorf("关闭脱敏后应原样透传: %s", body)
	}
}

func TestDropResponseHeaders(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Instance", "volc-node-7")
		w.Header().Set("X-Keep-Me", "yes")
		_, _ = w.Write([]byte(`{}`))
	}, func(c *config.Config) {
		c.Sanitize.DropHeaders = []string{"X-Upstream-Instance"}
	})

	resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
	defer resp.Body.Close()
	if resp.Header.Get("X-Upstream-Instance") != "" {
		t.Error("应清除的响应头仍然存在")
	}
	if resp.Header.Get("X-Keep-Me") != "yes" {
		t.Error("其他响应头应保留")
	}
}

// --- SSE ---

func TestSSEStreamSanitizedAndIncremental(t *testing.T) {
	release := make(chan struct{})
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fl := w.(http.Flusher)
		fmt.Fprint(w, `data: {"id":"chatcmpl-deepseek-v3-1","model":"deepseek-v3","choices":[{"delta":{"content":"A"}}]}`+"\n\n")
		fl.Flush()
		<-release // 卡住，验证网关不会缓冲第一块
		fmt.Fprint(w, `data: {"id":"chatcmpl-deepseek-v3-1","model":"deepseek-v3","choices":[{"delta":{"content":"B"}}]}`+"\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
		fl.Flush()
	}, nil)

	req, _ := http.NewRequest(http.MethodPost, gs.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"deepseek-pro","stream":true}`))
	req.Header.Set(MapHeader, "deepseek-pro:deepseek-v3")
	resp, err := gs.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}

	buf := make([]byte, 4096)
	done := make(chan string, 1)
	go func() {
		n, _ := resp.Body.Read(buf)
		done <- string(buf[:n])
	}()
	select {
	case first := <-done:
		if !strings.Contains(first, `"content":"A"`) {
			t.Fatalf("首块内容异常: %q", first)
		}
		if strings.Contains(first, "deepseek-v3") {
			t.Fatalf("首块泄漏上游模型: %q", first)
		}
		if !strings.Contains(first, `"model":"deepseek-pro"`) {
			t.Fatalf("首块未脱敏为对外模型: %q", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("首块被缓冲，未及时下发（流式转发失效）")
	}

	close(release)
	rest, _ := io.ReadAll(resp.Body)
	all := string(rest)
	if !strings.Contains(all, `"content":"B"`) || !strings.Contains(all, "data: [DONE]") {
		t.Errorf("后续块或 [DONE] 丢失: %s", all)
	}
	if strings.Contains(all, "deepseek-v3") {
		t.Errorf("后续块泄漏: %s", all)
	}
}

func TestSSEAnthropicMessages(t *testing.T) {
	gs, cap := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message_start\n")
		fmt.Fprint(w, `data: {"type":"message_start","message":{"id":"msg_up1","model":"claude-upstream"}}`+"\n\n")
		fmt.Fprint(w, "event: content_block_delta\n")
		fmt.Fprint(w, `data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`+"\n\n")
		w.(http.Flusher).Flush()
	}, nil)

	body := readAll(t, post(t, gs, "/v1/messages",
		`{"model":"claude-pub","stream":true,"max_tokens":16}`,
		map[string]string{MapHeader: "claude-pub:claude-upstream"}))

	upPath, upBody, _ := cap.snapshot()
	if upPath != "/v1/messages" {
		t.Errorf("上游路径 = %q", upPath)
	}
	if !strings.Contains(string(upBody), `"model":"claude-upstream"`) {
		t.Errorf("Anthropic 请求体未改写: %s", upBody)
	}
	if strings.Contains(body, "claude-upstream") {
		t.Errorf("Anthropic 流泄漏上游模型: %s", body)
	}
	if !strings.Contains(body, `"model":"claude-pub"`) {
		t.Errorf("message.model 未脱敏: %s", body)
	}
	for _, want := range []string{"event: message_start", "event: content_block_delta", `"text":"hi"`} {
		if !strings.Contains(body, want) {
			t.Errorf("SSE 结构丢失 %q: %s", want, body)
		}
	}
}

func TestSSEResponsesAPI(t *testing.T) {
	gs, cap := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.created\n")
		fmt.Fprint(w, `data: {"type":"response.created","response":{"id":"resp_up9","model":"o-upstream"}}`+"\n\n")
		w.(http.Flusher).Flush()
	}, nil)

	body := readAll(t, post(t, gs, "/v1/responses", `{"model":"o-pub","stream":true}`,
		map[string]string{MapHeader: "o-pub:o-upstream"}))

	if upPath, _, _ := cap.snapshot(); upPath != "/v1/responses" {
		t.Errorf("上游路径 = %q", upPath)
	}
	if strings.Contains(body, "o-upstream") {
		t.Errorf("responses 流泄漏: %s", body)
	}
	if !strings.Contains(body, `"model":"o-pub"`) {
		t.Errorf("response.model 未脱敏: %s", body)
	}
}

// --- URL 拼接 ---

func TestBuildURL(t *testing.T) {
	cases := []struct{ base, path, query, want string }{
		{"https://api.openai.com", "/v1/chat/completions", "", "https://api.openai.com/v1/chat/completions"},
		{"https://api.anthropic.com", "/v1/messages", "", "https://api.anthropic.com/v1/messages"},
		{"https://ark.cn-beijing.volces.com/api/v3", "/v1/chat/completions", "", "https://ark.cn-beijing.volces.com/api/v3/chat/completions"},
		{"https://ark.cn-beijing.volces.com/api/v3", "/v1/responses", "", "https://ark.cn-beijing.volces.com/api/v3/responses"},
		{"https://x.com/v1", "/v1/messages", "", "https://x.com/v1/messages"},
		{"https://x.com", "/v1/models", "limit=5", "https://x.com/v1/models?limit=5"},
		{"https://x.com/base", "/healthz", "", "https://x.com/base/healthz"},
	}
	for _, c := range cases {
		got, err := buildURL(c.base, &url.URL{Path: c.path, RawQuery: c.query})
		if err != nil {
			t.Fatal(err)
		}
		if got != c.want {
			t.Errorf("buildURL(%q, %q) = %q, want %q", c.base, c.path, got, c.want)
		}
	}
}

func TestExtractCredential(t *testing.T) {
	cases := []struct {
		hdr  map[string]string
		want string
	}{
		{map[string]string{"Authorization": "Bearer sk-1"}, "sk-1"},
		{map[string]string{"Authorization": "bearer sk-1"}, "sk-1"}, // 大小写不敏感
		{map[string]string{"Authorization": "sk-1"}, "sk-1"},
		{map[string]string{"x-api-key": "sk-2"}, "sk-2"},
		{map[string]string{"Api-Key": "sk-3"}, "sk-3"},
		// Authorization 优先于 x-api-key
		{map[string]string{"Authorization": "Bearer sk-1", "x-api-key": "sk-2"}, "sk-1"},
		{map[string]string{"Authorization": "Bearer "}, ""}, // 空 Bearer
		{nil, ""},
	}
	for _, c := range cases {
		h := http.Header{}
		for k, v := range c.hdr {
			h.Set(k, v)
		}
		if got := extractCredential(h); got != c.want {
			t.Errorf("extractCredential(%v) = %q, want %q", c.hdr, got, c.want)
		}
	}
}

// --- 健壮性 ---

func TestUpstreamDownReturns502(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1") // 必然拒绝连接
	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	defer tr.CloseIdleConnections()
	gs := httptest.NewServer(New(cfg, upstream.NewClient(tr, cfg.Upstream), testObs(t)))
	defer gs.Close()

	resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	body := readAll(t, resp)
	if !strings.Contains(body, `"type":"upstream_error"`) {
		t.Errorf("错误格式不符: %s", body)
	}
	var v map[string]any
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Errorf("错误响应非合法 JSON: %v", err)
	}
}

// 没有默认上游且客户端也没给 header 时，应给出明确指引。
func TestNoUpstreamConfigured(t *testing.T) {
	cfg := testConfig("")
	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	defer tr.CloseIdleConnections()
	gs := httptest.NewServer(New(cfg, upstream.NewClient(tr, cfg.Upstream), testObs(t)))
	defer gs.Close()

	resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if body := readAll(t, resp); !strings.Contains(body, BaseHeader) {
		t.Errorf("错误信息应提示如何配置: %s", body)
	}
}

func TestNonJSONBodyPassthrough(t *testing.T) {
	gs, cap := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("pong"))
	}, nil)

	if body := readAll(t, post(t, gs, "/v1/audio/speech", "raw-binary-ish", nil)); body != "pong" {
		t.Errorf("非 JSON 请求应原样透传，响应 = %q", body)
	}
	if _, upBody, _ := cap.snapshot(); string(upBody) != "raw-binary-ish" {
		t.Errorf("请求体被篡改: %q", upBody)
	}
}

func TestJSONWithoutModelPassthrough(t *testing.T) {
	gs, _ := newFixture(t, okJSON(`{"ok":true}`), nil)
	resp := post(t, gs, "/v1/embeddings", `{"input":"hi"}`, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	readAll(t, resp)
}

func TestBodyTooLarge(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("超限请求不应到达上游")
	}, func(c *config.Config) { c.Limits.MaxBodyBytes = 64 })

	resp := post(t, gs, "/v1/chat/completions", strings.Repeat("x", 512), nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	readAll(t, resp)
}

// 高并发下每个请求按各自的对外模型独立脱敏，且上游连接被复用。
func TestConcurrentRequestsAndConnReuse(t *testing.T) {
	var served atomic.Int64
	var mu sync.Mutex
	conns := map[string]struct{}{}

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		mu.Lock()
		conns[r.RemoteAddr] = struct{}{}
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"deepseek-v3","id":"chatcmpl-deepseek-v3-1"}`))
	}, nil)

	const (
		workers   = 32
		perWorker = 20
		total     = workers * perWorker
	)
	var wg sync.WaitGroup
	errs := make(chan string, total)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pub := []string{"deepseek-pro", "deepseek-flash"}[i%2]
			for j := 0; j < perWorker; j++ {
				body := readAll(t, post(t, gs, "/v1/chat/completions",
					fmt.Sprintf(`{"model":%q}`, pub),
					map[string]string{MapHeader: "deepseek-pro:deepseek-v3;deepseek-flash:deepseek-v3"}))
				if strings.Contains(body, "deepseek-v3") {
					errs <- fmt.Sprintf("worker %d 泄漏上游模型: %s", i, body)
					return
				}
				if !strings.Contains(body, fmt.Sprintf(`"model":%q`, pub)) {
					errs <- fmt.Sprintf("worker %d 脱敏串味，期望 %s，实际 %s", i, pub, body)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}

	if served.Load() != total {
		t.Errorf("上游收到 %d 个请求，期望 %d", served.Load(), total)
	}
	mu.Lock()
	nConns := len(conns)
	mu.Unlock()
	// 连接数应受并发度约束而非请求数。
	if nConns > workers+8 {
		t.Errorf("连接未有效复用：%d 个请求用了 %d 条连接（并发度仅 %d）", total, nConns, workers)
	}
	t.Logf("%d 个请求（并发 %d）复用了 %d 条上游连接", total, workers, nConns)
}

// --- 基准 ---

func benchFixture(b *testing.B, h http.HandlerFunc) *httptest.Server {
	b.Helper()
	us := httptest.NewServer(h)
	b.Cleanup(us.Close)

	cfg := testConfig(us.URL)
	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	b.Cleanup(tr.CloseIdleConnections)
	gs := httptest.NewServer(New(cfg, upstream.NewClient(tr, cfg.Upstream), testObs(b)))
	b.Cleanup(gs.Close)
	gs.Client().Transport.(*http.Transport).MaxIdleConnsPerHost = 256
	return gs
}

func BenchmarkNonStreamProxy(b *testing.B) {
	upResp := []byte(`{"id":"chatcmpl-deepseek-v3-abc","object":"chat.completion","model":"deepseek-v3","system_fingerprint":"fp_deepseek-v3_01","choices":[{"index":0,"message":{"role":"assistant","content":"这是一段中等长度的回答内容，用于模拟真实的响应体大小。"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":12,"total_tokens":21}}`)
	gs := benchFixture(b, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(upResp)
	})
	client := gs.Client()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodPost, gs.URL+"/v1/chat/completions",
				strings.NewReader(`{"model":"deepseek-pro","messages":[{"role":"user","content":"hello"}]}`))
			req.Header.Set(MapHeader, "deepseek-pro:deepseek-v3")
			resp, err := client.Do(req)
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

func BenchmarkStreamProxy(b *testing.B) {
	var chunks strings.Builder
	for i := 0; i < 50; i++ {
		chunks.WriteString(`data: {"id":"chatcmpl-1","object":"chat.completion.chunk","model":"deepseek-pro","choices":[{"index":0,"delta":{"content":"token"},"finish_reason":null}]}` + "\n\n")
	}
	chunks.WriteString("data: [DONE]\n\n")
	payload := chunks.String()

	gs := benchFixture(b, func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, payload)
	})
	client := gs.Client()

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req, _ := http.NewRequest(http.MethodPost, gs.URL+"/v1/chat/completions",
				strings.NewReader(`{"model":"deepseek-pro","stream":true}`))
			req.Header.Set(MapHeader, "deepseek-pro:deepseek-v3")
			resp, err := client.Do(req)
			if err != nil {
				b.Fatal(err)
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	})
}

// --- 指标标签基数控制 ---

// model 标签必须只收录「运维声明过」的模型名。
// 否则客户端每次填一个新 model 名就能让指标时间序列无限增长。
func TestMetricModelCardinalityBounded(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1")
	cfg.Mapping.Models = map[string][]string{"declared-model": {"up-v3"}}
	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	t.Cleanup(tr.CloseIdleConnections)
	gw := New(cfg, upstream.NewClient(tr, cfg.Upstream), testObs(t))

	cases := []struct {
		name   string
		model  string
		header map[string]string
		want   string
	}{
		{"静态配置声明的模型进标签", "declared-model", nil, "declared-model"},
		{"Header 声明的模型进标签", "hdr-model",
			map[string]string{"X-Model-Map": "hdr-model:up-v3"}, "hdr-model"},
		{"未声明的模型归一", "random-junk-1", nil, modelOther},
		{"另一个未声明的模型也归到同一值", "random-junk-2", nil, modelOther},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 只走 rewrite 并断言 metricModel：不经 HTTP，
			// 是为了精确观察标签值而不依赖指标导出管道。
			st := &state{route: "/v1/chat/completions", metricModel: modelOther}
			r, _ := http.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			for k, v := range c.header {
				r.Header.Set(k, v)
			}
			body := []byte(`{"model":"` + c.model + `"}`)
			if code := gw.rewrite(httptest.NewRecorder(), r, st, &body); code != 0 {
				t.Fatalf("rewrite 返回 %d", code)
			}
			if st.metricModel != c.want {
				t.Errorf("metricModel = %q, 期望 %q", st.metricModel, c.want)
			}
			// 真实模型名不受影响，仍然完整保留供脱敏与审计使用。
			if st.publicModel != c.model {
				t.Errorf("publicModel 被改动: %q", st.publicModel)
			}
		})
	}
}

// --- 并发闸门 ---

// 超过 MAX_INFLIGHT 的请求必须快速失败，而不是排队堆积到 OOM。
func TestInflightGateRejectsOverload(t *testing.T) {
	hold := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(2)

	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		arrived.Done()
		<-hold // 占住闸门，模拟上游变慢
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"up-v3"}`))
	}, func(c *config.Config) {
		c.Limits.MaxInflight = 2
	})

	// 先占满 2 个槽位。
	for i := 0; i < 2; i++ {
		go func() {
			resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
			_ = readAll(t, resp)
		}()
	}
	arrived.Wait() // 确认两个请求都已进入上游处理

	// 第 3 个必须立刻被拒，不能阻塞。
	done := make(chan *http.Response, 1)
	go func() {
		done <- post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
	}()
	select {
	case resp := <-done:
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("超限请求返回 %d，期望 503", resp.StatusCode)
		}
		if ra := resp.Header.Get("Retry-After"); ra == "" {
			t.Error("503 响应缺少 Retry-After 头")
		}
		body := readAll(t, resp)
		if !strings.Contains(body, "gateway_overloaded") {
			t.Errorf("错误体未标明过载原因: %s", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("超限请求被阻塞而非快速失败")
	}

	close(hold)
}

// MAX_INFLIGHT=0 表示放弃闸门，此时不应拒绝任何请求。
func TestInflightGateDisabled(t *testing.T) {
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"up-v3"}`))
	}, func(c *config.Config) {
		c.Limits.MaxInflight = 0
	})

	const n = 50
	var wg sync.WaitGroup
	var rejected atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
			if resp.StatusCode == http.StatusServiceUnavailable {
				rejected.Add(1)
			}
			_ = readAll(t, resp)
		}()
	}
	wg.Wait()
	if r := rejected.Load(); r != 0 {
		t.Errorf("闸门关闭时仍拒绝了 %d 个请求", r)
	}
}

// 闸门必须在读请求体之前生效：否则大 body 已占内存，保护失去意义。
func TestInflightGateRejectsBeforeReadingBody(t *testing.T) {
	hold := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(1)
	gs, cap := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		arrived.Done()
		<-hold
		_, _ = w.Write([]byte(`{}`))
	}, func(c *config.Config) {
		c.Limits.MaxInflight = 1
	})

	go func() {
		resp := post(t, gs, "/v1/chat/completions", `{"model":"m"}`, nil)
		_ = readAll(t, resp)
	}()
	arrived.Wait()

	// 发一个带大 body 的请求，它应当在 body 被消费前就被拒。
	big := `{"model":"m","padding":"` + strings.Repeat("x", 1<<20) + `"}`
	resp := post(t, gs, "/v1/chat/completions", big, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("返回 %d，期望 503", resp.StatusCode)
	}
	_ = readAll(t, resp)

	// 上游只应看到第一个请求，被拒的那个绝不能转发出去。
	cap.mu.Lock()
	forwarded := len(cap.body)
	cap.mu.Unlock()
	if forwarded > 1024 {
		t.Errorf("被拒请求的 body 仍被转发到上游（%d 字节）", forwarded)
	}

	close(hold)
}

// --- 故障切换 ---

// 首选上游返回 429/5xx 时应自动切到兜底模型，客户端只看到最终成功的响应。
func TestFailoverOnUpstreamError(t *testing.T) {
	for _, code := range []int{429, 500, 502, 503, 504} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var seen []string
			var mu sync.Mutex
			gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				m := gjsonModel(b)
				mu.Lock()
				seen = append(seen, m)
				mu.Unlock()

				if m == "primary-up" {
					w.WriteHeader(code)
					_, _ = w.Write([]byte(`{"error":{"message":"primary-up down"}}`))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"model":"` + m + `","id":"ok"}`))
			}, func(c *config.Config) {
				c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
				c.Mapping.Fallback = []string{"backup-up"}
				c.Mapping.FailoverOnError = true
			})

			resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
			body := readAll(t, resp)

			if resp.StatusCode != 200 {
				t.Errorf("切换后仍返回 %d，期望 200；body=%s", resp.StatusCode, body)
			}
			// 对外模型名必须仍是客户端请求的那个。
			if !strings.Contains(body, `"model":"pub"`) {
				t.Errorf("响应 model 未脱敏为对外名: %s", body)
			}
			// 两个上游模型名都不能泄漏。
			for _, leak := range []string{"primary-up", "backup-up"} {
				if strings.Contains(body, leak) {
					t.Errorf("泄漏上游模型 %q: %s", leak, body)
				}
			}
			mu.Lock()
			got := append([]string(nil), seen...)
			mu.Unlock()
			if len(got) != 2 || got[0] != "primary-up" || got[1] != "backup-up" {
				t.Errorf("上游收到的模型序列 = %v，期望 [primary-up backup-up]", got)
			}
		})
	}
}

// 4xx 中的请求类错误不该触发切换：换模型也是同样的失败。
func TestNoFailoverOnClientError(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int64
			gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
			}, func(c *config.Config) {
				c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
				c.Mapping.Fallback = []string{"backup-up"}
				c.Mapping.FailoverOnError = true
			})
			resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
			_ = readAll(t, resp)
			if resp.StatusCode != code {
				t.Errorf("状态码 %d 未原样透传，得到 %d", code, resp.StatusCode)
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("上游被调用 %d 次，期望 1（不应重试）", n)
			}
		})
	}
}

// 未开启开关时，即使配了兜底也不切换。
func TestFailoverDisabledByDefault(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":{}}`))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = false // 显式关闭
	})
	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`, nil)
	_ = readAll(t, resp)
	if resp.StatusCode != 503 {
		t.Errorf("返回 %d，期望原样透传 503", resp.StatusCode)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("上游被调用 %d 次，期望 1", n)
	}
}

// 已经命中兜底的请求失败后不再切换，避免无意义的二次尝试。
func TestNoFailoverWhenAlreadyFallback(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(503)
		_, _ = w.Write([]byte(`{"error":{}}`))
	}, func(c *config.Config) {
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = true
	})
	// unknown-model 未声明 -> 直接命中兜底 -> 失败后不应再切
	resp := post(t, gs, "/v1/chat/completions", `{"model":"unknown-model"}`, nil)
	_ = readAll(t, resp)
	if n := calls.Load(); n != 1 {
		t.Errorf("上游被调用 %d 次，期望 1（兜底失败不再切换）", n)
	}
	_ = resp
}

// 流式响应一旦开始下发就绝不能重试，否则客户端会收到拼接的两段流。
func TestNoFailoverAfterStreamStarted(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200) // 200 + SSE，正文开始下发
		fl, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"model\":\"primary-up\"}\n\n"))
		if fl != nil {
			fl.Flush()
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		c.Mapping.Fallback = []string{"backup-up"}
		c.Mapping.FailoverOnError = true
	})
	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub","stream":true}`, nil)
	body := readAll(t, resp)
	if n := calls.Load(); n != 1 {
		t.Errorf("上游被调用 %d 次，期望 1", n)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Errorf("流式响应不完整: %s", body)
	}
	if strings.Contains(body, "primary-up") {
		t.Errorf("流式响应泄漏上游模型: %s", body)
	}
}

// gjsonModel 从请求体里取出 model 字段，供上游桩函数辨认收到的是哪个模型。
func gjsonModel(b []byte) string {
	return gjson.GetBytes(b, "model").String()
}
