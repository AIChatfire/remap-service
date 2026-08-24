package protocol

import (
	"testing"
)

func TestDetect(t *testing.T) {
	cases := map[string]string{
		// OpenAI 家族
		"/v1/chat/completions": "openai",
		"/v1/completions":      "openai",
		"/v1/embeddings":       "openai",
		"/v1/models":           "openai",
		"/v1/models/gpt-4o":    "openai",
		"/v1/audio/speech":     "openai",
		// Responses API
		"/v1/responses":          "responses",
		"/v1/responses/resp_123": "responses",
		// Anthropic
		"/v1/messages":              "anthropic",
		"/v1/messages/count_tokens": "anthropic",
		// 未知端点兜底
		"/v1/something-new": "other",
		// 其他版本段
		"/v2/messages":     "anthropic",
		"/v1beta/messages": "anthropic",
		// 无版本段
		"/messages":  "anthropic",
		"/responses": "responses",
	}
	for path, want := range cases {
		if got := Detect(path).Name; got != want {
			t.Errorf("Detect(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestSpecAuth(t *testing.T) {
	cases := []struct {
		path       string
		wantHeader string
		wantValue  string
	}{
		{"/v1/chat/completions", "Authorization", "Bearer sk-x"},
		{"/v1/responses", "Authorization", "Bearer sk-x"},
		// Anthropic 用 x-api-key 且不加前缀
		{"/v1/messages", "x-api-key", "sk-x"},
	}
	for _, c := range cases {
		s := Detect(c.path)
		if s.AuthHeader != c.wantHeader {
			t.Errorf("%s: AuthHeader = %q, want %q", c.path, s.AuthHeader, c.wantHeader)
		}
		if got := s.AuthValue("sk-x"); got != c.wantValue {
			t.Errorf("%s: AuthValue = %q, want %q", c.path, got, c.wantValue)
		}
		if got := s.AuthValue(""); got != "" {
			t.Errorf("%s: 空 Key 应返回空串，got %q", c.path, got)
		}
	}
}

func TestAnthropicRequiredHeaders(t *testing.T) {
	s := Detect("/v1/messages")
	if s.RequiredHeaders["anthropic-version"] == "" {
		t.Error("anthropic 协议应自带 anthropic-version 必需头")
	}
	if len(Detect("/v1/chat/completions").RequiredHeaders) != 0 {
		t.Error("openai 协议不应有必需头")
	}
}

func TestByName(t *testing.T) {
	cases := map[string]string{
		"openai": "openai", "chat": "openai",
		"responses": "responses", "response": "responses",
		"anthropic": "anthropic", "claude": "anthropic", "messages": "anthropic",
		"  OpenAI  ": "openai",
	}
	for in, want := range cases {
		s := ByName(in)
		if s == nil || s.Name != want {
			t.Errorf("ByName(%q) = %v, want %q", in, s, want)
		}
	}
	if ByName("gemini") != nil {
		t.Error("未知协议名应返回 nil")
	}
	// Detect 的兜底 spec 不应通过 ByName 暴露给配置
	if ByName("other") != nil {
		t.Error("兜底 spec 不应可被配置引用")
	}
}

func TestAllSpecsWellFormed(t *testing.T) {
	for _, s := range All() {
		if s.Name == "" || s.AuthHeader == "" {
			t.Errorf("%+v 缺少必要字段", s)
		}
		if len(s.ModelPaths) == 0 || len(s.IDPaths) == 0 {
			t.Errorf("%s 缺少响应字段路径", s.Name)
		}
		if s.ModelPaths[0] != "model" || s.IDPaths[0] != "id" {
			t.Errorf("%s 应优先匹配顶层字段", s.Name)
		}
	}
}

func TestRouteLabel(t *testing.T) {
	cases := map[string]string{
		"/v1/chat/completions": "/v1/chat/completions",
		"/v1/responses":        "/v1/responses",
		"/v1/messages":         "/v1/messages",
		"/v1/embeddings":       "/v1/embeddings",
		"/v1/audio/speech":     "/v1/*",
		"/healthz":             "other",
	}
	for in, want := range cases {
		if got := RouteLabel(in); got != want {
			t.Errorf("RouteLabel(%q) = %q, want %q", in, got, want)
		}
	}
}
