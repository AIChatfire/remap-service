package gateway

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/betterme/remap-service/internal/config"
)

// capFixture 建一个「主力模型对指定能力报错、能力专用模型正常」的上游桩。
//
// 返回的 seen 记录上游依次收到的模型名，用于断言切换确实发生且只发生一次。

// --- 五项能力的切换：主力报错 -> 走能力模型 -> 对外仍是原模型名 ---

func TestCapabilityFailoverPerKind(t *testing.T) {
	cases := []struct {
		name    string
		header  string
		body    string
		capUp   string
		errBody string
	}{
		{
			name:    "识图",
			header:  "vision:vision-up",
			body:    `{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/1.png"}}]}]}`,
			capUp:   "vision-up",
			errBody: `{"error":{"message":"model does not support image input","code":"InvalidParameter"}}`,
		},
		{
			name:    "音频理解",
			header:  "audio:audio-up",
			body:    `{"model":"pub","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AA","format":"wav"}}]}]}`,
			capUp:   "audio-up",
			errBody: `{"error":{"message":"audio not supported"}}`,
		},
		{
			name:    "视频理解",
			header:  "video:video-up",
			body:    `{"model":"pub","messages":[{"role":"user","content":[{"type":"video_url","video_url":{"url":"http://x/1.mp4"}}]}]}`,
			capUp:   "video-up",
			errBody: `{"error":{"message":"video not supported"}}`,
		},
		{
			name:    "工具调用",
			header:  "tools:tools-up",
			body:    `{"model":"pub","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"f"}}]}`,
			capUp:   "tools-up",
			errBody: `{"error":{"message":"function calling is not supported"}}`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var mu sync.Mutex
			var seen []string
			gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				b, _ := io.ReadAll(r.Body)
				m := gjsonModel(b)
				mu.Lock()
				seen = append(seen, m)
				mu.Unlock()
				if m == "primary-up" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(400)
					_, _ = w.Write([]byte(c.errBody))
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"model":"` + m + `","id":"chatcmpl-` + m + `-1"}`))
			}, func(cfg *config.Config) {
				cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
			})

			resp := post(t, gs, "/v1/chat/completions", c.body,
				map[string]string{CapHeader: c.header})
			body := readAll(t, resp)

			if resp.StatusCode != 200 {
				t.Fatalf("切换后仍返回 %d，期望 200；body=%s", resp.StatusCode, body)
			}
			// 核心断言：返回的仍是客户端请求时用的模型名。
			if !strings.Contains(body, `"model":"pub"`) {
				t.Errorf("响应 model 未还原为对外名: %s", body)
			}
			for _, leak := range []string{"primary-up", c.capUp} {
				if strings.Contains(body, leak) {
					t.Errorf("泄漏上游模型 %q: %s", leak, body)
				}
			}
			mu.Lock()
			got := append([]string(nil), seen...)
			mu.Unlock()
			if len(got) != 2 || got[0] != "primary-up" || got[1] != c.capUp {
				t.Errorf("上游收到的模型序列 = %v，期望 [primary-up %s]", got, c.capUp)
			}
		})
	}
}

// 文档理解走前置路由：请求体含 file_id 时直接改走文档模型，
// 上游只会被调用一次，不经过「先撞错」。
func TestFileCapabilityRoutesUpfront(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"` + m + `","id":"resp-1"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	body := `{"model":"pub","messages":[{"role":"user","content":[` +
		`{"type":"file","file":{"file_id":"file-abc123"}},` +
		`{"type":"text","text":"总结这个文件"}]}]}`
	resp := post(t, gs, "/v1/chat/completions", body,
		map[string]string{CapHeader: "file:doc-up"})
	out := readAll(t, resp)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, out)
	}
	if !strings.Contains(out, `"model":"pub"`) {
		t.Errorf("响应 model 未还原为对外名: %s", out)
	}
	if strings.Contains(out, "doc-up") || strings.Contains(out, "primary-up") {
		t.Errorf("泄漏上游模型: %s", out)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "doc-up" {
		t.Errorf("上游收到 %v，期望仅 [doc-up]（前置路由，不应重试）", got)
	}
}

// 文档理解在上游报错时同样会切换（file_id 之外的形态，如 input_file）。
func TestFileCapabilityFailoverOnError(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "primary-up" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(422)
			_, _ = w.Write([]byte(`{"error":{"message":"document input unsupported"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"` + m + `","id":"resp-1"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	// input_file 带 file_data 而非 file_id：前置路由不触发（顶层 file_id 缺失时
	// 由 isFileRef 的 file_data 分支覆盖），这里用 filename 形态验证报错切换路径。
	body := `{"model":"pub","messages":[{"role":"user","content":[` +
		`{"type":"input_file","file":{"filename":"a.pdf"}}]}]}`
	resp := post(t, gs, "/v1/chat/completions", body,
		map[string]string{CapHeader: "file:doc-up"})
	out := readAll(t, resp)

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, body=%s", resp.StatusCode, out)
	}
	if !strings.Contains(out, `"model":"pub"`) {
		t.Errorf("响应 model 未还原: %s", out)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if got[len(got)-1] != "doc-up" {
		t.Errorf("最终上游模型 = %v，期望以 doc-up 结尾", got)
	}
}

// 请求没用到任何声明的能力时，绝不能因为一次 400 就换模型 ——
// 那是通用兜底的职责，而通用兜底刻意不覆盖 4xx。
func TestNoCapabilityFailoverForPlainRequest(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"message":"bad param"}}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{CapHeader: "vision:vision-up"})
	readAll(t, resp)

	if resp.StatusCode != 400 {
		t.Errorf("status = %d, 期望原样返回 400", resp.StatusCode)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("上游被调用 %d 次，期望 1 次（无能力命中不应重试）", n)
	}
}

// 401/403 是凭据问题，换任何模型都过不去，不该重试。
func TestNoCapabilityFailoverOnAuthError(t *testing.T) {
	for _, code := range []int{401, 403} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			var calls atomic.Int64
			gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"error":{"message":"invalid key"}}`))
			}, func(cfg *config.Config) {
				cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
			})

			resp := post(t, gs, "/v1/chat/completions",
				`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
				map[string]string{CapHeader: "vision:vision-up"})
			readAll(t, resp)

			if n := calls.Load(); n != 1 {
				t.Errorf("上游被调用 %d 次，期望 1 次", n)
			}
			if resp.StatusCode != code {
				t.Errorf("status = %d, 期望 %d", resp.StatusCode, code)
			}
		})
	}
}

// 能力模型自己也失败时，只重试一次，不会无限横跳。
func TestCapabilityFailoverRetriesOnce(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"message":"vision unsupported"}}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
		map[string]string{CapHeader: "vision:vision-up"})
	body := readAll(t, resp)

	if n := calls.Load(); n != 2 {
		t.Errorf("上游被调用 %d 次，期望恰好 2 次", n)
	}
	// 上游模型名不能出现在最终返回的错误里。
	for _, leak := range []string{"primary-up", "vision-up"} {
		if strings.Contains(body, leak) {
			t.Errorf("错误响应泄漏上游模型 %q: %s", leak, body)
		}
	}
}

// 能力模型与当前上游模型相同时不切换 —— 重试等于原地再试一次。
func TestNoCapabilityFailoverWhenSameModel(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"message":"nope"}}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"same-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
		map[string]string{CapHeader: "vision:same-up"})
	readAll(t, resp)

	if n := calls.Load(); n != 1 {
		t.Errorf("上游被调用 %d 次，期望 1 次", n)
	}
}

// 多能力共存时按优先级取多模态：识图 > 工具调用。
func TestCapabilityPriorityVisionBeforeTools(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "primary-up" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"unsupported"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"` + m + `"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	body := `{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}],` +
		`"tools":[{"type":"function","function":{"name":"f"}}]}`
	resp := post(t, gs, "/v1/chat/completions", body,
		map[string]string{CapHeader: "vision:vision-up;tools:tools-up"})
	readAll(t, resp)

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[1] != "vision-up" {
		t.Errorf("上游序列 = %v，期望切到 vision-up（多模态优先于工具）", got)
	}
}

// 能力切换优先于通用兜底：两者都配置时应走能力模型。
func TestCapabilityBeatsGenericFallback(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "primary-up" {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"` + m + `"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		cfg.Mapping.Fallback = []string{"generic-up"}
		cfg.Mapping.FailoverOnError = true
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
		map[string]string{CapHeader: "vision:vision-up"})
	readAll(t, resp)

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[1] != "vision-up" {
		t.Errorf("上游序列 = %v，期望能力切换优先于通用兜底", got)
	}
}

// 静态配置（MODEL_CAPABILITY）在请求头缺失时生效。
func TestCapabilityFromStaticConfig(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "primary-up" {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"no image"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"` + m + `"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
		cfg.Mapping.Capabilities = map[string][]string{"vision": {"static-vision-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`, nil)
	body := readAll(t, resp)

	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[1] != "static-vision-up" {
		t.Errorf("上游序列 = %v，期望走静态配置的 static-vision-up", got)
	}
	if !strings.Contains(body, `"model":"pub"`) {
		t.Errorf("响应 model 未还原: %s", body)
	}
}

// 能力头不得转发给上游。
func TestCapabilityHeaderNotForwarded(t *testing.T) {
	gs, cap := newFixture(t, okJSON(`{"model":"primary-up"}`), func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})
	resp := post(t, gs, "/v1/chat/completions", `{"model":"pub"}`,
		map[string]string{CapHeader: "vision:vision-up"})
	readAll(t, resp)

	_, _, h := cap.snapshot()
	if v := h.Get(CapHeader); v != "" {
		t.Errorf("能力头被转发给上游: %q", v)
	}
}

// 流式响应已开始下发后不得重试（会拼接出两段响应）。
// 首包即 SSE 头的情形由 pipeStream 前的状态码判定拦住，这里验证
// SSE 场景下的能力切换在首包前完成、且事件流中的模型名已脱敏。
func TestCapabilityFailoverStream(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "primary-up" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"error":{"message":"no image"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"model\":\"" + m + "\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		if fl != nil {
			fl.Flush()
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","stream":true,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
		map[string]string{CapHeader: "vision:vision-up"})
	body := readAll(t, resp)

	if !strings.Contains(body, `"model":"pub"`) {
		t.Errorf("SSE 中 model 未还原为对外名: %s", body)
	}
	if strings.Contains(body, "vision-up") {
		t.Errorf("SSE 泄漏上游模型: %s", body)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[1] != "vision-up" {
		t.Errorf("上游序列 = %v", got)
	}
}

// Anthropic 协议下的识图切换，验证跨协议一致。
func TestCapabilityFailoverAnthropic(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		m := gjsonModel(b)
		mu.Lock()
		seen = append(seen, m)
		mu.Unlock()
		if m == "claude-primary" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"image not supported"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"msg_1","model":"` + m + `","content":[]}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"claude-pub": {"claude-primary"}}
	})

	body := `{"model":"claude-pub","messages":[{"role":"user","content":[` +
		`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA"}}]}]}`
	resp := post(t, gs, "/v1/messages", body,
		map[string]string{CapHeader: "vision:claude-vision"})
	out := readAll(t, resp)

	if !strings.Contains(out, `"model":"claude-pub"`) {
		t.Errorf("响应 model 未还原: %s", out)
	}
	mu.Lock()
	got := append([]string(nil), seen...)
	mu.Unlock()
	if len(got) != 2 || got[1] != "claude-vision" {
		t.Errorf("上游序列 = %v，期望 [claude-primary claude-vision]", got)
	}
}

// 非法能力头不应让请求失败，只是退化为无能力切换。
func TestMalformedCapabilityHeaderIsHarmless(t *testing.T) {
	var calls atomic.Int64
	gs, _ := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"model":"primary-up"}`))
	}, func(cfg *config.Config) {
		cfg.Mapping.Models = map[string][]string{"pub": {"primary-up"}}
	})

	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"pub","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}]}`,
		map[string]string{CapHeader: ";;garbage;:;vision"})
	body := readAll(t, resp)

	if resp.StatusCode != 200 || calls.Load() != 1 {
		t.Errorf("status=%d calls=%d，期望正常透传", resp.StatusCode, calls.Load())
	}
	if !strings.Contains(body, `"model":"pub"`) {
		t.Errorf("响应 model 未还原: %s", body)
	}
}
