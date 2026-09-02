package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/betterme/remap-service/internal/config"
)

// ---------- 生产事故回归（2026-09-02）----------
//
// 线上现象：X-Model-Map 带 glm*:glm-5-2-260617 时，trace 显示
// gateway.upstream.model = glm-5.2（未映射），上游 404。
// 本地引擎与线上探测均证明通配匹配正确，怀疑对象收敛为
// 「声明写错 → 静默退化成透传」。这组测试锁死三条契约：
//
//	① 生产 Header 形态下，带点号版本名必须被通配规则改写；
//	② 映射决策（命中级别 + 表来源）必须进 span 属性，透传可被筛出；
//	③ 声明有语法错误时必须 400 拒绝，不得静默透传。

// prodMapHeader 是线上出问题时的完整 Header 值，原样固化。
const prodMapHeader = "glm*:glm-5-2-260617;deepseek*:deepseek-v4-flash-ga-260731;" +
	"doubao-seed*:doubao-seed-2-0-pro-260215;doubao-seed*:doubao-seed-2-0-lite-260428"

// 生产形态端到端：每个对外名都必须被改写为通配目标，一个都不能漏。
func TestProdHeaderRewritesAllModels(t *testing.T) {
	cases := []struct {
		public string
		wantIn []string // 改写结果必须落在候选集内
	}{
		{"glm-5.1", []string{"glm-5-2-260617"}},
		{"glm-5.2", []string{"glm-5-2-260617"}},
		{"glm-5.3", []string{"glm-5-2-260617"}},
		{"glm-5", []string{"glm-5-2-260617"}},
		{"doubao-seed-2-0-lite-260428", []string{"doubao-seed-2-0-pro-260215", "doubao-seed-2-0-lite-260428"}},
		{"doubao-seed-2-1-pro-260628", []string{"doubao-seed-2-0-pro-260215", "doubao-seed-2-0-lite-260428"}},
		{"doubao-seed-2-1-turbo-260628", []string{"doubao-seed-2-0-pro-260215", "doubao-seed-2-0-lite-260428"}},
		{"doubao-seed-evolving", []string{"doubao-seed-2-0-pro-260215", "doubao-seed-2-0-lite-260428"}},
		{"doubao-seed-2-0-pro-260215", []string{"doubao-seed-2-0-pro-260215", "doubao-seed-2-0-lite-260428"}},
		{"deepseek-chat", []string{"deepseek-v4-flash-ga-260731"}},
	}

	gs, cap := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}, nil)

	for _, c := range cases {
		t.Run(c.public, func(t *testing.T) {
			resp := post(t, gs, "/v1/chat/completions",
				`{"model":"`+c.public+`","messages":[{"role":"user","content":"hi"}]}`,
				map[string]string{MapHeader: prodMapHeader})
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d", resp.StatusCode)
			}

			_, body, _ := cap.snapshot()
			var got struct {
				Model string `json:"model"`
			}
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("上游收到的不是合法 JSON: %v", err)
			}
			// 命中结果必须落在候选集内。注意对外名本身可能就在候选集里
			//（doubao-seed-2-0-lite-260428 既是对外名也是通配目标），
			// 此时「改写结果 == 对外名」是合法的，不能一概断言必须改变。
			ok := false
			for _, want := range c.wantIn {
				if got.Model == want {
					ok = true
					break
				}
			}
			if !ok {
				if got.Model == c.public {
					t.Fatalf("模型未被改写，上游收到对外名 %q —— 正是线上 404 的形态", got.Model)
				}
				t.Errorf("上游收到 %q，不在候选集 %v 内", got.Model, c.wantIn)
			}
		})
	}
}

// 映射决策必须进 span 属性：wildcard 命中与 none 透传要在看板上可分。
func TestMappingDecisionOnSpan(t *testing.T) {
	t.Run("wildcard 命中", func(t *testing.T) {
		sr := tracetest.NewSpanRecorder()
		gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[]}`))
		}, nil)

		resp := post(t, gs, "/v1/chat/completions",
			`{"model":"glm-5.2","messages":[]}`,
			map[string]string{MapHeader: prodMapHeader})
		defer resp.Body.Close()

		sp := findSpanWithAttr(t, sr, "gateway.mapping.match")
		if got := attrString(sp, "gateway.mapping.match"); got != "wildcard" {
			t.Errorf("mapping.match = %q，期望 wildcard", got)
		}
		if got := attrString(sp, "gateway.mapping.source"); got != "header" {
			t.Errorf("mapping.source = %q，期望 header", got)
		}
		if got := attrString(sp, "gateway.upstream.model"); got != "glm-5-2-260617" {
			t.Errorf("upstream.model = %q，期望 glm-5-2-260617", got)
		}
	})

	t.Run("未命中透传标记 none", func(t *testing.T) {
		sr := tracetest.NewSpanRecorder()
		gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"choices":[]}`))
		}, nil)

		resp := post(t, gs, "/v1/chat/completions",
			`{"model":"totally-unknown","messages":[]}`,
			map[string]string{MapHeader: "other-*:up"})
		defer resp.Body.Close()

		sp := findSpanWithAttr(t, sr, "gateway.mapping.match")
		if got := attrString(sp, "gateway.mapping.match"); got != "none" {
			t.Errorf("mapping.match = %q，期望 none（透传必须可筛）", got)
		}
	})
}

// 声明有语法错误时必须 400 拒绝：静默透传会让上游 404，
// 而「规则写错」与「故意不配」在看板上完全同形。
//
// 同时锁定泄露边界：片段原文可能含真实上游模型名，而 X-Model-Map 常由
// new-api 这类中间层注入、400 会被透传给终端用户 —— 所以片段原文只进
// 看板（gateway.mapping.invalid），客户端只收通用格式说明。
func TestMalformedModelMapRejected(t *testing.T) {
	cases := []struct {
		name string
		hdr  string
		leak string // 客户端响应里绝不能出现的片段内容
	}{
		{"缺冒号", "glm*glm-5-2-260617", "glm-5-2-260617"},
		{"全角分号粘连", "glm*:g1；deepseek*:d1", "g1"},
		{"全角冒号", "glm*：glm-5-2-260617", "glm-5-2-260617"},
		{"缺上游", "glm*:", "glm*"},
		{"混合合法与非法", "glm*:ok;garbage", "garbage"},
	}
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}, nil)
	_ = gs // 每个用例独立 fixture，见下；这里只保留合法声明的反向断言

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 每个用例独立的 fixture + recorder：共享时 findSpanWithAttr
			// 会命中前一个用例的 span，断言的对象就错了。
			caseSR := tracetest.NewSpanRecorder()
			caseGS, _ := newFixtureWithRecorder(t, caseSR, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[]}`))
			}, nil)

			resp := post(t, caseGS, "/v1/chat/completions",
				`{"model":"glm-5.2","messages":[]}`,
				map[string]string{MapHeader: c.hdr})
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("非法声明应 400，实际 %d", resp.StatusCode)
			}
			b, _ := io.ReadAll(resp.Body)
			if !strings.Contains(string(b), MapHeader) {
				t.Errorf("错误信息应指明 %s 写错了，实际 %s", MapHeader, b)
			}
			// 泄露边界：客户端拿到的必须是通用描述，不含片段原文。
			if strings.Contains(string(b), c.leak) {
				t.Errorf("客户端响应泄露了片段内容 %q —— %s 由中间层注入时，终端用户会看到真实上游模型名", c.leak, b)
			}
			// 看板拿原文：排查需要知道具体哪一段写错了。
			sp := findSpanWithAttr(t, caseSR, "gateway.mapping.invalid")
			if got := attrString(sp, "gateway.mapping.invalid"); !strings.Contains(got, c.leak) {
				t.Errorf("看板应记录片段原文（含 %q），实际 %q", c.leak, got)
			}
		})
	}

	// 反向：完全合法的生产 Header 不受影响，也不产生 invalid 记录。
	t.Run("合法声明不受影响", func(t *testing.T) {
		resp := post(t, gs, "/v1/chat/completions",
			`{"model":"glm-5.2","messages":[]}`,
			map[string]string{MapHeader: prodMapHeader})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("合法声明应 200，实际 %d", resp.StatusCode)
		}
		for _, s := range sr.Ended() {
			if hasAttr(s, "gateway.mapping.invalid") {
				t.Error("合法声明不应记录 gateway.mapping.invalid")
			}
		}
	})
}

// 静态表兜底时 source 必须是 static：Header 表未命中回落静态表的路径，
// 决策来源写错会把排查引向错误的一层配置。
func TestMappingSourceStaticFallback(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	gs, _ := newFixtureWithRecorder(t, sr, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}, func(c *config.Config) {
		c.Mapping.Models = map[string][]string{"static-pub": {"static-up"}}
	})

	// Header 表只声明别的模型，static-pub 由静态表命中。
	resp := post(t, gs, "/v1/chat/completions",
		`{"model":"static-pub","messages":[]}`,
		map[string]string{MapHeader: "other:up"})
	defer resp.Body.Close()

	sp := findSpanWithAttr(t, sr, "gateway.mapping.match")
	if got := attrString(sp, "gateway.mapping.match"); got != "exact" {
		t.Errorf("mapping.match = %q，期望 exact", got)
	}
	if got := attrString(sp, "gateway.mapping.source"); got != "static" {
		t.Errorf("mapping.source = %q，期望 static（回落静态表后来源要跟着换）", got)
	}
}
