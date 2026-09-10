package protocol

import (
	"io"
	"strings"
	"testing"

	"github.com/betterme/remap-service/internal/sanitize"
)

// newLimitRep 构建一个长度阈值很小的替换器，让用例不必构造几百字符的字符串。
func newLimitRep(t testing.TB, upstream, public string, maxLen int) *sanitize.Replacer {
	t.Helper()
	return sanitize.NewRules(nil, nil, nil, maxLen).For(upstream, public)
}

// 含上游标识、但被长度阈值放行的值必须被报告为「抑制」。
//
// 这是唯一真正的静默漏点：客户端拿到的是上游原文（可能是带真实模型名的
// 长错误 message），而看板上既没有错误、也没有任何计数。信号落在这里，
// 「有没有漏」才从肉眼比对变成可查询。
func TestSanitizeReportsLengthSuppressed(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newLimitRep(t, "up-model", "public-model", 32)

	// 40 字符 > 32，且含上游标识。
	long := strings.Repeat("x", 30) + " up-model here"
	in := []byte(`{"model":"up-model","error":{"message":"` + long + `"}}`)

	out, report := s.SanitizeWithReport(in, "public-model", rep)
	if !report.Suppressed {
		t.Fatal("含标识的超长字段未被报告为抑制")
	}
	if !report.Changed {
		t.Error("model 字段被整体覆盖，Changed 应为真")
	}
	// 行为本身不变（有意放行生成内容），用例只确认信号：
	// 长值原样透出，正是「漏点」的定义。
	if !strings.Contains(string(out), long) {
		t.Error("超长值不应被改写，本用例只验证信号")
	}
}

// 生成内容字段被跳过**不算**抑制。
//
// 这是与上一条互补的负向用例，也是这条信号可用的前提：把 content/text/delta
// 被跳过的次数也算进来，计数会恒非零，于是「漏了」与「本来就该放过」
// 又变成同形 —— 那正是加这个信号要消灭的东西。
func TestSanitizeDoesNotReportContentFieldSuppression(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newLimitRep(t, "up-model", "public-model", 32)

	long := strings.Repeat("y", 60) + " up-model in generated text"
	in := []byte(`{"model":"public-model","choices":[{"delta":{"content":"` + long + `"}}]}`)

	_, report := s.SanitizeWithReport(in, "public-model", rep)
	if report.Suppressed {
		t.Fatal("生成内容字段被跳过不应计入抑制信号（会淹没真正的漏点）")
	}
}

// 短值被正常改写时不是抑制。
func TestSanitizeNoSuppressionWhenShortValueRewritten(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newLimitRep(t, "up-model", "public-model", 32)

	in := []byte(`{"id":"chatcmpl-up-model-1","error":{"message":"up-model overloaded"}}`)
	out, report := s.SanitizeWithReport(in, "public-model", rep)
	if report.Suppressed {
		t.Error("发生了实际改写，不应报抑制")
	}
	if !report.Changed {
		t.Fatal("应发生改写")
	}
	if strings.Contains(string(out), "up-model") {
		t.Errorf("短值未被改写: %s", out)
	}
}

// 响应里根本没出现上游标识时，零替换是正确结果，不是漏点。
// 少了这条，Suppressed 写成恒 true 也能让第一个用例通过。
func TestSanitizeNoSuppressionWhenIdentifierAbsent(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newLimitRep(t, "up-model", "public-model", 32)

	in := []byte(`{"model":"public-model","note":"` + strings.Repeat("z", 60) + `"}`)
	_, report := s.SanitizeWithReport(in, "public-model", rep)
	if report.Suppressed {
		t.Error("文本里没有上游标识，不应报抑制")
	}
}

// 旧的 Sanitize 只是包装：Changed 的语义必须与引入 report 之前完全一致
// （模型字段覆盖与子串替换任一发生即为真），否则所有既有调用方的行为
// 会在无人察觉的情况下改变。
func TestSanitizeWrapperKeepsChangedSemantics(t *testing.T) {
	s := Detect("/v1/chat/completions")
	rep := newLimitRep(t, "up-model", "public-model", 32)

	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"仅模型字段被覆盖", `{"model":"up-model"}`, true},
		{"仅子串被替换", `{"id":"x","note":"up-model"}`, true},
		{"两者都不变", `{"model":"public-model","note":"nothing"}`, false},
		{"超长值被放行且模型字段已是对外名", `{"model":"public-model","note":"` + strings.Repeat("w", 60) + ` up-model"}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, changed := s.Sanitize([]byte(c.in), "public-model", rep)
			if changed != c.want {
				t.Errorf("Sanitize 返回的 changed = %v, want %v (out=%s)", changed, c.want, out)
			}
			// 包装必须与带 report 的版本逐字节一致。
			other, _ := s.SanitizeWithReport([]byte(c.in), "public-model", rep)
			if string(out) != string(other) {
				t.Errorf("两个入口结果不一致:\n  %s\n  %s", out, other)
			}
		})
	}
}

// 流式路径的抑制信号要能被累加出来，且与 Rewrote 相互独立 ——
// 同一行可以「既改写了 model 字段、又放行了一个超长错误串」。
func TestPipeSSECountsSuppressed(t *testing.T) {
	tr := testTransformer{
		spec:   Detect("/v1/chat/completions"),
		rep:    newLimitRep(t, "up-model", "public-model", 32),
		public: "public-model",
	}
	long := strings.Repeat("x", 30) + " up-model here"
	in := strings.Join([]string{
		`data: {"model":"up-model","note":"` + long + `"}`,
		``,
		`data: [DONE]`,
		``,
		``,
	}, "\n")

	stats, err := PipeSSE(io.Discard, strings.NewReader(in), tr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Suppressed != 1 {
		t.Errorf("Suppressed = %d, want 1", stats.Suppressed)
	}
	// 同一行的 model 字段确实被覆盖了 —— 两个计数不是互斥关系，
	// 只看其中一个都会误判（这正是把它们并列上报的原因）。
	if stats.Rewrote != 1 {
		t.Errorf("Rewrote = %d, want 1", stats.Rewrote)
	}
}
