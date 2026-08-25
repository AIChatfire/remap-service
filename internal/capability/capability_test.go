package capability

import "testing"

func TestParseHeader(t *testing.T) {
	m := Parse("vision:gpt-4o;audio:qwen-audio;video:v-model;tools:tool-m;file:doc-m")
	for _, c := range []struct {
		k    Kind
		want string
	}{
		{Vision, "gpt-4o"},
		{Audio, "qwen-audio"},
		{Video, "v-model"},
		{Tools, "tool-m"},
		{File, "doc-m"},
	} {
		got, ok := m.Lookup(c.k)
		if !ok || got != c.want {
			t.Fatalf("%v: got %q ok=%v, want %q", c.k, got, ok, c.want)
		}
	}
	if m.Size() != 5 {
		t.Fatalf("size = %d, want 5", m.Size())
	}
}

// 配置键只认英文。中文写法必须被当成未知能力丢弃，
// 否则一份半中半英的配置会静默地只生效一半。
func TestParseRejectsNonASCIIKeys(t *testing.T) {
	m := Parse("识图:vm;音频理解:am;vision:ok")
	if v, _ := m.Lookup(Vision); v != "ok" {
		t.Fatalf("vision = %q, want ok", v)
	}
	if m.Want().Has(Audio) {
		t.Fatal("Chinese key must not register a capability")
	}
	if m.Size() != 1 {
		t.Fatalf("size = %d, want 1", m.Size())
	}
}

func TestParseKindAcceptsCaseAndSpace(t *testing.T) {
	for _, s := range []string{"Vision", " vision ", "IMAGE", "Tool", "FUNCTION", "Document", "doc"} {
		if _, ok := ParseKind(s); !ok {
			t.Fatalf("ParseKind(%q) rejected", s)
		}
	}
}

func TestParseSkipsInvalidSegments(t *testing.T) {
	m := Parse("bogus:x;vision:;:y;;vision:ok")
	if v, ok := m.Lookup(Vision); !ok || v != "ok" {
		t.Fatalf("got %q ok=%v", v, ok)
	}
	if m.Size() != 1 {
		t.Fatalf("size = %d, want 1", m.Size())
	}
}

func TestParseUpstreamNameWithColon(t *testing.T) {
	// 火山的 endpoint 名可能含冒号，只切第一个。
	m := Parse("vision:ep:20240101:abc")
	if v, _ := m.Lookup(Vision); v != "ep:20240101:abc" {
		t.Fatalf("got %q", v)
	}
}

func TestDetect(t *testing.T) {
	all := Set(0).Add(Vision).Add(Audio).Add(Video).Add(Tools).Add(File)

	cases := []struct {
		name string
		body string
		want Kind
	}{
		{"openai image_url", `{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/1.png"}}]}]}`, Vision},
		{"responses input_image", `{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"data:..."}]}]}`, Vision},
		{"anthropic image", `{"model":"m","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"x"}}]}]}`, Vision},
		{"input_audio", `{"model":"m","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"x","format":"wav"}}]}]}`, Audio},
		{"video_url", `{"model":"m","messages":[{"role":"user","content":[{"type":"video_url","video_url":{"url":"http://x/1.mp4"}}]}]}`, Video},
		{"file with file_id", `{"model":"m","messages":[{"role":"user","content":[{"type":"file","file":{"file_id":"file-abc"}}]}]}`, File},
		{"tools array", `{"model":"m","messages":[],"tools":[{"type":"function","function":{"name":"f"}}]}`, Tools},
		{"legacy functions", `{"model":"m","functions":[{"name":"f"}]}`, Tools},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Detect([]byte(c.body), all)
			if !got.Has(c.want) {
				t.Fatalf("body %s: got %v, want %v", c.body, got, c.want)
			}
		})
	}
}

func TestDetectNegatives(t *testing.T) {
	all := Set(0).Add(Vision).Add(Audio).Add(Video).Add(Tools).Add(File)

	cases := []struct{ name, body string }{
		{"plain text", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`},
		{"text parts", `{"model":"m","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`},
		// 空 tools 是 SDK 常见默认值，不能当成用到了工具调用。
		{"empty tools", `{"model":"m","messages":[],"tools":[]}`},
		{"tool_choice none", `{"model":"m","tool_choice":"none"}`},
		{"tool_choice auto", `{"model":"m","tool_choice":"auto"}`},
		// 裸 type=file 不带任何文件引用，语义不明，不算文档理解。
		{"bare file type", `{"model":"m","messages":[{"role":"user","content":[{"type":"file"}]}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Detect([]byte(c.body), all); !got.Empty() {
				t.Fatalf("body %s: got %v, want empty", c.body, got)
			}
		})
	}
}

func TestDetectOnlyScansWantedCapabilities(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}],"tools":[{"type":"function"}]}`)
	// 只声明了 tools，识图不该被报出来 —— 没有切换目标就没有识别意义。
	got := Detect(body, Set(0).Add(Tools))
	if got.Has(Vision) {
		t.Fatal("vision should not be detected when not wanted")
	}
	if !got.Has(Tools) {
		t.Fatal("tools should be detected")
	}
	if Detect(body, 0) != 0 {
		t.Fatal("empty want must short-circuit")
	}
}

func TestDetectMultipleCapabilities(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"u"}}]}],"tools":[{"type":"function","function":{"name":"f"}}]}`)
	got := Detect(body, Set(0).Add(Vision).Add(Tools))
	if !got.Has(Vision) || !got.Has(Tools) {
		t.Fatalf("got %v, want both vision and tools", got)
	}
}

func TestFromStatic(t *testing.T) {
	m := FromStatic(map[string][]string{
		"image":   {"vm"},
		"unknown": {"x"},
		"tools":   {"tm1", "tm2"},
	})
	if v, ok := m.Lookup(Vision); !ok || v != "vm" {
		t.Fatalf("vision = %q ok=%v", v, ok)
	}
	if !m.Want().Has(Tools) || m.Size() != 2 {
		t.Fatalf("want=%v size=%d", m.Want(), m.Size())
	}
}

func TestCacheReturnsSameTable(t *testing.T) {
	c := NewCache(64)
	raw := "vision:a;audio:b"
	if c.Get(raw) != c.Get(raw) {
		t.Fatal("cache should return the identical pointer")
	}
	if !c.Get("").Empty() {
		t.Fatal("empty header must yield empty map")
	}
}

func TestSetString(t *testing.T) {
	s := Set(0).Add(Vision).Add(Tools)
	if s.String() != "vision,tools" {
		t.Fatalf("got %q", s.String())
	}
	if Set(0).String() != "" {
		t.Fatal("empty set should stringify to empty")
	}
}

func BenchmarkDetectPlainText(b *testing.B) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hello world"}]}`)
	want := Set(0).Add(Vision).Add(Tools)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Detect(body, want)
	}
}

func BenchmarkDetectVision(b *testing.B) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://x/1.png"}}]}]}`)
	want := Set(0).Add(Vision)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Detect(body, want)
	}
}
