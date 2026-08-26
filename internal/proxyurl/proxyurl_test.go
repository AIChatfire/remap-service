package proxyurl

import "testing"

func TestValidateSchemes(t *testing.T) {
	for _, v := range []string{
		"http://127.0.0.1:8080",
		"https://proxy.example.com:443",
		"socks5://127.0.0.1:1080",
		"socks5h://127.0.0.1:1080",
		"socks5://user:pass@127.0.0.1:1080",
	} {
		if _, err := Validate(v); err != nil {
			t.Errorf("Validate(%q) 应通过，得到 %v", v, err)
		}
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]string{
		"空串":           "",
		"仅空白":          "   ",
		"裸 host:port":  "127.0.0.1:1080",
		"无 scheme 的域名": "proxy.example.com",
		"不支持的 scheme":  "ftp://127.0.0.1:21",
		"socks4":       "socks4://127.0.0.1:1080",
		"缺主机名":         "socks5://",
	}
	for name, v := range cases {
		if _, err := Validate(v); err == nil {
			t.Errorf("%s：Validate(%q) 应报错", name, v)
		}
	}
}

// scheme 大小写不同应规范化到同一个字符串，否则 Router 会为
// 同一个代理建出两个独立连接池。
func TestValidateNormalizesScheme(t *testing.T) {
	a, err := Validate("SOCKS5://127.0.0.1:1080")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Validate("socks5://127.0.0.1:1080")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("大小写不同应规范化为同一形式：%q vs %q", a, b)
	}
}

func TestRedactStripsCredentials(t *testing.T) {
	got := Redact("socks5://user:s3cret@127.0.0.1:1080")
	if got != "socks5://127.0.0.1:1080" {
		t.Errorf("脱敏结果 = %q", got)
	}
	for _, leak := range []string{"user", "s3cret"} {
		if contains(got, leak) {
			t.Errorf("脱敏后仍含 %q：%q", leak, got)
		}
	}
	if Redact("") != "" {
		t.Error("空串应返回空串")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
