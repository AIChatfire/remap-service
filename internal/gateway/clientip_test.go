package gateway

import (
	"net/http"
	"testing"
)

func req(remote, xff string) *http.Request {
	r := &http.Request{RemoteAddr: remote, Header: http.Header{}}
	if xff != "" {
		r.Header.Set("X-Forwarded-For", xff)
	}
	return r
}

func TestClientIP(t *testing.T) {
	cases := []struct {
		name   string
		remote string
		xff    string
		hops   int
		want   string
	}{
		{
			name:   "hops=0 忽略 XFF",
			remote: "10.0.0.1:5566",
			xff:    "1.2.3.4",
			hops:   0,
			want:   "10.0.0.1",
		},
		{
			name:   "hops=0 无 XFF 时去掉端口",
			remote: "203.0.113.9:443",
			hops:   0,
			want:   "203.0.113.9",
		},
		{
			name:   "hops=1 取 XFF 最右",
			remote: "10.0.0.1:5566",
			xff:    "1.2.3.4, 203.0.113.7",
			hops:   1,
			want:   "203.0.113.7",
		},
		{
			name:   "hops=2 向左跳一位",
			remote: "10.0.0.1:5566",
			xff:    "1.2.3.4, 203.0.113.7, 10.0.0.9",
			hops:   2,
			want:   "203.0.113.7",
		},
		{
			name:   "hops 超过链长时取最左",
			remote: "10.0.0.1:5566",
			xff:    "203.0.113.7",
			hops:   3,
			want:   "203.0.113.7",
		},
		{
			name:   "hops>0 但无 XFF 回落 RemoteAddr",
			remote: "10.0.0.1:5566",
			hops:   1,
			want:   "10.0.0.1",
		},
		{
			name:   "IPv6 RemoteAddr 保留原形",
			remote: "[2001:db8::1]:8443",
			hops:   0,
			want:   "2001:db8::1",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := clientIP(req(c.remote, c.xff), c.hops); got != c.want {
				t.Fatalf("clientIP = %q, want %q", got, c.want)
			}
		})
	}
}

// TestClientIPIgnoresSpoofedXFF 守住核心安全属性：hops=0 时客户端自行
// 追加的 XFF 一律不采纳。若哪天默认值被改成从最左取值，这条会失败。
func TestClientIPIgnoresSpoofedXFF(t *testing.T) {
	r := req("198.51.100.5:9000", "127.0.0.1, 10.0.0.1, evil")
	if got := clientIP(r, 0); got != "198.51.100.5" {
		t.Fatalf("伪造的 XFF 被采纳: %q", got)
	}
}
