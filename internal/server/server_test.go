package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/obs"
)

func testObs(t *testing.T) *obs.Provider {
	t.Helper()
	o, err := obs.New(context.Background(), config.Obs{LogLevel: "error", LogFormat: "text"})
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func testCfg() *config.Config {
	return &config.Config{
		Addr:     ":0",
		Upstream: config.Upstream{Base: "https://x.com", AllowBaseHeader: true},
	}
}

func TestHealthAndReady(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))

	// 未 Run 时 readyz 应为 503，避免实例未就绪就被打流量
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("未就绪时 readyz = %d, want 503", w.Code)
	}

	s.ready.Store(true)
	w = httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != 200 {
		t.Errorf("就绪后 readyz = %d", w.Code)
	}

	w = httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"ok"`) {
		t.Errorf("healthz = %d %s", w.Code, w.Body.String())
	}
}

// 网关不做准入鉴权：无凭据的请求也应进入代理链路（由上游判定合法性）。
func TestNoGatewayLevelAuth(t *testing.T) {
	var reached bool
	s := New(testCfg(), nil, testObs(t))
	// 直接替换 /v1 处理器，验证中间件没有拦截
	mux := http.NewServeMux()
	mux.Handle("/v1/", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(204)
	}))
	s.http.Handler = recoverMW(testObs(t))(mux)

	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
	if !reached || w.Code != 204 {
		t.Fatalf("无凭据请求应直达代理链路，code = %d reached = %v", w.Code, reached)
	}
}

func TestRecoverMiddleware(t *testing.T) {
	h := recoverMW(testObs(t))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/x", nil))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	if !strings.Contains(w.Body.String(), "gateway_error") {
		t.Errorf("panic 响应格式不符: %s", w.Body.String())
	}
}

func TestUnknownEndpoint(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))
	w := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/not-exist", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unknown endpoint") {
		t.Errorf("404 响应 = %s", w.Body.String())
	}
}

// 流式响应可能持续数分钟，不能设 WriteTimeout。
func TestNoWriteTimeout(t *testing.T) {
	s := New(testCfg(), nil, testObs(t))
	if s.http.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v，会误杀长连接流式响应", s.http.WriteTimeout)
	}
}
