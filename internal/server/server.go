// Package server 组装 HTTP 路由与生命周期管理。
//
// 网关不做准入鉴权：客户端凭据直接透传给上游，合法性由上游判定。
// 因此这里只保留路由、panic 恢复与优雅退出。
package server

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/gateway"
	"github.com/betterme/remap-service/internal/obs"
)

// Server 承载入口 HTTP 服务。
type Server struct {
	cfg   *config.Config
	http  *http.Server
	o     *obs.Provider
	ready atomic.Bool
}

// New 构建服务器。
func New(cfg *config.Config, gw *gateway.Gateway, o *obs.Provider) *Server {
	s := &Server{cfg: cfg, o: o}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, `{"status":"shutting_down"}`)
			return
		}
		// 暴露在途/容量，便于负载均衡器与运维判断是否接近过载。
		cur, limit := gw.InFlight()
		writeJSON(w, http.StatusOK, `{"status":"ready","inflight":`+
			strconv.Itoa(cur)+`,"limit":`+strconv.Itoa(limit)+`}`)
	})
	// 动态路由：/v1 下的所有路径统一透传，协议由网关按路径自行识别。
	mux.Handle("/v1/", gw)
	mux.Handle("/", notFound())

	s.http = &http.Server{
		Addr:              cfg.Addr,
		Handler:           recoverMW(o)(mux),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
		// 不设 WriteTimeout：流式响应可能持续数分钟。
		ErrorLog: slog.NewLogLogger(o.Logger.Handler(), slog.LevelWarn),
	}
	return s
}

// Run 启动服务并在 ctx 取消时优雅退出。
func (s *Server) Run(ctx context.Context) error {
	s.ready.Store(true)
	errCh := make(chan error, 1)
	go func() {
		s.o.Logger.Info("网关启动", s.cfg.Summary()...)
		if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// 先摘流量（readyz 转 503），给负载均衡器反应时间，再关闭连接。
	s.ready.Store(false)
	s.o.Logger.Info("开始优雅退出")
	sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return s.http.Shutdown(sctx)
}

// recoverMW 拦截 panic，避免单个请求打崩整个进程。
func recoverMW(o *obs.Provider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					if rec == http.ErrAbortHandler {
						panic(rec)
					}
					o.Logger.Error("请求处理 panic",
						slog.Any("panic", rec), slog.String("path", r.URL.Path))
					writeJSON(w, http.StatusInternalServerError,
						`{"error":{"message":"internal gateway error","type":"gateway_error","param":null,"code":null}}`)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func notFound() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound,
			`{"error":{"message":"unknown endpoint","type":"invalid_request_error","param":null,"code":null}}`)
	})
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
