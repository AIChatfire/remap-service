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

	// 先摘流量（readyz 转 503），等负载均衡器探测到这个 503 再关闭连接。
	//
	// 这里的等待不能省：LB 的健康检查间隔通常 5~10s，在此之前它仍会把新请求
	// 打到本实例上，而那些请求会直接撞上正在关停的进程（502 / 连接被拒），
	// 在 LB 层面表现为「这个实例偶发错误」。此前注释写了「给负载均衡器反应
	// 时间」但代码紧接着就关连接，那段窗口是实际存在的。
	//
	// 注意等待期间不响应第二次 SIGTERM（signal.NotifyContext 只取消一次）；
	// 真正的兜底仍是编排层的 stop_grace_period，超时后 SIGKILL 一定生效。
	lc := s.cfg.Lifecycle.Effective()
	s.ready.Store(false)
	s.o.Logger.Info("开始优雅退出",
		slog.String("drain_delay", lc.DrainDelay.String()),
		slog.String("shutdown_timeout", lc.ShutdownTimeout.String()),
	)
	if lc.DrainDelay > 0 {
		time.Sleep(lc.DrainDelay)
	}
	sctx, cancel := context.WithTimeout(context.Background(), lc.ShutdownTimeout)
	defer cancel()
	return s.http.Shutdown(sctx)
}

// statusWriter 记录「响应头是否已经下发」，供 recoverMW 判断还能不能改状态码。
//
// 为什么必须完整代理 http.Flusher：本进程的流式转发会直接做
// `w.(http.Flusher)` 断言（见 gateway.pipeStream），包装后一旦丢掉这个接口，
// SSE 会退化成「攒满缓冲才发」—— 表现为首字节从毫秒涨到秒级，
// 而且不报错、不失败，只是端到端延迟变了。Unwrap 供 http.ResponseController
// 之类的反射式取用（它按 Unwrap 链找真正的 writer）。
type statusWriter struct {
	http.ResponseWriter
	// wrote 只被同一个 goroutine 读写（panic 在同 goroutine 内 recover），
	// 无需原子操作。
	wrote bool
}

func (w *statusWriter) WriteHeader(code int) {
	w.wrote = true
	w.ResponseWriter.WriteHeader(code)
}

// Write 也置位：隐式 200 同样意味着状态码已经定死，之后不可能再改成 500。
func (w *statusWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// recoverMW 拦截 panic，避免单个请求打崩整个进程。
func recoverMW(o *obs.Provider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w}
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler {
					panic(rec)
				}
				// panic 只进日志时无法回答「这个月发生过几次」—— 与项目
				// 「静默失效最难发现」的取向一致，这里补一个计数器。
				// 用请求 ctx 而非 Background：SpanContext 随之带上，
				// 看板可以把这次 panic 与那条 trace 对上。
				obs.Add(r.Context(), o.Metrics.Panics, 1)
				o.Logger.Error("请求处理 panic",
					slog.Any("panic", rec), slog.String("path", r.URL.Path),
					slog.Bool("response_started", sw.wrote))

				// 响应头已经下发过就不能再写错误体：WriteHeader(500) 会被
				// net/http 记为 superfluous 而不生效，而那段 JSON 会被**直接
				// 追加**到已经下发的正文里 —— 流式转发中最容易踩到（响应头
				// 与多个 chunk 早已 flush 出去），结果客户端拿到一段结构损坏
				// 的响应（合法 SSE 后面跟一坨裸 JSON），而不是一个明确的错误。
				// 此时只能记日志：连接已定局，唯一的干净收尾是让客户端自己
				// 判定流被截断。
				if sw.wrote {
					return
				}
				writeJSON(sw, http.StatusInternalServerError,
					`{"error":{"message":"internal gateway error","type":"gateway_error","param":null,"code":null}}`)
			}()
			next.ServeHTTP(sw, r)
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
