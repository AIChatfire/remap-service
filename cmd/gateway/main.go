// Command gateway 是 LLM 模型映射网关的入口。
//
// 职责：请求改写（对外模型 -> 上游真实模型）+ 响应脱敏（还原为对外模型）。
// 上游协议由请求路径自动识别，客户端凭据直接透传，不做负载均衡与重试。
//
// 全部配置来自环境变量，默认读取 ./.env。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/gateway"
	"github.com/betterme/remap-service/internal/obs"
	"github.com/betterme/remap-service/internal/server"
	"github.com/betterme/remap-service/internal/upstream"
)

// 由 -ldflags 注入。
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var (
		envFile     = flag.String("env", envOr("ENV_FILE", ".env"), "环境变量文件路径")
		showVersion = flag.Bool("version", false, "打印版本并退出")
		checkOnly   = flag.Bool("check", false, "仅校验配置后退出")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("remap-gateway %s (commit %s, built %s, %s)\n", version, commit, date, runtime.Version())
		return
	}
	if err := run(*envFile, *checkOnly); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(envFile string, checkOnly bool) error {
	cfg, err := config.Load(envFile)
	if err != nil {
		return err
	}
	cfg.Obs.Version = version
	if checkOnly {
		fmt.Println("配置校验通过")
		return nil
	}

	// 网关是短生命周期对象密集型负载，默认 GOGC=100 会导致 GC 过于频繁。
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(300)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	o, err := obs.New(ctx, cfg.Obs)
	if err != nil {
		return fmt.Errorf("初始化可观测性失败: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		o.Shutdown(sctx)
	}()
	o.StartPrometheus(cfg.Obs.MetricsAddr)

	tr := upstream.NewTransport(cfg.Limits.MaxConns)
	defer tr.CloseIdleConnections()

	gw := gateway.New(cfg, upstream.NewClient(tr, cfg.Upstream), o)
	return server.New(cfg, gw, o).Run(ctx)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
