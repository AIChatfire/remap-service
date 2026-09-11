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
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/betterme/remap-service/internal/config"
	"github.com/betterme/remap-service/internal/gateway"
	"github.com/betterme/remap-service/internal/memlimit"
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
		healthCheck = flag.Bool("healthcheck", false, "自探本机 /healthz，成功后退出（供容器存活探针调用）")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("remap-gateway %s (commit %s, built %s, %s)\n", version, commit, date, runtime.Version())
		return
	}
	// 探针路径必须在 run 之前：它要能在配置不合法时也给出明确结论，
	// 而不是被 run 里的初始化错误掩盖成「探针本身失败」。
	if *healthCheck {
		if err := selfHealthCheck(*envFile); err != nil {
			fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := run(*envFile, *checkOnly); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// selfHealthCheck 在进程内探测本机 /healthz。
//
// 为什么要由二进制自己做，而不是交给 curl / wget：
//
//	镜像是 distroless（见 Dockerfile），既没有 shell 也没有任何 HTTP 客户端，
//	编排层的探针只能调用镜像内的命令。此前唯一可用的命令是 -check，而那是
//	启动期的**静态配置校验** —— 它与进程是否还能转发请求完全无关，网关死锁、
//	goroutine 全卡、监听 socket 不再 accept 时，它照样每次返回 0。
//	结果是 restart: unless-stopped 永不触发，等价于没有存活检查。
//
// 两个刻意的选择：
//
//   - **不经过任何代理**：HTTP_PROXY 会把 127.0.0.1 一起代理走，探针会去问
//     代理而不是本进程 —— 症状是拿到别的服务的响应、或莫名其妙的连接超时，
//     两者都会把「网关还活着吗」判断反。
//   - **不加载完整配置**：探针只回答「这个进程还在不在监听」。配置不合法时
//     进程根本起不来，轮不到探针来判断；把 config.Validate 拉进来只会让
//     一次配置写错表现为「探针异常」。
func selfHealthCheck(envFile string) error {
	// .env 也读一遍：容器里 env_file 已注入环境变量，两者等价；本地裸跑时
	// 保证探针与网关读到同一个 ADDR，否则探针会去敲一个没人监听的端口。
	if err := config.LoadDotEnv(envFile); err != nil {
		return err
	}
	host, port, err := probeHostPort(envOr("ADDR", ":8080"))
	if err != nil {
		return err
	}
	url := "http://" + net.JoinHostPort(host, port) + "/healthz"

	c := &http.Client{
		Timeout:   3 * time.Second,
		Transport: &http.Transport{Proxy: nil},
		// 健康检查不跟重定向：拿到 3xx 说明敲错了端点，这本身是失败。
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := c.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	// 必须把正文读完，否则连接无法复用（探针是短生命周期，这里只为干净收尾）；
	// 限长避免一个异常大的响应把探针拖成超时。
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: 状态码 %d", url, resp.StatusCode)
	}
	return nil
}

// probeHostPort 把监听地址 ADDR 拆成「可以直连的目标主机」与端口。
//
// 监听写法不能直接当 URL 主机用：":8080" 主机段为空、"0.0.0.0:8080" 与
// "[::]:8080" 则不是合法的目标地址（0.0.0.0 只能出现在 bind 一侧）。
// 三者一律换成回环地址 —— 探针与服务在同一个网络命名空间里，
// 回环必然可达，且不依赖外部 DNS。
//
// 地址**解析不了时必须报错，绝不回落到默认端口**。回落看着"友好"，
// 实际会制造假绿灯：ADDR 写成 "garbage" 时进程根本起不来（net.Listen 直接
// 报错），而探针却会去敲 8080 —— 那里若恰好有别的服务在应答，容器会一直
// 显示 healthy。实测本机 8080 上就有一个无关服务对 /healthz 返回 200。
// 让探针在这种情形下失败，才符合它「回答这个进程还在不在」的唯一职责。
func probeHostPort(addr string) (host, port string, err error) {
	s := strings.TrimSpace(addr)
	if s == "" {
		// 空值不是"猜"：config 的默认值就是 ":8080"，进程确实会监听它。
		return "127.0.0.1", "8080", nil
	}
	host, port, err = net.SplitHostPort(s)
	if err != nil {
		return "", "", fmt.Errorf("ADDR %q 不是合法的 host:port 形式: %w", s, err)
	}
	// SplitHostPort 只校验括号与冒号，不校验端口本身："host:port"、":-1"
	// 都能通过。这两者进程同样起不来（net.Listen 会报 unknown port），
	// 所以在这里一并拦掉，别让探针替一个起不来的进程报健康。
	if !isPort(port) {
		return "", "", fmt.Errorf("ADDR %q 的端口段 %q 不是纯数字", s, port)
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	return host, port, nil
}

// isPort 报告 s 是否为纯数字端口写法（不校验 1~65535，那由 net.Listen 判）。
func isPort(s string) bool {
	if s == "" || len(s) > 5 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
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
	//
	// 这个开关与下面的 GOMEMLIMIT 是一对，缺一不可：GOGC 决定「平时多懒」，
	// GOMEMLIMIT 决定「最坏能到多少」。只设 GOGC=300 等于「堆可以涨到
	// live set 的 4 倍且没有天花板」，上游一慢就会一路涨到被 cgroup
	// OOM-kill —— 而 OOM-kill 不留堆栈，比 GC 变激进难查得多。
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(300)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	o, err := obs.New(ctx, cfg.Obs)
	if err != nil {
		return fmt.Errorf("初始化可观测性失败: %w", err)
	}
	// 软内存上限由容器内存上限推导（见 memlimit 包），显式配置 GOMEMLIMIT 时让路。
	//
	// 放在可观测性初始化之后，是为了能把它记进启动日志：这个配置项配错的表现
	// 是「什么都没发生」，而它生效与否只体现在 GC 行为上 —— 没有一行日志，
	// 「配了没效果」与「本来就不需要」就没有任何区别。返回 0 时不打日志
	// （不在受限容器里 / 已有显式配置 / 上限过小），同样是为了不制造假信号。
	if n := memlimit.Apply(); n > 0 {
		o.Logger.Info("已按容器内存上限设置软内存上限",
			slog.Int64("gomemlimit_bytes", n), slog.String("gomemlimit", memlimit.Format(n)))
	}

	// 退出预算的三段在时间上串行：server.Run 内部先 drain 再 Shutdown，
	// 返回后才轮到这里的 flush。三段之和必须小于编排层的 stop_grace_period，
	// 否则最后这一段（唯一「不做就永久丢数据」的一段）会被 SIGKILL 掐掉。
	// 总和已由 cfg.Summary() 打进启动日志，便于与 compose 对齐。
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), cfg.Lifecycle.Effective().FlushTimeout)
		defer cancel()
		o.Shutdown(sctx)
	}()
	// o.CountH2Error 把 h2 的连接级事件（最要紧的是 conn_close_lost_ping）
	// 接进指标。这类事件不属于任何单个请求，端口留在 upstream 层、
	// 由这里注入，避免 upstream 反向依赖 obs。
	// o.CountProxyTransportEviction 同理由此注入：per-request 代理池的淘汰
	// 同样不属于任何单个请求，且是「上限是否过小」的唯一直接证据。
	tr := upstream.NewRoundTripper(cfg.Limits.MaxConns, cfg.Transport, cfg.Upstream,
		o.CountH2Error, o.CountProxyTransportEviction)
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
