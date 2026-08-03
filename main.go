// auto2api 入口：加载 YAML 配置，启动优先级自动切换网关。
// 可选启用熔断器与后台健康检查（见 config.Breaker / config.HealthCheck）。
// 服务保护三板斧（见 config.Protection）：
//   1. 并发限流：防上游超时导致连接堆积
//   2. 内存守护：防内存泄漏/OOM，超阈值自动降级
//   3. panic recovery：防未捕获异常导致进程退出
// 内置可视化管理台（/chat，免鉴权）：编辑配置、重启服务、查看日志。
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"auto2api/internal/config"
	"auto2api/internal/gateway"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	sched, err := gateway.NewScheduler(cfg)
	if err != nil {
		log.Fatalf("init scheduler: %v", err)
	}
	// 后台主动健康检查（可选）
	var hc *gateway.HealthChecker
	if cfg.HealthCheck.Enabled {
		interval, _ := time.ParseDuration(cfg.HealthCheck.Interval)
		timeout, _ := time.ParseDuration(cfg.HealthCheck.Timeout)
		hc = gateway.NewHealthChecker(sched, interval, timeout)
		sched.SetHealthChecker(hc)
		hc.Start()
		log.Printf("health checker enabled: interval=%s timeout=%s", interval, timeout)
	}

	logger := gateway.NewCallLogger(cfg.Log.Dir, cfg.Log.Enabled, cfg.Log.RedactKeys,
		cfg.Log.LogUpstream, cfg.Log.BodyLimit, cfg.Log.LogRespBody,
		cfg.Log.MaxSizeMB, cfg.Log.MaxAgeDays, cfg.Log.MaxBackups, cfg.Log.Compress)
	fwd := gateway.NewForwarder(logger)
	h := gateway.NewHandler(sched, fwd, cfg.Server.MaxModelSwitches, cfg.Server.APIKeys, logger)

	// 服务保护：并发限流 + 内存守护
	var limiter *gateway.ConcurrencyLimiter
	var memGuard *gateway.MemoryGuard
	if cfg.Protection.Enabled && cfg.Protection.MaxConcurrent > 0 {
		qt, _ := time.ParseDuration(cfg.Protection.QueueTimeout)
		limiter = gateway.NewConcurrencyLimiter(cfg.Protection.MaxConcurrent, cfg.Protection.MaxQueueSize, qt)
		log.Printf("concurrency limiter enabled: max=%d queue=%d timeout=%s",
			cfg.Protection.MaxConcurrent, cfg.Protection.MaxQueueSize, cfg.Protection.QueueTimeout)
	}
	if cfg.Protection.Enabled && cfg.Protection.MaxMemoryMB > 0 {
		ci, _ := time.ParseDuration(cfg.Protection.MemoryCheckInterval)
		memGuard = gateway.NewMemoryGuard(cfg.Protection.MaxMemoryMB, cfg.Protection.MemoryWarn, cfg.Protection.MemoryCritical, ci)
		memGuard.Start()
	}
	h.SetProtection(limiter, memGuard)

	// gin 引擎：用自定义 recovery 中间件替代 gin.Default() 的默认 recovery，
	// 捕获所有 panic 防止进程退出，并记录完整堆栈与计数。
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Logger())
	r.Use(gateway.RecoveryMiddleware())
	h.Register(r)

	// 可视化管理台（/chat，免鉴权）：编辑配置、重启服务、查看日志。
	// restartRequested 用于让重启回调通知主循环执行“启动新进程 + 优雅退出”。
	restartRequested := make(chan struct{}, 1)
	restartFn := func() error {
		select {
		case restartRequested <- struct{}{}:
		default: // 已在重启中
		}
		return nil
	}
	admin := gateway.NewAdminHandler(*cfgPath, cfg.Log.Dir, restartFn)
	admin.Register(r)

	breakerStatus := "disabled"
	if cfg.Breaker.Enabled {
		breakerStatus = "enabled"
	}
	log.Printf("auto2api listening on %s, chains: %v, breaker: %s, call_log: %v, admin: /chat",
		cfg.Server.Addr, sched.ListChains(), breakerStatus, logger.Enabled())

	// 监听端口：带重试，便于重启时等待旧进程释放端口。
	ln, err := listenWithRetry(cfg.Server.Addr, 10*time.Second)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.Server.Addr, err)
	}

	// HTTP server + 优雅关闭：收到 SIGINT/SIGTERM 或重启请求时停止接收新请求，
	// 等待在途请求完成（最多 30s），并停止后台 goroutine。
	srv := &http.Server{Handler: r}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	restarting := false
	select {
	case <-quit:
		log.Println("shutting down gracefully...")
	case <-restartRequested:
		restarting = true
		log.Println("restart requested, spawning new process...")
		if err := spawnSelf(); err != nil {
			log.Printf("spawn new process failed: %v (aborting restart, keep running)", err)
			// 启动失败则不退出，继续等待信号，避免服务中断
			<-quit
			restarting = false
			log.Println("shutting down gracefully...")
		}
	}

	if memGuard != nil {
		memGuard.Stop()
	}
	if hc != nil {
		hc.Stop()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("forced shutdown: %v", err)
	}
	if restarting {
		log.Println("old process stopped, new process is taking over")
	} else {
		log.Println("server stopped")
	}
}

// listenWithRetry 在指定地址监听，若端口被占用则重试直到超时。
// 用于服务重启时新进程等待旧进程释放端口。
func listenWithRetry(addr string, timeout time.Duration) (net.Listener, error) {
	deadline := time.Now().Add(timeout)
	for {
		ln, err := net.Listen("tcp", addr)
		if err == nil {
			return ln, nil
		}
		if !isAddrInUse(err) || time.Now().After(deadline) {
			return nil, err
		}
		log.Printf("port %s busy, waiting for previous instance to release...", addr)
		time.Sleep(300 * time.Millisecond)
	}
}

// isAddrInUse 判断错误是否为“端口被占用”。
func isAddrInUse(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "address already in use") ||
		strings.Contains(s, "only one usage of each socket address") || // Windows
		strings.Contains(s, "in use")
}

// spawnSelf 以相同可执行文件与参数启动一个新进程（继承标准输入输出与工作目录）。
// 新进程通过 listenWithRetry 等待本进程释放端口后接管服务。
func spawnSelf() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Dir, _ = os.Getwd()
	cmd.Env = os.Environ()
	return cmd.Start()
}
