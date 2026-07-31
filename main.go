// auto2api 入口：加载 YAML 配置，启动优先级自动切换网关。
// 可选启用熔断器与后台健康检查（见 config.Breaker / config.HealthCheck）。
// 服务保护三板斧（见 config.Protection）：
//   1. 并发限流：防上游超时导致连接堆积
//   2. 内存守护：防内存泄漏/OOM，超阈值自动降级
//   3. panic recovery：防未捕获异常导致进程退出
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
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

	breakerStatus := "disabled"
	if cfg.Breaker.Enabled {
		breakerStatus = "enabled"
	}
	log.Printf("auto2api listening on %s, chains: %v, breaker: %s, call_log: %v",
		cfg.Server.Addr, sched.ListChains(), breakerStatus, logger.Enabled())

	// HTTP server + 优雅关闭：收到 SIGINT/SIGTERM 时停止接收新请求，
	// 等待在途请求完成（最多 30s），并停止后台 goroutine。
	srv := &http.Server{Addr: cfg.Server.Addr, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down gracefully...")

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
	log.Println("server stopped")
}