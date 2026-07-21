// auto2api 入口：加载 YAML 配置，启动优先级自动切换网关。
// 可选启用熔断器与后台健康检查（见 config.Breaker / config.HealthCheck）。
package main

import (
	"flag"
	"log"
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
	if cfg.HealthCheck.Enabled {
		interval, _ := time.ParseDuration(cfg.HealthCheck.Interval)
		timeout, _ := time.ParseDuration(cfg.HealthCheck.Timeout)
		hc := gateway.NewHealthChecker(sched, interval, timeout)
		sched.SetHealthChecker(hc)
		hc.Start()
		log.Printf("health checker enabled: interval=%s timeout=%s", interval, timeout)
	}
	logger := gateway.NewCallLogger(cfg.Log.Dir, cfg.Log.Enabled, cfg.Log.RedactKeys, cfg.Log.LogUpstream, cfg.Log.BodyLimit)
	fwd := gateway.NewForwarder(logger)
	h := gateway.NewHandler(sched, fwd, cfg.Server.MaxModelSwitches, cfg.Server.APIKeys, logger)

	r := gin.Default()
	h.Register(r)
	breakerStatus := "disabled"
	if cfg.Breaker.Enabled {
		breakerStatus = "enabled"
	}
	log.Printf("auto2api listening on %s, chains: %v, breaker: %s, call_log: %v",
		cfg.Server.Addr, sched.ListChains(), breakerStatus, logger.Enabled())
	if err := r.Run(cfg.Server.Addr); err != nil {
		log.Fatalf("server: %v", err)
	}
}
