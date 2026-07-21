// auto2api 入口：加载 YAML 配置，启动优先级自动切换网关。
package main

import (
	"flag"
	"log"

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
	logger := gateway.NewCallLogger(cfg.Log.Dir, cfg.Log.Enabled, cfg.Log.RedactKeys, cfg.Log.LogUpstream, cfg.Log.BodyLimit)
	fwd := gateway.NewForwarder(logger)
	h := gateway.NewHandler(sched, fwd, cfg.Server.MaxModelSwitches, cfg.Server.APIKeys, logger)

	r := gin.Default()
	h.Register(r)
	log.Printf("auto2api listening on %s, chains: %v, call_log: %v", cfg.Server.Addr, sched.ListChains(), logger.Enabled())
	if err := r.Run(cfg.Server.Addr); err != nil {
		log.Fatalf("server: %v", err)
	}
}
