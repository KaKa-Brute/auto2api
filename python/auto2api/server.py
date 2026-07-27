"""Starlette 应用装配：加载配置、构建调度器/转发器/日志/健康检查，注册路由。
对齐 Go 版 main.go 的启动流程。
"""
import logging

from starlette.applications import Starlette
from starlette.routing import Route

from . import config as cfgmod
from .call_logger import CallLogger
from .forwarder import Forwarder
from .handler import Handler
from .healthcheck import HealthChecker
from .scheduler import Scheduler

_log = logging.getLogger("auto2api")


def parse_addr(addr: str):
    """把 Go 风格监听地址（如 ':8080' / '127.0.0.1:8080'）解析为 (host, port)。"""
    addr = addr.strip()
    if ":" not in addr:
        return "0.0.0.0", int(addr)
    host, _, port = addr.rpartition(":")
    host = host or "0.0.0.0"
    return host, int(port)


def build_app(cfg: cfgmod.Config) -> Starlette:
    """从配置构建 Starlette 应用（含调度器、转发器、日志、健康检查、路由）。"""
    scheduler = Scheduler(cfg)
    logger = CallLogger(cfg.log.dir, cfg.log.enabled, cfg.log.redact_keys,
                        cfg.log.log_upstream, cfg.log.body_limit)
    forwarder = Forwarder(logger)
    handler = Handler(scheduler, forwarder, cfg.server.max_model_switches,
                      cfg.server.api_keys, logger)

    health_checker = None
    if cfg.health_check.enabled:
        interval = cfgmod.parse_duration(cfg.health_check.interval)
        timeout = cfgmod.parse_duration(cfg.health_check.timeout)
        health_checker = HealthChecker.create(scheduler, interval, timeout)
        if health_checker is not None:
            scheduler.set_health_checker(health_checker)

    routes = [
        Route("/v1/chat/completions", handler.chat_completions, methods=["POST"]),
        Route("/chat/completions", handler.chat_completions, methods=["POST"]),
        Route("/v1/messages", handler.messages, methods=["POST"]),
        Route("/messages", handler.messages, methods=["POST"]),
        Route("/v1/models", handler.models, methods=["GET"]),
        Route("/v1/health", handler.health, methods=["GET"]),
    ]

    async def on_startup():
        breaker_status = "enabled" if cfg.breaker.enabled else "disabled"
        _log.info("auto2api(python) listening, chains: %s, breaker: %s, call_log: %s",
                  scheduler.list_chains(), breaker_status, logger.enabled())
        if health_checker is not None:
            health_checker.start()
            _log.info("health checker enabled: interval=%s timeout=%s",
                      cfg.health_check.interval, cfg.health_check.timeout)

    async def on_shutdown():
        if health_checker is not None:
            await health_checker.stop()
        await forwarder.aclose()

    app = Starlette(routes=routes, on_startup=[on_startup], on_shutdown=[on_shutdown])
    app.state.config = cfg
    return app
