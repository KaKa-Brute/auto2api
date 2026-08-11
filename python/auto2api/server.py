"""Starlette 应用装配：加载配置、构建调度器/转发器/日志/健康检查，注册路由。
对齐 Go 版 main.go 的启动流程。
"""
import logging

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.routing import Route

from . import config as cfgmod
from .admin import AdminHandler
from .call_logger import CallLogger
from .forwarder import Forwarder
from .handler import Handler
from .healthcheck import HealthChecker
from .limiter import create_limiter
from .memguard import create_memguard
from .recovery import RecoveryMiddleware
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


def build_app(cfg: cfgmod.Config, config_path: str = "config.yaml", restart=None) -> Starlette:
    """从配置构建 Starlette 应用（含调度器、转发器、日志、健康检查、路由）。

    config_path 用于管理台读写配置；restart 为触发服务重启的回调（可为 None）。
    """
    scheduler = Scheduler(cfg)
    
    # 定期清理超过 30 天的 token 分片（每天凌晨 1 点 UTC）
    import asyncio
    from datetime import datetime, timezone, timedelta
    async def cleanup_task():
        while True:
            now = datetime.now(timezone.utc)
            next_run = now.replace(hour=1, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            await asyncio.sleep((next_run - now).total_seconds())
            scheduler.cleanup_old_tokens(30)
    asyncio.create_task(cleanup_task())
    
    logger = CallLogger(cfg.log.dir, cfg.log.enabled, cfg.log.redact_keys,
                        cfg.log.log_upstream, cfg.log.body_limit,
                        log_resp_body=cfg.log.log_resp_body,
                        max_size_mb=cfg.log.max_size_mb,
                        max_age_days=cfg.log.max_age_days,
                        max_backups=cfg.log.max_backups,
                        compress=cfg.log.compress)
    forwarder = Forwarder(logger)
    handler = Handler(scheduler, forwarder, cfg.server.max_model_switches,
                      cfg.server.api_keys, logger)

    # 服务保护：并发限流 + 内存守护（防崩溃三板斧之二，第三为 recovery 中间件）
    limiter = None
    if cfg.protection.enabled and cfg.protection.max_concurrent > 0:
        qt = cfgmod.parse_duration(cfg.protection.queue_timeout)
        limiter = create_limiter(cfg.protection.max_concurrent,
                                 cfg.protection.max_queue_size, qt)
        _log.info("concurrency limiter enabled: max=%d queue=%d timeout=%s",
                  cfg.protection.max_concurrent, cfg.protection.max_queue_size,
                  cfg.protection.queue_timeout)
    memguard = None
    if cfg.protection.enabled and cfg.protection.max_memory_mb > 0:
        ci = cfgmod.parse_duration(cfg.protection.memory_check_interval)
        memguard = create_memguard(cfg.protection.max_memory_mb,
                                   cfg.protection.memory_warn,
                                   cfg.protection.memory_critical, ci)
    handler.set_protection(limiter, memguard)

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

    # 可视化管理台（/chat，免鉴权）：编辑配置、重启服务、查看日志。
    admin = AdminHandler(config_path, cfg.log.dir, restart)
    routes.extend(admin.routes())

    async def on_startup():
        breaker_status = "enabled" if cfg.breaker.enabled else "disabled"
        _log.info("auto2api(python) listening, chains: %s, breaker: %s, call_log: %s, admin: /chat",
                  scheduler.list_chains(), breaker_status, logger.enabled())
        if health_checker is not None:
            health_checker.start()
            _log.info("health checker enabled: interval=%s timeout=%s",
                      cfg.health_check.interval, cfg.health_check.timeout)
        # 内存守护后台任务须在事件循环内启动
        if memguard is not None:
            memguard.start()

    async def on_shutdown():
        # 优雅关闭：停止后台任务，关闭上游连接池
        if memguard is not None:
            await memguard.stop()
        if health_checker is not None:
            await health_checker.stop()
        await forwarder.aclose()

    # recovery 中间件：捕获所有未处理异常，记录堆栈并返回 500（防崩溃三板斧之三）
    middleware = [Middleware(RecoveryMiddleware)]
    app = Starlette(routes=routes, middleware=middleware,
                    on_startup=[on_startup], on_shutdown=[on_shutdown])
    app.state.config = cfg
    return app
