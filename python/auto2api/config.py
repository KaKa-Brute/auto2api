"""配置解析：多套命名链 + 每链多优先级模型。

支持 ${ENV} 环境变量展开、默认值填充、按 priority 升序排序。
"""
import os
import re
from dataclasses import dataclass, field
from typing import Dict, List

import yaml

_ENV_RE = re.compile(r"\$\{([A-Z0-9_]+)\}")
_DUR_RE = re.compile(r"([0-9]*\.?[0-9]+)(ns|us|µs|ms|s|m|h)")
_DUR_UNITS = {
    "ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3,
    "s": 1.0, "m": 60.0, "h": 3600.0,
}


def expand_env(s: str) -> str:
    """把 ${ENV} 替换成环境变量值（未设置则替换为空串）。"""
    return _ENV_RE.sub(lambda m: os.environ.get(m.group(1), ""), s)


def parse_duration(s: str) -> float:
    """解析 Go 风格时长字符串（如 "300ms" / "1.2s" / "60s"）为秒（float）。

    支持组合（如 "1h30m"）与单位 ns/us/µs/ms/s/m/h。空串或非法抛 ValueError。
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("empty duration")
    total = 0.0
    consumed = 0
    for m in _DUR_RE.finditer(s):
        if m.start() != consumed:
            raise ValueError(f"invalid duration {s!r}")
        total += float(m.group(1)) * _DUR_UNITS[m.group(2)]
        consumed = m.end()
    if consumed != len(s):
        raise ValueError(f"invalid duration {s!r}")
    return total


def parse_durations(ss: List[str]) -> List[float]:
    """把字符串列表解析成秒列表。"""
    return [parse_duration(x) for x in ss]


@dataclass
class UpstreamConfig:
    provider: str = ""          # 仅作标记，不影响转发逻辑
    base_url: str = ""          # 上游 API 根地址，自动追加 /v1/chat/completions
    model: str = ""             # 上游真实模型名（模型映射目标）
    api_key: str = ""           # 支持 ${ENV} 展开
    auth_header: str = ""       # Authorization（默认）/ x-api-key / x-goog-api-key
    timeout: str = ""


@dataclass
class RetryConfig:
    count: int = 0                          # 同模型内重试次数（不含首次）
    backoff: List[str] = field(default_factory=list)  # 指数退避序列
    retryable_status: List[int] = field(default_factory=list)


@dataclass
class FailoverConfig:
    trigger_status: List[int] = field(default_factory=list)  # 触发切换下一优先级
    cooldown: str = ""                                       # 失败后冷却不可用时间


@dataclass
class StreamConfig:
    idle_timeout: str = ""   # SSE 无数据超时
    keepalive: str = ""      # SSE keepalive ping 间隔


@dataclass
class ModelConfig:
    name: str = ""
    priority: int = 0
    upstream: UpstreamConfig = field(default_factory=UpstreamConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    failover: FailoverConfig = field(default_factory=FailoverConfig)
    stream: StreamConfig = field(default_factory=StreamConfig)


@dataclass
class ChainConfig:
    models: List[ModelConfig] = field(default_factory=list)


@dataclass
class ServerConfig:
    addr: str = ":8080"
    max_model_switches: int = 5
    api_keys: List[str] = field(default_factory=list)


@dataclass
class LogConfig:
    enabled: bool = False
    dir: str = "logs"
    redact_keys: bool = True
    body_limit: int = 8192
    log_upstream: bool = True
    log_resp_body: bool = True
    max_size_mb: int = 100
    max_age_days: int = 7
    max_backups: int = 10
    compress: bool = True


@dataclass
class BreakerConfig:
    enabled: bool = False
    failure_threshold: int = 0
    open_duration: str = ""
    half_open_max: int = 0


@dataclass
class HealthCheckConfig:
    enabled: bool = False
    interval: str = ""
    timeout: str = ""


@dataclass
class ProtectionConfig:
    """服务保护配置（防崩溃三板斧）。"""
    enabled: bool = False          # 总开关，默认关闭；关闭时不启用并发限流与内存守护
    # 并发限流：防止上游 API 超时导致连接堆积
    max_concurrent: int = 0        # 最大并发请求数，0=不限制
    max_queue_size: int = 0        # 等待队列长度，0=auto(2x concurrent)
    queue_timeout: str = ""        # 队列等待超时（如 "30s"）
    # 内存守护：防止内存泄漏或 OOM
    max_memory_mb: int = 0         # 最大允许内存（MB），0=不监控
    memory_warn: float = 0.0       # 警告阈值（0.8 = 80%）
    memory_critical: float = 0.0   # 临界阈值（0.9 = 90%）
    memory_check_interval: str = ""  # 检查周期（如 "10s"）


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    log: LogConfig = field(default_factory=LogConfig)
    breaker: BreakerConfig = field(default_factory=BreakerConfig)
    health_check: HealthCheckConfig = field(default_factory=HealthCheckConfig)
    protection: ProtectionConfig = field(default_factory=ProtectionConfig)
    chains: Dict[str, ChainConfig] = field(default_factory=dict)


def _model_from_dict(d: dict) -> ModelConfig:
    up = d.get("upstream", {}) or {}
    rt = d.get("retry", {}) or {}
    fo = d.get("failover", {}) or {}
    sm = d.get("stream", {}) or {}
    return ModelConfig(
        name=d.get("name", "") or "",
        priority=int(d.get("priority", 0) or 0),
        upstream=UpstreamConfig(
            provider=up.get("provider", "") or "",
            base_url=up.get("base_url", "") or "",
            model=up.get("model", "") or "",
            api_key=up.get("api_key", "") or "",
            auth_header=up.get("auth_header", "") or "",
            timeout=up.get("timeout", "") or "",
        ),
        retry=RetryConfig(
            count=int(rt.get("count", 0) or 0),
            backoff=list(rt.get("backoff", []) or []),
            retryable_status=[int(x) for x in (rt.get("retryable_status", []) or [])],
        ),
        failover=FailoverConfig(
            trigger_status=[int(x) for x in (fo.get("trigger_status", []) or [])],
            cooldown=fo.get("cooldown", "") or "",
        ),
        stream=StreamConfig(
            idle_timeout=sm.get("idle_timeout", "") or "",
            keepalive=sm.get("keepalive", "") or "",
        ),
    )


def load(path: str) -> Config:
    """读取并校验配置，填充默认值，按 priority 升序排序每条链的模型。"""
    with open(path, "r", encoding="utf-8") as fp:
        raw = yaml.safe_load(fp) or {}

    srv = raw.get("server", {}) or {}
    server = ServerConfig(
        addr=srv.get("addr", "") or "",
        max_model_switches=int(srv.get("max_model_switches", 0) or 0),
        api_keys=list(srv.get("api_keys", []) or []),
    )
    lg = raw.get("log", {}) or {}
    log = LogConfig(
        enabled=bool(lg.get("enabled", False)),
        dir=lg.get("dir", "") or "",
        redact_keys=bool(lg.get("redact_keys", True)),
        body_limit=int(lg.get("body_limit", 0) or 0),
        log_upstream=bool(lg.get("log_upstream", True)),
        log_resp_body=bool(lg.get("log_resp_body", True)),
        max_size_mb=int(lg.get("max_size_mb", 0) or 0),
        max_age_days=int(lg.get("max_age_days", 0) or 0),
        max_backups=int(lg.get("max_backups", 0) or 0),
        compress=bool(lg.get("compress", True)),
    )
    bk = raw.get("breaker", {}) or {}
    breaker = BreakerConfig(
        enabled=bool(bk.get("enabled", False)),
        failure_threshold=int(bk.get("failure_threshold", 0) or 0),
        open_duration=bk.get("open_duration", "") or "",
        half_open_max=int(bk.get("half_open_max", 0) or 0),
    )
    hc = raw.get("health_check", {}) or {}
    health = HealthCheckConfig(
        enabled=bool(hc.get("enabled", False)),
        interval=hc.get("interval", "") or "",
        timeout=hc.get("timeout", "") or "",
    )
    pr = raw.get("protection", {}) or {}
    protection = ProtectionConfig(
        enabled=bool(pr.get("enabled", False)),
        max_concurrent=int(pr.get("max_concurrent", 0) or 0),
        max_queue_size=int(pr.get("max_queue_size", 0) or 0),
        queue_timeout=pr.get("queue_timeout", "") or "",
        max_memory_mb=int(pr.get("max_memory_mb", 0) or 0),
        memory_warn=float(pr.get("memory_warn", 0.0) or 0.0),
        memory_critical=float(pr.get("memory_critical", 0.0) or 0.0),
        memory_check_interval=pr.get("memory_check_interval", "") or "",
    )

    cfg = Config(server=server, log=log, breaker=breaker, health_check=health,
                 protection=protection, chains={})

    # 默认值填充
    if not cfg.server.addr:
        cfg.server.addr = ":8080"
    if cfg.server.max_model_switches <= 0:
        cfg.server.max_model_switches = 5
    if not cfg.log.dir:
        cfg.log.dir = "logs"
    if cfg.log.body_limit <= 0:
        cfg.log.body_limit = 8 * 1024
    if cfg.log.max_size_mb <= 0:
        cfg.log.max_size_mb = 100
    if cfg.log.max_age_days <= 0:
        cfg.log.max_age_days = 7
    if cfg.log.max_backups <= 0:
        cfg.log.max_backups = 10
    if cfg.breaker.enabled:
        if cfg.breaker.failure_threshold <= 0:
            cfg.breaker.failure_threshold = 5
        if not cfg.breaker.open_duration:
            cfg.breaker.open_duration = "60s"
        parse_duration(cfg.breaker.open_duration)
        if cfg.breaker.half_open_max <= 0:
            cfg.breaker.half_open_max = 1
    if cfg.health_check.enabled:
        if not cfg.health_check.interval:
            cfg.health_check.interval = "30s"
        parse_duration(cfg.health_check.interval)
        if not cfg.health_check.timeout:
            cfg.health_check.timeout = "10s"
        parse_duration(cfg.health_check.timeout)
    # 服务保护默认值与校验（总开关关闭时整体禁用）
    if cfg.protection.enabled and cfg.protection.max_concurrent > 0:
        if cfg.protection.max_queue_size <= 0:
            cfg.protection.max_queue_size = cfg.protection.max_concurrent * 2
        if not cfg.protection.queue_timeout:
            cfg.protection.queue_timeout = "30s"
        parse_duration(cfg.protection.queue_timeout)
    if cfg.protection.enabled and cfg.protection.max_memory_mb > 0:
        if cfg.protection.memory_warn <= 0 or cfg.protection.memory_warn > 1:
            cfg.protection.memory_warn = 0.8
        if cfg.protection.memory_critical <= 0 or cfg.protection.memory_critical > 1:
            cfg.protection.memory_critical = 0.9
        if not cfg.protection.memory_check_interval:
            cfg.protection.memory_check_interval = "10s"
        parse_duration(cfg.protection.memory_check_interval)

    # 展开 ${ENV} 并去空
    keys: List[str] = []
    for k in cfg.server.api_keys:
        k = expand_env(str(k)).strip()
        if k:
            keys.append(k)
    cfg.server.api_keys = keys

    raw_chains = raw.get("chains", {}) or {}
    if not raw_chains:
        raise ValueError("no chains configured")

    for name, cval in raw_chains.items():
        # 兼容两种写法：直接列表 或 {models: [...]}
        if isinstance(cval, dict):
            model_list = cval.get("models", []) or []
        elif isinstance(cval, list):
            model_list = cval
        else:
            model_list = []
        models = [_model_from_dict(d) for d in model_list]
        if not models:
            raise ValueError(f"chain {name!r} has no models")
        models.sort(key=lambda m: m.priority)
        for m in models:
            if not m.name:
                m.name = f"{name}-{m.priority}"
            if not m.upstream.base_url or not m.upstream.model or not m.upstream.api_key:
                raise ValueError(
                    f"chain {name!r} model {m.name!r}: base_url/model/api_key 不可为空"
                )
            m.upstream.api_key = expand_env(m.upstream.api_key)
            if not m.upstream.auth_header:
                m.upstream.auth_header = "Authorization"
            if not m.upstream.timeout:
                m.upstream.timeout = "120s"
            parse_duration(m.upstream.timeout)
        cfg.chains[name] = ChainConfig(models=models)

    return cfg
