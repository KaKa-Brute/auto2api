"""优先级自动切换网关核心。对齐 Go 版 internal/gateway/scheduler.go。

多套命名链、按优先级选择模型、内存冷却、熔断器叠加、动态路由。
"""
import threading
import time
from typing import Dict, List, Optional

from . import config as cfgmod
from .breaker import BreakerConfig, CircuitBreaker, BREAKER_OPEN
from .metrics import Metrics

# 健康状态（与 healthcheck 对齐）
HEALTH_UNKNOWN = 0
HEALTH_HEALTHY = 1
HEALTH_UNHEALTHY = 2

# 默认重试状态码：即使配置遗漏也能对常见上游故障做退避重试
_DEFAULT_RETRYABLE = {429, 500, 502, 503, 529}

# 默认故障转移状态码：即使配置遗漏也能对常见上游故障自动切换下一优先级
_DEFAULT_FAILOVER = {401, 403, 429, 500, 502, 503, 529}


def health_status_name(s: int) -> str:
    return {HEALTH_HEALTHY: "healthy", HEALTH_UNHEALTHY: "unhealthy"}.get(s, "unknown")


class Model:
    """一条链里某个优先级模型的运行期表示，所有时长已解析为秒。"""

    def __init__(self, mc: cfgmod.ModelConfig, chain_name: str) -> None:
        self.cfg = mc
        self.chain_name = chain_name
        self.retry_count = max(0, mc.retry.count)
        try:
            self.backoffs = cfgmod.parse_durations(mc.retry.backoff)
        except ValueError:
            self.backoffs = []
        self.retryable = set(mc.retry.retryable_status) | _DEFAULT_RETRYABLE
        self.failover = set(mc.failover.trigger_status) | _DEFAULT_FAILOVER
        self.cooldown = cfgmod.parse_duration(mc.failover.cooldown or "60s")
        self.idle_timeout = cfgmod.parse_duration(mc.stream.idle_timeout or "30s")
        self.keepalive = cfgmod.parse_duration(mc.stream.keepalive or "5s")
        self.timeout = cfgmod.parse_duration(mc.upstream.timeout or "120s")

    def cooldown_key(self) -> str:
        """冷却表/熔断表/指标表中的唯一键，按 chain::name 隔离。"""
        return f"{self.chain_name}::{self.cfg.name}"


class Chain:
    def __init__(self, name: str, models: List[Model]) -> None:
        self.name = name
        self.models = models


class Scheduler:
    """管理所有链与内存冷却表、熔断器、运行时指标。线程安全。"""

    def __init__(self, cfg: cfgmod.Config) -> None:
        self._lock = threading.RLock()
        self._chains: Dict[str, Chain] = {}
        self._cooldown: Dict[str, float] = {}      # key -> 冷却到期时间（monotonic）
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._metrics: Dict[str, Metrics] = {}
        self._health = None                        # HealthChecker，可能为 None
        self._breaker_cfg = self._build_breaker_cfg(cfg.breaker)

        for name, cc in cfg.chains.items():
            models = [Model(mc, name) for mc in cc.models]
            models.sort(key=lambda m: m.cfg.priority)
            self._chains[name] = Chain(name, models)
        for ch in self._chains.values():
            for m in ch.models:
                self._breakers[m.cooldown_key()] = CircuitBreaker(
                    self._clone_breaker_cfg())
                self._metrics[m.cooldown_key()] = Metrics()

    @staticmethod
    def _build_breaker_cfg(c: cfgmod.BreakerConfig) -> BreakerConfig:
        bc = BreakerConfig(enabled=c.enabled)
        if c.failure_threshold > 0:
            bc.failure_threshold = c.failure_threshold
        if c.open_duration:
            try:
                d = cfgmod.parse_duration(c.open_duration)
                if d > 0:
                    bc.open_duration = d
            except ValueError:
                pass
        if c.half_open_max > 0:
            bc.half_open_max = c.half_open_max
        return bc

    def _clone_breaker_cfg(self) -> BreakerConfig:
        b = self._breaker_cfg
        return BreakerConfig(
            enabled=b.enabled,
            failure_threshold=b.failure_threshold,
            open_duration=b.open_duration,
            half_open_max=b.half_open_max,
        )

    def set_health_checker(self, h) -> None:
        with self._lock:
            self._health = h

    def get_chain(self, name: str) -> Optional[Chain]:
        with self._lock:
            return self._chains.get(name)

    def list_chains(self) -> List[str]:
        with self._lock:
            return sorted(self._chains.keys())

    def list_all_models(self) -> List[Model]:
        with self._lock:
            out: List[Model] = []
            for ch in self._chains.values():
                out.extend(ch.models)
            return out

    def _breaker_of(self, key: str) -> Optional[CircuitBreaker]:
        with self._lock:
            return self._breakers.get(key)

    def breaker_of(self, key: str) -> Optional[CircuitBreaker]:
        """供 healthcheck 访问熔断器。"""
        return self._breaker_of(key)

    def _metrics_of(self, m: Model) -> Optional[Metrics]:
        with self._lock:
            return self._metrics.get(m.cooldown_key())

    def is_cooling_down(self, m: Model) -> bool:
        with self._lock:
            exp = self._cooldown.get(m.cooldown_key())
            return exp is not None and time.monotonic() < exp

    def mark_cooldown(self, m: Model) -> None:
        with self._lock:
            self._cooldown[m.cooldown_key()] = time.monotonic() + m.cooldown

    def is_open(self, m: Model) -> bool:
        b = self._breaker_of(m.cooldown_key())
        return b is not None and b.state() == BREAKER_OPEN

    def allow_request(self, m: Model):
        """综合熔断器与冷却表判断是否放行，返回 (allowed, is_probe)。"""
        if self.is_cooling_down(m):
            return False, False
        b = self._breaker_of(m.cooldown_key())
        if b is None:
            return True, False
        return b.allow()

    def record_result(self, m: Model, latency_ms: float, success: bool) -> None:
        """把一次模型调用结果反馈给熔断器与指标器。"""
        mt = self._metrics_of(m)
        if mt is not None:
            mt.record(latency_ms, success)
        b = self._breaker_of(m.cooldown_key())
        if b is None:
            return
        if success:
            b.on_success()
        else:
            b.on_failure()

    def stats(self, m: Model):
        """返回 (延迟EMA毫秒, 成功率EMA, 总请求数, 总失败数)。"""
        mt = self._metrics_of(m)
        if mt is None:
            return 0.0, 1.0, 0, 0
        total, fail = mt.total()
        return mt.latency_ema(), mt.success_rate(), total, fail

    def breaker_state_name(self, m: Model) -> str:
        b = self._breaker_of(m.cooldown_key())
        return b.state_name() if b is not None else "closed"

    def consecutive_fails(self, m: Model) -> int:
        b = self._breaker_of(m.cooldown_key())
        return b.consecutive_fails() if b is not None else 0

    def health_status(self, m: Model) -> int:
        with self._lock:
            h = self._health
        if h is None:
            return HEALTH_UNKNOWN
        return h.status(m)

    def pick_next(self, chain: Chain, excluded: Dict[str, bool]) -> Optional[Model]:
        """按优先级升序返回下一个可用模型，跳过 excluded/冷却中/熔断 OPEN。"""
        with self._lock:
            now = time.monotonic()
            for m in chain.models:
                if excluded.get(m.cfg.name):
                    continue
                exp = self._cooldown.get(m.cooldown_key())
                if exp is not None and now < exp:
                    continue
                # 仅跳过冷却未到期的 OPEN 熔断器；到期的 OPEN 保留为候选，
                # 以便后续 allow_request 触发 OPEN→HALF_OPEN 探测恢复。
                b = self._breakers.get(m.cooldown_key())
                if b is not None and b.should_skip_routing():
                    continue
                return m
            return None

    def select_model(self, chain: Chain, excluded: Dict[str, bool]) -> Optional[Model]:
        """动态路由：可用集合内按 优先级 > 健康 > 成功率 > 延迟 综合排序选最优。"""
        with self._lock:
            now = time.monotonic()
            cands = []
            for m in chain.models:
                if excluded.get(m.cfg.name):
                    continue
                exp = self._cooldown.get(m.cooldown_key())
                if exp is not None and now < exp:
                    continue
                # 仅跳过冷却未到期的 OPEN 熔断器；到期的 OPEN 保留为候选，
                # 以便后续 allow_request 触发 OPEN→HALF_OPEN 探测恢复。
                b = self._breakers.get(m.cooldown_key())
                if b is not None and b.should_skip_routing():
                    continue
                latency, success, healthy = 0.0, 1.0, 0
                mt = self._metrics.get(m.cooldown_key())
                if mt is not None:
                    latency = mt.latency_ema()
                    success = mt.success_rate()
                if self._health is not None:
                    hs = self._health.status(m)
                    if hs == HEALTH_HEALTHY:
                        healthy = 1
                    elif hs == HEALTH_UNHEALTHY:
                        healthy = -1
                cands.append((m, latency, success, healthy))
            if not cands:
                return None
            # 排序键：优先级升序 → 健康降序 → 成功率降序 → 延迟升序（稳定排序保配置顺序）
            cands.sort(key=lambda c: (
                c[0].cfg.priority, -c[3], -c[2], c[1],
            ))
            return cands[0][0]
