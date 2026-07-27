"""熔断器：CLOSED → OPEN → HALF_OPEN → CLOSED 三态机。对齐 Go 版 breaker.go。

与 failover.cooldown 的关系：cooldown 是单次失败后短期冷却；
熔断器是连续失败累积后长期熔断 + 半开探测恢复，二者叠加。
"""
import threading
import time
from dataclasses import dataclass

BREAKER_CLOSED = 0    # 正常放行
BREAKER_OPEN = 1      # 熔断打开，拒绝请求；冷却到期转 half_open
BREAKER_HALF_OPEN = 2  # 半开，只允许有限探测请求


@dataclass
class BreakerConfig:
    enabled: bool = False
    failure_threshold: int = 0   # 连续失败多少次进入 OPEN
    open_duration: float = 0.0   # OPEN 持续时间（秒），到期转 HALF_OPEN
    half_open_max: int = 0       # HALF_OPEN 允许的并发探测请求数


class CircuitBreaker:
    """单模型熔断器，线程安全。未设置字段填默认值。"""

    def __init__(self, cfg: BreakerConfig) -> None:
        if cfg.failure_threshold <= 0:
            cfg.failure_threshold = 5
        if cfg.open_duration <= 0:
            cfg.open_duration = 60.0
        if cfg.half_open_max <= 0:
            cfg.half_open_max = 1
        self._lock = threading.Lock()
        self._cfg = cfg
        self._state = BREAKER_CLOSED
        self._consecutive_fails = 0
        self._opened_at = 0.0
        self._half_open_inflight = 0

    def allow(self):
        """判断是否放行，返回 (allowed, is_probe)。

        CLOSED 放行非探测；OPEN 冷却到期转 HALF_OPEN 占探测名额放行，否则拒绝；
        HALF_OPEN 有探测名额则放行，否则拒绝。
        """
        if not self._cfg.enabled:
            return True, False
        with self._lock:
            if self._state == BREAKER_CLOSED:
                return True, False
            if self._state == BREAKER_OPEN:
                if time.monotonic() - self._opened_at >= self._cfg.open_duration:
                    self._state = BREAKER_HALF_OPEN
                    self._half_open_inflight = 1
                    return True, True
                return False, False
            if self._state == BREAKER_HALF_OPEN:
                if self._half_open_inflight < self._cfg.half_open_max:
                    self._half_open_inflight += 1
                    return True, True
                return False, False
            return True, False

    def on_success(self) -> None:
        """成功：CLOSED 清零连续失败；HALF_OPEN 探测成功 → CLOSED。"""
        if not self._cfg.enabled:
            return
        with self._lock:
            self._consecutive_fails = 0
            if self._state == BREAKER_HALF_OPEN:
                self._state = BREAKER_CLOSED
                self._half_open_inflight = 0

    def on_failure(self) -> None:
        """失败：CLOSED 累计达阈值转 OPEN；HALF_OPEN 探测失败 → 重新 OPEN。"""
        if not self._cfg.enabled:
            return
        with self._lock:
            self._consecutive_fails += 1
            if self._state == BREAKER_CLOSED:
                if self._consecutive_fails >= self._cfg.failure_threshold:
                    self._state = BREAKER_OPEN
                    self._opened_at = time.monotonic()
            elif self._state == BREAKER_HALF_OPEN:
                self._state = BREAKER_OPEN
                self._opened_at = time.monotonic()
                self._half_open_inflight = 0

    def state(self) -> int:
        """当前状态（只读快照）。OPEN 到期但未被 allow 触发前仍报 OPEN。"""
        with self._lock:
            return self._state

    def state_name(self) -> str:
        s = self.state()
        return {BREAKER_CLOSED: "closed", BREAKER_OPEN: "open",
                BREAKER_HALF_OPEN: "half_open"}.get(s, "unknown")

    def consecutive_fails(self) -> int:
        with self._lock:
            return self._consecutive_fails
