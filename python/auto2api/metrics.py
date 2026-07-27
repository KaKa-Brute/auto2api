"""运行时指标：单模型的 EMA 延迟与成功率。对齐 Go 版 internal/gateway/metrics.go。

用于动态路由：同优先级下优先选延迟低、成功率高的模型。EMA 内存恒定、对短期波动敏感。
"""
import threading

_DEFAULT_ALPHA = 0.3  # EMA 平滑系数：新样本权重 0.3


class Metrics:
    """单模型运行时指标，线程安全。初始假设完全成功（乐观）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latency_ema = 0.0   # 延迟 EMA（毫秒）
        self._success_ema = 1.0   # 成功率 EMA [0,1]，初始 1.0
        self._total_req = 0
        self._total_fail = 0

    def record(self, latency_ms: float, success: bool) -> None:
        """记录一次调用结果。latency_ms 为本次总耗时（毫秒）。"""
        with self._lock:
            if self._total_req == 0:
                self._latency_ema = latency_ms
                self._success_ema = 1.0 if success else 0.0
            else:
                a = _DEFAULT_ALPHA
                self._latency_ema = a * latency_ms + (1 - a) * self._latency_ema
                s = 1.0 if success else 0.0
                self._success_ema = a * s + (1 - a) * self._success_ema
            self._total_req += 1
            if not success:
                self._total_fail += 1

    def latency_ema(self) -> float:
        with self._lock:
            return self._latency_ema

    def success_rate(self) -> float:
        with self._lock:
            return self._success_ema

    def total(self):
        """返回 (总请求数, 总失败数)。"""
        with self._lock:
            return self._total_req, self._total_fail
