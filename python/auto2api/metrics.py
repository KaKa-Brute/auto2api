"""运行时指标：单模型的 EMA 延迟与成功率。对齐 Go 版 internal/gateway/metrics.go。

用于动态路由：同优先级下优先选延迟低、成功率高的模型。EMA 内存恒定、对短期波动敏感。
Token 统计按日期分片，每天 UTC 0 点自动切换到新分片。
"""
import threading
from datetime import datetime, timezone
from typing import Dict, Tuple

_DEFAULT_ALPHA = 0.3  # EMA 平滑系数：新样本权重 0.3


class TokenShard:
    """某一天的 token 累计。"""
    def __init__(self) -> None:
        self.prompt_tok = 0
        self.complet_tok = 0


class Metrics:
    """单模型运行时指标，线程安全。初始假设完全成功（乐观）。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._latency_ema = 0.0   # 延迟 EMA（毫秒）
        self._success_ema = 1.0   # 成功率 EMA [0,1]，初始 1.0
        self._total_req = 0
        self._total_fail = 0
        # Token 按日期分片存储：key 为 "YYYY-MM-DD" 格式的日期字符串
        self._tokens_by_date: Dict[str, TokenShard] = {}

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

    def total(self) -> Tuple[int, int]:
        """返回 (总请求数, 总失败数)。"""
        with self._lock:
            return self._total_req, self._total_fail

    def record_tokens(self, prompt: int, completion: int) -> None:
        """累加一次调用的输入/输出 token 数到今日分片（0 值忽略）。"""
        if prompt <= 0 and completion <= 0:
            return
        with self._lock:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            shard = self._tokens_by_date.get(today)
            if shard is None:
                shard = TokenShard()
                self._tokens_by_date[today] = shard
            if prompt > 0:
                shard.prompt_tok += prompt
            if completion > 0:
                shard.complet_tok += completion

    def tokens(self) -> Tuple[int, int, int]:
        """返回今日累计 (输入 token, 输出 token, 总 token)。"""
        with self._lock:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            shard = self._tokens_by_date.get(today)
            if shard is None:
                return 0, 0, 0
            return shard.prompt_tok, shard.complet_tok, shard.prompt_tok + shard.complet_tok

    def tokens_for_date(self, date: str) -> Tuple[int, int, int]:
        """返回指定日期的 token 统计（日期格式 "YYYY-MM-DD"）。"""
        with self._lock:
            shard = self._tokens_by_date.get(date)
            if shard is None:
                return 0, 0, 0
            return shard.prompt_tok, shard.complet_tok, shard.prompt_tok + shard.complet_tok

    def cleanup_old_tokens(self, keep_days: int) -> None:
        """清理指定天数之前的 token 分片（减少内存占用）。"""
        if keep_days <= 0:
            return
        with self._lock:
            from datetime import timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d")
            to_delete = [date for date in self._tokens_by_date if date < cutoff]
            for date in to_delete:
                del self._tokens_by_date[date]
