"""并发限流器：防止上游 API 超时导致连接堆积。

用 asyncio.Semaphore 作令牌桶，waiting 计数记录排队数，超过 max_queue_size
直接拒绝，等待超时快速失败，避免请求无限堆积撑爆内存/句柄。
"""
import asyncio


class RateLimitError(Exception):
    """限流拒绝：队列满或等待超时。"""


class ConcurrencyLimiter:
    """限制全局并发请求数。max_concurrent<=0 表示禁用。"""

    def __init__(self, max_concurrent: int, max_queue_size: int,
                 queue_timeout: float) -> None:
        self._max_concurrent = max_concurrent if max_concurrent and max_concurrent > 0 else 0
        if self._max_concurrent == 0:
            return
        if max_queue_size <= 0:
            max_queue_size = max_concurrent * 2
        if queue_timeout <= 0:
            queue_timeout = 30.0
        self._max_queue_size = max_queue_size
        self._queue_timeout = queue_timeout
        self._sem = asyncio.Semaphore(max_concurrent)
        self._waiting = 0       # 当前排队等待数
        self._inflight = 0      # 当前占用槽位数（供监控）
        self._total_rejected = 0
        self._total_timeout = 0

    async def acquire(self) -> None:
        """获取一个并发槽位。成功后须调用 release()。
        队列满抛 RateLimitError，等待超时抛 RateLimitError。"""
        if self._max_concurrent == 0:
            return
        # 有空闲槽位：立即获取（不占用队列）。locked() 为 True 表示已满。
        # asyncio 单线程模型下 value>0 时 acquire() 不会挂起，无竞态。
        if not self._sem.locked():
            await self._sem.acquire()
            self._inflight += 1
            return
        # 无空闲：登记排队，超过队列上限直接拒绝
        if self._waiting >= self._max_queue_size:
            self._total_rejected += 1
            raise RateLimitError(
                f"rate_limit_exceeded: queue full ({self._max_queue_size})")
        self._waiting += 1
        try:
            await asyncio.wait_for(self._sem.acquire(), timeout=self._queue_timeout)
        except asyncio.TimeoutError:
            self._total_timeout += 1
            raise RateLimitError(
                f"rate_limit_timeout: waited {self._queue_timeout}s")
        finally:
            self._waiting -= 1
        self._inflight += 1

    def release(self) -> None:
        """释放一个并发槽位。"""
        if self._max_concurrent == 0:
            return
        self._sem.release()
        if self._inflight > 0:
            self._inflight -= 1

    def stats(self):
        """返回 (当前并发, 累计拒绝数, 累计超时数)。"""
        if self._max_concurrent == 0:
            return 0, 0, 0
        return self._inflight, self._total_rejected, self._total_timeout


def create_limiter(max_concurrent: int, max_queue_size: int,
                   queue_timeout: float):
    """构造并发限流器。max_concurrent<=0 返回 None（禁用）。"""
    if not max_concurrent or max_concurrent <= 0:
        return None
    return ConcurrencyLimiter(max_concurrent, max_queue_size, queue_timeout)
