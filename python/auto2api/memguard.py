"""内存守护：监控进程内存并自动降级防止 OOM。对齐 Go 版 internal/gateway/memguard.py。

后台 asyncio 任务周期检测进程 RSS，超警告阈值主动 gc.collect()，
超临界阈值进入降级模式（拒新请求 + 强制 GC），内存回落后自动恢复。

读取进程内存优先用 psutil（跨平台）；未安装 psutil 时回退到 resource（Unix），
Windows 无 psutil 则无法监控，构造返回 None 并告警。
"""
import asyncio
import gc
import logging
import time

_log = logging.getLogger("auto2api.memguard")

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except ImportError:  # pragma: no cover
    psutil = None
    _HAS_PSUTIL = False


def _read_rss_mb() -> int:
    """读取当前进程 RSS（MB）。psutil 优先，否则尝试 resource（Unix）。"""
    if _HAS_PSUTIL:
        return int(psutil.Process().memory_info().rss / 1024 / 1024)
    try:
        import resource  # Unix only
        # ru_maxrss：Linux 单位 KB，macOS 单位 Byte
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        import sys
        if sys.platform == "darwin":
            return int(maxrss / 1024 / 1024)
        return int(maxrss / 1024)
    except Exception:  # noqa: BLE001  # pragma: no cover
        return 0


class MemoryGuard:
    """周期性检测内存使用率，超临界阈值触发自动降级。"""

    def __init__(self, max_memory_mb: int, warn_percent: float,
                 critical_percent: float, check_interval: float) -> None:
        self._max_mb = max_memory_mb
        self._warn = warn_percent if 0 < warn_percent <= 1 else 0.8
        self._critical = critical_percent if 0 < critical_percent <= 1 else 0.9
        self._interval = check_interval if check_interval > 0 else 10.0
        self._degraded = False
        self._last_mb = 0
        self._last_gc = 0.0
        self._total_rejected = 0
        self._task = None
        self._stop = False

    def start(self) -> None:
        """启动后台监控任务（须在事件循环内调用）。"""
        self._task = asyncio.ensure_future(self._loop())
        _log.info("[memguard] started: max=%dMB, warn=%.0f%%, critical=%.0f%%, interval=%ss",
                  self._max_mb, self._warn * 100, self._critical * 100, self._interval)

    async def stop(self) -> None:
        """停止后台监控任务。"""
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None

    async def _loop(self) -> None:
        try:
            while not self._stop:
                await asyncio.sleep(self._interval)
                self._check()
        except asyncio.CancelledError:
            pass

    def _check(self) -> None:
        used_mb = _read_rss_mb()
        self._last_mb = used_mb
        usage = used_mb / self._max_mb if self._max_mb > 0 else 0.0

        # 超临界阈值：降级并强制 GC
        if usage >= self._critical:
            if not self._degraded:
                self._degraded = True
                _log.warning("[memguard] CRITICAL: memory=%dMB/%.1f%%, entering degraded mode",
                             used_mb, usage * 100)
            now = time.monotonic()
            if now - self._last_gc > 5:
                gc.collect()
                self._last_gc = now
                _log.warning("[memguard] forced GC triggered")
            return

        # 超警告阈值但未到临界：主动 GC + 告警
        if usage >= self._warn:
            _log.warning("[memguard] WARNING: memory=%dMB/%.1f%%, approaching limit",
                         used_mb, usage * 100)
            now = time.monotonic()
            if now - self._last_gc > 10:
                gc.collect()
                self._last_gc = now
            return

        # 低于警告阈值：恢复正常
        if self._degraded:
            self._degraded = False
            _log.info("[memguard] RECOVERED: memory=%dMB/%.1f%%, back to normal",
                      used_mb, usage * 100)

    def allow_request(self):
        """检查是否允许请求。降级模式下拒绝。返回 (allowed, reason)。"""
        if self._degraded:
            self._total_rejected += 1
            return False, f"memory_overload: {self._last_mb}MB/{self._max_mb}MB"
        return True, ""

    def stats(self):
        """返回 (当前内存MB, 是否降级, 累计拒绝数)。"""
        return self._last_mb, self._degraded, self._total_rejected


def create_memguard(max_memory_mb: int, warn_percent: float,
                    critical_percent: float, check_interval: float):
    """构造内存守护。max_memory_mb<=0 返回 None（禁用）。"""
    if not max_memory_mb or max_memory_mb <= 0:
        return None
    if not _HAS_PSUTIL and _read_rss_mb() == 0:
        _log.warning("[memguard] 无法读取进程内存（未安装 psutil 且非 Unix），内存守护禁用。"
                     "请 pip install psutil 以启用。")
        return None
    return MemoryGuard(max_memory_mb, warn_percent, critical_percent, check_interval)
