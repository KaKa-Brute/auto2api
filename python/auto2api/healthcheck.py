"""后台主动健康检查：周期性探活每个上游模型。

探活不经过 Forwarder（避免污染业务指标与调用日志），用独立 httpx.AsyncClient
发一条最小 user 消息 + max_tokens=1 的请求，只看状态码判断端点是否可达。
"""
import asyncio
import json
import logging
import threading
import time
from typing import Dict

import httpx

from . import forwarder as fwdmod
from .scheduler import HEALTH_HEALTHY, HEALTH_UNHEALTHY, HEALTH_UNKNOWN

_log = logging.getLogger("auto2api.healthcheck")


class HealthChecker:
    """后台周期性探活器。interval<=0 表示禁用（构造返回 None）。"""

    def __init__(self, scheduler, interval: float, timeout: float) -> None:
        self._scheduler = scheduler
        self._interval = interval
        self._timeout = timeout if timeout > 0 else 10.0
        self._statuses: Dict[str, int] = {}
        self._lock = threading.RLock()
        self._client = None
        self._task = None
        self._stop = False

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=None)
        return self._client

    @staticmethod
    def create(scheduler, interval: float, timeout: float):
        if interval <= 0:
            return None
        return HealthChecker(scheduler, interval, timeout)

    def _probe_body(self, m) -> bytes:
        """轻量探活请求体：一条最小 user 消息 + max_tokens=1。

        不能发空 messages——部分上游网关（如 new-api）对空 messages 返回 500
        参数校验错误，会被误判为服务故障并刷屏日志。带一条真实消息 + max_tokens=1
        既能真正探到"模型可生成"，又几乎不消耗 token。
        用该模型配置的真实上游模型名，避免模型名校验回 404/503 假阴性。
        """
        return json.dumps({
            "model": m.cfg.upstream.model,
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 1,
        }).encode("utf-8")

    def _probe_headers(self, m) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "auto2api-healthcheck",
        }
        fwdmod.set_auth_headers(headers, m.cfg.upstream.auth_header, m.cfg.upstream.api_key)
        return headers

    def start(self) -> None:
        self._task = asyncio.ensure_future(self._loop())

    async def stop(self) -> None:
        self._stop = True
        if self._task is not None:
            self._task.cancel()
        if self._client is not None:
            await self._client.aclose()

    async def _loop(self) -> None:
        await self._probe_all()  # 启动时立即探一轮
        while not self._stop:
            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                return
            if self._stop:
                return
            await self._probe_all()

    async def _probe_all(self) -> None:
        models = self._scheduler.list_all_models()
        await asyncio.gather(*(self._probe(m) for m in models), return_exceptions=True)

    async def _probe(self, m) -> None:
        url = fwdmod.build_url(m.cfg.upstream.base_url)
        timeout = httpx.Timeout(m.timeout, connect=m.timeout, read=m.timeout, write=m.timeout)
        start = time.monotonic()
        try:
            resp = await self._get_client().post(url, content=self._probe_body(m),
                                                  headers=self._probe_headers(m),
                                                  timeout=timeout)
        except Exception as e:  # noqa: BLE001 DNS/连接/超时 → 端点不可达
            _log.info("[healthcheck] %s: probe failed (status=0, dur=%.2fs): %s",
                      m.cooldown_key(), time.monotonic() - start, e)
            self._update(m, 0, False)
            return
        await resp.aclose()
        # 收到 HTTP 响应即认为端点可达（4xx/5xx 都说明 HTTP 服务活着），429 除外
        healthy = resp.status_code != 429
        if not healthy:
            _log.info("[healthcheck] %s: probe unhealthy (status=%d, dur=%.2fs)",
                      m.cooldown_key(), resp.status_code, time.monotonic() - start)
        self._update(m, resp.status_code, healthy)

    def _update(self, m, status: int, healthy: bool) -> None:
        key = m.cooldown_key()
        if status == 429:  # 限流不代表模型坏了
            return
        with self._lock:
            self._statuses[key] = HEALTH_HEALTHY if healthy else HEALTH_UNHEALTHY
        # 探针成功反馈熔断器帮助恢复；探针失败不驱动熔断器状态转换
        if healthy:
            b = self._scheduler.breaker_of(key)
            if b is not None:
                b.on_success()

    def status(self, m) -> int:
        with self._lock:
            return self._statuses.get(m.cooldown_key(), HEALTH_UNKNOWN)
