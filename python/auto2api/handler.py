"""HTTP 路由与两级 fallback 编排。对齐 Go 版 internal/gateway/handler.go。

层 1：同模型退避重试（retryable_status）
层 2：跨优先级模型故障转移（trigger_status）
"""
import asyncio
import json
import logging
from typing import Optional, Tuple

from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from . import claude as claudemod
from .call_logger import AttemptLog
from .forwarder import UpstreamError

_log = logging.getLogger("auto2api.handler")

# 单模型尝试（含重试）的结局
OUTCOME_SUCCESS = 0       # 响应已成功写完
OUTCOME_FAILOVER = 1      # 应切换到下一优先级模型
OUTCOME_CLIENT_ERROR = 2  # 上游非重试/非转移状态，已原样回给客户端


def extract_bearer_key(auth_header: str) -> str:
    """从 'Bearer <key>' 提取 key，允许裸 key。"""
    prefix = "Bearer "
    if len(auth_header) <= len(prefix):
        return ""
    if auth_header[:len(prefix)] != prefix:
        return auth_header
    return auth_header[len(prefix):]


def _extract_string(body: bytes, key: str) -> str:
    try:
        p = json.loads(body)
    except ValueError:
        return ""
    if isinstance(p, dict) and isinstance(p.get(key), str):
        return p[key]
    return ""


def _extract_bool(body: bytes, key: str) -> bool:
    try:
        p = json.loads(body)
    except ValueError:
        return False
    if isinstance(p, dict) and isinstance(p.get(key), bool):
        return p[key]
    return False


class _CommittedResponse(Exception):
    """内部信号：forwarder 已提交（成功）响应，携带 Starlette 响应对象。"""

    def __init__(self, response: Response) -> None:
        super().__init__("committed")
        self.response = response


def json_error(status: int, etype: str, msg: str, outbound_format: str) -> JSONResponse:
    """按出站格式构造错误响应。Claude 格式用 {"type":"error","error":{...}}。"""
    if outbound_format == "claude":
        return JSONResponse(status_code=status,
                            content={"type": "error",
                                     "error": {"type": etype, "message": msg}})
    return JSONResponse(status_code=status,
                        content={"error": {"message": msg, "type": etype}})


def upstream_error_response(e: UpstreamError, outbound_format: str) -> Response:
    """把上游错误原样回给客户端；错误体非法 JSON 时按出站格式包装。"""
    body = e.body
    valid = False
    if body:
        try:
            json.loads(body)
            valid = True
        except ValueError:
            valid = False
    if not body or not valid:
        if outbound_format == "claude":
            payload = {"type": "error",
                       "error": {"type": "upstream_error", "message": str(e)}}
        else:
            payload = {"error": {"message": str(e), "type": "upstream_error"}}
        return JSONResponse(status_code=e.status, content=payload)
    return Response(content=body, status_code=e.status, media_type="application/json")


class Handler:
    """所有 HTTP 路由的入口与 fallback 编排。"""

    def __init__(self, scheduler, forwarder, max_switches: int,
                 api_keys, logger) -> None:
        self._scheduler = scheduler
        self._forwarder = forwarder
        self._max_switches = max_switches
        self._logger = logger
        self._api_keys = set(k for k in (api_keys or []) if k)

    def _check_auth(self, request: Request, outbound_format: str):
        """校验客户端 key。未配置 api_keys 时放行。返回错误响应或 None。"""
        if not self._api_keys:
            return None
        key = extract_bearer_key(request.headers.get("Authorization", ""))
        if not key:
            key = request.headers.get("x-api-key", "")
        if not key:
            key = request.headers.get("x-goog-api-key", "")
        if not key or key not in self._api_keys:
            return json_error(401, "authentication_error",
                              "invalid or missing API key", outbound_format)
        return None

    async def chat_completions(self, request: Request) -> Response:
        """POST /v1/chat/completions（OpenAI 兼容）。"""
        outbound = ""
        err = self._check_auth(request, outbound)
        if err is not None:
            return err
        entry = self._begin(request, "openai")
        final_status = 200
        final_err: Optional[Exception] = None
        try:
            body = await request.body()
            if not _valid_json(body):
                final_status = 400
                final_err = ValueError("request body is not valid JSON")
                return json_error(400, "invalid_request_error", str(final_err), outbound)
            resp, final_status, final_err = await self._run_completion(
                request, body, entry, outbound)
            return resp
        finally:
            self._logger.end(entry, final_status, final_err)

    async def messages(self, request: Request) -> Response:
        """POST /v1/messages（Anthropic Claude Messages API 兼容）。"""
        outbound = "claude"
        err = self._check_auth(request, outbound)
        if err is not None:
            return err
        entry = self._begin(request, "claude")
        final_status = 200
        final_err: Optional[Exception] = None
        try:
            raw_body = await request.body()
            if not _valid_json(raw_body):
                final_status = 400
                final_err = ValueError("request body is not valid JSON")
                return json_error(400, "invalid_request_error", str(final_err), outbound)
            try:
                openai_body, _ = claudemod.claude_request_to_openai(raw_body)
            except Exception as ce:  # noqa: BLE001
                final_status = 400
                final_err = ce
                return json_error(400, "invalid_request_error",
                                  "convert claude request: " + str(ce), outbound)
            chain_name = _extract_string(openai_body, "model") or "auto"
            stream_requested = _extract_bool(openai_body, "stream")
            # 记录原始 Claude 请求体（转换前）
            self._logger.set_request(entry, raw_body, chain_name, stream_requested)
            resp, final_status, final_err = await self._run_completion(
                request, openai_body, entry, outbound, req_recorded=True)
            return resp
        finally:
            self._logger.end(entry, final_status, final_err)

    async def models(self, request: Request) -> Response:
        """GET /v1/models：列出所有链名。"""
        err = self._check_auth(request, "")
        if err is not None:
            return err
        names = self._scheduler.list_chains()
        data = [{"id": n, "object": "model", "owned_by": "auto2api"} for n in names]
        return JSONResponse({"object": "list", "data": data})

    async def health(self, request: Request) -> Response:
        """GET /v1/health：每条链各模型的实时健康（不鉴权）。"""
        from .scheduler import health_status_name
        out = {}
        for name in self._scheduler.list_chains():
            ch = self._scheduler.get_chain(name)
            models = []
            for m in ch.models:
                latency, success, total, fail = self._scheduler.stats(m)
                models.append({
                    "name": m.cfg.name,
                    "priority": m.cfg.priority,
                    "upstream_model": m.cfg.upstream.model,
                    "base_url": m.cfg.upstream.base_url,
                    "cooling_down": self._scheduler.is_cooling_down(m),
                    "breaker_state": self._scheduler.breaker_state_name(m),
                    "consecutive_fails": self._scheduler.consecutive_fails(m),
                    "health": health_status_name(self._scheduler.health_status(m)),
                    "latency_ema_ms": latency,
                    "success_rate": success,
                    "total_requests": total,
                    "total_failures": fail,
                })
            out[name] = models
        return JSONResponse(out)

    def _begin(self, request: Request, fmt: str):
        client_ip = request.client.host if request.client else ""
        return self._logger.begin(request.method, request.url.path,
                                  client_ip, request.headers, fmt)

    async def _run_completion(self, request: Request, body: bytes, entry,
                              outbound: str, req_recorded: bool = False):
        """按 model 字段选链并执行两级 fallback 编排。返回 (Response, status, err)。"""
        chain_name = _extract_string(body, "model") or "auto"
        chain = self._scheduler.get_chain(chain_name)
        if chain is None:
            resp = json_error(404, "invalid_request_error",
                              "unknown model/chain: " + chain_name, outbound)
            return resp, 404, ValueError("unknown chain: " + chain_name)
        stream_requested = _extract_bool(body, "stream")
        if not req_recorded:
            self._logger.set_request(entry, body, chain_name, stream_requested)

        excluded = {}
        last_err: Optional[Exception] = None
        switches = 0
        while True:
            m = self._scheduler.select_model(chain, excluded)
            if m is None:
                msg = "all models exhausted (circuit open or cooling down)"
                if last_err is not None:
                    msg = str(last_err)
                return (json_error(502, "upstream_unavailable", msg, outbound),
                        502, last_err)
            allowed, _ = self._scheduler.allow_request(m)
            if not allowed:
                excluded[m.cfg.name] = True
                continue

            outcome, err, resp = await self._try_model(
                request, body, m, stream_requested, entry, outbound)
            if err is not None:
                last_err = err
            if outcome in (OUTCOME_SUCCESS, OUTCOME_CLIENT_ERROR):
                status = resp.status_code if resp is not None else 200
                return resp, status, err
            # OUTCOME_FAILOVER：层 2 跨优先级故障转移
            self._scheduler.mark_cooldown(m)
            excluded[m.cfg.name] = True
            switches += 1
            if switches >= self._max_switches:
                msg = "max model switches reached"
                if last_err is not None:
                    msg = str(last_err)
                return (json_error(502, "upstream_unavailable", msg, outbound),
                        502, last_err)

    async def _try_model(self, request: Request, body: bytes, m,
                         stream_requested: bool, entry, outbound: str):
        """单模型层 1 退避重试。返回 (outcome, err, response)。"""
        ue: Optional[UpstreamError] = None
        req_headers = dict(request.headers)
        attempt = 0
        while attempt <= m.retry_count:
            out = await self._forwarder.forward(
                req_headers, body, m, stream_requested, outbound, entry)
            res = out.result
            if out.error is None:
                # 成功（响应已提交）
                self._scheduler.record_result(m, res.duration_ms, True)
                self._logger.add_attempt(entry, AttemptLog(
                    model=m.cfg.name, priority=m.cfg.priority,
                    upstream_model=res.upstream_model, status=res.status,
                    outcome="success", attempt=attempt,
                    duration_ms=res.duration_ms, first_token_ms=res.first_token_ms))
                return OUTCOME_SUCCESS, None, out.response

            err = out.error
            if isinstance(err, UpstreamError):
                ue = err
                status = err.status
                if status in m.retryable and attempt < m.retry_count:
                    self._scheduler.record_result(m, res.duration_ms, False)
                    self._logger.add_attempt(entry, AttemptLog(
                        model=m.cfg.name, priority=m.cfg.priority, status=status,
                        outcome="retry", attempt=attempt,
                        duration_ms=res.duration_ms, error=str(err)))
                    await self._sleep(m.backoffs, attempt)
                    attempt += 1
                    continue
                if status in m.failover:
                    self._scheduler.record_result(m, res.duration_ms, False)
                    self._logger.add_attempt(entry, AttemptLog(
                        model=m.cfg.name, priority=m.cfg.priority, status=status,
                        outcome="failover", attempt=attempt,
                        duration_ms=res.duration_ms, error=str(err)))
                    return OUTCOME_FAILOVER, err, None
                # 非重试/非转移（如 400/404）——不计熔断失败，原样返回
                self._scheduler.record_result(m, res.duration_ms, True)
                self._logger.add_attempt(entry, AttemptLog(
                    model=m.cfg.name, priority=m.cfg.priority, status=status,
                    outcome="client_error", attempt=attempt,
                    duration_ms=res.duration_ms, error=str(err)))
                return (OUTCOME_CLIENT_ERROR, err,
                        upstream_error_response(err, outbound))
            # 传输错误（DNS/连接/超时）——切下一优先级
            self._scheduler.record_result(m, res.duration_ms, False)
            self._logger.add_attempt(entry, AttemptLog(
                model=m.cfg.name, priority=m.cfg.priority, status=0,
                outcome="error", attempt=attempt,
                duration_ms=res.duration_ms, error=str(err)))
            return OUTCOME_FAILOVER, err, None

        # 重试耗尽：把最后一次错误回给客户端
        if ue is not None:
            self._scheduler.record_result(m, 0, False)
            self._logger.add_attempt(entry, AttemptLog(
                model=m.cfg.name, priority=m.cfg.priority, status=ue.status,
                outcome="client_error", attempt=m.retry_count, error=str(ue)))
            return (OUTCOME_CLIENT_ERROR, ue,
                    upstream_error_response(ue, outbound))
        return OUTCOME_FAILOVER, None, None

    async def _sleep(self, backoffs, attempt: int) -> None:
        """退避睡眠：指数退避封顶 3s。"""
        if attempt < len(backoffs):
            d = backoffs[attempt]
        elif backoffs:
            d = backoffs[-1] * 2
        else:
            d = 0.3
        if d > 3.0:
            d = 3.0
        await asyncio.sleep(d)


def _valid_json(body: bytes) -> bool:
    try:
        json.loads(body)
        return True
    except ValueError:
        return False
