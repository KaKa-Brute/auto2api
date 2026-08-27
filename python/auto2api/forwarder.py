"""转发器：手动构造上游请求，头部白名单透传 + 鉴权注入 + body 模型改写 + SSE 管道。

Python 异步流式在 handler 返回后才真正推流，因此用 httpx stream=True
先拿到状态码（非 2xx 可 fallback），2xx 才提交响应体（提交后不可再 fallback）。
"""
import asyncio
import json
import re
import time
from typing import Any, Dict, Optional, Tuple

import httpx

from . import claude as claudemod


class UpstreamError(Exception):
    """上游返回非 2xx（已读错误体，响应未提交，可 fallback）。"""

    def __init__(self, status: int, body: str) -> None:
        super().__init__(f"upstream {status}: {body}")
        self.status = status
        self.body = body


class ForwardResult:
    """一次转发的结果摘要。"""

    def __init__(self) -> None:
        self.status = 0
        self.body_committed = False
        self.stream = False
        self.upstream_model = ""
        self.first_token_ms = 0
        self.duration_ms = 0
        self.prompt_tokens = 0      # 本次调用上游返回的输入 token 数（无则 0）
        self.completion_tokens = 0  # 本次调用上游返回的输出 token 数（无则 0）


# 从客户端透传到上游的头部白名单（小写），其余一律丢弃（含 authorization/cookie）。
ALLOWED_HEADERS = {
    "accept", "accept-language", "accept-encoding", "user-agent",
    "openai-organization", "openai-project", "anthropic-version",
    "anthropic-beta", "x-request-id", "idempotency-key",
}


_VERSION_SUFFIX_RE = re.compile(r"/v\d+$")


def build_url(base_url: str, endpoint: str = "") -> str:
    """规整 base_url 并按 endpoint 追加路径。

    endpoint 为 "responses" 时追加 /v1/responses（Responses API），
    默认（空或 "chat"）追加 /v1/chat/completions。
    若 base_url 已以对应路径结尾则原样返回。
    """
    b = base_url.rstrip("/")
    ep = (endpoint or "").strip().lower()
    if ep == "responses":
        if b.endswith("/responses"):
            return b
        if _VERSION_SUFFIX_RE.search(b):
            return b + "/responses"
        return b + "/v1/responses"
    # 默认 chat/completions
    if b.endswith("/chat/completions"):
        return b
    if _VERSION_SUFFIX_RE.search(b):
        return b + "/chat/completions"
    return b + "/v1/chat/completions"


def set_auth_headers(headers: Dict[str, str], auth_header: str, api_key: str) -> None:
    """按配置注入鉴权头。"""
    h = (auth_header or "").strip().lower()
    if h in ("", "authorization", "bearer"):
        headers["Authorization"] = "Bearer " + api_key
    elif h == "x-api-key":
        headers["x-api-key"] = api_key
    elif h == "x-goog-api-key":
        headers["x-goog-api-key"] = api_key
    else:
        headers[auth_header] = api_key


def rewrite_model(body: bytes, new_model: str) -> bytes:
    """用上游真实模型名改写 body 的 model 字段，保留其余字段。"""
    obj = json.loads(body)
    if not isinstance(obj, dict):
        obj = {}
    obj["model"] = new_model
    return json.dumps(obj).encode("utf-8")


def ms(seconds: float) -> int:
    return int(seconds * 1000)


class ForwardOutcome:
    """forward 的返回：区分可 fallback 的错误路径与已提交的成功响应。"""

    def __init__(self) -> None:
        self.result = ForwardResult()
        self.error: Optional[Exception] = None   # UpstreamError / 传输错误 / None
        self.response = None                     # Starlette Response（committed 时）


class Forwarder:
    """持有共享 httpx.AsyncClient（流式安全：无总超时，由 per-request timeout 控制）。"""

    def __init__(self, logger=None) -> None:
        self._logger = logger
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop = None

    def _get_client(self) -> httpx.AsyncClient:
        """惰性创建 httpx client，并在事件循环变化时重建（连接池绑定 loop）。

        限制连接池上限，防止上游超时导致连接无限堆积撑爆内存/句柄。
        """
        loop = asyncio.get_event_loop()
        if self._client is None or self._client_loop is not loop:
            limits = httpx.Limits(
                max_connections=100,            # 全局最大连接数
                max_keepalive_connections=50,   # 最大保活连接数
                keepalive_expiry=90.0,          # 保活连接空闲回收（秒）
            )
            self._client = httpx.AsyncClient(timeout=None, follow_redirects=True,
                                             limits=limits)
            self._client_loop = loop
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _upstream_headers(self, req_headers: Dict[str, str], m) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        for k, v in req_headers.items():
            if k.lower() in ALLOWED_HEADERS:
                headers[k] = v
        headers["Content-Type"] = "application/json"
        set_auth_headers(headers, m.cfg.upstream.auth_header, m.cfg.upstream.api_key)
        return headers

    async def forward(self, req_headers, body: bytes, m, stream_requested: bool,
                      outbound_format: str, entry=None, endpoint: str = "") -> ForwardOutcome:
        """把请求体转发到指定上游模型。

        返回 ForwardOutcome：
          - 非 2xx：outcome.error 为 UpstreamError，未提交，可 fallback
          - 传输错误：outcome.error 为普通异常，未提交
          - 2xx：outcome.response 为 Starlette 响应（committed=True，不可再 fallback）
        """
        from starlette.responses import Response, StreamingResponse

        out = ForwardOutcome()
        res = out.result
        try:
            up_body = rewrite_model(body, m.cfg.upstream.model)
        except Exception as e:  # noqa: BLE001
            out.error = RuntimeError(f"rewrite model: {e}")
            return out
        if self._logger is not None and entry is not None:
            self._logger.set_upstream_request(entry, up_body)

        url = build_url(m.cfg.upstream.base_url, endpoint)
        headers = self._upstream_headers(req_headers, m)
        timeout = httpx.Timeout(m.timeout, connect=m.timeout, read=None, write=m.timeout)
        start = time.monotonic()

        try:
            client = self._get_client()
            req = client.build_request("POST", url, content=up_body,
                                       headers=headers, timeout=timeout)
            resp = await client.send(req, stream=True)
        except Exception as e:  # noqa: BLE001 传输错误（DNS/连接/超时）
            res.duration_ms = ms(time.monotonic() - start)
            out.error = e
            return out

        # 非 2xx：读取错误体后返回，不提交响应体（可 fallback）
        if resp.status_code < 200 or resp.status_code >= 300:
            try:
                err_body = await resp.aread()
            finally:
                await resp.aclose()
            res.status = resp.status_code
            res.duration_ms = ms(time.monotonic() - start)
            out.error = UpstreamError(resp.status_code,
                                      err_body[:8 * 1024].decode("utf-8", "replace"))
            return out

        res.status = resp.status_code
        res.upstream_model = m.cfg.upstream.model
        res.stream = stream_requested
        res.body_committed = True

        rec = getattr(entry, "rec", None) if entry is not None else None

        if stream_requested and outbound_format == "claude":
            out.response = self._stream_claude(resp, m, start, res, rec)
        elif stream_requested:
            out.response = self._stream_passthrough(resp, m, start, res, rec)
        elif outbound_format == "claude":
            out.response = await self._nonstream_claude(resp, m, start, res, rec)
        else:
            out.response = await self._nonstream(resp, start, res, rec)
        return out

    async def _nonstream(self, resp, start, res, rec) -> Any:
        from starlette.responses import Response
        try:
            body = await resp.aread()
        finally:
            await resp.aclose()
        res.duration_ms = ms(time.monotonic() - start)
        headers = _copy_response_headers(resp.headers)
        if rec is not None:
            rec.write(body)
        media = resp.headers.get("content-type", "application/json")
        return Response(content=body, status_code=res.status,
                        media_type=media, headers=headers)

    async def _nonstream_claude(self, resp, m, start, res, rec) -> Any:
        from starlette.responses import Response
        try:
            body = await resp.aread()
        finally:
            await resp.aclose()
        res.duration_ms = ms(time.monotonic() - start)
        try:
            out = claudemod.openai_response_to_claude(body, m.cfg.upstream.model)
        except Exception:  # noqa: BLE001 转换失败：原样回退
            if rec is not None:
                rec.write(body)
            headers = _copy_response_headers(resp.headers)
            media = resp.headers.get("content-type", "application/json")
            return Response(content=body, status_code=res.status,
                            media_type=media, headers=headers)
        if rec is not None:
            rec.write(out)
        return Response(content=out, status_code=200, media_type="application/json")

    def _stream_passthrough(self, resp, m, start, res, rec) -> Any:
        """SSE 管道式透传：逐行读上游、立即推送、空闲超时、keepalive ping。"""
        from starlette.responses import StreamingResponse

        async def gen():
            first = False
            try:
                queue: asyncio.Queue = asyncio.Queue(maxsize=16)
                reader = asyncio.ensure_future(_read_lines(resp, queue))
                idle = m.idle_timeout
                last_data = time.monotonic()
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=m.keepalive)
                    except asyncio.TimeoutError:
                        yield b": keepalive\n\n"
                        if time.monotonic() - last_data > idle:
                            break
                        continue
                    if item is None:
                        break
                    if isinstance(item, Exception):
                        break
                    if not first:
                        res.first_token_ms = ms(time.monotonic() - start)
                        first = True
                    last_data = time.monotonic()
                    chunk = (item + "\n").encode("utf-8")
                    if rec is not None:
                        rec.write(chunk)
                    yield chunk
                reader.cancel()
            finally:
                res.duration_ms = ms(time.monotonic() - start)
                await resp.aclose()

        return StreamingResponse(gen(), status_code=200,
                                 media_type="text/event-stream",
                                 headers=_sse_headers())

    def _stream_claude(self, resp, m, start, res, rec) -> Any:
        """读上游 OpenAI SSE，逐块转成 Claude SSE 事件推送。"""
        from starlette.responses import StreamingResponse

        st = claudemod.ClaudeStreamState(m.cfg.upstream.model)

        async def gen():
            first = False
            pending = []

            def write_event(name, data):
                b = json.dumps(data)
                pending.append(f"event: {name}\ndata: {b}\n\n".encode("utf-8"))

            try:
                queue: asyncio.Queue = asyncio.Queue(maxsize=16)
                reader = asyncio.ensure_future(_read_lines(resp, queue))
                idle = m.idle_timeout
                last_data = time.monotonic()
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), timeout=m.keepalive)
                    except asyncio.TimeoutError:
                        yield b": keepalive\n\n"
                        if time.monotonic() - last_data > idle:
                            break
                        continue
                    if item is None:
                        st.finish_stream(write_event)
                        for p in pending:
                            if rec is not None:
                                rec.write(p)
                            yield p
                        break
                    if isinstance(item, Exception):
                        break
                    line = item.strip()
                    if not line or not line.startswith("data:"):
                        continue
                    payload = line[len("data:"):].strip()
                    if payload == "[DONE]":
                        st.finish_stream(write_event)
                        for p in pending:
                            if rec is not None:
                                rec.write(p)
                            yield p
                        break
                    try:
                        chunk = json.loads(payload)
                    except ValueError:
                        continue
                    if not first:
                        res.first_token_ms = ms(time.monotonic() - start)
                        first = True
                    last_data = time.monotonic()
                    pending.clear()
                    st.handle_chunk(chunk, write_event)
                    for p in pending:
                        if rec is not None:
                            rec.write(p)
                        yield p
                reader.cancel()
            finally:
                res.duration_ms = ms(time.monotonic() - start)
                await resp.aclose()

        return StreamingResponse(gen(), status_code=200,
                                 media_type="text/event-stream",
                                 headers=_sse_headers())


async def _read_lines(resp, queue: asyncio.Queue) -> None:
    """后台读上游 SSE 行推入队列，结束推 None，异常推 Exception。"""
    try:
        async for line in resp.aiter_lines():
            await queue.put(line)
        await queue.put(None)
    except Exception as e:  # noqa: BLE001
        await queue.put(e)


def _sse_headers() -> Dict[str, str]:
    return {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",  # 禁用 nginx 缓冲
    }


def _copy_response_headers(src: httpx.Headers) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in src.items():
        lk = k.lower()
        if lk in ("content-length", "transfer-encoding", "connection", "content-encoding"):
            continue
        out[k] = v
    return out
