"""异常捕获中间件：捕获所有未处理异常防止请求处理崩溃。

记录完整堆栈并返回 500，累加 panic 计数供监控。Starlette/uvicorn 本身不会因
单个请求异常退出进程，但统一在此捕获可保证按出站格式返回错误并可观测。
"""
import logging
import traceback

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

_log = logging.getLogger("auto2api.recovery")

# 累计捕获的未处理异常次数（供 /v1/health 监控）
_total_panics = 0


def get_panic_stats() -> int:
    """返回累计捕获的未处理异常次数。"""
    return _total_panics


class RecoveryMiddleware(BaseHTTPMiddleware):
    """捕获下游处理中的未处理异常，记录完整堆栈并返回 500。"""

    async def dispatch(self, request: Request, call_next):
        global _total_panics
        try:
            return await call_next(request)
        except Exception as exc:  # noqa: BLE001
            _total_panics += 1
            _log.error("[PANIC RECOVERED] %s\n%s", exc, traceback.format_exc())
            # 路径含 /messages 视为 Claude 出站格式
            if "/messages" in request.url.path:
                return JSONResponse(status_code=500, content={
                    "type": "error",
                    "error": {"type": "internal_server_error",
                              "message": "internal server error (exception recovered)"},
                })
            return JSONResponse(status_code=500, content={
                "error": {"message": "internal server error (exception recovered)",
                          "type": "internal_server_error"},
            })
