"""调用日志：记录完整调用过程与输入输出，按日期落盘为 JSONL。
对齐 Go 版 internal/gateway/call_logger.go。文件名形如 logs/calls-2026-07-20.log，跨天轮转。
"""
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

_REDACT_HEADERS = {"authorization", "x-api-key", "x-goog-api-key"}


def _redact_value(s: str) -> str:
    """保留前 8 字符 + '***'。"""
    s = s.strip()
    if s.startswith("Bearer "):
        s = s[len("Bearer "):]
    if len(s) <= 8:
        return s + "***"
    return s[:8] + "***"


@dataclass
class AttemptLog:
    model: str = ""
    priority: int = 0
    upstream_model: str = ""
    status: int = 0
    outcome: str = ""       # success / retry / failover / client_error / error
    attempt: int = 0
    duration_ms: int = 0
    first_token_ms: int = 0
    error: str = ""

    def to_dict(self) -> dict:
        d = {"model": self.model, "priority": self.priority,
             "status": self.status, "outcome": self.outcome,
             "duration_ms": self.duration_ms}
        if self.upstream_model:
            d["upstream_model"] = self.upstream_model
        if self.attempt:
            d["attempt"] = self.attempt
        if self.first_token_ms:
            d["first_token_ms"] = self.first_token_ms
        if self.error:
            d["error"] = self.error
        return d


class ResponseRecorder:
    """捕获写给客户端的字节，用于把响应体（含格式转换后的输出）记入日志。"""

    def __init__(self, body_limit: int) -> None:
        self._buf = bytearray()
        self._limit = body_limit
        self.chunk_count = 0

    def write(self, b: bytes) -> None:
        if isinstance(b, str):
            b = b.encode("utf-8")
        if len(b) >= 5 and b[:5] == b"data:":
            self.chunk_count += 1
        if len(self._buf) < self._limit:
            remaining = self._limit - len(self._buf)
            self._buf.extend(b[:remaining])

    def body_str(self) -> str:
        return self._buf.decode("utf-8", "replace")


@dataclass
class CallEntry:
    id: str = ""
    timestamp: str = ""
    method: str = ""
    path: str = ""
    client_ip: str = ""
    chain: str = ""
    format: str = ""        # openai / claude
    stream: bool = False
    headers: Dict[str, str] = field(default_factory=dict)
    req_body: str = ""
    upstream_req_body: str = ""
    attempts: List[AttemptLog] = field(default_factory=list)
    resp_status: int = 0
    resp_body: str = ""
    resp_chunks: int = 0
    duration_ms: int = 0
    error: str = ""

    start: float = 0.0
    rec: Optional[ResponseRecorder] = None

    def to_dict(self) -> dict:
        d = {
            "id": self.id, "timestamp": self.timestamp, "method": self.method,
            "path": self.path, "client_ip": self.client_ip, "format": self.format,
            "stream": self.stream, "resp_status": self.resp_status,
            "duration_ms": self.duration_ms,
        }
        if self.chain:
            d["chain"] = self.chain
        if self.headers:
            d["headers"] = self.headers
        if self.req_body:
            d["req_body"] = self.req_body
        if self.upstream_req_body:
            d["upstream_req_body"] = self.upstream_req_body
        if self.attempts:
            d["attempts"] = [a.to_dict() for a in self.attempts]
        if self.resp_body:
            d["resp_body"] = self.resp_body
        if self.resp_chunks:
            d["resp_chunks"] = self.resp_chunks
        if self.error:
            d["error"] = self.error
        return d


def _truncate(s: str, limit: int) -> str:
    if limit <= 0 or len(s) <= limit:
        return s
    return s[:limit] + "...[truncated]"


class CallLogger:
    """按日期轮转的调用日志记录器。对齐 Go CallLogger。"""

    def __init__(self, dir_: str, enabled: bool, redact: bool,
                 upstream: bool, body_limit: int) -> None:
        self._lock = threading.Lock()
        self._enabled = enabled
        self._dir = dir_
        self._redact = redact
        self._upstream = upstream
        self._body_limit = body_limit
        self._file = None
        self._file_date = ""

    def enabled(self) -> bool:
        return self._enabled

    def _redact_headers(self, headers) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for k, v in headers.items():
            lk = k.lower()
            val = v if isinstance(v, str) else ", ".join(v)
            if self._redact and lk in _REDACT_HEADERS:
                val = _redact_value(val)
            out[lk] = val
        return out

    def begin(self, method: str, path: str, client_ip: str,
              headers, fmt: str) -> Optional[CallEntry]:
        """创建调用日志条目并返回；未启用返回 None。"""
        if not self._enabled:
            return None
        rec = ResponseRecorder(self._body_limit)
        return CallEntry(
            id="req_" + os.urandom(8).hex(),
            timestamp=datetime.now().astimezone().isoformat(),
            method=method, path=path, client_ip=client_ip,
            format=fmt, start=time.monotonic(),
            headers=self._redact_headers(headers), rec=rec,
        )

    def set_request(self, e: Optional[CallEntry], body: bytes,
                    chain: str, stream: bool) -> None:
        if e is None:
            return
        e.req_body = _truncate(body.decode("utf-8", "replace"), self._body_limit)
        e.chain = chain
        e.stream = stream

    def set_upstream_request(self, e: Optional[CallEntry], body: bytes) -> None:
        if e is None or not self._upstream:
            return
        e.upstream_req_body = _truncate(body.decode("utf-8", "replace"), self._body_limit)

    def add_attempt(self, e: Optional[CallEntry], a: AttemptLog) -> None:
        if e is None:
            return
        e.attempts.append(a)

    def end(self, e: Optional[CallEntry], status: int, err: Optional[Exception]) -> None:
        if e is None or not self._enabled:
            return
        e.resp_status = status
        e.duration_ms = int((time.monotonic() - e.start) * 1000)
        if e.rec is not None:
            e.resp_body = _truncate(e.rec.body_str(), self._body_limit)
            e.resp_chunks = e.rec.chunk_count
        if err is not None:
            e.error = str(err)
        self._write(e)

    def _write(self, e: CallEntry) -> None:
        with self._lock:
            if not self._rotate_file():
                return
            self._file.write(json.dumps(e.to_dict(), ensure_ascii=False))
            self._file.write("\n")
            self._file.flush()

    def _rotate_file(self) -> bool:
        today = datetime.now().strftime("%Y-%m-%d")
        if self._file is not None and today == self._file_date:
            return True
        if self._file is not None:
            self._file.close()
        try:
            os.makedirs(self._dir, exist_ok=True)
            path = os.path.join(self._dir, f"calls-{today}.log")
            self._file = open(path, "a", encoding="utf-8")
            self._file_date = today
            return True
        except OSError:
            return False
