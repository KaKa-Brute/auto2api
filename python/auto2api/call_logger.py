"""调用日志：记录完整调用过程与输入输出，按日期 + 大小轮转落盘为 JSONL。
正常调用写入 calls-YYYY-MM-DD.log，错误调用（err 非空或响应状态非 2xx）单独写入
errors-YYYY-MM-DD.log，两类文件独立按日期 + 大小轮转，旧文件可 gzip 压缩，超龄自动删除。
"""
import gzip
import json
import os
import shutil
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
    prompt_tokens: int = 0       # 本次尝试的输入 token
    completion_tokens: int = 0   # 本次尝试的输出 token

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
        if self.prompt_tokens:
            d["prompt_tokens"] = self.prompt_tokens
        if self.completion_tokens:
            d["completion_tokens"] = self.completion_tokens
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
    prompt_tokens: int = 0       # 本次请求总输入 token
    completion_tokens: int = 0   # 本次请求总输出 token

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
        if self.prompt_tokens:
            d["prompt_tokens"] = self.prompt_tokens
        if self.completion_tokens:
            d["completion_tokens"] = self.completion_tokens
        return d


def _truncate(s: str, limit: int) -> str:
    if limit <= 0 or len(s) <= limit:
        return s
    return s[:limit] + "...[truncated]"


def _log_date_from_name(name: str) -> str:
    """从文件名提取 YYYY-MM-DD 日期。
    支持格式：calls-2026-07-29.log、calls-2026-07-29.1.log、calls-2026-07-29.1.log.gz
    以及 errors- 同名格式。
    """
    for prefix in ("calls-", "errors-"):
        if name.startswith(prefix):
            rest = name[len(prefix):]
            if len(rest) >= 10:
                return rest[:10]
    return ""


def _is_rotated(name: str) -> bool:
    """判断是否为轮转备份文件（含 .N. 的文件名）。"""
    return ".log." in name and not name.endswith(".log")


_LOG_PREFIXES = ("calls-", "errors-")


class CallLogger:
    """按日期 + 大小轮转的调用日志记录器。
    正常调用写 calls-*.log，错误调用写 errors-*.log，互不混写。
    """

    def __init__(self, dir_: str, enabled: bool, redact: bool,
                 upstream: bool, body_limit: int, *,
                 log_resp_body: bool = True, max_size_mb: int = 100,
                 max_age_days: int = 7, max_backups: int = 10,
                 compress: bool = True) -> None:
        self._lock = threading.Lock()
        self._enabled = enabled
        self._dir = dir_
        self._redact = redact
        self._upstream = upstream
        self._body_limit = body_limit
        self._log_resp_body = log_resp_body
        self._max_size_bytes = max_size_mb * 1024 * 1024
        self._max_age_days = max_age_days
        self._max_backups = max_backups
        self._compress = compress
        # 前缀 -> (文件句柄, 文件日期)
        self._files: Dict[str, tuple] = {}

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

    def set_usage(self, e: Optional[CallEntry], prompt: int, completion: int) -> None:
        """累加本次请求的 token 用量（多次尝试时累加，取最后成功的返回值）。"""
        if e is None:
            return
        e.prompt_tokens = prompt
        e.completion_tokens = completion

    def end(self, e: Optional[CallEntry], status: int, err: Optional[Exception]) -> None:
        if e is None or not self._enabled:
            return
        e.resp_status = status
        e.duration_ms = int((time.monotonic() - e.start) * 1000)
        if self._log_resp_body and e.rec is not None:
            e.resp_body = _truncate(e.rec.body_str(), self._body_limit)
            e.resp_chunks = e.rec.chunk_count
        if err is not None:
            e.error = str(err)
        # 错误分流：err 非空或响应状态非 2xx 视为错误，单独写 errors-*.log
        prefix = "errors-" if (err is not None or status < 200 or status >= 300) else "calls-"
        self._write(e, prefix)

    def _write(self, e: CallEntry, prefix: str) -> None:
        with self._lock:
            f = self._rotate_file(prefix)
            if f is None:
                return
            f.write(json.dumps(e.to_dict(), ensure_ascii=False))
            f.write("\n")
            f.flush()

    def _rotate_file(self, prefix: str):
        today = datetime.now().strftime("%Y-%m-%d")
        f, date = self._files.get(prefix, (None, ""))
        # 跨天：关闭当前文件，清理旧文件
        if f is not None and today != date:
            f.close()
            f = None
            self._files[prefix] = (None, "")
            self._cleanup()
        # 打开新文件
        if f is None:
            try:
                os.makedirs(self._dir, exist_ok=True)
                path = os.path.join(self._dir, f"{prefix}{today}.log")
                f = open(path, "a", encoding="utf-8")
                self._files[prefix] = (f, today)
            except OSError:
                return None
        # 大小轮转：当前文件超限 → 重命名 + 压缩 + 开新文件
        if self._max_size_bytes > 0:
            try:
                size = f.tell()
            except OSError:
                size = 0
            if size >= self._max_size_bytes:
                f.close()
                self._rotate_and_compress(prefix, today)
                self._cleanup()
                try:
                    path = os.path.join(self._dir, f"{prefix}{today}.log")
                    f = open(path, "a", encoding="utf-8")
                    self._files[prefix] = (f, today)
                except OSError:
                    return None
        return f

    def _rotate_and_compress(self, prefix: str, date: str) -> None:
        """将当前日志文件重命名为带序号的备份，并按需 gzip 压缩。"""
        src = os.path.join(self._dir, f"{prefix}{date}.log")
        for n in range(1, 10000):
            dst = os.path.join(self._dir, f"{prefix}{date}.{n}.log")
            if not os.path.exists(dst) and not os.path.exists(dst + ".gz"):
                try:
                    os.rename(src, dst)
                except OSError:
                    return
                if self._compress:
                    self._gzip_file(dst)
                    try:
                        os.remove(dst)
                    except OSError:
                        pass
                return

    @staticmethod
    def _gzip_file(src: str) -> None:
        """将 src 压缩为 src.gz。"""
        try:
            with open(src, "rb") as f_in, \
                 gzip.open(src + ".gz", "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        except OSError:
            pass

    def _cleanup(self) -> None:
        """清理超龄文件和超额备份。"""
        if self._max_age_days <= 0 and self._max_backups <= 0:
            return
        try:
            entries = os.listdir(self._dir)
        except OSError:
            return
        now = datetime.now()
        # 按日期分组的轮转文件
        by_date: Dict[str, list] = {}
        for name in entries:
            if not name.startswith(_LOG_PREFIXES):
                continue
            path = os.path.join(self._dir, name)
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            # 超龄删除：按文件名中的日期判断
            if self._max_age_days > 0:
                date_str = _log_date_from_name(name)
                if date_str:
                    try:
                        t = datetime.strptime(date_str, "%Y-%m-%d")
                        if (now - t).days >= self._max_age_days:
                            try:
                                os.remove(path)
                            except OSError:
                                pass
                            continue
                    except ValueError:
                        pass
            # 收集轮转文件（非当前活跃文件），按 前缀+日期 分组
            if _is_rotated(name):
                prefix = next((p for p in _LOG_PREFIXES if name.startswith(p)), "")
                date_str = _log_date_from_name(name) or "_unknown"
                by_date.setdefault(prefix + date_str, []).append((name, mtime))
        # 按前缀+日期分组，超出 max_backups 的删最旧
        if self._max_backups > 0:
            for files in by_date.values():
                if len(files) <= self._max_backups:
                    continue
                files.sort(key=lambda x: x[1])
                for name, _ in files[:len(files) - self._max_backups]:
                    try:
                        os.remove(os.path.join(self._dir, name))
                    except OSError:
                        pass
