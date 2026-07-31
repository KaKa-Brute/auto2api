"""Claude（Anthropic Messages API）兼容层。对齐 Go 版 internal/gateway/claude.go。

入站：客户端按 Claude 格式 POST /v1/messages，请求体转 OpenAI 格式后走上游。
出站：上游 OpenAI 响应（非流式 JSON / 流式 SSE）转回 Claude 格式。
"""
import json
import os
from typing import Any, Dict, List, Optional, Tuple


def new_rand_id() -> str:
    """生成 12 字节十六进制 ID。"""
    return os.urandom(12).hex()


# ---- 请求转换：Claude → OpenAI ----

def claude_request_to_openai(body: bytes) -> Tuple[bytes, str]:
    """把 Claude Messages 请求体转成 OpenAI Chat 请求体，返回 (openai_body, model)。"""
    src = json.loads(body)
    if not isinstance(src, dict):
        src = {}
    dst: Dict[str, Any] = {}

    model = src.get("model") if isinstance(src.get("model"), str) else ""
    if model:
        dst["model"] = model

    # 系统提示：Claude 顶层 system → OpenAI system 消息
    if "system" in src:
        sys_text = _flatten_system(src["system"])
        msgs = src.get("messages")
        msgs = list(msgs) if isinstance(msgs, list) else []
        if sys_text:
            dst["messages"] = [{"role": "system", "content": sys_text}] + msgs
        else:
            dst["messages"] = msgs
    else:
        dst["messages"] = src.get("messages")

    if isinstance(dst.get("messages"), list):
        dst["messages"] = _convert_messages(dst["messages"])

    # 参数映射
    _copy_num(src, dst, "max_tokens")
    _copy_num(src, dst, "temperature")
    _copy_num(src, dst, "top_p")
    _copy_num(src, dst, "top_k")
    ss = src.get("stop_sequences")
    if isinstance(ss, list) and ss:
        dst["stop"] = ss
    if isinstance(src.get("stream"), bool):
        dst["stream"] = src["stream"]

    if isinstance(src.get("tools"), list):
        dst["tools"] = _convert_tools_to_openai(src["tools"])
    if "tool_choice" in src:
        dst["tool_choice"] = _convert_tool_choice_to_openai(src["tool_choice"])

    return json.dumps(dst).encode("utf-8"), model


def _flatten_system(sys: Any) -> str:
    """把 Claude system（string | [{type:text,text:...}]）拍平为字符串。"""
    if isinstance(sys, str):
        return sys
    if isinstance(sys, list):
        parts = []
        for b in sys:
            if isinstance(b, dict) and b.get("type") == "text":
                s = b.get("text")
                if isinstance(s, str) and s:
                    parts.append(s)
        return "\n".join(parts)
    return ""


def _convert_messages(msgs: List[Any]) -> List[Any]:
    """转换消息列表：处理 tool_use/tool_result/image 等内容块。"""
    out: List[Any] = []
    for raw in msgs:
        if not isinstance(raw, dict):
            out.append(raw)
            continue
        role = raw.get("role")
        content = raw.get("content")
        if role == "user" and isinstance(content, list):
            tool_results, rest = _split_tool_results(content)
            out.extend(tool_results)
            if rest:
                raw["content"] = _convert_content_blocks(rest)
                out.append(raw)
            continue
        if role == "assistant" and isinstance(content, list):
            converted, tool_calls = _convert_assistant_blocks(content)
            raw["content"] = converted
            if tool_calls:
                raw["tool_calls"] = tool_calls
            out.append(raw)
            continue
        if isinstance(content, str):
            pass  # 原样
        elif isinstance(content, list):
            raw["content"] = _convert_content_blocks(content)
        out.append(raw)
    return out


def _split_tool_results(blocks: List[Any]) -> Tuple[List[Any], List[Any]]:
    """把 tool_result 块拆成 OpenAI role=tool 消息，其余返回原样。"""
    tool_msgs: List[Any] = []
    rest: List[Any] = []
    for b in blocks:
        if not isinstance(b, dict):
            rest.append(b)
            continue
        if b.get("type") == "tool_result":
            tool_use_id = b.get("tool_use_id")
            if not tool_use_id:
                continue  # 过滤 id 为空的无效 tool_result
            content = _extract_tool_result_content(b.get("content"))
            tool_msgs.append({
                "role": "tool",
                "tool_call_id": tool_use_id,
                "content": content,
            })
            continue
        rest.append(b)
    return tool_msgs, rest


def _extract_tool_result_content(c: Any) -> str:
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        parts = []
        for b in c:
            if isinstance(b, dict) and b.get("type") == "text":
                s = b.get("text")
                if isinstance(s, str) and s:
                    parts.append(s)
        return "\n".join(parts)
    return ""


def _convert_assistant_blocks(blocks: List[Any]) -> Tuple[Any, List[Any]]:
    """转换 assistant 内容块：text 保留，tool_use → tool_calls。"""
    text_parts: List[str] = []
    has_text_block = False
    tool_calls: List[Any] = []
    for b in blocks:
        if not isinstance(b, dict):
            continue
        t = b.get("type")
        if t == "text":
            has_text_block = True
            s = b.get("text")
            if isinstance(s, str) and s:
                text_parts.append(s)
        elif t == "tool_use":
            tid = b.get("id")
            if not tid:
                continue  # 过滤 id 为空的无效 tool_use
            name = b.get("name") or ""
            args = json.dumps(b.get("input"))
            tool_calls.append({
                "id": tid,
                "type": "function",
                "function": {"name": name, "arguments": args},
            })
    if has_text_block:
        converted: Any = "".join(text_parts)
    elif tool_calls:
        converted = None  # OpenAI assistant 带 tool_calls 时 content 可为 null
    else:
        converted = blocks
    return converted, tool_calls


def _convert_content_blocks(blocks: List[Any]) -> List[Any]:
    """把 Claude 内容块转成 OpenAI 内容块（text/image）。"""
    out: List[Any] = []
    for b in blocks:
        if not isinstance(b, dict):
            out.append(b)
            continue
        t = b.get("type")
        if t == "text":
            out.append({"type": "text", "text": b.get("text")})
        elif t == "image":
            url = _claude_image_to_openai_url(b.get("source"))
            if url:
                out.append({"type": "image_url", "image_url": {"url": url}})
        elif t == "video":
            url = _claude_video_to_openai_url(b.get("source"))
            if url:
                out.append({"type": "video_url", "video_url": {"url": url}})
        else:
            out.append(b)  # 未知块原样透传
    return out


def _claude_image_to_openai_url(src: Any) -> str:
    if not isinstance(src, dict):
        return ""
    st = src.get("type")
    if st == "url":
        return src.get("url") or ""
    if st == "base64":
        media = src.get("media_type") or "image/png"
        data = src.get("data") or ""
        return f"data:{media};base64,{data}"
    return ""


def _claude_video_to_openai_url(src: Any) -> str:
    if not isinstance(src, dict):
        return ""
    st = src.get("type")
    if st == "url":
        return src.get("url") or ""
    if st == "base64":
        media = src.get("media_type") or "video/mp4"
        data = src.get("data") or ""
        return f"data:{media};base64,{data}"
    return ""


def _convert_tools_to_openai(tools: List[Any]) -> List[Any]:
    out: List[Any] = []
    for t in tools:
        if not isinstance(t, dict):
            out.append(t)
            continue
        out.append({
            "type": "function",
            "function": {
                "name": t.get("name") or "",
                "description": t.get("description") or "",
                "parameters": t.get("input_schema"),
            },
        })
    return out


def _convert_tool_choice_to_openai(tc: Any) -> Any:
    if not isinstance(tc, dict):
        return tc  # "any"/"auto"/"none" 字符串
    t = tc.get("type")
    if t == "auto":
        return "auto"
    if t == "any":
        return "required"
    if t == "tool":
        return {"type": "function", "function": {"name": tc.get("name") or ""}}
    return tc


def _copy_num(src: Dict[str, Any], dst: Dict[str, Any], key: str) -> None:
    if key in src and isinstance(src[key], (int, float)) and not isinstance(src[key], bool):
        dst[key] = src[key]


# ---- 响应转换：OpenAI → Claude（非流式）----

def openai_response_to_claude(body: bytes, upstream_model: str) -> bytes:
    """把 OpenAI chat completion JSON 转成 Claude message JSON。"""
    src = json.loads(body)
    cid = src.get("id") if isinstance(src.get("id"), str) else ""
    if not cid:
        mid = "msg_" + new_rand_id()
    else:
        mid = "msg_" + (cid[len("chatcmpl-"):] if cid.startswith("chatcmpl-") else cid)

    content = ""
    finish = "end_turn"
    tool_calls: List[Any] = []
    choices = src.get("choices")
    if isinstance(choices, list) and choices:
        ch = choices[0]
        if isinstance(ch, dict):
            msg = ch.get("message")
            if isinstance(msg, dict):
                if isinstance(msg.get("content"), str):
                    content = msg["content"]
                if isinstance(msg.get("tool_calls"), list):
                    tool_calls = msg["tool_calls"]
            if isinstance(ch.get("finish_reason"), str):
                finish = openai_to_claude_stop_reason(ch["finish_reason"])

    content_blocks: List[Any] = []
    if content:
        content_blocks.append({"type": "text", "text": content, "index": 0})
    for i, tc in enumerate(tool_calls):
        if isinstance(tc, dict):
            fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
            name = fn.get("name") or ""
            args_str = fn.get("arguments") or ""
            try:
                inp = json.loads(args_str) if args_str else None
            except (ValueError, TypeError):
                inp = None
            content_blocks.append({
                "type": "tool_use",
                "id": tc.get("id"),
                "name": name,
                "input": inp,
                "index": i + 1,
            })

    in_tok, out_tok = _extract_openai_usage(src.get("usage"))
    resp = {
        "id": mid,
        "type": "message",
        "role": "assistant",
        "content": content_blocks,
        "model": upstream_model,
        "stop_reason": finish,
        "stop_sequence": None,
        "usage": {"input_tokens": in_tok, "output_tokens": out_tok},
    }
    return json.dumps(resp).encode("utf-8")


def openai_to_claude_stop_reason(fr: str) -> str:
    """把 OpenAI finish_reason 映射成 Claude stop_reason。"""
    return {
        "stop": "end_turn",
        "length": "max_tokens",
        "tool_calls": "tool_use",
        "function_call": "tool_use",
        "content_filter": "end_turn",
        "": "end_turn",
    }.get(fr, fr)


def _extract_openai_usage(u: Any) -> Tuple[int, int]:
    if not isinstance(u, dict):
        return 0, 0
    pt = u.get("prompt_tokens")
    ct = u.get("completion_tokens")
    in_tok = int(pt) if isinstance(pt, (int, float)) else 0
    out_tok = int(ct) if isinstance(ct, (int, float)) else 0
    return in_tok, out_tok


# ---- 响应转换：OpenAI → Claude（流式 SSE）----

class ClaudeStreamState:
    """维护 OpenAI→Claude 流式转换的跨事件状态。对齐 Go claudeStreamState。"""

    def __init__(self, model: str) -> None:
        self.message_start_sent = False
        self.text_block_open = False
        self.tool_block_indexes: Dict[str, int] = {}
        self.tool_block_by_openai_index: Dict[int, int] = {}
        self.next_block_index = 1
        self.model = model
        self.message_id = "msg_" + new_rand_id()
        self.output_tokens = 0
        self.input_tokens = 0

    def handle_chunk(self, chunk: Dict[str, Any], write_event) -> bool:
        """处理单个 OpenAI SSE chunk，转成 Claude 事件写出。返回是否产生输出。"""
        choices = chunk.get("choices")
        if not isinstance(choices, list) or not choices:
            u = chunk.get("usage")
            if isinstance(u, dict):
                self._collect_usage(u)
            return False
        ch = choices[0] if isinstance(choices[0], dict) else {}
        delta = ch.get("delta") if isinstance(ch.get("delta"), dict) else {}

        if not self.message_start_sent:
            in_tok = 0
            u = chunk.get("usage")
            if isinstance(u, dict) and isinstance(u.get("prompt_tokens"), (int, float)):
                in_tok = int(u["prompt_tokens"])
            self.input_tokens = in_tok
            self.message_start_sent = True
            write_event("message_start", {
                "type": "message_start",
                "message": {
                    "id": self.message_id,
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": self.model,
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": in_tok, "output_tokens": 1},
                },
            })
            write_event("content_block_start", {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            })
            self.text_block_open = True

        text = delta.get("content")
        if isinstance(text, str) and text:
            write_event("content_block_delta", {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            })
            self.output_tokens += 1

        tcs = delta.get("tool_calls")
        if isinstance(tcs, list):
            for tc in tcs:
                if not isinstance(tc, dict):
                    continue
                block_index = self._ensure_tool_block(write_event, tc)
                if block_index < 0:
                    continue
                partial = _extract_tool_arguments_delta(tc)
                if partial:
                    write_event("content_block_delta", {
                        "type": "content_block_delta",
                        "index": block_index,
                        "delta": {"type": "input_json_delta", "partial_json": partial},
                    })

        fr = ch.get("finish_reason")
        if isinstance(fr, str) and fr:
            stop = openai_to_claude_stop_reason(fr)
            u = chunk.get("usage")
            if isinstance(u, dict):
                self._collect_usage(u)
            self._close_open_blocks(write_event)
            write_event("message_delta", {
                "type": "message_delta",
                "delta": {"stop_reason": stop, "stop_sequence": None},
                "usage": {"output_tokens": self.output_tokens},
            })
        return True

    def _ensure_tool_block(self, write_event, tc: Dict[str, Any]) -> int:
        tid = tc.get("id") or ""
        if not tid:
            tid = _nested_string(tc, "function", "name")
        if not tid:
            openai_idx = _tool_index_of(tc)
            if openai_idx >= 0 and openai_idx in self.tool_block_by_openai_index:
                return self.tool_block_by_openai_index[openai_idx]
            return -1
        if tid in self.tool_block_indexes:
            return self.tool_block_indexes[tid]
        idx = _tool_index_of(tc)
        if idx < 0:
            idx = self.next_block_index
        else:
            idx += 1
        if idx >= self.next_block_index:
            self.next_block_index = idx + 1
        name = _nested_string(tc, "function", "name")
        write_event("content_block_start", {
            "type": "content_block_start",
            "index": idx,
            "content_block": {"type": "tool_use", "id": tid, "name": name, "input": {}},
        })
        self.tool_block_indexes[tid] = idx
        openai_idx = _tool_index_of(tc)
        if openai_idx >= 0:
            self.tool_block_by_openai_index[openai_idx] = idx
        return idx

    def _close_open_blocks(self, write_event) -> None:
        if self.text_block_open:
            write_event("content_block_stop", {"type": "content_block_stop", "index": 0})
            self.text_block_open = False
        if not self.tool_block_indexes:
            return
        indexes = sorted(set(self.tool_block_indexes.values()))
        for idx in indexes:
            write_event("content_block_stop", {"type": "content_block_stop", "index": idx})
        self.tool_block_indexes = {}

    def _collect_usage(self, u: Dict[str, Any]) -> None:
        if isinstance(u.get("prompt_tokens"), (int, float)):
            self.input_tokens = int(u["prompt_tokens"])
        if isinstance(u.get("completion_tokens"), (int, float)):
            self.output_tokens = int(u["completion_tokens"])

    def finish_stream(self, write_event) -> None:
        """上游结束但未显式发 finish_reason 时补完 Claude 终止序列。"""
        self._close_open_blocks(write_event)
        write_event("message_delta", {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
            "usage": {"output_tokens": self.output_tokens},
        })
        write_event("message_stop", {"type": "message_stop"})


def _extract_tool_arguments_delta(tc: Dict[str, Any]) -> str:
    fn = tc.get("function")
    if isinstance(fn, dict) and isinstance(fn.get("arguments"), str):
        return fn["arguments"]
    return ""


def _tool_index_of(tc: Dict[str, Any]) -> int:
    v = tc.get("index")
    if isinstance(v, bool):
        return -1
    if isinstance(v, (int, float)):
        return int(v)
    return -1


def _nested_string(m: Dict[str, Any], *keys: str) -> str:
    cur: Any = m
    for k in keys:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(k)
    return cur if isinstance(cur, str) else ""
