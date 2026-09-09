"""OpenAI Responses API 兼容层（转换模式）。

入站：客户端按 Responses 格式 POST /v1/responses。
- 上游原生支持 /v1/responses 时直通（forwarder 走 endpoint="responses"）。
- 上游仅支持 /v1/chat/completions 时，本模块把请求体转成 Chat 格式，
  响应（非流式 JSON / 流式 SSE）再转回 Responses 格式。

与 claude.py 的转换管线同构：请求转换 → 上游 → 响应转换。
"""
import json
import os
from typing import Any, Dict, List, Optional, Tuple


def new_rand_id() -> str:
    return os.urandom(12).hex()


# ---- 请求转换：Responses → Chat ----

def responses_request_to_chat(body: bytes) -> Tuple[bytes, str]:
    """把 Responses 请求体转成 Chat Completions 请求体，返回 (chat_body, model)。"""
    src = json.loads(body)
    if not isinstance(src, dict):
        src = {}
    dst: Dict[str, Any] = {}

    model = src.get("model") if isinstance(src.get("model"), str) else ""
    if model:
        dst["model"] = model

    # instructions → system 消息
    msgs: List[Any] = []
    if isinstance(src.get("instructions"), str) and src["instructions"]:
        msgs.append({"role": "system", "content": src["instructions"]})

    # input：字符串 或 消息列表（Responses 消息与 Chat 消息结构相近）
    inp = src.get("input")
    if isinstance(inp, str):
        msgs.append({"role": "user", "content": inp})
    elif isinstance(inp, list):
        msgs.extend(_convert_input_messages(inp))
    dst["messages"] = msgs

    # 参数映射
    if isinstance(src.get("max_output_tokens"), (int, float)) and not isinstance(src["max_output_tokens"], bool):
        dst["max_tokens"] = int(src["max_output_tokens"])
    if isinstance(src.get("temperature"), (int, float)) and not isinstance(src["temperature"], bool):
        dst["temperature"] = src["temperature"]
    if isinstance(src.get("top_p"), (int, float)) and not isinstance(src["top_p"], bool):
        dst["top_p"] = src["top_p"]
    if isinstance(src.get("stream"), bool):
        dst["stream"] = src["stream"]

    # 工具映射：Responses function 工具 → Chat function 工具
    if isinstance(src.get("tools"), list):
        tools = _convert_tools_to_chat(src["tools"])
        if tools:
            dst["tools"] = tools
    if "tool_choice" in src:
        dst["tool_choice"] = _convert_tool_choice_to_chat(src["tool_choice"])

    return json.dumps(dst).encode("utf-8"), model


def _convert_input_messages(msgs: List[Any]) -> List[Any]:
    """转换 Responses input 消息列表为 Chat messages。

    兼容 codex 等客户端回放的历史项：
    - 顶层 function_call 项 → assistant tool_calls 消息
    - reasoning 项（思维链回放）→ 丢弃（Chat 无对应）
    - function_call_output → role=tool
    - developer 角色 → system
    """
    out: List[Any] = []
    for raw in msgs:
        if not isinstance(raw, dict):
            continue
        role = raw.get("role")
        itype = raw.get("type") if isinstance(raw.get("type"), str) else ""
        # Responses 的 function_call_output / 本地工具结果 → Chat role=tool
        if role == "function_call_output" or itype == "function_call_output":
            out.append({
                "role": "tool",
                "tool_call_id": raw.get("call_id") or "",
                "content": _content_to_text(raw.get("output")),
            })
            continue
        # 顶层 function_call 项（codex 历史回放）→ assistant tool_calls 消息
        if itype == "function_call":
            args = raw.get("arguments")
            if not isinstance(args, str):
                args = json.dumps(args or {})
            out.append({
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": raw.get("call_id") or raw.get("id") or "",
                    "type": "function",
                    "function": {"name": raw.get("name") or "", "arguments": args},
                }],
            })
            continue
        # reasoning 项（思维链回放）→ Chat 无对应，丢弃
        if itype == "reasoning" or role == "reasoning":
            continue
        # 无 role 且非 message 类型（local_shell_call 等）→ 跳过
        if not isinstance(role, str) and itype not in ("", "message"):
            continue
        r = role if isinstance(role, str) else "user"
        if r == "developer":
            r = "system"
        m: Dict[str, Any] = {"role": r}
        content = _convert_content(raw.get("content"))
        # assistant 的 function_call（历史回放）→ tool_calls
        fc = raw.get("function_call")
        if isinstance(fc, dict):
            if content is not None:
                m["content"] = content
            else:
                m["content"] = None
            args = fc.get("arguments")
            if not isinstance(args, str):
                args = json.dumps(args)
            m["tool_calls"] = [{
                "id": fc.get("call_id") or fc.get("id") or "",
                "type": "function",
                "function": {"name": fc.get("name") or "", "arguments": args},
            }]
            out.append(m)
            continue
        if content is None:
            # 无内容无工具调用：丢弃，避免产生非法空消息
            continue
        m["content"] = content
        out.append(m)
    return out


def _convert_content(content: Any) -> Any:
    """把 Responses content（字符串或内容块列表）转成 Chat content。"""
    if content is None:
        return None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[Any] = []
        for b in content:
            if not isinstance(b, dict):
                continue
            t = b.get("type")
            if t in ("input_text", "output_text", "text", "summary_text"):
                s = b.get("text")
                if isinstance(s, str):
                    parts.append({"type": "text", "text": s})
            elif t == "input_image":
                url = b.get("image_url")
                if isinstance(url, str):
                    parts.append({"type": "image_url", "image_url": {"url": url}})
            elif t == "refusal":
                s = b.get("refusal")
                if isinstance(s, str):
                    parts.append({"type": "text", "text": s})
        if not parts:
            return ""
        if all(p.get("type") == "text" for p in parts):
            return "\n".join(p["text"] for p in parts)
        return parts
    return content


def _content_to_text(c: Any) -> str:
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        parts = []
        for b in c:
            if isinstance(b, dict):
                s = b.get("text") or b.get("output") or b.get("content")
                if isinstance(s, str):
                    parts.append(s)
        return "\n".join(parts)
    if isinstance(c, dict):
        s = c.get("text") or c.get("output") or c.get("content")
        return s if isinstance(s, str) else ""
    return ""


def _convert_tools_to_chat(tools: List[Any]) -> List[Any]:
    out: List[Any] = []
    for t in tools:
        if not isinstance(t, dict):
            continue
        if t.get("type") in ("function", "") or "name" in t:
            out.append({
                "type": "function",
                "function": {
                    "name": t.get("name") or "",
                    "description": t.get("description") or "",
                    "parameters": t.get("parameters") or {"type": "object", "properties": {}},
                },
            })
    return out


def _convert_tool_choice_to_chat(tc: Any) -> Any:
    if isinstance(tc, str):
        return tc  # auto / none / required
    if isinstance(tc, dict):
        if tc.get("type") == "function":
            return {"type": "function", "function": {"name": tc.get("name") or ""}}
    return "auto"


# ---- 响应转换：Chat → Responses（非流式）----

def chat_response_to_responses(body: bytes, upstream_model: str) -> bytes:
    """把 Chat completion JSON 转成 Responses API JSON。"""
    src = json.loads(body)
    cid = src.get("id") if isinstance(src.get("id"), str) else ""
    rid = "resp_" + (cid[len("chatcmpl-"):] if cid.startswith("chatcmpl-") else cid or new_rand_id())

    output: List[Any] = []
    finish = ""
    choices = src.get("choices")
    if isinstance(choices, list) and choices and isinstance(choices[0], dict):
        ch = choices[0]
        msg = ch.get("message") if isinstance(ch.get("message"), dict) else {}
        if isinstance(msg.get("content"), str) and msg["content"]:
            output.append({
                "type": "message",
                "id": "msg_" + new_rand_id(),
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": msg["content"], "annotations": []}],
            })
        for tc in (msg.get("tool_calls") or []):
            if not isinstance(tc, dict):
                continue
            fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
            output.append({
                "type": "function_call",
                "id": tc.get("id") or ("fc_" + new_rand_id()),
                "call_id": tc.get("id") or ("call_" + new_rand_id()),
                "name": fn.get("name") or "",
                "arguments": fn.get("arguments") or "",
                "status": "completed",
            })
        fr = ch.get("finish_reason")
        if isinstance(fr, str):
            finish = fr

    status = "completed"
    if finish == "length":
        status = "incomplete"
    incomplete = {"reason": "max_output_tokens"} if finish == "length" else None

    u = src.get("usage") if isinstance(src.get("usage"), dict) else {}
    usage = {
        "input_tokens": _to_int(u.get("prompt_tokens")),
        "input_tokens_details": {"cached_tokens": 0},
        "output_tokens": _to_int(u.get("completion_tokens")),
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": _to_int(u.get("total_tokens")),
    }

    resp = {
        "id": rid,
        "object": "response",
        "created_at": _to_int(src.get("created")) or int(_now()),
        "status": status,
        "background": False,
        "error": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": upstream_model,
        "output": output,
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "store": False,
        "temperature": None,
        "tool_choice": "auto",
        "tools": [],
        "top_p": None,
        "truncation": "disabled",
        "usage": usage,
        "metadata": {},
    }
    if incomplete:
        resp["incomplete_details"] = incomplete
    return json.dumps(resp).encode("utf-8")


def _to_int(v: Any) -> int:
    if isinstance(v, bool):
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    return 0


def _now() -> float:
    import time
    return time.time()


# ---- 响应转换：Chat SSE → Responses SSE（流式）----

class ResponsesStreamState:
    """维护 Chat→Responses 流式转换的跨事件状态，产出 Responses SSE 事件。"""

    def __init__(self, model: str) -> None:
        self.response_id = "resp_" + new_rand_id()
        self.model = model
        self.created_at = int(_now())
        self.created_sent = False
        self.output_index = 0            # 下一个输出项的下标
        self.msg_item_id = ""            # 当前 message 项 id（open 后有效）
        self.text_parts: List[str] = []  # 已推送的文本（用于 output_text.done 回填全文）
        self.tool_items: Dict[str, int] = {}       # item_id -> output index
        self.tool_call_ids: Dict[str, str] = {}    # item_id -> 上游 call_id
        self.tool_names: Dict[str, str] = {}       # item_id -> 工具名
        self.tool_by_call: Dict[str, str] = {}     # 上游 call_id -> item_id
        self.tool_by_pos: Dict[int, str] = {}      # 上游 tool index -> item_id
        self.tool_args: Dict[str, str] = {}        # item_id -> 累计 arguments
        self.input_tokens = 0
        self.output_tokens = 0
        self.finish = ""

    def handle_chunk(self, chunk: Dict[str, Any], write_event) -> bool:
        """处理单个 Chat SSE chunk，转成 Responses 事件写出。返回是否产生输出。"""
        if not self.created_sent:
            self.created_sent = True
            write_event("response.created", {
                "type": "response.created",
                "response": self._response_base(),
            })
            write_event("response.in_progress", {
                "type": "response.in_progress",
                "response": self._response_base(),
            })

        choices = chunk.get("choices")
        if isinstance(choices, list) and choices:
            ch = choices[0] if isinstance(choices[0], dict) else {}
            delta = ch.get("delta") if isinstance(ch.get("delta"), dict) else {}

            text = delta.get("content")
            if isinstance(text, str) and text:
                self._ensure_message_item(write_event)
                if not self.text_parts:
                    write_event("response.content_part.added", {
                        "type": "response.content_part.added",
                        "item_id": self.msg_item_id,
                        "output_index": self.output_index,
                        "content_index": 0,
                        "part": {"type": "output_text", "text": "", "annotations": []},
                    })
                self.text_parts.append(text)
                write_event("response.output_text.delta", {
                    "type": "response.output_text.delta",
                    "item_id": self.msg_item_id,
                    "output_index": self.output_index,
                    "content_index": 0, "delta": text,
                })

            tcs = delta.get("tool_calls")
            if isinstance(tcs, list):
                for tc in tcs:
                    if not isinstance(tc, dict):
                        continue
                    tid = self._ensure_tool_item(write_event, tc)
                    if not tid:
                        continue
                    fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
                    args = fn.get("arguments")
                    if isinstance(args, str) and args:
                        self.tool_args[tid] = self.tool_args.get(tid, "") + args
                        write_event("response.function_call_arguments.delta", {
                            "type": "response.function_call_arguments.delta",
                            "item_id": tid,
                            "output_index": self.tool_items[tid],
                            "delta": args,
                        })

            fr = ch.get("finish_reason")
            if isinstance(fr, str) and fr:
                self.finish = fr
        u = chunk.get("usage")
        if isinstance(u, dict):
            if isinstance(u.get("prompt_tokens"), (int, float)):
                self.input_tokens = int(u["prompt_tokens"])
            if isinstance(u.get("completion_tokens"), (int, float)):
                self.output_tokens = int(u["completion_tokens"])
        return True

    def _ensure_message_item(self, write_event) -> None:
        if self.msg_item_id:
            return
        self.msg_item_id = "msg_" + new_rand_id()
        write_event("response.output_item.added", {
            "type": "response.output_item.added",
            "output_index": self.output_index,
            "item": {"type": "message", "id": self.msg_item_id,
                      "status": "in_progress", "role": "assistant", "content": []},
        })

    def _ensure_tool_item(self, write_event, tc: Dict[str, Any]) -> str:
        """定位或创建工具项，返回 item id；无法定位时返回空串。

        增量 chunk 中只有首个带 call_id 与工具名，后续增量（续传 arguments）
        仅带 index 或 id 二者之一，须按 call_id / index 精确映射回已建项。
        """
        cid = tc.get("id") or ""
        idx = tc.get("index")
        if cid and cid in self.tool_by_call:
            return self.tool_by_call[cid]
        if isinstance(idx, int) and idx in self.tool_by_pos:
            item_id = self.tool_by_pos[idx]
            # 首 chunk 缺 id 的少见场景：补记映射
            if cid:
                self.tool_by_call[cid] = item_id
                self.tool_call_ids[item_id] = cid
            return item_id
        if not cid:
            return ""
        # 新工具项：先关掉进行中的 message 项，工具项排在其后
        self._close_message_item(write_event)
        item_id = "fc_" + new_rand_id()
        pos = self.output_index
        self.output_index += 1
        fn = tc.get("function") if isinstance(tc.get("function"), dict) else {}
        self.tool_items[item_id] = pos
        self.tool_call_ids[item_id] = cid
        self.tool_names[item_id] = fn.get("name") or ""
        if isinstance(idx, int):
            self.tool_by_pos[idx] = item_id
        self.tool_by_call[cid] = item_id
        self.tool_args[item_id] = ""
        write_event("response.output_item.added", {
            "type": "response.output_item.added",
            "output_index": pos,
            "item": {"type": "function_call", "id": item_id, "call_id": cid,
                      "name": fn.get("name") or "", "arguments": "",
                      "status": "in_progress"},
        })
        return item_id

    def _close_message_item(self, write_event) -> None:
        """关闭当前 message 项（补 output_text.done / content_part.done / output_item.done）。"""
        if not self.msg_item_id:
            return
        full_text = "".join(self.text_parts)
        idx = self.output_index
        if full_text:
            write_event("response.output_text.done", {
                "type": "response.output_text.done",
                "item_id": self.msg_item_id, "output_index": idx,
                "content_index": 0, "text": full_text,
            })
            write_event("response.content_part.done", {
                "type": "response.content_part.done",
                "item_id": self.msg_item_id, "output_index": idx,
                "content_index": 0,
                "part": {"type": "output_text", "text": full_text, "annotations": []},
            })
        self.output_index += 1
        write_event("response.output_item.done", {
            "type": "response.output_item.done",
            "output_index": idx,
            "item": {"type": "message", "id": self.msg_item_id, "status": "completed",
                      "role": "assistant",
                      "content": [{"type": "output_text", "text": full_text,
                                    "annotations": []}]},
        })
        # 记录已完成项（response.completed 回填 output 用）
        self.closed_msg = {"type": "message", "id": self.msg_item_id,
                           "status": "completed", "role": "assistant",
                           "content": [{"type": "output_text", "text": full_text,
                                         "annotations": []}]}
        self.msg_item_id = ""
        self.text_parts = []

    def finish_stream(self, write_event) -> None:
        """上游结束，补完 Responses 终止事件序列。"""
        self._close_message_item(write_event)
        done_items: List[Any] = []
        for item_id, idx in sorted(self.tool_items.items(), key=lambda kv: kv[1]):
            args = self.tool_args.get(item_id, "")
            done_item = {
                "type": "function_call", "id": item_id,
                "call_id": self.tool_call_ids.get(item_id, ""),
                "name": self.tool_names.get(item_id, ""),
                "arguments": args, "status": "completed",
            }
            done_items.append(done_item)
            if args:
                write_event("response.function_call_arguments.done", {
                    "type": "response.function_call_arguments.done",
                    "item_id": item_id, "output_index": idx, "arguments": args,
                })
            write_event("response.output_item.done", {
                "type": "response.output_item.done",
                "output_index": idx,
                "item": done_item,
            })
        status = "incomplete" if self.finish == "length" else "completed"
        resp = self._response_base()
        resp["status"] = status
        # 回填本回合全部输出项（message + function_call），codex 依赖此字段取工具调用
        resp["output"] = self.final_output_items()
        resp["usage"] = {
            "input_tokens": self.input_tokens,
            "input_tokens_details": {"cached_tokens": 0},
            "output_tokens": self.output_tokens,
            "output_tokens_details": {"reasoning_tokens": 0},
            "total_tokens": self.input_tokens + self.output_tokens,
        }
        write_event("response.completed", {"type": "response.completed", "response": resp})

    def final_output_items(self) -> List[Any]:
        """汇总本回合全部输出项（供 response.completed 与非流式响应共用）。"""
        items: List[Any] = []
        # message 项：已关闭用快照；未关闭（上游未触发 close，如 [DONE] 前）
        if self.msg_item_id:
            full_text = "".join(self.text_parts)
            items.append({
                "type": "message", "id": self.msg_item_id,
                "status": "completed", "role": "assistant",
                "content": [{"type": "output_text", "text": full_text,
                              "annotations": []}],
            })
        elif getattr(self, "closed_msg", None):
            items.append(self.closed_msg)
        for item_id, _idx in sorted(self.tool_items.items(), key=lambda kv: kv[1]):
            items.append({
                "type": "function_call", "id": item_id,
                "call_id": self.tool_call_ids.get(item_id, ""),
                "name": self.tool_names.get(item_id, ""),
                "arguments": self.tool_args.get(item_id, ""),
                "status": "completed",
            })
        return items

    def _response_base(self) -> Dict[str, Any]:
        return {
            "id": self.response_id,
            "object": "response",
            "created_at": self.created_at,
            "status": "in_progress",
            "background": False,
            "model": self.model,
            "output": [],
            "parallel_tool_calls": True,
            "store": False,
            "tool_choice": "auto",
            "tools": [],
            "truncation": "disabled",
            "metadata": {},
        }
