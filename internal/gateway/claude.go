// Claude（Anthropic Messages API）兼容层：
//   入站：客户端按 Claude 格式 POST /v1/messages，请求体转 OpenAI 格式后走上游
//   出站：上游 OpenAI 响应（非流式 JSON / 流式 SSE）转回 Claude 格式回给客户端
// 上游统一为 OpenAI 兼容端点（forwarder 始终打 /v1/chat/completions）。
package gateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// ---- 请求转换：Claude → OpenAI ----

// claudeRequestToOpenAI 把 Claude Messages 请求体转成 OpenAI Chat 请求体，
// 并返回客户端填写的 model（=链名）。转换保留未知字段不丢。
func claudeRequestToOpenAI(body []byte) ([]byte, string, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, "", err
	}
	if src == nil {
		src = map[string]interface{}{}
	}
	dst := map[string]interface{}{}

	// model（链名）
	model, _ := src["model"].(string)
	if model != "" {
		dst["model"] = model
	}

	// 系统提示：Claude 顶层 system（string 或 array）→ OpenAI system 消息
	if sys, ok := src["system"]; ok {
		sysText := flattenSystem(sys)
		if sysText != "" {
			msgs, _ := src["messages"].([]interface{})
			msgs = append([]interface{}{
				map[string]interface{}{"role": "system", "content": sysText},
			}, msgs...)
			dst["messages"] = msgs
		} else {
			dst["messages"] = src["messages"]
		}
	} else {
		dst["messages"] = src["messages"]
	}

	// 转换消息内容块（Claude content blocks → OpenAI content blocks）
	if msgs, ok := dst["messages"].([]interface{}); ok {
		dst["messages"] = convertMessages(msgs)
	}

	// 参数映射
	copyStr(src, dst, "stream")
	copyNum(src, dst, "max_tokens")
	copyNum(src, dst, "temperature")
	copyNum(src, dst, "top_p")
	copyNum(src, dst, "top_k") // OpenAI 部分模型支持
	if ss, ok := src["stop_sequences"].([]interface{}); ok && len(ss) > 0 {
		dst["stop"] = ss
	}
	// 透传其余常见字段
	copyBool(src, dst, "stream")
	if v, ok := src["stream"].(bool); ok {
		dst["stream"] = v
	}

	// 工具与工具选择：Claude 与 OpenAI 结构不同，尽力转换
	if tools, ok := src["tools"].([]interface{}); ok {
		dst["tools"] = convertToolsToOpenAI(tools)
	}
	if tc, ok := src["tool_choice"]; ok {
		dst["tool_choice"] = convertToolChoiceToOpenAI(tc)
	}

	out, err := json.Marshal(dst)
	if err != nil {
		return nil, "", err
	}
	return out, model, nil
}

// flattenSystem 把 Claude system（string | [{type:text,text:...}]）拍平为字符串。
func flattenSystem(sys interface{}) string {
	switch v := sys.(type) {
	case string:
		return v
	case []interface{}:
		var parts []string
		for _, b := range v {
			if m, ok := b.(map[string]interface{}); ok {
				if t, _ := m["type"].(string); t == "text" {
					if s, _ := m["text"].(string); s != "" {
						parts = append(parts, s)
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// convertMessages 转换消息列表：处理 Claude 的 tool_use/tool_result/image 等内容块。
func convertMessages(msgs []interface{}) []interface{} {
	out := make([]interface{}, 0, len(msgs))
	for _, raw := range msgs {
		m, ok := raw.(map[string]interface{})
		if !ok {
			out = append(out, raw)
			continue
		}
		role, _ := m["role"].(string)
		content := m["content"]

		// tool_result 块必须拆成独立的 role=tool 消息
		switch role {
		case "user":
			if blocks, ok := content.([]interface{}); ok {
				toolResults, rest := splitToolResults(blocks)
				for _, tr := range toolResults {
					out = append(out, tr)
				}
				if len(rest) > 0 {
					m["content"] = convertContentBlocks(rest)
					out = append(out, m)
				}
				continue
			}
		case "assistant":
			if blocks, ok := content.([]interface{}); ok {
				converted, toolCalls := convertAssistantBlocks(blocks)
				m["content"] = converted
				if len(toolCalls) > 0 {
					m["tool_calls"] = toolCalls
				}
				out = append(out, m)
				continue
			}
		}
		// 字符串 content 或未知结构：尽力保留
		if _, ok := content.(string); ok {
			// 原样
		} else if blocks, ok := content.([]interface{}); ok {
			m["content"] = convertContentBlocks(blocks)
		}
		out = append(out, m)
	}
	return out
}

// splitToolResults 把 tool_result 块拆成 OpenAI role=tool 消息，其余返回原样。
func splitToolResults(blocks []interface{}) (toolMsgs []interface{}, rest []interface{}) {
	for _, b := range blocks {
		m, ok := b.(map[string]interface{})
		if !ok {
			rest = append(rest, b)
			continue
		}
		if t, _ := m["type"].(string); t == "tool_result" {
			id, _ := m["tool_use_id"].(string)
			content := extractToolResultContent(m["content"])
			toolMsgs = append(toolMsgs, map[string]interface{}{
				"role":         "tool",
				"tool_call_id": id,
				"content":      content,
			})
			continue
		}
		rest = append(rest, b)
	}
	return
}

func extractToolResultContent(c interface{}) string {
	switch v := c.(type) {
	case string:
		return v
	case []interface{}:
		var parts []string
		for _, b := range v {
			if m, ok := b.(map[string]interface{}); ok {
				if t, _ := m["type"].(string); t == "text" {
					if s, _ := m["text"].(string); s != "" {
						parts = append(parts, s)
					}
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// convertAssistantBlocks 转换 assistant 内容块：text 保留，tool_use → tool_calls。
func convertAssistantBlocks(blocks []interface{}) (converted interface{}, toolCalls []interface{}) {
	var textParts []string
	hasTextBlock := false
	for _, b := range blocks {
		m, ok := b.(map[string]interface{})
		if !ok {
			continue
		}
		t, _ := m["type"].(string)
		switch t {
		case "text":
			hasTextBlock = true
			if s, _ := m["text"].(string); s != "" {
				textParts = append(textParts, s)
			}
		case "tool_use":
			id, _ := m["id"].(string)
			name, _ := m["name"].(string)
			input := m["input"]
			args, _ := json.Marshal(input)
			toolCalls = append(toolCalls, map[string]interface{}{
				"id":   id,
				"type": "function",
				"function": map[string]interface{}{
					"name":      name,
					"arguments": string(args),
				},
			})
		}
	}
	if hasTextBlock {
		converted = strings.Join(textParts, "")
	} else if len(toolCalls) > 0 {
		converted = nil // OpenAI assistant 带 tool_calls 时 content 可为 null
	} else {
		converted = blocks
	}
	return
}

// convertContentBlocks 把 Claude 内容块转成 OpenAI 内容块（text/image）。
func convertContentBlocks(blocks []interface{}) []interface{} {
	out := make([]interface{}, 0, len(blocks))
	for _, b := range blocks {
		m, ok := b.(map[string]interface{})
		if !ok {
			out = append(out, b)
			continue
		}
		t, _ := m["type"].(string)
		switch t {
		case "text":
			out = append(out, map[string]interface{}{
				"type": "text",
				"text": m["text"],
			})
		case "image":
			if url := claudeImageToOpenAIURL(m["source"]); url != "" {
				out = append(out, map[string]interface{}{
					"type":      "image_url",
					"image_url": map[string]interface{}{"url": url},
				})
			}
		default:
			// 未知块原样透传（尽力兼容）
			out = append(out, b)
		}
	}
	return out
}

// claudeImageToOpenAIURL 把 Claude image source 转成 OpenAI image_url.url。
func claudeImageToOpenAIURL(src interface{}) string {
	m, ok := src.(map[string]interface{})
	if !ok {
		return ""
	}
	st, _ := m["type"].(string)
	switch st {
	case "url":
		u, _ := m["url"].(string)
		return u
	case "base64":
		media, _ := m["media_type"].(string)
		data, _ := m["data"].(string)
		if media == "" {
			media = "image/png"
		}
		return "data:" + media + ";base64," + data
	}
	return ""
}

// convertToolsToOpenAI 把 Claude tools 转成 OpenAI function tools。
func convertToolsToOpenAI(tools []interface{}) []interface{} {
	out := make([]interface{}, 0, len(tools))
	for _, t := range tools {
		m, ok := t.(map[string]interface{})
		if !ok {
			out = append(out, t)
			continue
		}
		name, _ := m["name"].(string)
		desc, _ := m["description"].(string)
		schema := m["input_schema"]
		out = append(out, map[string]interface{}{
			"type": "function",
			"function": map[string]interface{}{
				"name":        name,
				"description": desc,
				"parameters":  schema,
			},
		})
	}
	return out
}

// convertToolChoiceToOpenAI 把 Claude tool_choice 转成 OpenAI tool_choice。
func convertToolChoiceToOpenAI(tc interface{}) interface{} {
	m, ok := tc.(map[string]interface{})
	if !ok {
		// "any" / "auto" / "none" 字符串
		return tc
	}
	t, _ := m["type"].(string)
	switch t {
	case "auto":
		return "auto"
	case "any":
		return "required"
	case "tool":
		name, _ := m["name"].(string)
		return map[string]interface{}{
			"type": "function",
			"function": map[string]interface{}{"name": name},
		}
	}
	return tc
}

func copyStr(src map[string]interface{}, dst map[string]interface{}, key string) {
	if v, ok := src[key]; ok {
		dst[key] = v
	}
}
func copyNum(src map[string]interface{}, dst map[string]interface{}, key string) {
	if v, ok := src[key]; ok {
		switch v.(type) {
		case float64, int, int64:
			dst[key] = v
		}
	}
}
func copyBool(src map[string]interface{}, dst map[string]interface{}, key string) {
	if v, ok := src[key].(bool); ok {
		dst[key] = v
	}
}

// ---- 响应转换：OpenAI → Claude（非流式）----

// openaiResponseToClaude 把 OpenAI chat completion JSON 转成 Claude message JSON。
func openaiResponseToClaude(body []byte, upstreamModel string) ([]byte, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(body, &src); err != nil {
		return nil, err
	}
	id, _ := src["id"].(string)
	if id == "" {
		id = "msg_" + newRandID()
	} else {
		id = "msg_" + strings.TrimPrefix(id, "chatcmpl-")
	}

	// 取首个 choice 的 message.content 与 finish_reason
	content := ""
	finish := "end_turn"
	toolCalls := []interface{}{}
	if choices, ok := src["choices"].([]interface{}); ok && len(choices) > 0 {
		if ch, ok := choices[0].(map[string]interface{}); ok {
			if msg, ok := ch["message"].(map[string]interface{}); ok {
				if c, ok := msg["content"].(string); ok {
					content = c
				}
				if tc, ok := msg["tool_calls"].([]interface{}); ok {
					toolCalls = tc
				}
			}
			if fr, ok := ch["finish_reason"].(string); ok {
				finish = openaiToClaudeStopReason(fr)
			}
		}
	}

	contentBlocks := []interface{}{}
	if content != "" {
		contentBlocks = append(contentBlocks, map[string]interface{}{
			"type": "text",
			"text": content,
			"index": 0,
		})
	}
	// 工具调用 → tool_use 块
	for i, tc := range toolCalls {
		if m, ok := tc.(map[string]interface{}); ok {
			fn, _ := m["function"].(map[string]interface{})
			name, _ := fn["name"].(string)
			argsStr, _ := fn["arguments"].(string)
			var input interface{}
			_ = json.Unmarshal([]byte(argsStr), &input)
			contentBlocks = append(contentBlocks, map[string]interface{}{
				"type":  "tool_use",
				"id":    m["id"],
				"name":  name,
				"input": input,
				"index": i + 1,
			})
		}
	}

	// usage 转换
	inTok, outTok := extractOpenAIUsage(src["usage"])

	resp := map[string]interface{}{
		"id":            id,
		"type":          "message",
		"role":          "assistant",
		"content":       contentBlocks,
		"model":         upstreamModel,
		"stop_reason":   finish,
		"stop_sequence": nil,
		"usage": map[string]interface{}{
			"input_tokens":  inTok,
			"output_tokens": outTok,
		},
	}
	return json.Marshal(resp)
}

// openaiToClaudeStopReason 把 OpenAI finish_reason 映射成 Claude stop_reason。
func openaiToClaudeStopReason(fr string) string {
	switch fr {
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "tool_calls", "function_call":
		return "tool_use"
	case "content_filter":
		return "end_turn"
	case "":
		return "end_turn"
	}
	return fr
}

func extractOpenAIUsage(u interface{}) (in, out int) {
	m, ok := u.(map[string]interface{})
	if !ok {
		return 0, 0
	}
	if v, ok := m["prompt_tokens"].(float64); ok {
		in = int(v)
	}
	if v, ok := m["completion_tokens"].(float64); ok {
		out = int(v)
	}
	return
}

// ---- 响应转换：OpenAI → Claude（流式 SSE）----

// claudeStreamState 维护 OpenAI→Claude 流式转换的跨事件状态。
type claudeStreamState struct {
	messageStartSent bool
	blockStartSent   bool
	model            string
	messageID        string
	outputTokens     int
	inputTokens      int
}

// pipeSSEClaude 读上游 OpenAI SSE，逐块转成 Claude SSE 事件写给客户端。
func pipeSSEClaude(ctx context.Context, c *gin.Context, resp *http.Response, m *Model, begin time.Time) (int64, error) {
	defer resp.Body.Close()
	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, ok := w.(http.Flusher)
	if !ok {
		return 0, fmt.Errorf("server writer 不支持 streaming")
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	st := &claudeStreamState{
		model:     m.Cfg.Upstream.Model,
		messageID: "msg_" + newRandID(),
	}

	lines := make(chan string, 16)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			errCh <- err
		} else {
			errCh <- nil
		}
		close(lines)
	}()

	var firstTokenMs int64
	started := false
	idle := time.NewTimer(m.IdleTimeout)
	defer idle.Stop()
	keepalive := time.NewTicker(m.Keepalive)
	defer keepalive.Stop()

	flush := func() { flusher.Flush() }
	writeEvent := func(eventName string, data interface{}) {
		b, _ := json.Marshal(data)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", eventName, string(b))
	}

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				// 上游结束：补完 Claude 终止序列
				st.finishStream(w, flush)
				return firstTokenMs, <-errCh
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "[DONE]" {
				st.finishStream(w, flush)
				flush()
				return firstTokenMs, nil
			}
			var chunk map[string]interface{}
			if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
				continue
			}
			if !started {
				firstTokenMs = ms(time.Since(begin))
				started = true
			}
			if processed := st.handleChunk(w, chunk, flush, writeEvent); processed {
				idle.Reset(m.IdleTimeout)
			}
		case <-keepalive.C:
			fmt.Fprint(w, ": keepalive\n\n")
			flush()
		case <-idle.C:
			return firstTokenMs, fmt.Errorf("stream idle timeout after %s", m.IdleTimeout)
		case <-ctx.Done():
			return firstTokenMs, ctx.Err()
		case <-c.Request.Context().Done():
			return firstTokenMs, nil
		}
	}
}

// handleChunk 处理单个 OpenAI SSE chunk，转成 Claude 事件写出。返回是否产生了输出。
func (st *claudeStreamState) handleChunk(w io.Writer, chunk map[string]interface{}, flush func(), writeEvent func(string, interface{})) bool {
	choices, _ := chunk["choices"].([]interface{})
	if len(choices) == 0 {
		// 部分上游在最后带 usage（无 choices）
		if u, ok := chunk["usage"].(map[string]interface{}); ok {
			st.collectUsage(u)
		}
		return false
	}
	ch, _ := choices[0].(map[string]interface{})
	delta, _ := ch["delta"].(map[string]interface{})
	if delta == nil {
		delta = map[string]interface{}{}
	}

	// 首个 chunk：发 message_start + content_block_start
	if !st.messageStartSent {
		inTok := 0
		if u, ok := chunk["usage"].(map[string]interface{}); ok {
			inTok, _ = u["prompt_tokens"].(int)
		}
		st.inputTokens = inTok
		st.messageStartSent = true
		writeEvent("message_start", map[string]interface{}{
			"type": "message_start",
			"message": map[string]interface{}{
				"id":            st.messageID,
				"type":          "message",
				"role":          "assistant",
				"content":       []interface{}{},
				"model":         st.model,
				"stop_reason":   nil,
				"stop_sequence": nil,
				"usage": map[string]interface{}{
					"input_tokens":  inTok,
					"output_tokens": 1,
				},
			},
		})
		writeEvent("content_block_start", map[string]interface{}{
			"type":          "content_block_start",
			"index":         0,
			"content_block": map[string]interface{}{"type": "text", "text": ""},
		})
		st.blockStartSent = true
	}

	// 文本增量
	if text, ok := delta["content"].(string); ok && text != "" {
		writeEvent("content_block_delta", map[string]interface{}{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]interface{}{"type": "text_delta", "text": text},
		})
		st.outputTokens++
	}

	// 工具调用增量（简化：整包发 tool_use 块）
	if tcs, ok := delta["tool_calls"].([]interface{}); ok {
		for _, raw := range tcs {
			tc, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			fn, _ := tc["function"].(map[string]interface{})
			name, _ := fn["name"].(string)
			args, _ := fn["arguments"].(string)
			var input interface{}
			_ = json.Unmarshal([]byte(args), &input)
			writeEvent("content_block_start", map[string]interface{}{
				"type":  "content_block_start",
				"index": 1,
				"content_block": map[string]interface{}{
					"type":  "tool_use",
					"id":    tc["id"],
					"name":  name,
					"input": map[string]interface{}{},
				},
			})
			writeEvent("content_block_delta", map[string]interface{}{
				"type":  "content_block_delta",
				"index": 1,
				"delta": map[string]interface{}{
					"type":         "input_json_delta",
					"partial_json": args,
				},
			})
			writeEvent("content_block_stop", map[string]interface{}{
				"type":  "content_block_stop",
				"index": 1,
			})
		}
	}

	// finish_reason → 关闭内容块 + message_delta
	if fr, ok := ch["finish_reason"].(string); ok && fr != "" {
		stop := openaiToClaudeStopReason(fr)
		if u, ok := chunk["usage"].(map[string]interface{}); ok {
			st.collectUsage(u)
		}
		writeEvent("content_block_stop", map[string]interface{}{
			"type":  "content_block_stop",
			"index": 0,
		})
		writeEvent("message_delta", map[string]interface{}{
			"type":  "message_delta",
			"delta": map[string]interface{}{"stop_reason": stop, "stop_sequence": nil},
			"usage": map[string]interface{}{"output_tokens": st.outputTokens},
		})
		st.blockStartSent = false
	}
	flush()
	return true
}

func (st *claudeStreamState) collectUsage(u map[string]interface{}) {
	if v, ok := u["prompt_tokens"].(float64); ok {
		st.inputTokens = int(v)
	}
	if v, ok := u["completion_tokens"].(float64); ok {
		st.outputTokens = int(v)
	}
}

// finishStream 在上游结束但未显式发 finish_reason 时补完 Claude 终止序列。
func (st *claudeStreamState) finishStream(w io.Writer, flush func()) {
	if st.blockStartSent {
		fmt.Fprintf(w, "event: content_block_stop\ndata: %s\n\n", `{"type":"content_block_stop","index":0}`)
	}
	fmt.Fprintf(w, "event: message_delta\ndata: %s\n\n", fmt.Sprintf(
		`{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":%d}}`,
		st.outputTokens,
	))
	fmt.Fprintf(w, "event: message_stop\ndata: %s\n\n", `{"type":"message_stop"}`)
	flush()
}

// newRandID 生成 12 字节十六进制 ID。
func newRandID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
