// 转发器：手动构造上游 *http.Request（非 httputil.ReverseProxy），
// 头部白名单透传 + 鉴权注入 + body 模型改写 + SSE 管道式透传。
// 对齐 sub2api 的 gateway_forward / gateway_anthropic_passthrough 模式。
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// versionSuffixRe 匹配以 /v<数字> 结尾的 base_url（如 /v1、/v3）。
var versionSuffixRe = regexp.MustCompile(`/v\d+$`)

// UpstreamError 表示上游返回了非 2xx 状态码（已读取错误体，响应未提交，可 fallback）。
type UpstreamError struct {
	Status int
	Body   string
}

func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream %d: %s", e.Status, e.Body)
}

// Forwarder 持有共享的 *http.Client（流式安全：Timeout=0，由请求级 context 控制截止）。
type Forwarder struct {
	client *http.Client
	logger *CallLogger
}

func NewForwarder(logger *CallLogger) *Forwarder {
	return &Forwarder{client: &http.Client{Timeout: 0}, logger: logger}
}

// allowedHeaders 是从客户端透传到上游的头部白名单（小写），其余一律丢弃。
// 刻意剥离 authorization / x-api-key / cookie 等，避免泄露客户端凭据。
var allowedHeaders = map[string]bool{
	"accept":                true,
	"accept-language":       true,
	"accept-encoding":       true,
	"user-agent":            true,
	"openai-organization":   true,
	"openai-project":        true,
	"anthropic-version":     true,
	"anthropic-beta":        true,
	"x-request-id":          true,
	"idempotency-key":       true,
}

// Forward 把请求体转发到指定上游模型。
// streamRequested=true 时做 SSE 管道透传；否则原样透传非流式响应。
// 返回 ForwardResult 与 error：
//   - 非 2xx：返回 *UpstreamError，BodyCommitted=false，调用方可据状态码决定重试/fallback
//   - 传输错误：返回普通 error，BodyCommitted=false
//   - 已开始写响应体后出错：BodyCommitted=true，不可再 fallback
func (f *Forwarder) Forward(ctx context.Context, c *gin.Context, body []byte, m *Model, streamRequested bool) (*ForwardResult, error) {
	ctx, cancel := context.WithTimeout(ctx, m.Timeout)
	defer cancel()

	upBody, err := rewriteModel(body, m.Cfg.Upstream.Model)
	if err != nil {
		return &ForwardResult{}, fmt.Errorf("rewrite model: %w", err)
	}
	// 记录转发到上游的请求体（已做模型改写）
	if f.logger != nil {
		if val, exists := c.Get("call_entry"); exists {
			if e, ok := val.(*CallEntry); ok {
				f.logger.SetUpstreamRequest(e, upBody)
			}
		}
	}
	url := buildURL(m.Cfg.Upstream.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(upBody))
	if err != nil {
		return &ForwardResult{}, err
	}
	// 头部白名单透传
	for h, vals := range c.Request.Header {
		if allowedHeaders[strings.ToLower(h)] {
			for _, v := range vals {
				req.Header.Add(h, v)
			}
		}
	}
	req.Header.Set("Content-Type", "application/json")
	setAuth(req, m)

	start := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return &ForwardResult{DurationMs: ms(time.Since(start))}, err
	}

	// 非 2xx：读取错误体后返回，不提交响应体（可 fallback）
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 8*1024))
		resp.Body.Close()
		return &ForwardResult{Status: resp.StatusCode, DurationMs: ms(time.Since(start))},
			&UpstreamError{Status: resp.StatusCode, Body: string(errBody)}
	}

	res := &ForwardResult{Status: resp.StatusCode, UpstreamModel: m.Cfg.Upstream.Model, Stream: streamRequested}
	begin := time.Now()
	format := c.GetString("outbound_format")
	switch {
	case streamRequested && format == "claude":
		res.FirstTokenMs, err = pipeSSEClaude(ctx, c, resp, m, begin)
	case streamRequested:
		res.FirstTokenMs, err = f.pipeSSE(ctx, c, resp, m, begin)
	case format == "claude":
		err = f.pipeNonStreamClaude(c, resp, m)
	default:
		err = f.pipeNonStream(c, resp)
	}
	res.BodyCommitted = true
	res.DurationMs = ms(time.Since(start))
	return res, err
}

// buildURL 规整 base_url：已含 /chat/completions 则原样，否则追加 /chat/completions（如已有 /v1 则只追加 /chat/completions）。
func buildURL(baseURL string) string {
	b := strings.TrimRight(baseURL, "/")
	if strings.HasSuffix(b, "/chat/completions") {
		return b
	}
	// 已以版本段结尾（如 /v1、/v3）时只追加 /chat/completions，避免重复 /v1。
	if versionSuffixRe.MatchString(b) {
		return b + "/chat/completions"
	}
	return b + "/v1/chat/completions"
}

// setAuth 按配置注入鉴权头（Authorization: Bearer / x-api-key / x-goog-api-key / 自定义）。
func setAuth(req *http.Request, m *Model) {
	h := strings.ToLower(strings.TrimSpace(m.Cfg.Upstream.AuthHeader))
	switch h {
	case "", "authorization", "bearer":
		req.Header.Set("Authorization", "Bearer "+m.Cfg.Upstream.APIKey)
	case "x-api-key":
		req.Header.Set("x-api-key", m.Cfg.Upstream.APIKey)
	case "x-goog-api-key":
		req.Header.Set("x-goog-api-key", m.Cfg.Upstream.APIKey)
	default:
		req.Header.Set(m.Cfg.Upstream.AuthHeader, m.Cfg.Upstream.APIKey)
	}
}

// rewriteModel 用上游真实模型名改写 body 中的 "model" 字段（模型映射）。
// 使用 map[string]json.RawMessage 保留其余字段原样。
func rewriteModel(body []byte, newModel string) ([]byte, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		obj = map[string]json.RawMessage{}
	}
	b, err := json.Marshal(newModel)
	if err != nil {
		return nil, err
	}
	obj["model"] = b
	return json.Marshal(obj)
}

// pipeSSE 做 SSE 管道式透传：逐行读上游、立即 Flush、空闲超时、keepalive ping。
func (f *Forwarder) pipeSSE(ctx context.Context, c *gin.Context, resp *http.Response, m *Model, begin time.Time) (int64, error) {
	defer resp.Body.Close()
	w := c.Writer
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // 禁用 nginx 缓冲
	flusher, ok := w.(http.Flusher)
	if !ok {
		return 0, fmt.Errorf("server writer 不支持 streaming")
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	lines := make(chan string, 16)
	errCh := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 64*1024*1024) // 单行最大 64MB
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

	for {
		select {
		case line, ok := <-lines:
			if !ok {
				return firstTokenMs, <-errCh
			}
			if !started {
				firstTokenMs = ms(time.Since(begin))
				started = true
			}
			if _, err := fmt.Fprintln(w, line); err != nil {
				return firstTokenMs, nil // 客户端已断开
			}
			flusher.Flush()
			idle.Reset(m.IdleTimeout)
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return firstTokenMs, nil
			}
			flusher.Flush()
		case <-idle.C:
			return firstTokenMs, fmt.Errorf("stream idle timeout after %s", m.IdleTimeout)
		case <-ctx.Done():
			return firstTokenMs, ctx.Err()
		case <-c.Request.Context().Done():
			return firstTokenMs, nil // 客户端断开
		}
	}
}

// pipeNonStream 透传非流式响应（2xx），尽量保留上游状态码与关键响应头。
func (f *Forwarder) pipeNonStream(c *gin.Context, resp *http.Response) error {
	defer resp.Body.Close()
	w := c.Writer
	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err := io.Copy(w, resp.Body)
	return err
}

// pipeNonStreamClaude 读上游 OpenAI 非流式响应，转成 Claude message JSON 后写回客户端。
func (f *Forwarder) pipeNonStreamClaude(c *gin.Context, resp *http.Response, m *Model) error {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
	if err != nil {
		return err
	}
	out, err := openaiResponseToClaude(body, m.Cfg.Upstream.Model)
	if err != nil {
		// 转换失败：原样回退，避免完全无响应
		copyResponseHeaders(c.Writer.Header(), resp.Header)
		c.Writer.WriteHeader(resp.StatusCode)
		_, _ = c.Writer.Write(body)
		return err
	}
	c.Writer.Header().Set("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, err = c.Writer.Write(out)
	return err
}

func copyResponseHeaders(dst, src http.Header) {
	for k, vals := range src {
		lk := strings.ToLower(k)
		if lk == "content-length" || lk == "transfer-encoding" || lk == "connection" {
			continue
		}
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}

func ms(d time.Duration) int64 { return d.Milliseconds() }
