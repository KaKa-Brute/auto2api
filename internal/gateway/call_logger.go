// 调用日志：记录完整调用过程与输入输出，按日期落盘为 JSONL。
// 文件名形如 logs/calls-2026-07-20.log，跨天自动轮转。
package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// CallLogger 是按日期轮转的调用日志记录器。
type CallLogger struct {
	mu        sync.Mutex
	enabled   bool
	dir       string
	redact    bool
	bodyLimit int
	upstream  bool
	file      *os.File
	fileDate  string
}

// NewCallLogger 构造一个调用日志记录器。
func NewCallLogger(dir string, enabled, redact, upstream bool, bodyLimit int) *CallLogger {
	return &CallLogger{
		enabled:   enabled,
		dir:       dir,
		redact:    redact,
		bodyLimit: bodyLimit,
		upstream:  upstream,
	}
}

func (l *CallLogger) Enabled() bool { return l.enabled }

// redactHeaders 脱敏鉴权相关头部，其余头部原样保留。
func (l *CallLogger) redactHeaders(h http.Header) map[string]string {
	out := make(map[string]string, len(h))
	for k, v := range h {
		lk := strings.ToLower(k)
		val := strings.Join(v, ", ")
		if l.redact && (lk == "authorization" || lk == "x-api-key" || lk == "x-goog-api-key") {
			val = redactValue(val)
		}
		out[lk] = val
	}
	return out
}

// redactValue 保留前 8 字符 + "***"。
func redactValue(s string) string {
	s = strings.TrimSpace(s)
	const prefix = "Bearer "
	if strings.HasPrefix(s, prefix) {
		s = strings.TrimPrefix(s, prefix)
	}
	if len(s) <= 8 {
		return s + "***"
	}
	return s[:8] + "***"
}

// CallEntry 是单次请求的完整调用日志，随请求生命周期累积，结束时落盘。
type CallEntry struct {
	ID         string        `json:"id"`
	Timestamp  string        `json:"timestamp"`
	Method     string        `json:"method"`
	Path       string        `json:"path"`
	ClientIP   string        `json:"client_ip"`
	Chain      string        `json:"chain,omitempty"`
	Format     string        `json:"format"` // openai / claude
	Stream     bool          `json:"stream"`
	AuthKey    string        `json:"auth_key,omitempty"` // 已脱敏
	Headers    map[string]string `json:"headers,omitempty"`
	ReqBody    string        `json:"req_body,omitempty"`
	UpstreamReqBody string   `json:"upstream_req_body,omitempty"`
	Attempts   []AttemptLog `json:"attempts,omitempty"`
	RespStatus int           `json:"resp_status"`
	RespBody   string        `json:"resp_body,omitempty"`
	RespChunks int           `json:"resp_chunks,omitempty"` // 流式 SSE 数据块数
	DurationMs int64         `json:"duration_ms"`
	Error      string        `json:"error,omitempty"`

	start    time.Time
	rec      *responseRecorder
}

// AttemptLog 描述单模型一次尝试的结局。
type AttemptLog struct {
	Model      string `json:"model"`
	Priority   int    `json:"priority"`
	Upstream   string `json:"upstream_model,omitempty"`
	Status     int    `json:"status"`
	Outcome    string `json:"outcome"` // success / failover / client_error / error
	Attempt    int    `json:"attempt,omitempty"`
	DurationMs int64  `json:"duration_ms"`
	FirstTokenMs int64 `json:"first_token_ms,omitempty"`
	Error      string `json:"error,omitempty"`
}

// Begin 创建一次调用的日志条目，并把 c.Writer 包裹为录制器，
// 以捕获最终写给客户端的响应体（含已做格式转换后的输出）。
func (l *CallLogger) Begin(c *gin.Context, format string) *CallEntry {
	if !l.enabled {
		return nil
	}
	id := newID()
	rec := &responseRecorder{
		ResponseWriter: c.Writer,
		bodyLimit:      l.bodyLimit,
	}
	c.Writer = rec
	e := &CallEntry{
		ID:        id,
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Method:    c.Request.Method,
		Path:      c.Request.URL.Path,
		ClientIP:  c.ClientIP(),
		Format:    format,
		start:     time.Now(),
		Headers:   l.redactHeaders(c.Request.Header),
		rec:       rec,
	}
	c.Set("call_entry", e)
	return e
}

// SetRequest 记录原始请求体（已截断）。
func (l *CallLogger) SetRequest(e *CallEntry, body []byte, chain string, stream bool) {
	if e == nil {
		return
	}
	e.ReqBody = truncate(string(body), l.bodyLimit)
	e.Chain = chain
	e.Stream = stream
}

// SetUpstreamRequest 记录转发到上游的请求体（已截断）。
func (l *CallLogger) SetUpstreamRequest(e *CallEntry, body []byte) {
	if e == nil || !l.upstream {
		return
	}
	e.UpstreamReqBody = truncate(string(body), l.bodyLimit)
}

// AddAttempt 追加一次模型尝试记录。
func (l *CallLogger) AddAttempt(e *CallEntry, a AttemptLog) {
	if e == nil {
		return
	}
	e.Attempts = append(e.Attempts, a)
}

// End 结束本次调用日志，落盘。status 为最终响应状态，err 为最终错误（若有）。
func (l *CallLogger) End(e *CallEntry, status int, err error) {
	if e == nil || !l.enabled {
		return
	}
	e.RespStatus = status
	e.DurationMs = time.Since(e.start).Milliseconds()
	if e.rec != nil {
		e.RespBody = truncate(e.rec.buf.String(), l.bodyLimit)
		e.RespChunks = e.rec.chunkCount
	}
	if err != nil {
		e.Error = err.Error()
	}
	l.write(e)
}

func (l *CallLogger) write(e *CallEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rotateFile() != nil {
		return
	}
	b, _ := json.Marshal(e)
	l.file.Write(b)
	l.file.Write([]byte("\n"))
}

// rotateFile 按当天日期打开/切换日志文件（调用前已持锁）。
func (l *CallLogger) rotateFile() error {
	today := time.Now().Format("2006-01-02")
	if l.file != nil && today == l.fileDate {
		return nil
	}
	if l.file != nil {
		l.file.Close()
	}
	if err := os.MkdirAll(l.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(l.dir, "calls-"+today+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.file = f
	l.fileDate = today
	return nil
}

func truncate(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[:limit] + "...[truncated]"
}

// newID 生成 8 字节十六进制请求 ID。
func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "req_" + hex.EncodeToString(b[:])
}

// responseRecorder 包裹 gin.ResponseWriter，捕获写给客户端的字节，
// 用于在请求结束时把响应体（含格式转换后的输出）记入调用日志。
type responseRecorder struct {
	gin.ResponseWriter
	buf        bytes.Buffer
	bodyLimit int
	chunkCount int
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	// SSE 数据块计数（OpenAI 与 Claude SSE 均以 "data:" 开头）
	if len(b) >= 5 && b[0] == 'd' && string(b[:5]) == "data:" {
		r.chunkCount++
	}
	// 仅在限额内缓存（响应体可能很大）
	if r.buf.Len() < r.bodyLimit {
		remaining := r.bodyLimit - r.buf.Len()
		if len(b) < remaining {
			r.buf.Write(b)
		} else {
			r.buf.Write(b[:remaining])
		}
	}
	return r.ResponseWriter.Write(b)
}

func (r *responseRecorder) WriteString(s string) (int, error) {
	return r.Write([]byte(s))
}
