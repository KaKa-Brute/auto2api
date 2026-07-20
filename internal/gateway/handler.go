// Gin 路由与 fallback 编排：两级机制——
//   层 1 同模型退避重试（retryable_status，对齐 sub2api shouldRetryUpstreamError）
//   层 2 跨优先级模型故障转移（trigger_status，对齐 sub2api shouldFailoverUpstreamError / hermes fallback）
package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// Handler 是所有 HTTP 路由的入口。
type Handler struct {
	scheduler   *Scheduler
	forwarder   *Forwarder
	maxSwitches int
	apiKeys     map[string]bool // 已授权 key 集合，为空则不鉴权
}

func NewHandler(s *Scheduler, f *Forwarder, maxSwitches int, apiKeys []string) *Handler {
	h := &Handler{scheduler: s, forwarder: f, maxSwitches: maxSwitches}
	if len(apiKeys) > 0 {
		h.apiKeys = make(map[string]bool, len(apiKeys))
		for _, k := range apiKeys {
			if k != "" {
				h.apiKeys[k] = true
			}
		}
	}
	return h
}

// Register 把路由挂到 gin 引擎上（OpenAI 兼容）。
func (h *Handler) Register(r *gin.Engine) {
	auth := h.authMiddleware()
	r.POST("/v1/chat/completions", auth, h.ChatCompletions)
	r.POST("/chat/completions", auth, h.ChatCompletions) // 无 /v1 前缀的客户端兼容
	r.GET("/v1/models", auth, h.Models)
	r.GET("/v1/health", h.Health) // 健康检查不鉴权，方便监控探活
}

// authMiddleware 校验客户端 Authorization: Bearer <key>。
// 未配置 api_keys 时放行（向后兼容）。
func (h *Handler) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if len(h.apiKeys) == 0 {
			c.Next()
			return
		}
		key := extractBearerKey(c.GetHeader("Authorization"))
		if key == "" {
			key = c.GetHeader("x-api-key")
		}
		if key == "" {
			key = c.GetHeader("x-goog-api-key")
		}
		if key == "" || !h.apiKeys[key] {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"message": "invalid or missing API key",
					"type":    "authentication_error",
				},
			})
			return
		}
		c.Next()
	}
}

// extractBearerKey 从 "Bearer <key>" 提取 key。
func extractBearerKey(authHeader string) string {
	const prefix = "Bearer "
	if len(authHeader) <= len(prefix) {
		return ""
	}
	if authHeader[:len(prefix)] != prefix {
		return authHeader // 允许裸 key（部分客户端直接填 key）
	}
	return authHeader[len(prefix):]
}

// attemptOutcome 描述单模型尝试（含其重试）的结局。
type attemptOutcome int

const (
	outcomeSuccess     attemptOutcome = iota // 响应已成功写完
	outcomeFailover                          // 应切换到下一优先级模型
	outcomeClientError                       // 上游返回非重试/非转移状态，已原样回给客户端
)

// ChatCompletions 处理 POST /v1/chat/completions：按 model 字段选链，按优先级 fallback。
func (h *Handler) ChatCompletions(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", "read body: "+err.Error())
		return
	}
	if !json.Valid(body) {
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", "request body is not valid JSON")
		return
	}

	chainName := extractString(body, "model")
	if chainName == "" {
		chainName = "auto"
	}
	chain := h.scheduler.GetChain(chainName)
	if chain == nil {
		writeJSONError(c, http.StatusNotFound, "invalid_request_error", "unknown model/chain: "+chainName)
		return
	}
	streamRequested := extractBool(body, "stream")

	excluded := map[string]bool{}
	var lastErr error
	for switches := 0; ; switches++ {
		m := h.scheduler.PickNext(chain, excluded)
		if m == nil {
			msg := "all models exhausted"
			if lastErr != nil {
				msg = lastErr.Error()
			}
			writeJSONError(c, http.StatusBadGateway, "upstream_unavailable", msg)
			return
		}

		outcome, err := h.tryModel(c, body, m, streamRequested)
		if err != nil {
			lastErr = err
		}
		switch outcome {
		case outcomeSuccess, outcomeClientError:
			return
		case outcomeFailover:
			// 层 2：跨优先级模型故障转移
			h.scheduler.MarkCooldown(m)
			excluded[m.Cfg.Name] = true
			if switches+1 >= h.maxSwitches {
				msg := "max model switches reached"
				if lastErr != nil {
					msg = lastErr.Error()
				}
				writeJSONError(c, http.StatusBadGateway, "upstream_unavailable", msg)
				return
			}
		}
	}
}

// tryModel 在单个模型上执行层 1（同模型退避重试）。
// 状态码处理顺序：retryable 且仍有重试次数 → 退避重试；否则 failover 触发 → 转移；
// 否则视为客户端错误原样返回。这样 429（retryable+failover）会先重试、耗尽后再转移，
// 401/403/500（仅 failover）则立即转移。
func (h *Handler) tryModel(c *gin.Context, body []byte, m *Model, streamRequested bool) (attemptOutcome, error) {
	var ue *UpstreamError
	for attempt := 0; attempt <= m.RetryCount; attempt++ {
		res, ferr := h.forwarder.Forward(c.Request.Context(), c, body, m, streamRequested)
		if ferr == nil {
			return outcomeSuccess, nil
		}
		// 响应体已提交（流式中途失败）——无法 fallback，结束本次请求
		if res != nil && res.BodyCommitted {
			return outcomeSuccess, nil
		}
		ue, _ = ferr.(*UpstreamError)
		if ue == nil {
			// 传输错误（DNS/连接拒绝/超时）——切下一优先级
			return outcomeFailover, ferr
		}
		switch {
		case m.Retryable[ue.Status] && attempt < m.RetryCount:
			h.sleep(c, m.Backoffs, attempt)
			continue
		case m.Failover[ue.Status]:
			return outcomeFailover, ue
		default:
			// 非重试/非转移（如 400/404）——原样回给客户端
			writeUpstreamError(c, ue)
			return outcomeClientError, ue
		}
	}
	// 循环结束：最后一次是 retryable 但不在 failover 触发集中（重试耗尽）——把最后一次错误回给客户端
	if ue != nil {
		writeUpstreamError(c, ue)
		return outcomeClientError, ue
	}
	return outcomeFailover, nil
}

// sleep 在退避时睡指定时长，客户端断开则提前返回。
func (h *Handler) sleep(c *gin.Context, backoffs []time.Duration, attempt int) {
	var d time.Duration
	switch {
	case attempt < len(backoffs):
		d = backoffs[attempt]
	case len(backoffs) > 0:
		d = backoffs[len(backoffs)-1] * 2
	default:
		d = 300 * time.Millisecond
	}
	if d > 3*time.Second {
		d = 3 * time.Second
	}
	select {
	case <-time.After(d):
	case <-c.Request.Context().Done():
	}
}

// Models 返回所有可用链名（OpenAI /v1/models 兼容格式）。
func (h *Handler) Models(c *gin.Context) {
	names := h.scheduler.ListChains()
	data := make([]gin.H, 0, len(names))
	for _, n := range names {
		data = append(data, gin.H{"id": n, "object": "model", "owned_by": "auto2api"})
	}
	c.JSON(http.StatusOK, gin.H{"object": "list", "data": data})
}

// Health 返回每条链各模型的实时健康（优先级、上游模型、是否冷却中）。
func (h *Handler) Health(c *gin.Context) {
	names := h.scheduler.ListChains()
	out := gin.H{}
	for _, name := range names {
		ch := h.scheduler.GetChain(name)
		models := make([]gin.H, 0, len(ch.Models))
		for _, m := range ch.Models {
			models = append(models, gin.H{
				"name":           m.Cfg.Name,
				"priority":       m.Cfg.Priority,
				"upstream_model": m.Cfg.Upstream.Model,
				"base_url":       m.Cfg.Upstream.BaseURL,
				"cooling_down":   h.scheduler.IsCoolingDown(m),
			})
		}
		out[name] = models
	}
	c.JSON(http.StatusOK, out)
}

// ---- helpers ----

func extractString(body []byte, key string) string {
	var p map[string]json.RawMessage
	if err := json.Unmarshal(body, &p); err != nil {
		return ""
	}
	raw, ok := p[key]
	if !ok {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func extractBool(body []byte, key string) bool {
	var p map[string]json.RawMessage
	if err := json.Unmarshal(body, &p); err != nil {
		return false
	}
	raw, ok := p[key]
	if !ok {
		return false
	}
	var b bool
	if json.Unmarshal(raw, &b) == nil {
		return b
	}
	return false
}

func writeJSONError(c *gin.Context, status int, etype, msg string) {
	c.JSON(status, gin.H{"error": gin.H{"message": msg, "type": etype}})
}

func writeUpstreamError(c *gin.Context, e *UpstreamError) {
	body := e.Body
	if body == "" || !json.Valid([]byte(body)) {
		b, _ := json.Marshal(gin.H{"error": gin.H{"message": e.Error(), "type": "upstream_error"}})
		body = string(b)
	}
	c.Data(e.Status, "application/json", []byte(body))
}
