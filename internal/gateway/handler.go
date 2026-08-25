// Gin 路由与 fallback 编排：两级机制——
//   层 1 同模型退避重试（retryable_status，对齐 sub2api shouldRetryUpstreamError）
//   层 2 跨优先级模型故障转移（trigger_status，对齐 sub2api shouldFailoverUpstreamError / hermes fallback）
package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// Handler 是所有 HTTP 路由的入口。
type Handler struct {
	scheduler   *Scheduler
	forwarder   *Forwarder
	maxSwitches int
	apiKeys     map[string]bool // 已授权 key 集合，为空则不鉴权
	logger      *CallLogger
	limiter     *ConcurrencyLimiter // 并发限流器（可选，nil=不限流）
	memGuard    *MemoryGuard        // 内存守护（可选，nil=不监控）
}

func NewHandler(s *Scheduler, f *Forwarder, maxSwitches int, apiKeys []string, logger *CallLogger) *Handler {
	h := &Handler{scheduler: s, forwarder: f, maxSwitches: maxSwitches, logger: logger}
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

// SetProtection 注入并发限流器与内存守护（由 main 构造后注入）。
func (h *Handler) SetProtection(limiter *ConcurrencyLimiter, memGuard *MemoryGuard) {
	h.limiter = limiter
	h.memGuard = memGuard
}

// Register 把路由挂到 gin 引擎上（OpenAI 兼容 + Claude 兼容）。
func (h *Handler) Register(r *gin.Engine) {
	auth := h.authMiddleware()
	protect := h.protectionMiddleware() // 内存降级 + 并发限流
	// OpenAI 兼容端点（受保护）
	r.POST("/v1/chat/completions", protect, auth, h.ChatCompletions)
	r.POST("/chat/completions", protect, auth, h.ChatCompletions) // 无 /v1 前缀的客户端兼容
	// OpenAI Responses API 兼容端点（受保护，上游打 /v1/responses）
	r.POST("/v1/responses", protect, func(c *gin.Context) {
		c.Set("endpoint", "responses")
		c.Next()
	}, auth, h.ChatCompletions)
	// Claude（Anthropic Messages API）兼容端点（受保护）
	r.POST("/v1/messages", protect, auth, h.Messages)
	r.POST("/messages", protect, auth, h.Messages) // 无 /v1 前缀兼容
	r.GET("/v1/models", auth, h.Models)
	r.GET("/v1/health", h.Health) // 健康检查不鉴权，方便监控探活
}

// protectionMiddleware 组合内存降级检查与并发限流：
//   1. 内存降级：内存超临界阈值时直接拒绝（503），给 GC 时间回收，防 OOM
//   2. 并发限流：并发超限时排队等待，队列满或超时则拒绝（503），防连接堆积
// 未配置对应机制时自动跳过（nil 检查）。
func (h *Handler) protectionMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 标记出站格式，使错误按正确格式返回
		if strings.Contains(c.Request.URL.Path, "/messages") {
			c.Set("outbound_format", "claude")
		}
		// 层 1：内存降级检查
		if h.memGuard != nil {
			if allowed, reason := h.memGuard.AllowRequest(); !allowed {
				c.Header("Retry-After", "5")
				writeJSONError(c, http.StatusServiceUnavailable, "overloaded_error", "server overloaded: "+reason)
				c.Abort()
				return
			}
		}
		// 层 2：并发限流
		if h.limiter != nil {
			if err := h.limiter.Acquire(c.Request.Context()); err != nil {
				c.Header("Retry-After", "3")
				writeJSONError(c, http.StatusServiceUnavailable, "overloaded_error", "too many concurrent requests: "+err.Error())
				c.Abort()
				return
			}
			defer h.limiter.Release()
		}
		c.Next()
	}
}

// authMiddleware 校验客户端 Authorization: Bearer <key> / x-api-key。
// 未配置 api_keys 时放行（向后兼容）。按请求路径推断出站格式，使鉴权错误也按对应格式返回。
func (h *Handler) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 路径含 /messages 视为 Claude（Anthropic）出站格式
		if strings.Contains(c.Request.URL.Path, "/messages") {
			c.Set("outbound_format", "claude")
		}
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
			writeJSONError(c, http.StatusUnauthorized, "authentication_error", "invalid or missing API key")
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

// ChatCompletions 处理 POST /v1/chat/completions（OpenAI 兼容）：按 model 字段选链，按优先级 fallback。
func (h *Handler) ChatCompletions(c *gin.Context) {
	entry := h.logger.Begin(c, "openai")
	var finalErr error
	finalStatus := http.StatusOK
	defer func() {
		h.logger.End(entry, finalStatus, finalErr)
	}()

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		finalStatus = http.StatusBadRequest
		finalErr = err
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", "read body: "+err.Error())
		return
	}
	if !json.Valid(body) {
		finalStatus = http.StatusBadRequest
		finalErr = fmt.Errorf("request body is not valid JSON")
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", finalErr.Error())
		return
	}
	finalStatus, finalErr = h.runCompletion(c, body, entry)
}

// Messages 处理 POST /v1/messages（Anthropic Claude Messages API 兼容）：
// 把 Claude 请求体转成 OpenAI 格式后走上游，响应再转回 Claude 格式回给客户端。
func (h *Handler) Messages(c *gin.Context) {
	// 标记出站格式为 claude，forwarder 与错误写函数据此转换响应
	c.Set("outbound_format", "claude")
	entry := h.logger.Begin(c, "claude")
	var finalErr error
	finalStatus := http.StatusOK
	defer func() {
		h.logger.End(entry, finalStatus, finalErr)
	}()

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		finalStatus = http.StatusBadRequest
		finalErr = err
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", "read body: "+err.Error())
		return
	}
	if !json.Valid(rawBody) {
		finalStatus = http.StatusBadRequest
		finalErr = fmt.Errorf("request body is not valid JSON")
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", finalErr.Error())
		return
	}
	// Claude → OpenAI 请求转换（system 折成 system 消息、内容块/工具映射）
	openaiBody, _, cerr := claudeRequestToOpenAI(rawBody)
	if cerr != nil {
		finalStatus = http.StatusBadRequest
		finalErr = cerr
		writeJSONError(c, http.StatusBadRequest, "invalid_request_error", "convert claude request: "+cerr.Error())
		return
	}
	// 记录原始 Claude 请求体（转换前）；上游请求体由 forwarder 记录
	chainName := extractString(openaiBody, "model")
	if chainName == "" {
		chainName = "auto"
	}
	streamRequested := extractBool(openaiBody, "stream")
	h.logger.SetRequest(entry, rawBody, chainName, streamRequested)

	finalStatus, finalErr = h.runCompletion(c, openaiBody, entry)
}

// runCompletion 按 model 字段选链并执行两级 fallback 编排（层1同模型退避重试 + 层2跨优先级故障转移）。
// 返回最终响应状态码与编排级错误（若有）。body 须为合法 OpenAI Chat 请求 JSON。
func (h *Handler) runCompletion(c *gin.Context, body []byte, entry *CallEntry) (int, error) {
	chainName := extractString(body, "model")
	if chainName == "" {
		chainName = "auto"
	}
	chain := h.scheduler.GetChain(chainName)
	if chain == nil {
		writeJSONError(c, http.StatusNotFound, "invalid_request_error", "unknown model/chain: "+chainName)
		return http.StatusNotFound, fmt.Errorf("unknown chain: %s", chainName)
	}
	streamRequested := extractBool(body, "stream")
	// ChatCompletions 路径在此记录请求；Messages 路径已提前记录（原始 Claude body）
	if entry != nil && entry.Format == "openai" {
		h.logger.SetRequest(entry, body, chainName, streamRequested)
	}

	excluded := map[string]bool{}
	var lastErr error
	for switches := 0; ; switches++ {
		// 动态路由：综合熔断状态/健康/延迟/成功率选最优模型（跳过熔断 OPEN 与冷却中）
		m := h.scheduler.SelectModel(chain, excluded)
		if m == nil {
			msg := "all models exhausted (circuit open or cooling down)"
			if lastErr != nil {
				msg = lastErr.Error()
			}
			writeJSONError(c, http.StatusBadGateway, "upstream_unavailable", msg)
			return http.StatusBadGateway, lastErr
		}
		// 确认放行：触发熔断 OPEN→HALF_OPEN 转换，并发下 double check
		allowed, _ := h.scheduler.AllowRequest(m)
		if !allowed {
			excluded[m.Cfg.Name] = true
			continue
		}

		outcome, err := h.tryModel(c, body, m, streamRequested, entry)
		if err != nil {
			lastErr = err
		}
		switch outcome {
		case outcomeSuccess, outcomeClientError:
			return c.Writer.Status(), err
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
				return http.StatusBadGateway, lastErr
			}
		}
	}
}

// tryModel 在单个模型上执行层 1（同模型退避重试）。
// 状态码处理顺序：retryable 且仍有重试次数 → 退避重试；否则 failover 触发 → 转移；
// 否则视为客户端错误原样返回。这样 429（retryable+failover）会先重试、耗尽后再转移，
// 401/403/500（仅 failover）则立即转移。
func (h *Handler) tryModel(c *gin.Context, body []byte, m *Model, streamRequested bool, entry *CallEntry) (attemptOutcome, error) {
	var ue *UpstreamError
	for attempt := 0; attempt <= m.RetryCount; attempt++ {
		res, ferr := h.forwarder.Forward(c.Request.Context(), c, body, m, streamRequested)
		if ferr == nil {
			h.scheduler.RecordResult(m, msToDur(res.DurationMs), true)
			h.scheduler.RecordTokens(m, res.PromptTokens, res.CompletionTokens)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority, Upstream: res.UpstreamModel,
				Status: res.Status, Outcome: "success", Attempt: attempt,
				DurationMs: res.DurationMs, FirstTokenMs: res.FirstTokenMs,
				PromptTokens: res.PromptTokens, CompletionTokens: res.CompletionTokens,
			})
			h.logger.SetUsage(entry, res.PromptTokens, res.CompletionTokens)
			return outcomeSuccess, nil
		}
		// 响应体已提交（流式中途失败）——无法 fallback，结束本次请求
		if res != nil && res.BodyCommitted {
			// 已成功写入部分响应，不计熔断失败
			h.scheduler.RecordResult(m, msToDur(res.DurationMs), true)
			h.scheduler.RecordTokens(m, res.PromptTokens, res.CompletionTokens)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority, Upstream: res.UpstreamModel,
				Status: res.Status, Outcome: "success", Attempt: attempt,
				DurationMs: res.DurationMs, FirstTokenMs: res.FirstTokenMs,
				PromptTokens: res.PromptTokens, CompletionTokens: res.CompletionTokens,
				Error: "body committed, abort fallback",
			})
			h.logger.SetUsage(entry, res.PromptTokens, res.CompletionTokens)
			return outcomeSuccess, nil
		}
		ue, _ = ferr.(*UpstreamError)
		if ue == nil {
			// 传输错误（DNS/连接拒绝/超时）——切下一优先级
			h.scheduler.RecordResult(m, msToDur(durationOf(res)), false)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority,
				Status: 0, Outcome: "error", Attempt: attempt,
				DurationMs: durationOf(res), Error: ferr.Error(),
			})
			return outcomeFailover, ferr
		}
		switch {
		case m.Retryable[ue.Status] && attempt < m.RetryCount:
			// 上游故障类状态码：计入熔断失败
			h.scheduler.RecordResult(m, msToDur(durationOf(res)), false)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority,
				Status: ue.Status, Outcome: "retry", Attempt: attempt,
				DurationMs: durationOf(res), Error: ue.Error(),
			})
			h.sleep(c, m.Backoffs, attempt)
			continue
		case m.Failover[ue.Status]:
			// 触发转移：计入熔断失败
			h.scheduler.RecordResult(m, msToDur(durationOf(res)), false)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority,
				Status: ue.Status, Outcome: "failover", Attempt: attempt,
				DurationMs: durationOf(res), Error: ue.Error(),
			})
			return outcomeFailover, ue
		default:
			// 非重试/非转移（如 400/404）——上游正常响应只是客户端请求问题，不计熔断失败
			h.scheduler.RecordResult(m, msToDur(durationOf(res)), true)
			h.logger.AddAttempt(entry, AttemptLog{
				Model: m.Cfg.Name, Priority: m.Cfg.Priority,
				Status: ue.Status, Outcome: "client_error", Attempt: attempt,
				DurationMs: durationOf(res), Error: ue.Error(),
			})
			writeUpstreamError(c, ue)
			return outcomeClientError, ue
		}
	}
	// 循环结束：最后一次是 retryable 但不在 failover 触发集中（重试耗尽）——把最后一次错误回给客户端
	if ue != nil {
		// 重试耗尽属于上游持续故障，计入熔断失败
		h.scheduler.RecordResult(m, 0, false)
		h.logger.AddAttempt(entry, AttemptLog{
			Model: m.Cfg.Name, Priority: m.Cfg.Priority,
			Status: ue.Status, Outcome: "client_error", Attempt: m.RetryCount,
			DurationMs: durationOf(nil), Error: ue.Error(),
		})
		writeUpstreamError(c, ue)
		return outcomeClientError, ue
	}
	return outcomeFailover, nil
}

// msToDur 把毫秒整数转 time.Duration（用于指标记录）。
func msToDur(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// durationOf 安全取 ForwardResult.DurationMs（res 可能为 nil）。
func durationOf(res *ForwardResult) int64 {
	if res == nil {
		return 0
	}
	return res.DurationMs
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

// Health 返回每条链各模型的实时健康（优先级、上游模型、冷却/熔断状态、延迟、成功率、探活状态、今日 token 用量）。
func (h *Handler) Health(c *gin.Context) {
	names := h.scheduler.ListChains()
	out := gin.H{}
	for _, name := range names {
		ch := h.scheduler.GetChain(name)
		models := make([]gin.H, 0, len(ch.Models))
		for _, m := range ch.Models {
			latency, success, total, fail := h.scheduler.Stats(m)
			promptTok, completTok, totalTok := h.scheduler.TokenStats(m)
			models = append(models, gin.H{
				"name":              m.Cfg.Name,
				"priority":          m.Cfg.Priority,
				"upstream_model":    m.Cfg.Upstream.Model,
				"base_url":          m.Cfg.Upstream.BaseURL,
				"cooling_down":      h.scheduler.IsCoolingDown(m),
				"breaker_state":     h.scheduler.BreakerStateName(m),
				"consecutive_fails": h.scheduler.ConsecutiveFails(m),
				"health":            h.scheduler.HealthStatus(m).String(),
				"latency_ema_ms":    latency,
				"success_rate":      success,
				"total_requests":    total,
				"total_failures":    fail,
				"prompt_tokens":     promptTok,
				"completion_tokens": completTok,
				"total_tokens":      totalTok,
			})
		}
		out[name] = models
	}
	// 服务保护运行时状态（并发/内存/panic），供监控告警
	protection := gin.H{"total_panics": GetPanicStats()}
	if h.limiter != nil {
		cur, rejected, timeout := h.limiter.Stats()
		protection["concurrency"] = gin.H{
			"current":        cur,
			"total_rejected": rejected,
			"total_timeout":  timeout,
		}
	}
	if h.memGuard != nil {
		usedMB, degraded, rejected := h.memGuard.Stats()
		protection["memory"] = gin.H{
			"used_mb":        usedMB,
			"degraded":       degraded,
			"total_rejected": rejected,
		}
	}
	out["_protection"] = protection
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
	// Claude（Anthropic）错误格式：{"type":"error","error":{"type":...,"message":...}}
	if c.GetString("outbound_format") == "claude" {
		c.JSON(status, gin.H{
			"type":  "error",
			"error": gin.H{"type": etype, "message": msg},
		})
		return
	}
	c.JSON(status, gin.H{"error": gin.H{"message": msg, "type": etype}})
}

func writeUpstreamError(c *gin.Context, e *UpstreamError) {
	body := e.Body
	if body == "" || !json.Valid([]byte(body)) {
		var b []byte
		if c.GetString("outbound_format") == "claude" {
			b, _ = json.Marshal(gin.H{
				"type":  "error",
				"error": gin.H{"type": "upstream_error", "message": e.Error()},
			})
		} else {
			b, _ = json.Marshal(gin.H{"error": gin.H{"message": e.Error(), "type": "upstream_error"}})
		}
		body = string(b)
	}
	c.Data(e.Status, "application/json", []byte(body))
}
