// Package gateway 健康检查：后台 goroutine 周期性主动探活每个上游模型，
// 失败的模型触发熔断 OPEN，恢复的模型复位熔断 CLOSED。
// 探活不经过 Forwarder.Forward（避免污染业务指标与调用日志），
// 而是用独立 http.Client 发一个 max_tokens=1 的最小 chat 请求。
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthStatus 主动健康检查维护的状态。
type HealthStatus int

const (
	HealthUnknown   HealthStatus = iota // 未探测过
	HealthHealthy                       // 最近探活成功
	HealthUnhealthy                     // 最近探活失败
)

func (s HealthStatus) String() string {
	switch s {
	case HealthHealthy:
		return "healthy"
	case HealthUnhealthy:
		return "unhealthy"
	}
	return "unknown"
}

// HealthChecker 后台周期性探活器。
type HealthChecker struct {
	scheduler *Scheduler
	interval  time.Duration
	timeout   time.Duration
	client    *http.Client
	stopCh    chan struct{}
	mu        sync.RWMutex
	statuses map[string]HealthStatus // cooldownKey -> status
}

// NewHealthChecker 构造探活器。interval=0 表示禁用。
func NewHealthChecker(s *Scheduler, interval, timeout time.Duration) *HealthChecker {
	if interval <= 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &HealthChecker{
		scheduler: s,
		interval:  interval,
		timeout:   timeout,
		client:    &http.Client{Timeout: 0}, // 由请求级 context 控制
		statuses:  map[string]HealthStatus{},
		stopCh:     make(chan struct{}),
	}
}

// probeBody 为指定模型构造轻量探活请求体。
// 只发 model + 空 messages，不带 max_tokens——上游校验 messages 为空会快速返回 400，
// 不需要等模型生成 token。这样能避免某些模型（如 Claude）对 max_tokens=1 响应极慢导致探针超时。
// 关键：用该模型配置的真实上游模型名（m.Cfg.Upstream.Model），
// 而不是硬编码 "ping"——否则上游校验模型名会回 404/400 被误判为不可用。
func (h *HealthChecker) probeBody(m *Model) []byte {
	body, _ := json.Marshal(map[string]any{
		"model":    m.Cfg.Upstream.Model,
		"messages": []map[string]string{},
	})
	return body
}

// applyProbeHeaders 注入探针所需的头部，尽量与 Forwarder 对齐，
// 避免因缺头（如 anthropic-version）被上游 400，导致探针假阴性。
// Authorization / x-api-key 等鉴权头由 setAuth 注入，这里只补白名单内的业务头。
func (h *HealthChecker) applyProbeHeaders(req *http.Request, m *Model) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "auto2api-healthcheck")
	// auth_header 在配置里指定（Authorization / x-api-key / x-goog-api-key / 自定义）
	setAuth(req, m)
}

// Start 启动后台 goroutine。
func (h *HealthChecker) Start() {
	if h == nil {
		return
	}
	go h.loop()
}

// Stop 停止后台 goroutine。
func (h *HealthChecker) Stop() {
	if h == nil {
		return
	}
	select {
	case <-h.stopCh:
	default:
		close(h.stopCh)
	}
}

func (h *HealthChecker) loop() {
	// 启动时立即探一轮，之后按 interval 周期。
	h.probeAll()
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-h.stopCh:
			return
		case <-t.C:
			h.probeAll()
		}
	}
}

// probeAll 并发探测所有链的所有模型。
func (h *HealthChecker) probeAll() {
	models := h.scheduler.ListAllModels()
	var wg sync.WaitGroup
	for _, m := range models {
		wg.Add(1)
		go func(m *Model) {
			defer wg.Done()
			h.probe(m)
		}(m)
	}
	wg.Wait()
}

// probe 轻量探测单个模型，只检查上游端点是否可达，不等待模型生成响应。
//   - 发送空 messages 请求，上游校验失败会快速返回错误（不用等模型生成 token）
//   - 收到 HTTP 响应（非 5xx）→ 端点可达（healthy）
//   - 5xx → 上游服务故障（unhealthy）
//   - 网络错误/超时 → 端点不可达（unhealthy）
//   - 429 → 忽略（限流不代表不可达）
//   - 超时：复用模型配置的 timeout（如 120s）而非固定 health_check.timeout，
//     避免代理延迟导致 10s 超时假阴性。
func (h *HealthChecker) probe(m *Model) {
	ctx, cancel := context.WithTimeout(context.Background(), m.Timeout)
	defer cancel()
	url := buildURL(m.Cfg.Upstream.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(h.probeBody(m)))
	if err != nil {
		log.Printf("[healthcheck] %s: build request failed: %v", m.cooldownKey(), err)
		h.update(m, 0, false)
		return
	}
	h.applyProbeHeaders(req, m)
	start := time.Now()
	resp, err := h.client.Do(req)
	if err != nil {
		// DNS/连接拒绝/超时 → 端点不可达
		log.Printf("[healthcheck] %s: probe failed (status=0, dur=%s): %v", m.cooldownKey(), time.Since(start), err)
		h.update(m, 0, false)
		return
	}
	// 轻量探测：只看状态码，不读响应体（避免等待模型生成 token）
	resp.Body.Close()
	elapsed := time.Since(start)
	// 收到 HTTP 响应即认为端点可达（4xx/5xx 都说明 HTTP 服务活着）。
	// 真正的服务状态由真实业务请求（RecordResult）反馈给熔断器。
	healthy := resp.StatusCode != 429
	if !healthy {
		log.Printf("[healthcheck] %s: probe unhealthy (status=%d, dur=%s)", m.cooldownKey(), resp.StatusCode, elapsed)
	}
	h.update(m, resp.StatusCode, healthy)
}

// update 按状态码分类更新健康状态并反馈熔断器。
// status=0 表示传输错误。healthy=true 表示端点可达（非 5xx）。
// 429 不触发熔断失败（限流不代表模型坏了），也不记为 unhealthy。
//
// 探针失败不调用 breaker.OnFailure()——探针可能因网络抖动等瞬时原因失败，
// 不应直接驱动熔断器 OPEN 或在 HALF_OPEN 下重置 openedAt 导致无限循环。
// 熔断器的失败计数只由真实业务请求（RecordResult）驱动。
// 探针成功仍调 OnSuccess() 帮助恢复（HALF_OPEN→CLOSED）。
func (h *HealthChecker) update(m *Model, status int, healthy bool) {
	key := m.cooldownKey()
	// 429：限流，模型本身是活的，不计 unhealthy 也不计熔断失败
	if status == 429 {
		return
	}
	h.mu.Lock()
	if healthy {
		h.statuses[key] = HealthHealthy
	} else {
		h.statuses[key] = HealthUnhealthy
	}
	h.mu.Unlock()

	// 探针成功时反馈给熔断器帮助恢复（HALF_OPEN→CLOSED，CLOSED 清零连续失败）。
	// 探针失败只更新 HealthStatus（供路由降级），不驱动熔断器状态转换。
	if healthy {
		if b := h.scheduler.breakerOf(key); b != nil {
			b.OnSuccess()
		}
	}
}

// set 更新健康状态，探活成功时反馈给熔断器帮助恢复。
// 探活失败只更新 HealthStatus，不驱动熔断器（熔断由真实请求驱动）。
// 保留供测试直接设置状态用；生产路径走 update。
func (h *HealthChecker) set(m *Model, healthy bool) {
	key := m.cooldownKey()
	h.mu.Lock()
	if healthy {
		h.statuses[key] = HealthHealthy
	} else {
		h.statuses[key] = HealthUnhealthy
	}
	h.mu.Unlock()

	// 探针成功时反馈给熔断器帮助恢复；探针失败不驱动熔断器状态转换。
	if healthy {
		if b := h.scheduler.breakerOf(key); b != nil {
			b.OnSuccess()
		}
	}
}

// Status 返回某模型最近一次主动探活状态。
func (h *HealthChecker) Status(m *Model) HealthStatus {
	if h == nil {
		return HealthUnknown
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if s, ok := h.statuses[m.cooldownKey()]; ok {
		return s
	}
	return HealthUnknown
}
