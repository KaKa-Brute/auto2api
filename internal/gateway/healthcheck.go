// Package gateway 健康检查：后台 goroutine 周期性主动探活每个上游模型，
// 失败的模型触发熔断 OPEN，恢复的模型复位熔断 CLOSED。
// 探活不经过 Forwarder.Forward（避免污染业务指标与调用日志），
// 而是用独立 http.Client 发一个 max_tokens=1 的最小 chat 请求。
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
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
	body      []byte
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
	// 最小探活请求体：max_tokens=1、stream=false
	body, _ := json.Marshal(map[string]any{
		"model":    "ping",
		"messages": []map[string]string{{"role": "user", "content": "ping"}},
		"max_tokens": 1,
		"stream":    false,
	})
	return &HealthChecker{
		scheduler: s,
		interval:  interval,
		timeout:   timeout,
		body:      body,
		client:    &http.Client{Timeout: 0}, // 由请求级 context 控制
		statuses:  map[string]HealthStatus{},
		stopCh:     make(chan struct{}),
	}
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

// probe 探测单个模型，更新健康状态与熔断器。
func (h *HealthChecker) probe(m *Model) {
	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()
	url := buildURL(m.Cfg.Upstream.BaseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(h.body))
	if err != nil {
		h.set(m, false)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	setAuth(req, m)
	resp, err := h.client.Do(req)
	if err != nil {
		h.set(m, false)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	h.set(m, resp.StatusCode >= 200 && resp.StatusCode < 300)
}

// set 更新健康状态，并把结果反馈给熔断器（探活成功复位，探活失败触发熔断）。
func (h *HealthChecker) set(m *Model, healthy bool) {
	key := m.cooldownKey()
	h.mu.Lock()
	if healthy {
		h.statuses[key] = HealthHealthy
	} else {
		h.statuses[key] = HealthUnhealthy
	}
	h.mu.Unlock()

	// 反馈给熔断器：探活结果视为对该模型的一次真实调用观测
	if b := h.scheduler.breakerOf(key); b != nil {
		if healthy {
			b.OnSuccess()
		} else {
			b.OnFailure()
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
