// Package gateway 运行时指标：单模型的 EMA 延迟与成功率。
// 用于动态路由：同优先级下优先选延迟低、成功率高的模型。
// EMA（指数移动平均）实现简单、内存恒定，对短期波动敏感。
package gateway

import (
	"sync"
	"time"
)

// Metrics 单模型运行时指标，线程安全。
type Metrics struct {
	mu         sync.Mutex
	latencyEMA float64 // 延迟 EMA（毫秒）
	successEMA float64 // 成功率 EMA，范围 [0,1]，初始 1.0（乐观假设）
	totalReq   int64
	totalFail  int64
}

const defaultAlpha = 0.3 // EMA 平滑系数：新样本权重 0.3

// NewMetrics 构造指标器，初始假设完全成功。
func NewMetrics() *Metrics {
	return &Metrics{successEMA: 1.0}
}

// Record 记录一次调用结果。
func (m *Metrics) Record(latency time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ms := float64(latency.Milliseconds())
	if m.totalReq == 0 {
		m.latencyEMA = ms
		if success {
			m.successEMA = 1.0
		} else {
			m.successEMA = 0.0
		}
	} else {
		m.latencyEMA = defaultAlpha*ms + (1-defaultAlpha)*m.latencyEMA
		s := 0.0
		if success {
			s = 1.0
		}
		m.successEMA = defaultAlpha*s + (1-defaultAlpha)*m.successEMA
	}
	m.totalReq++
	if !success {
		m.totalFail++
	}
}

// LatencyEMA 返回延迟 EMA（毫秒）。
func (m *Metrics) LatencyEMA() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latencyEMA
}

// SuccessRate 返回成功率 EMA（0~1）。
func (m *Metrics) SuccessRate() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.successEMA
}

// Total 返回 (总请求数, 总失败数)。
func (m *Metrics) Total() (int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalReq, m.totalFail
}
