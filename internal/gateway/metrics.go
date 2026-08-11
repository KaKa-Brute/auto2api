// Package gateway 运行时指标：单模型的 EMA 延迟与成功率。
// 用于动态路由：同优先级下优先选延迟低、成功率高的模型。
// EMA（指数移动平均）实现简单、内存恒定，对短期波动敏感。
// Token 统计按日期分片，每天 UTC 0 点自动切换到新分片。
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
	// Token 按日期分片存储：key 为 "YYYY-MM-DD" 格式的日期字符串
	tokensByDate map[string]*tokenShard
}

// tokenShard 某一天的 token 累计。
type tokenShard struct {
	promptTok  int64
	completTok int64
}

const defaultAlpha = 0.3 // EMA 平滑系数：新样本权重 0.3

// NewMetrics 构造指标器，初始假设完全成功。
func NewMetrics() *Metrics {
	return &Metrics{
		successEMA:   1.0,
		tokensByDate: make(map[string]*tokenShard),
	}
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

// RecordTokens 累加一次调用的输入/输出 token 数到今日分片（0 值忽略）。
func (m *Metrics) RecordTokens(prompt, completion int) {
	if prompt <= 0 && completion <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	today := time.Now().UTC().Format("2006-01-02")
	shard, ok := m.tokensByDate[today]
	if !ok {
		shard = &tokenShard{}
		m.tokensByDate[today] = shard
	}
	if prompt > 0 {
		shard.promptTok += int64(prompt)
	}
	if completion > 0 {
		shard.completTok += int64(completion)
	}
}

// Tokens 返回今日累计 (输入 token, 输出 token, 总 token)。
func (m *Metrics) Tokens() (int64, int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	today := time.Now().UTC().Format("2006-01-02")
	shard, ok := m.tokensByDate[today]
	if !ok {
		return 0, 0, 0
	}
	return shard.promptTok, shard.completTok, shard.promptTok + shard.completTok
}

// TokensForDate 返回指定日期的 token 统计（日期格式 "YYYY-MM-DD"）。
func (m *Metrics) TokensForDate(date string) (int64, int64, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	shard, ok := m.tokensByDate[date]
	if !ok {
		return 0, 0, 0
	}
	return shard.promptTok, shard.completTok, shard.promptTok + shard.completTok
}

// CleanupOldTokens 清理指定天数之前的 token 分片（减少内存占用）。
func (m *Metrics) CleanupOldTokens(keepDays int) {
	if keepDays <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cutoff := time.Now().UTC().AddDate(0, 0, -keepDays).Format("2006-01-02")
	for date := range m.tokensByDate {
		if date < cutoff {
			delete(m.tokensByDate, date)
		}
	}
}
