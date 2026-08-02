// Package gateway 熔断器：CLOSED → OPEN → HALF_OPEN → CLOSED 三态机。
// 目的：单模型连续失败达阈值后直接跳过，避免每个新请求都先打 A（A 已坏时）。
// 与 failover.cooldown 的关系：cooldown 是单次失败后的短期冷却；
// 熔断器是连续失败累积后的长期熔断 + 半开探测恢复，二者叠加。
package gateway

import (
	"sync"
	"time"
)

// breakerState 熔断状态
type breakerState int

const (
	breakerClosed breakerState = iota // 正常放行
	breakerOpen                       // 熔断打开，拒绝请求；冷却到期转 half_open
	breakerHalfOpen                   // 半开，只允许有限探测请求
)

// BreakerConfig 熔断器参数。
type BreakerConfig struct {
	Enabled          bool          // 总开关，关闭则 Allow 永远返回 true
	FailureThreshold int           // 连续失败多少次进入 OPEN
	OpenDuration     time.Duration // OPEN 持续时间（冷却窗口），到期转 HALF_OPEN
	HalfOpenMax      int           // HALF_OPEN 允许的并发探测请求数（通常 1）
}

// CircuitBreaker 单模型熔断器，线程安全。
type CircuitBreaker struct {
	mu               sync.Mutex
	cfg              BreakerConfig
	state            breakerState
	consecutiveFails int
	openedAt         time.Time
	halfOpenInflight int
}

// NewCircuitBreaker 用配置构造熔断器，未设置字段填默认值。
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenDuration <= 0 {
		cfg.OpenDuration = 60 * time.Second
	}
	if cfg.HalfOpenMax <= 0 {
		cfg.HalfOpenMax = 1
	}
	return &CircuitBreaker{cfg: cfg, state: breakerClosed}
}

// Allow 判断是否允许放行请求。
// 返回 (allowed, isProbe)：
//   - CLOSED：放行，非探测
//   - OPEN：冷却到期则转 HALF_OPEN 并占用一个探测名额放行；否则拒绝
//   - HALF_OPEN：若还有探测名额则放行；否则拒绝（避免半开被打满）
func (b *CircuitBreaker) Allow() (allowed, isProbe bool) {
	if !b.cfg.Enabled {
		return true, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case breakerClosed:
		return true, false
	case breakerOpen:
		if time.Since(b.openedAt) >= b.cfg.OpenDuration {
			b.state = breakerHalfOpen
			b.halfOpenInflight = 1
			return true, true
		}
		return false, false
	case breakerHalfOpen:
		if b.halfOpenInflight < b.cfg.HalfOpenMax {
			b.halfOpenInflight++
			return true, true
		}
		return false, false
	}
	return true, false
}

// OnSuccess 记录成功：CLOSED 清零连续失败；HALF_OPEN 探测成功 → CLOSED。
func (b *CircuitBreaker) OnSuccess() {
	if !b.cfg.Enabled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails = 0
	if b.state == breakerHalfOpen {
		b.state = breakerClosed
		b.halfOpenInflight = 0
	}
}

// OnFailure 记录失败：CLOSED 累计，达阈值转 OPEN；HALF_OPEN 探测失败 → 重新 OPEN。
func (b *CircuitBreaker) OnFailure() {
	if !b.cfg.Enabled {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFails++
	switch b.state {
	case breakerClosed:
		if b.consecutiveFails >= b.cfg.FailureThreshold {
			b.state = breakerOpen
			b.openedAt = time.Now()
		}
	case breakerHalfOpen:
		b.state = breakerOpen
		b.openedAt = time.Now()
		b.halfOpenInflight = 0
	}
}

// State 返回当前状态（只读快照）。
func (b *CircuitBreaker) State() breakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 若 OPEN 已到期但还没被 Allow 触发转 HALF_OPEN，对外仍报 OPEN；
	// 真正转态发生在下一次 Allow，避免竞态。
	return b.state
}

// ShouldSkipRouting 判断路由选路时是否应跳过该模型（只读，不改状态）。
// 仅当熔断处于 OPEN 且冷却窗口未到期时才跳过；OPEN 已到期则不跳过——
// 让该模型重新成为候选，由后续 Allow 触发 OPEN→HALF_OPEN 并放行一个探测请求。
//
// 这是修复“熔断 OPEN 后永久 OPEN”的关键：此前选路用 State()==OPEN 直接跳过，
// 而唯一能触发 HALF_OPEN 探测的 Allow 只在被选中的模型上调用，被跳过的模型
// 永远得不到探测，只能重启进程复位。改用本方法后，到期的 OPEN 模型会被重新
// 纳入候选并获得探测机会。
func (b *CircuitBreaker) ShouldSkipRouting() bool {
	if !b.cfg.Enabled {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != breakerOpen {
		return false
	}
	// OPEN 且冷却未到期 → 跳过；已到期 → 不跳过（放行探测）。
	return time.Since(b.openedAt) < b.cfg.OpenDuration
}

// StateName 返回状态名字符串，供 /v1/health 展示。
func (b *CircuitBreaker) StateName() string {
	switch b.State() {
	case breakerClosed:
		return "closed"
	case breakerOpen:
		return "open"
	case breakerHalfOpen:
		return "half_open"
	}
	return "unknown"
}

// ConsecutiveFails 返回当前连续失败数（只读）。
func (b *CircuitBreaker) ConsecutiveFails() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutiveFails
}
