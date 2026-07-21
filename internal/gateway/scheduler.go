// Package gateway 实现优先级自动切换（fallback）的网关核心：
// 多套命名链、按优先级选择模型、内存冷却、FailoverState 编排。
// 在原有 failover.cooldown（单次失败短期冷却）基础上，
// 叠加熔断器（连续失败长期熔断 + 半开探测恢复）、
// 主动健康检查（后台探活反馈熔断器）、
// 动态路由（同优先级按健康/延迟/成功率排序）。
package gateway

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"auto2api/internal/config"
)

// Model 是一条链里某个优先级模型的运行期表示，所有耗时配置已解析。
type Model struct {
	Cfg         config.ModelConfig
	ChainName   string // 所属链名，冷却键按 chain::name 隔离，避免跨链同名污染
	RetryCount  int
	Backoffs    []time.Duration
	Retryable   map[int]bool // 同模型重试的状态码
	Failover    map[int]bool // 触发切换下一优先级模型的状态码
	Cooldown    time.Duration
	IdleTimeout time.Duration
	Keepalive   time.Duration
	Timeout     time.Duration
}

// cooldownKey 返回模型在冷却表/熔断表/指标表中的唯一键。
func (m *Model) cooldownKey() string { return m.ChainName + "::" + m.Cfg.Name }

// Chain 是按 priority 升序排列的模型列表，外部按链名调用。
type Chain struct {
	Name   string
	Models []*Model
}

// ForwardResult 是一次转发的结果摘要。
type ForwardResult struct {
	Status        int    // 上游 HTTP 状态（0=传输错误）
	BodyCommitted bool   // true 表示响应体已开始写，不可再 fallback
	Stream        bool
	UpstreamModel string
	FirstTokenMs  int64
	DurationMs    int64
}

// Scheduler 管理所有链与内存冷却表、熔断器、运行时指标。
type Scheduler struct {
	mu          sync.RWMutex
	chains      map[string]*Chain
	cooldown    map[string]time.Time          // model key -> 冷却到期时间（failover 单次冷却）
	breakers    map[string]*CircuitBreaker    // model key -> 熔断器
	metrics     map[string]*Metrics           // model key -> 运行时指标
	health      *HealthChecker                // 后台主动探活，可能为 nil
	breakerCfg  BreakerConfig                 // 熔断器默认配置
}

// NewScheduler 从配置构建调度器，每条链按 priority 升序、解析所有时长。
func NewScheduler(cfg *config.Config) (*Scheduler, error) {
	chains := make(map[string]*Chain, len(cfg.Chains))
	breakers := make(map[string]*CircuitBreaker)
	metrics := make(map[string]*Metrics)
	for name, cc := range cfg.Chains {
		models := make([]*Model, 0, len(cc.Models))
		for i := range cc.Models {
			m, err := buildModel(cc.Models[i])
			if err != nil {
				return nil, fmt.Errorf("chain %q model %q: %w", name, cc.Models[i].Name, err)
			}
			m.ChainName = name
			models = append(models, m)
		}
		sort.Slice(models, func(i, j int) bool {
			return models[i].Cfg.Priority < models[j].Cfg.Priority
		})
		chains[name] = &Chain{Name: name, Models: models}
	}
	breakerCfg := buildBreakerCfg(cfg.Breaker)
	s := &Scheduler{
		chains:     chains,
		cooldown:   map[string]time.Time{},
		breakers:   breakers,
		metrics:    metrics,
		breakerCfg: breakerCfg,
	}
	// 为每个模型创建熔断器与指标器
	for _, ch := range chains {
		for _, m := range ch.Models {
			s.breakers[m.cooldownKey()] = NewCircuitBreaker(breakerCfg)
			s.metrics[m.cooldownKey()] = NewMetrics()
		}
	}
	return s, nil
}

// SetHealthChecker 注入后台探活器（由 main 启动后注入，因依赖 Scheduler 已构造完成）。
func (s *Scheduler) SetHealthChecker(h *HealthChecker) {
	s.mu.Lock()
	s.health = h
	s.mu.Unlock()
}

// buildBreakerCfg 把 config.BreakerConfig 转为运行期 BreakerConfig。
func buildBreakerCfg(c config.BreakerConfig) BreakerConfig {
	bc := BreakerConfig{Enabled: c.Enabled}
	if c.FailureThreshold > 0 {
		bc.FailureThreshold = c.FailureThreshold
	}
	if d, err := time.ParseDuration(c.OpenDuration); err == nil && d > 0 {
		bc.OpenDuration = d
	}
	if c.HalfOpenMax > 0 {
		bc.HalfOpenMax = c.HalfOpenMax
	}
	return bc
}

func buildModel(mc config.ModelConfig) (*Model, error) {
	m := &Model{Cfg: mc, Retryable: map[int]bool{}, Failover: map[int]bool{}}
	td, err := time.ParseDuration(mc.Upstream.Timeout)
	if err != nil {
		return nil, fmt.Errorf("bad timeout: %w", err)
	}
	m.Timeout = td
	m.RetryCount = mc.Retry.Count
	if m.RetryCount < 0 {
		m.RetryCount = 0
	}
	if bs, err := config.ParseDurations(mc.Retry.Backoff); err == nil {
		m.Backoffs = bs
	}
	for _, s := range mc.Retry.RetryableStatus {
		m.Retryable[s] = true
	}
	for _, s := range mc.Failover.TriggerStatus {
		m.Failover[s] = true
	}
	cd, err := time.ParseDuration(orDefault(mc.Failover.Cooldown, "60s"))
	if err != nil {
		return nil, fmt.Errorf("bad cooldown: %w", err)
	}
	m.Cooldown = cd
	it, err := time.ParseDuration(orDefault(mc.Stream.IdleTimeout, "30s"))
	if err != nil {
		return nil, fmt.Errorf("bad idle_timeout: %w", err)
	}
	m.IdleTimeout = it
	ka, err := time.ParseDuration(orDefault(mc.Stream.Keepalive, "5s"))
	if err != nil {
		return nil, fmt.Errorf("bad keepalive: %w", err)
	}
	m.Keepalive = ka
	return m, nil
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// GetChain 按名取链，不存在返回 nil。
func (s *Scheduler) GetChain(name string) *Chain {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.chains[name]
}

// ListChains 返回所有链名（已排序），供 /v1/models 与启动日志使用。
func (s *Scheduler) ListChains() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.chains))
	for k := range s.chains {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// ListAllModels 返回所有链的所有模型扁平列表，供健康检查遍历。
func (s *Scheduler) ListAllModels() []*Model {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Model, 0)
	for _, ch := range s.chains {
		out = append(out, ch.Models...)
	}
	return out
}

// breakerOf 按 key 取熔断器（只读，可能 nil）。
func (s *Scheduler) breakerOf(key string) *CircuitBreaker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.breakers[key]
}

// metricsOf 按 model 取指标器。
func (s *Scheduler) metricsOf(m *Model) *Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.metrics[m.cooldownKey()]
}

// IsCoolingDown 判断某模型是否仍在冷却窗口内（failover 单次冷却）。
func (s *Scheduler) IsCoolingDown(m *Model) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	exp, ok := s.cooldown[m.cooldownKey()]
	return ok && time.Now().Before(exp)
}

// MarkCooldown 把模型置入冷却（冷却时长取自模型配置）。
func (s *Scheduler) MarkCooldown(m *Model) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cooldown[m.cooldownKey()] = time.Now().Add(m.Cooldown)
}

// IsOpen 判断熔断器是否处于 OPEN（拒绝请求）态。
// 注意：Allow 才会触发 OPEN→HALF_OPEN 转换；IsOpen 只读快照，
// 仅供 /v1/health 展示与路由预过滤，真正放行决策走 Allow。
func (s *Scheduler) IsOpen(m *Model) bool {
	b := s.breakerOf(m.cooldownKey())
	if b == nil {
		return false
	}
	return b.State() == breakerOpen
}

// AllowRequest 综合熔断器与冷却表判断是否放行。
// 返回 (allowed, isProbe)。熔断 OPEN 或在冷却窗口内均拒绝。
// 调用方在拿到请求结果后须调用 RecordResult 反馈熔断器与指标。
func (s *Scheduler) AllowRequest(m *Model) (bool, bool) {
	// 先看 failover 冷却
	if s.IsCoolingDown(m) {
		return false, false
	}
	b := s.breakerOf(m.cooldownKey())
	if b == nil {
		return true, false
	}
	return b.Allow()
}

// RecordResult 把一次模型调用的结果反馈给熔断器与指标器。
// latency 为本次总耗时；success=true 视为成功，false 视为失败。
// 探测请求（isProbe）的结果同样会驱动 HALF_OPEN → CLOSED/OPEN 转换。
func (s *Scheduler) RecordResult(m *Model, latency time.Duration, success bool) {
	if mt := s.metricsOf(m); mt != nil {
		mt.Record(latency, success)
	}
	b := s.breakerOf(m.cooldownKey())
	if b == nil {
		return
	}
	if success {
		b.OnSuccess()
	} else {
		b.OnFailure()
	}
}

// Stats 返回某模型的 (延迟EMA毫秒, 成功率EMA, 总请求数, 总失败数)，供 /v1/health 展示。
func (s *Scheduler) Stats(m *Model) (float64, float64, int64, int64) {
	mt := s.metricsOf(m)
	if mt == nil {
		return 0, 1.0, 0, 0
	}
	total, fail := mt.Total()
	return mt.LatencyEMA(), mt.SuccessRate(), total, fail
}

// BreakerStateName 返回熔断器状态名（closed/open/half_open），供 /v1/health 展示。
func (s *Scheduler) BreakerStateName(m *Model) string {
	b := s.breakerOf(m.cooldownKey())
	if b == nil {
		return "closed"
	}
	return b.StateName()
}

// ConsecutiveFails 返回熔断器当前连续失败数，供 /v1/health 展示。
func (s *Scheduler) ConsecutiveFails(m *Model) int {
	b := s.breakerOf(m.cooldownKey())
	if b == nil {
		return 0
	}
	return b.ConsecutiveFails()
}

// HealthStatus 返回主动健康检查维护的最近探活状态。
func (s *Scheduler) HealthStatus(m *Model) HealthStatus {
	s.mu.RLock()
	h := s.health
	s.mu.RUnlock()
	if h == nil {
		return HealthUnknown
	}
	return h.Status(m)
}

// PickNext 按优先级升序返回下一个可用模型，跳过 excluded 集合、冷却中、熔断 OPEN 的模型。
// 返回 nil 表示链已耗尽（调用方应回 502）。
// 这是简单选路：仅跳过不可用，不重排优先级。动态打分见 SelectModel。
func (s *Scheduler) PickNext(chain *Chain, excluded map[string]bool) *Model {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	for _, m := range chain.Models {
		if excluded[m.Cfg.Name] {
			continue
		}
		if exp, ok := s.cooldown[m.cooldownKey()]; ok && now.Before(exp) {
			continue
		}
		if b := s.breakers[m.cooldownKey()]; b != nil && b.State() == breakerOpen {
			continue
		}
		return m
	}
	return nil
}

// SelectModel 是动态路由：在同优先级组内按 健康状态 > 成功率 > 延迟 综合排序选最优模型。
// 跳过 excluded、冷却中、熔断 OPEN 的模型；返回 nil 表示链已耗尽。
// 与 PickNext 的区别：PickNext 严格按配置 priority 顺序取第一个可用；
// SelectModel 在“可用”集合内按动态指标重排，倾向更健康/更快/成功率更高的模型。
// 当所有模型熔断未恢复时回退到 PickNext（保证至少能尝试最高优先级）。
func (s *Scheduler) SelectModel(chain *Chain, excluded map[string]bool) *Model {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	// 收集可用模型
	type cand struct {
		m           *Model
		latency    float64
		success    float64
		healthy    int // 0 unknown, -1 unhealthy, 1 healthy
	}
	var cands []cand
	for _, m := range chain.Models {
		if excluded[m.Cfg.Name] {
			continue
		}
		if exp, ok := s.cooldown[m.cooldownKey()]; ok && now.Before(exp) {
			continue
		}
		if b := s.breakers[m.cooldownKey()]; b != nil && b.State() == breakerOpen {
			continue
		}
		c := cand{m: m, latency: 0, success: 1.0, healthy: 0}
		if mt := s.metrics[m.cooldownKey()]; mt != nil {
			c.latency = mt.LatencyEMA()
			c.success = mt.SuccessRate()
		}
		if s.health != nil {
			switch s.health.Status(m) {
			case HealthHealthy:
				c.healthy = 1
			case HealthUnhealthy:
				c.healthy = -1
			}
		}
		cands = append(cands, c)
	}
	if len(cands) == 0 {
		return nil
	}
	// 排序键：优先级升序 → 健康(1>0>-1) → 成功率降序 → 延迟升序 → 配置顺序
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.m.Cfg.Priority != b.m.Cfg.Priority {
			return a.m.Cfg.Priority < b.m.Cfg.Priority
		}
		if a.healthy != b.healthy {
			return a.healthy > b.healthy
		}
		if a.success != b.success {
			return a.success > b.success
		}
		if a.latency != b.latency {
			return a.latency < b.latency
		}
		return false
	})
	return cands[0].m
}
