// Package gateway 实现优先级自动切换（fallback）的网关核心：
// 多套命名链、按优先级选择模型、内存冷却、FailoverState 编排。
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
	ChainName  string // 所属链名，冷却键按 chain::name 隔离，避免跨链同名污染
	RetryCount  int
	Backoffs    []time.Duration
	Retryable   map[int]bool // 同模型重试的状态码
	Failover    map[int]bool // 触发切换下一优先级模型的状态码
	Cooldown    time.Duration
	IdleTimeout time.Duration
	Keepalive   time.Duration
	Timeout     time.Duration
}

// cooldownKey 返回模型在冷却表中的唯一键。
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

// Scheduler 管理所有链与内存冷却表。
type Scheduler struct {
	mu       sync.RWMutex
	chains   map[string]*Chain
	cooldown map[string]time.Time // model name -> 冷却到期时间
}

// NewScheduler 从配置构建调度器，每条链按 priority 升序、解析所有时长。
func NewScheduler(cfg *config.Config) (*Scheduler, error) {
	chains := make(map[string]*Chain, len(cfg.Chains))
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
	return &Scheduler{chains: chains, cooldown: map[string]time.Time{}}, nil
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

// IsCoolingDown 判断某模型是否仍在冷却窗口内。
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

// PickNext 按优先级升序返回下一个可用模型，跳过 excluded 集合与冷却中的模型。
// 返回 nil 表示链已耗尽（调用方应回 502）。
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
		return m
	}
	return nil
}
