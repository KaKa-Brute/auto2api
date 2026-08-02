package gateway

import (
	"testing"
	"time"

	"auto2api/internal/config"
)

func TestCircuitBreaker(t *testing.T) {
	cfg := &config.Config{
		Breaker: config.BreakerConfig{
			Enabled:          true,
			FailureThreshold: 2,
			OpenDuration:     "200ms",
			HalfOpenMax:      1,
		},
		Chains: map[string]config.ChainConfig{
			"test-chain": {
				Models: []config.ModelConfig{
					{
						Name:     "bad-model",
						Priority: 1,
						Upstream: config.UpstreamConfig{BaseURL: "https://bad.example.com", Model: "bad-upstream", APIKey: "test-key", Timeout: "1s"},
						Failover: config.FailoverConfig{Cooldown: "0s"},
						Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
					},
					{
						Name:     "good-model",
						Priority: 2,
						Upstream: config.UpstreamConfig{BaseURL: "https://good.example.com", Model: "good-upstream", APIKey: "test-key", Timeout: "1s"},
						Failover: config.FailoverConfig{Cooldown: "0s"},
						Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
					},
				},
			},
		},
	}

	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}

	chain := s.GetChain("test-chain")
	if chain == nil {
		t.Fatal("GetChain() returned nil")
	}
	if got := len(chain.Models); got != 2 {
		t.Fatalf("len(chain.Models) = %d, want 2", got)
	}

	bad := chain.Models[0]
	good := chain.Models[1]
	if bad.Cfg.Priority != 1 || good.Cfg.Priority != 2 {
		t.Fatalf("unexpected priorities: bad=%d good=%d", bad.Cfg.Priority, good.Cfg.Priority)
	}

	hc := NewHealthChecker(s, time.Second, time.Second)
	if hc == nil {
		t.Fatal("NewHealthChecker() returned nil")
	}
	s.SetHealthChecker(hc)

	for i := 0; i < 2; i++ {
		allowed, isProbe := s.AllowRequest(bad)
		if !allowed {
			t.Fatalf("AllowRequest(bad) before threshold = false at attempt %d", i+1)
		}
		if isProbe {
			t.Fatalf("AllowRequest(bad) before threshold marked probe at attempt %d", i+1)
		}
		s.RecordResult(bad, 10*time.Millisecond, false)
	}

	if state := s.BreakerStateName(bad); state != "open" {
		t.Fatalf("BreakerStateName(bad) = %q, want open", state)
	}
	if fails := s.ConsecutiveFails(bad); fails != 2 {
		t.Fatalf("ConsecutiveFails(bad) = %d, want 2", fails)
	}

	hc.set(bad, false)
	if health := s.HealthStatus(bad).String(); health != "unhealthy" {
		t.Fatalf("HealthStatus(bad) = %q, want unhealthy", health)
	}
	if state := s.BreakerStateName(bad); state != "open" {
		t.Fatalf("breaker state after health update = %q, want open", state)
	}

	selected := s.SelectModel(chain, nil)
	if selected == nil {
		t.Fatal("SelectModel() returned nil")
	}
	if selected.Cfg.Name != good.Cfg.Name {
		t.Fatalf("SelectModel() = %q, want %q", selected.Cfg.Name, good.Cfg.Name)
	}

	allowed, isProbe := s.AllowRequest(bad)
	if allowed {
		t.Fatalf("AllowRequest(bad) during open duration = allowed, want false")
	}
	if isProbe {
		t.Fatalf("AllowRequest(bad) during open duration marked probe, want false")
	}

	time.Sleep(250 * time.Millisecond)
	if state := s.BreakerStateName(bad); state != "open" {
		t.Fatalf("BreakerStateName(bad) after wait before probe = %q, want open", state)
	}

	allowed, isProbe = s.AllowRequest(bad)
	if !allowed {
		t.Fatalf("AllowRequest(bad) after open duration = false, want true")
	}
	if !isProbe {
		t.Fatalf("AllowRequest(bad) after open duration isProbe = false, want true")
	}
}

// TestBreakerRecoversAfterOpenExpiry 回归测试：熔断 OPEN 冷却到期后，
// 模型必须重新被选路纳入候选（此前 SelectModel 用 State()==OPEN 永久跳过，
// 导致 Allow 永远不在该模型上调用、熔断永久 OPEN，只能重启进程复位）。
func TestBreakerRecoversAfterOpenExpiry(t *testing.T) {
	cfg := &config.Config{
		Breaker: config.BreakerConfig{
			Enabled: true, FailureThreshold: 2, OpenDuration: "100ms", HalfOpenMax: 1,
		},
		Chains: map[string]config.ChainConfig{
			"c": {Models: []config.ModelConfig{{
				Name: "only", Priority: 1,
				Upstream: config.UpstreamConfig{BaseURL: "https://x.example.com", Model: "u", APIKey: "k", Timeout: "1s"},
				Failover: config.FailoverConfig{Cooldown: "0s"},
				Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
			}}},
		},
	}
	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}
	chain := s.GetChain("c")
	m := chain.Models[0]

	// 打到阈值 → OPEN
	for i := 0; i < 2; i++ {
		s.AllowRequest(m)
		s.RecordResult(m, 10*time.Millisecond, false)
	}
	if s.BreakerStateName(m) != "open" {
		t.Fatalf("state = %q, want open", s.BreakerStateName(m))
	}
	// OPEN 冷却窗口内：选路跳过，返回 nil（唯一模型不可用）
	if got := s.SelectModel(chain, nil); got != nil {
		t.Fatalf("SelectModel during open = %v, want nil", got)
	}

	// 冷却到期后：选路必须重新纳入该模型
	time.Sleep(150 * time.Millisecond)
	got := s.SelectModel(chain, nil)
	if got == nil {
		t.Fatal("SelectModel after open expiry = nil, want the model (regression: breaker stuck OPEN forever)")
	}
	// 放行探测 → 探测成功 → 恢复 CLOSED
	allowed, isProbe := s.AllowRequest(got)
	if !allowed || !isProbe {
		t.Fatalf("AllowRequest after expiry = (%v,%v), want (true,true)", allowed, isProbe)
	}
	s.RecordResult(got, 10*time.Millisecond, true)
	if s.BreakerStateName(m) != "closed" {
		t.Fatalf("state after successful probe = %q, want closed", s.BreakerStateName(m))
	}
}
