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
