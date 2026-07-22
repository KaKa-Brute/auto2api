package gateway

import (
	"encoding/json"
	"testing"
	"time"

	"auto2api/internal/config"
)

// TestProbeBodyUsesRealUpstreamModel 验证探针请求体的 model 字段
// 等于模型配置的真实上游模型名，而不是硬编码 "ping"。
// 这是最初 bug 的根因：上游校验模型名会回 404/400 → 被误判为不可用 → 熔断 OPEN。
func TestProbeBodyUsesRealUpstreamModel(t *testing.T) {
	cfg := &config.Config{
		Breaker: config.BreakerConfig{Enabled: true, FailureThreshold: 5, OpenDuration: "200ms", HalfOpenMax: 1},
		Chains: map[string]config.ChainConfig{
			"test": {Models: []config.ModelConfig{
				{
					Name:     "m1",
					Priority: 1,
					Upstream: config.UpstreamConfig{BaseURL: "https://example.com", Model: "gpt-4o-real", APIKey: "k", Timeout: "1s"},
					Failover: config.FailoverConfig{Cooldown: "0s"},
					Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
				},
			}},
		},
	}
	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	hc := NewHealthChecker(s, time.Second, time.Second)
	if hc == nil {
		t.Fatal("NewHealthChecker returned nil")
	}
	m := s.GetChain("test").Models[0]

	body := hc.probeBody(m)
	var p map[string]json.RawMessage
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatalf("unmarshal probe body: %v", err)
	}
	var got string
	if err := json.Unmarshal(p["model"], &got); err != nil {
		t.Fatalf("unmarshal model field: %v", err)
	}
	if got != "gpt-4o-real" {
		t.Fatalf("probe body model = %q, want %q (real upstream model, not \"ping\")", got, "gpt-4o-real")
	}
}

// TestHealthCheck429DoesNotTripBreaker 验证 429（限流）不计入熔断失败，
// 也不标记为 unhealthy。否则高负载的正常模型会被误摘。
func TestHealthCheck429DoesNotTripBreaker(t *testing.T) {
	cfg := &config.Config{
		Breaker: config.BreakerConfig{Enabled: true, FailureThreshold: 2, OpenDuration: "200ms", HalfOpenMax: 1},
		Chains: map[string]config.ChainConfig{
			"test": {Models: []config.ModelConfig{
				{
					Name:     "m1",
					Priority: 1,
					Upstream: config.UpstreamConfig{BaseURL: "https://example.com", Model: "real-model", APIKey: "k", Timeout: "1s"},
					Failover: config.FailoverConfig{Cooldown: "0s"},
					Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
				},
			}},
		},
	}
	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	hc := NewHealthChecker(s, time.Second, time.Second)
	s.SetHealthChecker(hc)
	m := s.GetChain("test").Models[0]

	// 连续 5 次 429：若误判失败，failure_threshold=2 早就该 OPEN 了
	for i := 0; i < 5; i++ {
		hc.update(m, 429, false)
	}
	if state := s.BreakerStateName(m); state != "closed" {
		t.Fatalf("after 5x 429, breaker = %q, want closed (429 must not trip breaker)", state)
	}
	if fails := s.ConsecutiveFails(m); fails != 0 {
		t.Fatalf("after 5x 429, consecutive_fails = %d, want 0", fails)
	}
	if st := s.HealthStatus(m); st != HealthUnknown {
		t.Fatalf("after 5x 429, health = %s, want unknown (429 must not mark unhealthy)", st.String())
	}
}

// TestHealthCheckProbeFailureDoesNotTripBreaker 验证探针失败（连接不可达）不直接驱动熔断器 OPEN。
// 探针失败只更新 HealthStatus 为 unhealthy 供路由降级；
// 熔断器的失败计数由真实业务请求（RecordResult）驱动。
func TestHealthCheckProbeFailureDoesNotTripBreaker(t *testing.T) {
	cfg := &config.Config{
		Breaker: config.BreakerConfig{Enabled: true, FailureThreshold: 2, OpenDuration: "200ms", HalfOpenMax: 1},
		Chains: map[string]config.ChainConfig{
			"test": {Models: []config.ModelConfig{
				{
					Name:     "m1",
					Priority: 1,
					Upstream: config.UpstreamConfig{BaseURL: "https://example.com", Model: "real-model", APIKey: "k", Timeout: "1s"},
					Failover: config.FailoverConfig{Cooldown: "0s"},
					Stream:   config.StreamConfig{IdleTimeout: "1s", Keepalive: "1s"},
				},
			}},
		},
	}
	s, err := NewScheduler(cfg)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	hc := NewHealthChecker(s, time.Second, time.Second)
	s.SetHealthChecker(hc)
	m := s.GetChain("test").Models[0]

	// 2 次连接失败（status=0）：不应触发熔断 OPEN
	hc.update(m, 0, false)
	hc.update(m, 0, false)
	if state := s.BreakerStateName(m); state != "closed" {
		t.Fatalf("after 2x probe failure, breaker = %q, want closed (probe failure must not trip breaker)", state)
	}
	if fails := s.ConsecutiveFails(m); fails != 0 {
		t.Fatalf("after 2x probe failure, consecutive_fails = %d, want 0 (probe failure must not count)", fails)
	}
	if st := s.HealthStatus(m); st != HealthUnhealthy {
		t.Fatalf("after 2x probe failure, health = %s, want unhealthy", st.String())
	}
}
