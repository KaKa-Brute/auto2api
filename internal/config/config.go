// Package config 解析 auto2api 的 YAML 配置（多套命名链 + 每链多优先级模型）。
package config

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 是整个配置文件的根结构。
type Config struct {
	Server ServerConfig            `yaml:"server"`
	Chains map[string]ChainConfig `yaml:"chains"`
}

type ServerConfig struct {
	Addr             string   `yaml:"addr"`
	MaxModelSwitches int      `yaml:"max_model_switches"`
	APIKeys          []string `yaml:"api_keys"` // 服务级鉴权 key 列表，为空则不鉴权
}

// ChainConfig 是一条命名优先级链，外部按链名调用。
type ChainConfig struct {
	Models []ModelConfig `yaml:"models"`
}

type ModelConfig struct {
	Name     string         `yaml:"name"`
	Priority int            `yaml:"priority"`
	Upstream UpstreamConfig `yaml:"upstream"`
	Retry    RetryConfig    `yaml:"retry"`
	Failover FailoverConfig `yaml:"failover"`
	Stream   StreamConfig   `yaml:"stream"`
}

type UpstreamConfig struct {
	Provider   string `yaml:"provider"`   // 仅作标记，不影响转发逻辑
	BaseURL    string `yaml:"base_url"`   // 上游 API 根地址，自动追加 /v1/chat/completions
	Model      string `yaml:"model"`       // 上游真实模型名（模型映射目标）
	APIKey     string `yaml:"api_key"`    // 支持 ${ENV} 展开
	AuthHeader string `yaml:"auth_header"` // Authorization（默认）/ x-api-key / x-goog-api-key
	Timeout    string `yaml:"timeout"`
}

type RetryConfig struct {
	Count           int      `yaml:"count"`            // 同模型内重试次数（不含首次）
	Backoff         []string `yaml:"backoff"`          // 指数退避序列
	RetryableStatus []int    `yaml:"retryable_status"` // 同模型重试的状态码
}

type FailoverConfig struct {
	TriggerStatus []int  `yaml:"trigger_status"` // 触发切换下一优先级模型的状态码
	Cooldown      string `yaml:"cooldown"`       // 失败后冷却不可用时间
}

type StreamConfig struct {
	IdleTimeout string `yaml:"idle_timeout"` // SSE 无数据超时
	Keepalive   string `yaml:"keepalive"`   // SSE keepalive ping 间隔
}

var envRe = regexp.MustCompile(`\$\{([A-Z0-9_]+)\}`)

// ExpandEnv 把 ${ENV} 替换成环境变量值。
func ExpandEnv(s string) string {
	return envRe.ReplaceAllStringFunc(s, func(m string) string {
		name := m[2 : len(m)-1]
		return os.Getenv(name)
	})
}

// ParseDurations 把字符串列表解析成 time.Duration 列表。
func ParseDurations(ss []string) ([]time.Duration, error) {
	out := make([]time.Duration, 0, len(ss))
	for _, s := range ss {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		out = append(out, d)
	}
	return out, nil
}

// Load 读取并校验配置，填充默认值，按 priority 升序排序每条链的模型。
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if c.Server.Addr == "" {
		c.Server.Addr = ":8080"
	}
	if c.Server.MaxModelSwitches <= 0 {
		c.Server.MaxModelSwitches = 5
	}
	// 展开 ${ENV} 并去空
	keys := c.Server.APIKeys[:0]
	for _, k := range c.Server.APIKeys {
		if k = strings.TrimSpace(ExpandEnv(k)); k != "" {
			keys = append(keys, k)
		}
	}
	c.Server.APIKeys = keys
	if len(c.Chains) == 0 {
		return nil, fmt.Errorf("no chains configured")
	}

	for name := range c.Chains {
		chain := c.Chains[name]
		if len(chain.Models) == 0 {
			return nil, fmt.Errorf("chain %q has no models", name)
		}
		sort.Slice(chain.Models, func(i, j int) bool {
			return chain.Models[i].Priority < chain.Models[j].Priority
		})
		for i := range chain.Models {
			m := &chain.Models[i]
			if m.Name == "" {
				m.Name = fmt.Sprintf("%s-%d", name, m.Priority)
			}
			if m.Upstream.BaseURL == "" || m.Upstream.Model == "" || m.Upstream.APIKey == "" {
				return nil, fmt.Errorf("chain %q model %q: base_url/model/api_key 不可为空", name, m.Name)
			}
			m.Upstream.APIKey = ExpandEnv(m.Upstream.APIKey)
			if m.Upstream.AuthHeader == "" {
				m.Upstream.AuthHeader = "Authorization"
			}
			if m.Upstream.Timeout == "" {
				m.Upstream.Timeout = "120s"
			}
			if _, err := time.ParseDuration(m.Upstream.Timeout); err != nil {
				return nil, fmt.Errorf("chain %q model %q: bad timeout %q: %w", name, m.Name, m.Upstream.Timeout, err)
			}
		}
		c.Chains[name] = chain
	}
	return &c, nil
}
