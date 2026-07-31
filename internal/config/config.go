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
	Server      ServerConfig            `yaml:"server"`
	Log         LogConfig               `yaml:"log"`
	Breaker     BreakerConfig           `yaml:"breaker"`
	HealthCheck HealthCheckConfig       `yaml:"health_check"`
	Protection  ProtectionConfig        `yaml:"protection"` // 新增：服务保护配置
	Chains      map[string]ChainConfig `yaml:"chains"`
}

type ServerConfig struct {
	Addr             string   `yaml:"addr"`
	MaxModelSwitches int      `yaml:"max_model_switches"`
	APIKeys          []string `yaml:"api_keys"` // 服务级鉴权 key 列表，为空则不鉴权
}

// LogConfig 控制调用日志（记录完整调用过程与输入输出，按日期落盘）。
type LogConfig struct {
	Enabled      bool   `yaml:"enabled"`        // 总开关，默认 false
	Dir          string `yaml:"dir"`            // 日志目录，默认 "logs"
	RedactKeys   bool   `yaml:"redact_keys"`    // 脱敏 Authorization / x-api-key，默认 true
	BodyLimit    int    `yaml:"body_limit"`     // 请求/响应体记录上限（字节），超出截断，默认 8192
	LogUpstream  bool   `yaml:"log_upstream"`   // 是否记录转发到上游的请求体，默认 true
	LogRespBody  bool   `yaml:"log_resp_body"`  // 是否记录响应体，默认 true
	MaxSizeMB    int    `yaml:"max_size_mb"`     // 单文件大小上限（MB），超出后按 .1/.2 后缀轮转，默认 100
	MaxAgeDays   int    `yaml:"max_age_days"`   // 日志最大保留天数，超期自动删除，默认 7
	MaxBackups   int    `yaml:"max_backups"`    // 每天最多保留的轮转文件数，超出删最旧，默认 10
	Compress     bool   `yaml:"compress"`       // 轮转后的旧文件自动 gzip 压缩，默认 true
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

// BreakerConfig 是熔断器全局配置（可选，未配则 enabled=false，熔断关闭）。
// 熔断器叠加在 failover.cooldown 之上：单次失败短期冷却由 cooldown 控制，
// 连续失败长期熔断 + 半开探测由本配置控制。
type BreakerConfig struct {
	Enabled          bool   `yaml:"enabled"`            // 总开关
	FailureThreshold int    `yaml:"failure_threshold"`  // 连续失败多少次进入 OPEN
	OpenDuration     string `yaml:"open_duration"`      // OPEN 持续时间，到期转 HALF_OPEN
	HalfOpenMax      int    `yaml:"half_open_max"`      // HALF_OPEN 允许的并发探测请求数
}

// HealthCheckConfig 是后台主动探活配置（可选，未配则禁用）。
// 启用后周期性对每个上游模型发 max_tokens=1 的最小请求，
// 失败反馈熔断器触发 OPEN，成功复位熔断器 CLOSED。
type HealthCheckConfig struct {
	Enabled  bool   `yaml:"enabled"`           // 总开关
	Interval string `yaml:"interval"`          // 探活周期（如 "30s"）
	Timeout  string `yaml:"timeout"`           // 单次探活请求超时（如 "10s"）
}

// ProtectionConfig 服务保护配置（防崩溃三板斧）。
type ProtectionConfig struct {
	Enabled bool `yaml:"enabled"` // 总开关，默认关闭；关闭时不启用并发限流与内存守护

	// 并发限流：防止上游 API 超时导致连接堆积
	MaxConcurrent  int    `yaml:"max_concurrent"`   // 最大并发请求数，0=不限制
	MaxQueueSize   int    `yaml:"max_queue_size"`   // 等待队列长度，0=auto(2x concurrent)
	QueueTimeout   string `yaml:"queue_timeout"`    // 队列等待超时（如 "30s"）
	
	// 内存守护：防止内存泄漏或 OOM
	MaxMemoryMB     int64   `yaml:"max_memory_mb"`     // 最大允许内存（MB），0=不限制
	MemoryWarn      float64 `yaml:"memory_warn"`       // 警告阈值（0.8 = 80%）
	MemoryCritical  float64 `yaml:"memory_critical"`   // 临界阈值（0.9 = 90%），超过后拒绝请求
	MemoryCheckInterval string `yaml:"memory_check_interval"` // 检查周期（如 "10s"）
	
	// panic recovery 自动启用，无需配置
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
	// 日志默认值
	if c.Log.Dir == "" {
		c.Log.Dir = "logs"
	}
	if c.Log.BodyLimit <= 0 {
		c.Log.BodyLimit = 8 * 1024
	}
	if c.Log.MaxSizeMB <= 0 {
		c.Log.MaxSizeMB = 100
	}
	if c.Log.MaxAgeDays <= 0 {
		c.Log.MaxAgeDays = 7
	}
	if c.Log.MaxBackups <= 0 {
		c.Log.MaxBackups = 10
	}
	// 熔断器默认值
	if c.Breaker.Enabled {
		if c.Breaker.FailureThreshold <= 0 {
			c.Breaker.FailureThreshold = 5
		}
		if c.Breaker.OpenDuration == "" {
			c.Breaker.OpenDuration = "60s"
		}
		if _, err := time.ParseDuration(c.Breaker.OpenDuration); err != nil {
			return nil, fmt.Errorf("breaker.open_duration: %w", err)
		}
		if c.Breaker.HalfOpenMax <= 0 {
			c.Breaker.HalfOpenMax = 1
		}
	}
	// 健康检查默认值
	if c.HealthCheck.Enabled {
		if c.HealthCheck.Interval == "" {
			c.HealthCheck.Interval = "30s"
		}
		if _, err := time.ParseDuration(c.HealthCheck.Interval); err != nil {
			return nil, fmt.Errorf("health_check.interval: %w", err)
		}
		if c.HealthCheck.Timeout == "" {
			c.HealthCheck.Timeout = "10s"
		}
		if _, err := time.ParseDuration(c.HealthCheck.Timeout); err != nil {
			return nil, fmt.Errorf("health_check.timeout: %w", err)
		}
	}
	// 服务保护默认值与校验（总开关关闭时整体禁用）
	if c.Protection.Enabled && c.Protection.MaxConcurrent > 0 {
		if c.Protection.MaxQueueSize <= 0 {
			c.Protection.MaxQueueSize = c.Protection.MaxConcurrent * 2
		}
		if c.Protection.QueueTimeout == "" {
			c.Protection.QueueTimeout = "30s"
		}
		if _, err := time.ParseDuration(c.Protection.QueueTimeout); err != nil {
			return nil, fmt.Errorf("protection.queue_timeout: %w", err)
		}
	}
	if c.Protection.Enabled && c.Protection.MaxMemoryMB > 0 {
		if c.Protection.MemoryWarn <= 0 || c.Protection.MemoryWarn > 1 {
			c.Protection.MemoryWarn = 0.8
		}
		if c.Protection.MemoryCritical <= 0 || c.Protection.MemoryCritical > 1 {
			c.Protection.MemoryCritical = 0.9
		}
		if c.Protection.MemoryCheckInterval == "" {
			c.Protection.MemoryCheckInterval = "10s"
		}
		if _, err := time.ParseDuration(c.Protection.MemoryCheckInterval); err != nil {
			return nil, fmt.Errorf("protection.memory_check_interval: %w", err)
		}
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
