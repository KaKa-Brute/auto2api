# auto2api

优先级自动切换的 OpenAI 兼容 API 网关。多套命名链、按优先级 fallback、同模型退避重试、跨模型故障转移、SSE 流式透传。

## 核心特性

- **命名链**：客户端在 `model` 字段填链名（如 `auto` / `cn`），即可走对应链的优先级组
- **两级 fallback**
  - 层 1：同模型退避重试（`retryable_status`，指数退避封顶 3s）
  - 层 2：跨优先级模型故障转移（`trigger_status`，耗尽重试后切下一优先级）
- **内存冷却**：失败模型按 `cooldown` 时长置入冷却，`PickNext` 自动跳过
- **模型映射**：客户端传链名，转发时改写为上游真实模型名
- **SSE 管道式透传**：逐行 Flush、空闲超时、keepalive ping、禁用 nginx 缓冲
- **鉴权注入**：支持 `Authorization`(默认) / `x-api-key` / `x-goog-api-key` / 自定义头
- **服务级 API Key**：可配置多个 key，客户端调用须携带其中之一；未配置则不鉴权
- **头部白名单**：刻意剥离 `authorization` / `cookie` 等，避免泄露客户端凭据
- **环境变量展开**：`api_key: "${DEEPSEEK_KEY}"` 自动替换

## 快速开始

```bash
go run . -config config.yaml
```

默认监听 `:8080`，启动日志会列出所有链名。

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/chat/completions` | OpenAI 兼容聊天接口，`model` 字段填链名 |
| POST | `/chat/completions` | 无 `/v1` 前缀的兼容路径 |
| GET  | `/v1/models` | 列出所有链名（OpenAI /v1/models 格式） |
| GET  | `/v1/health` | 每条链各模型的实时健康（优先级、上游模型、是否冷却中） |

### 调用示例

```bash
# 走 auto 链（gpt -> glm -> claude 三级 fallback）
# Authorization 填配置文件 server.api_keys 中的某个 key
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-auto2api-1127741X" \
  -d '{
    "model": "auto",
    "messages": [{"role":"user","content":"ping"}],
    "stream": false
  }'

# 流式
curl -N http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-auto2api-1127741X" \
  -d '{"model":"auto","messages":[{"role":"user","content":"hi"}],"stream":true}'

# 查看健康状态（无需鉴权）
curl http://localhost:8080/v1/health
```

> **两层鉴权**：客户端的 `Authorization` 用于**服务级鉴权**（须匹配 `server.api_keys`），通过后被剥离；转发上游时由 [setAuth](file:///d:\desktop\auto2api\internal\gateway\forwarder.go#L114-L126) 用每个模型配置的 `api_key` 重新注入。

## 配置文件

```yaml
server:
  addr: ":8080"
  max_model_switches: 5      # 单次请求最多切换模型次数（兜底防雪崩）
  api_keys:                  # 服务级鉴权 key 列表，为空则不鉴权
    - "sk-auto2api-1127741X"   # 客户端 Authorization: Bearer 须匹配其中之一
    - "${AUTO2API_KEY2}"       # 支持 ${ENV} 展开，可配多个方便轮换

chains:
  auto:                       # 链名 = 客户端 model 字段值
    - name: gpt               # 优先级 1（最高）
      priority: 1
      upstream:
        provider: openai       # 仅标记，不影响转发
        base_url: "https://oneapi.letright.com.cn/v1"  # 自动追加 /v1/chat/completions
        model: "openai-gpt-5.6-sol"   # 上游真实模型名
        api_key: "sk-xxx"             # 或 ${ENV}
        auth_header: "Authorization"  # 默认；或 x-api-key / x-goog-api-key
        timeout: "120s"
      retry:
        count: 2                       # 同模型内重试次数（不含首次）
        backoff: ["300ms","600ms","1.2s"]   # 指数退避，封顶 3s
        retryable_status: [429,500,502,503,529]
      failover:
        trigger_status: [401,403,429,500,502,503,529]  # 触发切下一优先级
        cooldown: "60s"                # 失败后冷却不可用时间
      stream:
        idle_timeout: "30s"            # SSE 无数据超时
        keepalive: "5s"                 # SSE ping 间隔
    # ...更多优先级
```

### 字段说明

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `server.addr` | `:8080` | 监听地址 |
| `server.max_model_switches` | `5` | 单请求最多切换模型次数 |
| `server.api_keys` | `[]` | 服务级鉴权 key 列表，为空则不鉴权 |
| `upstream.timeout` | `120s` | 上游请求超时 |
| `upstream.auth_header` | `Authorization` | 鉴权头类型 |
| `failover.cooldown` | `60s` | 失败模型冷却时长 |
| `stream.idle_timeout` | `30s` | SSE 空闲超时 |
| `stream.keepalive` | `5s` | SSE keepalive 间隔 |

## fallback 编排逻辑

```
请求 model="auto"
  ↓ 选优先级 1 (gpt)
  │ 层1: 同模型退避重试
  │   429/500/502/503/529 → 按 backoff 退避重试 (count 次)
  │   重试耗尽 ↓
  │ 层2: 跨优先级故障转移
  │   trigger_status 命中 → 标记 cooldown，排除当前模型
  ↓ 选优先级 2 (glm) — 跳过冷却中的
  │ ... 同上
  ↓ 选优先级 3 (claude) — 兜底
  ↓ 全部耗尽 → 502 upstream_unavailable
```

**状态码处理顺序**（关键）：`retryable` 且仍有重试次数 → 退避重试；否则 `failover` 触发 → 转移；否则视为客户端错误（如 400/404）原样返回。

这样 `429`（同时是 retryable + failover）会先重试、耗尽后再转移；`401/403/500`（仅 failover）则立即转移。

## 项目结构

```
auto2api/
├── main.go                       # 入口：加载配置，启动 gin 服务
├── config.yaml                    # 多链 + 优先级配置
├── internal/
│   ├── config/
│   │   └── config.go              # YAML 解析、默认值、${ENV} 展开
│   └── gateway/
│       ├── handler.go             # 路由 + fallback 编排（两级机制）
│       ├── scheduler.go           # 链管理、冷却表、优先级选择
│       └── forwarder.go           # 上游转发、模型改写、SSE 管道
└── go.mod
```

## 设计要点

- **手动构造上游 `*http.Request`**（非 `httputil.ReverseProxy`）：精确控制头部白名单与模型改写
- **`http.Client.Timeout=0`**：流式安全，由请求级 `context.WithTimeout` 控制截止
- **`BodyCommitted` 标记**：流式中途失败时响应体已写，不可再 fallback，直接结束
- **冷却键 `chain::name`**：按链隔离，避免跨链同名模型互相污染
- **单行 64MB 缓冲**：`bufio.Scanner` 容纳上游长行 SSE
