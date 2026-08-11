# auto2api

优先级自动切换的 OpenAI / Claude 兼容 API 网关。多套命名链、按优先级 fallback、同模型退避重试、跨模型故障转移、熔断器、主动健康检查、SSE 流式透传、调用日志按日期落盘、服务保护（并发限流 / 内存守护 / 异常恢复 / 优雅关闭）。

> **双运行时**：提供 **Go** 和 **Python** 两套功能完全对齐、彼此独立的实现，共用同一份 `config.yaml`。服务器上装了哪个运行时就用哪个启动，无需同时安装 Go 和 Python。所有能力（fallback、熔断、健康检查、Claude/OpenAI 转换、调用日志、服务保护）两版一致。

## 核心特性

- **Claude（Anthropic Messages API）兼容**：`POST /v1/messages` 入站，请求/响应与流式 SSE 自动做 Claude↔OpenAI 格式转换，Claude SDK 可直连
- **OpenAI 兼容**：`POST /v1/chat/completions` 透传
- **命名链**：客户端在 `model` 字段填链名（如 `auto` / `cn` / `coding`），即可走对应链的优先级组
- **两级 fallback**
  - 层 1：同模型退避重试（`retryable_status`，指数退避封顶 3s）
  - 层 2：跨优先级模型故障转移（`trigger_status`，耗尽重试后切下一优先级）
- **动态路由**：在可用模型集合内按 优先级 > 健康 > 成功率 > 延迟(EMA) 综合排序选最优
- **Token 统计（按日期分片）**：自动提取上游 `usage` 字段，按 UTC 日期分片存储每个模型的输入/输出 token 累计；`/v1/health` 显示今日用量，调用日志记录每次请求的 token 数；历史数据保留 30 天，每天凌晨 1 点自动清理过期分片
- **熔断器（可选）**：连续失败达阈值 → OPEN 长期摘除；冷却到期转 HALF_OPEN 半开探测；探测成功 → CLOSED 恢复
- **主动健康检查（可选）**：后台周期性对每个上游发轻量探测请求，探测成功复位熔断器
- **内存冷却**：失败模型按 `cooldown` 时长置入冷却，选择时自动跳过
- **模型映射**：客户端传链名，转发时改写为上游真实模型名
- **SSE 管道式透传**：逐行推送、空闲超时、keepalive ping、禁用 nginx 缓冲
- **鉴权注入**：支持 `Authorization`(默认) / `x-api-key` / `x-goog-api-key` / 自定义头
- **服务级 API Key**：可配置多个 key，客户端调用须携带其中之一；未配置则不鉴权
- **头部白名单**：刻意剥离 `authorization` / `cookie` 等，避免泄露客户端凭据
- **调用日志**：记录完整调用过程与输入输出（请求/上游请求/响应/每次尝试/token 用量），按日期落盘 `logs/calls-YYYY-MM-DD.log`
- **环境变量展开**：`api_key: "${DEEPSEEK_KEY}"` 自动替换
- **服务保护（可选）**：面向进程稳定性的防崩溃三板斧 + 优雅关闭（Go / Python 均实现）
  - **并发限流**：超过 `max_concurrent` 的请求进等待队列，队列满或等待超时立即返回 `503 + Retry-After`，防止上游超时导致连接无限堆积
  - **内存守护**：后台周期检测进程内存，超警告阈值主动 GC，超临界阈值进入降级模式（拒新请求 + 强制 GC），内存回落后自动恢复，防内存泄漏 / OOM
  - **异常恢复**：捕获所有未处理异常（Go panic / Python exception），记录完整堆栈与计数并返回 500，进程不退出（自动启用，无需配置）
  - **优雅关闭**：收到 `SIGINT`/`SIGTERM` 停止接收新请求，等待在途请求完成后再退出，并停止后台任务
  - **连接池上限**：上游 HTTP 客户端限制每主机连接数与空闲连接回收，防连接/句柄泄漏

## 快速开始

本项目提供 **Go** 和 **Python** 两套功能完全对齐、彼此独立的实现，共用同一份 `config.yaml`。服务器上装了哪个运行时就用哪个启动，二者互不依赖。

### 方式一：Python 模式（默认，无需安装 Go）

```bash
cd python
pip install -r requirements.txt          # 首次运行安装依赖：starlette / uvicorn / httpx / PyYAML / psutil
python main.py -config ../config.yaml     # -config 指向配置文件（可用相对/绝对路径）
```

### 方式二：Go 模式（无需安装 Python）

```bash
go run . -config config.yaml
# 或编译后运行
go build -o auto2api . && ./auto2api -config config.yaml
```

两种方式默认都监听 `config.yaml` 里的 `server.addr`（缺省 `:8080`），启动日志会列出所有链名与开关状态，例如：

```
concurrency limiter enabled: max=200 queue=400 timeout=30s
[memguard] started: max=1024MB, warn=80%, critical=90%, interval=10s
auto2api(python) listening, chains: ['cn', 'coding'], breaker: enabled, call_log: True
health checker enabled: interval=60s timeout=10s
Uvicorn running on http://0.0.0.0:8686
```

> **提示**：`log.dir` 用相对路径时，日志目录相对于**启动时的工作目录**。例如从 `python/` 目录启动则日志落在 `python/logs/`，从仓库根启动则落在 `logs/`。

所有端点、fallback 编排、Claude/OpenAI 转换、熔断、健康检查、调用日志行为在两种模式下完全一致。

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/chat/completions` | OpenAI 兼容聊天接口，`model` 字段填链名 |
| POST | `/chat/completions` | 无 `/v1` 前缀的兼容路径 |
| POST | `/v1/messages` | **Claude（Anthropic Messages API）兼容**，`model` 字段填链名 |
| POST | `/messages` | 无 `/v1` 前缀的 Claude 兼容路径 |
| GET  | `/v1/models` | 列出所有链名（OpenAI /v1/models 格式） |
| GET  | `/v1/health` | 每条链各模型的实时健康：优先级、上游模型、是否冷却中、熔断状态、连续失败数、探活健康、延迟 EMA、成功率、总请求/失败数、**今日 token 用量**（prompt_tokens / completion_tokens / total_tokens）；含 `_protection`（并发数、拒绝/超时数、内存用量、降级状态、异常计数）（无需鉴权） |

### 调用示例

```bash
# 走 auto 链（gpt -> glm -> claude 三级 fallback）—— OpenAI 格式
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-auto2api-1127741X" \
  -d '{
    "model": "auto",
    "messages": [{"role":"user","content":"ping"}],
    "stream": false
  }'

# Claude（Anthropic Messages API）格式
curl http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: sk-auto2api-1127741X" \
  -H "anthropic-version: 2023-06-01" \
  -d '{
    "model": "auto",
    "max_tokens": 1024,
    "system": "你是有用的助手",
    "messages": [{"role":"user","content":"ping"}],
    "stream": false
  }'

# Claude 流式
curl -N http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -H "x-api-key: sk-auto2api-1127741X" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"auto","max_tokens":1024,"messages":[{"role":"user","content":"hi"}],"stream":true}'

# 查看健康状态（无需鉴权）
curl http://localhost:8080/v1/health
```

> **Claude 兼容说明**：`/v1/messages` 入站为 Claude 格式，内部转成 OpenAI 格式走上游（上游须为 OpenAI 兼容端点），响应再转回 Claude 格式。支持 system 顶层、文本/图片内容块、工具调用（tool_use/tool_result）、stop_sequences 映射，流式 SSE 事件（message_start / content_block_delta / message_delta / message_stop）全套转换。

> **两层鉴权**：客户端的 `Authorization`/`x-api-key` 用于**服务级鉴权**（须匹配 `server.api_keys`），通过后被剥离；转发上游时由 [setAuth](file:///d:\desktop\auto2api\internal\gateway\forwarder.go) 用每个模型配置的 `api_key` 重新注入。

## 调用日志

在 `config.yaml` 开启后，每次调用都会以 JSONL 形式写入 `logs/calls-YYYY-MM-DD.log`（跨天自动轮转），包含：

- 请求 ID、时间戳、方法、路径、客户端 IP、已脱敏头部
- 原始请求体（Claude/OpenAI 原样）+ 转发上游的请求体（已做模型改写）
- 链名、是否流式、出站格式（openai/claude）
- 每次模型尝试：模型名、优先级、上游模型、状态码、结局（success/retry/failover/client_error/error）、耗时、首 token 耗时、**输入/输出 token 数**、错误
- 最终响应状态码、响应体（截断至 `body_limit`）、流式 SSE 数据块数、总耗时、**本次请求总 token 数**

日志字段已脱敏 `Authorization`/`x-api-key`，避免凭据落盘。

**日志示例**：

```json
{
  "id": "req_a1b2c3d4e5f6g7h8",
  "timestamp": "2026-08-11T12:34:56.789Z",
  "method": "POST",
  "path": "/v1/chat/completions",
  "client_ip": "127.0.0.1",
  "chain": "auto-deepseek-v4-flash",
  "format": "openai",
  "stream": false,
  "attempts": [
    {
      "model": "coding-plan",
      "priority": 1,
      "upstream_model": "deepseek-v4-flash",
      "status": 200,
      "outcome": "success",
      "duration_ms": 234,
      "first_token_ms": 89,
      "prompt_tokens": 123,
      "completion_tokens": 456
    }
  ],
  "resp_status": 200,
  "duration_ms": 240,
  "prompt_tokens": 123,
  "completion_tokens": 456
}
```

## 配置文件

```yaml
server:
  addr: ":8080"
  max_model_switches: 5      # 单次请求最多切换模型次数（兜底防雪崩）
  api_keys:                  # 服务级鉴权 key 列表，为空则不鉴权
    - "sk-auto2api-1127741X"   # 客户端 Authorization: Bearer 须匹配其中之一
    - "${AUTO2API_KEY2}"       # 支持 ${ENV} 展开，可配多个方便轮换

# 调用日志：记录完整调用过程与输入输出，按日期落盘
log:
  enabled: true                 # 总开关
  dir: "logs"                   # 日志目录（按日期轮转 calls-YYYY-MM-DD.log）
  redact_keys: true             # 脱敏 Authorization / x-api-key
  body_limit: 65536             # 请求/响应体记录上限（字节），超出截断
  log_upstream: true            # 是否记录转发到上游的请求体

# 熔断器（可选，默认关闭）。叠加在 failover.cooldown 之上：
# 单次失败 → cooldown 短期冷却；连续失败达阈值 → 熔断 OPEN，期间所有请求直接跳过该模型，
# 冷却到期转 HALF_OPEN 仅放行少量探测请求，探测成功 → CLOSED，探测失败 → 重新 OPEN。
breaker:
  enabled: false                # 总开关
  failure_threshold: 3          # 连续失败多少次进入 OPEN
  open_duration: "60s"          # OPEN 持续时间，到期转 HALF_OPEN
  half_open_max: 1              # HALF_OPEN 允许的并发探测请求数（通常 1）

# 后台主动健康检查（可选，默认关闭）。周期性对每个上游发轻量探测请求（空 messages），
# 探针失败不驱动熔断器（由真实请求驱动），探针成功复位熔断器 CLOSED；不计入业务指标。
health_check:
  enabled: false                # 总开关
  interval: "60s"               # 探活周期
  timeout: "10s"                # 单次探活请求超时

# 服务保护（可选，Go / Python 均支持，默认关闭）。总开关 enabled 关闭时下列功能均不启用。防崩溃三板斧：
#   1. 并发限流：防上游超时导致连接无限堆积
#   2. 内存守护：防内存泄漏/OOM，超阈值自动降级（Python 版需安装 psutil）
#   3. 异常恢复：自动启用，捕获未捕获异常防进程退出（无需配置）
protection:
  enabled: false                # 总开关，默认关闭；置 true 才启用并发限流与内存守护
  max_concurrent: 200           # 最大并发处理请求数，0=不限制；超出进队列等待
  max_queue_size: 400           # 等待队列长度，0=自动(2x max_concurrent)；队列满直接 503
  queue_timeout: "30s"          # 队列等待超时，超时返回 503 + Retry-After
  max_memory_mb: 1024           # 进程最大允许内存(MB)，0=不监控；建议设为容器上限的 80%
  memory_warn: 0.8              # 警告阈值(占比)，超过则主动 GC + 告警
  memory_critical: 0.9          # 临界阈值，超过则拒新请求(503)并强制 GC，回落后自动恢复
  memory_check_interval: "10s"  # 内存检查周期

chains:
  auto:                       # 链名 = 客户端 model 字段值
    models:                   # 也可省略 models: 直接写模型列表（两种写法均支持）
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
| `log.enabled` | `false` | 调用日志总开关 |
| `log.dir` | `logs` | 日志目录，按日期轮转 |
| `log.redact_keys` | `true` | 脱敏鉴权头部 |
| `log.body_limit` | `65536` | 请求/响应体记录上限（字节） |
| `log.log_upstream` | `true` | 是否记录转发上游的请求体 |
| `breaker.enabled` | `false` | 熔断器总开关 |
| `breaker.failure_threshold` | `5` | 连续失败多少次进入 OPEN |
| `breaker.open_duration` | `60s` | OPEN 持续时间，到期转 HALF_OPEN |
| `breaker.half_open_max` | `1` | HALF_OPEN 并发探测请求数 |
| `health_check.enabled` | `false` | 主动健康检查总开关 |
| `health_check.interval` | `30s` | 探活周期 |
| `health_check.timeout` | `10s` | 单次探活请求超时 |
| `protection.enabled` | `false` | 服务保护总开关，关闭则并发限流与内存守护均不启用 |
| `protection.max_concurrent` | `0`（禁用） | 最大并发请求数 |
| `protection.max_queue_size` | `2x concurrent` | 等待队列长度 |
| `protection.queue_timeout` | `30s` | 队列等待超时 |
| `protection.max_memory_mb` | `0`（禁用） | 进程最大允许内存(MB)，Python 版需 psutil |
| `protection.memory_warn` | `0.8` | 内存警告阈值(占比) |
| `protection.memory_critical` | `0.9` | 内存临界阈值(占比) |
| `protection.memory_check_interval` | `10s` | 内存检查周期 |
| `upstream.timeout` | `120s` | 上游请求超时 |
| `upstream.auth_header` | `Authorization` | 鉴权头类型 |
| `failover.cooldown` | `60s` | 失败模型冷却时长 |
| `stream.idle_timeout` | `30s` | SSE 空闲超时 |
| `stream.keepalive` | `5s` | SSE keepalive 间隔 |

## fallback 编排逻辑

```
请求 model="auto"（/v1/chat/completions 或 /v1/messages）
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

一次真实故障转移在调用日志里的 `attempts` 字段体现为（优先级 1 上游报错 → 切优先级 2 成功）：

```json
"attempts": [
  {"model": "bad",  "priority": 1, "status": 503, "outcome": "failover"},
  {"model": "good", "priority": 2, "status": 200, "outcome": "success"}
]
```

## 项目结构

```
auto2api/
├── config.yaml                    # 多链 + 优先级 + 日志配置（Go / Python 共用）
├── config.example.yaml            # 配置模板
│
├── main.go                        # Go 入口：加载配置，启动 gin 服务
├── go.mod
├── internal/                      # —— Go 实现 ——
│   ├── config/config.go           # YAML 解析、默认值、${ENV} 展开
│   └── gateway/
│       ├── handler.go             # 路由 + fallback 编排 + 调用日志集成
│       ├── scheduler.go           # 链管理、冷却表、优先级选择、动态路由
│       ├── forwarder.go           # 上游转发、模型改写、SSE 管道、token 提取
│       ├── claude.go              # Claude API 请求/响应/流式格式转换、token 提取
│       ├── breaker.go             # 熔断器三态机
│       ├── metrics.go             # EMA 延迟/成功率指标、按日期分片 token 统计
│       ├── healthcheck.go         # 后台主动探活
│       ├── limiter.go             # 并发限流器（令牌桶 + 等待队列）
│       ├── memguard.go            # 内存守护（周期检测 + 自动降级 + GC）
│       ├── recovery.go            # panic recovery 中间件
│       └── call_logger.go         # 按日期轮转调用日志 + 响应录制 + token 字段
│
└── python/                        # —— Python 实现（与 Go 功能对齐，完全独立）——
    ├── main.py                    # Python 入口：uvicorn + Starlette
    ├── requirements.txt           # starlette / uvicorn / httpx / PyYAML / psutil
    └── auto2api/
        ├── config.py              # YAML 解析、默认值、${ENV} 展开、时长解析
        ├── handler.py             # 路由 + 两级 fallback 编排 + 调用日志 + token 记录
        ├── scheduler.py           # 链管理、冷却表、优先级选择、动态路由
        ├── forwarder.py           # 上游转发（httpx）、模型改写、SSE 管道
        ├── claude.py              # Claude API 请求/响应/流式格式转换
        ├── breaker.py             # 熔断器三态机
        ├── metrics.py             # EMA 延迟/成功率指标、按日期分片 token 统计
        ├── healthcheck.py         # 后台主动探活（asyncio）
        ├── limiter.py             # 并发限流器（asyncio 信号量 + 等待队列）
        ├── memguard.py            # 内存守护（asyncio 后台任务 + 降级 + GC）
        ├── recovery.py            # 异常恢复中间件（Starlette）
        ├── call_logger.py         # 按日期轮转调用日志 + 响应录制 + token 字段
        └── server.py              # Starlette 应用装配、路由注册、生命周期、token 清理
```

> Go 版与 Python 版实现相同的端点与行为，可按服务器已安装的运行时任选其一启动，无需同时安装 Go 和 Python。

## 设计要点

**通用**

- **头部白名单 + 鉴权注入**：手动构造上游请求（非反向代理），精确控制透传头部与模型改写
- **冷却键 `chain::name`**：按链隔离，避免跨链同名模型互相污染
- **出站格式标记**：`/v1/messages` 置 `outbound_format=claude`，forwarder 据此分流 Claude 转换管线
- **响应录制器**：透写给客户端的同时缓存，请求结束时把最终输出（含格式转换后）记入日志
- **Token 统计按日期分片**：
  - `Metrics` 内存中维护 `map[date]*tokenShard`（Go）/ `dict[date, TokenShard]`（Python），按 UTC 日期自动分片
  - 从上游响应 JSON（非流式）或 SSE 数据块（流式）提取 `usage.prompt_tokens` / `completion_tokens`
  - `Tokens()` 返回今日累计，`TokensForDate(date)` 查询历史，`CleanupOldTokens(keepDays)` 清理过期分片
  - 每天凌晨 1 点 UTC 后台任务自动清理 30 天前数据，减少内存占用

**Go 版**

- 手动构造上游 `*http.Request`（非 `httputil.ReverseProxy`）
- `http.Client.Timeout=0`：流式安全，由请求级 `context.WithTimeout` 控制截止
- `BodyCommitted` 标记：流式中途失败时响应体已写，不可再 fallback，直接结束
- 单行 64MB 缓冲：`bufio.Scanner` 容纳上游长行 SSE
- **Token 提取**：OpenAI 流式用 `sniffSSEUsage` 逐行解析 SSE 末块 usage；非流式读完 body 后 `extractOpenAIUsageFromBody`；Claude 流式在 `claudeStreamState` 维护 `inputTokens`/`outputTokens`
- **并发限流**用带缓冲 channel 作信号量（令牌桶），`Acquire/Release` 无轮询、无 goroutine 泄漏；等待队列超限或超时快速失败
- **内存守护**后台 goroutine 读 `runtime.MemStats.Alloc`，用原子标志无锁判断降级状态，降级中 `AllowRequest` 直接拒绝
- **panic recovery** 自定义中间件替代 `gin.Recovery()`，记录 `debug.Stack()` 全量堆栈并累加计数
- **优雅关闭**用 `http.Server.Shutdown(ctx)` + 信号监听，关闭时同步停止内存守护与健康检查后台任务
- **连接池**自定义 `http.Transport`：`MaxConnsPerHost=100`、`MaxIdleConnsPerHost=50`、`IdleConnTimeout=90s`

**Python 版**

- 基于 `Starlette + uvicorn + httpx`（asyncio）
- 转发用 `httpx.stream(...)` 先拿状态码：非 2xx 仍可 fallback，2xx 才提交响应体（等价于 Go 的 `BodyCommitted` 语义）
- 共享 `httpx.AsyncClient`，并在事件循环变化时惰性重建连接池；连接池上限 `max_connections=100`、`max_keepalive_connections=50`、`keepalive_expiry=90s`
- 健康检查为 asyncio 后台任务，随应用 `on_startup/on_shutdown` 生命周期启停
- **Token 清理**：`server.py` 中 `asyncio.create_task(cleanup_task())` 启动后台任务，每天凌晨 1 点 UTC 调用 `scheduler.cleanup_old_tokens(30)`
- **并发限流**用 `asyncio.Semaphore` 作令牌桶 + `waiting` 计数控制队列上限，`asyncio.wait_for` 实现等待超时；流式请求的槽位释放挂在响应 `BackgroundTask`，确保推流完毕后才释放
- **内存守护**后台 asyncio 任务读进程 RSS（优先 `psutil`，未安装则自动禁用），超阈值 `gc.collect()` + 降级标志拒新请求
- **异常恢复**用 Starlette `BaseHTTPMiddleware` 捕获未处理异常，记录 `traceback` 全量堆栈并累加计数
- **优雅关闭**依赖 uvicorn 的信号处理 + `on_shutdown` 钩子，停止内存守护/健康检查任务并关闭连接池
