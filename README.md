# resilient-gateway-python

在既有 HTTP 服务（`GET /` 行为保持不变）之上，实现一套本地可验证的上游转发与
韧性治理流程：按上游维度的熔断状态机、安全重试与幂等去重、统一降级与错误归一化，
以及单次转发总时间预算、按上游并发隔离和只读运行指标查看入口。

## 对外结论（确定且可解释）

每个转发请求最终只会落到三类结论之一，通过响应体 `conclusion` 表达，
拒绝/降级原因通过 `error.category`（与服务端日志 `reason`）区分：

| 结论 | 触发条件 | HTTP 状态码 | 典型原因 category |
| --- | --- | --- | --- |
| `success` | 上游成功（可能经重试）；或同键请求命中已完成结果重放 | 200 | `idempotent_replay`（重放时） |
| `degraded` | 熔断打开快速失败；可重试失败耗尽重试预算；单次请求总时间预算耗尽 | 503 | `circuit_open` / `retry_exhausted` / `budget_exhausted` |
| `rejected` | 同键请求正在处理；非幂等请求无幂等键且首试失败；上游致命拒绝；上游并发在途超限 | 409 / 422 / 502 / 429 | `idempotency_conflict` / `non_idempotent_retry_forbidden` / `upstream_rejected` / `concurrency_limit_exceeded` |

所有响应都带 `X-Request-ID` 响应头，响应体含 `request_id`，与服务端
结构化日志中的关联标识一致。**上游原始异常文本不会透出给调用方**，
仅以归一化类别和稳定文案返回。

## 韧性策略

### 1. 熔断状态机（按上游维度，`app/breaker.py`）

```
CLOSED ── 失败窗口内失败数 >= failure_threshold ──▶ OPEN
OPEN   ── 距打开 >= open_state_seconds ───────────▶ HALF_OPEN（限量试探许可）
HALF_OPEN ── 试探成功 ─▶ CLOSED
HALF_OPEN ── 试探失败 ─▶ OPEN（重新计时）
```

- **滑动失败窗口**：CLOSED 下只统计 `failure_window_seconds` 窗口内的失败，
  短暂零星失败会随窗口滑出，不会误伤健康上游。
- **基于时间窗口的自动恢复**：OPEN 持续 `open_state_seconds` 后，
  下一个请求触发迁到 HALF_OPEN，只发放 `half_open_max_calls` 个试探许可。
- **并发一致**：每个上游一把可重入锁，状态迁移与许可发放都在锁内完成，
  并发请求不会产生重复迁移或超额试探。

### 2. 安全重试与幂等（`app/retry.py`、`app/idempotency.py`、`app/gateway.py`）

- **仅对可重试失败重试**：桩/客户端把结果标记为 `retryable_failure` 才重试；
  `fatal_failure` 立即归一化为 `upstream_rejected`，绝不重试。
- **非幂等请求的重试门槛**：GET/HEAD/PUT/DELETE/OPTIONS/TRACE 视为幂等可重试；
  POST/PATCH 等非幂等方法**只有携带客户端 `Idempotency-Key` 请求头才允许重试**，
  否则首试失败即以 `non_idempotent_retry_forbidden` 拒绝（与重试耗尽原因可区分）。
- **同键并发去重**：首个同键请求获得执行权，处理中的并发同键请求得到
  409 `idempotency_conflict`，上游只被调用一次，杜绝重复副作用。
- **同键重复提交重放**：首个请求成功后结果按键缓存，后续同键请求直接重放
  结果，不再触达上游。
- **失败释放键**：首个请求失败（含重试耗尽）不会缓存结果，键被释放，
  客户端可凭同一幂等键安全重提。

### 3. 降级与错误归一化（`app/errors.py`、`app/gateway.py`）

- 熔断打开或重试耗尽时返回**确定的降级负载**：
  `{"degraded": true, "upstream": <name>, "data": null}`。
- 上游异常（连接错误、超时、桩注入异常等）与内部异常统一映射到有限的
  `ErrorCategory`（`upstream_unavailable` / `upstream_timeout` /
  `upstream_rejected` / `retry_exhausted` / `circuit_open` /
  `budget_exhausted` / `concurrency_limit_exceeded` / `internal` 等），
  原始异常类型只记录在服务端日志（`upstream_exception` 事件）。
- 每条日志为一行 JSON，自动附带 `request_id`，贯穿熔断、重试、幂等等全部决策点。

### 4. 单次转发总时间预算（`app/gateway.py`）

- 调用方可随请求携带总时间预算（HTTP 头 `X-Request-Budget-Ms`，毫秒；
  或 `Gateway.handle(..., budget_seconds=...)`），从请求进入网关起算，
  **重试之间的退避等待也计入预算**（退避会被截断到预算边界，不超睡）。
- 每次发起上游尝试前检查预算：一旦耗尽就不再打上游，直接返回降级结论，
  原因为 `budget_exhausted`，与 `retry_exhausted`、`circuit_open` 明确区分。
- 预算按请求独立计算（各自的起始时间与截止时间），并发请求互不影响。
- 预算耗尽不占用幂等键：键被释放，客户端可凭同键安全重提。
- 不携带预算时行为与之前完全一致（不限制）。

### 5. 按上游并发隔离（`app/bulkhead.py`）

- 每个上游有独立的最大在途请求数（`BulkheadConfig`），超限请求**不进入
  上游调用**，直接以 429 `concurrency_limit_exceeded` 拒绝，与现有几类
  原因可区分。
- 被挡下的请求**不计入熔断失败统计**、**不触碰任何幂等键**（舱壁判定
  发生在幂等去重与熔断统计之前）。
- 在途计数的增减在同一把锁内完成，释放与获取严格配对：高并发下既不会
  超额放行，也不会漏减已结束的请求。

### 6. 只读运行指标（`app/metrics.py` + `GET /admin/upstreams`）

按上游返回当前运行状况，便于对照文档自行核对：

```json
{
  "stub": {
    "breaker_state": "closed",
    "window_failures": 4,
    "in_flight": 0,
    "retries": 3,
    "degraded": 1,
    "rejected": 0
  }
}
```

- `breaker_state`：当前熔断状态；`window_failures`：失败窗口内累计失败数；
- `retries` / `degraded` / `rejected`：服务启动以来累计的重试等待、
  降级结论、拒绝结论次数；`in_flight`：当前在途请求数。
- 该入口只读，不改动任何既有对外接口的行为与响应结构。

## 可配置项（`app/config.py`）

`CircuitBreakerConfig`：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `failure_threshold` | 5 | 失败窗口内触发熔断的失败数阈值 |
| `open_state_seconds` | 30.0 | OPEN 持续秒数，到期进入 HALF_OPEN |
| `half_open_max_calls` | 1 | HALF_OPEN 同时放行的试探请求数 |
| `failure_window_seconds` | 60.0 | 失败计数滑动窗口长度 |

`RetryConfig`：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `max_attempts` | 3 | 最大尝试次数（含首次） |
| `backoff_base_seconds` | 0.01 | 指数退避基数，第 n 次重试等待 `base * 2^(n-1)`（上限封顶 + 抖动） |
| `backoff_max_seconds` | 1.0 | 单次退避上限 |

`BulkheadConfig`：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `default_max_concurrent` | 32 | 未单独配置的上游的最大在途请求数 |
| `per_upstream` | `None` | 按上游名覆盖默认并发上限，如 `{"orders": 8}` |

请求级时间预算（非配置项，随请求携带）：

| 入口 | 缺省行为 |
| --- | --- |
| HTTP 头 `X-Request-Budget-Ms: <毫秒>`；或 `Gateway.handle(..., budget_seconds=<秒>)` | 不携带则不限制，行为与之前一致 |

时钟、睡眠函数与随机源均可在构造 `Gateway` 时注入（`clock` / `sleeper` / `rng`），
因此测试中的状态迁移、退避与恢复时序完全确定、可复现。

## HTTP 接口

- `GET /`：既有接口，行为不变，返回 `{"Hello": "World"}`。
- `POST /upstreams/{upstream}/call`：韧性转发，请求体
  `{"method": "POST", "path": "/orders", "body": {...}}`，
  可通过 `Idempotency-Key` 请求头携带幂等键，
  可通过 `X-Request-Budget-Ms` 请求头携带本次请求的总时间预算（毫秒，可选）。
- `GET /admin/circuits`：查看各上游当前熔断状态（既有行为不变）。
- `GET /admin/upstreams`：只读查看各上游熔断状态、窗口内失败数、
  当前在途数与启动以来累计的重试/降级/拒绝次数。

## 兼容范围

- 既有接口（`GET /`、`POST /upstreams/{upstream}/call`、`GET /admin/circuits`）
  的响应结构与状态码映射保持不变；新能力均为**可选叠加**：
  不带预算头、使用默认 `BulkheadConfig`（上限 32）时，行为与上一版完全一致。
- 新增内容只有：两个可选输入（预算头、舱壁配置）、一个只读入口
  （`GET /admin/upstreams`）、两个新的可区分原因类别
  （`budget_exhausted`、`concurrency_limit_exceeded`）。

## 本地验证

```bash
uv sync
uv run pytest -q            # 48 个单测/集成测试，全部确定性可复现
uv run pytest -q -s         # 加 -s 可看到每个新场景打印的[场景]/[结果]说明
uv run python scripts/demo_scenarios.py   # 七类场景演示，含结构化日志
uv run uvicorn main:app     # 启动后用 curl 访问上述接口
```

curl 速查（服务启动后）：

```bash
# 带 500ms 总预算的转发
curl -i -X POST localhost:8000/upstreams/stub/call \
  -H 'Content-Type: application/json' -H 'X-Request-Budget-Ms: 500' \
  -d '{"method": "GET", "path": "/x"}'
# 只读运行状况
curl -s localhost:8000/admin/upstreams | python -m json.tool
```

测试覆盖（本地脚本式桩上游 `StubUpstreamClient`，无需外部依赖）：

- **熔断恢复**：滑动窗口防误伤、CLOSED→OPEN→HALF_OPEN→CLOSED 全迁移、
  HALF_OPEN 失败重新打开并重新计时（`tests/test_breaker.py`、
  `tests/test_gateway_resilience.py`）。
- **并发状态迁移一致**：20 线程争抢半开唯一许可，仅 1 个通过。
- **安全重试**：可重试失败重试成功、预算耗尽降级、致命失败不重试、
  非幂等 POST 无键不重试/有键重试、上游异常归一化（`tests/test_gateway_retry.py`）。
- **同键并发与重复提交**：16 个并发同键 POST 仅 1 次上游副作用、
  15 个 409 拒绝；成功后重放零调用；失败后键释放可重提
  （`tests/test_idempotency.py`、`tests/test_gateway_resilience.py`）。
- **时间预算**：预算为 0 不打上游、退避计入预算且截断不超睡、
  `budget_exhausted` 与 `retry_exhausted`/`circuit_open` 可区分、
  预算耗尽释放幂等键、并发请求各自独立预算（`tests/test_budget.py`）。
- **并发隔离**：超限请求 429 拒绝且不触达上游、不计熔断失败、不动幂等键；
  10 线程限 3 并发下峰值在途恰为 3、结束后归零；按上游覆盖上限
  （`tests/test_bulkhead.py`）。
- **运行指标**：重试/降级/拒绝按上游累计、20 线程并发下计数无丢失、
  `GET /admin/upstreams` 返回熔断状态+窗口失败数+累计指标、
  预算头 HTTP 映射、既有 `/admin/circuits` 行为不变（`tests/test_metrics.py`）。
- **HTTP 层**：状态码映射、`X-Request-ID` 贯穿、异常文本不外泄、根路由行为不变
  （`tests/test_api.py`）。

测试与演示运行时会在日志中打印 `breaker_state_transition`（状态变化）、
`retry_decision`（重试决策）、`idempotency_*`（去重决策）、
`bulkhead_rejected`（并发超限）以及每行的 `request_id`；
新场景测试还会打印 `[场景]`/`[结果]` 行，标明覆盖了什么、结果如何。

## 代码结构

```
app/config.py       熔断/重试/并发隔离可配置项
app/context.py      请求关联标识与结构化日志
app/errors.py       错误类别归一化与对外稳定文案
app/breaker.py      按上游维度的熔断状态机与注册表
app/bulkhead.py     按上游维度的并发在途隔离（舱壁）
app/metrics.py      按上游维度的重试/降级/拒绝累计指标
app/idempotency.py  幂等键去重、结果缓存与失败释放
app/upstream.py     上游客户端抽象 + 脚本式本地桩
app/retry.py        幂等判定、可重试判定、退避计算
app/gateway.py      编排：舱壁 → 幂等 → 熔断 → 预算/重试 → 降级/归一化
app/api.py          FastAPI 路由装配（保留 GET /）
scripts/demo_scenarios.py  确定性场景演示
tests/              全部单元/集成测试
```
