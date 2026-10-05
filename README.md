# resilient-gateway-python

在既有 HTTP 服务（`GET /` 行为保持不变）之上，实现一套本地可验证的上游转发与
韧性治理流程：按上游维度的熔断状态机、安全重试与幂等去重、统一降级与错误归一化，
以及单次请求总时间预算、按上游并发隔离和只读运行状态查看入口。

## 对外结论（确定且可解释）

每个转发请求最终只会落到三类结论之一，通过响应体 `conclusion` 表达，
拒绝/降级原因通过 `error.category`（与服务端日志 `reason`）区分：

| 结论 | 触发条件 | HTTP 状态码 | 典型原因 category |
| --- | --- | --- | --- |
| `success` | 上游成功（可能经重试）；或同键请求命中已完成结果重放 | 200 | `idempotent_replay`（重放时） |
| `degraded` | 熔断打开快速失败；可重试失败耗尽重试预算；单次请求总时间预算耗尽 | 503 | `circuit_open` / `retry_exhausted` / `budget_exhausted` |
| `rejected` | 同键请求正在处理；非幂等请求无幂等键且首试失败；上游致命拒绝；上游在途并发超限 | 409 / 422 / 502 / 429 | `idempotency_conflict` / `non_idempotent_retry_forbidden` / `upstream_rejected` / `concurrency_limit_exceeded` |

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
  `upstream_rejected` / `retry_exhausted` / `circuit_open` / `internal` 等），
  原始异常类型只记录在服务端日志（`upstream_exception` 事件）。
- 每条日志为一行 JSON，自动附带 `request_id`，贯穿熔断、重试、幂等等全部决策点。

### 4. 单次请求总时间预算（`app/budget.py`、`app/gateway.py`）

- 调用方可随请求携带总预算秒数（HTTP 层为 `X-Budget-Seconds` 请求头，
  非法值返回 422 `invalid_budget_header`），从请求进入网关起算，
  **重试之间的退避等待也计入预算**。
- 每次触达上游前检查预算；退避等待若会耗尽预算，则不等待也不再打上游，
  直接返回确定的降级结论，原因 `budget_exhausted` 与
  `retry_exhausted` / `circuit_open` 明确可区分。
- 预算随请求独立计算，并发请求互不影响；预算耗尽视为失败，
  幂等键照常释放，可凭同键安全重提。
- 不携带预算且未配置默认预算时不限制，行为与既有版本完全一致。

### 5. 按上游并发隔离（`app/bulkhead.py`、`app/gateway.py`）

- 每个上游可配置最大在途请求数；超限请求在幂等/熔断/上游调用**之前**
  被确定性拒绝（429 `concurrency_limit_exceeded`），不阻塞、不排队。
- 被挡下的请求**不计入上游失败统计**（熔断窗口不受影响），
  **也不触碰处理中的幂等键**（同键请求稍后仍可正常重放/重提）。
- 在途计数在同一把锁内增减，许可释放幂等：高并发下既不超额放行，
  也不漏减已结束的请求。
- 未配置上限（默认）时不限制，行为与既有版本完全一致。

### 6. 只读运行状态查看（`app/metrics.py`、`GET /admin/status`）

- 按上游返回：当前熔断状态、失败窗口内累计失败数、当前在途数，
  以及服务启动以来累计的重试 / 降级 / 拒绝次数。
- 纯只读：不改变任何熔断、幂等、并发隔离状态，也不改变既有接口的行为与响应结构。

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

`BudgetConfig`：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `default_seconds` | `None`（不限制） | 调用方未随请求携带预算时使用的默认总预算秒数；请求级预算优先于该默认值 |

`BulkheadConfig`：

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `default_max_in_flight` | `None`（不限制） | 每个上游默认允许的最大在途请求数 |
| `per_upstream` | `{}` | 按上游名覆盖默认上限，如 `{"orders": 8}` |

时钟、睡眠函数与随机源均可在构造 `Gateway` 时注入（`clock` / `sleeper` / `rng`），
因此测试中的状态迁移、退避与恢复时序完全确定、可复现。

## 兼容范围

本次新增能力全部向后兼容，原有行为保持不变：

- 不携带 `X-Budget-Seconds`、不配置 `BudgetConfig` / `BulkheadConfig` 时，
  转发链路（幂等 → 熔断 → 重试 → 降级）的结论、原因与 HTTP 状态码与之前完全一致；
- 既有接口 `GET /`、`POST /upstreams/{upstream}/call`、`GET /admin/circuits`
  的行为与响应结构不变；`/admin/status` 为新增只读入口；
- 既有错误类别与对外文案不变，仅新增 `budget_exhausted` 与
  `concurrency_limit_exceeded` 两个可区分原因。

## HTTP 接口

- `GET /`：既有接口，行为不变，返回 `{"Hello": "World"}`。
- `POST /upstreams/{upstream}/call`：韧性转发，请求体
  `{"method": "POST", "path": "/orders", "body": {...}}`，
  可通过 `Idempotency-Key` 请求头携带幂等键，
  可通过 `X-Budget-Seconds` 请求头携带本次请求的总时间预算秒数。
- `GET /admin/circuits`：查看各上游当前熔断状态，便于本地验证。
- `GET /admin/status`：只读运行状态，按上游返回
  `breaker_state` / `window_failures` / `in_flight` /
  `retries` / `degraded` / `rejected`。

## 本地验证

```bash
uv sync
uv run pytest -q          # 58 个单测/集成测试，全部确定性可复现
uv run python scripts/demo_scenarios.py   # 七类场景演示，含结构化日志
uv run uvicorn main:app   # 启动后用 curl 访问上述接口
```

curl 手动验证示例：

```bash
# 携带总预算 0.5s 的转发；预算耗尽时返回 503 + budget_exhausted
curl -i -X POST localhost:8000/upstreams/stub/call \
  -H 'Content-Type: application/json' -H 'X-Budget-Seconds: 0.5' \
  -d '{"method": "GET", "path": "/x"}'

# 只读运行状态：熔断状态、窗口失败数、在途数与累计指标
curl -s localhost:8000/admin/status | python -m json.tool
```

测试运行结束时会在终端打印 `covered scenarios` 汇总，
逐条列出覆盖的场景与各自结果（如 `预算耗尽-进入即耗尽`、
`并发隔离-在途计数一致`、`指标累计-重试/降级/拒绝`）。

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
- **HTTP 层**：状态码映射、`X-Request-ID` 贯穿、异常文本不外泄、根路由行为不变
  （`tests/test_api.py`）。
- **总时间预算**：进入即耗尽零调用降级、退避超预算即停、预算内重试成功、
  跨尝试累计消耗、耗尽后幂等键释放、默认预算与请求级覆盖、并发请求预算各自独立
  （`tests/test_budget.py`）。
- **并发隔离**：超限确定性拒绝且零上游调用、被挡请求不计失败统计、
  不触碰处理中幂等键、20 线程下在途峰值不超上限且结束后归零、
  按上游覆盖上限、许可幂等释放（`tests/test_bulkhead.py`）。
- **指标与只读入口**：重试/降级/拒绝累计、预算耗尽计为降级、快照读取无副作用、
  窗口失败数随窗口滑动、`GET /admin/status` 结构与计数、
  `X-Budget-Seconds` 请求头、并发超限 429 映射、既有接口行为不变
  （`tests/test_metrics.py`、`tests/test_admin_status.py`）。

测试与演示运行时会在日志中打印 `breaker_state_transition`（状态变化）、
`retry_decision`（重试决策）、`idempotency_*`（去重决策）、
`bulkhead_*`（并发隔离决策）、`request_budget_exhausted`（预算耗尽）
以及每行的 `request_id`。

## 代码结构

```
app/config.py       熔断/重试/预算/并发隔离可配置项
app/context.py      请求关联标识与结构化日志
app/errors.py       错误类别归一化与对外稳定文案
app/breaker.py      按上游维度的熔断状态机与注册表
app/budget.py       单次请求总时间预算（Deadline）
app/bulkhead.py     按上游维度的并发隔离与在途许可
app/metrics.py      按上游维度的重试/降级/拒绝累计计数
app/idempotency.py  幂等键去重、结果缓存与失败释放
app/upstream.py     上游客户端抽象 + 脚本式本地桩
app/retry.py        幂等判定、可重试判定、退避计算
app/gateway.py      编排：并发隔离 → 幂等 → 熔断 → 预算内重试 → 降级/归一化
app/api.py          FastAPI 路由装配（保留 GET /，新增 GET /admin/status）
scripts/demo_scenarios.py  确定性场景演示
tests/              全部单元/集成测试
```
