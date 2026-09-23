# resilient-gateway-python

在既有 HTTP 服务（`GET /` 行为保持不变）之上，实现一套本地可验证的上游转发与
韧性治理流程：按上游维度的熔断状态机、安全重试与幂等去重、统一降级与错误归一化。

## 对外结论（确定且可解释）

每个转发请求最终只会落到三类结论之一，通过响应体 `conclusion` 表达，
拒绝/降级原因通过 `error.category`（与服务端日志 `reason`）区分：

| 结论 | 触发条件 | HTTP 状态码 | 典型原因 category |
| --- | --- | --- | --- |
| `success` | 上游成功（可能经重试）；或同键请求命中已完成结果重放 | 200 | `idempotent_replay`（重放时） |
| `degraded` | 熔断打开快速失败；或可重试失败耗尽重试预算 | 503 | `circuit_open` / `retry_exhausted` |
| `rejected` | 同键请求正在处理；非幂等请求无幂等键且首试失败；上游致命拒绝 | 409 / 422 / 502 | `idempotency_conflict` / `non_idempotent_retry_forbidden` / `upstream_rejected` |

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

时钟、睡眠函数与随机源均可在构造 `Gateway` 时注入（`clock` / `sleeper` / `rng`），
因此测试中的状态迁移、退避与恢复时序完全确定、可复现。

## HTTP 接口

- `GET /`：既有接口，行为不变，返回 `{"Hello": "World"}`。
- `POST /upstreams/{upstream}/call`：韧性转发，请求体
  `{"method": "POST", "path": "/orders", "body": {...}}`，
  可通过 `Idempotency-Key` 请求头携带幂等键。
- `GET /admin/circuits`：查看各上游当前熔断状态，便于本地验证。

## 本地验证

```bash
uv sync
uv run pytest -q          # 31 个单测/集成测试，全部确定性可复现
uv run python scripts/demo_scenarios.py   # 四类场景演示，含结构化日志
uv run uvicorn main:app   # 启动后用 curl 访问上述接口
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
- **HTTP 层**：状态码映射、`X-Request-ID` 贯穿、异常文本不外泄、根路由行为不变
  （`tests/test_api.py`）。

测试与演示运行时会在日志中打印 `breaker_state_transition`（状态变化）、
`retry_decision`（重试决策）、`idempotency_*`（去重决策）以及每行的 `request_id`。

## 代码结构

```
app/config.py       熔断/重试可配置项
app/context.py      请求关联标识与结构化日志
app/errors.py       错误类别归一化与对外稳定文案
app/breaker.py      按上游维度的熔断状态机与注册表
app/idempotency.py  幂等键去重、结果缓存与失败释放
app/upstream.py     上游客户端抽象 + 脚本式本地桩
app/retry.py        幂等判定、可重试判定、退避计算
app/gateway.py      编排：幂等 → 熔断 → 重试 → 降级/归一化
app/api.py          FastAPI 路由装配（保留 GET /）
scripts/demo_scenarios.py  确定性场景演示
tests/              全部单元/集成测试
```
