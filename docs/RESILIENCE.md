# 韧性网关：上游转发与韧性治理

本服务在原有 HTTP 服务基础上增加一套**本地可验证**的上游转发与韧性治理流程。
原有接口 `GET /`（返回 `{"Hello": "World"}`）行为保持不变，新增：

- `POST /gateway/invoke`：按上游维度转发并执行韧性治理；
- `GET /gateway/circuits`：查看各上游熔断器当前状态快照。

所有对外结论确定、可解释，并通过 `correlation_id`（关联标识）贯穿整条日志链路。

---

## 1. 总体流程

单次 `POST /gateway/invoke` 的处理顺序：

```
幂等去重  →  熔断判定  →  调用上游（含重试循环）  →  成功 / 降级 / 归一化错误
```

1. **幂等去重**：请求携带 `idempotency_key` 时，同键首个请求负责真正调用上游；
   并发同键请求阻塞等待并复用同一份结论；同键后续重复提交也直接复用。
   → 同键请求**最多产生一次上游副作用**。
2. **熔断判定**：每轮尝试（含重试）前都向该上游的熔断器申请放行。
3. **上游调用与重试**：仅对可重试失败、且满足幂等前提的请求重试，指数退避。
4. **结论归一化**：成功 / 确定性降级 / 可区分错误类别三选一，不回传任何上游原始错误细节。

---

## 2. 熔断状态机（按上游维度独立维护）

每个上游拥有独立熔断器，状态为 `CLOSED` / `OPEN` / `HALF_OPEN`，互不影响。

```
                 窗口内失败数 >= failure_threshold
        CLOSED ───────────────────────────────────► OPEN
          ▲                                            │
          │ 半开试探成功数 >= half_open_success_threshold
          │                                            │ 等待 open_recovery_seconds
          │                                            ▼
          └──────────────────────────────────────  HALF_OPEN
                       试探失败数 >= half_open_failure_threshold（回到 OPEN，刷新计时）
```

**抗抖动设计（避免误伤健康上游）：**

- 只统计 `window_seconds` 时间窗口内的失败，窗口外失败自动过期；
- 未达到 `failure_threshold` 的短暂失败不会打开熔断器；
- CLOSED 下一次成功会清空窗口内失败计数。

**时间窗口自动恢复：**

- OPEN 持续 `open_recovery_seconds` 后，下一个请求原子地迁移到 HALF_OPEN；
- HALF_OPEN 同一时刻**只允许一个试探请求在途**，其余请求直接降级；
- 试探成功累计到阈值 → CLOSED；试探失败达到阈值 → 重新 OPEN 并刷新恢复计时。

**并发一致性：** 熔断器的全部判定与迁移都在同一把可重入锁内完成；
OPEN→HALF_OPEN 由“抢到迁移权的那个请求”原子完成，因此并发下状态迁移唯一、确定。

---

## 3. 安全重试与幂等保证

### 3.1 哪些失败可重试

上游 HTTP 状态码归一化（`app/errors.py`）：

| 状态码 | 错误类别 | 可重试 |
| --- | --- | --- |
| 408 / 429 / 500 / 502 / 503 / 504 / 404 | `upstream_unavailable` / `upstream_timeout` | 是 |
| 其余 4xx | `upstream_rejected` | 否 |
| 其它无法处理响应 | `upstream_bad_response` | 否 |
| 服务内部未知异常 | `internal_error` | 否（归一化兜底） |

### 3.2 幂等前提

- 幂等方法（`GET/HEAD/PUT/DELETE/OPTIONS`）：天然可重试；
- 非幂等方法（`POST/PATCH` 等）：**只有携带客户端 `idempotency_key` 才允许重试**。

### 3.3 拒绝/放弃重试的可区分原因

`reason` 字段与日志中给出明确原因：

- `non_idempotent_without_idempotency_key`：非幂等且未带幂等键；
- `non_retryable_failure`：失败本身不可重试（如业务 4xx）；
- `max_attempts_reached`：达到最大尝试次数；
- `circuit_open`：重试过程中熔断器已打开，立即降级不再试探。

重试退避采用指数退避并截断到 `max_backoff_seconds`。

### 3.4 幂等去重

- 首个同键请求返回 `FIRST` 并执行上游调用；
- 并发同键请求等待首个完成，返回同一结论（`DUPLICATE`），自身不产生副作用；
- 首个完成后的任何同键重复提交都复用已存结论。
- 复用结论会以**当前请求自己的 `correlation_id`** 返回，便于链路追踪。

---

## 4. 统一降级与错误归一化

对外结论为统一结构（`GatewayResult`）：

| 场景 | `status` | `degraded` | HTTP | error_category 示例 |
| --- | --- | --- | --- | --- |
| 上游成功 | `SUCCESS` | false | 200 | null |
| 上游不可用 / 超时 / 熔断打开 / 内部异常 / 上游未配置 | `DEGRADED` | true | 503 | `upstream_unavailable` / `upstream_timeout` / `internal_error` |
| 上游业务拒绝 / 错误响应 | `ERROR` | false | 502 | `upstream_rejected` / `upstream_bad_response` |

降级负载是**确定性**的：

```json
{"message": "service degraded", "upstream": "<name>", "reason": "<reason>"}
```

- 上游原始异常、堆栈、内部路径等细节**绝不透出**；内部未知异常统一归一化为
  `internal_error` 并走降级。
- 每个请求自动生成或接收外部传入的 `correlation_id`，状态变化、重试决策、
  去重判定等日志全部携带该标识（无请求上下文的内部日志为 `cid=-`）。

---

## 5. 可配置项（`app/config.py: ResilienceConfig`）

| 配置项 | 默认值 | 含义 |
| --- | --- | --- |
| `failure_threshold` | 5 | 窗口内连续失败达到该值则熔断打开 |
| `window_seconds` | 10.0 | 熔断失败统计时间窗口（秒） |
| `open_recovery_seconds` | 1.0 | OPEN 后多久允许一次半开试探（秒） |
| `half_open_success_threshold` | 2 | 半开试探成功达到该值则关闭 |
| `half_open_failure_threshold` | 1 | 半开试探失败达到该值则重新打开 |
| `max_attempts` | 3 | 最大尝试次数（含首次） |
| `backoff_base_seconds` | 0.01 | 指数退避基数（秒） |
| `max_backoff_seconds` | 0.5 | 单次退避上限（秒） |

---

## 6. 本地验证方式

### 6.1 运行单元测试（推荐，无需外部依赖）

```bash
uv sync
uv run pytest -s
```

测试全部使用**本地桩上游**（`StubUpstreamClient`），覆盖：

- `tests/test_circuit_breaker.py`：阈值触发、窗口过期抗抖动、半开恢复/失败重开、
  并发恢复只放一个试探、并发失败只迁移一次、上游间隔离；
- `tests/test_retry.py`：可重试性、非幂等需幂等键、三类放弃原因、退避上限；
- `tests/test_idempotency.py`：并发同键单次执行、在途等待、重复提交复用；
- `tests/test_gateway.py`：完整链路（重试恢复、拒绝重试原因、业务拒绝归一化、
  熔断打开→恢复、8 并发同键单次副作用、同键失败后重复复用、内部异常不透出）；
- `tests/test_http_api.py`：`GET /` 原行为不变、转发接口 200/503、熔断快照端点。

`-s` 可在测试输出中看到 `[state]`（状态变化）、`[retry]`（重试决策）、
`[idempotency]`（去重判定）与 `cid=`（关联标识）日志。

### 6.2 启动服务手工验证

```bash
uv run uvicorn main:app --port 8000
```

内置两个桩上游：`upstream-a`（恒成功）、`upstream-flaky`（恒失败）。

```bash
# 原行为不变
curl -s http://127.0.0.1:8000/

# 成功转发（非幂等 POST 需带幂等键才会重试）
curl -s -X POST http://127.0.0.1:8000/gateway/invoke \
  -H 'Content-Type: application/json' \
  -d '{"upstream":"upstream-a","method":"POST","idempotency_key":"k1","payload":{"x":1}}'

# 故障上游：观察降级与 reason
curl -s -X POST http://127.0.0.1:8000/gateway/invoke \
  -H 'Content-Type: application/json' \
  -d '{"upstream":"upstream-flaky","method":"POST"}'

# 观察熔断状态
curl -s http://127.0.0.1:8000/gateway/circuits
```

### 6.3 复现并发/恢复场景

在测试中用注入的 `FakeClock`（熔断测试）可无损、确定性地推进时间，
用线程 + `Barrier`（并发测试）制造同一时刻竞争，因此状态迁移与判定结论可重复复现。
