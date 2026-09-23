# resilient-gateway-python

在 FastAPI 服务上实现的一套**本地可验证**的上游转发与韧性治理流程：
按上游熔断、安全重试与幂等去重、统一降级与错误归一化。

- 原有接口 `GET /` 行为保持不变；
- 新增 `POST /gateway/invoke`（转发 + 韧性治理）与 `GET /gateway/circuits`（熔断快照）。

## 快速开始

```bash
uv sync
uv run pytest -s          # 本地桩上游全量测试（并发/熔断恢复/同键重复）
uv run uvicorn main:app --port 8000
```

## 模块结构

| 文件 | 职责 |
| --- | --- |
| `app/circuit_breaker.py` | 按上游维度的熔断状态机（CLOSED/OPEN/HALF_OPEN）与时间恢复 |
| `app/retry.py` | 重试决策、幂等前提判定、退避计算、可区分放弃原因 |
| `app/idempotency.py` | 幂等键去重，同键并发/重复只产生一次副作用 |
| `app/errors.py` | 上游状态码归一化为可区分错误类别 |
| `app/upstream.py` | 上游客户端协议与本地桩 `StubUpstreamClient` |
| `app/gateway.py` | 编排：去重 → 熔断 → 调用/重试 → 降级/归一化 |
| `app/config.py` | 全部韧性策略可配置项 |
| `app/logging_utils.py` | 带 `correlation_id` 的结构化日志 |

详细策略说明、配置项与验证方式见 [`docs/RESILIENCE.md`](docs/RESILIENCE.md)。
