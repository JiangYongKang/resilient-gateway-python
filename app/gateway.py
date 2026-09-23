"""网关编排。

单次请求流程（每一步都带 correlation_id 日志）：
  幂等去重 -> 熔断判定 -> 调用上游（带重试）-> 成功上报 / 失败降级归一化。

对外结论（GatewayResult）确定且可解释：
  - degraded=True ：上游不可用/熔断打开时的确定性降级结果（HTTP 503）；
  - error_category：非降级类的上游业务拒绝/错误（HTTP 502），类别可区分；
  - 正常成功（HTTP 200）。
任何上游原始异常或内部异常都不会直接透出。
"""

from __future__ import annotations

import time
import uuid

from app.circuit_breaker import CircuitBreakerRegistry, CircuitState
from app.config import ResilienceConfig
from app.errors import ErrorCategory, GatewayError, UpstreamCallError
from app.idempotency import DedupeStatus, IdempotencyStore
from app.logging_utils import get_logger, with_cid
from app.models import GatewayResult, UpstreamRequest
from app.retry import (
    RetryDecision,
    RetryRefuseReason,
    compute_backoff,
    decide_retry,
)
from app.upstream import UpstreamClient

logger = get_logger("gateway.flow")

# 触发降级的错误类别
_DEGRADED_CATEGORIES = frozenset(
    {
        ErrorCategory.UPSTREAM_UNAVAILABLE,
        ErrorCategory.UPSTREAM_TIMEOUT,
        ErrorCategory.INTERNAL_ERROR,
    }
)


def _fallback_payload(upstream: str, reason: str) -> dict:
    """确定性降级结果：同一上游/原因下内容稳定、可复现。"""
    return {"message": "service degraded", "upstream": upstream, "reason": reason}


class Gateway:
    def __init__(
        self,
        clients: dict[str, UpstreamClient],
        config: ResilienceConfig | None = None,
        *,
        breaker_registry: CircuitBreakerRegistry | None = None,
        idempotency_store: IdempotencyStore | None = None,
        sleep=None,
    ) -> None:
        self.config = config or ResilienceConfig()
        self.clients = clients
        self.breakers = breaker_registry or CircuitBreakerRegistry(self.config)
        self.idempotency = idempotency_store or IdempotencyStore()
        self._sleep = sleep or time.sleep

    # ---- 结果构造 --------------------------------------------------------

    def _success(self, cid: str, req: UpstreamRequest, body: dict, attempts: int) -> GatewayResult:
        return GatewayResult(
            correlation_id=cid,
            upstream=req.upstream,
            degraded=False,
            status="SUCCESS",
            data=body,
            attempts=attempts,
        )

    def _degraded(self, cid: str, req: UpstreamRequest, reason: str, attempts: int,
                  category: ErrorCategory) -> GatewayResult:
        return GatewayResult(
            correlation_id=cid,
            upstream=req.upstream,
            degraded=True,
            status="DEGRADED",
            data=_fallback_payload(req.upstream, reason),
            attempts=attempts,
            error_category=category.value,
            reason=reason,
        )

    def _error(self, cid: str, req: UpstreamRequest, reason: str, attempts: int,
               category: ErrorCategory) -> GatewayResult:
        return GatewayResult(
            correlation_id=cid,
            upstream=req.upstream,
            degraded=False,
            status="ERROR",
            data={"message": "request failed", "reason": reason},
            attempts=attempts,
            error_category=category.value,
            reason=reason,
        )

    # ---- 主流程 ----------------------------------------------------------

    def invoke(self, request: UpstreamRequest, *, correlation_id: str | None = None) -> GatewayResult:
        cid = correlation_id or uuid.uuid4().hex[:12]
        log = with_cid(logger, cid)
        log.info("invoke start upstream=%s method=%s key=%s",
                 request.upstream, request.method, request.idempotency_key)

        # 1) 幂等去重：有键且非首个请求 -> 复用既有结论，不再产生副作用
        if request.idempotency_key:
            outcome = self.idempotency.begin(request.idempotency_key)
            if outcome.status == DedupeStatus.DUPLICATE:
                assert outcome.result is not None
                log.info("dedupe duplicate key=%s reusing attempts=%d",
                         request.idempotency_key, outcome.result.attempts)
                # 复用结论但以当前请求的关联标识返回，保证链路可追踪
                return self._reidentify(outcome.result, cid)

        try:
            result = self._do_invoke(request, cid, log)
        except BaseException:
            # 防御性兜底：任何未预期异常都归一化，绝不透出生成器内部细节
            log.exception("unexpected internal error")
            result = self._degraded(
                cid, request, "internal_error", 0, ErrorCategory.INTERNAL_ERROR
            )

        if request.idempotency_key:
            self.idempotency.finish(request.idempotency_key, result)
        log.info("invoke end status=%s degraded=%s attempts=%d category=%s",
                 result.status, result.degraded, result.attempts, result.error_category)
        return result

    @staticmethod
    def _reidentify(result: GatewayResult, cid: str) -> GatewayResult:
        return GatewayResult(
            correlation_id=cid,
            upstream=result.upstream,
            degraded=result.degraded,
            status=result.status,
            data=result.data,
            attempts=result.attempts,
            error_category=result.error_category,
            reason=result.reason,
        )

    def _do_invoke(self, request: UpstreamRequest, cid: str, log) -> GatewayResult:
        # 2) 未知上游：确定性降级，不产生调用
        client = self.clients.get(request.upstream)
        if client is None:
            log.info("upstream unknown upstream=%s -> degraded", request.upstream)
            return self._degraded(
                cid, request, "upstream_not_configured", 0,
                ErrorCategory.UPSTREAM_UNAVAILABLE,
            )

        breaker = self.breakers.get(request.upstream)

        # 3) 熔断判定（重试每一轮都判定）
        if not breaker.allow_request(cid=cid):
            state = breaker.state
            log.info("circuit reject state=%s -> degraded", state.value)
            return self._degraded(
                cid, request, f"circuit_{state.value.lower()}", 0,
                ErrorCategory.UPSTREAM_UNAVAILABLE,
            )

        attempt = 0
        last_category = ErrorCategory.UPSTREAM_UNAVAILABLE
        last_reason = "upstream_unavailable"

        while attempt < self.config.max_attempts:
            attempt += 1
            log.info("attempt %d/%d", attempt, self.config.max_attempts)
            try:
                response = client.call(request)
            except UpstreamCallError as exc:
                breaker.record_failure(cid=cid)
                last_category = exc.category
                log.info("attempt %d upstream-failure category=%s retryable=%s",
                         attempt, exc.category.value, exc.retryable)
                verdict = decide_retry(
                    attempt=attempt,
                    max_attempts=self.config.max_attempts,
                    failure_retryable=exc.retryable,
                    method=request.method,
                    idempotency_key=request.idempotency_key,
                )
                if verdict.decision == RetryDecision.RETRY:
                    # 重试前再次确认熔断器：若失败已触发打开则立即降级
                    if not breaker.allow_request(cid=cid):
                        log.info("circuit opened after failure -> degraded")
                        return self._degraded(
                            cid, request, "circuit_open", attempt,
                            ErrorCategory.UPSTREAM_UNAVAILABLE,
                        )
                    backoff = compute_backoff(attempt, self.config)
                    log.info("retry-decision RETRY reason=None backoff=%.4f", backoff)
                    self._sleep(backoff)
                    continue
                last_reason = verdict.reason.value if verdict.reason else "give_up"
                log.info("retry-decision GIVE_UP reason=%s", last_reason)
                break
            except Exception as exc:  # noqa: BLE001 - 内部/未知异常归一化
                breaker.record_failure(cid=cid)
                last_category = ErrorCategory.INTERNAL_ERROR
                last_reason = "internal_error"
                log.error("attempt %d internal-error type=%s", attempt, type(exc).__name__)
                break
            else:
                # 4) 成功：上报熔断并返回
                breaker.record_success(cid=cid)
                log.info("attempt %d success status=%d", attempt, response.status_code)
                return self._success(cid, request, response.body, attempt)

        # 5) 终止后的归一化结论
        if last_category in _DEGRADED_CATEGORIES:
            return self._degraded(cid, request, last_reason, attempt, last_category)
        return self._error(cid, request, last_reason, attempt, last_category)
