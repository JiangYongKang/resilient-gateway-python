"""网关编排：熔断 + 幂等 + 安全重试 + 统一降级/错误归一化。

对外结论（:class:`Conclusion`）只有三类，保持确定、可解释：

* ``SUCCESS``  —— 上游成功（可能经过若干次重试），或同键已完成结果的幂等重放；
* ``DEGRADED`` —— 熔断打开 / 上游持续失败，返回确定的降级结果；
* ``REJECTED`` —— 同键请求处理中（409）或非幂等请求禁止重试后的致命失败。

拒绝与重试各自的原因通过 ``reason`` 字段给出，二者可明确区分。
"""
from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from app.breaker import BreakerRegistry, BreakerState, CircuitBreaker
from app.budget import Deadline
from app.bulkhead import BulkheadRegistry
from app.config import (
    BudgetConfig,
    BulkheadConfig,
    CircuitBreakerConfig,
    RetryConfig,
)
from app.context import bind_context, log_event, new_context
from app.errors import (
    ErrorCategory,
    GatewayError,
    normalize_outcome_failure,
)
from app.idempotency import DedupStatus, IdempotencyStore
from app.metrics import MetricsRegistry
from app.retry import (
    compute_backoff_seconds,
    is_retryable_outcome,
    retry_allowed,
)
from app.upstream import (
    UpstreamClient,
    UpstreamOutcome,
    UpstreamRequest,
)


class Conclusion(str, Enum):
    SUCCESS = "success"
    DEGRADED = "degraded"
    REJECTED = "rejected"


@dataclass(frozen=True)
class GatewayResult:
    conclusion: Conclusion
    upstream: str
    attempts: int
    payload: object | None = None
    reason: str | None = None
    request_id: str | None = None


def default_degraded_payload(upstream: str) -> dict[str, object]:
    return {"degraded": True, "upstream": upstream, "data": None}


class Gateway:
    def __init__(
        self,
        clients: dict[str, UpstreamClient],
        breaker_config: CircuitBreakerConfig | None = None,
        retry_config: RetryConfig | None = None,
        budget_config: BudgetConfig | None = None,
        bulkhead_config: BulkheadConfig | None = None,
        idempotency_store: IdempotencyStore | None = None,
        metrics: MetricsRegistry | None = None,
        degraded_payload_factory: Callable[[str], object] = default_degraded_payload,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.clients = clients
        self.breaker_config = breaker_config or CircuitBreakerConfig()
        self.breakers = BreakerRegistry(self.breaker_config, clock=clock)
        self.retry_config = retry_config or RetryConfig()
        self.budget_config = budget_config or BudgetConfig()
        self.bulkheads = BulkheadRegistry(bulkhead_config or BulkheadConfig())
        self.metrics = metrics or MetricsRegistry()
        self._clock = clock
        self.idempotency = idempotency_store or IdempotencyStore()
        self._make_degraded_payload = degraded_payload_factory
        self._sleeper = sleeper
        self._rng = rng

    def handle(
        self,
        upstream: str,
        request: UpstreamRequest,
        idempotency_key: str | None = None,
        budget_seconds: float | None = None,
    ) -> GatewayResult:
        ctx = new_context()
        with bind_context(ctx):
            return self._handle(upstream, request, idempotency_key, budget_seconds, ctx.request_id)

    def status_snapshot(self) -> dict[str, dict[str, object]]:
        """只读运行状况视图：按上游返回熔断状态、窗口失败数与启动以来累计指标。

        纯读取，不改变任何熔断/幂等/并发隔离状态。
        """
        metrics = self.metrics.snapshot()
        names = set(self.clients) | set(metrics)
        snapshot: dict[str, dict[str, object]] = {}
        for name in sorted(names):
            breaker = self.breakers.get(name)
            counters = metrics.get(
                name, {"retries": 0, "degraded": 0, "rejected": 0}
            )
            snapshot[name] = {
                "breaker_state": breaker.state.value,
                "window_failures": breaker.failure_count_in_window(),
                "in_flight": self.bulkheads.in_flight(name),
                "retries": counters["retries"],
                "degraded": counters["degraded"],
                "rejected": counters["rejected"],
            }
        return snapshot

    # ------------------------------------------------------------------

    def _handle(
        self,
        upstream: str,
        request: UpstreamRequest,
        idempotency_key: str | None,
        budget_seconds: float | None,
        request_id: str,
    ) -> GatewayResult:
        client = self.clients.get(upstream)
        if client is None:
            raise GatewayError(
                ErrorCategory.UPSTREAM_UNAVAILABLE,
                f"unknown upstream: {upstream}",
                upstream=upstream,
            )

        breaker = self.breakers.get(upstream)
        # 单次请求的总时间预算：从进入网关起算，退避等待也计入；
        # 未携带预算且未配置默认预算时为 None（不限），行为与既有版本一致。
        effective_budget = (
            budget_seconds if budget_seconds is not None
            else self.budget_config.default_seconds
        )
        deadline = Deadline(effective_budget, self._clock)
        log_event(
            "request_start",
            upstream=upstream,
            method=request.method,
            path=request.path,
            idempotency_key_present=idempotency_key is not None,
            breaker_state=breaker.state.value,
        )

        # 0) 并发隔离：超限请求在幂等/熔断/上游调用之前被确定性拒绝，
        #    不计入上游失败统计，也不触碰处理中的幂等键。
        permit = self.bulkheads.try_acquire(upstream)
        if permit is None:
            log_event("request_rejected", reason="concurrency_limit_exceeded")
            self.metrics.record_rejected(upstream)
            return GatewayResult(
                Conclusion.REJECTED,
                upstream,
                attempts=0,
                reason=ErrorCategory.CONCURRENCY_LIMIT_EXCEEDED.value,
                request_id=request_id,
            )
        try:
            result = self._handle_inner(
                upstream,
                request,
                client,
                breaker,
                idempotency_key,
                deadline,
                request_id,
            )
        finally:
            permit.release()
        if result.conclusion is Conclusion.DEGRADED:
            self.metrics.record_degraded(upstream)
        elif result.conclusion is Conclusion.REJECTED:
            self.metrics.record_rejected(upstream)
        return result

    def _handle_inner(
        self,
        upstream: str,
        request: UpstreamRequest,
        client: UpstreamClient,
        breaker: CircuitBreaker,
        idempotency_key: str | None,
        deadline: Deadline,
        request_id: str,
    ) -> GatewayResult:
        # 1) 幂等键处理：并发同键拒绝；已完成同键直接重放。
        if idempotency_key is not None:
            dedup = self.idempotency.acquire(idempotency_key)
            if dedup.status is DedupStatus.IN_PROGRESS:
                log_event("request_rejected", reason="idempotency_in_progress")
                return GatewayResult(
                    Conclusion.REJECTED,
                    upstream,
                    attempts=0,
                    reason=ErrorCategory.IDEMPOTENCY_CONFLICT.value,
                    request_id=request_id,
                )
            if dedup.status is DedupStatus.COMPLETED:
                log_event("request_idempotent_replay")
                return GatewayResult(
                    Conclusion.SUCCESS,
                    upstream,
                    attempts=0,
                    payload=dedup.payload,
                    reason="idempotent_replay",
                    request_id=request_id,
                )

        # 2) 熔断快速失败 -> 确定降级结果。
        if not breaker.allow_request():
            log_event("request_degraded", reason="circuit_open")
            return self._degraded_result(
                upstream,
                request_id,
                ErrorCategory.CIRCUIT_OPEN.value,
                idempotency_key,
                attempts=0,
            )

        # 3) 带安全重试的上游调用。
        attempts = 0
        can_retry = retry_allowed(request.method, idempotency_key)
        last_category: ErrorCategory | None = None

        while attempts < self.retry_config.max_attempts:
            # 预算耗尽：不再触达上游，直接给出确定的降级结论。
            if deadline.expired():
                log_event(
                    "request_budget_exhausted",
                    reason="budget_exhausted_before_attempt",
                    attempts=attempts,
                )
                return self._degraded_result(
                    upstream,
                    request_id,
                    ErrorCategory.BUDGET_EXHAUSTED.value,
                    idempotency_key,
                    attempts=attempts,
                )
            attempts += 1
            try:
                response = client.call(request)
            except Exception as exc:  # noqa: BLE001 - 任何上游异常都需归一化
                breaker.record_failure()
                log_event(
                    "upstream_exception",
                    attempts=attempts,
                    error_type=type(exc).__name__,
                )
                last_category = ErrorCategory.UPSTREAM_UNAVAILABLE
                if not self._should_retry(
                    attempts, UpstreamOutcome.RETRYABLE_FAILURE, can_retry
                ):
                    break
                if not self._wait_before_retry(upstream, attempts, deadline):
                    return self._budget_exhausted_result(
                        upstream, request_id, idempotency_key, attempts
                    )
                continue

            outcome = response.outcome
            log_event("upstream_response", attempts=attempts, outcome=outcome.value)

            if outcome is UpstreamOutcome.SUCCESS:
                breaker.record_success()
                result = GatewayResult(
                    Conclusion.SUCCESS,
                    upstream,
                    attempts,
                    payload=response.payload,
                    request_id=request_id,
                )
                if idempotency_key is not None:
                    self.idempotency.complete(idempotency_key, response.payload)
                log_event("request_success", attempts=attempts)
                return result

            last_category = normalize_outcome_failure(outcome)
            breaker.record_failure()

            # 熔断器在本次失败后打开：不再重试，直接降级。
            if breaker.state is BreakerState.OPEN:
                log_event("request_degraded", reason="circuit_tripped_during_attempts")
                return self._degraded_result(
                    upstream,
                    request_id,
                    ErrorCategory.CIRCUIT_OPEN.value,
                    idempotency_key,
                    attempts=attempts,
                )

            if not self._should_retry(attempts, outcome, can_retry):
                break
            if not self._wait_before_retry(upstream, attempts, deadline):
                return self._budget_exhausted_result(
                    upstream, request_id, idempotency_key, attempts
                )

        # 4) 重试循环结束仍未成功。失败不占用幂等键，允许凭同键安全重提。
        if idempotency_key is not None:
            self.idempotency.fail(idempotency_key)

        assert last_category is not None
        # 非幂等请求（无幂等键）遭遇可重试失败时不允许重试：给出明确拒绝原因。
        if (
            last_category is ErrorCategory.UPSTREAM_UNAVAILABLE
            and not can_retry
        ):
            log_event("request_rejected", reason="non_idempotent_retry_forbidden")
            return GatewayResult(
                Conclusion.REJECTED,
                upstream,
                attempts,
                reason=ErrorCategory.NON_IDEMPOTENT_RETRY_FORBIDDEN.value,
                request_id=request_id,
            )

        # 上游致命拒绝（不可重试），归一化为可区分类别后拒绝。
        if last_category is ErrorCategory.UPSTREAM_REJECTED:
            log_event("request_rejected", reason="upstream_fatal")
            return GatewayResult(
                Conclusion.REJECTED,
                upstream,
                attempts,
                reason=ErrorCategory.UPSTREAM_REJECTED.value,
                request_id=request_id,
            )

        # 可重试失败耗尽预算 -> 统一降级。
        log_event("request_degraded", reason="retry_exhausted", attempts=attempts)
        return self._degraded_result(
            upstream,
            request_id,
            ErrorCategory.RETRY_EXHAUSTED.value,
            None,  # 键已在上方释放
            attempts=attempts,
        )

    # ------------------------------------------------------------------

    def _should_retry(
        self,
        attempts: int,
        outcome: UpstreamOutcome,
        can_retry: bool,
    ) -> bool:
        if attempts >= self.retry_config.max_attempts:
            decision = False
        elif not is_retryable_outcome(outcome):
            decision = False
        elif not can_retry:
            decision = False
        else:
            decision = True
        log_event(
            "retry_decision",
            attempts=attempts,
            will_retry=decision,
            outcome=outcome.value,
        )
        return decision

    def _wait_before_retry(
        self, upstream: str, attempts: int, deadline: Deadline
    ) -> bool:
        """退避等待；若等待本身会耗尽预算则不等待并返回 False。"""
        delay = compute_backoff_seconds(attempts, self.retry_config, self._rng)
        if not deadline.fits(delay):
            log_event(
                "retry_backoff_skipped",
                attempts=attempts,
                delay_seconds=delay,
                reason="budget_exhausted",
            )
            return False
        log_event("retry_backoff", attempts=attempts, delay_seconds=delay)
        self.metrics.record_retry(upstream)
        self._sleeper(delay)
        return True

    def _budget_exhausted_result(
        self,
        upstream: str,
        request_id: str,
        idempotency_key: str | None,
        attempts: int,
    ) -> GatewayResult:
        log_event(
            "request_budget_exhausted",
            reason="backoff_exceeds_budget",
            attempts=attempts,
        )
        return self._degraded_result(
            upstream,
            request_id,
            ErrorCategory.BUDGET_EXHAUSTED.value,
            idempotency_key,
            attempts=attempts,
        )

    def _degraded_result(
        self,
        upstream: str,
        request_id: str,
        reason: str,
        idempotency_key: str | None,
        attempts: int,
    ) -> GatewayResult:
        if idempotency_key is not None:
            # 未成功不占位，允许客户端稍后凭同键安全重提。
            self.idempotency.fail(idempotency_key)
        return GatewayResult(
            Conclusion.DEGRADED,
            upstream,
            attempts,
            payload=self._make_degraded_payload(upstream),
            reason=reason,
            request_id=request_id,
        )
