def test_skeleton_importable():
    from app.config import ResilienceConfig
    from app.circuit_breaker import CircuitBreakerRegistry
    from app.gateway import Gateway
    from app.idempotency import IdempotencyStore
    from app.retry import decide_retry
    from app.upstream import StubUpstreamClient

    assert ResilienceConfig().failure_threshold > 0
    assert CircuitBreakerRegistry is not None
    assert Gateway is not None
    assert IdempotencyStore is not None
    assert decide_retry is not None
    assert StubUpstreamClient is not None
