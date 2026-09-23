"""带关联标识的结构化日志。

未显式携带关联标识的内部日志（如熔断器状态迁移）使用 cid=- 占位，
保证同一条请求的全部日志都能用 correlation_id 串起来。
"""

from __future__ import annotations

import logging
import sys

_CONFIGURED = False


class _DefaultCidFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "correlation_id"):
            record.correlation_id = "-"
        return True


def configure_logging(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] cid=%(correlation_id)s %(name)s: %(message)s"
        )
    )
    handler.addFilter(_DefaultCidFilter())
    logger = logging.getLogger("gateway")
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addFilter(_DefaultCidFilter())
    logger.propagate = False
    _CONFIGURED = True


def get_logger(name: str = "gateway") -> logging.Logger:
    configure_logging()
    return logging.getLogger(name)


class CorrelationAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.setdefault("extra", {})
        extra["correlation_id"] = self.extra.get("correlation_id", "-")
        return msg, kwargs


def with_cid(logger: logging.Logger, correlation_id: str) -> logging.LoggerAdapter:
    return CorrelationAdapter(logger, {"correlation_id": correlation_id})
