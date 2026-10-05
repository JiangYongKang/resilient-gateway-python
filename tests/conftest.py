"""测试场景覆盖日志。

每个用例通过 :func:`log_scenario` 记录"场景 -> 结果"，
pytest 结束时在终端汇总打印，便于核对覆盖了哪些场景、各自结果。
"""
from __future__ import annotations

import pytest

SCENARIO_LOG: list[str] = []


def log_scenario(scenario: str, outcome: str) -> None:
    SCENARIO_LOG.append(f"[scenario] {scenario} -> {outcome}")


@pytest.hookimpl(tryfirst=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config):
    if SCENARIO_LOG:
        terminalreporter.write_sep("-", "covered scenarios")
        for line in SCENARIO_LOG:
            terminalreporter.write_line(line)
