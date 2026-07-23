# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the OCC retry utility (conflict simulation).

Covers (Property 5 / Requirement 5.2):
- ``is_occ_conflict`` detects ``SQLSTATE 40001`` via the ``sqlstate`` attribute.
- An operation that raises ``40001`` N times then succeeds is retried and its
  result returned, with the expected call count.
- An operation that always raises ``40001`` re-raises after exactly
  ``max_attempts`` tries (the original error is not swallowed).
- A non-``40001`` error propagates immediately without retry.
- Backoff grows exponentially with jitter, capped at ``max_delay`` (asserted via
  an injected sleep recorder and an injected deterministic jitter source; no real
  sleeping occurs).
- ``functools.wraps`` metadata is preserved on the wrapped callable.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.occ import (
    DEFAULT_MAX_ATTEMPTS,
    OCC_SQLSTATE,
    is_occ_conflict,
    with_occ_retry,
)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeSerializationFailure(Exception):
    """A fake psycopg-like error exposing ``sqlstate`` (simulates 40001)."""

    def __init__(self, sqlstate: str = OCC_SQLSTATE, message: str = "conflict") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class _SleepRecorder:
    """An injectable sleep function that records the delays it was asked for."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def _constant_jitter(value: float) -> "_ConstantJitter":
    return _ConstantJitter(value)


class _ConstantJitter:
    """A deterministic jitter source returning a fixed multiplier."""

    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _FlakyOperation:
    """An operation that raises a 40001 conflict ``failures`` times then succeeds."""

    def __init__(self, failures: int, result: object = "ok") -> None:
        self.failures = failures
        self.result = result
        self.calls = 0

    def __call__(self) -> object:
        self.calls += 1
        if self.calls <= self.failures:
            raise _FakeSerializationFailure()
        return self.result


# ---------------------------------------------------------------------------
# is_occ_conflict
# ---------------------------------------------------------------------------


def test_is_occ_conflict_detects_40001() -> None:
    assert is_occ_conflict(_FakeSerializationFailure()) is True


def test_is_occ_conflict_rejects_other_sqlstates() -> None:
    assert is_occ_conflict(_FakeSerializationFailure(sqlstate="23505")) is False


def test_is_occ_conflict_rejects_errors_without_sqlstate() -> None:
    assert is_occ_conflict(RuntimeError("boom")) is False


# ---------------------------------------------------------------------------
# Retry then succeed
# ---------------------------------------------------------------------------


def test_retries_on_conflict_then_returns_result() -> None:
    operation = _FlakyOperation(failures=3, result="loaded")
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(
        max_attempts=5, sleep=sleeper, jitter=_constant_jitter(0.5)
    )(operation)
    result = decorated()

    assert result == "loaded"
    assert operation.calls == 4  # 3 conflicts + 1 success
    assert len(sleeper.delays) == 3  # one sleep before each retry


def test_succeeds_first_try_without_sleeping() -> None:
    operation = _FlakyOperation(failures=0, result=42)
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(sleep=sleeper, jitter=_constant_jitter(0.5))(operation)
    result = decorated()

    assert result == 42
    assert operation.calls == 1
    assert sleeper.delays == []


def test_arguments_are_forwarded_to_wrapped_operation() -> None:
    sleeper = _SleepRecorder()

    @with_occ_retry(sleep=sleeper, jitter=_constant_jitter(0.0))
    def add(a: int, b: int) -> int:
        return a + b

    assert add(2, b=3) == 5


# ---------------------------------------------------------------------------
# Max attempts exhausted
# ---------------------------------------------------------------------------


def test_reraises_after_max_attempts_when_always_conflicting() -> None:
    operation = _FlakyOperation(failures=1000)  # always conflicts
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(
        max_attempts=4, sleep=sleeper, jitter=_constant_jitter(0.5)
    )(operation)

    with pytest.raises(_FakeSerializationFailure) as exc_info:
        decorated()

    assert exc_info.value.sqlstate == OCC_SQLSTATE
    assert operation.calls == 4  # exactly max_attempts tries
    assert len(sleeper.delays) == 3  # no sleep after the final failed attempt


def test_giveup_logs_warning_with_attempts_and_last_error(caplog) -> None:
    # On exhaustion, a WARNING is logged with the attempt count and last error so a
    # diagnostic run shows WHY a batch finally failed (budget vs storm) without
    # relying on DEBUG level or timing inference.
    import logging

    operation = _FlakyOperation(failures=1000)  # always conflicts
    decorated = with_occ_retry(
        max_attempts=4, sleep=_SleepRecorder(), jitter=_constant_jitter(0.5)
    )(operation)
    with caplog.at_level(logging.WARNING, logger="dsql_migrator.core.occ"):
        with pytest.raises(_FakeSerializationFailure):
            decorated()
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings, "expected a give-up WARNING"
    msg = warnings[-1].getMessage()
    assert "gave up after 4 attempts" in msg
    assert "_FakeSerializationFailure" in msg
    assert OCC_SQLSTATE in msg


def test_single_attempt_does_not_sleep_and_reraises() -> None:
    operation = _FlakyOperation(failures=1000)
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(
        max_attempts=1, sleep=sleeper, jitter=_constant_jitter(0.5)
    )(operation)

    with pytest.raises(_FakeSerializationFailure):
        decorated()

    assert operation.calls == 1
    assert sleeper.delays == []


# ---------------------------------------------------------------------------
# Non-OCC errors propagate immediately
# ---------------------------------------------------------------------------


def test_non_occ_error_propagates_without_retry() -> None:
    calls = {"count": 0}
    sleeper = _SleepRecorder()

    @with_occ_retry(max_attempts=5, sleep=sleeper, jitter=_constant_jitter(0.5))
    def operation() -> None:
        calls["count"] += 1
        raise ValueError("not a conflict")

    with pytest.raises(ValueError, match="not a conflict"):
        operation()

    assert calls["count"] == 1  # tried once, no retry
    assert sleeper.delays == []


# ---------------------------------------------------------------------------
# Backoff: exponential growth with jitter, capped
# ---------------------------------------------------------------------------


def test_backoff_grows_exponentially_with_jitter() -> None:
    operation = _FlakyOperation(failures=1000)
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(
        max_attempts=5,
        base_delay=0.1,
        max_delay=100.0,
        sleep=sleeper,
        jitter=_constant_jitter(0.5),
    )(operation)

    with pytest.raises(_FakeSerializationFailure):
        decorated()

    # base_delay * 2**attempt * jitter, for attempt = 0..3.
    assert sleeper.delays == [
        0.1 * 1 * 0.5,
        0.1 * 2 * 0.5,
        0.1 * 4 * 0.5,
        0.1 * 8 * 0.5,
    ]


def test_backoff_is_capped_at_max_delay() -> None:
    operation = _FlakyOperation(failures=1000)
    sleeper = _SleepRecorder()

    decorated = with_occ_retry(
        max_attempts=6,
        base_delay=1.0,
        max_delay=4.0,
        sleep=sleeper,
        jitter=_constant_jitter(1.0),
    )(operation)

    with pytest.raises(_FakeSerializationFailure):
        decorated()

    # Uncapped would be [1, 2, 4, 8, 16]; capped at 4.0 then jitter x1.0.
    assert sleeper.delays == [1.0, 2.0, 4.0, 4.0, 4.0]


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_max_attempts_must_be_at_least_one() -> None:
    with pytest.raises(ValueError):
        with_occ_retry(max_attempts=0)


def test_base_delay_must_be_non_negative() -> None:
    with pytest.raises(ValueError):
        with_occ_retry(base_delay=-1.0)


def test_max_delay_must_be_non_negative() -> None:
    with pytest.raises(ValueError):
        with_occ_retry(max_delay=-1.0)


# ---------------------------------------------------------------------------
# functools.wraps metadata preservation
# ---------------------------------------------------------------------------


def test_wraps_preserves_function_metadata() -> None:
    @with_occ_retry()
    def load_chunk(chunk_id: int) -> int:
        """Load a single chunk into the target."""
        return chunk_id

    assert load_chunk.__name__ == "load_chunk"
    assert load_chunk.__doc__ == "Load a single chunk into the target."


def test_default_max_attempts_matches_design_default() -> None:
    assert DEFAULT_MAX_ATTEMPTS == 10
