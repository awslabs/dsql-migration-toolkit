# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Optimistic-concurrency (OCC) retry utility for Aurora DSQL writes.

Aurora DSQL uses optimistic concurrency control: a write transaction can fail at
commit with ``SQLSTATE 40001`` (a serialization failure) when it conflicts with a
concurrent transaction. DSQL reports two subcodes under ``40001``:

- ``OC000`` — a data (row) conflict, and
- ``OC001`` — a schema conflict.

Because every target write in this tool is designed to be **idempotent**
(e.g. ``INSERT ... ON CONFLICT``, ``CREATE ... IF NOT EXISTS``), simply retrying
the operation is safe. :func:`with_occ_retry` encapsulates that policy: it retries
the wrapped callable on ``40001`` using exponential backoff with jitter, and when
the maximum number of attempts is exhausted it re-raises the last ``40001`` error
so the caller can isolate the failure (e.g. mark a chunk ``FAILED`` and continue).

Conflict detection is based on the ``sqlstate`` attribute of the raised exception
(``getattr(exc, "sqlstate", None) == "40001"``). This works both for real
``psycopg`` (v3) errors such as ``psycopg.errors.SerializationFailure`` and for
test fakes, and avoids a hard import dependency on a specific exception class. Any
exception that is not a ``40001`` conflict propagates immediately without retry.

Correctness: Property 5 (OCC safety) — an unresolved ``40001`` conflict never
leaves partial or corrupt state; the wrapped operation is idempotent and, when
retries are exhausted, the original error surfaces rather than being swallowed.

This utility is intentionally minimal (Requirement 5.2): retry-on-40001, backoff
with jitter, and give-up-after-max-attempts. It does not enforce idempotency and
does not provide a generalized retry framework.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from typing import Callable, Optional, TypeVar

_LOGGER = logging.getLogger(__name__)

# SQLSTATE class for an optimistic-concurrency / serialization failure in DSQL.
OCC_SQLSTATE = "40001"

# Default retry budget and base backoff (design.md OCC handling signature).
DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_BASE_DELAY_SECONDS = 0.05

# Cap a single backoff sleep so exponential growth cannot block indefinitely.
DEFAULT_MAX_DELAY_SECONDS = 5.0

# Injectable sleep function (default ``time.sleep``); injectable in tests so they
# run instantly and deterministically without real waiting.
SleepFunc = Callable[[float], None]

# Injectable jitter source returning a multiplier in ``[0, 1)`` (default
# ``random.random``); injectable so tests are deterministic.
JitterFunc = Callable[[], float]

T = TypeVar("T")


def is_occ_conflict(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is a DSQL ``SQLSTATE 40001`` OCC conflict.

    Detection relies on the exception's ``sqlstate`` attribute rather than its
    type, so it matches both real ``psycopg`` errors (e.g.
    ``psycopg.errors.SerializationFailure``) and test fakes. Both OCC subcodes
    (``OC000`` data conflict, ``OC001`` schema conflict) share the ``40001``
    SQLSTATE and are therefore treated identically. Reused by callers that need
    OCC retry (e.g. the data migrator and the schema applier).
    """
    return getattr(exc, "sqlstate", None) == OCC_SQLSTATE


def with_occ_retry(
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    *,
    max_delay: float = DEFAULT_MAX_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
    retryable: Callable[[BaseException], bool] = is_occ_conflict,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Retry a target operation on ``SQLSTATE 40001`` with backoff and jitter.

    The wrapped operation **MUST be idempotent** (Property 5): this decorator
    does not enforce idempotency, it relies on it. On each retryable error the
    operation is retried after sleeping ``min(max_delay, base_delay * 2**attempt)``
    seconds scaled by a random jitter multiplier in ``[0, 1)``, where ``attempt``
    is the zero-based retry index. After ``max_attempts`` failed tries the last
    error is re-raised so the caller can isolate the failure. Any exception that
    is not retryable propagates immediately.

    ``retryable`` decides which errors are retried; it defaults to
    :func:`is_occ_conflict` (OCC ``40001`` only). Callers whose operation can also
    recover from a transient connection drop / expired token (e.g. the batched
    loader, which leases a fresh connection per attempt) pass a wider predicate.

    ``sleep`` and ``jitter`` are injectable so tests are deterministic and never
    sleep for real.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if base_delay < 0:
        raise ValueError("base_delay must be non-negative")
    if max_delay < 0:
        raise ValueError("max_delay must be non-negative")

    def decorator(operation: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(operation)
        def wrapper(*args: object, **kwargs: object) -> T:
            last_conflict: Optional[Exception] = None
            started = time.monotonic()
            for attempt in range(max_attempts):
                try:
                    return operation(*args, **kwargs)
                except Exception as exc:  # noqa: BLE001 - re-raised below if not retryable
                    if not retryable(exc):
                        raise
                    last_conflict = exc
                    if attempt + 1 >= max_attempts:
                        break
                    delay = _backoff_delay(attempt, base_delay, max_delay, jitter)
                    # Per-attempt trace (DEBUG): which error, its SQLSTATE, and the
                    # backoff — so a diagnostic run can see exactly how a batch was
                    # retried (e.g. every attempt a ConnectionTimeout during a storm).
                    _LOGGER.debug(
                        "occ-retry %d/%d after %s (sqlstate=%s) -> backoff %.2fs",
                        attempt + 1, max_attempts, type(exc).__name__,
                        getattr(exc, "sqlstate", None), delay,
                    )
                    sleep(delay)
            # Retries exhausted: surface the last error (Property 5). Log the
            # give-up at WARNING (always visible, no DEBUG needed) with the attempt
            # count, total elapsed, and last error/SQLSTATE -- direct evidence of
            # WHY a batch finally failed (budget too small vs a storm longer than
            # the budget vs a non-transient error), instead of inferring from timing.
            assert last_conflict is not None  # loop ran at least once
            _LOGGER.warning(
                "occ-retry gave up after %d attempts over %.1fs; last=%s sqlstate=%s: %s",
                max_attempts, time.monotonic() - started,
                type(last_conflict).__name__,
                getattr(last_conflict, "sqlstate", None),
                (str(last_conflict).splitlines() or [""])[0][:160],
            )
            raise last_conflict

        return wrapper

    return decorator


def _backoff_delay(
    attempt: int, base_delay: float, max_delay: float, jitter: JitterFunc
) -> float:
    """Compute the jittered, capped exponential backoff delay for a retry.

    The uncapped delay grows as ``base_delay * 2**attempt`` and is capped at
    ``max_delay`` before jitter is applied, so the returned value never exceeds
    ``max_delay``.
    """
    capped = min(max_delay, base_delay * (2 ** attempt))
    return capped * jitter()


__all__ = [
    "with_occ_retry",
    "is_occ_conflict",
    "OCC_SQLSTATE",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_BASE_DELAY_SECONDS",
    "DEFAULT_MAX_DELAY_SECONDS",
    "SleepFunc",
    "JitterFunc",
]
