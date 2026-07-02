"""Structured, file-based activity log for the migration tool.

The migration is a multi-step, high-stakes operation, so an operator needs an
auditable timeline of *what happened and when*: connection tests, the
assessment run, every per-object schema apply, every per-table Full Load
outcome, and CDC control-plane actions. This module records each such event as
**one line** in a file (NDJSON), prefixed with a **UTC** timestamp, so the whole
timeline can be downloaded from the UI and read or sorted by time.

Design notes:

- **One transport, no new service (no-bloat).** Events flow through the standard
  :mod:`logging` framework on a dedicated logger (:data:`ACTIVITY_LOGGER_NAME`)
  with a JSON-per-line :class:`FileHandler`. Any module -- core or UI -- emits an
  event with :func:`log_activity` without threading a new seam through call
  stacks, mirroring how the rest of the code already uses ``logging``.
- **Success *and* failure.** Unlike the job-scoped
  :class:`~dsql_migrator.core.error_log.ErrorLogStore` (which captures data
  errors only), this log records successful per-table/per-object outcomes too,
  so the file is a complete record of the run, not just its failures.
- **Confidentiality (Property 7).** Only the structured fields below are
  serialized; callers pass English, credential-free ``detail`` text. Connection
  events log non-secret coordinates (host/db/region) only -- never passwords or
  IAM tokens.
- **One entry per line, UTC, time-sortable.** Each record is a single JSON
  object beginning with ``ts`` (UTC ISO-8601, ``Z`` suffix), so ``sort`` and
  ``grep`` work and the UI can render it chronologically.
"""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
import traceback
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, Union

# Dedicated logger for activity events; configured with a JSON FileHandler by
# :func:`configure_activity_file_log` and kept off the root logger so the
# structured file never mixes with the human-readable terminal stream.
ACTIVITY_LOGGER_NAME = "dsql_migrator.activity"

# Rotation bounds for the activity-log file: cap each segment and keep a small
# number of rotated backups so the on-disk audit trail -- and the in-memory
# download that concatenates the segments -- stays bounded in a long-lived
# deployment instead of growing without limit (large-scale: never let a file or
# its whole-file read grow unbounded). Total on-disk cap is roughly
# ``_ACTIVITY_MAX_BYTES * (_ACTIVITY_BACKUP_COUNT + 1)`` (~100 MiB by default).
_ACTIVITY_MAX_BYTES = 20 * 1024 * 1024  # 20 MiB per segment
_ACTIVITY_BACKUP_COUNT = 4  # keep 4 rotated backups (5 segments => ~100 MiB)

# Structured fields carried on each log record via ``extra`` and serialized (when
# present) into the one-line JSON object. ``stacktrace`` is only attached for a
# failure when DEBUG logging is enabled (see :func:`log_activity`).
_ACTIVITY_FIELDS = (
    "category",
    "action",
    "status",
    "target",
    "detail",
    "error_code",
    "stacktrace",
)


class ActivityCategory(str, Enum):
    """The workflow area an activity event belongs to (used for filtering)."""

    CONNECTION = "connection"
    ASSESSMENT = "assessment"
    SCHEMA_CONVERSION = "schema_conversion"
    FULL_LOAD = "full_load"
    CDC = "cdc"
    VALIDATION = "validation"
    SYSTEM = "system"


class ActivityStatus(str, Enum):
    """The outcome an activity event records."""

    STARTED = "started"
    SUCCESS = "success"
    FAILURE = "failure"
    INFO = "info"


_CategoryArg = Union[ActivityCategory, str]
_StatusArg = Union[ActivityStatus, str]


class ActivityJsonFormatter(logging.Formatter):
    """Render one activity record as a single-line JSON object (UTC ``ts``).

    The timestamp is derived from the record's creation time in UTC (ISO-8601
    with a ``Z`` suffix) so lines sort chronologically. Structured fields set via
    :func:`log_activity` are included when present; a record logged without those
    fields keeps its plain ``message`` so ad-hoc logs are not lost.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
        }
        for field in _ACTIVITY_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = value
        # Always keep a human-readable message (the summary built by
        # ``log_activity`` or an ad-hoc message), so the line reads on its own.
        payload["message"] = record.getMessage()
        return json.dumps(payload, ensure_ascii=False)


def log_activity(
    category: _CategoryArg,
    action: str,
    *,
    status: _StatusArg = ActivityStatus.INFO,
    target: Optional[str] = None,
    detail: Optional[str] = None,
    error_code: Optional[str] = None,
    exc: Optional[BaseException] = None,
) -> None:
    """Record one activity event (one line in the activity log).

    ``category`` is the workflow area (see :class:`ActivityCategory`), ``action``
    a short English verb phrase (e.g. ``"load table"``), ``status`` the outcome,
    ``target`` the object/table the event concerns, ``detail`` an optional
    English, credential-free explanation (e.g. an error message), and
    ``error_code`` an optional SQLSTATE-like code. A ``FAILURE`` is logged at
    ``ERROR`` level; everything else at ``INFO``. Safe to call from any thread
    (the logging framework is thread-safe) and a no-op for output when no file
    handler is configured (the record is still emitted to the logger).

    ``exc`` is the caught exception (when any): its full traceback is attached as
    a ``stacktrace`` field **only when DEBUG logging is enabled**
    (``DSQL_MIGRATOR_LOG_LEVEL=DEBUG``), so routine logs stay clean while a
    debugging run captures the failing call stack. The traceback never includes
    row values or credentials (it is the Python call stack, not data); ``detail``
    remains the credential-free summary message in all cases.
    """
    cat = category.value if isinstance(category, ActivityCategory) else str(category)
    st = status.value if isinstance(status, ActivityStatus) else str(status)
    parts = [f"[{cat}]", action, f"({st})"]
    if target:
        parts.append(f"target={target}")
    if error_code:
        parts.append(f"code={error_code}")
    if detail:
        parts.append(f"- {detail}")
    summary = " ".join(parts)
    level = logging.ERROR if st == ActivityStatus.FAILURE.value else logging.INFO
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    # Attach the full traceback for debugging only when DEBUG is enabled, so the
    # detailed call stack is available on demand without bloating routine logs.
    stacktrace: Optional[str] = None
    if exc is not None and logger.isEnabledFor(logging.DEBUG):
        stacktrace = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ).strip()
    logger.log(
        level,
        summary,
        extra={
            "category": cat,
            "action": action,
            "status": st,
            "target": target,
            "detail": detail,
            "error_code": error_code,
            "stacktrace": stacktrace,
        },
    )


def configure_activity_file_log(
    path: Union[str, Path], *, level: int = logging.INFO
) -> Path:
    """Attach a rotating JSON-per-line file handler at ``path`` (idempotent).

    Sets the activity logger's level, stops propagation to the root logger (so
    the structured file stays separate from the terminal stream), and adds a
    :class:`~logging.handlers.RotatingFileHandler` with an
    :class:`ActivityJsonFormatter`. The handler caps each segment at
    :data:`_ACTIVITY_MAX_BYTES` and keeps :data:`_ACTIVITY_BACKUP_COUNT` rotated
    backups, so the audit trail never grows without bound in a long-lived
    deployment. Calling again for the same path is a no-op, so it is safe to
    invoke on every app start. Returns the resolved path.
    """
    resolved = Path(path)
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    target = str(resolved)
    for handler in logger.handlers:
        if getattr(handler, "_activity_path", None) == target:
            return resolved
    if resolved.parent and not resolved.parent.exists():
        resolved.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        resolved,
        maxBytes=_ACTIVITY_MAX_BYTES,
        backupCount=_ACTIVITY_BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setFormatter(ActivityJsonFormatter())
    # Tag the handler so a repeat call can detect and skip the duplicate.
    handler._activity_path = target  # type: ignore[attr-defined]
    logger.addHandler(handler)
    return resolved


def configure_activity_stdout_log(*, level: int = logging.INFO) -> None:
    """Also emit activity events to stdout as JSON lines (idempotent, opt-in).

    Companion to :func:`configure_activity_file_log`: on ECS the container's
    awslogs driver forwards stdout to CloudWatch Logs, so enabling this gives a
    durable, queryable copy of the audit trail that survives task replacement
    (the rotating file lives on the task's ephemeral storage). Uses the same
    :class:`ActivityJsonFormatter`, so each event is one JSON line in CloudWatch
    too. A no-op on repeat calls (the stdout handler is tagged), and harmless
    off ECS (the lines simply print to the console).
    """
    import sys

    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    for handler in logger.handlers:
        if getattr(handler, "_activity_stdout", False):
            return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ActivityJsonFormatter())
    # Tag the handler so a repeat call can detect and skip the duplicate.
    handler._activity_stdout = True  # type: ignore[attr-defined]
    logger.addHandler(handler)


def set_activity_log_level(level: int) -> None:
    """Change the activity logger's level at runtime (no restart needed).

    Lets an operator flip INFO<->DEBUG while troubleshooting without a redeploy;
    at DEBUG, subsequent failure events carry the full stacktrace. Process-wide
    (the activity logger is a singleton), which suits the single-task app.
    """
    logging.getLogger(ACTIVITY_LOGGER_NAME).setLevel(level)


def current_activity_log_level() -> int:
    """Return the activity logger's current level (e.g. ``logging.INFO``)."""
    return logging.getLogger(ACTIVITY_LOGGER_NAME).level


def disable_activity_stdout_log() -> None:
    """Detach the stdout activity handler if present (stop the CloudWatch mirror).

    Inverse of :func:`configure_activity_stdout_log`; the rotating file handler
    is unaffected. A no-op when no stdout handler is attached.
    """
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    for handler in list(logger.handlers):
        if getattr(handler, "_activity_stdout", False):
            logger.removeHandler(handler)
            handler.close()


def activity_stdout_enabled() -> bool:
    """Return whether activity events are currently mirrored to stdout."""
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    return any(getattr(h, "_activity_stdout", False) for h in logger.handlers)


def render_activity_text(raw: bytes) -> bytes:
    """Convert NDJSON activity bytes into a readable, one-line-per-entry text.

    Each JSON line becomes ``<ts> <STATUS> [<category>] <action> [target=..]
    [code=..] [- detail]``; non-JSON lines pass through unchanged. The result is
    still one entry per line, ordered as written (chronologically), for the
    download view.
    """
    lines: list[str] = []
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            lines.append(line)
            continue
        parts = [
            str(obj.get("ts", "")),
            str(obj.get("status", "")).upper(),
            f"[{obj.get('category', '')}]",
            str(obj.get("action") or obj.get("message", "")),
        ]
        if obj.get("target"):
            parts.append(f"target={obj['target']}")
        if obj.get("error_code"):
            parts.append(f"code={obj['error_code']}")
        if obj.get("detail"):
            parts.append(f"- {obj['detail']}")
        lines.append(" ".join(part for part in parts if part))
        # When a DEBUG run captured a traceback, attach it as indented
        # continuation lines beneath the entry so the timeline stays scannable
        # (each event's main line still starts with its UTC timestamp at col 0).
        stacktrace = obj.get("stacktrace")
        if stacktrace:
            for trace_line in str(stacktrace).splitlines():
                lines.append(f"    {trace_line}")
    text = "\n".join(lines)
    return (text + "\n").encode("utf-8") if text else b""


def _activity_log_segments(path: Path) -> list[Path]:
    """Return existing activity-log segments oldest-first.

    :class:`~logging.handlers.RotatingFileHandler` writes the current file at
    ``path`` and rotated backups at ``path.1`` (newest backup) through
    ``path.N`` (oldest), where ``N`` is :data:`_ACTIVITY_BACKUP_COUNT`.
    Chronological order is therefore the highest-numbered backup down to ``.1``
    and finally the current file. Missing segments are skipped.
    """
    segments = [
        Path(f"{path}.{i}") for i in range(_ACTIVITY_BACKUP_COUNT, 0, -1)
    ]
    segments.append(path)
    return [segment for segment in segments if segment.exists()]


def read_activity_log(
    path: Union[str, Path], fmt: str = "ndjson"
) -> bytes:
    """Read the activity log at ``path`` as downloadable bytes (UTF-8).

    Spans the rotated backups so the downloaded timeline is the full retained
    history (oldest-first), not just the current segment, while staying bounded
    by the rotation cap. ``ndjson`` (default) returns the concatenated
    one-JSON-object-per-line bytes; ``text`` returns the human-readable
    rendering from :func:`render_activity_text`. Returns empty bytes when no
    segment exists yet.
    """
    resolved = Path(path)
    segments = _activity_log_segments(resolved)
    if not segments:
        return b""
    raw = b"".join(segment.read_bytes() for segment in segments)
    if fmt == "text":
        return render_activity_text(raw)
    return raw


__all__ = [
    "ACTIVITY_LOGGER_NAME",
    "ActivityCategory",
    "ActivityStatus",
    "ActivityJsonFormatter",
    "log_activity",
    "configure_activity_file_log",
    "configure_activity_stdout_log",
    "disable_activity_stdout_log",
    "activity_stdout_enabled",
    "set_activity_log_level",
    "current_activity_log_level",
    "render_activity_text",
    "read_activity_log",
]
