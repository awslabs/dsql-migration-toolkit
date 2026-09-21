# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the structured file-based activity log (core/activity_log.py)."""

from __future__ import annotations

import json
import logging

import pytest

from dsql_migrator.core.activity_log import (
    ACTIVITY_LOGGER_NAME,
    ActivityCategory,
    ActivityStatus,
    activity_stdout_enabled,
    configure_activity_file_log,
    configure_activity_stdout_log,
    current_activity_log_level,
    disable_activity_stdout_log,
    log_activity,
    read_activity_log,
    render_activity_text,
    set_activity_log_level,
)


def _reset_activity_logger() -> None:
    """Detach any file handlers so each test configures a fresh path."""
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


def test_log_activity_writes_one_json_line_per_event_with_utc_ts(tmp_path) -> None:
    _reset_activity_logger()
    path = tmp_path / "activity.log"
    configure_activity_file_log(path)

    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.SUCCESS,
        target="orders",
        detail="1000 rows",
    )
    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.FAILURE,
        target="customers",
        error_code="OC000",
        detail="serialization failure",
    )

    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2  # one entry per line
    first = json.loads(lines[0])
    assert first["category"] == "full_load"
    assert first["action"] == "load table"
    assert first["status"] == "success"
    assert first["target"] == "orders"
    # UTC timestamp with a Z suffix so lines sort chronologically.
    assert first["ts"].endswith("Z")
    second = json.loads(lines[1])
    assert second["status"] == "failure"
    assert second["error_code"] == "OC000"
    assert second["level"] == "ERROR"  # failures log at ERROR


def test_configure_activity_file_log_is_idempotent(tmp_path) -> None:
    _reset_activity_logger()
    path = tmp_path / "activity.log"
    configure_activity_file_log(path)
    configure_activity_file_log(path)  # second call must not add a 2nd handler

    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    file_handlers = [
        h for h in logger.handlers if getattr(h, "_activity_path", None) == str(path)
    ]
    assert len(file_handlers) == 1

    log_activity(ActivityCategory.CONNECTION, "test source", status=ActivityStatus.SUCCESS)
    # A single handler means the event is written exactly once.
    assert len(path.read_text(encoding="utf-8").splitlines()) == 1


def test_configure_activity_file_log_raises_on_unwritable_path(tmp_path) -> None:
    # On a non-root Fargate container WORKDIR /app is read-only, so opening the
    # rotating file there raises OSError. app.main() catches this and falls back to
    # stdout logging instead of crash-looping the task (which ECS rolls back as
    # NotStabilized). This asserts the failure surfaces as an OSError subclass so
    # that guard's `except OSError` actually catches it.
    _reset_activity_logger()
    ro_dir = tmp_path / "readonly"
    ro_dir.mkdir()
    ro_dir.chmod(0o500)  # r-x: no write
    try:
        with pytest.raises(OSError):
            configure_activity_file_log(ro_dir / "activity.log")
    finally:
        ro_dir.chmod(0o700)  # restore so tmp cleanup can remove it


def test_read_activity_log_ndjson_and_text(tmp_path) -> None:
    _reset_activity_logger()
    path = tmp_path / "activity.log"
    configure_activity_file_log(path)
    log_activity(
        ActivityCategory.SCHEMA_CONVERSION,
        "apply object",
        status=ActivityStatus.SUCCESS,
        target="v_orders",
        detail="CREATED",
    )

    ndjson = read_activity_log(path, "ndjson")
    assert ndjson.decode("utf-8").count("\n") == 1
    assert json.loads(ndjson.decode("utf-8").splitlines()[0])["target"] == "v_orders"

    text = read_activity_log(path, "text").decode("utf-8")
    assert text.count("\n") == 1  # still one entry per line
    assert "[schema_conversion]" in text
    assert "apply object" in text
    assert "target=v_orders" in text
    assert "CREATED" in text


def test_read_activity_log_missing_file_returns_empty(tmp_path) -> None:
    assert read_activity_log(tmp_path / "nope.log") == b""


def test_activity_log_rotates_and_read_spans_backups(tmp_path, monkeypatch) -> None:
    """The handler rotates at a size cap and read_activity_log concatenates the
    retained backups oldest-first (bounded audit trail, not unbounded growth)."""
    from logging.handlers import RotatingFileHandler

    import dsql_migrator.core.activity_log as al

    _reset_activity_logger()
    # Force frequent rotation with a tiny segment cap so a handful of events
    # roll over without writing megabytes.
    monkeypatch.setattr(al, "_ACTIVITY_MAX_BYTES", 300)
    monkeypatch.setattr(al, "_ACTIVITY_BACKUP_COUNT", 3)
    path = tmp_path / "activity.log"
    configure_activity_file_log(path)

    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    handler = next(
        h for h in logger.handlers if getattr(h, "_activity_path", None) == str(path)
    )
    assert isinstance(handler, RotatingFileHandler)
    assert handler.maxBytes == 300
    assert handler.backupCount == 3

    for i in range(20):
        log_activity(
            ActivityCategory.FULL_LOAD,
            "load table",
            status=ActivityStatus.SUCCESS,
            target=f"t{i:02d}",
        )

    # Rotation produced at least one backup beyond the current segment, and the
    # backup count is capped (never grows past backupCount).
    assert (tmp_path / "activity.log.1").exists()
    assert not (tmp_path / "activity.log.4").exists()

    # The download spans the retained segments oldest-first: targets are in
    # chronological (ascending) order and include the most recent event.
    raw = read_activity_log(path).decode("utf-8")
    targets = [
        json.loads(line)["target"]
        for line in raw.splitlines()
        if line.strip()
    ]
    assert targets, "expected retained events"
    assert targets == sorted(targets)  # oldest-first across backups
    assert targets[-1] == "t19"  # current segment (newest) is last


def test_runtime_log_level_and_stdout_toggle_are_adjustable(tmp_path) -> None:
    """Log level and the stdout (CloudWatch) mirror are changeable at runtime,
    so an operator can troubleshoot without a redeploy."""
    _reset_activity_logger()
    configure_activity_file_log(tmp_path / "activity.log", level=logging.INFO)
    assert current_activity_log_level() == logging.INFO

    # Flip to DEBUG live (subsequent failures then carry stacktraces).
    set_activity_log_level(logging.DEBUG)
    assert current_activity_log_level() == logging.DEBUG

    # The stdout mirror toggles on and off at runtime.
    assert activity_stdout_enabled() is False
    configure_activity_stdout_log(level=logging.DEBUG)
    assert activity_stdout_enabled() is True
    disable_activity_stdout_log()
    assert activity_stdout_enabled() is False


def test_configure_activity_stdout_log_adds_idempotent_stream_handler(capsys) -> None:
    """The opt-in stdout handler emits one JSON line per event (for CloudWatch
    via the awslogs driver) and is not added twice on repeat calls."""
    _reset_activity_logger()
    configure_activity_stdout_log()
    configure_activity_stdout_log()  # idempotent: must not add a 2nd handler
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    stdout_handlers = [
        h for h in logger.handlers if getattr(h, "_activity_stdout", False)
    ]
    assert len(stdout_handlers) == 1

    log_activity(
        ActivityCategory.SYSTEM, "app started", status=ActivityStatus.INFO
    )
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if ln.strip()]
    assert lines, "expected a JSON line on stdout"
    obj = json.loads(lines[-1])
    assert obj["category"] == "system"
    assert obj["action"] == "app started"


def test_session_reset_emits_system_success_audit_line(capsys) -> None:
    """"Start over" records a SYSTEM 'session reset' (success) audit line.

    The audit log is process-wide and deliberately NOT cleared by a reset, so the
    reset itself must leave a trace (this is the shape ``_reset_session`` emits)."""
    _reset_activity_logger()
    configure_activity_stdout_log()

    log_activity(
        ActivityCategory.SYSTEM,
        "session reset",
        status=ActivityStatus.SUCCESS,
        detail="Start over — session workbench and saved snapshot cleared",
    )
    out = capsys.readouterr().out
    obj = json.loads([ln for ln in out.splitlines() if ln.strip()][-1])
    assert obj["category"] == "system"
    assert obj["action"] == "session reset"
    assert obj["status"] == "success"
    assert "Start over" in obj["detail"]


def test_configure_activity_file_log_uses_default_rotation_bounds(tmp_path) -> None:
    """By default the handler caps each segment at ~20 MiB and keeps 4 backups
    (~100 MiB total retained)."""
    from logging.handlers import RotatingFileHandler

    import dsql_migrator.core.activity_log as al

    _reset_activity_logger()
    path = tmp_path / "activity.log"
    configure_activity_file_log(path)
    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    handler = next(
        h for h in logger.handlers if getattr(h, "_activity_path", None) == str(path)
    )
    assert isinstance(handler, RotatingFileHandler)
    assert handler.maxBytes == al._ACTIVITY_MAX_BYTES == 20 * 1024 * 1024
    assert handler.backupCount == al._ACTIVITY_BACKUP_COUNT == 4


def test_failure_stacktrace_only_when_debug(tmp_path) -> None:
    _reset_activity_logger()
    path = tmp_path / "activity.log"

    try:
        raise ValueError("boom-detail")
    except ValueError as exc:
        captured = exc

    # INFO level: failure recorded, but no stacktrace field (clean routine logs).
    configure_activity_file_log(path, level=logging.INFO)
    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.FAILURE,
        target="orders",
        detail="ValueError: boom-detail",
        exc=captured,
    )
    info_obj = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    assert info_obj["status"] == "failure"
    assert "stacktrace" not in info_obj

    # DEBUG level: the full traceback is attached for debugging.
    _reset_activity_logger()
    path2 = tmp_path / "activity_debug.log"
    configure_activity_file_log(path2, level=logging.DEBUG)
    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.FAILURE,
        target="orders",
        detail="ValueError: boom-detail",
        exc=captured,
    )
    debug_obj = json.loads(path2.read_text(encoding="utf-8").splitlines()[-1])
    assert "stacktrace" in debug_obj
    assert "ValueError: boom-detail" in debug_obj["stacktrace"]
    assert "Traceback" in debug_obj["stacktrace"]

    # The text rendering attaches the trace as indented continuation lines while
    # the entry's main line still starts with the UTC timestamp.
    text = read_activity_log(path2, "text").decode("utf-8").splitlines()
    assert any(line.startswith("    ") and "ValueError" in line for line in text)


def test_debug_stacktrace_does_not_leak_exception_detail_row_values(tmp_path) -> None:
    # Property 7: even the DEBUG-only stacktrace must not carry a driver error's
    # DETAIL / "Failing row contains (...)" line (row column values). We keep the
    # stack FRAMES + a value-free "Type: first-line" tail, dropping the value dump.
    _reset_activity_logger()
    path = tmp_path / "activity_debug.log"

    try:
        raise ValueError(
            'duplicate key value violates unique constraint "users_email_key"\n'
            "DETAIL:  Key (email)=(alice@example.com) already exists, "
            "Failing row contains (1, alice@example.com, secrettoken123)."
        )
    except ValueError as exc:
        captured = exc

    configure_activity_file_log(path, level=logging.DEBUG)
    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.FAILURE,
        target="app.users",
        detail="UniqueViolation",
        exc=captured,
    )
    obj = json.loads(path.read_text(encoding="utf-8").splitlines()[-1])
    trace = obj["stacktrace"]
    # The actionable primary line survives; the row values do not.
    assert 'unique constraint "users_email_key"' in trace
    assert "Traceback" in trace  # still a readable traceback
    assert "alice@example.com" not in trace
    assert "secrettoken123" not in trace
    assert "Failing row" not in trace and "DETAIL" not in trace


def test_render_activity_text_passes_through_non_json_lines() -> None:
    out = render_activity_text(b"not json\n")
    assert out == b"not json\n"


# ---------------------------------------------------------------------------
# detail is sanitized centrally (Property 7)
# ---------------------------------------------------------------------------


def test_detail_keeps_only_the_first_line_so_row_values_cannot_leak(tmp_path) -> None:
    """A driver error's later lines carry the offending row's COLUMN VALUES.

    Several call sites interpolate a raw exception (``detail=f"Apply failed: {exc}"``), and
    psycopg keeps the server's ``DETAIL:`` / ``Failing row contains (...)`` lines in
    ``str(exc)`` -- so the durable log and its CloudWatch mirror carried row values.
    Enforced in ``log_activity`` rather than per caller so a future site cannot reintroduce
    it (Property 7).
    """
    from dsql_migrator.core.activity_log import (
        ActivityCategory,
        ActivityStatus,
        configure_activity_file_log,
        log_activity,
    )

    path = tmp_path / "a.ndjson"
    configure_activity_file_log(path)
    raw = (
        'duplicate key value violates unique constraint "users_email_key"\n'
        "DETAIL:  Key (email)=(alice@example.com) already exists.\n"
        "Failing row contains (14, alice@example.com, secret-token)."
    )
    log_activity(
        ActivityCategory.SCHEMA_CONVERSION, "apply object",
        status=ActivityStatus.FAILURE, detail=f"Apply failed: {raw}",
    )
    written = path.read_text()

    assert "alice@example.com" not in written, written
    assert "secret-token" not in written
    assert "Failing row contains" not in written
    # The actionable primary line survives.
    assert "users_email_key" in written
    # One event stays one line.
    assert len([ln for ln in written.splitlines() if ln.strip()]) == 1


def test_detail_is_length_capped_and_whitespace_collapsed(tmp_path) -> None:
    from dsql_migrator.core.activity_log import (
        _MAX_DETAIL_CHARS,
        ActivityCategory,
        ActivityStatus,
        _safe_detail,
        configure_activity_file_log,
        log_activity,
    )

    assert _safe_detail(None) is None
    assert _safe_detail("   ") is None
    assert _safe_detail("a\t\t b   c") == "a b c"
    long = _safe_detail("x" * (_MAX_DETAIL_CHARS + 200))
    assert long is not None and len(long) == _MAX_DETAIL_CHARS and long.endswith("...")

    path = tmp_path / "b.ndjson"
    configure_activity_file_log(path)
    log_activity(
        ActivityCategory.CDC, "start CDC connectors",
        status=ActivityStatus.STARTED, detail="g" * 4000,
    )
    assert len(path.read_text()) < 4000


def test_warning_status_maps_to_the_warning_log_level() -> None:
    # The calibrated middle: nothing failed, but the event is not routine (a waived
    # invariant, an operator stop). INFO buried it; FAILURE would claim a break.
    import logging

    from dsql_migrator.core.activity_log import ActivityStatus

    assert ActivityStatus.WARNING.value == "warning"
    import inspect

    from dsql_migrator.core import activity_log as al

    src = inspect.getsource(al.log_activity)
    assert "logging.WARNING if st == ActivityStatus.WARNING.value" in src
    assert logging.WARNING < logging.ERROR


def test_the_settings_tab_records_level_and_mirror_changes() -> None:
    """The audit trail must record changes to its own recording.

    A reader comparing two runs needs to know the quieter one was CONFIGURED that way rather
    than idle -- and turning the CloudWatch mirror off is, on ECS, what makes the trail stop
    surviving a task replacement. The mirror-off line is emitted BEFORE the handler is
    removed, or it would be the one event the sink it just removed never carried, leaving the
    CloudWatch copy ending with no explanation.
    """
    import inspect

    from dsql_migrator.ui import app as app_mod

    src = inspect.getsource(app_mod._render_activity_log_controls)
    assert '"activity log level changed"' in src
    assert '"activity log mirror changed"' in src
    # Ordering: the OFF line precedes disable_activity_stdout_log().
    off = src.index("CloudWatch Logs mirroring turned OFF")
    assert off < src.index("disable_activity_stdout_log()", off - 2000 if off > 2000 else 0) or \
        off < src.rindex("disable_activity_stdout_log()")
    # Severity: turning the mirror off is a real (deliberate) durability loss.
    mirror_off_block = src[off - 400:off + 400]
    assert "ActivityStatus.WARNING" in mirror_off_block, mirror_off_block[:200]
    # The change handlers, never the render body.
    assert "def _on_level" in src and "def _on_toggle" in src


def test_a_stall_reap_is_wired_to_an_audit_line() -> None:
    # core must not import the activity log, so the reap reports through a listener; without
    # the wiring the listener is dead code and a reaped job stays unaudited.
    import inspect

    from dsql_migrator.ui import app as app_mod

    src = inspect.getsource(app_mod)
    assert "JOB_MANAGER.set_stall_listener(_log_stalled_job)" in src
    fn = inspect.getsource(app_mod._log_stalled_job)
    assert '"job stalled"' in fn
    assert "ActivityCategory.SYSTEM" in fn, "the watchdog reaps validation jobs too"
    assert "ActivityStatus.FAILURE" in fn
    assert "unfinished" in fn and "marked FAILED" in fn
