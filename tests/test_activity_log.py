# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the structured file-based activity log (core/activity_log.py)."""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import types

import pytest

from dsql_migrator.core.activity_log import (
    ACTIVITY_LOGGER_NAME,
    ActivityCategory,
    ActivityStatus,
    activity_stdout_destination,
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
    # Still hard-bounded (this file is NDJSON with a size cap and one event per line)...
    assert long is not None and len(long) == _MAX_DETAIL_CHARS
    # ...but a truncation now SAYS how much it dropped. A bare "..." left the reader unable
    # to tell a complete message from a clipped one, so they had to distrust all of them --
    # and it hid that the missing half may be the part naming the failing column.
    assert long.endswith("[+200 chars truncated]"), long[-40:]
    # The cap must fit a realistic dead-letter reason intact: driver text + PK + operation +
    # the failing SQL template. At 500 it cut mid-identifier inside the ON CONFLICT clause,
    # exactly where the message starts to be diagnostic.
    assert _MAX_DETAIL_CHARS >= 1500, _MAX_DETAIL_CHARS
    realistic = (
        "quarantine record to DLQ target=ecommerce.products code=07006 - DLQ offset=19: "
        "sqlstate=07006 Can't infer the SQL type to use for an instance of "
        "java.util.ArrayList. Use setObject() with an explicit Types value to specify the "
        "type to use. | pk: id=5 | op: u | sql: INSERT INTO \"ecommerce\".\"products\" "
        + ", ".join(f'"col{i}"' for i in range(12))
        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (\"id\") DO UPDATE SET "
        + ", ".join(f'"col{i}" = EXCLUDED."col{i}"' for i in range(12))
    )
    kept = _safe_detail(realistic)
    assert kept == " ".join(realistic.split()), "a real dead-letter reason must survive whole"
    assert "truncated]" not in kept

    path = tmp_path / "b.ndjson"
    configure_activity_file_log(path)
    log_activity(
        ActivityCategory.CDC, "start CDC connectors",
        status=ActivityStatus.STARTED, detail="g" * 40_000,
    )
    # The written line is bounded by the cap, not by the input: the point of this assertion
    # is that an unbounded interpolation (a full GTID set, a driver message carrying a whole
    # statement) cannot write an unbounded NDJSON line. Keyed to _MAX_DETAIL_CHARS plus room
    # for the envelope, so raising the cap does not silently make this vacuous.
    written = path.read_text()
    assert written.count("\n") == 1, "one event must stay one line"
    record = json.loads(written)
    # Assert the DETAIL field, not the whole line: the envelope (and the human-readable
    # message the renderer builds from it) also carry text, so a whole-line bound would be
    # measuring the wrong thing. What matters is that an unbounded interpolation -- a full
    # GTID set, a driver message carrying a whole statement -- cannot write an unbounded
    # field, and that the reader is told it was cut.
    assert len(record["detail"]) == _MAX_DETAIL_CHARS
    assert record["detail"].endswith("chars truncated]")


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


# ---------------------------------------------------------------------------
# Where the stdout mirror goes, and what the Settings tab says about it
# ---------------------------------------------------------------------------

_ECS = {"ECS_CONTAINER_METADATA_URI_V4": "http://169.254.170.2/v4/0123abcd-task"}
_STDOUT = types.SimpleNamespace(st_dev=8, st_ino=12345)


def test_the_mirror_destination_is_cloudwatch_on_ecs_whatever_the_journal_stream() -> None:
    # ECS is checked first: its awslogs driver takes stdout, whatever else is set.
    assert activity_stdout_destination(_ECS, _STDOUT) == "cloudwatch"
    assert activity_stdout_destination({**_ECS, "JOURNAL_STREAM": "8:12345"}, _STDOUT) == (
        "cloudwatch"
    )
    # Set means non-blank, as for the task-IP lookup that reads the same variable.
    assert activity_stdout_destination(
        {"ECS_CONTAINER_METADATA_URI_V4": "  "}, _STDOUT
    ) == "console"


def test_the_mirror_destination_is_journald_only_when_the_journal_stream_is_stdout() -> None:
    # systemd sets JOURNAL_STREAM to "<device>:<inode>" of the journal connection. A process
    # started from such a service inherits it with its own stdout elsewhere, so it must be
    # compared with stdout, not merely present (systemd.exec(5)).
    assert activity_stdout_destination({"JOURNAL_STREAM": "8:12345"}, _STDOUT) == "journald"
    for mismatched in ("8:12346", "9:12345", "12345:8"):
        assert activity_stdout_destination({"JOURNAL_STREAM": mismatched}, _STDOUT) == (
            "console"
        ), mismatched
    for malformed in (
        "", "8", "8:", ":12345", "8:12345:0", "a:b", " 8:12345", "8:12345 ", "8:12345\n",
        "8:-1", "+8:12345", "0x8:12345", "８:12345",
    ):
        assert activity_stdout_destination({"JOURNAL_STREAM": malformed}, _STDOUT) == (
            "console"
        ), repr(malformed)
    assert activity_stdout_destination({}, _STDOUT) == "console"


def test_the_mirror_destination_reads_the_environment_and_stdout_itself(
    monkeypatch, tmp_path
) -> None:
    # The defaults: os.environ, and fstat of sys.stdout -- the stream the mirror's handler
    # writes to -- read at call time.
    monkeypatch.delenv("ECS_CONTAINER_METADATA_URI_V4", raising=False)
    with open(tmp_path / "journal-stream", "w", encoding="utf-8") as stream:
        stat = os.fstat(stream.fileno())
        monkeypatch.setattr(sys, "stdout", stream)
        monkeypatch.setenv("JOURNAL_STREAM", f"{stat.st_dev}:{stat.st_ino}")
        assert activity_stdout_destination() == "journald"
        monkeypatch.setenv("JOURNAL_STREAM", f"{stat.st_dev}:{stat.st_ino + 1}")
        assert activity_stdout_destination() == "console"
        monkeypatch.delenv("JOURNAL_STREAM")
        assert activity_stdout_destination() == "console"
        monkeypatch.setenv("ECS_CONTAINER_METADATA_URI_V4", _ECS["ECS_CONTAINER_METADATA_URI_V4"])
        assert activity_stdout_destination() == "cloudwatch"


def test_the_mirror_destination_is_console_when_stdout_cannot_be_inspected(
    monkeypatch, tmp_path
) -> None:
    import dsql_migrator.core.activity_log as al

    stream = {"JOURNAL_STREAM": "8:12345"}
    with open(tmp_path / "closed", "w", encoding="utf-8") as closed:
        pass
    # fileno() raises AttributeError, io.UnsupportedOperation and ValueError respectively.
    for stdout in (None, io.StringIO(), closed):
        monkeypatch.setattr(sys, "stdout", stdout)
        assert activity_stdout_destination(stream) == "console", stdout

    def _refuse(_fd: int) -> os.stat_result:
        raise OSError(9, "Bad file descriptor")

    with open(tmp_path / "open", "w", encoding="utf-8") as stdout, monkeypatch.context() as m:
        m.setattr(sys, "stdout", stdout)
        m.setattr(al.os, "fstat", _refuse)
        assert activity_stdout_destination(stream) == "console"


class _SettingsUi:
    """NiceGUI double for the Activity log tab: keeps each control's change handler and the
    notifications sent, so a test can turn the controls the way the browser does."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.handlers: dict[str, object] = {}
        self.notices: list[str] = []

    class _El:
        def __init__(self, ui) -> None:
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def style(self, *_a, **_k):
            return self

        def tooltip(self, text="", *_a, **_k):
            self._ui.texts.append(str(text))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._El(self)

    def select(self, *_a, on_change=None, **_k):
        self.handlers["level"] = on_change
        return self._El(self)

    def switch(self, *_a, on_change=None, **_k):
        self.handlers["mirror"] = on_change
        return self._El(self)

    def button(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def icon(self, *_a, **_k):
        return self._El(self)

    def separator(self, *_a, **_k):
        return self._El(self)

    def notify(self, message="", *_a, **_k):
        self.notices.append(str(message))


# What each destination's switch records and says (the stdout mirror's three homes).
_MIRROR_CHANGES = {
    "cloudwatch": (
        ActivityStatus.WARNING,
        "CloudWatch Logs mirroring turned OFF -- the activity log now survives only in the "
        "container's file sink, which a task replacement discards",
        "Stopped mirroring the activity log to CloudWatch Logs.",
        "CloudWatch Logs mirroring turned ON",
        "Mirroring the activity log to CloudWatch Logs (via stdout on ECS).",
    ),
    "journald": (
        ActivityStatus.INFO,
        "system journal mirroring turned OFF -- the activity log file is unaffected",
        "Stopped mirroring the activity log to the system journal.",
        "system journal mirroring turned ON",
        "Mirroring the activity log to the system journal (journalctl -u dsql-migrator).",
    ),
    "console": (
        ActivityStatus.INFO,
        "console mirroring turned OFF -- the activity log file is unaffected",
        "Stopped mirroring the activity log to the console.",
        "console mirroring turned ON",
        "Mirroring the activity log to the console.",
    ),
}


@pytest.mark.parametrize("destination", sorted(_MIRROR_CHANGES))
def test_the_settings_tab_records_level_and_mirror_changes(monkeypatch, destination) -> None:
    """The audit trail must record changes to its own recording.

    A reader comparing two runs needs to know the quieter one was CONFIGURED that way rather
    than idle. The mirror-off line is emitted BEFORE the handler is removed, or it would be
    the one event the sink it just removed never carried, leaving the mirrored copy ending
    with no explanation. It is a WARNING on ECS only, where the mirror is what makes the
    trail survive a task replacement; on the EC2 host the file stays on the retained state
    volume, and a local run keeps it on disk, so there turning the mirror off loses nothing.
    """
    import dsql_migrator.core.activity_log as al
    from dsql_migrator.ui import app as app_mod

    off_status, off_detail, off_notice, on_detail, on_notice = _MIRROR_CHANGES[destination]
    calls: list[tuple] = []
    monkeypatch.setattr(app_mod, "activity_stdout_destination", lambda: destination)
    monkeypatch.setattr(al, "log_activity", lambda *a, **k: calls.append(("log", a, k)))
    monkeypatch.setattr(al, "disable_activity_stdout_log", lambda: calls.append(("off",)))
    monkeypatch.setattr(
        al, "configure_activity_stdout_log", lambda **k: calls.append(("on", k))
    )
    monkeypatch.setattr(al, "set_activity_log_level", lambda lvl: calls.append(("lvl", lvl)))
    ui = _SettingsUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    app_mod._render_activity_log_controls("/tmp/does-not-matter.ndjson")
    assert calls == [] and ui.notices == []  # rendering changes nothing

    ui.handlers["level"](types.SimpleNamespace(value="DEBUG"))
    assert calls[0] == ("lvl", logging.DEBUG)
    assert calls[1][1] == (ActivityCategory.SYSTEM, "activity log level changed")
    assert calls[1][2]["status"] == ActivityStatus.INFO
    assert calls[1][2]["detail"].endswith("-> DEBUG")
    calls.clear()

    ui.handlers["mirror"](types.SimpleNamespace(value=False))
    assert [call[0] for call in calls] == ["log", "off"]  # the line, THEN the handler goes
    assert calls[0][1:] == (
        (ActivityCategory.SYSTEM, "activity log mirror changed"),
        {"status": off_status, "detail": off_detail},
    )
    assert ui.notices[-1] == off_notice
    calls.clear()

    ui.handlers["mirror"](types.SimpleNamespace(value=True))
    assert [call[0] for call in calls] == ["on", "log"]
    assert calls[1][1:] == (
        (ActivityCategory.SYSTEM, "activity log mirror changed"),
        {"status": ActivityStatus.INFO, "detail": on_detail},
    )
    assert ui.notices[-1] == on_notice


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
