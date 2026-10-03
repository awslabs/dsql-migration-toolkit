# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G5: the MySQL CDC start-point check -- its verdict, copy and the bounded read."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.cdc_catalog_drift import (
    REASON_COUNT,
    LiveColumn,
    LiveTable,
    start_point_value,
)
from dsql_migrator.core.models import Watermark
from dsql_migrator.ui.cdc_start_point_gate import (
    E1_HEADER,
    E2_HEADER,
    W1_HEADER,
    PROBLEM_NO_BASELINE,
    PROBLEM_NO_SOURCE,
    PROBLEM_READ_FAILED,
    StartPointVerdict,
    point_coord,
    point_label,
    start_point_activity,
    start_point_notice,
    start_point_tooltip,
)
from dsql_migrator.ui.data_migration._models import MigrationType

_WM = Watermark(
    binlog_file="mysql-bin.000100", binlog_position=4,
    snapshot_timestamp=datetime(2026, 10, 1, 9, 30, tzinfo=timezone.utc),
)


def _changed(n=1):
    return StartPointVerdict(
        kind="changed",
        changed=tuple((f"app.t{i}", REASON_COUNT.format(n=3, m=4)) for i in range(n)),
    )


def test_point_labels_name_the_start_point() -> None:
    assert point_label(_WM, "full_load") == (
        "the Full Load watermark (binlog mysql-bin.000100:4, captured 2026-10-01 09:30 UTC)"
    )
    assert point_label(_WM, "manual") == "your Manual start point (binlog mysql-bin.000100:4)"
    assert point_label(None, "full_load") == "this stack's recorded stream position"
    assert point_coord(_WM) == "binlog mysql-bin.000100:4"


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_a_changed_table_is_an_error_whose_ways_never_loop(mtype) -> None:
    tone, icon, header, body = start_point_notice(
        _changed(), point_label=point_label(_WM, "full_load"), coord=point_coord(_WM),
        migration_type=mtype,
    )
    assert (tone, icon, header) == ("error", "schema", E1_HEADER)
    assert "app.t0 (columns added or dropped: 3 → 4)" in body
    assert "Changed since binlog mysql-bin.000100:4" in body
    assert "Fetch current position" in body and "CHECKSUM" in body
    assert "No connectors were changed." in body
    # Start/Retry/recovery mode are named only as what does NOT help.
    assert "Start CDC again" not in body
    assert "Starting again, Retry CDC and Debezium's \"recovery mode\" all replay" in body
    if mtype is MigrationType.FULL_LOAD_AND_CDC:
        assert "Drop & reload" in body and "Re-run Full Load" in body
    else:
        assert "Re-run Full Load" not in body and "Aurora DSQL" in body
    assert start_point_tooltip(_changed()) == (
        "A captured table changed since the CDC start point — see the notice above."
    )


def test_the_changed_rows_are_capped_at_six() -> None:
    _t, _i, _h, body = start_point_notice(
        _changed(9), point_label="p", coord="binlog f:1",
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    assert "app.t5 (" in body and "app.t6 (" not in body and "(+3 more)" in body


def test_an_unreadable_source_is_an_error_naming_the_way_back() -> None:
    no_source = StartPointVerdict(kind="unreadable", problem=PROBLEM_NO_SOURCE)
    tone, icon, header, body = start_point_notice(
        no_source, point_label="the Full Load watermark", coord="c",
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    assert (tone, icon, header) == ("error", "link_off", E2_HEADER)
    assert "Test the source connection" in body
    failed = StartPointVerdict(
        kind="unreadable", problem=PROBLEM_READ_FAILED, error_class="OperationalError"
    )
    body2 = start_point_notice(
        failed, point_label="p", coord="c", migration_type=MigrationType.CDC_ONLY
    )[3]
    assert "(OperationalError)" in body2 and "reachable" in body2
    assert no_source.blocks and failed.blocks
    assert start_point_tooltip(failed).startswith("The captured tables could not be checked")
    action, status, detail = start_point_activity(failed, point_label="p")
    assert status is ActivityStatus.FAILURE and "OperationalError" in detail


def test_no_baseline_only_warns_and_omits_the_cause_list_without_a_seed() -> None:
    v = StartPointVerdict(kind="unverified", unverified=("app.t",), problem=PROBLEM_NO_BASELINE)
    assert not v.blocks
    tone, icon, header, body = start_point_notice(
        v, point_label=point_label(_WM, "full_load"), coord=point_coord(_WM),
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    assert (tone, icon, header) == ("warning", "help_outline", W1_HEADER)
    assert body.startswith("The Full Load watermark (binlog")
    assert "earlier version of the tool" in body
    assert 'Drop & reload' in body
    cdc_only = start_point_notice(
        v, point_label=point_label(None, "full_load"), coord="",
        migration_type=MigrationType.CDC_ONLY,
    )[3]
    assert cdc_only.startswith("This stack's recorded stream position has no recorded")
    assert "earlier version" not in cdc_only and "Fetch current position" in cdc_only
    action, status, _detail = start_point_activity(v, point_label="p")
    assert (action, status) == ("CDC start point not checked", ActivityStatus.INFO)
    assert start_point_tooltip(v) is None


def test_ok_renders_nothing() -> None:
    ok = StartPointVerdict(kind="ok")
    assert start_point_notice(
        ok, point_label="p", coord="c", migration_type=MigrationType.CDC_ONLY
    ) is None
    assert start_point_activity(ok, point_label="p") is None and not ok.blocks


# --- the bounded read (read_cdc_start_point_once) -------------------------------------------


def _live(cols):
    return LiveTable(
        columns=tuple(LiveColumn(n, t, nl, False, i) for i, (n, t, nl) in enumerate(cols)),
        primary_key=frozenset({"id"}),
    )


_OLD = (("id", "int", False), ("qty", "int", True))


class _Engine:
    def __init__(self, fail=None):
        self.fail, self.disposed, self.executed = fail, False, 0

    def connect(self):
        engine = self

        class _Ctx:
            def __enter__(self):
                if engine.fail is not None:
                    raise engine.fail
                return SimpleNamespace()

            def __exit__(self, *_a):
                return False

        return _Ctx()

    def dispose(self):
        self.disposed = True


def _session(password="pw"):
    return SimpleNamespace(source_config=SimpleNamespace(), source_password=password)


def _patch_read(monkeypatch, engine, live):
    import dsql_migrator.core.cdc_catalog_drift as drift
    import dsql_migrator.ui.data_migration._cdc_status as status

    calls = []
    monkeypatch.setattr(
        status, "make_source_engine_factory",
        lambda pw, read_timeout_seconds=None: (
            calls.append(read_timeout_seconds) or (lambda _cfg: engine)
        ),
    )

    def _read(_conn, names, source_type=None):
        engine.executed += 1
        return {n: live[n] for n in names if n in live}

    monkeypatch.setattr(drift, "read_live_catalog", _read)
    return calls


def test_the_read_compares_and_disposes(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    engine = _Engine()
    calls = _patch_read(
        monkeypatch, engine, {"app.orders": _live(_OLD + (("x", "int", True),))}
    )
    recorded = {"app.orders": start_point_value(_live(_OLD))}
    verdict = read_cdc_start_point_once(_session(), ["app.orders"], recorded)
    assert verdict.kind == "changed" and verdict.blocks
    assert verdict.changed == (("app.orders", "columns added or dropped: 2 → 3"),)
    assert engine.disposed and engine.executed == 1 and calls == [10]
    same = read_cdc_start_point_once(
        _session(), ["app.orders"], {"app.orders": start_point_value(
            _live(_OLD + (("x", "int", True),)))},
    )
    assert same.kind == "ok"


def test_the_read_never_connects_without_what_it_needs(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    engine = _Engine()
    calls = _patch_read(monkeypatch, engine, {})
    v = read_cdc_start_point_once(_session(), ["app.orders"], {})
    assert (v.kind, v.problem) == ("unverified", PROBLEM_NO_BASELINE)
    assert read_cdc_start_point_once(_session(), [], {"app.orders": "1:1:x:y"}).kind == "ok"
    v2 = read_cdc_start_point_once(
        _session(None), ["app.orders"], {"app.orders": start_point_value(_live(_OLD))}
    )
    assert (v2.kind, v2.problem) == ("unreadable", PROBLEM_NO_SOURCE)
    # A value that cannot be compared (another fingerprint version, malformed) is no
    # baseline to defend: no read, and no refusal even without a source connection.
    v3 = read_cdc_start_point_once(
        _session(None), ["app.orders"],
        {"app.orders": "1" + start_point_value(_live(_OLD))[1:]},
    )
    assert (v3.kind, v3.unverified, v3.blocks) == ("unverified", ("app.orders",), False)
    assert calls == [] and engine.executed == 0


def test_a_failed_read_keeps_only_the_error_class(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    engine = _Engine(fail=RuntimeError("Can't connect to db-secret-host.internal:3306"))
    _patch_read(monkeypatch, engine, {})
    v = read_cdc_start_point_once(
        _session(), ["app.orders"], {"app.orders": start_point_value(_live(_OLD))}
    )
    assert (v.kind, v.problem, v.error_class) == ("unreadable", PROBLEM_READ_FAILED, "RuntimeError")
    assert engine.disposed
    assert "db-secret-host" not in repr(v)


def test_nothing_recorded_for_the_captured_tables_warns_without_reading(monkeypatch) -> None:
    # Fingerprints exist, but for none of the captured tables (a Manual point fetched with
    # another selection): nothing can be compared, so no read and no refusal -- even
    # without a source connection.
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    engine = _Engine()
    calls = _patch_read(monkeypatch, engine, {})
    v = read_cdc_start_point_once(_session(None), ["app.orders"], {"app.other": "1:1:a:b"})
    assert (v.kind, v.unverified, v.problem) == ("unverified", ("app.orders",), None)
    assert not v.blocks and calls == [] and engine.executed == 0
    body = start_point_notice(
        v, point_label="your Manual start point (binlog f:1)", coord="binlog f:1",
        migration_type=MigrationType.CDC_ONLY,
    )[3]
    assert body.startswith(
        "These captured tables were not recorded with your Manual start point (binlog f:1): "
        "app.orders."
    )


# --- review follow-ups ------------------------------------------------------------------


def test_the_manual_route_says_a_reload_resets_it_to_automatic_in_full_load_and_cdc() -> None:
    # Reload specific tables resets a Full load + CDC Manual point to Automatic, which
    # offers this start point again: without re-choosing Manual the route loops.
    body = start_point_notice(
        _changed(), point_label=point_label(_WM, "full_load"), coord=point_coord(_WM),
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )[3]
    assert "back to Automatic" in body and "choose Manual again" in body
    cdc_only = start_point_notice(
        _changed(), point_label=point_label(_WM, "full_load"), coord=point_coord(_WM),
        migration_type=MigrationType.CDC_ONLY,
    )[3]
    assert "back to Automatic" not in cdc_only and "choose Manual again" not in cdc_only


def test_the_cdc_only_ways_say_to_choose_manual_first() -> None:
    # Fetch current position is rendered only under Manual: naming the button alone sent a
    # CDC-only run on Automatic (seeded from a Full Load watermark) to a control not shown.
    no_baseline = StartPointVerdict(
        kind="unverified", unverified=("app.t",), problem=PROBLEM_NO_BASELINE
    )
    for verdict in (_changed(), no_baseline):
        body = start_point_notice(
            verdict, point_label=point_label(_WM, "full_load"), coord=point_coord(_WM),
            migration_type=MigrationType.CDC_ONLY,
        )[3]
        assert "choose Manual, then Fetch current position" in body


def test_the_no_baseline_causes_match_where_the_start_point_came_from() -> None:
    v = StartPointVerdict(kind="unverified", unverified=("app.t",), problem=PROBLEM_NO_BASELINE)

    def _body(origin):
        return start_point_notice(
            v, point_label=point_label(_WM, origin), coord=point_coord(_WM),
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
        )[3]

    watermark = _body("full_load")
    assert "typed by hand" not in watermark and "earlier version of the tool" in watermark
    manual = _body("manual")
    assert "typed by hand" in manual and "earlier version of the tool" in manual
    detail = start_point_activity(v, point_label=point_label(_WM, "full_load"))[2]
    assert "typed by hand" not in detail


def test_w1_does_not_call_a_length_change_harmful() -> None:
    v = StartPointVerdict(kind="unverified", unverified=("app.t",), problem=PROBLEM_NO_BASELINE)
    body = start_point_notice(
        v, point_label="p", coord="c", migration_type=MigrationType.FULL_LOAD_AND_CDC
    )[3]
    assert "retyped" not in body
    assert "a new length or DECIMAL precision aside" in body


def test_a_checked_resume_says_it_reads_from_the_recorded_position() -> None:
    # The only checked resume is the history-missing one: it rebuilds the history at the
    # stack's COMMITTED position, so the copy must not say it reads from the watermark.
    label = point_label(_WM, "full_load")
    verdicts = (
        _changed(),
        StartPointVerdict(kind="unreadable", problem=PROBLEM_NO_SOURCE),
        StartPointVerdict(kind="unverified", unverified=("app.t",), problem=PROBLEM_NO_BASELINE),
    )
    for verdict in verdicts:
        kw = dict(point_label=label, coord=point_coord(_WM),
                  migration_type=MigrationType.FULL_LOAD_AND_CDC)
        resumed = start_point_notice(verdict, resumes=True, **kw)
        assert "recorded stream position" in resumed[3]
        assert "read the binary log from the Full Load watermark" not in resumed[3]
        assert "reads the binary log from the Full Load watermark" not in resumed[3]
        assert "Start CDC again: " not in resumed[3]
        plain = start_point_notice(verdict, **kw)
        assert plain == start_point_notice(verdict, resumes=False, **kw)
        assert resumed[:3] == plain[:3]  # same tone, icon and header
    changed = start_point_notice(_changed(), resumes=True, point_label=label,
                                 coord=point_coord(_WM),
                                 migration_type=MigrationType.FULL_LOAD_AND_CDC)[3]
    assert "the tool cannot tell the two apart" in changed
    assert "Changed since binlog mysql-bin.000100:4: app.t0" in changed


def test_a_partly_recorded_start_point_is_unverified_not_ok(monkeypatch) -> None:
    # One captured table has a fingerprint and matches; the other was not recorded (the
    # capture's read did not see it). This must warn, never pass silently.
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    engine = _Engine()
    _patch_read(monkeypatch, engine, {"app.orders": _live(_OLD), "app.items": _live(_OLD)})
    v = read_cdc_start_point_once(
        _session(), ["app.orders", "app.items"], {"app.orders": start_point_value(_live(_OLD))}
    )
    assert engine.executed == 1
    assert (v.kind, v.unverified, v.problem) == ("unverified", ("app.items",), None)
    assert not v.blocks
    tone, _i, header, body = start_point_notice(
        v, point_label="p", coord="c", migration_type=MigrationType.FULL_LOAD_AND_CDC
    )
    assert (tone, header) == ("warning", W1_HEADER)
    assert "app.items" in body and "app.orders" not in body
    action, status, detail = start_point_activity(v, point_label="p")
    assert status is ActivityStatus.INFO and "app.items" in detail


def test_the_unverified_names_are_capped_at_six() -> None:
    v = StartPointVerdict(kind="unverified", unverified=tuple(f"app.t{i}" for i in range(8)))
    body = start_point_notice(
        v, point_label="p", coord="c", migration_type=MigrationType.FULL_LOAD_AND_CDC
    )[3]
    assert "app.t5 (+2 more)" in body and "app.t6" not in body
    detail = start_point_activity(v, point_label="p")[2]
    assert "(+2 more)" in detail and "app.t7" not in detail
