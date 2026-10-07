# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G19: the MySQL CDC GTID start check -- pure verdict, copy and the bounded source read.

Debezium 2.7.4 positions by GTID only when the source's gtid_mode is exactly ON, and then
starts after the GTID set of its offset (ignoring the offset's binlog file:position); an
offset with no set makes it ask MySQL for everything retained. The live Seoul failure
(2026-10-07) was that replay. These tests pin the rules Start CDC applies before anything
changes, the refusal copy, and the read (no AWS, no MySQL: doubles only).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc_coords import (
    gtid_mode_is_on,
    gtid_set_summary,
    normalize_gtid_set,
)
from dsql_migrator.core.models import Watermark
from dsql_migrator.ui.cdc_gtid_gate import (
    BLOCK_TOOLTIP,
    G1_HEADER,
    G2_HEADER,
    G3_HEADER,
    G3_TOOLTIP,
    G4_CHECK_HEADER,
    G4_CHECK_TOOLTIP,
    G4_HEADER,
    MALFORMED_HEADER,
    MALFORMED_TOOLTIP,
    GtidSourceState,
    gtid_notice_point,
    gtid_positioning_line,
    gtid_start_activity,
    gtid_start_check_due,
    gtid_start_notice,
    gtid_start_tooltip,
    mysql_gtid_start_verdict,
)
from dsql_migrator.ui.cdc_start_point_gate import (
    RELOAD_RESETS_MANUAL,
    point_coord,
    point_label,
)

_SET = "1DFF02DB-0000-0000-0000-000000000001:1-812"


# --------------------------------------------------------------------------- #
# (a) Debezium's mode predicate and the GTID set normalization
# --------------------------------------------------------------------------- #
def test_gtid_mode_is_on_is_debeziums_predicate() -> None:
    for value in ("ON", " on ", "On"):
        assert gtid_mode_is_on(value) is True
    for value in ("ON_PERMISSIVE", "OFF_PERMISSIVE", "OFF", "", None):
        assert gtid_mode_is_on(value) is False


def test_normalize_gtid_set_drops_whitespace_and_comments() -> None:
    assert normalize_gtid_set("a:1-5,\n b:1-3 /*x*/") == "a:1-5,b:1-3"
    assert normalize_gtid_set(None) == ""
    assert normalize_gtid_set("  ") == ""


def test_gtid_set_summary_caps_a_long_set() -> None:
    assert gtid_set_summary("a:1-5,\nb:1-3") == "a:1-5,b:1-3"
    long_set = ",".join(f"{i:08d}-0000-0000-0000-000000000000:1-9" for i in range(10))
    capped = gtid_set_summary(long_set)
    assert len(capped) == 160 + len(" …") and capped.endswith(" …")
    assert capped.startswith(long_set[:160])


# --------------------------------------------------------------------------- #
# (b) the verdict truth table (rules R1-R6)
# --------------------------------------------------------------------------- #
def _v(state, *, gtids="", cap=None, seedable=True, resumes=False, stack=None,
       stack_point=None, same=False, malformed=False):
    return mysql_gtid_start_verdict(
        state, seed_gtids=gtids, seed_gtid_mode=cap, seedable=seedable, resumes=resumes,
        stack_gtids=stack, stack_point=stack_point, seed_is_stack_point=same,
        seed_set_malformed=malformed,
    )


_OK_SUBSET = {"within_executed": True, "covers_purged": True}


@pytest.mark.parametrize("mode", ["OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", "off"])
@pytest.mark.parametrize("gtids", ["", _SET])
@pytest.mark.parametrize("cap", ["OFF", "ON", None])
@pytest.mark.parametrize("resumes", [False, True])
def test_a_source_that_is_not_on_proceeds_by_file_position(mode, gtids, cap, resumes) -> None:
    verdict = _v(GtidSourceState(mode=mode), gtids=gtids, cap=cap, resumes=resumes)
    assert verdict.kind == "proceed" and not verdict.blocks
    assert verdict.positioning == ("binlog-resume" if resumes else "binlog")


@pytest.mark.parametrize("cap", ["ON", None])
def test_on_with_a_usable_set_proceeds_by_gtid(cap) -> None:
    verdict = _v(GtidSourceState(mode="ON", **_OK_SUBSET), gtids=_SET, cap=cap)
    assert verdict.kind == "proceed" and verdict.positioning == "gtid"


def test_on_with_an_empty_set_recorded_while_on_needs_an_empty_purged_set() -> None:
    ok = _v(GtidSourceState(mode="ON", **_OK_SUBSET), gtids="", cap="ON")
    assert ok.kind == "proceed" and ok.positioning == "gtid"
    purged = _v(
        GtidSourceState(mode="ON", within_executed=True, covers_purged=False),
        gtids="", cap="ON",
    )
    assert (purged.kind, purged.reason) == ("not-usable", "purged")


def test_on_with_a_gtid_less_seed_and_no_recorded_mode_is_no_gtid() -> None:
    verdict = _v(GtidSourceState(mode="ON"), gtids="", cap=None)
    assert verdict.kind == "no-gtid" and verdict.blocks and not verdict.resumes


@pytest.mark.parametrize("gtids", ["", _SET])
@pytest.mark.parametrize("resumes", [False, True])
def test_on_now_but_recorded_while_not_on_is_mode_changed(gtids, resumes) -> None:
    verdict = _v(
        GtidSourceState(mode="ON", **_OK_SUBSET), gtids=gtids, cap="OFF_PERMISSIVE",
        resumes=resumes,
    )
    assert verdict.kind == "mode-changed" and verdict.blocks
    assert verdict.captured_mode == "OFF_PERMISSIVE" and verdict.resumes is resumes


@pytest.mark.parametrize(
    "state, reason",
    [
        (GtidSourceState(mode="ON", within_executed=False, covers_purged=True), "not-executed"),
        (GtidSourceState(mode="ON", within_executed=True, covers_purged=False), "purged"),
        (GtidSourceState(mode="ON", problem="check-failed", error_class="OperationalError"),
         "check-failed"),
        (GtidSourceState(mode="ON"), "check-failed"),  # the check could not run
        (GtidSourceState(mode="ON", within_executed=True), "check-failed"),
    ],
)
def test_on_with_a_set_the_source_cannot_stream_from_is_not_usable(state, reason) -> None:
    verdict = _v(state, gtids=_SET, cap=None)
    assert (verdict.kind, verdict.reason) == ("not-usable", reason)


def test_on_resumes() -> None:
    on = GtidSourceState(mode="ON")
    # A seedable GTID-less seed and no recorded mode: that stack streamed by file:pos (or
    # an earlier version started it), so continuing by GTID is refused.
    no_set = _v(on, gtids="", cap=None, resumes=True)
    assert no_set.kind == "no-gtid" and no_set.resumes is True
    # Recorded while ON (an empty set), or with a set: continue after the committed set.
    assert _v(on, gtids="", cap="ON", resumes=True).positioning == "gtid-resume"
    assert _v(on, gtids=_SET, cap=None, resumes=True).positioning == "gtid-resume"
    # A resume with no seed at all.
    assert _v(on, gtids="", cap=None, seedable=False, resumes=True).positioning == (
        "gtid-resume"
    )


@pytest.mark.parametrize(
    "state",
    [
        GtidSourceState(problem="no-source-connection"),
        GtidSourceState(problem="read-failed", error_class="OperationalError"),
        GtidSourceState(problem="read-failed"),  # an empty value
        None,
    ],
)
def test_an_unread_mode(state) -> None:
    assert _v(state, gtids="", resumes=True).positioning == "unknown-resume"
    seeded = _v(state, gtids=_SET)
    assert seeded.kind == "proceed" and seeded.positioning == "unknown"
    assert _v(state, gtids=_SET, cap="ON").positioning == "unknown"
    refused = _v(state, gtids="")
    assert refused.kind == "unreadable" and refused.blocks
    assert refused.reason in ("no-source-connection", "read-failed")
    # A set recorded while the mode was not ON does not describe the point if the source
    # is ON now (MySQL could skip changes written without a GTID): unknown -> refused.
    recorded_off = _v(state, gtids=_SET, cap="OFF_PERMISSIVE")
    assert recorded_off.kind == "unreadable" and recorded_off.blocks
    body = _notice(recorded_off)[3]
    assert "was recorded while the source's gtid_mode was OFF_PERMISSIVE" in body
    assert "is still not ON" in body and body.endswith("No connectors were changed.")


# --------------------------------------------------------------------------- #
# (c) when the check is due
# --------------------------------------------------------------------------- #
def _seed(file="mysql-bin.000008", pos=5150730, gtids=None, mode=None):
    return Watermark(
        binlog_file=file, binlog_position=pos, gtid_executed=gtids, gtid_mode=mode,
        snapshot_timestamp=datetime(2026, 10, 7, 0, 20, 39, tzinfo=timezone.utc),
    )


def test_gtid_start_check_due() -> None:
    seedable = _seed()
    facts = SimpleNamespace(resumes=False, live_mode=None)
    assert gtid_start_check_due(seedable, facts) is True
    assert gtid_start_check_due(seedable, None) is True
    assert gtid_start_check_due(None, SimpleNamespace(resumes=True, live_mode=None)) is True
    # Connectors exist (Retry CDC, a live re-Start): they keep what they run.
    assert gtid_start_check_due(seedable, SimpleNamespace(resumes=True, live_mode="recovery")) is False
    # A GTID set alone seeds nothing and is not a resume.
    gtid_only = Watermark(gtid_executed=_SET, snapshot_timestamp=seedable.snapshot_timestamp)
    assert gtid_start_check_due(gtid_only, facts) is False
    assert gtid_start_check_due(None, facts) is False


# --------------------------------------------------------------------------- #
# (d) refusal copy
# --------------------------------------------------------------------------- #
_CDC_ONLY = "cdc_only"
_FL_CDC = "full_load_and_cdc"


def _notice(verdict, *, origin="full_load", migration_type=_FL_CDC, seed=None):
    seed = seed if seed is not None else _seed()
    return gtid_start_notice(
        verdict, point_label=point_label(seed, origin), coord=point_coord(seed),
        origin=origin, migration_type=migration_type,
    )


def _all_refusals():
    on = GtidSourceState(mode="ON")
    return {
        "G1-FL": (_v(on, gtids=""), "full_load"),
        "G1-manual": (_v(on, gtids=""), "manual"),
        "G1-resume": (_v(on, gtids="", resumes=True), "full_load"),
        "G2-seeded": (_v(on, gtids=_SET, cap="OFF"), "full_load"),
        "G2-resume": (_v(on, gtids=_SET, cap="OFF", resumes=True), "full_load"),
        "G3-none": (_v(GtidSourceState(problem="no-source-connection"), gtids=""), "manual"),
        "G3-failed": (
            _v(GtidSourceState(problem="read-failed", error_class="OperationalError"), gtids=""),
            "full_load",
        ),
        "G4-purged-FL": (
            _v(GtidSourceState(mode="ON", within_executed=True, covers_purged=False), gtids=_SET),
            "full_load",
        ),
        "G4-purged-manual": (
            _v(GtidSourceState(mode="ON", within_executed=True, covers_purged=False), gtids=_SET),
            "manual",
        ),
        "G4-unexecuted-manual": (
            _v(GtidSourceState(mode="ON", within_executed=False, covers_purged=True), gtids=_SET),
            "manual",
        ),
        "G4-check-FL": (
            _v(GtidSourceState(mode="ON", problem="check-failed", error_class="DataError"),
               gtids=_SET),
            "full_load",
        ),
        "G4-check-manual": (
            _v(GtidSourceState(mode="ON", problem="check-failed", error_class="DataError"),
               gtids=_SET),
            "manual",
        ),
        "MF-manual": (_v(None, gtids="'x'", malformed=True), "manual"),
    }


@pytest.mark.parametrize("migration_type", [_CDC_ONLY, _FL_CDC])
def test_every_refusal_is_an_error_that_changes_nothing(migration_type) -> None:
    from dsql_migrator.core.cdc_deployer import DEFINITION_SNAPSHOT_MARK, SCHEMA_HISTORY_MARK

    headers = {"G1": G1_HEADER, "G2": G2_HEADER, "G3": G3_HEADER, "G4": G4_HEADER,
               "G4-check": G4_CHECK_HEADER, "MF": MALFORMED_HEADER}
    for name, (verdict, origin) in _all_refusals().items():
        tone, icon, header, body = _notice(verdict, origin=origin, migration_type=migration_type)
        assert tone == "error" and icon, name
        prefix = "G4-check" if name.startswith("G4-check") else name.split("-")[0]
        assert header == headers[prefix], name
        assert body.endswith(" No connectors were changed."), name
        assert SCHEMA_HISTORY_MARK not in body and DEFINITION_SNAPSHOT_MARK not in body
        # Positions, modes and class names only -- never the GTID set itself.
        assert _SET not in body and "'x'" not in body, name
        if name.startswith(("G3", "G4-check")):
            # Could not read / could not check: the way forward is to check again.
            assert "Start CDC again" in body and "Re-run Full Load" not in body, name
            assert "not replicated" not in body, name
            continue
        if migration_type == _CDC_ONLY:
            assert "correct them on Aurora DSQL" in body, name
            assert "Re-run Full Load" not in body, name
            assert RELOAD_RESETS_MANUAL not in body, name
        else:
            assert RELOAD_RESETS_MANUAL in body, name


def test_the_keep_remedy_names_on_permissive() -> None:
    for name in ("G1-FL", "G1-manual", "G1-resume", "G2-seeded", "G2-resume"):
        verdict, origin = _all_refusals()[name]
        body = _notice(verdict, origin=origin)[3]
        assert "set the source's gtid_mode back to ON_PERMISSIVE until cut over" in body, name
    resume = _notice(*_all_refusals()["G1-resume"][:1])[3]
    assert "streams from where this stack stopped by binlog file:position" in resume


def test_refusal_copy_names_the_cause() -> None:
    r = _all_refusals()
    g1_fl = _notice(r["G1-FL"][0])[3]
    assert g1_fl.startswith(
        "This source runs gtid_mode=ON, so Debezium starts streaming after the GTID set "
        "recorded with the start point, not at its binlog file:position. The Full Load "
        "watermark (binlog mysql-bin.000008:5150730, captured 2026-10-07 00:20 UTC) has no "
        "GTID set"
    )
    assert "re-apply changes the Full Load already copied" in g1_fl
    g1_manual = _notice(r["G1-manual"][0], origin="manual")[3]
    assert "Your Manual start point (binlog mysql-bin.000008:5150730) has no GTID set" in g1_manual
    assert "enter the GTID set it had executed at exactly that position" in g1_manual
    g2 = _notice(r["G2-seeded"][0])[3]
    assert "was recorded while the source's gtid_mode was OFF, and it is ON now" in g2
    g3 = _notice(r["G3-failed"][0])[3]
    assert "reading it failed (OperationalError)." in g3
    assert "Check that the source database is reachable from the tool" in g3
    g3_none = _notice(r["G3-none"][0], origin="manual")[3]
    assert "this session has no source connection" in g3_none
    purged = _notice(r["G4-purged-FL"][0])[3]
    assert "already purged binary logs" in purged
    assert "binlog retention hours" in purged
    unexecuted = _notice(r["G4-unexecuted-manual"][0], origin="manual")[3]
    assert "names transactions this source has not executed" in unexecuted
    assert "switched over since), or entered incorrectly —" in unexecuted
    assert "Correct the GTID set you entered, or choose Fetch current position" in unexecuted
    # A Full Load watermark's set is MySQL's own: never "entered incorrectly".
    unexecuted_fl = _notice(
        _v(GtidSourceState(mode="ON", within_executed=False, covers_purged=True), gtids=_SET)
    )[3]
    assert "switched over since) — so streaming from it would fail" in unexecuted_fl
    assert "entered incorrectly" not in unexecuted_fl


def test_tooltips_and_activity() -> None:
    r = _all_refusals()
    assert gtid_start_tooltip(r["G1-FL"][0]) == BLOCK_TOOLTIP
    assert gtid_start_tooltip(r["G2-seeded"][0]) == BLOCK_TOOLTIP
    assert gtid_start_tooltip(r["G4-purged-FL"][0]) == BLOCK_TOOLTIP
    assert gtid_start_tooltip(r["G3-none"][0]) == G3_TOOLTIP
    assert gtid_start_tooltip(r["G4-check-FL"][0]) == G4_CHECK_TOOLTIP
    assert gtid_start_tooltip(r["MF-manual"][0]) == MALFORMED_TOOLTIP
    assert gtid_start_activity(r["G4-check-FL"][0], coord="binlog f:4")[2] == (
        f"{G4_CHECK_HEADER}: binlog f:4"
    )
    proceed = _v(GtidSourceState(mode="OFF"))
    assert gtid_start_tooltip(proceed) is None and gtid_start_tooltip(None) is None
    assert gtid_start_notice(proceed, point_label="p", coord="c", origin="full_load",
                             migration_type=_FL_CDC) is None
    action, status, detail = gtid_start_activity(r["G1-FL"][0], coord="binlog f:4")
    assert action == "start blocked: CDC start point's GTID position"
    assert status.name == "FAILURE" and detail == f"{G1_HEADER}: binlog f:4"
    action, status, detail = gtid_start_activity(proceed, coord="binlog f:4")
    assert action == "CDC start position" and status.name == "INFO"
    assert detail == "Source gtid_mode=OFF: Debezium starts at binlog f:4 by binlog file:position."


# --------------------------------------------------------------------------- #
# (e) positioning lines
# --------------------------------------------------------------------------- #
def test_positioning_lines() -> None:
    coord = "binlog mysql-bin.000008:5150730"
    on = GtidSourceState(mode="ON", **_OK_SUBSET)
    assert gtid_positioning_line(_v(on, gtids=_SET), coord=coord, seed_gtids=_SET) == (
        f"Source gtid_mode=ON: Debezium starts after the start point's GTID set {_SET} "
        f"({coord})."
    )
    assert gtid_positioning_line(
        _v(on, gtids="", cap="ON"), coord=coord, seed_gtids=""
    ) == f"Source gtid_mode=ON: Debezium starts after the start point's GTID set (empty) ({coord})."
    assert gtid_positioning_line(
        _v(on, gtids=_SET, resumes=True), coord=coord, seed_gtids=_SET
    ) == "Source gtid_mode=ON: Debezium continues after the GTID set it last committed."
    assert gtid_positioning_line(
        _v(GtidSourceState(mode="on_permissive")), coord=coord, seed_gtids=""
    ) == f"Source gtid_mode=ON_PERMISSIVE: Debezium starts at {coord} by binlog file:position."
    assert gtid_positioning_line(
        _v(GtidSourceState(mode="OFF"), resumes=True), coord=coord, seed_gtids=""
    ) == (
        "Source gtid_mode=OFF: Debezium continues from the binlog file:position it last "
        "committed."
    )
    unknown = _v(GtidSourceState(problem="read-failed", error_class="TimeoutError"), gtids=_SET)
    assert gtid_positioning_line(unknown, coord=coord, seed_gtids=_SET) == (
        "The source's gtid_mode could not be read (the read failed: TimeoutError): the start "
        f"offset carries {coord} and the GTID set {_SET}; Debezium uses whichever the "
        "source's gtid_mode requires."
    )
    assert gtid_positioning_line(
        _v(GtidSourceState(problem="no-source-connection"), resumes=True),
        coord=coord, seed_gtids="",
    ) == (
        "The source's gtid_mode could not be read (this session has no source connection): "
        "Debezium continues from the position it last committed."
    )
    # A refusal has no positioning line.
    assert gtid_positioning_line(_v(GtidSourceState(mode="ON")), coord=coord, seed_gtids="") is None


# --------------------------------------------------------------------------- #
# The bounded source read (doubles only)
# --------------------------------------------------------------------------- #
class _Result:
    def __init__(self, scalar=None, row=None):
        self._scalar, self._row = scalar, row

    def scalar(self):
        return self._scalar

    def mappings(self):
        return self

    def first(self):
        return self._row


def _patch_engine(monkeypatch, *, mode="ON", mode_error=None, subset=(1, 1), subset_error=None,
                  connect_error=None):
    import dsql_migrator.ui.data_migration._cdc_status as _status

    log: list = []

    class _Conn:
        def execute(self, statement, params=None):
            sql = str(statement)
            log.append((sql, dict(params or {})))
            if "gtid_mode" in sql:
                if mode_error is not None:
                    raise mode_error
                return _Result(scalar=mode)
            if "GTID_SUBSET" in sql:
                if subset_error is not None:
                    raise subset_error
                return _Result(row={"within_executed": subset[0], "covers_purged": subset[1]})
            raise AssertionError(f"unexpected statement {sql}")

    class _Engine:
        def connect(self):
            if connect_error is not None:
                raise connect_error

            class _Ctx:
                def __enter__(self):
                    return _Conn()

                def __exit__(self, *_a):
                    return False

            return _Ctx()

        def dispose(self):
            log.append(("dispose", {}))

    monkeypatch.setattr(
        _status, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine())
    )
    return log


_SESSION = SimpleNamespace(source_config=SimpleNamespace(), source_password="pw")


def test_the_read_needs_a_source_connection_and_connects_to_nothing_without_one(
    monkeypatch,
) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_source_gtid_state_once

    log = _patch_engine(monkeypatch)
    for session in (
        SimpleNamespace(source_config=None, source_password="pw"),
        SimpleNamespace(source_config=SimpleNamespace(), source_password=None),
        object(),
    ):
        state = read_source_gtid_state_once(session, _SET)
        assert state.problem == "no-source-connection" and state.mode is None
    assert log == []


def test_the_read_checks_a_set_only_on_on_and_binds_it(monkeypatch) -> None:
    from dsql_migrator.core.introspector import is_write_or_ddl
    from dsql_migrator.ui.data_migration._cdc_status import read_source_gtid_state_once

    log = _patch_engine(monkeypatch, mode="ON", subset=(1, 0))
    state = read_source_gtid_state_once(_SESSION, _SET)
    assert (state.mode, state.within_executed, state.covers_purged) == ("ON", True, False)
    assert state.problem is None
    sqls = [sql for sql, _p in log if sql != "dispose"]
    assert sqls[0] == "SELECT @@GLOBAL.gtid_mode"
    assert "GTID_SUBSET(:g, @@GLOBAL.gtid_executed)" in sqls[1]
    assert "GTID_SUBSET(@@GLOBAL.gtid_purged, :g)" in sqls[1]
    assert log[1][1] == {"g": _SET}  # bound, never interpolated
    assert _SET not in sqls[1]
    assert all(not is_write_or_ddl(sql) for sql in sqls)
    assert log[-1][0] == "dispose"
    # No set to check (a resume, or a point recorded while not ON): the mode only.
    log = _patch_engine(monkeypatch, mode="ON")
    assert read_source_gtid_state_once(_SESSION, None).mode == "ON"
    assert [sql for sql, _p in log if "GTID_SUBSET" in sql] == []
    # Not ON: the set is never checked.
    log = _patch_engine(monkeypatch, mode="OFF_PERMISSIVE")
    state = read_source_gtid_state_once(_SESSION, _SET)
    assert state.mode == "OFF_PERMISSIVE" and state.within_executed is None
    assert [sql for sql, _p in log if "GTID_SUBSET" in sql] == []


def test_the_read_reports_failures_by_class_name_only(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_status import read_source_gtid_state_once

    class OperationalError(Exception):
        pass

    _patch_engine(monkeypatch, mode_error=OperationalError("Lost connection to db:3306"))
    state = read_source_gtid_state_once(_SESSION, _SET)
    assert (state.problem, state.error_class) == ("read-failed", "OperationalError")
    _patch_engine(monkeypatch, mode="")
    assert read_source_gtid_state_once(_SESSION, _SET).problem == "read-failed"
    _patch_engine(monkeypatch, connect_error=OperationalError("refused"))
    assert read_source_gtid_state_once(_SESSION, _SET).problem == "read-failed"
    log = _patch_engine(monkeypatch, mode="ON", subset_error=OperationalError("Malformed"))
    state = read_source_gtid_state_once(_SESSION, "not a set")
    assert (state.mode, state.problem, state.error_class) == (
        "ON", "check-failed", "OperationalError"
    )
    assert log[-1][0] == "dispose"


# --------------------------------------------------------------------------- #
# _probe_cdc_gtid_start: what set it asks the source to check
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "seed, facts, expected",
    [
        (_seed(gtids=_SET), None, _SET),                         # a seeded set
        (_seed(gtids=f"{_SET},\n x:1"), None, f"{_SET},x:1"),    # normalized
        (_seed(gtids=None, mode="ON"), None, ""),                # empty, recorded while ON
        (_seed(gtids=None), None, None),                         # nothing to check
        (_seed(gtids=_SET, mode="OFF"), None, None),             # refused whatever the set
        (_seed(gtids=_SET), SimpleNamespace(resumes=True, live_mode=None), None),
    ],
)
def test_the_probe_checks_only_the_set_debezium_will_position_by(
    monkeypatch, seed, facts, expected
) -> None:
    import dsql_migrator.ui.data_migration._cdc_status as _status
    from dsql_migrator.ui.data_migration import _cdc_ui

    asked: list = []
    monkeypatch.setattr(
        _status, "read_source_gtid_state_once",
        lambda _s, gtid_set: asked.append(gtid_set) or GtidSourceState(mode="OFF"),
    )
    verdict = _cdc_ui._probe_cdc_gtid_start(None, _SESSION, seed=seed, facts=facts)
    assert verdict is not None and verdict.kind == "proceed"
    assert asked == [expected]


def test_the_probe_is_never_due_for_postgres_or_live_connectors(monkeypatch) -> None:
    import dsql_migrator.ui.data_migration._cdc_status as _status
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui

    def _never(*_a, **_k):
        raise AssertionError("must not read")

    monkeypatch.setattr(_status, "read_source_gtid_state_once", _never)
    pg = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES),
                         source_password="pw")
    assert _cdc_ui._probe_cdc_gtid_start(None, pg, seed=_seed(), facts=None) is None
    live = SimpleNamespace(resumes=False, live_mode="schema_only")
    assert _cdc_ui._probe_cdc_gtid_start(None, _SESSION, seed=_seed(), facts=live) is None


# --------------------------------------------------------------------------- #
# Review fixes: a resume is judged by the stack's own record of its last start offset
# --------------------------------------------------------------------------- #
_STACK_POINT = "binlog mysql-bin-changelog.000010:4"


@pytest.mark.parametrize("seedable", [False, True])
def test_on_a_resume_of_a_stack_offered_no_gtid_set_is_refused_with_or_without_a_seed(
    seedable,
) -> None:
    # The stack's Watermark* say its last Start offered a binlog position and NO GTID set:
    # it streamed by file:position (its committed offset has no set), so continuing by GTID
    # on an ON source would skip or re-read changes -- whether or not the session still
    # holds a start point (an attached stack, a GTID set alone, a reset session).
    on = GtidSourceState(mode="ON")
    verdict = _v(on, gtids="", seedable=seedable, resumes=True, stack="",
                 stack_point=_STACK_POINT)
    assert verdict.kind == "no-gtid" and verdict.blocks and verdict.resumes
    assert verdict.stack_point == _STACK_POINT  # named by the stack's own record
    label, coord = gtid_notice_point(verdict, seed=None, origin="full_load")
    assert coord == _STACK_POINT
    assert label == f"the start offset this stack was last started from ({_STACK_POINT})"
    body = gtid_start_notice(verdict, point_label=label, coord=coord, origin="full_load",
                             migration_type=_FL_CDC)[3]
    assert body.startswith(
        f"The start offset this stack was last started from ({_STACK_POINT}) has no GTID set, "
        "and the source's gtid_mode is ON now."
    )
    assert "set the source's gtid_mode back to ON_PERMISSIVE until cut over" in body
    action, _status, detail = gtid_start_activity(verdict, coord=coord)
    assert detail == f"{G1_HEADER}: {_STACK_POINT}"


def test_on_a_resume_is_judged_by_the_stack_record_not_by_an_edited_session_point() -> None:
    on = GtidSourceState(mode="ON")
    # The stack was offered a GTID set; the session's point was edited to a GTID-less
    # position since: Debezium continues after the set it committed -- not refused.
    flip = _v(on, gtids="", cap=None, resumes=True, stack=_SET, stack_point=_STACK_POINT)
    assert flip.kind == "proceed" and flip.positioning == "gtid-resume"
    # The session's point IS the stack's: named by the session's point, as before.
    same = _v(on, gtids="", cap=None, resumes=True, stack="", stack_point=_STACK_POINT,
              same=True)
    assert same.kind == "no-gtid" and same.stack_point is None
    # An empty set recorded while ON at that very point: a GTID start, continue.
    on_empty = _v(on, gtids="", cap="ON", resumes=True, stack="", stack_point=_STACK_POINT,
                  same=True)
    assert on_empty.kind == "proceed" and on_empty.positioning == "gtid-resume"
    # ...but not when the stack was started from ANOTHER point (that one had no set).
    other = _v(on, gtids="", cap="ON", resumes=True, stack="", stack_point=_STACK_POINT)
    assert other.kind == "no-gtid" and other.stack_point == _STACK_POINT
    # A point recorded while not ON speaks for the stack only when it IS the stack's point.
    assert _v(on, gtids=_SET, cap="OFF", resumes=True, stack=_SET, stack_point=_STACK_POINT,
              same=True).kind == "mode-changed"
    assert _v(on, gtids=_SET, cap="OFF", resumes=True, stack=_SET,
              stack_point=_STACK_POINT).positioning == "gtid-resume"
    # No record (Watermark* blank): the session's point decides, exactly as before.
    assert _v(on, gtids=_SET, cap="OFF", resumes=True).kind == "mode-changed"
    assert _v(on, gtids="", cap=None, resumes=True).kind == "no-gtid"
    assert _v(on, gtids="", seedable=False, resumes=True).positioning == "gtid-resume"


@pytest.mark.parametrize("mode", ["OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE"])
@pytest.mark.parametrize("stack", [None, "", _SET])
def test_the_stack_record_changes_nothing_on_a_source_that_is_not_on(mode, stack) -> None:
    for seedable in (False, True):
        verdict = _v(GtidSourceState(mode=mode), seedable=seedable, resumes=True,
                     stack=stack, stack_point=_STACK_POINT)
        assert verdict.kind == "proceed" and verdict.positioning == "binlog-resume"


def test_the_stack_record_changes_nothing_when_the_mode_is_unread() -> None:
    for state in (GtidSourceState(problem="no-source-connection"), None):
        verdict = _v(state, seedable=False, resumes=True, stack="", stack_point=_STACK_POINT)
        assert verdict.kind == "proceed" and verdict.positioning == "unknown-resume"


# --------------------------------------------------------------------------- #
# Review fixes: a Manual GTID set that is not a GTID set is refused on every mode
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "state",
    [GtidSourceState(mode=m) for m in ("ON", "OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE")]
    + [GtidSourceState(problem="no-source-connection"), None],
)
def test_a_malformed_manual_set_is_refused_on_every_mode(state) -> None:
    verdict = _v(state, gtids="'x'", malformed=True)
    assert verdict.kind == "malformed" and verdict.blocks
    tone, icon, header, body = _notice(verdict, origin="manual")
    assert (tone, header) == ("error", MALFORMED_HEADER) and icon
    assert "is not a GTID set Debezium can read" in body
    assert "<server UUID>:<interval>, comma-separated" in body
    assert "stop at the connector's next restart" in body
    assert "clear it if the source's gtid_mode is not ON" in body
    assert body.endswith("No connectors were changed.")
    # Only a seeded start writes the set; a resume never seeds it.
    assert _v(state, gtids="'x'", malformed=True, resumes=True).kind != "malformed"


def test_check_failed_says_check_again_and_never_recommends_a_new_start_point() -> None:
    # A dropped connection or a read timeout on the GTID_SUBSET read: not "unusable".
    verdict = _v(
        GtidSourceState(mode="ON", problem="check-failed", error_class="OperationalError"),
        gtids=_SET,
    )
    tone, icon, header, body = _notice(verdict)
    assert header == G4_CHECK_HEADER and icon == "link_off"
    assert "Checking that GTID set against the source failed (OperationalError), so Start " \
           "CDC cannot confirm that the source can stream from it." in body
    assert body.endswith(
        "Check that the source database is reachable from the tool, then Start CDC again. "
        "No connectors were changed."
    )
    for absent in ("syntax", "Re-run Full Load", "Fetch current position", "by hand"):
        assert absent not in body
    manual = _notice(verdict, origin="manual")[3]
    assert "If you entered the GTID set by hand, check that it is a valid MySQL GTID set." in manual
    assert gtid_start_tooltip(verdict) == G4_CHECK_TOOLTIP


# --------------------------------------------------------------------------- #
# Review fixes: the probe -- the stack record, a seedless resume, a malformed set
# --------------------------------------------------------------------------- #
def _probe(monkeypatch, *, mode="ON", seed=None, facts=None, origin=None, state=None,
           stack_params=None):
    import dsql_migrator.ui.data_migration._cdc_status as _status
    from dsql_migrator.ui.data_migration import _cdc_ui

    asked: list = []

    def _read(_s, gtid_set):
        asked.append(gtid_set)
        return GtidSourceState(mode=mode) if mode else GtidSourceState(
            problem="no-source-connection"
        )

    monkeypatch.setattr(_status, "read_source_gtid_state_once", _read)
    verdict = _cdc_ui._probe_cdc_gtid_start(
        state, _SESSION, seed=seed, facts=facts, origin=origin, stack_params=stack_params,
    )
    return verdict, asked


_RESUME = SimpleNamespace(resumes=True, live_mode=None)
_STOPPED = {
    "MskBootstrapServers": "", "DeploySink": "true",
    "WatermarkBinlogFile": "mysql-bin-changelog.000010", "WatermarkBinlogPos": "4",
    "WatermarkGtids": "",
}


def test_a_seedless_resume_with_no_stack_record_continues_by_gtid(monkeypatch) -> None:
    # Nothing in the session, nothing recorded on the stack: the committed offset decides.
    verdict, asked = _probe(monkeypatch, facts=_RESUME)
    assert (verdict.kind, verdict.positioning, verdict.resumes) == (
        "proceed", "gtid-resume", True
    )
    assert asked == [None]  # the mode only, no set to check
    verdict, _ = _probe(monkeypatch, facts=_RESUME,
                        stack_params={"MskBootstrapServers": "", "DeploySink": "true"})
    assert verdict.positioning == "gtid-resume"


def test_a_seedless_resume_of_a_stack_offered_no_gtid_set_is_refused(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    # The worker: from its fresh describe.
    verdict, asked = _probe(monkeypatch, facts=_RESUME, stack_params=dict(_STOPPED))
    assert verdict.kind == "no-gtid" and verdict.resumes
    assert verdict.stack_point == "binlog mysql-bin-changelog.000010:4"
    assert asked == [None]
    # The dialog: from the phase probe's cache of the same describe.
    state = DataMigrationState()
    state.set_cdc_deployed_watermark(("mysql-bin-changelog.000010", "4"), gtids="")
    verdict, _ = _probe(monkeypatch, facts=_RESUME, state=state)
    assert verdict.kind == "no-gtid"
    # A record with a set continues; so does an unknown record (an older cache).
    state.set_cdc_deployed_watermark(("mysql-bin-changelog.000010", "4"), gtids=_SET)
    assert _probe(monkeypatch, facts=_RESUME, state=state)[0].positioning == "gtid-resume"
    state.set_cdc_deployed_watermark(("mysql-bin-changelog.000010", "4"))
    assert _probe(monkeypatch, facts=_RESUME, state=state)[0].positioning == "gtid-resume"
    # Not ON: by file:position, whatever the record says.
    assert _probe(monkeypatch, mode="OFF_PERMISSIVE", facts=_RESUME,
                  stack_params=dict(_STOPPED))[0].positioning == "binlog-resume"


def test_the_probe_judges_the_session_point_against_the_stack_record(monkeypatch) -> None:
    # The session point IS the stack's point (same file:position, the deployed position is
    # a CloudFormation string): named by the session's point.
    seed = _seed(file="mysql-bin-changelog.000010", pos=4)
    verdict, _ = _probe(monkeypatch, seed=seed, facts=_RESUME, stack_params=dict(_STOPPED))
    assert verdict.kind == "no-gtid" and verdict.stack_point is None
    # An earlier, GTID-less session point on a stack offered a set: continues.
    params = dict(_STOPPED, WatermarkBinlogPos="900", WatermarkGtids=f"{_SET},\n x:1")
    verdict, _ = _probe(monkeypatch, seed=seed, facts=_RESUME, stack_params=params)
    assert verdict.kind == "proceed" and verdict.positioning == "gtid-resume"


@pytest.mark.parametrize("mode", ["ON", "OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", None])
@pytest.mark.parametrize(
    "typed", [f"'{_SET}'", f"{_SET};", f"SET @@GLOBAL.GTID_PURGED='{_SET}'"]
)
def test_the_probe_refuses_a_malformed_manual_set_without_reading(monkeypatch, mode, typed):
    seed = _seed(gtids=typed)
    verdict, asked = _probe(monkeypatch, mode=mode, seed=seed, origin="manual")
    assert verdict.kind == "malformed" and verdict.blocks
    assert asked == []  # refused before any read: no source round trip
    # A valid set proceeds on that mode as before; a Full Load watermark's set (MySQL's own
    # output) and a resume are never judged by the Manual syntax check.
    ok, _ = _probe(monkeypatch, mode=mode, seed=_seed(gtids=_SET), origin="manual")
    assert ok.kind != "malformed"
    for origin, facts in (("full_load", None), ("manual", _RESUME)):
        other, _ = _probe(monkeypatch, mode=mode, seed=seed, origin=origin, facts=facts)
        assert other.kind != "malformed"
