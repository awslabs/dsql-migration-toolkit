# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-13: the column-exclusion overlay at Start CDC (ui/data_migration/_cdc_overlay.py)
and its wiring into the Start CDC job, over a fake catalog with the REAL facts reader and
overlay unit."""

import copy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.column_exclusion import set_digest
from dsql_migrator.core.overlay_ledger import (
    InMemoryOverlayLedger,
    LedgerRead,
    OverlayLedgerColumn,
    OverlayLedgerError,
    OverlayLedgerTable,
    ddl_digest,
)
from dsql_migrator.core.schema_applier import apply_exclusion_overlay
from dsql_migrator.core.target_introspector import target_overlay_facts
from dsql_migrator.ui.data_migration._cdc_overlay import (
    OverlayFactsUnknown,
    StartCdcOverlayBlocked,
    apply_start_cdc_overlay,
    probe_cdc_overlay_changes,
)

from tests.test_overlay_core import Denied, FakeDb, Occ

KEY = "k"
T0 = datetime(2026, 9, 1, tzinfo=timezone.utc)
DDL = "CREATE TABLE a (id INT PRIMARY KEY, c TEXT NOT NULL DEFAULT 'v')"


def _catalog(tables, rows=None):
    return FakeDb({t: {"id": {"notnull": True, "default": None},
                       "c": {"notnull": True, "default": "'v'::text"}} for t in tables},
                  rows=dict(rows or {}))


class _Handle:
    def __init__(self, cancel_after=None):
        self.cancelled, self.beats, self._cancel_after = False, 0, cancel_after

    def heartbeat(self):
        self.beats += 1
        if self._cancel_after is not None and self.beats >= self._cancel_after:
            self.cancelled = True


class _Log:
    def __init__(self):
        self.events = []

    def __call__(self, action, *, status=None, detail=None, exc=None):
        self.events.append((action, getattr(status, "value", status), detail or ""))


def _run(db, ledger, *, overlay, captured=None, in_use=False, handle=None, ddls=None, facts_reader=None,
         applier=None, log=None, key=KEY):
    log = log if log is not None else _Log()
    apply_start_cdc_overlay(
        handle if handle is not None else _Handle(),
        overlay_by_table=overlay,
        captured=captured if captured is not None else sorted(set(overlay) | set(db.tables)),
        excluded_by_table=overlay,
        source_columns_for=lambda _t: ["id", "c"],
        ledger=ledger, ledger_key=key, ledger_in_use=in_use,
        connection_factory=db.connect,
        effective_target_ddls=(lambda: ddls) if ddls is not None else (lambda: {t: DDL for t in db.tables}),
        log=log,
        facts_reader=facts_reader or (lambda names, cols, **kw: target_overlay_facts(
            names, cols, sleep=lambda _s: None, **kw)),
        applier=applier or (lambda t, p, **kw: apply_exclusion_overlay(
            t, p, sleep=lambda _s: None, jitter=lambda: 0.0, **kw)),
    )
    return log


def _alters(db):
    return [t.split(" ALTER COLUMN ")[0].split("TABLE ")[1] + " " + t.split(" ALTER COLUMN ")[1]
            for _c, t in db.log if t.startswith("ALTER TABLE")]


def _record(table, columns=("c",), empty=True, restored=False):
    return OverlayLedgerTable(
        table=table, set_digest=set_digest(list(columns)), set_columns=list(columns), empty_when_applied=empty,
        applied_at=T0, columns=[OverlayLedgerColumn(column=c, original_default="'v'::text", dropped_not_null=True,
                                                    restored_at=T0 if restored else None) for c in columns])


def test_the_units_run_serially_in_order_and_a_40001_is_retried_before_the_next_table():
    tables = ["t0", "t1", "t2"]
    db = _catalog(tables)
    db.fault(r'"t1" ALTER COLUMN "c" DROP NOT NULL', Occ())
    inflight = {"now": 0, "max": 0}

    def applier(t, p, **kw):
        inflight["now"] += 1
        inflight["max"] = max(inflight["max"], inflight["now"])
        try:
            return apply_exclusion_overlay(t, p, sleep=lambda _s: None, jitter=lambda: 0.0, **kw)
        finally:
            inflight["now"] -= 1

    ledger = InMemoryOverlayLedger()
    handle = _Handle()
    _run(db, ledger, overlay={t: frozenset({"c"}) for t in tables}, applier=applier, handle=handle)
    assert inflight["max"] == 1 and handle.beats == 3
    assert _alters(db) == ['"t0" "c" DROP NOT NULL', '"t0" "c" DROP DEFAULT',
                           '"t1" "c" DROP NOT NULL', '"t1" "c" DROP NOT NULL', '"t1" "c" DROP DEFAULT',
                           '"t2" "c" DROP NOT NULL', '"t2" "c" DROP DEFAULT']
    rec = ledger.read(KEY).tables["t0"]
    assert rec.column("c").original_default == "'v'::text" and rec.column("c").recorded_ddl_digest == ddl_digest(DDL)


@pytest.mark.parametrize("failure", ["alter", "readback", "verify"])
def test_a_failing_unit_blocks_start_cdc(failure):
    db = _catalog(["t0", "t1"])
    if failure == "alter":
        db.fault(r'"t0" ALTER COLUMN "c" DROP NOT NULL', Denied())
    elif failure == "readback":
        db.fault(r"pg_attribute", Denied(), skip=2)  # the unit's pre-read passes, its read-back fails
    else:
        db.ignore_alters = True
    log = _Log()
    with pytest.raises(StartCdcOverlayBlocked, match="^Start CDC blocked: the change to t0 failed") as err:
        _run(db, InMemoryOverlayLedger(), overlay={"t0": frozenset({"c"}), "t1": frozenset({"c"})}, log=log)
    assert "No connector was created or changed" in str(err.value)
    assert not any("t1" in a for a in _alters(db))  # stopped at the first failure
    assert [e for e in log.events if e[0] == "overlay failed"]


def test_after_a_full_load_the_overlay_is_a_verified_no_op():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}},
                rows={"t0": True})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t0": _record("t0")})
    merges = []
    real = ledger.merge_tables
    ledger.merge_tables = lambda k, m: merges.append(sorted(m)) or real(k, m)
    reads = []
    real_read = ledger.read
    ledger.read = lambda k: reads.append(k) or real_read(k)
    connects_before = db.connects
    _run(db, ledger, overlay={"t0": frozenset({"c"})})
    assert _alters(db) == [] and len(reads) == 1 and db.connects - connects_before == 1  # one facts read
    assert ledger.read(KEY).tables["t0"] == _record("t0")  # unchanged (no rewrite)


def test_a_restored_default_is_dropped_again_and_owed_again():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": "'v'::text"}}},
                rows={"t0": True})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t0": _record("t0", restored=True)})
    _run(db, ledger, overlay={"t0": frozenset({"c"})})
    assert _alters(db) == ['"t0" "c" DROP DEFAULT']
    assert ledger.read(KEY).tables["t0"].column("c").restored_at is None


class _Untouchable:
    """Records every touch (an exception would be absorbed as "unreadable")."""

    def __init__(self, touched):
        self._touched = touched

    def __getattr__(self, name):
        self._touched.append(name)
        return lambda *a, **k: self._touched.append(name)

    def __call__(self, *a, **k):
        self._touched.append("call")


def test_nothing_captured_and_not_in_use_makes_no_call():
    touched = []
    apply_start_cdc_overlay(
        _Handle(), overlay_by_table={}, captured=["t0"], excluded_by_table={}, source_columns_for=lambda _t: [],
        ledger=_Untouchable(touched), ledger_key=KEY, ledger_in_use=False, connection_factory=_Untouchable(touched),
        effective_target_ddls=lambda: None, log=lambda *a, **k: touched.append("log"),
        facts_reader=_Untouchable(touched), applier=_Untouchable(touched))
    assert touched == []


def test_legacy_entries_are_never_overlaid():
    # A legacy entry is not in the overlay set (overlay_lob_exclusions drops it).
    test_nothing_captured_and_not_in_use_makes_no_call()


@pytest.mark.parametrize("which", ["overlay", "recorded-only"])
def test_an_unknown_catalog_blocks(which):
    db = _catalog(["t0"])
    ledger = InMemoryOverlayLedger()
    overlay = {"t0": frozenset({"c"})}
    if which == "recorded-only":
        ledger.put_tables(KEY, {"t0": _record("t0")})
        overlay = {}
    with pytest.raises(StartCdcOverlayBlocked, match="could not read the Aurora DSQL catalog of t0"):
        _run(db, ledger, overlay=overlay, in_use=True, facts_reader=lambda names, cols, **kw: {n: None for n in names})
    assert _alters(db) == []


def test_an_unreadable_ledger_blocks_with_overlay_columns_or_a_session_that_used_it():
    # As the Full Load: a table CDC now writes values into could otherwise keep a record
    # saying it is NULL in every row.
    db = _catalog(["t0"])
    ledger = InMemoryOverlayLedger()
    ledger.read = lambda k: LedgerRead("unreadable", {}, "AccessDenied")
    with pytest.raises(StartCdcOverlayBlocked, match="could not be read"):
        _run(db, ledger, overlay={"t0": frozenset({"c"})})
    with pytest.raises(StartCdcOverlayBlocked, match="could not be read"):
        _run(db, ledger, overlay={}, in_use=True)
    assert _alters(db) == [] and db.connects == 0


def test_no_ledger_handle_with_overlay_columns_blocks():
    db = _catalog(["t0"])
    with pytest.raises(StartCdcOverlayBlocked, match="no overlay ledger configured"):
        _run(db, None, overlay={"t0": frozenset({"c"})})


def test_an_empty_recorded_only_table_is_reconciled_and_one_with_rows_is_left_alone():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}},
                 "t1": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}},
                rows={"t0": False, "t1": True})
    ledger = InMemoryOverlayLedger()
    # t1's recorded column is gone from the target: nothing re-included, so it is unchanged.
    ledger.put_tables(KEY, {"t0": _record("t0"), "t1": _record("t1", columns=("gone",))})
    _run(db, ledger, overlay={}, in_use=True)
    after = ledger.read(KEY).tables
    assert after["t0"].set_columns == [] and after["t0"].set_digest == set_digest([])  # rewritten, entry kept
    assert after["t0"].column("c") is not None and after["t1"] == _record("t1", columns=("gone",))


def test_a_recorded_only_table_with_rows_is_never_batched():
    # Rule (a): nothing to write, so a failing ledger write cannot block Start CDC over it.
    db = FakeDb({"t1": {"id": {"notnull": True, "default": None}}}, rows={"t1": True})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t1": _record("t1", columns=("gone",))})
    ledger.merge_tables = lambda k, m: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    _run(db, ledger, overlay={}, in_use=True)
    assert _alters(db) == []


def test_a_default_changed_after_it_was_recorded_is_refused_before_any_alter():
    # The unit is bound to the DEFAULT the batch recorded: one set in between is never
    # dropped unrecorded.
    db = _catalog(["t0"])
    ledger = InMemoryOverlayLedger()
    real = ledger.merge_tables

    def _merge(k, m):
        out = real(k, m)
        db.tables["t0"]["c"]["default"] = "'w'::text"
        return out

    ledger.merge_tables = _merge
    with pytest.raises(StartCdcOverlayBlocked, match="the change to t0 failed"):
        _run(db, ledger, overlay={"t0": frozenset({"c"})})
    assert _alters(db) == [] and db.tables["t0"]["c"]["default"] == "'w'::text"


def test_a_column_re_included_while_cdc_streams_downgrades_the_record():
    # CDC writes values into new rows of a column the last set held NULL: the record no
    # longer says every row is NULL there, so a later non-exempt append is refused.
    db = FakeDb({"t1": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}},
                rows={"t1": True})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t1": _record("t1")})
    _run(db, ledger, overlay={}, in_use=True)
    after = ledger.read(KEY).tables["t1"]
    assert after.empty_when_applied is False and after.set_columns == ["c"]


def test_a_recorded_only_empty_table_with_no_entry_left_is_deleted():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}},
                rows={"t0": False})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t0": OverlayLedgerTable(table="t0", set_digest=set_digest(["c"]), set_columns=["c"],
                                                     empty_when_applied=True, applied_at=T0, columns=[])})
    _run(db, ledger, overlay={}, in_use=True)
    assert "t0" not in ledger.read(KEY).tables


def test_a_raising_batch_blocks_start_cdc_even_with_only_reconcile_entries():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}, "c": {"notnull": False, "default": None}}},
                rows={"t0": False})
    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"t0": _record("t0")})
    ledger.merge_tables = lambda k, m: (_ for _ in ()).throw(OverlayLedgerError("AccessDenied"))
    with pytest.raises(StartCdcOverlayBlocked, match="cannot record the change to the exclusion record of t0"):
        _run(db, ledger, overlay={}, in_use=True)
    db2 = _catalog(["t0"])
    with pytest.raises(StartCdcOverlayBlocked, match="cannot record the DEFAULTs to be dropped on t0"):
        _run(db2, ledger, overlay={"t0": frozenset({"c"})})
    assert _alters(db2) == []


def test_recorded_ddl_fields_come_from_the_provider_and_none_gives_no_digest():
    db = _catalog(["t0"])
    ledger = InMemoryOverlayLedger()
    _run(db, ledger, overlay={"t0": frozenset({"c"})}, ddls={})
    entry = ledger.read(KEY).tables["t0"].column("c")
    assert entry.recorded_ddl_digest is None and entry.recorded_ddl_state == "unreadable"


def test_the_scope_is_the_captured_tables_and_computed_columns_are_omitted():
    db = _catalog(["t0", "t1"])
    db.tables["t0"]["c"]["generated"] = "s"
    ledger = InMemoryOverlayLedger()
    _run(db, ledger, overlay={"t0": frozenset({"c"}), "t1": frozenset({"c"})}, captured=["t0"])
    assert _alters(db) == []  # t1 not captured; t0's column is computed (omit-only)


def test_cancel_between_tables_returns_and_heartbeats_per_table():
    db = _catalog(["t0", "t1"])
    handle = _Handle(cancel_after=1)
    _run(db, InMemoryOverlayLedger(), overlay={"t0": frozenset({"c"}), "t1": frozenset({"c"})}, handle=handle)
    assert handle.beats == 1 and not any("t1" in a for a in _alters(db))


def test_the_budget_is_one_facts_connection_one_read_and_one_write():
    tables = [f"t{i}" for i in range(5)]
    db = _catalog(tables)
    ledger = InMemoryOverlayLedger()
    merges, reads = [], []
    real_merge, real_read = ledger.merge_tables, ledger.read
    ledger.merge_tables = lambda k, m: merges.append(len(m)) or real_merge(k, m)
    ledger.read = lambda k: reads.append(k) or real_read(k)
    _run(db, ledger, overlay={t: frozenset({"c"}) for t in tables})
    assert len(reads) == 1 and merges == [5] and db.connects == 1 + len(tables)


def test_the_dialog_probe_lists_the_statements_or_says_it_cannot():
    db = _catalog(["t0", "t1"], rows={"t1": True})
    preview = probe_cdc_overlay_changes({"t0": {"c"}, "t1": {"c"}}, connection_factory=db.connect)
    assert [s.action for s in preview.statements] == ["DROP NOT NULL", "DROP DEFAULT"] * 2
    assert preview.tables_with_rows == ("t1",)
    with pytest.raises(OverlayFactsUnknown):
        probe_cdc_overlay_changes({"t0": {"c"}}, connection_factory=None)
    with pytest.raises(OverlayFactsUnknown):
        probe_cdc_overlay_changes({"t0": {"c"}}, connection_factory=db.connect,
                                  facts_reader=lambda names, cols, **kw: {n: None for n in names})
    assert probe_cdc_overlay_changes({}, connection_factory=None).statements == []


# ---------------------------------------------------------------------------
# Wiring into the Start CDC job
# ---------------------------------------------------------------------------


def _drive_start_cdc(monkeypatch, *, overlay_raises=False, run_raises=False, backstop_raises=False,
                     exclude=False, cancel_in_overlay=False, swap_target=False):
    import dsql_migrator.config as _config
    import dsql_migrator.core.cdc_deployer as _dep
    import dsql_migrator.ui.data_migration._cdc_ui as _cdcui
    from dsql_migrator.core.models import SourceType, TableSelection
    from dsql_migrator.ui.data_migration import DataMigrationState

    from tests.test_ui_data_migration import _inventory

    order = []

    class _Deployer:
        template_s3_bucket = ""

        def _client(self, _svc):
            return SimpleNamespace(get_caller_identity=lambda: {"Account": "111122223333"})

    def _run(*_a, **kw):
        order.append("run_cdc_start")
        if run_raises:
            raise RuntimeError("connector create failed")

    monkeypatch.setattr(_dep, "build_cdc_stack_deployer", lambda *a, **k: order.append("deployer") or _Deployer())
    monkeypatch.setattr(_dep, "run_cdc_start", _run)
    monkeypatch.setattr(_cdcui, "_read_cdc_template_body", lambda: "T")
    monkeypatch.setattr(_cdcui, "_log_cdc_event", lambda *a, **k: None)

    def _fk(*a, **k):
        order.append("fk")
        if backstop_raises:
            return SimpleNamespace(blocking=True)
        return None

    monkeypatch.setattr(_cdcui, "_probe_cdc_blocking_foreign_keys", _fk)
    monkeypatch.setattr(_cdcui, "_cdc_fk_block_body", lambda *a, **k: "fk body")
    monkeypatch.setattr(_cdcui, "_cdc_fk_block_reason", lambda *a, **k: ("fk reason",))

    def _overlay(handle, **plan):
        order.append("overlay")
        if cancel_in_overlay:
            handle.cancelled = True
        if overlay_raises:
            raise StartCdcOverlayBlocked("the change to t failed")

    monkeypatch.setattr(_cdcui, "_apply_start_cdc_overlay", _overlay)
    monkeypatch.setattr(_cdcui, "classify_restored_exclusions", lambda *a, **k: order.append("classify"))
    monkeypatch.setattr(_config, "load_config", lambda *a, **k: SimpleNamespace(
        cdc_seed_mode="lambda", cdc_host_subnet_cidr="", cdc_msk_access=True, cdc_sink_mcu_count=1))
    monkeypatch.setattr(_cdcui, "render_notice", lambda ui, **kw: None)

    class _JM:
        def __init__(self):
            self.work = None

        def submit(self, work):
            order.append("submit")
            self.work = work
            return "job-1"

        def get_status(self, job_id):
            from dsql_migrator.core.job_manager import JobNotFoundError

            raise JobNotFoundError(job_id)

    session = SimpleNamespace(
        target_config=SimpleNamespace(region="us-east-1", cluster_endpoint="ep.dsql.amazonaws.com",
                                      database="postgres", username="admin"),
        aws_profile=None, source_password=None, source_secret_id=None,
        source_config=SimpleNamespace(source_type=SourceType.MYSQL, database="app", port=3306))
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef

    inventory = _inventory()
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["orders"]))
    if exclude:
        inventory = SourceInventory(tables=[TableDef(name="orders", primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="body", mysql_type="text")])])
        state.set_lob_exclusion("orders", "body", True)
    real_mark = state.mark_overlay_ledger_in_use
    state.mark_overlay_ledger_in_use = lambda: order.append("mark") or real_mark()
    state.on_cdc_start = lambda: order.append("on_cdc_start")
    jm = _JM()
    _cdcui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), state, jm, lambda: None,
                             inventory=inventory, session=session)
    if swap_target:
        session.target_config = SimpleNamespace(region="us-east-1", cluster_endpoint="other.dsql.amazonaws.com",
                                                database="postgres", username="admin")
    error = None
    try:
        jm.work(SimpleNamespace(cancelled=False))
    except Exception as exc:  # noqa: BLE001
        error = exc
    return order, error


def test_the_overlay_runs_after_the_backstops_and_before_the_deployer(monkeypatch):
    order, error = _drive_start_cdc(monkeypatch)
    assert error is None
    assert order == ["classify", "on_cdc_start", "submit", "fk", "overlay", "deployer", "run_cdc_start",
                     "on_cdc_start"]


def test_a_start_with_overlay_columns_marks_the_ledger_in_use_before_submit(monkeypatch):
    order, error = _drive_start_cdc(monkeypatch, exclude=True)
    assert error is None and order.index("mark") < order.index("submit")
    order, _error = _drive_start_cdc(monkeypatch)
    assert "mark" not in order


def test_a_cancel_during_the_overlay_creates_no_connector(monkeypatch):
    order, error = _drive_start_cdc(monkeypatch, cancel_in_overlay=True)
    assert error is None and "deployer" not in order and "run_cdc_start" not in order
    assert order[-1] == "on_cdc_start"


@pytest.mark.parametrize("case", ["overlay", "run", "backstop"])
def test_on_cdc_start_runs_in_the_finally_whatever_fails(monkeypatch, case):
    order, error = _drive_start_cdc(monkeypatch, overlay_raises=case == "overlay", run_raises=case == "run",
                                    backstop_raises=case == "backstop")
    assert error is not None and order[-1] == "on_cdc_start" and order[:2] == ["classify", "on_cdc_start"]
    if case == "overlay":
        assert "deployer" not in order and "run_cdc_start" not in order  # nothing created or changed


# ---------------------------------------------------------------------------
# The CDC Start dialog (CDC only): one live read, the statements, or "could not read"
# ---------------------------------------------------------------------------


def _drive_cdc_dialog(monkeypatch, *, migration_type, probe):
    import asyncio

    import nicegui

    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    from tests.test_ui_data_migration import _DialogUi

    ui = _DialogUi()
    monkeypatch.setattr(cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_binlog_resume_gap", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_pg_objects_gate_block", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_current_job", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_render_notice",
                        lambda _ui, *, tone="info", icon=None, header="", body="": ui.notices.append((tone, header)))
    calls = []

    def _probe(*a, **k):
        calls.append(1)
        return probe()

    monkeypatch.setattr(cdc_ui, "_probe_cdc_overlay_changes", _probe)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    inventory = SourceInventory(tables=[TableDef(name="app.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="c", mysql_type="text")])])
    state = DataMigrationState()
    state.set_migration_type(migration_type)
    state.set_selection(__import__("dsql_migrator.core.models", fromlist=["TableSelection"]).TableSelection(
        selected_tables=["app.t"]))
    state.set_lob_exclusion("app.t", "c", True)
    asyncio.run(cdc_ui._open_cdc_start_dialog(ui, state, lambda: None, session=object(), job_manager=object(),
                                              inventory=inventory))
    return ui, calls, MigrationType


def test_the_cdc_only_start_dialog_lists_the_alters_from_one_read(monkeypatch):
    from dsql_migrator.ui.data_migration import MigrationType

    db = _catalog(["app.t"])
    stmts = probe_cdc_overlay_changes({"app.t": {"c"}}, connection_factory=db.connect)
    ui, calls, _mt = _drive_cdc_dialog(monkeypatch, migration_type=MigrationType.CDC_ONLY, probe=lambda: stmts)
    assert calls == [1]
    headers = [h for _t, h in ui.notices]
    assert ("Aurora DSQL changes when CDC starts: 1 x DROP NOT NULL (one-way), "
            "1 x DROP DEFAULT (restored at Cut over)") in headers
    assert ui.buttons["Start CDC"].enabled is True  # never gates Start


def test_the_cdc_only_start_dialog_says_it_could_not_read_and_keeps_start(monkeypatch):
    from dsql_migrator.ui.data_migration import MigrationType

    def _boom():
        raise OverlayFactsUnknown("t")

    ui, calls, _mt = _drive_cdc_dialog(monkeypatch, migration_type=MigrationType.CDC_ONLY, probe=_boom)
    assert ("warning", "Could not read the Aurora DSQL target") in ui.notices
    assert ui.buttons["Start CDC"].enabled is True


def test_a_full_load_and_cdc_start_dialog_makes_no_overlay_read(monkeypatch):
    from dsql_migrator.ui.data_migration import MigrationType

    ui, calls, _mt = _drive_cdc_dialog(monkeypatch, migration_type=MigrationType.FULL_LOAD_AND_CDC,
                                       probe=lambda: [])
    assert calls == []


# ---------------------------------------------------------------------------
# Review fixes (P2-13 adversarial review)
# ---------------------------------------------------------------------------


def test_a_cdc_start_clears_the_real_cut_over_drained_attestation():
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import DataMigrationStore, build_data_migration_screen
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore

    migration_store, validation_store = DataMigrationStore(), ValidationStore()
    build_data_migration_screen(
        SessionStore(), "s1", job_manager=JobManager(), eval_store=EvaluationStore(),
        migration_store=migration_store, conv_store=SchemaConversionStore(), validation_store=validation_store)
    vstate = validation_store.get_or_create("s1")
    vstate.set_cutover_cdc_drained_confirmed(True)
    cdc_ui._note_cdc_start(migration_store.get_or_create("s1"))
    assert vstate.cutover_cdc_drained_confirmed is False


def test_the_overlay_connects_to_the_target_the_start_was_prepared_for(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration import DataMigrationState

    seen = []
    monkeypatch.setattr(cdc_ui, "_dsql_connect_for", lambda target, profile: (lambda: seen.append(target)))
    a = SimpleNamespace(cluster_endpoint="a.dsql", database="postgres", region="us-east-1")
    session = SimpleNamespace(target_config=a, aws_profile=None)
    plan = cdc_ui._start_cdc_overlay_plan(DataMigrationState(), None, [], session, a)
    session.target_config = SimpleNamespace(cluster_endpoint="b.dsql", database="postgres", region="us-east-1")
    plan["connection_factory"]()
    assert seen == [a]


def test_a_target_changed_after_the_click_blocks_before_anything(monkeypatch):
    order, error = _drive_start_cdc(monkeypatch, swap_target=True)
    assert error is not None and "target changed" in str(error)
    assert "fk" not in order and "overlay" not in order and "deployer" not in order
    assert order[-1] == "on_cdc_start"


def test_an_absent_overlay_column_with_no_record_writes_nothing():
    db = FakeDb({"t0": {"id": {"notnull": True, "default": None}}}, rows={"t0": True})
    ledger = InMemoryOverlayLedger()
    ledger.merge_tables = lambda k, m: (_ for _ in ()).throw(OverlayLedgerError("SlowDown"))
    for _ in range(2):
        _run(db, ledger, overlay={"t0": frozenset({"gone"})})
    assert _alters(db) == [] and ledger.read(KEY).tables == {}


def test_a_delete_of_no_record_is_unchanged():
    from dsql_migrator.core.overlay_ledger import plan_ledger_record

    decision = plan_ledger_record(None, table="t", overlay_facts={}, has_rows=False, just_recreated=False,
                                  effective_ddl=None, now=T0)
    assert decision.kind == "unchanged" and decision.record is None
    existing = _record("t", columns=())
    existing = existing.model_copy(update={"columns": []})
    assert plan_ledger_record(existing, table="t", overlay_facts={}, has_rows=False, just_recreated=False,
                              effective_ddl=None, now=T0).kind == "delete"


def test_streaming_onto_rows_not_loaded_with_the_set_warns():
    db = _catalog(["t0"], rows={"t0": True})
    log = _run(db, InMemoryOverlayLedger(), overlay={"t0": frozenset({"c"})})
    warned = [e for e in log.events if e[0] == "overlay append" and e[1] == "warning"]
    assert warned and "streamed anyway" in warned[0][2] and "keep what they hold in c" in warned[0][2]
    empty = _run(_catalog(["t1"]), InMemoryOverlayLedger(), overlay={"t1": frozenset({"c"})})
    assert not [e for e in empty.events if e[0] == "overlay append"]


def test_the_dialog_says_which_tables_keep_their_values(monkeypatch):
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration._cdc_overlay import CdcOverlayPreview

    notices = []
    monkeypatch.setattr(cdc_ui, "_render_notice", lambda _ui, **kw: notices.append(kw))
    cdc_ui._render_cdc_overlay_changes(SimpleNamespace(), CdcOverlayPreview([], ("app.t",)))
    assert notices and notices[0]["tone"] == "info"
    assert "Rows already in app.t keep what they hold" in notices[0]["body"]
