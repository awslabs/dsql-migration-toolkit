# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 (Full Load copy): what the Full Load step says it does to the source and the indexes.

1. The confirm dialog's source-write sentence follows the ACTION, not just the engine and the
   migration type. On a PostgreSQL Full load + CDC, Start and Re-run all tables take a new
   snapshot. That run creates the CDC publication (or reuses one, setting its
   publish_via_partition_root to true) and REPLACES the replication slot. Retry unfinished
   tables, a per-table Reload and Reload specific tables keep the original watermark. They
   only read the source, except that a table selected after the Full Load (G17) joins CDC's
   publication if it is not a member yet (and the guard sets the publication's
   publish_via_partition_root to true if it is false): an earlier guarded attempt that
   joined it can leave it pending (the wait after the join timed out, or the copy failed or
   quarantined), and the guard then writes nothing.
2. The index notice takes its "NOT created / a constraint is missing" wording only from the
   indexes known to be missing. In a mix it names both groups. It is judged from EVERY index
   failure, one entry each, not from each table's latest message, and its header calls them
   "Unique index" only when every one is unique (else it counts the unique ones apart).
   Two or more unconfirmed UNIQUE indexes are named in the plural, in a mix too.
3. A CREATE INDEX whose connection dropped while the statement ran may have committed. It
   is "not confirmed", like an XX000. A failure while connecting, or on a cursor of a
   connection the client knows is closed, ran no CREATE.

Doubles only: no AWS, no source, no Aurora DSQL, no network.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import psycopg
import pytest

from dsql_migrator.core.batched_import import (
    UNCONFIRMED_INDEX_DROP_NOTE,
    UNCONFIRMED_INDEX_NOTE,
    BatchedImporter,
    BatchedImportOptions,
    _index_name_of,
    is_unconfirmed_index_failure,
)
from dsql_migrator.core.models import SourceType, StepStatus
from dsql_migrator.ui.data_migration import MigrationType
from dsql_migrator.ui.data_migration._full_load_ui import full_load_confirm_summary

from tests.test_batched_import import _IndexCatalog, _UNIQUE_DDL, _index_importer
from tests.test_g12_table_set_after_load import _c, _job, _Jobs, _loaded
from tests.test_g16_full_load_and_cdc_narrowing import _fc
from tests.test_g17_full_load_and_cdc_additions import _G17PgSession, _G17Session, _with
from tests.test_ui_data_migration import _ConfirmDialogUi, _index_failures_panel

PG, MY = SourceType.POSTGRES, SourceType.MYSQL
FLC, FLO = MigrationType.FULL_LOAD_AND_CDC, MigrationType.FULL_LOAD_ONLY
_SLOT = "dsqlmig_s"
_READ_ONLY = "; the source is accessed read only."
_JOINS_C = (
    "; the source is accessed read only, except that app.c joins CDC's publication on the "
    "source if it is not a member yet, and the publication's publish_via_partition_root is "
    "set to true if it is false."
)
_PROVISIONS = (
    "the source's tables are only read, but the tool also creates the CDC publication on "
    "the source (or reuses one, adding any missing table and setting its "
    "publish_via_partition_root to true if it is false), replaces the replication slot (a "
    "slot left by an earlier run is dropped and a new one is created at this snapshot) and "
    "sets REPLICA IDENTITY FULL on a table whose CDC key was changed."
)
# The words the summary names each kind of catalog write with (G25): a source write the
# action can make and the dialog does not name is the defect these tests guard against.
_WRITE_WORDS = {
    "CREATE PUBLICATION": "creates the CDC publication on the source",
    "REPLICA IDENTITY FULL": "sets REPLICA IDENTITY FULL on a table whose CDC key was changed",
    " ADD TABLE ": "adding any missing table",
    "SET (PUBLISH_VIA_PARTITION_ROOT = TRUE)": "publish_via_partition_root to true",
    "PG_DROP_REPLICATION_SLOT": "a slot left by an earlier run is dropped",
    "PG_CREATE_LOGICAL_REPLICATION_SLOT": "a new one is created at this snapshot",
}
_JOIN_WORDS = {
    " ADD TABLE ": "joins CDC's publication on the source if it is not a member yet",
    "SET (PUBLISH_VIA_PARTITION_ROOT = TRUE)": (
        "the publication's publish_via_partition_root is set to true if it is false"
    ),
}


def _catalog_writes(statements) -> list[str]:
    """The catalog writes among ``statements`` (normalized upper case): DDL and the slot
    functions. The transaction ID the G17 wait takes is no catalog write."""
    out = []
    for sql in statements:
        up = " ".join(str(sql).upper().split())
        if up.startswith(("ALTER ", "CREATE ", "DROP ")) or (
            "PG_CREATE_LOGICAL_REPLICATION_SLOT" in up or "PG_DROP_REPLICATION_SLOT" in up
        ):
            out.append(up)
    return out


def _assert_every_write_is_named(writes, summary, words) -> None:
    """Each write is one known kind, and the summary names that kind."""
    for write in writes:
        (kind,) = [k for k in words if k in write]  # an unknown kind of write fails here
        assert words[kind] in summary, (write, summary)


# ---------------------------------------------------------------------------
# 1. The confirm summary follows the action kind (pure)
# ---------------------------------------------------------------------------


def test_a_new_snapshot_on_postgresql_full_load_and_cdc_says_the_slot_is_replaced() -> None:
    # FAILS WITHOUT THE FIX: "creates or reuses the CDC publication and replication slot" --
    # provision_pg_replication never reuses a slot: one left by an earlier run (a stopped
    # stream's) is dropped and a new one created at this snapshot.
    text = full_load_confirm_summary(["app.a", "app.b"], PG, FLC, keeps_watermark=False)
    assert text == (
        "2 tables selected for Full Load. The target tables will receive the snapshot rows; "
        + _PROVISIONS
    )
    assert "reuses the CDC publication and replication slot" not in text


def test_a_retry_or_reload_only_reads_the_source() -> None:
    # FAILS WITHOUT THE FIX: every PostgreSQL Full load + CDC dialog claimed slot, publication
    # and REPLICA IDENTITY writes, though the retry engine keeps the original watermark and
    # never provisions.
    text = full_load_confirm_summary(["app.a"], PG, FLC, keeps_watermark=True)
    assert text == (
        "1 table selected for Full Load. The target tables will receive the snapshot rows"
        + _READ_ONLY
    )


def test_a_retry_names_the_additions_that_join_the_publication() -> None:
    # A possibility, never a promised write: an addition an earlier guarded attempt already
    # joined (its copy then failed or quarantined) is still pending, and the guard finds it
    # a member and writes nothing.
    one = full_load_confirm_summary(
        ["app.a", "app.c"], PG, FLC, keeps_watermark=True, publication_additions={"app.c"}
    )
    assert one.endswith(_JOINS_C)
    two = full_load_confirm_summary(
        ["app.c", "app.d"], PG, FLC, keeps_watermark=True,
        publication_additions=frozenset({"app.c", "app.d"}),
    )
    assert two.endswith(
        "; the source is accessed read only, except that app.c, app.d join CDC's publication "
        "on the source if they are not members yet, and the publication's "
        "publish_via_partition_root is set to true if it is false."
    )
    assert "added to" not in one + two
    # Only the TICKED additions are named; with none ticked the source is only read.
    assert full_load_confirm_summary(
        ["app.a"], PG, FLC, keeps_watermark=True, publication_additions={"app.c"}
    ).endswith(_READ_ONLY)
    # A long list stays bounded.
    many = [f"app.t{i:02d}" for i in range(20)]
    bounded = full_load_confirm_summary(many, PG, FLC, keeps_watermark=True,
                                        publication_additions=set(many))
    assert "(+14 more) join CDC's publication on the source if they are not members" in bounded


def test_a_new_snapshot_while_cdc_streams_provisions_nothing() -> None:
    # The handoff stack is None while CDC streams (_pg_cdc_handoff_stack): no slot, no
    # publication -- only the WAL position and the row estimates are read.
    assert full_load_confirm_summary(
        ["app.a"], PG, FLC, keeps_watermark=False, cdc_live=True
    ).endswith(_READ_ONLY)


@pytest.mark.parametrize("engine,mtype,keeps", [
    (MY, FLC, True), (PG, FLO, False), (PG, FLO, True), (MY, FLO, False), (MY, FLO, True),
])
def test_every_other_engine_and_type_only_reads_the_source(engine, mtype, keeps) -> None:
    assert full_load_confirm_summary(
        ["app.c"], engine, mtype, keeps_watermark=keeps, publication_additions={"app.c"}
    ).endswith(_READ_ONLY)


def test_a_mysql_full_load_and_cdc_new_snapshot_names_its_flush() -> None:
    # G147: the run that records CDC's start point runs FLUSH BINARY LOGS unless the source
    # runs gtid_mode=ON; the dialog names it, as it names PostgreSQL's catalog writes.
    summary = full_load_confirm_summary(["app.a"], MY, FLC, keeps_watermark=False)
    assert "unless the source runs gtid_mode=ON the tool runs FLUSH BINARY LOGS" in summary
    assert not summary.endswith(_READ_ONLY)


def test_the_action_kind_is_required() -> None:
    # A caller that forgets the kind fails loudly rather than getting a sentence that may
    # name source writes the action never makes.
    with pytest.raises(TypeError):
        full_load_confirm_summary(["app.a"], PG, FLC)  # type: ignore[call-arg]
    assert full_load_confirm_summary([], PG, FLC, keeps_watermark=True).startswith(
        "No tables ticked yet"
    )


def _leaf_named_source(**kw):
    """The PostgreSQL fake whose publication, if there is one, is at
    publish_via_partition_root = false -- as Debezium's filtered autocreate leaves it."""
    from tests.test_cdc_pg_slot import _FakeConn, _Result

    class _Conn(_FakeConn):
        def execute(self, statement, params=None):
            if " ".join(str(statement).upper().split()).startswith("SELECT PUBVIAROOT"):
                self.statements.append((str(statement), params or {}))
                return _Result([(False,)])
            return super().execute(statement, params)

    return _Conn(**kw)


@pytest.mark.parametrize("reused", [True, False], ids=["reused-publication", "new-publication"])
def test_a_new_snapshot_names_every_source_write_the_provisioning_makes(reused) -> None:
    # The REAL provisioning a Start / Re-run all tables runs. Reused: the publication is at
    # publish_via_partition_root = false and lacks the re-keyed app.b, and the stack's
    # earlier slot is still there. FAILS WITHOUT THE FIX: its ALTER PUBLICATION ... SET
    # (publish_via_partition_root = true), made before the slot, went unnamed in the dialog
    # the operator approves.
    from dsql_migrator.core.cdc_pg_slot import provision_pg_replication

    conn = (
        _leaf_named_source(pubs={"dsqlmig_pub_s": ["app.a"]}, slots={_SLOT})
        if reused
        else _leaf_named_source()
    )
    provision_pg_replication(conn, slot_name=_SLOT, publication_name="dsqlmig_pub_s",
                             tables=["app.a", "app.b"], full_identity_tables=["app.b"])
    writes = _catalog_writes(sql for sql, _params in conn.statements)
    kinds = {k for w in writes for k in _WRITE_WORDS if k in w}
    assert kinds == (
        set(_WRITE_WORDS) - {"CREATE PUBLICATION"}
        if reused
        else {"CREATE PUBLICATION", "REPLICA IDENTITY FULL", "PG_CREATE_LOGICAL_REPLICATION_SLOT"}
    ), writes
    summary = full_load_confirm_summary(["app.a", "app.b"], PG, FLC, keeps_watermark=False)
    _assert_every_write_is_named(writes, summary, _WRITE_WORDS)


# ---------------------------------------------------------------------------
# 1b. The real dialog, from each entry point
# ---------------------------------------------------------------------------


def _render_step(monkeypatch, *, state, job, session, tables, rows=(), cdc=False, **step):
    """Render the real Full Load step over a job manager that returns ``job`` (so the live
    region, with its per-table Reload, renders), the target probe patched; ``step`` goes to
    the renderer as is. Every label's ``set_text`` is recorded as ``SET:<text>``."""
    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_start_dialog_overlay import _DialogUi
    from tests.test_ui_data_migration import _StubConnector

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())
    monkeypatch.setattr(fl, "tables_with_rows",
                        lambda names, **_k: [n for n in names if n in rows])
    monkeypatch.setattr(fl, "target_primary_keys", lambda names, **k: {n: ["id"] for n in names})
    monkeypatch.setattr(fl, "tables_present", lambda names, **k: list(names))
    monkeypatch.setattr(fl, "cdc_streaming_started", lambda *a, **k: cdc)
    monkeypatch.setattr(_ConfirmDialogUi._El, "set_text",
                        lambda self, text: self._ui.texts.append(f"SET:{text}"), raising=False)
    ui = _DialogUi()
    dm._render_full_load_step(
        ui, state, _Jobs({"job-1": job} if job is not None else {}), session, job=job,
        status=StepStatus.NOT_STARTED if job is None else StepStatus.FAILED,
        selected_names=list(tables), retry_tables=lambda _names: None, guard_reason=None,
        start_full_load=lambda: None, retry_failed_load=lambda *a: None,
        reload_table=lambda *a: None, accept_quarantine_and_continue=lambda *a: None,
        stop_full_load=lambda *a: None, refresh=lambda *a: None,
        schema_recreate_candidates=lambda: [], **step,
    )
    return ui


def _click(ui, label, *args):
    (handler,) = [h for (text, h) in ui.handlers if text == label]
    result = handler(*args)
    if asyncio.iscoroutine(result):
        asyncio.run(result)


def _summaries(ui):
    """Every summary the dialog rendered or set, in order."""
    return [
        t.removeprefix("SET:") for t in ui.texts
        if "The target tables will receive the snapshot rows;" in t
    ]


def _tick(ui, table, value):
    (toggle,) = [h for h in ui.checkbox_handlers if getattr(h, "__defaults__", None) == (table,)]
    toggle(SimpleNamespace(value=value))


def _failed_b():
    """A PostgreSQL Full load + CDC record whose app.b failed; the load's slot recorded."""
    return _with(
        _job("FAILED", [_c("app.a"), _c("app.b", status="FAILED")],
             counts={"app.a": 1, "app.b": 1}),
        slot=_SLOT,
    )


def test_start_says_the_new_snapshot_replaces_the_slot(monkeypatch) -> None:
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=None,
                      session=_G17PgSession(), tables=["app.a", "app.b"])
    _click(ui, "Start Full Load")
    assert _summaries(ui)[-1].endswith(_PROVISIONS)


@pytest.mark.parametrize("label,job", [
    ("Re-run all tables", _failed_b()),
    ("Re-run Full Load", _with(_loaded(), slot=_SLOT)),
])
def test_rerun_says_the_new_snapshot_replaces_the_slot(monkeypatch, label, job) -> None:
    # FAILS WITHOUT THE FIX: the slot was said to be created "or reused"; it is dropped.
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=job,
                      session=_G17PgSession(), tables=["app.a", "app.b"])
    _click(ui, label)
    assert _summaries(ui)[-1].endswith(_PROVISIONS)


def test_rerun_while_cdc_streams_says_the_source_is_only_read(monkeypatch) -> None:
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=_failed_b(),
                      session=_G17PgSession(), tables=["app.a", "app.b"], cdc=True)
    _click(ui, "Re-run all tables")
    assert "CDC is currently streaming" in " ".join(ui.texts)
    assert _summaries(ui)[-1].endswith(_READ_ONLY)


def test_retry_unfinished_only_reads_the_source(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the retry dialog listed slot / publication / REPLICA IDENTITY
    # writes the retry engine never makes.
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=_failed_b(),
                      session=_G17PgSession(), tables=["app.a", "app.b"])
    _click(ui, "Retry unfinished tables (1)")
    (summary,) = set(_summaries(ui))
    assert summary == (
        "1 table selected for Full Load. The target tables will receive the snapshot rows"
        + _READ_ONLY
    )


def test_retry_names_a_ticked_addition_and_follows_the_ticks(monkeypatch) -> None:
    # app.c was selected after the Full Load: its load first joins it to CDC's publication
    # unless it is a member already (the one source write a retry makes); unticked, the
    # retry only reads the source.
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b", "app.c"]), job=_failed_b(),
                      session=_G17PgSession(), tables=["app.a", "app.b", "app.c"])
    _click(ui, "Retry unfinished tables (2)")
    assert _summaries(ui)[-1].endswith(_JOINS_C)
    _tick(ui, "app.c", False)
    assert _summaries(ui)[-1] == (
        "1 table selected for Full Load. The target tables will receive the snapshot rows"
        + _READ_ONLY
    )


def test_a_retry_names_every_source_write_its_guard_makes(monkeypatch) -> None:
    # The REAL PostgreSQL guard of the Retry below, over a publication left at
    # publish_via_partition_root = false: it sets the publication's setting before app.c's
    # ADD. FAILS WITHOUT THE FIX: the Retry dialog named only the join.
    from tests.test_g17_full_load_and_cdc_additions import (
        _guard,
        _pg_migrator,
        _pg_watermark,
        _PgSource,
    )

    source = _PgSource(members=("app.a", "app.b"), via_root=False)
    out, _beats = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": None}
    writes = _catalog_writes(source.writes())
    assert {k for w in writes for k in _JOIN_WORDS if k in w} == set(_JOIN_WORDS), writes
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b", "app.c"]), job=_failed_b(),
                      session=_G17PgSession(), tables=["app.a", "app.b", "app.c"])
    _click(ui, "Retry unfinished tables (2)")
    _assert_every_write_is_named(writes, _summaries(ui)[-1], _JOIN_WORDS)


def test_an_addition_without_the_loads_slot_joins_no_publication(monkeypatch) -> None:
    # No slot on the watermark: Start CDC re-snapshots every captured table, so the guard
    # adds nothing to a publication (_guard_pg_additions).
    job = _job("FAILED", [_c("app.a"), _c("app.b", status="FAILED")],
               counts={"app.a": 1, "app.b": 1})
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b", "app.c"]), job=job,
                      session=_G17PgSession(), tables=["app.a", "app.b", "app.c"])
    _click(ui, "Retry unfinished tables (2)")
    assert _summaries(ui)[-1].endswith(_READ_ONLY)


def test_reload_specific_tables_only_reads_the_source(monkeypatch) -> None:
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=_with(_loaded(), slot=_SLOT),
                      session=_G17PgSession(), tables=["app.a", "app.b"])
    _click(ui, "Reload specific tables…")
    assert _summaries(ui) == []  # nothing ticked yet
    _tick(ui, "app.a", True)
    assert _summaries(ui)[-1] == (
        "1 table selected for Full Load. The target tables will receive the snapshot rows"
        + _READ_ONLY
    )


def test_a_per_table_reload_only_reads_the_source(monkeypatch) -> None:
    job = _with(
        _job("DONE", [_c("app.a", rows_quarantined=1, rows_loaded=4), _c("app.b")],
             counts={"app.a": 5, "app.b": 1}),
        slot=_SLOT,
    )
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=job,
                      session=_G17PgSession(), tables=["app.a", "app.b"])
    _click(ui, "Reload", None)
    assert "Reload app.a?" in ui.texts
    assert _summaries(ui)[-1] == (
        "1 table selected for Full Load. The target tables will receive the snapshot rows"
        + _READ_ONLY
    )


def test_a_per_table_reload_of_an_addition_names_its_publication_join(monkeypatch) -> None:
    # app.c was selected after the Full Load and its guarded load quarantined rows, so it is
    # still pending -- and already a member: that guarded attempt joined it before its copy
    # (ch.13: a table added for a load that then fails stays in the publication), so this
    # Reload's guard finds it a member and writes nothing. FAILS WITHOUT THE FIX: "except
    # that app.c is added to the CDC publication on the source", a write that never happens.
    job = _with(
        _job("DONE", [_c("app.a"), _c("app.b"), _c("app.c", rows_quarantined=2)],
             counts={"app.a": 1, "app.b": 1}),
        slot=_SLOT,
    )
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b", "app.c"]), job=job,
                      session=_G17PgSession(), tables=["app.a", "app.b", "app.c"])
    _click(ui, "Reload", None)
    assert "Reload app.c?" in ui.texts
    assert _summaries(ui)[-1].endswith(_JOINS_C)
    assert "is added to the CDC publication" not in " ".join(_summaries(ui))


def test_the_reload_after_a_guarded_attempt_joined_claims_no_add_it_will_not_make(
    monkeypatch,
) -> None:
    # The chain end to end: the REAL PostgreSQL guard and retry engine, then the REAL dialog
    # on the record they leave. FAILS WITHOUT THE FIX at the dialog: "app.c is added to the
    # CDC publication", though the Reload it confirms issues no ALTER PUBLICATION at all.
    from dsql_migrator.core.batched_import import QuarantineRecord
    from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult

    from tests.test_g17_full_load_and_cdc_additions import (
        _PgSource,
        _pg_migrator,
        _pg_watermark,
        _run_retry,
    )

    class _Copy:
        """The real guard; a copy that quarantines ``dropped[table]`` rows."""

        def __init__(self, guard, dropped):
            self.guard, self.dropped = guard, dropped

        def guard_cdc_additions(self, names, **kw):
            return self.guard.guard_cdc_additions(names, **kw)

        def migrate_table(self, table, *, on_rows=None, **_kw):
            if on_rows is not None:
                on_rows(4, 0)
            dropped = self.dropped.get(table.name, 0)
            if not dropped:
                return 4
            return TableLoadResult(
                rows_loaded=4, rows_quarantined=dropped,
                quarantine_records=tuple(
                    QuarantineRecord(table=table.name, primary_key=f"id={i}",
                                     error_code="54000", message="value too large")
                    for i in range(dropped)
                ),
            )

    def _adds():
        return [w for w in source.writes() if " ADD TABLE " in w]

    source = _PgSource(members=("app.a", "app.b"))
    guard = _pg_migrator(monkeypatch, source)
    first = _run_retry(monkeypatch, _Copy(guard, {"app.c": 2}), retry=("app.c",),
                       additions=("app.c",), watermark=_pg_watermark())
    assert _adds() == ['ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE ONLY "APP"."C"']
    assert not (first.job.watermark.cdc_added_tables or {})  # quarantined: still pending
    record = first.job.model_copy(deep=True)
    record.job_id = "job-1"
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b", "app.c"]), job=record,
                      session=_G17PgSession(), tables=["app.a", "app.b", "app.c"])
    _click(ui, "Reload", None)
    assert "Reload app.c?" in ui.texts
    assert _summaries(ui)[-1].endswith(_JOINS_C)
    # What that Reload runs: the guard finds app.c a member and adds nothing.
    second = _run_retry(monkeypatch, _Copy(guard, {}), retry=("app.c",), additions=("app.c",),
                        watermark=first.job.watermark, prior=list(first.job.chunks))
    assert len(_adds()) == 1
    assert second.job.watermark.cdc_added_tables == {"app.c": 4}


@pytest.mark.parametrize("label, flush", [
    ("Re-run all tables", True), ("Retry unfinished tables (1)", False),
])
def test_mysql_full_load_and_cdc_names_the_flush_only_for_a_new_start_point(
        monkeypatch, label, flush):
    # G147: Re-run all tables records (and fences) a new start point; a Retry keeps the
    # original one and only reads the source.
    ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=_failed_b(),
                      session=_G17Session(), tables=["app.a", "app.b"])
    _click(ui, label)
    assert ("FLUSH BINARY LOGS" in _summaries(ui)[-1]) is flush
    assert _summaries(ui)[-1].endswith(_READ_ONLY) is not flush


def test_the_ledger_retry_reopens_the_dialog_with_the_same_action_kind(monkeypatch) -> None:
    # The dialog's own Retry (the excluded-column record could not be read) re-opens it: a
    # Retry unfinished tables dialog must not come back as a new snapshot's.
    from tests.test_start_dialog_overlay import PAGES, _view

    reads: list = []

    def _unreadable():
        reads.append(1)
        raise RuntimeError("boom")

    job = _with(_job("FAILED", [_c(PAGES, status="FAILED")], counts={PAGES: 1}), slot=_SLOT)
    ui = _render_step(monkeypatch, state=_fc([PAGES]), job=job, session=_G17PgSession(),
                      tables=[PAGES], overlay_ledger_read=_unreadable,
                      exclusion_overlay_view=lambda _tables: _view())
    _click(ui, "Retry unfinished tables (1)")
    assert "Could not read the record of excluded-column changes" in " ".join(ui.texts)
    first = len(_summaries(ui))
    _click(ui, "Retry")
    assert len(reads) == 2 and len(_summaries(ui)) > first  # re-read, re-opened
    assert all(s.endswith(_READ_ONLY) for s in _summaries(ui)), _summaries(ui)


# ---------------------------------------------------------------------------
# 2. The index notice: "NOT created" only from the indexes known missing
# ---------------------------------------------------------------------------


def _unconfirmed(ddl=_UNIQUE_DDL):
    """An index whose CREATE answered XX000 but landed: it exists; the retry got 42P07."""
    created, failures = _index_importer(_IndexCatalog(failures=[("XX000", True)])).create_indexes(
        [ddl]
    )
    assert created == 0 and len(failures) == 1 and is_unconfirmed_index_failure(failures[0])
    return failures


_PLAIN_MISSING = ["ix_total: more than 24 indexes per table are not allowed"]
_UNIQUE_MISSING = ["UNIQUE ux_b: more than 24 indexes per table are not allowed"]


def test_an_unconfirmed_unique_index_beside_a_missing_plain_one_is_no_missing_constraint(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: "Unique index NOT created (2) -- the data loaded, but a
    # constraint is missing ... the target now does not [enforce it] ... create it BEFORE
    # cutting over", for a unique index that exists and a missing index that is not unique.
    _messages, notices, badges = _index_failures_panel(
        monkeypatch, {"shop.orders": _unconfirmed(), "shop.items": _PLAIN_MISSING}
    )
    (notice,) = notices
    assert notice["tone"] == "warning"  # the unique one may still be missing
    assert notice["header"] == (
        "Indexes not created (1), not confirmed (1) — the data loaded; check that the unique "
        "index exists before cutting over"
    )
    assert "constraint is missing" not in notice["header"]
    assert "the target now does not" not in notice["body"]
    assert "one of them is a UNIQUE index, so check BEFORE cutting over." in notice["body"]
    assert "shop.orders · Done — index not confirmed" in badges
    assert "shop.items · Done — index missing" in badges


def test_a_missing_unique_index_beside_an_unconfirmed_plain_one_still_says_not_created(
    monkeypatch,
) -> None:
    _messages, notices, _badges = _index_failures_panel(
        monkeypatch,
        {"shop.orders": _unconfirmed("CREATE INDEX ASYNC ix_a ON t (a)"),
         "shop.items": _UNIQUE_MISSING},
    )
    (notice,) = notices
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes NOT created (1), not confirmed (1) — the data loaded, but a constraint is "
        "missing"
    )
    assert "create it BEFORE cutting over" in notice["body"]
    assert "one of them is a UNIQUE index" not in notice["body"]


def test_a_mix_of_unique_indexes_names_both_remedies(monkeypatch) -> None:
    _messages, notices, _badges = _index_failures_panel(
        monkeypatch, {"shop.orders": _unconfirmed(), "shop.items": _UNIQUE_MISSING}
    )
    (notice,) = notices
    assert notice["header"].startswith("Indexes NOT created (1), not confirmed (1)")
    assert "create it BEFORE cutting over" in notice["body"]
    assert "one of them is a UNIQUE index, so check BEFORE cutting over." in notice["body"]


def test_a_mix_of_plain_indexes_names_both_groups_as_an_info(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Indexes not created (2)" for an index that may exist.
    _messages, notices, _badges = _index_failures_panel(
        monkeypatch,
        {"shop.orders": _unconfirmed("CREATE INDEX ASYNC ix_a ON t (a)"),
         "shop.items": _PLAIN_MISSING},
    )
    (notice,) = notices
    assert notice["tone"] == "info"
    assert notice["header"] == (
        "Indexes not created (1), not confirmed (1) — the data loaded completely"
    )
    assert "create only the ones that are missing." in notice["body"]


# ---------------------------------------------------------------------------
# 2b. Every index failure counts, not each table's latest message
# ---------------------------------------------------------------------------

_IX_B = "CREATE INDEX ASYNC ix_b ON t (b)"


def _index_pass(catalog, ddls=(_UNIQUE_DDL, _IX_B)):
    """The real post-load index pass over one table: UNIQUE first, as MySQL's SHOW CREATE
    TABLE lists a table's keys. Every index fails."""
    created, failures = _index_importer(catalog, occ_max_attempts=3).create_indexes(list(ddls))
    assert created == 0 and len(failures) == len(ddls)
    return failures


def _as_the_step_renders_it(monkeypatch, failures_by_table, *, then=None):
    """Record each table's index failures as the Full Load does (one record per index, in
    DDL order), then render the panel exactly as ``_live_detail`` does: one row per table
    from its LATEST message, and EVERY record as ``quarantine_records``. ``then`` records
    ``{table: message}`` after them, that table's chunk FAILED. Returns ``(index notices,
    badges, labels)``."""
    from datetime import datetime, timezone

    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.models import ChunkState, DataErrorRecord, MigrationJob
    from dsql_migrator.ui.data_migration import _full_load_ui as flu
    from dsql_migrator.ui.data_migration._cdc_status import (
        full_load_error_records_for,
        full_load_error_summary_for,
        full_load_latest_messages_for,
    )
    from dsql_migrator.ui.data_migration._full_load_engine import _record_index_failures
    from dsql_migrator.ui.data_migration._models import build_full_load_table_rows

    from tests.test_ui_data_migration import _DetailRowUi

    log = ErrorLogStore()
    for table, failures in failures_by_table.items():
        _record_index_failures(log, "j1", table, failures)
    for table, message in (then or {}).items():
        log.record("j1", DataErrorRecord(table=table, chunk_id=table, error_code=None,
                                         message=message,
                                         occurred_at=datetime.now(timezone.utc)))
    job = MigrationJob(job_id="j1", status="DONE", progress_pct=100.0, chunks=[
        ChunkState(chunk_id=t, status="FAILED" if t in (then or {}) else "DONE",
                   rows_loaded=3, attempts=1)
        for t in failures_by_table
    ])
    rows = build_full_load_table_rows(
        job, full_load_error_summary_for(log, job), full_load_latest_messages_for(log, job)
    )
    notices: list = []
    monkeypatch.setattr(flu, "render_notice", lambda _ui, **kw: notices.append(kw))
    ui = _DetailRowUi()
    flu._render_full_load_progress(
        ui, job, rows, reload_table=lambda _n: None,
        quarantine_records=[
            (str(r.table), str(r.message)) for r in full_load_error_records_for(log, job)
        ],
    )
    return [n for n in notices if "ndex" in str(n.get("header", ""))], ui.badges, ui.labels


def test_two_unconfirmed_indexes_on_one_table_keep_the_unique_warning(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: a sustained XX000 fails ux_a then ix_b; judged by its latest
    # message (ix_b) the table read "Indexes not confirmed (1) -- the data loaded
    # completely" (info), with no word of the unique index.
    failures = _index_pass(_IndexCatalog(failures=[("XX000", False)] * 6))
    notices, badges, labels = _as_the_step_renders_it(monkeypatch, {"shop.orders": failures})
    (notice,) = notices
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes not confirmed (2, 1 unique) — the data loaded; check that the unique index "
        "exists before cutting over"
    )
    assert badges.count("shop.orders · Done — index not confirmed") == 2
    # Each index is listed, the unique one too, so the operator knows which to check.
    assert [m.split(":")[1].strip() for m in labels if m.startswith("index not")] == [
        "UNIQUE ux_a", "ix_b"
    ]


def test_two_missing_indexes_on_one_table_keep_the_missing_constraint(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Indexes not created (1) -- the data loaded completely" (info)
    # for a table whose UNIQUE index is known missing. Both names were another table's.
    failures = _index_pass(_IndexCatalog(existing={"ux_a", "ix_b"}))
    notices, badges, _labels = _as_the_step_renders_it(monkeypatch, {"shop.orders": failures})
    (notice,) = notices
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes NOT created (2, 1 unique) — the data loaded, but a constraint is missing"
    )
    assert "create it BEFORE cutting over" in notice["body"]
    assert badges.count("shop.orders · Done — index missing") == 2


_PLAIN_DDLS = tuple(f"CREATE INDEX ASYNC ix_{c} ON t ({c})" for c in "bcd")
_UX_E = "CREATE UNIQUE INDEX ASYNC ux_e ON t (e)"


def test_one_unique_among_four_unconfirmed_indexes_is_counted_apart(monkeypatch) -> None:
    # A sustained XX000 outlasts the retries of every CREATE on shop.orders. FAILS WITHOUT
    # THE FIX: "Unique index not confirmed (4)" -- four unique indexes, where one exists.
    failures = _index_pass(_IndexCatalog(failures=[("XX000", False)] * 12),
                           ddls=(_UNIQUE_DDL, *_PLAIN_DDLS))
    (notice,), badges, _labels = _as_the_step_renders_it(monkeypatch, {"shop.orders": failures})
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes not confirmed (4, 1 unique) — the data loaded; check that the unique index "
        "exists before cutting over"
    )
    assert badges.count("shop.orders · Done — index not confirmed") == 4


def test_one_unique_among_four_missing_indexes_is_counted_apart(monkeypatch) -> None:
    # Every name was another table's. FAILS WITHOUT THE FIX: "Unique index NOT created (4)".
    failures = _index_pass(_IndexCatalog(existing={"ux_a", "ix_b", "ix_c", "ix_d"}),
                           ddls=(_UNIQUE_DDL, *_PLAIN_DDLS))
    (notice,), _badges, _labels = _as_the_step_renders_it(
        monkeypatch, {"shop.orders": failures}
    )
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes NOT created (4, 1 unique) — the data loaded, but a constraint is missing"
    )


def test_the_unique_count_spans_the_tables(monkeypatch) -> None:
    # One unique index on one table, a plain one on each of two others. FAILS WITHOUT THE
    # FIX: "Unique index NOT created (3)".
    _messages, (notice,), _badges = _index_failures_panel(
        monkeypatch,
        {"shop.orders": _UNIQUE_MISSING, "shop.items": _PLAIN_MISSING,
         "shop.users": ["ix_name: more than 24 indexes per table are not allowed"]},
    )
    assert notice["header"] == (
        "Indexes NOT created (3, 1 unique) — the data loaded, but a constraint is missing"
    )


def test_two_unique_among_three_unconfirmed_indexes_are_named_in_the_plural(monkeypatch):
    failures = _index_pass(_IndexCatalog(failures=[("XX000", False)] * 9),
                           ddls=(_UNIQUE_DDL, _IX_B, _UX_E))
    (notice,), _badges, _labels = _as_the_step_renders_it(
        monkeypatch, {"shop.orders": failures}
    )
    assert notice["header"] == (
        "Indexes not confirmed (3, 2 unique) — the data loaded; check that the unique indexes "
        "exist before cutting over"
    )


_UX_B = "CREATE UNIQUE INDEX ASYNC ux_b ON t (b)"


@pytest.mark.parametrize("second,check,note", [
    (_UX_B, "the unique indexes exist", "2 of them are UNIQUE indexes"),
    (_IX_B, "the unique index exists", "one of them is a UNIQUE index"),
], ids=["two-unique", "one-unique"])
def test_the_unconfirmed_unique_indexes_of_a_mix_are_counted(monkeypatch, second, check, note):
    # UNIQUE ux_a on shop.orders and (two-unique) UNIQUE ux_b on shop.users answered XX000
    # but landed; plain ix_total on shop.items is missing at the 24-index limit. FAILS
    # WITHOUT THE FIX (two-unique): "check that the unique index exists" and "one of them is
    # a UNIQUE index", so an operator checked one constraint and could cut over with the
    # other one missing.
    _messages, (notice,), badges = _index_failures_panel(
        monkeypatch,
        {"shop.orders": _unconfirmed(), "shop.users": _unconfirmed(second),
         "shop.items": _PLAIN_MISSING},
    )
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        f"Indexes not created (1), not confirmed (2) — the data loaded; check that {check} "
        "before cutting over"
    )
    assert notice["body"].endswith(
        f"create only the ones that are missing — {note}, so check BEFORE cutting over."
    )
    assert [b for b in badges if "· Done — index" in b] == [
        "shop.orders · Done — index not confirmed",
        "shop.users · Done — index not confirmed",
        "shop.items · Done — index missing",
    ]


def test_two_unconfirmed_unique_indexes_beside_a_missing_unique_one_are_counted(monkeypatch):
    # The missing UNIQUE index keeps the "constraint is missing" header and its create; the
    # check names both unconfirmed UNIQUE indexes. FAILS WITHOUT THE FIX: "one of them is a
    # UNIQUE index".
    _messages, (notice,), _badges = _index_failures_panel(
        monkeypatch,
        {"shop.orders": _unconfirmed(), "shop.users": _unconfirmed(_UX_E),
         "shop.items": _UNIQUE_MISSING},
    )
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes NOT created (1), not confirmed (2) — the data loaded, but a constraint is "
        "missing"
    )
    assert "so create it BEFORE cutting over." in notice["body"]
    assert notice["body"].endswith(
        "create only the ones that are missing — 2 of them are UNIQUE indexes, so check "
        "BEFORE cutting over."
    )


@pytest.mark.parametrize("catalog,header", [
    (lambda: _IndexCatalog(existing={"ux_a", "ux_e"}),
     "Unique index NOT created (2) — the data loaded, but a constraint is missing"),
    # Two of them: "check that they exist" (G25 round 3), not "check that it exists".
    (lambda: _IndexCatalog(failures=[("XX000", False)] * 6),
     "Unique index not confirmed (2) — the data loaded; check that they exist before cutting "
     "over"),
], ids=["missing", "not-confirmed"])
def test_only_unique_indexes_keep_the_unique_index_header(monkeypatch, catalog, header):
    failures = _index_pass(catalog(), ddls=(_UNIQUE_DDL, _UX_E))
    (notice,), _badges, _labels = _as_the_step_renders_it(
        monkeypatch, {"shop.orders": failures}
    )
    assert (notice["tone"], notice["header"]) == ("warning", header)


def test_a_missing_unique_then_an_unconfirmed_index_on_one_table_names_both(monkeypatch):
    # FAILS WITHOUT THE FIX: "Indexes not confirmed (1)" (info) -- the docs' "names both"
    # was false whenever both were on one table.
    # ux_a is refused outright (0A000); ix_b's XX000 landed, its retry found the name taken.
    failures = _index_pass(_IndexCatalog(failures=[("0A000", False), ("XX000", True)]))
    notices, badges, _labels = _as_the_step_renders_it(monkeypatch, {"shop.orders": failures})
    (notice,) = notices
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes NOT created (1), not confirmed (1) — the data loaded, but a constraint is "
        "missing"
    )
    assert [b for b in badges if "· Done — index" in b] == [
        "shop.orders · Done — index missing", "shop.orders · Done — index not confirmed"
    ]


def test_an_unconfirmed_unique_then_a_missing_index_on_one_table_names_both(monkeypatch):
    # ux_a's XX000 landed (its retry then found the name taken); ix_b's name was another
    # table's from the start.
    failures = _index_pass(_IndexCatalog(failures=[("XX000", True)], existing={"ix_b"}))
    notices, badges, _labels = _as_the_step_renders_it(monkeypatch, {"shop.orders": failures})
    (notice,) = notices
    assert notice["tone"] == "warning"
    assert notice["header"] == (
        "Indexes not created (1), not confirmed (1) — the data loaded; check that the unique "
        "index exists before cutting over"
    )
    assert [b for b in badges if "· Done — index" in b] == [
        "shop.orders · Done — index not confirmed", "shop.orders · Done — index missing"
    ]


def test_a_table_listed_among_the_failures_gets_no_index_entry(monkeypatch) -> None:
    # Its latest record is a real failure: it is not "Done", so its earlier index records
    # are not reported as a Done table's (as before: it was never judged by them).
    notices, badges, _labels = _as_the_step_renders_it(
        monkeypatch,
        {"shop.orders": _index_pass(_IndexCatalog(existing={"ux_a", "ix_b"})),
         "shop.items": _PLAIN_MISSING},
        then={"shop.orders": "shard 0 FAILED (rows_loaded=1): boom"},
    )
    (notice,) = notices
    assert notice["header"] == "Indexes not created (1) — the data loaded completely"
    assert badges.count("shop.items · Done — index missing") == 1
    assert not any(b.startswith("shop.orders · Done") for b in badges)


def test_the_live_step_judges_the_index_notice_from_every_record(monkeypatch) -> None:
    # The real step's live region: rows built from the latest message per table, every
    # record passed through. FAILS WITHOUT THE FIX: the info "Indexes not confirmed (1)".
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from dsql_migrator.ui.data_migration._full_load_engine import _record_index_failures

    notices: list = []
    monkeypatch.setattr(fl, "render_notice", lambda _ui, **kw: notices.append(kw))
    state = _fc(["app.a", "app.b"])
    _record_index_failures(
        state.error_log, "job-1", "app.a",
        _index_pass(_IndexCatalog(failures=[("XX000", False)] * 6)),
    )
    ui = _render_step(monkeypatch, state=state, job=_loaded(), session=_G17Session(),
                      tables=["app.a", "app.b"])
    (notice,) = [n for n in notices if "ndex" in str(n.get("header", ""))]
    assert notice["tone"] == "warning"
    assert notice["header"].startswith("Indexes not confirmed (2, 1 unique)")
    assert ui.texts.count("app.a · Done — index not confirmed") == 2


# ---------------------------------------------------------------------------
# 3. A connection that drops while the CREATE INDEX runs leaves it "not confirmed"
# ---------------------------------------------------------------------------


class _Catalog:
    """Index names shared by every connection; ``script`` scripts the next DDL executes as
    ``(exception factory, lands)`` -- ``lands`` adds the index before the error, as a
    committed statement whose reply was lost does."""

    def __init__(self, script=(), existing=()):
        self.script = list(script)
        self.names = set(existing)
        self.executes = 0
        self.connects = 0


class _Cursor:
    def __init__(self, catalog):
        self._catalog = catalog

    def execute(self, ddl, params=None):
        catalog = self._catalog
        catalog.executes += 1
        name = _index_name_of(ddl)
        if catalog.script:
            factory, lands = catalog.script.pop(0)
            if lands:
                catalog.names.add(name)
            raise factory()
        if name in catalog.names:
            raise psycopg.errors.lookup("42P07")(f'relation "{name}" already exists')
        catalog.names.add(name)

    def close(self):
        pass


class _Connection:
    def __init__(self, catalog, *, closed=False):
        self._catalog, self._closed = catalog, closed

    def cursor(self):
        if self._closed:
            # psycopg's own check: the client knows the connection is gone, sends nothing.
            raise psycopg.OperationalError("the connection is closed")
        return _Cursor(self._catalog)

    def close(self):
        pass


def _importer(catalog, factory=None, attempts=5):
    def _connect():
        catalog.connects += 1
        return _Connection(catalog)

    return BatchedImporter(
        BatchedImportOptions(parallelism=1),
        connection_factory=factory or _connect,
        occ_max_attempts=attempts,
        occ_base_delay=0.0,
        sleep=lambda _s: None,
        jitter=lambda: 0.0,
    )


def _eof():
    return psycopg.OperationalError("consuming input failed: SSL SYSCALL error: EOF detected")


def _class08():
    return psycopg.errors.lookup("08006")("server closed the connection unexpectedly")


@pytest.mark.parametrize("drop", [_eof, _class08], ids=["no-sqlstate", "class-08"])
def test_a_create_whose_connection_dropped_after_it_landed_is_not_confirmed(
    monkeypatch, drop
) -> None:
    # FAILS WITHOUT THE FIX: the retry's 42P07 was reported "index not created ... a
    # constraint is missing ... create it BEFORE cutting over" for an index that exists.
    catalog = _Catalog(script=[(drop, True)])
    created, failures = _importer(catalog).create_indexes([_UNIQUE_DDL])
    assert (created, catalog.executes, "ux_a" in catalog.names) == (0, 2, True)
    (failure,) = failures
    assert failure == (
        'UNIQUE ux_a: DuplicateTable: relation "ux_a" already exists' + UNCONFIRMED_INDEX_DROP_NOTE
    )
    assert is_unconfirmed_index_failure(failure)
    assert UNCONFIRMED_INDEX_NOTE not in failure  # no XX000 claimed
    messages, notices, badges = _index_failures_panel(monkeypatch, {"shop.orders": failures})
    assert messages["shop.orders"].startswith("index not confirmed: UNIQUE ux_a: ")
    (notice,) = notices
    assert notice["header"] == (
        "Unique index not confirmed (1) — the data loaded; check that it exists before "
        "cutting over"
    )
    assert "or the connection dropped" in notice["body"]
    assert "shop.orders · Done — index not confirmed" in badges


def test_a_drop_whose_create_never_landed_is_simply_created_on_the_retry() -> None:
    catalog = _Catalog(script=[(_eof, False)])
    assert _importer(catalog).create_indexes([_UNIQUE_DDL]) == (1, [])
    assert catalog.names == {"ux_a"}


def test_drops_that_outlast_the_budget_are_reported_not_confirmed() -> None:
    catalog = _Catalog(script=[(_eof, False)] * 5)
    created, (failure,) = _importer(catalog, attempts=3).create_indexes([_UNIQUE_DDL])
    assert (created, catalog.executes) == (0, 3)
    assert failure.startswith("UNIQUE ux_a: OperationalError: consuming input failed")
    assert failure.endswith(UNCONFIRMED_INDEX_DROP_NOTE)


def test_a_drop_while_connecting_does_not_mark_the_index() -> None:
    # Opening the connection ran no CREATE: the later 42P07 keeps its plain meaning.
    catalog = _Catalog(existing={"ux_a"})
    opened: list = []

    def _connect():
        opened.append(1)
        if len(opened) == 1:
            raise _eof()
        return _Connection(catalog)

    created, failures = _importer(catalog, factory=_connect).create_indexes([_UNIQUE_DDL])
    assert (created, len(opened), catalog.executes) == (0, 2, 1)
    assert failures == ['UNIQUE ux_a: DuplicateTable: relation "ux_a" already exists']
    assert not is_unconfirmed_index_failure(failures[0])


def test_a_cursor_on_a_connection_known_closed_does_not_mark_the_index() -> None:
    # psycopg refuses the cursor before anything is sent; the retry's fresh connection then
    # finds the name taken by an index that was there before.
    catalog = _Catalog(existing={"ux_a"})
    opened: list = []

    def _connect():
        opened.append(1)
        return _Connection(catalog, closed=len(opened) == 1)

    created, failures = _importer(catalog, factory=_connect).create_indexes([_UNIQUE_DDL])
    assert (created, len(opened), catalog.executes) == (0, 2, 1)
    assert failures == ['UNIQUE ux_a: DuplicateTable: relation "ux_a" already exists']


def test_a_permanent_error_on_the_statement_does_not_mark_the_index() -> None:
    catalog = _Catalog(script=[(lambda: psycopg.errors.lookup("42601")("syntax error"), False)])
    _created, (failure,) = _importer(catalog).create_indexes([_UNIQUE_DDL])
    assert failure == "UNIQUE ux_a: SyntaxError: syntax error"
    assert not is_unconfirmed_index_failure(failure)


def test_both_notes_are_recognised_and_nothing_else_is() -> None:
    assert is_unconfirmed_index_failure("UNIQUE ux_a: E: x" + UNCONFIRMED_INDEX_NOTE)
    assert is_unconfirmed_index_failure("ux_a: E: x" + UNCONFIRMED_INDEX_DROP_NOTE)
    assert not is_unconfirmed_index_failure("ux_a: E: relation already exists")
    assert not is_unconfirmed_index_failure(None)
