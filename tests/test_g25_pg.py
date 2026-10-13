# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 repair round 1: the PostgreSQL publication / REPLICA IDENTITY review findings.

* the fix for an inheritance child's missing identity offered a new primary key first
  (since round 3's second repair, the children are not graded at all);
* the Start CDC worker's slot-position check never fired: the loop records the Start window
  right after submitting, before the worker reads it -- and a Start the check refused then
  exempted every later one (since round 4 no window counts at all: the check judges a
  stack's first Start by what the stack records, tests/test_g25_pg_r4.py);
* a refused re-keyed member of a reused publication was audited as "not published";
* the CDC monitor printed an unquoted ALTER TABLE;
* on PostgreSQL 18 a re-keyed table already FULL with a generated column passed the re-key
  check that tells it to keep FULL, while the identity check told it to leave FULL.

Each defect's test FAILS on the code before this round; the new-table audit line and the
restart are guards against over-correcting. Fakes only: no real database (the child fix
and the worker check were also run live on PostgreSQL 16, see the G25 notes).
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType, Watermark
from tests.test_g25_pg_publication import _Conn, _objects, _provision, _read


# ---------------------------------------------------------------------------
# An inheritance parent is graded by its own identity (round 3, repair round 2)
# ---------------------------------------------------------------------------


def test_a_parent_is_published_and_added_whatever_its_children() -> None:
    # Round 1 graded a parent's INHERITS children; round 3's second repair stopped: nothing the
    # tool runs publishes them (FOR TABLE ONLY / ADD TABLE ONLY), so a parent whose own
    # identity is usable is neither refused by provisioning nor by the one-table ADD (G17).
    from dsql_migrator.core.cdc_pg_slot import add_table_to_publication

    row = ("r", "d", True, 0, [])
    _provision(_Conn(usability={"app.parent": row}), ["app.parent"])
    added = _Conn(pubs={"p": ["app.a"]}, usability={"app.parent": row})
    assert add_table_to_publication(added, name="p", table="app.parent") is True
    assert added.writes() == ['ALTER PUBLICATION "p" ADD TABLE ONLY "app"."parent"']


# ---------------------------------------------------------------------------
# The audit line of a refused re-keyed table says whether it stays published
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pubs, outcome",
    [
        ({"dsqlmig_pub_s": ["app.orders"]}, "changed (it stays in the publication)"),
        (None, "changed and it is not published"),
    ],
    ids=["already-published", "new"],
)
def test_a_refused_rekeyed_tables_audit_line_says_whether_it_stays_published(
    pubs, outcome
) -> None:
    # FAILS WITHOUT THE FIX: a member of the reused publication was audited "it is not
    # published" -- a source-write record claiming a state that is not true.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _Conn(pubs=pubs, usability={"app.orders": ("r", "d", True, 0, [], False, True,
                                                      False)})
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError):
        _provision(conn, ["app.orders"], full=["app.orders"], logs=logs)
    (line,) = [entry for entry in logs if "is re-keyed" in entry]
    assert line.endswith(f"its REPLICA IDENTITY was not {outcome}"), line
    assert conn.writes() == []


# ---------------------------------------------------------------------------
# PostgreSQL 18: a re-keyed table already FULL with a generated column
# ---------------------------------------------------------------------------


class _ProbeResult:
    def __init__(self, sql, rows):
        self._sql, self._rows = sql, rows

    def fetchall(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def scalar(self):
        low = self._sql.lower()
        answers = {"wal_level": "logical", "max_replication_slots": "10",
                   "max_wal_senders": "10", "max_slot_wal_keep_size": "-1",
                   "pg_is_in_recovery": False, "has_database_privilege": True}
        return next((v for k, v in answers.items() if k in low), None)


class _ProbeConn:
    """The prerequisite probe's reads for app.t: its code ``identity`` and the G17/G25
    usability row ``usable`` (PostgreSQL 18, a generated column: ``full_refused``)."""

    def __init__(self, identity, usable):
        self.identity, self.usable = identity, usable

    def rollback(self):
        pass

    def execute(self, statement, params=None):
        sql = str(statement)
        if "AS identity_usable" in sql:
            return _ProbeResult(sql, [("app.t", *self.usable)])
        if "c.relreplident, c.relpersistence" in sql:
            return _ProbeResult(sql, [("app.t", self.identity, "p", "owner", False, False)])
        return _ProbeResult(sql, [])


def _identity_rows(identity, usable, rekeyed):
    from dsql_migrator.core.models import PrerequisiteCheckId, TableDef
    from dsql_migrator.core.prerequisites_postgres import check_postgres_cdc_prerequisites
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        _ProbeConn(identity, usable), ["app.t"])
    rows = check_postgres_cdc_prerequisites(
        facts, [TableDef(name="app.t", primary_key=["id"])],
        message_key_columns={"app.t": ["lead", "id"]} if rekeyed else {},
    )
    wanted = {PrerequisiteCheckId.REPLICA_IDENTITY: "identity",
              PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY: "covers"}
    return {wanted[r.check_id]: r for r in rows if r.check_id in wanted}


def test_a_rekeyed_full_table_postgresql_18_refuses_fails_the_rekey_check_too() -> None:
    # FAILS WITHOUT THE FIX: REPLICA_IDENTITY_COVERS_KEY PASSed on FULL ("a DELETE carries
    # lead") beside a REPLICA_IDENTITY FAIL saying FULL fails every UPDATE/DELETE -- and the
    # one fix it showed, DEFAULT, only then turned the re-key check red.
    from dsql_migrator.core.models import PrerequisiteStatus

    full_refused = ("r", "f", False, 0, [], False, True, False)
    rows = _identity_rows("f", full_refused, rekeyed=True)
    assert rows["identity"].status is PrerequisiteStatus.FAIL
    covers = rows["covers"]
    assert covers.status is PrerequisiteStatus.FAIL and covers.required is True
    assert "PostgreSQL 18" in covers.detail and "generated columns" in covers.detail
    assert covers.remediation.startswith('Set app.t back to "Keep source PK"')
    assert "ALTER TABLE" not in covers.remediation  # no FULL statement for it
    # Both fixes done (Keep source PK, then DEFAULT): nothing fails.
    after = _identity_rows("d", ("r", "d", True, 0, [], False, True, False), rekeyed=False)
    assert after["identity"].status is PrerequisiteStatus.PASS and "covers" not in after
    # PostgreSQL 13-17, or no generated column: FULL still passes the re-key check.
    plain = _identity_rows("f", ("r", "f", True, 0, []), rekeyed=True)
    assert plain["covers"].status is PrerequisiteStatus.PASS


# ---------------------------------------------------------------------------
# The CDC monitor's statement quotes the name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["opened", "reopened"])
def test_the_cdc_monitors_fix_quotes_a_mixed_case_name(kind) -> None:
    # FAILS WITHOUT THE FIX: "Fix: ALTER TABLE MixS.Orders REPLICA IDENTITY FULL" --
    # PostgreSQL folds it to mixs.orders: 'schema "mixs" does not exist', or FULL on a
    # lower-case twin while Cut over stays held.
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        ReplicaIdentityTransition,
        _transition_line,
    )
    from tests.test_cdc_replica_identity_gate import _finding

    _status, _action, detail = _transition_line(
        ReplicaIdentityTransition(kind=kind, table="MixS.Orders", finding=_finding()))
    assert 'Fix: ALTER TABLE "MixS"."Orders" REPLICA IDENTITY FULL on the source' in detail
    assert "ALTER TABLE MixS.Orders" not in detail
    assert "deletes on MixS.Orders can miss" in detail  # the prose keeps the bare name


# ---------------------------------------------------------------------------
# The Start CDC worker's slot-position check (residuals[2] defect B(b))
# ---------------------------------------------------------------------------

CAPTURED = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
AT_START = "0/19C26D0"  # the Full Load's start point: where its slot was created
MOVED = "0/19C2CE0"  # where another run with the same CDC stack name re-created it
SLOT_REFUSAL = "CDC's replication slot is not at this Full Load's start point"


def _watermark():
    return Watermark(slot_name="slot", publication_name="pub", wal_lsn=AT_START,
                     snapshot_timestamp=CAPTURED, table_row_counts={"app.orders": 1})


class _Engine:
    def connect(self):
        class _Connection:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, statement, _params=None):
                # The inheritance read of a Start that snapshots -- the Start dialog judges
                # its re-snapshot remedy's Start with it (G25): no captured table has any.
                assert "pg_catalog.pg_inherits" in str(statement), statement
                return SimpleNamespace(fetchall=lambda: [])

        return _Connection()

    def dispose(self):
        pass


def _slot_at(monkeypatch, confirmed):
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui import connect

    monkeypatch.setattr(connect, "make_source_engine_factory",
                        lambda *_a, **_k: (lambda _cfg: _Engine()))
    monkeypatch.setattr(
        cdc_pg_slot, "read_pg_replication_objects",
        lambda *_a, **_k: _objects(publication_name="pub", slot_name="slot",
                                   confirmed=confirmed,
                                   publication_tables=frozenset({"app.orders"})),
    )


def _start(monkeypatch, *, confirmed, state=None):
    """The real Start CDC, its job run AFTER ``_start_cdc_deploy`` returned -- the
    production order: the loop records the Start window before the worker's check runs."""
    from tests.test_ui_data_migration import _g5_inventory, _run_start_cdc_capture

    _slot_at(monkeypatch, confirmed)
    return _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, job_watermark=_watermark(),
        source_password="pw", selected=["app.orders"], inventory=_g5_inventory(),
        catch=True, msk_access=True, state=state,
    )


def _dialog_gate(monkeypatch, state, *, confirmed):
    """What the Start dialog shows before the next Start: the shared gate, by default."""
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _g5_inventory

    _slot_at(monkeypatch, confirmed)
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password="pw")
    return _cdc_ui._pg_objects_gate_block(state, session, _g5_inventory(), _watermark())


def test_the_workers_slot_check_is_not_exempted_by_its_own_start(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the loop set the Start window to this watermark's time right
    # after submitting, so the worker -- reading it after its I/O -- skipped the check and
    # streamed from the moved slot (the changes in between silently lost).
    run = _start(monkeypatch, confirmed=MOVED)
    assert run["submitted"]
    assert run["state"].cdc_ops_window_start == CAPTURED  # recorded before the check ran
    assert run["error"] is not None and SLOT_REFUSAL in str(run["error"])
    assert run["kwargs"] == {}  # no connector was created


def test_a_start_the_check_refused_never_exempts_a_later_one(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the refused Start's window exempted the next one -- the Start
    # dialog then showed no block, and the next Start streamed from the moved slot.
    state = _start(monkeypatch, confirmed=MOVED)["state"]
    blocked = _dialog_gate(monkeypatch, state, confirmed=MOVED)
    assert blocked is not None and blocked[0] == SLOT_REFUSAL
    again = _start(monkeypatch, confirmed=MOVED, state=state)
    assert SLOT_REFUSAL in str(again["error"]) and again["kwargs"] == {}


def test_a_start_refused_before_the_slot_check_does_not_count_for_it(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: a Start refused before the check (here: the target changed
    # after the click) recorded the window, so once the slot moved no Start was checked.
    from dsql_migrator.ui.data_migration import _cdc_ui

    same_target = _cdc_ui._same_target
    monkeypatch.setattr(_cdc_ui, "_same_target", lambda *_a: False)
    first = _start(monkeypatch, confirmed=AT_START)
    assert "the Aurora DSQL target changed" in str(first["error"])
    monkeypatch.setattr(_cdc_ui, "_same_target", same_target)
    later = _start(monkeypatch, confirmed=MOVED, state=first["state"])
    assert SLOT_REFUSAL in str(later["error"]) and later["kwargs"] == {}


def test_a_restart_after_a_start_that_went_ahead_is_never_judged_on_the_slot(
    monkeypatch,
) -> None:
    # The guard must not block a real restart: once a Start went ahead, its connector moves
    # the slot legitimately -- in this process, and in a restored session. Since round 4 the
    # stack's own record says so (Stop CDC leaves DeploySink=true with a blank
    # MskBootstrapServers, which the stack-phase probe reads: tests/test_g25_pg_r4.py).
    from dsql_migrator.ui.data_migration import DataMigrationState

    first = _start(monkeypatch, confirmed=AT_START)
    assert first["error"] is None and first["kwargs"]  # the connectors were created
    first["state"].set_cdc_has_committed_offset(True)  # Stop CDC, as the phase probe reads it
    restart = _start(monkeypatch, confirmed=MOVED, state=first["state"])
    assert restart["error"] is None and restart["kwargs"]
    assert _dialog_gate(monkeypatch, first["state"], confirmed=MOVED) is None
    restored = DataMigrationState()
    restored.set_cdc_has_committed_offset(True)  # the phase probe after the restart
    assert _dialog_gate(monkeypatch, restored, confirmed=MOVED) is None
    # A Start window alone exempts nothing: the stack records no Start (round 4).
    window_only = DataMigrationState()
    window_only.set_cdc_ops_window_start(CAPTURED)
    blocked = _dialog_gate(monkeypatch, window_only, confirmed=MOVED)
    assert blocked is not None and blocked[0] == SLOT_REFUSAL


def test_the_workers_slot_refusal_keeps_its_fix_for_the_cdc_card(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the job error was the whole ~470-character refusal and the
    # JobManager keeps 300, so the fix (the route rule's, G25 round 5: the re-snapshot the
    # dialog offers when its checks pass, or a Full Load re-run) was cut off. Now the job keeps
    # a short pointer and the card the full notice.
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import _cdc_ui

    real = _cdc_ui._logged_cdc_lifecycle

    def _lifecycle(action, *, detail, work, residue=None):
        wrapped = real(action, detail=detail, work=work, residue=residue)
        return lambda _bare: wrapped(SimpleNamespace(job_id="job-1", cancelled=False))

    monkeypatch.setattr(_cdc_ui, "_logged_cdc_lifecycle", _lifecycle)
    run = _start(monkeypatch, confirmed=MOVED)
    error = run["error"]
    jm = JobManager(stall_timeout_seconds=None)

    def _raise(_handle):
        raise error

    job_id = jm.submit(_raise)
    jm.wait(job_id, timeout=5)
    kept = jm.get_error(job_id)
    assert kept.startswith(f"RuntimeError: Start CDC blocked: {SLOT_REFUSAL}. Nothing was "
                           "changed; the CDC step shows the cause and the fix")
    state = run["state"]
    tone, _icon, header, body = _cdc_ui._start_refusal_notice(state, state.cdc_deploy_job_id)
    assert (tone, header) == ("error", SLOT_REFUSAL)
    assert body.endswith(
        "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
        "re-snapshots passes its own checks), or Re-run Full Load on the Full Load step choosing "
        "'Drop & reload' (it creates the slot again at its own snapshot point; give this migration "
        "its own CDC stack name if another migration shares this one), then Start CDC.")
