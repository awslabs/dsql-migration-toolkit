# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 3, repair round 1: the PostgreSQL findings the review of round 3 confirmed.

* a Start CDC that snapshots (``snapshot.mode=initial``: every CDC-only Start, a re-snapshot,
  the automatic one for an unfinished load) copied an inheritance parent's child rows into the
  parent's Aurora DSQL table -- Debezium's snapshot reads it without ``ONLY`` -- where CDC never
  changes them; the Full Load and Validation read it ``FROM ONLY`` since round 3. Start CDC
  now refuses it (the descendant check whose wording this repair fixed was removed in the
  second repair);
* the publish-list block told an operator to "let this run re-snapshot and create its own" (a
  re-snapshot reuses the publication as it is), and on a stack started before it was followed
  by routes that change nothing either; the Start dialog offered the re-snapshot button for it;
* on CDC only, the CDC monitor told a PostgreSQL 18 re-keyed table to "reload it" -- there is
  no Full Load step to reload from -- and with every open table refused still led with
  "Run ALTER TABLE <table> REPLICA IDENTITY FULL".

Every test FAILS on the code before this round. Fakes only: no real database (the catalog read
and the publish-list fix were also run live on PostgreSQL 16, see the repair notes).
"""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_g25_pg_publication import _objects

_INHERIT = "CDC cannot snapshot a table that has inheritance child tables"
_PUBLISH = "CDC's publication does not publish every change type"
_ALTER = "ALTER PUBLICATION \"{pub}\" SET (publish = 'insert, update, delete')"


# ---------------------------------------------------------------------------
# A source double: the publication/slot read, and the inheritance read on its connection
# ---------------------------------------------------------------------------


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _InheritConn:
    """Answers only the inheritance read (``children``: its rows), recording each call."""

    def __init__(self, children=()):
        self.children, self.calls = list(children), []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.calls.append((sql, params))
        assert "pg_catalog.pg_inherits" in sql, sql
        names = set((params or {}).get("names") or ())
        return _Rows([row for row in self.children if row[0] in names])


def _pg_source(monkeypatch, *, children=(), publishes_all=True, slot_usable=True):
    """The PostgreSQL source the Start gate reads: a publication covering the captured
    tables (or one restricted to a subset of INSERT/UPDATE/DELETE), its slot, and the
    inheritance children of each table. Returns the connection (its ``calls``)."""
    from dsql_migrator.core import cdc_pg_slot as pg_slot

    conn = _InheritConn(children)

    def _read(_conn, *, publication_name, slot_name):
        return pg_slot.PgReplicationObjects(
            publication_name=publication_name, slot_name=slot_name,
            publication_present=True, publication_publishes_all_dml=publishes_all,
            publication_tables=frozenset({"orders", "customers"}),
            slot_present_any_database=slot_usable, slot_usable=slot_usable,
            slot_confirmed_flush_lsn="0/41204A0" if slot_usable else None,
        )

    class _Engine:
        def connect(self):
            return conn

        def dispose(self):
            return None

    monkeypatch.setattr(pg_slot, "read_pg_replication_objects", _read)
    monkeypatch.setattr(
        "dsql_migrator.ui.connect.make_source_engine_factory",
        lambda *_a, **_k: (lambda *_b, **_c: _Engine()),
    )
    return conn


def _gate(state, watermark=None):
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _inventory

    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password="pw")
    return _cdc_ui._pg_objects_gate_block(state, session, _inventory(), watermark)


# ---------------------------------------------------------------------------
# A Start that snapshots refuses a captured table with inheritance child tables
# ---------------------------------------------------------------------------


def test_the_blocker_refuses_a_snapshot_of_a_table_with_inheritance_children() -> None:
    # FAILS WITHOUT THE FIX: no block -- Debezium's snapshot ("SELECT ... FROM app.par", no
    # ONLY) copied app.kid's rows into app.par on Aurora DSQL, where CDC never changes them.
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    children = (("app.par", 2, ("app.kid", "app.kid2")), ("app.p2", 5, ("a.b", "a.c", "a.d")))
    objects = _objects(publication_tables=frozenset({"app.par", "app.p2", "app.t"}),
                       inheritance_children=children)
    verdict = blocker(objects, ["app.par", "app.p2", "app.t"], resumes_from_slot=False,
                      connector_autocreates_publication=True, snapshots=True)
    assert verdict is not None and verdict[0] == _INHERIT
    body = verdict[1]
    assert body.startswith(
        "app.par has 2 inheritance (INHERITS) child tables: app.kid, app.kid2 (and 1 more "
        "captured table like it). This Start sends snapshot.mode=initial")
    assert "copied into app.par on Aurora DSQL, where CDC never updates or deletes them" in body
    assert '"Full load + CDC" run whose Full Load has finished reads only app.par\'s own' in body
    assert body.endswith("or untick app.par: each child table migrates as a table of its own.")
    # A sample cut short says so.
    many = blocker(_objects(publication_tables=frozenset({"app.p2"}),
                            inheritance_children=children[1:]),
                   ["app.p2"], resumes_from_slot=False, snapshots=True)
    assert "5 inheritance (INHERITS) child tables: a.b, a.c, a.d …." in many[1]
    # The gapless route reads each table FROM ONLY and snapshots nothing: no block.
    assert blocker(objects, ["app.par"], resumes_from_slot=True, snapshots=False) is None
    # A table with children that is not captured is not this Start's concern.
    assert blocker(objects, ["app.t"], resumes_from_slot=False, snapshots=True) is None


def test_the_inheritance_read_is_one_catalog_statement_of_real_children() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        _INHERITANCE_CHILDREN_SQL,
        read_inheritance_children,
        with_inheritance_children,
    )

    sql = " ".join(_INHERITANCE_CHILDREN_SQL.split())
    # Children, not relhassubclass (it stays true after the last child is dropped); a plain
    # parent only (a partitioned table's partitions are CDC's own); another session's
    # temporary child is left out, as PostgreSQL's own expansion leaves it out.
    assert "JOIN pg_catalog.pg_inherits i ON i.inhparent = c.oid" in sql
    assert "WHERE c.relkind = 'r' AND k.relpersistence <> 't'" in sql
    assert "relhassubclass" not in sql
    for word in ("INSERT", "UPDATE", "DELETE", "LOCK", "CREATE", "ALTER"):
        assert word not in sql.upper().replace("PG_CATALOG", ""), word
    conn = _InheritConn([("app.par", 4, ["app.k1", "app.k2", "app.k3"])])
    assert read_inheritance_children(conn, ["app.par", "app.t", "app.par"]) == (
        ("app.par", 4, ("app.k1", "app.k2", "app.k3")),)
    assert conn.calls[0][1] == {"names": ["app.par", "app.t"]}
    idle = _InheritConn()
    assert read_inheritance_children(idle, []) == () and idle.calls == []  # no statement
    # Unreadable: None, which refuses a Start that snapshots (round 3's second repair: fail
    # closed -- nothing else stops it).
    objects = _objects()

    class _Broken:
        def execute(self, *_a, **_k):
            raise RuntimeError("permission denied")

    assert with_inheritance_children(_Broken(), objects, ["app.par"]).inheritance_children is None
    assert with_inheritance_children(conn, objects, ["app.par"]).inheritance_children == (
        ("app.par", 4, ("app.k1", "app.k2", "app.k3")),)


def test_a_cdc_only_start_capturing_an_inheritance_parent_is_refused(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the dialog showed no block, and the worker created the connectors
    # -- whose snapshot copied the children's rows into the parent's table.
    from tests.test_g25_cdc_ui import _open_pg_start_dialog, _pg_start_state, _with_job_handle
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    conn = _pg_source(monkeypatch, children=[("orders", 1, ["orders_2026"])])
    state = _pg_start_state(MigrationType.CDC_ONLY, committed=False)
    verdict = _gate(state)
    assert verdict is not None and verdict[0] == _INHERIT
    assert conn.calls and conn.calls[0][1] == {"names": ["orders"]}
    # The Start dialog: blocked, Start disabled, and no re-snapshot offered (it IS one).
    ui, bodies = _open_pg_start_dialog(monkeypatch, _pg_start_state(MigrationType.CDC_ONLY,
                                                                    committed=False))
    assert ("error", _INHERIT) in ui.notices
    assert "Re-snapshot every table instead" not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    assert bodies[_INHERIT] == verdict[1]
    # The worker's backstop (Retry CDC, a stale dialog): refused before anything is created.
    _pg_source(monkeypatch, children=[("orders", 1, ["orders_2026"])])
    _with_job_handle(monkeypatch)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, job_watermark=None, committed=False,
        source_password="pw", migration_type=MigrationType.CDC_ONLY, inventory=_inventory(),
        catch=True, state=None,
    )
    assert run["submitted"] and run["kwargs"] == {}, "no connector may be created"
    assert str(run["error"]).splitlines()[0].startswith(f"Start CDC blocked: {_INHERIT}.")
    _job, tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    assert (tone, header) == ("error", _INHERIT) and "orders_2026" in body
    # A table without children is not refused (the read answers nothing for it).
    _pg_source(monkeypatch, children=[])
    assert _gate(_pg_start_state(MigrationType.CDC_ONLY, committed=False)) is None


def test_the_gapless_start_asks_nothing_about_children(monkeypatch) -> None:
    # Full load + CDC from its slot (snapshot.mode=never): the Full Load read the parent FROM
    # ONLY and nothing snapshots, so the gate neither reads children nor blocks on them.
    from tests.test_g25_cdc_ui import _RESUMED_WM, _pg_start_state

    conn = _pg_source(monkeypatch, children=[("orders", 1, ["orders_2026"])])
    state = _pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=False)
    assert _gate(state, _RESUMED_WM) is None
    assert conn.calls == []


def test_on_a_resumed_stack_an_inheritance_block_keeps_its_own_fix(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (the block did not exist; the routes would follow any block): the
    # gapless routes end in "its first Start CDC can re-snapshot every selected table" -- the
    # very snapshot this block refuses. G25 round 5: its own fix (a Full load + CDC run) stays,
    # and on a stack started before it is told to get new topics first, as on a rolled-back one.
    from tests.test_g25_cdc_ui import _pg_start_state

    _pg_source(monkeypatch, children=[("orders", 1, ["orders_2026"])])
    state = _pg_start_state(MigrationType.CDC_ONLY, committed=True)
    verdict = _gate(state)
    assert verdict is not None and verdict[0] == _INHERIT
    assert "a re-snapshot cannot be relied on here" not in verdict[1]
    assert "re-snapshot every selected table" not in verdict[1]
    assert "each child table migrates as a table of its own. This stack's connectors were " \
        "started before" in verdict[1]
    assert verdict[1].endswith(
        "So before that Full Load, use Delete CDC infrastructure and deploy it again (the new "
        "stack starts with empty topics).")


# ---------------------------------------------------------------------------
# A narrowed publish list names its real fix, alone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("committed", [False, True], ids=["never-started", "resumed-stack"])
def test_the_publish_list_block_names_alter_publication_alone(monkeypatch, committed) -> None:
    # FAILS WITHOUT THE FIX: "Recreate it with all three, or let this run re-snapshot and
    # create its own" -- Debezium's filtered mode reuses an existing publication and keeps its
    # publish list -- and on a stack started before, routes that change it no more.
    # G25 round 6 (decision C): the ALTER is the FIRST step, never "then Start CDC again" --
    # pgoutput decides what to publish from the publication as it stood at each change's
    # commit, so the narrowed window is not recovered by setting the list back; the route rule
    # names the way out after it (the never-started route is added by the dialog and the job,
    # test_g25_pg_r6.py; a stack started before gets new topics here).
    from tests.test_g25_cdc_ui import _RESUMED_WM, _pg_start_state

    _pg_source(monkeypatch, publishes_all=False)
    state = _pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=committed)
    verdict = _gate(state, _RESUMED_WM)
    assert verdict is not None and verdict[0] == _PUBLISH
    body = verdict[1]
    assert "let this run re-snapshot" not in body
    assert "A re-snapshot and a Full Load re-run both reuse it as it is." in body
    assert (
        f"As its owner, run {_ALTER.format(pub='dsqlmig_pub_s1')} on the source first. Setting "
        "the list back does not bring back the changes committed while it was narrowed"
    ) in body
    assert "then Start CDC again" not in body
    if committed:
        assert "After that fix, the gapless route: use Delete CDC infrastructure" in body
    else:
        assert "a re-snapshot cannot be relied on here" not in body
        assert body.endswith("so a Start that continues from the slot would still skip them.")


def test_the_start_dialog_offers_no_re_snapshot_for_a_narrowed_publish_list(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the never-started stack's dialog offered "Re-snapshot every table
    # instead", which unlocked Start -- and the worker then refused the same publication.
    from tests.test_g25_cdc_ui import _open_pg_start_dialog, _pg_start_state

    _pg_source(monkeypatch, publishes_all=False)
    ui, bodies = _open_pg_start_dialog(
        monkeypatch, _pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=False))
    assert ("error", _PUBLISH) in ui.notices
    assert "Re-snapshot every table instead" not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    assert "ALTER PUBLICATION" in bodies[_PUBLISH]


def test_the_prerequisite_names_the_same_publish_list_fix() -> None:
    # FAILS WITHOUT THE FIX: "Recreate the publication with all three operations, or choose
    # the re-snapshot option on the CDC start card" -- beside a row a re-snapshot does not
    # re-grade, because it does not repair it.
    from dsql_migrator.core.models import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects
    from tests.test_prerequisites import _pg_facts, _tabs

    for resnapshots in (False, True):
        row = check_cdc_replication_objects(
            _pg_facts(publication_publishes_all_dml=False), _tabs("app.orders"),
            provisions_replication=False, cdc_start_resnapshots=resnapshots)
        assert row.status is PrerequisiteStatus.FAIL
        assert "re-snapshot option" not in row.remediation
        assert row.remediation.startswith("As its owner, run ALTER PUBLICATION \"")
        assert "SET (publish = 'insert, update, delete') on the source" in row.remediation


# ---------------------------------------------------------------------------
# PostgreSQL 18 re-keyed table: the CDC monitor's fix for the migration type
# ---------------------------------------------------------------------------


def _refused_probe(tables, *, refused, migration_type=None):
    from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead
    from dsql_migrator.ui.cdc_replica_identity_gate import CdcReplicaIdentityProbe

    started = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    return CdcReplicaIdentityProbe(
        started_at=started, finished_at=started + timedelta(seconds=1), ok=True,
        scope={t: ("tenant", "id") for t in tables},
        reads={t: RekeyedIdentityRead("d", ("id",), 0, (), t in refused) for t in tables},
        migration_type=migration_type,
    )


@pytest.mark.parametrize("mtype", [MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC])
def test_the_monitor_leads_with_its_own_fix_when_full_is_no_fix(mtype) -> None:
    # FAILS WITHOUT THE FIX: the notice opened with "Run ALTER TABLE <table> REPLICA IDENTITY
    # FULL ... Cut over is held until REPLICA IDENTITY is FULL again" for the one table FULL
    # breaks, and told CDC only to "reload it" -- with no Full Load step to reload from.
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        monitor_weak_notice,
    )

    probe = _refused_probe(["app.orders"], refused={"app.orders"}, migration_type=mtype)
    findings, _t = fold_replica_identity_probe({}, probe)
    _header, body = monitor_weak_notice(findings, has_password=True, last_probe=probe)
    assert "REPLICA IDENTITY FULL on the source now" not in body
    assert "until REPLICA IDENTITY is FULL again" not in body
    assert "FULL is no fix here: on PostgreSQL 18 its generated columns" in body
    if mtype is MigrationType.CDC_ONLY:
        assert ('so use Start over to migrate it again with "Keep source PK" chosen in Schema '
                "Conversion.") in body
        assert "reload" not in body
    else:
        assert 'so set it back to "Keep source PK" in Schema Conversion and reload it.' in body


def test_a_mixed_monitor_notice_keeps_full_for_the_others() -> None:
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        fold_replica_identity_probe,
        monitor_weak_notice,
    )

    probe = _refused_probe(["app.a", "app.b"], refused={"app.b"},
                           migration_type=MigrationType.CDC_ONLY)
    findings, _t = fold_replica_identity_probe({}, probe)
    _header, body = monitor_weak_notice(findings, has_password=True, last_probe=probe)
    assert "Run ALTER TABLE <table> REPLICA IDENTITY FULL on the source now" in body
    assert body.count("Not for app.b: on PostgreSQL 18 its generated columns") == 1
    assert ('so use Start over to migrate it again with "Keep source PK" chosen in Schema '
            "Conversion instead.") in body


def test_a_landed_read_carries_its_migration_type_into_the_activity_line(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the FAILURE line told a CDC-only migration "Fix: set it back to
    # "Keep source PK" in Schema Conversion and reload it".
    import dsql_migrator.core.activity_log as act
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.ui.cdc_replica_identity_gate import _probe_and_land
    from dsql_migrator.ui.data_migration import DataMigrationState

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    lines: list = []
    monkeypatch.setattr(act, "log_activity", lambda *_a, **kw: lines.append(kw["detail"]))
    monkeypatch.setattr(status, "read_cdc_replica_identity_once",
                        lambda _s, scope: _refused_probe(list(scope), refused=set(scope)))
    state = DataMigrationState()
    state.migration_type = MigrationType.CDC_ONLY
    assert state.begin_cdc_replica_identity_probe()
    asyncio.run(_probe_and_land(SimpleNamespace(), state, {"app.orders": ("tenant", "id")}))
    assert state.cdc_replica_identity_last_probe.migration_type is MigrationType.CDC_ONLY
    (line,) = lines
    assert ('Fix: use Start over to migrate it again with "Keep source PK" chosen in Schema '
            "Conversion -- not FULL") in line
    assert "reload" not in line


@pytest.mark.parametrize("mtype", [MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC])
def test_cut_overs_notice_names_the_same_fix(mtype) -> None:
    # FAILS WITHOUT THE FIX: Cut over's hold told every open table "Run ALTER TABLE <table>
    # REPLICA IDENTITY FULL on the source" -- the one fix PostgreSQL 18 refuses there.
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        ReplicaIdentityGate,
        cutover_weak_notice,
        fold_replica_identity_probe,
        _weak_issues,
    )

    def _notice(tables, refused):
        probe = _refused_probe(tables, refused=refused, migration_type=mtype)
        findings, _t = fold_replica_identity_probe({}, probe)
        gate = ReplicaIdentityGate(weak=tuple(_weak_issues(findings)), last_probe=probe,
                                   migration_type=mtype, has_source_connection=True)
        return cutover_weak_notice(gate)[1]

    alone = _notice(["app.orders"], {"app.orders"})
    assert "Run ALTER TABLE <table> REPLICA IDENTITY FULL" not in alone
    assert "FULL is no fix here: on PostgreSQL 18 its generated columns" in alone
    start_over = 'use Start over to migrate it again with "Keep source PK"'
    assert (start_over in alone) is (mtype is MigrationType.CDC_ONLY)
    mixed = _notice(["app.a", "app.b"], {"app.b"})
    assert "Run ALTER TABLE <table> REPLICA IDENTITY FULL" in mixed
    assert "Not for app.b: on PostgreSQL 18 its generated columns" in mixed
