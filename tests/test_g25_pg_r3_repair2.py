# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 3, repair round 2: the PostgreSQL findings still open after two repair rounds.

* an inheritance parent was refused for its INHERITS children's REPLICA IDENTITY -- by the
  prerequisite (a required FAIL), the Full Load's provisioning, the one-table ADD and the
  Retry guard -- although every publication the tool writes names it ``ONLY``. A parent is
  now graded by its own identity alone; the one path that would publish the children, a
  Start that snapshots (Debezium's own publication has no ``ONLY``), now refuses when it
  cannot read which captured tables have any;
* nothing said before the load that a parent's child tables migrate only as tables of their
  own -- the prerequisite now does (and, for a Start that snapshots, that Start CDC refuses
  the parent);
* the CDC monitor's source high-water read a parent's ``MAX(pk)`` over its children, so an
  in-sync parent showed "N behind (PK)" for the whole run;
* PostgreSQL 18's "FULL is no fix" advice came from the latest probe alone: a failed re-read
  or a restart turned it back into "Run ALTER TABLE <table> REPLICA IDENTITY FULL", and
  Validation's verdict never had it.

(The slot-position check across a restart -- remaining[5] -- is the restart tests of
``tests/test_g25_pg_r3.py``; round 4 made that check stateless, ``tests/test_g25_pg_r4.py``.)

Every test FAILS on the code before this repair. Fakes only: no real database (the catalog
reads were also run live on PostgreSQL 16, see the repair notes).
"""

from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    PrerequisiteCheckId,
    PrerequisiteStatus,
    SourceType,
    TableDef,
)
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_g25_pg_publication import _Conn, _objects, _provision, _read, _writes

_UNREAD = "Whether a captured table has inheritance child tables could not be read"


def _table(name: str) -> TableDef:
    return TableDef(name=name, columns=[ColumnDef(name="id", mysql_type="integer")],
                    primary_key=["id"])


# ---------------------------------------------------------------------------
# A parent is graded by its own identity: its children are never published with it
# ---------------------------------------------------------------------------


def test_a_parent_whose_children_have_no_identity_is_published_alone(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: refused before any write ("1 of its child tables has none ...
    # set REPLICA IDENTITY FULL on each child table") -- a required prerequisite FAIL and a
    # Full Load refusal that protected nothing (the publication names it ONLY) and asked for
    # DDL on every child of a production source.
    from dsql_migrator.core.cdc_pg_slot import add_table_to_publication, replica_identity_problem
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect
    from tests.test_g17_full_load_and_cdc_additions import (
        _guard,
        _pg_migrator,
        _pg_watermark,
        _PgSource,
    )
    from tests.test_prerequisites import _G20ProbeConnection

    row = ("r", "d", True, 1, ["app.nokey"])  # what the descendant walk reported
    assert replica_identity_problem(_read(*row)) is None
    conn = _Conn(usability={"app.par": row})
    _provision(conn, ["app.par"])
    (ddl,) = [s for s in _writes(conn) if s.startswith("CREATE PUBLICATION")]
    assert 'FOR TABLE ONLY "APP"."PAR" WITH' in ddl
    added = _Conn(pubs={"p": ["app.a"]}, usability={"app.par": row})
    assert add_table_to_publication(added, name="p", table="app.par") is True
    source = _PgSource(usability={"app.c": row})
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": None}
    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        _G20ProbeConnection(usable={"app.par": row}), ["app.par"])
    result = check_replica_identity(
        _table("app.par"), replace(facts, replica_identity={"app.par": "d"}))
    assert result.status is PrerequisiteStatus.PASS


def test_the_identity_read_asks_nothing_about_inheritance_children() -> None:
    # FAILS WITHOUT THE FIX: a recursive walk of every plain table's INHERITS tree, and a
    # ninth column, on every prerequisite, provisioning and one-table ADD read.
    from dsql_migrator.core.cdc_pg_slot import (
        ReplicaIdentityUsability,
        _REPLICA_IDENTITY_USABLE_SQL,
        read_replica_identity_usability,
    )
    from tests.test_cdc_pg_slot import _IdentityConn

    flat = " ".join(_REPLICA_IDENTITY_USABLE_SQL.split())
    assert "pg_inherits" not in flat and "relhassubclass" not in flat
    assert flat.split(" FROM pg_catalog.pg_class c ")[0].endswith("AS key_generated")
    assert "children_full_refused" not in ReplicaIdentityUsability._fields
    conn = _IdentityConn([("app.t", "r", "d", True, 0, [], False, True, False)])
    assert read_replica_identity_usability(conn, ["app.t"])["app.t"] == (
        ReplicaIdentityUsability("r", "d", True, 0, (), False, True, False))


# ---------------------------------------------------------------------------
# A Start that snapshots never goes ahead without the inheritance read (fail closed)
# ---------------------------------------------------------------------------


class _Broken:
    def execute(self, *_a, **_k):
        raise RuntimeError("canceling statement due to statement timeout")


def test_a_snapshot_is_refused_when_the_inheritance_read_fails() -> None:
    # FAILS WITHOUT THE FIX: the failed read degraded to silence and the snapshot went ahead
    # -- copying a parent's child rows into it, and letting Debezium's own publication (no
    # ONLY) add child tables whose REPLICA IDENTITY nothing grades.
    from dsql_migrator.core.cdc_pg_slot import (
        BLOCKS_WITH_THEIR_OWN_FIX,
        INHERITANCE_UNREADABLE_HEADER,
        pg_replication_objects_blocker as blocker,
        with_inheritance_children,
    )

    assert INHERITANCE_UNREADABLE_HEADER == _UNREAD
    objects = with_inheritance_children(
        _Broken(), _objects(publication_tables=frozenset({"app.par"})), ["app.par"])
    assert objects.inheritance_children is None
    verdict = blocker(objects, ["app.par"], resumes_from_slot=False,
                      connector_autocreates_publication=True, snapshots=True)
    assert verdict is not None and verdict[0] == _UNREAD
    assert verdict[1].startswith("This Start sends snapshot.mode=initial.")
    assert verdict[1].endswith(
        "Check that the source is reachable from the tool, then Start CDC again.")
    # Its fix is to read again, on any stack: no gapless routes, no re-snapshot button.
    assert _UNREAD in BLOCKS_WITH_THEIR_OWN_FIX
    # A Start that does not snapshot never reads it, and is never refused for it.
    assert blocker(objects, ["app.par"], resumes_from_slot=False,
                   connector_autocreates_publication=True, snapshots=False) is None


def test_a_cdc_only_start_is_refused_when_the_inheritance_read_fails(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: no block in the dialog, and the worker created the connectors.
    from dsql_migrator.core import cdc_pg_slot as pg_slot
    from tests.test_g25_cdc_ui import _pg_start_state, _with_job_handle
    from tests.test_g25_pg_r3_repair import _gate, _pg_source
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    def _fail(*_a, **_k):
        raise RuntimeError("canceling statement due to statement timeout")

    _pg_source(monkeypatch)
    monkeypatch.setattr(pg_slot, "read_inheritance_children", _fail)
    verdict = _gate(_pg_start_state(MigrationType.CDC_ONLY, committed=False))
    assert verdict is not None and verdict[0] == _UNREAD
    resumed = _gate(_pg_start_state(MigrationType.CDC_ONLY, committed=True))
    assert resumed is not None and resumed[0] == _UNREAD
    assert "a re-snapshot cannot be relied on here" not in resumed[1]  # its own fix alone
    # The worker's backstop (Retry CDC, a stale dialog): refused before anything is created.
    _pg_source(monkeypatch)
    monkeypatch.setattr(pg_slot, "read_inheritance_children", _fail)
    _with_job_handle(monkeypatch)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, job_watermark=None, committed=False,
        source_password="pw", migration_type=MigrationType.CDC_ONLY, inventory=_inventory(),
        catch=True, state=None,
    )
    assert run["submitted"] and run["kwargs"] == {}, "no connector may be created"
    first = str(run["error"]).splitlines()[0]
    assert first.startswith(f"Start CDC blocked: {_UNREAD}.") and len(first) <= 300


def test_the_gapless_start_still_reads_nothing_about_children(monkeypatch) -> None:
    # Full load + CDC from its slot (snapshot.mode=never) never reads them, so a failing read
    # cannot refuse it.
    from dsql_migrator.core import cdc_pg_slot as pg_slot
    from tests.test_g25_cdc_ui import _RESUMED_WM, _pg_start_state
    from tests.test_g25_pg_r3_repair import _gate, _pg_source

    _pg_source(monkeypatch)
    monkeypatch.setattr(pg_slot, "read_inheritance_children",
                        lambda *_a, **_k: pytest.fail("a gapless Start read the children"))
    assert _gate(_pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=False),
                 _RESUMED_WM) is None


# ---------------------------------------------------------------------------
# The prerequisite says how a parent's child tables migrate -- before the load
# ---------------------------------------------------------------------------

_CHILDREN = (12, ("app.m_2026_01", "app.m_2026_02", "app.m_2026_03"))


@pytest.mark.parametrize("snapshots", [False, True], ids=["gapless", "snapshots"])
def test_the_prerequisite_names_a_parents_child_tables(snapshots) -> None:
    # FAILS WITHOUT THE FIX: no row at all -- a parent whose rows all live in unselected child
    # tables migrated empty and matched (0 = 0), and a CDC-only Start was the first to say
    # it cannot snapshot such a parent.
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_inheritance_children,
    )

    facts = PostgresCdcFacts(inheritance_children={"app.measurement": _CHILDREN})
    row = check_inheritance_children(_table("app.measurement"), facts, snapshots=snapshots)
    assert row is not None and row.check_id is PrerequisiteCheckId.TABLE_REPLICABLE
    assert row.target == "app.measurement"
    assert row.detail.startswith(
        "app.measurement has 12 inheritance (INHERITS) child tables: app.m_2026_01, "
        "app.m_2026_02, app.m_2026_03 ….")
    if snapshots:
        assert (row.status, row.required) == (PrerequisiteStatus.FAIL, True)
        assert "Start CDC refuses it" in row.detail
        assert row.remediation.startswith("Untick app.measurement (each child table migrates")
    else:
        assert (row.status, row.required) == (PrerequisiteStatus.WARN, False)
        assert "take only app.measurement's own rows (FROM ONLY)" in row.detail
        assert "only if it is selected" in row.detail
        assert row.remediation.startswith("Select each child table whose rows you need")
    # No children, or not read: no row (Start CDC reads it again, and fails closed).
    assert check_inheritance_children(_table("app.other"), facts, snapshots=snapshots) is None
    assert check_inheritance_children(
        _table("app.measurement"), PostgresCdcFacts(), snapshots=snapshots) is None


@pytest.mark.parametrize(
    "provisions, resnapshots, status",
    [(False, True, PrerequisiteStatus.FAIL),   # CDC only: its Start snapshots
     (True, True, PrerequisiteStatus.WARN),    # Full load + CDC before its load: provisions
     (False, False, PrerequisiteStatus.WARN)],  # Full load + CDC after it: resumes the slot
    ids=["cdc-only", "before-the-load", "after-the-load"],
)
def test_the_report_grades_the_row_by_whether_start_cdc_snapshots(
    provisions, resnapshots, status
) -> None:
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_postgres_cdc_prerequisites,
    )

    facts = PostgresCdcFacts(replica_identity={"app.measurement": "d"},
                             inheritance_children={"app.measurement": _CHILDREN})
    rows = [r for r in check_postgres_cdc_prerequisites(
        facts, [_table("app.measurement")], provisions_replication=provisions,
        cdc_start_resnapshots=resnapshots) if "child tables" in r.title]
    assert [r.status for r in rows] == [status]


def test_the_probe_reads_the_children_in_one_isolated_catalog_read() -> None:
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect
    from tests.test_prerequisites import _G20ProbeConnection, _G20Result

    class _Probe(_G20ProbeConnection):
        def __init__(self, *, fail=False):
            super().__init__(usable={"app.par": ("r", "d", True, 0, [])})
            self.inherit_fail, self.asked = fail, []

        def execute(self, statement, params=None):
            sql = str(statement)
            if "pg_catalog.pg_inherits" in sql:
                self.asked.append(sorted((params or {}).get("names") or ()))
                if self.inherit_fail:
                    self._aborted = True
                    raise RuntimeError("permission denied for pg_inherits")
                return _G20Result(sql, [("app.par", 2, ["app.k1", "app.k2"])])
            return super().execute(statement, params)

    probe = _Probe()
    facts = PostgresSourceDialect().probe_cdc_prerequisites(probe, ["app.par", "app.t"])
    assert probe.asked == [["app.par", "app.t"]]
    assert facts.inheritance_children == {"app.par": (2, ("app.k1", "app.k2"))}
    # A failure blanks only its own fact (rolled back), never "none".
    broken = _Probe(fail=True)
    facts = PostgresSourceDialect().probe_cdc_prerequisites(broken, ["app.par"])
    assert facts.inheritance_children is None and broken.rollbacks >= 1
    assert facts.replica_identity_problems == {"app.par": None}  # read before it, intact
    assert facts.wal_level == "logical"  # read after it, intact


# ---------------------------------------------------------------------------
# The CDC monitor's source high-water: a parent's own rows, as the Full Load copies them
# ---------------------------------------------------------------------------


class _MaxConn:
    """Answers the inheritance read with ``parent`` and every MAX with 7, recording SQL."""

    def __init__(self, parent):
        self.parent, self.sql = parent, []

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        self.sql.append(sql)
        if "AS inheritance_parent" in sql:
            return SimpleNamespace(scalar=lambda: self.parent)
        return SimpleNamespace(scalar=lambda: 7)


@pytest.mark.parametrize("parent", [True, False], ids=["inheritance-parent", "other"])
def test_the_source_high_water_reads_an_inheritance_parents_own_rows(parent) -> None:
    # FAILS WITHOUT THE FIX: SELECT MAX("id") FROM "app"."par" also maxed over its child
    # tables -- ids its Aurora DSQL table rightly lacks -- so an idle, caught-up parent read
    # "N behind (PK)" / "replicating…" for the whole CDC run.
    from dsql_migrator.core.source_dialect import dialect_for
    from dsql_migrator.core.watermark import max_pk_source

    conn = _MaxConn(parent)
    assert max_pk_source(conn, {"app.par": "id"}, dialect_for(SourceType.POSTGRES)) == {
        "app.par": 7}
    (statement,) = [s for s in conn.sql if "MAX(" in s]
    assert statement == f'SELECT MAX("id") FROM {"ONLY " if parent else ""}"app"."par"'


def test_mysql_and_an_unreadable_catalog_keep_their_high_water_reads() -> None:
    from dsql_migrator.core.source_dialect import dialect_for
    from dsql_migrator.core.watermark import max_pk_source

    conn = _MaxConn(True)
    assert max_pk_source(conn, {"app.par": "id"}, dialect_for(SourceType.MYSQL)) == {
        "app.par": 7}
    assert conn.sql == ["SELECT MAX(`id`) FROM `app`.`par`"]  # no catalog read, no ONLY

    class _Broken(_MaxConn):
        def execute(self, statement, params=None):
            if "AS inheritance_parent" in str(statement):
                raise RuntimeError("catalog unreadable")
            return super().execute(statement, params)

    # Unknown, never a high-water over the child tables.
    assert max_pk_source(_Broken(None), {"app.par": "id"},
                         dialect_for(SourceType.POSTGRES)) == {"app.par": None}


# ---------------------------------------------------------------------------
# PostgreSQL 18: "FULL is no fix" survives a failed re-read, and a restart is qualified
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
_NO_FIX = "FULL is no fix here"
_FULL = "Run ALTER TABLE <table> REPLICA IDENTITY FULL on the source"
_UNKNOWN = "Until the tool reads the source again it cannot tell whether FULL is a fix here"


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows)

    def fetchall(self):
        return list(self._rows)


class _IdentityEngine:
    def __init__(self, *, rows=None, fail=None):
        self.rows, self.fail = rows, fail

    def connect(self):
        if self.fail is not None:
            raise self.fail
        rows = self.rows

        class _Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_a):
                return False

            def execute(self, *_a, **_k):
                return _Rows(rows)

        return _Connection()

    def dispose(self):
        pass


def _ri_session_and_state(monkeypatch, engine):
    import dsql_migrator.core.activity_log as act
    import dsql_migrator.ui.data_migration._cdc_status as status
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.session import SessionConnectionState

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fake = types.ModuleType("nicegui")
    fake.run = _Run  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(act, "log_activity", lambda *_a, **_k: None)
    monkeypatch.setattr(status, "make_source_engine_factory",
                        lambda _pw, read_timeout_seconds=None: (lambda _cfg: engine["e"]))
    session = SessionConnectionState()
    session.set_source(
        SourceConnectionConfig(source_type=SourceType.POSTGRES, host="db.example.internal",
                               port=5432, database="app", username="u"),
        SecretValue("pw"),
    )
    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    state.cdc_deployed_message_key_columns = {"app.orders": ["tenant", "id"]}
    state.set_cdc_ops_window_start(_T0)
    return session, state


def _land(session, state):
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        _probe_and_land,
        replica_identity_read_scope,
    )

    scope = replica_identity_read_scope(state, session)
    assert state.begin_cdc_replica_identity_probe()
    asyncio.run(_probe_and_land(session, state, scope))


def _bodies(state, session):
    """The monitor's and Cut over's notices, with the inputs their renderers pass."""
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cutover_weak_notice,
        has_source_connection,
        monitor_weak_notice,
        replica_identity_gate_for,
    )
    from dsql_migrator.ui.validation import ValidationState

    monitor = monitor_weak_notice(
        dict(state.cdc_replica_identity_findings or {}),
        has_password=has_source_connection(session),
        last_probe=state.cdc_replica_identity_last_probe,
    )[1]
    gate = replica_identity_gate_for(state, ValidationState(), session, cdc_in_use=True,
                                     release="clean", summary=None)
    return monitor, cutover_weak_notice(gate)[1]


def test_a_failed_re_read_keeps_the_postgresql_18_fix(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the failed read replaced the probe that carried the refusal, so
    # both notices told the DBA to set FULL -- which fails every UPDATE and DELETE of the
    # published table on the source again.
    import sqlalchemy.exc

    engine = {"e": _IdentityEngine(rows=[("app.orders", "d", ["id"], 0, [], True)])}
    session, state = _ri_session_and_state(monkeypatch, engine)
    _land(session, state)
    for body in _bodies(state, session):
        assert _NO_FIX in body and _FULL not in body
    engine["e"] = _IdentityEngine(
        fail=sqlalchemy.exc.OperationalError("connect", {}, Exception("blip")))
    _land(session, state)
    assert state.cdc_replica_identity_last_probe.ok is False
    for body in _bodies(state, session):
        assert _NO_FIX in body and _FULL not in body
    # A later successful read decides again: the generated column is gone, FULL is the fix.
    engine["e"] = _IdentityEngine(rows=[("app.orders", "d", ["id"], 0, [], False)])
    _land(session, state)
    for body in _bodies(state, session):
        assert _NO_FIX not in body and _FULL in body and _UNKNOWN not in body


def test_after_a_restart_the_full_advice_says_it_is_not_known_yet(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: restored findings, no read yet -- both notices said "Run ALTER
    # TABLE <table> REPLICA IDENTITY FULL" with no word that PostgreSQL 18 refuses it there.
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )
    from dsql_migrator.ui.validation import ValidationState

    engine = {"e": _IdentityEngine(rows=[("app.orders", "d", ["id"], 0, [], True)])}
    session, state = _ri_session_and_state(monkeypatch, engine)
    _land(session, state)
    raw = capture_session_snapshot("s1", session, EvaluationState(), SchemaConversionState(),
                                   state, ValidationState()).model_dump_json()
    s2, m2 = SessionConnectionState(), DataMigrationState()
    apply_session_snapshot(SessionSnapshot.model_validate_json(raw), s2, EvaluationState(),
                           SchemaConversionState(), m2, ValidationState())
    assert m2.cdc_replica_identity_last_probe is None and m2.cdc_replica_identity_findings
    monitor, cutover = _bodies(m2, s2)
    for body in (monitor, cutover):
        assert _FULL in body and _UNKNOWN in body
        assert "on PostgreSQL 18 a published FULL table with generated columns fails every " \
               "UPDATE and DELETE on the source" in body
    # No read is known after a restart, so not its migration type either: a neutral fix.
    assert ('set it back to "Keep source PK" in Schema Conversion and migrate it again '
            "instead.") in monitor
    # Cut over knows the migration type.
    assert 'set it back to "Keep source PK" in Schema Conversion and reload it instead.' in \
        cutover


def test_validations_verdict_names_the_postgresql_18_fix_too() -> None:
    # FAILS WITHOUT THE FIX: "Run ALTER TABLE … REPLICA IDENTITY FULL" right after a read that
    # found PostgreSQL 18 refusing it.
    from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        CdcReplicaIdentityProbe,
        ReplicaIdentityGate,
        _weak_issues,
        fold_replica_identity_probe,
        replica_identity_verdict_hold,
    )

    def _verdict(refused):
        probe = CdcReplicaIdentityProbe(
            started_at=_T0, finished_at=_T0 + timedelta(seconds=1), ok=True,
            scope={"app.orders": ("tenant", "id")},
            reads={"app.orders": RekeyedIdentityRead("d", ("id",), 0, (), refused)},
            migration_type=MigrationType.FULL_LOAD_AND_CDC)
        findings, _t = fold_replica_identity_probe({}, probe)
        gate = ReplicaIdentityGate(applies=True, weak=tuple(_weak_issues(findings)),
                                   last_probe=probe,
                                   migration_type=MigrationType.FULL_LOAD_AND_CDC,
                                   has_source_connection=True)
        return replica_identity_verdict_hold(gate, total=3, as_of="now")

    tone, _header, body = _verdict(True)
    assert tone == "error"  # still held
    assert "REPLICA IDENTITY FULL on the source (on each partition too)" not in body
    assert ('FULL is no fix here: on PostgreSQL 18 its generated columns make a published '
            "REPLICA IDENTITY FULL fail every UPDATE and DELETE on the source, so set it back "
            'to "Keep source PK" in Schema Conversion and reload it') in body
    # PostgreSQL 13-17 (no refusal read): unchanged.
    _t2, _h2, plain = _verdict(False)
    assert "Run ALTER TABLE … REPLICA IDENTITY FULL on the source (on each partition too)" \
        in plain and _NO_FIX not in plain
