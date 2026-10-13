# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25: the PostgreSQL publication / REPLICA IDENTITY findings of the max-effort review.

* inheritance children were pulled into the publication with no identity check (FOR TABLE
  / ADD TABLE without ONLY) -- now each table is published ONLY (and, since round 3's
  second repair, the children are not graded: nothing the tool runs publishes them);
* PostgreSQL 18 refuses UPDATE/DELETE on a published table whose replica identity holds an
  unpublished generated column, and REPLICA IDENTITY FULL holds every column;
* a DEFERRABLE primary key was called "no valid primary key" (and "dropped");
* the prerequisite's remedy printed an unquoted ALTER TABLE, and gave every cause one fix;
* a reused publication at publish_via_partition_root = false got its slot before the flip;
* a same-named slot of another pipeline was dropped as "stale", and the Start check never
  compared the slot's position with the Full Load's start point;
* the one-table ADD's fail-closed lines were unbounded.

Every test FAILS on the pre-G25 code. Statement-dispatch fakes only: no real database.
(Each was also checked live on PostgreSQL 16; see the G25 notes.)
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import pytest

from tests.test_cdc_pg_slot import _FakeConn, _Result


def _norm(sql) -> str:
    return " ".join(str(sql).upper().split())


class _Conn(_FakeConn):
    """``_FakeConn`` plus what G25 reads and writes: a publication's
    ``publish_via_partition_root`` (``via_root``), a same-named slot's state
    (``slot_state``: ``(reusable, active, same_database, database)``), the confirmed position
    of the slot (``confirmed``), the relkind read of a failed repair (``partitioned``), and
    failures of the repair ALTER, of CREATE PUBLICATION and of ADD TABLE."""

    def __init__(self, *, via_root=True, slot_state=None, confirmed=None, partitioned=(),
                 alter_error=None, create_error=None, **kw):
        super().__init__(**kw)
        self.via_root = via_root
        self.slot_state = slot_state
        self.confirmed = confirmed
        self.partitioned = set(partitioned)
        self.alter_error = alter_error
        self.create_error = create_error

    def execute(self, statement, params=None):
        up = _norm(statement)
        params = params or {}
        if "AS REUSABLE" in up:
            self.statements.append((str(statement), params))
            if params.get("name") not in self._slots:
                return _Result([])
            return _Result([self.slot_state or (True, False, True, "app")])
        if "PUBALLTABLES" in up:
            self.statements.append((str(statement), params))
            name = params.get("name")
            if name not in self._pubs:
                return _Result([])
            return _Result([(name in self._all_tables, self.via_root)])
        if up.startswith("SELECT PUBVIAROOT"):
            self.statements.append((str(statement), params))
            return _Result([(self.via_root,)] if params.get("name") in self._pubs else [])
        if up.startswith("ALTER PUBLICATION") and up.endswith(
            "(PUBLISH_VIA_PARTITION_ROOT = TRUE)"
        ):
            self.statements.append((str(statement), params))
            if self.alter_error is not None:
                raise self.alter_error
            self.via_root = True
            return _Result([])
        if "C.RELKIND = 'P'" in up and "LIMIT 1" in up:
            self.statements.append((str(statement), params))
            return _Result([(1,)] if set(params.get("names") or ()) & self.partitioned else [])
        if "CONFIRMED_FLUSH_LSN" in up:
            self.statements.append((str(statement), params))
            return _Result([(self.confirmed,)])
        if up.startswith("CREATE PUBLICATION") and self.create_error is not None:
            self.statements.append((str(statement), params))
            # Another run with the same CDC stack name created it first.
            self._pubs.setdefault("dsqlmig_pub_s", ["app.orders"])
            raise self.create_error
        return super().execute(statement, params)


def _provision(conn, tables, *, full=(), logs=None):
    from dsql_migrator.core.cdc_pg_slot import provision_pg_replication

    return provision_pg_replication(
        conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s", tables=tables,
        full_identity_tables=list(full), on_log=(logs.append if logs is not None else None),
    )


def _read(kind="r", identity="d", usable=False, count=0, sample=(), **flags):
    from dsql_migrator.core.cdc_pg_slot import ReplicaIdentityUsability

    return ReplicaIdentityUsability(kind, identity, usable, count, tuple(sample), **flags)


def _writes(conn):
    return [_norm(sql) for sql, _ in conn.statements if _norm(sql).startswith(
        ("CREATE PUBLICATION", "DROP PUBLICATION", "ALTER PUBLICATION", "ALTER TABLE",
         "SELECT LSN FROM PG_CREATE", "SELECT PG_DROP"))]


class _Sqlstate(Exception):
    def __init__(self, code):
        super().__init__(f"sqlstate {code}")
        self.sqlstate = code


# ---------------------------------------------------------------------------
# 1. Inheritance children (confirmed[20])
# ---------------------------------------------------------------------------


def test_the_publication_names_each_table_with_only() -> None:
    # FAILS WITHOUT THE FIX: "FOR TABLE t" also published every inheritance child -- live
    # on PostgreSQL 16 a no-PK child then failed every UPDATE/DELETE, even through the parent.
    from dsql_migrator.core.cdc_pg_slot import create_publication

    conn = _Conn()
    assert create_publication(conn, name="dsqlmig_pub_s", tables=["app.a", "app.b"]) is True
    (ddl,) = [s for s in _writes(conn) if s.startswith("CREATE PUBLICATION")]
    assert 'FOR TABLE ONLY "APP"."A", ONLY "APP"."B" WITH' in ddl


def test_the_one_table_add_names_only_and_the_allowlist_requires_it() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        PgReplicationError,
        _assert_allowed,
        add_table_to_publication,
    )

    conn = _Conn(pubs={"p": ["app.a"]})
    assert add_table_to_publication(conn, name="p", table="app.c") is True
    assert conn.writes() == ['ALTER PUBLICATION "p" ADD TABLE ONLY "app"."c"']
    with pytest.raises(PgReplicationError):  # the shape that adds the children
        _assert_allowed('ALTER PUBLICATION "p" ADD TABLE "app"."c"')


def test_inheritance_descendants_are_not_graded_in_the_catalog_read() -> None:
    # Round 3, repair round 2: nothing the tool runs publishes a parent's INHERITS children
    # (FOR TABLE ONLY / ADD TABLE ONLY, and Start CDC refuses the snapshot that would let
    # Debezium's own publication add them), so their identity is not this read's question:
    # the leaves of a partitioned table only.
    from dsql_migrator.core.cdc_pg_slot import _REPLICA_IDENTITY_USABLE_SQL

    flat = " ".join(_REPLICA_IDENTITY_USABLE_SQL.split())
    for gone in ("relhassubclass", "pg_inherits", "WITH RECURSIVE", "children_full_refused"):
        assert gone not in flat, gone
    assert flat.count("FROM pg_catalog.pg_partition_tree(c.oid) t") == 2


def test_a_parent_whose_own_identity_is_usable_has_no_problem() -> None:
    from dsql_migrator.core.cdc_pg_slot import replica_identity_problem

    # A plain table is graded by its own identity alone, whatever its children.
    assert replica_identity_problem(_read("r", "d", True)) is None
    assert replica_identity_problem(_read("r", "f", True, full_refused=False)) is None
    assert replica_identity_problem(_read("r", "n", False)) == "REPLICA IDENTITY NOTHING"


def test_provisioning_publishes_a_parent_alone_whatever_its_children() -> None:
    conn = _Conn(usability={"app.parent": ("r", "d", True, 0, [])})
    handles = _provision(conn, ["app.parent"])
    assert handles.consistent_lsn
    (ddl,) = [s for s in _writes(conn) if s.startswith("CREATE PUBLICATION")]
    assert 'FOR TABLE ONLY "APP"."PARENT" WITH' in ddl


def test_the_prerequisite_passes_a_parent_whose_own_identity_is_usable() -> None:
    from dsql_migrator.core.models import PrerequisiteStatus, TableDef
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    from tests.test_prerequisites import _G20ProbeConnection

    connection = _G20ProbeConnection(usable={"app.parent": ("r", "d", True, 0, [])})
    facts = PostgresSourceDialect().probe_cdc_prerequisites(connection, ["app.parent"])
    # The fake answers only the usability read; the code read says DEFAULT.
    facts = replace(facts, replica_identity={"app.parent": "d"})
    result = check_replica_identity(TableDef(name="app.parent", primary_key=["id"]), facts)
    assert result.status is PrerequisiteStatus.PASS and result.required is True


# ---------------------------------------------------------------------------
# 2. PostgreSQL 18: generated columns in the replica identity (confirmed[21])
# ---------------------------------------------------------------------------


def test_the_postgresql_18_rule_is_in_the_shared_identity_predicate() -> None:
    from dsql_migrator.core.cdc_pg_slot import _REPLICA_IDENTITY_USABLE_SQL, _identity_usable_sql

    flat = " ".join(_identity_usable_sql("c").split())
    gate = "pg_catalog.current_setting('server_version_num')::int >= 180000"
    # FULL: any generated column; DEFAULT / USING INDEX: a generated KEY column (INCLUDE
    # columns are no part of a replica identity), each only from PostgreSQL 18.
    assert flat.startswith(f"(c.relreplident = 'f' AND NOT ({gate} AND EXISTS (")
    assert ("a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped "
            "AND a.attgenerated <> ''") in flat
    assert flat.count("a.attnum = ANY((i.indkey::int2[])[0:i.indnkeyatts - 1])") == 2
    usable = " ".join(_REPLICA_IDENTITY_USABLE_SQL.split())
    for column in ("AS pk_deferrable", "AS full_refused", "AS key_generated"):
        assert column in usable, column


@pytest.mark.parametrize(
    "fields, flags, cause, remedy",
    [
        (("r", "f", False), {"full_refused": True},
         "REPLICA IDENTITY FULL, but it has generated columns (PostgreSQL 18)",
         "give it a primary key and REPLICA IDENTITY DEFAULT"),
        (("r", "d", False), {"full_refused": True, "key_generated": True},
         "REPLICA IDENTITY DEFAULT, but its primary key has a generated column",
         "use a key with no generated column as its identity"),
        (("r", "i", False), {"full_refused": True, "key_generated": True},
         "REPLICA IDENTITY USING INDEX, but that index has a generated column",
         "use a key with no generated column as its identity"),
        (("r", "d", False), {"full_refused": True},
         "REPLICA IDENTITY DEFAULT, but no valid primary key",
         "give it a primary key that is not DEFERRABLE"),
        (("r", "n", False), {"full_refused": True}, "REPLICA IDENTITY NOTHING",
         "give it a primary key and REPLICA IDENTITY DEFAULT"),
        (("p", "d", True, 1, ["app.p_1"]), {"full_refused": True},
         "1 of its partitions has none: app.p_1",
         "give each partition a primary key (not DEFERRABLE) and REPLICA IDENTITY DEFAULT"),
    ],
    ids=["full", "pk-on-generated", "index-on-generated", "no-pk", "nothing", "partitions"],
)
def test_postgresql_18_generated_columns_get_their_cause_and_never_a_full_remedy(
    fields, flags, cause, remedy
) -> None:
    from dsql_migrator.core.cdc_pg_slot import replica_identity_problem, replica_identity_remedy

    read = _read(*fields, **flags)
    assert replica_identity_problem(read) == cause
    assert replica_identity_remedy(read) == remedy
    assert "FULL" not in replica_identity_remedy(read, table="app.t")


def test_a_partitioned_parents_own_full_stays_a_fix_on_postgresql_18() -> None:
    # PostgreSQL never checks a partitioned parent's own identity -- only its leaves'.
    from dsql_migrator.core.cdc_pg_slot import replica_identity_remedy

    assert replica_identity_remedy(_read("p", "n", False, full_refused=True)) == (
        "set REPLICA IDENTITY FULL on it, or DEFAULT with a primary key"
    )
    assert replica_identity_remedy(_read("p", "n", False, 2, ["a", "b"], full_refused=True)) == (
        "set REPLICA IDENTITY FULL on it and DEFAULT with a primary key on each partition"
    )


@pytest.mark.parametrize("member", [False, True], ids=["new", "already-published"])
def test_a_rekeyed_table_postgresql_18_would_refuse_under_full_is_never_widened(member) -> None:
    # FAILS WITHOUT THE FIX: graded as the FULL it gets, then ALTER TABLE ... REPLICA IDENTITY
    # FULL and CREATE PUBLICATION -- and on PostgreSQL 18 every UPDATE/DELETE on it fails.
    # A member of a reused publication is published already: the ALTER alone arms it.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _Conn(
        pubs={"dsqlmig_pub_s": ["app.orders"]} if member else None,
        usability={"app.orders": ("r", "d", True, 0, [], False, True, False)},
    )
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.orders"], full=["app.orders"], logs=logs)
    assert str(ei.value) == (
        "app.orders: needs REPLICA IDENTITY FULL, but has generated columns (PostgreSQL 18). "
        "Published for CDC, its UPDATE/DELETE would fail on the source. Nothing was changed "
        'there: set it back to "Keep source PK" in Schema Conversion, then re-run the Full '
        "Load."
    )
    assert ei.value.remedy == 'set it back to "Keep source PK" in Schema Conversion'
    assert _writes(conn) == []
    assert any("is re-keyed" in line for line in logs)
    # A partitioned re-keyed table is still widened: PostgreSQL never checks the parent.
    part = _Conn(usability={"app.p": ("p", "d", True, 0, [], False, True, False)})
    _provision(part, ["app.p"], full=["app.p"])
    assert any(s.startswith("ALTER TABLE") for s in _writes(part))


def test_the_rekey_prerequisite_offers_no_full_when_postgresql_18_refuses_it() -> None:
    from dsql_migrator.core.models import TableDef
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_replica_identity_covers_key,
    )

    table = TableDef(name="app.orders", primary_key=["id"])
    for leaves in ({}, {"app.orders": {"app.orders_1": "d"}}):
        facts = PostgresCdcFacts(
            replica_identity={"app.orders": "d"}, leaf_replica_identity=leaves,
            full_identity_refused={"app.orders": True},
        )
        result = check_replica_identity_covers_key(table, facts, ["tenant", "id"])
        assert result.remediation.startswith('Set app.orders back to "Keep source PK"')
        assert "REPLICA IDENTITY FULL on" not in result.remediation
    # PostgreSQL 13-17: the FULL remedy, now quoted.
    plain = check_replica_identity_covers_key(
        table, PostgresCdcFacts(replica_identity={"app.orders": "d"}), ["tenant", "id"])
    assert plain.remediation.startswith('Run ALTER TABLE "app"."orders" REPLICA IDENTITY FULL')


def test_the_probe_carries_full_refused_and_the_remedies() -> None:
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    from tests.test_prerequisites import _G20ProbeConnection

    connection = _G20ProbeConnection(usable={
        "app.g": ("r", "f", False, 0, [], False, True, False),
        "app.ok": ("r", "d", True, 0, [], False, True, False),
        "app.nopk": ("r", "d", False, 0, []),
    })
    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        connection, ["app.g", "app.ok", "app.nopk"])
    assert facts.full_identity_refused == {"app.g": True, "app.ok": True}
    assert facts.replica_identity_remedies == {
        "app.g": "give it a primary key and REPLICA IDENTITY DEFAULT",
        "app.nopk": 'give it a primary key that is not DEFERRABLE, or REPLICA IDENTITY FULL '
                    '(ALTER TABLE "app"."nopk" REPLICA IDENTITY FULL)',
    }
    failing = PostgresSourceDialect().probe_cdc_prerequisites(
        _G20ProbeConnection(fail=True), ["app.g"])
    assert failing.replica_identity_remedies is None and failing.full_identity_refused == {}


# ---------------------------------------------------------------------------
# 3. A DEFERRABLE primary key is named as such (confirmed[22])
# ---------------------------------------------------------------------------


def test_a_deferrable_primary_key_is_named_and_never_called_dropped() -> None:
    from dsql_migrator.core.cdc_pg_slot import replica_identity_problem, replica_identity_remedy
    from dsql_migrator.core.models import TableDef
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_replica_identity,
    )

    read = _read("r", "d", False, pk_deferrable=True)
    assert replica_identity_problem(read) == (
        "REPLICA IDENTITY DEFAULT, but its primary key is DEFERRABLE")
    assert replica_identity_remedy(read) == (
        "re-add the key NOT DEFERRABLE, or set REPLICA IDENTITY FULL")

    def detail(problem):
        return check_replica_identity(
            TableDef(name="app.t", primary_key=["id"]),
            PostgresCdcFacts(replica_identity={"app.t": "d"},
                             replica_identity_problems={"app.t": problem}),
        ).detail

    assert "dropped" not in detail(replica_identity_problem(read))
    assert "dropped" not in detail("1 of its partitions has none: app.t_1")
    # A key or index that is gone keeps the explanation.
    assert "dropped" in detail(replica_identity_problem(_read("r", "d", False)))
    assert "dropped" in detail(replica_identity_problem(_read("r", "i", False)))


def test_every_new_cause_still_fits_one_bounded_line() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        _REKEYED_FULL_REFUSED,
        _publish_refusal_message,
        replica_identity_problem,
    )

    name = "s" * 63 + "." + "t" * 63
    reads = [
        _read("r", "d", False, pk_deferrable=True),
        _read("r", "d", False, pk_deferrable=True, full_refused=True),
        _read("r", "f", False, full_refused=True),
        _read("r", "d", False, full_refused=True, key_generated=True),
        _read("r", "i", False, full_refused=True, key_generated=True),
        _read("p", "d", True, 999_999, ["a"] * 5, full_refused=True),
        _read("p", "n", False, 999_999, ["a"] * 5, full_refused=True),
    ]
    cases = [(replica_identity_problem(r), r) for r in reads]
    cases.append((_REKEYED_FULL_REFUSED, _read("r", "d", True, full_refused=True)))
    for problem, read in cases:
        msg = _publish_refusal_message([(name, problem, read)] * 100_000, ["app.gone"])
        assert len(msg) <= 300 and "\n" not in msg, (problem, len(msg))
        assert msg.endswith(", then re-run the Full Load."), problem


# ---------------------------------------------------------------------------
# 4/5. The prerequisite's remedy: per cause, quoted (confirmed[16], confirmed[23])
# ---------------------------------------------------------------------------


def test_the_prerequisite_uses_the_remedy_for_each_cause() -> None:
    # FAILS WITHOUT THE FIX: a partition whose USING INDEX index is gone was told to "give
    # app.t a valid primary key" -- which never changes a partition's relreplident.
    from dsql_migrator.core.models import PrerequisiteStatus, TableDef
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    from tests.test_prerequisites import _G20ProbeConnection

    connection = _G20ProbeConnection(usable={"app.t": ("p", "d", True, 1, ["app.t_1"])})
    facts = PostgresSourceDialect().probe_cdc_prerequisites(connection, ["app.t"])
    # The leaf's code alone ('i') passes; only the live grading sees its index is gone.
    facts = replace(facts, replica_identity={"app.t": "d"},
                    leaf_replica_identity={"app.t": {"app.t_1": "i"}})
    result = check_replica_identity(TableDef(name="app.t", primary_key=["id"]), facts)
    assert result.status is PrerequisiteStatus.FAIL
    assert result.remediation == (
        "To fix app.t, set REPLICA IDENTITY FULL on each partition, then re-run the "
        "prerequisite checks."
    )


def test_every_printed_statement_quotes_the_table_name() -> None:
    # FAILS WITHOUT THE FIX: "ALTER TABLE MixS.Orders ..." -- PostgreSQL folds it to
    # mixs.orders: 'schema "mixs" does not exist' (live, PostgreSQL 16).
    from dsql_migrator.core.cdc_pg_slot import replica_identity_remedy
    from dsql_migrator.core.models import TableDef
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_replica_identity,
        check_replica_identity_covers_key,
        check_table_replicable,
    )

    table = TableDef(name="MixS.Orders", primary_key=["id"])
    quoted = '"MixS"."Orders"'
    problem = "REPLICA IDENTITY DEFAULT, but no valid primary key"
    with_remedy = PostgresCdcFacts(
        replica_identity={"MixS.Orders": "d"},
        replica_identity_problems={"MixS.Orders": problem},
        replica_identity_remedies={
            "MixS.Orders": replica_identity_remedy(_read("r", "d", False), table="MixS.Orders")
        },
    )
    without = PostgresCdcFacts(replica_identity={"MixS.Orders": "d"},
                               replica_identity_problems={"MixS.Orders": problem})
    for facts in (with_remedy, without):
        remediation = check_replica_identity(table, facts).remediation
        assert f"ALTER TABLE {quoted} REPLICA IDENTITY FULL" in remediation, remediation
        assert "ALTER TABLE MixS.Orders" not in remediation
    nothing = check_replica_identity(
        table, PostgresCdcFacts(replica_identity={"MixS.Orders": "n"}))
    assert f"ALTER TABLE {quoted} REPLICA IDENTITY FULL" in nothing.remediation
    gone = check_replica_identity(table, PostgresCdcFacts(
        replica_identity={"MixS.Orders": "i"}, identity_index_valid={"MixS.Orders": False}))
    assert f"ALTER TABLE {quoted} REPLICA IDENTITY USING INDEX" in gone.remediation
    warn = check_replica_identity(table, PostgresCdcFacts(
        replica_identity={"MixS.Orders": "i"}, identity_index_valid={"MixS.Orders": True},
        identity_index_is_primary={"MixS.Orders": False}))
    assert f"Prefer ALTER TABLE {quoted} REPLICA IDENTITY DEFAULT" in warn.remediation
    covers = check_replica_identity_covers_key(
        table, PostgresCdcFacts(replica_identity={"MixS.Orders": "d"}), ["tenant", "id"])
    assert f"Run ALTER TABLE {quoted} REPLICA IDENTITY FULL" in covers.remediation
    unlogged = check_table_replicable(table, PostgresCdcFacts(unlogged={"MixS.Orders": True}))
    assert f"ALTER TABLE {quoted} SET LOGGED" in unlogged.remediation


def test_no_identity_remedy_offers_full_for_a_table_postgresql_18_refuses_it_on() -> None:
    from dsql_migrator.core.models import TableDef
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts, check_replica_identity

    table = TableDef(name="app.t", primary_key=["id"])
    refused = {"app.t": True}
    for facts in (
        PostgresCdcFacts(replica_identity={"app.t": "n"}, full_identity_refused=refused),
        PostgresCdcFacts(replica_identity={"app.t": "d"}, full_identity_refused=refused,
                         leaf_replica_identity={"app.t": {"app.t_1": "n"}}),
        PostgresCdcFacts(replica_identity={"app.t": "i"}, full_identity_refused=refused,
                         identity_index_valid={"app.t": False}),
        PostgresCdcFacts(replica_identity={"app.t": "i"}, full_identity_refused=refused,
                         identity_index_valid={"app.t": True},
                         identity_index_is_primary={"app.t": False}),
    ):
        remediation = check_replica_identity(table, facts).remediation
        assert "REPLICA IDENTITY FULL" not in remediation and "/ FULL" not in remediation
        assert " or FULL" not in remediation, remediation


# ---------------------------------------------------------------------------
# 6A. A reused publication is repaired BEFORE the slot (residuals[2] defect A)
# ---------------------------------------------------------------------------


def test_a_reused_publication_publishes_via_the_root_before_its_slot_exists() -> None:
    # FAILS WITHOUT THE FIX: the slot was created while the reused publication was still at
    # publish_via_partition_root = false, so every change to a partitioned table committed
    # during the Full Load decoded under its partition and was discarded (live, PG 16).
    conn = _Conn(pubs={"dsqlmig_pub_s": ["app.pt_1", "app.pt_2", "app.a"]}, via_root=False)
    roots = {"app.pt_1": "app.pt", "app.pt_2": "app.pt"}
    real = conn.execute

    def execute(statement, params=None):
        if "PG_PARTITION_ROOT" in _norm(statement):
            conn.statements.append((str(statement), params or {}))
            return _Result(list(roots.items()))
        return real(statement, params)

    conn.execute = execute  # type: ignore[assignment]
    logs: list[str] = []
    _provision(conn, ["app.pt", "app.a"], logs=logs)
    writes = _writes(conn)
    flip = writes.index(
        'ALTER PUBLICATION "DSQLMIG_PUB_S" SET (PUBLISH_VIA_PARTITION_ROOT = TRUE)')
    slot = next(i for i, s in enumerate(writes) if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in s)
    assert flip < slot
    assert not any(s.startswith(("CREATE PUBLICATION", "DROP PUBLICATION")) for s in writes)
    assert not any("ADD TABLE" in s for s in writes)  # the parent is a member, rolled up
    # The reuse line never calls a requested parent's leaves tables this run does not load.
    assert not any("app.pt_1" in line for line in logs), logs
    assert any(line.startswith("repaired publication") and "before this Full Load's "
               "replication slot is created" in line for line in logs)


def test_no_repair_write_when_the_setting_is_right_or_the_publication_is_new() -> None:
    for conn in (_Conn(pubs={"dsqlmig_pub_s": ["app.a"]}, via_root=True), _Conn()):
        _provision(conn, ["app.a"])
        assert not any("PUBLISH_VIA_PARTITION_ROOT = TRUE)" in s and s.startswith("ALTER")
                       for s in _writes(conn))


def test_a_failed_repair_refuses_a_partitioned_table_before_the_slot() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicationError

    conn = _Conn(pubs={"dsqlmig_pub_s": ["app.pt"]}, via_root=False, partitioned={"app.pt"},
                 alter_error=_Sqlstate("42501"))
    with pytest.raises(PgReplicationError) as ei:
        _provision(conn, ["app.pt"])
    msg = str(ei.value)
    assert msg.startswith(
        'Run ALTER PUBLICATION "dsqlmig_pub_s" SET (publish_via_partition_root = true) as its '
        "owner, then run the Full Load again")
    assert len(msg) <= 300
    assert not any("PG_CREATE_LOGICAL_REPLICATION_SLOT" in s for s in _writes(conn))
    # Nothing partitioned is captured through it: reused as is, the slot is created.
    plain = _Conn(pubs={"dsqlmig_pub_s": ["app.a"]}, via_root=False,
                  alter_error=_Sqlstate("42501"), lsn="5/5")
    logs: list[str] = []
    assert _provision(plain, ["app.a"], logs=logs).consistent_lsn == "5/5"
    assert any("keeps publish_via_partition_root = false" in line for line in logs)


# ---------------------------------------------------------------------------
# 6B(a). Another pipeline's same-named slot is refused, not dropped
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "state, reason",
    [
        ((False, True, True, "app"), "is in use (another CDC pipeline streams from it)"),
        ((False, False, False, "other"), "belongs to database other"),
        ((False, False, True, "app"), "is not a logical pgoutput slot"),
    ],
    ids=["active", "another-database", "another-plugin"],
)
def test_a_same_named_slot_of_another_pipeline_is_refused_before_any_write(state, reason) -> None:
    # FAILS WITHOUT THE FIX: dropped as "stale from a prior run" -- with the default CDC
    # stack name, another migration's slot (its changes since its own start point lost).
    from dsql_migrator.core.cdc_pg_slot import PgReplicationError

    conn = _Conn(slots={"dsqlmig_s"}, slot_state=state)
    logs: list[str] = []
    with pytest.raises(PgReplicationError) as ei:
        _provision(conn, ["app.a"], logs=logs)
    assert str(ei.value).startswith(f"Replication slot dsqlmig_s {reason}; it was not dropped.")
    assert "Nothing was changed on the source" in str(ei.value)
    assert len(str(ei.value)) <= 300
    assert _writes(conn) == []
    assert logs == [f"refused before any write: replication slot dsqlmig_s {reason}"]


def test_this_stacks_own_stale_slot_is_still_dropped_and_recreated() -> None:
    conn = _Conn(slots={"dsqlmig_s"}, slot_state=(True, False, True, "app"), lsn="6/6")
    assert _provision(conn, ["app.a"]).consistent_lsn == "6/6"
    writes = _writes(conn)
    drop = next(i for i, s in enumerate(writes) if s.startswith("SELECT PG_DROP"))
    create = next(i for i, s in enumerate(writes) if "PG_CREATE_LOGICAL" in s)
    assert drop < create


# ---------------------------------------------------------------------------
# 6B(b). The Start check compares the slot's position with the Full Load's start point
# ---------------------------------------------------------------------------


def _objects(confirmed="0/19C2CE0", **kw):
    from dsql_migrator.core.cdc_pg_slot import PgReplicationObjects

    base = dict(
        publication_name="p", slot_name="s", publication_present=True,
        publication_publishes_all_dml=True, publication_tables=frozenset({"app.a"}),
        slot_present_any_database=True, slot_usable=True, slot_confirmed_flush_lsn=confirmed,
    )
    base.update(kw)
    return PgReplicationObjects(**base)


def test_a_slot_moved_before_any_start_blocks_the_resume() -> None:
    # FAILS WITHOUT THE FIX: live, a second run with the same stack name re-created the
    # slot at a later LSN, A's Start check passed, and the change in between was gone.
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    # The stack records no Start of its connectors (round 4: the stack's record decides).
    kw = dict(resumes_from_slot=True, start_lsn="0/19C26D0", stack_started=False)
    moved = blocker(_objects(), ["app.a"], **kw)
    assert moved is not None
    assert moved[0] == "CDC's replication slot is not at this Full Load's start point"
    assert "0/19C2CE0" in moved[1] and "0/19C26D0" in moved[1]
    # Its way out is the route rule's (pg_cdc_route, G25 round 5), not the block's own.
    assert moved[1].endswith("silently skip the changes in between.")
    # Still at its start point: no block (same LSN, spelled differently).
    assert blocker(_objects(confirmed="0/19c26d0"), ["app.a"], **kw) is None


def test_a_restart_or_a_re_snapshot_is_never_judged_on_the_slot_position() -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    kw = dict(start_lsn="0/19C26D0")
    # The stack's connectors were started before: they move the slot legitimately.
    assert blocker(
        _objects(), ["app.a"], resumes_from_slot=True, stack_started=True, **kw) is None
    # Not a resume (snapshot.mode=initial): the position does not matter.
    assert blocker(_objects(), ["app.a"], resumes_from_slot=False, **kw) is None
    # Unknown facts never block: no position read, or an unparsable one.
    for objects in (_objects(confirmed=None), _objects(confirmed="1")):
        assert blocker(objects, ["app.a"], resumes_from_slot=True, **kw) is None
    # Without a start point (an older caller): unchanged.
    assert blocker(_objects(), ["app.a"], resumes_from_slot=True) is None


def test_the_objects_read_takes_the_usable_slots_confirmed_position() -> None:
    from dsql_migrator.core.cdc_pg_slot import read_pg_replication_objects

    from tests.test_cdc_pg_slot import _ObjectsConn

    class _Positioned(_ObjectsConn):
        def execute(self, statement, params=None):
            if "CONFIRMED_FLUSH_LSN" in str(statement).upper():
                self.statements.append(str(statement))
                return _Result([("0/ABC",)])
            return super().execute(statement, params)

    assert read_pg_replication_objects(
        _Positioned(), publication_name="p", slot_name="s").slot_confirmed_flush_lsn == "0/ABC"
    unusable = _Positioned(slot_ok=0)
    got = read_pg_replication_objects(unusable, publication_name="p", slot_name="s")
    assert got.slot_confirmed_flush_lsn is None
    assert not any("CONFIRMED_FLUSH_LSN" in s.upper() for s in unusable.statements)


def test_the_start_gate_passes_the_watermark_and_the_stack_record(monkeypatch) -> None:
    # The one call site (Start dialog, Deploy dialog and the worker backstop share it).
    from types import SimpleNamespace

    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui import connect
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _Engine:
        def connect(self):
            class _C:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return _C()

        def dispose(self):
            pass

    monkeypatch.setattr(connect, "make_source_engine_factory",
                        lambda *_a, **_k: (lambda _cfg: _Engine()))
    monkeypatch.setattr(cdc_pg_slot, "read_pg_replication_objects",
                        lambda *_a, **_k: _objects(publication_name="pub", slot_name="slot"))
    captured = datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
    watermark = SimpleNamespace(slot_name="slot", publication_name="pub", wal_lsn="0/19C26D0",
                                snapshot_timestamp=captured)
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES),
                              source_password=None)
    tables = [SimpleNamespace(name="app.a")]

    def probe(**record):
        state = SimpleNamespace(cdc_stack_name="dsql-cdc-stack", **record)
        return _cdc_ui._probe_pg_replication_objects(
            state, session, tables, watermark, snapshot_mode="never")

    refused = "CDC's replication slot is not at this Full Load's start point"
    assert probe()[0] == refused
    # Round 4: a Start window exempts nothing; what the stack records does -- started then
    # stopped (a committed offset), or connectors in place (Retry CDC).
    assert probe(cdc_ops_window_start=captured)[0] == refused
    assert probe(cdc_has_committed_offset=True) is None
    assert probe(cdc_stack_phase="running") is None


# ---------------------------------------------------------------------------
# Narrow races (residuals[2])
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", ["42710", "23505"])
def test_a_create_that_loses_the_race_reuses_the_publication(code) -> None:
    from dsql_migrator.core.cdc_pg_slot import create_publication

    conn = _Conn(create_error=_Sqlstate(code))
    assert create_publication(conn, name="dsqlmig_pub_s", tables=["app.orders"]) is False
    other = _Conn(create_error=_Sqlstate("42501"))
    with pytest.raises(_Sqlstate):
        create_publication(other, name="dsqlmig_pub_s", tables=["app.orders"])


def test_an_add_to_a_publication_dropped_meanwhile_is_publication_missing() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgPublicationMissingError, add_table_to_publication

    conn = _Conn(pubs={"p": ["app.a"]}, add_errors={"app.c": _Sqlstate("42704")})
    with pytest.raises(PgPublicationMissingError):
        add_table_to_publication(conn, name="p", table="app.c")


# ---------------------------------------------------------------------------
# residuals[3]: the one-table ADD's fail-closed lines are bounded
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("case", ["unreadable", "not-found"])
def test_the_one_table_adds_fail_closed_lines_fit_one_bounded_line(case) -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, add_table_to_publication

    table = "s" * 63 + "." + "t" * 63
    publication = "p" * 63

    long_class = type("OperationalError" + "X" * 80, (Exception,), {})
    conn = (_Conn(pubs={publication: []}, usability_error=long_class("x"))
            if case == "unreadable"
            else _Conn(pubs={publication: []}, usability_absent={table}))
    with pytest.raises(PgReplicaIdentityError) as ei:
        add_table_to_publication(conn, name=publication, table=table)
    msg = str(ei.value)
    assert ei.value.unreadable is True
    assert len(msg) <= 300 and "\n" not in msg
    assert msg.startswith("s" * 29 + "...: ")
    assert msg.endswith("then re-run the Full Load.")
    assert conn.writes() == []


def test_a_name_with_a_line_separator_is_shown_on_one_line() -> None:
    from dsql_migrator.core.cdc_pg_slot import _clip_table_name

    assert _clip_table_name("app.order\u2028s") == "app.order?s"
    assert _clip_table_name("app.a\nb\tc") == "app.a?b?c"
    assert _clip_table_name("app.orders") == "app.orders"
