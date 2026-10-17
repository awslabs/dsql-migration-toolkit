# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G31 -- PostgreSQL source objects Evaluation never read: row-level security and
standalone sequences (Evaluation review rules-postgres-5, rules-postgres-12,
introspection-1).

Row-level security: a tenant-isolation policy vanished on Aurora DSQL (which has no RLS)
with Evaluation reading "Ready", and a migration role the policies applied to copied and
validated only the rows they showed it. Now the policies grade the table UNSUPPORTED, every
source session reads with ``row_security = off`` (sent as a SET after connect) so such a
role fails loudly, and the Data Migration prerequisites say so before the load.

Standalone sequences: a sequence no column owns was never inventoried, so the application's
nextval() failed after cut over; now each is a MANUAL item carrying the exact Aurora DSQL
CREATE SEQUENCE. The non-key sequence advice ("add an identity") now says an identity needs
a bigint column.

No real PostgreSQL, Aurora DSQL or AWS is reached: catalog reads run over fakes.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.assessor import (
    PG_CATALOG_UNREAD_RULE_ID,
    PG_ROW_LEVEL_SECURITY_RULE_ID,
    PG_STANDALONE_SEQUENCE_RULE_ID,
    CompatibilityAssessor,
    Finding,
    ObjectKey,
    carry_probe_results,
    with_object_findings,
)
from dsql_migrator.core.assessor_postgres import (
    PgNonKeySequenceRule,
    catalog_unread_items,
    standalone_sequence_items,
    with_pg_catalog_facts,
)
from dsql_migrator.core.catalog_postgres import (
    PgCatalogFacts,
    RowSecurityFacts,
    RowSecurityPolicy,
    RowSecurityTable,
    SourceLogin,
    StandaloneSequence,
    row_security_exemption,
)
from dsql_migrator.core.converter import IDENTITY_FALLBACK_CAVEAT, SchemaConverter
from dsql_migrator.core.converter_postgres import (
    dsql_create_sequence_sql,
    pg_identity_fallback,
    pg_sequence_next_value,
)
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConnectionResult,
    ConversionNoteKind,
    EffortLevel,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteCheckRequest,
    PrerequisiteStatus,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    TargetInventory,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.postgres import (
    pg_read_row_security,
    pg_read_standalone_sequences,
)
from dsql_migrator.ui.evaluation import (
    EvaluationInputs,
    compute_migration_score,
    run_evaluation,
    with_refreshed_catalogs,
)

_BIGINT_MAX = 2**63 - 1


# ---------------------------------------------------------------------------
# Fakes and builders
# ---------------------------------------------------------------------------


class _Rows(list):
    """``Result.mappings()``: iterable rows plus ``first()``."""

    def first(self):  # noqa: ANN201
        return self[0] if self else None


class _Result:
    def __init__(self, rows: "list[dict]") -> None:
        self._rows = rows

    def mappings(self) -> _Rows:
        return _Rows(self._rows)

    def first(self):  # noqa: ANN201
        return self._rows[0] if self._rows else None


class _CatalogConnection:
    """A PostgreSQL-typed connection answering catalog reads by SQL substring.

    ``handlers`` maps a substring to ``rows`` or to an exception to raise; the first match
    wins and an unmatched statement returns no rows. Every executed statement and its bind
    parameters are recorded."""

    class _Dialect:
        name = "postgresql"

    dialect = _Dialect()

    def __init__(self, handlers: dict) -> None:
        self.handlers = handlers
        self.executed: list[tuple[str, dict]] = []

    def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        self.executed.append((sql, dict(parameters or {})))
        for needle, answer in self.handlers.items():
            if needle in sql:
                if isinstance(answer, BaseException):
                    raise answer
                return _Result(list(answer))
        return _Result([])


def _policy(name="tenant_isolation", command="ALL", using="(tenant_id = 1)", **kwargs):
    return RowSecurityPolicy(name=name, command=command, permissive=True,
                             roles=("app_user",), using=using, **kwargs)


def _rls_table(name="app.orders", *, owner="app_owner", owns=False, forced=False,
               policies=None) -> RowSecurityTable:
    return RowSecurityTable(
        table=name, owner=owner, login_owns=owns, forced=forced,
        policies=tuple(policies if policies is not None else [_policy()]),
    )


def _login(name="migrator", *, superuser=False, bypass=False) -> SourceLogin:
    return SourceLogin(name=name, superuser=superuser, bypass_rls=bypass)


def _sequence(name="app.invoice_no_seq", *, data_type="bigint", start=1, minimum=1,
              maximum=_BIGINT_MAX, increment=1, cycle=False, cache=1, last=1042,
              readable=True, called=None) -> StandaloneSequence:
    return StandaloneSequence(
        name=name, data_type=data_type, start_value=start, min_value=minimum,
        max_value=maximum, increment=increment, cycle=cycle, cache_size=cache,
        last_value=last, readable=readable,
        is_called=(last is not None) if called is None and last is not None else called,
    )


def _pg_table(name: str, *extra: ColumnDef) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False), *extra],
        primary_key=["id"],
    )


def _pg_assess(*tables: TableDef):
    return CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=list(tables))
    )


def _inputs(source_type: SourceType) -> EvaluationInputs:
    return EvaluationInputs(
        source_config=SourceConnectionConfig(
            host="db", database="appdb", source_type=source_type
        ),
        source_password=None,
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.example", region="us-east-1"
        ),
    )


class _Browser:
    def browse(self, _conn):  # noqa: ANN001, ANN201
        return TargetInventory()


# ---------------------------------------------------------------------------
# Catalog reads (source dialect)
# ---------------------------------------------------------------------------


def test_the_row_security_read_is_one_statement_when_no_table_has_it() -> None:
    conn = _CatalogConnection({"relrowsecurity": []})
    facts = pg_read_row_security(conn, ["app", "public"])
    assert facts == RowSecurityFacts(login=None)
    assert len(conn.executed) == 1
    sql, params = conn.executed[0]
    assert params == {"schemas": ["app", "public"]}
    # Only enabled RLS, plain + partitioned tables, never a partition or an extension's own.
    assert "c.relrowsecurity" in sql and "relkind IN ('r', 'p')" in sql
    assert "NOT c.relispartition" in sql
    assert "d.deptype = 'e'" in sql
    # Ownership is decided as PostgreSQL decides the owner exemption (inherited privileges).
    assert "pg_has_role(c.relowner, 'USAGE')" in sql


def test_the_row_security_read_collects_tables_policies_and_the_login() -> None:
    conn = _CatalogConnection({
        "relrowsecurity": [
            {"nsp": "app", "rel": "orders", "forced": False, "owner": "app_owner", "owned": False},
            {"nsp": "app", "rel": "audit", "forced": True, "owner": "migrator", "owned": True},
        ],
        "pg_roles": [{"login": "migrator", "superuser": False, "bypassrls": True}],
        "pg_policies": [
            {"nsp": "app", "rel": "orders", "name": "tenant_isolation", "permissive": "PERMISSIVE",
             "roles": ["app_user", "reporting"], "cmd": "ALL",
             "qual": "(tenant_id = 7)", "with_check": None},
            {"nsp": "app", "rel": "orders", "name": "no_archived", "permissive": "RESTRICTIVE",
             "roles": ["public"], "cmd": "SELECT", "qual": "(NOT archived)", "with_check": None},
            # A dormant policy on a table WITHOUT row-level security enabled is ignored.
            {"nsp": "app", "rel": "dormant", "name": "p", "permissive": "PERMISSIVE",
             "roles": ["public"], "cmd": "ALL", "qual": "true", "with_check": None},
        ],
    })
    facts = pg_read_row_security(conn, ["app"])
    # The policy read asks only for the tables with row-level security (deparsing a policy
    # locks its table, so a dormant one elsewhere could hold the read up).
    ((policy_sql, policy_params),) = [(q, b) for q, b in conn.executed if "pg_policies" in q]
    assert "tablename = ANY(:tables)" in policy_sql
    assert policy_params == {"schemas": ["app"], "tables": ["audit", "orders"]}
    assert facts.login == SourceLogin(name="migrator", superuser=False, bypass_rls=True)
    orders, audit = facts.tables
    assert (orders.table, orders.owner, orders.login_owns, orders.forced) == (
        "app.orders", "app_owner", False, False)
    assert [p.name for p in orders.policies] == ["tenant_isolation", "no_archived"]
    assert orders.policies[0].roles == ("app_user", "reporting")
    assert orders.policies[0].permissive and not orders.policies[1].permissive
    assert orders.policies[0].using == "(tenant_id = 7)"
    assert (audit.table, audit.forced, audit.login_owns, audit.policies) == (
        "app.audit", True, True, ())


def test_standalone_sequences_exclude_owned_identity_and_extension_sequences() -> None:
    conn = _CatalogConnection({"pg_sequences": [
        {"nsp": "app", "name": "invoice_no_seq", "data_type": "integer", "start_value": 1,
         "min_value": 1, "max_value": 2147483647, "increment_by": 1, "cycle": False,
         "cache_size": 1, "last_value": 1042, "readable": True},
        {"nsp": "app", "name": "hidden_seq", "data_type": "bigint", "start_value": 1,
         "min_value": 1, "max_value": _BIGINT_MAX, "increment_by": 1, "cycle": False,
         "cache_size": 1, "last_value": None, "readable": False},
        {"nsp": "app", "name": "reset_seq", "data_type": "bigint", "start_value": 1,
         "min_value": 1, "max_value": _BIGINT_MAX, "increment_by": 1, "cycle": False,
         "cache_size": 1, "last_value": None, "readable": True},
    ], 'FROM "app"."reset_seq"': [(5000, False)]})
    sequences = pg_read_standalone_sequences(conn, ["app"])
    # pg_sequences hides a reset sequence's value; its own row has it (RESTART WITH 5000).
    assert (sequences[2].last_value, sequences[2].is_called) == (5000, False)
    assert not any('"hidden_seq"' in sql for sql, _ in conn.executed)  # not readable: not read
    sql, params = conn.executed[0]
    assert params == {"schemas": ["app"]}
    # 'a' = a serial column / OWNED BY, 'i' = an identity column: governed by their column.
    assert "deptype IN ('a', 'i')" in sql
    assert "d.deptype = 'e'" in sql
    assert "has_sequence_privilege(c.oid, 'SELECT,USAGE')" in sql
    assert sequences[0] == _sequence(data_type="integer", maximum=2147483647)
    assert sequences[1].readable is False and sequences[1].last_value is None
    assert pg_read_standalone_sequences(conn, []) == ()


def test_one_failed_part_of_the_catalog_read_does_not_hide_the_other() -> None:
    class _LockNotAvailable(Exception):
        pass

    conn = _CatalogConnection({
        "NOT LIKE": [{"nspname": "app"}, {"nspname": "information_schema"}],
        "relrowsecurity": _LockNotAvailable("canceling statement due to lock timeout"),
        "pg_sequences": [],
    })
    facts = dialect_for(SourceType.POSTGRES).read_source_catalog(conn)
    assert facts.row_security is None
    assert facts.sequences == ()
    assert facts.read_errors == ("row-level security: _LockNotAvailable",)
    # Scoped to the schemas introspection reflects (the system schema dropped).
    assert all(p.get("schemas") == ["app"] for _sql, p in conn.executed if p)
    # MySQL has neither fact.
    assert dialect_for(SourceType.MYSQL).read_source_catalog(conn) is None
    assert dialect_for(SourceType.MYSQL).read_row_security(conn) is None


def test_the_rls_refusal_is_recognised_by_sqlstate_and_message() -> None:
    class _InsufficientPrivilege(Exception):
        sqlstate = "42501"

    pg = dialect_for(SourceType.POSTGRES)
    refusal = _InsufficientPrivilege(
        'query would be affected by row-level security policy for table "orders"'
    )
    assert pg.is_row_security_refusal(refusal)
    # Wrapped the way SQLAlchemy wraps the driver error (.orig).
    wrapper = RuntimeError("(psycopg.errors.InsufficientPrivilege) ...")
    wrapper.orig = refusal  # type: ignore[attr-defined]
    assert pg.is_row_security_refusal(wrapper)
    # The same SQLSTATE for an ordinary missing grant is not this refusal.
    assert not pg.is_row_security_refusal(
        _InsufficientPrivilege("permission denied for table orders")
    )
    assert not dialect_for(SourceType.MYSQL).is_row_security_refusal(refusal)


# ---------------------------------------------------------------------------
# row_security = off on every source session, as a SET after connect
# ---------------------------------------------------------------------------


def test_postgres_sessions_turn_row_security_off_with_a_set_not_a_startup_option() -> None:
    pg = dialect_for(SourceType.POSTGRES)
    assert pg.session_setup_sql == ("SET row_security = off",)
    assert dialect_for(SourceType.MYSQL).session_setup_sql == ()
    # Not a libpq ``options`` startup parameter, which RDS Proxy rejects.
    for timeout in (None, 300):
        options = pg.engine_kwargs(read_timeout_seconds=timeout)["connect_args"]["options"]
        assert "row_security" not in options


def test_the_session_setup_runs_on_every_new_connection_and_is_committed() -> None:
    from sqlalchemy import create_engine

    from dsql_migrator.core.introspector import install_session_setup

    engine = create_engine("sqlite+pysqlite:///:memory:")
    install_session_setup(engine, ("SET row_security = off",))
    (listener,) = [
        fn for fn in engine.pool.dispatch.connect if hasattr(fn, "session_setup_sql")
    ]
    calls: list = []

    class _Cursor:
        def execute(self, statement):  # noqa: ANN001, ANN201
            calls.append(("execute", statement))

        def close(self):  # noqa: ANN201
            calls.append(("close",))

    class _DbapiConnection:
        def cursor(self):  # noqa: ANN201
            return _Cursor()

        def commit(self):  # noqa: ANN201
            # Committed, or the pool's rollback-on-return would undo the SET.
            calls.append(("commit",))

    listener(_DbapiConnection(), None)
    assert calls == [("execute", "SET row_security = off"), ("close",), ("commit",)]
    # Nothing to set up: no listener at all.
    bare = create_engine("sqlite+pysqlite:///:memory:")
    install_session_setup(bare, ())
    assert not [fn for fn in bare.pool.dispatch.connect if hasattr(fn, "session_setup_sql")]
    engine.dispose()
    bare.dispose()


@pytest.mark.parametrize("factory_name", ["ui", "core"])
def test_every_source_engine_factory_installs_the_session_setup(factory_name: str) -> None:
    from dsql_migrator.core.introspector import _default_engine_factory
    from dsql_migrator.ui.connect import make_source_engine_factory

    factory = (
        make_source_engine_factory(None) if factory_name == "ui" else _default_engine_factory
    )

    def _installed(source_type: SourceType) -> list:
        engine = factory(
            SourceConnectionConfig(host="h", database="d", username="u", source_type=source_type)
        )
        try:
            return [
                fn.session_setup_sql
                for fn in engine.pool.dispatch.connect
                if hasattr(fn, "session_setup_sql")
            ]
        finally:
            engine.dispose()

    assert _installed(SourceType.POSTGRES) == [("SET row_security = off",)]
    assert _installed(SourceType.MYSQL) == []


# ---------------------------------------------------------------------------
# Who reads every row
# ---------------------------------------------------------------------------


def test_the_exemption_follows_postgresql_order() -> None:
    table = _rls_table(owner="app_owner")
    assert row_security_exemption(table, None) is None
    assert row_security_exemption(table, _login(superuser=True)) == "a superuser"
    assert row_security_exemption(table, _login(bypass=True)) == "a role with BYPASSRLS"
    assert row_security_exemption(table, _login()) == ""
    owned = _rls_table(owner="app_owner", owns=True)
    assert row_security_exemption(owned, _login(name="app_owner")) == "the table's owner"
    assert "owning role (app_owner)" in row_security_exemption(owned, _login())
    # FORCE ROW LEVEL SECURITY subjects the owner to the policies too.
    assert row_security_exemption(_rls_table(owns=True, forced=True), _login()) == ""
    assert row_security_exemption(
        _rls_table(owns=True, forced=True), _login(bypass=True)
    ) == "a role with BYPASSRLS"


# ---------------------------------------------------------------------------
# Evaluation grading
# ---------------------------------------------------------------------------


def test_an_rls_table_is_graded_unsupported_on_its_own_item_with_its_policy() -> None:
    report = _pg_assess(_pg_table("app.orders"), _pg_table("app.items"))
    facts = PgCatalogFacts(
        row_security=RowSecurityFacts(login=_login(), tables=(_rls_table(),)),
        sequences=(),
    )
    graded = with_pg_catalog_facts(report, facts)
    # Property 8: still exactly one item per table -- the finding grades the TABLE -- plus
    # the note that the probe ran.
    assert [i.object_name for i in graded.items] == [
        "app.orders", "app.items", "source catalog checks"
    ]
    orders, items, checked = graded.items
    assert checked.rule_id == "PG_CATALOG_CHECKED" and checked.classification is Classification.AUTO
    assert "1 table(s) with row-level security" in checked.concerns[0].risk
    assert orders.classification is Classification.UNSUPPORTED
    assert orders.rule_id == PG_ROW_LEVEL_SECURITY_RULE_ID
    assert orders.effort is EffortLevel.MEDIUM
    (concern,) = orders.concerns
    assert concern.note_kind is ConversionNoteKind.LOSS
    assert "tenant_isolation (FOR ALL TO app_user) USING ((tenant_id = 1))" in concern.risk
    assert "Aurora DSQL has no row-level security" in concern.risk
    # This login is subject to the policies: say the load stops, and why.
    assert "row_security = off" in concern.risk
    assert "not the table's owner, app_owner" in concern.risk
    assert "enforce this in the application" in concern.recommendation
    assert "BYPASSRLS, or the table's owner (app_owner)" in concern.recommendation
    assert "write side" in concern.recommendation  # FOR ALL also decides what is written
    assert items.classification is Classification.AUTO
    # A clean report reads "Ready"; this one does not.
    assert compute_migration_score(report).band == "Ready"
    assert compute_migration_score(graded).band == "Unsupported objects"


def test_an_exempt_login_and_a_default_deny_table_are_worded_for_what_happens() -> None:
    report = _pg_assess(_pg_table("app.orders"), _pg_table("app.vault"))
    facts = PgCatalogFacts(
        row_security=RowSecurityFacts(
            login=_login(bypass=True),
            tables=(_rls_table(), _rls_table("app.vault", policies=[], forced=True)),
        ),
        sequences=(),
    )
    orders, vault, _checked = with_pg_catalog_facts(report, facts).items
    assert "reads every row as a role with BYPASSRLS" in orders.concerns[0].risk
    assert "row_security = off" not in orders.concerns[0].risk
    assert "prerequisite" not in orders.concerns[0].recommendation
    risk = vault.concerns[0].risk
    assert "with no policy" in risk and "default deny" in risk
    assert "FORCED" in risk
    assert "restrict the grants" in vault.concerns[0].recommendation


def test_a_login_that_could_not_be_read_is_said_so() -> None:
    facts = PgCatalogFacts(
        row_security=RowSecurityFacts(login=None, tables=(_rls_table(),)), sequences=()
    )
    orders, _checked = with_pg_catalog_facts(_pg_assess(_pg_table("app.orders")), facts).items
    assert "could not be read" in orders.concerns[0].risk
    assert "prerequisite checks re-check" in orders.concerns[0].recommendation


def test_policy_text_is_bounded() -> None:
    long_expr = "(tenant_id = " + "1 + " * 200 + "1)"
    policies = [_policy(name=f"p{i}", using=long_expr) for i in range(8)]
    facts = PgCatalogFacts(
        row_security=RowSecurityFacts(login=_login(), tables=(_rls_table(policies=policies),)),
        sequences=(),
    )
    orders, _checked = with_pg_catalog_facts(_pg_assess(_pg_table("app.orders")), facts).items
    risk = orders.concerns[0].risk
    assert "with 8 policies" in risk
    assert "p5 (" in risk and "p6 (" not in risk
    assert "and 2 more policies" in risk
    assert "[truncated; the full expression is in pg_policies on the source]" in risk
    assert len(risk) < 4000


def test_a_standalone_sequence_becomes_a_manual_item_with_the_dsql_statement() -> None:
    (item,) = standalone_sequence_items([_sequence(data_type="integer", maximum=2147483647)])
    assert (item.kind, item.object_name, item.rule_id) == (
        "SEQUENCE", "app.invoice_no_seq", PG_STANDALONE_SEQUENCE_RULE_ID)
    assert item.classification is Classification.MANUAL
    assert item.effort is EffortLevel.SIMPLE
    assert len(item.concerns) == 1
    # Exact Aurora DSQL syntax: CACHE stated (1 or >= 65536), starting after the last value,
    # bigint-typed with the source's integer bound kept.
    assert (
        'CREATE SEQUENCE "app"."invoice_no_seq" CACHE 1 MAXVALUE 2147483647 START WITH 1043;'
        in item.recommendation
    )
    assert "Its last value on the source is 1042" in item.risk
    assert "always bigint" in item.risk
    assert "SELECT setval(\'\"app\".\"invoice_no_seq\"\', <that value>)" in item.recommendation
    assert "neither creates nor syncs it" in item.recommendation


def test_the_create_sequence_statement_follows_the_dsql_rules() -> None:
    # A source that caches values: the smallest cached size Aurora DSQL accepts.
    assert dsql_create_sequence_sql(_sequence(cache=20)) == (
        'CREATE SEQUENCE "app"."invoice_no_seq" CACHE 65536 START WITH 1043;'
    )
    # Nothing drawn since it was created or reset: its row's last_value IS the next value
    # -- after RESTART WITH 5000 that is 5000, never the start value.
    assert pg_sequence_next_value(_sequence(start=100, last=100, called=False)) == 100
    assert pg_sequence_next_value(_sequence(start=1, last=5000, called=False)) == 5000
    (reset,) = standalone_sequence_items([_sequence(start=1, last=5000, called=False)])
    assert "its next value is 5000" in reset.risk and "START WITH 5000;" in reset.recommendation
    # Its row could not be read (USAGE without SELECT): a placeholder, not the start value.
    assert pg_sequence_next_value(_sequence(start=100, last=None)) is None
    # Not readable: a placeholder that fails loudly rather than restarting at 1.
    unreadable = _sequence(last=None, readable=False)
    assert pg_sequence_next_value(unreadable) is None
    assert dsql_create_sequence_sql(unreadable).endswith("START WITH <next value>;")
    # A descending sequence keeps its own bounds only where they differ from bigint's.
    down = _sequence(increment=-1, minimum=-(2**63), maximum=-1, start=-1, last=-10)
    assert dsql_create_sequence_sql(down) == (
        'CREATE SEQUENCE "app"."invoice_no_seq" CACHE 1 INCREMENT BY -1 START WITH -11;'
    )
    # At its bound: CYCLE wraps, NO CYCLE is exhausted.
    at_max = dict(maximum=10, last=10)
    assert pg_sequence_next_value(_sequence(cycle=True, **at_max)) == 1
    assert pg_sequence_next_value(_sequence(**at_max)) is None
    assert "CYCLE" in dsql_create_sequence_sql(_sequence(cycle=True, **at_max))
    (exhausted,) = standalone_sequence_items([_sequence(**at_max)])
    assert "It is exhausted" in exhausted.risk


def test_an_unread_probe_part_is_reported_not_taken_as_clean() -> None:
    (both,) = catalog_unread_items(None)
    assert both.rule_id == PG_CATALOG_UNREAD_RULE_ID and both.kind == "DATABASE"
    assert "row-level security and sequences could not be read" in both.risk
    (one,) = catalog_unread_items(
        PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=None,
                       read_errors=("sequences: LockNotAvailable",))
    )
    assert "sequences could not be read (sequences: LockNotAvailable)" in one.risk
    assert "row-level security" not in one.risk.split("(")[0]
    assert catalog_unread_items(
        PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=())
    ) == []


# ---------------------------------------------------------------------------
# Re-grading and carrying the probe's findings over a re-assessment
# ---------------------------------------------------------------------------


def _rls_finding(name="app.orders") -> Finding:
    return Finding(
        object=ObjectKey("table", name),
        rule_id=PG_ROW_LEVEL_SECURITY_RULE_ID,
        classification=Classification.UNSUPPORTED,
        risk="r",
        recommendation="x",
        effort=EffortLevel.MEDIUM,
    )


def test_object_findings_regrade_idempotently_and_skip_unknown_objects() -> None:
    report = _pg_assess(
        _pg_table("app.orders", ColumnDef(name="amount", mysql_type="numeric")),
    )
    before = report.items[0]
    assert before.classification is Classification.MANUAL
    once = with_object_findings(report, [_rls_finding(), _rls_finding("app.ghost")])
    twice = with_object_findings(once, [_rls_finding()])
    for graded in (once, twice):
        (item,) = graded.items
        assert item.classification is Classification.UNSUPPORTED
        assert [c.rule_id for c in item.concerns] == [
            PG_ROW_LEVEL_SECURITY_RULE_ID, "PG_UNCONSTRAINED_NUMERIC"]
    assert twice.summary[Classification.UNSUPPORTED] == 1


def test_a_refresh_and_a_restore_keep_the_probe_findings() -> None:
    inventory = SourceInventory(tables=[_pg_table("app.orders"), _pg_table("app.items")])

    class _Introspector:
        def __init__(self) -> None:
            self.probed = 0

        def introspect(self, conn, *, progress=None):  # noqa: ANN001, ANN201
            return inventory

        def source_catalog(self, conn):  # noqa: ANN001, ANN201
            self.probed += 1
            return PgCatalogFacts(
                row_security=RowSecurityFacts(login=_login(), tables=(_rls_table(),)),
                sequences=(_sequence(),),
            )

    introspector = _Introspector()
    result = run_evaluation(
        _inputs(SourceType.POSTGRES),
        introspector_factory=lambda _p: introspector,
        target_browser_factory=lambda _p: _Browser(),
    )
    assert introspector.probed == 1
    by_name = {i.object_name: i for i in result.assessment.items}
    assert by_name["app.orders"].classification is Classification.UNSUPPORTED
    assert by_name["app.invoice_no_seq"].kind == "SEQUENCE"

    # A re-assessment from the inventory alone (catalog refresh, session restore).
    refreshed = with_refreshed_catalogs(
        result, inventory=inventory, target_inventory=TargetInventory(), target_endpoint=None,
        source_type=SourceType.POSTGRES,
    )
    assert [(i.object_name, i.rule_id) for i in refreshed.assessment.items] == [
        (i.object_name, i.rule_id) for i in result.assessment.items]
    assert refreshed.assessment.summary == result.assessment.summary

    # A table gone from the re-read inventory takes its concern with it.
    smaller = SourceInventory(tables=[_pg_table("app.items")])
    shrunk = with_refreshed_catalogs(
        result, inventory=smaller, target_inventory=TargetInventory(), target_endpoint=None,
    )
    names = {i.object_name: i for i in shrunk.assessment.items}
    assert "app.orders" not in names and "app.invoice_no_seq" in names
    assert names["app.items"].classification is Classification.AUTO

    # Not across an engine switch: they describe the other engine's source.
    switched = with_refreshed_catalogs(
        result, inventory=inventory, target_inventory=TargetInventory(), target_endpoint=None,
        source_type=SourceType.MYSQL,
    )
    assert not any(
        c.rule_id.startswith("PG_ROW_LEVEL") or i.kind == "SEQUENCE"
        for i in switched.assessment.items for c in i.concerns
    )


def test_carrying_over_is_pure_and_keeps_whole_probe_items_once() -> None:
    previous = with_pg_catalog_facts(
        _pg_assess(_pg_table("app.orders")),
        PgCatalogFacts(row_security=None, sequences=(_sequence(),)),
    )
    recomputed = _pg_assess(_pg_table("app.orders"))
    carried = carry_probe_results(previous, recomputed)
    assert [i.rule_id for i in carried.items].count(PG_STANDALONE_SEQUENCE_RULE_ID) == 1
    assert [i.rule_id for i in carried.items].count(PG_CATALOG_UNREAD_RULE_ID) == 1
    assert carried.items[0].classification is Classification.AUTO


def test_a_postgres_run_whose_probe_could_not_read_the_source_says_so() -> None:
    class _Introspector:
        def introspect(self, conn, *, progress=None):  # noqa: ANN001, ANN201
            return SourceInventory(tables=[_pg_table("app.orders")])

        def source_catalog(self, conn):  # noqa: ANN001, ANN201
            return None

    result = run_evaluation(
        _inputs(SourceType.POSTGRES),
        introspector_factory=lambda _p: _Introspector(),
        target_browser_factory=lambda _p: _Browser(),
    )
    assert any(i.rule_id == PG_CATALOG_UNREAD_RULE_ID for i in result.assessment.items)


def test_a_mysql_run_never_calls_the_postgres_probe() -> None:
    class _Introspector:
        def introspect(self, conn, *, progress=None):  # noqa: ANN001, ANN201
            return SourceInventory(tables=[_pg_table("shop.orders")])

        def source_catalog(self, conn):  # noqa: ANN001, ANN201
            raise AssertionError("a MySQL source has no row-level security to probe")

    result = run_evaluation(
        _inputs(SourceType.MYSQL),
        introspector_factory=lambda _p: _Introspector(),
        target_browser_factory=lambda _p: _Browser(),
    )
    assert not any(i.rule_id.startswith("PG_") for i in result.assessment.items)


def test_the_introspector_probe_reads_in_autocommit_with_a_lock_bound() -> None:
    from dsql_migrator.core.introspector import SourceIntrospector

    statements: list[str] = []

    class _Conn:
        dialect = SimpleNamespace(name="postgresql")

        def execution_options(self, **options):  # noqa: ANN003, ANN201
            assert options == {"isolation_level": "AUTOCOMMIT"}
            return self

        def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
            statements.append(str(statement))
            if "NOT LIKE" in str(statement):
                return _Result([{"nspname": "app"}])
            return _Result([])

        def __enter__(self):  # noqa: ANN204
            return self

        def __exit__(self, *_exc):  # noqa: ANN002, ANN204
            return False

    class _Engine:
        def connect(self):  # noqa: ANN201
            return _Conn()

        def dispose(self) -> None:
            pass

    facts = SourceIntrospector(engine_factory=lambda _c: _Engine()).source_catalog(
        SourceConnectionConfig(host="h", database="d", source_type=SourceType.POSTGRES)
    )
    assert facts == PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=())
    assert statements[0] == "SET lock_timeout = '5s'"

    def _broken(_c):  # noqa: ANN001, ANN202
        raise OSError("unreachable")

    assert SourceIntrospector(engine_factory=_broken).source_catalog(
        SourceConnectionConfig(host="h", database="d", source_type=SourceType.POSTGRES)
    ) is None


# ---------------------------------------------------------------------------
# The non-key sequence advice: an identity needs a bigint column
# ---------------------------------------------------------------------------


def test_the_identity_fallback_says_an_integer_column_must_be_declared_bigint_first() -> None:
    table = TableDef(
        name="shop.invoices",
        columns=[
            ColumnDef(name="id", mysql_type="uuid", nullable=False),
            ColumnDef(name="invoice_no", mysql_type="integer", nullable=False,
                      default="nextval('shop.invoices_invoice_no_seq'::regclass)"),
            ColumnDef(name="legacy_no", mysql_type="bigint", nullable=False, identity=True),
        ],
        primary_key=["id"],
    )
    narrow, wide = table.columns[1], table.columns[2]
    assert "'invoice_no' (integer) is not bigint" in pg_identity_fallback([narrow])
    assert "in the target DDL before applying" in pg_identity_fallback([narrow])
    assert "'legacy_no' can get one after apply" in pg_identity_fallback([wide])
    # ADD ... AS IDENTITY needs NOT NULL, which Aurora DSQL cannot add after apply.
    nullable = ColumnDef(name="opt_no", mysql_type="bigint", nullable=True,
                         default="nextval('shop.opt_seq'::regclass)")
    assert "'opt_no' is nullable" in pg_identity_fallback([nullable])
    assert "after apply (ALTER" not in pg_identity_fallback([nullable])
    for clause in (pg_identity_fallback([narrow]), pg_identity_fallback([wide])):
        assert "GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)" in clause
        assert "RESTART WITH <largest value + 1>" in clause

    # Evaluation and Schema Conversion give the same per-column instruction.
    (finding,) = PgNonKeySequenceRule().evaluate(SourceInventory(tables=[table]))
    assert pg_identity_fallback([narrow, wide]) in finding.recommendation
    assert "add an identity to the column on the target" not in finding.recommendation
    notes = {
        w.column_name: w.message
        for w in SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table).warnings
        if "takes its value from a sequence" in w.message
    }
    assert pg_identity_fallback([narrow]) in notes["invoice_no"]
    assert pg_identity_fallback([wide]) in notes["legacy_no"]
    assert "no source sequence to point at" not in notes["invoice_no"]


def test_the_composite_key_identity_fallback_carries_the_bigint_caveat() -> None:
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    table = TableDef(
        name="ecommerce.order_events",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False, identity=True),
            ColumnDef(name="occurred_at", mysql_type="timestamp", nullable=False),
        ],
        primary_key=["id", "occurred_at"],
        auto_increment_column="id",
    )
    for engine in (SourceType.MYSQL, SourceType.POSTGRES):
        report = CompatibilityAssessor(source_type=engine).assess(
            SourceInventory(tables=[table])
        )
        finding = " ".join(i.recommendation for i in report.items)
        conversion = " ".join(
            w.message
            for w in SchemaConverter(source_type=engine).convert_table(
                table, SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.KEEP_INTEGER)
            ).warnings
        )
        for text in (finding, conversion):
            assert IDENTITY_FALLBACK_CAVEAT in text, (engine, text)


# ---------------------------------------------------------------------------
# The Data Migration prerequisite
# ---------------------------------------------------------------------------


def test_the_row_security_prerequisite_grades_each_table() -> None:
    from dsql_migrator.core.prerequisites_postgres import check_row_security

    orders = TableDef(name="app.orders", primary_key=["id"])
    clean = TableDef(name="app.clean", primary_key=["id"])
    blocked = RowSecurityFacts(login=_login(), tables=(_rls_table(),))
    assert check_row_security(clean, blocked) is None

    fail = check_row_security(orders, blocked)
    assert (fail.check_id, fail.status, fail.required, fail.target) == (
        PrerequisiteCheckId.ROW_SECURITY, PrerequisiteStatus.FAIL, True, "app.orders")
    assert "row_security = off" in fail.detail
    assert "or its owner app_owner" in fail.remediation
    assert "Or untick app.orders" in fail.remediation

    forced = check_row_security(
        orders, RowSecurityFacts(login=_login(), tables=(_rls_table(owns=True, forced=True),))
    )
    assert forced.status is PrerequisiteStatus.FAIL
    assert "not the owner: FORCE ROW LEVEL SECURITY" in forced.remediation

    exempt = check_row_security(
        orders, RowSecurityFacts(login=_login(superuser=True), tables=(_rls_table(),))
    )
    assert exempt.status is PrerequisiteStatus.PASS and "as a superuser" in exempt.detail

    unknown = check_row_security(orders, RowSecurityFacts(login=None, tables=(_rls_table(),)))
    assert unknown.status is PrerequisiteStatus.INFO and unknown.required is False


class _Source:
    def __init__(self, row_security=None, *, raises: bool = False) -> None:
        self._facts = row_security
        self._raises = raises
        self.asked: list = []

    def reachable(self) -> ConnectionResult:
        return ConnectionResult(success=True)

    def grants(self) -> list[str]:
        return ["ALL PRIVILEGES"]

    def variables(self) -> dict:
        return {}

    def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):  # noqa: ANN001, ANN201
        return None

    def row_security(self, table_names):  # noqa: ANN001, ANN201
        self.asked.append(list(table_names))
        if self._raises:
            raise RuntimeError("lost")
        return self._facts


class _Target:
    def reachable(self) -> bool:
        return True

    def iam_auth(self) -> ConnectionResult:
        return ConnectionResult(success=True)

    def relation_exists(self, qualified_name: str) -> bool:
        return True

    def required_columns_without_default(self, qualified_name: str):  # noqa: ANN201
        return []


def _run_checks(source: _Source, source_type: SourceType, mode=MigrationMode.FULL_LOAD):
    from dsql_migrator.core.prerequisites import PrerequisiteChecker

    tables = [TableDef(name="app.orders", primary_key=["id"]),
              TableDef(name="app.items", primary_key=["id"])]
    return PrerequisiteChecker(source_probe=source, target_probe=_Target()).check(
        PrerequisiteCheckRequest(mode=mode, tables=[t.name for t in tables],
                                 source_type=source_type),
        tables=tables,
    )


def test_the_checker_blocks_a_load_the_policies_would_filter() -> None:
    source = _Source(RowSecurityFacts(login=_login(), tables=(_rls_table(),)))
    report = _run_checks(source, SourceType.POSTGRES)
    assert source.asked == [["app.orders", "app.items"]]
    (row,) = [r for r in report.results if r.check_id is PrerequisiteCheckId.ROW_SECURITY]
    assert row.target == "app.orders" and row.status is PrerequisiteStatus.FAIL
    assert report.can_proceed is False
    # CDC runs it too (its Full Load and Validation read the same way).
    cdc = _run_checks(source, SourceType.POSTGRES, MigrationMode.CDC)
    assert any(r.check_id is PrerequisiteCheckId.ROW_SECURITY for r in cdc.results)
    # An exempt login passes; a failed or absent read adds no row; MySQL is never asked.
    ok = _run_checks(
        _Source(RowSecurityFacts(login=_login(bypass=True), tables=(_rls_table(),))),
        SourceType.POSTGRES,
    )
    assert ok.can_proceed is True
    assert not [r for r in _run_checks(_Source(raises=True), SourceType.POSTGRES).results
                if r.check_id is PrerequisiteCheckId.ROW_SECURITY]
    mysql = _Source(RowSecurityFacts(login=_login(), tables=(_rls_table(),)))
    _run_checks(mysql, SourceType.MYSQL)
    assert mysql.asked == []


def test_the_row_security_row_is_filed_with_the_table_rows() -> None:
    from dsql_migrator.ui.data_migration import prereq_phase_tag
    from dsql_migrator.ui.data_migration._models import (
        _PREREQ_CATEGORY_BY_CHECK,
        PrereqCategory,
    )

    assert _PREREQ_CATEGORY_BY_CHECK[PrerequisiteCheckId.ROW_SECURITY] is (
        PrereqCategory.SCHEMA_TABLES
    )
    # It gates the Full Load as much as CDC.
    assert prereq_phase_tag(PrerequisiteCheckId.ROW_SECURITY, combined=True) == (
        "Full Load + CDC"
    )


def test_the_session_probe_filters_to_the_selected_tables(monkeypatch) -> None:
    from dsql_migrator.ui import prerequisite_probes
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    class _Engine:
        def connect(self):  # noqa: ANN201
            return _Ctx()

        def dispose(self) -> None:
            pass

    executed: list[str] = []

    class _Connection:
        class dialect:  # noqa: N801 - mirrors SQLAlchemy's attribute
            name = "postgresql"

        def execution_options(self, **kwargs):  # noqa: ANN003, ANN201
            executed.append(f"options {kwargs}")
            return self

        def execute(self, statement, *_a):  # noqa: ANN001, ANN201
            executed.append(str(statement))

    class _Ctx:
        def __enter__(self):  # noqa: ANN204
            return _Connection()

        def __exit__(self, *_exc):  # noqa: ANN002, ANN204
            return False

    facts = RowSecurityFacts(login=_login(), tables=(_rls_table(), _rls_table("app.other")))
    monkeypatch.setattr(
        type(dialect_for(SourceType.POSTGRES)), "read_row_security", lambda self, c: facts
    )
    monkeypatch.setattr(prerequisite_probes, "make_source_engine_factory",
                        lambda _pw: lambda _cfg: _Engine())
    probe = SessionSourceProbe(
        SourceConnectionConfig(host="h", database="d", source_type=SourceType.POSTGRES), None
    )
    read = probe.row_security(["app.orders"])
    assert [t.table for t in read.tables] == ["app.orders"] and read.login == _login()
    # Bounded like Evaluation's read: a policy's table locked by another session cannot
    # make "Run checks" wait for it.
    assert executed == ["options {'isolation_level': 'AUTOCOMMIT'}", "SET lock_timeout = '5s'"]
    mysql = SessionSourceProbe(SourceConnectionConfig(host="h", database="d"), None)
    assert mysql.row_security(["app.orders"]) is None


# ---------------------------------------------------------------------------
# What the operator reads when the load or Validation is refused
# ---------------------------------------------------------------------------


class _InsufficientPrivilege(Exception):
    sqlstate = "42501"


_REFUSAL = _InsufficientPrivilege(
    'query would be affected by row-level security policy for table "orders"'
)


def test_a_refused_load_explains_itself() -> None:
    from dsql_migrator.core.introspector import SOURCE_ROW_SECURITY_HINT, source_error_hint

    assert source_error_hint(_REFUSAL, SourceType.POSTGRES) == SOURCE_ROW_SECURITY_HINT
    assert source_error_hint(_REFUSAL, SourceType.MYSQL) != SOURCE_ROW_SECURITY_HINT
    assert "BYPASSRLS" in SOURCE_ROW_SECURITY_HINT
    assert "row_security = off" in SOURCE_ROW_SECURITY_HINT


def test_a_refused_validation_table_explains_itself() -> None:
    from dsql_migrator.core.models import ValidationMode
    from dsql_migrator.core.validator import Validator

    validator = Validator()

    def _raise(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise _REFUSAL

    validator._compare_table = _raise  # type: ignore[method-assign]
    result = validator._validate_table(
        None, None, TableDef(name="app.orders", primary_key=["id"]), ValidationMode.ROW_COUNT,
        None, False, lambda: False, dialect_for(SourceType.POSTGRES),
    )
    assert result.error.startswith("_InsufficientPrivilege: query would be affected")
    assert "BYPASSRLS" in result.error


def test_the_default_multi_process_load_keeps_the_refusal_hint(monkeypatch) -> None:
    """The process workers returned the bare driver text; only the thread fallback (one
    table at a time) added the hint."""
    from types import SimpleNamespace

    from dsql_migrator.core.introspector import SOURCE_ROW_SECURITY_HINT
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    def _refuse(*_a, **_k):
        raise _REFUSAL

    monkeypatch.setattr(engine, "BatchedTableMigrator", _refuse)
    inputs = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES), excluded_lob_columns={}
    )
    table = _pg_table("app.orders")
    result = engine._migrate_one_table_in_process(
        engine._TableWorkerArgs(job_id="j", table=table, inputs=inputs)
    )
    assert result.status == "FAILED" and SOURCE_ROW_SECURITY_HINT in result.error_message
    shard = engine._migrate_shard_in_process(
        engine._ShardWorkerArgs(
            job_id="j", table=table, inputs=inputs, pk_lower=None, pk_upper=None, shard_index=0
        )
    )
    assert shard.status == "FAILED" and SOURCE_ROW_SECURITY_HINT in shard.error_message


def test_a_refused_shard_plan_loads_the_table_with_one_reader() -> None:
    """The planning MIN/MAX of a large RLS table raised inside the parent's planning loop
    and ended the whole run before any table was loaded."""
    from types import SimpleNamespace

    from dsql_migrator.ui.data_migration._full_load_engine import _planned_shard_ranges

    class _Exporter:
        def plan_pk_shard_ranges(self, *_a, **_k):
            raise _REFUSAL

    migrator = SimpleNamespace(_exporter=_Exporter(), _inputs=SimpleNamespace(source_config=None))
    assert _planned_shard_ranges(migrator, _pg_table("app.orders"), 4, 0) == []


def test_a_sequence_shared_by_column_defaults_is_re_attached_not_replaced_by_identities() -> None:
    """Two tables' order_no DEFAULT nextval('shop.order_no_seq'): the sequence item said
    create the sequence (which restores no DEFAULT) and NON_KEY_SEQUENCE told each table to
    add its own identity -- two counters handing out the same order numbers."""
    from dsql_migrator.core.converter_postgres import pg_identity_fallback, pg_nextval_sequence

    shared = "nextval('shop.order_no_seq'::regclass)"
    tables = [
        _pg_table(name, ColumnDef(name="order_no", mysql_type="bigint", nullable=False, default=shared))
        for name in ("shop.web_orders", "shop.store_orders")
    ]
    (item,) = standalone_sequence_items(
        [_sequence(name="shop.order_no_seq")], SourceInventory(tables=tables)
    )
    assert "shop.web_orders.order_no, shop.store_orders.order_no" in item.risk
    assert (
        'ALTER TABLE "shop"."web_orders" ALTER COLUMN "order_no" SET DEFAULT '
        "nextval('\"shop\".\"order_no_seq\"');" in item.recommendation
    )
    assert "Do not give those columns an identity instead" in item.recommendation
    # Both CACHE choices are offered, and CACHE 1 is described as in-order, not gap-free.
    advice = pg_identity_fallback([tables[0].columns[1]])
    assert "CACHE 1 hands them out one at a time and in order" in advice
    assert "without gaps" not in advice
    assert pg_nextval_sequence("""nextval('"Shop"."Seq"'::regclass)""") == "Shop.Seq"


def test_the_sequence_item_follows_the_catalog_and_keeps_formatted_defaults() -> None:
    """Callers came from DEFAULT text, whose names follow the search_path (a cross-schema
    caller was missed); a formatted DEFAULT was re-attached as a bare nextval; and a primary
    key the conversion makes an identity was told to SET DEFAULT, which it cannot take."""
    formatted = "('ORD-'::text || lpad((nextval('order_seq'::regclass))::text, 8, '0'::text))"
    sequence = _sequence(name="public.order_seq").__class__(
        **{**_sequence(name="public.order_seq").__dict__,
           "callers": (("app.orders", "order_no", formatted), ("app.invoices", "id", "nextval('order_seq'::regclass)"))}
    )
    orders = _pg_table("app.orders", ColumnDef(name="order_no", mysql_type="text", default=formatted))
    invoices = TableDef(
        name="app.invoices",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False,
                           default="nextval('order_seq'::regclass)")],
        primary_key=["id"], auto_increment_column="id",
    )
    (item,) = standalone_sequence_items([sequence], SourceInventory(tables=[orders, invoices]))
    assert "A DEFAULT calls it on app.orders.order_no;" in item.risk  # from the catalog
    assert (
        "SET DEFAULT ('ORD-'::text || lpad((nextval('\"public\".\"order_seq\"'))::text, 8, "
        "'0'::text));" in item.recommendation
    )
    # The identity primary key: no SET DEFAULT, and how to keep the shared numbering.
    assert '"invoices" ALTER COLUMN "id" SET DEFAULT' not in item.recommendation
    assert "It is the primary key DEFAULT of app.invoices.id" in item.risk
    assert "choose 'Keep source PK' for those tables" in item.recommendation


def test_a_report_without_the_probe_says_so_after_a_refresh_or_restore() -> None:
    """A PostgreSQL report from before the probe kept reading 'Ready' over tables with
    tenant-isolation policies: it held no trace of the probe, like one that found nothing."""
    from dsql_migrator.core.models import TargetInventory
    from dsql_migrator.ui.evaluation import (
        EvaluationResult,
        compute_migration_score,
        with_refreshed_catalogs,
    )

    inventory = SourceInventory(tables=[_pg_table("app.orders")])
    old = EvaluationResult(
        inventory=inventory,
        assessment=_pg_assess(_pg_table("app.orders")),
        target_inventory=TargetInventory(),
        target_conflicts=[],
        source_type=SourceType.POSTGRES,
    )
    refreshed = with_refreshed_catalogs(
        old, inventory=inventory, target_inventory=TargetInventory(), target_endpoint=None
    )
    (unread,) = [i for i in refreshed.assessment.items if i.rule_id == "PG_CATALOG_UNREAD"]
    assert "were not checked" in unread.risk and "re-run Step 1" in unread.risk
    # A report whose probe ran (and found nothing) carries its note instead.
    probed = old.__class__(**{
        **old.__dict__,
        "assessment": with_pg_catalog_facts(
            old.assessment, PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=())
        ),
    })
    again = with_refreshed_catalogs(
        probed, inventory=inventory, target_inventory=TargetInventory(), target_endpoint=None
    )
    rule_ids = [i.rule_id for i in again.assessment.items]
    assert "PG_CATALOG_CHECKED" in rule_ids and "PG_CATALOG_UNREAD" not in rule_ids
    # The note is not an object to migrate: it does not dilute the score.
    assert compute_migration_score(again.assessment).score == compute_migration_score(
        old.assessment
    ).score


def test_the_probe_note_is_never_counted_as_an_object() -> None:
    """An empty database read 'Ready 100/100' over the probe's own note, and every count
    was one higher than the objects."""
    from dsql_migrator.core.models import AssessmentReport
    from dsql_migrator.ui.evaluation import compute_migration_score

    empty = with_pg_catalog_facts(
        AssessmentReport.from_items([]),
        PgCatalogFacts(row_security=RowSecurityFacts(login=None), sequences=()),
    )
    assert [i.rule_id for i in empty.items] == ["PG_CATALOG_CHECKED"]  # kept for refresh
    assert empty.assessed_items == []
    assert sum(empty.summary.values()) == 0
    assert compute_migration_score(empty) is None
