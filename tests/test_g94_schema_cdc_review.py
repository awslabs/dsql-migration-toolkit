"""G94: the review of G90/G92 -- an edit kept beneath the composite refusal comment, comments
typed in an edited script, the CTE scanner's scope and speed (and, in test_g92, a PostgreSQL
start that snapshots)."""

from __future__ import annotations

import time

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef

_COMMENT = "-- Could not apply a composite primary key to shop.orders: too large;"
_TYPED = (
    'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, "cid" BIGINT, PRIMARY KEY ("id"));\n'
    'CREATE INDEX ASYNC "ix_cid" ON "shop"."orders" ("cid");'
)


def _orders() -> TableDef:
    return TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="cid", mysql_type="bigint")],
        primary_key=["id"], indexes=[{"name": "ix_cid", "columns": ["cid"]}],
    )


def test_statements_typed_beneath_the_refusal_comment_stand_alone() -> None:
    """0.1.570 stored the comment with its ';' inside it, so the typed CREATE read as one
    comment-led statement: Schema Apply refused it, Full Load used the generated DDL
    without the edit's index."""
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    state = SchemaConversionState()
    state.set_edited_target_ddl("shop.orders", f"{_COMMENT}\n{_TYPED}")
    assert state.get_edited_target_ddl("shop.orders") == _TYPED
    # A snapshot an earlier build wrote with the comment still leading the edit.
    state.edited_target_ddls = {"shop.orders": f"{_COMMENT}\n{_TYPED}"}
    snapshot = capture_session_snapshot(
        "s1", SessionConnectionState(), EvaluationState(), state, DataMigrationState()
    )
    restored = SchemaConversionState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), EvaluationState(), restored, DataMigrationState(),
    )
    assert restored.edited_target_ddls == {"shop.orders": _TYPED}


def test_comments_typed_in_an_edit_are_applied_and_classified() -> None:
    """A note above each statement made it one comment-led statement: Schema Apply refused
    the CREATE, Full Load kept no index, and a commented FK ALTER went to Schema Apply."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion_apply import (
        ApplyObject,
        applied_table_conversions,
        override_apply_objects,
    )

    table = _orders()
    result = SchemaConverter().convert(SourceInventory(tables=[table]))
    edited = (
        "-- the key is kept as on the source\n" + _TYPED.replace("CREATE INDEX", "/* by cid */ CREATE INDEX")
        + '\n-- fk\nALTER TABLE "shop"."orders" ADD CONSTRAINT "fk" FOREIGN KEY ("cid") '
        'REFERENCES "shop"."customers" ("id") NOT VALID;\n-- end of script'
    )
    applied = applied_table_conversions(result, {"shop.orders": edited})["shop.orders"]
    assert applied.target_ddl.startswith('CREATE TABLE "shop"."orders"')
    assert applied.index_ddls == ['CREATE INDEX ASYNC "ix_cid" ON "shop"."orders" ("cid")']
    assert len(applied.foreign_key_ddls) == 1
    (obj,) = override_apply_objects([ApplyObject(object_name="shop.orders", ddls=())],
                                    {"shop.orders": edited}, conversion=result)
    assert [ddl.split(" ", 2)[1] for ddl in obj.ddls] == ["TABLE", "INDEX"]


def test_a_nested_with_named_like_an_outer_read_keeps_that_read() -> None:
    from dsql_migrator.core.converter import _scan_sql_names, views_in_dependency_order
    from dsql_migrator.core.converter import ViewConversion

    body = (
        'SELECT o.id FROM ((orders o JOIN customers c ON (c.id = o.cid)) JOIN LATERAL '
        '(WITH orders AS (SELECT 1 AS n) SELECT orders.n FROM orders) x ON (true))'
    )
    assert (None, "orders") in _scan_sql_names(body).relations
    views = [
        ViewConversion(view="public.a_report", target_ddl=f'CREATE VIEW "public"."a_report" AS {body}'),
        ViewConversion(view="public.orders", target_ddl='CREATE VIEW "public"."orders" AS SELECT 1 AS id'),
    ]
    assert [v.view for v in views_in_dependency_order(views)] == ["public.orders", "public.a_report"]
    # The CTE's own scope still reads it as the statement's own.
    assert _scan_sql_names("WITH orders AS (SELECT 1) SELECT * FROM orders").relations == []


def test_a_long_literal_does_not_slow_the_scan() -> None:
    """The CTE pattern split a masked literal's blanks between two \\s*: a 40,000-character
    literal took seconds."""
    from dsql_migrator.core.converter import _scan_sql_names

    literal = "'" + "x" * 40000 + "'"
    started = time.perf_counter()
    _scan_sql_names(f"SELECT CASE WHEN a THEN {literal}::text END AS v FROM t")
    assert time.perf_counter() - started < 0.5
