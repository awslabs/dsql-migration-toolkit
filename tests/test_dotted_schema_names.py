# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A '.' inside a database (MySQL) or schema (PostgreSQL) name: ``DOTTED_SCHEMA_NAME``.

Every step splits a qualified name at its first '.', so database ``my.db``'s table
``orders`` (``my.db.orders``) is taken for table ``db.orders`` of database ``my`` and
cannot be migrated, yet Evaluation graded it Automatic. The qualified names alone cannot
tell it from a '.' inside a table name (``shop.a.b``, which the Full Load handles), so the
catalog read records the database's own name and Evaluation grades from that.

Covers:
- the catalog read records each dotted database or schema that holds a table or view, in
  all three read modes (MySQL cluster-wide, MySQL one database, PostgreSQL);
- ``introspect()`` keeps them for Evaluation (``last_dotted_schemas``), for one run only;
- the finding: Unsupported on each table and view, one per object (from the longest dotted
  prefix), worded for the engine; ``shop.a.b`` and ``my.dbx.z`` are left alone;
- Evaluation adds it, a refresh or restore of the same source and engine keeps it, and an
  engine switch drops it;
- the manual (en, ko, ja): chapter 2's Both engines Unsupported table names it with the
  rename on each engine (on MySQL, dropping the triggers first), and chapter 6 names it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from dsql_migrator.core.assessor import (
    DOTTED_SCHEMA_RULE_ID,
    CompatibilityAssessor,
    dotted_schema_findings,
    with_object_findings,
)
from dsql_migrator.core.introspector import SourceIntrospector, _assemble_inventory
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    EffortLevel,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    TargetInventory,
    ViewDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui.evaluation import EvaluationInputs, run_evaluation, with_refreshed_catalogs
from tests.test_introspector import (
    _FakeInspector,
    _NonMysqlConnection,
    _sqlite_factory,
)


def _table(name: str) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"],
    )


def _inventory(*tables: str, views: tuple = ()) -> SourceInventory:
    return SourceInventory(
        tables=[_table(name) for name in tables],
        views=[ViewDef(name=name, definition="SELECT 1") for name in views],
    )


def _concerns(report, name: str) -> list:
    (item,) = [item for item in report.items if item.object_name == name]
    return [c for c in item.concerns if c.rule_id == DOTTED_SCHEMA_RULE_ID]


# ---------------------------------------------------------------------------
# 1. The catalog read records the database's or schema's own name
# ---------------------------------------------------------------------------


def _read(catalog: dict, connection, database, engine: SourceType):  # noqa: ANN001
    dotted: list[str] = []
    inventory = _assemble_inventory(
        _FakeInspector(catalog), connection, database, dialect=dialect_for(engine),
        dotted_out=dotted,
    )
    return inventory, dotted


def test_the_catalog_read_records_each_dotted_database_or_schema_holding_a_table_or_view() -> None:
    # MySQL, cluster-wide: one holding nothing is not recorded, one holding a view only is.
    inventory, dotted = _read(
        {
            "my.db": {"tables": {"orders": {}}, "views": {"v": "SELECT 1"}},
            "shop": {"tables": {"a.b": {}}},
            "empty.db": {},
            "views.only": {"views": {"w": "SELECT 1"}},
        },
        _NonMysqlConnection(), None, SourceType.MYSQL,
    )
    assert dotted == ["my.db", "views.only"]
    # What the names alone cannot tell apart: database my.db, and table a.b of shop.
    assert {table.name for table in inventory.tables} == {"my.db.orders", "shop.a.b"}
    assert {view.name for view in inventory.views} == {"my.db.v", "views.only.w"}
    # MySQL, one database selected: that database, as named on Connect.
    single = {None: {"tables": {"orders": {}}}}
    assert _read(single, _NonMysqlConnection(), "my.db", SourceType.MYSQL)[1] == ["my.db"]
    assert _read(single, _NonMysqlConnection(), "shop", SourceType.MYSQL)[1] == []
    # PostgreSQL: every schema of the connected database.
    catalog = {"my.app": {"tables": {"orders": {}}}, "public": {"tables": {"docs": {}}}}
    _, dotted = _read(
        catalog, _NonMysqlConnection(schemas=list(catalog)), "mydb", SourceType.POSTGRES
    )
    assert dotted == ["my.app"]


def test_introspect_keeps_the_dotted_names_of_its_own_read_for_evaluation() -> None:
    # The real catalog read, one database selected on Connect (SQLite stands in for the
    # source, a fresh copy of its tables on every read).
    introspector = SourceIntrospector(engine_factory=lambda conn: _sqlite_factory()(conn))
    assert introspector.last_dotted_schemas == []
    inventory = introspector.introspect(SourceConnectionConfig(host="db", database="my.db"))
    assert "my.db.orders" in {table.name for table in inventory.tables}
    assert introspector.last_dotted_schemas == ["my.db"]
    # The next read starts afresh: a database renamed meanwhile is no longer named.
    inventory = introspector.introspect(SourceConnectionConfig(host="db", database="my_db"))
    assert "my_db.orders" in {table.name for table in inventory.tables}
    assert introspector.last_dotted_schemas == []


# ---------------------------------------------------------------------------
# 2. The finding
# ---------------------------------------------------------------------------


def test_each_table_and_view_of_a_dotted_database_is_unsupported_and_nothing_else() -> None:
    inventory = _inventory("my.db.orders", "shop.a.b", "my.dbx.z", views=("my.db.v",))
    report = CompatibilityAssessor(source_type=SourceType.MYSQL).assess(inventory)
    graded = with_object_findings(
        report, dotted_schema_findings(inventory, ["my.db", "my.db"])
    )
    for name in ("my.db.orders", "my.db.v"):
        (item,) = [item for item in graded.items if item.object_name == name]
        assert item.classification is Classification.UNSUPPORTED
        assert item.rule_id == DOTTED_SCHEMA_RULE_ID
        (concern,) = _concerns(graded, name)
        assert concern.effort is EffortLevel.MEDIUM
    # A '.' inside a table name, and a database that only starts like a dotted one.
    for name in ("shop.a.b", "my.dbx.z"):
        (item,) = [item for item in graded.items if item.object_name == name]
        assert item.classification is Classification.AUTO and not _concerns(graded, name)
    (view,) = _concerns(graded, "my.db.v")
    assert "between the database and the view" in view.risk
    assert "takes my.db.v for view db.v of database my" in view.risk


def test_a_nested_dotted_database_gets_one_finding_naming_its_own_database() -> None:
    inventory = _inventory("my.db.x.t", "my.db.t")
    report = with_object_findings(
        CompatibilityAssessor(source_type=SourceType.MYSQL).assess(inventory),
        dotted_schema_findings(inventory, ["my.db", "my.db.x"]),
    )
    (nested,) = _concerns(report, "my.db.x.t")
    assert nested.risk.startswith("Its database, my.db.x, has a '.' in its name.")
    assert "takes my.db.x.t for table db.x.t of database my," in nested.risk
    (outer,) = _concerns(report, "my.db.t")
    assert outer.risk.startswith("Its database, my.db, has a '.' in its name.")


def test_the_finding_names_the_engines_own_unit_and_remedy() -> None:
    inventory = _inventory("my.db.orders")
    (mysql,) = dotted_schema_findings(inventory, ["my.db"])
    (postgres,) = dotted_schema_findings(inventory, ["my.db"], is_postgres=True)
    assert mysql.risk == (
        "Its database, my.db, has a '.' in its name. The tool reads the first '.' of a "
        "qualified name as the one between the database and the table, so every step takes "
        "my.db.orders for table db.orders of database my, and it cannot be migrated."
    )
    assert "Its schema, my.db," in postgres.risk and "of schema my," in postgres.risk
    assert "RENAME TABLE, dropping its triggers first" in mysql.recommendation
    assert "ALTER SCHEMA" not in mysql.recommendation
    assert "ALTER SCHEMA ... RENAME TO ..." in postgres.recommendation
    assert "RENAME TABLE" not in postgres.recommendation
    for finding in (mysql, postgres):
        assert finding.classification is Classification.UNSUPPORTED
        assert finding.recommendation.endswith(
            "or leave its tables and views out of the migration: do not apply them in "
            "Schema Conversion, and do not select them in Data Migration."
        )


# ---------------------------------------------------------------------------
# 3. Evaluation grades it, a refresh keeps it, an engine switch drops it
# ---------------------------------------------------------------------------


class _Introspector:
    def __init__(self, inventory: SourceInventory, dotted: list) -> None:
        self._inventory = inventory
        self.last_dotted_schemas = dotted

    def introspect(self, conn, *, progress=None):  # noqa: ANN001, ANN201
        return self._inventory


class _Browser:
    def browse(self, _conn):  # noqa: ANN001, ANN201
        return TargetInventory()


def _evaluate(engine: SourceType, dotted: list):
    inventory = _inventory("my.db.orders", "shop.a.b", views=("my.db.v",))
    return run_evaluation(
        EvaluationInputs(
            source_config=SourceConnectionConfig(host="db", database=None, source_type=engine),
            source_password=None,
            target_config=TargetConnectionConfig(
                cluster_endpoint="c.dsql.example", region="us-east-1"
            ),
        ),
        introspector_factory=lambda _p: _Introspector(inventory, dotted),
        target_browser_factory=lambda _p: _Browser(),
    )


def test_evaluation_grades_it_and_a_refresh_of_the_same_source_keeps_it() -> None:
    result = _evaluate(SourceType.MYSQL, ["my.db"])
    for name in ("my.db.orders", "my.db.v"):
        (item,) = [item for item in result.assessment.items if item.object_name == name]
        assert item.classification is Classification.UNSUPPORTED
        assert _concerns(result.assessment, name)
    assert not _concerns(result.assessment, "shop.a.b")
    # A refresh or a restore re-grades from the inventory alone, which cannot tell.
    refreshed = with_refreshed_catalogs(
        result, inventory=result.inventory, target_inventory=TargetInventory(), target_endpoint=None
    )
    (item,) = [item for item in refreshed.assessment.items if item.object_name == "my.db.orders"]
    assert item.classification is Classification.UNSUPPORTED
    assert _concerns(refreshed.assessment, "my.db.v")
    # Not across an engine switch: that read named another server's databases.
    switched = with_refreshed_catalogs(
        result,
        inventory=result.inventory,
        target_inventory=TargetInventory(),
        target_endpoint=None,
        source_type=SourceType.POSTGRES,
    )
    assert not _concerns(switched.assessment, "my.db.orders")
    # Nothing dotted read: nothing added.
    assert not _concerns(_evaluate(SourceType.MYSQL, []).assessment, "my.db.orders")


def test_evaluation_of_a_postgres_source_names_the_schema() -> None:
    (concern,) = _concerns(_evaluate(SourceType.POSTGRES, ["my.db"]).assessment, "my.db.orders")
    assert concern.risk.startswith("Its schema, my.db, has a '.' in its name.")
    assert "ALTER SCHEMA" in concern.recommendation


# ---------------------------------------------------------------------------
# 4. The manual says it in every language
# ---------------------------------------------------------------------------


_BOTH_ENGINES = {"en": "#### Both engines", "ko": "#### 두 엔진 공통", "ja": "#### 両方のエンジン"}
# MySQL's RENAME TABLE refuses to move a table that has triggers into another database
# (ER_TRG_IN_WRONG_SCHEMA), so the rename drops them first, as the finding says.
_DROP_TRIGGERS_FIRST = {
    "en": "dropping its triggers first",
    "ko": "트리거를 먼저 삭제한 뒤",
    "ja": "トリガーを先に削除してから",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_lists_the_finding_as_blocking_on_both_engines(language: str) -> None:
    assert DOTTED_SCHEMA_RULE_ID == "DOTTED_SCHEMA_NAME"
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual" / language
    chapter = " ".join(
        (manual / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split()
    )
    # 2.4 is chapter 2's findings reference (the rule catalogue 2.1 points to), so
    # Evaluation's text is 2.1 plus 2.4.
    evaluation = chapter.split("## 2.1", 1)[1].split("## 2.2", 1)[0] + chapter.split("## 2.4", 1)[1]
    # The Both engines block's Unsupported table: up to its first Review needed label.
    blocking = evaluation.split(_BOTH_ENGINES[language], 1)[1].split("(Review needed)", 1)[0]
    assert "(Unsupported)" in blocking
    assert f"(`{DOTTED_SCHEMA_RULE_ID}`) |" in blocking
    remedy = blocking.split(f"(`{DOTTED_SCHEMA_RULE_ID}`) |", 1)[1].split(" |", 1)[0]
    assert "`RENAME TABLE`" in remedy and "`ALTER SCHEMA … RENAME TO …`" in remedy
    assert _DROP_TRIGGERS_FIRST[language] in remedy
    limits = " ".join((manual / "06-limitations.md").read_text(encoding="utf-8").split())
    process = limits.split("## 6.2", 1)[1].split("## 6.3", 1)[0]
    assert re.search(rf"\*\*Unsupported\*\* ?\(`{DOTTED_SCHEMA_RULE_ID}`\)", process)
