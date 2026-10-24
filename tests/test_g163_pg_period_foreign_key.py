# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G163 (K16): a PostgreSQL 18 FOREIGN KEY ... PERIOD.

SQLAlchemy 2.0's PostgreSQL dialect reads every foreign key of a table at once and parses
each one out of ``pg_get_constraintdef`` with a regular expression that cannot read
``FOREIGN KEY (room_id, PERIOD during) REFERENCES rev.rooms(id, PERIOD valid)``: the match is
None and ``.groups()`` raised AttributeError, which failed the whole Evaluation of any
PostgreSQL 18 database with such a key -- the usual partner of a WITHOUT OVERLAPS key, so
G151's PG_TEMPORAL_KEY was never seen on that design.

* Introspection reads such a table's keys from pg_constraint instead
  (``pg_foreign_keys_from_catalog``), in the shape SQLAlchemy returns -- on PostgreSQL 13.23,
  17.11 and 18.6 the catalog read returned exactly what SQLAlchemy reflected for every table
  it could read -- and marks a PERIOD key ``ForeignKeyDef.period``.
* A PERIOD key is not created on Aurora DSQL, which has no PERIOD (a plain key on the same
  columns would demand an equal period), and is left out of every place a NOT ENFORCED key
  is: the FK statements, the FK note, Validation's orphan check, the exclusion rules. Schema
  Conversion notes it and Evaluation grades it (PG_FK_PERIOD, MANUAL).
* The manual says so, and FAQ Q20, the 2.3 FK row and the chapter 8 row now name both PG18
  keys that are not created; chapter 5 says a MATCH FULL key's orphan is any key not all
  NULL.

The catalog rows below are what PostgreSQL 18.6 returned (throwaway local container); the
tests use doubles only.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql.base import PGDialect

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.introspector import _reflect_tables
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    EffortLevel,
    ForeignKeyDef,
    SourceInventory,
    SourceType,
    TableDef,
    TemporalKeyDef,
)
from dsql_migrator.core.source_dialect.postgres import (
    pg_foreign_keys_from_catalog,
    pg_foreign_keys_sql,
)

_PG = SourceType.POSTGRES
_PERIOD_DEF = "FOREIGN KEY (room_id, PERIOD during) REFERENCES rev.rooms(id, PERIOD valid)"


def test_sqlalchemy_cannot_parse_a_period_key_which_is_why_the_fallback_exists() -> None:
    """Pinned: when a SQLAlchemy upgrade parses it, the catalog read is no longer needed."""
    dialect = PGDialect()
    with pytest.raises(AttributeError):
        dialect._parse_fk(_PERIOD_DEF)  # noqa: SLF001
    assert dialect._parse_fk(  # noqa: SLF001
        "FOREIGN KEY (room_id) REFERENCES rev.rooms(id)"
    )[2] == "rooms"


# ---------------------------------------------------------------------------
# 1. The catalog read
# ---------------------------------------------------------------------------


# pg_constraint for rev.bookings as PostgreSQL 18.6 returned it for the statement below.
_BOOKINGS_ROWS = [
    {"conname": "Period FK", "columns": ["room_id", "during"], "ref_schema": "rev",
     "ref_table": "rooms", "ref_columns": ["id", "valid"], "on_update": "a", "on_delete": "a",
     "match_type": "s", "deferrable": False, "deferred": False,
     "definition": _PERIOD_DEF, "comment": None, "period": True},
    {"conname": "full_fk", "columns": ["a", "b"], "ref_schema": "rev", "ref_table": "parent",
     "ref_columns": ["a", "b"], "on_update": "c", "on_delete": "n", "match_type": "f",
     "deferrable": True, "deferred": True,
     "definition": "FOREIGN KEY (a, b) REFERENCES rev.parent(a, b) MATCH FULL ON UPDATE "
                   "CASCADE ON DELETE SET NULL (b) DEFERRABLE INITIALLY DEFERRED",
     "comment": "kept", "period": False},
    {"conname": "x_fk", "columns": ["oid_ref"], "ref_schema": "other", "ref_table": "op",
     "ref_columns": ["id"], "on_update": "a", "on_delete": "r", "match_type": "s",
     "deferrable": True, "deferred": False,
     "definition": "FOREIGN KEY (oid_ref) REFERENCES other.op(id) ON DELETE RESTRICT "
                   "DEFERRABLE", "comment": None, "period": False},
]


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _Catalog:
    """A connection answering the foreign-key read for ``rev.bookings`` only."""

    def __init__(self, version: tuple) -> None:
        self.dialect = SimpleNamespace(name="postgresql", server_version_info=version)
        self.executed: list[tuple[str, dict]] = []

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        self.executed.append((sql, dict(params or {})))
        if "con.conperiod" in sql and self.dialect.server_version_info < (18,):
            raise RuntimeError('column con.conperiod does not exist')
        rows = _BOOKINGS_ROWS if (params or {}).get("rel") == "bookings" else []
        if "con.conperiod" not in sql:
            rows = [{k: v for k, v in row.items() if k != "period"} for row in rows]
        return _Result(rows)


@pytest.mark.parametrize("version", [None, (13, 23), (17, 11)])
def test_the_read_names_conperiod_only_from_18(version) -> None:  # noqa: ANN001
    sql = pg_foreign_keys_sql(version)
    assert "conperiod" not in sql
    assert "con.contype = 'f'" in sql and "n.nspname = :nsp AND c.relname = :rel" in sql
    assert "unnest(con.conkey) WITH ORDINALITY" in sql
    assert "unnest(con.confkey) WITH ORDINALITY" in sql
    assert "con.conperiod AS period" in pg_foreign_keys_sql((18, 6))


def test_the_catalog_read_returns_what_sqlalchemy_reflects() -> None:
    keys = pg_foreign_keys_from_catalog(_Catalog((18, 6)), "rev", "bookings")
    assert keys == [
        {"name": "Period FK", "constrained_columns": ["room_id", "during"],
         "referred_schema": "rev", "referred_table": "rooms", "referred_columns": ["id", "valid"],
         "options": {}, "comment": None, "period": True},
        {"name": "full_fk", "constrained_columns": ["a", "b"], "referred_schema": "rev",
         "referred_table": "parent", "referred_columns": ["a", "b"],
         # A PostgreSQL 15+ column list is kept as SQLAlchemy keeps it.
         "options": {"onupdate": "CASCADE", "ondelete": "SET NULL (b)",
                     "initially": "DEFERRED", "deferrable": True, "match": "FULL"},
         "comment": "kept", "period": False},
        {"name": "x_fk", "constrained_columns": ["oid_ref"], "referred_schema": "other",
         "referred_table": "op", "referred_columns": ["id"],
         "options": {"ondelete": "RESTRICT", "deferrable": True}, "comment": None,
         "period": False},
    ]
    # On a server before 18 the statement never names conperiod, and nothing is PERIOD.
    older = _Catalog((17, 11))
    assert not any(k["period"] for k in pg_foreign_keys_from_catalog(older, "rev", "bookings"))
    assert all("conperiod" not in sql for sql, _ in older.executed)


# ---------------------------------------------------------------------------
# 2. Reflection falls back for the one table SQLAlchemy cannot read
# ---------------------------------------------------------------------------


class _Inspector:
    """SQLAlchemy's Inspector over three tables; its parse fails on ``bookings``'s keys."""

    _PLAIN_FK = {"name": "plain_child_pid_fkey", "constrained_columns": ["pid"],
                 "referred_schema": "rev", "referred_table": "rooms",
                 "referred_columns": ["id"], "options": {}, "comment": None}

    def __init__(self, dialect_name: str = "postgresql") -> None:
        self.dialect = SimpleNamespace(name=dialect_name)
        self.bind = _Catalog((18, 6))

    def get_table_names(self, schema=None):  # noqa: ANN001, ANN201
        return ["rooms", "bookings", "plain_child"]

    def get_columns(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return [{"name": "id", "type": "INTEGER", "nullable": False}]

    def get_pk_constraint(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"constrained_columns": ["id"]}

    def get_indexes(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_foreign_keys(self, table_name, schema=None):  # noqa: ANN001, ANN201
        if table_name == "bookings":
            return PGDialect()._parse_fk(_PERIOD_DEF)  # noqa: SLF001 - raises, as 2.0.50 does
        return [self._PLAIN_FK] if table_name == "plain_child" else []

    def get_check_constraints(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return []

    def get_table_comment(self, table_name, schema=None):  # noqa: ANN001, ANN201
        return {"text": None}


def test_reflection_reads_that_tables_keys_from_the_catalog_and_the_rest_as_before() -> None:
    inspector = _Inspector()
    tables = {t.name: t for t in _reflect_tables(inspector, schema="rev")}
    assert set(tables) == {"rooms", "bookings", "plain_child"}
    keys = {fk.name: fk for fk in tables["bookings"].foreign_keys}
    period = keys["Period FK"]
    assert period.period is True and period.created_on_target is False
    assert (period.columns, period.referenced_table, period.referenced_columns) == (
        ["room_id", "during"], "rev.rooms", ["id", "valid"]
    )
    full = keys["full_fk"]
    assert (full.period, full.match_full, full.deferrable, full.initially_deferred) == (
        False, True, True, True
    )
    assert (full.on_update, full.on_delete) == ("CASCADE", "SET NULL (b)")
    assert keys["x_fk"].referenced_table == "other.op" and keys["x_fk"].on_delete == "RESTRICT"
    # Read once, for the table that failed only.
    assert [params["rel"] for _sql, params in inspector.bind.executed] == ["bookings"]
    (plain,) = tables["plain_child"].foreign_keys
    assert plain.name == "plain_child_pid_fkey" and plain.period is False


def test_a_mysql_parse_error_is_not_swallowed() -> None:
    with pytest.raises(AttributeError):
        _reflect_tables(_Inspector("mysql"), schema="rev")


# ---------------------------------------------------------------------------
# 3. Not created, noted, graded, and left out of the orphan check and exclusion rules
# ---------------------------------------------------------------------------

_PK = ColumnDef(name="id", mysql_type="integer", nullable=False)


def _rooms() -> TableDef:
    return TableDef(
        name="rev.rooms",
        columns=[_PK, ColumnDef(name="valid", mysql_type="tstzrange", nullable=False)],
        primary_key=["id", "valid"],
        temporal_keys=[TemporalKeyDef(name="rooms_pkey", primary=True, index="rooms_pkey",
                                      definition="PRIMARY KEY (id, valid WITHOUT OVERLAPS)")],
    )


_PERIOD_FK = ForeignKeyDef(name="Period FK", columns=["room_id", "during"],
                           referenced_table="rev.rooms", referenced_columns=["id", "valid"],
                           period=True)
_PLAIN_FK = ForeignKeyDef(name="plain_fk", columns=["room_id"], referenced_table="rev.parent",
                          referenced_columns=["id"])


def _bookings(*keys: ForeignKeyDef) -> TableDef:
    return TableDef(
        name="rev.bookings",
        columns=[_PK, ColumnDef(name="room_id", mysql_type="integer"),
                 ColumnDef(name="during", mysql_type="tstzrange")],
        primary_key=["id"],
        foreign_keys=list(keys),
    )


def _parent() -> TableDef:
    return TableDef(name="rev.parent", columns=[_PK], primary_key=["id"])


def _conversion(*tables: TableDef):
    result = SchemaConverter(source_type=_PG).convert(SourceInventory(tables=list(tables)))
    return {t.table: t for t in result.tables}


def test_a_period_key_is_not_created_and_the_note_says_why() -> None:
    bookings = _conversion(_rooms(), _parent(), _bookings(_PERIOD_FK, _PLAIN_FK))["rev.bookings"]
    assert bookings.foreign_key_ddls == [
        'ALTER TABLE "rev"."bookings" ADD CONSTRAINT "plain_fk" FOREIGN KEY ("room_id") '
        'REFERENCES "rev"."parent" ("id") NOT VALID'
    ]
    # Kept as metadata, like a NOT ENFORCED key.
    assert [fk.name for fk in bookings.preserved_foreign_keys] == ["Period FK", "plain_fk"]
    (note,) = [w for w in bookings.warnings if "PERIOD foreign key(s)" in w.message]
    assert note.classification is Classification.MANUAL and note.kind is ConversionNoteKind.LOSS
    assert ("are NOT created — Period FK: (room_id, PERIOD during) REFERENCES rev.rooms "
            "(id, PERIOD valid).") in note.message
    assert "a plain foreign key on the same columns would demand an equal period" in note.message
    preserved = [w for w in bookings.warnings if w.message.startswith("Foreign key constraints")]
    assert preserved and "plain_fk" in preserved[0].message
    assert "Period FK" not in preserved[0].message


def test_evaluation_grades_it_in_the_conversions_words() -> None:
    inventory = SourceInventory(tables=[_rooms(), _parent(), _bookings(_PERIOD_FK)])
    report = CompatibilityAssessor(source_type=_PG).assess(inventory)
    (item,) = [i for i in report.items if i.object_name == "rev.bookings"]
    concerns = {c.rule_id: c for c in item.concerns}
    concern = concerns["PG_FK_PERIOD"]
    assert concern.classification is Classification.MANUAL
    assert concern.effort is EffortLevel.MEDIUM
    assert concern.risk.startswith("PERIOD foreign key(s) are NOT created")
    assert "periods cover its period" in concern.recommendation
    # The only key is not created, so there is nothing to call preserved.
    assert "FK_PRESERVED" not in concerns
    # Both steps grade the table alike, and a MySQL source never gets it.
    assert item.classification is Classification.MANUAL


def test_validation_does_not_check_a_period_key() -> None:
    from tests.test_validator import (
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        _FakeSourceConnection,
        _FakeTargetConnection,
        _table,
        _validator,
    )

    period = ForeignKeyDef(name="fk_orders_period", columns=["customer_id"],
                           referenced_table="customers", referenced_columns=["id"], period=True)
    orders = _table("orders", columns=("id", "customer_id"), primary_key=("id",),
                    foreign_keys=[period])
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 2})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [orders], check_orphans=True
    )
    assert report.orphan_findings == [] and report.is_match is True
    assert all("NOT EXISTS" not in text and "LEFT JOIN" not in text for text in target.executed)


def test_a_column_a_period_key_references_can_be_excluded() -> None:
    from dsql_migrator.core.column_exclusion import build_exclusion_context

    context = build_exclusion_context(
        SourceInventory(tables=[_rooms(), _parent(), _bookings(_PERIOD_FK, _PLAIN_FK)])
    )
    assert context.fk_referenced == {"rev.parent": {"id": ("rev.bookings.plain_fk",)}}


def test_the_flag_stays_out_of_the_payload_while_false_and_round_trips() -> None:
    plain = _bookings(_PLAIN_FK).model_dump()
    assert "period" not in plain["foreign_keys"][0]
    payload = json.loads(_bookings(_PERIOD_FK).model_dump_json())
    assert payload["foreign_keys"][0]["period"] is True
    assert TableDef.model_validate(payload) == _bookings(_PERIOD_FK)


def test_the_source_panel_and_the_ai_detail_show_the_period() -> None:
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    rendered = render_source_table_ddl(_bookings(_PERIOD_FK, _PLAIN_FK), source_type=_PG)
    assert ('CONSTRAINT "Period FK" FOREIGN KEY ("room_id", PERIOD "during") REFERENCES '
            '"rev"."rooms" ("id", PERIOD "valid")') in rendered
    assert 'CONSTRAINT "plain_fk" FOREIGN KEY ("room_id") REFERENCES "rev"."parent" ("id")' in rendered

    from dsql_migrator.ui.ai_tools import build_ai_tool_executor

    class _Obj:
        def __init__(self, **kw) -> None:  # noqa: ANN003
            self.__dict__.update(kw)

    class _Store:
        def __init__(self, obj=None, *, get_returns=None) -> None:  # noqa: ANN001
            self._obj, self._get = obj, get_returns

        def get_or_create(self, _sid):  # noqa: ANN001, ANN202
            return self._obj

        def get(self, _sid):  # noqa: ANN001, ANN202
            return self._get

    inventory = SourceInventory(tables=[_bookings(_PERIOD_FK, _PLAIN_FK)])
    execute = build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False,
                                  source_config=None, source_password=None)),
        evaluation_store=_Store(get_returns=_Obj(result=_Obj(inventory=inventory))),
        schema_conversion_store=_Store(_Obj(generated_node_ids=[], apply_results=[])),
        validation_store=_Store(_Obj(result=None)),
        data_migration_store=_Store(_Obj(job_id=None, get_prereq_report=lambda _m: None)),
        job_manager=_Obj(),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )
    detail = json.loads(execute("get_source_object_detail", {"object_name": "rev.bookings"}))
    keys = {fk["name"]: fk for fk in detail["foreign_keys"]}
    assert keys["Period FK"]["period"] is True and "period" not in keys["plain_fk"]


# ---------------------------------------------------------------------------
# 4. The manual
# ---------------------------------------------------------------------------

_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


_FAQ_HEAD = {"en": "**Q20.", "ko": "**Q20.", "ja": "**Q20."}
_ORPHAN = {
    "en": "(non-null; for a `MATCH FULL` key, not all-NULL) foreign key",
    "ko": "(NULL이 아닌, `MATCH FULL` 키라면 모두 NULL은 아닌) 외래 키",
    "ja": "（NULL でない、`MATCH FULL` のキーならすべてが NULL ではない）外部キー",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_which_keys_are_not_created(language: str) -> None:
    conversion = _chapter(language, "02-evaluation-and-schema-conversion.md")
    evaluation = conversion.split("## 2.1", 1)[1].split("## 2.2", 1)[0]
    assert "`PG_FK_PERIOD`" in evaluation and "`PERIOD`" in evaluation, language
    reference = conversion.split("## 2.3", 1)[1]
    assert reference.count("`PG_FK_PERIOD`") >= 2, language  # the constraints note and FK row
    assert "`PG_FK_NOT_ENFORCED`" in reference, language
    limits = _chapter(language, "06-limitations.md").split("## 6.1", 1)[1].split("## 6.2", 1)[0]
    assert limits.count("`PG_FK_PERIOD`") >= 2, language
    faq = _chapter(language, "11-customer-faq.md").split(_FAQ_HEAD[language], 1)[1]
    faq = faq.split("**Q21.", 1)[0]
    for spelled in ("`NOT ENFORCED`", "`PG_FK_NOT_ENFORCED`", "`FOREIGN KEY … PERIOD`",
                    "`PG_FK_PERIOD`", "`MATCH FULL`"):
        assert spelled in faq, (language, spelled)
    tests_chapter = _chapter(language, "08-testing-and-verification.md")
    assert "`NOT ENFORCED`" in tests_chapter and "`PERIOD`" in tests_chapter, language
    assert _ORPHAN[language] in _chapter(language, "05-validation.md"), language
