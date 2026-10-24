# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G153: pgvector and the other extension types under PostgreSQL CDC.

The bundled Debezium 2.7.4 PostgreSQL connector recognises a type defined outside pg_catalog
by NAME only, and knows six (TypeRegistry: geometry, geography, citext, hstore, ltree, isbn,
and their arrays). Every other base type outside pg_catalog -- pgvector's vector / halfvec /
sparsevec, cube, seg, the isn types but isbn, ltree's lquery / ltxtquery -- is
java.sql.Types.OTHER to it, gets no schema ("No converter found for column ... The column
will not be part of change events for that table"), and is left out of every change event,
because cdc-stack.yaml does not set include.unknown.datatypes. Such a column arrives NULL
(or its DEFAULT; a NOT NULL one with no DEFAULT has every insert and update refused, 23502),
exactly like tsvector -- and nothing said so: CDC_UNCARRYABLE_COLUMN and Step 3's catalog
prerequisite considered pg_catalog types only.

* Step 3's probe returns, in the same single catalog round trip, every base type outside
  pg_catalog the connector does not know (a third branch, flagged ``outside_catalog``), and
  COLUMN_REPLICABLE grades it like tsvector: WARN, non-blocking, the same remedies.
* Evaluation's CDC_UNCARRYABLE_COLUMN names the common ones by name (``format_type``
  qualifies one whose schema is not on the search path); PG_UNSUPPORTED_TYPE says CDC cannot
  carry them either.
* The manual (en / ko / ja) says so in chapter 4 and chapter 6 (6.2).

The catalog rows below are what the probe returned from PostgreSQL 17.11 with pgvector 0.8.7
and the contrib extensions (and the same shapes from 13.23) while this was written, in
throwaway local containers; the tests use doubles only.
"""

from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteStatus,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.prerequisites_postgres import (
    CDC_CARRIED_EXTENSION_TYPES,
    UNCARRYABLE_COLUMN_ARRIVES_NULL,
    PostgresCdcFacts,
    _CDC_CARRIED_EXTENSION_TYPES,
    _CDC_DROPPED_EXTENSION_TYPES,
    cdc_drops_extension_type,
    check_columns_replicable,
    check_postgres_cdc_prerequisites,
    classify_uncarryable_cdc_column,
    uncarryable_column_texts,
    uncarryable_columns_of,
)
from dsql_migrator.core.source_dialect.postgres import (
    _PG_CDC_UNCARRYABLE_COLUMNS_SQL,
    PostgresSourceDialect,
)

_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 1. The bundled connector: which extension types it knows, and that it drops the rest
# ---------------------------------------------------------------------------


def _connector_classes(*names: str) -> "dict[str, bytes]":
    """The named classes of the committed PostgreSQL source plugin's connector jar."""
    bundle_path = _ROOT / "connectors/plugins/debezium-postgres-plugin.zip"
    found: dict[str, bytes] = {}
    with zipfile.ZipFile(bundle_path) as bundle:
        jars = [
            n for n in bundle.namelist()
            if re.search(r"/debezium-connector-postgres-[^/]*\.jar$", n)
        ]
        assert len(jars) == 1, jars
        # A jar is itself a DEFLATE-compressed ZIP: the class has to be read out of it.
        with bundle.open(jars[0]) as raw, zipfile.ZipFile(io.BytesIO(raw.read())) as jar:
            for name in names:
                found[name] = jar.read(f"io/debezium/connector/postgresql/{name}.class")
    return found


def _utf8_constants(class_bytes: bytes, pattern: bytes) -> "set[str]":
    """The class file's CONSTANT_Utf8 entries (tag 1, a 2-byte length, the bytes) that match
    ``pattern`` in full."""
    out = set()
    for match in re.finditer(rb"\x01(..)(" + pattern + rb")", class_bytes, re.S):
        if int.from_bytes(match.group(1), "big") == len(match.group(2)):
            out.add(match.group(2).decode())
    return out


def test_the_carried_extension_types_are_the_bundled_connectors_own() -> None:
    """The six names are the ``TYPE_NAME_*`` constants of the shipped TypeRegistry -- what
    javap prints for debezium-connector-postgres-2.7.4.Final.jar -- and the connector has
    nothing for pgvector. A newer connector that knows another type must change the list
    (and the probe that binds it), or a carried column is reported as lost, or a lost one as
    carried."""
    classes = _connector_classes("TypeRegistry", "PostgresValueConverter")
    registry = classes["TypeRegistry"]
    fields = _utf8_constants(registry, rb"TYPE_NAME_[A-Z_]+")
    scalars = {f[len("TYPE_NAME_"):].lower() for f in fields if not f.endswith("_ARRAY")}
    assert scalars == set(_CDC_CARRIED_EXTENSION_TYPES)
    # Each constant's VALUE is the type name it is compared with (pg_type.typname).
    for name in _CDC_CARRIED_EXTENSION_TYPES:
        assert _utf8_constants(registry, re.escape(name.encode())), name
    for blob in classes.values():
        for pgvector in (b"vector", b"halfvec", b"sparsevec"):
            assert not _utf8_constants(blob, pgvector), pgvector
    # What the probe binds is the same set, sorted.
    assert CDC_CARRIED_EXTENSION_TYPES == tuple(sorted(_CDC_CARRIED_EXTENSION_TYPES))
    # The Evaluation list never names a type the connector carries.
    assert not _CDC_DROPPED_EXTENSION_TYPES & _CDC_CARRIED_EXTENSION_TYPES


def _cdc_template():
    yaml = pytest.importorskip("yaml")

    class _Loader(yaml.SafeLoader):
        pass

    def _any(loader, _suffix, node):
        if isinstance(node, yaml.ScalarNode):
            return loader.construct_scalar(node)
        if isinstance(node, yaml.SequenceNode):
            return loader.construct_sequence(node)
        return loader.construct_mapping(node)

    _Loader.add_multi_constructor("!", _any)
    text = (_ROOT / "deploy/cdc-stack/cdc-stack.yaml").read_text(encoding="utf-8")
    return yaml.load(text, Loader=_Loader)


def test_the_postgres_connector_omits_an_unknown_type_rather_than_sending_bytes() -> None:
    """``include.unknown.datatypes`` (default false: "omits the fields") is what makes an
    unknown type a column LEFT OUT of the event -- set, it would arrive as raw bytes, a
    different consequence. And hstore is a JSON string only under the default
    ``hstore.handling.mode``."""
    config = _cdc_template()["Resources"]["PostgresSourceConnector"]["Properties"][
        "ConnectorConfiguration"
    ]
    assert config["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
    assert "include.unknown.datatypes" not in config
    assert "hstore.handling.mode" not in config
    assert not any(key.startswith("converters") for key in config), "no custom converter"


# ---------------------------------------------------------------------------
# 2. The classifier
# ---------------------------------------------------------------------------


def _c(name, *, array=False, typmod=-1, extension=True):
    return classify_uncarryable_cdc_column(
        is_array=array, base_type_name=name, element_typmod=typmod, extension=extension
    )


@pytest.mark.parametrize(
    "name",
    ["vector", "halfvec", "sparsevec", "cube", "seg", "ean13", "isbn13", "ismn", "ismn13",
     "issn", "issn13", "upc", "lquery", "ltxtquery", "hll", "semver"],
)
def test_an_extension_type_the_connector_does_not_know_arrives_null(name: str) -> None:
    """Scalar or array, whatever the modifier (vector(3) has atttypmod 3): left out of the
    event. The rule is the connector's, not a list -- ``hll`` / ``semver`` are not in the
    Evaluation list and are still graded when the probe returns them."""
    assert _c(name) == UNCARRYABLE_COLUMN_ARRIVES_NULL
    assert _c(name, array=True) == UNCARRYABLE_COLUMN_ARRIVES_NULL
    assert _c(name, typmod=3) == UNCARRYABLE_COLUMN_ARRIVES_NULL


@pytest.mark.parametrize("name", sorted(_CDC_CARRIED_EXTENSION_TYPES))
def test_an_extension_type_the_connector_knows_is_not_flagged(name: str) -> None:
    assert _c(name) is None
    assert _c(name, array=True) is None


def test_the_extension_flag_keeps_the_built_in_tables_apart() -> None:
    """An extension's type that shares a built-in name is graded as an extension's; and a
    built-in row (the first two branches) is graded exactly as before."""
    assert _c("numeric", array=True) == UNCARRYABLE_COLUMN_ARRIVES_NULL  # not the typmod rule
    assert _c("text") == UNCARRYABLE_COLUMN_ARRIVES_NULL
    assert _c("tsvector", extension=False) == UNCARRYABLE_COLUMN_ARRIVES_NULL
    assert _c("numeric", array=True, typmod=655366, extension=False) is None
    assert _c("vector", extension=False) is None, "a built-in row never carries this name"
    assert _c(None) is None


# ---------------------------------------------------------------------------
# 3. Step 3: the catalog probe and the COLUMN_REPLICABLE row
# ---------------------------------------------------------------------------

# (qname, column, declared_type, is_array, base name, element atttypmod, not_null,
# outside_catalog) -- verbatim from the probe on PostgreSQL 17.11 with pgvector 0.8.7, over a
# table that also held isbn, isbn[], ltree, ltree[], citext, citext[], hstore, hstore[], a base
# type named geometry and its array, an enum, an enum[], a composite, text and text[] (none
# returned). earth is earthdistance's domain over cube.
_PG17_ROWS = [
    ("app.items", "emb", "vector(3)", False, "vector", 3, False, True),
    ("app.items", "emb_nn", "vector(3)", False, "vector", 3, True, True),
    ("app.items", "hv", "halfvec(3)", False, "halfvec", 3, False, True),
    ("app.items", "sv", "sparsevec(5)", False, "sparsevec", 5, False, True),
    ("app.items", "emb_arr", "vector(3)[]", True, "vector", 3, False, True),
    ("app.items", "cb", "cube", False, "cube", -1, False, True),
    ("app.items", "sg", "seg", False, "seg", -1, False, True),
    ("app.items", "ean", "ean13", False, "ean13", -1, False, True),
    ("app.items", "lq", "lquery", False, "lquery", -1, False, True),
    ("app.items", "earth_col", "earth", False, "cube", -1, False, True),
    ("app.items", "dom_vec", "app.embedding", False, "vector", 3, False, True),
    ("app.items", "dom_vec_nn", "app.embedding_nn", False, "vector", 3, True, True),
    ("app.items", "dom_vec_arr", "app.embedding[]", True, "vector", -1, False, True),
    ("app.items", "tsv", "tsvector", False, "tsvector", -1, False, False),
]


class _Conn:
    """One connection for the whole probe; returns ``rows`` for the uncarryable statement
    and records what it was bound with."""

    def __init__(self, rows) -> None:
        self.rows = rows
        self.params: list[dict] = []

    def rollback(self) -> None:
        pass

    def execute(self, statement, params=None):
        sql = str(statement)
        if "typelem" in sql:
            self.params.append(dict(params or {}))
        return _Result(self.rows if "typelem" in sql else [])


class _Result:
    def __init__(self, rows) -> None:
        self._rows = rows

    def scalar(self):
        return None

    def fetchall(self):
        return list(self._rows)


def _probe(rows):
    conn = _Conn(rows)
    facts = PostgresSourceDialect().probe_cdc_prerequisites(conn, ["app.items"])
    return conn, facts


def test_the_probe_asks_for_every_base_type_outside_pg_catalog_but_the_carried_six() -> None:
    sql = " ".join(_PG_CDC_UNCARRYABLE_COLUMNS_SQL.split())
    assert "<> 'pg_catalog'::regnamespace) AS outside_catalog" in sql
    # G171 widened the branch to the user's composite / range / multirange types as well.
    assert (
        "OR (COALESCE(ebt.typtype, bt.typtype) IN ('b', 'c', 'r', 'm') "
        "AND COALESCE(ebt.typnamespace, bt.typnamespace) <> 'pg_catalog'::regnamespace "
        "AND COALESCE(ebt.typname, bt.typname) <> ALL(:carried_extension_types))"
    ) in sql
    # Still catalog-only and bounded by the selected tables.
    assert "n.nspname || '.' || c.relname = ANY(:names)" in sql
    assert " FROM pg_catalog.pg_attribute a " in sql
    conn, _facts = _probe([])
    (params,) = conn.params
    assert params["carried_extension_types"] == list(CDC_CARRIED_EXTENSION_TYPES)
    assert params["names"] == ["app.items"]


def test_the_probe_grades_each_extension_column_as_left_out_of_the_event() -> None:
    conn, facts = _probe(_PG17_ROWS)
    assert len(conn.params) == 1, "still ONE catalog round trip for all selected tables"
    found = {c.column: c for c in (facts.cdc_uncarryable_columns or {})["app.items"]}
    assert list(found) == [row[1] for row in _PG17_ROWS]
    for column in found.values():
        assert column.reason == UNCARRYABLE_COLUMN_ARRIVES_NULL, column
        assert column.in_event is False, "left OUT of the event, not a field that failed"
    assert found["emb_nn"].nullable is False and found["dom_vec_nn"].nullable is False
    assert found["emb"].nullable is True
    assert found["earth_col"].declared_type == "earth", "the column's own spelling"


def test_a_carried_name_or_an_older_row_shape_is_not_graded_as_an_extension() -> None:
    """The server filters the six out; the classifier would too. A row without the eighth
    column (a test double of the earlier shape) is a built-in row."""
    rows = [
        ("app.items", "ct", "citext", False, "citext", -1, False, True),
        ("app.items", "emb", "vector(3)", False, "vector", 3, False),
    ]
    _conn, facts = _probe(rows)
    assert facts.cdc_uncarryable_columns == {}


def _facts(**over) -> PostgresCdcFacts:
    _conn, probed = _probe(_PG17_ROWS)
    base = dict(
        wal_level="logical", is_superuser=False, has_replication_role=True,
        max_replication_slots=10, used_replication_slots=1, max_wal_senders=10,
        used_wal_senders=1, is_in_recovery=False, max_slot_wal_keep_size_mb=-1,
        has_database_create=True, tables_not_owned=(),
        replica_identity={"app.items": "d"}, unlogged={"app.items": False},
        cdc_uncarryable_columns=probed.cdc_uncarryable_columns,
    )
    base.update(over)
    return PostgresCdcFacts(**base)


def test_the_row_names_each_column_and_warns_without_blocking() -> None:
    """The same grade and remedies as a tsvector column: WARN, not required. A NOT NULL one
    with no DEFAULT has every insert and update refused, so the NOT NULL route is offered."""
    table = TableDef(name="app.items", primary_key=["id"])
    row = check_columns_replicable(table, _facts())
    assert row is not None
    assert row.check_id is PrerequisiteCheckId.COLUMN_REPLICABLE
    assert row.status is PrerequisiteStatus.WARN and row.required is False
    assert "`emb` (vector(3))" in row.detail and "`hv` (halfvec(3))" in row.detail
    assert "`emb_arr` (vector(3)[])" in row.detail and "`cb` (cube)" in row.detail
    assert "dropped from the change event, so a row CDC inserts gets NULL" in row.detail
    assert (
        "`emb_nn` (vector(3)), `dom_vec_nn` (app.embedding_nn) are dropped from the change "
        "event but NOT NULL with no DEFAULT on the target" in row.detail
    )
    assert "drop the NOT NULL from the target DDL" in row.remediation
    assert "exclude the column in Exclude tables & columns" in row.remediation
    # An excluded column is never captured, so it is not judged.
    only_tsv = check_columns_replicable(
        table, _facts(), excluded=[r[1] for r in _PG17_ROWS if r[1] != "tsv"]
    )
    assert only_tsv is not None and "`emb`" not in only_tsv.detail

    results = check_postgres_cdc_prerequisites(_facts(), [table])
    report = PrerequisiteReport.build(mode=MigrationMode.CDC, results=results)
    assert report.can_proceed is True, "the Full Load carries these columns: never a FAIL"


# ---------------------------------------------------------------------------
# 4. Evaluation: CDC_UNCARRYABLE_COLUMN and PG_UNSUPPORTED_TYPE
# ---------------------------------------------------------------------------


def _col(name: str, declared: str, **kw) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=declared, **kw)


def _table(*columns: ColumnDef) -> TableDef:
    return TableDef(
        name="app.items",
        columns=[_col("id", "bigint", nullable=False), *columns],
        primary_key=["id"],
    )


def _concerns(table: TableDef) -> dict:
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    (item,) = [i for i in report.items if i.object_name == table.name]
    return {c.rule_id: c for c in item.concerns}


def test_evaluation_names_a_vector_column_as_step_3_does() -> None:
    table = _table(
        _col("emb", "vector(1536)"),
        _col("emb_ext", "ext.vector(3)"),  # format_type outside the search path
        _col("hv_arr", "halfvec(3)[]"),
        _col("sv", "sparsevec(5)"),
        _col("cb", "cube"),
        _col("ean", "ean13"),
        _col("lq", "lquery"),
        _col("ct", "citext"),
        _col("ct_arr", "citext[]"),
        _col("hs", "hstore"),
        _col("lt", "ltree"),
        _col("isbn_col", "isbn"),
        _col("geom", "geometry"),
        _col("geog", "public.geography"),
        _col("mood", "vector", type_kind="enum", enum_labels=("a", "b")),
        _col("pair", "cube", type_kind="composite"),
    )
    concern = _concerns(table)["CDC_UNCARRYABLE_COLUMN"]
    assert concern.classification is Classification.MANUAL
    # A composite is left out of the event by its KIND (G171), whatever it is named.
    for flagged in ("`emb` (vector(1536))", "`emb_ext` (ext.vector(3))",
                    "`hv_arr` (halfvec(3)[])", "`sv` (sparsevec(5))", "`cb` (cube)",
                    "`ean` (ean13)", "`lq` (lquery)", "`pair` (cube)"):
        assert flagged in concern.risk, flagged
    for carried in ("`ct`", "`ct_arr`", "`hs`", "`lt`", "`isbn_col`", "`geom`", "`geog`",
                    "`mood`"):
        assert carried not in concern.risk, carried
    assert "dropped from the change event" in concern.risk
    detail, remediation = uncarryable_column_texts(uncarryable_columns_of(table))
    assert concern.risk == "If you replicate with CDC: " + detail
    assert concern.recommendation == remediation
    assert all(c.in_event is False for c in uncarryable_columns_of(table))


def test_a_not_null_vector_column_gets_the_not_null_route_at_evaluation_too() -> None:
    concern = _concerns(_table(_col("emb", "vector(3)", nullable=False)))[
        "CDC_UNCARRYABLE_COLUMN"
    ]
    assert "NOT NULL with no DEFAULT on the target" in concern.risk
    assert "(SQLSTATE 23502)" in concern.risk
    assert "drop the NOT NULL from the target DDL" in concern.recommendation


def test_a_table_of_carried_extension_types_has_no_cdc_finding() -> None:
    concerns = _concerns(
        _table(_col("ct", "citext"), _col("hs", "hstore"), _col("lt", "ltree[]"),
               _col("isbn_col", "isbn"), _col("geom", "geometry"))
    )
    assert "CDC_UNCARRYABLE_COLUMN" not in concerns
    # They are still types Aurora DSQL does not have -- and say nothing about CDC.
    assert "CDC cannot carry" not in concerns["PG_UNSUPPORTED_TYPE"].risk


def test_cdc_drops_extension_type_reads_the_name_not_the_schema_or_kind() -> None:
    assert cdc_drops_extension_type(_col("e", "vector(3)"))
    assert cdc_drops_extension_type(_col("e", '"My Ext".vector(3)[]'))
    assert not cdc_drops_extension_type(_col("e", "citext"))
    assert not cdc_drops_extension_type(_col("e", "vector", type_kind="enum"))
    assert not cdc_drops_extension_type(_col("e", "app.embedding", type_kind="domain"))
    assert not cdc_drops_extension_type(_col("e", "tsvector"))


def test_the_unsupported_type_finding_says_cdc_cannot_carry_a_vector_either() -> None:
    """The finding advises a TARGET remodel (jsonb or text), and the connector decides by the
    SOURCE type, so that remodel does not reach CDC: say so where it is advised."""
    unsupported = _concerns(_table(_col("emb", "vector(3)"), _col("ip", "inet")))[
        "PG_UNSUPPORTED_TYPE"
    ]
    assert unsupported.classification is Classification.UNSUPPORTED
    assert unsupported.risk.endswith(
        ' CDC cannot carry "emb" either, whatever its type on Aurora DSQL: the bundled CDC '
        "connector does not know its extension type and leaves it out of every change event."
    )
    assert '"ip"' not in unsupported.risk.split("CDC cannot carry", 1)[1]
    # An array of one is re-typed (MANUAL) and gets the same sentence, in the plural here.
    retyped = _concerns(_table(_col("a", "vector(3)[]"), _col("b", "cube[]")))[
        "PG_UNSUPPORTED_TYPE"
    ]
    assert retyped.classification is Classification.MANUAL
    assert retyped.risk.endswith(
        ' CDC cannot carry "a", "b" either, whatever their type on Aurora DSQL: the bundled '
        "CDC connector does not know their extension types and leaves them out of every "
        "change event."
    )
    plain = _concerns(_table(_col("ip", "inet"), _col("tags", "text[]")))["PG_UNSUPPORTED_TYPE"]
    assert "CDC cannot carry" not in plain.risk


# ---------------------------------------------------------------------------
# 5. The manual
# ---------------------------------------------------------------------------

_MANUAL = _ROOT / "docs" / "manual"
_LIMIT_HEAD = {
    "en": "**PostgreSQL: CDC does not carry most extension types.**",
    "ko": "**PostgreSQL: CDC는 대부분의 확장 타입을 옮기지 못함.**",
    "ja": "**PostgreSQL: CDC はほとんどの拡張機能の型を運びません。**",
}


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_names_the_extension_types_cdc_does_not_carry(language: str) -> None:
    limits = _chapter(language, "06-limitations.md").split("## 6.2", 1)[1].split("## 6.3", 1)[0]
    bullet = limits.split(_LIMIT_HEAD[language], 1)[1].split(" - **", 1)[0]
    for spelled in ("`citext`", "`hstore`", "`ltree`", "`isbn`", "`geometry`", "`geography`",
                    "`vector`", "`halfvec`", "`sparsevec`", "`cube`", "`seg`", "`lquery`",
                    "`isn`", "`23502`", "`CDC_UNCARRYABLE_COLUMN`",
                    "*Every column's type can be carried by CDC*", "**Drop & reload**",
                    "13-excluding-tables-and-columns.md"):
        assert spelled in bullet, (language, spelled)
    cdc = _chapter(language, "04-cdc-and-dsql-constraints.md")
    listed = cdc.split("`CDC_UNCARRYABLE_COLUMN`", 1)[1].split(" - ", 1)[0]
    assert "pgvector" in listed and "`vector`" in listed, language
