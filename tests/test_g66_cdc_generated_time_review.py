"""G66: the review of G44-G55 -- the CDC capture of GENERATED columns, and MySQL TIME
values Validation and Full Load read by the applied type."""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef

_GENERATED_DDL = (
    'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, "price" NUMERIC(10, 2), '
    '"qty" INT, "total" NUMERIC(12, 2) GENERATED ALWAYS AS ((price * qty)) STORED, '
    'PRIMARY KEY ("id"))'
)


def _orders() -> TableDef:
    return TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="price", mysql_type="decimal(10,2)"),
            ColumnDef(name="qty", mysql_type="int"),
            ColumnDef(name="total", mysql_type="decimal(12,2)"),
        ],
        primary_key=["id"],
    )


def test_a_stack_deployed_with_nothing_excluded_gets_the_new_generated_exclusion() -> None:
    """'' is also the 0.1.561 formula's value for a table with no stored exclusions, so a
    stack deployed that way kept '' after the column became GENERATED: every insert and
    update was refused (428C9) while the card read 'as deployed'."""
    from dsql_migrator.ui.data_migration._models import capture_exclusions_for

    orders = _orders()
    inventory = SourceInventory(tables=[orders])
    applied = {"shop.orders": SimpleNamespace(target_ddl=_GENERATED_DDL)}
    capture = capture_exclusions_for({}, {}, inventory, [orders], "", applied=applied)
    assert capture.structured == {"shop.orders": ["total"]}
    assert capture.sent == capture.value != ""
    assert capture.legacy is None  # the deployer may not keep '' either


def test_start_cdc_refuses_when_the_target_does_not_compute_what_the_capture_leaves_out(
    monkeypatch,
) -> None:
    """A GENERATED edit that never reached the target: CDC would stop writing an ordinary
    column, silently, while Full Load's append path (the live catalog) wrote it."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    orders = _orders()
    state = SimpleNamespace(
        applied_conversions=lambda: (
            {"shop.orders": SimpleNamespace(target_ddl=_GENERATED_DDL)}, True
        )
    )
    monkeypatch.setattr(cdc_ui, "_cdc_tables_for_config", lambda *_a: [orders])
    catalog = {"shop.orders": {"id": "", "price": "", "qty": "", "total": ""}}
    broken: set = set()
    monkeypatch.setattr(
        cdc_ui, "_cdc_fk_connection_factory", lambda _s: _catalog_factory(catalog, broken)
    )
    mismatched = cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None)
    assert mismatched == [("shop.orders", ["total"], [])]
    _tone, _icon, header, body = cdc_ui._cdc_generated_mismatch_notice(mismatched)
    assert "differ from Schema Conversion" in header
    assert "total GENERATED in Schema Conversion but ordinary on the target" in body
    catalog["shop.orders"]["total"] = "s"
    assert cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None) == []
    broken.add("shop.orders")  # unreadable: never taken as agreeing
    assert cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None) == [
        ("shop.orders", ["total"], None)
    ]


def test_the_generated_column_reader_is_cached_by_ddl() -> None:
    """Every capture-set reading parsed each table's DDL again: 10-20x slower per pass."""
    from dsql_migrator.core.converter import _generated_columns, parse_target_generated_columns

    first = parse_target_generated_columns(_GENERATED_DDL, folded=True)
    hits = _generated_columns.cache_info().hits
    second = parse_target_generated_columns(_GENERATED_DDL, folded=True)
    assert first == second == {"total"}
    assert _generated_columns.cache_info().hits == hits + 1
    second.add("x")  # a fresh copy each call
    assert parse_target_generated_columns(_GENERATED_DDL, folded=True) == {"total"}


def test_every_interval_spelling_is_a_duration_target() -> None:
    """INTERVAL(6) read 'interval 6' and INTERVAL HOUR TO SECOND(6) made the DDL unreadable:
    a TIME remapped that way was loaded as a raw timedelta ('1 day 06:00:00') or refused."""
    from dsql_migrator.core.converter import parse_target_column_types
    from dsql_migrator.core.exporter import ValueConverter
    from dsql_migrator.core.validation_sql import _checksum_kind

    table = TableDef(
        name="app.shifts",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="worked", mysql_type="time(6)")],
        primary_key=["id"],
    )
    for spelling in ("INTERVAL(6)", "INTERVAL HOUR TO SECOND", "INTERVAL HOUR TO SECOND(6)"):
        ddl = f'CREATE TABLE "app"."shifts" ("id" INT NOT NULL, "worked" {spelling}, PRIMARY KEY ("id"))'
        types = parse_target_column_types(ddl)
        assert types["worked"] == "interval", spelling
        converter = ValueConverter(table, target_types=types)
        assert converter.convert_value("worked", timedelta(hours=30)) == "30:00:00"
        column = table.columns[1].model_copy(update={"target_type": spelling.lower()})
        assert _checksum_kind(column) == "duration", spelling


def test_a_time_of_24_00_00_validates_as_written() -> None:
    """DATE_FORMAT read a TIME as today plus the time, so 24:00:00 printed as 00:00:00 while
    the target printed 24:00:00 -- a mismatch no step could fix."""
    from dsql_migrator.core.validation_sql import _checksum_kind, _mysql_checksum_expr

    column = ColumnDef(name="closes_at", mysql_type="time")
    assert _checksum_kind(column) == "time"
    assert _mysql_checksum_expr(column).startswith("TIME_FORMAT(`closes_at`")


# --- G75: the review of G66 -------------------------------------------------------------


class _CatalogCursor:
    """One target catalog: ``{table: {column: attgenerated}}``; a name in ``broken``
    raises, as an unreadable catalog does."""

    def __init__(self, catalog, broken, queries):
        self._catalog, self._broken, self._queries, self._rows = catalog, broken, queries, []

    def execute(self, _sql, params):
        name = f"{params['schema']}.{params['table']}" if "schema" in params else params["table"]
        self._queries.append(name)
        if name in self._broken:
            raise RuntimeError("catalog read failed")
        columns = self._catalog.get(name)
        self._rows = [] if columns is None else [(c, g) for c, g in columns.items()] or [(None, None)]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


def _catalog_factory(catalog, broken=(), connects=None, queries=None):
    connects = [] if connects is None else connects
    queries = [] if queries is None else queries

    def connect():
        connects.append(1)
        return SimpleNamespace(
            cursor=lambda: _CatalogCursor(catalog, set(broken), queries), close=lambda: None
        )

    return connect


def test_the_bulk_generated_reader_uses_one_connection_and_tells_missing_from_unreadable() -> None:
    """One DSQL connect per table (IAM token + TLS) on every Start / Retry CDC, and a
    missing table read the same as an unreadable one."""
    from dsql_migrator.core.target_introspector import target_generated_columns_of

    connects: list = []
    read = target_generated_columns_of(
        ["shop.orders", "shop.items", "shop.gone", "shop.broken"],
        connection_factory=_catalog_factory(
            {"shop.orders": {"id": "", "total": "s"}, "shop.items": {"id": ""}},
            broken={"shop.broken"},
            connects=connects,
        ),
    )
    assert len(connects) == 1
    assert read.columns == {"shop.orders": {"total"}, "shop.items": set()}
    assert read.missing == ("shop.gone",)
    assert read.unreadable == ("shop.broken",)

    def refuse():
        raise RuntimeError("no target")

    down = target_generated_columns_of(["shop.orders"], connection_factory=refuse)
    assert down.unreadable == ("shop.orders",) and down.columns == {}


def _probe(monkeypatch, tables, catalog, applied, broken=()):
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    state = SimpleNamespace(applied_conversions=lambda: (applied, True))
    monkeypatch.setattr(cdc_ui, "_cdc_tables_for_config", lambda *_a: tables)
    monkeypatch.setattr(
        cdc_ui, "_cdc_fk_connection_factory", lambda _s: _catalog_factory(catalog, broken)
    )
    return cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None)


def test_a_column_only_the_target_computes_is_no_concern_of_cdc(monkeypatch) -> None:
    """A computed column the operator added in Schema Conversion (the source has none) was
    reported as 'GENERATED on the target but not in Schema Conversion', refusing every
    Start CDC with a remedy that changed nothing: CDC never sends a non-source column."""
    orders = _orders()
    ddl = _GENERATED_DDL.replace(
        ', PRIMARY KEY ("id"))',
        ', "name_key" TEXT GENERATED ALWAYS AS (lower(\'x\')) STORED, PRIMARY KEY ("id"))',
    )
    catalog = {"shop.orders": {"id": "", "price": "", "qty": "", "total": "s", "name_key": "s"}}
    applied = {"shop.orders": SimpleNamespace(target_ddl=ddl)}
    assert _probe(monkeypatch, [orders], catalog, applied) == []


def test_case_variant_columns_map_exactly_first(monkeypatch) -> None:
    """"Total" (GENERATED) beside "total" (ordinary): the last case-variant won the map, so
    the probe reported a mismatch both ways that no edit could clear."""
    items = TableDef(
        name="shop.items",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="Total", mysql_type="numeric(12,2)"),
            ColumnDef(name="total", mysql_type="numeric(12,2)"),
        ],
        primary_key=["id"],
    )
    ddl = (
        'CREATE TABLE "shop"."items" ("id" BIGINT NOT NULL, "Total" NUMERIC(12, 2) '
        'GENERATED ALWAYS AS ((total * 2)) STORED, "total" NUMERIC(12, 2), PRIMARY KEY ("id"))'
    )
    catalog = {"shop.items": {"id": "", "Total": "s", "total": ""}}
    applied = {"shop.items": SimpleNamespace(target_ddl=ddl)}
    assert _probe(monkeypatch, [items], catalog, applied) == []


def test_an_outside_built_target_that_computes_a_sent_column_is_refused(monkeypatch) -> None:
    """Only tables with a GENERATED column in Schema Conversion were read, so a target built
    outside the tool that computes the MySQL generated column `total` (ordinary in Schema
    Conversion) dead-lettered every change (428C9)."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    orders = _orders()
    orders.columns[3].generated = True
    orders.columns[3].generated_kind = "STORED"
    ordinary = _GENERATED_DDL.replace(" GENERATED ALWAYS AS ((price * qty)) STORED", "")
    catalog = {"shop.orders": {"id": "", "price": "", "qty": "", "total": "s"}}
    applied = {"shop.orders": SimpleNamespace(target_ddl=ordinary)}
    mismatched = _probe(monkeypatch, [orders], catalog, applied)
    assert mismatched == [("shop.orders", [], ["total"])]
    _tone, _icon, header, body = cdc_ui._cdc_generated_mismatch_notice(mismatched)
    assert "differ from Schema Conversion" in header
    assert "total GENERATED on the target but not in Schema Conversion" in body
    assert "DROP EXPRESSION" in body


def test_the_refusal_never_advises_a_replace_and_says_what_it_could_not_check(monkeypatch) -> None:
    """'Apply the table again (Replace)' empties a loaded table; on a PostgreSQL source
    'edit the DDL back' only silenced the check; an unreadable or missing table was told
    it 'differs'."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    orders = _orders()
    applied = {"shop.orders": SimpleNamespace(target_ddl=_GENERATED_DDL)}
    differs = [("shop.orders", ["total"], [])]
    _t, _i, _h, mysql_body = cdc_ui._cdc_generated_mismatch_notice(differs)
    assert "'Drop & reload'" in mysql_body and "would empty the loaded table" in mysql_body
    assert "edit the DDL back to ordinary if CDC should write it" in mysql_body
    _t, _i, _h, pg_body = cdc_ui._cdc_generated_mismatch_notice(differs, is_pg=True)
    assert "edit the DDL back to ordinary if CDC should write it" not in pg_body
    assert "never publishes a generated column" in pg_body

    broken = _probe(monkeypatch, [orders], {}, applied, broken={"shop.orders"})
    assert broken == [("shop.orders", ["total"], None)]
    _t, _i, header, body = cdc_ui._cdc_generated_mismatch_notice(broken)
    assert header == "The target's generated columns could not be verified"
    assert "check the target connection" in body and "Replace" not in body

    missing = _probe(monkeypatch, [orders], {}, applied)
    assert missing == [("shop.orders", ["total"], cdc_ui._TARGET_TABLE_MISSING)]
    _t, _i, header, body = cdc_ui._cdc_generated_mismatch_notice(missing)
    assert header == "A captured table does not exist on the target"
    assert "shop.orders does not exist on the target" in body
    assert "apply it from Schema Conversion" in body


def test_a_not_null_bit_array_blocks_cdc_like_an_unlogged_table() -> None:
    """The row said the connector STOPS for every captured table, yet stayed a WARN that
    called accepting it valid, 'every other table streams normally'. A DOMAIN over
    bit(n)[] was judged by its declared name and described as dropped from the event."""
    from dsql_migrator.core.prerequisites import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import (
        UNCARRYABLE_COLUMN_ARRIVES_NULL,
        PostgresCdcFacts,
        UncarryableCdcColumn,
        check_columns_replicable,
        uncarryable_column_texts,
    )

    flags = UncarryableCdcColumn(
        column="flags", declared_type="flagset", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=False, in_event=True,
    )
    detail, remediation = uncarryable_column_texts([flags])
    assert "STOPS the PostgreSQL source connector" in detail
    assert "23502" not in detail
    assert "every other table streams normally" not in remediation
    table = TableDef(
        name="app.devices",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="flags", mysql_type="flagset", nullable=False)],
        primary_key=["id"],
    )
    facts = PostgresCdcFacts(cdc_uncarryable_columns={"app.devices": [flags]})
    result = check_columns_replicable(table, facts)
    assert result.status is PrerequisiteStatus.FAIL and result.required

    nullable = UncarryableCdcColumn(
        column="flags", declared_type="flagset", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=True, in_event=True,
    )
    result = check_columns_replicable(
        table, PostgresCdcFacts(cdc_uncarryable_columns={"app.devices": [nullable]})
    )
    assert result.status is PrerequisiteStatus.WARN and not result.required
    assert "NULL on every row CDC delivers" in result.detail


def test_every_silent_column_loss_names_the_checksum() -> None:
    """A nullable bit(n)[] and a dropped column with a target DEFAULT lost the sentence
    naming Validation's CHECKSUM as the only surface that sees the loss."""
    from dsql_migrator.core.prerequisites_postgres import (
        UNCARRYABLE_COLUMN_ARRIVES_NULL,
        UncarryableCdcColumn,
        uncarryable_column_texts,
    )

    bits = UncarryableCdcColumn(
        column="flags", declared_type="bit(8)[]", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=True,
    )
    spans = UncarryableCdcColumn(
        column="spans", declared_type="interval[]", reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=False,
    )
    for found, defaulted in (([bits], ()), ([spans], ("spans",))):
        detail, _remediation = uncarryable_column_texts(found, defaulted=defaulted)
        assert "only a Validation CHECKSUM can see it" in detail, found


def test_the_428c9_backfill_follows_the_branch_taken() -> None:
    """'Keep it GENERATED -> Stop/Start' was followed by 'backfill ... after removing
    GENERATED from its target DDL' -- un-GENERATE it to backfill a column kept GENERATED."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import _ddl_runbook
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = SimpleNamespace(
        table="shop.orders", kind="generated-column", columns=[], codes=["428C9"]
    )
    runbook = _ddl_runbook([group], MigrationType.FULL_LOAD_AND_CDC, is_pg=False)
    assert "after removing GENERATED" not in runbook
    assert "If you made the column ordinary, first remove GENERATED" in runbook
    assert "if it stays GENERATED, reload it as it is" in runbook


def test_a_mysql_generated_expression_is_never_taken_for_a_preserved_one() -> None:
    """G69 stored MySQL's GENERATION_EXPRESSION where a PostgreSQL STORED expression goes,
    so every source-neutral reader took a MySQL STORED column for one Aurora DSQL computes:
    the CDC capture left it out (an ordinary target column, then stale)."""
    from dsql_migrator.core.converter import pg_preserved_generated_columns
    from dsql_migrator.core.introspector import enrich_columns
    from dsql_migrator.ui.data_migration._models import (
        cdc_excluded_columns,
        target_generated_capture_columns,
    )
    from tests.test_introspector import _FakeConnection

    tables = [TableDef(
        name="orders",
        columns=[ColumnDef(name="id", mysql_type="INT"), ColumnDef(name="total", mysql_type="INT")],
        primary_key=["id"],
    )]
    rows = [
        ("orders", "id", None, "auto_increment", "int", None, ""),
        ("orders", "total", None, "STORED GENERATED", "int", None, "(`id` * 2)"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)
    assert tables[0].columns[1].mysql_generated_expression == "(`id` * 2)"
    assert pg_preserved_generated_columns(tables[0]) == []
    assert target_generated_capture_columns(tables[0], None) == []
    assert cdc_excluded_columns({}, SourceInventory(tables=tables)) == {}
    dumped = tables[0].columns[0].model_dump()
    assert "mysql_generated_expression" not in dumped  # omitted when empty


def test_a_schema_without_a_generated_column_opens_no_connection(monkeypatch) -> None:
    """Every Start CDC would otherwise pay a DSQL connect (IAM token + TLS) for nothing."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    orders = _orders()
    ordinary = _GENERATED_DDL.replace(" GENERATED ALWAYS AS ((price * qty)) STORED", "")
    state = SimpleNamespace(
        applied_conversions=lambda: ({"shop.orders": SimpleNamespace(target_ddl=ordinary)}, True)
    )
    monkeypatch.setattr(cdc_ui, "_cdc_tables_for_config", lambda *_a: [orders])

    def no_connection(_session):
        raise AssertionError("no table to read")

    monkeypatch.setattr(cdc_ui, "_cdc_fk_connection_factory", no_connection)
    assert cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None) == []
