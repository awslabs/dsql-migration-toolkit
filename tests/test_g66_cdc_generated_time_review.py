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
    monkeypatch.setattr(cdc_ui, "_cdc_fk_connection_factory", lambda _s: (lambda: object()))
    live: dict = {"shop.orders": set()}
    monkeypatch.setattr(
        "dsql_migrator.core.target_introspector.target_generated_columns",
        lambda name, *, connection_factory: live[name],
    )
    mismatched = cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None)
    assert mismatched == [("shop.orders", ["total"], [])]
    _tone, _icon, header, body = cdc_ui._cdc_generated_mismatch_notice(mismatched)
    assert "differ from Schema Conversion" in header
    assert "total GENERATED in Schema Conversion but ordinary on the target" in body
    live["shop.orders"] = {"total"}
    assert cdc_ui._probe_cdc_generated_mismatch(state, object(), None, None) == []
    live["shop.orders"] = None  # unreadable: never taken as agreeing
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
