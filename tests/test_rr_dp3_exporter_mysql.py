# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DP-3: the MySQL-only value conversion lives in ``core/exporter_mysql.py``.

The twin of ``core/exporter_postgres.py``: ``ValueConverter`` and its helpers (``_utc``,
``_target_kind``, the ``VALUE_FIT_*`` verdicts, ``_BOOLEAN_SPEC``, ``_digits_beyond``,
``value_fit_rule``, ``_DURATION_TARGET_KINDS``, ``duration_text``) moved out of
``core/exporter.py``, which re-exports every one of them, private ones included. The two
error types the conversion raises (``ExportError``, ``ValueConversionError``) moved to the
leaf ``core/export_errors.py``, re-exported by the exporter too: the exporter imports the
source dialects, so a module the MySQL dialect imports at its top level cannot import the
exporter. The MySQL dialect now imports ``ExportError``, ``duration_text`` and
``ValueConverter`` at its top level (four lazy back-imports gone; the governor's
``_read_threads_running`` one stays, it is no value conversion), and Validation's copy of
``_DURATION_TARGET_KINDS`` (``core/validation_sql.py``) is the exporter's one set.

Pinned here:

* golden values: a MySQL table's conversion (every converted kind, zero dates, the
  passthrough columns, ``convert_row`` and ``row_converter``, both refusals),
  ``duration_text``, ``_target_kind``, ``_utc``, ``value_fit_rule`` and its constants, the
  MySQL dialect's converter / cursor values / walk errors, and Validation's duration
  classification -- all read through the old import paths;
* the old paths still give the very same objects (re-exports), the governor's default
  metric reader is unchanged, and an error still pickles;
* structure: the moved names are defined once, the new modules import neither the
  exporter nor the dialects at their top level, every import order works in a fresh
  interpreter, and the MySQL dialect's only exporter back-import is the governor's.

Pure: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import pathlib
import pickle
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from dsql_migrator.core import exporter as ex
from dsql_migrator.core import validation_sql as vsql
from dsql_migrator.core.converter import TargetValueSpec
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect import mysql as mysql_dialect_module

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"

_MOVED = (
    "_utc", "_target_kind", "VALUE_FIT_NOT_ZERO_OR_ONE", "VALUE_FIT_FRACTION",
    "VALUE_FIT_SCALE", "_BOOLEAN_SPEC", "_digits_beyond", "value_fit_rule",
    "_DURATION_TARGET_KINDS", "duration_text", "ValueConverter",
)
_MOVED_ERRORS = ("ExportError", "ValueConversionError")


def _table() -> TableDef:
    return TableDef(
        name="app.t",
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="flag", mysql_type="tinyint(1)"),
            ColumnDef(name="payload", mysql_type="blob"),
            ColumnDef(name="created", mysql_type="datetime(6)"),
            ColumnDef(name="updated", mysql_type="timestamp"),
            ColumnDef(name="t", mysql_type="time"),
            ColumnDef(name="dur", mysql_type="time(6)"),
            ColumnDef(name="dur_txt", mysql_type="time"),
            ColumnDef(name="b", mysql_type="bit(16)"),
            ColumnDef(name="d", mysql_type="date", nullable=True),
            ColumnDef(name="dn", mysql_type="date", nullable=False),
            ColumnDef(name="j", mysql_type="json"),
            ColumnDef(name="n", mysql_type="decimal(10,2)"),
        ],
        primary_key=["id"],
    )


_TARGETS = {"dur": "interval(6)", "dur_txt": "text"}

_ROW = {
    "id": 7, "flag": 1, "payload": bytearray(b"\x00\x01"),
    "created": datetime(2024, 5, 6, 7, 8, 9, 123456),
    "updated": datetime(2024, 5, 6, 7, 8, 9, tzinfo=timezone(timedelta(hours=9))),
    "t": timedelta(hours=23, minutes=59, seconds=59, microseconds=5),
    "dur": timedelta(days=1, hours=6, microseconds=500000),
    "dur_txt": timedelta(minutes=-30),
    "b": b"\x01\x02", "d": "0000-00-00", "dn": "2020-02-31", "j": '{"a": 1}',
    "n": Decimal("1.25"),
}

_CONVERTED = {
    "id": 7, "flag": True, "payload": b"\x00\x01",
    "created": datetime(2024, 5, 6, 7, 8, 9, 123456),
    "updated": datetime(2024, 5, 5, 22, 8, 9, tzinfo=timezone.utc),
    "t": time(23, 59, 59, 5), "dur": "30:00:00.5", "dur_txt": "-00:30:00", "b": 258,
    "d": None, "dn": date(1970, 1, 1), "j": '{"a": 1}', "n": Decimal("1.25"),
}


# ---------------------------------------------------------------------------
# Golden values, through the old import paths
# ---------------------------------------------------------------------------


def test_a_mysql_tables_conversion_is_unchanged() -> None:
    converter = ex.ValueConverter(_table(), target_types=_TARGETS)
    assert sorted(converter._kinds.items()) == [
        ("b", "int"), ("created", "timestamp"), ("d", "date"), ("dn", "date"),
        ("dur", "interval"), ("dur_txt", "text"), ("flag", "boolean"), ("id", "int"),
        ("j", "json"), ("n", "decimal"), ("payload", "bytea"), ("t", "time"),
        ("updated", "timestamptz"),
    ]
    assert sorted(converter._passthrough_columns) == ["id", "j", "n"]
    assert sorted(converter._duration_columns) == ["dur", "dur_txt"]
    assert sorted(converter._zero_dates) == ["created", "d", "dn", "updated"]
    converted = converter.convert_row(_ROW)
    assert converted == _CONVERTED
    assert type(converted["payload"]) is bytes and list(converted) == list(_ROW)
    names = list(_ROW)
    assert converter.row_converter(names)([_ROW[n] for n in names]) == _CONVERTED
    assert converter.zero_dates == {("d", "zero"): 2, ("dn", "invalid"): 2}
    assert converter.zero_date_entries() == (
        ("d", "zero", "NULL", 2), ("dn", "invalid", "1970-01-01", 2),
    )
    assert converter.convert_value("t", timedelta(hours=24)) == "24:00:00"
    assert converter.convert_value("flag", None) is None


@pytest.mark.parametrize("column, value, start", [
    ("flag", 2, "column 'flag' maps to DSQL boolean, but the source holds a value other"),
    ("t", timedelta(hours=25), "column 't' maps to DSQL time, but a source value is outside"),
])
def test_the_two_refusals_are_unchanged(column, value, start) -> None:
    converter = ex.ValueConverter(_table(), target_types=_TARGETS)
    with pytest.raises(ex.ValueConversionError) as raised:
        converter.convert_value(column, value)
    assert str(raised.value).startswith(start)
    assert isinstance(raised.value, ex.ExportError)
    assert isinstance(raised.value, RuntimeError)
    # A worker process's error crosses back pickled.
    again = pickle.loads(pickle.dumps(raised.value))
    assert type(again) is ex.ValueConversionError and str(again) == str(raised.value)


def test_the_helpers_are_unchanged() -> None:
    assert [ex.duration_text(v) for v in (
        timedelta(0), timedelta(minutes=-30), timedelta(days=2, microseconds=10),
        timedelta(hours=-838, minutes=-59, seconds=-59),
    )] == ["00:00:00", "-00:30:00", "48:00:00.00001", "-838:59:59"]
    assert [ex._target_kind(t) for t in (
        "tinyint(1)", "datetime(6)", "timestamp", "time", "bit(1)", "blob", "json",
        "decimal(10,2)", "bogus type(",
    )] == ["boolean", "timestamp", "timestamptz", "time", "smallint", "bytea", "json",
           "decimal", "bogus type("]
    assert ex._utc(datetime(2024, 1, 1)) == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert ex._utc(datetime(2024, 1, 1, tzinfo=timezone(timedelta(hours=-5)))) == datetime(
        2024, 1, 1, 5, tzinfo=timezone.utc)
    assert [ex._digits_beyond(Decimal(x), p) for x, p in (
        ("1.20", 1), ("1.21", 1), ("100", 0), ("1E+2", 0))] == [False, True, False, False]
    assert (ex.VALUE_FIT_NOT_ZERO_OR_ONE, ex.VALUE_FIT_FRACTION, ex.VALUE_FIT_SCALE) == (
        "not-0-or-1", "fraction", "scale")
    assert ex._BOOLEAN_SPEC == TargetValueSpec("boolean", 0, False, "boolean")
    assert ex._DURATION_TARGET_KINDS == frozenset(
        {"interval", "text", "varchar", "character varying", "char", "character", "bpchar"})


def test_value_fit_rule_is_unchanged() -> None:
    specs = [
        TargetValueSpec("boolean", 0, False, "boolean"),
        TargetValueSpec("integral", 0, False, "integer"),
        TargetValueSpec("numeric", 2, False, "numeric(10,2)"),
        TargetValueSpec("numeric", 6, True, "numeric"),
    ]
    values = [None, True, 0, 1, 2, 0.5, 1.0, Decimal("1.00"), Decimal("1.005"),
              Decimal("NaN"), Decimal("1.0000001"), float("nan")]
    n1, fr, sc = "not-0-or-1", "fraction", "scale"
    assert [[ex.value_fit_rule(v, s) for v in values] for s in specs] == [
        [None, None, None, None, n1, n1, None, None, n1, n1, n1, n1],
        [None, None, None, None, None, fr, None, None, fr, None, fr, None],
        [None, None, None, None, None, None, None, None, sc, None, sc, None],
        [None, None, None, None, None, None, None, None, None, None, sc, None],
    ]
    assert ex.value_fit_rule(Decimal("1.0000001"), specs[3], unconstrained_source=True) is None


def test_the_mysql_dialect_converts_and_walks_as_before() -> None:
    mysql = dialect_for(SourceType.MYSQL)
    converter = mysql.value_converter(_table(), target_types=_TARGETS)
    assert type(converter) is ex.ValueConverter
    assert converter.convert_row(_ROW) == _CONVERTED
    cursor = mysql_dialect_module._cursor_value
    assert cursor("time", timedelta(minutes=-30)) == "-00:30:00"
    assert cursor("bit(8)", b"\x01") == 1
    assert cursor("int", 5) == 5
    member = mysql_dialect_module._unknown_member("st", "enum")
    assert type(member) is ex.ExportError
    assert str(member).startswith("key column 'st' holds an ENUM value that is not one of")
    unreadable = mysql_dialect_module._unreadable_members("st", "why", "remedy")
    assert type(unreadable) is ex.ExportError
    assert str(unreadable) == (
        "key column 'st' why, so the read cannot write a cursor that keeps the key order "
        "and stops instead of loading the table with rows missing. remedy")


@pytest.mark.parametrize("applied, kind", [
    ("interval", "duration"), ("interval(6)", "duration"), ("interval hour to second", "duration"),
    ("text", "duration"), ("varchar(20)", "duration"), ("character varying", "duration"),
    ("char(8)", "duration"), ("character(8)", "duration"), ("bpchar", "duration"),
    ("time", "time"), (None, "time"),
])
def test_validations_duration_classification_is_unchanged(applied, kind) -> None:
    column = ColumnDef(name="t", mysql_type="time(6)", target_type=applied)
    assert vsql._checksum_kind(column) == kind
    assert vsql._checksum_kind(column, source_is_postgres=True) != "duration"


def test_the_governors_default_metric_reader_is_unchanged() -> None:
    import inspect

    default = inspect.signature(ex.SourceLoadGovernor).parameters["metric_reader"].default
    assert default is ex._read_threads_running


# ---------------------------------------------------------------------------
# Re-exports: the old paths give the very same objects
# ---------------------------------------------------------------------------


def test_every_moved_name_is_re_exported_by_the_exporter() -> None:
    from dsql_migrator.core import export_errors, exporter_mysql

    for name in _MOVED:
        assert getattr(ex, name) is getattr(exporter_mysql, name), name
    for name in _MOVED_ERRORS:
        assert getattr(ex, name) is getattr(export_errors, name), name
    assert ex.ValueConverter.__module__ == "dsql_migrator.core.exporter_mysql"
    assert issubclass(ex.UnsupportedPrimaryKeyError, export_errors.ExportError)
    assert issubclass(ex.ExportCancelled, export_errors.ExportError)
    # The MySQL dialect and Validation hold the same objects.
    assert mysql_dialect_module.ValueConverter is exporter_mysql.ValueConverter
    assert mysql_dialect_module.duration_text is exporter_mysql.duration_text
    assert mysql_dialect_module.ExportError is export_errors.ExportError
    assert vsql._DURATION_TARGET_KINDS is exporter_mysql._DURATION_TARGET_KINDS


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------


def _top_level_definitions() -> "dict[str, list[str]]":
    found: dict[str, list[str]] = {}
    for path in _SRC.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            names = []
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                names = [node.name]
            elif isinstance(node, ast.Assign):
                names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            for name in names:
                found.setdefault(name, []).append(path.relative_to(_SRC).as_posix())
    return found


def test_each_moved_name_is_defined_once() -> None:
    # Among the export / Validation / dialect modules (other modules have helpers of their
    # own named _utc, unrelated to this one).
    def ours(path: str) -> bool:
        return path.startswith(("core/export", "core/validation_sql", "core/validator",
                                "core/source_dialect/"))

    found = _top_level_definitions()
    for name in _MOVED:
        assert [p for p in found[name] if ours(p)] == ["core/exporter_mysql.py"], name
    for name in _MOVED_ERRORS:
        assert found[name] == ["core/export_errors.py"], (name, found[name])


def _top_level_imports(module: str) -> "set[str]":
    tree = ast.parse((_SRC / "core" / module).read_text(encoding="utf-8"))
    return {
        node.module for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name for node in tree.body if isinstance(node, ast.Import)
        for alias in node.names
    }


def test_the_new_modules_import_neither_the_exporter_nor_the_dialects() -> None:
    for module in ("exporter_mysql.py", "export_errors.py"):
        imports = _top_level_imports(module)
        assert "dsql_migrator.core.exporter" not in imports, module
        assert not any(name.startswith("dsql_migrator.core.source_dialect") for name in imports)
    assert not any(
        name.startswith("dsql_migrator") for name in _top_level_imports("export_errors.py"))


@pytest.mark.parametrize("module", [
    "dsql_migrator.core.source_dialect",
    "dsql_migrator.core.source_dialect.mysql",
    "dsql_migrator.core.exporter_mysql",
    "dsql_migrator.core.export_errors",
    "dsql_migrator.core.validation_sql",
    "dsql_migrator.core.exporter",
    "dsql_migrator.core.validator",
])
def test_every_import_order_works_in_a_fresh_interpreter(module: str) -> None:
    code = (
        f"import importlib; importlib.import_module({module!r}); "
        "import dsql_migrator.core.exporter as ex, dsql_migrator.core.exporter_mysql as em; "
        "assert ex.ValueConverter is em.ValueConverter; print('ok')"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"


def test_the_mysql_dialects_only_exporter_back_import_is_the_governors() -> None:
    tree = ast.parse((_SRC / "core" / "source_dialect" / "mysql.py").read_text(encoding="utf-8"))
    back = [
        alias.name for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "dsql_migrator.core.exporter"
        for alias in node.names
    ]
    assert back == ["_read_threads_running"]
