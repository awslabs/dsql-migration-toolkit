# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G156: what G150's zero-date advice left out.

* When to correct a day the month does not have: the advice said "before Start CDC", but
  CDC replays every binary-log change after its start point, which a Full load + CDC run
  records before it reads any table -- and the UPDATE that corrects such a row is itself a
  change whose before-image holds the date, so CDC stopped at it. The advice (the Full Load
  WARNING line, ZERO_DATES, chapter 6 section 6.2) now says to correct them before the start
  point is recorded, or else to take a newer one (Re-run Full Load with Drop & reload).
* Reads: a view or query that compares a date with the zero date (the soft-delete idiom
  ``deleted_at = '0000-00-00 00:00:00'``) was graded AUTO and carried verbatim, and Aurora
  DSQL rejects the literal (22008) -- the CREATE VIEW failed -- while the rows it meant now
  hold NULL, the DEFAULT or 1970-01-01. The conversion reports it (the view is MANUAL, the
  Query Converter's statement too), and ZERO_DATES and section 6.2 cover reads as well as
  writes.

Doubles only: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import ZERO_DATES_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, table_type_index
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)
from dsql_migrator.core.mysql_zero_dates import INVALID, ZERO, dateless_kind
from dsql_migrator.core.query_converter import QueryConverter
from dsql_migrator.ui.data_migration import _full_load_engine as engine


def _users() -> TableDef:
    return TableDef(
        name="shop.users",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="deleted_at", mysql_type="datetime", nullable=False,
                      default="0000-00-00 00:00:00"),
            ColumnDef(name="born", mysql_type="date"),
            ColumnDef(name="seen", mysql_type="timestamp"),
            ColumnDef(name="nick", mysql_type="varchar(20)"),
        ],
        primary_key=["id"],
    )


def _view(where: str) -> ViewDef:
    return ViewDef(
        name="shop.v",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`a`@`%` SQL SECURITY DEFINER VIEW `shop`.`v` AS "
            f"select `shop`.`users`.`id` AS `id` from `shop`.`users` where ({where})"
        ),
    )


# ---------------------------------------------------------------------------
# 1. A date Aurora DSQL has no value for, compared with a date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "kind"),
    [("0000-00-00", ZERO), ("0000-00-00 00:00:00", ZERO), ("2024-00-15", ZERO),
     ("0000-01-01", ZERO), ("2024-02-31", INVALID), ("2023-02-29 10:00:00", INVALID),
     ("2024-02-29", None), ("2024-12-31 23:59:59", None), ("N/A", None), ("", None)],
)
def test_a_dateless_date_is_told_by_its_text(text: str, kind) -> None:  # noqa: ANN001
    assert dateless_kind(text) == kind
    # A TIMESTAMP cannot hold a day the month does not have; its zero is still one.
    assert dateless_kind(text, timestamp=True) == (None if kind == INVALID else kind)


@pytest.mark.parametrize(
    "where",
    [
        "`shop`.`users`.`deleted_at` = '0000-00-00 00:00:00'",
        "`shop`.`users`.`deleted_at` <> '0000-00-00 00:00:00'",
        "`shop`.`users`.`born` in ('2024-01-01','0000-00-00')",
        "`shop`.`users`.`born` between '2024-00-15' and '2024-12-31'",
        "`shop`.`users`.`born` > '2024-02-31'",
        "`shop`.`users`.`seen` = '0000-00-00 00:00:00'",
    ],
)
def test_a_view_that_compares_a_date_with_one_is_manual_and_says_what_to_compare_with(
    where: str,
) -> None:
    table = _users()
    converted = SchemaConverter().convert_view(_view(where), tables=[table])
    (warning,) = converted.warnings
    assert warning.classification is Classification.MANUAL
    assert "Aurora DSQL rejects that literal (SQLSTATE 22008)" in warning.message
    assert "NULL, the column's DEFAULT or 1970-01-01" in warning.message
    assert "Compare with IS NULL or that value instead." in warning.message
    report = CompatibilityAssessor(source_type=SourceType.MYSQL).assess(
        SourceInventory(tables=[table], views=[_view(where)])
    )
    (item,) = [i for i in report.items if i.object_name == "shop.v"]
    assert item.classification is Classification.MANUAL
    assert [c.rule_id for c in item.concerns] == ["VIEW_NOT_CONVERTED"]


@pytest.mark.parametrize(
    "where",
    [
        "`shop`.`users`.`deleted_at` > '2024-02-29 10:00:00'",   # a real date
        "`shop`.`users`.`nick` = '0000-00-00'",                  # a string, not a date
        "`shop`.`users`.`born` is null",
    ],
)
def test_a_view_without_one_is_carried_as_before(where: str) -> None:
    converted = SchemaConverter().convert_view(_view(where), tables=[_users()])
    assert converted.warnings == [] and converted.auto_converted


def test_the_query_converter_reports_application_sql_comparing_with_one() -> None:
    converter = QueryConverter(column_types=tuple(table_type_index([_users()]).values()))
    result = converter.convert(
        "SELECT id FROM shop.users WHERE deleted_at = '0000-00-00 00:00:00'"
    )
    assert result.classification is Classification.MANUAL
    assert any("SQLSTATE 22008" in str(getattr(w, "message", w)) for w in result.warnings)
    plain = converter.convert("SELECT id FROM shop.users WHERE nick = '0000-00-00'")
    assert plain.classification is Classification.AUTO


# ---------------------------------------------------------------------------
# 2. When to correct a day the month does not have
# ---------------------------------------------------------------------------


def _zero_dates(sql_mode: str):
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_users()], server_settings={"sql_mode": sql_mode})
    )
    (item,) = [i for i in report.items if i.rule_id == ZERO_DATES_RULE_ID]
    return item


def test_zero_dates_says_to_correct_an_invalid_day_before_cdcs_start_point() -> None:
    rec = _zero_dates("ALLOW_INVALID_DATES").recommendation
    assert ("Correct every day the month does not have before CDC's start point is recorded "
            "-- the Full Load in Full load + CDC, Start CDC in CDC only") in rec
    assert "a correction made after that point is such a change" in rec
    assert "Re-run Full Load choosing Drop & reload" in rec
    assert "before CDC starts" not in rec
    # Without ALLOW_INVALID_DATES no such day gets in: no start-point sentence.
    assert "start point" not in _zero_dates("NO_ENGINE_SUBSTITUTION").recommendation


def test_zero_dates_covers_the_applications_reads_too() -> None:
    rec = _zero_dates("NO_ENGINE_SUBSTITUTION").recommendation
    assert "writes or compares with such a date" in rec
    assert "in an INSERT, an UPDATE or a WHERE (SQLSTATE 22008)" in rec
    assert "compare with IS NULL or that value instead" in rec


def test_the_warning_line_sends_a_full_load_plus_cdc_migration_to_a_newer_start_point(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lines: list = []
    monkeypatch.setattr(
        engine, "log_activity",
        lambda category, action, *, status, target, detail: lines.append(detail),
    )
    engine._record_zero_dates("app.orders", [("due", INVALID, "1970-01-01", 3)])
    (detail,) = lines
    assert "correcting such a row now is such a change" in detail
    assert "then Re-run Full Load choosing Drop & reload (a newer start point) before Start " \
           "CDC." in detail
    assert "correct those rows on the source before Start CDC" not in detail


# ---------------------------------------------------------------------------
# 3. The manual
# ---------------------------------------------------------------------------


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_CORRECTION_IS_A_CHANGE = {
    "en": "Correcting such a row is itself such a change",
    "ko": "그런 행을 고치는 것 자체가 그런 변경이므로",
    "ja": "そうした行の修正そのものがそうした変更であるため",
}
_READS = {
    "en": "or a `WHERE` (`22008`)",
    "ko": "`WHERE`에 들어온 그런 날짜를 거부하고",
    "ja": "`WHERE` のそうした日付を拒否し",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_when_to_correct_and_covers_reads(language: str) -> None:
    text = " ".join((_MANUAL / language / "06-limitations.md").read_text(encoding="utf-8").split())
    section = text.split("## 6.2", 1)[1].split("## 6.3", 1)[0]
    assert _CORRECTION_IS_A_CHANGE[language] in section
    assert "**Full load + CDC**" in section and "**CDC only**" in section
    assert "**Re-run Full Load**" in section and "Drop & reload" in section
    assert _READS[language] in section
    assert "`deleted_at = '0000-00-00 00:00:00'`" in section and "`IS NULL`" in section
