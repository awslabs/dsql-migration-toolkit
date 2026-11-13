"""A12: the matching phase of ``resolve_exclusion_rules`` is its own unit.

``_match_rules`` says what each parsed line matches -- tables, table/column pairs, the type
keys a type line resolved -- and the E2 misses that matching itself finds. It judges
nothing: no selection, no lock, no eligibility context reaches it. ``_TypeKeys`` is the
index it shares with the per-line status that follows, computed at most once and only when
something asks.
"""

from __future__ import annotations

import inspect

from dsql_migrator.core.exclusion_rules import (
    _match_rules,
    _TypeKeys,
    parse_rules,
    resolve_exclusion_rules,
)
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

TABLES = [
    TableDef(
        name=name,
        primary_key=["id"],
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="note", mysql_type="longtext"),
        ],
    )
    for name in ("shop.orders", "shop.audit_1")
]


def _universe():
    tables = {table.name: table for table in TABLES}
    order = list(tables)
    split = {name: tuple(name.split(".", 1)) for name in order}
    return tables, order, split


def _match(text, *, source_type=SourceType.MYSQL):
    rules, parse_errors = parse_rules(text)
    assert not parse_errors, parse_errors
    tables, order, split = _universe()
    return _match_rules(
        rules,
        [raw for raw in text.split("\n")],
        tables=tables,
        order=order,
        split=split,
        source_type=source_type,
        type_keys=_TypeKeys(tables, order, split, source_type),
    )


def test_it_reports_the_hits_of_every_kind_of_line() -> None:
    matched = _match(
        "exclude table shop.audit_*\n"
        "exclude shop.orders.note\n"
        "exclude type:longtext\n"
    )
    assert matched.tables == {1: ["shop.audit_1"]}
    assert matched.columns[2] == [("shop.orders", "note")]
    # A type line's hits are columns too, with the keys it resolved beside them.
    assert matched.columns[3] == [("shop.orders", "note"), ("shop.audit_1", "note")]
    assert matched.types == {3: {"longtext": "longtext"}}
    assert matched.errors == {}


def test_a_name_or_type_that_matches_nothing_is_the_miss_it_was() -> None:
    matched = _match("exclude table shop.nope\nexclude type:geometry\n")
    assert matched.tables == {} and matched.columns == {}
    assert [error.code for error in matched.errors[1]] == ["E2"]
    assert [error.code for error in matched.errors[2]] == ["E2"]


def test_it_judges_nothing_about_the_selection_or_the_locks() -> None:
    taken = set(inspect.signature(_match_rules).parameters)
    for judged in (
        "effective_selection",
        "rule_excluded_prev",
        "selection_lock_reason",
        "retick_overrides",
        "context_factory",
        "capture",
        "reselect_lock",
        "unselect_lock",
    ):
        assert judged not in taken, judged


def test_the_type_index_is_built_once_and_only_when_asked() -> None:
    tables, order, split = _universe()
    keys = _TypeKeys(tables, order, split, SourceType.MYSQL)
    first = keys()
    assert first is keys()  # memoized: one pass over every column
    assert first[("shop.orders", "note")] == "longtext"
    # A rule set with no type line never asks, so the index stays unbuilt.
    assert _match("exclude table shop.audit_*\n").types == {}


def test_the_resolution_still_reports_a_parse_error_before_the_match_error() -> None:
    # Line 1 cannot parse (E1); line 2 parses and matches nothing (E2). One list per line,
    # in the order the single pass produced them.
    resolution = resolve_exclusion_rules(
        "exclude\nexclude table shop.nope\n",
        migratable=TABLES,
        source_type=SourceType.MYSQL,
        effective_selection=[table.name for table in TABLES],
    )
    assert [error.code for error in resolution.errors[1]] == ["E1"]
    assert [error.code for error in resolution.errors[2]] == ["E2"]
