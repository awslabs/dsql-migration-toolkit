# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""X12: the P1 shard and block assignment is one pure ``_assign_group_blocks``.

On a PostgreSQL run the process-pool planner (``_migrate_tables_in_parallel``) plans a table
whose key ranges share the run's snapshot as one "group" unit, then reads each such table by
as many shards as its share of the run's estimated rows, each shard a contiguous block of the
table's ranges. That assignment -- the shards a table gets, their blocks, ``index`` and
``base`` (the table-wide index a range's batch ids and resume point are numbered from) and the
order of the units -- was inline in the planner. It is now the module-level, pure
``_assign_group_blocks``; the planner calls it and logs the "reader shards" lines it returns.

Pinned here:

* golden: the assignment of every plan below, captured before the extraction by executing the
  inline block of ``_migrate_tables_in_parallel`` at fdce80e on these same inputs;
* determinism: the same plan always gives the same assignment and leaves its inputs as they were;
* order and ties: a table's assignment does not depend on the order of the plan (a tie for an
  idle slot goes to the first name, a half share rounds to even), and its units take its place;
* blocks: over a grid, a table's shards read its ranges once, contiguously, in order;
* the planner: its "reader shards" lines in plan order through ``_migrate_tables_in_parallel``,
  as captured before the extraction; it reaches the helper and ``_largest_first`` (which the
  perf harness patches) through the module, and the helper reads, logs and stamps nothing;
* what the planner hands the helper: the tables it withheld, which leave their slots to a
  shared-snapshot table, and the start point's estimates, which size a retry's shares as they
  sized the run that took it (the plans pass on the engine before the extraction too).

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import inspect
import itertools

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g137_mysql_reader_shards import _table as _source_table
from tests.test_p1_key_range_spread import spread  # noqa: F401 - the planner on the inline pool

_M = 1_000_000
_READER_SHARDS = "DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS"


@pytest.fixture(autouse=True)
def _automatic(monkeypatch):
    """Every test starts with Reader shards unset (the default, automatic), as in
    tests/test_p1_key_range_spread.py, whose ``spread`` the planner runs below borrow: their
    expected shards and lines are the automatic split's. Whatever was set before the test is
    restored after it."""
    monkeypatch.setenv(_READER_SHARDS, "")  # records the value before the test ...
    monkeypatch.delenv(_READER_SHARDS)      # ... which the test then runs without


def _table(name: str) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"],
    )


def _ranges(k: int) -> tuple:
    """A table's K key ranges as the planning read returns them, open at both ends."""
    bounds = [None] + [i * 100 for i in range(1, k)] + [None]
    return tuple(zip(bounds, bounds[1:]))


# A plan, one entry per unit: ("group", t, K) a table whose K ranges share the run's snapshot
# (P1); ("table", t) a whole table; ("table", t, K) a whole table one reader session reads in
# its K ranges (P2); ("shard", t, i, K) the i-th of K ranges read from its own snapshot (G137).
PLANS = {
    "no-group": dict(
        plan=[("table", "app.a"), ("shard", "app.b", 0, 2), ("shard", "app.b", 1, 2),
              ("table", "app.c", 3)],
        estimates={"app.a": 5, "app.b": 50, "app.c": 9}, tp=4, withheld=()),
    "one-table-one-range-each": dict(
        plan=[("group", "app.events", 4)], estimates={"app.events": 10 * _M}, tp=4, withheld=()),
    "one-table-in-blocks": dict(
        plan=[("group", "app.events", 32)], estimates={"app.events": 100 * _M}, tp=4,
        withheld=()),
    "uneven-blocks": dict(
        plan=[("group", "app.events", 32)], estimates={"app.events": 100 * _M}, tp=3,
        withheld=()),
    "k-caps-the-shards": dict(
        plan=[("group", "app.events", 4)], estimates={"app.events": 10 * _M}, tp=16,
        withheld=()),
    "one-shard-is-one-session": dict(
        plan=[("group", "app.events", 4), ("table", "app.t0"), ("table", "app.t1"),
              ("table", "app.t2")],
        estimates={"app.events": 10 * _M, "app.t0": 20 * _M, "app.t1": 20 * _M,
                   "app.t2": 20 * _M},
        tp=4, withheld=()),
    "a-tie-goes-to-the-first-name": dict(
        plan=[("group", "app.c", 4), ("group", "app.a", 4), ("group", "app.b", 4)],
        estimates={"app.a": 10 * _M, "app.b": 10 * _M, "app.c": 10 * _M}, tp=4, withheld=()),
    "the-tie-in-another-order": dict(
        plan=[("group", "app.b", 4), ("group", "app.c", 4), ("group", "app.a", 4)],
        estimates={"app.a": 10 * _M, "app.b": 10 * _M, "app.c": 10 * _M}, tp=4, withheld=()),
    "a-half-share-rounds-to-even": dict(
        plan=[("group", "app.b", 8), ("group", "app.a", 8)],
        estimates={"app.a": 10 * _M, "app.b": 10 * _M}, tp=5, withheld=()),
    "a-withheld-table-frees-a-slot": dict(
        plan=[("group", "app.big", 12), ("table", "app.y"), ("table", "app.x")],
        estimates={"app.big": 30 * _M, "app.y": 30 * _M}, tp=4, withheld=("app.x",)),
    "nothing-withheld": dict(
        plan=[("group", "app.big", 12), ("table", "app.y"), ("table", "app.x")],
        estimates={"app.big": 30 * _M, "app.y": 30 * _M}, tp=4, withheld=()),
    "no-estimate-in-the-run": dict(
        plan=[("group", "app.a", 4), ("group", "app.b", 8)], estimates={}, tp=16, withheld=()),
    "a-table-without-an-estimate": dict(
        plan=[("group", "app.known", 8), ("group", "app.unknown", 8)],
        estimates={"app.known": 40 * _M}, tp=4, withheld=()),
    "a-zero-estimate": dict(
        plan=[("group", "app.empty", 4), ("group", "app.full", 4)],
        estimates={"app.empty": 0, "app.full": 10 * _M}, tp=4, withheld=()),
    "a-mixed-plan": dict(
        plan=[("table", "app.small"), ("group", "app.big", 16), ("table", "app.refused", 4),
              ("group", "app.mid", 4), ("table", "app.tiny")],
        estimates={"app.small": 1 * _M, "app.big": 60 * _M, "app.refused": 20 * _M,
                   "app.mid": 15 * _M},
        tp=8, withheld=("app.tiny",)),
}


def _plan(entries) -> list:
    """The planner's units for ``entries`` (see PLANS); a table's units share one TableDef."""
    tables: dict = {}
    units = []
    for kind, name, *rest in entries:
        table = tables.setdefault(name, _table(name))
        if kind == "group":
            (k,) = rest
            units.append(engine._Unit("group", table, ranges=_ranges(k)))
        elif kind == "shard":
            i, k = rest
            lo, hi = _ranges(k)[i]
            units.append(engine._Unit("shard", table, lo, hi, i))
        elif rest:
            (k,) = rest
            units.append(engine._Unit("table", table, ranges=_ranges(k)))
        else:
            units.append(engine._Unit("table", table))
    return units


def _ranges_of(entries) -> dict:
    """Each table's whole set of ranges, by name."""
    out: dict = {}
    for kind, name, *rest in entries:
        if kind in ("group", "table") and rest:
            out[name] = _ranges(rest[0])
        elif kind == "shard":
            out[name] = _ranges(rest[1])
    return out


def _encode(units, ranges_of) -> list:
    """Each unit as (kind, table, lo, hi, index, ranges, base), its ranges as the [start, stop)
    slice of its table's ranges they are (None for none). A unit whose ranges are not one
    contiguous slice of its table's fails here."""
    out = []
    for unit in units:
        span = None
        if unit.ranges:
            whole = ranges_of[unit.table.name]
            start = whole.index(unit.ranges[0])
            span = (start, start + len(unit.ranges))
            assert whole[span[0]:span[1]] == unit.ranges, unit
        out.append(
            (unit.kind, unit.table.name, unit.lo, unit.hi, unit.index, span, unit.base)
        )
    return out


def _assign(case: dict):
    """``_assign_group_blocks`` on a fresh copy of ``case``'s inputs, encoded."""
    units, parts, details = engine._assign_group_blocks(
        _plan(case["plan"]), dict(case["estimates"]), case["tp"], set(case["withheld"])
    )
    return {
        "units": _encode(units, _ranges_of(case["plan"])),
        "parts": list(parts.items()),
        "lines": list(details),
    }


# Captured before the extraction (fdce80e): the inline block of _migrate_tables_in_parallel,
# executed on each plan above with ``planned_parts`` empty and ``log_activity`` recorded.
# "lines" are its "reader shards" calls -- every one ActivityCategory.FULL_LOAD with
# status=ActivityStatus.INFO -- as (target, detail), in call order; "parts" is what it put
# in ``planned_parts``, in insertion order.
GOLDEN: dict = {
    "no-group": {
        "units": [
            ("table", "app.a", None, None, 0, None, 0),
            ("shard", "app.b", None, 100, 0, None, 0),
            ("shard", "app.b", 100, None, 1, None, 0),
            ("table", "app.c", None, None, 0, (0, 3), 0),
        ],
        "parts": [],
        "lines": [],
    },
    "one-table-one-range-each": {
        "units": [
            ("shard", "app.events", None, 100, 0, (0, 1), 0),
            ("shard", "app.events", 100, 200, 1, (1, 2), 1),
            ("shard", "app.events", 200, 300, 2, (2, 3), 2),
            ("shard", "app.events", 300, None, 3, (3, 4), 3),
        ],
        "parts": [("app.events", 4)],
        "lines": [("app.events", "read in 4 primary-key ranges sharing one snapshot")],
    },
    "one-table-in-blocks": {
        "units": [
            ("shard", "app.events", None, 800, 0, (0, 8), 0),
            ("shard", "app.events", 800, 1600, 1, (8, 16), 8),
            ("shard", "app.events", 1600, 2400, 2, (16, 24), 16),
            ("shard", "app.events", 2400, None, 3, (24, 32), 24),
        ],
        "parts": [("app.events", 4)],
        "lines": [
            ("app.events", "read in 32 primary-key ranges by 4 readers sharing one snapshot"),
        ],
    },
    "uneven-blocks": {
        "units": [
            ("shard", "app.events", None, 1000, 0, (0, 10), 0),
            ("shard", "app.events", 1000, 2100, 1, (10, 21), 10),
            ("shard", "app.events", 2100, None, 2, (21, 32), 21),
        ],
        "parts": [("app.events", 3)],
        "lines": [
            ("app.events", "read in 32 primary-key ranges by 3 readers sharing one snapshot"),
        ],
    },
    "k-caps-the-shards": {
        "units": [
            ("shard", "app.events", None, 100, 0, (0, 1), 0),
            ("shard", "app.events", 100, 200, 1, (1, 2), 1),
            ("shard", "app.events", 200, 300, 2, (2, 3), 2),
            ("shard", "app.events", 300, None, 3, (3, 4), 3),
        ],
        "parts": [("app.events", 4)],
        "lines": [("app.events", "read in 4 primary-key ranges sharing one snapshot")],
    },
    "one-shard-is-one-session": {
        "units": [
            ("table", "app.events", None, None, 0, (0, 4), 0),
            ("table", "app.t0", None, None, 0, None, 0),
            ("table", "app.t1", None, None, 0, None, 0),
            ("table", "app.t2", None, None, 0, None, 0),
        ],
        "parts": [],
        "lines": [
            ("app.events", "read in 4 primary-key ranges by one reader session (one snapshot)"),
        ],
    },
    "a-tie-goes-to-the-first-name": {
        "units": [
            ("table", "app.c", None, None, 0, (0, 4), 0),
            ("shard", "app.a", None, 200, 0, (0, 2), 0),
            ("shard", "app.a", 200, None, 1, (2, 4), 2),
            ("table", "app.b", None, None, 0, (0, 4), 0),
        ],
        "parts": [("app.a", 2)],
        "lines": [
            ("app.c", "read in 4 primary-key ranges by one reader session (one snapshot)"),
            ("app.a", "read in 4 primary-key ranges by 2 readers sharing one snapshot"),
            ("app.b", "read in 4 primary-key ranges by one reader session (one snapshot)"),
        ],
    },
    "the-tie-in-another-order": {
        "units": [
            ("table", "app.b", None, None, 0, (0, 4), 0),
            ("table", "app.c", None, None, 0, (0, 4), 0),
            ("shard", "app.a", None, 200, 0, (0, 2), 0),
            ("shard", "app.a", 200, None, 1, (2, 4), 2),
        ],
        "parts": [("app.a", 2)],
        "lines": [
            ("app.b", "read in 4 primary-key ranges by one reader session (one snapshot)"),
            ("app.c", "read in 4 primary-key ranges by one reader session (one snapshot)"),
            ("app.a", "read in 4 primary-key ranges by 2 readers sharing one snapshot"),
        ],
    },
    "a-half-share-rounds-to-even": {
        "units": [
            ("shard", "app.b", None, 400, 0, (0, 4), 0),
            ("shard", "app.b", 400, None, 1, (4, 8), 4),
            ("shard", "app.a", None, 200, 0, (0, 2), 0),
            ("shard", "app.a", 200, 500, 1, (2, 5), 2),
            ("shard", "app.a", 500, None, 2, (5, 8), 5),
        ],
        "parts": [("app.b", 2), ("app.a", 3)],
        "lines": [
            ("app.b", "read in 8 primary-key ranges by 2 readers sharing one snapshot"),
            ("app.a", "read in 8 primary-key ranges by 3 readers sharing one snapshot"),
        ],
    },
    "a-withheld-table-frees-a-slot": {
        "units": [
            ("shard", "app.big", None, 400, 0, (0, 4), 0),
            ("shard", "app.big", 400, 800, 1, (4, 8), 4),
            ("shard", "app.big", 800, None, 2, (8, 12), 8),
            ("table", "app.y", None, None, 0, None, 0),
            ("table", "app.x", None, None, 0, None, 0),
        ],
        "parts": [("app.big", 3)],
        "lines": [("app.big", "read in 12 primary-key ranges by 3 readers sharing one snapshot")],
    },
    "nothing-withheld": {
        "units": [
            ("shard", "app.big", None, 600, 0, (0, 6), 0),
            ("shard", "app.big", 600, None, 1, (6, 12), 6),
            ("table", "app.y", None, None, 0, None, 0),
            ("table", "app.x", None, None, 0, None, 0),
        ],
        "parts": [("app.big", 2)],
        "lines": [("app.big", "read in 12 primary-key ranges by 2 readers sharing one snapshot")],
    },
    "no-estimate-in-the-run": {
        "units": [
            ("table", "app.a", None, None, 0, (0, 4), 0),
            ("table", "app.b", None, None, 0, (0, 8), 0),
        ],
        "parts": [],
        "lines": [
            ("app.a", "read in 4 primary-key ranges by one reader session (one snapshot)"),
            ("app.b", "read in 8 primary-key ranges by one reader session (one snapshot)"),
        ],
    },
    "a-table-without-an-estimate": {
        "units": [
            ("shard", "app.known", None, 200, 0, (0, 2), 0),
            ("shard", "app.known", 200, 400, 1, (2, 4), 2),
            ("shard", "app.known", 400, 600, 2, (4, 6), 4),
            ("shard", "app.known", 600, None, 3, (6, 8), 6),
            ("table", "app.unknown", None, None, 0, (0, 8), 0),
        ],
        "parts": [("app.known", 4)],
        "lines": [
            ("app.known", "read in 8 primary-key ranges by 4 readers sharing one snapshot"),
            ("app.unknown", "read in 8 primary-key ranges by one reader session (one snapshot)"),
        ],
    },
    "a-zero-estimate": {
        "units": [
            ("table", "app.empty", None, None, 0, (0, 4), 0),
            ("shard", "app.full", None, 100, 0, (0, 1), 0),
            ("shard", "app.full", 100, 200, 1, (1, 2), 1),
            ("shard", "app.full", 200, 300, 2, (2, 3), 2),
            ("shard", "app.full", 300, None, 3, (3, 4), 3),
        ],
        "parts": [("app.full", 4)],
        "lines": [
            ("app.empty", "read in 4 primary-key ranges by one reader session (one snapshot)"),
            ("app.full", "read in 4 primary-key ranges sharing one snapshot"),
        ],
    },
    "a-mixed-plan": {
        "units": [
            ("table", "app.small", None, None, 0, None, 0),
            ("shard", "app.big", None, 300, 0, (0, 3), 0),
            ("shard", "app.big", 300, 600, 1, (3, 6), 3),
            ("shard", "app.big", 600, 900, 2, (6, 9), 6),
            ("shard", "app.big", 900, 1200, 3, (9, 12), 9),
            ("shard", "app.big", 1200, None, 4, (12, 16), 12),
            ("table", "app.refused", None, None, 0, (0, 4), 0),
            ("table", "app.mid", None, None, 0, (0, 4), 0),
            ("table", "app.tiny", None, None, 0, None, 0),
        ],
        "parts": [("app.big", 5)],
        "lines": [
            ("app.big", "read in 16 primary-key ranges by 5 readers sharing one snapshot"),
            ("app.mid", "read in 4 primary-key ranges by one reader session (one snapshot)"),
        ],
    },
}


@pytest.mark.parametrize("name", sorted(PLANS))
def test_the_assignment_is_the_one_captured_before_the_extraction(name) -> None:
    assert _assign(PLANS[name]) == GOLDEN[name]


def test_every_plan_has_a_golden_assignment() -> None:
    assert set(GOLDEN) == set(PLANS)


# ---------------------------------------------------------------------------
# Determinism, order and ties
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(PLANS))
def test_the_same_plan_always_gives_the_same_assignment(name) -> None:
    case = PLANS[name]
    assert _assign(case) == _assign(case)
    # The same objects twice: the same units, and the inputs are left as they were.
    plan, estimates = _plan(case["plan"]), dict(case["estimates"])
    withheld = set(case["withheld"])
    kept = (list(plan), dict(estimates), set(withheld))
    first = engine._assign_group_blocks(plan, estimates, case["tp"], withheld)
    second = engine._assign_group_blocks(plan, estimates, case["tp"], withheld)
    assert first == second
    assert (plan, estimates, withheld) == kept


def test_a_plan_without_a_group_comes_back_as_it_is() -> None:
    plan = _plan(PLANS["no-group"]["plan"])
    units, parts, details = engine._assign_group_blocks(plan, {"app.a": 5}, 4, set())
    assert units is plan and parts == {} and details == []


def _by_table(entries) -> list:
    """``entries`` cut into each table's run of consecutive entries, in plan order."""
    return [list(run) for _, run in itertools.groupby(entries, key=lambda entry: entry[1])]


@pytest.mark.parametrize("name", sorted(PLANS))
def test_a_tables_assignment_does_not_depend_on_the_order_of_the_plan(name) -> None:
    # Every order of the plan's tables (each table's own units stay together, as the planner
    # lists them): each table gets the units, part count and line it gets in the captured
    # order, and they stand where the table stands.
    case = PLANS[name]
    golden = GOLDEN[name]
    units_of: dict = {}
    for unit in golden["units"]:
        units_of.setdefault(unit[1], []).append(unit)
    parts_of = dict(golden["parts"])
    line_of = dict(golden["lines"])
    for order in itertools.permutations(_by_table(case["plan"])):
        plan = [entry for run in order for entry in run]
        names = [run[0][1] for run in order]
        got = _assign({**case, "plan": plan})
        assert got["units"] == [unit for n in names for unit in units_of[n]], names
        assert got["parts"] == [(n, parts_of[n]) for n in names if n in parts_of], names
        assert got["lines"] == [(n, line_of[n]) for n in names if n in line_of], names


def test_an_idle_slot_goes_to_the_first_name_and_a_half_share_rounds_to_even() -> None:
    # Three tables of one size at 4: a share of 4/3 each rounds to 1, the idle slot goes to
    # app.a, whatever the order.
    for name in ("a-tie-goes-to-the-first-name", "the-tie-in-another-order"):
        assert dict(_assign(PLANS[name])["parts"]) == {"app.a": 2}
    # Two of one size at 5: 2.5 each rounds to 2 (to even), the idle slot goes to app.a.
    assert dict(_assign(PLANS["a-half-share-rounds-to-even"])["parts"]) == {
        "app.a": 3, "app.b": 2}


# ---------------------------------------------------------------------------
# Blocks: a table's shards read its ranges once, contiguously, in order
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tp", [2, 3, 4, 5, 8, 16])
def test_a_tables_shards_read_its_ranges_once_in_order(tp) -> None:
    # Exhaustive over a small grid (no hypothesis in the venv): two group tables with 0, 1 or
    # 3 whole tables between them, sizes from 0 to 100M rows.
    for (ka, kb), (sa, sb), others in itertools.product(
        itertools.product((2, 3, 5, 8, 12, 32), repeat=2),
        itertools.product((0, 3 * _M, 10 * _M, 100 * _M), repeat=2),
        (0, 1, 3),
    ):
        others_ = [f"app.o{i}" for i in range(others)]
        entries = [("group", "app.a", ka), *[("table", n) for n in others_], ("group", "app.b", kb)]
        estimates = {"app.a": sa, "app.b": sb, **{n: 1 * _M for n in others_}}
        units, parts, details = engine._assign_group_blocks(
            _plan(entries), estimates, tp, set()
        )
        shape = (tp, ka, kb, sa, sb, others)
        mine = {n: [u for u in units if u.table.name == n] for n in ("app.a", *others_, "app.b")}
        # The plan's order, each group table's units where it stood.
        assert [u.table.name for u in units] == [n for n in mine for _ in mine[n]], shape
        assert [n for n, _ in details] == ["app.a", "app.b"], shape
        for name, k in (("app.a", ka), ("app.b", kb)):
            ranges = _ranges(k)
            got = mine[name]
            if len(got) == 1 and got[0].kind == "table":
                # One shard: one whole-table unit reading the table's ranges in turn (P2).
                assert got[0] == engine._Unit("table", got[0].table, ranges=ranges), shape
                assert name not in parts, shape
                continue
            g = len(got)
            assert {u.kind for u in got} == {"shard"} and parts[name] == g, shape
            assert 2 <= g <= min(k, tp), shape
            assert [u.index for u in got] == list(range(g)), shape
            assert sum((u.ranges for u in got), ()) == ranges, shape
            assert all(u.base == ranges.index(u.ranges[0]) for u in got), shape
            assert all((u.lo, u.hi) == (u.ranges[0][0], u.ranges[-1][1]) for u in got), shape
            sizes = [len(u.ranges) for u in got]
            assert max(sizes) - min(sizes) <= 1, shape
        assert set(parts) <= {"app.a", "app.b"}, shape


# ---------------------------------------------------------------------------
# The planner: its lines in plan order, the helper reached through the module
# ---------------------------------------------------------------------------


def _mixed_run(spread, monkeypatch):  # noqa: F811 - the fixture's run, passed in
    """A PostgreSQL run with Reader shards automatic at Tables in parallel 4: app.small (1M
    rows, whole), app.refused (20M: 8 ranges, its split refused by the guard, so one reader
    session), app.mid (15M: 6 ranges) and app.big (60M: 24 ranges), the last two sharing the
    run's snapshot -- in that order, which is not their names' order. Every activity call
    recorded."""
    said: list = []
    monkeypatch.setattr(engine, "log_activity", lambda category, action, **kw: said.append(
        (category, action, kw.get("status"), kw.get("target"), kw.get("detail"))))
    monkeypatch.setattr(engine, "_split_refusal", lambda migrator, table, lead, **kw: (
        "a test refusal" if table.name == "app.refused" else None))
    names = ["app.small", "app.refused", "app.mid", "app.big"]
    r = spread(
        [_source_table(n) for n in names],
        {"app.small": 1 * _M, "app.refused": 20 * _M, "app.big": 60 * _M, "app.mid": 15 * _M},
        source=SourceType.POSTGRES, binlog=False,
    )
    return r, [entry for entry in said if entry[1] == "reader shards"]


def test_the_planner_logs_its_reader_shard_lines_in_plan_order(
        spread, monkeypatch) -> None:  # noqa: F811
    # Captured before the extraction: the refused split's line from the planning loop, then
    # the two shared-snapshot tables' lines in plan order (app.mid before app.big) -- app.mid's
    # 0.625 share of 4 rounds to 1, app.big's 2.5 to 2 (to even), two other units: no idle slot.
    r, lines = _mixed_run(spread, monkeypatch)
    assert lines == [
        (ActivityCategory.FULL_LOAD, "reader shards", ActivityStatus.INFO, "app.refused",
         "read in 8 primary-key ranges by one reader session (one snapshot)"),
        (ActivityCategory.FULL_LOAD, "reader shards", ActivityStatus.INFO, "app.mid",
         "read in 6 primary-key ranges by one reader session (one snapshot)"),
        (ActivityCategory.FULL_LOAD, "reader shards", ActivityStatus.INFO, "app.big",
         "read in 24 primary-key ranges by 2 readers sharing one snapshot"),
    ]
    assert [(a.table.name, a.shard_index, a.range_base, len(a.ranges)) for a in r.shards] == [
        ("app.big", 0, 0, 12), ("app.big", 1, 12, 12)]
    assert {n: len(a.read_ranges) for n, a in r.wholes.items()} == {
        "app.refused": 8, "app.mid": 6, "app.small": 0}
    # R2: the largest per unit first (app.big's 30M a shard), its shards together.
    assert _InlineExecutor.submitted == [
        "app.big", "app.big", "app.refused", "app.mid", "app.small"]
    assert r.parts == {"app.small": None, "app.refused": None, "app.big": 2, "app.mid": None}
    assert r.anchors == 1


def test_the_planner_reaches_the_helper_and_largest_first_through_the_module(
        spread, monkeypatch) -> None:  # noqa: F811
    # A patch of either module global reaches the planner (scripts/measure_performance.py
    # patches engine._largest_first for its --inventory-order arm).
    seen: list = []
    helper, largest_first = engine._assign_group_blocks, engine._largest_first

    def _helper(work_units, estimates, tp, withheld):
        seen.append(("helper", [u.table.name for u in work_units], dict(estimates), tp,
                     set(withheld)))
        return helper(work_units, estimates, tp, withheld)

    def _largest(work_units, estimates):
        seen.append(("largest_first", [u.table.name for u in work_units]))
        return largest_first(work_units, estimates)

    monkeypatch.setattr(engine, "_assign_group_blocks", _helper)
    monkeypatch.setattr(engine, "_largest_first", _largest)
    _mixed_run(spread, monkeypatch)
    estimates = {"app.small": 1 * _M, "app.refused": 20 * _M, "app.big": 60 * _M,
                 "app.mid": 15 * _M}
    assert seen == [
        ("helper", ["app.small", "app.refused", "app.mid", "app.big"], estimates, 4, set()),
        ("largest_first", ["app.small", "app.refused", "app.mid", "app.big", "app.big"]),
    ]


def _function(name: str) -> ast.FunctionDef:
    tree = ast.parse(inspect.getsource(engine))
    (node,) = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    return node


def test_the_planner_calls_both_by_their_module_global_names() -> None:
    # Each is called once, by its bare name, which the planner never binds: a parameter's
    # default (bound once, at import) or a local import would not see a patch of the module's.
    planner = _function("_migrate_tables_in_parallel")
    calls = [n for n in ast.walk(planner) if isinstance(n, ast.Call)]
    for name in ("_assign_group_blocks", "_largest_first"):
        (call,) = [c for c in calls if ast.unparse(c.func) == name]
        assert isinstance(call.func, ast.Name)
    bound = {
        n.id for n in ast.walk(planner) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
    } | {a.arg for a in ast.walk(planner) if isinstance(a, ast.arg)} | {
        alias.asname or alias.name for n in ast.walk(planner)
        if isinstance(n, (ast.Import, ast.ImportFrom)) for alias in n.names
    }
    assert not bound & {"_assign_group_blocks", "_largest_first"}


def test_the_helper_reads_logs_and_stamps_nothing() -> None:
    # Pure: its only calls build the units and the line texts, and size the shards.
    called = {ast.unparse(c.func) for c in ast.walk(_function("_assign_group_blocks"))
              if isinstance(c, ast.Call)}
    assert called == {
        "_unit_counts", "_Unit", "len", "range", "sum", "planned.append", "details.append",
        "_SPLIT_ONE_SNAPSHOT.format", "_SPLIT_SHARED_SNAPSHOT.format",
        "_SPLIT_SHARED_SNAPSHOT_BLOCKS.format",
    }


# ---------------------------------------------------------------------------
# What the planner hands the helper: what it withheld, the start point's estimates
# ---------------------------------------------------------------------------

# Two names that agree in their first 63 bytes share one stored name on Aurora DSQL, so the run
# withholds both (BatchedTableMigrator finds them in the inventory: name_clash_tables): they are
# never recreated, split or loaded.
_CLASHING = ("app." + "c" * 63 + "1", "app." + "c" * 63 + "2")


def _beside(spread, others):  # noqa: F811 - the fixture's run, passed in
    """A PostgreSQL run at Tables in parallel 4: app.big (30M rows: 12 ranges) shares the run's
    snapshot, app.keyed (30M, a text key: one reader) does not, and ``others`` have no rows."""
    return spread(
        [_source_table("app.big"), _source_table("app.keyed", lead="text"),
         *[_source_table(name) for name in others]],
        {"app.big": 30 * _M, "app.keyed": 30 * _M, **dict.fromkeys(others, 0)},
        source=SourceType.POSTGRES, binlog=False,
    )


def _alone(spread, *, fresh):  # noqa: F811 - the fixture's run, passed in
    """app.events (100M rows: 32 ranges) alone, on PostgreSQL at Tables in parallel 4;
    ``fresh=False``: a retry, its start point captured by an earlier run."""
    return spread([_source_table("app.events")], {"app.events": 100 * _M},
                  source=SourceType.POSTGRES, binlog=False, fresh=fresh)


def _shards(r) -> list:
    """Each shard the run started: (table, index, base, how many ranges it reads)."""
    return [(a.table.name, a.shard_index, a.range_base, len(a.ranges)) for a in r.shards]


def test_a_table_the_planner_withheld_leaves_its_slot_to_a_shared_snapshot_table(
        spread) -> None:  # noqa: F811
    # app.big's share of the 4 processes is 2.0 beside app.keyed. The two clashing tables are
    # withheld and hold no slot, so the idle one goes to app.big: 3 shards of 4 ranges. Two
    # empty tables that do load hold one each, so none is idle: 2 shards of 6.
    withheld = _beside(spread, _CLASHING)
    assert {name: withheld.status[name] for name in _CLASHING} == dict.fromkeys(
        _CLASHING, "FAILED")
    assert set(withheld.wholes) == {"app.keyed"}
    assert _shards(withheld) == [("app.big", 0, 0, 4), ("app.big", 1, 4, 4), ("app.big", 2, 8, 4)]
    assert withheld.lines == [
        ("app.big", "read in 12 primary-key ranges by 3 readers sharing one snapshot")]
    loaded = _beside(spread, ("app.e1", "app.e2"))
    assert _shards(loaded) == [("app.big", 0, 0, 6), ("app.big", 1, 6, 6)]
    assert loaded.lines == [
        ("app.big", "read in 12 primary-key ranges by 2 readers sharing one snapshot")]


def test_a_retry_spreads_a_shared_snapshot_table_as_the_run_that_took_its_start_point(
        spread) -> None:  # noqa: F811
    # A retry's start point may be days old, so its estimates no longer spare a planning read
    # (the planner's _estimates is empty), but they still size the table's split and its share
    # of the run: all 4 processes, 4 shards of 8 ranges, not one reader session.
    for fresh in (True, False):
        r = _alone(spread, fresh=fresh)
        assert _shards(r) == [("app.events", g, 8 * g, 8) for g in range(4)], fresh
        assert r.lines == [
            ("app.events", "read in 32 primary-key ranges by 4 readers sharing one snapshot"),
        ], fresh
        assert r.wholes == {} and r.parts == {"app.events": 4}, fresh


def test_the_planner_hands_the_helper_what_it_withheld_and_its_start_points_estimates(
        spread, monkeypatch) -> None:  # noqa: F811
    # The two runs above, as the helper sees them: the tables the planner withheld, and the
    # start point's estimates of the tables that may start (_order_estimates) -- without the
    # withheld ones, which the fresh run's _estimates still holds (0 rows each), and also on a
    # retry, whose _estimates is empty.
    handed: list = []
    helper = engine._assign_group_blocks

    def _spy(work_units, estimates, tp, withheld):
        handed.append((dict(estimates), tp, set(withheld)))
        return helper(work_units, estimates, tp, withheld)

    monkeypatch.setattr(engine, "_assign_group_blocks", _spy)
    _beside(spread, _CLASHING)
    _alone(spread, fresh=False)
    assert handed == [
        ({"app.big": 30 * _M, "app.keyed": 30 * _M}, 4, set(_CLASHING)),
        ({"app.events": 100 * _M}, 4, set()),
    ]
