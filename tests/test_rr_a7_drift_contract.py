"""A7 (JS-2): the sink's per-table drift cool-down set and the control plane's drift map agree.

``DsqlSinkTask.isStructuralDrift`` holds "the drift kinds of core/cdc.py _DRIFT_BY_SQLSTATE":
the SQLSTATEs with which Aurora DSQL rejects a table's statement SHAPE, so every row of it fails
the same way and the table's later rows go one per transaction while every other table keeps
batching. G29 mapped 428C9 (a value sent for a GENERATED column) in Python and the Java switch
never followed, with nothing to notice it (sink plugin v55 adds it). This pins the two
together: every Java case is a mapped code, and every mapped code the Java set leaves out is
excluded on purpose with its reason, so a new code must be classified when it is added.
"""

from __future__ import annotations

import pathlib
import re

from dsql_migrator.core.cdc import _DRIFT_BY_SQLSTATE

_SINK_TASK = (
    pathlib.Path(__file__).resolve().parents[1]
    / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
)

# The mapped drift codes that do NOT start the sink's per-table cool-down, each with why.
_NOT_STRUCTURAL: tuple[tuple[str, str], ...] = (
    ("23505", "one row's conflict: an insert or update is held on its own partition first "
              "(DsqlSinkTask.holdOrReport), never a table-wide shape"),
    ("23514", "per value: a CHECK refuses one value (a new enum value), the next row may pass"),
    ("07006", "per value: pgjdbc cannot bind the value's Java type, at bind time"),
    ("22001", "per value: a string longer than its column (VALUE_DOES_NOT_FIT)"),
    ("22003", "per value: a number outside its column's range (VALUE_DOES_NOT_FIT)"),
    ("SOURCE_TRUNCATE", "not a SQLSTATE: the sink refuses a source TRUNCATE while parsing it, "
                        "before any write, so no Aurora DSQL rejection reaches the switch"),
    ("VALUE_DOES_NOT_FIT", "not a SQLSTATE: the sink's own pre-write value-fit refusal "
                           "(ValueFit), never an Aurora DSQL rejection"),
)


def _java_structural_codes() -> list[str]:
    """The case literals of ``isStructuralDrift``'s switch, comments removed."""
    source = _SINK_TASK.read_text(encoding="utf-8")
    start = source.index("static boolean isStructuralDrift(String sqlState) {")
    body = source[start : source.index("default -> false;", start)]
    body = re.sub(r"//[^\n]*", "", body)
    head = "switch (sqlState) {"
    switch = body[body.index(head) + len(head) : body.index("-> true;")]
    assert switch.strip().startswith("case "), switch
    return re.findall(r'"([^"]*)"', switch)


def test_every_structural_code_of_the_sink_is_a_mapped_drift_code() -> None:
    codes = _java_structural_codes()
    assert codes, "no case literal found in isStructuralDrift"
    assert len(codes) == len(set(codes)), codes
    assert set(codes) <= set(_DRIFT_BY_SQLSTATE), sorted(set(codes) - set(_DRIFT_BY_SQLSTATE))


def test_every_mapped_drift_code_is_structural_or_excluded_with_a_reason() -> None:
    structural = set(_java_structural_codes())
    excluded = dict(_NOT_STRUCTURAL)
    assert len(excluded) == len(_NOT_STRUCTURAL), "an exclusion is listed twice"
    # A code is classified exactly once: in the Java set or in the exclusions, never both.
    assert not structural & set(excluded), sorted(structural & set(excluded))
    unclassified = set(_DRIFT_BY_SQLSTATE) - structural - set(excluded)
    assert not unclassified, (
        f"{sorted(unclassified)}: add each to DsqlSinkTask.isStructuralDrift (a whole table's "
        "statement shape is rejected) or to _NOT_STRUCTURAL with the reason it is not"
    )
    # No stale exclusion: each names a code the map still has, with a one-line reason.
    for code, reason in _NOT_STRUCTURAL:
        assert code in _DRIFT_BY_SQLSTATE, code
        assert reason.strip() and "\n" not in reason, code


def test_a_value_for_a_generated_column_cools_the_table_down() -> None:
    # Owner decision (A7): 428C9 loses every insert and update of its table, like a missing
    # column, so it is structural; the cool-down ends on the table's first applied insert or
    # update (armDriftCooldown's `any` is false: only a delete or a missing table/schema sets it).
    assert "428C9" in _java_structural_codes()
    source = _SINK_TASK.read_text(encoding="utf-8")
    arm = source[source.index("private void armDriftCooldown(") :]
    arm = arm[: arm.index("\n  }\n")]
    assert 'boolean any = event.isDelete() || "42P01".equals(sqlState) || "3F000".equals(sqlState);' in arm
