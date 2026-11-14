# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""JS-10: the CDC sink's three row-key equality rules name each other, and stay three.

- ``DebeziumEvents.sameKey``: did one update move its composite key (numbers by value, bytes
  by content); "same" only for keys Aurora DSQL sees as one row.
- ``DsqlSinkTask.rowIdentity``: can two moves share a group (also any number type, text
  without trailing spaces); the loosest, since a doubt only costs batching.
- exact ``pkValues()`` equality: ``DsqlSinkTask.dedupeRunByPk`` folds a run and
  ``DsqlSinkTask.replaces`` skips a superseded change only for equal values.

Each errs to its own safe side, so they are not merged; the javadoc of each names the others,
so a change to one meets them. This pins those cross-references.
"""

from __future__ import annotations

import pathlib
import re

import pytest

_CONNECT = (
    pathlib.Path(__file__).resolve().parents[1]
    / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect"
)


def _javadoc(file: str, signature: str) -> str:
    """The javadoc right above ``signature`` in ``file``, whitespace collapsed."""
    src = (_CONNECT / file).read_text(encoding="utf-8")
    end = src.index(signature)
    start = src.rindex("/**", 0, end)
    doc = src[start:end]
    assert doc.rstrip().endswith("*/"), doc  # nothing but the javadoc between the two
    return re.sub(r"\s*\*\s*", " ", re.sub(r"\s+", " ", doc))


@pytest.mark.parametrize(
    ("file", "signature", "names"),
    [
        (
            "DebeziumEvents.java",
            "static boolean sameKey(",
            ("DsqlSinkTask.rowIdentity", "DsqlSinkTask#dedupeRunByPk", "DsqlSinkTask.replaces"),
        ),
        (
            "DsqlSinkTask.java",
            "private static List<Object> rowIdentity(",
            ("DebeziumEvents#sameKey", "#dedupeRunByPk", "#replaces"),
        ),
        (
            "DsqlSinkTask.java",
            "static List<Applicable> dedupeRunByPk(",
            ("DebeziumEvents#sameKey", "#rowIdentity", "#replaces"),
        ),
        (
            "DsqlSinkTask.java",
            "private static boolean replaces(",
            ("DebeziumEvents#sameKey", "#rowIdentity", "#dedupeRunByPk"),
        ),
    ],
)
def test_each_row_key_rule_names_the_others(file: str, signature: str, names: tuple) -> None:
    doc = _javadoc(file, signature)
    assert "three row-key" in doc or "third row-key rule" in doc, doc
    for name in names:
        assert name in doc, (signature, name)


def test_the_three_rules_are_still_three() -> None:
    sink = (_CONNECT / "DsqlSinkTask.java").read_text(encoding="utf-8")
    events = (_CONNECT / "DebeziumEvents.java").read_text(encoding="utf-8")
    assert "static boolean sameKey(" in events
    assert "rowIdentity(first.sourceKeyValues())" in sink  # moveGroupEnd's rule
    assert "byPk.put(a.event().pkValues(), a);" in sink  # dedupeRunByPk: exact List.equals
    assert "!java.util.Objects.equals(le.pkValues(), fe.pkValues())" in sink  # replaces
