# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-14 (core): the column-exclusion NULL proof in Validation -- target-only proof
columns on the ONE target pass that produces the row count, the isolated fallback, the
verdict policy and the report."""

import re
from typing import Any, Optional

import pytest

import dsql_migrator.core.validator as v
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
    ValidationMode,
)
from dsql_migrator.core.validation_sql import (
    build_pg_count_sql,
    build_pg_pk_first_page_sql,
    build_pg_pk_next_page_sql,
)
from dsql_migrator.core.validator import (
    ValidationCancelled,
    Validator,
    _NullProofAccumulator,
    _iter_target_pks,
    render_text_report,
    with_null_proof_metadata,
)

from tests.test_validator import (
    _SOURCE_CONFIG,
    _TARGET_CONFIG,
    _FakeSourceConnection,
    _FakeSourceEngine,
    _FakeTargetConnection,
    _keyset_page,
    _last_double_quoted_table,
    _limit_in,
    _table,
)

_FLAG = re.compile(r'\("((?:[^"]|"")+)" IS NOT NULL\)')
_COUNT_COL = re.compile(r'COUNT\("((?:[^"]|"")+)"\)')


class _Occ(Exception):
    sqlstate = "40001"


class _ProofTarget(_FakeTargetConnection):
    """The target fake, answering the proof flags / COUNT(c) from ``values``
    ({table: {column: pks holding a value}}); ``fail`` raises on proof statements."""

    def __init__(self, *, values=None, fail=None, fail_times=1, fail_after=0, **kw):
        super().__init__(**kw)
        self._values = values or {}
        self._fail = fail
        self._fail_left = fail_times
        self._fail_skip = fail_after

    def resolve(self, statement: Any, parameters: Any = None) -> object:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        flags = [c.replace('""', '"') for c in _FLAG.findall(text)]
        counted = [c.replace('""', '"') for c in _COUNT_COL.findall(text)]
        if (flags or counted) and self._fail is not None and self._fail_skip > 0:
            self._fail_skip -= 1
        elif (flags or counted) and self._fail is not None and self._fail_left > 0:
            self.executed.append(text)
            self._fail_left -= 1
            raise self._fail
        table = _last_double_quoted_table(text)
        if table in self._missing_tables:
            self.executed.append(text)
            raise RuntimeError(f'relation "{table}" does not exist')
        values = self._values.get(table, {})
        if flags:
            self.executed.append(text)
            params = parameters or {}
            page = _keyset_page(self._pk_sets.get(table, []), last=params.get("last"),
                                page=_limit_in(text))
            return [(pk,) + tuple(pk in values.get(c, ()) for c in flags) for pk, in page]
        if counted:
            self.executed.append(text)
            return (self._counts.get(table, 0),) + tuple(len(values.get(c, ())) for c in counted)
        return super().resolve(statement, parameters)


def _validator(source, target, *, overlay=None, legacy=None, blocks=True, page_size=2):
    engine = _FakeSourceEngine(source)
    return Validator(
        source_engine_factory=lambda _c: engine,
        target_connection_factory=lambda _t: target,
        reconcile_page_size=page_size,
        null_proof_columns=overlay,
        legacy_proof_columns=legacy,
        null_proof_blocks_match=blocks,
    )


def _proof_statements(target):
    return [s for s in target.executed if "IS NOT NULL" in s or 'COUNT("' in s]


def _target_pages(target, table="orders"):
    return [s for s in target.executed if f'"{table}"' in s and ("AS pk" in s or "COUNT(" in s)
            and "md5(" not in s and "tok" not in s]


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------


def test_the_page_and_count_sql_are_byte_identical_without_proof_columns():
    table = _table("orders")
    assert build_pg_pk_first_page_sql(table, "id", 7).as_string(None) == (
        'SELECT "id" AS pk FROM "orders" ORDER BY "id" LIMIT 7')
    assert build_pg_pk_next_page_sql(table, "id", 7).as_string(None) == (
        'SELECT "id" AS pk FROM "orders" WHERE "id" > %(last)s ORDER BY "id" LIMIT 7')
    assert build_pg_count_sql("app.orders").as_string(None) == 'SELECT COUNT(*) FROM "app"."orders"'


def test_the_proof_columns_are_quoted_identifiers():
    table = _table("orders")
    text = build_pg_pk_first_page_sql(table, "id", 7, ["summary", 'we"ird.x']).as_string(None)
    assert text == ('SELECT "id" AS pk, ("summary" IS NOT NULL), ("we""ird.x" IS NOT NULL) '
                    'FROM "orders" ORDER BY "id" LIMIT 7')
    assert build_pg_count_sql("orders", ["a"]).as_string(None) == 'SELECT COUNT(*), COUNT("a") FROM "orders"'


# ---------------------------------------------------------------------------
# Counting on every target count path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["reconcile", "keyset", "deep_only_equal", "deep_only_diff", "composite"])
def test_the_proof_counts_on_every_count_path(path):
    pks = [1, 2, 3, 4, 5]
    table = _table("orders", columns=("id", "k"), primary_key=("id", "k") if path == "composite" else ("id",))
    source = _FakeSourceConnection(counts={"orders": 5}, pk_sets={"orders": pks})
    target_pks = pks if path != "deep_only_diff" else pks[:4]
    target = _ProofTarget(counts={"orders": len(target_pks)}, pk_sets={"orders": target_pks},
                          values={"orders": {"c": {2, 4}, "d": set()}})
    report = _validator(source, target, overlay={"orders": ["c", "d"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [table],
        reconcile=path in ("reconcile", "deep_only_diff"),
        deep_only_on_count_mismatch=path.startswith("deep_only"),
    )
    item = report.items[0]
    assert item.proof_state == "complete" and item.proof_reason is None
    assert item.excluded_nonnull_counts == {"c": 2, "d": 0}
    if path == "deep_only_diff":
        # Only the up-front count carries the proof; the reconcile runs without it.
        assert item.reconcile is not None
        first = [s for s in target.executed if "IS NOT NULL" in s]
        assert first and all("AS pk, (" in s for s in first)
        assert any("AS pk FROM" in s for s in target.executed)  # reconcile's plain pages


def test_the_proof_adds_no_target_pass():
    for reconcile in (False, True):
        runs = []
        for overlay in (None, {"orders": ["c"]}):
            source = _FakeSourceConnection(counts={"orders": 5})
            target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {3}}})
            _validator(source, target, overlay=overlay).validate(
                _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=reconcile)
            runs.append(len(_target_pages(target)))
        assert runs[0] == runs[1] > 0


def test_the_checksum_pass_never_carries_the_proof():
    source = _FakeSourceConnection(counts={"orders": 3}, checksums={"orders": "7"})
    target = _ProofTarget(counts={"orders": 3}, checksums={"orders": "7"}, values={"orders": {"c": set()}})
    _validator(source, target, overlay={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM)
    assert not any("md5(" in s and "IS NOT NULL" in s for s in target.executed)


def test_the_pg_source_shim_never_carries_proof_columns(monkeypatch):
    table = _table("orders")
    pg_side = _ProofTarget(counts={"orders": 5})
    monkeypatch.setattr(v, "PgSourceConnection", lambda _conn: pg_side)
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": set()}})
    pg = SourceConnectionConfig(host="db.example.com", database="app", source_type=SourceType.POSTGRES)
    for reconcile in (False, True):
        report = _validator(source, target, overlay={"orders": ["c"]}).validate(
            pg, _TARGET_CONFIG, [table], reconcile=reconcile)
        assert report.items[0].proof_state == "complete", report.items[0]
    assert pg_side.executed and not _proof_statements(pg_side)
    assert _proof_statements(target)


def test_an_unexhausted_target_stream_is_not_determined():
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {1}}})
    acc = _NullProofAccumulator(columns=("c",), legacy=frozenset())
    stream = _iter_target_pks(target, _table("orders"), "id", 2, proof=acc)
    next(stream)
    stream.close()
    assert acc.complete is False
    assert Validator()._proof_fields(acc) == {"proof_state": "not_determined", "proof_reason": "incomplete"}


def test_a_cancel_is_never_absorbed_by_the_proof_fallback():
    acc = _NullProofAccumulator(columns=("c",), legacy=frozenset())

    def run(proof):
        if proof is not None:
            proof.failed = RuntimeError("x")
        raise ValidationCancelled("stop")

    with pytest.raises(ValidationCancelled):
        Validator._with_proof_fallback(run, acc)


# ---------------------------------------------------------------------------
# The isolated fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", ["keyset", "reconcile", "composite"])
def test_a_proof_query_error_reruns_without_the_proof_and_keeps_the_verdict(path):
    table = _table("orders", columns=("id", "k"), primary_key=("id", "k") if path == "composite" else ("id",))

    def run(overlay, fail):
        source = _FakeSourceConnection(counts={"orders": 5})
        target = _ProofTarget(counts={"orders": 4}, pk_sets={"orders": [1, 2, 3, 5]},
                              values={"orders": {"c": {1}}}, fail=fail, fail_times=99)
        report = _validator(source, target, overlay=overlay).validate(
            _SOURCE_CONFIG, _TARGET_CONFIG, [table], reconcile=path == "reconcile")
        return report.items[0], target

    plain, plain_target = run(None, None)
    item, target = run({"orders": ["c"]}, RuntimeError("column does not exist"))
    assert (item.target_row_count, item.row_count_match, item.matched, item.error) == (
        plain.target_row_count, plain.row_count_match, plain.matched, plain.error)
    assert item.reconcile == plain.reconcile
    assert (item.proof_state, item.proof_reason) == ("not_determined", "query_error")
    assert item.excluded_nonnull_counts == {} and not item.excluded_values_block
    # Exactly one failed proof statement, then one no-proof pass.
    assert len(_proof_statements(target)) == 1
    assert len(_target_pages(target)) == len(_target_pages(plain_target)) + 1


def test_a_stale_schema_40001_is_retried_once_with_the_proof():
    def run(times):
        source = _FakeSourceConnection(counts={"orders": 5})
        target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {2}}}, fail=_Occ(), fail_times=times)
        return _validator(source, target, overlay={"orders": ["c"]}).validate(
            _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]).items[0]

    once = run(1)
    assert once.proof_state == "complete" and once.excluded_nonnull_counts == {"c": 1}
    # A 40001 on a LATER page: the counts of the pages before it are reset, not doubled.
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {2}}}, fail=_Occ(), fail_after=1)
    late = _validator(source, target, overlay={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]).items[0]
    assert late.proof_state == "complete" and late.excluded_nonnull_counts == {"c": 1}
    twice = run(2)
    assert (twice.proof_state, twice.proof_reason) == ("not_determined", "query_error")
    assert twice.target_row_count == 5 and twice.error is None


def test_a_source_error_is_never_taken_for_a_proof_failure():
    # A transient source failure during reconcile: the table errors as today; it is not
    # re-run "without the proof".
    class _FlakySource(_FakeSourceConnection):
        failed = False

        def execute(self, statement, parameters=None):
            if "AS PK" in str(statement).upper() and not self.failed:
                self.failed = True
                raise RuntimeError("source read failed")
            return super().execute(statement, parameters)

    source = _FlakySource(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": set()}})
    item = _validator(source, target, overlay={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True).items[0]
    assert item.error is not None and (item.proof_state, item.proof_reason) == ("not_determined", "table_error")


def test_a_column_given_as_both_overlay_and_legacy_is_overlay():
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {1}}})
    item = _validator(source, target, overlay={"orders": ["c"]}, legacy={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]).items[0]
    assert item.excluded_values_block is True and item.legacy_nonnull_counts == {}
    assert sum(s.count("IS NOT NULL") for s in target.executed[:1]) == 1


def test_a_missing_target_table_still_errors_the_table():
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, missing_tables={"orders"})
    item = _validator(source, target, overlay={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]).items[0]
    assert item.error is not None and item.matched is False
    assert (item.proof_state, item.proof_reason) == ("not_determined", "table_error")


def test_a_table_without_proof_columns_carries_no_proof_state():
    source = _FakeSourceConnection(counts={"orders": 5, "other": 1})
    target = _ProofTarget(counts={"orders": 5, "other": 1}, missing_tables={"other"})
    report = _validator(source, target, overlay={"orders": ["c"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders"), _table("other")])
    other = [i for i in report.items if i.table == "other"][0]
    assert other.proof_state is None and other.proof_reason is None


# ---------------------------------------------------------------------------
# Verdict policy
# ---------------------------------------------------------------------------


def test_the_full_load_policy_fails_a_table_whose_excluded_column_holds_values():
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {1, 2, 3}}})
    report = _validator(source, target, overlay={"orders": ["c"]}, blocks=True).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")])
    item = report.items[0]
    assert item.row_count_match is True and item.matched is False and item.excluded_values_block is True
    assert item.excluded_nonnull_counts == {"c": 3}
    assert report.is_match is False


def test_the_cdc_only_policy_records_counts_without_flipping_matched():
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {1, 2, 3}}})
    report = _validator(source, target, overlay={"orders": ["c"]}, blocks=False).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")])
    item = report.items[0]
    assert item.matched is True and item.excluded_values_block is False
    assert item.excluded_nonnull_counts == {"c": 3} and report.is_match is True


def test_legacy_counts_are_information_never_a_mismatch():
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _ProofTarget(counts={"orders": 5}, values={"orders": {"old": {1, 2, 3, 4, 5}, "c": set()}})
    report = _validator(source, target, overlay={"orders": ["c"]}, legacy={"orders": ["old"]}).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")])
    item = report.items[0]
    assert item.matched is True and not item.excluded_values_block
    assert item.legacy_nonnull_counts == {"old": 5} and item.excluded_nonnull_counts == {"c": 0}


def test_values_in_an_excluded_column_are_not_explained_by_quarantined_rows():
    item = v.TableValidationResult(
        table="t", source_row_count=5, target_row_count=4, row_count_match=False, matched=False,
        rows_quarantined=1, excluded_values_block=True)
    assert item.deficit_explained_by_quarantine is False
    assert item.model_copy(update={"excluded_values_block": False}).deficit_explained_by_quarantine is True


def test_the_parallel_path_gives_identical_items():
    def run(workers):
        def _src(_cfg):
            return _FakeSourceEngine(_FakeSourceConnection(counts={"a": 3, "b": 4}))

        def _tgt(_cfg):
            return _ProofTarget(counts={"a": 3, "b": 4}, values={"a": {"c": {1}}, "b": {"c": set()}})

        return Validator(source_engine_factory=_src, target_connection_factory=_tgt, reconcile_page_size=2,
                         null_proof_columns={"a": ["c"], "b": ["c"]}, null_proof_blocks_match=True).validate(
            _SOURCE_CONFIG, _TARGET_CONFIG, [_table("a"), _table("b")], reconcile=True, max_workers=workers).items

    assert run(1) == run(2)


# ---------------------------------------------------------------------------
# Metadata and report
# ---------------------------------------------------------------------------


def _report(**items):
    source = _FakeSourceConnection(counts={"orders": 2, "docs": 2})
    target = _ProofTarget(counts={"orders": 2, "docs": 2}, values={"orders": {"c": {1}}, "docs": {"c": set()}})
    return _validator(source, target, **items).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders"), _table("docs")])


def test_absent_and_unknown_tables_are_stamped_without_touching_matched():
    report = _report(overlay={"orders": ["c"]}, blocks=False)
    stamped = with_null_proof_metadata(report, absent={"orders": ["gone"]}, unknown={"docs", "orders"})
    by = {i.table: i for i in stamped.items}
    assert by["orders"].proof_absent_columns == ["gone"]
    assert by["orders"].proof_state == "complete"  # a state the comparison recorded is kept
    assert (by["docs"].proof_state, by["docs"].proof_reason) == ("not_determined", "catalog_unreadable")
    assert [i.matched for i in stamped.items] == [i.matched for i in report.items]
    assert with_null_proof_metadata(report) is report


def test_the_text_report_names_the_proof():
    report = _report(overlay={"orders": ["c"], "docs": ["c"]}, blocks=True)
    report = v.with_migration_excluded_columns(report, {"orders": ["c"], "docs": ["c"]})
    text = render_text_report(report)
    assert "excluded VALUES: c=1" in text and "excluded NULL: proven" in text
    assert "HOLD VALUES (expected NULL): orders (c: 1 rows)" in text
    assert "NULL in every row (proven): docs (c)" in text
    undetermined = report.model_copy(update={"items": [
        report.items[0].model_copy(update={"proof_state": "not_determined", "proof_reason": "query_error",
                                           "excluded_nonnull_counts": {}})]})
    text = render_text_report(undetermined)
    assert "excluded proof: not determined" in text and "stopped" not in text


class _Lost(Exception):
    sqlstate = "08006"


def test_a_lost_connection_during_the_proof_is_retried_with_the_proof_never_without_it(monkeypatch):
    # A connection failure is not caused by the proof: dropping the proof would only lose
    # it (and could release a table whose excluded column holds values).
    monkeypatch.setattr(v.time, "sleep", lambda _s: None)
    attempts = v._TARGET_RECONNECT_MAX_ATTEMPTS

    def run(times):
        source = _FakeSourceConnection(counts={"orders": 5})
        target = _ProofTarget(counts={"orders": 5}, values={"orders": {"c": {1, 2}}}, fail=_Lost("reset"),
                              fail_times=times)
        return _validator(source, target, overlay={"orders": ["c"]}).validate(
            _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]).items[0]

    recovered = run(attempts)  # the reconnect budget of one pass, then it works
    assert recovered.proof_state == "complete" and recovered.excluded_values_block is True
    assert recovered.matched is False
    lost = run(99)
    assert lost.error is not None and lost.matched is False
    assert lost.proof_reason == "table_error"  # never "query_error" with a released table
