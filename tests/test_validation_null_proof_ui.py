# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-14 (UI): the Validation NULL proof -- the candidate split, ONE live facts read per
run, the verdict policy, the notice / recovery / chip / log caveat."""

from types import SimpleNamespace

import pytest

from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import (
    AssessmentReport,
    ColumnDef,
    ReconcileResult,
    SourceConnectionConfig,
    SourceInventory,
    TableDef,
    TableValidationResult,
    TargetConnectionConfig,
    TargetInventory,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.core.secrets import SecretValue
from dsql_migrator.core.target_introspector import ColumnNullFacts, TableOverlayFacts
from dsql_migrator.ui import validation as uiv
from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
from dsql_migrator.ui.data_migration._models import RestoredExclusionFacts
from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.validation import (
    ValidationInputs,
    ValidationStore,
    build_validation_screen,
    cutover_release_state,
    excluded_values_remedy,
    live_proof_columns,
    null_proof_lines,
    proof_blocks_match,
    run_validation,
    summarize_validation,
    verdict_evidence_caveats,
)

from tests.test_ui_validation import _FakeValidator, _recheck_closure


def _col(not_null=False, default=None, identity=False, generated=False):
    return ColumnNullFacts(not_null=not_null, default_expr=default, identity=identity, generated=generated)


def _facts(columns, exists=True):
    return TableOverlayFacts(exists=exists, has_rows=None, primary_key=None, columns=columns)


def _inventory():
    def _t(name):
        return TableDef(name=name, primary_key=["id"], columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="body", mysql_type="longtext"),
            ColumnDef(name="old", mysql_type="longtext")])

    return SourceInventory(tables=[_t("app.docs"), _t("app.more"), _t("app.plain")])


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_live_proof_columns_follow_the_live_catalog():
    overlay = {"a": {"c", "gen", "gone"}, "b": {"c"}, "u": {"c"}, "x": {"c"}}
    legacy = {"a": {"old", "c"}}
    facts = {
        "a": _facts({"c": _col(), "gen": _col(generated=True), "gone": None, "old": _col(), "id": _col(identity=True)}),
        "b": _facts({"c": None}, exists=False),
        "u": None,
    }
    null, leg, absent, unknown = live_proof_columns(overlay, legacy, facts)
    assert null == {"a": ("c",)}  # the computed column is omit-only
    assert leg == {"a": ("old",)}  # an overlay column is never also legacy
    assert absent == {"a": ("gone",), "b": ("c",)}
    assert unknown == frozenset({"u", "x"})  # x: missing from the facts


@pytest.mark.parametrize("mtype,job_id,blocks", [
    (MigrationType.FULL_LOAD_ONLY, None, True),
    (MigrationType.FULL_LOAD_AND_CDC, None, True),
    (MigrationType.CDC_ONLY, None, False),
    (MigrationType.CDC_ONLY, "job-1", True),  # a Full Load wrote rows here anyway
])
def test_the_verdict_policy(mtype, job_id, blocks):
    assert proof_blocks_match(SimpleNamespace(migration_type=mtype, job_id=job_id)) is blocks


def test_the_validator_factory_hands_over_the_proof(monkeypatch):
    captured = {}
    monkeypatch.setattr(uiv, "Validator", lambda **kw: captured.update(kw))
    uiv._default_validator_factory(ValidationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"), source_password=SecretValue("pw"),
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        inventory=_inventory(), null_proof_columns={"app.docs": ("body",)},
        legacy_proof_columns={"app.docs": ("old",)}, null_proof_blocks_match=True))
    assert captured["null_proof_columns"] == {"app.docs": ("body",)}
    assert captured["legacy_proof_columns"] == {"app.docs": ("old",)}
    assert captured["null_proof_blocks_match"] is True


def test_run_validation_stamps_absent_and_unknown():
    items = [TableValidationResult(table=t, source_row_count=1, target_row_count=1, row_count_match=True,
                                   matched=True) for t in ("app.docs", "app.more")]
    report = run_validation(
        ValidationInputs(
            source_config=SourceConnectionConfig(host="db", database="app"), source_password=None,
            target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
            inventory=_inventory(), proof_absent_columns={"app.docs": ("gone",)},
            proof_unknown_tables=frozenset({"app.more"})),
        validator_factory=lambda _i: _FakeValidator(ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=items)),
        max_workers=1)
    by = {i.table: i for i in report.items}
    assert by["app.docs"].proof_absent_columns == ["gone"]
    assert (by["app.more"].proof_state, by["app.more"].proof_reason) == ("not_determined", "catalog_unreadable")


# ---------------------------------------------------------------------------
# The screen: split, one read per run, scoping, classification first
# ---------------------------------------------------------------------------


def _screen(*, exclude=(("app.docs", "body"), ("app.more", "body")), reader=None, mtype=MigrationType.FULL_LOAD_ONLY,
            legacy=(), pending=None, selected=None):
    sid = "s1"
    store, eval_store, migration_store, validation_store = (
        SessionStore(), EvaluationStore(), DataMigrationStore(), ValidationStore())
    manager = JobManager()
    session = store.get_or_create(sid)
    session.set_source(SourceConnectionConfig(host="db", database="app"), SecretValue("pw"))
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    session.set_source_verified(True)
    session.set_target_verified(True)
    eval_store.get_or_create(sid).set_result(EvaluationResult(
        inventory=_inventory(), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(), target_conflicts=[]))
    state = migration_store.get_or_create(sid)
    state.set_migration_type(mtype)
    if selected is not None:
        from dsql_migrator.core.models import TableSelection

        state.set_selection(TableSelection(selected_tables=list(selected)))
    for table, column in exclude:
        state.set_lob_exclusion(table, column, True)
    for table, column in legacy:
        state.restore_lob_exclusion(table, column)
    if legacy:
        state.restore_legacy_exclusions([f"{t}:{c}" for t, c in legacy])
    for table, column in (pending or ()):
        state.restore_lob_exclusion(table, column)
    reads, inputs_seen = [], []

    def _reader(names, columns):
        reads.append((list(names), {t: list(c) for t, c in columns.items()}))
        if reader is not None:
            return reader(names, columns)
        return {n: _facts({c: _col(not_null=False) for c in columns[n]}) for n in names}

    items = [TableValidationResult(table=t.name, source_row_count=1, target_row_count=1, row_count_match=True,
                                   matched=True) for t in _inventory().tables]

    def _factory(inputs):
        inputs_seen.append(inputs)
        return _FakeValidator(ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=items))

    content, runner = build_validation_screen(
        store, sid, job_manager=manager, eval_store=eval_store, migration_store=migration_store,
        validation_store=validation_store, validator_factory=_factory, sync_sequences=lambda *a, **k: {},
        overlay_facts_reader=_reader)
    return SimpleNamespace(content=content, runner=runner, state=validation_store.get_or_create(sid),
                           manager=manager, reads=reads, inputs=inputs_seen, migration_state=state)


def _run(s):
    s.runner()
    assert s.state.job_id is not None
    assert s.manager.wait(s.state.job_id, timeout=5.0) is True
    return s.inputs[-1]


def test_one_facts_read_per_validation_run_and_none_without_exclusions():
    s = _screen()
    inputs = _run(s)
    assert len(s.reads) == 1 and s.reads[0][0] == ["app.docs", "app.more"]
    assert inputs.null_proof_columns == {"app.docs": ("body",), "app.more": ("body",)}
    assert inputs.null_proof_blocks_match is True
    s0 = _screen(exclude=())
    inputs0 = _run(s0)
    assert s0.reads == [] and inputs0.null_proof_columns == {} and inputs0.proof_unknown_tables == frozenset()


def test_the_recheck_makes_its_own_single_read_over_its_tables():
    s = _screen()
    _run(s)
    recheck = _recheck_closure(s.content)
    recheck(["app.docs"])
    assert s.manager.wait(s.state.job_id, timeout=5.0) is True
    assert len(s.reads) == 2 and s.reads[1][0] == ["app.docs"]
    assert s.inputs[-1].null_proof_columns == {"app.docs": ("body",)}


def test_the_candidates_are_scoped_to_the_validated_tables():
    s = _screen(selected=["app.docs", "app.plain"])
    inputs = _run(s)
    assert s.reads[0][0] == ["app.docs"] and inputs.null_proof_columns == {"app.docs": ("body",)}


def test_absent_proof_columns_are_filtered_by_the_live_presence_read():
    s = _screen(reader=lambda names, columns: {"app.docs": _facts({"body": None}), "app.more": _facts({"body": _col()})})
    inputs = _run(s)
    assert inputs.null_proof_columns == {"app.more": ("body",)}
    assert inputs.proof_absent_columns == {"app.docs": ("body",)}


def test_an_unreadable_catalog_marks_the_proof_not_determined_and_enters_no_sql():
    def _boom(names, columns):
        raise RuntimeError("connect failed")

    s = _screen(reader=_boom)
    inputs = _run(s)
    assert inputs.null_proof_columns == {} and inputs.proof_unknown_tables == frozenset({"app.docs", "app.more"})
    by = {i.table: i for i in s.state.result.items}
    assert by["app.docs"].proof_state == "not_determined" and by["app.docs"].proof_reason == "catalog_unreadable"
    assert by["app.plain"].proof_state is None


def test_generated_or_identity_live_columns_are_not_proof_columns():
    s = _screen(reader=lambda names, columns: {
        "app.docs": _facts({"body": _col(generated=True)}), "app.more": _facts({"body": _col(identity=True)})})
    inputs = _run(s)
    assert inputs.null_proof_columns == {} and inputs.proof_absent_columns == {}


def test_legacy_entries_are_legacy_proof_columns():
    s = _screen(exclude=(("app.docs", "body"),), legacy=(("app.more", "old"),))
    inputs = _run(s)
    assert inputs.null_proof_columns == {"app.docs": ("body",)}
    assert inputs.legacy_proof_columns == {"app.more": ("old",)}


@pytest.mark.parametrize("job_id,legacy", [("gone", True), (None, False)])
def test_a_restored_pending_classification_is_settled_before_the_proof(job_id, legacy):
    s = _screen(exclude=(), pending=(("app.docs", "body"),))
    s.migration_state.set_restored_exclusion_facts(RestoredExclusionFacts(
        entries=frozenset({("app.docs", "body")}), job_id=job_id, connector_names=(), cdc_started=False,
        stack_possible=False))
    inputs = _run(s)
    assert s.migration_state.exclusion_classification_pending() is False
    if legacy:
        assert inputs.legacy_proof_columns == {"app.docs": ("body",)} and inputs.null_proof_columns == {}
    else:
        assert inputs.null_proof_columns == {"app.docs": ("body",)} and inputs.legacy_proof_columns == {}


def test_the_cdc_only_policy_reaches_the_inputs():
    inputs = _run(_screen(mtype=MigrationType.CDC_ONLY))
    assert inputs.null_proof_blocks_match is False


# ---------------------------------------------------------------------------
# Summary, notice, recovery, chip, caveat
# ---------------------------------------------------------------------------


def _item(table, *, counts=None, block=False, state="complete", reason=None, legacy=None, match=True, absent=(),
          excluded=("c",)):
    return TableValidationResult(
        table=table, source_row_count=5, target_row_count=5, row_count_match=match, matched=match and not block,
        migration_excluded_columns=list(excluded), excluded_nonnull_counts=counts or {},
        legacy_nonnull_counts=legacy or {}, proof_state=state, proof_reason=reason, excluded_values_block=block,
        proof_absent_columns=list(absent))


def _summary(*items):
    report = ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=list(items))
    return summarize_validation(report), report


def test_a_full_load_count_blocks_cut_over_with_an_error_notice():
    summary, _report = _summary(_item("a", counts={"c": 3}, block=True), _item("b", counts={"c": 0}))
    assert summary.excluded_values == {"a": {"c": 3}} and summary.null_proven == {"b": ("c",)}
    assert summary.excluded_values_only_tables == ("a",)
    assert cutover_release_state(summary) == "blocked"
    tone, header, lines = null_proof_lines(summary, "REMEDY")
    assert tone == "error" and "hold values" in header
    assert any("a: 3 row(s) hold a value in c (expected NULL)" in line for line in lines)
    assert "REMEDY" in lines
    assert any("1 column(s) in 1 table(s) are NULL in every row" in line for line in lines)


def test_a_cdc_only_count_warns_and_stays_ready():
    summary, _report = _summary(_item("a", counts={"c": 3}, block=False))
    assert summary.ready_for_cutover is True and summary.excluded_values_warned == {"a": {"c": 3}}
    tone, header, _lines = null_proof_lines(summary)
    assert tone == "warning" and "before CDC started" in header


def test_legacy_is_information_and_names_no_later_control():
    summary, _report = _summary(_item("a", counts={}, legacy={"old": 5}, excluded=("old",)))
    tone, _header, lines = null_proof_lines(summary)
    assert tone == "info" and summary.ready_for_cutover is True
    assert any("earlier version" in line and "target DEFAULT (5 non-NULL)" in line and "Start over" in line
               for line in lines)
    assert not any("Change & reload" in line or "Exclude tables" in line for line in lines)


@pytest.mark.parametrize("reason", ["query_error", "catalog_unreadable", "table_error", "incomplete"])
def test_not_determined_reason_texts_never_say_stopped(reason):
    summary, _report = _summary(_item("a", state="not_determined", reason=reason))
    _tone, _header, lines = null_proof_lines(summary)
    text = " ".join(lines)
    assert "not determined" in text and "stopped" not in text and "unknown reason" not in text


def test_the_absent_line():
    summary, _report = _summary(_item("a", counts={"c": 0}, absent=("gone",)))
    _tone, _header, lines = null_proof_lines(summary)
    assert any("a (gone): not on the Aurora DSQL table; nothing was written." in line for line in lines)


class _Ui:
    """A NiceGUI double that records notices and labels."""

    def __init__(self):
        self.texts = []

    def __getattr__(self, name):
        return self._element

    def _element(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            self.texts.append(args[0])
        return _Chain(self)


class _Chain:
    def __init__(self, ui):
        self._ui = ui

    def __getattr__(self, name):
        return lambda *a, **k: self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_the_recovery_names_drop_and_reload_and_not_backfill(monkeypatch):
    notices = []
    monkeypatch.setattr(uiv, "render_notice", lambda ui, **kw: notices.append(kw))
    summary, _report = _summary(_item("a", counts={"c": 3}, block=True))
    uiv._render_recovery_section(_Ui(), summary, uiv.format_drift(_report), excluded_values_remedy="R.")
    headers = [n["header"] for n in notices]
    assert any("Drop & reload" in h for h in headers)
    assert not any("backfill" in h.lower() for h in headers)
    assert "R." in notices[0]["body"]


def test_mixed_mismatches_render_both_recoveries(monkeypatch):
    notices = []
    monkeypatch.setattr(uiv, "render_notice", lambda ui, **kw: notices.append(kw))
    other = TableValidationResult(table="b", source_row_count=5, target_row_count=4, row_count_match=False,
                                  matched=False)
    summary, report = _summary(_item("a", counts={"c": 3}, block=True), other)
    uiv._render_recovery_section(_Ui(), summary, uiv.format_drift(report))
    headers = [n["header"] for n in notices]
    assert any("Drop & reload" in h for h in headers) and any("backfill" in h.lower() for h in headers)


def test_the_readiness_notice_and_check_detail(monkeypatch):
    notices, rows = [], []
    monkeypatch.setattr(uiv, "render_notice", lambda ui, **kw: notices.append(kw))
    monkeypatch.setattr(uiv, "_render_check_row", lambda ui, **kw: rows.append(kw))
    summary, report = _summary(_item("a", counts={"c": 3}, block=True))
    uiv._render_readiness_checks(_Ui(), summary, uiv.format_drift(report), excluded_values_remedy="R.")
    assert "(1 only because an excluded column holds values.)" in rows[0]["detail"]
    proof = [n for n in notices if n["header"] == "Excluded columns hold values on Aurora DSQL"]
    assert proof and proof[0]["tone"] == "error" and "R." in proof[0]["body"]


def test_the_chip_column_appears_only_with_proof_data():
    plain = ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=[TableValidationResult(
        table="a", source_row_count=1, target_row_count=1, row_count_match=True, matched=True)])
    captured = []

    class _TableUi(_Ui):
        def table(self, *, columns, rows, **kw):
            captured.append((columns, rows))
            return _Chain(self)

    uiv._render_tables(_TableUi(), plain)
    assert "excluded" not in [c["name"] for c in captured[-1][0]]
    _summary_, proof = _summary(_item("a", counts={"c": 2}, block=True), _item("b", counts={"c": 0}),
                                _item("n", state="not_determined", reason="query_error"),
                                _item("l", counts={}, legacy={"old": 1}))
    uiv._render_tables(_TableUi(), proof)
    columns, rows = captured[-1]
    assert "excluded" in [c["name"] for c in columns]
    cells = {r["table"]: r["excluded"] for r in rows}
    assert cells["a"]["text"] == "2 non-NULL" and cells["a"]["color"] == "red-6"
    assert cells["b"]["text"] == "NULL" and cells["n"]["text"] == "not determined"
    assert cells["l"]["text"] == "DEFAULT (legacy)"


def test_the_failure_reason_names_the_values():
    reasons = uiv._failure_reasons(_item("a", counts={"c": 3}, block=True))
    assert any("Excluded column(s) hold values" in r and "c (3 rows)" in r for r in reasons)


def test_the_log_caveat_carries_the_proof_state_and_stays_bounded():
    from dsql_migrator.core.activity_log import _MAX_DETAIL_CHARS

    items = [_item(f"t{i}", counts={"c": i % 2}, block=bool(i % 2)) for i in range(200)]
    summary, report = _summary(*items)
    caveat = verdict_evidence_caveats(summary, report)
    assert "VALUES found in 100 table(s)" in caveat and "NULL proven in 100 table(s)" in caveat
    assert len(caveat) < _MAX_DETAIL_CHARS


# ---------------------------------------------------------------------------
# The remedy matrix
# ---------------------------------------------------------------------------


class _Jobs:
    def __init__(self, known):
        self._known = known

    def get_status(self, job_id):
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id not in self._known:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(status="DONE", chunks=[])


@pytest.mark.parametrize("mtype,job_id,known,streaming,stopping,expect,absent", [
    (MigrationType.FULL_LOAD_ONLY, "j", {"j"}, False, False, "Reload specific tables", "Migration type"),
    # Stop CDC is enough (the excluded set is unchanged): never delete the infrastructure.
    (MigrationType.FULL_LOAD_AND_CDC, "j", {"j"}, True, False, "Stop CDC, then reload the table",
     "delete the CDC infrastructure"),
    (MigrationType.FULL_LOAD_AND_CDC, "j", {"j"}, True, True, "wait for the Stop CDC that is running",
     "delete"),
    (MigrationType.FULL_LOAD_ONLY, "j", set(), False, False, "Start over", "Reload specific"),
    (MigrationType.FULL_LOAD_AND_CDC, "j", set(), True, False, "Start over", "Stop CDC"),
    (MigrationType.CDC_ONLY, None, set(), False, False, "before CDC started", "Reload specific"),
    # CDC only has no Full Load step: the remedy names the type switch that shows it.
    (MigrationType.CDC_ONLY, "j", {"j"}, False, False, "Set Migration type to Full load + CDC", "Stop CDC"),
    (MigrationType.CDC_ONLY, "j", {"j"}, True, False, "Stop CDC, then set Migration type to Full load + CDC",
     "delete"),
])
def test_the_remedy_matrix(monkeypatch, mtype, job_id, known, streaming, stopping, expect, absent):
    import dsql_migrator.ui.data_migration._cdc_monitoring as cdc_monitoring
    import dsql_migrator.ui.data_migration._cdc_state as cdc_state

    monkeypatch.setattr(cdc_state, "cdc_streaming_started", lambda *_a, **_k: streaming)
    monkeypatch.setattr(cdc_monitoring, "cdc_stop_in_flight", lambda *_a, **_k: stopping)
    text = excluded_values_remedy(SimpleNamespace(migration_type=mtype, job_id=job_id), _Jobs(known))
    assert expect in text and absent not in text, text
    assert "Reload specific tables" in text or "Start over" in text or "before CDC started" in text


def test_values_only_plus_known_dropped_rows_never_advise_backfill(monkeypatch):
    notices, labels = [], []
    monkeypatch.setattr(uiv, "render_notice", lambda ui, **kw: notices.append(kw))
    dropped = TableValidationResult(table="q", source_row_count=5, target_row_count=4, row_count_match=False,
                                    matched=False, rows_quarantined=1)
    summary, report = _summary(_item("a", counts={"c": 3}, block=True), dropped)
    assert summary.quarantine_explained_tables == ("q",) and summary.excluded_values_only_tables == ("a",)
    ui = _Ui()
    uiv._render_recovery_section(ui, summary, uiv.format_drift(report))
    headers = [n["header"] for n in notices]
    assert any("Drop & reload" in h for h in headers)
    assert any("can't be stored as-is" in h for h in headers)
    assert not any("backfill" in h.lower() for h in headers)
    assert not any("Stop CDC" in t for t in ui.texts)


def test_values_in_an_excluded_column_block_a_quarantine_acceptance():
    item = _item("a", counts={"c": 1}, block=True, match=False)
    item = item.model_copy(update={"rows_quarantined": 0, "source_row_count": 5, "target_row_count": 5})
    summary, _report = _summary(item)
    assert summary.quarantine_explained_tables == () and cutover_release_state(summary) == "blocked"


def test_a_reconcile_inconsistency_is_not_values_only():
    item = _item("a", counts={"c": 3}, block=True).model_copy(update={"reconcile": ReconcileResult(
        pk_column="id", source_count=5, target_count=5, missing_on_target=1, extra_on_target=1,
        consistent=False)})
    summary, _report = _summary(item)
    assert summary.excluded_values == {"a": {"c": 3}} and summary.excluded_values_only_tables == ()
