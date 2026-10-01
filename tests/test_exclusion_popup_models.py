"""P3-C11: the exclusion popup's pure composition over live inputs."""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.models import ColumnDef, SourceType, TableDef
from dsql_migrator.ui.data_migration import DataMigrationState
from dsql_migrator.ui.data_migration._models import (
    ExclusionInputs,
    LobExclusionCandidate,
    exclusion_drift,
    exclusion_row_summary,
    open_lob_candidates,
    popup_open_text,
    popup_resolution,
    quarantine_rule_suggestion,
)

MY = SourceType.MYSQL


def _t(name, **cols):
    return TableDef(name=name, primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="bigint")]
                    + [ColumnDef(name=c, mysql_type=t) for c, t in cols.items()])


INV = SimpleNamespace(tables=[
    _t("shop.t", a="longtext", b="mediumtext", j="json"),
    _t("shop.u", note="text"),
    _t("shop.audit_1", x="text"),
])
NAMES = [t.name for t in INV.tables]


def _inputs(**kw):
    base = dict(selected=list(NAMES), applied={}, preserve_foreign_keys=True,
                migratable=list(NAMES), signature=("sig",))
    base.update(kw)
    return ExclusionInputs(**base)


# --- quarantine_rule_suggestion (plan tests 2069-2075) --------------------------------

CANDS = (("a", "longtext"), ("b", "mediumtext"))


def test_text_reason_with_a_excluded_suggests_b_only():
    s = quarantine_rule_suggestion("shop.t", "datatype limit ... not supported for text", CANDS,
                                   open_excluded={"a"}, saved_excluded={"a"})
    assert s.outcome == "A"
    assert s.lines == ("exclude shop.t.b  # from the Full Load quarantine",)


def test_json_reason_with_the_only_longtext_saved_excluded_is_c():
    s = quarantine_rule_suggestion("shop.t", "... for jsonb", (("a", "longtext"),),
                                   open_excluded={"a"}, saved_excluded={"a"})
    assert (s.outcome, s.lines) == ("C", ())


def test_json_reason_with_an_open_longtext_is_b_and_not_saved_when_only_unsaved_excludes():
    s = quarantine_rule_suggestion("shop.t", "... for jsonb", (("a", "longtext"),),
                                   open_excluded=set(), saved_excluded=set())
    assert (s.outcome, s.lines, s.filter_table) == ("B", (), True)
    s = quarantine_rule_suggestion("shop.t", "... for jsonb", (("a", "longtext"),),
                                   open_excluded={"a"}, saved_excluded=set(),
                                   open_excluded_by={"a": (3,)})
    assert (s.outcome, s.filter_table, s.not_saved_lines) == ("B", False, (3,))


def test_an_unsaved_or_pending_line_gives_not_saved_naming_it():
    s = quarantine_rule_suggestion("shop.t", "... for text", (("a", "longtext"),),
                                   open_excluded={"a"}, saved_excluded=set(),
                                   open_excluded_by={"a": (3,)})
    assert (s.outcome, s.lines, s.not_saved_lines) == ("NOT_SAVED", (), (3,))


def test_an_include_kept_column_returns_its_line_and_no_rule():
    s = quarantine_rule_suggestion("shop.t", "... for text", CANDS, open_excluded=set(),
                                   saved_excluded=set(), include_keepers={"a": (5,)})
    assert s.outcome == "A" and s.kept == {"a": (5,)}
    assert s.lines == ("exclude shop.t.b  # from the Full Load quarantine",)


def test_section_four_rows_equal_open_lob_candidates():
    cands = [LobExclusionCandidate("shop.t", ("a", "b")), LobExclusionCandidate("shop.u", ("note",))]
    assert open_lob_candidates(cands, {"shop.t": {"a"}, "shop.u": {"note"}}) == [
        LobExclusionCandidate("shop.t", ("b",))]
    assert open_lob_candidates(CANDS, {"a"}) == (("b", "mediumtext"),)


# --- the memo ------------------------------------------------------------------------


def test_the_memo_hits_on_identical_inputs_and_misses_on_each_component():
    state = DataMigrationState()
    inputs = _inputs()
    first = popup_resolution(state, INV, "exclude shop.t.a", inputs=inputs, source_type=MY)
    assert popup_resolution(state, INV, "exclude shop.t.a", inputs=inputs, source_type=MY) is first
    variants = [
        dict(inputs=_inputs(signature=("other",))),                       # target/DDL refresh
        dict(inputs=_inputs(applied={})),                                  # a new applied set
        dict(inputs=_inputs(live_columns={"shop.t": frozenset()})),       # live columns
        dict(selection_lock_reason="Locked."),                             # lock change
        dict(source_type=SourceType.POSTGRES),                             # engine
    ]
    for kw in variants:
        args = dict(inputs=inputs, source_type=MY)
        args.update(kw)
        again = popup_resolution(state, INV, "exclude shop.t.a", **args)
        assert again is not first, kw
        popup_resolution(state, INV, "exclude shop.t.a", inputs=inputs, source_type=MY)
    state.cdc_message_key_columns = lambda: {"shop.t": ["a", "id"]}  # re-key change
    assert popup_resolution(state, INV, "exclude shop.t.a", inputs=inputs, source_type=MY) is not first
    state2 = DataMigrationState()
    base = popup_resolution(state2, INV, "exclude shop.t.a", inputs=inputs, source_type=MY)
    state2.restore_lob_exclusion("shop.t", "a")
    state2.restore_legacy_exclusions(["shop.t:a"])                       # legacy change
    assert popup_resolution(state2, INV, "exclude shop.t.a", inputs=inputs, source_type=MY) is not base
    second = popup_resolution(state2, INV, "exclude shop.t.a", inputs=inputs, source_type=MY)
    state2.restore_exclusion_rules(None, ["shop.audit_1"])               # provenance
    third = popup_resolution(state2, INV, "exclude shop.t.a", inputs=inputs, source_type=MY)
    assert third is not second and third.rule_excluded_after == frozenset()


def test_the_open_text_is_the_rules_plus_pending_lines():
    state = DataMigrationState()
    state.restore_lob_exclusion("shop.t", "a")
    assert popup_open_text(state, INV) == ("exclude shop.t.a", [])
    state.add_pending_rule("exclude shop.t.b  # from the Full Load quarantine")
    text, pending = popup_open_text(state, INV)
    assert text.splitlines() == ["exclude shop.t.a", "exclude shop.t.b  # from the Full Load quarantine"]
    assert pending == ["exclude shop.t.b  # from the Full Load quarantine"]


# --- drift ---------------------------------------------------------------------------


def test_drift_reports_a_refresh_addition_and_not_a_retick():
    state = DataMigrationState()
    inputs = _inputs()
    res = popup_resolution(state, INV, "exclude shop.*.note", inputs=inputs, source_type=MY)
    assert state.set_column_exclusions(res, text="exclude shop.*.note") is None
    assert exclusion_drift(state, popup_resolution(
        state, INV, "exclude shop.*.note", inputs=inputs, source_type=MY)) is None
    grown = SimpleNamespace(tables=INV.tables + [_t("shop.v", note="text")])
    again = popup_resolution(state, grown, "exclude shop.*.note",
                             inputs=_inputs(selected=NAMES + ["shop.v"], migratable=NAMES + ["shop.v"]),
                             source_type=MY)
    drift = exclusion_drift(state, again)
    assert drift is not None and drift.added == (("shop.v", "note"),)
    # a re-ticked rule-excluded table is an override, not drift
    text = "exclude table shop.audit_*"
    res = popup_resolution(state, INV, text, inputs=inputs, source_type=MY)
    assert state.set_column_exclusions(res, text=text) is None
    state.note_tree_retick(["shop.audit_1"])
    later = popup_resolution(state, INV, text, inputs=_inputs(selected=list(NAMES)), source_type=MY)
    assert later.tables_to_unselect == ("shop.audit_1",)
    assert exclusion_drift(state, later) is None


# --- the row summary -------------------------------------------------------------------


def test_summary_variants():
    state = DataMigrationState()
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason=None)
    assert (s.variant, s.text) == ("nothing", "Nothing excluded")
    inputs = _inputs()
    text = "exclude shop.t.a\nexclude shop.t.b\nexclude table shop.audit_*"
    assert state.set_column_exclusions(
        popup_resolution(state, INV, text, inputs=inputs, source_type=MY), text=text) is None
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason=None)
    assert (s.variant, s.text) == ("counts", "Rules exclude 1 table, 2 columns")
    state.set_selection(state.selection.model_copy(update={"selected_tables": ["shop.t"]}))
    s = exclusion_row_summary(state, locked=True, drift=None, block_reason=None)
    assert (s.variant, s.text) == ("locked", "1 table, 2 columns excluded - locked")
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason="x", blocking_count=2)
    assert s.variant == "blocking" and s.text == (
        "2 saved exclusions no longer apply. Starting is blocked until you review them.")
    from dsql_migrator.ui.data_migration._models import ExclusionDrift

    s = exclusion_row_summary(state, locked=False, block_reason=None,
                              drift=ExclusionDrift((("shop.new", "ssn"),), (), (), ()))
    assert s.variant == "drift" and s.text == "Your rules now match 1 more column (shop.new.ssn) - review"


def test_the_lob_line_is_info_until_a_measured_offender_is_open():
    state = DataMigrationState()
    cands = [LobExclusionCandidate("shop.t", ("a", "b"))]
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason=None,
                              lob_candidates=cands)
    assert s.lob_count == 2 and s.lob_tone == "info" and s.lob_text.startswith(
        "2 columns in the selected tables can exceed Aurora DSQL's value limit (type-based).")
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason=None,
                              lob_candidates=cands, measured_over={"shop.t.a"})
    assert s.lob_tone == "warning"


# --- review fixes (C05-C11 review) ----------------------------------------------------


def test_drift_survives_an_errored_line_and_reports_it():
    state = DataMigrationState()
    inv = SimpleNamespace(tables=[_t("shop.pages", body="longtext", summary="longtext"),
                                  _t("shop.orders", note="text")])
    names = [t.name for t in inv.tables]
    inputs = _inputs(selected=names, migratable=names)
    text = "exclude type:longtext\ninclude shop.pages.body"
    assert state.set_column_exclusions(
        popup_resolution(state, inv, text, inputs=inputs, source_type=MY), text=text) is None
    grown = SimpleNamespace(tables=[_t("shop.pages", summary="longtext"),
                                    _t("shop.orders", note="text", gift_note="longtext")])
    drift = exclusion_drift(state, popup_resolution(state, grown, text, inputs=inputs,
                                                    source_type=MY))
    assert drift is not None and drift.added == (("shop.orders", "gift_note"),)
    assert drift.error_lines == (2,)
    s = exclusion_row_summary(state, locked=False, block_reason=None, drift=drift)
    assert s.variant == "drift" and "no longer resolve (L2)" in s.text


def test_drift_names_column_rules_on_a_reticked_table():
    state = DataMigrationState()
    inputs = _inputs()
    text = "exclude table shop.audit_*\nexclude shop.*.x"
    assert state.set_column_exclusions(
        popup_resolution(state, INV, text, inputs=inputs, source_type=MY), text=text) is None
    state.note_tree_retick(["shop.audit_1"])
    drift = exclusion_drift(state, popup_resolution(state, INV, text, inputs=inputs,
                                                    source_type=MY))
    assert drift is not None and drift.retick_columns == (("shop.audit_1", "x"),)
    assert not drift.unselects
    s = exclusion_row_summary(state, locked=False, block_reason=None, drift=drift)
    assert "shop.audit_1.x" in s.text and "ticked again" in s.text


def test_the_table_count_is_of_tables_the_tree_shows():
    state = DataMigrationState()
    state.restore_exclusion_rules("exclude table shop.audit_*", ["shop.audit_1", "shop.gone"])
    s = exclusion_row_summary(state, locked=False, drift=None, block_reason=None,
                              migratable=NAMES)
    assert s.text == "Rules exclude 1 table"


def test_a_quarantine_column_whose_name_breaks_a_line_gets_no_rule():
    s = quarantine_rule_suggestion("shop.t", "... for text", (("a\nb", "longtext"), ("c", "longtext")),
                                   open_excluded=set(), saved_excluded=set())
    assert s.outcome == "A" and s.unwritable == ("a\nb",)
    assert s.lines == ("exclude shop.t.c  # from the Full Load quarantine",)
    state = DataMigrationState()
    assert not state.add_pending_rule('exclude shop.t."a\nb"')


def test_a_retick_survives_a_snapshot_round_trip():
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import apply_session_snapshot, capture_session_snapshot

    state = DataMigrationState()
    inputs = _inputs()
    text = "exclude table shop.audit_*"
    assert state.set_column_exclusions(
        popup_resolution(state, INV, text, inputs=inputs, source_type=MY), text=text) is None
    state.note_tree_retick(["shop.audit_1"])
    snap = capture_session_snapshot("s1", SessionConnectionState(), EvaluationState(),
                                    SchemaConversionState(), state)
    restored = DataMigrationState()
    apply_session_snapshot(SessionSnapshot.model_validate_json(snap.model_dump_json()),
                           SessionConnectionState(), EvaluationState(), SchemaConversionState(),
                           restored)
    assert restored.rule_reticked_tables() == {"shop.audit_1"}
    assert exclusion_drift(restored, popup_resolution(restored, INV, text, inputs=inputs,
                                                      source_type=MY)) is None
