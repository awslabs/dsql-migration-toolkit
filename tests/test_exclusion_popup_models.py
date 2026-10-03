"""P3-C11: the exclusion popup's pure composition over live inputs."""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
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


def test_a_mysql_json_reason_pre_ticks_an_open_json_candidate():
    # G13: a MySQL JSON column is a candidate and converts to DSQL json, so a reason naming
    # json is outcome A with the json column's rule pre-inserted -- not B (pick it yourself).
    s = quarantine_rule_suggestion(
        "shop.t", "datatype limit greater than 1048576 bytes not supported for json",
        (("a", "longtext"), ("j", "json")), open_excluded=set(), saved_excluded=set())
    assert (s.outcome, s.lines) == ("A", ("exclude shop.t.j  # from the Full Load quarantine",))


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
                              lob_candidates=cands, measured_over={("shop.t", "a")})
    assert s.lob_tone == "warning"
    # P3 LIVE 8: the measured offender is named, not only toned.
    assert s.lob_text.startswith("1 column in the selected tables holds a value over Aurora "
                                 "DSQL's limit (measured)")


def test_the_lob_line_stays_info_for_type_based_json_candidates():
    # G13 severity guard: a MySQL JSON candidate is a type-based ceiling, not a prediction,
    # so a schema of many small JSON columns reads a calm info line until a size check
    # (Check sizes or the prerequisite checks) has measured a value over the limit.
    from dsql_migrator.ui.data_migration._models import lob_exclusion_candidates

    inv = SourceInventory(tables=[TableDef(name="ecommerce.users", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="metadata", mysql_type="json"),
        ColumnDef(name="bio", mysql_type="text")])])
    cands = lob_exclusion_candidates(inv, source_type=MY)
    s = exclusion_row_summary(DataMigrationState(), locked=False, drift=None, block_reason=None,
                              lob_candidates=cands)
    assert s.lob_count == 1 and s.lob_tone == "info" and "(type-based)" in s.lob_text
    s = exclusion_row_summary(DataMigrationState(), locked=False, drift=None, block_reason=None,
                              lob_candidates=cands,
                              measured_over={("ecommerce.users", "metadata")})
    assert s.lob_tone == "warning"


# --- review fixes (C05-C11 review) ----------------------------------------------------


def test_an_untick_is_not_drift_but_a_table_gone_from_the_inventory_is():
    # Final check: unticking a table whose columns the rules exclude is not "no longer
    # match"; the rules still match it, the table is simply not migrated.
    state = DataMigrationState()
    res = popup_resolution(state, INV, "exclude shop.t.a", inputs=_inputs(), source_type=MY)
    assert state.set_column_exclusions(res, text="exclude shop.t.a") is None
    unticked = _inputs(selected=[n for n in NAMES if n != "shop.t"])
    again = popup_resolution(state, INV, "exclude shop.t.a", inputs=unticked, source_type=MY)
    assert exclusion_drift(state, again, migratable=unticked.migratable) is None
    summary = exclusion_row_summary(state, locked=False, drift=None, block_reason=None)
    assert summary.variant != "drift"
    # Re-ticked: still nothing.
    again = popup_resolution(state, INV, "exclude shop.t.a", inputs=_inputs(), source_type=MY)
    assert exclusion_drift(state, again, migratable=NAMES) is None
    # The inverse still drifts: saved with shop.t unticked, then ticked.
    state2 = DataMigrationState()
    res = popup_resolution(state2, INV, "exclude shop.t.a", inputs=unticked, source_type=MY)
    assert state2.set_column_exclusions(res, text="exclude shop.t.a") is None
    ticked = popup_resolution(state2, INV, "exclude shop.t.a", inputs=_inputs(), source_type=MY)
    assert exclusion_drift(state2, ticked, migratable=NAMES).added == (("shop.t", "a"),)
    # A stored entry on a table that left the inventory still reports.
    gone = [n for n in NAMES if n != "shop.t"]
    shrunk = popup_resolution(state, INV, "exclude shop.t.a",
                              inputs=_inputs(selected=gone, migratable=gone), source_type=MY)
    assert exclusion_drift(state, shrunk, migratable=gone).removed == (("shop.t", "a"),)


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


# --- P3-C21: the Start Full Load and Deploy CDC disclosures ---------------------------


def test_the_start_dialog_notes_unselected_tables_and_drift_but_never_gates():
    from dsql_migrator.ui.data_migration._models import start_dialog_exclusion_notes

    state = DataMigrationState()
    state.set_exclusion_inputs_resolver(lambda: _inputs())
    assert start_dialog_exclusion_notes(state) == []
    state.restore_exclusion_rules("exclude table shop.audit_*", ["shop.audit_1", "gone.t"])
    # the rules unselected shop.audit_1 (a provenance table is not in the selection)
    state.set_exclusion_inputs_resolver(
        lambda: _inputs(selected=[n for n in NAMES if n != "shop.audit_1"]))
    state.set_exclusion_drift_text("Your rules now match 1 more column (shop.u.x) - review")
    notes = start_dialog_exclusion_notes(state)
    assert notes == [
        ("info", "1 table unselected by your rules is not loaded (left as is on Aurora DSQL)."),
        ("warning", "Your rules now match 1 more column (shop.u.x) - review in Exclude tables & columns."),
    ]
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_ui

    src = inspect.getsource(_full_load_ui)
    i = src.index("start_dialog_exclusion_notes(migration_state)")
    assert "disable" not in src[i:i + 200]  # a note, never a gate


def test_the_deploy_disclosure_appears_only_when_something_is_excluded_and_shows_what_is_sent():
    from dsql_migrator.ui.data_migration._models import (
        cdc_capture_exclusions,
        deploy_exclusion_disclosure,
    )

    inv = SourceInventory(tables=list(INV.tables))
    state = DataMigrationState()
    state.set_exclusion_inputs_resolver(lambda: _inputs())
    assert deploy_exclusion_disclosure(state, inv) is None
    state.restore_lob_exclusion("shop.t", "a")
    header, details = deploy_exclusion_disclosure(state, inv)
    assert header == ("Excluded: 1 column. They are fixed once deployed: changing them later "
                      "needs Delete CDC infrastructure.")
    sent = cdc_capture_exclusions(state, inv, inv.tables).sent
    assert sent == "shop.t.a"
    # Identical halves are said once, not listed twice.
    assert details == "Columns (sent as ColumnExcludeList unchanged): shop.t.a."
    assert "Sent as" not in details
    # The two halves differ when the column's table is not captured.
    other = DataMigrationState()
    other.set_exclusion_inputs_resolver(lambda: _inputs(selected=["shop.u", "shop.audit_1"]))
    other.restore_lob_exclusion("shop.t", "a")
    assert deploy_exclusion_disclosure(other, inv)[1] == (
        "Columns: shop.t.a. Sent as ColumnExcludeList: (nothing excluded at capture)")
    # ...and when the capture list escapes a name: both halves, so the escape is visible.
    inv_d = SourceInventory(tables=[_t("shop.t", **{"a$b": "longtext"})])
    dollar = DataMigrationState()
    dollar.set_exclusion_inputs_resolver(
        lambda: _inputs(selected=["shop.t"], migratable=["shop.t"]))
    dollar.restore_lob_exclusion("shop.t", "a$b")
    sent_d = cdc_capture_exclusions(dollar, inv_d, inv_d.tables).sent
    assert sent_d and sent_d != "shop.t.a$b"
    assert deploy_exclusion_disclosure(dollar, inv_d)[1] == (
        f"Columns: shop.t.a$b. Sent as ColumnExcludeList: {sent_d}")
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._open_cdc_infra_dialog)
    after = src[src.index("_disclosure = deploy_exclusion_disclosure"):]
    assert "deploy_btn.props" not in after.split("deploy_btn = ui.button")[0]
