# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""column.exclude.list: the exact-match formatter, the legacy-preserving chooser, the
4 KB / comma guards and preview == sent (P1 of the column-exclusion work).

Debezium compiles each entry as an anchored, CASE-INSENSITIVE regex; the checks below
emulate that with Python's ``re.fullmatch(entry, name, re.IGNORECASE)``, whose handling of
the constructs used here (escapes, ``(?-i)``, ``(?:a|b)``) matches Java's.
"""

import re
from types import SimpleNamespace

from dsql_migrator.core.cdc import (
    CDC_PARAMETER_BYTE_LIMIT,
    apply_column_exclude_chooser,
    choose_column_exclude_value,
    column_exclude_inexact_entries,
    format_column_exclude_value,
)
from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef


def _t(name, cols, pk=("id",), generated=()):
    return TableDef(
        name=name,
        columns=[ColumnDef(name=c, mysql_type="text", generated=c in generated,
                           generated_kind=("STORED" if c in generated else None),
                           generated_expression=("id * 2" if c in generated else None))
                 for c in cols],
        primary_key=list(pk),
    )


def _debezium_excludes(value, qualified):
    """Would Debezium drop ``db.table.column`` given this ColumnExcludeList value?

    Emulates debezium-core 2.7.4: the list is split on UNESCAPED commas, each entry is
    ``String.trim()``-ed, then compiled CASE_INSENSITIVE and fully matched. Java accepts a
    leading global ``(?-i)``; Python only the scoped ``(?-i:...)``, which means the same
    thing for a whole-entry prefix. (The same cases were checked against the real jar.)
    """
    from dsql_migrator.core.cdc import split_column_exclude_value

    def _py(entry):
        entry = entry.strip(" \t\r\n\x00\x0b\x0c")
        return f"(?-i:{entry[5:]})" if entry.startswith("(?-i)") else entry

    return any(
        re.fullmatch(_py(entry), qualified, re.IGNORECASE)
        for entry in split_column_exclude_value(value)
    )


def test_plain_names_are_byte_identical_to_the_legacy_form():
    from dsql_migrator.ui.data_migration._models import format_column_exclude_list

    cols = {"shop.docs": ["body", "summary"], "shop.notes": ["text_blob"]}
    captured = [_t("shop.docs", ["id", "body", "summary"]), _t("shop.notes", ["id", "text_blob"])]
    assert format_column_exclude_value(cols, captured) == format_column_exclude_list(cols)
    assert format_column_exclude_value({}, captured) == ""


def test_metacharacters_are_escaped_so_each_entry_matches_exactly_its_column():
    cols = {"shop.docs": ["col$1", "a.b", "x(y"]}
    captured = [_t("shop.docs", ["id", "col$1", "a.b", "aXb", "x(y"])]
    value = format_column_exclude_value(cols, captured)
    # A non-plain entry escapes its separators too: 'shop.docs.a\.b' would also drop
    # 'shop.docs_a.b' (the separator dot is any character) -- verified on 2.7.4.
    assert value == r"shop\.docs\.a\.b,shop\.docs\.col\$1,shop\.docs\.x\(y"
    for name in ("col$1", "a.b", "x(y"):
        assert _debezium_excludes(value, f"shop.docs.{name}"), name
    assert not _debezium_excludes(value, "shop.docs.aXb")  # '.' no longer matches any char
    # The legacy (unescaped) entry for col$1 matches NOTHING -- the defect being fixed.
    assert not _debezium_excludes("shop.docs.col$1", "shop.docs.col$1")
    assert column_exclude_inexact_entries(cols, captured) == [
        "shop.docs.a.b", "shop.docs.col$1", "shop.docs.x(y"]


def test_case_twins_get_a_case_sensitive_entry():
    # Twins in the column part (t.note / t.Note), the table part (T / t) and the db part
    # (Shop.x / shop.x); shop.y and shop.docs collide with nothing.
    captured = [_t("shop.T", ["id", "note"]), _t("shop.t", ["id", "note", "Note"]),
                _t("Shop.x", ["id", "c"]), _t("shop.x", ["id", "c"]), _t("shop.y", ["id", "c"]),
                _t("shop.docs", ["id", "body"])]
    value = format_column_exclude_value(
        {"shop.T": ["note"], "shop.t": ["Note"], "Shop.x": ["c"], "shop.y": ["c"],
         "shop.docs": ["body"]}, captured)
    assert set(value.split(",")) == {
        "(?-i)shop.T.note", "(?-i)shop.t.Note", "(?-i)Shop.x.c", "shop.y.c", "shop.docs.body"}
    # Each entry drops exactly its column and not its twin.
    for kept, twin in (("shop.T.note", "shop.t.note"), ("shop.t.Note", "shop.t.note"),
                       ("Shop.x.c", "shop.x.c")):
        assert _debezium_excludes(value, kept) and not _debezium_excludes(value, twin), kept


def test_compact_form_only_over_4_kib_and_still_exact():
    cols = {f"shop.table_{i:03d}": [f"big_payload_column_{j}" for j in range(3)] for i in range(40)}
    captured = [_t(t, ["id", *c]) for t, c in cols.items()] + [_t("shop.other", ["id", "big_payload_column_0"])]
    plain = ",".join(f"{t}.{c}" for t in sorted(cols) for c in sorted(cols[t]))
    assert len(plain) > CDC_PARAMETER_BYTE_LIMIT
    value = format_column_exclude_value(cols, captured)
    assert len(value.encode()) <= CDC_PARAMETER_BYTE_LIMIT and "," not in value  # one group
    for t, cs in cols.items():
        for c in cs:
            assert _debezium_excludes(value, f"{t}.{c}")
    assert not _debezium_excludes(value, "shop.other.big_payload_column_0")
    assert not _debezium_excludes(value, "shop.table_001.big_payload_column_3")
    # Under the limit nothing is compacted.
    small = {k: cols[k] for k in list(cols)[:3]}
    assert format_column_exclude_value(small, captured).count(",") == 8


def test_chooser_keeps_the_legacy_value_only_for_a_stack_deployed_with_it():
    assert choose_column_exclude_value("new", "old", "old") == "old"
    assert choose_column_exclude_value("new", "old", "different") == "new"
    assert choose_column_exclude_value("new", "old", None) == "new"   # no stack yet
    assert choose_column_exclude_value("new", None, "old") == "new"   # no legacy computed


def test_start_cdc_keeps_a_legacy_stack_byte_identical_and_updates_only_on_change():
    from tests.test_cdc_deploy_stages import (
        SINK, SRC, _FakeDeployer, _FakeHandle, _start_params, run_cdc_start,
    )

    def _params():
        p = _start_params()
        filled = [(k, r"shop.docs.col\$1" if k == "ColumnExcludeList" else v) for k, v in p.filled]
        return p.model_copy(update={"filled": filled, "column_exclude_legacy": "shop.docs.col$1"})

    def _deployed(value):
        base = {k: v for k, v in _params().filled if k not in {"MskBootstrapServers", "DeploySink"}}
        return {**base, "ColumnExcludeList": value}

    for deployed, expect_update in (("shop.docs.col$1", False), ("", True)):
        deployer = _FakeDeployer(connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
                                 discovery_params=_deployed(deployed))
        logs: list = []
        run_cdc_start(_FakeHandle(), stack_name="mysql-dsql-cdc-stack", params=_params(),
                      deployer=deployer, on_log=lambda ts, m: logs.append(m),
                      sleep=lambda _s: None, connector_timeout_seconds=5.0,
                      poll_interval_seconds=0.0)
        if expect_update:
            assert len(deployer.updates) == 1
            assert dict(deployer.updates[0])["ColumnExcludeList"] == r"shop.docs.col\$1"
        else:  # a RUNNING pipeline on the legacy form gets NO connector update
            assert deployer.updates == [], deployed
            assert any("already RUNNING with the requested configuration" in m for m in logs)


def test_apply_chooser_leaves_params_alone_without_a_legacy_or_a_deployed_value():
    from tests.test_cdc_deploy_stages import _start_params

    p = _start_params()
    assert apply_column_exclude_chooser(p, {"ColumnExcludeList": "x"}) is p  # no legacy
    q = p.model_copy(update={"column_exclude_legacy": "x"})
    assert apply_column_exclude_chooser(q, None) is q
    assert apply_column_exclude_chooser(q, {"TableIncludeList": "a"}) is q  # param absent


# ---------------------------------------------------------------------------
# The UI helper: effective set, captured generated columns, guards, preview == sent
# ---------------------------------------------------------------------------


def _state(entries, deployed=None):
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    for table, col in entries:
        state.set_lob_exclusion(table, col, True)
    state.set_cdc_deployed_column_exclude_list(deployed)
    return state


DOCS = _t("shop.docs", ["id", "body", "gen"], generated=("gen",))
OTHER = _t("shop.other", ["id", "g2"], generated=("g2",))
INV = SourceInventory(tables=[DOCS, OTHER])


def test_capture_set_is_effective_plus_captured_generated_and_legacy_is_0_1_561():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    # A PK entry (dropped from the effective set) and a real LOB exclusion.
    cx = cdc_capture_exclusions(_state([("shop.docs", "id"), ("shop.docs", "body")]), INV, [DOCS])
    assert cx.structured == {"shop.docs": ["body", "gen"]}  # no key, only CAPTURED generated
    assert cx.value == "shop.docs.body,shop.docs.gen"
    # 0.1.561 sent the raw set plus EVERY inventory table's generated columns.
    assert cx.legacy == "shop.docs.body,shop.docs.gen,shop.docs.id,shop.other.g2"
    assert cx.sent == cx.value and cx.error is None  # no stack yet


def test_a_stack_on_the_legacy_form_keeps_it_and_its_inexact_names_are_flagged():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    inv = SourceInventory(tables=[_t("shop.docs", ["id", "col$1"])])
    legacy = "shop.docs.col$1"
    cx = cdc_capture_exclusions(_state([("shop.docs", "col$1")], deployed=legacy), inv, inv.tables)
    assert cx.sent == legacy and cx.value == r"shop\.docs\.col\$1"
    assert cx.inexact == ("shop.docs.col$1",) and cx.error is None
    # Any other deployed value -> the exact form, no warning.
    other = cdc_capture_exclusions(_state([("shop.docs", "col$1")], deployed=""), inv, inv.tables)
    assert other.sent == other.value and other.inexact == ()


def test_a_comma_is_carried_escaped_and_only_the_size_refuses():
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    # Debezium's splitter reads '\\,' as a literal comma, so a comma in a name is carried
    # exactly -- including a PG generated column the operator never excluded.
    inv = SourceInventory(tables=[_t("shop.docs", ["id", "a,b", "a", "b"])])
    cx = cdc_capture_exclusions(_state([("shop.docs", "a,b")]), inv, inv.tables)
    assert cx.error is None and cx.value == r"shop\.docs\.a\,b"
    assert _debezium_excludes(cx.value, "shop.docs.a,b")
    assert not _debezium_excludes(cx.value, "shop.docs.a") and not _debezium_excludes(cx.value, "shop.docs.b")
    # Byte-identical to a deployed legacy value: kept, never refused.
    kept = cdc_capture_exclusions(_state([("shop.docs", "a,b")], deployed="shop.docs.a,b"), inv, inv.tables)
    assert kept.error is None and kept.sent == "shop.docs.a,b"

    many = [f"column_with_a_rather_long_name_{i:04d}" for i in range(200)]
    wide = SourceInventory(tables=[_t(f"shop.t{i}", ["id", many[i]]) for i in range(200)])
    big = cdc_capture_exclusions(
        _state([(f"shop.t{i}", many[i]) for i in range(200)]), wide, wide.tables)
    # The FACT only; each caller appends the remedy true in its state.
    assert big.error and "4,096-byte" in big.error and f"{len(big.sent.encode()):,} bytes" in big.error
    assert "Exclude fewer" not in big.error


def test_edge_whitespace_is_escaped_because_debezium_trims_each_entry():
    names = [" lead", "lead", "trail ", "trail"]
    inv = [_t("shop.docs", ["id", *names])]
    value = format_column_exclude_value({"shop.docs": [" lead", "trail "]}, inv)
    assert value == r"shop.docs. lead,shop\.docs\.trail\u0020"  # inner space: not an edge
    assert _debezium_excludes(value, "shop.docs.trail ") and not _debezium_excludes(value, "shop.docs.trail")
    assert _debezium_excludes(value, "shop.docs. lead") and not _debezium_excludes(value, "shop.docs.lead")
    lead_schema = format_column_exclude_value({" sp.t": ["c"]}, [_t(" sp.t", ["id", "c"]), _t("sp.t", ["id", "c"])])
    assert lead_schema == r"\u0020sp\.t\.c" and not _debezium_excludes(lead_schema, "sp.t.c")


def test_a_case_twin_of_an_uncaptured_excluded_table_is_protected():
    # The excluded table is deselected (not captured); its twin IS captured.
    value = format_column_exclude_value({"Shop.T": ["note"]}, [_t("shop.t", ["id", "note"])])
    assert value == "(?-i)Shop.T.note" and not _debezium_excludes(value, "shop.t.note")


def test_every_cdc_surface_shows_what_start_sends():
    """The connector preview, the CDC-only card and Start/Deploy read the same helper."""
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui)
    # Preview: the SENT value. Start/Deploy: the exact form, Start with the legacy attached.
    assert "exclude_list = split_column_exclude_value(_capture.sent) or None" in src
    assert src.count("exclude_list = split_column_exclude_value(_capture.value) or None") == 2
    assert 'params = params.model_copy(update={"column_exclude_legacy": _capture.legacy})' in src
    # P3-C22: the CDC step's read-only summary shows the same SENT value (unless the live
    # connector or a refused mismatch makes the deployed value what CDC receives).
    assert "capture = cdc_capture_exclusions(migration_state, inventory, captured)" in src
    assert "session_value = capture.sent" in src


def test_discovery_records_the_deployed_value_and_adopt_clears_it():
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    state.set_cdc_deployed_column_exclude_list("shop.docs.body")
    assert state.cdc_deployed_column_exclude_list == "shop.docs.body"
    state.set_cdc_deployed_column_exclude_list("")
    assert state.cdc_deployed_column_exclude_list == ""  # a real, empty value
    state.adopt_cdc_stack("dsql-cdc-other")
    assert state.cdc_deployed_column_exclude_list is None


def test_the_phase_probe_records_the_deployed_column_exclude_list(monkeypatch):
    import dsql_migrator.core.cdc_deployer as deployer_mod
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    def _probe(params):
        class _Deployer:
            def describe_stack_or_none(self, name):
                if params is None:
                    return None
                return SimpleNamespace(stack_status="UPDATE_COMPLETE", current_parameters=params,
                                       is_stable=True, stack_name=name)

            def list_cdc_stacks(self):
                return []

        monkeypatch.setattr(deployer_mod, "build_cdc_stack_deployer", lambda *a, **k: _Deployer())
        state = DataMigrationState()
        state.set_cdc_deployed_column_exclude_list("stale")
        _probe_cdc_stack_phase(state, SimpleNamespace(
            target_config=SimpleNamespace(region="us-east-1"), aws_profile=None))
        return state.cdc_deployed_column_exclude_list

    assert _probe({"ColumnExcludeList": "shop.docs.col$1", "MskBootstrapServers": ""}) == "shop.docs.col$1"
    assert _probe({"ColumnExcludeList": ""}) == ""
    assert _probe({"TableIncludeList": "shop.docs"}) is None  # no such parameter
    assert _probe(None) is None                                # no stack


def test_start_blocks_a_generated_rekey_column_even_with_a_regex_character(monkeypatch):
    """The composite guard reads the STRUCTURED set, not the formatted entries.

    A preserved STORED generated column used as the re-key's leading column (a hash
    bucket, say) is dropped at capture because DSQL computes it -- yet the record key needs
    it. With a '$' in its name the formatted entry is 'shop.docs.bucket\\$1', so a string
    comparison against the formatted list never saw the key column and Start proceeded.
    """
    import dsql_migrator.ui.data_migration._cdc_ui as cdcui
    from dsql_migrator.core.table_selection import TableSelection
    from dsql_migrator.ui.data_migration import DataMigrationState

    docs = _t("shop.docs", ["id", "bucket$1", "body"], generated=("bucket$1",))
    inv = SourceInventory(tables=[docs])
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["shop.docs"]))
    state.set_cdc_message_key_columns({"shop.docs": ["bucket$1", "id"]})
    notices: list = []
    monkeypatch.setattr(cdcui, "render_notice", lambda ui, **kw: notices.append(kw))

    class _JM:
        submitted = False

        def get_status(self, _job_id):
            raise KeyError(_job_id)

        def submit(self, _work):
            _JM.submitted = True
            return "job-1"

    session = SimpleNamespace(source_config=None, target_config=None, aws_profile=None,
                              restored_source_type=None, source_password=None)
    cdcui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), state, _JM(),
                            lambda: None, inventory=inv, session=session)
    headers = [n.get("header") for n in notices]
    assert "Composite key column is excluded from capture" in headers, headers
    body = next(
        n["body"] for n in notices if n.get("header") == "Composite key column is excluded from capture")
    assert "shop.docs.bucket$1" in body
    # True for both routings (G15): on a v51+ stack the record key is the SOURCE primary key and
    # it is the sink that cannot build the composite key without the column.
    assert "record key" not in body and "composite primary key" in body, body
    assert not _JM.submitted


# --- P3-C07: the pure capture preview for an UNSAVED resolution ---------------------


def test_the_wrapper_is_the_pure_function_over_the_stored_state():
    from dsql_migrator.ui.data_migration._models import (
        capture_exclusions_for,
        cdc_capture_exclusions,
        effective_lob_exclusions,
    )

    for entries, deployed, inv, captured in [
        ([("shop.docs", "id"), ("shop.docs", "body")], None, INV, [DOCS]),
        ([("shop.docs", "body")], "shop.docs.body,shop.docs.gen,shop.other.g2", INV, INV.tables),
    ]:
        state = _state(entries, deployed=deployed)
        assert cdc_capture_exclusions(state, inv, captured) == capture_exclusions_for(
            effective_lob_exclusions(state, inv), state.lob_exclusions(), inv, captured, deployed)


def _capture_hook(inv, deployed=None):
    from dsql_migrator.ui.data_migration._models import capture_exclusions_for

    def hook(columns, selected):
        captured = [t for t in inv.tables if t.name in set(selected)]
        return capture_exclusions_for(columns, columns, inv, captured, deployed)
    return hook


def test_preview_equals_sent_after_the_save():
    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    res = resolve_exclusion_rules(
        "exclude shop.docs.body", migratable=INV.tables, source_type=SourceType.POSTGRES,
        effective_selection=[t.name for t in INV.tables], capture=_capture_hook(INV))
    state = _state([(t, c) for t, cs in res.columns.items() for c in cs])
    assert res.cdc_value == cdc_capture_exclusions(state, INV, INV.tables).sent
    assert res.cdc_bytes == len(res.cdc_value.encode()) and res.can_save


def test_an_oversized_capture_is_e4_unless_generated_alone_overflow_and_deployed_is_kept():
    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules
    from dsql_migrator.core.models import SourceType

    many = [f"column_with_a_rather_long_name_{i:04d}" for i in range(200)]
    wide = SourceInventory(tables=[_t(f"shop.t{i}", ["id", many[i]]) for i in range(200)])
    names = [t.name for t in wide.tables]
    res = resolve_exclusion_rules("exclude shop.*.column_with_a_rather_long_name_*",
                                  migratable=wide.tables, source_type=SourceType.MYSQL,
                                  effective_selection=names, capture=_capture_hook(wide))
    (error,) = res.global_errors
    assert error.code == "E4" and f"{res.cdc_bytes:,} bytes" in error.message
    assert error.message.endswith("Narrow the rules: exclude fewer columns in the tables CDC captures.")
    kept = resolve_exclusion_rules("exclude shop.*.column_with_a_rather_long_name_*",
                                   migratable=wide.tables, source_type=SourceType.MYSQL,
                                   effective_selection=names,
                                   capture=_capture_hook(wide, deployed=res.cdc_value))
    assert kept.can_save and kept.cdc_value == res.cdc_value
    gen = SourceInventory(tables=[_t(f"shop.g{i}", ["id", many[i], "x"], generated=(many[i],))
                                  for i in range(200)])
    only_gen = resolve_exclusion_rules("exclude shop.*.x", migratable=gen.tables,
                                       source_type=SourceType.POSTGRES,
                                       effective_selection=[t.name for t in gen.tables],
                                       capture=_capture_hook(gen))
    assert not any(e.code == "E4" for e in only_gen.global_errors)
    assert only_gen.capture_warning and "alone need" in only_gen.capture_warning


def test_the_applied_ddl_decides_which_columns_cdc_leaves_out():
    """A column made GENERATED in Schema Conversion (the MySQL generated-column advice) was
    still captured, so every CDC insert and update of the table was refused (428C9); one
    edited back to ordinary stayed excluded, so CDC never wrote it."""
    from dsql_migrator.ui.data_migration._models import (
        cdc_capture_exclusions,
        target_generated_capture_columns,
    )

    orders = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name=c, mysql_type="int") for c in ("id", "qty", "price", "Total")],
        primary_key=["id"],
    )
    ddl = (
        'CREATE TABLE "shop"."orders" ("id" integer GENERATED ALWAYS AS (1) STORED NOT NULL, '
        '"qty" integer, "price" integer, '
        'total integer GENERATED ALWAYS AS ("qty" * "price") STORED, PRIMARY KEY ("id"))'
    )
    applied = {"shop.orders": SimpleNamespace(target_ddl=ddl, index_ddls=[])}
    # By the SOURCE name (an unquoted edit folds Total), and never a key column.
    assert target_generated_capture_columns(orders, applied) == ["Total"]

    state = _state([])
    state.set_applied_conversions(applied, preserve_foreign_keys=True)
    inv = SourceInventory(tables=[orders, DOCS])
    cx = cdc_capture_exclusions(state, inv, inv.tables)
    assert cx.structured == {"shop.docs": ["gen"], "shop.orders": ["Total"]}

    # DOCS' preserved STORED column edited back to ordinary is captured again.
    plain = 'CREATE TABLE "shop"."docs" ("id" text NOT NULL, "body" text, "gen" text, PRIMARY KEY ("id"))'
    state.set_applied_conversions(
        {**applied, "shop.docs": SimpleNamespace(target_ddl=plain, index_ddls=[])},
        preserve_foreign_keys=True,
    )
    assert cdc_capture_exclusions(state, inv, inv.tables).structured == {"shop.orders": ["Total"]}
