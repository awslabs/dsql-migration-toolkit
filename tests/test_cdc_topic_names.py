# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC topic names: a captured table whose database / schema or table name holds a character
Kafka refuses in a topic name is refused before anything is uploaded or created, and warned
about before a Full load + CDC load -- as the CDC table cap is (G128, G131).

CDC names each captured table's Kafka topic ``dsqlcdc.<db>.<table>`` (MySQL) or
``dsqlcdc.<schema>.<table>`` (PostgreSQL) as written, and Kafka allows only ASCII letters,
digits, '.', '_' and '-' in a topic name. Both topic pre-create paths caught only an existing
topic, so such a table failed Start CDC with an opaque Kafka error after the infrastructure was
deployed (v0.1.578's Known issue): a rollback on the Lambda path, about 2 minutes of retries
blaming the network on the External one. And a comma, or whitespace ending a PostgreSQL table
name, was trimmed into a legal name, so Start succeeded and the table was never captured
(read from the code). Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import ast
import inspect
import string
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core import cdc_deployer
from dsql_migrator.core.cdc import (
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_TOPIC_NAME_REMEDY,
    cdc_expected_connector_names,
    topic_name_refusal,
    topic_name_refused_tables,
)
from dsql_migrator.core.cdc_deployer import CdcDeployError, run_cdc_start
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType, StepStatus, Watermark
from dsql_migrator.ui.data_migration import (
    MigrationType,
    data_migration_exclusion_block,
    pre_load_topic_name_warning,
    table_set_editable_after_load,
)
from dsql_migrator.ui.data_migration._models import (
    CDC_TABLE_LIST_HEADER,
    CDC_TOPIC_NAME_HEADER,
    PRE_LOAD_SPLIT_CARVE_OUT,
    cdc_pre_load_topic_name_routes,
    cdc_pre_load_topic_name_warning,
    cdc_topic_name_refusal,
    cdc_topic_name_refused,
    topic_name_hold_remedy,
)

from tests.test_cdc_deploy_stages import (
    SINK,
    SRC,
    STACK,
    _FakeDeployer,
    _FakeHandle,
    _FakeS3,
    _logs,
    _run_infra,
    _start_with,
    _statuses,
)
from tests.test_g12_table_set_after_load import _Jobs
from tests.test_g128_deploy_review import _band, _infra, _inventory, _start, _state
from tests.test_g131_table_list_warning import (
    _click_deploy,
    _deploy_ready,
    _fake_nicegui,
    _loaded,
    _render_deploy,
)
from tests.test_ui_data_migration import _RecordingUi

FLC = MigrationType.FULL_LOAD_AND_CDC
_TS = datetime(2026, 10, 1, tzinfo=timezone.utc)
_KAFKA = frozenset(string.ascii_letters + string.digits + "._-")

# One table CDC cannot stream beside one it can.
_NAMES = ["shop.orders", "shop.a b"]
_FACT = (
    "CDC cannot stream shop.a b: each table's Kafka topic is named after its database or schema "
    "and its table, and Kafka allows only ASCII letters, digits, '.', '_' and '-' in a topic "
    "name."
)
_REMEDY = (
    "Leave such a table out of this CDC pipeline (it stays on Aurora DSQL as it is, and CDC "
    "and Validation skip it) and copy it in a Full load only migration once its writes stop, "
    "or rename it at the source before migrating it."
)
_REFUSAL = f"{_FACT} {_REMEDY}"
_ROUTES = (
    "The Full Load can still load such a table: after it, until CDC starts, untick it in the "
    "table list (it stays on Aurora DSQL as loaded; CDC and Validation skip it) and copy it "
    "again in a Full load only migration once its writes stop."
)
_ROUTES_OR = (
    " Or, before the load, rename it at the source and re-run Step 1 (Evaluation), or leave it "
    "out: "
)
_OPEN = "Deselect tables in the table picker."
_FULL_LOAD_LOCK = (
    "Locked — a Full Load has already run for this table set. Use 'Start over' to migrate a "
    "different set of tables."
)
_INFRA_LOCK = (
    "Locked — CDC infrastructure is deployed for this table set, and each table's Kafka topic "
    "partitions are fixed when it is created (so is the sink's task budget), sized for the "
    "tables selected then. A table added now would stream on the plan's lowest partition "
    "count. To change the set, delete the CDC infrastructure on the CDC step first."
)
_MISSING_HEADER = "A source column is missing from the Aurora DSQL table"


def _lists(names, prefix=CDC_DEFAULT_TOPIC_PREFIX):
    """TopicPrefix and SinkTopics as the parameter builders fill them for ``names``."""
    return [("TopicPrefix", prefix), ("SinkTopics", ",".join(f"{prefix}.{n}" for n in names))]


def _counts(names):
    return {name: 1_000 for name in names}


# ---------------------------------------------------------------------------
# 1. The check on its own: Kafka's character set, judged on SinkTopics as sent
# ---------------------------------------------------------------------------

_ACCEPTED = ["shop.orders", "Shop.Order_Items-2", "shop.a.b", "db2024.t0123456789"]
_REFUSED = [
    "shop.order$items", "shop.order items", "shop.pct%off", "shop.a:b",
    "shop.주문", "shop.注文", "shop.テーブル", "shop.t\U0001f600",
    "shop.a,b",           # one name, not two
    "shop.a,,b",
    "public.orders ",     # never stripped (Kafka Connect and Debezium would trim it)
    "shop.orders\n",      # a set test: a regex's "$" lets a trailing newline through
    "shop.or\tders", "shop.or\u00a0ders", "shop.or\u200bders",  # tab, NBSP, zero width
    "shop.\uff11", "shop.\u00e9",  # a fullwidth digit, a Latin-1 letter
    "판매.orders",          # the database name counts as much as the table's
]


def test_names_kafka_accepts_are_not_refused() -> None:
    assert topic_name_refused_tables(_lists(_ACCEPTED)) == []
    assert topic_name_refusal(_lists(_ACCEPTED)) is None


@pytest.mark.parametrize("name", _REFUSED)
def test_each_name_kafka_refuses_is_named_exactly(name) -> None:
    # FAILS WITHOUT THE FIX: no check existed, and Start CDC failed after the deploy.
    assert topic_name_refused_tables(_lists(["shop.orders", name, "shop.items"])) == [name]
    assert topic_name_refusal(_lists([name])).startswith(f"CDC cannot stream {name}: ")


def test_a_name_holding_the_prefix_after_a_comma_reads_as_two_legal_names() -> None:
    # The known, accepted gap (named in the docstring): ",dsqlcdc." inside a name.
    assert topic_name_refused_tables(_lists(["shop.a,dsqlcdc.b"])) == []


def test_a_character_is_refused_exactly_when_kafka_refuses_it() -> None:
    # Every code point of the BMP and a sample of the astral planes, at both ends of a name
    # and inside it.
    astral = [*range(0x10000, 0x110000, 0x1111), 0x1F600, 0x10FFFF]
    for code in [*range(0x10000), *astral]:
        ch = chr(code)
        for name in (f"shop.t{ch}", f"{ch}shop.t", f"shop.{ch}t"):
            expected = [] if ch in _KAFKA else [name]
            assert topic_name_refused_tables(_lists([name])) == expected, (hex(code), name)


def test_the_prefix_is_read_from_the_parameters_and_sinktopics_order_is_kept() -> None:
    other = [("TopicPrefix", "other"),
             ("SinkTopics", "other.shop.z z,other.shop.ok,other.shop.a,b")]
    assert topic_name_refused_tables(other) == ["shop.z z", "shop.a,b"]
    # Without TopicPrefix, the default one.
    assert topic_name_refused_tables([("SinkTopics", "dsqlcdc.shop.a,b,dsqlcdc.shop.c")]) == [
        "shop.a,b"]
    # Topics under another prefix than the one sent are one name, judged whole.
    assert topic_name_refused_tables(
        [("TopicPrefix", "other"), ("SinkTopics", "dsqlcdc.shop.a,dsqlcdc.shop.b")]
    ) == ["dsqlcdc.shop.a,dsqlcdc.shop.b"]
    # No topics, nothing to refuse.
    for filled in ([], [("SinkTopics", "")], [("SinkTopics", None)], [("TopicPrefix", "x")]):
        assert topic_name_refused_tables(filled) == [], filled
        assert topic_name_refusal(filled) is None, filled


def test_the_refusal_names_the_tables_the_rule_and_the_remedy() -> None:
    assert CDC_TOPIC_NAME_REMEDY == _REMEDY
    assert topic_name_refusal(_lists(_NAMES)) == _REFUSAL
    # Sorted, five named and the rest counted.
    seven = [f"shop.t{c} x" for c in "gfedcba"]
    assert topic_name_refusal(_lists(seven)) == (
        "CDC cannot stream shop.ta x, shop.tb x, shop.tc x, shop.td x, shop.te x and 2 more: "
        "each table's Kafka topic is named after its database or schema and its table, and "
        "Kafka allows only ASCII letters, digits, '.', '_' and '-' in a topic name. " + _REMEDY
    )


# ---------------------------------------------------------------------------
# 2. The UI judges exactly what the builders send
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_the_ui_judges_exactly_what_the_builders_send(engine) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders", f"{db}.order$items", f"{db}.a,b", f"{db}.ok_2"]
    refusal = cdc_topic_name_refusal(names)
    assert refusal is not None
    assert refusal == topic_name_refusal(_start(engine, names).filled) == (
        topic_name_refusal(_infra(engine, names, _counts(names)).filled))
    assert cdc_topic_name_refused(names) == topic_name_refused_tables(
        _start(engine, names).filled) == [f"{db}.order$items", f"{db}.a,b"]
    legal = [f"{db}.orders", f"{db}.ok_2"]
    assert cdc_topic_name_refusal(legal) is None
    assert topic_name_refusal(_start(engine, legal).filled) is None
    assert topic_name_refusal(_infra(engine, legal).filled) is None


# ---------------------------------------------------------------------------
# 3. What the UI says
# ---------------------------------------------------------------------------


def _split_job(names, split):
    """A finished MySQL Full load + CDC record that read ``split`` in parts."""
    return MigrationJob(
        job_id="job-1",
        status="DONE",
        chunks=[
            ChunkState(chunk_id=n, status="DONE", attempts=1,
                       reader_shards=4 if n in split else None)
            for n in names
        ],
        watermark=Watermark(
            binlog_file="mysql-bin.000042", binlog_position=120, snapshot_timestamp=_TS,
            table_row_counts=_counts(names),
        ),
    )


def _loaded_state(names, **kw):
    state = _state(names, phase="infra", **kw)
    state.job_id = "job-1"
    return state


def test_the_hold_remedy_follows_the_table_picker() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _table_lock, _topic_name_hold_remedy

    assert topic_name_hold_remedy(_REFUSAL, None) == f"{_REFUSAL} {_OPEN}"
    # A Full Load ran and CDC started for this migration: the picker's Full Load lock.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    started = _loaded_state(_NAMES)
    started.set_cdc_ops_window_start(_TS)
    assert _table_lock(started, jobs, None) == _FULL_LOAD_LOCK
    assert _topic_name_hold_remedy(_REFUSAL, started, jobs, _table_lock(started, jobs, None)) == (
        f"{_REFUSAL} The table picker is locked. {_FULL_LOAD_LOCK}")
    # CDC only with its infrastructure deployed: the infrastructure lock.
    cdc_only = _state(_NAMES, MigrationType.CDC_ONLY, phase="infra")
    assert _table_lock(cdc_only, _Jobs({}), None) == _INFRA_LOCK
    assert topic_name_hold_remedy(_REFUSAL, _INFRA_LOCK) == (
        f"{_REFUSAL} The table picker is locked. {_INFRA_LOCK}")


def test_a_refused_table_read_in_parts_is_named_as_one_that_cannot_be_deselected() -> None:
    # G137: a refused table the Full Load read in parts is named with its reload route; one
    # read in parts that CDC can stream is not this refusal's business.
    from dsql_migrator.ui.data_migration._cdc_ui import _topic_name_hold_remedy

    names = ["app.users", "app.ev ents", "app.big"]
    job = _split_job(names, split={"app.ev ents", "app.big"})
    state = _loaded_state(names)
    refusal = cdc_topic_name_refusal(names)
    split_text = (
        "The Full Load read app.ev ents in parts, so it cannot be deselected: to deselect it, "
        "set Reader shards per large table to 1 in Settings and reload it (Reload specific "
        "tables, which drops and recreates it) first. Once CDC starts, the table set is fixed "
        "(Start over to change it). Nor can the migration switch to Full load only until that "
        "reload, or until a match in Validation in CHECKSUM mode proves CDC's replay of it."
    )
    assert _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": job}), None) == (
        f"{refusal} {_OPEN} {split_text}")
    assert topic_name_hold_remedy(refusal, None, ["app.ev ents"], job) == (
        f"{refusal} {_OPEN} {split_text}")
    # No refused table read in parts: the picker's remedy alone.
    legal_split = _split_job(names, split={"app.big"})
    assert _topic_name_hold_remedy(refusal, state, _Jobs({"job-1": legal_split}), None) == (
        f"{refusal} {_OPEN}")
    # CDC only has no Full Load step: the reload goes through Full load + CDC.
    through = (
        "to deselect it, switch the migration type to Full load + CDC, set Reader shards per "
        "large table to 1 in Settings and reload it on its Full Load step (Reload specific "
        "tables, which drops and recreates it) first."
    )
    cdc_only = _state(names, MigrationType.CDC_ONLY, phase="infra")
    cdc_only.job_id = "job-1"
    assert through in _topic_name_hold_remedy(refusal, cdc_only, _Jobs({"job-1": job}), None)
    assert through in topic_name_hold_remedy(refusal, None, ["app.ev ents"], job, cdc_only=True)
    assert through not in topic_name_hold_remedy(refusal, None, ["app.ev ents"], job)


def test_the_pre_load_routes_and_warning() -> None:
    assert cdc_pre_load_topic_name_routes(None) == _ROUTES + _ROUTES_OR + _OPEN
    assert cdc_pre_load_topic_name_routes("Locked — X.", split=True) == (
        _ROUTES + PRE_LOAD_SPLIT_CARVE_OUT + _ROUTES_OR
        + "The table picker is locked. Locked — X.")
    assert cdc_pre_load_topic_name_warning(_NAMES, planned=False, table_lock=None) == (
        CDC_TOPIC_NAME_HEADER,
        f"{_FACT} Deploy CDC infrastructure and Start CDC refuse a capture holding such a "
        "table. " + _ROUTES + _ROUTES_OR + _OPEN,
    )
    assert cdc_pre_load_topic_name_warning(_NAMES, planned=True, table_lock="Locked — X.") == (
        CDC_TOPIC_NAME_HEADER,
        f"{_FACT} Start CDC refuses a capture holding such a table. " + _ROUTES + _ROUTES_OR
        + "The table picker is locked. Locked — X.",
    )
    assert cdc_pre_load_topic_name_warning(["shop.orders"], planned=False, table_lock=None) is None


# ---------------------------------------------------------------------------
# 4. Where the UI says it: the Full Load card (a warning), Deploy and Start (holds)
# ---------------------------------------------------------------------------


def test_start_full_load_is_warned_never_held() -> None:
    from dsql_migrator.ui.data_migration import full_load_run_guard_reason

    state, inv = _state(_NAMES), _inventory(_NAMES)
    assert data_migration_exclusion_block(state, _Jobs({}), None, inv) is None
    assert full_load_run_guard_reason(state, inv, has_run=True, exclusion_block=None) is None
    assert pre_load_topic_name_warning(state, _Jobs({}), None, inv) == (
        cdc_pre_load_topic_name_warning(_NAMES, planned=False, table_lock=None))


def test_the_pre_load_topic_warning_follows_the_state() -> None:
    inv = _inventory(_NAMES)
    jobs = _Jobs({})
    header, body = pre_load_topic_name_warning(_state(_NAMES), jobs, None, inv)
    assert header == CDC_TOPIC_NAME_HEADER
    assert "Deploy CDC infrastructure and Start CDC refuse a capture" in body
    # A stack deployed before the load: only Start CDC is still to refuse; the picker is
    # locked by that stack.
    _header, deployed = pre_load_topic_name_warning(_state(_NAMES, phase="infra"), jobs, None, inv)
    assert f"{_FACT} Start CDC refuses a capture holding such a table." in deployed
    assert deployed.endswith(f"leave it out: The table picker is locked. {_INFRA_LOCK}")
    # Nothing to say: Full load only, CDC only, after a load, once CDC started, no inventory,
    # and a selection that leaves the table out.
    for mtype in (MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY):
        assert pre_load_topic_name_warning(_state(_NAMES, mtype), jobs, None, inv) is None, mtype
    loaded = _state(_NAMES)
    loaded.job_id = "job-1"
    assert pre_load_topic_name_warning(loaded, jobs, None, inv) is None
    started = _state(_NAMES)
    started.set_cdc_ops_window_start(_TS)
    assert pre_load_topic_name_warning(started, jobs, None, inv) is None
    assert pre_load_topic_name_warning(_state(_NAMES), jobs, None, None) is None
    assert pre_load_topic_name_warning(_state(["shop.orders"]), jobs, None, inv) is None


def _render_card(monkeypatch, *, job, warnings):
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_g12_table_set_after_load import _VerifiedSession
    from tests.test_ui_data_migration import _ConfirmDialogUi

    notices: list = []
    real_notice = fl.render_notice

    def _notice(ui, *, tone, header, body="", **k):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    monkeypatch.setattr(fl, "render_notice", _notice)
    monkeypatch.setattr(fl, "_render_full_load_progress", lambda *a, **k: None)
    state = _state(_NAMES)
    if job is not None:
        state.job_id = job.job_id
    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, _Jobs({} if job is None else {job.job_id: job}), _VerifiedSession(),
        job=job, status=StepStatus.NOT_STARTED if job is None else StepStatus.DONE,
        selected_names=_NAMES, guard_reason=None, pre_load_warning=warnings[0],
        pre_load_topic_warning=warnings[1],
        start_full_load=noop, retry_failed_load=noop, reload_table=noop,
        accept_quarantine_and_continue=noop, stop_full_load=noop, refresh=noop,
        retry_tables=noop, schema_recreate_candidates=lambda: [],
    )
    return ui, [n for n in notices if n[1] in ("H", CDC_TOPIC_NAME_HEADER)]


def test_the_full_load_card_warns_before_the_load_beside_the_list_warning(monkeypatch) -> None:
    topic = pre_load_topic_name_warning(_state(_NAMES), _Jobs({}), None, _inventory(_NAMES))
    ui, alone = _render_card(monkeypatch, job=None, warnings=(None, topic))
    assert alone == [("warning", CDC_TOPIC_NAME_HEADER, topic[1])]
    assert "Start Full Load" in ui.buttons
    # Both apply: two warnings, the table list's first.
    _ui, both = _render_card(monkeypatch, job=None, warnings=(("H", "W"), topic))
    assert both == [("warning", "H", "W"), ("warning", CDC_TOPIC_NAME_HEADER, topic[1])]
    # Once a load ran the CDC step's holds speak instead.
    _ui, after = _render_card(
        monkeypatch, job=_loaded(_NAMES, _counts(_NAMES)), warnings=(("H", "W"), topic))
    assert after == []


def test_the_screen_hands_the_topic_warning_to_the_full_load_card() -> None:
    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    (step,) = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
               and getattr(n.func, "id", None) == "_render_full_load_step"]
    (kw,) = [k for k in step.keywords if k.arg == "pre_load_topic_warning"]
    assert ast.unparse(kw.value) == "_topic_name_warning"
    (assign,) = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                 and any(getattr(t, "id", None) == "_topic_name_warning" for t in n.targets)]
    assert ast.unparse(assign.value.func) == "pre_load_topic_name_warning"
    # The gate every Full Load entry point reads never judges the names.
    assert "cdc_topic_name_refusal" not in inspect.getsource(dm.data_migration_exclusion_block)


def test_the_deploy_form_holds_with_the_route_that_is_open(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    inv = _inventory(_NAMES)
    # Before a Full load + CDC load (the early deploy on Prerequisites): the load-then-untick
    # route of the Full Load card's warning, in place of the refusal's own remedy.
    before = _deploy_ready(_NAMES, FLC)
    routes = f"{_FACT} {_ROUTES}{_ROUTES_OR}{_OPEN}"
    assert _deploy_block_notice(before, _Jobs({}), inv, None) == (CDC_TOPIC_NAME_HEADER, routes)
    ui, notices = _render_deploy(monkeypatch, before, _Jobs({}), inv)
    assert ("error", CDC_TOPIC_NAME_HEADER, routes) in notices
    assert ui.deploy_button().enabled is False
    # The Prerequisites card's early Deploy is that same form (render_notice still records).
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_ui_data_migration import _GateUi

    seen = len(notices)
    early = _GateUi()
    _cdc_ui._render_cdc_infra_prep_section(
        early, before, _Jobs({}), lambda: None, inventory=inv, session=None)
    assert ("error", CDC_TOPIC_NAME_HEADER, routes) in notices[seen:]
    assert early.deploy_button().enabled is False
    # CDC only, and after the load: the refusal with the table picker's remedy.
    after = _deploy_ready(_NAMES, FLC, job_id="job-1")
    loaded = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    for state, jobs in ((_deploy_ready(_NAMES), _Jobs({})), (after, loaded)):
        assert _deploy_block_notice(state, jobs, inv, None) == (
            CDC_TOPIC_NAME_HEADER, f"{_REFUSAL} {_OPEN}"), state.migration_type
        ui, notices = _render_deploy(monkeypatch, state, jobs, inv)
        assert ("error", CDC_TOPIC_NAME_HEADER, f"{_REFUSAL} {_OPEN}") in notices
        assert ui.deploy_button().enabled is False
    # Attach sends no parameter: not held.
    assert _deploy_block_notice(
        before, _Jobs({}), inv, None, include_missing=False, include_capture=False) is None
    # Names Kafka accepts: not held.
    legal = ["shop.orders", "shop.items"]
    assert _deploy_block_notice(
        _deploy_ready(legal, FLC), _Jobs({}), _inventory(legal), None) is None


def test_the_card_and_the_early_deploy_carve_out_a_table_read_in_parts(monkeypatch) -> None:
    # G137: while the load may read a large MySQL table in parts, the load-then-untick route
    # says such a table cannot be unticked until it is reloaded with one reader.
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    inv = _inventory(_NAMES)

    def _texts(source_type):
        session = SimpleNamespace(source_config=SimpleNamespace(source_type=source_type),
                                  restored_source_type=None)
        _h, card = pre_load_topic_name_warning(_state(_NAMES), _Jobs({}), session, inv)
        _h, deploy = _deploy_block_notice(_deploy_ready(_NAMES, FLC), _Jobs({}), inv, session)
        return card, deploy

    for knob in ("READER_SHARDS", "TABLE_PARALLELISM"):
        monkeypatch.delenv(f"DSQL_MIGRATOR_FULL_LOAD_{knob}", raising=False)
    assert not [t for t in _texts(SourceType.MYSQL) if PRE_LOAD_SPLIT_CARVE_OUT in t]
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    assert all(PRE_LOAD_SPLIT_CARVE_OUT in t for t in _texts(SourceType.MYSQL))
    assert not [t for t in _texts(SourceType.POSTGRES) if PRE_LOAD_SPLIT_CARVE_OUT in t]


def test_the_deploy_click_refuses_before_any_read(monkeypatch) -> None:
    calls = _fake_nicegui(monkeypatch, None)
    inv = _inventory(_NAMES)
    assert _click_deploy(_deploy_ready(_NAMES, FLC), inv) == [
        (f"{_FACT} {_ROUTES}{_ROUTES_OR}{_OPEN}", "negative")]
    assert _click_deploy(_deploy_ready(_NAMES), inv) == [(f"{_REFUSAL} {_OPEN}", "negative")]
    assert calls == []  # not even the source's row estimates


def test_without_the_check_the_deploy_click_reads_the_source(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    calls = _fake_nicegui(monkeypatch, None)
    monkeypatch.setattr(_cdc_ui, "cdc_topic_name_refusal", lambda names: None)
    with pytest.raises(AssertionError, match="no AWS call may run before the refusal"):
        _click_deploy(_deploy_ready(_NAMES), _inventory(_NAMES))
    assert calls[0] is _cdc_ui._estimate_cdc_table_rows


def test_the_start_section_holds_start_with_the_refusal(monkeypatch) -> None:
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui

    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    # A source column missing from the target waits its turn: one notice at a time.
    monkeypatch.setattr(_cdc_ui, "missing_column_block_reason", lambda *a, **k: "MISSING")
    state = _state(_NAMES, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)  # the CDC checks passed (they gate first)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _Jobs({}), lambda: None, inventory=_inventory(_NAMES), session=None,
    )
    (hit,) = [n for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER]
    assert hit["tone"] == "error"
    assert hit["body"] == f"{_REFUSAL} The table picker is locked. {_INFRA_LOCK}"
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in start.props_seen
    assert hit["body"] in ui.tooltips and "MISSING" not in ui.tooltips
    assert not [n for n in notices if n.get("header") == _MISSING_HEADER]


_SESSION = SimpleNamespace(source_config=None, target_config=None, aws_profile=None,
                           restored_source_type=None, source_password=None)


def _start_click(monkeypatch, state, jobs, names, *, retry=False, header=CDC_TOPIC_NAME_HEADER):
    """Start CDC's handler (Start click, or the Retry CDC button of a partial pipeline),
    returning ``(notices under header, submitted)``."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    ui = _RecordingUi()
    if retry:
        source, sink = cdc_expected_connector_names(state.cdc_stack_name)
        _cdc_ui._render_cdc_partial_actions(
            ui, state, jobs, lambda: None, names=[source], expected=(source, sink),
            inventory=_inventory(names), session=_SESSION,
        )
        (button,) = [b for b in ui.buttons if b.text == "Retry CDC"]
        button.on_click()
    else:
        _cdc_ui._start_cdc_deploy(
            ui, state, jobs, lambda: None, inventory=_inventory(names), session=_SESSION,
        )
    return [n for n in notices if n.get("header") == header], jobs.submitted


@pytest.mark.parametrize("retry", [False, True], ids=["start", "retry"])
def test_start_and_retry_refuse_before_the_submit_that_fixes_the_set(monkeypatch, retry) -> None:
    # FAILS WITHOUT THE FIX: the Start was submitted, which stamps the CDC operations window,
    # and from then on the table set was fixed -- so the failed Start left only Start over.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    state = _loaded_state(_NAMES)
    assert table_set_editable_after_load(state, jobs) is True
    hits, submitted = _start_click(monkeypatch, state, jobs, _NAMES, retry=retry)
    assert [h["body"] for h in hits] == [f"{_REFUSAL} {_OPEN}"]
    assert hits[0]["tone"] == "error"
    assert submitted == []
    assert state.cdc_ops_window_start is None
    # So the table can still be unticked.
    assert table_set_editable_after_load(state, jobs) is True


def test_a_set_fixed_by_an_earlier_failed_start_is_refused_with_start_over(monkeypatch) -> None:
    # v0.1.578: the first Start failed on such a name after its submit fixed the table set.
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    state = _loaded_state(_NAMES)
    state.set_cdc_ops_window_start(_TS)
    hits, submitted = _start_click(monkeypatch, state, jobs, _NAMES)
    assert [h["body"] for h in hits] == [
        f"{_REFUSAL} The table picker is locked. {_FULL_LOAD_LOCK}"]
    assert submitted == []


def test_a_pipeline_that_streamed_with_a_comma_in_a_name_is_refused_at_its_next_start(
    monkeypatch,
) -> None:
    # Accepted (and to be announced): a v0.1.578 Start trimmed "shop.a,b" into two legal names
    # and never streamed it; its next Start is refused, with the lock's own remedy.
    names = ["shop.orders", "shop.a,b"]
    jobs = _Jobs({"job-1": _loaded(names, _counts(names))})
    state = _loaded_state(names)
    state.set_cdc_has_committed_offset(True)
    state.set_cdc_deployed_watermark(("mysql-bin.000042", "120"))
    hits, submitted = _start_click(monkeypatch, state, jobs, names)
    (hit,) = hits
    assert hit["body"].startswith("CDC cannot stream shop.a,b: ")
    assert hit["body"].endswith(f"The table picker is locked. {_FULL_LOAD_LOCK}")
    assert submitted == []


def test_without_the_check_start_cdc_builds_the_connector_config(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _Reached(Exception):
        pass

    def _spy(*_a, **_k):
        raise _Reached()

    monkeypatch.setattr(_cdc_ui, "dispatch_source_config", _spy)
    jobs = _Jobs({"job-1": _loaded(_NAMES, _counts(_NAMES))})
    hits, _submitted = _start_click(monkeypatch, _loaded_state(_NAMES), jobs, _NAMES)
    assert len(hits) == 1  # refused before the config is built
    monkeypatch.setattr(_cdc_ui, "cdc_topic_name_refusal", lambda names: None)
    with pytest.raises(_Reached):
        _start_click(monkeypatch, _loaded_state(_NAMES), jobs, _NAMES)


def test_the_table_cap_speaks_first_when_both_apply(monkeypatch) -> None:
    # One notice at a time: a capture over one pipeline's table cap that also holds a name CDC
    # cannot stream is refused for the cap first, on every surface that holds both.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice

    many = [*_band(300), "shop.a b"]
    inv = _inventory(many)
    assert cdc_topic_name_refusal(many) is not None
    assert _deploy_block_notice(_deploy_ready(many), _Jobs({}), inv, None)[0] == (
        CDC_TABLE_LIST_HEADER)
    calls = _fake_nicegui(monkeypatch, None)
    ((text, _tone),) = _click_deploy(_deploy_ready(many), inv)
    assert "(SinkTopics), over CloudFormation's" in text and calls == []
    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(
        _cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw) or real(ui, **kw)
    )
    state = _state(many, phase="infra")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        ui, state, _Jobs({}), lambda: None, inventory=inv, session=None,
    )
    (cap,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert not [n for n in notices if n.get("header") == CDC_TOPIC_NAME_HEADER]
    assert cap["body"] in ui.tooltips
    jobs = _Jobs({"job-1": _loaded(many, _counts(many))})
    hits, submitted = _start_click(
        monkeypatch, _loaded_state(many), jobs, many, header=CDC_TABLE_LIST_HEADER)
    assert len(hits) == 1 and submitted == []


# ---------------------------------------------------------------------------
# 5. The core backstops refuse before anything is changed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
def test_deploy_refuses_before_the_bucket_or_any_upload(engine) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders", f"{db}.order$items"]
    handle = _FakeHandle()
    logs, on_log = _logs()
    s3 = _FakeS3(absent=True)
    deployer = _FakeDeployer(stack_statuses=("CREATE_IN_PROGRESS", "CREATE_COMPLETE"))
    with pytest.raises(CdcDeployError) as exc:
        _run_infra(
            handle, deployer, on_log, s3_client=s3, stack_name="dsql-cdc-shop",
            template_body="T", params=_infra(engine, names, _counts(names)),
            create_timeout_seconds=5.0, poll_interval_seconds=0.0,
        )
    assert str(exc.value) == cdc_topic_name_refusal(names)
    assert s3.calls == [] and deployer.calls == [] and deployer.created == []
    assert set(_statuses(handle).values()) == {"PENDING"}
    assert handle.job.error_count == 1
    assert any(m.startswith(f"ERROR: CDC cannot stream {db}.order$items") for m in logs)


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
@pytest.mark.parametrize("seed_mode", ["lambda", "external"])
@pytest.mark.parametrize("running", [False, True], ids=["create", "both_running"])
def test_start_refuses_before_bootstrap_seed_or_update(engine, seed_mode, running) -> None:
    db = "shop" if engine == "mysql" else "public"
    names = [f"{db}.orders", f"{db}.a,b"]
    params = _start(engine, names)
    states = ["RUNNING"] if running else ["CREATING", "RUNNING"]
    deployer = _FakeDeployer(
        connector_states={SRC: list(states), SINK: list(states)},
        # Both RUNNING on this very config: the fast path that sends nothing.
        discovery_params=dict(params.filled) if running else None,
    )
    seeds: list = []
    handle = _FakeHandle()
    with pytest.raises(CdcDeployError) as exc:
        run_cdc_start(
            handle, stack_name=STACK, params=params, deployer=deployer,
            on_log=lambda ts, msg: None, sleep=lambda _s: None,
            connector_timeout_seconds=5.0, poll_interval_seconds=0.0,
            seed_mode=seed_mode, seed_fn=lambda **kw: seeds.append(kw) or "true",
        )
    assert str(exc.value) == cdc_topic_name_refusal(names)
    assert deployer.updates == []
    assert "submit_update" not in deployer.calls
    assert "get_bootstrap_brokers" not in deployer.calls
    assert seeds == []
    # The stage the job failed in: validate_params (discover_stack only reads).
    statuses = _statuses(handle)
    assert statuses["discover_stack"] == "DONE"
    assert statuses["validate_params"] == "IN_PROGRESS"
    assert statuses["fetch_bootstrap"] == "PENDING"
    assert handle.job.error_count == 1


def test_without_the_core_check_start_sends_the_bad_topics(monkeypatch) -> None:
    monkeypatch.setattr(cdc_deployer, "topic_name_refusal", lambda filled: None)
    deployer = _FakeDeployer(
        connector_states={SRC: ["CREATING", "RUNNING"], SINK: ["CREATING", "RUNNING"]}
    )
    _start_with(_start("mysql", _NAMES), deployer)
    assert "submit_update" in deployer.calls
    assert dict(deployer.updates[0])["SinkTopics"] == "dsqlcdc.shop.orders,dsqlcdc.shop.a b"


# ---------------------------------------------------------------------------
# 6. The manual says it, in English, Korean and Japanese alike
# ---------------------------------------------------------------------------

_DOC_FACTS = {
    "en": ("`.`, `_` and `-`", "**A table whose name CDC cannot stream**"),
    "ko": ("`.`, `_`, `-`", "**CDC가 스트리밍할 수 없는 이름의 테이블**"),
    "ja": ("`.`、`_`、`-`", "**CDC がストリーミングできない名前のテーブル**"),
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_names_the_refusal_and_the_rule(lang) -> None:
    from tests.test_g25_docs import _has, _page

    chars, row = _DOC_FACTS[lang]
    limitations = _page(lang, "06-limitations.md")
    assert _has(lang, limitations, f"*{CDC_TOPIC_NAME_HEADER}*")
    assert _has(lang, limitations, chars)
    assert _has(lang, _page(lang, "13-excluding-tables-and-columns.md"), row)
