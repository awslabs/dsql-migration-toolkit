"""A fresh session attaching a stack deployed WITH excluded columns (P2 follow-up).

Live, 2026-10-01 (Seoul): the stack carried ColumnExcludeList = the 5 columns below, a
fresh session showed them unticked + locked, and Start CDC sent the session's empty set --
the stack stopped excluding columns its streamed rows hold as NULL. The session now takes
the stack's set after the probe (round-trip proven), and Start refuses a set that differs
from a stack that streamed.
"""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.cdc import format_column_exclude_value, infer_column_exclusions
from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
from dsql_migrator.ui.data_migration import DataMigrationState
from dsql_migrator.ui.data_migration._cdc_status import adopt_deployed_exclusions_if_safe
from dsql_migrator.ui.data_migration._models import (
    capture_exclusion_mismatch,
    cdc_capture_exclusions,
    deployed_capture_exclusions,
)

P = "migration_schema.p2l4729801_"
LIVE_VALUE = (
    "migration_schema.p2l4729801_big.extra,migration_schema.p2l4729801_bin.payload,"
    "migration_schema.p2l4729801_docs.body,migration_schema.p2l4729801_pre.note,"
    "migration_schema\\.p2l4729801_sib\\.n\\$b"
)
LIVE_COLUMNS = {
    f"{P}big": ["extra"], f"{P}bin": ["payload"], f"{P}docs": ["body"],
    f"{P}pre": ["note"], f"{P}sib": ["n$b"],
}


def _table(name, *cols):
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)]
        + [ColumnDef(name=c, mysql_type="mediumtext") for c in cols],
        primary_key=["id"],
    )


def _inventory():
    return SourceInventory(tables=[
        _table(f"{P}big", "payload", "extra"), _table(f"{P}bin", "label", "payload"),
        _table(f"{P}docs", "title", "body"), _table(f"{P}pre", "note", "val"),
        _table(f"{P}sib", "n$b", "nb", "nxb"),
    ])


def _attached(value=LIVE_VALUE, *, streamed=True, phase="infra"):
    state = DataMigrationState()
    state.set_cdc_stack_name("dsql-cdc-p2live")
    state.set_cdc_stack_phase(phase, status="UPDATE_COMPLETE")
    state.set_cdc_deployed_column_exclude_list(value)
    state.cdc_reconciled_table_names = [t.name for t in _inventory().tables]
    state.set_cdc_has_committed_offset(streamed)
    return state


def _facts(*, rows=True, nullable_no_default=True, exists=True, unknown=()):
    """A facts reader: every table exists (or not), holds rows (or not), and each column is
    nullable with no DEFAULT (the earlier overlay) or NOT NULL with a DEFAULT."""
    from dsql_migrator.core.target_introspector import ColumnNullFacts, TableOverlayFacts

    def _read(tables, columns_by_table):
        out = {}
        for table in tables:
            if table in unknown:
                out[table] = None
                continue
            cols = {
                c: ColumnNullFacts(not_null=not nullable_no_default,
                                   default_expr=None if nullable_no_default else "''::text")
                for c in columns_by_table[table]
            }
            out[table] = TableOverlayFacts(exists, rows if exists else False, None, cols)
        return out

    return _read


def _session(ledger=None):
    return SimpleNamespace(target_config=SimpleNamespace(cluster_endpoint="x.dsql", region="r", database="postgres"),
                           aws_profile=None, _ledger=ledger)


def test_the_live_value_is_read_back_exactly_and_round_trips():
    inv = _inventory()
    found = infer_column_exclusions(LIVE_VALUE, inv.tables)
    assert found == LIVE_COLUMNS  # n$b only -- not its siblings nb / nxb
    assert format_column_exclude_value(found, inv.tables) == LIVE_VALUE
    deployed = deployed_capture_exclusions(_attached(), inv)
    assert deployed is not None and deployed.columns == LIVE_COLUMNS
    assert deployed.form == "exact"  # an escaped entry proves the 0.1.562+ overlay semantics


def test_inference_refuses_what_it_cannot_read():
    tables = _inventory().tables
    assert infer_column_exclusions("migration_schema.nope.x", tables) is None
    assert infer_column_exclusions("migration_schema\\.p2l4729801_big\\.(", tables) is None
    assert infer_column_exclusions("", tables) == {}
    # a (?-i) entry matches its exact case only; without it ASCII case folds
    twin = [_table("shop.T", "note"), _table("shop.t", "note")]
    assert infer_column_exclusions("(?-i)shop.T.note", twin) == {"shop.T": ["note"]}
    assert infer_column_exclusions("shop.t.note", twin) == {"shop.T": ["note"], "shop.t": ["note"]}


def test_a_fresh_session_adopts_the_stacks_set_and_start_then_sends_it_unchanged():
    inv = _inventory()
    # the live case: A's run overlaid the columns (nullable, no DEFAULT), then it streamed
    state = _attached()
    # before: Start would send "" over the 5 excluded columns of a streamed stack -> refused
    before = cdc_capture_exclusions(state, inv, inv.tables)
    assert before.sent == ""
    reason = capture_exclusion_mismatch(state, inv, before, inv.tables)
    assert reason and "p2l4729801_sib.n$b" in reason and "no columns" in reason
    detail = adopt_deployed_exclusions_if_safe(state, _session(), inv, facts_reader=_facts())
    assert detail and "dsql-cdc-p2live" in detail
    assert {t: sorted(c) for t, c in state.lob_exclusions().items()} == LIVE_COLUMNS
    assert not state.legacy_exclusions()  # overlay: the target columns are already overlaid
    after = cdc_capture_exclusions(state, inv, inv.tables)
    assert after.sent == LIVE_VALUE  # byte-for-byte: Start changes nothing
    assert capture_exclusion_mismatch(state, inv, after, inv.tables) is None


def test_adoption_never_overwrites_or_guesses():
    inv = _inventory()
    # a session with its own set keeps it (and Start refuses the difference instead)
    own = _attached()
    own.set_lob_exclusion(f"{P}docs", "title", True)
    assert adopt_deployed_exclusions_if_safe(own, _session(), inv) is None
    assert own.lob_exclusions() == {f"{P}docs": {"title"}}
    assert capture_exclusion_mismatch(own, inv, cdc_capture_exclusions(own, inv, inv.tables), inv.tables)
    # a committed Full Load in this session: never adopted
    loaded = _attached()
    loaded.job_id = "job-1"
    assert adopt_deployed_exclusions_if_safe(loaded, _session(), inv) is None
    # not probed yet / probe failed / no stack value / no inventory
    unprobed = _attached()
    unprobed.cdc_stack_phase_checked = False
    assert adopt_deployed_exclusions_if_safe(unprobed, _session(), inv) is None
    assert adopt_deployed_exclusions_if_safe(_attached(None), _session(), inv) is None
    assert adopt_deployed_exclusions_if_safe(_attached(), _session(), None) is None
    # a value this inventory cannot reproduce
    assert adopt_deployed_exclusions_if_safe(_attached("migration_schema.gone.x"), _session(), inv) is None


def test_overlay_or_omit_only_is_decided_per_column_from_the_live_target():
    """Review rounds 1-2: neither the value's form, the migration type nor the ledger can
    decide it safely. The LIVE target can: overlay where it ALTERs nothing that holds data
    (column already nullable with no DEFAULT, or the table empty / absent), omit-only where
    the table holds rows and the column keeps NOT NULL or a DEFAULT."""
    from dsql_migrator.ui.data_migration import MigrationType

    inv = _inventory()
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        for streamed in (True, False):
            for facts, overlay in ((_facts(), True), (_facts(rows=False, nullable_no_default=False), True),
                                   (_facts(exists=False), True),
                                   (_facts(nullable_no_default=False), False)):
                state = _attached(streamed=streamed)
                state.set_migration_type(mtype)
                detail = adopt_deployed_exclusions_if_safe(state, _session(), inv, facts_reader=facts)
                assert detail, (mtype, streamed)
                assert (not state.legacy_exclusions()) is overlay, (mtype, streamed, overlay)
                assert ("omit-only for" in detail) is (not overlay)


def test_an_unreadable_target_adopts_nothing_so_the_next_discovery_retries():
    inv = _inventory()
    state = _attached()
    assert adopt_deployed_exclusions_if_safe(
        state, _session(), inv, facts_reader=_facts(unknown=(f"{P}docs",))) is None
    assert not state.lob_exclusions()

    def _boom(*_a):
        raise RuntimeError("token expired")

    assert adopt_deployed_exclusions_if_safe(state, _session(), inv, facts_reader=_boom) is None
    assert adopt_deployed_exclusions_if_safe(state, _session(), inv, facts_reader=_facts())  # retried


def test_adoption_rechecks_under_the_lock_that_no_run_started_meanwhile():
    state = _attached()
    state.job_id = "job-started-during-the-read"
    assert state.adopt_deployed_exclusions(LIVE_COLUMNS) is False
    state2 = _attached()
    state2.cdc_deploy_job_id = "start-cdc-1"
    assert state2.adopt_deployed_exclusions(LIVE_COLUMNS) is False


def test_a_legacy_value_with_a_regex_character_is_read_as_literal_omit_only():
    inv = _inventory()
    legacy = "migration_schema.p2l4729801_docs.body,migration_schema.p2l4729801_sib.n$b"
    deployed = deployed_capture_exclusions(_attached(legacy), inv)
    assert deployed is not None and deployed.form == "legacy"
    assert deployed.columns == {f"{P}docs": ["body"], f"{P}sib": ["n$b"]}


def test_no_refusal_when_nothing_streamed_or_the_value_is_unknown():
    inv = _inventory()
    fresh = _attached(streamed=False)
    capture = cdc_capture_exclusions(fresh, inv, inv.tables)
    assert capture_exclusion_mismatch(fresh, inv, capture, inv.tables) is None
    unknown = _attached(None)
    assert capture_exclusion_mismatch(unknown, inv, cdc_capture_exclusions(unknown, inv, inv.tables), inv.tables) is None


def test_discovery_adopts_and_both_start_paths_refuse_a_mismatch():
    import inspect

    import dsql_migrator.ui.data_migration as dm
    from dsql_migrator.ui.data_migration import _cdc_ui

    builder = inspect.getsource(dm.build_data_migration_screen)
    disc = builder[builder.index("async def _discover_cdc"):]
    assert disc.index("_ensure_cdc_controller") < disc.index("adopt_deployed_exclusions_if_safe")
    assert "if _adopted or _vpc_derived" in disc
    start_job = inspect.getsource(_cdc_ui._start_cdc_deploy)
    assert "capture_exclusion_mismatch(" in start_job
    assert start_job.index("capture_exclusion_mismatch(") < start_job.rindex("run_cdc_start")
    section = inspect.getsource(_cdc_ui._render_cdc_start_button)
    assert "capture_exclusion_mismatch(migration_state, inventory, _capture, _captured_now)" in section



def test_no_adoption_from_a_stack_being_torn_down():
    inv = _inventory()
    deleting = _attached(phase="unstable")
    deleting.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
    assert adopt_deployed_exclusions_if_safe(deleting, _session(), inv) is None
    marked = _attached()
    marked.cdc_teardown_job_id = "teardown-1"
    assert adopt_deployed_exclusions_if_safe(marked, _session(), inv) is None


def test_a_legacy_value_naming_a_table_the_stack_does_not_capture_is_still_read():
    """0.1.561 sent the RAW stored set, captured or not: a saved exclusion on a deselected
    table is a literal entry of a non-captured inventory table."""
    inv = _inventory()
    legacy = "migration_schema.p2l4729801_docs.body,migration_schema.p2l4729801_sib.n$b"
    state = _attached(legacy)
    state.cdc_reconciled_table_names = [f"{P}docs"]  # sib is not captured by the stack
    deployed = deployed_capture_exclusions(state, inv)
    assert deployed is not None and deployed.form == "legacy"
    assert deployed.columns == {f"{P}docs": ["body"], f"{P}sib": ["n$b"]}


def test_the_refusal_offers_only_remedies_true_in_its_state(monkeypatch):
    inv = _inventory()
    unreadable = _attached("migration_schema.gone.x")
    msg = capture_exclusion_mismatch(unreadable, inv, cdc_capture_exclusions(unreadable, inv, inv.tables), inv.tables)
    assert msg and "cannot be read" in msg and "Start over" not in msg
    loaded = _attached()
    loaded.job_id = "job-1"
    msg = capture_exclusion_mismatch(loaded, inv, cdc_capture_exclusions(loaded, inv, inv.tables), inv.tables)
    assert msg and "Start over" not in msg and "Full Load already wrote" in msg
    fresh = _attached()
    msg = capture_exclusion_mismatch(fresh, inv, cdc_capture_exclusions(fresh, inv, inv.tables), inv.tables)
    assert msg and "Start over and attach it again" in msg


def test_start_cdc_refuses_when_the_live_stack_value_changed_since_the_screen_read_it():
    import inspect

    from dsql_migrator.core import cdc_deployer
    from dsql_migrator.core.cdc import CdcStackParams
    from dsql_migrator.ui.data_migration import _cdc_ui

    assert "column_exclude_seen" in CdcStackParams.model_fields
    src = inspect.getsource(cdc_deployer.run_cdc_start)
    assert src.index("params.column_exclude_seen") < src.index("apply_column_exclude_chooser(params")
    assert '"column_exclude_seen": migration_state.cdc_deployed_column_exclude_list' in inspect.getsource(
        _cdc_ui._start_cdc_deploy)


def test_run_cdc_start_refuses_a_stack_value_that_changed_since_the_ui_judged_it():
    import pytest

    from dsql_migrator.core.cdc_deployer import CdcDeployError
    from tests.test_cdc_deploy_stages import (
        SINK, SRC, _FakeDeployer, _FakeHandle, _start_params, run_cdc_start,
    )

    def _deployed(value):
        base = {k: v for k, v in _start_params().filled if k not in {"MskBootstrapServers", "DeploySink"}}
        return {**base, "ColumnExcludeList": value}

    def _run(seen, live):
        deployer = _FakeDeployer(connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
                                 discovery_params=_deployed(live))
        params = _start_params().model_copy(update={"column_exclude_seen": seen})
        run_cdc_start(_FakeHandle(), stack_name="mysql-dsql-cdc-stack", params=params,
                      deployer=deployer, on_log=lambda ts, m: None, sleep=lambda _s: None,
                      connector_timeout_seconds=5.0, poll_interval_seconds=0.0)
        return deployer

    with pytest.raises(CdcDeployError, match="changed since this screen read them"):
        _run(seen="shop.docs.body", live="")
    _run(seen="", live="")      # unchanged: proceeds
    _run(seen=None, live="x")   # not judged: proceeds (nothing to compare)


def test_a_stack_ever_started_counts_as_streamed_for_the_refusal():
    """Review round 2: a stack stuck in UPDATE_ROLLBACK_FAILED ('unstable') or with detected
    connectors but no committed-offset read was treated as never streamed, so Retry CDC sent
    a fresh session's empty set over it unchecked."""
    inv = _inventory()
    for setup in ("unstable", "partial", "connectors"):
        state = _attached(streamed=False, phase="infra")
        if setup == "connectors":
            state.cdc_connector_names = ["dsql-cdc-p2live-debezium-source"]
        else:
            state.set_cdc_stack_phase(setup, status="UPDATE_ROLLBACK_FAILED")
        capture = cdc_capture_exclusions(state, inv, inv.tables)
        assert capture_exclusion_mismatch(state, inv, capture, inv.tables), setup


def test_the_unreadable_refusal_is_job_aware():
    inv = _inventory()
    loaded = _attached("migration_schema.gone.x")
    loaded.job_id = "job-1"
    msg = capture_exclusion_mismatch(loaded, inv, cdc_capture_exclusions(loaded, inv, inv.tables), inv.tables)
    assert msg and "choose the columns to exclude on the card" not in msg and "Start over" in msg


def test_a_legacy_value_reads_back_after_a_generated_column_appears_elsewhere():
    """Review round 2: the 0.1.561 reading re-formatted with TODAY's generated columns, so a
    generated column added after deploy made a readable value 'unreadable'."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef

    def gen(name):
        return ColumnDef(name=name, mysql_type="integer", generated=True,
                         generated_kind="STORED", generated_expression="id * 2")

    a = TableDef(name="public.a", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False), ColumnDef(name="blob", mysql_type="bytea")])
    b = TableDef(name="public.b", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False), gen("g")])
    c_new = TableDef(name="public.c", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False), gen("h")])
    inv = SourceInventory(tables=[a, b, c_new])
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_cdc_deployed_column_exclude_list("public.a.blob,public.b.g")
    state.cdc_reconciled_table_names = ["public.a"]
    deployed = deployed_capture_exclusions(state, inv)
    assert deployed is not None and deployed.form == "legacy"
    assert deployed.columns == {"public.a": ["blob"]}  # generated columns are not operator entries


def test_run_cdc_start_fails_closed_when_the_screen_never_read_a_started_stacks_value():
    import pytest

    from dsql_migrator.core.cdc_deployer import CdcDeployError
    from tests.test_cdc_deploy_stages import (
        SINK, SRC, _FakeDeployer, _FakeHandle, _start_params, run_cdc_start,
    )

    base = {k: v for k, v in _start_params().filled if k not in {"MskBootstrapServers", "DeploySink"}}
    deployer = _FakeDeployer(connector_states={SRC: ["RUNNING"], SINK: ["RUNNING"]},
                             discovery_params={**base, "ColumnExcludeList": "shop.docs.body",
                                               "DeploySink": "true", "MskBootstrapServers": ""})
    with pytest.raises(CdcDeployError, match="could not read the CDC stack's excluded columns"):
        run_cdc_start(_FakeHandle(), stack_name="mysql-dsql-cdc-stack",
                      params=_start_params().model_copy(update={"column_exclude_seen": None}),
                      deployer=deployer, on_log=lambda ts, m: None, sleep=lambda _s: None,
                      connector_timeout_seconds=5.0, poll_interval_seconds=0.0)
