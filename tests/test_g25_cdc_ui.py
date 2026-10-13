"""G25 (CDC UI / copy): fixes from the max-effort review of G23/G24.

Each test pins one confirmed finding: a CDC-only target-side banner that sent the operator to
a Full Load step it does not have; AI seeds, tooltips and facts that disagreed with the banner
or claimed a schema change; a verified table whose notice said Cut over no longer holds it
while another Cut over check did; a "Reloaded and verified" header for a reload nothing checks;
AI context that still listed
verified tables as open; one sink-stall remedy for every SQLSTATE; a PostgreSQL start line that
claimed a gap; a dead-letter window proof judged against a failed read; a cut-over drain step
that did not wait out a unique-index hold; and Start CDC refusals cut to 300 characters.

Repair round 1 (the review of those fixes): a CHECKSUM-proven table is verified whatever its
kinds and only its target-side groups leave the banner (087d0f9's per-group rule; G25 round 4:
whatever open finding also holds it, which its Verified notice names -- see
test_g25_cdc_ui_r4.py); a refused Start's cause and fix reach the durable activity log; a
PostgreSQL forced restart with a committed offset records the slot it continues from; CDC
only's Delete-and-Deploy advice names what the delete costs; and the refusal card test
checks the job id the worker itself records.

Repair round 2 (the review of round 1): a PostgreSQL Start on a stack whose connectors were
started before never offers a re-snapshot (Debezium would skip it) and has its slot checked
whatever snapshot.mode it sends; its start line states only what the stack records (whether
its source connector ever streamed, it does not); a CHECKSUM-verified table's TRUNCATE group
moves into the Verified notice too; and the worker's foreign-key refusal keeps its fix.

Repair round 3: on CDC only the dead-letter card repairs like the banner above it, never by a
reload; the PostgreSQL Start CDC card, start-point card and Stop dialog state only what the stack
records, like the start line; and a refused Start's durable activity record keeps every fix,
however many keys or tables it names.

Repair round 4 (the review of round 3): the missing-slot block on a resumed PostgreSQL stack
states both outcomes (the source connector streamed, or its task failed before it created the
slot) and stays closed for both; and a refused Start's pointer stays true after an app restart,
which restores the failed Start but neither its notice nor (on Fargate) the activity log.

Repair round 5 (the review of round 4): no connector-log cause a Stop CDC keeps names the
"Re-snapshot every table instead" button a stopped stack's Start never offers; the PostgreSQL
dead-task card no longer promises "the recorded stream position"; the dead-letter card's AI DBA
gets each reason as the record list shows it (no source TRUNCATE "reload ... on the Full Load
step"); and CDC only's hand repair says the record list shows a key value only when it is an
integer or a UUID, sending a table whose key values it withholds to Start over (the DDL,
recreate and key-change runbooks on the same panel, and the Cut over step's runbook, say which
values it shows, too).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.models import (
    CdcCatalogDriftFinding,
    CdcDlqLedgerEntry,
    CdcReplicaIdentityFinding,
    SchemaDriftSummary,
    SourceType,
    Watermark,
)
from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_cdc_dlq import _AiBannerUi, _render_drift
from tests.test_g23_unique_conflict import _item, _ledger, _state, _validation

T0 = datetime(2026, 10, 9, 1, 0, tzinfo=timezone.utc)
_MATCHED = T0 + timedelta(minutes=5)


def _drift(*groups):
    return [SchemaDriftSummary(**g) for g in groups]


def _ask(groups, *, migration_type=None, migration_state=None):
    """Render the banner with a capturing AI opener; return (blob, facts, seed, tooltips)."""
    ui = _AiBannerUi()
    calls: list = []
    cm._render_cdc_schema_drift_banner(
        ui, SimpleNamespace(schema_drift=_drift(*groups)), migration_type=migration_type,
        migration_state=migration_state,
        cdc_ai_opener=lambda scope, facts, seed: calls.append((scope, facts, seed)),
    )
    ui.clicks["Ask AI DBA"]()
    _scope, facts, seed = calls[-1]
    return " ".join(ui.texts), facts, seed, ui.tooltips


# --- confirmed[24]: CDC only has no Full Load step --------------------------------------------


@pytest.mark.parametrize("kind", ["unique-conflict", "missing-table", "unbindable-value"])
def test_a_cdc_only_target_side_banner_repairs_by_hand_never_on_a_full_load_step(kind) -> None:
    blob, _ = _render_drift(
        [{"table": "shop.customers", "kind": kind, "count": 3}], MigrationType.CDC_ONLY
    )
    assert "Stop CDC and repair these tables" in blob
    for absent in ("on the Full Load step", "'Drop & reload'", "Drop & reload",
                   "reload the table as below"):
        assert absent not in blob, (kind, absent)
    for needle in ("correct the dead-lettered rows on Aurora DSQL by hand",
                   "at most 3,000 rows per transaction", "Start CDC and run Validation (step 4)",
                   "use Start over"):
        assert needle in blob, (kind, needle)
    # A dropped table also lost rows no dead letter lists: only Start over restores them.
    assert ("which no dead letter lists: use Start over for it" in blob) is (
        kind == "missing-table"
    )
    # Full load + CDC keeps its reload notice byte for byte.
    full, _ = _render_drift(
        [{"table": "shop.customers", "kind": kind, "count": 3}],
        MigrationType.FULL_LOAD_AND_CDC,
    )
    assert "Stop CDC and reload these tables" in full
    assert "reload just that table on the Full Load step with 'Drop & reload'" in full


# --- confirmed[27] / [28]: one predicate for seed, tooltip and facts ---------------------------


@pytest.mark.parametrize(
    "groups, expect",
    [
        ([{"table": "shop.events", "kind": "source-truncate", "count": 1}], "truncate"),
        (
            [{"table": "shop.events", "kind": "source-truncate", "count": 1},
             {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2}],
            "truncate-value-fit",
        ),
    ],
)
def test_a_truncate_banner_never_asks_the_ai_dba_for_ddl(groups, expect) -> None:
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        blob, facts, seed, tooltips = _ask(groups, migration_type=mtype)
        assert "Source TRUNCATE was not replicated" in blob
        assert "source schema changed" not in seed.lower() and "what DDL" not in seed
        assert "TRUNCATE" in seed and "not a schema change" in seed
        assert not facts.startswith("Source schema drift detected"), facts
        assert facts.startswith("A source TRUNCATE was not replicated (not a schema change")
        assert tooltips and not any("DDL" in t for t in tooltips), tooltips
        assert cm._drift_ai_case(_drift(*groups)) == expect


def test_a_target_side_seed_and_facts_do_not_rule_out_a_source_index_change() -> None:
    _blob, facts, seed, _tips = _ask(
        [{"table": "shop.customers", "kind": "unique-conflict", "count": 3}]
    )
    assert "(not necessarily a source schema change)" in seed
    assert facts.startswith(
        "CDC dead-lettered rows the target refused (not necessarily a source schema change"
    )
    # The model is told the source-side cause too (nothing else grounds it on it).
    assert "the source dropped or relaxed a UNIQUE index that Aurora DSQL still has" in facts
    # The 23505 explanation names the source-side cause, by migration type.
    why = cm._DRIFT_LABELS["unique-conflict"][1]
    assert "A source that dropped or relaxed this UNIQUE index gives the same refusal" in why
    assert "drop it on Aurora DSQL too" in why and "re-run Step 1 (Evaluation)" in why
    cdc_only = cm._drift_why(
        SimpleNamespace(kind="unique-conflict"), is_pg=False, cdc_only=True
    )
    assert "drop it on Aurora DSQL too" in cdc_only and "reload" not in cdc_only


def test_a_verified_group_beside_a_truncate_gives_one_story_to_the_ai_dba() -> None:
    # The verified unique-conflict group is filtered out of the banner; the seed AND the facts
    # must both follow the open truncate group (the facts used the unfiltered list).
    state = _state(
        {
            "app.customers": CdcDlqLedgerEntry(count=3, last_at=T0, kinds=["unique-conflict"]),
            "app.events": CdcDlqLedgerEntry(
                count=1, last_at=T0 + timedelta(minutes=20), kinds=["source-truncate"]
            ),
        },
        _validation(
            [_item("app.customers"), _item("app.events", matched=False)], run_start=_MATCHED
        ),
    )
    blob, facts, seed, tooltips = _ask(
        [{"table": "app.customers", "kind": "unique-conflict", "count": 3},
         {"table": "app.events", "kind": "source-truncate", "count": 1}],
        migration_state=state,
    )
    assert "Verified by CHECKSUM" in blob
    assert seed == cm._CDC_TRUNCATE_SEED
    assert facts.startswith("A source TRUNCATE was not replicated")
    assert "- app.customers: unique-conflict" not in facts
    assert "- app.events: source-truncate" in facts
    # ...and the verified table is named once, so the AI never advises reloading it again.
    assert (
        cm._VERIFIED_FACTS_LEAD + "app.customers matched by CHECKSUM in a Validation "
        "comparison started 2026-10-09 01:05 UTC, after its last dead-lettered record"
    ) in facts
    assert tooltips == [cm._DRIFT_AI_TOOLTIP["truncate"]]


# --- confirmed[7] / [9]: the verified notice speaks for the dead-letter check only -----------


def _proven_state(**findings):
    state = _state(_ledger("app.orders"), _validation([_item("app.orders")], run_start=_MATCHED))
    state.cdc_catalog_drift_findings = findings.get("catalog", {})
    state.cdc_replica_identity_findings = findings.get("identity", {})
    return state


_OPEN_RI = CdcReplicaIdentityFinding(key_columns=["tenant_id", "id"], first_weak_at=T0)
_OPEN_CATALOG = CdcCatalogDriftFinding(kinds=["nullability-change"], first_seen_at=T0)


@pytest.mark.parametrize(
    "findings, check",
    [({"identity": {"app.orders": _OPEN_RI}}, "REPLICA IDENTITY"),
     ({"catalog": {"app.orders": _OPEN_CATALOG}}, "source schema")],
    ids=["open REPLICA IDENTITY finding", "open source-schema finding"],
)
def test_a_table_another_cut_over_check_holds_is_verified_for_its_dead_letters_only(
    findings, check
) -> None:
    # G25 round 4 (f[5]): the CHECKSUM proves its dead-lettered rows caught up whatever else
    # holds it, so it is shown as verified -- never as "Stop CDC and reload" (a reload fixes
    # neither finding) -- and its notice names the open finding that still holds Cut over.
    group = [{"table": "app.orders", "kind": "unique-conflict", "count": 2}]
    blob, _ = _render_drift(group, migration_state=_proven_state(**findings))
    assert "Verified by CHECKSUM" in blob
    assert "Cut over's dead-letter check no longer holds these tables" in blob
    assert "Cut over no longer holds" not in blob
    assert (
        f"Cut over still holds app.orders for its open {check} finding (see that notice above)."
    ) in blob
    for absent in ("Stop CDC and reload these tables", "Rows may be missing from the target",
                   "will not catch up on its own"):
        assert absent not in blob, absent
    # Resolved findings do not hold it here: the scoped sentence speaks for the dead-letter
    # check only (the other gates show their own CHECKSUM wait).
    resolved = {
        "identity": {"app.orders": _OPEN_RI.model_copy(
            update={"restored_at": T0 + timedelta(minutes=1), "restored_by": "full"}
        )},
        "catalog": {"app.orders": _OPEN_CATALOG.model_copy(
            update={"resolved_at": T0 + timedelta(minutes=1), "resolved_by": "matched"}
        )},
    }
    which = next(iter(findings))
    blob, _ = _render_drift(group, migration_state=_proven_state(**{which: resolved[which]}))
    assert "Verified by CHECKSUM" in blob
    assert "Cut over's dead-letter check no longer holds these tables" in blob
    assert "Cut over no longer holds" not in blob and "Cut over still holds" not in blob


def test_the_verified_header_names_what_was_proven_not_a_reload() -> None:
    blob, _ = _render_drift(
        [{"table": "app.orders", "kind": "unique-conflict", "count": 2}],
        migration_state=_proven_state(),
    )
    assert "Verified by CHECKSUM" in blob and "Reloaded and verified" not in blob
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_resolved_body

    body = cdc_dlq_resolved_body({"app.orders": _MATCHED})
    assert body.startswith(
        "app.orders matched by CHECKSUM in a Validation comparison started 2026-10-09 01:05 UTC"
    )
    assert "Cut over's dead-letter check no longer holds these tables" in body


# --- confirmed[10]: the AI context knows which tables are verified -----------------------------


def _executor(state, validation):
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor
    from tests.test_ai_tools import _Obj, _Store

    return build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False)),
        evaluation_store=_Store(get_returns=None),
        schema_conversion_store=_Store(_Obj(generated_node_ids=[], apply_results=[])),
        validation_store=_Store(validation),
        data_migration_store=_Store(state),
        job_manager=_Obj(),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )


def test_get_cdc_status_lists_the_verified_tables_and_excepts_them_in_the_note(
    monkeypatch,
) -> None:
    from dsql_migrator.ui.data_migration import _cdc_status
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    state.cdc_dlq_ledger = {
        "app.customers": CdcDlqLedgerEntry(count=3, last_at=T0, kinds=["unique-conflict"]),
        "app.orders": CdcDlqLedgerEntry(count=1, last_at=T0, kinds=["add-column"]),
        "app.held": CdcDlqLedgerEntry(count=1, last_at=T0, kinds=["unique-conflict"]),
    }
    state.cdc_replica_identity_findings = {"app.held": _OPEN_RI}
    groups = _drift(
        {"table": "app.customers", "kind": "unique-conflict", "count": 3, "codes": ["23505"]},
        {"table": "app.orders", "kind": "add-column", "count": 1, "codes": ["42703"]},
        {"table": "app.held", "kind": "unique-conflict", "count": 1, "codes": ["23505"]},
    )
    monkeypatch.setattr(_cdc_status, "cdc_schema_drift_summary", lambda *_a: groups)
    validation = _validation(
        [_item("app.customers"), _item("app.orders"), _item("app.held")], run_start=_MATCHED
    )
    payload = json.loads(_executor(state, validation)("get_cdc_status", {}))
    # Every table a CHECKSUM proves after its last dead letter (Repair round 1: whatever its
    # kinds; G25 round 4: whatever else holds it -- an open REPLICA IDENTITY / source schema
    # finding is named in still_held_by, as the banner's Verified notice names it).
    assert payload["resolved_tables"] == [
        {"table": "app.customers", "checksum_compared_from": "2026-10-09T01:05:00+00:00"},
        {"table": "app.held", "checksum_compared_from": "2026-10-09T01:05:00+00:00",
         "still_held_by": ["REPLICA IDENTITY"]},
        {"table": "app.orders", "checksum_compared_from": "2026-10-09T01:05:00+00:00"},
    ]
    assert payload["resolved_tables_total"] == 3
    assert "resolve before cut over, except resolved_tables" in payload["note"]
    # "No repair" is claimed for the target-side kinds and a TRUNCATE only (Repair round 2: a
    # CHECKSUM match proves a truncate's extra rows gone too): app.orders' add-column keeps the
    # DDL fix its banner group still shows.
    assert (
        "its unique-conflict, missing-table, unbindable-value or source-truncate dead letters "
        "need no repair" in payload["note"]
    )
    assert "any other kind listed for it keeps its own fix" in payload["note"]
    assert "the source dropped or relaxed a UNIQUE index" in payload["note"]
    # Nothing verified without a validation result.
    empty = json.loads(_executor(state, _validation([]))("get_cdc_status", {}))
    assert empty["resolved_tables"] == [] and empty["resolved_tables_total"] == 0


def test_the_dlq_cards_ai_facts_name_the_verified_tables(monkeypatch) -> None:
    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.ui.data_migration import _cdc_status

    records = [
        CdcConnectorError(table="app.customers", message="sqlstate=23505 dup", error_code="23505")
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda *_a: records)
    monkeypatch.setattr(
        _cdc_status, "cdc_schema_drift_summary",
        lambda *_a: _drift({"table": "app.customers", "kind": "unique-conflict", "count": 1}),
    )
    state = _state(_ledger("app.customers"), _validation([_item("app.customers")],
                                                       run_start=_MATCHED))
    state.cdc_activity = None
    facts = cm._cdc_dlq_facts(state, "k")
    assert cm._VERIFIED_FACTS_LEAD in facts
    assert "app.customers matched by CHECKSUM" in facts


# --- confirmed[14]: the sink-stall remedy follows the SQLSTATE ---------------------------------


def test_the_sink_stall_notice_splits_its_remedy_by_sqlstate() -> None:
    body = cm.SINK_STALL_NOTICE_BODY
    assert '"DSQL apply failed (transient sqlstate="' in body  # the sink's own prefix
    _ejected, transient = body.split('A repeating "DSQL apply failed', 1)
    xx000, rest = transient.split("08xxx, 57xxx or null", 1)
    network, occ = rest.split("40001 means", 1)
    assert "XX000" in xx000 and "temporary Aurora DSQL condition" in xx000
    assert "contact AWS Support" in xx000
    for needle in ("cannot reach the cluster", "still exists", "port 5432",
                   "Aurora DSQL VPC endpoint", "IAM role"):
        assert needle in network, needle
    assert "AWS Support" not in network and "temporary Aurora DSQL" not in network
    assert "something else is writing the same rows on Aurora DSQL" in occ
    assert "AWS Support" not in occ
    assert "do NOT cut over until the sink send rate recovers" in occ


# --- confirmed[29]: a PostgreSQL start without a Full Load watermark claims no gap -------------


def test_a_postgres_start_without_a_watermark_names_the_slot_or_the_snapshot() -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    slot = pg_slot_name("dsql-cdc-x")
    resumed = _cdc_start_detail(
        "dsql-cdc-x", None, mode="auto", source_type=SourceType.POSTGRES, resumes=True
    )
    assert f"start point: replication slot {slot}" in resumed
    assert "where streaming stopped" in resumed
    first = _cdc_start_detail("dsql-cdc-x", None, mode="auto", source_type=SourceType.POSTGRES)
    assert "start point: a snapshot" in first and "snapshots every selected table" in first
    for line in (resumed, first):
        for absent in ("CURRENT position", "NOT replicated", "read again", "since the Full Load"):
            assert absent not in line, (line, absent)
    # MySQL keeps its CURRENT-position sentence: there it is true.
    mysql = _cdc_start_detail("dsql-cdc-x", None, mode="auto", source_type=SourceType.MYSQL)
    assert "the source's CURRENT position" in mysql and "NOT replicated" in mysql


# --- confirmed[26]: the window proof is judged against the read that succeeded -----------------


def _flaky_controller(stream_created):
    from dsql_migrator.core.msk_connect_controller import MskConnectController

    controller = MskConnectController(region="us-east-1")
    calls = {"filter": 0, "describe": []}

    class _Logs:
        def filter_log_events(self, **kwargs):
            calls["filter"] += 1
            if calls["filter"] == 1:
                raise RuntimeError("AccessDeniedException")
            return {"events": []}

        def describe_log_streams(self, **kwargs):
            calls["describe"].append(kwargs)
            return {"logStreams": [
                {"logStreamName": "w1", "creationTime": int(stream_created.timestamp() * 1000)}
            ]}

    controller._client = lambda _svc: _Logs()  # type: ignore[assignment]
    return controller, calls


def test_a_first_read_that_failed_never_proves_a_later_reads_window() -> None:
    group = "/msk-connect/x"
    first, later = datetime(2026, 10, 9, 5, 0, tzinfo=timezone.utc), datetime(
        2026, 10, 9, 7, 0, tzinfo=timezone.utc
    )
    # A stream created between the failed attempt's window start (23:00) and the successful
    # read's (01:00): a dead letter in it is older than what the 07:00 read covered.
    controller, calls = _flaky_controller(datetime(2026, 10, 8, 23, 30, tzinfo=timezone.utc))
    assert controller.dlq_errors(group, now=first) == []
    assert controller.dlq_read_state(group).ok is False
    controller.dlq_errors(group, now=later)
    assert controller.dlq_read_state(group).ok is True
    assert calls["describe"], "the proof ran on the read that succeeded"
    assert "6h" in (controller.dlq_coverage_bound(group) or "")
    # A stream created inside the successful read's own window still proves it.
    young, _ = _flaky_controller(datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc))
    young.dlq_errors(group, now=first)
    young.dlq_errors(group, now=later)
    assert young.dlq_coverage_bound(group) is None


# --- confirmed[8] (app part): the drain step outlasts a unique-index hold ----------------------


def test_the_cut_over_drain_step_waits_out_a_unique_index_hold() -> None:
    from dsql_migrator.ui.validation import _render_cutover_section
    from tests.test_ui_validation import _cutover_summary, _CutoverUi, _no_drift

    ui = _CutoverUi()
    _render_cutover_section(ui, _cutover_summary(), _no_drift(), cdc_in_use=True)
    (step,) = [t for t in ui.texts if t.startswith("Wait for the final drain")]
    assert "until lag is zero again, and at least 5 minutes after the last source write" in step
    assert "a change waiting on a unique index is held that long" in step


# --- residuals[3]: Start CDC refusals keep their remedy ----------------------------------------

# The pointer the capped job error and the activity log's lifecycle line keep. Repair round 4:
# it no longer names the activity log or a notice the CDC step holds now -- a restart loses
# both and restores the pointer -- only what stays true: every Start runs the checks again.
_POINTER_TAIL = (
    ". Nothing was changed; the CDC step shows the cause and the fix whenever Start CDC (or "
    "Retry CDC) runs its checks."
)


def _with_job_handle(monkeypatch, job_id: str = "job-1") -> None:
    """Run the Start job body with the handle a real JobManager passes: one carrying the id
    the job was submitted under (the harness's submit returns "job-1", but it calls the body
    with a bare namespace). The real lifecycle wrapper still runs around it."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    real = _cdc_ui._logged_cdc_lifecycle

    def _lifecycle(action, *, detail, work, residue=None):
        wrapped = real(action, detail=detail, work=work, residue=residue)
        return lambda _bare: wrapped(SimpleNamespace(job_id=job_id, cancelled=False))

    monkeypatch.setattr(_cdc_ui, "_logged_cdc_lifecycle", _lifecycle)


def _g20_refusal(monkeypatch):
    from tests.test_ui_data_migration import _G20_STOPPED_SEEDLESS, _g5_never, _g5_run

    _with_job_handle(monkeypatch)
    return _g5_run(
        monkeypatch, committed=True,
        describe_params=dict(_G20_STOPPED_SEEDLESS, StartGtidMode="ON_PERMISSIVE"),
        start_point_read=_g5_never, gtid_state="ON",
    )


def _safe(detail):
    """A ``detail`` as the activity log keeps it (its own sanitizer: the first line,
    whitespace collapsed, capped at 2,000 characters with a marker)."""
    from dsql_migrator.core.activity_log import _safe_detail

    return _safe_detail(detail)


def _kept_in_activity_log(events):
    """Each captured ``_log_cdc_event`` call as ``(action, kept detail)``."""
    return [(action, _safe(kw.get("detail"))) for action, kw in events]


def test_a_start_refusal_keeps_a_short_job_error_and_its_full_notice_for_the_cdc_card(
    monkeypatch,
) -> None:
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.cdc_gtid_gate import G2_STACK_HEADER
    from dsql_migrator.ui.data_migration import _cdc_ui

    run = _g20_refusal(monkeypatch)
    err = run["error"]
    assert isinstance(err, RuntimeError)
    # The job keeps only a first line, capped at 300: it must be the whole pointer.
    jm = JobManager(stall_timeout_seconds=None)

    def _raise(_handle):
        raise err

    job_id = jm.submit(_raise)
    jm.wait(job_id, timeout=5)
    assert jm.get_error(job_id) == (
        f"RuntimeError: Start CDC blocked: {G2_STACK_HEADER}{_POINTER_TAIL}"
    )
    # The full notice, remedy included, is kept for the CDC card -- recorded by the worker
    # under ITS job's id, the very id the card looks the failed Start up by.
    state = run["state"]
    assert state.cdc_deploy_job_id == "job-1"
    recorded = state.cdc_start_refusal_notice
    recorded_for, tone, _icon, header, body = recorded
    assert recorded_for == state.cdc_deploy_job_id
    assert (tone, header) == ("error", G2_STACK_HEADER)
    assert len(body) > 300
    assert "Delete CDC infrastructure on the CDC step, deploy it again" in body
    assert body.endswith("No connectors were changed.")
    # ...rendered under the failed Start whose job recorded it, as the worker left it...
    notices = _render_failed_start(monkeypatch, state, err)
    assert [n["header"] for n in notices] == ["CDC start failed", G2_STACK_HEADER]
    assert notices[1]["body"] == body and notices[1]["tone"] == "error"
    # ...and never under a later Start's failure.
    state.set_cdc_deploy_job_id("job-2", kind="start")
    other = _render_failed_start(monkeypatch, state, err)
    assert [n["header"] for n in other] == ["CDC start failed"]
    assert _cdc_ui._start_refusal_notice(state, None) is None


def _render_failed_start(monkeypatch, state, err):
    """Render the CDC card's job region for the state's own FAILED Start job."""
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui

    job = MigrationJob(job_id=state.cdc_deploy_job_id)
    job.status = "FAILED"
    first_line = str(err).splitlines()[0]

    class _JM:
        def get_status(self, _jid):
            return job

        def get_error(self, _jid):
            return f"RuntimeError: {first_line}"

    class _Ui:
        def refreshable(self, fn):
            fn.refresh = lambda: None
            return fn

        def timer(self, *_a, **_k):
            return None

    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda _ui, **kw: notices.append(kw))
    monkeypatch.setattr(_cdc_ui, "_render_deploy_stages", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_render_deploy_log", lambda *_a, **_k: None)
    _cdc_ui._render_cdc_deploy_live(_Ui(), state, _JM(), lambda: None)
    return notices


def test_the_start_point_refusal_is_kept_whole_too(monkeypatch) -> None:
    from tests.test_ui_data_migration import (
        _G5_NEW, _g5_live, _g5_patch_source, _g5_run, _g5_wm,
    )

    _g5_patch_source(monkeypatch, live={"app.orders": _g5_live(_G5_NEW)})
    run = _g5_run(monkeypatch, job_watermark=_g5_wm(), source_password="pw")
    first_line = str(run["error"]).splitlines()[0]
    assert first_line == (
        "Start CDC blocked: Captured tables changed since the CDC start point" + _POINTER_TAIL
    )
    _job, _tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    assert header == "Captured tables changed since the CDC start point"
    assert "app.orders (columns added or dropped: 2 → 3)" in body


# === Repair round 1 ===========================================================================


# --- the verified rule is per group again (087d0f9's), now with the open-finding exclusion ----

# A CHECKSUM match after a table's last dead letter moves its TARGET-SIDE groups into the
# Verified notice; its value-fit and DDL groups keep their own headers (Repair round 2: a
# truncate group moves too -- see test_a_verified_tables_truncate_group_moves_into_the_
# verified_notice). The drift groups come from an append-only error log, so they stay until
# Start over: a table that ever had one of these must still be shown as verified once its
# CHECKSUM proves it.
_OWN_HEADER = {
    "value-does-not-fit": "Rows set aside: a value does not fit its Aurora DSQL column",
    "add-column": "Source schema change detected",
}


@pytest.mark.parametrize("other", sorted(_OWN_HEADER))
def test_a_verified_table_keeps_only_its_source_side_group_in_the_banner(other) -> None:
    state = _state(
        {"app.flags": CdcDlqLedgerEntry(count=4, last_at=T0, kinds=["unique-conflict", other])},
        _validation([_item("app.flags")], run_start=_MATCHED),
    )
    groups = [
        {"table": "app.flags", "kind": "unique-conflict", "count": 3},
        {"table": "app.flags", "kind": other, "count": 1},
    ]
    blob, _ = _render_drift(groups, MigrationType.FULL_LOAD_AND_CDC, migration_state=state)
    assert "Verified by CHECKSUM" in blob and "app.flags matched by CHECKSUM" in blob
    assert _OWN_HEADER[other] in blob  # the other group keeps its own header and runbook
    assert "app.flags: a unique constraint rejected the row" not in blob
    for absent in ("Stop CDC and reload these tables", "Rows may be missing from the target",
                   "will not catch up on its own"):
        assert absent not in blob, (other, absent)
    # The AI DBA gets the same story: the open group, the verified table, no open 23505.
    _blob, facts, seed, _tips = _ask(
        groups, migration_type=MigrationType.FULL_LOAD_AND_CDC, migration_state=state
    )
    assert "- app.flags: unique-conflict" not in facts
    assert f"- app.flags: {other}" in facts
    assert cm._VERIFIED_FACTS_LEAD + "app.flags matched by CHECKSUM" in facts
    assert seed == cm._DRIFT_AI_SEED[cm._drift_ai_case(_drift(groups[1]))]
    # An open finding holds Cut over on its own (confirmed[7]); G25 round 4: the table is
    # still verified for its dead letters, and its notice names the finding.
    state.cdc_replica_identity_findings = {"app.flags": _OPEN_RI}
    held, _ = _render_drift(groups, MigrationType.FULL_LOAD_AND_CDC, migration_state=state)
    assert "Verified by CHECKSUM" in held and _OWN_HEADER[other] in held
    assert (
        "Cut over still holds app.flags for its open REPLICA IDENTITY finding (see that notice "
        "above)."
    ) in held
    assert "Stop CDC and reload these tables" not in held


def test_confirmed_10s_own_scenario_is_verified_on_every_surface(monkeypatch) -> None:
    # db.customers: 23505s proven by CHECKSUM, beside an add-column group of the same table.
    from dsql_migrator.ui.data_migration import _cdc_status
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    groups = [
        {"table": "db.customers", "kind": "unique-conflict", "count": 3,
         "columns": ["uk_email"], "codes": ["23505"]},
        {"table": "db.customers", "kind": "add-column", "count": 1, "codes": ["42703"]},
    ]
    ledger = {
        "db.customers": CdcDlqLedgerEntry(
            count=4, last_at=T0, kinds=["unique-conflict", "add-column"]
        )
    }
    validation = _validation([_item("db.customers")], run_start=_MATCHED)
    state = DataMigrationState()
    state.cdc_dlq_ledger = ledger
    monkeypatch.setattr(_cdc_status, "cdc_schema_drift_summary", lambda *_a: _drift(*groups))
    payload = json.loads(_executor(state, validation)("get_cdc_status", {}))
    assert payload["resolved_tables"] == [
        {"table": "db.customers", "checksum_compared_from": "2026-10-09T01:05:00+00:00"}
    ]
    blob, facts, seed, _tips = _ask(groups, migration_state=_state(ledger, validation))
    assert "Stop CDC and reload these tables" not in blob
    assert "Source schema change detected" in blob and "Verified by CHECKSUM" in blob
    assert "- db.customers: unique-conflict" not in facts
    assert "- db.customers: add-column" in facts
    assert cm._VERIFIED_FACTS_LEAD + "db.customers matched by CHECKSUM" in facts
    assert seed == cm._CDC_DRIFT_SEED  # the open group is a source DDL change


# --- the durable activity log keeps a refused Start's cause and fix ---------------------------


def test_a_gtid_start_refusal_writes_its_cause_and_fix_to_the_activity_log(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import START_REFUSAL_ACTION

    run = _g20_refusal(monkeypatch)
    _job, _tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    kept = _kept_in_activity_log(run["events"])
    # The whole notice survives the log's one-line, 2,000-character cap, remedy included...
    assert (START_REFUSAL_ACTION, f"{header}. {body}") in kept
    assert "Delete CDC infrastructure on the CDC step, deploy it again" in body
    # ...and the lifecycle FAILURE line's pointer names where the cause and fix are kept.
    (failure,) = [
        _safe(kw.get("detail")) for action, kw in run["events"]
        if action == "start CDC connectors" and kw.get("status") is ActivityStatus.FAILURE
    ]
    assert failure.endswith(f"RuntimeError: Start CDC blocked: {header}{_POINTER_TAIL}")


def test_a_start_point_refusal_writes_its_cause_and_fix_to_the_activity_log(
    monkeypatch,
) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import START_REFUSAL_ACTION
    from tests.test_ui_data_migration import (
        _G5_NEW, _g5_live, _g5_patch_source, _g5_run, _g5_wm,
    )

    _g5_patch_source(monkeypatch, live={"app.orders": _g5_live(_G5_NEW)})
    run = _g5_run(monkeypatch, job_watermark=_g5_wm(), source_password="pw")
    _job, _tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    kept = dict(_kept_in_activity_log(run["events"]))
    assert kept[START_REFUSAL_ACTION] == f"{header}. {body}"
    for remedy in ("Fetch current position", "Drop & reload"):
        assert remedy in kept[START_REFUSAL_ACTION], remedy


# --- a PostgreSQL forced (Manual) restart records the slot it continues from ------------------


def _pg_watermark(slot_name):
    return SimpleNamespace(
        gtid_executed=None, binlog_file=None, binlog_position=None, wal_lsn="0/41204A0",
        slot_name=slot_name, publication_name=None, snapshot_timestamp=None,
    )


def test_a_postgres_forced_restart_records_the_slot_not_a_snapshot() -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    slot = pg_slot_name("dsql-cdc-x")
    # A stack started before: Debezium's snapshot.mode=initial skips the snapshot once its
    # source connector has streamed and continues from the slot (live-checked), and takes it
    # when that connector never did (Repair round 2: the stack does not record which, so the
    # line names the slot and states both -- never "no snapshot" alone).
    for watermark, named in (
        (None, slot), (_pg_watermark(None), slot), (_pg_watermark("dsqlmig_x"), "dsqlmig_x"),
    ):
        line = _cdc_start_detail(
            "dsql-cdc-x", watermark, mode="manual", source_type=SourceType.POSTGRES,
            resumes=True, force_snapshot=True,
        )
        assert (
            f"start point: replication slot {named} (this stack's connectors were started "
            "before" in line
        )
        assert "this stack streamed before" not in line
        assert "where streaming stopped" in line
        assert _states_both_outcomes(line), line
        assert line.endswith(
            "Manual's full snapshot runs only if this stack's source connector never streamed"
        )
        for absent in ("none runs for this start", "a FULL initial snapshot is forced",
                       "read again", "re-snapshot", "CURRENT position", "NOT replicated"):
            assert absent not in line, (absent, line)
    # No committed offset: a forced start really snapshots, and says so.
    first = _cdc_start_detail(
        "dsql-cdc-x", None, mode="manual", source_type=SourceType.POSTGRES, force_snapshot=True
    )
    assert "start point: a snapshot" in first
    assert first.endswith("a FULL initial snapshot is forced for this start")
    again = _cdc_start_detail(
        "dsql-cdc-x", _pg_watermark("dsqlmig_x"), mode="manual",
        source_type=SourceType.POSTGRES, force_snapshot=True,
    )
    assert "re-snapshot: every selected table is read again" in again
    assert again.endswith("a FULL initial snapshot is forced for this start")


def _states_both_outcomes(line: str) -> bool:
    """A resumed ``snapshot.mode=initial`` line: what happens if the source connector
    streamed, AND if it never did -- the stack records neither (Repair round 2)."""
    return all(
        claim in line
        for claim in (
            "if its source connector streamed, no snapshot runs and the connector continues "
            "from the slot's confirmed position, where streaming stopped",
            "if it never streamed -- its task failed, or was stopped, before its snapshot "
            "completed -- Debezium snapshots every selected table first, then streams from "
            "the slot",
        )
    )


def test_the_real_postgres_start_records_the_slot_for_a_manual_restart(monkeypatch) -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name
    from tests.test_ui_data_migration import _run_start_cdc_capture

    runs = {
        mode: _run_start_cdc_capture(
            monkeypatch, source_type=SourceType.POSTGRES,
            migration_type=MigrationType.CDC_ONLY, committed=True, start_mode=mode,
        )
        for mode in ("manual", "auto")
    }
    sent = {mode: dict(run["kwargs"]["params"].filled) for mode, run in runs.items()}
    assert sent["manual"] == sent["auto"]  # Manual sends nothing a resume does not
    assert sent["manual"]["PgSnapshotMode"] == "initial"
    for mode, run in runs.items():
        slot = pg_slot_name(run["state"].cdc_stack_name)
        started = run["details"][0]
        assert f"start point: replication slot {slot}" in started, (mode, started)
        assert _states_both_outcomes(started), (mode, started)
        assert "CURRENT position" not in started and "NOT replicated" not in started


# --- CDC only: the Delete-and-Deploy advice names what the delete costs -----------------------


def _pg_session():
    return SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )


@pytest.mark.parametrize("kind", ["unique-conflict", "unbindable-value"])
def test_a_cdc_only_delete_and_deploy_advice_names_what_the_delete_costs(kind) -> None:
    group = [{"table": "shop.customers", "kind": kind, "count": 3}]
    mysql, _ = _render_drift(group, MigrationType.CDC_ONLY)
    assert "Delete and Deploy CDC infrastructure" in mysql
    assert (
        "On MySQL, deleting the CDC infrastructure also deletes the stream position: changes "
        "made before the new Start are not replicated."
    ) in mysql
    pg, _ = _render_drift(group, MigrationType.CDC_ONLY, session=_pg_session())
    assert (
        "On PostgreSQL, deleting the stack drops its replication slot, so the next Start CDC "
        "snapshots the captured tables again."
    ) in pg
    for blob in (mysql, pg):
        assert ".." not in blob and ". ." not in blob, blob
    # Full load + CDC keeps its 23505 / 07006 explanation byte for byte.
    why = cm._drift_why(SimpleNamespace(kind=kind), is_pg=False)
    assert why == cm._DRIFT_LABELS[kind][1]
    assert "deletes the stream position" not in why


# === Repair round 2 ===========================================================================


# --- a resumed PostgreSQL Start: no re-snapshot that would not run, the slot always checked ---

_SLOT_MISSING = "CDC's replication slot does not exist on the source"
_RESUMED_WM = Watermark(
    wal_lsn="0/41204A0", slot_name="dsqlmig_s1", publication_name="dsqlmig_pub_s1",
    snapshot_timestamp=T0, table_row_counts={},
)


def _pg_catalog(monkeypatch, *, slot_usable: bool, publication_present: bool = True) -> list:
    """A PostgreSQL source whose publication covers the captured tables (or is gone) and whose
    CDC slot is usable (at the watermark's LSN) or gone -- dropped while CDC was stopped.
    Returns the slot names the probes read."""
    from dsql_migrator.core import cdc_pg_slot as pg_slot

    reads: list = []

    def _read(_conn, *, publication_name, slot_name):
        reads.append(slot_name)
        return pg_slot.PgReplicationObjects(
            publication_name=publication_name, slot_name=slot_name,
            publication_present=publication_present,
            publication_publishes_all_dml=publication_present,
            publication_tables=(
                frozenset({"orders", "customers"}) if publication_present else frozenset()
            ),
            slot_present_any_database=slot_usable, slot_usable=slot_usable,
            slot_confirmed_flush_lsn="0/41204A0" if slot_usable else None,
        )

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def execute(self, statement, _params=None):
            # A Start that snapshots also reads which captured tables have inheritance
            # child tables (G25), and refuses when that read fails: none has any here.
            assert "pg_catalog.pg_inherits" in str(statement)
            return SimpleNamespace(fetchall=lambda: [])

    class _Eng:
        def connect(self):
            return _Conn()

        def dispose(self):
            return None

    monkeypatch.setattr(pg_slot, "read_pg_replication_objects", _read)
    monkeypatch.setattr(
        "dsql_migrator.ui.connect.make_source_engine_factory",
        lambda *_a, **_k: (lambda *_b, **_c: _Eng()),
    )
    return reads


def _pg_start_state(mtype, *, committed: bool, start_mode: str = "auto"):
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    state.cdc_stack_name = "dsql-cdc-stack"
    state.migration_type = mtype
    state.set_selection(TableSelection(selected_tables=["orders"]))
    state.set_cdc_start_mode(start_mode)
    if mtype is MigrationType.FULL_LOAD_AND_CDC:
        state.job_id = "fl-job"
    state.set_cdc_has_committed_offset(committed)
    return state


def _open_pg_start_dialog(monkeypatch, state):
    """The REAL Start CDC dialog for a PostgreSQL stack (its publication/slot gate reads the
    fake catalog); returns (ui, notice bodies by header)."""
    import asyncio

    import nicegui

    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from tests.test_ui_data_migration import _DialogUi, _inventory

    ui = _DialogUi()
    bodies: dict = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        bodies[header] = body

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    fl_job = SimpleNamespace(job_id="fl-job", watermark=_RESUMED_WM, status="DONE")
    monkeypatch.setattr(cdc_ui, "_render_notice", _notice)
    monkeypatch.setattr(cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_binlog_resume_gap", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_log_cdc_event", lambda *a, **k: None)
    monkeypatch.setattr(
        cdc_ui, "_current_job",
        lambda _jm, job_id: fl_job if job_id == "fl-job" else None,
    )
    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None,
    )
    asyncio.run(cdc_ui._open_cdc_start_dialog(
        ui, state, lambda: None, session=session, job_manager=object(),
        inventory=_inventory(),
    ))
    return ui, bodies


@pytest.mark.parametrize(
    "mtype, start_mode",
    [
        (MigrationType.FULL_LOAD_AND_CDC, "auto"),
        (MigrationType.FULL_LOAD_AND_CDC, "manual"),
        (MigrationType.CDC_ONLY, "auto"),
    ],
)
def test_a_resumed_postgres_start_never_offers_a_re_snapshot_that_would_not_run(
    monkeypatch, mtype, start_mode
) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    # Stopped after streaming, then the slot was dropped (a DBA freeing WAL). Before the fix:
    # Full load + CDC (auto) blocked but offered "Re-snapshot every table instead", which set
    # Manual -- snapshot.mode=initial, whose snapshot Debezium skips on a stack that streamed --
    # and unlocked Start; Manual and CDC only (initial) were not checked at all.
    _pg_catalog(monkeypatch, slot_usable=False)
    state = _pg_start_state(mtype, committed=True, start_mode=start_mode)
    ui, bodies = _open_pg_start_dialog(monkeypatch, state)
    assert ui.notices == [("error", _SLOT_MISSING)], ui.notices
    assert "Re-snapshot every table instead" not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    body = bodies[_SLOT_MISSING]
    assert body.endswith(_pg_block_route(state, _SLOT_MISSING))
    assert "so a re-snapshot cannot be relied on here" in body
    assert "use Delete CDC infrastructure and deploy it again" in body
    # Full load + CDC also has its Full Load step; CDC only names none.
    assert ("Re-run Full Load on the Full Load step choosing 'Drop & reload'" in body) is (
        mtype is MigrationType.FULL_LOAD_AND_CDC
    )
    if mtype is MigrationType.CDC_ONLY:
        assert "Full Load" not in body and "Validation reports it as an EXTRA row" in body


def test_a_first_postgres_start_keeps_its_re_snapshot_remedy(monkeypatch) -> None:
    # No committed position (a new stack): Manual's snapshot does run, so the remedy stays.
    _pg_catalog(monkeypatch, slot_usable=False)
    state = _pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=False)
    ui, bodies = _open_pg_start_dialog(monkeypatch, state)
    assert ("error", _SLOT_MISSING) in ui.notices
    assert ("info", "Re-snapshot instead — gapless, no second Full Load") in ui.notices
    assert "cannot be relied on" not in bodies[_SLOT_MISSING]
    ui.buttons["Re-snapshot every table instead"].on_click()
    assert ui.buttons["Start CDC"].enabled is True and state.cdc_start_mode() == "manual"
    # CDC only's first Start snapshots: a missing slot is no gap and does not block.
    first, _ = _open_pg_start_dialog(
        monkeypatch, _pg_start_state(MigrationType.CDC_ONLY, committed=False)
    )
    assert ("error", _SLOT_MISSING) not in first.notices
    assert first.buttons["Start CDC"].enabled is True


@pytest.mark.parametrize(
    "mtype, start_mode",
    [
        (MigrationType.FULL_LOAD_AND_CDC, "manual"),
        (MigrationType.FULL_LOAD_AND_CDC, "auto"),
        (MigrationType.CDC_ONLY, "auto"),
    ],
)
def test_the_worker_refuses_a_resumed_postgres_start_whose_slot_is_gone(
    monkeypatch, mtype, start_mode
) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    # Retry CDC (no dialog) or a stale dialog: the worker's backstop is the same gate.
    reads = _pg_catalog(monkeypatch, slot_usable=False)
    _with_job_handle(monkeypatch)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, start_mode=start_mode,
        job_watermark=_RESUMED_WM if mtype is MigrationType.FULL_LOAD_AND_CDC else None,
        committed=True, source_password="pw", migration_type=mtype,
        inventory=_inventory(), catch=True,
    )
    assert run["submitted"] and run["kwargs"] == {}, "no connector may be created"
    assert str(run["error"]).splitlines()[0] == (
        f"Start CDC blocked: {_SLOT_MISSING}{_POINTER_TAIL}"
    )
    _job, tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    assert (_job, tone, header) == ("job-1", "error", _SLOT_MISSING)
    assert body.endswith(_pg_block_route(run["state"], _SLOT_MISSING))
    # The slot read is the one the connector is given: the Full Load's, else the stack's own.
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name

    expected = (
        "dsqlmig_s1" if mtype is MigrationType.FULL_LOAD_AND_CDC
        else pg_slot_name(run["state"].cdc_stack_name)
    )
    assert set(reads) == {expected}


@pytest.mark.parametrize(
    "mtype, start_mode",
    [
        (MigrationType.FULL_LOAD_AND_CDC, "manual"),
        (MigrationType.FULL_LOAD_AND_CDC, "auto"),
        (MigrationType.CDC_ONLY, "auto"),
    ],
)
def test_a_resumed_postgres_start_whose_slot_exists_goes_ahead(
    monkeypatch, mtype, start_mode
) -> None:
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    _pg_catalog(monkeypatch, slot_usable=True)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, start_mode=start_mode,
        job_watermark=_RESUMED_WM if mtype is MigrationType.FULL_LOAD_AND_CDC else None,
        committed=True, source_password="pw", migration_type=mtype,
        inventory=_inventory(), catch=True,
    )
    assert run["error"] is None and run["kwargs"], run["error"]


# --- a resumed PostgreSQL start line states only what the stack records ----------------------


def test_a_resumed_postgres_start_line_never_claims_a_stream_the_stack_does_not_record(
    monkeypatch,
) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    # snapshot.mode=never (Full load + CDC, auto): no snapshot runs whether or not the source
    # connector streamed, and the slot stands where streaming stopped -- or at the watermark,
    # when its task failed before streaming (the stack keeps DeploySink=true either way).
    _pg_catalog(monkeypatch, slot_usable=True)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, job_watermark=_RESUMED_WM,
        committed=True, source_password="pw", migration_type=MigrationType.FULL_LOAD_AND_CDC,
        inventory=_inventory(), catch=True,
    )
    assert dict(run["kwargs"]["params"].filled)["PgSnapshotMode"] == "never"
    started = run["details"][0]
    # G25 round 6 (decision A): no "if a Full Load run since then recreated the slot" -- a Full
    # Load that takes a new start point waits for new topics on a stack that has streamed.
    assert (
        "start point: replication slot dsqlmig_s1 (this stack's connectors were started "
        "before; snapshot.mode=never, so no snapshot runs and the connector continues from "
        "the slot's confirmed position -- where streaming stopped, or the Full Load watermark "
        "WAL LSN 0/41204A0 if the connector never streamed from it)"
    ) in started
    assert "recreated the slot" not in started
    assert "this stack streamed before" not in started
    # snapshot.mode=initial: both outcomes, whatever the slot's origin; the sent mode decides.
    for watermark, mode in ((None, "auto"), (_RESUMED_WM, "manual")):
        line = _cdc_start_detail(
            "dsql-cdc-x", watermark, mode=mode, source_type=SourceType.POSTGRES,
            resumes=True, force_snapshot=mode == "manual", snapshot_mode="initial",
        )
        assert _states_both_outcomes(line), line
        assert "snapshot.mode=never" not in line, line
    # A first start (nothing started before) keeps its lines.
    assert "(gapless from the Full Load watermark)" in _cdc_start_detail(
        "dsql-cdc-x", _RESUMED_WM, mode="auto", source_type=SourceType.POSTGRES,
        snapshot_mode="never",
    )


# --- a verified table's TRUNCATE group moves into the Verified notice ------------------------


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_a_verified_tables_truncate_group_moves_into_the_verified_notice(mtype) -> None:
    # A CHECKSUM match after the table's last dead letter proves the rows the source dropped
    # are gone from the target too; before the fix the truncate warning ("The target still
    # holds the rows the source dropped ... Drop & reload") stood beside "Verified by CHECKSUM".
    def _verified_state(kinds):
        return _state(
            {"app.flags": CdcDlqLedgerEntry(count=2, last_at=T0, kinds=kinds)},
            _validation([_item("app.flags")], run_start=_MATCHED),
        )

    truncate = {"table": "app.flags", "kind": "source-truncate", "count": 1}
    unique = {"table": "app.flags", "kind": "unique-conflict", "count": 1}
    value_fit = {"table": "app.flags", "kind": "value-does-not-fit", "count": 1}
    for groups in ([truncate], [truncate, unique]):
        state = _verified_state([g["kind"] for g in groups])
        blob, _ = _render_drift(groups, mtype, migration_state=state)
        assert "Verified by CHECKSUM" in blob and "app.flags matched by CHECKSUM" in blob
        for absent in ("Source TRUNCATE was not replicated", "The target still holds",
                       "Drop & reload", "Stop CDC and", "Ask AI DBA"):
            assert absent not in blob, (groups, absent)
    # Beside a value-fit group the value-fit group keeps its header and runbook, and the AI DBA
    # is told that story only.
    state = _verified_state(["source-truncate", "value-does-not-fit"])
    blob, facts, seed, tooltips = _ask(
        [truncate, value_fit], migration_type=mtype, migration_state=state
    )
    assert "Rows set aside: a value does not fit its Aurora DSQL column" in blob
    assert "Verified by CHECKSUM" in blob and "Source TRUNCATE was not replicated" not in blob
    assert seed == cm._CDC_VALUE_FIT_SEED
    assert tooltips == [cm._DRIFT_AI_TOOLTIP["value-fit"]]
    assert "- app.flags: source-truncate" not in facts
    assert "- app.flags: value-does-not-fit" in facts
    assert cm._VERIFIED_FACTS_LEAD + "app.flags matched by CHECKSUM" in facts
    assert "unbindable-value or source-truncate dead letters need no repair" in facts
    # Not verified (the comparison started before the last dead letter): the warning stays.
    open_state = _state(
        {"app.flags": CdcDlqLedgerEntry(count=1, last_at=_MATCHED, kinds=["source-truncate"])},
        _validation([_item("app.flags")], run_start=_MATCHED),
    )
    blob, _ = _render_drift([truncate], mtype, migration_state=open_state)
    assert "Source TRUNCATE was not replicated" in blob and "Verified by CHECKSUM" not in blob


# --- the worker's foreign-key refusal keeps its fix -------------------------------------------


def _fk_refusal(monkeypatch, fk_state):
    """Run the REAL Start CDC worker against a foreign-key probe verdict (Retry CDC: no
    dialog). Returns (state, error, logged events); nothing may be built or created."""
    import dsql_migrator.config as _config
    import dsql_migrator.core.cdc_deployer as _dep
    import dsql_migrator.ui.data_migration._cdc_ui as _cdcui
    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.data_migration._state import DataMigrationState
    from tests.test_ui_data_migration import _inventory

    events: list = []
    built: list = []
    monkeypatch.setattr(_dep, "build_cdc_stack_deployer", lambda *a, **k: built.append("d"))
    monkeypatch.setattr(_dep, "run_cdc_start", lambda *a, **k: built.append("start"))
    monkeypatch.setattr(_cdcui, "_read_cdc_template_body", lambda: "T")
    monkeypatch.setattr(_cdcui, "_log_cdc_event", lambda a, **k: events.append((a, k)))
    monkeypatch.setattr(_cdcui, "_probe_cdc_blocking_foreign_keys", lambda *a, **k: fk_state)
    monkeypatch.setattr(_cdcui, "render_notice", lambda _ui, **_kw: None)
    monkeypatch.setattr(_config, "load_config", lambda *a, **k: SimpleNamespace(
        cdc_seed_mode="lambda", cdc_host_subnet_cidr="", cdc_msk_access=True,
        cdc_sink_mcu_count=1,
    ))
    _with_job_handle(monkeypatch)

    class _JM:
        work = None

        def submit(self, work):
            self.work = work
            return "job-1"

        def get_status(self, job_id):
            raise JobNotFoundError(job_id)

    session = SimpleNamespace(
        target_config=SimpleNamespace(
            region="us-east-1", cluster_endpoint="ep.dsql.amazonaws.com",
            database="postgres", username="admin",
        ),
        aws_profile=None, source_password=None, source_secret_id=None,
        source_config=SimpleNamespace(source_type=SourceType.MYSQL, database="app", port=3306),
    )
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["orders"]))
    jm = _JM()
    from dsql_migrator.ui.data_migration._cdc_ui import _start_cdc_deploy

    _start_cdc_deploy(
        SimpleNamespace(notify=lambda *a, **k: None), state, jm, lambda: None,
        inventory=_inventory(), session=session,
    )
    with pytest.raises(RuntimeError) as excinfo:
        jm.work(SimpleNamespace())
    assert built == [], "nothing may be built or created"
    return state, excinfo.value, events


@pytest.mark.parametrize(
    "ours, unknown, header, fix",
    [
        (3, 0, "The target still has enforced foreign keys",
         "Reopen Start CDC and use 'Remove foreign keys'"),
        (15, 0, "The target still has enforced foreign keys",
         "Reopen Start CDC and use 'Remove foreign keys'"),
        (0, 12, "The target's foreign keys could not be verified",
         "Re-test the target connection and retry"),
    ],
)
def test_the_foreign_key_refusal_keeps_its_fix_for_the_cdc_card(
    monkeypatch, ours, unknown, header, fix
) -> None:
    from dsql_migrator.core.cdc import CdcBlockingForeignKeys
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration._cdc_ui import START_REFUSAL_ACTION

    fk = CdcBlockingForeignKeys(
        ours=[(f"shop.order_items{i}", f"order_items{i}_order_id_fkey") for i in range(ours)],
        foreign=[],
        unknown_tables=[f"ecommerce_demo.table_{i:02d}" for i in range(unknown)],
    )
    state, err, events = _fk_refusal(monkeypatch, fk)
    # The job keeps a first line capped at 300 characters: it is the whole pointer now.
    jm = JobManager(stall_timeout_seconds=None)

    def _raise(_handle):
        raise err

    job_id = jm.submit(_raise)
    jm.wait(job_id, timeout=5)
    assert jm.get_error(job_id) == f"RuntimeError: Start CDC blocked: {header}{_POINTER_TAIL}"
    # The full notice, fix included, is kept for the CDC card under the Start's own job...
    recorded_for, tone, icon, recorded_header, body = state.cdc_start_refusal_notice
    assert (recorded_for, tone, icon, recorded_header) == ("job-1", "error", "link", header)
    assert fix in body and len(body) > 300
    notices = _render_failed_start(monkeypatch, state, err)
    assert [n["header"] for n in notices] == ["CDC start failed", header]
    assert notices[1]["body"] == body
    # ...and the durable activity log keeps it whole, on the refusal's own line (it replaces
    # the separate "start blocked by target foreign keys" line).
    kept = _kept_in_activity_log(events)
    assert (START_REFUSAL_ACTION, f"{header}. {body}") in kept
    assert "start blocked by target foreign keys" not in [action for action, _ in kept]


# === Repair round 3 ===========================================================================


# --- CDC only: the dead-letter card repairs like the banner above it, never by a reload ------

# The card's "Confirm the effect of these drops in Validation" body on Full load + CDC: round 3
# changed CDC only alone, so this stays byte for byte what it was.
_DLQ_CARD_FULL_LOAD = (
    "A dead-lettered record is a row the stream did NOT apply, and it is never retried; "
    "Validation is what shows the consequence on the target. Rows MISSING on the target are a "
    "dropped insert (reload that table to backfill it); rows EXTRA on the target are a dropped "
    "delete (a reload cannot remove it — the table must be dropped and reloaded, or the row "
    "deleted by hand). Both repairs need Stop CDC first: while the sink is streaming, Full "
    "Load collides with it and the drop-and-recreate option is not offered at all. A dropped "
    "UPDATE changes no row count, so the default row-count mode cannot see it: Validation "
    "therefore also compares these tables by CHECKSUM, and Cut over stays locked until each "
    "has matched in a CHECKSUM comparison that started after its last dead-lettered record. "
    "CHECKSUM cannot value-compare json columns (nor, from a MySQL source, floating-point "
    "ones); the result names them per table."
)
_DLQ_CARD_LEAD = "A dead-lettered record is a row the stream did NOT apply"


def _dlq_card(mtype, kind):
    """The REAL dead-letter card for 3 dead-lettered rows of shop.customers: (the whole card's
    text, its "Confirm the effect of these drops in Validation" body)."""
    from dsql_migrator.core.cdc import build_cdc_status_view
    from dsql_migrator.core.models import ErrorLogSummary, MigrationJob
    from dsql_migrator.ui.data_migration._state import DataMigrationState
    from tests.test_ui_data_migration import _DlqUi, _JobJM

    state = DataMigrationState()
    state.migration_type = mtype
    view = build_cdc_status_view(
        [], ErrorLogSummary(total_errors=3, errors_by_table={"shop.customers": 3}),
        dlq_depth=3, schema_drift=_drift({"table": "shop.customers", "kind": kind, "count": 3}),
    )
    ui = _DlqUi()
    cm._render_cdc_dlq_panel(ui, state, _JobJM(MigrationJob(job_id="job-cdc")), view)
    (card,) = [text for text in ui.texts if text.startswith(_DLQ_CARD_LEAD)]
    return " ".join(ui.texts), card


@pytest.mark.parametrize("kind", ["unique-conflict", "missing-table", "unbindable-value"])
def test_a_cdc_only_dlq_card_repairs_like_the_banner_never_by_a_reload(kind) -> None:
    # Before the fix the card under the banner's "Stop CDC and repair these tables" said
    # "reload that table to backfill it ... Full Load collides with it": a Full Load step CDC
    # only does not have, contradicting the notice right above it.
    blob, card = _dlq_card(MigrationType.CDC_ONLY, kind)
    assert "Stop CDC and repair these tables" in blob
    assert (
        "Rows MISSING on the target, or holding a stale value, are a dropped insert or update: "
        "with CDC stopped, correct them on Aurora DSQL by hand (at most 3,000 rows per "
        "transaction), or migrate the table again with Start over. Rows EXTRA on the target "
        "are a dropped delete: delete them by hand, with CDC stopped too. The record list "
        "below names each record's primary key, but shows a key value only when it is an "
        "integer or a UUID (any other is withheld): for a table whose key values it "
        "withholds, use Start over."
    ) in card
    for absent in ("reload that table", "Full Load collides", "drop-and-recreate"):
        assert absent not in blob, (kind, absent)
    assert "reload" not in card and "Full Load" not in card, card
    # What Validation shows, and the CHECKSUM rule Cut over holds to, are the same text.
    assert card.startswith(_DLQ_CARD_FULL_LOAD[: _DLQ_CARD_FULL_LOAD.index(" Rows MISSING")])
    assert card.endswith(_DLQ_CARD_FULL_LOAD[_DLQ_CARD_FULL_LOAD.index(" A dropped UPDATE"):])


def test_the_full_load_and_cdc_dlq_card_is_unchanged() -> None:
    from dsql_migrator.core.models import ErrorLogSummary
    from tests.test_ui_data_migration import _DlqUi

    _blob, card = _dlq_card(MigrationType.FULL_LOAD_AND_CDC, "unique-conflict")
    assert card == _DLQ_CARD_FULL_LOAD
    # A caller that names no migration type (the earlier signature) gets the same card.
    ui = _DlqUi()
    cm._render_cdc_dlq_breakdown(ui, SimpleNamespace(
        error_summary=ErrorLogSummary(total_errors=1, errors_by_table={"app.t": 1})
    ))
    assert _DLQ_CARD_FULL_LOAD in ui.texts


# --- PostgreSQL: the resume surfaces state what the stack records, like the start line -------

# What a stopped PostgreSQL stack does not record: whether its source connector ever streamed.
_UNRECORDED = ("streamed before", "no re-load", "nothing re-applied")
# G25 round 5: no "with no gap" / "No gap either way" -- the changes the sink had not applied
# when CDC stopped come from the CDC topics, so the resume is gapless only while they retain
# them (_RETAINED).
_BOTH_OUTCOMES = (
    "If its source connector streamed, it picks up where streaming stopped. If it never "
    "completed its initial snapshot (its task failed, or CDC was stopped, before then), "
    "Debezium snapshots every selected table first, then streams: a full re-read of the "
    "source, which takes a while on a large one."
)
# G25 round 6 (decision A): no resume from a slot a Full Load re-created after the stack
# streamed -- such a Full Load waits for new topics.
_NO_SNAPSHOT = (
    "continues from that slot with no snapshot (snapshot.mode=never): from where streaming "
    "stopped, or from the Full Load's start point if its source connector never streamed."
)
_RETAINED = (
    "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so "
    "this is gapless only while the topics still retain them"
)
_WM_WITH_TABLE = Watermark(
    wal_lsn="0/41204A0", slot_name="dsqlmig_s1", publication_name="dsqlmig_pub_s1",
    snapshot_timestamp=T0, table_row_counts={"orders": 1},
)


def _pg_resume_surfaces(mtype, *, watermark=None, start_mode="auto"):
    """A stopped PostgreSQL stack's three resume surfaces, rendered for real: (the Start CDC
    card, the start-point card through its caller, the Stop dialog), as text."""
    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _CardUi
    from tests.test_ui_data_migration import _RecordingUi

    state = _pg_start_state(mtype, committed=True, start_mode=start_mode)
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    fl_job = SimpleNamespace(job_id="fl-job", watermark=watermark, status="DONE")

    class _JM:
        def get_status(self, job_id):
            if job_id == "fl-job" and watermark is not None:
                return fl_job
            raise JobNotFoundError(job_id)

    start, card, stop = _RecordingUi(), _CardUi(), _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        start, state, _JM(), lambda: None, inventory=None, session=_pg_session()
    )
    _cdc_ui._render_cdc_source_config_card(
        card, state, _JM(), lambda: None, inventory=None, migration_type=mtype,
        session=_pg_session(),
    )
    _cdc_ui._open_cdc_stop_dialog(stop, state, lambda: None, session=_pg_session())
    return state, " ".join(start.texts), " ".join(card.texts), " ".join(stop.texts)


@pytest.mark.parametrize(
    "mtype, watermark, start_mode",
    [
        (MigrationType.CDC_ONLY, None, "auto"),
        (MigrationType.FULL_LOAD_AND_CDC, _WM_WITH_TABLE, "manual"),
    ],
)
def test_a_postgres_resume_never_promises_a_stream_the_stack_does_not_record(
    mtype, watermark, start_mode
) -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name

    # snapshot.mode=initial on a stack whose connectors were started before: a source
    # connector stopped before its snapshot completed re-snapshots every table on this Start
    # (live: "Previous snapshot was cancelled before completion; a new snapshot will be
    # taken"), while the cards promised "streamed before ... no re-load" / "nothing re-applied".
    state, start, card, stop = _pg_resume_surfaces(
        mtype, watermark=watermark, start_mode=start_mode
    )
    slot = "dsqlmig_s1" if watermark is not None else pg_slot_name(state.cdc_stack_name)
    assert "Ready to resume CDC" in start
    assert "Resuming from this stack's replication slot" in card
    for surface in (start, card):
        assert f"not replication slot {slot}." in surface, surface
        assert _BOTH_OUTCOMES in surface, surface
        assert _RETAINED in surface and "with no gap" not in surface, surface
    # G25 round 6: no unconditional "no gap" -- the sink's unapplied changes at the Stop come
    # from the CDC topics, so the condition and the route past it are stated.
    assert (
        "Start CDC re-creates the connectors and a source connector that has streamed "
        "continues from where streaming stopped, with no Full Load or start point "
        "needed again. One still taking its initial snapshot has not streamed yet: stopping "
        "it now cancels that snapshot, and the next Start snapshots every selected table "
        "again first. Changes the sink has not applied when CDC stops are applied from the "
        "CDC topics on the next Start, so a restart is gapless only while the topics still "
        "retain them: once their retention period on MSK has passed, a Start would skip "
        "them, so instead use Delete CDC infrastructure and deploy it again"
    ) in stop
    assert "no gap" not in stop
    for surface in (start, card, stop):
        for claim in _UNRECORDED:
            assert claim not in surface, (claim, surface)


def test_a_postgres_resume_with_snapshot_mode_never_says_no_snapshot_runs() -> None:
    # Full load + CDC, Automatic, the slot the load recorded: snapshot.mode=never, so there is
    # no snapshot to take again -- whether or not the source connector streamed.
    _state, start, card, stop = _pg_resume_surfaces(
        MigrationType.FULL_LOAD_AND_CDC, watermark=_WM_WITH_TABLE
    )
    for surface in (start, card):
        assert "not replication slot dsqlmig_s1." in surface
        assert _NO_SNAPSHOT in surface, surface
        assert _RETAINED in surface and "No gap either way" not in surface, surface
        # Its route once the topics no longer hold them: new topics, then the Full Load re-run.
        assert "so instead use Delete CDC infrastructure and deploy it again" in surface
        assert "snapshots every selected table" not in surface, surface
    for surface in (start, card, stop):
        for claim in _UNRECORDED:
            assert claim not in surface, (claim, surface)


def test_the_mysql_resume_surfaces_keep_their_wording() -> None:
    # MySQL: the seeder records the offset, so the resume promise stands -- G25 round 6: with
    # the condition the topics set on it (Stop removes the sink without a drain, and the sink
    # re-applies from its last commit), never an unconditional "no gap" / "nothing re-applied".
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _CardUi, _JM, _WM, _job, _state as _mysql
    from tests.test_ui_data_migration import _RecordingUi

    deployed = ("mysql-bin-changelog.000007", "108063011")
    start, card, stop = _RecordingUi(), _CardUi(), _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        start, _mysql(deployed=deployed), _JM(_job(_WM)), lambda: None, inventory=None,
        session=None,
    )
    _cdc_ui._render_cdc_source_config_card(
        card, _mysql(deployed=deployed), _JM(_job(_WM)), lambda: None, inventory=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, session=None,
    )
    _cdc_ui._open_cdc_stop_dialog(stop, _mysql(deployed=deployed), lambda: None)
    retained = (
        "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so "
        "this is gapless only while the topics still retain them (their retention period on "
        "MSK)."
    )
    assert (
        "This pipeline streamed before and its resume position is still recorded on MSK — "
        "stopping CDC removed only the connectors, not that position. Start CDC re-creates "
        "them and continues from exactly where streaming stopped, with no re-load and no "
        f"need to re-enter a start point. {retained} It takes a few minutes; progress appears "
        "below."
    ) in " ".join(start.texts)
    assert (
        "This pipeline has streamed before, so its resume position is already committed on MSK "
        "(stopping CDC deleted the connectors, not the position). Streaming continues from "
        f"there — no start point needed and no Full Load watermark required. {retained}"
    ) in " ".join(card.texts)
    assert (
        "and so is the recorded stream position. Start CDC re-creates the connectors and "
        "continues from exactly where streaming stopped, with no Full Load or start point "
        "needed again. Changes the sink has not applied when CDC stops are applied from the "
        "CDC topics on the next Start, so a restart is gapless only while the topics still "
        "retain them (their retention period on MSK). Restart as often as you like — but do "
        "not LEAVE it stopped."
    ) in " ".join(stop.texts)
    for surface in (start, card, stop):
        blob = " ".join(surface.texts)
        assert "no gap" not in blob and "nothing re-applied" not in blob, blob


# --- a refused Start's durable record keeps every fix, however long its lists ---------------


def _refusal_records(events):
    from dsql_migrator.ui.data_migration._cdc_ui import START_REFUSAL_ACTION

    return [detail for action, detail in _kept_in_activity_log(events)
            if action == START_REFUSAL_ACTION]


def test_a_large_foreign_key_refusal_keeps_every_fix_in_the_activity_log(monkeypatch) -> None:
    # 60 migration-created keys, 40 the migration does not account for, and a connect blip
    # on 200 tables: each unbounded list pushed the fix after it past the activity log's
    # 2,000-character line ("... [+N chars truncated]"), so after an app restart nothing the
    # job's pointer names held the fix.
    from dsql_migrator.core.cdc import CdcBlockingForeignKeys

    fk = CdcBlockingForeignKeys(
        ours=[(f"ecommerce_demo.order_items_{i:02d}", f"order_items_{i:02d}_order_id_fkey")
              for i in range(60)],
        foreign=[(f"ecommerce_demo.audit_{i:02d}", f"audit_{i:02d}_actor_fkey")
                 for i in range(40)],
        unknown_tables=[f"ecommerce_demo.table_{i:03d}" for i in range(200)],
    )
    state, err, events = _fk_refusal(monkeypatch, fk)
    (record,) = _refusal_records(events)
    assert "chars truncated" not in record
    for fix in ("Reopen Start CDC and use 'Remove foreign keys'",
                "Drop them yourself before starting CDC",
                "Re-test the target connection and retry"):
        assert fix in record, fix
    # Each list names six, then counts the rest.
    for bound in ("order_items_05_order_id_fkey (+54 more)", "audit_05_actor_fkey (+34 more)",
                  "ecommerce_demo.table_005 (+194 more)"):
        assert bound in record, bound
    assert "order_items_06_order_id_fkey" not in record
    # The CDC card shows that same notice, and the job keeps its whole pointer.
    _job, _tone, _icon, header, body = state.cdc_start_refusal_notice
    assert record == f"{header}. {body}"
    pointer = str(err).splitlines()[0]
    assert len(pointer) <= 300 and pointer.endswith(_POINTER_TAIL)


def test_the_start_cdc_dialog_still_lists_every_foreign_key() -> None:
    # The dialog is where the keys are removed or dropped by hand: it names every one.
    from dsql_migrator.core.cdc import CdcBlockingForeignKeys
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_fk_block_body

    fk = CdcBlockingForeignKeys(
        ours=[], foreign=[(f"app.t{i}", f"t{i}_fkey") for i in range(40)], unknown_tables=[]
    )
    body = _cdc_fk_block_body(fk)
    assert all(f"app.t{i}.t{i}_fkey" in body for i in range(40)) and "more)" not in body


def test_a_start_point_refusal_longer_than_a_log_line_is_recorded_whole(monkeypatch) -> None:
    # A character-set conversion changes every table "by definition": eight such tables make
    # the start-point refusal longer than one activity-log line, and the cut fell in its fix.
    from dsql_migrator.core.activity_log import _MAX_DETAIL_CHARS
    from dsql_migrator.core.cdc_catalog_drift import REASON_DEF
    from dsql_migrator.ui.cdc_start_point_gate import (
        KIND_CHANGED,
        RELOAD_RESETS_MANUAL,
        StartPointVerdict,
    )
    from tests.test_ui_data_migration import _g5_run, _g5_wm

    changed = tuple((f"ecommerce_demo.order_items_{i:02d}", REASON_DEF) for i in range(8))
    _with_job_handle(monkeypatch)
    run = _g5_run(
        monkeypatch, job_watermark=_g5_wm(), source_password="pw",
        start_point_read=lambda *_a: StartPointVerdict(kind=KIND_CHANGED, changed=changed),
    )
    _job, _tone, _icon, header, body = run["state"].cdc_start_refusal_notice
    notice = f"{header}. {body}"
    fix_end = f"{RELOAD_RESETS_MANUAL} No connectors were changed."
    assert notice.endswith(fix_end)
    # As one line, the log's own cap would cut the notice inside its fix.
    assert "chars truncated" in _safe(notice) and fix_end not in _safe(notice)
    records = _refusal_records(run["events"])
    assert [line[:6] for line in records] == ["(1/2) ", "(2/2) "]
    assert all(len(line) <= _MAX_DETAIL_CHARS for line in records)
    assert not any("chars truncated" in line for line in records)
    # Read in order, the parts are the whole notice -- its fix, which comes last, included.
    assert " ".join(line[6:] for line in records) == notice
    assert str(run["error"]).splitlines()[0].endswith(_POINTER_TAIL)


# === Repair round 4 (the review of round 3) ====================================================


# --- a slot the source connector never created: the block states both outcomes --------------


# What the slot block says when the source has no slot of that name: what each outcome means,
# and that the stack does not record which (a dropped slot and one never created look alike).
_NEVER_CREATED_OUTCOMES = (
    "If this stack's source connector streamed, Start CDC continues the position it recorded "
    "in that slot, and a slot cannot be created at a past position: the connector would "
    "create one at the CURRENT WAL and silently skip every change committed since streaming "
    "stopped.",
    "If it never streamed (its task failed, or CDC was stopped, before its snapshot "
    "completed, for example before it created the slot), the Start would snapshot every "
    "selected table first, with no gap.",
    "The stack does not record which, so Start CDC is blocked either way.",
)


def test_a_never_created_slot_block_states_both_outcomes_and_stays_closed(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    # CDC only: the first Start's source task failed before Debezium created its slot (every
    # slot in use, a missing replication role), and the operator did what the CDC card said --
    # Stop CDC, then Start CDC. The stopped stack keeps DeploySink=true, so this Start resumes,
    # and no slot exists. The CDC card says such a Start snapshots first; the block said,
    # unconditionally, "Start CDC continues the stream position this stack's source connector
    # recorded ... silently skip every change committed since streaming stopped".
    _pg_catalog(monkeypatch, slot_usable=False)
    state = _pg_start_state(MigrationType.CDC_ONLY, committed=True)
    ui, bodies = _open_pg_start_dialog(monkeypatch, state)
    # Still fail-closed: a dropped slot and one never created look the same on the source.
    assert ui.notices == [("error", _SLOT_MISSING)], ui.notices
    assert ui.buttons["Start CDC"].enabled is False
    body = bodies[_SLOT_MISSING]
    assert "Start CDC continues the stream position" not in body
    for claim in _NEVER_CREATED_OUTCOMES:
        assert claim in body, (claim, body)
    assert body.endswith(_pg_block_route(state, _SLOT_MISSING))
    assert "use Delete CDC infrastructure and deploy it again" in body
    # The CDC card above it says the same: a connector that never streamed snapshots first.
    _state, start, _card, _stop = _pg_resume_surfaces(MigrationType.CDC_ONLY)
    assert "Debezium snapshots every selected table first" in start
    # The worker's backstop (Retry CDC, a stale dialog) refuses with the same story.
    _with_job_handle(monkeypatch)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, committed=True, source_password="pw",
        migration_type=MigrationType.CDC_ONLY, inventory=_inventory(), catch=True,
    )
    assert run["kwargs"] == {}, "no connector may be created"
    _job, _tone, _icon, header, refused = run["state"].cdc_start_refusal_notice
    assert header == _SLOT_MISSING
    assert all(claim in refused for claim in _NEVER_CREATED_OUTCOMES), refused


# --- after an app restart, a refused Start's pointer still says where the fix is -------------


def _render_job_region(monkeypatch, state, job_manager):
    """The CDC card's job region for the state's own Start job, read off ``job_manager``."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _Ui:
        def refreshable(self, fn):
            fn.refresh = lambda: None
            return fn

        def timer(self, *_a, **_k):
            return None

    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda _ui, **kw: notices.append(kw))
    monkeypatch.setattr(_cdc_ui, "_render_deploy_stages", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_render_deploy_log", lambda *_a, **_k: None)
    _cdc_ui._render_cdc_deploy_live(_Ui(), state, job_manager, lambda: None)
    return notices


def test_a_refused_starts_pointer_stays_true_after_an_app_restart(monkeypatch, tmp_path) -> None:
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.job_store import SqliteJobStore
    from dsql_migrator.core.session_state_store import SqliteSessionStateStore
    from dsql_migrator.ui.cdc_gtid_gate import G2_STACK_HEADER
    from dsql_migrator.ui.data_migration._state import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )
    from tests.test_ui_data_migration import _G20_STOPPED_SEEDLESS, _g5_never, _g5_run

    # The full notice is process memory, and the default Fargate task's activity log is its
    # own /tmp file (the CloudWatch mirror is off by default). A restart -- a task replacement
    # on redeploy -- brings back the FAILED Start and its pointer from the durable job store
    # and the session snapshot, so the pointer must not send the operator to either.
    run = _g20_refusal(monkeypatch)
    err, state = run["error"], run["state"]
    jobs = str(tmp_path / "jobs.sqlite")
    jm = JobManager(store=SqliteJobStore(jobs), stall_timeout_seconds=None)

    def _raise(_handle):
        raise err

    job_id = jm.submit(_raise, job_id=state.cdc_deploy_job_id)
    jm.wait(job_id, timeout=5)
    sessions = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    sessions.save(capture_session_snapshot(
        "s1", SessionConnectionState(), EvaluationState(), SchemaConversionState(), state
    ))
    # --- the restart: a new process, the same stores ---
    restarted = JobManager(stall_timeout_seconds=None)
    restarted.attach_store(SqliteJobStore(jobs))
    fresh = DataMigrationState()
    apply_session_snapshot(
        sessions.load("s1"), SessionConnectionState(), EvaluationState(),
        SchemaConversionState(), fresh, job_manager=restarted,
    )
    assert fresh.cdc_deploy_job_id == job_id
    assert getattr(fresh, "cdc_start_refusal_notice", None) is None  # not persisted
    pointer = restarted.get_error(job_id)
    assert pointer == f"RuntimeError: Start CDC blocked: {G2_STACK_HEADER}{_POINTER_TAIL}"
    assert "activity log" not in pointer
    assert "whenever Start CDC (or Retry CDC) runs its checks" in pointer
    notices = _render_job_region(monkeypatch, fresh, restarted)
    assert [n["header"] for n in notices] == ["CDC start failed"]
    assert notices[0]["body"].endswith(f"Reported cause: {pointer}")
    # ...and what it promises holds: the next Start (here Retry CDC, no dialog) runs the checks
    # again, and the CDC step shows the cause and the fix under it.
    fresh.set_cdc_has_committed_offset(True)  # what the CDC step's phase probe reads back
    again = _g5_run(
        monkeypatch, state=fresh, start_point_read=_g5_never, gtid_state="ON",
        describe_params=dict(_G20_STOPPED_SEEDLESS, StartGtidMode="ON_PERMISSIVE"),
    )
    assert str(again["error"]).splitlines()[0] == (
        f"Start CDC blocked: {G2_STACK_HEADER}{_POINTER_TAIL}"
    )
    shown = _render_failed_start(monkeypatch, fresh, again["error"])
    assert [n["header"] for n in shown] == ["CDC start failed", G2_STACK_HEADER]
    assert "Delete CDC infrastructure on the CDC step, deploy it again" in shown[1]["body"]


# === Repair round 5 (the review of round 4) ====================================================


# --- a stopped stack's connector-log cause names no button its Start never offers ------------

# Debezium's own line (debezium-connector-postgres 2.7.4) when snapshot.mode=never finds no
# publication to stream from (publication.autocreate.mode=disabled).
_PUBLICATION_GONE_TAIL = (
    "ERROR WorkerSourceTask{id=dsql-cdc-stack-debezium-source-0} Task threw an uncaught and "
    "unrecoverable exception.\n"
    "org.apache.kafka.connect.errors.ConnectException: Publication autocreation is disabled, "
    "please create one and restart the connector."
)


def test_no_connector_cause_a_stop_keeps_names_a_re_snapshot_its_start_never_offers(
    monkeypatch,
) -> None:
    # Full load + CDC, Automatic: the source task died on a dropped publication. The cause, kept
    # across Stop CDC, said "Stop CDC, then Start CDC again and choose 'Re-snapshot every table
    # instead' when the dialog offers it". Stop keeps DeploySink=true, so every later Start
    # resumes, and a resumed Start never offers that button (G25): the step could not be done.
    from dsql_migrator.core.cdc import cdc_expected_connector_names
    from dsql_migrator.core.cdc_deployer import _CONNECTOR_LOG_DIAGNOSES, diagnose_connector_log
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    for needle, guidance in _CONNECTOR_LOG_DIAGNOSES:
        assert "Re-snapshot every table instead" not in guidance, needle
    state = _pg_start_state(MigrationType.FULL_LOAD_AND_CDC, committed=True)
    source, _sink = cdc_expected_connector_names(state.cdc_stack_name)
    cause = diagnose_connector_log(source, _PUBLICATION_GONE_TAIL, episode_only=True)
    assert cause is not None
    assert cause.headline == "the PostgreSQL CDC publication does not exist"
    # The Start CDC dialog it sends the operator to, on that stopped stack: blocked, with no
    # re-snapshot button, and naming the same two routes as the cause.
    _pg_catalog(monkeypatch, slot_usable=True, publication_present=False)
    ui, bodies = _open_pg_start_dialog(monkeypatch, state)
    block = "CDC's publication does not exist on the source"
    assert ui.notices == [("error", block)], ui.notices
    assert "Re-snapshot every table instead" not in ui.buttons
    assert bodies[block].endswith(_pg_block_route(state, block))
    for route in ("Re-run Full Load on the Full Load step choosing 'Drop & reload'",
                  "Delete CDC infrastructure and deploy it again"):
        assert route in cause.guidance and route in bodies[block], route
    assert "Do NOT create a replication slot by hand" in cause.guidance


# --- PostgreSQL: the dead-task card claims no stream position the stack does not record ------


@pytest.mark.parametrize("stopped", ["source", "sink"])
def test_a_postgres_dead_task_card_claims_no_position_the_stack_does_not_record(stopped) -> None:
    # CDC only (snapshot.mode=initial): a source task that stopped in its initial snapshot never
    # streamed, so after Stop CDC the next Start snapshots every table again (or, with no slot
    # created yet, is blocked). The card said "Stop CDC, then Start CDC: it continues from the
    # recorded stream position" -- what the three resume surfaces no longer claim.
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _RecordingUi, _g7_names, _g7_state

    source, sink = _g7_names()

    def _dead_task_card(session):
        state = _g7_state()
        state.migration_type = MigrationType.CDC_ONLY
        ui = _RecordingUi()
        _cdc_ui._render_cdc_partial_actions(
            ui, state, object(), lambda: None, names=[source, sink], expected=(source, sink),
            failed=[source if stopped == "source" else sink], session=session,
        )
        return " ".join(ui.texts)

    postgres = _dead_task_card(_pg_session())
    assert "CDC is not streaming — a connector stopped" in postgres
    assert "continues from the recorded stream position" not in postgres
    assert (
        "Once the cause is fixed, Stop CDC; the CDC step then says where Start CDC continues "
        "from."
    ) in postgres
    # ...which the card shown once CDC is stopped does, with both outcomes.
    _state, start, _start_point, _stop = _pg_resume_surfaces(MigrationType.CDC_ONLY)
    assert "Ready to resume CDC" in start and _BOTH_OUTCOMES in start
    # MySQL: the seeder records the offset, so the promise stands.
    assert (
        "Once the cause is fixed, Stop CDC, then Start CDC: it continues from the recorded "
        "stream position."
    ) in _dead_task_card(None)


# --- the dead-letter card: its AI DBA and its hand repair say what the record list shows -----

# The sink's own dead-letter line for a source TRUNCATE (DebeziumEvents.truncateNotApplicable,
# logged by DsqlSinkTask.quarantine): its reason ends in a Full Load "Recovery:" clause.
_TRUNCATE_LINE = (
    "Quarantined record to DLQ (topic=dsqlcdc.shop.events, partition=0, offset=7): Cannot "
    "apply a source TRUNCATE of table shop.events: Aurora DSQL has no TRUNCATE statement, so "
    "there is no correct apply and this event is quarantined rather than guessed at. The "
    "target still holds the rows the source truncated, so the two now differ by exactly those "
    "rows (Validation reports them as extra). Recovery: reload this table on the Full Load "
    "step with 'Drop & reload', which recreates it and re-reads the source -- an upsert-only "
    "reload cannot remove rows the source no longer has. "
    "(dev.dsqlmigrator.connect.DsqlSinkTask:818)"
)


def _dlq_panel(state, lines):
    """Land the sink's dead-letter log ``lines`` the way the CDC poll does, render the REAL
    dead-letter panel with a capturing AI opener and click each Ask AI DBA; returns (ui, the
    AI facts by scope)."""
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.cdc_dlq import parse_dlq_log_message
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status
    from tests.test_ui_data_migration import _DlqUi, _JobJM

    class _Ui(_DlqUi):
        def __init__(self):
            super().__init__()
            self.asks: list = []

        def button(self, text="", *a, on_click=None, **k):
            if text == "Ask AI DBA":
                self.asks.append(on_click)
            return super().button(text, *a, on_click=on_click, **k)

    _apply_cdc_status(state, (
        [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
        {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        [parse_dlq_log_message(line, occurred_at=T0) for line in lines],
    ))
    ui, facts = _Ui(), {}
    cm._render_cdc_dlq_panel(
        ui, state, _JobJM(MigrationJob(job_id="job-cdc")), state.cdc_status_view,
        cdc_ai_opener=lambda scope, text, _seed: facts.__setitem__(scope, text),
    )
    for ask in ui.asks:
        ask()
    return ui, facts


@pytest.mark.parametrize("mtype", [MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC])
def test_the_dlq_cards_ai_dba_is_never_told_to_reload_a_truncated_table(mtype) -> None:
    # CDC only: the banner gives the hand runbook and the record list drops the sink's
    # "Recovery: reload this table on the Full Load step" clause, while the card's own Ask AI
    # DBA sent it as an authoritative fact, right beside "Migration type: CDC only.".
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    state.migration_type = mtype
    ui, facts = _dlq_panel(state, [_TRUNCATE_LINE])
    sample = (
        "- shop.events | code=SOURCE_TRUNCATE | DLQ offset=7: Cannot apply a source TRUNCATE "
        "of table shop.events:"
    )
    assert sample in facts["dlq"] and "Validation reports them as extra)." in facts["dlq"]
    for absent in ("Recovery:", "Full Load step", "Drop & reload"):
        assert absent not in facts["dlq"], absent
    # The AI DBA's dead-letter samples tool gives the same reason as the record list.
    (row,) = ui.table_rows
    tool = json.loads(_executor(state, _validation([]))("list_cdc_dlq_samples", {}))
    (listed,) = tool["samples"]
    assert listed["message"] == row["message"]
    assert row["message"].endswith("(Validation reports them as extra).")
    # Display only: the stored record (what the download renders) keeps the whole reason.
    (stored,) = cm.cdc_dlq_records(state, cm.cdc_error_log_key(state))
    assert "Recovery: reload this table on the Full Load step" in stored.message


# A CDC-only 23505 on a table keyed by email (Property 7: the sink withholds a natural-key
# value) and one on a table keyed by an integer, in the sink's own log format.
_NATURAL_KEY_LINE = (
    "Quarantined record to DLQ (topic=dsqlcdc.shop.customers, partition=0, offset=100): "
    "sqlstate=23505 duplicate key value violates unique constraint \"customers_email_key\" "
    "| pk: email=<withheld> | op: c/r | sql: INSERT INTO \"shop\".\"customers\" (\"email\", "
    "\"name\") VALUES (?, ?) ON CONFLICT (\"email\") DO UPDATE SET \"name\" = EXCLUDED.\"name\""
)
_INTEGER_KEY_LINE = (
    "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, offset=41): "
    "sqlstate=23505 duplicate key value violates unique constraint \"orders_ref_key\" "
    "| pk: id=42 | op: u | sql: INSERT INTO \"shop\".\"orders\" (\"id\", \"ref\") VALUES (?, ?) "
    "ON CONFLICT (\"id\") DO UPDATE SET \"ref\" = EXCLUDED.\"ref\""
)
_KEY_VALUES_SHOWN = (
    "names each record's primary key, but shows a key value only when it is an integer or a "
    "UUID (any other is withheld)"
)


def test_a_cdc_only_hand_repair_says_which_key_values_the_record_list_shows() -> None:
    # The banner and the card sent the operator to the record list, which "names each record's
    # primary key", to correct the rows by hand -- and the list shows "pk: email=<withheld>",
    # so such a table's rows could not be found from it.
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    state.migration_type = MigrationType.CDC_ONLY
    ui, _facts = _dlq_panel(state, [_NATURAL_KEY_LINE, _INTEGER_KEY_LINE])
    blob = " ".join(ui.texts)
    assert "Stop CDC and repair these tables" in blob
    assert (
        f"this panel cannot. The dead-letter queue card {_KEY_VALUES_SHOWN}: for a table "
        "whose key values it withholds, or instead of the repair by hand, use Start over to "
        "migrate the table again with a Full Load."
    ) in blob
    assert (
        f"The record list below {_KEY_VALUES_SHOWN}: for a table whose key values it "
        "withholds, use Start over."
    ) in blob
    for claim in ("lists each record's primary key;", "names each record's primary key;"):
        assert claim not in blob, claim
    # The list shows what the two say: an integer key's value, never a natural key's.
    reasons = {row["table"]: row["message"] for row in ui.table_rows}
    assert "| pk: email=<withheld> |" in reasons["shop.customers"]
    assert "| pk: id=42 |" in reasons["shop.orders"]


@pytest.mark.parametrize("kind", [
    "add-column", "drop-column", "type-change", "missing-table", "unique-conflict",
    "unbindable-value", "source-truncate", "value-does-not-fit", "key-change",
    "nullability-change", "check-violation",
])
def test_no_cdc_only_runbook_promises_a_key_value_the_record_list_withholds(kind) -> None:
    # The DDL, recreate and key-change runbooks on the same CDC-only panel sent the hand repair
    # to "the dead-letter queue card lists each record's primary key" too.
    import re

    from dsql_migrator.core.cdc import SchemaDriftKind

    assert {k.value for k in SchemaDriftKind} >= {kind}
    blob, _card = _dlq_card(MigrationType.CDC_ONLY, kind)
    claims = re.findall(r"(?:lists|names) each record's primary key[^.:;)]*", blob)
    assert claims, blob
    for claim in claims:
        assert claim in (
            "lists each record's primary key, with its value when that is an integer or a UUID",
            "names each record's primary key, but shows a key value only when it is an "
            "integer or a UUID (any other is withheld",
        ), (kind, claim)


def test_the_cut_over_hand_repair_says_which_key_values_the_card_shows() -> None:
    # The Cut over step's CDC-only runbook sent the same hand repair to "the dead-letter queue
    # card lists each record's primary key and Kafka offset", which shows "email=<withheld>"
    # for a natural key: such a table's rows cannot be found from it.
    from dsql_migrator.ui.cdc_dlq_gate import way_forward_lines
    from tests.test_cdc_dlq_gate import _cdc_only_gate, _entry

    steps = way_forward_lines(
        _cdc_only_gate({"shop.customers": _entry(kinds=["unique-conflict"])}, "shop.customers")
    )
    (repair,) = [s for s in steps if s.startswith("CDC only has no Full Load step")]
    assert repair == (
        "CDC only has no Full Load step to reload from: correct these rows on Aurora DSQL by "
        "hand (the dead-letter queue card lists each record's primary key and Kafka offset, "
        "with the key's value when that is an integer or a UUID), and delete any row a lost "
        "DELETE left behind. For a table whose key values the card withholds, use Start over."
    )
