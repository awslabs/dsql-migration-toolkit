# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CDC screen's connector handling read-models.

These cover the NiceGUI-agnostic helpers that surface the CDC connector
handling behavior in the CDC screen:

- Oversized-LOB exclusion candidates + Debezium ``column.exclude.list`` value
  (H13), including PK exclusion and stable ordering.
- DLQ health classification against the circuit-breaker threshold (H6/H11).
- Connector health rows derived from managed state + lag (H1/H2/H9).
- The static CDC handling contract (H1-H13).
- ``DataMigrationState`` persistence of the opt-in LOB exclusion selection.
"""

from __future__ import annotations

from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    TableDef,
)
from dsql_migrator.ui.data_migration import (
    DataMigrationState,
    assess_dlq_health,
    cdc_activity_summary,
    cdc_handling_facts,
    connector_health_rows,
    format_column_exclude_list,
    lob_exclusion_candidates,
    scope_lob_candidates,
)
from dsql_migrator.core.msk_connect_controller import ConnectorHealth


# ---------------------------------------------------------------------------
# Oversized-LOB exclusion (H13)
# ---------------------------------------------------------------------------


def _inventory_with_lobs() -> SourceInventory:
    return SourceInventory(
        tables=[
            TableDef(
                name="customers_sample.customers",
                columns=[
                    ColumnDef(name="id", mysql_type="BIGINT"),
                    ColumnDef(name="preferences", mysql_type="LONGTEXT"),
                    ColumnDef(name="avatar", mysql_type="MEDIUMBLOB"),
                    ColumnDef(name="name", mysql_type="VARCHAR(255)"),
                ],
                primary_key=["id"],
            ),
            TableDef(
                name="customers_sample.orders",
                columns=[
                    ColumnDef(name="order_id", mysql_type="BIGINT"),
                    ColumnDef(name="total", mysql_type="DECIMAL(10,2)"),
                ],
                primary_key=["order_id"],
            ),
        ]
    )


def test_lob_exclusion_candidates_flags_only_oversized_lob_columns() -> None:
    candidates = lob_exclusion_candidates(_inventory_with_lobs())
    assert len(candidates) == 1
    assert candidates[0].table == "customers_sample.customers"
    assert candidates[0].columns == ("preferences", "avatar")


def test_lob_exclusion_candidates_flags_mysql_json() -> None:
    """G13: a MySQL JSON column can hold a document over DSQL's 1 MiB json limit, so it is
    offered for exclusion like LONGTEXT (it used to be missing from the offer, so the only
    recovery came after rows were quarantined). Plain TEXT and a bounded VARCHAR are not."""
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._models import LobExclusionCandidate

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="ecommerce.users",
                columns=[
                    ColumnDef(name="id", mysql_type="bigint"),
                    ColumnDef(name="metadata", mysql_type="json"),
                    ColumnDef(name="bio", mysql_type="text"),
                    ColumnDef(name="email", mysql_type="varchar(255)"),
                ],
                primary_key=["id"],
            )
        ]
    )
    assert lob_exclusion_candidates(inventory, source_type=SourceType.MYSQL) == [
        LobExclusionCandidate("ecommerce.users", ("metadata",))
    ]


def test_lob_exclusion_candidates_flags_pg_text_and_bytea() -> None:
    # For a PostgreSQL source, column types are PG names (text/bytea), which the MySQL
    # LOB set never matched -- so a PG source offered NO exclusions and the panel
    # falsely reported none. The PG set must flag unbounded text/bytea (not a bounded
    # varchar, not the PK).
    from dsql_migrator.core.models import SourceType

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="public.docs",
                columns=[
                    ColumnDef(name="id", mysql_type="bigint"),
                    ColumnDef(name="body", mysql_type="text"),
                    ColumnDef(name="blob", mysql_type="bytea"),
                    ColumnDef(name="title", mysql_type="character varying(200)"),
                ],
                primary_key=["id"],
            ),
        ]
    )
    # MySQL set finds nothing (none of these PG types -- text/bytea/varchar -- is in it).
    assert lob_exclusion_candidates(inventory) == []
    # PG set flags text + bytea, not the bounded varchar.
    pg = lob_exclusion_candidates(inventory, source_type=SourceType.POSTGRES)
    assert len(pg) == 1
    assert pg[0].columns == ("body", "blob")


def test_lob_exclusion_candidates_never_offers_pk_columns() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.t",
                # A LOB column that is ALSO the primary key must not be offered.
                columns=[ColumnDef(name="doc", mysql_type="LONGTEXT")],
                primary_key=["doc"],
            )
        ]
    )
    assert lob_exclusion_candidates(inventory) == []


def test_lob_exclusion_candidates_empty_for_none_or_no_lobs() -> None:
    assert lob_exclusion_candidates(None) == []
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.t",
                columns=[ColumnDef(name="id", mysql_type="INT")],
                primary_key=["id"],
            )
        ]
    )
    assert lob_exclusion_candidates(inventory) == []


def test_scope_lob_candidates_filters_to_effective_selection() -> None:
    # Two schemas each with an oversized-LOB table; the user selected only ecommerce.
    candidates = lob_exclusion_candidates(
        SourceInventory(
            tables=[
                TableDef(
                    name="ecommerce.orders",
                    columns=[ColumnDef(name="notes", mysql_type="LONGTEXT")],
                    primary_key=["id"],
                ),
                TableDef(
                    name="analytics.events",
                    columns=[ColumnDef(name="payload", mysql_type="LONGBLOB")],
                    primary_key=["id"],
                ),
            ]
        )
    )
    assert {c.table for c in candidates} == {"ecommerce.orders", "analytics.events"}
    # Effective selection = only the ecommerce table -> analytics is filtered out,
    # even though the raw stored selection is still the empty "= all" default.
    scoped = scope_lob_candidates(
        candidates, selected_tables=["ecommerce.orders"], stored_selection=[]
    )
    assert [c.table for c in scoped] == ["ecommerce.orders"]


def test_scope_lob_candidates_none_falls_back_to_stored_or_all() -> None:
    candidates = lob_exclusion_candidates(
        SourceInventory(
            tables=[
                TableDef(
                    name="ecommerce.orders",
                    columns=[ColumnDef(name="notes", mysql_type="LONGTEXT")],
                    primary_key=["id"],
                ),
                TableDef(
                    name="analytics.events",
                    columns=[ColumnDef(name="payload", mysql_type="LONGBLOB")],
                    primary_key=["id"],
                ),
            ]
        )
    )
    # No effective selection supplied + empty stored selection => all (legacy).
    assert len(scope_lob_candidates(
        candidates, selected_tables=None, stored_selection=[]
    )) == 2
    # No effective selection but a non-empty stored selection => filter to it.
    scoped = scope_lob_candidates(
        candidates, selected_tables=None, stored_selection=["analytics.events"]
    )
    assert [c.table for c in scoped] == ["analytics.events"]


def test_scope_lob_candidates_empty_effective_selection_shows_none() -> None:
    # An explicit empty EFFECTIVE selection means nothing is being migrated -> no
    # candidates (not "all"), distinct from the None fallback above.
    candidates = lob_exclusion_candidates(
        SourceInventory(
            tables=[
                TableDef(
                    name="ecommerce.orders",
                    columns=[ColumnDef(name="notes", mysql_type="LONGTEXT")],
                    primary_key=["id"],
                )
            ]
        )
    )
    assert scope_lob_candidates(
        candidates, selected_tables=[], stored_selection=[]
    ) == []


def test_lob_exclusion_candidates_sorted_by_table() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="db.zeta",
                columns=[ColumnDef(name="blob_z", mysql_type="LONGBLOB")],
            ),
            TableDef(
                name="db.alpha",
                columns=[ColumnDef(name="blob_a", mysql_type="LONGBLOB")],
            ),
        ]
    )
    tables = [c.table for c in lob_exclusion_candidates(inventory)]
    assert tables == ["db.alpha", "db.zeta"]


def test_format_column_exclude_list_qualified_and_sorted() -> None:
    value = format_column_exclude_list(
        {
            "db.t2": ["b", "a"],
            "db.t1": ["z"],
        }
    )
    # db.table.column, sorted by table then column for a stable, reviewable value.
    assert value == "db.t1.z,db.t2.a,db.t2.b"


def test_format_column_exclude_list_empty() -> None:
    assert format_column_exclude_list({}) == ""


# ---------------------------------------------------------------------------
# DLQ health / circuit-breaker (H6/H11)
# ---------------------------------------------------------------------------


def test_assess_dlq_health_none_when_no_signal() -> None:
    assert assess_dlq_health(None) is None


def test_assess_dlq_health_ok_below_half_threshold() -> None:
    health = assess_dlq_health(10, threshold=100)
    assert health is not None
    assert health.level == "ok"
    assert "10" in health.message


def test_assess_dlq_health_zero_is_ok_no_records() -> None:
    health = assess_dlq_health(0, threshold=100)
    assert health is not None
    assert health.level == "ok"
    assert "No records" in health.message


def test_assess_dlq_health_warn_over_half_threshold() -> None:
    health = assess_dlq_health(60, threshold=100)
    assert health is not None
    assert health.level == "warn"


def test_assess_dlq_health_alarm_at_threshold() -> None:
    health = assess_dlq_health(100, threshold=100)
    assert health is not None
    assert health.level == "alarm"
    assert "systematic" in health.message.lower()


def test_assess_dlq_health_clamps_negative() -> None:
    health = assess_dlq_health(-5, threshold=100)
    assert health is not None
    assert health.depth == 0
    assert health.level == "ok"


# ---------------------------------------------------------------------------
# Connector health (H1/H2/H9)
# ---------------------------------------------------------------------------


def test_connector_health_running_is_ok_with_lag_detail() -> None:
    rows = connector_health_rows({"src": "RUNNING"}, lag_seconds=1.2)
    assert len(rows) == 1
    assert rows[0].tone == "ok"
    assert "1.2s" in rows[0].detail


def test_connector_health_failed_is_bad() -> None:
    rows = connector_health_rows({"sink": "FAILED"})
    assert rows[0].tone == "bad"


def test_connector_health_non_running_is_warn() -> None:
    rows = connector_health_rows({"sink": "PROVISIONING"})
    assert rows[0].tone == "warn"


def test_connector_health_elevated_lag_nudges_to_warn() -> None:
    rows = connector_health_rows(
        {"src": "RUNNING"}, lag_seconds=45.0, lag_warn_seconds=30.0
    )
    assert rows[0].tone == "warn"
    assert "elevated" in rows[0].detail


def test_connector_health_sorted_by_name_and_empty() -> None:
    rows = connector_health_rows({"z": "RUNNING", "a": "RUNNING"})
    assert [r.name for r in rows] == ["a", "z"]
    assert connector_health_rows({}) == []


def test_connector_role_label_friendly_names() -> None:
    from dsql_migrator.ui.data_migration import connector_role_label

    assert connector_role_label("mysql-dsql-cdc-spike-debezium-source").startswith("Source")
    assert connector_role_label("mysql-dsql-cdc-spike-dsql-sink-v6").startswith("Sink")
    # An unrecognized name falls back to the raw id (never hidden).
    assert connector_role_label("mystery-connector") == "mystery-connector"


def test_connector_health_rows_carry_friendly_label() -> None:
    rows = connector_health_rows({"mysql-dsql-cdc-spike-debezium-source": "RUNNING"})
    assert rows[0].label.startswith("Source")
    assert rows[0].name == "mysql-dsql-cdc-spike-debezium-source"  # raw id preserved


def test_connector_health_ordered_source_before_sink() -> None:
    # Real connector names: data-flow order (Source first) regardless of name sort.
    rows = connector_health_rows(
        {
            "mysql-dsql-cdc-spike-dsql-sink-v6": "RUNNING",
            "mysql-dsql-cdc-spike-debezium-source": "RUNNING",
        }
    )
    assert [r.label.split(" ")[0] for r in rows] == ["Source", "Sink"]


# ---------------------------------------------------------------------------
# CDC handling contract (H1-H13)
# ---------------------------------------------------------------------------


def test_cdc_handling_facts_cover_handled_and_caveats() -> None:
    facts = cdc_handling_facts()
    handled = [f for f in facts if f.handled]
    caveats = [f for f in facts if not f.handled]
    # Both the guarantees (no duplicates, inserts/updates/deletes, …) and the
    # caveats (DDL not replicated, large-value exclusion, DLQ accumulation) must be
    # present. Titles are customer-facing (no internal jargon / spike H-codes).
    assert handled and caveats
    blob = " ".join((f.title + " " + f.detail) for f in facts).lower()
    # The guarantee that retries don't duplicate, and the must-know limits.
    assert "duplicate" in blob
    assert "ddl" in blob  # the critical "DDL not replicated" caveat
    assert "dead-letter" in blob or "dlq" in blob
    # The internal spike hypothesis codes are retained on the model for
    # traceability but must NOT leak into the user-facing title/detail text.
    assert all(f.evidence for f in facts)
    assert "h4" not in blob and "h13" not in blob and "§4" not in blob


def test_cdc_handling_ddl_fact_does_not_route_ddl_through_schema_conversion() -> None:
    # G11: Schema Conversion's Apply is blocked while CDC streams (and Replace drops the
    # table), and some source DDL dead-letters nothing -- the panel must say so.
    (fact,) = [f for f in cdc_handling_facts() if "DDL" in f.title]
    detail = fact.detail.lower()
    assert "through schema conversion" not in detail
    assert "fix target schema" in detail
    assert "blocked while cdc streams" in detail
    assert "checksum" in detail


def test_cdc_handling_ddl_fact_names_the_recreate_for_a_type_or_key_change() -> None:
    # G11 review: "otherwise an ALTER on Aurora DSQL" sent a user with a type or key change
    # looking for an ALTER that does not exist (no ALTER COLUMN ... TYPE, no primary-key
    # change). The panel must give the same split as the manual's 4.2 drift table: DROP NOT
    # NULL for a relaxed NULL rule, a recreate with CDC stopped for a type or key change,
    # and the CDC-only path (no reload exists there).
    (fact,) = [f for f in cdc_handling_facts() if "DDL" in f.title]
    detail = fact.detail.lower()
    assert "drop not null" in detail
    assert "recreate" in detail and "cdc stopped" in detail
    assert "replace" in detail and "drop & reload" in detail
    assert "cdc only" in detail and "start over" in detail
    assert "otherwise an alter" not in detail


def test_cdc_handling_replicate_fact_discloses_truncate() -> None:
    (fact,) = [
        f
        for f in cdc_handling_facts()
        if f.title.startswith("Inserts, updates, and deletes")
    ]
    assert "truncate" in fact.detail.lower()


# ---------------------------------------------------------------------------
# DataMigrationState: opt-in LOB exclusion persistence (H13)
# ---------------------------------------------------------------------------


def test_state_lob_exclusion_toggle_on_and_off() -> None:
    state = DataMigrationState()
    assert state.lob_exclusions() == {}

    state.set_lob_exclusion("db.t", "doc", True)
    state.set_lob_exclusion("db.t", "blob", True)
    assert state.lob_exclusions() == {"db.t": {"doc", "blob"}}

    # Unticking the last column drops the table key entirely (clean empty state).
    state.set_lob_exclusion("db.t", "doc", False)
    state.set_lob_exclusion("db.t", "blob", False)
    assert state.lob_exclusions() == {}


def test_state_lob_exclusion_returns_a_copy() -> None:
    state = DataMigrationState()
    state.set_lob_exclusion("db.t", "doc", True)
    snapshot = state.lob_exclusions()
    snapshot["db.t"].add("mutated")
    # Mutating the returned copy must not affect the stored selection.
    assert state.lob_exclusions() == {"db.t": {"doc"}}


# ---------------------------------------------------------------------------
# cdc_activity_summary — the "no changes flowing" (cutover) signal
# ---------------------------------------------------------------------------


def test_activity_idle_when_both_rates_zero() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=0.0),
        "sink": ConnectorHealth(send_rate=0.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is True
    assert summary.source_poll_rate == 0.0
    assert summary.sink_send_rate == 0.0


def test_activity_not_idle_when_streaming() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=5.0),
        "sink": ConnectorHealth(send_rate=4.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is False


def test_activity_idle_none_when_poll_rate_unknown() -> None:
    # Source poll rate unknown -> cannot assert idle (must be None, never True).
    health = {
        "src": ConnectorHealth(poll_rate=None),
        "sink": ConnectorHealth(send_rate=0.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.idle is None


def test_activity_idle_none_when_send_rate_unknown() -> None:
    health = {
        "src": ConnectorHealth(poll_rate=0.0),
        "sink": ConnectorHealth(send_rate=None),
    }
    assert cdc_activity_summary(health).idle is None


def test_activity_empty_health_is_unknown() -> None:
    summary = cdc_activity_summary({})
    assert summary.idle is None
    assert summary.source_poll_rate is None
    assert summary.sink_send_rate is None


def test_activity_takes_max_rate_across_connectors() -> None:
    # Worst-case "still flowing": the highest known rate wins.
    health = {
        "a": ConnectorHealth(poll_rate=0.0, send_rate=0.0),
        "b": ConnectorHealth(poll_rate=2.0, send_rate=1.0),
    }
    summary = cdc_activity_summary(health)
    assert summary.source_poll_rate == 2.0
    assert summary.sink_send_rate == 1.0
    assert summary.idle is False


def test_activity_idle_absorbs_source_heartbeat_floor() -> None:
    # The source (Debezium) connector never fully goes silent: heartbeats
    # (heartbeat.interval.ms=300000) keep SourceRecordPollRate at a small floor
    # (~0.03/s on the CloudWatch average) even when the captured tables are idle.
    # The threshold sits above that floor, so a drained pipeline (sink send 0) still
    # reads as idle instead of lingering as "streaming".
    health = {
        "src": ConnectorHealth(poll_rate=0.03),  # heartbeat residual, no real changes
        "sink": ConnectorHealth(send_rate=0.0),  # nothing being applied
    }
    assert cdc_activity_summary(health).idle is True


def test_activity_not_idle_when_sink_stalled() -> None:
    # Safety property of the AND-both-rates rule: if the SOURCE is still producing
    # real changes (poll rate well above the heartbeat floor) but the SINK is not
    # sending (stalled / lagging), the pipeline is NOT drained -- it must read as
    # "streaming", never idle, so cut-over is not wrongly signalled as safe.
    health = {
        "src": ConnectorHealth(poll_rate=5.0),  # real change traffic
        "sink": ConnectorHealth(send_rate=0.0),  # sink not keeping up
    }
    assert cdc_activity_summary(health).idle is False


def _diverged(previous=None):
    """One poll of the stall signature: source producing, sink applying nothing."""
    return cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=5.0),
            "sink": ConnectorHealth(send_rate=0.0),
        },
        previous=previous,
    )


def test_sink_stall_is_reported_separately_from_idle() -> None:
    # The reported failure: source producing, sink applying nothing. `idle` must stay
    # False (a stalled pipeline is NOT drained -- that guards cut-over), which is
    # exactly why the stall was invisible: not-idle rendered as "Streaming — changes
    # are flowing". `sink_stalled` is the signal that names the divergence.
    summary = _diverged()
    assert summary.idle is False
    assert summary.sink_stalled is True


def test_a_single_poll_divergence_is_not_reported_as_a_stall() -> None:
    # Regression (hit live on a healthy pipeline): both rates are CloudWatch averages
    # over a trailing window, so right after a burst of writes ends the source still
    # shows its residual while the sink has legitimately gone quiet. That one-poll
    # divergence is indistinguishable from a real stall, and it raised a red "Sink
    # stalled" alarm on a pipeline that was replicating fine. Only a SUSTAINED
    # divergence may be reported.
    first = _diverged()
    assert first.sink_stalled is True          # the observation stands
    assert first.sink_stall_confirmed is False  # ...but it is not yet a verdict
    second = _diverged(previous=first)
    assert second.sink_stall_confirmed is False


def test_a_sustained_divergence_is_confirmed_as_a_stall() -> None:
    # A real stall is permanent (an ejected consumer never rejoins by itself), so
    # persistence costs nothing in detection.
    s = None
    for _ in range(3):
        s = _diverged(previous=s)
    assert s.sink_stall_polls == 3
    assert s.sink_stall_confirmed is True


def test_the_stall_streak_resets_as_soon_as_the_sink_sends() -> None:
    s = _diverged(previous=_diverged())          # 2 polls of divergence
    assert s.sink_stall_polls == 2
    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=4.8)},
        previous=s,
    )
    assert healthy.sink_stall_polls == 0
    assert healthy.sink_stall_confirmed is False


def test_an_unknown_rate_clears_the_stall_streak() -> None:
    # An unreadable CloudWatch metric is a monitoring failure, not a data one -- it
    # must never accumulate toward a stall verdict.
    s = _diverged(previous=_diverged())
    unknown = cdc_activity_summary({"src": ConnectorHealth(poll_rate=5.0)}, previous=s)
    assert unknown.sink_stalled is None
    assert unknown.sink_stall_polls == 0
    assert unknown.sink_stall_confirmed is False


def test_drained_pipeline_is_idle_but_not_stalled() -> None:
    # Both rates at ~0 = drained/caught up. That must NOT read as a stall, or every
    # quiet pipeline would raise a false alarm (the source's 5-min heartbeat leaves an
    # irreducible ~0.03/s floor, so "source quiet" is a small non-zero rate).
    summary = cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=0.03),
            "sink": ConnectorHealth(send_rate=0.0),
        }
    )
    assert summary.idle is True
    assert summary.sink_stalled is False


def test_healthy_streaming_is_neither_idle_nor_stalled() -> None:
    summary = cdc_activity_summary(
        {
            "src": ConnectorHealth(poll_rate=5.0),
            "sink": ConnectorHealth(send_rate=4.8),
        }
    )
    assert summary.idle is False
    assert summary.sink_stalled is False


def test_sink_stall_is_never_asserted_on_an_unknown_rate() -> None:
    # Same honesty rule as `idle`: an unreadable CloudWatch metric must not be
    # reported as a stall (that would alarm on a monitoring failure, not a data one).
    only_source = cdc_activity_summary({"src": ConnectorHealth(poll_rate=5.0)})
    assert only_source.sink_stalled is None
    only_sink = cdc_activity_summary({"sink": ConnectorHealth(send_rate=0.0)})
    assert only_sink.sink_stalled is None


def test_dlq_zero_depth_is_not_painted_green_while_the_sink_is_stalled() -> None:
    # A stalled sink never reaches a record to quarantine, so depth is 0 -- which the
    # panel used to paint as a green "success" all-clear during total data loss.
    from dsql_migrator.ui.data_migration._cdc_ui import _dlq_panel_tone
    from dsql_migrator.ui.data_migration._models import assess_dlq_health

    clean = assess_dlq_health(0)
    assert _dlq_panel_tone(clean) == "success"  # genuinely clean stream: unchanged
    assert _dlq_panel_tone(clean, sink_stalled=True) == "info"  # proves nothing now


def test_sink_stall_and_recovery_each_log_once_on_transition() -> None:
    # The CDC poll runs every few seconds, so the event must fire on the STATE CHANGE
    # only -- otherwise the durable activity log fills with identical lines.
    from dsql_migrator.ui.data_migration import _cdc_status as status_mod

    logged: list[tuple] = []

    class _FakeLog:
        @staticmethod
        def log_activity(category, action, **kwargs):
            logged.append((action, kwargs.get("status"), kwargs.get("detail") or ""))

    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=5.0)}
    )
    # A CONFIRMED stall: the log event keys off sink_stall_confirmed, so a one-poll
    # blip must not write a FAILURE line (see the single-poll test above).
    stalled = None
    for _ in range(3):
        stalled = _diverged(previous=stalled)

    import dsql_migrator.core.activity_log as real_log

    original = real_log.log_activity
    real_log.log_activity = _FakeLog.log_activity  # type: ignore[assignment]
    try:
        # healthy -> stalled: one FAILURE event.
        status_mod._log_sink_stall_transition(healthy, stalled)
        # stalled -> stalled (repeated polls): silent.
        status_mod._log_sink_stall_transition(stalled, stalled)
        status_mod._log_sink_stall_transition(stalled, stalled)
        # stalled -> healthy: one recovery event.
        status_mod._log_sink_stall_transition(stalled, healthy)
        status_mod._log_sink_stall_transition(healthy, healthy)
    finally:
        real_log.log_activity = original  # type: ignore[assignment]

    assert [action for action, _, _ in logged] == ["sink stalled", "sink recovered"]
    stall_detail = logged[0][2]
    # Says what happened, with the rates, and what NOT to do next.
    assert "5.00 rec/s" in stall_detail and "0.00 rec/s" in stall_detail
    assert "cut over" in stall_detail.lower()


def test_an_unconfirmed_divergence_writes_no_activity_log_event() -> None:
    # The regression: a post-burst one-poll divergence put a FAILURE line in the
    # durable log for a healthy pipeline. Only a confirmed stall may be recorded.
    from dsql_migrator.ui.data_migration import _cdc_status as status_mod
    import dsql_migrator.core.activity_log as real_log

    logged: list = []
    healthy = cdc_activity_summary(
        {"src": ConnectorHealth(poll_rate=5.0), "sink": ConnectorHealth(send_rate=5.0)}
    )
    blip = _diverged(previous=healthy)  # 1 poll only -> not confirmed
    assert blip.sink_stalled is True and blip.sink_stall_confirmed is False

    original = real_log.log_activity
    real_log.log_activity = lambda *a, **k: logged.append(a)  # type: ignore[assignment]
    try:
        status_mod._log_sink_stall_transition(healthy, blip)
    finally:
        real_log.log_activity = original  # type: ignore[assignment]
    assert logged == []


def test_change_flow_renders_the_stall_instead_of_streaming_all_clear() -> None:
    # The regression this closes: with source 5 rec/s and sink 0, `idle is False` fell
    # through to "Streaming — changes are flowing", asserting health off the SOURCE rate
    # while nothing reached DSQL. The stall branch must win, and must say what to do.
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    # A CONFIRMED stall (the divergence held for the required consecutive polls) --
    # a single-poll blip must not render this, which the test above covers.
    stalled = None
    for _ in range(3):
        stalled = _diverged(previous=stalled)
    assert stalled.sink_stall_confirmed is True
    ui = _RecordingUi()
    _render_change_flow_status(ui, stalled)
    text = " ".join(ui.texts)
    assert "changes are flowing" not in text  # the misleading all-clear is gone
    assert "Sink stalled" in text
    assert "NOT reaching DSQL" in text
    # Actionable, per the project's "what happened, what to do next" rule.
    assert "RUNNING is not evidence" in text
    assert "Commit of offsets timed out" in text
    assert "do NOT cut over" in text or "not cut over" in text.lower()
    # Both rate bars still render, so the operator sees the numbers behind the verdict.
    assert "5.00 rec/s" in text and "0.00 rec/s" in text


def test_change_flow_still_shows_streaming_when_the_sink_keeps_up() -> None:
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    ui = _RecordingUi()
    _render_change_flow_status(
        ui,
        cdc_activity_summary(
            {
                "src": ConnectorHealth(poll_rate=5.0),
                "sink": ConnectorHealth(send_rate=4.8),
            }
        ),
    )
    text = " ".join(ui.texts)
    assert "Streaming — changes are flowing" in text
    assert "Sink stalled" not in text


def test_change_flow_shows_idle_for_a_drained_pipeline() -> None:
    from tests.test_ui_data_migration import _RecordingUi
    from dsql_migrator.ui.data_migration._cdc_ui import _render_change_flow_status

    ui = _RecordingUi()
    _render_change_flow_status(
        ui,
        cdc_activity_summary(
            {
                "src": ConnectorHealth(poll_rate=0.03),
                "sink": ConnectorHealth(send_rate=0.0),
            }
        ),
    )
    text = " ".join(ui.texts)
    # The rate that reaches this branch is 0.03 rec/s -- which is a REAL change stream, not
    # a drained one: measured live, 6 change events in 72s produced source 0.06 rec/s and
    # landed here. So the line must NOT assert absence. It states the measured rates and the
    # limit of what they can rule out; the words that used to be here ("No changes flowing",
    # "pipeline idle") were the all-clear an operator reads when timing cut-over.
    assert "At or below the idle floor" in text
    assert "0.03 rec/s in" in text
    assert "indistinguishable from drained" in text
    assert "No changes flowing" not in text
    assert "Sink stalled" not in text

    # THE DANGEROUS CASE, and the reason the copy fix exists: a source still producing while
    # the sink applies NOTHING -- a total replication outage -- falls in the same band, so
    # `sink_stalled` is False and this identical line renders. It must not read as drained.
    ui2 = _RecordingUi()
    _render_change_flow_status(
        ui2,
        cdc_activity_summary(
            {
                "src": ConnectorHealth(poll_rate=0.0833),  # ~5 changes/min
                "sink": ConnectorHealth(send_rate=0.0),  # nothing applied at all
            }
        ),
    )
    stalled_band = " ".join(ui2.texts)
    assert "At or below the idle floor" in stalled_band
    assert "indistinguishable from drained" in stalled_band
    assert "No changes flowing" not in stalled_band


def test_the_idle_line_and_the_empty_lag_line_are_not_green_all_clears() -> None:
    """Tone is severity, not decoration: neither line may read as the strongest all-clear.

    Both used to: the idle icon was ``color="positive"`` and the empty-lag row was a green
    ``check_circle`` labelled "Caught up — no replication lag". Each is rendered from the
    ABSENCE of a datapoint, and the same absence is produced by a sink that has stopped
    applying entirely -- so a total replication outage was shown in green.
    """
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_monitoring

    # Both lines live inside an element tree the CDC doubles do not reproduce, so they are
    # pinned at the source -- the same way this repo pins other render-site properties.
    src = inspect.getsource(_cdc_monitoring)
    # The idle icon must not carry the Quasar `positive` tone.
    assert 'ui.icon("pause_circle", color="positive")' not in src
    assert 'ui.icon("pause_circle").classes("text-base text-sky-600")' in src
    # "no datapoint" must not be rendered as "no lag", and not in green.
    assert "No replication-lag datapoint in the recent window " in src
    assert "Caught up — no replication lag" not in src
    assert 'ui.icon("check_circle").classes("text-green-600 text-base")' not in src


# ---------------------------------------------------------------------------
# PostgreSQL CDC replication-slot WAL-health monitor (Phase C6)
# ---------------------------------------------------------------------------


class _StubSlotState:
    """Minimal migration-state stub for the slot-health fetch/render."""

    def __init__(self, stack_name="mysql-dsql-cdc-stack"):
        self.cdc_stack_name = stack_name
        self.cdc_slot_health = "unset"

    def set_cdc_slot_health(self, health):
        self.cdc_slot_health = health


def test_render_cdc_slot_health_panel() -> None:
    from types import SimpleNamespace

    from dsql_migrator.core.cdc_postgres import SlotHealth
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_slot_health
    from tests.test_ui_data_migration import _RecordingUi

    # None / no slot -> nothing rendered (inherently MySQL-safe: MySQL never populates it).
    ui = _RecordingUi()
    _render_cdc_slot_health(ui, SimpleNamespace(cdc_slot_health=None))
    assert ui.texts == []
    ui2 = _RecordingUi()
    _render_cdc_slot_health(ui2, SimpleNamespace(cdc_slot_health=SlotHealth("s", exists=False)))
    assert ui2.texts == []

    # Invalidated slot -> an error notice naming the problem.
    ui3 = _RecordingUi()
    _render_cdc_slot_health(
        ui3,
        SimpleNamespace(
            cdc_slot_health=SlotHealth("s", exists=True, active=True, wal_status="lost")
        ),
    )
    assert any("invalidated" in t.lower() for t in ui3.texts), ui3.texts

    # Healthy slot -> a success notice.
    ui4 = _RecordingUi()
    _render_cdc_slot_health(
        ui4,
        SimpleNamespace(
            cdc_slot_health=SlotHealth(
                "s", exists=True, active=True, wal_status="reserved", safe_wal_size=1000
            )
        ),
    )
    assert any("healthy" in t.lower() for t in ui4.texts), ui4.texts


def test_slot_snapshot_is_invalidated_when_the_pipeline_starts_or_stops() -> None:
    """A slot read taken in the OTHER pipeline state must never be shown.

    The slot is read only on the source/target counts refresh, but the panel renders every
    5 s tick. Live-observed: counts refreshed while CDC was stopped captured
    ``active=False``; after Start CDC the panel kept saying "Replication slot has no
    consumer" -- advising deletion of the CDC infrastructure -- while the real slot was
    ``active=True`` and draining its backlog.
    """
    from dsql_migrator.core.cdc_postgres import SlotHealth
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    stopped_read = SlotHealth("s", exists=True, active=False, wal_status="reserved")

    # Read while stopped (nothing running), then CDC starts -> the snapshot is dropped.
    state.set_cdc_connector_running_names([])
    state.set_cdc_slot_health(stopped_read)
    assert state.cdc_slot_health is stopped_read and state.cdc_slot_health_at is not None
    state.set_cdc_connector_running_names(["x-debezium-source", "x-dsql-sink"])
    assert state.cdc_slot_health is None
    assert state.cdc_slot_health_at is None

    # A read while running SURVIVES further polls in the same state -- the invalidation is
    # on the transition, not on every tick (else the panel could never show anything).
    live = SlotHealth("s", exists=True, active=True, wal_status="reserved")
    state.set_cdc_slot_health(live)
    state.set_cdc_connector_running_names(["x-debezium-source", "x-dsql-sink"])
    assert state.cdc_slot_health is live

    # ...and is dropped again when the pipeline stops.
    state.set_cdc_connector_running_names([])
    assert state.cdc_slot_health is None


def test_slot_health_panel_states_when_the_slot_was_read() -> None:
    # The snapshot must not read as live state: the panel names the read time and how to
    # re-read it.
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from dsql_migrator.core.cdc_postgres import SlotHealth
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_slot_health
    from tests.test_ui_data_migration import _RecordingUi

    ui = _RecordingUi()
    _render_cdc_slot_health(
        ui,
        SimpleNamespace(
            cdc_slot_health=SlotHealth(
                "s", exists=True, active=True, wal_status="reserved", safe_wal_size=1000
            ),
            cdc_slot_health_at=datetime(2026, 9, 28, 7, 5, 9, tzinfo=timezone.utc),
        ),
    )
    blob = " ".join(ui.texts)
    assert "Slot read at 07:05:09 UTC" in blob
    assert "refresh the source/target counts" in blob


def test_refresh_pg_slot_health_reads_for_pg_and_noops_for_mysql() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.data_migration._cdc_status import _refresh_pg_slot_health

    class _R:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    class _Conn:
        def __init__(self, row):
            self._row = row

        def execute(self, statement, params=None):
            return _R(self._row)

    # MySQL source -> the dialect has no slot, so health is set to None (never crashes).
    mysql_state = _StubSlotState()
    _refresh_pg_slot_health(
        mysql_state,
        SourceConnectionConfig(source_type=SourceType.MYSQL, host="db", database="app"),
        _Conn(None),
    )
    assert mysql_state.cdc_slot_health is None

    # PostgreSQL source -> reads pg_replication_slots for the stack's deterministic slot.
    pg_state = _StubSlotState(stack_name="mysql-dsql-cdc-stack")
    _refresh_pg_slot_health(
        pg_state,
        SourceConnectionConfig(source_type=SourceType.POSTGRES, host="pg", database="app"),
        _Conn((True, "reserved", 999, "0/16B3748", "0/16B3800", 184_320)),
    )
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name

    assert pg_state.cdc_slot_health is not None
    assert pg_state.cdc_slot_health.slot_name == pg_slot_name("mysql-dsql-cdc-stack")
    assert pg_state.cdc_slot_health.wal_status == "reserved"


def test_cdc_resume_signal_is_engine_aware() -> None:
    # The shared CDC start signal differs by engine: MySQL Manual seeds a binlog
    # resume_override; PostgreSQL Manual has no coordinate and instead re-snapshots
    # (force_initial_snapshot), because Debezium PG resumes only from the slot.
    from types import SimpleNamespace
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_resume_signal

    def _session(source_type):
        return SimpleNamespace(
            source_config=SourceConnectionConfig(
                source_type=source_type, host="h", database="app"
            )
        )

    # PostgreSQL: no override value; Manual -> force initial (re-snapshot), Auto -> gapless.
    pg = _session(SourceType.POSTGRES)
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    assert _cdc_resume_signal(state, pg) == (None, True)
    state.set_cdc_start_mode("auto")
    assert _cdc_resume_signal(state, pg) == (None, False)

    # MySQL: force_initial_snapshot is never set. A Manual binlog file:position becomes the
    # resume_override (it is what Start seeds); a GTID set alone seeds nothing, so it is not.
    my = _session(SourceType.MYSQL)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100")
    assert _cdc_resume_signal(state, my) == (None, False)
    state.set_cdc_start_position(binlog_file="mysql-bin.000042", binlog_pos=1234)
    override, force = _cdc_resume_signal(state, my)
    assert force is False
    assert override is not None and override.can_seed_offset()
    # MySQL Manual with nothing entered -> no override (nothing to seed).
    state.set_cdc_start_position(gtid=None, binlog_file=None, binlog_pos=None)
    assert _cdc_resume_signal(state, my) == (None, False)
    # MySQL Automatic -> no override.
    state.set_cdc_start_mode("auto")
    assert _cdc_resume_signal(state, my) == (None, False)


class _FakeEl:
    def __init__(self, sink):
        self._sink = sink
        # Recorded, not discarded: whether a control is DISABLED and what its tooltip says is
        # the difference between "shown with the reason" and "silently dead" -- a distinction
        # several of this repo's UI rules turn on.
        self.props_seen: list[str] = []
        self.on_click = None
        self.text = ""

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        if _a:
            self.props_seen.append(str(_a[0]))
        return self

    def tooltip(self, text="", *_a, **_k):
        if text and self._sink is not None:
            self._sink.tooltips.append(str(text))
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


class _FakeUi:
    """Minimal NiceGUI double for rendering the CDC start-point card (collects text)."""

    def __init__(self):
        self.texts: list[str] = []
        self.radios: list[dict] = []
        # Buttons and hover-only text, kept apart from `texts`: a test that drives a click has
        # to identify WHICH action, and guidance must not live only in a tooltip.
        self.buttons: list = []
        self.tooltips: list[str] = []
        # Material icon names, so a test can pin a notice's TONE: render_notice picks the
        # leading glyph from design.NOTICE_STYLE[tone], and an unrecorded icon made the
        # tone -- the whole severity calibration -- invisible to every assertion.
        self.icons: list[str] = []

    def _el(self):
        return _FakeEl(self)

    def card(self, *_a, **_k):
        return self._el()

    def row(self, *_a, **_k):
        return self._el()

    def column(self, *_a, **_k):
        return self._el()

    def icon(self, name="", *_a, **_k):
        self.icons.append(str(name))
        return self._el()

    def space(self, *_a, **_k):
        return self._el()

    def spinner(self, *_a, **_k):
        return self._el()

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el()

    def badge(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._el()

    def radio(self, options=None, *_a, **_k):
        if isinstance(options, dict):
            self.radios.append(options)
        return self._el()

    def input(self, label="", *_a, **_k):
        self.texts.append(str(label))
        return self._el()

    def button(self, text="", *_a, on_click=None, **_k):
        self.texts.append(str(text))
        el = self._el()
        el.on_click = on_click
        el.text = str(text)
        self.buttons.append(el)
        return el

    def notify(self, *_a, **_k):
        return None


def test_estimate_cdc_table_rows_threads_source_dialect(monkeypatch) -> None:
    # Regression: the CDC topic-sizing estimate must run with the SOURCE-engine dialect, so
    # a PostgreSQL source uses pg_class.reltuples -- not the MySQL information_schema default,
    # which raises on PG and (under the broad except) silently returns None -> uniform
    # partitions. Assert the dialect actually threaded through matches the source engine.
    from types import SimpleNamespace
    import dsql_migrator.core.watermark as wm
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.core.source_dialect import MySQLSourceDialect, PostgresSourceDialect
    from dsql_migrator.ui.data_migration._cdc_ui import _estimate_cdc_table_rows

    class _RoConn:
        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

    class _RoEngine:
        def connect(self):
            return _RoConn()

        def dispose(self):
            pass

    seen = {}

    def _fake_estimate(conn, tables, dialect):
        seen["dialect"] = dialect
        return {t: 42 for t in tables}

    monkeypatch.setattr(connect, "make_source_engine_factory", lambda pw, **k: (lambda src: _RoEngine()))
    monkeypatch.setattr(wm, "estimate_source_rows", _fake_estimate)

    def _session(source_type):
        return SimpleNamespace(
            source_config=SourceConnectionConfig(
                source_type=source_type, host="h", database="app", username="u"
            ),
            source_password=SecretValue("pw"),
            has_source=lambda: True,
        )

    pg = _estimate_cdc_table_rows(_session(SourceType.POSTGRES), ["public.orders"])
    assert pg == {"public.orders": 42}
    assert isinstance(seen["dialect"], PostgresSourceDialect)  # NOT the MySQL default

    my = _estimate_cdc_table_rows(_session(SourceType.MYSQL), ["orders"])
    assert my == {"orders": 42}
    assert isinstance(seen["dialect"], MySQLSourceDialect)


def test_the_postgres_card_offers_no_start_point_radio_at_all() -> None:
    """PostgreSQL has no start point to CHOOSE, so it must not present one.

    "Manual — re-snapshot from scratch (initial)" was a trap, not a choice: executed over
    the whole state space it changes the outcome in exactly one state family. With no
    watermark, and with a Full-load-only watermark, BOTH modes yield snapshot.mode=initial;
    a Full-load-+-CDC job that is not DONE has its slot suppressed, so both yield initial
    there too. Only a DONE load that recorded a slot differs -- and there Manual discards
    the gapless resume to re-copy every row through the STREAMING pipeline (~19-26K rows/s
    against the loader's ~103K) while being unable to remove rows deleted since the load.

    MySQL keeps its radio: there Manual supplies a REAL coordinate (a GTID or binlog
    position) that `_cdc_resume_signal` consumes as `resume_override`.
    """
    from dsql_migrator.core.cdc import CdcResumePoint
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    def card(source_type, **kw):
        state = DataMigrationState()
        state.set_cdc_start_mode(kw.get("mode", "auto"))
        fake = _FakeUi()
        _render_cdc_start_point_card(
            fake, state, lambda: None,
            wm_resume=kw.get("wm_resume"), wm_usable=kw.get("wm_usable", False),
            effective_resume=kw.get("effective_resume"), mode=kw.get("mode", "auto"),
            locked=False, session=None, source_type=source_type,
            resnapshot_reason=kw.get("resnapshot_reason"),
        )
        return fake

    pg = card(SourceType.POSTGRES, resnapshot_reason="no_slot")
    assert pg.radios == [], "PostgreSQL must render no start-point radio"
    # The disclosure carries the whole message instead, and no MySQL coordinate wording
    # leaks into the PG card.
    joined = " ".join(pg.texts)
    assert "read from the source a second time" in joined
    assert "GTID set" not in pg.texts

    # MySQL still gets its two options.
    my = card(SourceType.MYSQL)
    assert any(
        "Manual — enter a binlog file:position" in v  # a GTID alone is not a start point
        for r in my.radios
        for v in r.values()
    ), my.radios


def test_the_postgres_card_confirms_gapless_only_when_the_start_really_is() -> None:
    """The confirm line is keyed on the effective mode, not on a radio value.

    Two traps here, both execution-confirmed. (1) Keying it on `mode == "manual"` printed
    the GAPLESS line for a slot-bearing watermark that an escape button had switched to a
    re-snapshot -- a state session restore reloads. (2) Re-deriving the predicate from
    `wm_resume` inverts it: that is a `CdcResumePoint`, whose fields are binlog_file /
    binlog_position / gtid_executed / server_uuid / wal_lsn and NOT `slot_name`, and
    `pg_snapshot_mode` decides `never` via `bool(getattr(watermark, "slot_name", None))` --
    so a CdcResumePoint always looks like a re-snapshot and the gapless happy path would be
    mislabelled, pushing the operator toward a re-load they do not need.
    """
    from dsql_migrator.core.cdc import CdcResumePoint
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    def confirm(*, reason, mode="auto"):
        wm = CdcResumePoint(wal_lsn="3/AF012B8")
        state = DataMigrationState()
        state.set_cdc_start_mode(mode)
        fake = _FakeUi()
        _render_cdc_start_point_card(
            fake, state, lambda: None,
            wm_resume=wm, wm_usable=reason is None, effective_resume=wm, mode=mode,
            locked=False, session=None, source_type=SourceType.POSTGRES,
            resnapshot_reason=reason,
        )
        return " ".join(fake.texts)

    # Gapless: the WAL LSN is confirmed and the re-snapshot line must NOT appear.
    gapless = confirm(reason=None)
    assert "gapless from the replication slot" in gapless
    assert "3/AF012B8" in gapless
    assert "re-snapshot every table" not in gapless

    # An escape button recorded "manual" against a slot-bearing watermark: the card must
    # report the re-snapshot, which is what the connector is actually configured for.
    switched = confirm(reason="manual", mode="manual")
    assert "re-snapshot every table (snapshot.mode=initial)" in switched
    assert "gapless from the replication slot" not in switched

    # THE case that proves the confirm line is not keyed on the radio value: mode is the
    # "auto" default (there is no PG radio to change) and the start still re-snapshots.
    default_mode = confirm(reason="no_slot", mode="auto")
    assert "re-snapshot every table (snapshot.mode=initial)" in default_mode
    assert "gapless from the replication slot" not in default_mode


def test_pg_resnapshot_predicate_and_cause_agree_with_the_connector_config() -> None:
    """The disclosure predicate must be the SAME decision the connector config makes.

    Pinned AGAINST ``pg_snapshot_mode`` rather than against a hand-written table, so the
    two cannot drift -- the whole reason this is one function. Separately, the CAUSE has to
    be read off the RAW job: ``_cdc_watermark`` suppresses ``slot_name`` for an unfinished
    load, so a stripped watermark cannot tell "never provisioned" from "not honoured yet",
    and those two have OPPOSITE remedies (re-run the whole load vs. retry a few tables).
    """
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from dsql_migrator.core.cdc_postgres import pg_snapshot_mode
    from dsql_migrator.core.models import SourceType, Watermark
    from dsql_migrator.ui.data_migration._cdc_ui import (
        _cdc_watermark,
        pg_resnapshot_reason,
        pg_start_resnapshots,
    )

    def wm(slot=None):
        return Watermark(
            wal_lsn="0/16B3748", slot_name=slot, publication_name="p",
            snapshot_timestamp=datetime(2026, 9, 26, tzinfo=timezone.utc),
        )

    jobs = {
        "no_slot": SimpleNamespace(watermark=wm(), status="DONE"),
        None: SimpleNamespace(watermark=wm("dbz_slot"), status="DONE"),
        "unfinished_load": SimpleNamespace(watermark=wm("dbz_slot"), status="FAILED"),
        "no_watermark": None,
    }
    for expected_auto_reason, job in jobs.items():
        for mode in ("auto", "manual"):
            effective = _cdc_watermark(job)
            got = pg_start_resnapshots(
                effective, source_type=SourceType.POSTGRES, start_mode=mode
            )
            # THE anti-drift assertion: identical to what the config will emit.
            assert got is (
                pg_snapshot_mode(
                    effective, force_initial_snapshot=(mode == "manual")
                )
                == "initial"
            ), (expected_auto_reason, mode)
            reason = pg_resnapshot_reason(
                job, source_type=SourceType.POSTGRES, start_mode=mode
            )
            # A re-snapshot always has a nameable cause, and a gapless start never does.
            assert (reason is not None) is got, (expected_auto_reason, mode, reason)
        assert (
            pg_resnapshot_reason(
                job, source_type=SourceType.POSTGRES, start_mode="auto"
            )
            == expected_auto_reason
        )
    # Choosing Manual over an AVAILABLE slot is its own cause -- not "no slot was recorded".
    assert (
        pg_resnapshot_reason(
            jobs[None], source_type=SourceType.POSTGRES, start_mode="manual"
        )
        == "manual"
    )
    # MySQL never re-snapshots on this axis, whatever its watermark says.
    assert (
        pg_start_resnapshots(None, source_type=SourceType.MYSQL, start_mode="manual")
        is False
    )
    assert (
        pg_resnapshot_reason(
            jobs["no_slot"], source_type=SourceType.MYSQL, start_mode="auto"
        )
        is None
    )


def test_pg_resnapshot_notice_never_claims_a_second_read_without_a_first() -> None:
    """One copy source for every surface, and the REMEDY must match the CAUSE.

    The failure this prevents: a single "this load recorded no replication slot" message
    reused for all causes. It would send an operator whose load merely FAILED into a full
    re-load when retrying the failed tables was enough, and would blame a missing slot for
    a re-copy the operator deliberately chose. It must also not claim the source is read a
    SECOND time when no Full Load ran in this session at all.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import pg_resnapshot_notice

    assert pg_resnapshot_notice(None) is None
    assert pg_resnapshot_notice("not-a-cause") is None

    after_load = {"no_slot", "unfinished_load", "manual"}
    for reason in after_load | {"no_watermark"}:
        header, body = pg_resnapshot_notice(reason)
        assert header and body
        # Lossless is stated everywhere: the route is legitimate, not a defect.
        assert "Nothing is lost" in body, reason
        # No wall-clock equivalence to the Full Load -- the repo's own measurements put the
        # streaming pipeline and the bulk loader an order of magnitude apart.
        assert "as long as the Full Load" not in body, reason
        # "a second time" / the deleted-row residue are claims ABOUT a preceding load.
        assert ("a second time" in body) is (reason in after_load), reason
        assert ("DELETED on the source" in body) is (reason in after_load), reason

    # Cause-specific remedies, each pointing at the cheapest thing that actually works.
    assert '"Full load + CDC"' in pg_resnapshot_notice("no_slot")[1]
    assert "Retry the failed tables" in pg_resnapshot_notice("unfinished_load")[1]
    assert "back to Automatic" in pg_resnapshot_notice("manual")[1]
    # ...and the unfinished load is NOT told its slot was never recorded.
    assert "recorded no replication slot" not in pg_resnapshot_notice(
        "unfinished_load"
    )[1]


def test_cdc_start_card_pg_auto_without_a_slot_is_resolved_and_discloses_the_reread() -> None:
    """PostgreSQL Automatic with no slot is a RESOLVED start, and it must say what it does.

    Three contradictions lived here at once: an amber "Action needed" badge directly above
    an ENABLED Start CDC button, the PRE-SELECTED default labelled "(unavailable)", and a
    notice offering the re-snapshot as what MANUAL would do -- so leaving the default alone
    read as "I did not choose this", while it is exactly what Automatic does.
    """
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    state = DataMigrationState()
    state.set_cdc_start_mode("auto")
    fake = _FakeUi()
    _render_cdc_start_point_card(
        fake, state, lambda: None,
        wm_resume=None, wm_usable=False, effective_resume=None,
        mode="auto", locked=False, session=None, source_type=SourceType.POSTGRES,
        resnapshot_reason="no_slot",
    )
    # No radio at all now -- the disclosure below carries the whole message. There was never
    # a choice to make: without a slot, Automatic and Manual produce the identical config.
    assert fake.radios == []
    joined = " ".join(fake.texts)
    # The cost is disclosed, with the remedy, rather than steering to Manual as an escape.
    assert "read from the source a second time" in joined
    assert '"Full load + CDC"' in joined
    assert "choose Manual to re-snapshot" not in joined
    # A resolved start point must not raise an alarm badge.
    assert "Action needed" not in joined, joined


def test_start_cdc_detail_does_not_certify_a_resnapshot_as_gapless() -> None:
    """The permanent audit record must not assert gaplessness for a re-snapshotting start.

    Worse than silence: "(gapless from the Full Load watermark)" was appended whenever a
    watermark existed, never consulting ``slot_name``. On a PostgreSQL Full-load-only run
    that certified the one route that re-reads the source AND leaves rows the source has
    since deleted -- and this line is the only durable artifact a Step 4 reader has to
    explain those extra rows.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    def detail(slot, *, force=False, source=SourceType.POSTGRES):
        wm = SimpleNamespace(
            gtid_executed=None, binlog_file=None, binlog_position=None,
            wal_lsn="0/16B3748", slot_name=slot,
        )
        return _cdc_start_detail(
            "dsql-cdc-demo", wm, mode="auto", source_type=source, force_snapshot=force
        )

    no_slot = detail(None)
    assert "gapless" not in no_slot, no_slot
    assert "re-snapshot" in no_slot
    assert "DELETED on the source" in no_slot
    assert "0/16B3748" in no_slot  # the coordinate is still recorded

    # A slot-bearing PostgreSQL watermark IS gapless...
    assert "gapless from the Full Load watermark" in detail("dbz_slot")
    # ...unless a full snapshot is forced, which re-reads just the same.
    assert "gapless" not in detail("dbz_slot", force=True)
    # MySQL is unaffected: its watermark carries no slot by design.
    assert "gapless from the Full Load watermark" in detail(
        None, source=SourceType.MYSQL
    )


def test_start_cdc_detail_names_the_resume_point() -> None:
    """Where the stream resumes FROM cannot be reconstructed afterwards.

    The detail was the stack name alone, so a downloaded activity log recorded THAT CDC was
    started but not from where -- while the connector's own offsets are consumed and the
    deploy log is per-action and ephemeral. It is the fact that makes the Full Load -> CDC
    handoff gapless.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    # MySQL GTID wins the precedence (the portable coordinate).
    wm = SimpleNamespace(
        gtid_executed="aaa:1-9", binlog_file="bin.000004", binlog_position=1234,
        wal_lsn=None,
    )
    detail = _cdc_start_detail(
        "dsql-cdc-stack", wm, mode="auto", source_type=SourceType.MYSQL
    )
    assert "dsql-cdc-stack" in detail
    assert "GTID aaa:1-9" in detail
    assert "gapless" in detail
    assert "mode auto" in detail

    # file:pos when there is no GTID.
    wm2 = SimpleNamespace(
        gtid_executed=None, binlog_file="bin.000004", binlog_position=1234, wal_lsn=None
    )
    assert "binlog bin.000004:1234" in _cdc_start_detail(
        "s", wm2, mode="auto", source_type=SourceType.MYSQL
    )

    # PostgreSQL WAL LSN.
    wm3 = SimpleNamespace(
        gtid_executed=None, binlog_file=None, binlog_position=None, wal_lsn="0/1A2B3C"
    )
    assert "WAL LSN 0/1A2B3C" in _cdc_start_detail(
        "s", wm3, mode="auto", source_type=SourceType.POSTGRES
    )

    # NO watermark is the case a reader must be able to spot: changes before now are lost.
    none_detail = _cdc_start_detail(
        "s", None, mode="manual", source_type=SourceType.MYSQL
    )
    assert "CURRENT position" in none_detail
    assert "NOT replicated" in none_detail
    assert "mode manual" in none_detail

    # A forced initial snapshot and LOB exclusions are stated, as counts only.
    rich = _cdc_start_detail(
        "s", wm, mode="manual", source_type=SourceType.POSTGRES,
        force_snapshot=True, exclusions={"db.t": ["blob_a", "blob_b"], "db.u": ["c"]},
    )
    assert "FULL initial snapshot is forced" in rich
    assert "3 column(s) excluded" in rich
    # Column COUNTS, never a row value.
    assert "blob_a" not in rich


def test_start_cdc_uses_the_shared_watermark_coordinate() -> None:
    # The same precedence as the Full Load watermark line, so the two lines in one log
    # agree instead of spelling the coordinate two ways.
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._cdc_start_detail)
    assert "watermark_coordinate(" in src
    start = inspect.getsource(_cdc_ui._start_cdc_deploy)
    assert "_cdc_start_detail(" in start


def test_the_dlq_panel_points_at_validation_and_names_the_blind_spot() -> None:
    """A dead-letter record says a row was dropped; only Validation says what that COST.

    The two surfaces were not linked at all: Validation never reads a dead-letter record, and
    the quarantine count feeding its "known gap" acknowledgement is Full-Load-only, so a CDC
    quarantine can never populate it. Worse, the operator cannot reach the right answer by
    default -- a dropped UPDATE changes no row count, and ROW_COUNT is the default validation
    mode, so nothing catches it unless CHECKSUM is chosen (and even CHECKSUM skips json and
    floating-point columns). Saying that is the difference between verified and assumed.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import ErrorLogSummary
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_dlq_breakdown

    ui = _FakeUi()
    view = SimpleNamespace(
        error_summary=ErrorLogSummary(
            total_errors=4,
            errors_by_table={"ecommerce.product_media": 3, "ecommerce.orders": 1},
        )
    )
    _render_cdc_dlq_breakdown(ui, view)
    body = " ".join(ui.texts)

    # The per-table chips still render (the at-a-glance answer is unchanged).
    assert "ecommerce.product_media ×3" in body

    # It points at Validation in the HEADER, not only buried in the body: the bold header is
    # what a scanning operator reads, and a body-only mention survived a mutation that reworded
    # the header to say nothing actionable.
    assert "Confirm the effect of these drops in Validation" in ui.texts, ui.texts
    # ...and splits the two recoveries by what Validation actually reports.
    assert "Validation" in body
    assert "MISSING on the target" in body and "dropped insert" in body
    assert "EXTRA on the target" in body and "dropped delete" in body
    # The extra-row case must say a reload cannot fix it -- that is the whole distinction.
    assert "reload cannot" in body
    # It must name the PRECONDITION for both repairs. This card is only on screen while the
    # sink streams, and in that state the Full Load step runs no reload at all (G129: Retry and
    # Reload are disabled, their dialog cannot be confirmed) -- so prescribing "drop and
    # reload" without "Stop CDC first" sent the operator after a button the tool deliberately
    # withholds, with the two screens contradicting each other. G132: the card says so, rather
    # than that only the drop-and-recreate option is withheld.
    assert "Stop CDC first" in body
    assert "the Full Load step runs no reload" in body
    assert "drop-and-recreate option is not offered" not in body
    # And it must not promise an unconditional repair: a reload runs only with CDC stopped.
    assert "a reload fixes them" not in body
    # The blind spot, including that the DEFAULT mode is the one that misses it.
    assert "CHECKSUM" in body
    assert "row-count mode cannot see it" in body
    assert "json" in body

    # Nothing to say when the stream is clean.
    quiet = _FakeUi()
    _render_cdc_dlq_breakdown(quiet, SimpleNamespace(error_summary=None))
    assert "Validation" not in " ".join(quiet.texts)


def _oversized_record(table: str, pk: str, column: str = "content"):
    from datetime import datetime, timezone

    from dsql_migrator.core.models import DataErrorRecord

    return DataErrorRecord(
        table=table,
        pk=pk,
        error_code="OVERSIZED_VALUE",
        message=(
            f"DLQ offset=6: Value for column '{column}' exceeds DSQL's 1048576-byte "
            "limit; quarantined."
        ),
        occurred_at=datetime.now(timezone.utc),
    )


def test_the_oversized_quarantine_groups_only_the_class_that_has_a_remedy() -> None:
    """Offering "exclude the column" for a duplicate-key or a drift rejection would be a lie.

    Only the per-value-limit class has an in-app remedy, and the sink NAMES the column in its
    reason -- better evidence than the Full Load has for the same recovery, where the driver
    message names the TYPE and the picker has to infer candidates from a type token.
    """
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        cdc_oversized_quarantine_groups,
    )

    records = [
        _oversized_record("ecommerce.product_media", "id=1"),
        _oversized_record("ecommerce.product_media", "id=2"),
        _oversized_record("ecommerce.product_media", "id=1"),  # duplicate pk, deduped
        DataErrorRecord(
            table="ecommerce.orders", pk="id=510", error_code="23505",
            message="DLQ offset=7: duplicate key",
            occurred_at=datetime.now(timezone.utc),
        ),
        # A record whose TABLE is unusable: grouping must skip it rather than render a card
        # with no table to act on. (The model forbids an empty table, so this models the
        # defensive path with a stand-in object -- the grouper reads attributes, not a model.)
        SimpleNamespace(table="", pk="id=9", error_code="OVERSIZED_VALUE", message="x"),
    ]
    groups = cdc_oversized_quarantine_groups(records)
    assert len(groups) == 1, groups
    table, pks, columns = groups[0]
    assert table == "ecommerce.product_media"
    assert pks == ["id=1", "id=2"]
    assert columns == ["content"]
    # Nothing to group -> nothing rendered at all.
    assert cdc_oversized_quarantine_groups([]) == []
    assert cdc_oversized_quarantine_groups(None) == []


def test_the_cdc_quarantine_card_offers_the_in_app_recovery() -> None:
    """The CDC dead-letter surface was read-only while the Full Load had a recovery card.

    So an operator who hit the 1 MiB limit on the load could fix it in-app and one who hit it
    on the stream could not -- and the AI assistant, asked what to do, filled that vacuum by
    telling them to hand-edit column.exclude.list on the Debezium connector, which this UI
    cannot do and does not need (the tool owns that setting).
    """
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _render_cdc_oversized_recovery,
    )

    class _State:
        pass

    ui = _FakeUi()
    captured: list = []
    _render_cdc_oversized_recovery(
        ui,
        _State(),
        "log",
        lob_candidates_for=lambda _t: [("content", "bytea")],
        exclude_columns=lambda t, c: captured.append((t, c)),
        records=[_oversized_record("ecommerce.product_media", "id=1")],
    )
    body = " ".join(ui.texts)
    assert "ecommerce.product_media" in body
    assert "id=1" in body                      # the dropped row is named
    assert "never retried" in body             # permanent, not a retry
    assert "Exclude column…" in body           # the action exists
    btn = next((b for b in ui.buttons if "Exclude column" in getattr(b, "text", "")), None)
    assert btn is not None and btn.on_click is not None


def test_the_exclude_button_is_enabled_in_every_state_and_routes() -> None:
    """P3-C20: the button no longer refuses while CDC streams; it records a PENDING rule and
    opens the exclusion popup in the mode the lock gives (read-only with the lock's own
    steps while a stack holds the set). So it is enabled in every state."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _render_cdc_oversized_recovery,
    )

    ui = _FakeUi()
    routed: list = []
    _render_cdc_oversized_recovery(
        ui,
        object(),
        "log",
        lob_candidates_for=lambda _t: [("content", "bytea")],
        exclude_columns=lambda t, c: routed.append((t, c)),
        records=[_oversized_record("ecommerce.product_media", "id=1")],
    )
    btn = next(b for b in ui.buttons if "Exclude column" in getattr(b, "text", ""))
    assert not any("disable" in p for p in btn.props_seen), btn.props_seen
    btn.on_click()
    assert routed and routed[0][0] == "ecommerce.product_media"


def test_the_card_renders_nothing_without_an_in_app_action() -> None:
    """No exclusion handler wired (AI off / older caller) -> no dead card."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _render_cdc_oversized_recovery,
    )

    ui = _FakeUi()
    _render_cdc_oversized_recovery(
        ui, object(), "log", records=[_oversized_record("t", "id=1")]
    )
    assert ui.texts == []


# ---------------------------------------------------------------------------
# Source-connector value-conversion failures (a column delivered as NULL)
# ---------------------------------------------------------------------------


class _ConvController:
    """Fake MskConnectController exposing only what the CDC poll needs."""

    def __init__(self, failures):
        self._failures = failures
        self.reads = 0

    def connector_statuses(self, _names):
        from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus

        return [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)]

    def connector_health(self, _names):
        return {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)}

    def value_conversion_failures(self, log_group, **_kw):
        assert log_group.startswith("/msk-connect/")
        self.reads += 1
        return list(self._failures)


def _conv_failure(table="v45arr.arr_ok", column="vt_nan", type_name="numeric"):
    from dsql_migrator.core.cdc_value_conversion import CdcValueConversionFailure

    return CdcValueConversionFailure(table=table, column=column, type_name=type_name)


def test_fetch_merges_value_conversion_failures_into_a_sticky_set() -> None:
    from dsql_migrator.ui.data_migration import _fetch_cdc_status

    state = DataMigrationState()
    state.set_cdc_controller(_ConvController([_conv_failure(), _conv_failure()]))
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state)
    # The tuple shape is UNCHANGED -- the signal rides on the state, not on a 7th
    # element, so every existing 6-way unpack of this function still works.
    assert fetched is not None and len(fetched) == 6
    assert fetched[2] == []  # and it is NOT a dead letter
    assert [f.qualified_column for f in state.cdc_value_conversion_failures] == [
        "v45arr.arr_ok.vt_nan"
    ]
    # A later poll re-reporting the same column does not grow the set: the same value
    # fails on every row, so a count here would mean nothing.
    _fetch_cdc_status(state)
    assert len(state.cdc_value_conversion_failures) == 1


def test_a_value_conversion_failure_is_never_counted_as_quarantined() -> None:
    """THE regression this must not introduce: the row LANDED, so the DLQ depth must not move.

    A fix that inflates the quarantine count would be worse than the blind spot it
    closes -- that count is what the cut-over decision reads.
    """
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = DataMigrationState()
    state.cdc_value_conversion_failures = [_conv_failure()]
    _apply_cdc_status(state, ([], {}))
    assert state.cdc_status_view is not None
    assert state.cdc_status_view.dlq_depth == 0
    assert state.cdc_status_view.schema_drift == []


def test_the_value_conversion_audit_writes_one_durable_line_per_column() -> None:
    # The poll runs every ~5 s and the activity log is append-only and permanent, so the
    # line must be written once per column -- and must not read as a quarantine, since
    # the log is what a cut-over reviewer counts.
    import dsql_migrator.core.activity_log as real_log
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    logged: list = []
    state = DataMigrationState()
    state.cdc_value_conversion_failures = [_conv_failure()]
    original = real_log.log_activity
    real_log.log_activity = lambda *a, **k: logged.append((a, k))  # type: ignore[assignment]
    try:
        _apply_cdc_status(state, ([], {}))
        _apply_cdc_status(state, ([], {}))
    finally:
        real_log.log_activity = original  # type: ignore[assignment]
    assert len(logged) == 1
    args, kwargs = logged[0]
    assert kwargs["status"] is real_log.ActivityStatus.WARNING
    assert kwargs["target"] == "v45arr.arr_ok"
    assert "vt_nan" in kwargs["detail"]
    assert "quarantin" not in str(args[1]).lower()


def test_the_value_conversion_notice_names_the_column_and_disowns_the_dlq_count() -> None:
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _render_cdc_value_conversion_notice,
    )

    state = DataMigrationState()
    quiet = _FakeUi()
    _render_cdc_value_conversion_notice(quiet, state)
    assert quiet.texts == []  # inert on a healthy stream / a source that never logged one

    state.cdc_value_conversion_failures = [_conv_failure()]
    ui = _FakeUi()
    _render_cdc_value_conversion_notice(ui, state)
    text = " ".join(ui.texts)
    # NAMED (table.column + type): without that the operator cannot act at all.
    assert "v45arr.arr_ok.vt_nan" in text and "numeric" in text
    assert "NULL" in text  # the consequence
    assert "dead-letter" in text  # ...and that this is NOT one
    assert "Validation" in text  # the only surface that can see it
    # The class is broader than the proven members, and the wording must say so.
    assert "only what it has logged so far" in text
    # ...and the TONE is `error`, not a "be aware": the named column is silently wrong on
    # the target for every row CDC delivered and stays wrong until acted on, which will
    # fail Validation and therefore blocks cut over. Pinned via the design system's own
    # glyph table so the tone cannot be softened unnoticed.
    from dsql_migrator.ui.design import NOTICE_STYLE

    assert NOTICE_STYLE["error"][3] in ui.icons
    assert NOTICE_STYLE["info"][3] not in ui.icons


def test_start_over_clears_the_value_conversion_signal() -> None:
    """Start over must not carry a prior migration's NULLed columns into the next one.

    The sticky set is session state, not controller state (CDC discovery rebuilds the
    controller on every probe), so ``reset_in_place`` -- which re-runs ``__init__`` on the
    SAME object the builders captured -- is the only thing that clears it. Without the
    ``__init__`` declarations the attributes would survive a reset and the next migration
    would open with the previous one's findings.
    """
    from dsql_migrator.ui.data_migration import DataMigrationStore

    store = DataMigrationStore()
    state = store.get_or_create("s-conv")
    state.cdc_value_conversion_failures = [_conv_failure()]
    state.cdc_value_conversion_audited_keys.add(("v45arr.arr_ok", "vt_nan", "numeric"))

    store.reset_in_place("s-conv")

    assert store.get_or_create("s-conv") is state  # same captured instance
    assert state.cdc_value_conversion_failures == []
    assert state.cdc_value_conversion_audited_keys == set()


def test_the_notice_is_wired_into_the_live_monitor_before_the_dlq_card() -> None:
    """Without the call site the whole signal is dead code, so pin the wiring.

    Source inspection, as the drift-banner wiring assertions do: the notice must be
    invoked from the live CDC section, and BEFORE the dead-letter panel -- an error-level
    integrity finding must not be filed under a "0 quarantined" all-clear, and it must
    never render INSIDE a card about records that did NOT land.
    """
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_monitoring

    src = inspect.getsource(_cdc_monitoring._render_cdc_live_monitoring)
    assert "_render_cdc_value_conversion_notice(ui, migration_state)" in src
    assert src.index("_render_cdc_value_conversion_notice(ui, migration_state)") < src.index(
        "            _render_cdc_dlq_panel("
    )


# ---------------------------------------------------------------------------
# Source rows (est.): say what the estimate is and how old it is
# ---------------------------------------------------------------------------


def test_source_estimate_cell_labels_never_analyzed_and_dates_the_rest() -> None:
    from datetime import datetime, timezone

    from dsql_migrator.ui.data_migration._cdc_monitoring import _source_estimate_cell

    when = datetime(2026, 9, 24, 22, 22, tzinfo=timezone.utc)
    stats = {"users": when, "products": None}

    # Never analyzed (present with None, no estimate): labelled, not a bare dash that
    # reads as a failed read.
    label, tip = _source_estimate_cell(None, exact=False, stats_at=stats, table="products")
    assert label == "not analyzed"
    assert "never been analyzed or vacuumed" in tip and "Validation" in tip

    # Refreshed: the number, plus WHEN -- live-seen: users read 100 vs a true 108 because
    # its statistics were four days old.
    label, tip = _source_estimate_cell(100, exact=False, stats_at=stats, table="users")
    assert label == "100"
    assert "2026-09-24 22:22 UTC" in tip and "may not be in it" in tip
    assert "least recently refreshed partition" in tip

    # An estimate with NO time covering it (a partitioned table with a never-analyzed
    # leaf: live order_events read 600 vs 638) must not be dated -- its age is unknown.
    label, tip = _source_estimate_cell(600, exact=False, stats_at=stats, table="products")
    assert label == "600"
    assert "is unknown" in tip and "partition that has never been analyzed" in tip
    assert "UTC" not in tip

    # Unknown (absent: MySQL, or the read failed): the plain rendering, no invented label.
    assert _source_estimate_cell(None, exact=False, stats_at=stats, table="x") == ("—", "")
    assert _source_estimate_cell(7, exact=False, stats_at={}, table="x") == ("7", "")

    # An EXACT count keeps its marker and needs no staleness note.
    assert _source_estimate_cell(9, exact=True, stats_at=stats, table="users") == (
        "9 (exact)", ""
    )


def test_source_estimate_header_tip_is_worded_for_the_source_engine() -> None:
    # It described only MySQL ("information_schema ... InnoDB") even on a PostgreSQL
    # source, where the figure is pg_class.reltuples and goes stale until ANALYZE.
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_monitoring import _source_estimate_header_tip

    pg = _source_estimate_header_tip(SourceType.POSTGRES)
    assert "pg_class.reltuples" in pg and "ANALYZE" in pg and "not analyzed" in pg
    assert "information_schema" not in pg and "InnoDB" not in pg

    my = _source_estimate_header_tip(SourceType.MYSQL)
    assert "information_schema" in my and "InnoDB" in my
    assert "information_schema_stats_expiry" in my  # MySQL 8.0+ caches TABLE_ROWS
    assert "pg_class" not in my
    # PostgreSQL: VACUUM refreshes reltuples too, and no platform-specific thresholds
    # are quoted (RDS/Aurora ship different autovacuum defaults).
    assert "VACUUM" in pg
    assert "50 rows" not in pg and "10%" not in pg
    for tip in (pg, my):
        assert "never runs a COUNT(*)" in tip and "Validation (step 4)" in tip


def _fetch_with_fake_source(monkeypatch, stats_behaviour):
    """Drive _fetch_migration_row_counts over a fake source; no DB, no AWS."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    import dsql_migrator.core.source_dialect as sd
    import dsql_migrator.core.watermark as wm
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration import _cdc_status

    class _Engine:
        @contextmanager
        def connect(self):
            yield object()

    class _Dialect:
        def estimate_stats_at(self, connection, tables):
            return stats_behaviour(tables)

    monkeypatch.setattr(_cdc_status, "make_source_engine_factory", lambda _pw: lambda _c: _Engine())
    monkeypatch.setattr(sd, "dialect_for", lambda _t: _Dialect())
    monkeypatch.setattr(wm, "estimate_source_rows", lambda c, t, d: {n: 5 for n in t})
    monkeypatch.setattr(wm, "max_pk_source", lambda c, p, d: {})
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=None),
        source_password=object(),
        has_source=lambda: True,
        has_target=lambda: False,
        target_config=None,
    )
    state = DataMigrationState()
    out = _cdc_status._fetch_migration_row_counts(state, session, ["t1"])
    return state, out


def test_counts_refresh_records_when_each_estimate_was_taken(monkeypatch) -> None:
    from datetime import datetime, timezone

    when = datetime(2026, 9, 28, 6, 45, tzinfo=timezone.utc)
    state, out = _fetch_with_fake_source(monkeypatch, lambda tables: {"t1": when})
    source_counts, *_rest, source_available = out
    assert source_counts == {"t1": 5} and source_available is True
    assert state.source_estimate_stats_at == {"t1": when}


def test_a_failing_stats_read_never_costs_the_counts(monkeypatch) -> None:
    # Display-only metadata: if it raises, the estimate and MAX(pk) must still land.
    def _boom(_tables):
        raise RuntimeError("permission denied for pg_stat_all_tables")

    state, out = _fetch_with_fake_source(monkeypatch, _boom)
    source_counts, *_rest, source_available = out
    assert source_counts == {"t1": 5} and source_available is True
    assert state.source_estimate_stats_at == {}


def test_status_table_renders_the_estimate_label_age_and_engine_tip() -> None:
    # Wiring: the helpers are dead code unless the status table uses them. The table is
    # built inside a live NiceGUI render (timers, slots), so it is guarded at the source.
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_monitoring as m

    src = inspect.getsource(m._render_migration_table_status)
    assert "_source_estimate_cell(" in src
    assert "source_estimate_stats_at" in src  # the age comes from the refresh, via state
    assert '"source_tip": source_tip' in src
    assert '"body-cell-source"' in src and "props.row.source_tip" in src
    # Engine read through the restore-aware helper, never a raw source_config getattr.
    assert "_source_estimate_header_tip(session_source_type(session))" in src


def test_estimate_age_is_read_after_slot_health_so_it_cannot_poison_it(monkeypatch) -> None:
    # The source connection is not autocommit and a server-side error in the estimate
    # helper is swallowed WITHOUT a rollback, leaving the transaction aborted. Run before the
    # slot-health read, one failure silently failed that read too (InFailedSqlTransaction).
    from dsql_migrator.ui.data_migration import _cdc_status

    order: list[str] = []
    monkeypatch.setattr(
        _cdc_status, "_refresh_pg_slot_health", lambda *a, **k: order.append("slot")
    )

    def _stats(tables):
        order.append("stats")
        return {}

    _fetch_with_fake_source(monkeypatch, _stats)
    assert order == ["slot", "stats"]


def test_estimate_age_is_cleared_when_the_source_cannot_be_read(monkeypatch) -> None:
    # A refresh that cannot read the source falls back to the Full Load watermark figure;
    # keeping the previous refresh's ANALYZE times would date that different figure.
    from contextlib import contextmanager
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from dsql_migrator.ui.data_migration import DataMigrationState, _cdc_status

    class _DeadEngine:
        @contextmanager
        def connect(self):
            raise OSError("source unreachable")
            yield  # pragma: no cover

    monkeypatch.setattr(_cdc_status, "make_source_engine_factory", lambda _pw: lambda _c: _DeadEngine())
    state = DataMigrationState()
    state.set_source_estimate_stats_at({"t1": datetime(2026, 9, 1, tzinfo=timezone.utc)})
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=None),
        source_password=object(),
        has_source=lambda: True,
        has_target=lambda: False,
        target_config=None,
    )
    *_counts, source_available = _cdc_status._fetch_migration_row_counts(state, session, ["t1"])
    assert source_available is False
    assert state.source_estimate_stats_at == {}


def test_the_dlq_panel_says_cut_over_waits_for_a_checksum_after_the_last_record() -> None:
    """G1: the card used to say Validation never reads a dead letter and named a blind spot
    that is wrong for a PostgreSQL source (its floats ARE compared)."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import ErrorLogSummary
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_dlq_breakdown

    ui = _FakeUi()
    _render_cdc_dlq_breakdown(ui, SimpleNamespace(
        error_summary=ErrorLogSummary(total_errors=1, errors_by_table={"app.t": 1})
    ))
    body = " ".join(ui.texts)
    assert "Cut over stays locked until each has matched in a CHECKSUM comparison" in body
    assert "never retried" in body
    assert "json and floating-point columns" not in body
    assert "nor, from a MySQL source, floating-point ones" in body


# ---------------------------------------------------------------------------
# G7: why a connector stopped, on the Pipeline-health row (worker-log diagnosis)
# ---------------------------------------------------------------------------

_G7_SRC = "dsql-cdc-x-debezium-source"
_G7_SINK = "dsql-cdc-x-dsql-sink"
_G7_TAIL = "\n".join([
    f"[Worker-1] [ts] ERROR [{_G7_SRC}|task-0] Error during binlog processing. Last offset "
    "stored = {...}, binlog reader near position = mysql-bin-changelog.000123/4567",
    "[Worker-1] Caused by: io.debezium.text.ParsingException: DDL statement couldn't be "
    "parsed. Please open a Jira issue with the statement 'CREATE PROCEDURE p() BEGIN "
    "SELECT 'secret-ish'; END'",
    f"[Worker-1] [ts] INFO [{_G7_SRC}|task-0] App info kafka.producer unregistered",
])
_G7_HEADLINE = "Debezium could not parse a DDL statement in the binary log"


def _g7_diagnosis():
    from dsql_migrator.core.cdc_deployer import diagnose_connector_log

    diagnosis = diagnose_connector_log(_G7_SRC, _G7_TAIL, episode_only=True)
    assert diagnosis is not None
    return diagnosis


class _DiagController:
    """Fake MskConnectController: connector states/health plus a recorded log reader."""

    def __init__(self, statuses, health, tail=_G7_TAIL):
        self._statuses = statuses
        self._health = health
        self._tail = tail
        self.calls: list[tuple[str, str]] = []

    def connector_statuses(self, _names):
        from dsql_migrator.core.cdc import ConnectorStatus

        return [ConnectorStatus(name=n, state=s) for n, s in self._statuses]

    def connector_health(self, _names):
        return dict(self._health)

    def connector_log_tail(self, stack, connector, **_kw):
        self.calls.append((stack, connector))
        return self._tail


def _g7_state(controller=None):
    state = DataMigrationState()
    state.set_cdc_stack_name("dsql-cdc-x")
    if controller is not None:
        state.set_cdc_controller(controller)
    state.set_cdc_connector_names([_G7_SRC, _G7_SINK])
    return state


def test_health_row_shows_the_diagnosed_cause_for_a_failed_connector() -> None:
    states = {_G7_SRC: "FAILED", _G7_SINK: "RUNNING"}
    rows = connector_health_rows(states, diagnoses={_G7_SRC: _G7_HEADLINE})
    by_name = {r.name: r for r in rows}
    assert by_name[_G7_SRC].detail == f"Stopped — {_G7_HEADLINE}."
    assert by_name[_G7_SRC].tone == "bad"
    assert by_name[_G7_SINK].detail == "Streaming normally."
    # No diagnosis: the generic text, byte for byte.
    plain = {r.name: r for r in connector_health_rows(states)}
    assert plain[_G7_SRC].detail == "Stopped — a task is not running. Restart required."
    # A cause for a RUNNING connector is ignored.
    running = connector_health_rows({_G7_SINK: "RUNNING"}, diagnoses={_G7_SINK: "x"})
    assert running[0].detail == "Streaming normally." and running[0].tone == "ok"


def test_fetch_reads_the_worker_log_only_for_a_stopped_connector_throttled_and_sticky() -> None:
    from dsql_migrator.core.cdc import ConnectorState
    from dsql_migrator.ui.data_migration import _fetch_cdc_status
    from dsql_migrator.ui.data_migration._cdc_status import _read_connector_diagnoses

    controller = _DiagController(
        [(_G7_SRC, ConnectorState.RUNNING), (_G7_SINK, ConnectorState.RUNNING)],
        {
            _G7_SRC: ConnectorHealth(running_tasks=0, errored_tasks=1),
            _G7_SINK: ConnectorHealth(running_tasks=1, errored_tasks=0),
        },
    )
    state = _g7_state(controller)
    fetched = _fetch_cdc_status(state)
    assert fetched is not None and len(fetched) == 6  # the tuple shape is unchanged
    assert controller.calls == [("dsql-cdc-x", _G7_SRC)]
    held = state.cdc_connector_diagnoses
    assert set(held) == {_G7_SRC} and held[_G7_SRC].headline == _G7_HEADLINE
    assert "secret-ish" not in held[_G7_SRC].guidance
    _fetch_cdc_status(state)
    assert controller.calls == [("dsql-cdc-x", _G7_SRC)]  # no second read within the interval

    # Sticky, not merely throttled: on an injected clock a held cause is never re-read,
    # however long past the read interval the page stays open.
    held_ctl = _DiagController([(_G7_SRC, ConnectorState.FAILED)], {})
    h_state = _g7_state(held_ctl)
    for t in (0.0, 61.0, 1000.0):
        _read_connector_diagnoses(h_state, held_ctl, held_ctl.connector_statuses([]), {}, now=t)
    assert held_ctl.calls == [("dsql-cdc-x", _G7_SRC)]
    assert set(h_state.cdc_connector_diagnoses) == {_G7_SRC}

    # Unmatched: re-read at most once per interval (injected clock).
    quiet = _DiagController(
        [(_G7_SRC, ConnectorState.FAILED)], {}, tail="nothing recognisable"
    )
    q_state = _g7_state(quiet)
    statuses = quiet.connector_statuses([])
    for t in (0.0, 30.0, 61.0):
        _read_connector_diagnoses(q_state, quiet, statuses, {}, now=t)
    assert len(quiet.calls) == 2 and q_state.cdc_connector_diagnoses == {}

    # Same fold as the status view: errored tasks fold only a RUNNING connector (CREATING
    # maps to UNASSIGNED).
    creating = _DiagController(
        [(_G7_SRC, ConnectorState.UNASSIGNED)],
        {_G7_SRC: ConnectorHealth(running_tasks=0, errored_tasks=1)},
    )
    _read_connector_diagnoses(
        _g7_state(creating), creating, creating.connector_statuses([]),
        creating.connector_health([]), now=0.0,
    )
    assert creating.calls == []
    # A healthy pipeline makes no log call at all.
    healthy = _DiagController(
        [(_G7_SRC, ConnectorState.RUNNING)],
        {_G7_SRC: ConnectorHealth(running_tasks=1, errored_tasks=0)},
    )
    _fetch_cdc_status(_g7_state(healthy))
    assert healthy.calls == []

    # A controller without the reader is a no-op (older doubles, other callers).
    class _NoReader:
        pass

    _read_connector_diagnoses(_g7_state(), _NoReader(), statuses, {}, now=0.0)


def test_the_poll_scopes_the_worker_log_to_the_latest_task_run() -> None:
    # The poll must read the tail with episode_only: the pure scoping alone does not stop
    # the health row from being mislabelled if the poll reads the tail whole.
    from dsql_migrator.core.cdc import ConnectorState
    from dsql_migrator.ui.data_migration._cdc_status import _read_connector_diagnoses

    # A PostgreSQL start's INFO slot line must not label a later, unrelated failure...
    slot_info = (
        "[W] [ts] INFO [c|task-0] Obtained valid replication slot ReplicationSlot [...]\n"
        "[W] [ts] ERROR [c|task-0] WorkerSourceTask{id=c-0} Task threw an uncaught "
        "exception: org.apache.kafka.connect.errors.ConnectException: something else"
    )
    # ...nor an EARLIER task run's DDL failure label this run.
    restarted = (
        f"{_G7_TAIL}\n"
        "[W] [ts] INFO [c|task-0] Starting MySqlConnectorTask with configuration:\n"
        "[W] [ts] WARN [c|task-0] something harmless"
    )
    for tail in (slot_info, restarted):
        controller = _DiagController([(_G7_SRC, ConnectorState.FAILED)], {}, tail=tail)
        state = _g7_state(controller)
        _read_connector_diagnoses(
            state, controller, controller.connector_statuses([]), {}, now=0.0
        )
        assert controller.calls == [("dsql-cdc-x", _G7_SRC)]
        assert state.cdc_connector_diagnoses == {}


def test_a_late_read_after_a_clear_is_dropped() -> None:
    diagnosis = _g7_diagnosis()

    def _late(state, end_episode):
        epoch = state.claim_cdc_diagnosis_read(_G7_SRC, now=0.0, interval=60.0)
        assert epoch is not None
        end_episode(state)
        state.record_cdc_connector_diagnosis(_G7_SRC, diagnosis, epoch=epoch)
        return state.cdc_connector_diagnoses

    assert _late(_g7_state(), lambda s: s.clear_cdc_connector_diagnoses()) == {}
    assert _late(_g7_state(), lambda s: s.set_cdc_stack_name("dsql-cdc-other")) == {}
    assert _late(_g7_state(), lambda s: s.adopt_cdc_stack("dsql-cdc-other")) == {}
    # A recovery (the per-name clear the apply step does) drops a late read too: another
    # tab's read that began before it must not re-hold a cause the connector recovered from.
    assert _late(_g7_state(), lambda s: s.clear_cdc_connector_diagnoses([_G7_SRC])) == {}
    # ...while a read in the same episode lands.
    state = _g7_state()
    epoch = state.claim_cdc_diagnosis_read(_G7_SRC, now=0.0, interval=60.0)
    state.record_cdc_connector_diagnosis(_G7_SRC, diagnosis, epoch=epoch)
    assert state.cdc_connector_diagnoses == {_G7_SRC: diagnosis}
    # A recovery also forgets the throttle: a re-failure is read at once, not up to 60 s on.
    state.clear_cdc_connector_diagnoses([_G7_SRC])
    assert state.claim_cdc_diagnosis_read(_G7_SRC, now=1.0, interval=60.0) is not None


def test_apply_clears_a_diagnosis_only_on_a_positive_healthy_signal() -> None:
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    def _held_state():
        state = _g7_state()
        epoch = state.claim_cdc_diagnosis_read(_G7_SRC, now=0.0, interval=60.0)
        state.record_cdc_connector_diagnosis(_G7_SRC, _g7_diagnosis(), epoch=epoch)
        return state

    running = [ConnectorStatus(name=_G7_SRC, state=ConnectorState.RUNNING)]
    healthy = _held_state()
    _apply_cdc_status(
        healthy, (running, {_G7_SRC: ConnectorHealth(running_tasks=1, errored_tasks=0)})
    )
    assert healthy.cdc_connector_diagnoses == {}
    unknown = _held_state()
    _apply_cdc_status(unknown, (running, {_G7_SRC: ConnectorHealth()}))
    assert set(unknown.cdc_connector_diagnoses) == {_G7_SRC}
    absent = _held_state()
    _apply_cdc_status(absent, ([], {}))
    assert set(absent.cdc_connector_diagnoses) == {_G7_SRC}
    # Only RUNNING with no errored task is a recovery: MSK still reporting the connector
    # FAILED keeps the cause, even when the newest ErroredTaskCount datapoint reads 0.
    failed_zero = _held_state()
    _apply_cdc_status(
        failed_zero,
        (
            [ConnectorStatus(name=_G7_SRC, state=ConnectorState.FAILED)],
            {_G7_SRC: ConnectorHealth(running_tasks=0, errored_tasks=0)},
        ),
    )
    assert set(failed_zero.cdc_connector_diagnoses) == {_G7_SRC}


def test_a_failed_connector_with_zero_errored_tasks_is_read_once_and_logged_once() -> None:
    # Through the real poll: were FAILED + ErroredTaskCount 0 taken for a recovery, the
    # clear (which also drops the read throttle) would re-read the log on every poll and
    # never hold the cause or write its activity line.
    import dsql_migrator.core.activity_log as real_log
    from dsql_migrator.core.cdc import ConnectorState
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    controller = _DiagController(
        [(_G7_SRC, ConnectorState.FAILED)],
        {_G7_SRC: ConnectorHealth(running_tasks=0, errored_tasks=0)},
    )
    state = _g7_state(controller)
    logged: list = []
    original = real_log.log_activity
    real_log.log_activity = lambda *a, **k: logged.append((a, k))  # type: ignore[assignment]
    try:
        for _ in range(3):
            _apply_cdc_status(state, _fetch_cdc_status(state))
    finally:
        real_log.log_activity = original  # type: ignore[assignment]
    assert controller.calls == [("dsql-cdc-x", _G7_SRC)]
    assert set(state.cdc_connector_diagnoses) == {_G7_SRC}
    assert len([e for e in logged if "stopped:" in str(e[0][1])]) == 1


def test_apply_logs_each_stop_once_without_the_raw_tail() -> None:
    import dsql_migrator.core.activity_log as real_log
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = _g7_state()

    def _hold():
        epoch = state.claim_cdc_diagnosis_read(_G7_SRC, now=0.0, interval=0.0)
        state.record_cdc_connector_diagnosis(_G7_SRC, _g7_diagnosis(), epoch=epoch)

    failed = (
        [ConnectorStatus(name=_G7_SRC, state=ConnectorState.FAILED)],
        {_G7_SRC: ConnectorHealth(running_tasks=0, errored_tasks=1)},
    )
    logged: list = []
    original = real_log.log_activity
    real_log.log_activity = lambda *a, **k: logged.append((a, k))  # type: ignore[assignment]
    try:
        _hold()
        _apply_cdc_status(state, failed)
        _apply_cdc_status(state, failed)
        mine = [entry for entry in logged if "stopped:" in str(entry[0][1])]
        assert len(mine) == 1
        (args, kwargs), = mine
        assert args[1] == f"connector {_G7_SRC} stopped: {_G7_HEADLINE}"
        assert kwargs["status"] is real_log.ActivityStatus.FAILURE
        assert kwargs["detail"] == _g7_diagnosis().guidance
        assert "App info kafka.producer" not in kwargs["detail"]
        assert "secret-ish" not in kwargs["detail"]
        # A new episode (cleared, then diagnosed again) is logged again.
        state.clear_cdc_connector_diagnoses()
        _hold()
        _apply_cdc_status(state, failed)
        assert len([e for e in logged if "stopped:" in str(e[0][1])]) == 2
    finally:
        real_log.log_activity = original  # type: ignore[assignment]


def test_pipeline_health_renders_the_runbook_notice_for_a_diagnosed_failure() -> None:
    from dsql_migrator.core.models import LoadKind, LoadStatusView
    from dsql_migrator.ui.data_migration._cdc_monitoring import _render_cdc_pipeline_health
    from dsql_migrator.ui.design import NOTICE_STYLE

    diagnosis = _g7_diagnosis()
    view = LoadStatusView(
        kind=LoadKind.CDC, connector_states={_G7_SRC: "FAILED", _G7_SINK: "RUNNING"}
    )
    ui = _FakeUi()
    _render_cdc_pipeline_health(
        ui, view, None, diagnoses={_G7_SRC: diagnosis}, log_group="/msk-connect/x-cdc"
    )
    # The FAILED row's own status glyph is the same Material icon, so count: one for the
    # row, one for the error notice.
    error_icon = NOTICE_STYLE["error"][3]
    assert ui.icons.count(error_icon) == 2
    assert f"Source (→ Kafka) stopped: {_G7_HEADLINE}" in ui.texts
    body = next(t for t in ui.texts if t.startswith(diagnosis.guidance))
    assert "/msk-connect/x-cdc" in body
    assert f"Stopped — {_G7_HEADLINE}." in ui.texts
    # No diagnosis, or one for a RUNNING connector: no notice (and the positional call works).
    plain = _FakeUi()
    _render_cdc_pipeline_health(plain, view, None)
    assert plain.icons.count(error_icon) == 1
    assert not any("stopped:" in t for t in plain.texts)
    assert "Stopped — a task is not running. Restart required." in plain.texts
    sink_only = _FakeUi()
    _render_cdc_pipeline_health(sink_only, view, None, diagnoses={_G7_SINK: diagnosis})
    assert sink_only.icons.count(error_icon) == 1
    assert not any("stopped:" in t for t in sink_only.texts)


def test_g22_the_sink_stall_notice_tells_a_transient_target_from_an_ejected_consumer() -> None:
    # G22: since plugin v52 a sustained Aurora DSQL XX000 "server unavailable" stalls the sink
    # (retried, then redelivered by Connect -- never dead-lettered), which the rate gauges show
    # exactly like an ejected sink consumer. Only the connector log tells them apart, and only
    # the ejected consumer needs Stop + Start: the notice must name both log lines, quote the
    # sink's own transient-failure prefix, and not claim the gap never closes on its own.
    import inspect
    from pathlib import Path

    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    body = cm.SINK_STALL_NOTICE_BODY
    assert '"Commit of offsets timed out"' in body
    assert "Stop CDC and Start CDC to rejoin the group" in body
    assert "XX000" in body and "resumes on its own once Aurora DSQL answers" in body
    assert "contact AWS Support" in body
    assert "do NOT cut over until the sink send rate recovers" in body
    # The prefix is the sink's own log text (DsqlSinkTask.transientRetryException).
    assert '"DSQL apply failed (transient sqlstate="' in body
    root = Path(__file__).resolve().parents[1]
    sink = (
        root / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
    ).read_text(encoding="utf-8")
    assert '"DSQL apply failed (transient sqlstate=" + e.getSQLState()' in sink
    # "will not close on its own" is said of the ejected consumer only.
    ejected, transient = body.split("A repeating \"DSQL apply failed", 1)
    assert "will not close on its own" in ejected and "will not close" not in transient
    # The confirmed-stall branch renders this constant.
    assert "body=SINK_STALL_NOTICE_BODY" in inspect.getsource(cm._render_change_flow_status)


def test_g126_the_sink_stall_notice_explains_a_connection_aurora_dsql_refused_to_open() -> None:
    # G126: since sink plugin v54 a connection Aurora DSQL refuses to OPEN -- its IAM token
    # (28xxx), too many open connections (53300), the connection rate (53400) -- is retried
    # and redelivered (it used to dead-letter every row), so it now shows as a stall whose log
    # line carries that SQLSTATE. The notice the operator reads must say what each one means.
    from pathlib import Path

    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    body = cm.SINK_STALL_NOTICE_BODY
    _network, refused = body.split("28xxx, 53300 or 53400 means", 1)
    refused = refused.split("40001 means", 1)[0]
    for needle in ("refused to open", "IAM token", "IAM role", "53300", "open connections",
                   "raise the quota", "53400", "resumes once a connection opens"):
        assert needle in refused, needle
    assert "AWS Support" not in refused
    # The SQLSTATE the notice reads is the one the sink logs for a refused connect.
    sink = (
        Path(__file__).resolve().parents[1]
        / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
    ).read_text(encoding="utf-8")
    assert "sql == null ? \"08001\" : sql.getSQLState()" in sink


def test_a_postgres_restart_names_the_slot_not_the_full_load_lsn() -> None:
    """Live PG E2E (2026-10-09): the resumed Start's audit line said "start point WAL LSN
    0/41204A0 (gapless from the Full Load watermark)" while Debezium resumed from the slot at
    0/42F8428 -- the permanent record named a position the stream had long passed."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    wm = SimpleNamespace(
        gtid_executed=None, binlog_file=None, binlog_position=None,
        wal_lsn="0/41204A0", slot_name="dsqlmig_x",
    )
    resumed = _cdc_start_detail(
        "dsql-cdc-x", wm, mode="auto", source_type=SourceType.POSTGRES, resumes=True
    )
    assert "replication slot dsqlmig_x" in resumed
    assert "continues from the slot's confirmed position" in resumed
    assert "where streaming stopped" in resumed
    # The watermark is named for a connector that never streamed from the slot. (G25 round 6:
    # no slot a Full Load re-created since -- such a Full Load waits for new topics on a stack
    # that has streamed.)
    assert "WAL LSN 0/41204A0 if the connector never streamed from it" in resumed
    assert "recreated the slot" not in resumed
    assert "gapless from the Full Load watermark" not in resumed
    # The first start keeps its line; a forced snapshot says it re-snapshots only when no
    # offset is committed (G25: snapshot.mode=initial skips a completed snapshot's offset, so a
    # forced restart continues from the slot like any other).
    first = _cdc_start_detail("dsql-cdc-x", wm, mode="auto", source_type=SourceType.POSTGRES)
    assert "gapless from the Full Load watermark" in first
    forced = _cdc_start_detail(
        "dsql-cdc-x", wm, mode="manual", source_type=SourceType.POSTGRES,
        resumes=True, force_snapshot=True,
    )
    assert "replication slot dsqlmig_x" in forced and "re-snapshot" not in forced
    forced_first = _cdc_start_detail(
        "dsql-cdc-x", wm, mode="manual", source_type=SourceType.POSTGRES, force_snapshot=True,
    )
    assert "re-snapshot" in forced_first


def test_the_monitor_reads_the_source_in_autocommit(monkeypatch) -> None:
    """One refused MAX(pk) (row-level security, with row_security = off) aborted the shared
    transaction, so every later table's MAX(pk) and the slot-health read failed with it."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    import dsql_migrator.core.source_dialect as sd
    import dsql_migrator.core.watermark as wm
    from dsql_migrator.ui.data_migration import DataMigrationState, _cdc_status

    seen: list = []

    class _Connection:
        def execution_options(self, **kwargs):
            seen.append(kwargs)
            return self

    class _Engine:
        @contextmanager
        def connect(self):
            yield _Connection()

    class _Dialect:
        def estimate_stats_at(self, connection, tables):
            return {}

    monkeypatch.setattr(_cdc_status, "make_source_engine_factory", lambda _pw: lambda _c: _Engine())
    monkeypatch.setattr(sd, "dialect_for", lambda _t: _Dialect())
    monkeypatch.setattr(wm, "estimate_source_rows", lambda c, t, d: {n: 5 for n in t})
    monkeypatch.setattr(wm, "max_pk_source", lambda c, p, d: {})
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=None),
        source_password=object(),
        has_source=lambda: True,
        has_target=lambda: False,
        target_config=None,
    )
    _cdc_status._fetch_migration_row_counts(DataMigrationState(), session, ["t1"])
    assert seen == [{"isolation_level": "AUTOCOMMIT"}]
