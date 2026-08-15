# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Control + read-only monitoring of already-deployed MSK Connect connectors.

The tool's UI does NOT deploy the cdc-stack (decision-change 8) -- the CDC data
plane (MSK Serverless + MSK Connect, Debezium source + custom DSQL sink) is
provisioned separately via CloudFormation. This module lets the control plane
*observe and stop* those already-deployed connectors:

  * read-only: ``list_connectors`` / ``describe_connector`` for connector state +
    task health, and CloudWatch ``MilliSecondsBehindSource`` for replication lag.
  * one guarded mutation: ``delete_connector`` (MSK Connect has no pause/resume
    API, so "stop" is a delete). The controller performs the delete with NO
    confirmation of its own -- the UI caller owns the type-the-name confirmation
    dialog. Keeping the guard in the UI keeps it reviewable and testable.

Mirrors :class:`~dsql_migrator.ui.prerequisite_probes.SessionMskProbe`: the shared
profile-aware :func:`build_session` builds boto3 clients lazily (so importing or
constructing this never reaches AWS), the ``session`` arg is a test-injection
seam, and every read method turns any access error into a falsy result so the UI
degrades gracefully instead of crashing. Property 1 (read-only except the one
guarded stop) and Property 7 (no credential value is read or logged) hold.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.cdc import CdcConnectorError, ConnectorState, ConnectorStatus
from dsql_migrator.core.cdc_dlq import parse_dlq_log_message


@dataclass
class ConnectorHealth:
    """CloudWatch task-health snapshot for one connector (latest datapoint).

    All fields are ``None`` when the metric has no recent datapoint or CloudWatch
    could not be read, so the UI shows "unknown" rather than a wrong zero.
    """

    running_tasks: Optional[int] = None
    errored_tasks: Optional[int] = None
    send_rate: Optional[float] = None
    poll_rate: Optional[float] = None

# MSK Connect connectorState -> our coarse ConnectorState. MSK Connect reports
# RUNNING/PAUSED/FAILED/CREATING/UPDATING/DELETING; we map the transient
# lifecycle states to the closest monitoring bucket.
_CONNECTOR_STATE_MAP = {
    "RUNNING": ConnectorState.RUNNING,
    "PAUSED": ConnectorState.PAUSED,
    "FAILED": ConnectorState.FAILED,
    "CREATING": ConnectorState.UNASSIGNED,
    "UPDATING": ConnectorState.UNASSIGNED,
    "DELETING": ConnectorState.PAUSED,
}

# CloudWatch namespace for MSK Connect connector health/throughput metrics.
# NOTE: MSK Connect does NOT publish a replication-lag metric to CloudWatch
# (Debezium's MilliSecondsBehindSource is a JMX metric, not auto-exported). The
# CloudWatch path here covers connector health (running/errored task counts,
# send rate); end-to-end replication lag is measured separately against the DSQL
# target (now - max applied timestamp), which is the most accurate signal and
# needs no cdc-stack change.
_MSK_CONNECT_NAMESPACE = "AWS/KafkaConnect"
_RUNNING_TASKS_METRIC = "RunningTaskCount"
_ERRORED_TASKS_METRIC = "ErroredTaskCount"
_SEND_RATE_METRIC = "SinkRecordSendRate"
# Source connector poll rate: change events read from the binlog per second.
# Together with the sink send rate it tells whether changes are still flowing
# (both ~0 after the source is quiesced = pipeline idle / caught up).
_SOURCE_POLL_RATE_METRIC = "SourceRecordPollRate"

# The custom DSQL sink's per-table applied-ops metrics (connectors/dsql-sink emits a
# per-offset-commit COUNT of inserts / updates / deletes, dimensioned Stack + Table).
# Summed over the window here to get a DMS-style change breakdown per table — a
# source-scan-free CDC signal that replaces COUNT(*)-ing the source in the live
# monitor, and (unlike the old NetRowsApplied) makes UPDATE traffic visible.
_CDC_METRIC_NAMESPACE = "MysqlDsqlMigrator/CDC"
_APPLIED_OP_METRICS = {  # result key -> CloudWatch metric name
    "inserts": "InsertsApplied",
    "updates": "UpdatesApplied",
    "deletes": "DeletesApplied",
}
# CloudWatch GetMetricData accepts at most 500 MetricDataQueries per request. The
# applied-ops read builds 3 queries per matched table, so a large-scale migration
# (>~166 tracked tables) would exceed this in one call -- batch to stay under it.
_GMD_MAX_QUERIES = 500
# End-to-end replication lag (ms) the sink emits per table = apply-wall-clock minus
# the event's source commit time (source.ts_ms). Time-based + PK-agnostic, so it is
# a far more accurate "how far behind is the target" signal than the UI's MAX(pk)
# leading-edge check. Read with Stat=Maximum (worst recent lag) over a short window.
_REPLICATION_LAG_METRIC = "ReplicationLagMs"
# ReplicationLagMs is EVENT-DRIVEN: the sink emits a datapoint only when it applies
# an event. When the source is quiesced and the pipeline drains, emission stops -- so
# the newest datapoint still inside the read window is the LAST applied event's lag,
# not the current state (nothing is in flight -> caught up). Treat a "current lag"
# datapoint older than this cutoff as absent, so a drained pipeline reads as caught up
# (lag drops to 0) instead of freezing at the last value until it ages out of the
# window. Sized above the metric's 1-min resolution + CloudWatch ingestion delay so an
# actively-streaming pipeline (which emits every offset-commit) is never falsely idled.
_LAG_FRESHNESS_SECONDS = 180

# TTL for the per-(stack, metric) Table-dimension discovery cache. Every CDC poll
# (~5 s) reads applied-ops (3 metrics) + per-table lag + the lag trend, each of which
# calls _list_metric_dimensions -- 5 ListMetrics-pagination passes per poll for data
# ("which tables emit this metric") that changes only when a NEW table starts
# emitting. Memoizing discovery collapses the 5 within a poll AND the re-discovery
# across polls to at most one pagination per metric per window, cutting CloudWatch
# ListMetrics calls ~6x at the 5 s poll rate (and the throttling risk with them). A
# table that newly starts emitting appears within this window; the live get_metric_data
# DATA reads are never cached (they must stay current).
_METRIC_DIM_CACHE_TTL_SECONDS = 30

# Initial DLQ look-back on the FIRST poll (before a cursor exists). CDC often runs
# headless / as a resumed stream and dead-letters rows for a while BEFORE an operator
# opens the UI and the monitor first reads the log; a 1 h look-back silently omitted
# every quarantine older than that (and once the cursor moves forward they can never be
# picked up). A wider first-read window captures a realistic late/headless attach's
# backlog; the oldest-first read + forward cursor then drain it over subsequent polls.
# Not unbounded: a per-call `limit` caps each read, and this is only the FIRST window
# (later polls advance from the cursor), so it does not re-scan history every poll.
_DLQ_INITIAL_LOOKBACK_SECONDS = 6 * 3600


def _match_metric_tables(
    requested: Sequence[str], discovered: Sequence[str]
) -> dict[str, str]:
    """Map each requested table name to a published metric ``Table`` dimension value.

    The sink emits ``Table`` schema-qualified (``db.table``); the tool's names may be
    bare or qualified. An exact match wins; otherwise the bare table name (the last
    dotted segment) must match **exactly one** published value. Ambiguous bare matches
    (the same table name under two schemas) are skipped rather than risk attributing
    another schema's rows to the wrong table — that table just falls back to the
    COUNT-based figure. Pure, so it is unit-tested without AWS.
    """
    discovered_set = set(discovered)
    by_bare: dict[str, list[str]] = {}
    for value in discovered:
        by_bare.setdefault(value.rsplit(".", 1)[-1], []).append(value)
    matched: dict[str, str] = {}
    for name in requested:
        if name in discovered_set:  # exact (cluster mode / already qualified)
            matched[name] = name
            continue
        candidates = by_bare.get(name.rsplit(".", 1)[-1], [])
        if len(candidates) == 1:  # unambiguous bare match (single-db mode)
            matched[name] = candidates[0]
        # 0 candidates (not published yet) or >1 (ambiguous) -> skip -> COUNT fallback
    return matched


class MskConnectController:
    """Read-only status + guarded stop over already-deployed MSK Connect connectors."""

    def __init__(
        self,
        region: str,
        *,
        aws_profile: Optional[str] = None,
        session: Optional[BotoSessionLike] = None,
        monotonic: Optional[Callable[[], float]] = None,
    ) -> None:
        """Bind the controller to the MSK Connect region + global AWS profile.

        ``session`` is an injection seam for tests (a fake ``boto3.Session``);
        when omitted the shared profile-aware session is built lazily on first
        use so constructing the controller never reaches AWS. ``monotonic`` is the
        clock seam for the metric-dimension discovery cache (defaults to
        :func:`time.monotonic`); tests inject a fake to exercise TTL expiry.
        """
        self._region = region
        self._aws_profile = aws_profile
        self._session = session
        self._monotonic = monotonic or time.monotonic
        # Cursor + de-dup state for incremental DLQ log reads (see dlq_errors):
        # only events newer than the last seen are returned, and an eventId set
        # guards the timestamp boundary so a record is never surfaced twice.
        self._dlq_cursor_ms: Optional[int] = None
        self._dlq_seen_ids: set[str] = set()
        # Short-TTL cache for _list_metric_dimensions, keyed on (stack, metric_name):
        # {key: (expiry_monotonic, dims)}. Collapses the 5 per-poll discovery passes
        # (and cross-poll re-discovery) so ListMetrics is not paged every 5 s for data
        # that changes only when a new table starts emitting. See the TTL constant.
        self._dim_cache: dict[tuple[str, str], tuple[float, list[str]]] = {}

    def _client(self, service_name: str) -> object:
        session = self._session or build_session(self._aws_profile)
        return session.client(service_name, region_name=self._region)

    def dlq_errors(
        self,
        log_group: str,
        *,
        now: Optional[datetime] = None,
        limit: int = 100,
        window_seconds: int = _DLQ_INITIAL_LOOKBACK_SECONDS,
    ) -> list[CdcConnectorError]:
        """Read NEW sink dead-letter (DLQ) events from the connector log group.

        MSK Connect publishes no DLQ-topic depth metric, but the custom DSQL sink
        logs every quarantined record to its CloudWatch worker log group
        (``/msk-connect/<stack>-cdc``) with the affected topic, Kafka offset and
        reason (``errors.log.include.messages=true``). This filters that group for
        dead-letter lines newer than the last read (a timestamp cursor + eventId
        set de-dup across polls) and parses each into a credential-free
        :class:`CdcConnectorError` (no row values, no SQL). Returns ``[]`` on any
        access error (fail-closed) so the monitor degrades gracefully.

        ``window_seconds`` is the FIRST-read look-back only (once a cursor exists,
        reads are incremental from it). It defaults to
        :data:`_DLQ_INITIAL_LOOKBACK_SECONDS` so a headless / late-attach run's earlier
        quarantines are captured rather than silently dropped -- see that constant.

        Intended to be called on the CDC status poll's worker thread (it does
        blocking network I/O); the caller records the returned errors into the
        single error log so the DLQ depth / per-table "Quarantined" surface
        reflects the real pipeline.
        """
        now_dt = now or datetime.now(timezone.utc)
        now_ms = int(now_dt.timestamp() * 1000)
        start_ms = (
            self._dlq_cursor_ms
            if self._dlq_cursor_ms is not None
            else now_ms - window_seconds * 1000
        )
        try:
            logs = self._client("logs")
            response = logs.filter_log_events(
                logGroupName=log_group,
                startTime=start_ms,
                filterPattern=(
                    '?"Quarantined record to DLQ" ?"Dropping unapplicable record"'
                ),
                limit=limit,
            )
            events = list(response.get("events", []) or [])
        except Exception:  # noqa: BLE001 - advisory monitoring read, never crash
            return []
        errors: list[CdcConnectorError] = []
        max_ts = self._dlq_cursor_ms or 0
        for event in events:
            message = str(event.get("message", ""))
            timestamp = event.get("timestamp")
            event_id = str(
                event.get("eventId") or f"{timestamp}:{message[:48]}"
            )
            if event_id in self._dlq_seen_ids:
                continue
            occurred_at = (
                datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                if isinstance(timestamp, (int, float))
                else None
            )
            record = parse_dlq_log_message(message, occurred_at=occurred_at)
            self._dlq_seen_ids.add(event_id)
            if record is not None:
                errors.append(record)
            if isinstance(timestamp, (int, float)) and timestamp > max_ts:
                max_ts = int(timestamp)
        if max_ts:
            self._dlq_cursor_ms = max_ts + 1
        # Bound the de-dup set so a long-running session never grows it unbounded.
        if len(self._dlq_seen_ids) > 5000:
            self._dlq_seen_ids = set(list(self._dlq_seen_ids)[-2000:])
        return errors

    # -- read-only ----------------------------------------------------------

    def list_connectors(self) -> list[dict]:
        """Return the raw ``connectors`` list from ``kafkaconnect:ListConnectors``.

        Empty list on any access error (fail-closed).
        """
        try:
            client = self._client("kafkaconnect")
            response = client.list_connectors()
            return list(response.get("connectors", []) or [])
        except Exception:  # noqa: BLE001 - treated as "no connectors visible"
            return []

    def describe_connector(self, connector_arn: str) -> Optional[dict]:
        """Return the raw ``describe_connector`` response, or ``None`` on error."""
        try:
            client = self._client("kafkaconnect")
            return client.describe_connector(connectorArn=connector_arn)
        except Exception:  # noqa: BLE001 - treated as "not describable"
            return None

    def connector_statuses(
        self, connector_names: Sequence[str]
    ) -> list[ConnectorStatus]:
        """Return :class:`ConnectorStatus` for the named connectors.

        Lists all connectors once, filters by name, and maps each
        ``connectorState`` to a coarse :class:`ConnectorState`. Unknown states map
        to ``UNASSIGNED``. Returns ``[]`` when ``connector_names`` is empty (zero
        AWS calls) or on any access error.
        """
        wanted = {name for name in connector_names if name}
        if not wanted:
            return []
        statuses: list[ConnectorStatus] = []
        for connector in self.list_connectors():
            name = connector.get("connectorName")
            if name not in wanted:
                continue
            raw_state = str(connector.get("connectorState", "")).upper()
            state = _CONNECTOR_STATE_MAP.get(raw_state, ConnectorState.UNASSIGNED)
            statuses.append(ConnectorStatus(name=name, state=state))
        return statuses

    def connector_health(
        self, connector_names: Sequence[str], *, window_seconds: int = 300
    ) -> dict[str, "ConnectorHealth"]:
        """Return CloudWatch task-health metrics per connector.

        Reads ``RunningTaskCount``, ``ErroredTaskCount`` and ``SinkRecordSendRate``
        (dimension ``ConnectorName``) over the trailing ``window_seconds`` and
        takes the most recent datapoint of each. These are the health/throughput
        signals MSK Connect actually publishes -- replication *lag* is measured
        separately against the DSQL target. Returns an empty :class:`ConnectorHealth`
        for every connector on any error or missing datapoint, so the UI renders
        "unknown" rather than crashing.

        ``now`` is the system clock (a live monitoring read, not a deterministic
        build step); tests inject a fake CloudWatch client with canned datapoints.
        """
        names = [name for name in connector_names if name]
        result: dict[str, ConnectorHealth] = {name: ConnectorHealth() for name in names}
        if not names:
            return result
        metrics = [
            (_RUNNING_TASKS_METRIC, "running", "Maximum"),
            (_ERRORED_TASKS_METRIC, "errored", "Maximum"),
            (_SEND_RATE_METRIC, "send_rate", "Average"),
            (_SOURCE_POLL_RATE_METRIC, "poll_rate", "Average"),
        ]
        try:
            client = self._client("cloudwatch")
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, tuple[str, str]] = {}
            for ni, name in enumerate(names):
                for mi, (metric, _field, stat) in enumerate(metrics):
                    qid = f"m{ni}_{mi}"
                    id_map[qid] = (name, metrics[mi][1])
                    queries.append(
                        {
                            "Id": qid,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": _MSK_CONNECT_NAMESPACE,
                                    "MetricName": metric,
                                    "Dimensions": [
                                        {"Name": "ConnectorName", "Value": name}
                                    ],
                                },
                                "Period": 60,
                                "Stat": stat,
                            },
                            "ReturnData": True,
                        }
                    )
            response = client.get_metric_data(
                MetricDataQueries=queries, StartTime=start, EndTime=now
            )
            for item in response.get("MetricDataResults", []):
                name_field = id_map.get(item.get("Id"))
                values = item.get("Values", [])
                if name_field is None or not values:
                    continue
                name, field = name_field
                # get_metric_data returns newest-first by default.
                latest = float(values[0])
                health = result[name]
                if field == "running":
                    health.running_tasks = int(latest)
                elif field == "errored":
                    health.errored_tasks = int(latest)
                elif field == "send_rate":
                    health.send_rate = latest
                elif field == "poll_rate":
                    health.poll_rate = latest
        except Exception:  # noqa: BLE001 - treated as "health unknown"
            return {name: ConnectorHealth() for name in names}
        return result

    def applied_ops_by_table(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 14 * 24 * 3600,
    ) -> dict[str, dict[str, int]]:
        """Per-table applied-ops breakdown CDC has applied, from the sink's
        ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` metrics.

        Returns ``{table: {"inserts": N, "updates": N, "deletes": N}}`` (a DMS-style
        change breakdown), each summed over the trailing ``window_seconds`` — a
        lightweight, **source-scan-free** per-table signal that replaces COUNT(*)-ing
        the source in the live monitor. Unlike the old net-rows metric it makes UPDATE
        traffic visible (net = inserts - deletes hid updates entirely).

        The sink emits the ``Table`` dimension **schema-qualified** (``db.table``),
        while the tool's names can be bare or qualified, so we ``ListMetrics`` to
        discover the published dimension values (UNION across the three op metrics —
        an update-only table has no ``InsertsApplied`` dimension) then match each
        requested name to one, working in either mode.

        Best-effort: returns ``{}`` on any error / no datapoint; approximate under
        replay (a monitor, not the exact reconciliation, which is Validation). Uses
        the live clock; tests inject a fake CloudWatch client.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return {}
        result: dict[str, dict[str, int]] = {}
        try:
            client = self._client("cloudwatch")
            # 1) Discover the Table dims the sink published, UNION across the three op
            #    metrics (a table with only updates has no InsertsApplied dimension).
            discovered: set[str] = set()
            for metric in _APPLIED_OP_METRICS.values():
                discovered |= set(
                    self._list_metric_dimensions(client, stack, metric) or []
                )
            if not discovered:
                return {}
            # 2) Map each requested table name to a published dimension value.
            by_dim = _match_metric_tables(names, sorted(discovered))
            if not by_dim:
                return {}
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, tuple[str, str]] = {}  # qid -> (table name, op key)
            i = 0
            for name, dim in by_dim.items():
                for op_key, metric in _APPLIED_OP_METRICS.items():
                    qid = f"o{i}"
                    i += 1
                    id_map[qid] = (name, op_key)
                    queries.append(
                        {
                            "Id": qid,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": _CDC_METRIC_NAMESPACE,
                                    "MetricName": metric,
                                    "Dimensions": [
                                        {"Name": "Stack", "Value": stack},
                                        {"Name": "Table", "Value": dim},
                                    ],
                                },
                                # Daily Sum buckets over the window; summed below =
                                # total ops applied (avoids an over-large single Period).
                                "Period": 86400,
                                "Stat": "Sum",
                            },
                            "ReturnData": True,
                        }
                    )
            # get_metric_data caps at 500 MetricDataQueries/request and we build 3 per
            # matched table, so at >~166 tables a single call would exceed the cap and
            # raise -- which the outer except would swallow, blanking the WHOLE monitor
            # (every table, not just the overflow). Batch into <=500-query requests and
            # merge, so the per-op monitor scales to a large table set. Each query id is
            # unique across batches, so merging by (table, op) never double-counts.
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch, StartTime=start, EndTime=now
                )
                for item in response.get("MetricDataResults", []):
                    key = id_map.get(item.get("Id"))
                    values = item.get("Values", [])
                    if key is None or not values:
                        continue
                    name, op_key = key
                    bucket = result.setdefault(
                        name, {"inserts": 0, "updates": 0, "deletes": 0}
                    )
                    bucket[op_key] = int(round(sum(float(v) for v in values)))
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return {}
        return result

    def replication_lag_by_table(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 15 * 60,
    ) -> dict[str, int]:
        """Per-table end-to-end replication lag in **milliseconds**, from the sink's
        ``ReplicationLagMs`` metric (apply-wall-clock minus the event's source commit
        time). Time-based and PK-agnostic — a far more accurate replication-lag signal
        than the UI's ``MAX(pk)`` leading-edge check.

        Discovery + name matching are identical to :meth:`applied_ops_by_table` (the
        sink emits the same schema-qualified ``Table`` dimension). Read with ``Stat=Maximum``
        (worst lag) at 1-minute resolution over the trailing ``window_seconds`` and
        return the **most recent** minute's value per table (current worst lag). Idle
        tables (no recent events) have no datapoint and are simply absent — the stream
        is caught up.

        Because the metric is event-driven (a datapoint is emitted only when the sink
        applies an event), a drained pipeline stops emitting: the newest datapoint left
        in the window is then the LAST applied event's lag, which is stale, not the
        current state. So a "most recent" datapoint older than ``_LAG_FRESHNESS_SECONDS``
        is treated as absent — the table reads as caught up (lag drops) once the source
        is quiesced, instead of freezing at the last value until it ages out of the
        window. Best-effort: ``{}`` on any error / no (fresh) datapoint.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return {}
        result: dict[str, int] = {}
        try:
            client = self._client("cloudwatch")
            discovered = self._list_metric_dimensions(
                client, stack, _REPLICATION_LAG_METRIC
            )
            if not discovered:
                return {}
            by_dim = _match_metric_tables(names, discovered)
            if not by_dim:
                return {}
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, str] = {}
            for i, (name, dim) in enumerate(by_dim.items()):
                qid = f"l{i}"
                id_map[qid] = name
                queries.append(
                    {
                        "Id": qid,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": _CDC_METRIC_NAMESPACE,
                                "MetricName": _REPLICATION_LAG_METRIC,
                                "Dimensions": [
                                    {"Name": "Stack", "Value": stack},
                                    {"Name": "Table", "Value": dim},
                                ],
                            },
                            # 1-minute worst-lag buckets; the newest bucket is the
                            # current lag. Maximum (not Average) so a lag spike shows.
                            "Period": 60,
                            "Stat": "Maximum",
                        },
                        "ReturnData": True,
                    }
                )
            fresh_cutoff = now.timestamp() - _LAG_FRESHNESS_SECONDS
            # get_metric_data caps at 500 MetricDataQueries/request (one per matched
            # table here), so at >500 tables a single call would exceed the cap and
            # raise -- which the outer except would swallow, blanking the WHOLE lag
            # surface (every table, not just the overflow). Batch into <=500-query
            # requests and merge by id, mirroring applied_ops_by_table, so the lag
            # monitor scales to a large table set. TimestampDescending -> Values[0]
            # is the most recent minute.
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch,
                    StartTime=start,
                    EndTime=now,
                    ScanBy="TimestampDescending",
                )
                for item in response.get("MetricDataResults", []):
                    name = id_map.get(item.get("Id"))
                    values = item.get("Values", [])
                    if name is None or not values:
                        continue
                    # Drop a stale "current lag": if the newest datapoint predates the
                    # freshness cutoff, the pipeline has drained (no recent applies) ->
                    # the table is caught up, so omit it rather than report the frozen
                    # last-event lag. Timestamps[0] aligns with Values[0]
                    # (TimestampDescending). When timestamps are absent (defensive /
                    # older shape), keep the value.
                    timestamps = item.get("Timestamps") or []
                    if timestamps:
                        newest = timestamps[0]
                        newest_ts = (
                            newest.timestamp() if hasattr(newest, "timestamp")
                            else float(newest)
                        )
                        if newest_ts < fresh_cutoff:
                            continue
                    result[name] = int(round(float(values[0])))
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return {}
        return result

    def replication_lag_series(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 15 * 60,
    ) -> list[tuple[int, int]]:
        """Pipeline-wide replication-lag TIME SERIES for the trend chart.

        Same ``ReplicationLagMs`` metric / dimensions / read as
        :meth:`replication_lag_by_table`, but keeps the WHOLE trailing
        ``window_seconds`` (not just the newest 1-minute bucket) and collapses the
        per-table series into ONE worst-case line -- the MAX per-table lag per bucket
        -- answering "is the pipeline catching up or falling behind?" for the cutover
        decision. Returns ``[(epoch_seconds, max_lag_ms), ...]`` ascending by time;
        buckets with no datapoint on any table are simply absent (the stream is caught
        up there). The datapoints are already fetched by the per-table read but
        discarded; this keeps them. Best-effort: ``[]`` on any error / no data.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return []
        try:
            client = self._client("cloudwatch")
            discovered = self._list_metric_dimensions(
                client, stack, _REPLICATION_LAG_METRIC
            )
            if not discovered:
                return []
            by_dim = _match_metric_tables(names, discovered)
            if not by_dim:
                return []
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = [
                {
                    "Id": f"l{i}",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": _CDC_METRIC_NAMESPACE,
                            "MetricName": _REPLICATION_LAG_METRIC,
                            "Dimensions": [
                                {"Name": "Stack", "Value": stack},
                                {"Name": "Table", "Value": dim},
                            ],
                        },
                        "Period": 60,
                        "Stat": "Maximum",
                    },
                    "ReturnData": True,
                }
                for i, (_name, dim) in enumerate(by_dim.items())
            ]
            # Collapse every table's per-minute series into one worst-case line:
            # MAX lag across tables per timestamp bucket. get_metric_data caps at 500
            # MetricDataQueries/request (one per matched table here), so at >500 tables
            # a single call would raise -- which the outer except would swallow,
            # blanking the WHOLE trend chart. Batch into <=500-query requests and
            # accumulate by_bucket across batches (MAX is associative, so merging
            # batches is correct), mirroring applied_ops_by_table.
            by_bucket: dict[int, float] = {}
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch,
                    StartTime=start,
                    EndTime=now,
                    ScanBy="TimestampAscending",
                )
                for item in response.get("MetricDataResults", []):
                    timestamps = item.get("Timestamps", []) or []
                    values = item.get("Values", []) or []
                    for ts, val in zip(timestamps, values):
                        epoch = (
                            int(ts.timestamp()) if hasattr(ts, "timestamp") else int(ts)
                        )
                        fval = float(val)
                        if epoch not in by_bucket or fval > by_bucket[epoch]:
                            by_bucket[epoch] = fval
            return [(epoch, int(round(by_bucket[epoch]))) for epoch in sorted(by_bucket)]
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return []

    def _list_metric_dimensions(
        self, client: object, stack: str, metric_name: str
    ) -> list[str]:
        """Discover the ``Table`` dimension values the sink published for ``stack``
        under ``metric_name`` (e.g. ``InsertsApplied`` or ``ReplicationLagMs``).

        Filters ``ListMetrics`` to that metric with this ``Stack`` dimension and returns
        each metric's ``Table`` value. Paginates over ``NextToken`` with a bounded loop
        (500 metrics/page) so a large schema is covered without an unbounded call chain.

        Memoized per ``(stack, metric_name)`` for ``_METRIC_DIM_CACHE_TTL_SECONDS`` so
        the 5 discovery passes a single CDC poll makes (3 op metrics + 2 lag readers),
        and the re-discovery every poll, collapse to one pagination per metric per
        window -- discovery ("which tables emit this metric") changes only when a new
        table starts emitting, and the live get_metric_data DATA reads are never cached.
        Only successful reads are cached; an error propagates (uncached) to the caller's
        best-effort handler, so a transient failure is retried on the next poll.
        """
        key = (stack, metric_name)
        now = self._monotonic()
        cached = self._dim_cache.get(key)
        if cached is not None and now < cached[0]:
            return list(cached[1])
        tables: list[str] = []
        token: Optional[str] = None
        for _ in range(20):  # 500 metrics/page -> up to 10k tables; a hard cap
            kwargs: dict = {
                "Namespace": _CDC_METRIC_NAMESPACE,
                "MetricName": metric_name,
                "Dimensions": [{"Name": "Stack", "Value": stack}],
            }
            if token:
                kwargs["NextToken"] = token
            resp = client.list_metrics(**kwargs)  # type: ignore[attr-defined]
            for metric in resp.get("Metrics", []):
                for dim in metric.get("Dimensions", []):
                    if dim.get("Name") == "Table" and dim.get("Value"):
                        tables.append(dim["Value"])
            token = resp.get("NextToken")
            if not token:
                break
        # Cache only a fully-read result (the loop above completed without raising).
        self._dim_cache[key] = (now + _METRIC_DIM_CACHE_TTL_SECONDS, list(tables))
        return tables

    # -- guarded mutation ---------------------------------------------------

    def delete_connector(self, connector_arn: str, current_version: str) -> bool:
        """Delete (stop) a connector. Returns ``True`` on success, ``False`` on error.

        DESTRUCTIVE and irreversible: MSK Connect has no pause/resume, so stopping
        a connector means deleting it. This method has NO confirmation of its own;
        the UI caller MUST gate it behind an explicit type-the-name dialog.
        """
        try:
            client = self._client("kafkaconnect")
            client.delete_connector(
                connectorArn=connector_arn, currentVersion=current_version
            )
            return True
        except Exception:  # noqa: BLE001 - surfaced to the caller as failure
            return False


def build_msk_connect_controller(
    region: str, *, aws_profile: Optional[str] = None
) -> MskConnectController:
    """Build a profile-aware controller for the given MSK Connect region."""
    return MskConnectController(region, aws_profile=aws_profile)


def target_lag_seconds(
    cursor: object, qualified_table: str, ts_column: str, *, now: datetime
) -> Optional[float]:
    """Return end-to-end replication lag (seconds) from the DSQL target.

    Computes ``now - max(ts_column)`` for ``qualified_table`` on the target: the
    most recently applied row's source timestamp tells how far behind the sink
    is, end-to-end (the most accurate lag signal, and free of any CloudWatch /
    cdc-stack dependency). ``cursor`` is an open DB-API cursor on the DSQL
    target. Returns ``None`` when the table is empty, the max is NULL, or any
    query error occurs (UI shows "lag unknown").

    The table/column names are quoted by splitting on the schema dot; they come
    from the tool's own inventory (not user free-text), so this is safe for the
    monitoring read. ``now`` is passed in so the computation is deterministic and
    unit-testable.
    """
    parts = qualified_table.split(".")
    quoted_table = ".".join('"' + p.replace('"', '""') + '"' for p in parts)
    quoted_col = '"' + ts_column.replace('"', '""') + '"'
    try:
        cursor.execute(f"SELECT max({quoted_col}) FROM {quoted_table}")  # type: ignore[attr-defined]
        row = cursor.fetchone()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - treated as "lag unknown"
        return None
    if not row or row[0] is None:
        return None
    max_ts = row[0]
    if not isinstance(max_ts, datetime):
        return None
    # Normalize naive timestamps to UTC so the subtraction is well-defined.
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    reference = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    delta = (reference - max_ts).total_seconds()
    return max(0.0, delta)


__all__ = [
    "ConnectorHealth",
    "MskConnectController",
    "build_msk_connect_controller",
    "target_lag_seconds",
]
