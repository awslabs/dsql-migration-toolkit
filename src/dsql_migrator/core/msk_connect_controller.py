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

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Sequence

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


class MskConnectController:
    """Read-only status + guarded stop over already-deployed MSK Connect connectors."""

    def __init__(
        self,
        region: str,
        *,
        aws_profile: Optional[str] = None,
        session: Optional[BotoSessionLike] = None,
    ) -> None:
        """Bind the controller to the MSK Connect region + global AWS profile.

        ``session`` is an injection seam for tests (a fake ``boto3.Session``);
        when omitted the shared profile-aware session is built lazily on first
        use so constructing the controller never reaches AWS.
        """
        self._region = region
        self._aws_profile = aws_profile
        self._session = session
        # Cursor + de-dup state for incremental DLQ log reads (see dlq_errors):
        # only events newer than the last seen are returned, and an eventId set
        # guards the timestamp boundary so a record is never surfaced twice.
        self._dlq_cursor_ms: Optional[int] = None
        self._dlq_seen_ids: set[str] = set()

    def _client(self, service_name: str) -> object:
        session = self._session or build_session(self._aws_profile)
        return session.client(service_name, region_name=self._region)

    def dlq_errors(
        self,
        log_group: str,
        *,
        now: Optional[datetime] = None,
        limit: int = 100,
        window_seconds: int = 3600,
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
