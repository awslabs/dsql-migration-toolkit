"""B2 (JS-1): the sink's CloudWatch metrics, their reader and their IAM grant agree, and the
quarantine line the sink logs is the one the dead-letter read selects and parses.

Three files spell the CDC monitor metric contract by hand. DsqlSinkTask.java emits
InsertsApplied / UpdatesApplied / DeletesApplied / ReplicationLagMs under the namespace
MysqlDsqlMigrator/CDC with the dimensions Stack + Table. core/msk_connect_controller reads them
back. deploy/cdc-stack/cdc-stack.yaml grants cloudwatch:PutMetricData only under that
namespace. If one side is renamed alone, IAM refuses the sink's PutMetricData (the sink logs a
WARN and drops the window) or the UI reads an empty namespace. Either way the CDC activity
columns go blank and nothing failed.

The dead-letter read is in the same state: it filters the connector log for, and parses, the
line DsqlSinkTask.quarantine() logs, and was tested only with hand-written lines.

These tests read the names from the Java source (no plugin rebuild is needed) and fail on the
first one-sided change.
"""

from __future__ import annotations

import pathlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from dsql_migrator.core import msk_connect_controller as mcc
from dsql_migrator.core.msk_connect_controller import MskConnectController

yaml = pytest.importorskip("yaml")

_ROOT = pathlib.Path(__file__).resolve().parents[1]
_CONNECT = _ROOT / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect"
_SINK_TASK = _CONNECT / "DsqlSinkTask.java"
_SINK_CONFIG = _CONNECT / "DsqlSinkConnectorConfig.java"
_CDC_STACK = _ROOT / "deploy/cdc-stack/cdc-stack.yaml"
_SINK_CLASS = "dev.dsqlmigrator.connect.DsqlSinkConnector"

_LITERAL = re.compile(r'"((?:[^"\\]|\\.)*)"')

_STACK = "dsql-cdc-orders"
_TABLE = "app.orders"


def _joined(expression: str) -> str:
    """A Java string expression's literals, joined (``"a" + "b"`` -> ``ab``)."""
    return "".join(_LITERAL.findall(expression))


def _sink_task() -> str:
    return _SINK_TASK.read_text(encoding="utf-8")


def _sink_metrics() -> dict[str, Any]:
    """What DsqlSinkTask emits: its namespace, each counter's metric, the lag metric, and the
    dimensions of every datum as (name, Java value expression) pairs."""
    src = _sink_task()
    constants = {
        m.group(1): _joined(m.group(2))
        for m in re.finditer(r"static final String (METRIC_\w+)\s*=\s*(.*?);\n", src, re.DOTALL)
    }
    # One PutMetricData request, under one namespace.
    assert re.findall(r"\.namespace\((\w+)\)", src) == ["METRIC_NAMESPACE"]
    # Each counter is emitted under its constant: emitOpCounter(data, insertsByTable, ...).
    counters = re.findall(r"emitOpCounter\(data, (\w+)ByTable, (METRIC_\w+)\);", src)
    assert counters, "no emitOpCounter call found"
    # Every datum: its metricName argument and its dimensions.
    datums = re.findall(
        r"MetricDatum\.builder\(\)\s*\.metricName\((\w+)\)\s*\.dimensions\((.*?)\)\n\s*\.value\(",
        src,
        re.DOTALL,
    )
    assert len(datums) == src.count("MetricDatum.builder()") == 2, datums
    dimension = re.compile(r'Dimension\.builder\(\)\.name\("([^"]+)"\)\.value\((.*?)\)\.build\(\)')
    by_metric = {name: dimension.findall(dims) for name, dims in datums}
    assert sum(len(d) for d in by_metric.values()) == src.count("Dimension.builder()")
    # The counters' datum takes emitOpCounter's metricName parameter; the other is the lag.
    assert set(by_metric) == {"metricName", "METRIC_REPLICATION_LAG"}, set(by_metric)
    assert by_metric["metricName"] == by_metric["METRIC_REPLICATION_LAG"], by_metric
    # A METRIC_ constant the sink adds must be read (or removed): none is left unclassified.
    used = {"METRIC_NAMESPACE", "METRIC_REPLICATION_LAG", *(c for _, c in counters)}
    assert set(constants) == used, sorted(set(constants) ^ used)
    return {
        "namespace": constants["METRIC_NAMESPACE"],
        "counters": {kind: constants[c] for kind, c in counters},
        "lag": constants["METRIC_REPLICATION_LAG"],
        "dimensions": by_metric["metricName"],
    }


# ---------------------------------------------------------------------------
# 1. The metric names, namespace and dimensions
# ---------------------------------------------------------------------------


def test_the_controller_constants_are_the_names_the_sink_emits() -> None:
    sink = _sink_metrics()
    assert mcc._CDC_METRIC_NAMESPACE == sink["namespace"]
    assert mcc._APPLIED_OP_METRICS == sink["counters"]
    assert mcc._REPLICATION_LAG_METRIC == sink["lag"]
    # Stack is the cdc-stack name, Table the event's db.table.
    assert sink["dimensions"] == [("Stack", "metricsStack"), ("Table", "e.getKey()")]


class _CloudWatch:
    """A CloudWatch stand-in holding what the sink published, matched the way CloudWatch
    matches: ListMetrics by namespace, metric name and a subset of the dimensions;
    GetMetricData only by namespace, metric name and the exact dimension set."""

    def __init__(self, published: list[dict], values: dict[str, float]) -> None:
        self._published = published
        self._values = values
        self.calls: list[tuple[str, dict]] = []

    @staticmethod
    def _dims(dimensions: list[dict]) -> set[tuple[str, str]]:
        return {(d["Name"], d["Value"]) for d in dimensions}

    def list_metrics(self, **kwargs: Any) -> dict:
        self.calls.append(("list_metrics", kwargs))
        wanted = self._dims(kwargs.get("Dimensions", []))
        return {
            "Metrics": [
                m
                for m in self._published
                if m["Namespace"] == kwargs.get("Namespace")
                and m["MetricName"] == kwargs.get("MetricName")
                and wanted <= self._dims(m["Dimensions"])
            ]
        }

    def get_metric_data(self, **kwargs: Any) -> dict:
        self.calls.append(("get_metric_data", kwargs))
        results = []
        for query in kwargs["MetricDataQueries"]:
            metric = query["MetricStat"]["Metric"]
            if any(
                m["Namespace"] == metric["Namespace"]
                and m["MetricName"] == metric["MetricName"]
                and self._dims(m["Dimensions"]) == self._dims(metric["Dimensions"])
                for m in self._published
            ):
                results.append(
                    {
                        "Id": query["Id"],
                        "Values": [self._values[metric["MetricName"]]],
                        "Timestamps": [kwargs["EndTime"] - timedelta(seconds=30)],
                    }
                )
        return {"MetricDataResults": results}


class _Session:
    def __init__(self, client: object) -> None:
        self._client = client

    def client(self, service_name: str, **kwargs: Any) -> object:
        return self._client


def test_the_reader_finds_the_metrics_the_sink_publishes() -> None:
    sink = _sink_metrics()
    value_of = {"metricsStack": _STACK, "e.getKey()": _TABLE}
    dimensions = [{"Name": name, "Value": value_of[expr]} for name, expr in sink["dimensions"]]
    names = [*sink["counters"].values(), sink["lag"]]
    published = [
        {"Namespace": sink["namespace"], "MetricName": name, "Dimensions": dimensions}
        for name in names
    ]
    counts = {"inserts": 3.0, "updates": 2.0, "deletes": 1.0}
    values = {sink["counters"][kind]: count for kind, count in counts.items()}
    values[sink["lag"]] = 1500.0
    cloudwatch = _CloudWatch(published, values)
    controller = MskConnectController("us-east-1", session=_Session(cloudwatch))

    # FAILS ON A ONE-SIDED RENAME: a namespace, metric or dimension name the sink does not
    # publish finds nothing, and each reader returns empty.
    assert controller.applied_ops_by_table(_STACK, [_TABLE]) == {
        _TABLE: {kind: int(count) for kind, count in counts.items()}
    }
    assert controller.replication_lag_by_table(_STACK, [_TABLE]) == {_TABLE: 1500}
    series = controller.replication_lag_series(_STACK, [_TABLE])
    assert [lag for _, lag in series] == [1500], series

    # Every request names the sink's namespace and only metrics the sink emits.
    listed = [kw for call, kw in cloudwatch.calls if call == "list_metrics"]
    queried = [
        q["MetricStat"]["Metric"]
        for call, kw in cloudwatch.calls
        if call == "get_metric_data"
        for q in kw["MetricDataQueries"]
    ]
    assert {kw["MetricName"] for kw in listed} == set(names)
    assert {m["MetricName"] for m in queried} == set(names)
    assert {kw["Namespace"] for kw in listed} | {m["Namespace"] for m in queried} == {
        sink["namespace"]
    }


# ---------------------------------------------------------------------------
# 2. The IAM grant and the Stack dimension's value, in the cdc-stack
# ---------------------------------------------------------------------------


def _template() -> dict:
    # Long-form intrinsics only (Fn::If, Fn::GetAtt, Ref), so the safe loader reads it.
    return yaml.safe_load(_CDC_STACK.read_text(encoding="utf-8"))


def _sink_connectors(resources: dict) -> dict[str, dict]:
    sinks = {
        name: res["Properties"]
        for name, res in resources.items()
        if res.get("Type") == "AWS::KafkaConnect::Connector"
        and res["Properties"]["ConnectorConfiguration"].get("connector.class") == _SINK_CLASS
    }
    assert len(sinks) == 2, sorted(sinks)  # the Lambda-deployed sink and the external one
    return sinks


def _put_metric_statements(resources: dict) -> list[tuple[str, dict]]:
    """Every IAM statement of the stack that can allow cloudwatch:PutMetricData."""
    found = []
    for name, res in resources.items():
        props = res.get("Properties") or {}
        if res.get("Type") == "AWS::IAM::Role":
            documents = [p["PolicyDocument"] for p in props.get("Policies", []) or []]
        elif res.get("Type") in ("AWS::IAM::Policy", "AWS::IAM::ManagedPolicy"):
            documents = [props["PolicyDocument"]]
        else:
            continue
        for document in documents:
            for statement in document["Statement"]:
                actions = statement.get("Action", [])
                actions = [actions] if isinstance(actions, str) else actions
                if any(a in ("cloudwatch:PutMetricData", "cloudwatch:*", "*") for a in actions):
                    found.append((name, statement))
    return found


def test_the_sink_role_may_put_metrics_only_under_the_namespace_the_sink_emits() -> None:
    sink = _sink_metrics()
    resources = _template()["Resources"]
    grants = _put_metric_statements(resources)
    assert len(grants) == 1, grants
    role, statement = grants[0]
    assert statement["Effect"] == "Allow"
    assert statement["Condition"] == {
        "StringEquals": {"cloudwatch:namespace": sink["namespace"]}
    }
    # ...and it is the role both sink connectors run as.
    for name, props in _sink_connectors(resources).items():
        assert props["ServiceExecutionRoleArn"] == {"Fn::GetAtt": [role, "Arn"]}, name


def test_both_sink_connectors_set_the_stack_dimension_to_the_stack_name() -> None:
    # The reader queries Stack = the cdc-stack name; a blank metrics.stack disables emission.
    config = _SINK_CONFIG.read_text(encoding="utf-8")
    stack_key = re.search(r'String METRICS_STACK = "([^"]+)";', config).group(1)
    enabled_key = re.search(r'String METRICS_ENABLED = "([^"]+)";', config).group(1)
    assert "this.metricsStack = config.metricsStack();" in _sink_task()
    for name, props in _sink_connectors(_template()["Resources"]).items():
        configuration = props["ConnectorConfiguration"]
        assert configuration.get(stack_key) == {"Ref": "AWS::StackName"}, name
        assert str(configuration.get(enabled_key, "true")).lower() == "true", name


# ---------------------------------------------------------------------------
# 3. The quarantine line
# ---------------------------------------------------------------------------


def _quarantine_log_calls() -> list[tuple[str, list[str]]]:
    """Each log call of DsqlSinkTask.quarantine(): its format string and its arguments."""
    src = _sink_task()
    head = "private void quarantine(SinkRecord record, Exception cause, String reason) {"
    body = src[src.index(head) :]
    body = body[: body.index("\n  }\n")]
    calls = re.findall(
        r'log\.\w+\(\s*((?:"(?:[^"\\]|\\.)*"\s*\+?\s*)+),\s*(.*?)\);', body, re.DOTALL
    )
    assert calls, "no log call found in quarantine()"
    return [(_joined(fmt), [a.strip() for a in args.split(",")]) for fmt, args in calls]


def _filled(template: str, values: list[str]) -> str:
    """The line as logged: each ``{}`` of the format filled, in order, from ``values``."""
    parts = template.split("{}")
    return parts[0] + "".join(values[i % len(values)] + p for i, p in enumerate(parts[1:]))


def _reason_tags() -> dict[str, Any]:
    """The pieces quarantine reasons are built from, as the sink writes them."""
    src = _sink_task()
    assert 'return (state == null || state.isEmpty()) ? "" : "sqlstate=" + state + " ";' in src
    assert 'return formattedPk.isEmpty() ? "" : " | pk: " + formattedPk;' in src
    assert 'message + pk + op + " | sql: " + sql;' in src
    assert "quarantine(record, cause, sqlStateTag(cause) + reason);" in src
    body = src[src.index("static String opSuffix(ChangeEvent event) {") :]
    body = body[: body.index("\n  }\n")]
    ops = re.findall(r'return "( \| op: [^"]+)";', body)
    assert len(ops) == 3, ops  # delete, insert, update (a tombstone is skipped since v57)
    return {"sqlstate": "sqlstate={} ", "pk": " | pk: {}", "ops": ops, "sql": " | sql: {}"}


def _worker_line(line: str) -> str:
    # The MSK Connect worker's layout around the sink's message (AWS-managed).
    return (
        "[Worker-0a1] [2026-11-08 01:02:03,456] WARN [dsql-cdc-orders-dsql-sink|task-0] "
        f"{line} (dev.dsqlmigrator.connect.DsqlSinkTask:2455)"
    )


class _Logs:
    """CloudWatch Logs stand-in that applies the read's filter pattern to its events."""

    def __init__(self, messages: list[str], at: datetime) -> None:
        stamp = int(at.timestamp() * 1000)
        self._events = [
            {"message": m, "timestamp": stamp, "eventId": str(i)} for i, m in enumerate(messages)
        ]
        self.patterns: list[str] = []

    def filter_log_events(self, **kwargs: Any) -> dict:
        pattern = kwargs["filterPattern"]
        self.patterns.append(pattern)
        # The read's pattern is an OR of quoted terms, each matched as an exact phrase.
        terms = re.findall(r'\?"([^"]+)"', pattern)
        assert terms and pattern == " ".join(f'?"{t}"' for t in terms), pattern
        return {"events": [e for e in self._events if any(t in e["message"] for t in terms)]}


def test_the_dead_letter_read_selects_and_parses_the_line_the_sink_logs() -> None:
    calls = _quarantine_log_calls()
    quarantined = [c for c in calls if c[0].startswith("Quarantined record to DLQ")]
    assert len(quarantined) == 1, calls
    template, args = quarantined[0]
    assert template.count("{}") == 4, template
    assert args == ["record.topic()", "record.kafkaPartition()", "record.kafkaOffset()", "reason"]
    tags = _reason_tags()

    expected = []
    lines = []
    for i, op_tag in enumerate(tags["ops"]):
        reason = (
            tags["sqlstate"].format("23502")
            + 'null value in column "email" violates not-null constraint'
            + tags["pk"].format("id=14")
            + op_tag
            + tags["sql"].format('UPDATE "app"."orders" SET "email" = ? WHERE "id" = ?')
        )
        offset = str(1234 + i)
        lines.append(_worker_line(_filled(template, ["dsqlcdc.app.orders", "3", offset, reason])))
        expected.append((int(offset), op_tag.split("op: ", 1)[1], reason))
    # quarantine()'s other line (a failed DLQ produce) names the same record, which is counted
    # once, from its WARN.
    others = [
        _worker_line(_filled(fmt, ["dsqlcdc.app.orders", "3", "1234", "TimeoutException"]))
        for fmt, _args in calls
        if fmt != template
    ]

    now = datetime(2026, 11, 8, 1, 3, tzinfo=timezone.utc)
    logs = _Logs([*lines, *others], now - timedelta(minutes=1))
    errors = MskConnectController("us-east-1", session=_Session(logs)).dlq_errors(
        "/msk-connect/dsql-cdc-orders-cdc", now=now
    )

    assert logs.patterns, "the dead-letter read made no filter_log_events call"
    assert [(e.offset, e.op) for e in errors] == [(o, op) for o, op, _ in expected]
    for error, (offset, _op, reason) in zip(errors, expected):
        assert (error.topic, error.partition, error.table) == ("dsqlcdc.app.orders", 3, _TABLE)
        assert error.error_code == "23502"
        assert error.pk == "id=14"
        # The reason as the sink wrote it, without the worker's prefix or its logger suffix.
        assert error.message == f"DLQ offset={offset}: {reason}"
