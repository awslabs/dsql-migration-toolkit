"""G25 (sink plugin v53, rebuilt in place): cross-language guards for the sink's new log lines.

The sink now logs one INFO line per offset commit with the transactions Aurora DSQL asked it to
retry, and closes every logged unique-index hold with a line (replaced by a later change of its
row, or dropped on a revoke). The tool reads the connector log only for dead letters (the
CloudWatch filter and ``parse_dlq_log_message``), for value-conversion failures, and tells the
operator to grep the RetriableException text for a stall. None of those lines may be mistaken
for any of them: a retry or a hold is not a dead letter and not a stall.

One new WARN is the exception on purpose: a held change's try that Aurora DSQL does not answer
no longer fails the put, so Connect no longer logs its RetriableException, and the sink logs the
failure itself with the same leading text -- the one the stall notice and the manual name. It is
still never read as a dead letter.
"""

from __future__ import annotations

import pathlib
import re

from dsql_migrator.core.cdc_dlq import parse_dlq_log_message
from dsql_migrator.core.cdc_value_conversion import VALUE_CONVERSION_FILTER_PATTERN

_SINK = (
    pathlib.Path(__file__).resolve().parents[1]
    / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
)


def _java_constant(name: str) -> str:
    """The value of a ``static final String`` of DsqlSinkTask, its literals joined."""
    src = _SINK.read_text(encoding="utf-8")
    match = re.search(rf"static final String {name}\s*=\s*(.*?);\n", src, re.DOTALL)
    assert match, name
    return "".join(re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1)))


def _rendered(template: str) -> str:
    """The line as logged, every placeholder filled (counts, a table, a partition, ...)."""
    parts = template.split("{}")
    values = ["app.customers", "3", "42", "uk_email", "7"]
    out = parts[0]
    for i, part in enumerate(parts[1:]):
        out += values[i % len(values)] + part
    return out


def test_g25_the_new_sink_lines_are_never_read_as_a_dead_letter_or_a_stall() -> None:
    stall_prefix = "DSQL apply failed (transient sqlstate="
    dlq_terms = ("Quarantined record to DLQ", "Dropping unapplicable record")
    conversion = VALUE_CONVERSION_FILTER_PATTERN.strip('"')
    for name in ("RETRY_SUMMARY", "UNIQUE_HOLD_REPLACED", "UNIQUE_HOLD_DROPPED"):
        template = _java_constant(name)
        assert template, name
        line = _rendered(template)
        assert parse_dlq_log_message(line) is None, name
        for term in (*dlq_terms, conversion, stall_prefix):
            assert term not in line, (name, term)
        assert "sqlstate=" not in line.lower(), name
    # The stall notice still finds the sink's own RetriableException text.
    assert f'"{stall_prefix}" + e.getSQLState()' in _SINK.read_text(encoding="utf-8")


def test_g25_a_held_changes_failed_try_is_a_transient_apply_failure_not_a_dead_letter() -> None:
    stall_prefix = "DSQL apply failed (transient sqlstate="
    template = _java_constant("UNIQUE_HOLD_TRY_FAILED")
    # SQLSTATE, table, partition, offset and the next wait: no value, no driver message.
    assert template.count("{}") == 5, template
    parts = template.split("{}")
    values = ["08001", "app.customers", "1", "10", "500"]
    line = parts[0] + "".join(v + p for v, p in zip(values, parts[1:]))
    assert line.startswith(f"{stall_prefix}08001); "), line
    assert parse_dlq_log_message(line) is None
    for term in ("Quarantined record to DLQ", "Dropping unapplicable record"):
        assert term not in line, term
    assert VALUE_CONVERSION_FILTER_PATTERN.strip('"') not in line


def test_g25_the_retry_summary_carries_counts_and_sqlstate_codes_only() -> None:
    template = _java_constant("RETRY_SUMMARY")
    assert template.count("{}") == 3, template
    assert "40001" in template and "XX000" in template, template
