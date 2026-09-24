# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Parse MSK Connect sink dead-letter (DLQ) log lines into error records.

The custom DSQL sink (``connectors/dsql-sink``) logs each permanently-rejected
record to the connector's CloudWatch worker log group as one of::

    Quarantined record to DLQ (topic=<t>, partition=<p>, offset=<o>): <reason>
    Dropping unapplicable record (no DLQ configured) topic=<t>, partition=<p>, offset=<o>: <reason>

This module turns those lines into credential-free
:class:`~dsql_migrator.core.cdc.CdcConnectorError` records (affected table from
the topic, the failure reason, the Kafka offset, and an optional SQLSTATE). On a
DSQL apply failure the sink appends the rendered SQL **template** to the reason
(``... | sql: INSERT INTO ... VALUES (?, ?) ON CONFLICT ...``): column names with
``?`` placeholders only, so it still carries **no row values and no credentials**
(Property 7) -- there is also no *source* SQL since CDC is row-based.

The reason may also carry the failed row's **primary key** (``... | pk: id=14``)
so an engineer can locate the exact source row to fix (a quarantined event is
never retried automatically -- see ``DsqlSinkTask``). PK **column names** are
always included; PK **values** only for surrogate keys (integer / UUID). A natural
key value that may be sensitive (e.g. an email or account-number PK) is withheld
(``email=<withheld>``), so this still carries no arbitrary row values (Property 7).

The result feeds the single downloadable error log and the UI's DLQ depth /
per-table "Quarantined" surface.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

from dsql_migrator.core.cdc import CdcConnectorError

# One regex handles both the "Quarantined record to DLQ (...)" and the
# "Dropping unapplicable record (...) ..." forms: the ``)`` before the colon is
# optional so both shapes match. ``topic`` stops at the first comma/space/paren.
_DLQ_LINE = re.compile(
    r"(?:Quarantined record to DLQ|Dropping unapplicable record)"
    r".*?topic=(?P<topic>[^,\s)]+)"
    r".*?partition=(?P<partition>\d+)"
    r".*?offset=(?P<offset>\d+)\)?:\s*(?P<reason>.*)",
    re.IGNORECASE | re.DOTALL,
)
# Extract a SQLSTATE-like code (e.g. "sqlstate=42804") when the sink included it.
_SQLSTATE = re.compile(r"sqlstate[=:\s]+(?P<state>[0-9A-Za-z]{5})", re.IGNORECASE)

# The failed row's PRIMARY KEY, which the sink appends as " | pk: id=1". It was left buried
# in the message text, so an operator had to regex the tool's own JSON to get it. Stops at the
# next " | " section (the sink may append " | op: ..." and " | sql: ...") and at the log4j
# suffix below, so it never swallows either.
_PK = re.compile(r"\|\s*pk:\s*(?P<pk>.+?)(?=\s*\||\s*\([\w.$]+:\d+\)\s*$|$)")

# The DML operation the sink now tags (plugin v43): " | op: d", " | op: c/r", " | op: u",
# " | op: d (tombstone)". THE field that decides the recovery, because CDC replicates state: a
# quarantined INSERT or UPDATE converges by re-reading the source's current row, while a
# quarantined DELETE cannot -- the row is gone from the source, so no source query can reveal
# that the target still holds it. Absent on a record produced by an older sink, in which case
# it stays None rather than being guessed.
_OP = re.compile(r"\|\s*op:\s*(?P<op>[a-z](?:/[a-z])?)(?P<tombstone>\s*\(tombstone\))?")

# log4j's own "(logger:line)" tail. The MSK Connect worker's layout is AWS-managed and appends
# %c:%L to every line, so "(dev.dsqlmigrator.connect.DsqlSinkTask:759)" was landing inside the
# operator-facing message. It cannot be turned off at the source; strip it here.
_LOG4J_TAIL = re.compile(r"\s*\([\w.$]+:\d+\)\s*$")

# A STABLE code for the quarantines that carry no SQLSTATE, so the UI can branch on the error
# CLASS instead of matching prose. The sink's pre-write size guard raises a DataException --
# there is no server error and therefore no SQLSTATE to report (asserted in tests/test_cdc_dlq
# as intended behaviour), yet it is the most common quarantine in practice and the one with a
# specific remedy. Deliberately NOT a 5-character SQLSTATE shape: classify_schema_drift() keys
# the source-schema-drift banner off error_code, and a synthetic code must never be mistaken
# for one of the SQLSTATEs it maps.
OVERSIZED_VALUE_CODE = "OVERSIZED_VALUE"
_OVERSIZED = re.compile(r"exceeds DSQL's\s+\d+-byte limit", re.IGNORECASE)

# The reason may carry the sink's rendered SQL TEMPLATE (column names + `?`
# placeholders, never values), so allow a longer message than a bare error string
# while still bounding it so a pathological line can't bloat the error log.
_MAX_MESSAGE_LEN = 2000


# The offending column, which the sink names verbatim: "Value for column 'content' exceeds
# DSQL's 1048576-byte limit". This is BETTER evidence than the Full Load has for the same
# recovery -- there, the driver message names the TYPE and not the column, so the picker has to
# infer candidates from a type token (see preselect_lob_columns_for_reason). Here the column is
# stated, so the exclusion picker can pre-tick exactly the right one.
_OVERSIZED_COLUMN = re.compile(r"Value for column '(?P<column>[^']+)'", re.IGNORECASE)


def oversized_column_from_reason(message: Optional[str]) -> Optional[str]:
    """Return the column a quarantine reason blames for exceeding the per-value limit. Pure.

    ``None`` when the message is not an oversized-value quarantine (or a future sink wording
    stops naming the column), so the caller falls back to making the operator choose rather
    than pre-ticking a guess.
    """
    if not message:
        return None
    match = _OVERSIZED_COLUMN.search(message)
    return match.group("column") if match else None


def _table_from_topic(topic: str) -> str:
    """Return the ``db.table`` identity from a ``<prefix>.<db>.<table>`` Kafka topic.

    The db-QUALIFIED name (not the bare table) is what the rest of the tool keys a
    table on: the CloudWatch monitor's ``Table`` dimension, the connector's
    ``table.include.list``, and the target's schema-qualified table (a source
    ``db.table`` maps to a DSQL table in schema ``db``). Returning the bare table
    here made the DLQ per-table surface inconsistent with those AND broke the
    ADD COLUMN drift recovery, whose ``information_schema`` reads need ``db.table``
    (a bare name splits to ``schema=<table>, name=""`` and matches zero rows).

    The db + table are always the LAST two dot-segments regardless of how many
    segments the prefix itself has, so take those. Falls back to the whole topic
    when it is not dotted (so the record still surfaces under a stable, non-empty
    key rather than being dropped).
    """
    parts = [segment for segment in topic.split(".") if segment]
    if len(parts) >= 2:
        return f"{parts[-2]}.{parts[-1]}"
    return parts[-1] if parts else topic


def parse_dlq_log_message(
    message: Optional[str], *, occurred_at: Optional[datetime] = None
) -> Optional[CdcConnectorError]:
    """Parse one sink DLQ log line into a :class:`CdcConnectorError`.

    Returns ``None`` for a line that is not a recognizable dead-letter message,
    so non-DLQ log noise is ignored. The ``message`` kept on the record includes
    the Kafka offset for traceability and the sink's reason, truncated to a sane
    length. The reason may carry the sink's rendered SQL TEMPLATE (``... | sql:
    INSERT INTO ... VALUES (?, ?) ON CONFLICT ...``) -- column names with ``?``
    placeholders only -- and/or the failed row's primary key (``... | pk: id=14``,
    column names always, surrogate values only, natural-key values withheld). Both
    ride through this parser inside the reason with no special handling.

    On row values (Property 7): the sink strips the server's ``DETAIL`` field from the
    driver message before logging (``DsqlSinkTask.safeCauseMessage``, plugin v29),
    because pgjdbc appends it to ``getMessage()`` and DETAIL is the FAILING ROW for a
    not-null violation. Before v29 a row could arrive here. What can still appear is a
    single offending literal for the few SQLSTATEs where the server puts it in the
    PRIMARY message (``22P02: invalid input syntax for type integer: "abc"``), so this
    parser bounds the message length rather than assuming it is value-free.
    """
    if not message:
        return None
    match = _DLQ_LINE.search(message)
    if match is None:
        return None
    reason = (match.group("reason") or "").strip() or "quarantined to DLQ"
    reason = _LOG4J_TAIL.sub("", reason).strip() or "quarantined to DLQ"
    code = _SQLSTATE.search(message)
    # A synthetic class only when the sink reported no SQLSTATE: a real one always wins, so
    # this can never mask a server error.
    error_code = code.group("state") if code else None
    if error_code is None and _OVERSIZED.search(reason):
        error_code = OVERSIZED_VALUE_CODE
    pk_match = _PK.search(reason)
    op_match = _OP.search(reason)
    op = None
    if op_match is not None:
        op = op_match.group("op")
        if op_match.group("tombstone"):
            op = f"{op} (tombstone)"
    surfaced = f"DLQ offset={match.group('offset')}: {reason}"
    return CdcConnectorError(
        table=_table_from_topic(match.group("topic")),
        message=surfaced[:_MAX_MESSAGE_LEN],
        error_code=error_code,
        # Promoted OUT of the message into structured fields. Both stay in the message too --
        # the message is what the downloadable error log renders, and truncating history to
        # move a field would lose information.
        pk=pk_match.group("pk").strip() if pk_match else None,
        op=op,
        # Kafka coordinates, parsed since the first version of this regex and then discarded.
        # They are how an operator finds the ORIGINAL record (key + value + Connect context
        # headers) on the dead-letter topic, which for a quarantined DELETE is the only place
        # the row still exists at all -- the source no longer has it.
        topic=match.group("topic"),
        partition=int(match.group("partition")),
        offset=int(match.group("offset")),
        occurred_at=occurred_at,
    )
