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

# The reason may carry the sink's rendered SQL TEMPLATE (column names + `?`
# placeholders, never values), so allow a longer message than a bare error string
# while still bounding it so a pathological line can't bloat the error log.
_MAX_MESSAGE_LEN = 2000


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
    code = _SQLSTATE.search(message)
    surfaced = f"DLQ offset={match.group('offset')}: {reason}"
    return CdcConnectorError(
        table=_table_from_topic(match.group("topic")),
        message=surfaced[:_MAX_MESSAGE_LEN],
        error_code=code.group("state") if code else None,
        occurred_at=occurred_at,
    )
