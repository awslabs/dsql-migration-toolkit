# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Parse SOURCE-connector value-conversion failures out of the connector log group.

A Debezium source connector that cannot convert ONE column's value does not reject the
record: it logs the column at ERROR and delivers that field as **NULL**, so the row
LANDS on the target with the column empty. Read from the shipped jar rather than the
docs -- ``debezium-core-2.7.4.Final.jar`` (byte-identical in BOTH bundled plugins, md5
``ac4ad9dd15a7a41b2262e624bb4fb752``), ``javap -c
io/debezium/relational/TableSchemaBuilder``: the converter call sits in a catch that
loads ``Failed to properly convert data value for '{}.{}' of type {}`` and, with the
shipped default handling mode (the field is null), hands it to
``Loggings.logErrorAndTraceRecord``; the field is never ``Struct.put``, so it stays
null. Nothing is dead-lettered, so this is invisible to EVERY count-based signal --
only Validation's CHECKSUM can see it.

Credential-free by construction (Property 7): ``Loggings.logErrorAndTraceRecord`` puts
only (table, column, type) + the throwable on the ERROR line and the ROW on a separate
TRACE line the MSK Connect worker never emits. This module keeps ONLY the identifiers;
the record has no free-text field at all, so the converter exception's rendering of a
value can never reach the UI or the durable log.

Live-measured on Aurora PostgreSQL 17.7: a ``numeric`` holding ``NaN`` and a
``bit(n)[]`` column each produced this line and arrived NULL. The class is BROADER than
those two -- which values a given engine cannot convert was not enumerated -- so this
parser reports whatever the connector NAMES instead of matching a type list.

Deliberately NOT a :class:`~dsql_migrator.core.cdc.CdcConnectorError` and NOT a
:class:`~dsql_migrator.core.cdc.SchemaDriftKind`: those are what the dead-letter depth
counts and what the drift banner explains, and these rows were APPLIED. Counting them
as quarantined would inflate the one number a cut-over decision turns on.
"""

from __future__ import annotations

import re
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field

# The CloudWatch filter pattern that selects these lines, kept beside the regex that
# parses them so the selector and the parser can never drift apart.
VALUE_CONVERSION_FILTER_PATTERN = '"Failed to properly convert data value"'

# How many DISTINCT (table, column, type) triples the monitor tracks. The signal is a
# set, not a count, so a bound here costs no accuracy -- it only stops a pathological
# schema from growing session state without limit.
VALUE_CONVERSION_MAX_TRACKED = 50

# ``Failed to properly convert data value for '<tableId>.<column>' of type <type>``.
# The type is 1-4 word tokens (PostgreSQL ``numeric`` / ``_bit`` / ``timestamp with time
# zone``, MySQL ``BIGINT UNSIGNED``). A token is only taken when it is NOT followed by a
# ``.`` or ``:``, so if a future layout renders the converter exception on the same line
# (``... of type numeric org.postgresql.util.PSQLException: ...``) the package name is
# not swallowed into the type -- and log4j's own ``(logger:line)`` tail never is.
_CONVERSION_LINE = re.compile(
    r"Failed to properly convert data value for "
    r"'(?P<ident>[^']+)' of type "
    r"(?P<type>[A-Za-z0-9_]+(?:(?![.:]) [A-Za-z0-9_]+(?![\w.:])){0,3})"
)


class CdcValueConversionFailure(BaseModel):
    """One column the source connector could not convert, so it delivered NULL.

    NOT a dead letter: the row was applied and only this column is empty. Frozen (and
    therefore hashable) because the operator-facing fact is a SET -- the same value
    fails on every row that carries it, so "this column arrives NULL" is the finding,
    not a number of occurrences (which no log-derived read could bound anyway).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # ``db.table`` / ``schema.table`` -- the same key shape every other per-table CDC
    # surface uses (the sink metrics' ``Table`` dimension, ``cdc_dlq._table_from_topic``),
    # so this reads consistently beside them.
    table: str = Field(min_length=1)
    column: str = Field(min_length=1)
    type_name: str = Field(min_length=1)

    @property
    def qualified_column(self) -> str:
        """``db.table.column`` -- how the notice and the activity log name it."""
        return f"{self.table}.{self.column}"


def parse_value_conversion_failure(
    message: Optional[str],
) -> Optional[CdcValueConversionFailure]:
    """Parse one connector log line into a :class:`CdcValueConversionFailure`, else ``None``.

    Pure. ``None`` for any other line, so ordinary log noise -- including every sink
    dead-letter line -- is ignored; symmetrically ``cdc_dlq.parse_dlq_log_message``
    returns ``None`` for these, so the two reads are disjoint and nothing is counted
    twice.

    The identifier is ``<tableId>.<column>``: the column is its LAST dot-segment and the
    table the (up to) two before it -- the same "last two segments" rule
    ``cdc_dlq._table_from_topic`` applies to a topic, so a 2-part MySQL ``db.table`` and
    a 3-part PostgreSQL id both yield the db-qualified table the rest of the tool keys
    on. A column name containing a dot would be mis-split; that is accepted rather than
    guessed at, since the connector gives no other delimiter.
    """
    if not message:
        return None
    match = _CONVERSION_LINE.search(message)
    if match is None:
        return None
    parts = [segment for segment in match.group("ident").split(".") if segment]
    if len(parts) < 2:
        return None
    return CdcValueConversionFailure(
        table=".".join(parts[-3:-1]),
        column=parts[-1],
        type_name=match.group("type").strip(),
    )
