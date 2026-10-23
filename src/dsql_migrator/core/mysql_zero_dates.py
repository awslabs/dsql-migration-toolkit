# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Zero and invalid MySQL dates (G150): the value the bundled CDC writes for one, which Full
Load writes too and Validation compares the source by.

MySQL can store dates Aurora DSQL has no value for -- a zero date ``'0000-00-00'``, a date
with a zero year, month or day (``'2020-00-15'``), and, under ``ALLOW_INVALID_DATES``, a day
the month does not have (``'2020-02-31'``) -- in ``DATE`` and ``DATETIME`` columns, and the
zero ``TIMESTAMP`` (stored as 0 seconds) in ``TIMESTAMP`` columns. PyMySQL hands each over as
a ``str``, which Aurora DSQL rejects (SQLSTATE 22008), so every such row was quarantined.

What the bundled CDC writes for one (read from the shipped Debezium 2.7.4 jars and the DSQL
sink; the source connector sets neither ``event.processing.failure.handling.mode`` nor
``time.precision.mode``):

* The binlog deserializers (``debezium-connector-binlog`` ``RowDeserializers.deserializeDate``
  / ``deserializeDatetime`` / ``deserializeDatetimeV2``) return ``null`` when the year, month
  or day is 0. ``JdbcValueConverters.convertValue`` then writes NULL for a column that allows
  NULL and, for a ``NOT NULL`` one, the field schema's default -- the column's ``DEFAULT``
  as ``BinlogDefaultValueConverter`` parsed it from the schema history -- or else the
  fallback, epoch day 0 / epoch 0: ``1970-01-01`` / ``1970-01-01 00:00:00``. That parser
  turns a zero default and ``CURRENT_TIMESTAMP`` into the epoch, an expression default into
  no default, and a literal into its date (SMART resolution: a day past the month's end
  becomes its last day).
* A day the month does not have makes ``LocalDate.of`` throw; ``handleException`` then
  raises under the default ``fail`` mode (``warn`` / ``skip`` substitute
  ``LocalDate.EPOCH``, midnight for a DATETIME), so the source connector stops at the first
  binlog row event holding one -- any table's: the deserializer has no table filter -- and
  does not get past it. Full Load writes Debezium's substitute, the epoch.
* ``deserializeTimestamp`` / ``deserializeTimestampV2`` read the zero TIMESTAMP as the
  instant 0, never null, so it is ``1970-01-01 00:00:00 UTC`` whatever the column's
  nullability.
* The DSQL sink binds NULL as NULL and the Debezium date / timestamp encodings as the
  ``LocalDate`` / ``LocalDateTime`` / ``OffsetDateTime`` they hold
  (``DebeziumTypeConverter.convert``), so nothing changes on the way in.

:func:`zero_date_mapping` builds a column's :class:`ZeroDateMapping` from Step 1's
:class:`~dsql_migrator.core.models.ColumnDef` (type, nullability, ``DEFAULT``): the Full Load
reader maps the driver's ``str`` with :meth:`ZeroDateMapping.value`, and Validation renders
the source through :meth:`ZeroDateMapping.sql`, so a table whose only differences are such
values compares equal and any other difference still differs. Pure: no database, no AWS.
"""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Union

from dsql_migrator.core.models import ColumnDef, TableDef

# The MySQL types that can hold a zero or invalid date.
TEMPORAL_BASES = frozenset({"date", "datetime", "timestamp"})

EPOCH_DATE = date(1970, 1, 1)
EPOCH_DATETIME = datetime(1970, 1, 1)

# The two kinds of value mapped: a zero year, month or day; a day the month does not have.
ZERO = "zero"
INVALID = "invalid"

Temporal = Union[date, datetime]

# The date at the start of the text MySQL (and PyMySQL, which returns it as a str) prints for
# a stored DATE / DATETIME / TIMESTAMP: 'YYYY-MM-DD', then ' HH:MM:SS[.ffffff]'.
_STORED_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

# BinlogDefaultValueConverter's EPOCH_EQUIVALENT_DATE / EPOCH_EQUIVALENT_TIMESTAMP, matched
# against the whole (trimmed) DEFAULT: a zero day, month or year, as a date or at midnight.
_EPOCH_EQUIVALENT_DATE_RE = re.compile(r"\d{4}-\d{2}-00|\d{4}-00-\d{2}|0000-\d{2}-\d{2}")
_EPOCH_EQUIVALENT_TIMESTAMP_RE = re.compile(
    r"(?:\d{4}-\d{2}-00|\d{4}-00-\d{2}|0000-\d{2}-\d{2}) 00:00:00(?:\.\d{1,6})?"
)
# What its formatters parse: 'yyyy-MM-dd', then optionally ' ' and ISO_LOCAL_TIME
# (HH:mm[:ss[.f]]). MySQL prints a temporal DEFAULT with four year digits.
_DEFAULT_LITERAL_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})(?: (\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?"
)


def temporal_base(column: Optional[ColumnDef]) -> Optional[str]:
    """``"date"`` / ``"datetime"`` / ``"timestamp"`` for such a MySQL column, else None."""
    if column is None or not column.mysql_type:
        return None
    base = column.mysql_type.strip().lower().split("(", 1)[0].split()
    return base[0] if base and base[0] in TEMPORAL_BASES else None


def _debezium_default(column: ColumnDef, base: str) -> Temporal:
    """What the CDC connector writes for a zero date in this ``NOT NULL`` DATE / DATETIME
    column: its ``DEFAULT`` as Debezium parses it, or the epoch. Mirrors
    ``BinlogDefaultValueConverter.convertToLocalDate`` / ``convertToLocalDateTime`` (the
    DDL parser has already turned CURRENT_TIMESTAMP / NOW() into '1970-01-01 00:00:00' and
    an expression default into no default)."""
    epoch: Temporal = EPOCH_DATE if base == "date" else EPOCH_DATETIME
    if column.default is None or column.default_is_expression:
        return epoch
    text = column.default.strip()
    zero = _EPOCH_EQUIVALENT_TIMESTAMP_RE.fullmatch(text) is not None or text == "0"
    if base == "date":
        zero = zero or _EPOCH_EQUIVALENT_DATE_RE.fullmatch(text) is not None
    if zero:
        return epoch
    match = _DEFAULT_LITERAL_RE.fullmatch(text)
    if match is None:
        return epoch
    year, month, day = (int(part) for part in match.groups()[:3])
    hour, minute, second = (int(part or 0) for part in match.groups()[3:6])
    fraction = (match.group(7) or "").ljust(6, "0")[:6]
    if not (1 <= year <= 9999 and 1 <= month <= 12 and 1 <= day <= 31):
        return epoch
    if hour > 23 or minute > 59 or second > 59:
        return epoch
    # SMART resolution: a day past the end of the month is the month's last day.
    day = min(day, calendar.monthrange(year, month)[1])
    if base == "date":
        return date(year, month, day)
    return datetime(year, month, day, hour, minute, second, int(fraction))


@dataclass(frozen=True)
class ZeroDateMapping:
    """What one MySQL DATE / DATETIME / TIMESTAMP column's zero and invalid dates become.

    ``zero`` is the value a zero date (a zero year, month or day; for a TIMESTAMP the zero
    TIMESTAMP) is written as, ``None`` for NULL; ``invalid`` the value a day the month does
    not have is written as (a TIMESTAMP cannot hold one). Both are what the driver returns
    for a stored value of the column's type -- a ``date``, or a naive ``datetime`` in the
    session's UTC -- so the reader's usual conversion applies to them as to any value."""

    base: str
    zero: Optional[Temporal]
    invalid: Temporal

    def value(self, text: str) -> "Optional[tuple[str, Optional[Temporal]]]":
        """``(ZERO | INVALID, replacement)`` for a stored value the driver returned as
        ``text``; None when ``text`` is a date after all (passed on as it is) or not one."""
        match = _STORED_DATE_RE.match(text)
        if match is None:
            return None
        year, month, day = (int(part) for part in match.groups())
        if year == 0 or month == 0 or day == 0:
            return ZERO, self.zero
        if self.base == "timestamp":
            return None
        try:
            date(year, month, day)
        except ValueError:
            return INVALID, self.invalid
        return None

    def replacement_text(self, rule: str) -> str:
        """The replacement as a log line or a finding names it (never a row's value)."""
        value = self.zero if rule == ZERO else self.invalid
        if value is None:
            return "NULL"
        text = _literal_text(value)
        return text + " UTC" if self.base == "timestamp" else text

    def sql(self, column_sql: str) -> str:
        """The MySQL expression for ``column_sql`` (a quoted column) with its zero and
        invalid dates replaced as Full Load and CDC write them, of the column's own type, so
        it renders like the column: ``CASE WHEN <zero> THEN <zero> WHEN <invalid> THEN
        <epoch> ELSE <column> END``. YEAR() / MONTH() / DAYOFMONTH() return a stored zero
        part as 0 and LAST_DAY() the month's last day, whatever the session's sql_mode."""
        branches = f"WHEN {zero_sql(column_sql)} THEN {_literal_sql(self.zero)} "
        if self.base != "timestamp":
            branches += f"WHEN {invalid_sql(column_sql)} THEN {_literal_sql(self.invalid)} "
        return f"CASE {branches}ELSE {column_sql} END"


def _literal_text(value: Temporal) -> str:
    """``YYYY-MM-DD`` or ``YYYY-MM-DD HH:MM:SS[.ffffff]``, four year digits whatever the
    platform's strftime does below the year 1000."""
    if not isinstance(value, datetime):
        return value.isoformat()
    text = (
        f"{value.year:04d}-{value.month:02d}-{value.day:02d} "
        f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
    )
    return text + (f".{value.microsecond:06d}" if value.microsecond else "")


def _literal_sql(value: Optional[Temporal]) -> str:
    """A MySQL typed literal (``DATE'...'`` / ``TIMESTAMP'...'``) or NULL. Built from a
    parsed value, so only digits and separators reach the SQL."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return f"TIMESTAMP'{_literal_text(value)}'"
    return f"DATE'{_literal_text(value)}'"


def zero_sql(column_sql: str) -> str:
    """MySQL: ``column_sql`` holds a zero year, month or day (NULL for NULL)."""
    return f"(YEAR({column_sql}) = 0 OR MONTH({column_sql}) = 0 OR DAYOFMONTH({column_sql}) = 0)"


def invalid_sql(column_sql: str) -> str:
    """MySQL: ``column_sql`` holds a day its month does not have (read after :func:`zero_sql`:
    LAST_DAY() of a zero month is NULL)."""
    return f"DAYOFMONTH({column_sql}) > DAYOFMONTH(LAST_DAY({column_sql}))"


def zero_date_mapping(column: Optional[ColumnDef]) -> Optional[ZeroDateMapping]:
    """The mapping for a MySQL DATE / DATETIME / TIMESTAMP column, else None. Pure."""
    base = temporal_base(column)
    if base is None:
        return None
    if base == "timestamp":
        return ZeroDateMapping(base, EPOCH_DATETIME, EPOCH_DATETIME)
    epoch: Temporal = EPOCH_DATE if base == "date" else EPOCH_DATETIME
    zero = None if column.nullable else _debezium_default(column, base)  # type: ignore[union-attr]
    return ZeroDateMapping(base, zero, epoch)


def check_query(table: TableDef) -> Optional[str]:
    """A query the operator can run on the source to count ``table``'s rows that hold a zero
    or invalid date, or None when it has no DATE / DATETIME / TIMESTAMP column. Evaluation
    reads no rows; this reads the whole table once."""
    from dsql_migrator.core.validation_sql import _quote_mysql_identifier, _quote_mysql_table

    tests = []
    for column in table.columns:
        base = temporal_base(column)
        if base is None:
            continue
        quoted = _quote_mysql_identifier(column.name)
        tests.append(zero_sql(quoted))
        if base != "timestamp":
            tests.append(invalid_sql(quoted))
    if not tests:
        return None
    return f"SELECT COUNT(*) FROM {_quote_mysql_table(table.name)} WHERE {' OR '.join(tests)};"


__all__ = [
    "EPOCH_DATE",
    "EPOCH_DATETIME",
    "INVALID",
    "TEMPORAL_BASES",
    "ZERO",
    "ZeroDateMapping",
    "check_query",
    "invalid_sql",
    "temporal_base",
    "zero_date_mapping",
    "zero_sql",
]
