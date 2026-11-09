# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""MySQL-source value conversion for Full Load.

The twin of :mod:`dsql_migrator.core.exporter_postgres`, per the per-engine separation
principle. A MySQL value must be translated for its Aurora DSQL column (``TINYINT(1)`` ->
bool, ``BLOB`` -> bytea, ``DATETIME`` -> a naive wall clock, ``TIME`` timedelta -> time or a
duration's text, ``BIT`` bytes -> int, a zero date -> NULL / the column's DEFAULT / the
epoch), keyed off the same type mapping the Schema Converter uses
(:func:`~dsql_migrator.core.converter.map_mysql_type`). :class:`ValueConverter` is what the
MySQL source dialect's ``value_converter`` returns; :func:`value_fit_rule` is also the
exporter's ``ValueFitGuard`` rule.

Imports neither the exporter nor the source dialects (the MySQL dialect imports this module
at its top level); :mod:`dsql_migrator.core.exporter` re-exports every name.
"""

from __future__ import annotations

import math
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from functools import partial
from typing import Callable, Mapping, Optional, Sequence

from dsql_migrator.core.converter import TargetValueSpec, map_mysql_type
from dsql_migrator.core.export_errors import ValueConversionError
from dsql_migrator.core.models import TableDef
from dsql_migrator.core.mysql_zero_dates import ZeroDateMapping, zero_date_mappings


def _utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC (assume naive datetimes are already UTC).

    MySQL ``DATETIME`` has no time zone: the driver returns its wall clock naive, and
    the converter maps it to a DSQL ``timestamp``. A naive value therefore gets UTC
    attached -- the same wall clock, which the ``timestamp`` caller strips again, so it
    is bound unchanged in whatever zone the source wrote it (G149); a tz-aware value is
    converted to UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _target_kind(mysql_type: str) -> str:
    """Return the normalized DSQL target type name for a MySQL type string.

    Reuses :func:`dsql_migrator.core.converter.map_mysql_type` so value
    conversion stays consistent with schema conversion (no divergent mapping).
    Parameters and casing are stripped (e.g. ``numeric(20, 0)`` -> ``numeric``).
    Unparseable types degrade to a pass-through (their lower-cased text).
    """
    try:
        target_type, _ = map_mysql_type(mysql_type)
    except ValueError:
        return mysql_type.strip().lower()
    return target_type.split("(", 1)[0].strip().lower()


# The verdicts of :func:`value_fit_rule` (the same names as the CDC sink's ValueFit.Rule, in
# the shared tests/fixtures/value_fit_contract.tsv).
VALUE_FIT_NOT_ZERO_OR_ONE = "not-0-or-1"
VALUE_FIT_FRACTION = "fraction"
VALUE_FIT_SCALE = "scale"

_BOOLEAN_SPEC = TargetValueSpec("boolean", 0, False, "boolean")


def _digits_beyond(value: Decimal, places: int) -> bool:
    """Whether a FINITE ``value`` has a non-zero digit past ``places`` decimal places.

    Reads the decimal's own digits (``as_tuple``): no quantize, no context, so it can neither
    round nor raise, and it costs O(digits)."""
    _sign, digits, exponent = value.as_tuple()
    beyond = -int(exponent) - places
    if beyond <= 0:
        return False
    return any(digits[-beyond:])


def value_fit_rule(
    value: object, spec: TargetValueSpec, *, unconstrained_source: bool = False
) -> Optional[str]:
    """The rule ``value`` breaks for its target column, or ``None`` when it is stored exactly.

    Pure; mirrors the CDC sink's ``ValueFit.rule`` (the shared verdict table is
    ``tests/fixtures/value_fit_contract.tsv``). Aurora DSQL ACCEPTS these and stores something
    else, with no error:

    * ``boolean``: a number other than exactly 0 or 1 (it would become true/false);
    * ``integral``: a finite decimal or float with a fractional part (rounded to a whole);
    * ``numeric``: a finite decimal with a non-zero digit beyond the column's scale
      (rounded) -- unless the column is Aurora DSQL's default ``numeric(18,6)`` and the source
      numeric is unconstrained, whose rounding is documented and warned in Schema Conversion.

    ``None`` and ``bool`` always fit. A float into ``numeric`` is not checked (PostgreSQL
    renders it to 15 significant digits first; the sink leaves it unchecked too).
    """
    if value is None or isinstance(value, bool):
        return None
    kind = spec.kind
    if kind == "boolean":
        if isinstance(value, (int, float, Decimal)):
            if isinstance(value, Decimal) and not value.is_finite():
                return VALUE_FIT_NOT_ZERO_OR_ONE
            return None if value in (0, 1) else VALUE_FIT_NOT_ZERO_OR_ONE
        return None
    if kind == "integral":
        if isinstance(value, Decimal):
            return VALUE_FIT_FRACTION if value.is_finite() and _digits_beyond(value, 0) else None
        if isinstance(value, float):
            return (
                VALUE_FIT_FRACTION
                if math.isfinite(value) and not value.is_integer()
                else None
            )
        return None
    if kind == "numeric":
        if not isinstance(value, Decimal) or not value.is_finite():
            return None
        if not _digits_beyond(value, spec.scale):
            return None
        if spec.default_numeric and unconstrained_source:
            return None
        return VALUE_FIT_SCALE
    return None


# Target kinds a MySQL TIME duration is written to as text (see ValueConverter).
_DURATION_TARGET_KINDS = frozenset(
    {"interval", "text", "varchar", "character varying", "char", "character", "bpchar"}
)


def duration_text(value: timedelta) -> str:
    """``[-]HH:MM:SS[.f]`` of a MySQL TIME duration: hours not capped at 24, the fraction
    without trailing zeros -- the text PostgreSQL prints for the interval the CDC sink binds
    (IntervalStyle postgres), so both write paths store the same text. Pure."""
    micros = value.days * 86_400_000_000 + value.seconds * 1_000_000 + value.microseconds
    sign = "-" if micros < 0 else ""
    micros = abs(micros)
    hours, rest = divmod(micros, 3_600_000_000)
    minutes, rest = divmod(rest, 60_000_000)
    seconds, fraction = divmod(rest, 1_000_000)
    text = f"{sign}{hours:02d}:{minutes:02d}:{seconds:02d}"
    return text + (f".{fraction:06d}".rstrip("0") if fraction else "")


class ValueConverter:
    """Converts MySQL cell values to canonical values for the DSQL target type.

    The conversion is keyed off the DSQL type produced by the shared type mapping
    (Requirement 5.1, consistent with the Schema Converter). Canonical outputs:

    - ``boolean``: Python ``bool`` (``TINYINT(1)`` ``0/1`` -> ``False/True``),
    - ``bytea``: Python ``bytes`` (MySQL ``BLOB`` payload preserved),
    - ``timestamp``: tz-aware UTC :class:`datetime` (``DATETIME`` normalized),
    - everything else: the value unchanged.
    """

    def __init__(
        self,
        table: TableDef,
        *,
        target_types: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Precompute the per-column target type kinds for ``table``.

        ``target_types`` (optional) maps a column name to the *applied* target
        type (e.g. parsed from the converted/edited DDL via
        :func:`~dsql_migrator.core.converter.parse_target_column_types`). When a
        column is present there, the conversion follows that applied type instead
        of the source-derived mapping -- so a user remap in Schema Conversion
        (e.g. ``TINYINT(1)`` -> ``smallint`` instead of ``boolean``) takes effect
        on the Full Load value conversion. Columns absent from the override keep
        the source-derived kind.
        """
        overrides = target_types or {}
        self._kinds: dict[str, str] = {}
        for column in table.columns:
            if column.name in overrides:
                self._kinds[column.name] = (
                    overrides[column.name].split("(", 1)[0].strip().lower()
                )
            else:
                self._kinds[column.name] = _target_kind(column.mysql_type)
        # Source MySQL BIT(n) columns: the driver returns the value as big-endian
        # bytes, but BIT maps to an integer target in DSQL (the bit type is
        # unsupported), so those bytes must be decoded to an int at convert time.
        self._bit_columns: frozenset[str] = frozenset(
            column.name
            for column in table.columns
            if column.mysql_type.strip().lower().split("(", 1)[0] == "bit"
        )
        # Source MySQL TIME columns remapped to interval or a text type (durations): the
        # driver's timedelta is written as the CDC sink writes it, [-]HH:MM:SS[.f] with the
        # hours not capped at 24 -- psycopg's own interval text ('1 day 6:00:00') stored
        # another value in a text column, and another spelling in an interval one.
        self._duration_columns: frozenset[str] = frozenset(
            column.name
            for column in table.columns
            if column.mysql_type.strip().lower().split("(", 1)[0] == "time"
            and (
                self._kinds.get(column.name) in _DURATION_TARGET_KINDS
                or (self._kinds.get(column.name) or "").startswith("interval")
            )
        )
        # Source MySQL DATE / DATETIME / TIMESTAMP columns (G150): PyMySQL returns a zero
        # date ('0000-00-00', '2020-00-15'), a day the month does not have ('2020-02-31')
        # and the zero TIMESTAMP as a str, which Aurora DSQL rejects (22008). Each is
        # written as the bundled CDC writes it (core.mysql_zero_dates: NULL or the
        # column's DEFAULT / the epoch), whatever the target type, and counted per column
        # for the table's one activity-log line -- except in a primary-key or unique-key
        # column (G154, G187), where mapped values fall onto one and the row's str is left
        # for Aurora DSQL to reject, quarantining that row alone. These columns leave the
        # pass-through fast path below (a DATE column took it before): a real value costs
        # one convert_value call, O(1), and only a str is parsed.
        self._zero_dates: dict[str, ZeroDateMapping] = zero_date_mappings(table)
        # (column, ZERO | INVALID) -> how many values this converter mapped.
        self.zero_dates: dict[tuple[str, str], int] = {}
        # Fast-path: columns whose target kind needs no conversion (int, varchar,
        # numeric, text, etc.). For a typical OLTP table 80-90% of columns fall
        # here. convert_row skips convert_value entirely for these, replacing 7+
        # Python ops per cell with a single frozenset lookup.
        _NEEDS_CONVERSION_KINDS = frozenset(
            {"boolean", "bytea", "timestamp", "timestamptz", "time"}
        )
        self._passthrough_columns: frozenset[str] = frozenset(
            name
            for name, kind in self._kinds.items()
            if kind not in _NEEDS_CONVERSION_KINDS
            and name not in self._bit_columns
            and name not in self._duration_columns
            and name not in self._zero_dates
        )
        # Every other column's conversion, resolved once for the table: what convert_value
        # does to a value of it that is not None (see _value_function). Used by
        # row_converter, the Full Load read's per-row path.
        self._value_functions: dict[str, Callable[[object], object]] = {
            name: self._value_function(name)
            for name in self._kinds
            if name not in self._passthrough_columns
        }

    def _value_function(self, column_name: str) -> Callable[[object], object]:
        """``convert_value(column_name, value)`` for a value that is not None, with the
        column's dispatch decided here, once: the type PyMySQL returns for the column takes a
        fast path giving what convert_value gives for it, and every other value -- a zero date
        (a str), a subclass, a value out of range -- goes to convert_value itself, so the
        result, the zero-date tally and every error stay convert_value's."""
        slow = partial(self.convert_value, column_name)
        if column_name in self._bit_columns:
            # Checked before the target kind, as convert_value does.
            def bit(value: object) -> object:
                if type(value) is bytes:
                    return int.from_bytes(value, byteorder="big")
                return slow(value)

            return bit
        if column_name in self._duration_columns:
            def duration(value: object) -> object:
                if type(value) is timedelta:
                    return duration_text(value)
                return slow(value)

            return duration
        kind = self._kinds.get(column_name)
        if kind == "boolean":
            def boolean(value: object) -> object:
                if type(value) is int and (value == 0 or value == 1):
                    return value == 1
                return slow(value)

            return boolean
        if kind == "bytea":
            def bytea(value: object) -> object:
                # bytes(value) is value itself for a bytes object.
                return value if type(value) is bytes else slow(value)

            return bytea
        if kind == "timestamp":
            def timestamp(value: object) -> object:
                # A naive DATETIME: _utc(value).replace(tzinfo=None) is the same wall clock.
                if type(value) is datetime and value.tzinfo is None:
                    return value
                return slow(value)

            return timestamp
        if kind == "timestamptz":
            def timestamptz(value: object) -> object:
                if type(value) is datetime and value.tzinfo is None:
                    return value.replace(tzinfo=timezone.utc)
                return slow(value)

            return timestamptz
        if kind == "time" or column_name not in self._zero_dates:
            return slow

        # A zero-date column whose target kind converts nothing: only a str is mapped.
        def zero_dates_only(value: object) -> object:
            return slow(value) if isinstance(value, str) else value

        return zero_dates_only

    def zero_date_entries(self) -> "tuple[tuple[str, str, str, int], ...]":
        """The zero / invalid dates this converter mapped, as ``(column, ZERO | INVALID,
        what it was written as, count)`` -- names and counts only, never a value."""
        return tuple(
            (column, rule, self._zero_dates[column].replacement_text(rule), count)
            for (column, rule), count in sorted(self.zero_dates.items())
        )

    def convert_value(self, column_name: str, value: object) -> object:
        """Convert a single cell value for ``column_name`` (``None`` passes through)."""
        if value is None:
            return None
        if column_name in self._zero_dates and isinstance(value, str):
            mapped = self._zero_dates[column_name].value(value)
            if mapped is not None:
                rule, value = mapped
                key = (column_name, rule)
                self.zero_dates[key] = self.zero_dates.get(key, 0) + 1
                if value is None:
                    return None
        if column_name in self._bit_columns:
            # MySQL BIT(n) -> integer target. The driver returns big-endian bytes
            # (e.g. b'\xdb'); decode to the unsigned integer the bit pattern holds.
            # A driver that already returns an int (some configs) passes through.
            if isinstance(value, (bytes, bytearray, memoryview)):
                return int.from_bytes(bytes(value), byteorder="big")
            return value
        if column_name in self._duration_columns and isinstance(value, timedelta):
            return duration_text(value)
        kind = self._kinds.get(column_name)
        if kind == "boolean":
            if isinstance(value, bool):
                return value
            # TINYINT(1) -> DSQL boolean. The (1) is display width, not a value
            # constraint, so the column can legally hold values outside {0, 1};
            # bool(int(value)) would flatten e.g. 2 or -1 to True (and int(0.5) to
            # False) and silently lose the magnitude. Fail loudly so a wrong boolean
            # never lands -- the same refusal as the CDC sink's value-fit guard -- naming
            # the column, never the value. A genuine 0/1 boolean column is unaffected.
            number = (
                value
                if isinstance(value, (int, float, Decimal))
                else int(value)  # type: ignore[call-overload]
            )
            if value_fit_rule(number, _BOOLEAN_SPEC) is not None:
                raise ValueConversionError(
                    f"column '{column_name}' maps to DSQL boolean, but the source holds "
                    "a value other than 0 or 1 in it; a TINYINT(1) storing values beyond "
                    "0/1 cannot be a boolean without data loss. In Schema Conversion, "
                    "remap this column's target type to smallint/integer (the converted "
                    "DDL is editable), re-apply, then retry this table -- the value then "
                    "loads as an integer. Alternatively, restrict the source values to 0/1."
                )
            return number == 1
        if kind == "bytea":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            return value
        if kind == "timestamp":
            # MySQL DATETIME -> DSQL ``timestamp`` WITHOUT TIME ZONE. Normalize to
            # UTC, then drop the tzinfo: binding a tz-AWARE datetime to a no-tz
            # column makes PostgreSQL/psycopg convert it to the session TimeZone and
            # discard the offset, so a non-UTC session would silently shift the
            # stored wall-clock by hours. A naive value -- the source's own wall clock,
            # in whatever zone it was written -- is stored as it is, regardless of the
            # session's TimeZone.
            if isinstance(value, datetime):
                return _utc(value).replace(tzinfo=None)
            return value
        if kind == "timestamptz":
            # MySQL TIMESTAMP -> DSQL ``timestamptz``. A tz-aware UTC value stores
            # the correct instant independent of the session TimeZone.
            if isinstance(value, datetime):
                return _utc(value)
            return value
        if kind == "time":
            # MySQL TIME comes off the driver as a timedelta; psycopg binds a
            # timedelta to interval, not to a DSQL ``time`` column (type mismatch
            # that fails the row). Convert an in-range (0 <= t < 24h) value to a
            # datetime.time. MySQL TIME's full range is -838:59:59..838:59:59, and a
            # value outside [0, 24h) has NO ``time`` representation -- passing the
            # raw timedelta through would silently bind to interval (or emit a
            # non-time text cell), corrupting the column. Fail loudly (naming the
            # column, never the value) instead, matching the TINYINT(1)-out-of-range
            # guard.
            if isinstance(value, timedelta):
                total = value.total_seconds()
                if value == timedelta(hours=24):
                    # PostgreSQL's time holds 24:00:00 exactly (datetime.time cannot), as
                    # the CDC sink writes it.
                    return "24:00:00"
                if 0 <= total < 86400:
                    secs = int(total)
                    micros = value.microseconds
                    return time(
                        hour=secs // 3600,
                        minute=(secs % 3600) // 60,
                        second=secs % 60,
                        microsecond=micros,
                    )
                raise ValueConversionError(
                    f"column '{column_name}' maps to DSQL time, but a source value is "
                    "outside 00:00:00..23:59:59.999999; MySQL "
                    "TIME allows -838:59:59..838:59:59, which has no DSQL 'time' "
                    "representation and would corrupt the column. In Schema "
                    "Conversion, remap this column's target type to interval or "
                    "text (the converted DDL is editable), re-apply, then retry "
                    "this table. Alternatively, restrict the source TIME values to "
                    "the 0..24h range."
                )
            return value
        return value

    def convert_row(self, row: Mapping[str, object]) -> dict[str, object]:
        """Convert every known cell in ``row`` (unknown columns pass through).

        Fast path: columns in ``_passthrough_columns`` (the majority for a typical
        OLTP table — int, varchar, numeric, text) skip ``convert_value`` entirely.
        Only columns that actually need type translation (boolean, bytea, timestamp,
        timestamptz, time, bit) go through the full method.
        """
        passthrough = self._passthrough_columns
        return {
            name: (value if name in passthrough else self.convert_value(name, value))
            for name, value in row.items()
        }

    def row_converter(
        self, names: Sequence[str]
    ) -> Callable[[Sequence[object]], dict[str, object]]:
        """The function that converts a row whose values come in ``names`` order (an
        SQLAlchemy ``Row``, a tuple) to the dict :meth:`convert_row` returns for the mapping
        of ``names`` to those values -- the same keys in the same order, the same values,
        tally and errors. ``names`` must be distinct.

        Resolved once for ``names`` (a keyset page's columns): the row is zipped into a dict,
        and only the columns convert_value changes are visited, by position, through the
        functions resolved for them (:meth:`_value_function`); a None stays None."""
        names = tuple(names)
        functions = self._value_functions
        visits = tuple(
            (position, name, functions[name])
            for position, name in enumerate(names)
            if name in functions
        )

        def convert(values: Sequence[object]) -> dict[str, object]:
            row = dict(zip(names, values))
            for position, name, function in visits:
                value = values[position]
                if value is not None:
                    row[name] = function(value)
            return row

        return convert


__all__ = [
    "VALUE_FIT_FRACTION",
    "VALUE_FIT_NOT_ZERO_OR_ONE",
    "VALUE_FIT_SCALE",
    "ValueConverter",
    "duration_text",
    "value_fit_rule",
]
