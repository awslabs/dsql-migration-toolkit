# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The source's time zone (G149): the zone Schema Conversion writes into what reads the
clock, and what Evaluation reports about it.

A MySQL source's ``NOW()`` / ``CURRENT_TIMESTAMP`` / ``CURDATE()`` read the wall clock of
the session's ``time_zone``, and a PostgreSQL source's ``now()`` written into a ``timestamp
without time zone`` or ``date`` column reads the session's ``TimeZone``; a DATETIME /
``timestamp`` column stores what they return as it is, and Full Load and CDC copy those
values unchanged. An Aurora DSQL session's zone is UTC unless the session sets another, so
a clock default or a view carried as written computes another wall clock there. The zone
an application session gets by default is the one Evaluation read (G148's
``SourceInventory.server_settings``): MySQL's GLOBAL ``time_zone`` (``SYSTEM`` is the
server's own, ``system_time_zone``), PostgreSQL's default ``TimeZone``.

:func:`resolve_source_zone` classifies it, and :class:`SourceZone` writes the source's wall
clock as a PostgreSQL expression. Only a zone the target reads the same way is written: a
zone NAME (``now() AT TIME ZONE 'Asia/Seoul'``), or a fixed offset in the INTERVAL form
(``(now() AT TIME ZONE 'UTC') + INTERVAL '+09:00'``) -- never an offset as a zone name,
which PostgreSQL reads as a POSIX zone, west of UTC (``AT TIME ZONE '+09:00'`` is UTC-9).
UTC itself, and a zone that cannot be read or told (``SYSTEM`` reported by an abbreviation,
a POSIX rule string with daylight saving), change nothing: the conversion keeps the
session's clock, as before. Pure: no database, no AWS.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core.models import SourceType, ViewDef

# The names of UTC itself, case-insensitively, as MySQL's time zone tables and PostgreSQL's
# TimeZone spell them.
_UTC_NAMES = frozenset(
    {
        "utc", "uct", "gmt", "gmt0", "gmt+0", "gmt-0", "greenwich", "universal", "zulu",
        "etc/utc", "etc/uct", "etc/gmt", "etc/gmt0", "etc/gmt+0", "etc/gmt-0",
        "etc/greenwich", "etc/universal", "etc/zulu",
    }
)
# A zone name: IANA's Area/Location form ('Asia/Seoul', 'America/Argentina/Buenos_Aires',
# 'Etc/GMT-9') or one of its single words ('Japan', 'EST5EDT'). The source accepted it, so
# its shape is all that is checked; it is quoted when written, never interpolated raw.
_ZONE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_+\-]*(?:/[A-Za-z0-9_+\-]+)*$")
# Names PostgreSQL reads in AT TIME ZONE as an ABBREVIATION -- it looks those up first --
# with a fixed offset, while the zone of that name keeps daylight saving: AT TIME ZONE 'CET'
# is +01:00 all year. Each is written as the zone whose rules it follows now (tzdata links
# them so since 2024b), so a summer default is not an hour off.
_ABBREVIATION_NAMED_ZONES = {
    "cet": "Europe/Brussels",
    "met": "Europe/Brussels",
    "eet": "Europe/Athens",
    "wet": "Europe/Lisbon",
}
# MySQL's fixed offset: '+09:00', '-05:30' (its time_zone accepts -13:59 to +14:00).
_MYSQL_OFFSET_RE = re.compile(r"^([+-])(\d{1,2}):(\d{2})$")
# A POSIX TZ string with no daylight-saving part, as PostgreSQL's TimeZone shows a fixed
# offset: an abbreviation (letters, or <...>) and/or a signed offset in hours WEST of UTC
# -- '<+09>-09' (what SET TIME ZONE 9 shows), '<+05:30>-05:30' (SET TIME ZONE 5.5, or
# INTERVAL '+05:30' HOUR TO MINUTE; verified on 16 and 17), 'JST-9', 'UTC+9', '+09:00'.
_POSIX_FIXED_RE = re.compile(
    r"^(<[A-Za-z0-9+\-:]*>|[A-Za-z]{3,})?([+-]?)(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?$"
)
# The largest offset either engine takes, in minutes (MySQL's +14:00).
_MAX_OFFSET_MINUTES = 14 * 60


@dataclass(frozen=True)
class SourceZone:
    """The source's time zone as the conversion writes it (:func:`resolve_source_zone`).

    ``kind`` is one of:

    * ``utc`` -- UTC itself (an equivalent name, a zero offset): nothing to write;
    * ``named`` -- a zone name; ``zone`` is the name the target reads ('Asia/Seoul');
    * ``offset`` -- a fixed offset; ``zone`` is it east of UTC, ISO-signed ('+09:00');
    * ``unknown`` -- not read, or not one of those: the conversion assumes UTC.

    ``setting`` is what the source reported (``time_zone 'Asia/Seoul'``), as the findings
    quote it; empty when nothing was read. Frozen, so a conversion cache can key on it."""

    kind: str
    zone: str = ""
    setting: str = ""

    @property
    def shifts(self) -> bool:
        """Whether the conversion writes this zone: a known zone that is not UTC."""
        return self.kind in ("named", "offset")

    @property
    def label(self) -> str:
        """The zone as a finding names it: ``'Asia/Seoul'`` or ``UTC+09:00``."""
        return f"UTC{self.zone}" if self.kind == "offset" else f"'{self.zone}'"

    def clock_sql(self, instant: str = "now()") -> str:
        """``instant`` (a timestamptz: ``now()``, ``clock_timestamp()``) as this zone's wall
        clock, a PostgreSQL ``timestamp`` expression. A fixed offset is added to the UTC wall
        clock: as a zone name it would be read with the POSIX sign."""
        if self.kind == "named":
            return f"({instant} AT TIME ZONE {_sql_string(self.zone)})"
        if self.kind == "offset":
            return f"(({instant} AT TIME ZONE 'UTC') + INTERVAL {_sql_string(self.zone)})"
        raise ValueError(f"no wall clock to write for a {self.kind} zone")

    def date_sql(self, instant: str = "now()") -> str:
        """The date of :meth:`clock_sql` (``CURRENT_DATE`` in this zone)."""
        return f"({self.clock_sql(instant)}::date)"

    def time_sql(self, instant: str = "now()") -> str:
        """The time of day of :meth:`clock_sql` (``LOCALTIME`` in this zone)."""
        return f"({self.clock_sql(instant)}::time)"


UNREAD_ZONE = SourceZone("unknown")


def init_connect_key(name: str) -> str:
    """The ``server_settings`` key of the value a MySQL source's ``init_connect`` sets for
    ``name`` (``time_zone (init_connect)``, G161): the SQL MySQL runs at the start of every
    session of a user without CONNECTION_ADMIN or SUPER -- the application's -- after the
    GLOBAL defaults, so such a session gets that value instead."""
    return f"{name} (init_connect)"


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _offset(sign: str, hours: int, minutes: int) -> Optional[str]:
    """``'+09:00'`` for an offset east of UTC; ``'+00:00'`` for zero; None out of range."""
    if minutes >= 60 or hours * 60 + minutes > _MAX_OFFSET_MINUTES:
        return None
    if hours == 0 and minutes == 0:
        return "+00:00"
    return f"{sign}{hours:02d}:{minutes:02d}"


def _named(value: str, setting: str) -> SourceZone:
    """A zone named ``value``: UTC, a name the target reads alike, or unknown."""
    folded = value.lower()
    if folded in _UTC_NAMES:
        return SourceZone("utc", "UTC", setting)
    if folded == "localtime" or not _ZONE_NAME_RE.match(value):
        return SourceZone("unknown", "", setting)
    return SourceZone("named", _ABBREVIATION_NAMED_ZONES.get(folded, value), setting)


def _mysql_zone(settings: Mapping[str, str]) -> SourceZone:
    value = settings.get("time_zone")
    assigned = settings.get(init_connect_key("time_zone"))
    if isinstance(assigned, str):
        # The zone init_connect sets is the one the application's sessions run in (G161).
        glob = f"; GLOBAL time_zone '{value.strip()}'" if isinstance(value, str) else ""
        return _mysql_zone_of(assigned.strip(), settings, f" set by init_connect{glob}")
    if not isinstance(value, str):
        return UNREAD_ZONE
    return _mysql_zone_of(value.strip(), settings, "")


def _mysql_zone_of(value: str, settings: Mapping[str, str], origin: str) -> SourceZone:
    """The zone of a MySQL ``time_zone`` value -- ``origin`` says where the session gets it,
    as the findings quote it."""
    setting = f"time_zone '{value}'{origin}"
    if value.upper() == "SYSTEM":
        # The server's own zone, which MySQL reports by its abbreviation at start-up
        # (system_time_zone 'KST', 'CEST', '+04'): only UTC can be told from that -- an
        # abbreviation names no single zone, and daylight saving may since have changed it.
        system = (settings.get("system_time_zone") or "").strip()
        setting += f", system_time_zone '{system}'" if system else ""
        if system.lower() in _UTC_NAMES:
            return SourceZone("utc", "UTC", setting)
        return SourceZone("unknown", "", setting)
    offset = _MYSQL_OFFSET_RE.match(value)
    if offset is not None:
        sign, hours, minutes = offset.group(1), int(offset.group(2)), int(offset.group(3))
        iso = _offset(sign, hours, minutes)
        if iso is None:
            return SourceZone("unknown", "", setting)
        if iso == "+00:00":
            return SourceZone("utc", "UTC", setting)
        return SourceZone("offset", iso, setting)
    return _named(value, setting)


def _postgres_zone(settings: Mapping[str, str]) -> SourceZone:
    value = settings.get("TimeZone")
    if not isinstance(value, str):
        return UNREAD_ZONE
    value = value.strip()
    setting = f"TimeZone '{value}'"
    posix = _POSIX_FIXED_RE.match(value)
    # A bare number is no POSIX string (SET TIME ZONE reads it ISO-signed, and shows it as
    # '<+09>-09'), so only an abbreviation or a sign makes one.
    if posix is not None and (posix.group(1) or posix.group(2)):
        if posix.group(5) not in (None, "00"):
            return SourceZone("unknown", "", setting)  # an offset with seconds
        west = posix.group(2) != "-"  # POSIX: a positive offset is WEST of UTC
        iso = _offset("-" if west else "+", int(posix.group(3)), int(posix.group(4) or 0))
        if iso is None:
            return SourceZone("unknown", "", setting)
        if iso == "+00:00":
            return SourceZone("utc", "UTC", setting)
        return SourceZone("offset", iso, setting)
    return _named(value, setting)


# A view's clock, as each engine prints a view body: MySQL's SHOW CREATE VIEW writes NOW()
# and its synonyms as now(), CURDATE() as curdate(), every name backquoted; PostgreSQL's
# pg_get_viewdef writes now(), CURRENT_TIMESTAMP, CURRENT_DATE, LOCALTIMESTAMP ... (a column
# named now is never followed by a parenthesis).
_MYSQL_VIEW_CLOCK_RE = re.compile(
    r"\b(?:now|sysdate|curdate|curtime)\s*\("
    r"|\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime)\b",
    re.IGNORECASE,
)
_PG_VIEW_CLOCK_RE = re.compile(
    r"\b(?:now|transaction_timestamp|statement_timestamp|clock_timestamp|timeofday)\s*\("
    r"|\b(?:current_timestamp|current_date|current_time|localtimestamp|localtime)\b",
    re.IGNORECASE,
)
# What holds no call: a string literal, a quoted name.
_MYSQL_QUOTED_RE = re.compile(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.|\"\")*\"|`(?:[^`]|``)*`")
_PG_QUOTED_RE = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")


def views_reading_the_clock(views: "list[ViewDef]", source_type: SourceType) -> "list[str]":
    """The views whose body calls a clock function (NOW(), CURDATE() ... / now(),
    CURRENT_DATE ...), outside its string literals and quoted names, in inventory order. A
    view with no body, or of a kind no view is created for, has none. Pure."""
    postgres = source_type is SourceType.POSTGRES
    quoted, clock = (
        (_PG_QUOTED_RE, _PG_VIEW_CLOCK_RE) if postgres else (_MYSQL_QUOTED_RE, _MYSQL_VIEW_CLOCK_RE)
    )
    return [
        view.name
        for view in views
        if not view.unsupported_kind
        and clock.search(quoted.sub(" ", view.definition or "")) is not None
    ]


def resolve_source_zone(settings: Mapping[str, str], source_type: SourceType) -> SourceZone:
    """The zone an application session on the source gets by default, from what Evaluation
    read (``SourceInventory.server_settings``). Pure.

    MySQL: the GLOBAL ``time_zone`` -- a zone name, a fixed offset ('+09:00'), or ``SYSTEM``,
    the server's own zone, known only when ``system_time_zone`` is UTC -- or the one the
    source's ``init_connect`` sets, which the application's sessions get (G161; an expression
    there names no zone that can be told). PostgreSQL: the
    ``TimeZone`` a new session gets -- a zone name, or a POSIX string with a fixed offset
    ('<+09>-09' is UTC+09:00); a POSIX rule with a daylight-saving part is not written. Anything not
    read, or not one of these, is ``unknown``."""
    if not isinstance(settings, Mapping):
        return UNREAD_ZONE
    if source_type is SourceType.POSTGRES:
        return _postgres_zone(settings)
    return _mysql_zone(settings)


__all__ = [
    "SourceZone", "UNREAD_ZONE", "init_connect_key", "resolve_source_zone", "views_reading_the_clock",
]
