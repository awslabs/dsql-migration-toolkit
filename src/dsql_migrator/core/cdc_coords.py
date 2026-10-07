# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure validators/parsers for user-entered CDC start coordinates.

When the operator seeds a CDC run manually (no Full Load watermark in the
session, or a custom offset), they type either a MySQL GTID set or a binlog
``file:position`` into the UI. This module turns those raw strings into a
validated, structured form for :class:`~dsql_migrator.core.cdc.CdcResumePoint`.

Design choice -- **advisory, not blocking** (Property: user-convenience-first):
``validate_gtid``/``validate_binlog_file`` return an *optional message* describing
why a value looks wrong rather than raising, so the UI can show a hint while still
storing what the user entered. MySQL GTID sets have many legal shapes
(multi-source, multi-interval, whitespace, ``gtid_purged``-style comments), so the
structure is validated liberally. The one hard stop is at Start CDC: a Manual GTID
set that fails :func:`validate_gtid` is refused there on every ``gtid_mode``, because
Debezium writes the start point's set into every offset it commits and cannot read a
malformed one back at its next task start.

No dependency on the rest of the package -- these are standalone string helpers,
trivially unit-testable without any database or AWS access.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

# A single MySQL server UUID, e.g. 3E11FA47-71CA-11E1-9E33-C80AA9429562.
# Case-insensitive: MySQL prints upper-case but accepts either.
_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"

# One transaction interval: "5" or "1-100". A GTID set component is
# "<uuid>:<interval>[:<interval>...]" and a set is several of those joined by ",".
_INTERVAL = r"\d+(?:-\d+)?"
_GTID_COMPONENT = rf"{_UUID}(?::{_INTERVAL})+"
_GTID_SET_RE = re.compile(rf"^{_GTID_COMPONENT}(?:,{_GTID_COMPONENT})*$")

# A binlog file name: a base name, a dot, then a numeric suffix
# (e.g. "mysql-bin.000123", "binlog.000001"). No whitespace.
_BINLOG_FILE_RE = re.compile(r"^\S+\.\d+$")

# A PostgreSQL WAL LSN: two hex halves joined by '/', e.g. "3/AF012B8". This is the
# PG analog of MySQL's binlog file:position -- the coordinate a PostgreSQL CDC
# catch-up resumes from.
_WAL_LSN_RE = re.compile(r"^[0-9A-Fa-f]+/[0-9A-Fa-f]+$")


def _strip_noise(value: str) -> str:
    """Collapse whitespace and drop ``/* ... */`` comments from a coordinate.

    ``SHOW MASTER STATUS`` / ``gtid_purged`` output is often pasted with newlines
    or a leading comment; normalize it to a single comma-joined token so the
    structural regex can match.
    """
    no_comments = re.sub(r"/\*.*?\*/", "", value, flags=re.DOTALL)
    # Remove all whitespace (GTID sets never contain meaningful spaces).
    return "".join(no_comments.split())


def normalize_gtid_set(value: Optional[str]) -> str:
    """A GTID set with every ``/* ... */`` comment and all whitespace removed. Pure.

    ``""`` for ``None`` or a blank value. MySQL prints ``Executed_Gtid_Set`` and
    ``@@GLOBAL.gtid_executed`` with ``",\\n"`` between server UUIDs, and a pasted set can
    carry spaces or a comment. Debezium strips only ``\\n`` and ``\\r``, and the binlog
    client it hands the set to splits it on ``,`` and parses each server UUID without
    trimming, so a space left in the set fails the source connector at start. A GTID set
    never contains meaningful whitespace, so removing it changes nothing else.
    """
    if not value:
        return ""
    return _strip_noise(str(value))


# How many characters of a GTID set a log line or a notice shows before " …".
GTID_SUMMARY_LIMIT = 160


def gtid_set_summary(value: Optional[str], limit: int = GTID_SUMMARY_LIMIT) -> str:
    """A GTID set for a log line or a notice: normalized, capped at ``limit`` + ``" …"``.

    A set holds one interval list per server UUID that ever wrote to the source, so it can
    run to kilobytes; a line names its beginning only. Pure.
    """
    text = normalize_gtid_set(value)
    return text if len(text) <= limit else text[:limit] + " …"


def gtid_mode_is_on(value: Optional[str]) -> bool:
    """True when a MySQL ``gtid_mode`` value is exactly ``ON`` (case-insensitive). Pure.

    Debezium's own test (2.7.4 ``MySqlConnection.isGtidModeEnabled``): only ``ON`` makes it
    position by GTID set. ``ON_PERMISSIVE``, ``OFF_PERMISSIVE`` and ``OFF`` stream by
    binlog file:position, and MySQL refuses GTID auto-positioning unless ``gtid_mode`` is
    ``ON``.
    """
    return (value or "").strip().upper() == "ON"


def validate_gtid(value: str) -> Optional[str]:
    """Return an advisory message if ``value`` is not a well-formed GTID set.

    Returns ``None`` when the value looks like a valid set
    (``<uuid>:<interval>[,<uuid>:<interval>...]``). Whitespace and ``/*...*/``
    comments are tolerated. An empty string returns a message (callers that
    treat GTID as optional should check for blank before calling).
    """
    cleaned = _strip_noise(value)
    if not cleaned:
        return "Enter a GTID set, e.g. 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100."
    if not _GTID_SET_RE.match(cleaned):
        return (
            "This does not look like a MySQL GTID set "
            "(expected <uuid>:<interval>, e.g. "
            "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100)."
        )
    return None


def validate_binlog_file(value: str) -> Optional[str]:
    """Return an advisory message if ``value`` is not a binlog file name.

    Returns ``None`` for a name like ``mysql-bin.000123`` (a non-empty base, a
    dot, and a numeric suffix, no whitespace).
    """
    cleaned = value.strip()
    if not cleaned:
        return "Enter a binlog file name, e.g. mysql-bin.000123."
    if not _BINLOG_FILE_RE.match(cleaned):
        return "This does not look like a binlog file name (expected e.g. mysql-bin.000123)."
    return None


def parse_binlog_coordinate(value: str) -> Optional[Tuple[str, int]]:
    """Parse a ``file:position`` string into ``(file, position)``.

    Returns ``None`` when the value is blank, missing the ``:position`` part, has
    a non-integer or negative position, or whose file part fails
    :func:`validate_binlog_file`. The position is the last ``:``-separated field
    so binlog file names (which never contain ``:``) split unambiguously.
    """
    cleaned = value.strip()
    if not cleaned or ":" not in cleaned:
        return None
    file_part, _, pos_part = cleaned.rpartition(":")
    file_part = file_part.strip()
    pos_part = pos_part.strip()
    if validate_binlog_file(file_part) is not None:
        return None
    if not pos_part.isdigit():
        return None
    return file_part, int(pos_part)


def validate_wal_lsn(value: str) -> Optional[str]:
    """Return an advisory message if ``value`` is not a PostgreSQL WAL LSN.

    Returns ``None`` for a well-formed LSN like ``3/AF012B8`` (two hex halves joined
    by ``/``). Advisory, not blocking (matching :func:`validate_gtid`): a liberal
    shape check for the manual PostgreSQL CDC start position -- the connector remains
    the final authority. An empty string returns a message.
    """
    cleaned = value.strip()
    if not cleaned:
        return "Enter a WAL LSN, e.g. 3/AF012B8."
    if not _WAL_LSN_RE.match(cleaned):
        return "This does not look like a PostgreSQL WAL LSN (expected e.g. 3/AF012B8)."
    return None


__all__ = [
    "GTID_SUMMARY_LIMIT",
    "gtid_mode_is_on",
    "gtid_set_summary",
    "normalize_gtid_set",
    "validate_gtid",
    "validate_binlog_file",
    "parse_binlog_coordinate",
    "validate_wal_lsn",
]
