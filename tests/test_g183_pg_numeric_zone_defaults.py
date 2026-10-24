# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G183: a numeric or INTERVAL ALTER DATABASE / ALTER ROLE TimeZone (follow-up to G164).

When the migration login has a TimeZone default of its own, G164 takes the source's default
from pg_db_role_setting -- the text ALTER DATABASE stored -- and that text was read as SHOW
output. PostgreSQL's check_timezone reads a value that is wholly a number as hours EAST of
UTC and an 'INTERVAL ...' as an interval east of UTC, while SHOW gives both in POSIX form
('<-05>+05', a positive offset WEST): so ALTER DATABASE app SET timezone = -5 (UTC-05:00, the
numeric form the PostgreSQL docs use) was read as UTC+05:00 -- clock defaults ten hours off,
and SET TIME ZONE INTERVAL '+05:00' advised, the wrong sign --, '+9' as UTC-09:00, and
INTERVAL '05:30:00' and 9.5 as unknown (UTC assumed).

Live on PostgreSQL 17.11 (throwaway container): pg_db_role_setting held -5, +9,
INTERVAL '05:30:00', 9.5, INTERVAL '-05:30:00', 0, INTERVAL 'PT5H45M' (IntervalStyle
iso_8601) and -3.5, and new sessions got UTC-05:00, +09:00, +05:30, +09:30, -05:30, UTC,
+05:45 and -03:30. Doubles here.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import _pg_role_zones_shift, check_postgres_time_zone
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_time_zone import resolve_source_zone
from tests.test_g148_source_server_settings import _PgConnection, _ProbeEngine, _probe

_PG = SourceType.POSTGRES


@pytest.mark.parametrize(
    ("value", "kind", "zone"),
    [
        ("-5", "offset", "-05:00"),
        ("+9", "offset", "+09:00"),
        ("9", "offset", "+09:00"),
        ("9.5", "offset", "+09:30"),
        ("-3.5", "offset", "-03:30"),
        ("0", "utc", "UTC"),
        ("INTERVAL '05:30:00'", "offset", "+05:30"),
        ("INTERVAL '-05:30:00'", "offset", "-05:30"),
        ("INTERVAL '00:00:00'", "utc", "UTC"),
        # SHOW's own forms are read as before: POSIX signs, a positive offset WEST.
        ("<-05>+05", "offset", "-05:00"),
        ("<+05:30>-05:30", "offset", "+05:30"),
        ("+09:00", "offset", "-09:00"),
        ("UTC+9", "offset", "-09:00"),
        ("America/New_York", "named", "America/New_York"),
        # What cannot be told is unknown (the conversion assumes UTC, an advisory item).
        ("5.3333", "unknown", ""),
        ("INTERVAL 'PT5H45M'", "unknown", ""),
        ("INTERVAL '05:30:15'", "unknown", ""),
    ],
)
def test_a_stored_timezone_is_read_as_check_timezone_reads_it(value: str, kind: str, zone: str) -> None:
    resolved = resolve_source_zone({"TimeZone": value}, _PG)
    assert (resolved.kind, resolved.zone) == (kind, zone)


def _clocks() -> TableDef:
    return TableDef(
        name="rev.clocks",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="t1", mysql_type="timestamp without time zone", default="now()"),
        ],
        primary_key=["id"],
    )


@pytest.mark.parametrize(
    ("stored", "offset"),
    [("-5", "-05:00"), ("+9", "+09:00"), ("INTERVAL '05:30:00'", "+05:30"), ("9.5", "+09:30")],
)
def test_the_login_fallback_writes_the_offset_the_database_gives(
    monkeypatch, stored: str, offset: str  # noqa: ANN001
) -> None:
    """ALTER DATABASE ... SET timezone = <stored>, the migration login set to Asia/Seoul."""
    _probe(monkeypatch, _ProbeEngine(("Asia/Seoul", "user")))
    settings = dialect_for(_PG).server_settings(
        _PgConnection([("", True, stored, False), ("migr", False, "Asia/Seoul", True)])
    )
    assert settings["TimeZone"] == stored
    inventory = SourceInventory(tables=[_clocks()], server_settings=settings)
    (table,) = SchemaConverter(source_type=_PG).convert(inventory).tables
    assert f"((now() AT TIME ZONE 'UTC') + INTERVAL '{offset}')" in " ".join(table.target_ddl.split())
    (item,) = check_postgres_time_zone(inventory)
    assert f"UTC{offset}" in item.risk
    assert f"SET TIME ZONE INTERVAL '{offset}' HOUR TO MINUTE" in item.recommendation


def test_a_role_set_to_a_zero_offset_is_utc() -> None:
    for value in ("0", "INTERVAL '00:00:00'"):
        assert not _pg_role_zones_shift({"TimeZone (ALTER ROLE app)": value})
    assert _pg_role_zones_shift({"TimeZone (ALTER ROLE app)": "-5"})
