# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ``SourceDialect`` adapter and its ``dialect_for`` registry (Phase 0).

Phase 0 introduces the seam with MySQL as the sole, byte-identical dialect: it must
report the same driver scheme / system schemas / engine kwargs the introspector used
inline before, so routing the engine factories through it changes nothing.
"""

import pytest

from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect import (
    MySQLSourceDialect,
    dialect_for,
)


def test_dialect_for_mysql_returns_mysql_dialect() -> None:
    d = dialect_for(SourceType.MYSQL)
    assert isinstance(d, MySQLSourceDialect)
    assert d.source_type is SourceType.MYSQL


def test_dialect_for_is_a_singleton() -> None:
    assert dialect_for(SourceType.MYSQL) is dialect_for(SourceType.MYSQL)


def test_mysql_dialect_connection_constants_match_introspector() -> None:
    from dsql_migrator.core.introspector import MYSQL_DRIVER, MYSQL_SYSTEM_SCHEMAS

    d = dialect_for(SourceType.MYSQL)
    assert d.driver_scheme == MYSQL_DRIVER == "mysql+pymysql"
    assert d.default_port == 3306
    assert d.system_schemas == MYSQL_SYSTEM_SCHEMAS
    assert "information_schema" in d.system_schemas


def test_mysql_dialect_engine_kwargs_match_source_engine_kwargs() -> None:
    from dsql_migrator.core.introspector import source_engine_kwargs

    d = dialect_for(SourceType.MYSQL)
    assert d.engine_kwargs() == source_engine_kwargs()
    assert d.engine_kwargs(read_timeout_seconds=30) == source_engine_kwargs(
        read_timeout_seconds=30
    )


def test_dialect_for_postgres_not_yet_registered() -> None:
    # PostgreSQL gets its dialect in a later phase; until then it fails loudly rather
    # than silently reading as MySQL.
    with pytest.raises(NotImplementedError):
        dialect_for(SourceType.POSTGRES)
