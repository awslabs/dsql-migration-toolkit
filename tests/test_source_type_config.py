# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The ``source_type`` discriminator on ``SourceConnectionConfig``.

Phase 0 of PostgreSQL-source support: the config gains a ``source_type`` that
defaults to MySQL (so every existing caller/config is unchanged) and can be set to
PostgreSQL. The value later selects the source-reading dialect.
"""

import pytest
from pydantic import ValidationError

from dsql_migrator.core.models import SourceConnectionConfig, SourceType


def test_source_type_defaults_to_mysql() -> None:
    cfg = SourceConnectionConfig(host="db.example.com")
    assert cfg.source_type is SourceType.MYSQL
    # The MySQL default port is preserved (the UI supplies 5432 for PostgreSQL).
    assert cfg.port == 3306


def test_source_type_postgres_round_trips() -> None:
    cfg = SourceConnectionConfig(
        host="pg.example.com", port=5432, source_type=SourceType.POSTGRES
    )
    restored = SourceConnectionConfig.model_validate(cfg.model_dump())
    assert restored.source_type is SourceType.POSTGRES
    # A str-enum serializes to its plain value in JSON.
    assert '"postgres"' in cfg.model_dump_json()


def test_source_type_accepts_plain_string() -> None:
    cfg = SourceConnectionConfig.model_validate(
        {"host": "pg.example.com", "source_type": "postgres"}
    )
    assert cfg.source_type is SourceType.POSTGRES


def test_source_type_rejects_unknown_engine() -> None:
    with pytest.raises(ValidationError):
        SourceConnectionConfig(host="h", source_type="oracle")
