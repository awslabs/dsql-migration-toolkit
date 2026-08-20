# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source compatibility rules for the Evaluation step.

Kept in its own module -- not tangled into ``assessor.py``'s MySQL rules -- so each
source engine's assessment rules stay separate.

v1 is the **source-neutral, target-DSQL structural** rule set: foreign keys, check
constraints, triggers / procedures / events, missing primary key, partitioning, and the
DSQL column / index / key-column-count limits. These read structural inventory fields
(not source type strings), so they are correct for a PostgreSQL source as-is.

Excluded from v1 (all inspect MySQL specifics, so they would misfire or mislead on a
PostgreSQL source): the MySQL type/feature rules (ENUM/SET, TINYINT(1), BIT, YEAR,
AUTO_INCREMENT, MySQL collation, MySQL spatial + LOB, ON UPDATE CURRENT_TIMESTAMP, MySQL
index types, DECIMAL-precision parsed from a MySQL type), the MySQL-binlog CDC cascade
rule (its guidance is entirely MySQL/Debezium-framed; PostgreSQL CDC is deferred), and
the view rule (its linter targets MySQL application-query anti-patterns).

PostgreSQL-specific TYPE / view / index rules (DSQL-unsupported PG types, identity/serial
notes, GIN/GiST/BRIN index methods) are a later refinement, and stored
trigger/function/event flagging depends on PostgreSQL-catalog enrichment (also later).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dsql_migrator.core.assessor import Rule


def default_rules() -> "list[Rule]":
    """Ordered PostgreSQL-source compatibility rules (v1: structural, source-neutral).

    Reuses the shared, source-neutral rule classes from :mod:`assessor` (imported
    lazily to avoid an import cycle: ``assessor.default_rules`` delegates here). Order
    is significant -- it breaks classification ties -- and mirrors the shared rules'
    order in the MySQL list.
    """
    from dsql_migrator.core.assessor import (
        CheckConstraintRule,
        EventRule,
        ForeignKeyRule,
        NoPrimaryKeyRule,
        PartitionedTableRule,
        ProcedureRule,
        TooManyColumnsRule,
        TooManyIndexesRule,
        TooManyKeyColumnsRule,
        TriggerRule,
    )

    return [
        ForeignKeyRule(),
        CheckConstraintRule(),
        TriggerRule(),
        ProcedureRule(),
        EventRule(),
        NoPrimaryKeyRule(),
        PartitionedTableRule(),
        TooManyColumnsRule(),
        TooManyIndexesRule(),
        TooManyKeyColumnsRule(),
    ]


__all__ = ["default_rules"]
