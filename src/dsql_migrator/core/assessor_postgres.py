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
rule (its guidance is entirely MySQL/Debezium-framed; PostgreSQL CDC uses its own
logical-replication readiness checks instead), and the view rule (its linter targets
MySQL application-query anti-patterns).

The DSQL-unsupported PostgreSQL TYPE rule IS included (``UnsupportedPostgresTypeRule``,
below) so Evaluation flags an unsupported column type the same as Schema Conversion does.
The remaining PG-specific refinements -- identity/serial notes and GIN/GiST/BRIN index
methods -- and stored trigger/function/event flagging (which depends on PostgreSQL-catalog
enrichment) are still later refinements.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dsql_migrator.core.assessor import KIND_TABLE, Finding, ObjectKey, Rule
from dsql_migrator.core.models import Classification, EffortLevel

if TYPE_CHECKING:
    from dsql_migrator.core.models import SourceInventory


class UnsupportedPostgresTypeRule(Rule):
    """Flag columns whose PostgreSQL type Aurora DSQL does not support as a column type.

    Surfaces at Evaluation (Step 1) the SAME DSQL-unsupported PG column types the Schema
    Conversion step warns about via ``unsupported_dsql_reason`` (arrays, geometric,
    network, xml, money, bit, range, tsvector, enum/composite, pgvector). Without it a
    table using such a type reads AUTO at Evaluation and the problem only appears at
    Schema Conversion. Reuses that single source of truth so the two steps never drift.
    PostgreSQL-source only.
    """

    rule_id = "PG_UNSUPPORTED_TYPE"

    def evaluate(self, inventory: "SourceInventory") -> "list[Finding]":
        from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

        findings: list[Finding] = []
        for table in inventory.tables:
            bad = [
                (col.name, col.mysql_type)
                for col in table.columns
                if unsupported_dsql_reason(col.mysql_type) is not None
            ]
            if not bad:
                continue
            cols = ", ".join(f"{name} ({typ})" for name, typ in bad)
            findings.append(
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.UNSUPPORTED,
                    risk=(
                        f"Column(s) {cols} use PostgreSQL types Aurora DSQL does not "
                        "support as column types, so this table's CREATE would be "
                        "rejected as-is."
                    ),
                    recommendation=(
                        "Remodel each to a DSQL-supported type before migrating; Schema "
                        "Conversion names the target per type (array -> jsonb or a child "
                        "table; inet/cidr/xml/tsvector/bit -> text; money -> numeric; "
                        "range -> text; enum -> text; composite -> columns or jsonb)."
                    ),
                    effort=EffortLevel.MEDIUM,
                )
            )
        return findings


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
        # PG-specific: DSQL-unsupported column types (the only UNSUPPORTED-level rule here;
        # first so it is prominent, though severity ordering already makes it win ties).
        UnsupportedPostgresTypeRule(),
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
