# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Operator-approved ADD COLUMN recovery for CDC source-schema drift.

CDC does not propagate DDL: when the source runs ``ALTER TABLE ... ADD COLUMN``
the target is unchanged, so the first row written under the new source schema is
rejected by Aurora DSQL (SQLSTATE 42703) and dead-lettered. Phase 1 detects and
classifies that (``core.cdc.classify_schema_drift`` -> the DLQ panel's "source
schema change detected" banner) but deliberately stops there. This module is the
opt-in recovery for the dominant case: it computes the missing columns, renders
the exact ``ALTER TABLE ... ADD COLUMN`` statements, and applies them **only**
after a human approves them.

Design constraints this module enforces (they are not incidental):

* **ADD COLUMN only.** A source DROP COLUMN or an incompatible type change is
  never auto-repaired -- those stay alert-only, because reconciling them can
  destroy or rewrite target data. Only additive evolution is safe to offer.
* **No silent schema mutation (Property 6).** Nothing here runs on the CDC hot
  path or on a timer. :func:`plan_add_columns` is pure and produces DDL *text* for
  review; :func:`apply_add_columns` runs only when the caller (the UI, after an
  explicit confirmation) hands it that plan.
* **Nullable, no default.** A new column is added as NULLable with no default.
  Adding ``NOT NULL`` without a default to a table that already has rows is
  rejected by the engine, and inventing a default would silently fabricate data
  for the rows CDC already applied. Existing rows therefore read NULL until the
  operator backfills them (per-table Reload), while new change events carry the
  real value.
* **Never guess a type.** A source type that the converter cannot map is skipped
  and reported (:class:`SkippedColumn`), not approximated.
* **One DDL per transaction.** Aurora DSQL rejects two DDL statements in one
  transaction ("multiple ddl statements not supported in a transaction" --
  verified live), so each ``ALTER`` is executed on its own autocommit connection,
  mirroring :mod:`dsql_migrator.core.schema_applier`.

The planner is deliberately IO-free so the mapping/ordering/skip rules are unit
tested without a MySQL or DSQL connection; the two read helpers and the applier
hold the only IO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Sequence

from sqlglot import exp

from dsql_migrator.core.converter import map_mysql_type
from dsql_migrator.core.converter_postgres import (
    clamp_pg_numeric,
    unsupported_dsql_reason,
)
from dsql_migrator.core.models import SourceType

# Mirrors converter._quote_pg_identifier. Kept local like exporter/validator keep
# their own MySQL quoter: the rule is one line and a local copy cannot drift out
# from under this module's rendered DDL.
_POSTGRES = "postgres"


def _quote_ident(name: str) -> str:
    """Return ``name`` as a safely double-quoted PostgreSQL/DSQL identifier."""
    return exp.to_identifier(name, quoted=True).sql(dialect=_POSTGRES)


def _quote_qualified(name: str) -> str:
    """Double-quote a possibly schema-qualified name as ``"schema"."table"``."""
    if "." in name:
        schema, _, obj = name.partition(".")
        return f"{_quote_ident(schema)}.{_quote_ident(obj)}"
    return _quote_ident(name)


@dataclass(frozen=True)
class AddColumnStep:
    """One reviewed ``ALTER TABLE ... ADD COLUMN`` to run on the target."""

    column: str
    source_type: str
    target_type: str
    ddl: str
    warning: Optional[str] = None


@dataclass(frozen=True)
class SkippedColumn:
    """A source column that is NOT offered, with why (never silently dropped)."""

    column: str
    source_type: str
    reason: str


@dataclass(frozen=True)
class AddColumnPlan:
    """The reviewable result of diffing one table's source and target columns."""

    table: str
    steps: tuple[AddColumnStep, ...] = ()
    skipped: tuple[SkippedColumn, ...] = ()

    @property
    def is_empty(self) -> bool:
        """True when there is nothing to apply (target already has every column)."""
        return not self.steps

    @property
    def ddl_text(self) -> str:
        """The plan's statements as one reviewable block, one per line."""
        return "\n".join(step.ddl for step in self.steps)


@dataclass(frozen=True)
class AddColumnOutcome:
    """Per-statement result of applying a plan (ordered like the plan)."""

    column: str
    ddl: str
    applied: bool
    error: Optional[str] = None


def _resolve_target_type(
    source_type_str: str, source_engine: SourceType
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Map one source column type to its Aurora DSQL type.

    Returns ``(target_type, warning_message, skip_reason)`` -- exactly one of
    ``target_type`` / ``skip_reason`` is set. MySQL types go through the shared
    ``map_mysql_type`` remap; a PostgreSQL type is near-identity (it IS a PG type
    already), so it is emitted verbatim except that a DSQL-unsupported PG type is
    skipped (never approximated) and an over-precise numeric is clamped with a note --
    mirroring the converter's PG table path.
    """
    if source_engine is SourceType.POSTGRES:
        reason = unsupported_dsql_reason(source_type_str)
        if reason is not None:
            return None, None, reason
        clamped, clamp_note = clamp_pg_numeric(source_type_str)
        return clamped, clamp_note, None
    try:
        target_type, warning = map_mysql_type(source_type_str)
    except ValueError as exc:
        return None, None, f"no Aurora DSQL mapping for this type ({exc})"
    return target_type, (warning.message if warning is not None else None), None


def plan_add_columns(
    table: str,
    source_columns: Sequence[tuple[str, str]],
    target_columns: Iterable[str],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> AddColumnPlan:
    """Diff a table's columns and render the additive DDL the target is missing.

    ``source_columns`` is ``[(column_name, source_column_type), ...]`` in the
    source's ordinal order, and ``target_columns`` is the column names the target
    already has. Comparison is case-insensitive on the target side because the
    converter lower-cases identifiers when it creates the target table, so a source
    ``Full_Name`` maps to a target ``full_name`` and must not be reported as missing.

    ``source_type`` selects how each column type is mapped: a MySQL type is remapped
    to its DSQL equivalent; a PostgreSQL type is emitted near-verbatim (DSQL-unsupported
    PG types are skipped, over-precise numerics clamped) -- matching the schema
    converter, so an ADD COLUMN recovery on a PostgreSQL source produces PG DDL instead
    of erroring through the MySQL remap.

    Pure: no connection, no clock, no IO. Ordering follows the source so a
    multi-column ALTER sequence reads like the source's own change history.
    """
    have = {name.lower() for name in target_columns}
    steps: list[AddColumnStep] = []
    skipped: list[SkippedColumn] = []
    for column, source_column_type in source_columns:
        if column.lower() in have:
            continue
        target_type, warning, skip_reason = _resolve_target_type(
            source_column_type, source_type
        )
        if target_type is None:
            # Unsupported/unmappable source type: report it, never approximate.
            skipped.append(
                SkippedColumn(
                    column=column,
                    source_type=source_column_type,
                    reason=skip_reason or "no Aurora DSQL mapping for this type",
                )
            )
            continue
        steps.append(
            AddColumnStep(
                column=column,
                source_type=source_column_type,
                target_type=target_type,
                # Nullable, no default -- see the module docstring.
                ddl=(
                    f"ALTER TABLE {_quote_qualified(table)} "
                    f"ADD COLUMN {_quote_ident(column)} {target_type}"
                ),
                warning=warning,
            )
        )
    return AddColumnPlan(table=table, steps=tuple(steps), skipped=tuple(skipped))


def read_source_columns(
    connection, table: str, *, source_type: SourceType = SourceType.MYSQL
) -> list[tuple[str, str]]:
    """Read ``[(column_name, source_type)]`` for ``schema.table`` from the source.

    For MySQL this reads ``COLUMN_TYPE`` (not ``DATA_TYPE``) from
    ``information_schema.columns`` -- it keeps the precision/length/unsigned detail
    the mapping depends on (the same reason ``introspector.enrich_columns`` prefers
    it). For PostgreSQL it reads ``format_type(atttypid, atttypmod)`` from
    ``pg_attribute`` -- the exact PG type string the converter expects (PG's
    ``information_schema`` has no ``COLUMN_TYPE`` and loses array/precision detail).
    Read-only (Property 1: the source is never modified, and this is scan-free).
    """
    schema, _, name = table.partition(".")
    if source_type is SourceType.POSTGRES:
        with connection.cursor() as cursor:
            if schema:
                cursor.execute(
                    "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = %s AND n.nspname = %s "
                    "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                    (name, schema),
                )
            else:
                cursor.execute(
                    "SELECT a.attname, format_type(a.atttypid, a.atttypmod) "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = %s AND n.nspname NOT IN "
                    "('pg_catalog', 'information_schema', 'pg_toast') "
                    "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                    (name,),
                )
            return [(row[0], row[1]) for row in cursor.fetchall()]
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.columns "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION",
            (schema, name),
        )
        return [(row[0], row[1]) for row in cursor.fetchall()]


def read_target_columns(connection, table: str) -> list[str]:
    """Read the column names ``schema.table`` currently has on the target."""
    schema, _, name = table.partition(".")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, name),
        )
        return [row[0] for row in cursor.fetchall()]


def apply_add_columns(
    plan: AddColumnPlan,
    connection_factory: Callable[[], object],
) -> tuple[AddColumnOutcome, ...]:
    """Apply a reviewed plan, one DDL per transaction, stopping at the first error.

    ``connection_factory`` returns a fresh autocommit DSQL connection per call --
    the same shape :mod:`schema_applier` uses, because Aurora DSQL rejects two DDL
    statements in one transaction. Execution stops at the first failure so a
    partially-evolved table is reported honestly instead of the caller having to
    guess which statements ran; the columns already added are durable (each ALTER
    committed on its own), so re-running the plan is safe -- a column that now
    exists simply drops out of the next :func:`plan_add_columns`.
    """
    outcomes: list[AddColumnOutcome] = []
    for step in plan.steps:
        try:
            connection = connection_factory()
        except Exception as exc:  # noqa: BLE001 - surfaced to the operator verbatim
            outcomes.append(
                AddColumnOutcome(
                    column=step.column, ddl=step.ddl, applied=False, error=str(exc)
                )
            )
            break
        try:
            with connection.cursor() as cursor:  # type: ignore[attr-defined]
                cursor.execute(step.ddl)
            outcomes.append(
                AddColumnOutcome(column=step.column, ddl=step.ddl, applied=True)
            )
        except Exception as exc:  # noqa: BLE001 - one bad type must not hide the rest
            outcomes.append(
                AddColumnOutcome(
                    column=step.column, ddl=step.ddl, applied=False, error=str(exc)
                )
            )
            break
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:  # noqa: BLE001 - best effort
                    pass
    return tuple(outcomes)


__all__ = [
    "AddColumnOutcome",
    "AddColumnPlan",
    "AddColumnStep",
    "SkippedColumn",
    "apply_add_columns",
    "plan_add_columns",
    "read_source_columns",
    "read_target_columns",
]
