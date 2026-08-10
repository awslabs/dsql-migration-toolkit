# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema (DDL) converter: MySQL -> Aurora DSQL (PostgreSQL 16) type mapping.

This module implements the foundation of the :class:`SchemaConverter` component
(design.md section "3. Schema Converter"): a declarative MySQL -> DSQL type
mapping plus identifier-quoting conversion via ``sqlglot`` transpilation
(Requirements 3.1, 3.2).

The converter parses each source table with ``sqlglot`` in the ``mysql`` dialect,
rewrites column data types according to the DSQL type-mapping table, and renders
the result in the ``postgres`` dialect. Identifier quoting (MySQL backticks ->
PostgreSQL double quotes) is handled by the ``postgres`` renderer.

Type mapping table (MySQL -> DSQL/PostgreSQL):

==================  =====================  ====================================
MySQL               DSQL (PostgreSQL)      Notes
==================  =====================  ====================================
``TINYINT(1)``      ``boolean``            Semantic mapping (MANUAL warning).
``TINYINT UNSIGNED``  ``smallint``         Widened to preserve the 0..255 range.
``SMALLINT UNSIGNED`` ``integer``          Widened to preserve range.
``MEDIUMINT UNSIGNED``  ``integer``        Widened to preserve range.
``INT UNSIGNED``    ``bigint``             Widened to preserve range.
``BIGINT UNSIGNED`` ``numeric(20, 0)``     No wider integer; range preserved.
``DATETIME``        ``timestamp``          UTC normalization.
``BLOB``/``*BLOB``  ``bytea``              All BLOB sizes map to ``bytea``.
``ENUM``            ``text`` + CHECK        DSQL has no ENUM (MANUAL warning).
``SET``             ``text``               No lossless mapping (MANUAL warning).
``JSON``            ``json``               Rendered directly by ``sqlglot``.
==================  =====================  ====================================

Property 6 (no silent data loss): when a mapping is semantic or lossy rather
than strictly lossless, the converter does not convert silently. It records a
structured :class:`ConversionWarning` (classified ``MANUAL``/``UNSUPPORTED``)
alongside the converted DDL.

DSQL constraints (Requirements 3.3, 3.4, 3.5, 3.7) are applied on top of the
type-mapping foundation:

- Foreign keys are removed from the target DDL and preserved as referential
  metadata (``TableConversion.preserved_foreign_keys``) so that referential
  integrity can be enforced in the application layer (Requirement 3.3).
- Secondary indexes are emitted as separate ``CREATE INDEX ASYNC`` statements
  (``TableConversion.index_ddls``), matching DSQL asynchronous index creation
  (Requirement 3.4).
- A primary-key strategy (:class:`PrimaryKeyStrategy`) controls how a
  monotonic ``AUTO_INCREMENT`` key is handled, and the converter emits
  hot-partition and primary-key-required warnings (Requirement 3.5).
- Triggers and procedural stored procedures/functions are never auto-converted;
  they are flagged as manual reimplementation targets (Requirement 3.7).

Execution-unit output (Requirement 3.6) builds on the per-table DDL above: a
:class:`SchemaConversionResult` is flattened into an ordered list of
:class:`ExecutionUnit` objects, each holding exactly one DDL statement so it can
run in its own transaction (DSQL allows only one DDL statement per transaction).
Schema conversion emits DDL units only; data DML is produced separately by the
data migrator, keeping the DDL/DML separation rule intact (Property 2).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import sqlglot
from sqlglot import exp
from pydantic import BaseModel, ConfigDict, Field, model_validator

from dsql_migrator.core.models import (
    Classification,
    ConversionNoteKind,
    ForeignKeyDef,
    SourceInventory,
    TableDef,
    ViewDef,
)

# Cache size applied to an identity primary key under the IDENTITY_WITH_CACHE
# strategy. A per-node cache hands out blocks of values so concurrent inserts do
# not contend on a single hot key range in DSQL.
#
# MUST be 1 or >= 65536: Aurora DSQL requires CACHE to be stated explicitly and accepts
# only those two ranges (documented on CREATE SEQUENCE, and confirmed live -- CACHE 100
# fails with "CACHE (100) must be greater than or equal to 65536 or equal to 1"). The
# previous value of 100 was in neither range, so every table converted with this strategy
# produced DDL that Aurora DSQL rejected outright. 65536 is the smallest accepted cached
# value, which is also the right pick here: the whole point of the strategy is to spread
# concurrent inserts, and CACHE 1 is the non-cached behavior it exists to avoid.
_IDENTITY_CACHE_SIZE = 65536

# Aurora DSQL numeric limits, confirmed live: "NUMERIC precision 39 must be between 1 and
# 38" and "NUMERIC scale 38 must be between 0 and 37". MySQL allows DECIMAL up to
# precision 65 / scale 30, so a wide source column produces DDL that DSQL rejects
# outright unless it is clamped here.
_DSQL_NUMERIC_MAX_PRECISION = 38
_DSQL_NUMERIC_MAX_SCALE = 37


def _clamp_numeric_spec(params: list) -> tuple[str, Optional[str]]:
    """Clamp a DECIMAL(p[,s]) spec to what Aurora DSQL accepts.

    Returns ``(spec_text, warning)``. Clamping the PRECISION loses range and clamping the
    SCALE loses decimal places, so either change is reported -- the alternative is DDL
    that fails to apply, which is strictly worse, but the operator still has to know the
    column can no longer hold every source value.
    """
    values: list[int] = []
    for param in params:
        text = param.sql(dialect=_MYSQL).strip()
        try:
            values.append(int(text))
        except ValueError:
            # Not a plain integer (unexpected); leave the spec untouched.
            return ", ".join(p.sql(dialect=_MYSQL) for p in params), None
    if not values:
        return "", None

    precision = values[0]
    scale = values[1] if len(values) > 1 else None
    notes: list[str] = []

    if precision > _DSQL_NUMERIC_MAX_PRECISION:
        notes.append(
            f"precision {precision} exceeds the Aurora DSQL maximum of "
            f"{_DSQL_NUMERIC_MAX_PRECISION} and was reduced"
        )
        precision = _DSQL_NUMERIC_MAX_PRECISION
    if scale is not None:
        if scale > _DSQL_NUMERIC_MAX_SCALE:
            notes.append(
                f"scale {scale} exceeds the Aurora DSQL maximum of "
                f"{_DSQL_NUMERIC_MAX_SCALE} and was reduced"
            )
            scale = _DSQL_NUMERIC_MAX_SCALE
        # The scale can never exceed the (possibly reduced) precision.
        if scale > precision:
            notes.append(f"scale was further reduced to {precision} to fit the precision")
            scale = precision

    spec = f"{precision}" if scale is None else f"{precision}, {scale}"
    warning = None
    if notes:
        warning = (
            "DECIMAL "
            + "; ".join(notes)
            + ". Values needing the original precision/scale will not fit -- review this "
            "column before migrating."
        )
    return spec, warning


# Aurora DSQL hard limits on primary/secondary keys (see the DSQL quotas docs).
# Used to validate a COMPOSITE_KEY request so the converter never emits DDL that
# DSQL would reject at CREATE/INSERT time. These are not configurable in DSQL.
_DSQL_MAX_PK_COLUMNS = 8  # >8 -> error 54011 "more than 8 column keys ..."
_DSQL_MAX_KEY_BYTES = 1024  # combined key >1 KiB -> error 54000 "key size too large"

_MYSQL = "mysql"
_POSTGRES = "postgres"

_DType = exp.DataType.Type


# ---------------------------------------------------------------------------
# Conversion result models
# ---------------------------------------------------------------------------

# ``ConversionNoteKind`` now lives in core.models because Evaluation needs it too; it is
# imported above and re-exported here so existing ``from ...converter import
# ConversionNoteKind`` call sites keep working.


class ConversionWarning(BaseModel):
    """A structured warning emitted when a conversion is not lossless or safe.

    Property 6 (no silent data loss): semantic or lossy type mappings, and DSQL
    structural constraints (removed foreign keys, primary-key requirements,
    hot-partition risk, triggers/routines that need reimplementation), are
    surfaced here instead of being applied silently. ``classification`` reuses
    the shared :class:`Classification` enum (``MANUAL``/``UNSUPPORTED``).

    ``source_type``/``target_type`` carry type context for column-level type
    mappings and are ``None`` for object-level warnings (e.g., a removed foreign
    key or a missing primary key) that are not about a single column's type.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1, description="Owning object name.")
    column_name: Optional[str] = Field(
        default=None, description="Column the warning applies to, if any."
    )
    source_type: Optional[str] = Field(
        default=None, description="Original MySQL type, for type-level warnings."
    )
    target_type: Optional[str] = Field(
        default=None, description="Mapped DSQL type, or None when not type-level."
    )
    classification: Classification = Field(
        description="Severity: MANUAL (review) or UNSUPPORTED (redesign)."
    )
    kind: "ConversionNoteKind" = Field(
        default_factory=lambda: ConversionNoteKind.LOSS,
        description=(
            "Whether this note reports something the conversion could not carry "
            "over (LOSS -- the default, and what every note historically meant) or "
            "advice on an otherwise-complete conversion (RECOMMENDATION). The UI "
            "shows the two in separate sections so advice is not styled as a problem."
        ),
    )
    message: str = Field(min_length=1, description="Human-readable reason (English).")


class TableConversion(BaseModel):
    """The converted target DDL for a single source table plus its warnings.

    ``index_ddls`` holds the table's secondary indexes rendered as separate
    ``CREATE INDEX ASYNC`` statements (Requirement 3.4). ``preserved_foreign_keys``
    keeps the source foreign keys that were removed from ``target_ddl`` so that
    referential integrity can be re-established in the application layer
    (Requirement 3.3).
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    target_ddl: str = Field(min_length=1)
    schema_ddls: list[str] = Field(
        default_factory=list,
        description=(
            "CREATE SCHEMA IF NOT EXISTS statements for the table's schema "
            "(empty when the table name is unqualified). Applied before the "
            "table so a qualified table lands in its schema."
        ),
    )
    index_ddls: list[str] = Field(
        default_factory=list,
        description="CREATE INDEX ASYNC statements for the table's indexes.",
    )
    preserved_foreign_keys: list[ForeignKeyDef] = Field(
        default_factory=list,
        description="Foreign keys removed from the DDL, kept as referential metadata.",
    )
    warnings: list[ConversionWarning] = Field(default_factory=list)


class ViewConversion(BaseModel):
    """The converted target DDL for a single source view plus its warnings.

    Aurora DSQL is PostgreSQL-compatible and supports views, so a view's MySQL
    definition is transpiled to a PostgreSQL ``CREATE VIEW`` (re-targeted to the
    view's possibly schema-qualified name). ``schema_ddls`` create the view's
    schema first (empty for an unqualified name). ``auto_converted`` is ``False``
    when the definition is missing or could not be parsed/transpiled -- the view
    is then surfaced for manual reimplementation (``target_ddl`` is a comment
    placeholder) with a ``MANUAL`` warning, never silently broken DDL.
    """

    model_config = ConfigDict(extra="forbid")

    view: str = Field(min_length=1)
    target_ddl: str = Field(min_length=1)
    schema_ddls: list[str] = Field(
        default_factory=list,
        description=(
            "CREATE SCHEMA IF NOT EXISTS statements for the view's schema "
            "(empty when the view name is unqualified)."
        ),
    )
    auto_converted: bool = Field(
        default=True,
        description="False when the view needs manual reimplementation.",
    )
    warnings: list[ConversionWarning] = Field(default_factory=list)


class PrimaryKeyStrategy(str, Enum):
    """Strategy for handling a monotonic (``AUTO_INCREMENT``) primary key.

    DSQL stores rows in primary-key order, so a monotonically increasing key
    concentrates writes on one key range (a hot partition). The strategy chooses
    how the converter rewrites such a key (Requirement 3.5):

    - ``KEEP_INTEGER``: keep the integer key unchanged and warn about the
      hot-partition risk. The safest default: it never silently changes the key
      type, leaving the choice to the user.
    - ``CONVERT_TO_UUID``: change the key column to ``uuid`` so values are
      randomly distributed; the application must supply UUID key values.
    - ``IDENTITY_WITH_CACHE``: keep an integer key but generate it as an identity
      column with a per-node cache so concurrent inserts do not contend on one
      key range.
    - ``COMPOSITE_KEY``: prepend a high-cardinality existing column to the primary
      key, making it ``(leading_column, original_pk...)`` so DSQL scatters writes
      across partitions instead of funnelling them to one key range. The source
      data is unchanged; only the target key definition changes. The application's
      queries/joins/upserts must key on the new composite key, and the leading
      column must be immutable (DSQL primary keys cannot change after creation, and
      CDC keys on it). Opt-in and per-table -- never the default.
    """

    KEEP_INTEGER = "KEEP_INTEGER"
    CONVERT_TO_UUID = "CONVERT_TO_UUID"
    IDENTITY_WITH_CACHE = "IDENTITY_WITH_CACHE"
    COMPOSITE_KEY = "COMPOSITE_KEY"


class SchemaConvertOptions(BaseModel):
    """Options controlling schema conversion.

    ``primary_key_strategy`` controls how a monotonic ``AUTO_INCREMENT`` primary
    key is rewritten for DSQL (Requirement 3.5). The default (``KEEP_INTEGER``)
    is the least invasive: it preserves the source key type and surfaces a
    hot-partition warning rather than silently changing the schema.

    ``composite_leading_column`` names the existing column to prepend to the
    primary key when ``primary_key_strategy`` is ``COMPOSITE_KEY``. It is required
    for -- and only valid with -- that strategy; the model validator enforces that
    pairing so a caller cannot silently request a composite key without a leading
    column (or set a leading column that no strategy would use).
    """

    model_config = ConfigDict(extra="forbid")

    primary_key_strategy: PrimaryKeyStrategy = Field(
        default=PrimaryKeyStrategy.KEEP_INTEGER,
        description="How to handle a monotonic AUTO_INCREMENT primary key.",
    )
    composite_leading_column: Optional[str] = Field(
        default=None,
        description=(
            "Existing column to prepend to the primary key for COMPOSITE_KEY; "
            "required for and only valid with that strategy."
        ),
    )

    @model_validator(mode="after")
    def _check_composite_pairing(self) -> "SchemaConvertOptions":
        is_composite = self.primary_key_strategy is PrimaryKeyStrategy.COMPOSITE_KEY
        has_leading = self.composite_leading_column is not None
        if is_composite and not has_leading:
            raise ValueError(
                "COMPOSITE_KEY requires composite_leading_column to name the "
                "column to prepend to the primary key."
            )
        if has_leading and not is_composite:
            raise ValueError(
                "composite_leading_column is only valid with the COMPOSITE_KEY "
                "primary_key_strategy."
            )
        return self


class ExecutionUnitKind(str, Enum):
    """The kind of statement an :class:`ExecutionUnit` carries.

    Schema conversion produces ``DDL`` units only. The enum exists to make the
    DDL/DML separation boundary explicit in the result type (Property 2): data
    DML is emitted by the data migrator, never interleaved into these units.
    """

    DDL = "DDL"


class ExecutionUnit(BaseModel):
    """A single statement to execute in its own transaction.

    DSQL allows only one DDL statement per transaction, so each execution unit
    holds exactly one statement (Requirement 3.6 / Property 2). ``object_name``
    identifies the owning object (the table the statement creates or indexes) for
    ordering and reporting.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1, description="Owning object name.")
    kind: ExecutionUnitKind = Field(
        default=ExecutionUnitKind.DDL,
        description="Statement kind; schema conversion emits DDL only.",
    )
    sql: str = Field(min_length=1, description="The single DDL statement (no terminator).")


class SchemaConversionResult(BaseModel):
    """Result of converting a whole source inventory's tables."""

    model_config = ConfigDict(extra="forbid")

    tables: list[TableConversion] = Field(default_factory=list)
    views: list[ViewConversion] = Field(
        default_factory=list,
        description="Converted views (PostgreSQL CREATE VIEW), applied after tables.",
    )
    warnings: list[ConversionWarning] = Field(
        default_factory=list,
        description="All table and object warnings aggregated for convenience.",
    )

    def execution_units(self) -> list[ExecutionUnit]:
        """Flatten the conversion into an ordered list of single-DDL units.

        Each unit carries exactly one DDL statement, so a unit is the boundary of
        one transaction when applied: DSQL allows only one DDL statement per
        transaction (Requirement 3.6 / Property 2). Producing the ordered,
        DDL-only units is this method's only job; running each unit in its own
        transaction is the Schema Applier's responsibility (Task 15).

        Ordering: ``CREATE SCHEMA IF NOT EXISTS`` units come first (deduplicated,
        in first-seen order), then all ``CREATE TABLE`` units (in inventory
        order), then all ``CREATE INDEX ASYNC`` units (in inventory order, then
        each table's index order). A schema is created before its tables and an
        index references its table, so every table is created before any index;
        DSQL removes foreign keys, so there is no cross-table ``CREATE TABLE``
        ordering constraint and inventory order is a stable, deterministic global
        order. No DML is interleaved, preserving the DDL/DML separation rule
        (Property 2).
        """
        table_units: list[ExecutionUnit] = []
        index_units: list[ExecutionUnit] = []
        schema_units: list[ExecutionUnit] = []
        seen_schema_ddls: set[str] = set()
        for table in self.tables:
            for schema_ddl in table.schema_ddls:
                if schema_ddl not in seen_schema_ddls:
                    seen_schema_ddls.add(schema_ddl)
                    schema_units.append(
                        ExecutionUnit(object_name=table.table, sql=schema_ddl)
                    )
            table_units.append(
                ExecutionUnit(object_name=table.table, sql=table.target_ddl)
            )
            for index_ddl in table.index_ddls:
                index_units.append(
                    ExecutionUnit(object_name=table.table, sql=index_ddl)
                )
        # Views come AFTER tables/indexes because a view selects from the tables.
        view_units: list[ExecutionUnit] = []
        for view in self.views:
            for schema_ddl in view.schema_ddls:
                if schema_ddl not in seen_schema_ddls:
                    seen_schema_ddls.add(schema_ddl)
                    schema_units.append(
                        ExecutionUnit(object_name=view.view, sql=schema_ddl)
                    )
            view_units.append(
                ExecutionUnit(object_name=view.view, sql=view.target_ddl)
            )
        return schema_units + table_units + index_units + view_units

    def to_script(self) -> str:
        """Serialize the ordered execution units into a runnable DDL script.

        Each unit is rendered as a single terminated statement, separated by a
        blank line. Each statement is meant to run in its own transaction
        (single DDL per transaction); the blank-line separation keeps the units
        visually distinct without implying they share a transaction.
        """
        statements = [
            f"{unit.sql.rstrip().rstrip(';')};" for unit in self.execution_units()
        ]
        return "\n\n".join(statements)

    @classmethod
    def from_tables(
        cls,
        tables: list[TableConversion],
        extra_warnings: Optional[list[ConversionWarning]] = None,
        views: Optional[list["ViewConversion"]] = None,
    ) -> "SchemaConversionResult":
        """Build a result and aggregate every warning into one list.

        ``extra_warnings`` carries object-level warnings that are not tied to a
        single table conversion (e.g., triggers and routines flagged for manual
        reimplementation). ``views`` carries the converted views (their warnings
        are aggregated too).
        """
        view_list = list(views or [])
        warnings = [warning for table in tables for warning in table.warnings]
        warnings.extend(warning for view in view_list for warning in view.warnings)
        warnings.extend(extra_warnings or [])
        return cls(tables=list(tables), views=view_list, warnings=warnings)


# ---------------------------------------------------------------------------
# Type mapping
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Mapping:
    """The outcome of mapping a single MySQL data type to a DSQL type.

    ``enum_values`` is set only for ENUM, signalling that a ``CHECK ... IN (...)``
    constraint should be added to preserve the value domain. ``message``/
    ``classification`` are set only when the mapping is not lossless.
    """

    target: exp.DataType
    enum_values: Optional[tuple[str, ...]] = None
    message: Optional[str] = None
    classification: Optional[Classification] = None


# Unsigned integer types widened to the next signed type that preserves range.
# BIGINT UNSIGNED has no wider integer, so it maps to numeric(20, 0).
_UNSIGNED_WIDENING: dict[exp.DataType.Type, str] = {
    _DType.UTINYINT: "smallint",
    _DType.USMALLINT: "integer",
    _DType.UMEDIUMINT: "integer",
    _DType.UINT: "bigint",
    _DType.UBIGINT: "numeric(20, 0)",
}

# Signed integer kinds whose DSQL spelling is already correct EXCEPT that MySQL's
# display width (e.g. ``int(11)``, ``bigint(20)``) leaks through sqlglot as an
# invalid ``INT(11)`` modifier ("syntax error at or near ("). PostgreSQL integers
# take no length modifier, so the width is stripped. (TINYINT is handled earlier:
# TINYINT(1)->boolean; a wider TINYINT(n) maps to smallint here.) The MySQL display
# width is purely cosmetic and carries no storage/constraint meaning.
_SIGNED_INT_TARGET: dict[exp.DataType.Type, str] = {
    _DType.TINYINT: "smallint",
    _DType.SMALLINT: "smallint",
    _DType.INT: "integer",
    _DType.BIGINT: "bigint",
}

# Approximate-numeric kinds that sqlglot renders to a type DSQL rejects:
# ``DOUBLE UNSIGNED``->``UDOUBLE`` (nonexistent). Maps to a plain PG float type
# (unsigned-ness is not representable and carries no storage meaning). ``FLOAT
# UNSIGNED`` is not handled because sqlglot cannot even parse it as a standalone
# type. ``FLOAT(M,D)``, ``DOUBLE(M,D)`` and ``DECIMAL UNSIGNED`` are handled inline
# in map_data_type.
_FLOAT_TARGET: dict[exp.DataType.Type, str] = {
    _DType.UDOUBLE: "double precision",
}

# All MySQL binary types map to a single PostgreSQL bytea. This includes the BLOB
# sizes AND fixed/variable BINARY -- the latter must be remapped explicitly because
# PostgreSQL's bytea takes NO length modifier, so the default sqlglot rendering of
# MySQL ``BINARY(16)``/``VARBINARY(255)`` to ``BYTEA(16)``/``BYTEA(255)`` is invalid
# DDL ("type modifier is not allowed for type bytea"). Mapping to a bare ``bytea``
# drops the modifier. (MySQL right-pads BINARY(n) with 0x00; bytea does not enforce
# the fixed width, an accepted, lossless-for-content widening.)
_BLOB_TYPES = frozenset(
    {
        _DType.BLOB, _DType.TINYBLOB, _DType.MEDIUMBLOB, _DType.LONGBLOB,
        _DType.BINARY, _DType.VARBINARY,
    }
)


def _is_tinyint_one(data_type: exp.DataType) -> bool:
    """Return ``True`` for ``TINYINT(1)`` (the MySQL boolean convention)."""
    if data_type.this is not _DType.TINYINT:
        return False
    params = data_type.expressions
    if not params:
        return False
    param = params[0]
    literal = param.this if isinstance(param, exp.DataTypeParam) else param
    return isinstance(literal, exp.Literal) and literal.this == "1"


def _bit_length(data_type: exp.DataType) -> int:
    """Return the declared bit length of a MySQL ``BIT(n)`` (default 1).

    Bare ``BIT`` is ``BIT(1)`` in MySQL. A non-numeric / missing length falls back
    to 1 so the mapping always picks a valid integer target.
    """
    params = data_type.expressions
    if not params:
        return 1
    param = params[0]
    literal = param.this if isinstance(param, exp.DataTypeParam) else param
    try:
        return int(literal.this) if isinstance(literal, exp.Literal) else 1
    except (TypeError, ValueError):
        return 1


def _enum_values(data_type: exp.DataType) -> tuple[str, ...]:
    """Return the string literals of an ENUM/SET data type, in order."""
    return tuple(
        expression.this
        for expression in data_type.expressions
        if isinstance(expression, exp.Literal)
    )


def _build(target_sql: str) -> exp.DataType:
    """Build a PostgreSQL :class:`exp.DataType` from a type string."""
    return exp.DataType.build(target_sql, dialect=_POSTGRES)


def map_data_type(data_type: exp.DataType) -> Optional[_Mapping]:
    """Map a parsed MySQL data type to a DSQL type, or ``None`` to leave it.

    Returning ``None`` means no DSQL-specific rewrite is needed and the
    ``postgres`` renderer already produces a correct, lossless type (e.g.,
    ``INT``, ``VARCHAR``, ``JSON``).
    """
    kind = data_type.this

    if _is_tinyint_one(data_type):
        return _Mapping(
            target=_build("boolean"),
            message=(
                "TINYINT(1) is mapped to boolean by convention; values outside "
                "0/1 would not be represented and require review."
            ),
            classification=Classification.MANUAL,
        )

    if kind in _UNSIGNED_WIDENING:
        # Lossless: the signed target preserves the full unsigned range.
        return _Mapping(target=_build(_UNSIGNED_WIDENING[kind]))

    if kind is _DType.MEDIUMINT:
        # PostgreSQL has no 3-byte integer; sqlglot renders signed MEDIUMINT to a
        # literal ``MEDIUMINT`` which does not exist in DSQL ("type mediumint does
        # not exist"). integer (4 bytes) losslessly covers the signed 24-bit range.
        return _Mapping(target=_build("integer"))

    if kind is _DType.BIT:
        # Aurora DSQL does not support the bit/bit-string type at all ("datatype
        # bit not supported", SQLSTATE 0A000). MySQL BIT(n) holds an n-bit unsigned
        # integer (1..64 bits), so map to the smallest signed integer that holds
        # the unsigned range: n<=15 -> smallint, <=31 -> integer, <=63 -> bigint,
        # else numeric(20,0) (BIT(64) needs the full unsigned 64-bit range).
        bits = _bit_length(data_type)
        if bits <= 15:
            target_sql = "smallint"
        elif bits <= 31:
            target_sql = "integer"
        elif bits <= 63:
            target_sql = "bigint"
        else:
            target_sql = "numeric(20, 0)"
        return _Mapping(
            target=_build(target_sql),
            message=(
                f"MySQL BIT({bits}) has no Aurora DSQL equivalent (bit type "
                f"unsupported); mapped to {target_sql} holding the integer value."
            ),
            classification=Classification.MANUAL,
        )

    if kind is _DType.YEAR:
        # PostgreSQL has no YEAR type; sqlglot renders it as a literal ``YEAR``
        # which does not exist in DSQL. MySQL YEAR holds 1901-2155 (and 0000), so a
        # smallint (>= -32768) covers the range; the value converter stores the
        # integer year. A warning records that the 1-4 digit YEAR display semantics
        # are not preserved (it becomes a plain integer year).
        return _Mapping(
            target=_build("smallint"),
            message=(
                "MySQL YEAR has no Aurora DSQL equivalent; mapped to smallint "
                "holding the integer year. YEAR's 1901-2155 range fits; display "
                "formatting and the YEAR type semantics are not preserved."
            ),
            classification=Classification.MANUAL,
        )

    if kind in _SIGNED_INT_TARGET and data_type.expressions:
        # A signed integer carrying a MySQL display width (e.g. int(11), bigint(20),
        # tinyint(4), smallint(5)). sqlglot would emit ``INT(11)`` etc., which DSQL
        # rejects ("syntax error at or near ("). Re-emit the bare integer type; the
        # width is cosmetic. (Width-less integers render fine, so they fall through.)
        return _Mapping(target=_build(_SIGNED_INT_TARGET[kind]))

    if kind in _FLOAT_TARGET:
        # DOUBLE UNSIGNED -> sqlglot ``UDOUBLE`` (nonexistent in DSQL). Map to a plain
        # PG float type; unsigned-ness is not representable and is storage-irrelevant.
        return _Mapping(target=_build(_FLOAT_TARGET[kind]))

    if kind is _DType.FLOAT and len(data_type.expressions) >= 2:
        # MySQL FLOAT(M,D) -> sqlglot ``FLOAT(10, 2)``; PostgreSQL FLOAT takes a single
        # precision (1-53), not a scale, so the two-arg form is a syntax error. The
        # (M,D) display spec carries no storage meaning -> plain ``real``.
        return _Mapping(target=_build("real"))

    if kind is _DType.DOUBLE and len(data_type.expressions) >= 2:
        # MySQL DOUBLE(M,D) -> sqlglot renders a two-arg ``FLOAT(10, 2)`` (kind
        # DOUBLE, not UDOUBLE, so it misses ``_FLOAT_TARGET`` above). PostgreSQL
        # ``double precision`` takes NO arguments, so the (M,D) form is a syntax
        # error. The display spec carries no storage meaning -> plain
        # ``double precision``. (Bare DOUBLE renders correctly and falls through;
        # DOUBLE UNSIGNED -> UDOUBLE is handled by ``_FLOAT_TARGET`` above.)
        return _Mapping(target=_build("double precision"))

    if kind in (_DType.UDECIMAL, _DType.DECIMAL):
        # DECIMAL(p,s) UNSIGNED -> sqlglot ``UDECIMAL(p,s)`` (nonexistent), so it must be
        # mapped to numeric here; unsigned-ness is not representable. SIGNED DECIMAL
        # renders correctly on its own, but is routed through the same branch so the
        # DSQL precision/scale ceiling is applied to both -- MySQL allows DECIMAL(65,30)
        # and DSQL rejects anything past (38,37).
        params = data_type.expressions
        if params:
            spec, clamp_warning = _clamp_numeric_spec(list(params))
            if spec:
                return _Mapping(
                    target=_build(f"numeric({spec})"), message=clamp_warning
                )
        return _Mapping(target=_build("numeric"))

    if kind is _DType.DATETIME:
        # Lossless value mapping; values are treated as UTC.
        return _Mapping(target=_build("timestamp"))

    if kind is _DType.TIME:
        # MySQL TIME is a DURATION spanning -838:59:59..838:59:59, but PostgreSQL/DSQL
        # `time` is a time-of-day in 00:00:00..24:00:00. The DDL converts cleanly, but a
        # value outside [0, 24h) has NO `time` representation and fails PER ROW during
        # Full Load (ValueConversionError) -- so warn up front (matching the ENUM/BIT/
        # YEAR pattern) instead of surfacing only mid-load on the offending row.
        return _Mapping(
            target=data_type,
            message=(
                "MySQL TIME is a duration (-838:59:59..838:59:59); Aurora DSQL 'time' is "
                "a time-of-day (00:00:00..24:00:00). In-range values convert cleanly, but "
                "any value outside that range has no 'time' representation and fails per "
                "row during Full Load. If this column stores durations, remap it to "
                "interval or text (the converted DDL is editable) before loading."
            ),
            classification=Classification.MANUAL,
        )

    if kind in _BLOB_TYPES:
        return _Mapping(target=_build("bytea"))

    if kind is _DType.ENUM:
        return _Mapping(
            target=_build("text"),
            enum_values=_enum_values(data_type),
            message=(
                "ENUM is not supported by Aurora DSQL; mapped to text with a "
                "CHECK constraint. ENUM ordering semantics are not preserved."
            ),
            classification=Classification.MANUAL,
        )

    if kind is _DType.SET:
        return _Mapping(
            target=_build("text"),
            message=(
                "SET has no lossless mapping in Aurora DSQL; mapped to text. "
                "Multi-value set semantics must be handled in the application."
            ),
            classification=Classification.MANUAL,
        )

    return None


def parse_target_column_types(create_ddl: str) -> dict[str, str]:
    """Return ``{column_name: normalized_target_type}`` from a CREATE TABLE DDL.

    Parses an applied/edited DSQL (PostgreSQL) ``CREATE TABLE`` and extracts each
    column's target type, normalized to its base name (lower-cased, parameters
    stripped -- e.g. ``numeric(20, 0)`` -> ``numeric``). This lets the Full Load
    value conversion follow the *applied* target type (including a user remap such
    as ``boolean`` -> ``smallint`` made in Schema Conversion) instead of
    re-deriving it from the source type. Returns an empty mapping when the DDL is
    not a parseable ``CREATE TABLE`` (the caller then falls back to the
    source-derived mapping).
    """
    try:
        parsed = sqlglot.parse_one(create_ddl, read="postgres")
    except Exception:  # noqa: BLE001 - unparseable DDL -> no overrides (safe default)
        return {}
    if not isinstance(parsed, exp.Create):
        return {}
    types: dict[str, str] = {}
    for column_def in parsed.find_all(exp.ColumnDef):
        name = column_def.name
        kind = column_def.args.get("kind")
        if not name or kind is None:
            continue
        types[name] = kind.sql(dialect="postgres").split("(", 1)[0].strip().lower()
    return types


def parse_target_primary_key(create_ddl: str) -> list[str]:
    """Return the PRIMARY KEY column names (in key order) from a CREATE TABLE DDL.

    Parses an applied/edited DSQL (PostgreSQL) ``CREATE TABLE`` and extracts the
    primary-key column list, handling both a table-level ``PRIMARY KEY (a, b)``
    constraint and an inline ``col ... PRIMARY KEY`` single-column key. This lets
    the Full Load loader use the *applied* target PK (e.g. a composite
    ``(leading, id)`` chosen in Schema Conversion) as the ``ON CONFLICT`` /
    ``SKIP_EXISTING`` conflict key instead of assuming it equals the source PK.

    Returns an empty list when the DDL is not a parseable ``CREATE TABLE`` or
    declares no primary key. IMPORTANT: an empty list means "unknown" -- the caller
    must NOT silently substitute the source PK for a table the user opted into a
    changed (composite) key; it should require the target PK explicitly and fail
    loudly on disagreement (see the Full Load engine wiring).
    """
    try:
        parsed = sqlglot.parse_one(create_ddl, read="postgres")
    except Exception:  # noqa: BLE001 - unparseable DDL -> unknown (empty list)
        return []
    if not isinstance(parsed, exp.Create):
        return []
    # Table-level: PRIMARY KEY (a, b) lives as an exp.PrimaryKey inside the schema
    # expressions (the same list that holds the ColumnDefs).
    for schema in parsed.find_all(exp.Schema):
        for e in schema.expressions:
            if isinstance(e, exp.PrimaryKey):
                cols = [c.name for c in e.expressions if c.name]
                if cols:
                    return cols
    # Inline: col <type> PRIMARY KEY (single-column key on a ColumnDef).
    for column_def in parsed.find_all(exp.ColumnDef):
        for _ in column_def.find_all(exp.PrimaryKeyColumnConstraint):
            if column_def.name:
                return [column_def.name]
    return []


def map_mysql_type(mysql_type: str) -> tuple[str, Optional[ConversionWarning]]:
    """Map a MySQL type string to a DSQL type string and optional warning.

    Convenience entry point for the type-mapping table independent of a full
    table conversion. The returned warning (if any) has no table/column context.
    Raises ``ValueError`` if ``mysql_type`` cannot be parsed.
    """
    if is_spatial_mysql_type(mysql_type):
        # Spatial types have no Aurora DSQL equivalent and are not parseable by
        # sqlglot's MySQL dialect; preserve the data as raw WKB bytes in bytea.
        # Full Load reads them via ST_AsBinary and the CDC sink extracts
        # Debezium's geometry WKB, so both paths store identical bytes.
        return "bytea", ConversionWarning(
            object_name="<type>",
            column_name=None,
            source_type=mysql_type,
            target_type="bytea",
            classification=Classification.MANUAL,
            message=(
                f"MySQL type '{mysql_type}' has no Aurora DSQL equivalent; "
                "preserved as raw bytes (bytea, WKB)."
            ),
        )
    try:
        data_type = sqlglot.parse_one(
            _normalize_mysql_type(mysql_type), into=exp.DataType, read=_MYSQL
        )
    except sqlglot.errors.SqlglotError as exc:
        # SqlglotError, not ParseError: a TOKENIZER failure (TokenError) is a sibling
        # class, not a subclass, so a ParseError-only guard let it escape as an unhandled
        # exception instead of the intended ValueError.
        raise ValueError(f"unable to parse MySQL type {mysql_type!r}: {exc}") from exc

    mapping = map_data_type(data_type)
    target_type = (
        mapping.target if mapping is not None else data_type
    ).sql(dialect=_POSTGRES)

    warning: Optional[ConversionWarning] = None
    if mapping is not None and mapping.message is not None:
        warning = ConversionWarning(
            object_name="<type>",
            column_name=None,
            source_type=mysql_type,
            target_type=target_type,
            classification=mapping.classification or Classification.MANUAL,
            message=mapping.message,
        )
    return target_type, warning


# ---------------------------------------------------------------------------
# DSQL write contract (shared by the bulk loader and the CDC sink)
# ---------------------------------------------------------------------------

# The migration tool writes to Aurora DSQL through TWO independent code paths:
# the Python bulk loader (Full Load) and the Java/Kafka-Connect sink (CDC). They
# MUST agree on how each MySQL source type lands in DSQL, or the same row migrates
# differently depending on the path -- a silent data bug at cutover. This table is
# the single declarative source of truth for that agreement: for each boundary
# MySQL type it records the DSQL target kind (from the mapping above), how Debezium
# encodes the value on the CDC path, and the canonical stored form both paths must
# produce. It is serialized to ``tests/fixtures/dsql_write_contract.json`` and
# loaded by BOTH the Python and Java parity tests, so one artifact governs both.
#
# Each entry: (mysql_type, dsql_kind, debezium_schema_name, note). ``dsql_kind`` is
# the normalized target kind (see exporter._target_kind). ``debezium_schema_name``
# is the Kafka Connect logical-type schema name Debezium stamps on the field
# (None = a plain primitive), which the sink uses to drive its conversion.
DSQL_WRITE_CONTRACT_CASES: tuple[tuple[str, str, Optional[str], str], ...] = (
    ("DATETIME", "timestamp", "io.debezium.time.Timestamp",
     "naive wall-clock; bulk loader attaches UTC, sink converts Long ms -> timestamp"),
    ("DATETIME(6)", "timestamp", "io.debezium.time.MicroTimestamp",
     "microsecond precision; sink converts Long micros -> timestamp (was the H5 DLQ bug)"),
    ("TIMESTAMP", "timestamptz", "io.debezium.time.ZonedTimestamp",
     "tz-aware; bulk loader -> UTC, sink parses ISO-8601 string -> timestamp"),
    ("BIGINT UNSIGNED", "decimal", "org.apache.kafka.connect.data.Decimal",
     "widened to numeric(20,0) (kind 'decimal'); needs bigint.unsigned.handling.mode=precise so Debezium sends BigDecimal not an overflowing Long"),
    ("DECIMAL(10,4)", "decimal", "org.apache.kafka.connect.data.Decimal",
     "exact decimal; Debezium precise mode sends BigDecimal, binds to numeric"),
    ("TINYINT(1)", "boolean", None,
     "MySQL boolean convention; 0/1 -> False/True"),
    ("BLOB", "bytea", None, "binary payload -> bytea"),
    ("LONGBLOB", "bytea", None, "long binary payload -> bytea"),
    ("GEOMETRY", "bytea", "io.debezium.data.geometry.Geometry",
     "spatial has no DSQL type; preserved as raw WKB bytes -> bytea. Full Load reads "
     "ST_AsBinary(col); the sink extracts Debezium geometry's .wkb. SRID is dropped "
     "on both paths (plain WKB) so the stored bytes are identical."),
    ("JSON", "json", "io.debezium.data.Json",
     "JSON text; sink wraps in a PGobject(type=json) so the column type matches"),
    ("ENUM('a','b')", "text", None, "ENUM -> text"),
    ("SET('x','y')", "text", None, "SET -> text"),
    # --- Integer family + unsigned widening (Debezium sends plain primitives) ----
    ("TINYINT", "smallint", None, "signed 8-bit -> smallint"),
    ("SMALLINT", "smallint", None, "signed 16-bit -> smallint"),
    ("MEDIUMINT", "int", None, "signed 24-bit -> integer (PG has no 3-byte int)"),
    ("INT", "int", None, "signed 32-bit -> integer"),
    ("BIGINT", "bigint", None, "signed 64-bit -> bigint"),
    ("TINYINT UNSIGNED", "smallint", None, "0..255 widened -> smallint"),
    ("SMALLINT UNSIGNED", "int", None, "0..65535 widened -> integer"),
    ("MEDIUMINT UNSIGNED", "int", None, "0..16M widened -> integer"),
    ("INT UNSIGNED", "bigint", None, "0..4.29B widened -> bigint"),
    # --- BIT: DSQL has no bit type; mapped to a sized integer (value bytes->int) ---
    ("BIT(8)", "smallint", "io.debezium.data.Bits",
     "BIT(n<=15) -> smallint; Debezium Bits=little-endian byte[], sink decodes to int"),
    ("BIT(64)", "decimal", "io.debezium.data.Bits",
     "BIT(64) needs the full unsigned 64-bit range -> numeric(20,0)"),
    # --- Temporal (no-tz timestamp covered above; here the date/time/year kinds) --
    ("DATE", "date", "io.debezium.time.Date",
     "epoch-day INT32 -> date (sink) / datetime.date (loader)"),
    ("TIME", "time", "io.debezium.time.MicroTime",
     "micros-since-midnight -> time; loader timedelta->time, sink Long->java.sql.Time"),
    ("YEAR", "smallint", "io.debezium.time.Year",
     "MySQL YEAR (1901-2155) -> smallint integer year"),
    # --- Approximate numerics ---------------------------------------------------
    ("FLOAT", "real", None, "single-precision float -> real"),
    ("DOUBLE", "double precision", None, "double-precision float"),
    # --- Strings + binary -------------------------------------------------------
    ("CHAR(10)", "char", None, "fixed-length char"),
    ("VARCHAR(255)", "varchar", None, "variable-length char"),
    ("BINARY(16)", "bytea", None, "fixed binary -> bytea (modifier dropped)"),
    ("VARBINARY(255)", "bytea", None, "variable binary -> bytea (modifier dropped)"),
    ("TINYTEXT", "text", None, "text -> text"),
    ("MEDIUMTEXT", "text", None, "text -> text"),
    ("LONGTEXT", "text", None, "text -> text"),
    ("MEDIUMBLOB", "bytea", None, "binary payload -> bytea"),
)


# ---------------------------------------------------------------------------
# Source DDL construction
# ---------------------------------------------------------------------------


def _quote_mysql_identifier(name: str) -> str:
    """Return ``name`` as a safely backtick-quoted MySQL identifier."""
    return exp.to_identifier(name, quoted=True).sql(dialect=_MYSQL)


def _split_qualified(name: str) -> tuple[Optional[str], str]:
    """Split a possibly schema-qualified ``schema.object`` name.

    Returns ``(schema, object)`` for a qualified name (split on the first dot),
    or ``(None, name)`` when the name is unqualified. In MySQL a "schema" is a
    database; cluster-wide introspection qualifies names as ``database.table``,
    which must map to a PostgreSQL ``schema.table`` on Aurora DSQL rather than a
    single flat identifier.
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return schema, obj
    return None, name


def _quote_mysql_qualified(name: str) -> str:
    """Backtick-quote a possibly-qualified name as ``\\`schema\\`.\\`table\\```.

    An unqualified name renders exactly like :func:`_quote_mysql_identifier`, so
    only schema-qualified names change behavior.
    """
    schema, obj = _split_qualified(name)
    if schema is not None:
        return f"{_quote_mysql_identifier(schema)}.{_quote_mysql_identifier(obj)}"
    return _quote_mysql_identifier(obj)


# FLOAT/REAL base types whose ``UNSIGNED`` sqlglot's MySQL dialect cannot parse as a
# standalone type (``FLOAT UNSIGNED``/``FLOAT(M,D) UNSIGNED`` -> ParseError), unlike
# ``DOUBLE UNSIGNED`` (parses to UDOUBLE). Unsigned-ness on an APPROXIMATE numeric is
# not representable in PostgreSQL and carries no storage meaning (same rationale as the
# _FLOAT_TARGET UDOUBLE mapping), so stripping it is lossless and lets the type parse.
# Restricted to float/real so an INTEGER ``unsigned`` (whose range-widening the mapping
# DOES handle, e.g. ``int unsigned`` -> bigint) is never touched here.
_FLOAT_UNSIGNED_RE = re.compile(
    r"^(\s*(?:float|real)\b[^,]*?(?:\([^)]*\))?)\s+unsigned\b(.*)$",
    re.IGNORECASE,
)


def _normalize_mysql_type(mysql_type: str) -> str:
    """Strip MySQL display-only attributes that leak invalid tokens into DSQL DDL.

    ``ZEROFILL`` is a display attribute (left-pads with zeros) that carries no
    storage meaning and is not a PostgreSQL token -- left in, it renders as a
    literal ``ZEROFILL`` ("syntax error at or near ZEROFILL") and also breaks the
    standalone type parser. It always implies UNSIGNED in MySQL, so dropping only
    ZEROFILL preserves the unsigned-ness (which the type mapping handles). Matched
    case-insensitively as a whole word.

    ``FLOAT UNSIGNED`` / ``FLOAT(M,D) UNSIGNED`` additionally has its ``UNSIGNED``
    stripped: sqlglot's MySQL dialect cannot parse it as a standalone type (it raises
    ParseError, which used to abort the WHOLE table to an UNSUPPORTED placeholder while
    Evaluation still reported it AUTO/compatible). Unsigned-ness is not representable on
    an approximate numeric and carries no storage meaning, so dropping it maps the column
    to ``real`` -- the same treatment ``DOUBLE UNSIGNED`` already gets.
    """
    normalized = re.sub(r"\s+zerofill\b", "", mysql_type, flags=re.IGNORECASE).strip()
    normalized = _FLOAT_UNSIGNED_RE.sub(r"\1\2", normalized).strip()
    return normalized


# MySQL expression defaults whose PostgreSQL/DSQL spelling is IDENTICAL, so they can be
# emitted verbatim. Verified on a live DSQL cluster. ``CURRENT_TIMESTAMP(n)`` (fractional
# seconds) is matched by prefix because PostgreSQL accepts the same spelling.
_PASSTHROUGH_EXPRESSION_DEFAULTS: frozenset[str] = frozenset(
    {
        "CURRENT_TIMESTAMP",
        "CURRENT_TIMESTAMP()",
        "NOW()",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "LOCALTIME",
        "LOCALTIMESTAMP",
    }
)

# MySQL expression defaults with a DIFFERENT but equivalent DSQL spelling.
_TRANSLATED_EXPRESSION_DEFAULTS: dict[str, str] = {
    "UUID()": "gen_random_uuid()",
    "CURDATE()": "CURRENT_DATE",
    "CURTIME()": "CURRENT_TIME",
    "UTC_TIMESTAMP()": "(now() AT TIME ZONE 'UTC')",
    "UTC_DATE()": "(CURRENT_DATE AT TIME ZONE 'UTC')",
}

def _strip_on_update(default: str) -> str:
    """Drop a trailing ``ON UPDATE <expr>`` from a reflected default.

    Belt-and-braces: ``information_schema.COLUMN_DEFAULT`` (now the authoritative source,
    see ``introspector.enrich_columns``) already excludes the clause, but SQLAlchemy's
    regex reflection folds it INTO the default -- ``datetime DEFAULT CURRENT_TIMESTAMP ON
    UPDATE CURRENT_TIMESTAMP`` reflects as the single string
    ``"CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"``. Emitting that verbatim is a
    target syntax error (``syntax error at or near "ON"``), so it is stripped here too for
    any persisted inventory or caller that still carries the reflected form. Nothing is
    lost: the ON UPDATE fact rides on ``ColumnDef.auto_update_timestamp`` and is already
    reported MANUAL by the assessor's ``ON_UPDATE_TIMESTAMP`` rule (DSQL has no ON UPDATE
    clause and no triggers, so it is unreproducible either way).
    """
    return re.split(r"\s+ON\s+UPDATE\s+", default, maxsplit=1, flags=re.IGNORECASE)[0].strip()


def _unwrap_expression_default(default: str) -> str:
    """Peel MySQL 8's wrapping parentheses off an expression default.

    MySQL 8 stores an expression default parenthesized and reports it that way, so
    ``DEFAULT (uuid())`` arrives as ``"(uuid())"``. Without unwrapping, the expression is
    unrecognizable and the parenthesized text used to be emitted straight through -- which
    truncated to ``DEFAULT (`` in the rebuilt DDL and failed with ``syntax error at or
    near ")"``.
    """
    text_value = default.strip()
    while text_value.startswith("(") and text_value.endswith(")"):
        inner = text_value[1:-1].strip()
        if not inner:
            break
        text_value = inner
    return text_value


def _quote_default_literal(literal: str) -> str:
    """Render a literal default as a safely quoted MySQL string literal.

    ``information_schema.COLUMN_DEFAULT`` returns a literal UNQUOTED (an int default is
    ``"0"``, a string default is ``a'b`` with the quote unescaped), so the value must be
    re-quoted here. This is also the injection boundary: the default is the one value
    interpolated into the reconstructed DDL, and it originates from the source schema, so
    it is escaped rather than concatenated raw (Requirement 9.4). Numeric and boolean
    literals are emitted bare so the target keeps its native type.
    """
    stripped = literal.strip()
    # A number (or a signed number) needs no quotes and must not get them: a quoted
    # value would be a string literal the target then has to coerce.
    if re.fullmatch(r"[+-]?\d+(\.\d+)?", stripped):
        return stripped
    if stripped.upper() in ("TRUE", "FALSE", "NULL"):
        return stripped.upper()
    # MySQL bit/hex literals carry their own syntax and are handled by the caller.
    if re.fullmatch(r"(?i)(b|x)'[0-9a-f]*'", stripped) or stripped.lower().startswith("0x"):
        return stripped
    # Everything else is a string: single-quote it, doubling any embedded quote. Handles
    # the reflected-and-truncated backslash form too, since the value is re-escaped from
    # scratch rather than trusted.
    escaped = stripped.replace("\\'", "'").replace("'", "''")
    return f"'{escaped}'"


def _column_default_sql(
    column: ColumnDef, target_type: str, enum_values: Optional[list[str]] = None
) -> tuple[Optional[str], Optional[str]]:
    """Return ``(default_sql, warning)`` for a column's MySQL default.

    ``default_sql`` is the text to place after ``DEFAULT`` in the generated MySQL DDL,
    which sqlglot then transpiles with the rest of the statement. ``None`` means emit no
    default; ``warning`` is set whenever something had to be dropped or reshaped, so a
    change in insert behavior is never silent.

    Aurora DSQL DOES support column defaults -- ``DEFAULT default_expr`` is in its
    documented ``CREATE TABLE`` column_constraint grammar, and literals, expressions,
    ``CURRENT_TIMESTAMP``, ``gen_random_uuid()`` and ``NOT NULL DEFAULT`` are all verified
    working on a live cluster. Dropping them was a converter gap, not a platform limit,
    and it mattered most for a **NOT NULL column with a default**: MySQL accepts an INSERT
    that omits it while the target rejects the same INSERT with a not-null violation -- an
    application break that only appears after cut-over.

    ``target_type`` is the type the column is being MAPPED TO, not the source type. That
    matters: the decision has to key off the target, because several MySQL types change
    class on the way over (``tinyint(1)`` -> boolean, ``tinyint(1) unsigned`` -> smallint,
    ``bit`` -> integer, ``blob`` -> bytea) and a default valid for the source type is a
    hard error against the mapped one.

    Handled cases, and why:

    * **No default / generated column / AUTO_INCREMENT** -> nothing to emit. A generated
      column's value is computed, and PostgreSQL rejects an identity column that also
      carries a DEFAULT.
    * **``ON UPDATE`` folded into the default** -> stripped (see :func:`_strip_on_update`).
    * **Expression default** -> emitted verbatim when PostgreSQL spells it the same way,
      translated when there is a known equivalent (``UUID()`` -> ``gen_random_uuid()``),
      and dropped with a warning otherwise. A MySQL-only artifact (a ``_utf8mb4``
      introducer, a column reference) is dropped rather than risking invalid DDL.
    * **boolean target** -> a ``0``/``1`` literal becomes ``FALSE``/``TRUE``; ``DEFAULT 1``
      on a boolean is a hard error on DSQL.
    * **ENUM mapped to text + CHECK** -> a default outside the enum members is dropped,
      because it would apply cleanly and then fail the CHECK on every defaulted INSERT.

    Rarer literal/target mismatches (a bit-string default on an integer target, a binary
    default on ``bytea``, MySQL's ``0000-00-00`` zero date) deliberately have NO dedicated
    branch: none occurs in practice, and each would add a code path plus tests for a case
    nobody hits. They fall through to the general rule -- the literal is emitted, and if
    the target rejects it the failure is loud at Schema Conversion time rather than
    silent. Add a branch only when a real schema produces one.
    """
    raw = (column.default or "").strip()
    if not raw:
        return None, None
    if column.generated:
        return None, None  # computed value; a default cannot apply

    raw = _strip_on_update(raw)
    if not raw:
        return None, None

    target = (target_type or "").strip().lower()
    is_boolean_target = target.startswith("bool")

    # --- expression defaults -------------------------------------------------------
    # MySQL's own DEFAULT_GENERATED flag decides this, with no shape heuristic to
    # second-guess it. There is deliberately NO fallback: the only supported input is an
    # inventory that has been through ``introspector.enrich_columns``, which every MySQL
    # source goes through unconditionally (reflect, then enrich -- introspector.py:738).
    # That single supported shape is what makes the translation exact:
    #   * the default is the UNQUOTED value from information_schema.COLUMN_DEFAULT
    #   * an expression is marked by EXTRA's DEFAULT_GENERATED
    #   * ON UPDATE stays in EXTRA and never contaminates the default
    # Accepting the raw SQLAlchemy-reflected form too meant guessing from quoting, which
    # cannot tell the literal string "CURRENT_TIMESTAMP" from the function call.
    if column.default_is_expression:
        expression = _unwrap_expression_default(raw)
        canonical = expression.upper().replace(" ", "")
        if canonical in {v.replace(" ", "") for v in _PASSTHROUGH_EXPRESSION_DEFAULTS} or (
            canonical.startswith("CURRENT_TIMESTAMP(") or canonical.startswith("NOW(")
        ):
            # A no-timezone target (MySQL DATETIME -> `timestamp`) must not inherit the
            # session TimeZone: the loader normalizes migrated rows to naive UTC, so the
            # default is the naive-UTC wall-clock `now() AT TIME ZONE 'UTC'`.
            #
            # A timestamptz target (MySQL TIMESTAMP) must NOT get that: `now() AT TIME
            # ZONE 'UTC'` is a naive value, which a timestamptz column re-interprets in
            # the session TimeZone -- shifting a defaulted insert by the offset. It wants
            # plain CURRENT_TIMESTAMP/now() (an instant). The guard must therefore fire
            # ONLY for the naive `timestamp`/`timestamp(n)` target, never `timestamptz`
            # or `timestamp with time zone`.
            is_naive_timestamp = (
                target == "timestamp"
                or target.startswith("timestamp(")
            ) and "tz" not in target and "with time zone" not in target
            if is_naive_timestamp:
                return "(now() AT TIME ZONE 'UTC')", None
            return expression, None
        translated = _TRANSLATED_EXPRESSION_DEFAULTS.get(
            expression.upper() if expression.upper().endswith(")") else f"{expression.upper()}()"
        ) or _TRANSLATED_EXPRESSION_DEFAULTS.get(expression.upper())
        if translated:
            return translated, None
        return None, (
            f"MySQL expression default {raw} has no Aurora DSQL equivalent, so the column "
            "is created without a default. Set the value explicitly in the application, or "
            "use a DSQL expression default if the semantics match."
        )

    # --- literal defaults ----------------------------------------------------------
    unquoted = raw.strip().strip("'\"")

    if is_boolean_target:
        if unquoted in ("0", "1"):
            return ("TRUE" if unquoted == "1" else "FALSE"), None
        if unquoted.upper() in ("TRUE", "FALSE"):
            return unquoted.upper(), None
        return None, (
            f"TINYINT(1) default {raw} is not 0 or 1, so it cannot be mapped to a boolean "
            "default; the column is created without one."
        )

    if enum_values is not None and unquoted not in enum_values:
        return None, (
            f"ENUM default {raw} is not one of the allowed values, so it would fail the "
            "generated CHECK constraint on every defaulted INSERT; the column is created "
            "without a default."
        )

    return _quote_default_literal(raw), None


def _resolve_column_default(
    column: ColumnDef,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve a column's emitted DEFAULT against its MAPPED target type.

    The decision cannot be made from the source type alone -- ``tinyint(1)`` becomes
    boolean while ``tinyint(1) unsigned`` becomes smallint, ``bit`` becomes an integer and
    ``blob`` becomes bytea -- so the type mapping is run first and its result drives the
    translation. Used by both the DDL builder and the warning pass so the two can never
    disagree about what was emitted.
    """
    try:
        target_type, _mapping_warning = map_mysql_type(column.mysql_type)
    except ValueError:
        # An unparseable type is reported elsewhere (the table is surfaced as
        # UNSUPPORTED); emit no default rather than guessing.
        return None, None
    enum_values: Optional[list[str]] = None
    if target_type.strip().lower().startswith("text"):
        try:
            parsed = sqlglot.parse_one(
                f"CAST(NULL AS {_normalize_mysql_type(column.mysql_type)})", read=_MYSQL
            ).to
            if isinstance(parsed, exp.DataType) and parsed.this is _DType.ENUM:
                enum_values = list(_enum_values(parsed))
        except Exception:  # noqa: BLE001 - enum detection is advisory
            enum_values = None
    return _column_default_sql(column, target_type, enum_values)


def _build_source_ddl(table: TableDef) -> str:
    """Build a MySQL ``CREATE TABLE`` string for ``table`` (columns + PK).

    Identifiers are quoted via ``sqlglot`` to avoid injection (Requirement 9.4).
    A schema-qualified ``database.table`` name is rendered as a qualified
    identifier so it transpiles to a PostgreSQL ``"schema"."table"`` rather than
    a single flat identifier. Foreign keys and secondary indexes are
    intentionally not emitted here: foreign keys are removed for DSQL (preserved
    as metadata by the caller) and indexes are rendered separately as
    ``CREATE INDEX ASYNC``. Raises ``ValueError`` if the table has no columns.
    """
    if not table.columns:
        raise ValueError(f"table {table.name!r} has no columns to convert")

    column_clauses: list[str] = []
    for column in table.columns:
        clause = (
            f"{_quote_mysql_identifier(column.name)} "
            f"{_normalize_mysql_type(column.mysql_type)}"
        )
        if not column.nullable:
            clause += " NOT NULL"
        # Carry the source DEFAULT across. It is emitted into the MySQL DDL and
        # transpiled with the rest of the statement, so sqlglot handles the dialect
        # rendering instead of this building PostgreSQL text by hand. Skipped for the
        # AUTO_INCREMENT column, which becomes GENERATED BY DEFAULT AS IDENTITY -- an
        # identity column carrying a DEFAULT is rejected by PostgreSQL.
        if column.name != table.auto_increment_column:
            default_sql, _warning = _resolve_column_default(column)
            if default_sql is not None:
                clause += f" DEFAULT {default_sql}"
        column_clauses.append(clause)

    if table.primary_key:
        pk_columns = ", ".join(
            _quote_mysql_identifier(name) for name in table.primary_key
        )
        column_clauses.append(f"PRIMARY KEY ({pk_columns})")

    body = ", ".join(column_clauses)
    return f"CREATE TABLE {_quote_mysql_qualified(table.name)} ({body})"


def build_source_table_ddl(table: TableDef) -> str:
    """Return the reconstructed MySQL ``CREATE TABLE`` DDL for ``table``.

    Public wrapper over the internal builder so the AI routing layer (Task 16.4)
    can ground a suggestion prompt with the already-extracted source DDL without
    reaching into a private helper or the UI. This only reads the in-memory
    :class:`TableDef`; it never touches the source database. Raises
    ``ValueError`` if the table has no columns.
    """
    return _build_source_ddl(table)


# MySQL spatial types have no Aurora DSQL (PostgreSQL, no PostGIS) equivalent and
# are not recognized as data types by sqlglot's MySQL dialect, so a table using
# one cannot be reconstructed/transpiled. Detected to give a precise reason.
_SPATIAL_TYPES = frozenset(
    {
        "geometry",
        "point",
        "linestring",
        "polygon",
        "multipoint",
        "multilinestring",
        "multipolygon",
        "geometrycollection",
    }
)


def _spatial_columns(table: TableDef) -> list[str]:
    """Return ``"name type"`` for each column whose base type is a spatial type."""
    found: list[str] = []
    for column in table.columns:
        tokens = column.mysql_type.strip().lower().replace("(", " ").split()
        base = tokens[0] if tokens else ""
        if base in _SPATIAL_TYPES:
            found.append(f"{column.name} {column.mysql_type}")
    return found


def is_spatial_mysql_type(mysql_type: str) -> bool:
    """Return True when ``mysql_type``'s base type is a MySQL spatial type.

    Shared with the Full Load exporter, which reads such columns as
    ``ST_AsBinary(col)`` (WKB bytes -> bytea) to match what Debezium delivers for
    CDC, keeping Full Load and CDC byte-identical.
    """
    tokens = mysql_type.strip().lower().replace("(", " ").split()
    base = tokens[0] if tokens else ""
    return base in _SPATIAL_TYPES


def _substitute_unsupported_types(table: TableDef) -> tuple[TableDef, list[str]]:
    """Retype DSQL-unsupported source columns to ``bytea`` (preserve, never NULL).

    MySQL spatial types (geometry/point/...) have no Aurora DSQL (PostgreSQL, no
    PostGIS) equivalent and are not even parseable by sqlglot's MySQL dialect, so
    a table using one would otherwise fail to convert entirely. Instead of losing
    the table -- or silently NULLing the column -- the column is mapped to
    ``bytea`` so the data is PRESERVED as raw bytes (the geometry's WKB). Returns
    the rewritten table (spatial columns retyped to ``LONGBLOB`` -> ``bytea``) and
    the substituted column names so the caller attaches a MANUAL warning. The Full
    Load exporter reads these columns as ``ST_AsBinary(col)`` (WKB) and the CDC
    sink writes Debezium's WKB bytes, so both paths preserve identical bytes.
    """
    substituted: list[str] = []
    new_columns = []
    for column in table.columns:
        tokens = column.mysql_type.strip().lower().replace("(", " ").split()
        base = tokens[0] if tokens else ""
        if base in _SPATIAL_TYPES:
            substituted.append(column.name)
            new_columns.append(column.model_copy(update={"mysql_type": "LONGBLOB"}))
        else:
            new_columns.append(column)
    if not substituted:
        return table, []
    return table.model_copy(update={"columns": new_columns}), substituted


def _unparsable_table_conversion(
    table: TableDef, exc: Exception
) -> "TableConversion":
    """Build an UNSUPPORTED conversion for a table whose source DDL won't parse.

    Mirrors the view fallback (comment placeholder + classified warning, never a
    raise) so one unparsable table does not blank the whole Schema Conversion
    step. Names the offending spatial column(s) when that is the cause.
    """
    spatial = _spatial_columns(table)
    if spatial:
        reason = (
            "uses MySQL spatial column(s) (" + ", ".join(spatial) + ") that "
            "Aurora DSQL does not support (no PostGIS/geometry types)"
        )
    else:
        reason = (
            "uses a MySQL type the converter cannot parse for Aurora DSQL "
            f"({type(exc).__name__})"
        )
    return TableConversion(
        table=table.name,
        target_ddl=(
            f"-- Could not auto-convert table {table.name} for Aurora DSQL; it "
            f"{reason}. Redesign the table (remove or replace the unsupported "
            "column) and reimplement it manually."
        ),
        schema_ddls=[],
        index_ddls=[],
        preserved_foreign_keys=list(table.foreign_keys),
        warnings=[
            ConversionWarning(
                object_name=table.name,
                classification=Classification.UNSUPPORTED,
                message=(
                    f"Table {table.name} could not be auto-converted: it {reason}. "
                    "Redesign it and reimplement manually on Aurora DSQL."
                ),
            )
        ],
    )


def _invalid_composite_conversion(table: TableDef, reason: str) -> "TableConversion":
    """Build an UNSUPPORTED conversion for an invalid COMPOSITE_KEY request.

    Mirrors :func:`_unparsable_table_conversion`: a bad leading-column choice
    surfaces this one table as UNSUPPORTED (comment placeholder + classified
    warning) so the user must fix the choice, rather than raising and blanking the
    whole Schema Conversion step. The UI validates before applying, so this is the
    backstop for a stale/forced options object.
    """
    return TableConversion(
        table=table.name,
        target_ddl=(
            f"-- Could not apply a composite primary key to {table.name}: {reason}"
        ),
        schema_ddls=[],
        index_ddls=[],
        preserved_foreign_keys=list(table.foreign_keys),
        warnings=[
            ConversionWarning(
                object_name=table.name,
                classification=Classification.UNSUPPORTED,
                message=(
                    f"Composite primary key not applied to {table.name}: {reason}"
                ),
            )
        ],
    )


def _strip_column_collation(column_def: exp.ColumnDef) -> Optional[str]:
    """Remove a MySQL ``COLLATE <name>`` constraint from ``column_def`` in place.

    Returns a human message naming the dropped collation when one was removed, or
    ``None`` when the column had no collation. DSQL/PostgreSQL does not recognize
    MySQL collation names, and the reconstructed type renders the clause as
    ``COLLATE '<name>'`` (invalid PostgreSQL DDL), so the clause must be dropped;
    the column then uses the database default collation.
    """
    constraints = list(column_def.args.get("constraints") or [])
    kept: list[exp.Expression] = []
    dropped: Optional[str] = None
    for constraint in constraints:
        kind = getattr(constraint, "kind", None)
        if isinstance(kind, exp.CollateColumnConstraint):
            collation = kind.this
            dropped = collation.name if hasattr(collation, "name") else str(collation)
            continue
        kept.append(constraint)
    if dropped is None:
        return None
    column_def.set("constraints", kept)
    return (
        f"MySQL collation {dropped!r} is dropped; Aurora DSQL uses the default "
        "collation. Sort order and case-/accent-insensitive equality may differ, "
        "which can affect unique keys and ORDER BY on this column."
    )


def _add_enum_check(column_def: exp.ColumnDef, enum_values: tuple[str, ...]) -> None:
    """Append a ``CHECK (col IN (...))`` constraint preserving the ENUM domain."""
    if not enum_values:
        return
    check = exp.column(column_def.name, quoted=True).isin(
        *(exp.Literal.string(value) for value in enum_values)
    )
    constraint = exp.ColumnConstraint(kind=exp.CheckColumnConstraint(this=check))
    constraints = list(column_def.args.get("constraints") or [])
    constraints.append(constraint)
    column_def.set("constraints", constraints)


# ---------------------------------------------------------------------------
# DSQL constraint application (foreign keys, indexes, primary-key strategy)
# ---------------------------------------------------------------------------


def _quote_pg_identifier(name: str) -> str:
    """Return ``name`` as a safely double-quoted PostgreSQL identifier."""
    return exp.to_identifier(name, quoted=True).sql(dialect=_POSTGRES)


def _quote_pg_qualified(name: str) -> str:
    """Double-quote a possibly-qualified name as ``"schema"."table"``.

    An unqualified name renders exactly like :func:`_quote_pg_identifier`.
    """
    schema, obj = _split_qualified(name)
    if schema is not None:
        return f"{_quote_pg_identifier(schema)}.{_quote_pg_identifier(obj)}"
    return _quote_pg_identifier(obj)


def _wide_indexes(table: TableDef) -> list[tuple[str, int]]:
    """Return ``(index name, column count)`` for indexes over the DSQL key-column cap.

    Shared by :func:`_build_index_ddls` (which skips them) and
    :func:`_too_many_key_columns_warning` (which explains why), so the emitted DDL
    and the note can never disagree about which indexes were dropped.
    """
    return [
        (index.name, len(index.columns))
        for index in table.indexes
        if len(index.columns) > _DSQL_MAX_PK_COLUMNS
    ]


def _build_index_ddls(table: TableDef) -> list[str]:
    """Render the table's secondary indexes as ``CREATE INDEX ASYNC`` statements.

    DSQL builds secondary indexes asynchronously, so each index is emitted as a
    standalone ``CREATE INDEX ASYNC`` (Requirement 3.4). Identifiers are quoted
    via ``sqlglot`` to avoid injection (Requirement 9.4). The index name stays
    unqualified (created in the table's schema); the table reference is
    schema-qualified when the table name is.

    An index over DSQL's 8-column key limit is SKIPPED rather than emitted: MySQL
    allows 16 columns per index, and DSQL rejects a 9+-column key with error 54011.
    Because secondary indexes are built AFTER the data loads, emitting it anyway put
    the guaranteed failure at the very end of a multi-hour Full Load. Skipping keeps
    the applied script one that can actually succeed; the operator is told which
    indexes were left out (and why) by :func:`_too_many_key_columns_warning`.
    """
    table_identifier = _quote_pg_qualified(table.name)
    statements: list[str] = []
    for index in table.indexes:
        if len(index.columns) > _DSQL_MAX_PK_COLUMNS:
            continue
        unique = "UNIQUE " if index.unique else ""
        columns = ", ".join(_quote_pg_identifier(column) for column in index.columns)
        statements.append(
            f"CREATE {unique}INDEX ASYNC {_quote_pg_identifier(index.name)} "
            f"ON {table_identifier} ({columns})"
        )
    return statements


def _find_column_def(create: exp.Expression, column_name: str) -> Optional[exp.ColumnDef]:
    """Return the parsed :class:`exp.ColumnDef` named ``column_name``, if any."""
    for column_def in create.find_all(exp.ColumnDef):
        if column_def.name == column_name:
            return column_def
    return None


class CompositeKeyError(ValueError):
    """A requested COMPOSITE_KEY leading column is invalid for Aurora DSQL.

    Raised by :func:`_apply_composite_key` (and surfaced by the pure validator
    :func:`validate_composite_leading_column`) when the chosen leading column
    would produce a primary key DSQL rejects -- e.g. the column does not exist, is
    nullable, is already in the key, or the composite key would exceed DSQL's
    column-count or byte limits. The message is user-facing (English).
    """


# Best-effort byte sizes for the DSQL 1 KiB combined-key budget, keyed by the
# converted (target) sqlglot DataType. DSQL enforces the real limit at runtime;
# this catches an obviously-too-large composite key at conversion time so the user
# sees the problem in Schema Conversion instead of a 54000 error mid-load.
_T = exp.DataType.Type
_KEY_TYPE_BYTES: dict = {
    _T.BOOLEAN: 1,
    _T.TINYINT: 1,
    _T.SMALLINT: 2,
    _T.INT: 4,
    _T.BIGINT: 8,
    _T.FLOAT: 4,
    _T.DOUBLE: 8,
    _T.DATE: 4,
    _T.TIME: 8,
    _T.TIMESTAMP: 8,
    _T.TIMESTAMPTZ: 8,
    _T.UUID: 16,
}
# Per-column cap DSQL applies to a variable-length (char/varchar/text) column when
# it participates in a key, regardless of the column's declared length.
_DSQL_MAX_VARLEN_KEY_BYTES = 255
_VARLEN_KEY_TYPES = frozenset({_T.VARCHAR, _T.CHAR, _T.NCHAR, _T.NVARCHAR, _T.BPCHAR, _T.TEXT})


def _estimate_key_column_bytes(create: exp.Expression, column_name: str) -> int:
    """Estimate the DSQL key bytes a converted column contributes (upper bound).

    Reads the post-type-mapping :class:`exp.DataType` from ``create`` so the
    estimate reflects the TARGET type (e.g. a MySQL ``INT`` widened to ``bigint``).
    Variable-length string types are counted at the DSQL 255-byte key cap (their
    per-column ceiling in a key); ``numeric(p, s)`` at roughly ``ceil(p/2) + 2``;
    an unknown type conservatively at the 255-byte cap so we never under-count.
    """
    column_def = _find_column_def(create, column_name)
    data_type = column_def.args.get("kind") if column_def is not None else None
    if not isinstance(data_type, exp.DataType):
        return _DSQL_MAX_VARLEN_KEY_BYTES
    kind = data_type.this
    if kind in _KEY_TYPE_BYTES:
        return _KEY_TYPE_BYTES[kind]
    if kind in (_T.DECIMAL,):
        params = [int(e.name) for e in data_type.expressions if e.name.isdigit()]
        precision = params[0] if params else 38
        return (precision // 2) + 2
    if kind in _VARLEN_KEY_TYPES:
        return _DSQL_MAX_VARLEN_KEY_BYTES
    # Any other converted type (e.g. bytea fallback) -- count the key cap so a
    # pathological choice is caught rather than silently under-counted.
    return _DSQL_MAX_VARLEN_KEY_BYTES


def _composite_key_columns(table: TableDef, leading: str) -> list[str]:
    """The resulting composite key order: leading column first, then the source PK."""
    return [leading] + [c for c in table.primary_key if c != leading]


def validate_composite_leading_column(
    table: TableDef, leading: str, create: Optional[exp.Expression] = None
) -> Optional[str]:
    """Return an error message if ``leading`` is an invalid composite-key leader.

    Pure, side-effect-free validation of the DSQL structural rules (so the UI can
    call it to gate the picker and render an inline error, and the converter can
    call it before mutating the DDL -- one source of truth). Returns ``None`` when
    the leading column is valid. ``create`` is the parsed (post-type-mapping)
    CREATE node used for the byte estimate; when omitted the byte check is skipped
    (the UI pre-check does not have it, and DSQL still enforces the limit).

    Blocking rules (never emit a key DSQL would reject):
    - the table has a primary key to prepend to;
    - the leading column exists on the table;
    - the leading column is NOT NULL (a key column cannot be nullable);
    - the leading column is not already part of the primary key (no-op / reorder);
    - the resulting key has at most 8 columns;
    - the estimated combined key size is at most 1 KiB.
    """
    if not table.primary_key:
        return (
            f"Table '{table.name}' has no primary key to extend into a composite "
            "key. Add a primary key first."
        )
    column = next((c for c in table.columns if c.name == leading), None)
    if column is None:
        return (
            f"Leading column '{leading}' does not exist on table '{table.name}'. "
            "Choose an existing column."
        )
    if column.nullable:
        return (
            f"Leading column '{leading}' is nullable; a primary-key column must be "
            "NOT NULL. Choose a NOT NULL column."
        )
    if leading in table.primary_key:
        return (
            f"Leading column '{leading}' is already part of the primary key, so "
            "prepending it would not change write distribution. Choose a "
            "high-cardinality column that is not already in the key."
        )
    key_columns = _composite_key_columns(table, leading)
    if len(key_columns) > _DSQL_MAX_PK_COLUMNS:
        return (
            f"The composite key {tuple(key_columns)} has {len(key_columns)} "
            f"columns; Aurora DSQL allows at most {_DSQL_MAX_PK_COLUMNS}."
        )
    if create is not None:
        total = sum(_estimate_key_column_bytes(create, c) for c in key_columns)
        if total > _DSQL_MAX_KEY_BYTES:
            return (
                f"The composite key {tuple(key_columns)} is estimated at ~{total} "
                f"bytes, over Aurora DSQL's {_DSQL_MAX_KEY_BYTES}-byte key limit. "
                "Choose a smaller leading column."
            )
    return None


def _apply_composite_key(
    create: exp.Expression, table: TableDef, leading: str
) -> ConversionWarning:
    """Prepend ``leading`` to the primary key in ``create`` (COMPOSITE_KEY).

    Rewrites the parsed table-level ``PRIMARY KEY`` node to
    ``(leading, original_pk...)`` so DSQL scatters writes by the high-cardinality
    leading column instead of funnelling them into one key range. The source data
    is untouched -- only the target key definition changes. Raises
    :class:`CompositeKeyError` when the leading column is invalid (see
    :func:`validate_composite_leading_column`). Returns a MANUAL warning stating
    the consequence (the application must key on the new composite key, and the
    leading column must be immutable).
    """
    error = validate_composite_leading_column(table, leading, create)
    if error is not None:
        raise CompositeKeyError(error)

    key_columns = _composite_key_columns(table, leading)
    primary_key = next(iter(create.find_all(exp.PrimaryKey)), None)
    if primary_key is None:
        # _build_source_ddl always emits a table-level PRIMARY KEY, so this is a
        # defensive guard, not an expected path.
        raise CompositeKeyError(
            f"Table '{table.name}' has no table-level PRIMARY KEY clause to rewrite."
        )
    primary_key.set(
        "expressions",
        [exp.to_identifier(name, quoted=True) for name in key_columns],
    )
    original = ", ".join(table.primary_key)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        message=(
            f"Primary key changed to composite {tuple(key_columns)} (leading "
            f"column '{leading}') to spread writes across Aurora DSQL partitions "
            f"and avoid the hot partition a monotonic key ({original}) causes. "
            "IMPORTANT: the application's queries, joins, and upserts must now key "
            f"on {tuple(key_columns)}, and the leading column '{leading}' must be "
            "immutable (DSQL primary keys cannot change after creation, and CDC "
            f"keys on it). A UNIQUE INDEX on the original key ({original}) is added "
            "to preserve its uniqueness."
        ),
    )


def _composite_unique_index_ddl(table: TableDef) -> str:
    """A ``CREATE UNIQUE INDEX ASYNC`` preserving the original PK's uniqueness.

    A composite key ``(leading, id)`` no longer makes the original key (``id``)
    unique on its own, so global uniqueness of the source key would be silently
    lost. This emits a unique async index on the original primary-key columns to
    keep that guarantee. Identifiers are quoted via ``sqlglot`` (Requirement 9.4);
    the index name is unqualified (created in the table's schema).
    """
    _, obj = _split_qualified(table.name)
    index_name = "ux_" + "_".join([obj, *table.primary_key])
    columns = ", ".join(_quote_pg_identifier(c) for c in table.primary_key)
    return (
        f"CREATE UNIQUE INDEX ASYNC {_quote_pg_identifier(index_name)} "
        f"ON {_quote_pg_qualified(table.name)} ({columns})"
    )


def _apply_pk_strategy(
    create: exp.Expression, table: TableDef, strategy: PrimaryKeyStrategy
) -> list[ConversionWarning]:
    """Apply the primary-key strategy to a monotonic AUTO_INCREMENT key.

    Rewrites the auto-increment column in ``create`` according to ``strategy``
    and returns the warnings describing the applied strategy (Requirement 3.5):
    always a hot-partition RECOMMENDATION, plus -- for ``IDENTITY_WITH_CACHE`` on a
    key whose mapped type is wider than signed BIGINT (``bigint unsigned`` ->
    ``numeric(20,0)``) -- a range-narrowing LOSS warning, because DSQL identity
    columns must be BIGINT and any existing source value above 2^63-1 would then fail
    to load. Returns ``[]`` when the table has no AUTO_INCREMENT column.
    """
    column_name = table.auto_increment_column
    if not column_name:
        return []

    column_def = _find_column_def(create, column_name)
    # Set when the identity widening NARROWED the declared range (see below), so the
    # caller can add a LOSS warning alongside the throughput recommendation.
    narrowed_from: Optional[str] = None

    if strategy is PrimaryKeyStrategy.CONVERT_TO_UUID:
        if column_def is not None:
            column_def.set("kind", _build("uuid"))
        message = (
            f"The primary key from AUTO_INCREMENT column '{column_name}' was "
            "converted to uuid, which spreads inserts across Aurora DSQL partitions "
            "(a monotonically increasing key concentrates writes on one, since DSQL "
            "stores rows in primary-key order). The application must now generate "
            "UUID key values."
        )
    elif strategy is PrimaryKeyStrategy.IDENTITY_WITH_CACHE:
        if column_def is not None:
            # Aurora DSQL identity columns must be BIGINT: its sequences are BIGINT-only,
            # so an INT/SMALLINT identity is rejected outright ("datatype integer not
            # supported, identity column type must be bigint" -- confirmed live). A MySQL
            # `int AUTO_INCREMENT` primary key is extremely common, so without this
            # widening the whole CREATE TABLE failed for the typical table. For the
            # signed integer types the widening is LOSSLESS (BIGINT holds every INT value).
            #
            # DECIMAL is here because an UNSIGNED integer key maps to numeric/DECIMAL to
            # preserve its range (`bigint unsigned` -> numeric(20,0)), and
            # `bigint unsigned AUTO_INCREMENT` is a common primary key. For future
            # sequence-GENERATED values this is safe (a DSQL identity is BIGINT-bounded
            # anyway). BUT it NARROWS the declared range for EXISTING rows: a source
            # `bigint unsigned` value above 2^63-1 (up to 2^64-1) does not fit BIGINT and
            # would fail to load (SQLSTATE 22003). That is not silent -- we flag it as a
            # LOSS warning via ``narrowed_from`` so the operator can decide.
            data_type = column_def.args.get("kind")
            if isinstance(data_type, exp.DataType) and data_type.this in (
                exp.DataType.Type.INT,
                exp.DataType.Type.SMALLINT,
                exp.DataType.Type.TINYINT,
                exp.DataType.Type.MEDIUMINT,
                exp.DataType.Type.DECIMAL,
                exp.DataType.Type.BIGINT,
            ):
                # numeric/DECIMAL is the only source kind whose range EXCEEDS signed
                # BIGINT, so it is the only one the BIGINT widening actually narrows.
                if data_type.this is exp.DataType.Type.DECIMAL:
                    narrowed_from = data_type.sql(dialect=_POSTGRES)
                column_def.set("kind", exp.DataType.build("BIGINT"))
            # sqlglot cannot render an identity CACHE clause, so the constraint
            # text is injected as a fixed (non-user) constant.
            identity = exp.var(
                f"GENERATED BY DEFAULT AS IDENTITY (CACHE {_IDENTITY_CACHE_SIZE})"
            )
            constraints = list(column_def.args.get("constraints") or [])
            constraints.append(exp.ColumnConstraint(kind=identity))
            column_def.set("constraints", constraints)
        message = (
            f"The primary key from AUTO_INCREMENT column '{column_name}' was "
            f"converted to a cached identity (CACHE {_IDENTITY_CACHE_SIZE}), which "
            "spreads inserts across Aurora DSQL nodes (a monotonically increasing "
            "key concentrates writes on one partition, since DSQL stores rows in "
            "primary-key order)."
        )
    else:  # PrimaryKeyStrategy.KEEP_INTEGER
        message = (
            f"The integer key from AUTO_INCREMENT column '{column_name}' was kept as a "
            "plain integer. IMPORTANT: Aurora DSQL will NOT auto-generate this key after "
            "cut-over (there is no AUTO_INCREMENT/identity on it), so the application "
            "must supply the value on every insert — an app that relied on the database "
            "generating it will fail or collide. Choose the 'Server-generated (IDENTITY)' "
            "strategy instead if you want DSQL to fill the key. For higher insert "
            "throughput, consider a UUID/random key or a cached identity: DSQL stores "
            "rows in primary-key order, so a monotonically increasing key concentrates "
            "writes on one partition."
        )

    # A RECOMMENDATION, not a LOSS: every strategy produces a correct, complete target
    # key -- this is throughput advice about DSQL's partitioning, so it must not be
    # styled like a removed foreign key (which genuinely lost a constraint).
    warnings = [
        ConversionWarning(
            object_name=table.name,
            column_name=column_name,
            classification=Classification.MANUAL,
            kind=ConversionNoteKind.RECOMMENDATION,
            message=message,
        )
    ]
    # The identity widening narrowed the declared range (a `bigint unsigned` key mapped
    # to numeric(20,0), which DSQL cannot use for an identity, so it became BIGINT). New
    # generated ids are unaffected, but an EXISTING source value above 2^63-1 will not
    # fit and would fail Full Load with a numeric-out-of-range error -- a real, possible
    # data-load failure, hence a LOSS the operator must weigh, not silent advice.
    if narrowed_from is not None:
        warnings.append(
            ConversionWarning(
                object_name=table.name,
                column_name=column_name,
                source_type=narrowed_from,
                target_type="bigint",
                classification=Classification.MANUAL,
                kind=ConversionNoteKind.LOSS,
                message=(
                    f"The identity key '{column_name}' was narrowed from "
                    f"{narrowed_from} to bigint. Aurora DSQL identity columns must be "
                    "bigint, so an unsigned/large-decimal key cannot keep its full "
                    "range. Newly generated ids are unaffected, but any EXISTING source "
                    "value above 9223372036854775807 (2^63-1) will not fit and that row "
                    "would fail to load. If this key never exceeds that value the "
                    "conversion is safe; otherwise keep the source PK (no identity) or "
                    "move to a uuid key instead."
                ),
            )
        )
    return warnings


def _foreign_key_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Return a warning that the table's foreign keys were removed, if any.

    DSQL does not support foreign keys, so they are removed from the target DDL
    and preserved as metadata; referential integrity must move to the
    application layer (Requirement 3.3).
    """
    if not table.foreign_keys:
        return None
    names = ", ".join(fk.name for fk in table.foreign_keys)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        message=(
            f"Foreign key constraints ({names}) are not supported by Aurora DSQL "
            "and were removed from the DDL. They are preserved as referential "
            "metadata; enforce referential integrity in the application layer."
        ),
    )


# Aurora DSQL limits + type sets, imported from the assessor rather than restated, so the
# Evaluation rule and the matching conversion warning can never disagree. Restating a
# literal in both places is exactly how the two screens drifted apart before.
from dsql_migrator.core.assessor import (  # noqa: E402 - avoids duplicate literals
    _MAX_COLUMNS_PER_TABLE,
    _MAX_INDEXES_PER_TABLE,
    _MAX_SECONDARY_INDEXES_PER_TABLE,
    _OVERSIZED_LOB_BASES,
    _UNSUPPORTED_INDEX_TYPES,
    _base_type,
)


def _generated_column_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn that a MySQL generated column becomes an ordinary column on the target.

    The conversion looks harmless -- the column is there with the right type -- but the
    computation is gone: MySQL maintained the value, and DSQL will not. Full Load copies
    whatever the source had computed, so the target starts CORRECT and then silently drifts
    the first time the application inserts or updates a row without supplying the value.
    Evaluation reports it (``GENERATED_COLUMN``); conversion said nothing, and the target
    DDL looks identical to a plain column, so there was no way to see it here.

    The expression itself is not in the inventory (only the boolean), which is why this
    names the columns and points at the source rather than quoting the formula.
    """
    columns = [column.name for column in table.columns if column.generated]
    if not columns:
        return None
    names = ", ".join(columns)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"Columns ({names}) are MySQL GENERATED (computed) columns; Aurora DSQL has "
            "no equivalent, so they are created as ORDINARY columns. Full Load copies the "
            "values the source already computed, so the target starts correct — but "
            "nothing maintains them afterwards, so any insert/update that does not supply "
            "the value will drift. Compute it in the application (or in the query) before "
            "cut over. The generating expression is not captured here; read it from the "
            "source with SHOW CREATE TABLE."
        ),
    )


def _collation_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn about case-INSENSITIVE collations, which change query results on the target.

    The quietest change in the whole conversion: nothing in the DDL looks different, no row
    is lost, and every count and checksum matches -- but ``WHERE email = 'A@x.com'`` stops
    matching ``a@x.com``, and a UNIQUE index that rejected ``Bob`` beside ``bob`` now
    accepts both. Evaluation flags it (``CI_COLLATION``); conversion was silent, so the one
    screen showing the DDL side by side gave no hint.

    Only ``_ci`` collations are reported: a case-SENSITIVE (``_cs``) or binary (``_bin``)
    collation already matches PostgreSQL's default comparison, so it is not a change.
    """
    columns = [
        column.name
        for column in table.columns
        if column.collation and column.collation.strip().lower().endswith("_ci")
    ]
    if not columns:
        return None
    names = ", ".join(columns)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"Columns ({names}) use a case-INSENSITIVE MySQL collation. Aurora DSQL "
            "compares text case-SENSITIVELY, so equality, LIKE, ORDER BY and UNIQUE "
            "behaviour change even though the data migrates exactly and every row count "
            "and checksum will match. Queries that relied on case-insensitive matching "
            "need LOWER(...) on both sides (with a matching expression index), and a "
            "UNIQUE column that rejected 'Bob' beside 'bob' will now accept both."
        ),
    )


def _on_update_timestamp_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn that ``ON UPDATE CURRENT_TIMESTAMP`` auto-maintenance is not reproduced.

    The DEFAULT survives, so the column looks fully converted -- and it is, for INSERTs.
    What is gone is the UPDATE half: MySQL refreshed the value on every row change, and
    DSQL has neither an ON UPDATE clause nor triggers to do it. So ``updated_at`` freezes
    at its insert time and every consumer of it (cache invalidation, incremental syncs,
    audit trails, "recently changed" queries) quietly reads stale data. Evaluation reports
    it (``ON_UPDATE_TIMESTAMP``); the conversion screen did not.
    """
    columns = [column.name for column in table.columns if column.auto_update_timestamp]
    if not columns:
        return None
    names = ", ".join(columns)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"Columns ({names}) use MySQL ON UPDATE CURRENT_TIMESTAMP. The DEFAULT is "
            "kept, so inserts still stamp them, but Aurora DSQL has no ON UPDATE clause "
            "and no triggers — nothing refreshes the value on an UPDATE. The application "
            "must set it explicitly on every write, or the column freezes at insert time "
            "and anything reading it (cache invalidation, incremental syncs, audit "
            "trails) silently goes stale."
        ),
    )


def _too_many_columns_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn when the table exceeds Aurora DSQL's hard 255-column limit.

    This is a HARD limit, so the generated CREATE TABLE is rejected at apply -- the
    conversion looks clean, then the apply fails. Evaluation rates it UNSUPPORTED /
    SIGNIFICANT; conversion said nothing, which is the worst combination for a limit the
    operator cannot discover any other way until the apply errors.
    """
    count = len(table.columns)
    if count <= _MAX_COLUMNS_PER_TABLE:
        return None
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.UNSUPPORTED,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"This table has {count} columns; Aurora DSQL allows at most "
            f"{_MAX_COLUMNS_PER_TABLE} per table. The generated CREATE TABLE will be "
            "REJECTED as-is — split the table (vertical partitioning by access pattern, "
            "sharing the primary key) or drop unused columns before applying."
        ),
    )


def _too_many_indexes_warning(
    table: TableDef, extra_secondary_indexes: int = 0
) -> Optional[ConversionWarning]:
    """Warn when the table's secondary indexes exceed DSQL's per-table budget.

    DSQL allows 24 indexes per table and the PRIMARY KEY counts toward that budget
    (verified on a live cluster), so a migrated table can carry at most 23 secondary
    indexes. Past that the ``CREATE INDEX ASYNC`` statements fail with error 54000 -- and
    because indexes are applied AFTER the table, the table itself succeeds first, leaving a
    partially-indexed target. Nothing on the conversion screen said so.

    ``extra_secondary_indexes`` counts index(es) the CONVERSION adds beyond the source's
    own -- notably the 1 UNIQUE index the COMPOSITE_KEY strategy emits to preserve the
    original key's uniqueness. Omitting it undercounted by one, so a table with exactly
    23 source indexes converted with COMPOSITE_KEY produced 25 total (> 24) yet passed
    this gate silently and failed the extra CREATE INDEX ASYNC after the load.

    Counts only the indexes actually EMITTED: one over DSQL's 8-column key limit is
    skipped by :func:`_build_index_ddls` (and reported by
    :func:`_too_many_key_columns_warning`), so counting it here would claim a budget
    overflow the applied script cannot hit.
    """
    count = (
        len(table.indexes) - len(_wide_indexes(table)) + extra_secondary_indexes
    )
    if count <= _MAX_SECONDARY_INDEXES_PER_TABLE:
        return None
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"This table has {count} secondary indexes; with the required primary key "
            f"that is {count + 1} against Aurora DSQL's limit of "
            f"{_MAX_INDEXES_PER_TABLE} per table, so at most "
            f"{_MAX_SECONDARY_INDEXES_PER_TABLE} can be created. The extra CREATE INDEX "
            "ASYNC statements will fail (error 54000) after the table itself is created, "
            "leaving the target partially indexed — drop the indexes you no longer need "
            "before applying, keeping the ones your queries actually use."
        ),
    )


def _too_many_key_columns_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn when the primary key or a secondary index exceeds DSQL's 8-column key cap.

    MySQL allows 16 columns per index, Aurora DSQL only 8 (error 54011 "more than 8
    column keys are not allowed"), so a 9..16-column key comes from a perfectly valid
    source and must be surfaced here -- the conversion otherwise looked clean:

    - a **primary key** over the cap makes the generated ``CREATE TABLE`` REJECTED at
      apply, so the table and its data never migrate (UNSUPPORTED);
    - a **secondary index** over the cap is now SKIPPED by :func:`_build_index_ddls`
      (emitting it would guarantee a post-load ``CREATE INDEX ASYNC`` failure), so the
      target is missing that index -- a real LOSS the operator has to know about, since
      queries relying on it will fall back to scans.

    Reported as one note per table, classified by the worst case present.
    """
    pk_columns = len(table.primary_key)
    pk_over = pk_columns > _DSQL_MAX_PK_COLUMNS
    wide = _wide_indexes(table)
    if not pk_over and not wide:
        return None

    parts: list[str] = []
    if pk_over:
        parts.append(
            f"the primary key spans {pk_columns} columns "
            f"({', '.join(table.primary_key)}), so the generated CREATE TABLE will be "
            "REJECTED as-is and neither the table nor its data migrates — narrow the "
            f"key to at most {_DSQL_MAX_PK_COLUMNS} columns (move the trailing columns "
            "to a secondary index) before applying"
        )
    if wide:
        listed = ", ".join(f"{name} ({count} columns)" for name, count in wide)
        parts.append(
            f"secondary index(es) {listed} exceed the limit and were therefore NOT "
            "emitted — the target will be missing them (emitting them would fail the "
            "post-load CREATE INDEX ASYNC after every row was already written). Trim "
            f"each to at most {_DSQL_MAX_PK_COLUMNS} columns, most selective leftmost, "
            "and add it after the migration"
        )
    return ConversionWarning(
        object_name=table.name,
        classification=(
            Classification.UNSUPPORTED if pk_over else Classification.MANUAL
        ),
        kind=ConversionNoteKind.LOSS,
        message=(
            f"Aurora DSQL allows at most {_DSQL_MAX_PK_COLUMNS} columns in a primary "
            "key or secondary index (MySQL allows 16): "
            f"{'; also, '.join(parts)}."
        ),
    )


def _oversized_lob_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn about LOB/TEXT columns whose values can exceed DSQL's 1 MiB per-value cap.

    The DDL converts and applies fine; the failure lands later, per ROW, during Full Load
    or CDC -- which is why it belongs on this screen too. Those rows are quarantined
    (Full Load) or dead-lettered (CDC), so the migration reports a gap that cannot be
    fixed by reloading: the value simply does not fit. Naming the columns here is what lets
    the operator act before the load rather than triage dropped rows afterwards.
    """
    columns = [
        column.name
        for column in table.columns
        if _base_type(column.mysql_type) in _OVERSIZED_LOB_BASES
    ]
    if not columns:
        return None
    names = ", ".join(columns)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.RECOMMENDATION,
        message=(
            f"Columns ({names}) are MySQL LOB/TEXT types that can hold more than Aurora "
            "DSQL's ~1 MiB per-value limit. The DDL itself is fine — the limit bites per "
            "ROW during migration: any oversized value is permanently dropped "
            "(quarantined in Full Load, dead-lettered in CDC) and reloading cannot fix "
            "it. Check the largest values now; if any exceed 1 MiB, move that content to "
            "Amazon S3 and store a reference instead."
        ),
    )


def _unsupported_index_type_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn that a FULLTEXT / SPATIAL index becomes an ordinary B-tree index.

    The converter emits every secondary index as ``CREATE INDEX ASYNC``, which for a
    FULLTEXT index is not an equivalent -- it is a plain B-tree on the same column, and
    the ``MATCH ... AGAINST`` queries that index existed for cannot run against it at all.
    Yet the conversion produced the SAME DDL as an ordinary index with no note, while
    Evaluation rates this UNSUPPORTED with SIGNIFICANT effort (``UNSUPPORTED_INDEX_TYPE``)
    -- its most severe rating. An operator reading only the conversion screen had no
    indication that a full-text search feature had just been silently dropped.

    Classified LOSS, not RECOMMENDATION: unlike a partitioned table (where DSQL's own
    distribution replaces the mechanism), there is no target-side substitute -- the
    capability is gone and the queries must be rebuilt elsewhere.
    """
    flagged = [
        f"{index.name} ({index.index_type})"
        for index in table.indexes
        if index.index_type and index.index_type.strip().lower() in _UNSUPPORTED_INDEX_TYPES
    ]
    if not flagged:
        return None
    names = ", ".join(flagged)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.UNSUPPORTED,
        kind=ConversionNoteKind.LOSS,
        message=(
            f"Aurora DSQL has no FULLTEXT or SPATIAL index type, so {names} is emitted as "
            "an ordinary CREATE INDEX ASYNC on the same column(s). The index is created, "
            "but it is NOT an equivalent: MATCH ... AGAINST / spatial-operator queries "
            "cannot use it and will fail or table-scan. Move that search outside the "
            "database (e.g. Amazon OpenSearch Service) or redesign the query, and drop "
            "the index if nothing else uses those columns."
        ),
    )


def _prefix_index_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Warn that a MySQL prefix index (``KEY (col(N))``) becomes a FULL-column index.

    MySQL indexes only the first N bytes/chars of a long column; DSQL has no prefix-
    index equivalent, so the converter emits ``CREATE INDEX ASYNC`` on the WHOLE column.
    Two consequences the operator must see BEFORE a multi-hour load:

    - a variable-length column whose full value exceeds DSQL's ~255-byte key limit makes
      the ``CREATE INDEX ASYNC`` fail (after the table + data are already applied), and
    - even when it fits, indexing the full value is a semantic change from the prefix.

    Without this the only signal was the per-value OVERSIZED_LOB note (about row size,
    not the index), so the index failure surfaced only post-load. RECOMMENDATION, not a
    hard LOSS: the index is still created for columns within the key limit.
    """
    flagged: list[str] = []
    for index in table.indexes:
        for column, length in sorted(index.prefix_lengths.items()):
            flagged.append(f"{index.name}({column}({length}))")
    if not flagged:
        return None
    names = ", ".join(flagged)
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.RECOMMENDATION,
        message=(
            f"Prefix index(es) {names} index only the first N characters/bytes in MySQL, "
            "but Aurora DSQL has no prefix index — the converter indexes the FULL "
            "column. If a value exceeds DSQL's ~255-byte index-key limit the "
            "CREATE INDEX ASYNC fails AFTER the table and its data are loaded, leaving "
            "the table without that index. Before loading, confirm the column's values "
            "fit the key limit, or replace the prefix index with an expression index on "
            "a bounded substring (e.g. on left(col, N)) / drop it if unused."
        ),
    )


def _partitioned_table_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Return a warning that the source table's native partitioning was dropped.

    The TARGET DDL is already correct -- DSQL has no user-visible partitioning (it
    distributes by primary key internally), so emitting no ``PARTITION BY`` is right. But
    the conversion screen said NOTHING about it, so a partitioned source converted to a
    silently-plain table: the operator saw a clean conversion with no note, even though
    Evaluation had reported it MANUAL (``PARTITIONED_TABLE``) with MEDIUM effort. Same
    inconsistency as the source DDL omitting AUTO_INCREMENT -- one screen names it, the
    next does not.

    A RECOMMENDATION, not a LOSS: nothing about the data or the key changed, and DSQL
    distributes automatically, so this is not a dropped constraint like a foreign key.
    What matters to the operator is that partition-specific SQL and operations do not
    carry over.
    """
    if not table.partitioned:
        return None
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.MANUAL,
        kind=ConversionNoteKind.RECOMMENDATION,
        message=(
            "The source table uses MySQL native partitioning, which Aurora DSQL does "
            "not have — DSQL distributes rows automatically by primary key, so no "
            "PARTITION BY clause is emitted and no data is lost. Review anything that "
            "depended on the partitions themselves: partition-scoped SQL "
            "(PARTITION (p1)), partition pruning assumptions, and partition-level "
            "maintenance such as DROP/TRUNCATE PARTITION for archiving — the last one "
            "needs a different approach on DSQL (deleting by range), since it has no "
            "TRUNCATE."
        ),
    )


def _no_primary_key_warning(table: TableDef) -> Optional[ConversionWarning]:
    """Return a warning when the table has no primary key (DSQL requires one)."""
    if table.primary_key:
        return None
    return ConversionWarning(
        object_name=table.name,
        classification=Classification.UNSUPPORTED,
        message=(
            "Aurora DSQL requires every table to have a primary key. Add a "
            "primary key (e.g., a UUID/random key) before migrating the table."
        ),
    )


def _object_reimplementation_warnings(
    inventory: SourceInventory,
) -> list[ConversionWarning]:
    """Flag triggers and procedural routines as unsupported objects.

    DSQL has no trigger object and no procedural stored procedures/functions, so
    there is no target to convert them into; each is flagged ``UNSUPPORTED`` with
    a reimplementation recommendation (Requirement 3.7).
    """
    warnings: list[ConversionWarning] = []
    for trigger in inventory.triggers:
        warnings.append(
            ConversionWarning(
                object_name=trigger.name,
                classification=Classification.UNSUPPORTED,
                message=(
                    "Triggers are not supported by Aurora DSQL and are not "
                    "auto-converted. Reimplement the trigger logic in the "
                    "application or with event-driven processing."
                ),
            )
        )
    for routine in inventory.routines:
        warnings.append(
            ConversionWarning(
                object_name=routine.name,
                classification=Classification.UNSUPPORTED,
                message=(
                    "Procedural stored procedures/functions are not "
                    "auto-converted. Reimplement them as LANGUAGE SQL functions "
                    "or move the logic to the application."
                ),
            )
        )
    for event in inventory.events:
        warnings.append(
            ConversionWarning(
                object_name=event.name,
                classification=Classification.UNSUPPORTED,
                message=(
                    "Scheduled events are not supported by Aurora DSQL (no event "
                    "scheduler). Reimplement the schedule with Amazon EventBridge "
                    "Scheduler invoking a Lambda function."
                ),
            )
        )
    return warnings


# ---------------------------------------------------------------------------
# Converter
# ---------------------------------------------------------------------------


class SchemaConverter:
    """Converts MySQL table DDL to DSQL-compatible PostgreSQL DDL (Req 3.1, 3.2).

    This implements the type-mapping + transpile + warning-collection
    foundation. Each table is parsed with ``sqlglot`` (mysql), its column types
    are rewritten per the DSQL type-mapping table, and the result is rendered as
    ``postgres`` DDL with double-quoted identifiers.
    """

    def convert_table(
        self, table: TableDef, options: SchemaConvertOptions | None = None
    ) -> TableConversion:
        """Convert a single table definition to DSQL-compatible DDL.

        Applies the type mapping (Property 6) plus DSQL structural constraints:
        foreign keys are removed and preserved as metadata (Requirement 3.3),
        secondary indexes are emitted as ``CREATE INDEX ASYNC`` (Requirement 3.4),
        and the primary-key strategy plus hot-partition / primary-key-required
        warnings are applied (Requirement 3.5).
        """
        options = options or SchemaConvertOptions()
        # DSQL-unsupported source types (e.g. MySQL spatial) are substituted with
        # bytea so the table still converts and the data is PRESERVED as raw bytes
        # (never silently dropped/NULLed); a MANUAL warning is added per column.
        table_for_ddl, bytea_fallback_columns = _substitute_unsupported_types(table)
        source_ddl = _build_source_ddl(table_for_ddl)
        # A single table whose reconstructed DDL cannot be parsed (e.g. a MySQL
        # spatial type that sqlglot's MySQL dialect does not recognize and Aurora
        # DSQL has no equivalent for) must NOT abort the whole Schema Conversion
        # step. Isolate the failure: surface this one table as UNSUPPORTED with a
        # clear reason and let the caller keep converting the rest.
        try:
            create = sqlglot.parse_one(source_ddl, read=_MYSQL)
        except sqlglot.errors.SqlglotError as exc:
            # SqlglotError covers BOTH ParseError and TokenError. Catching only
            # ParseError meant a tokenizer failure -- e.g. an unbalanced quote in a
            # reflected column default, or a MySQL charset introducer like
            # ``_utf8mb4'x'`` -- escaped this guard and aborted the ENTIRE Schema
            # Conversion step instead of isolating the one table.
            return _unparsable_table_conversion(table, exc)

        source_types = {column.name: column.mysql_type for column in table.columns}
        warnings: list[ConversionWarning] = []

        # A source DEFAULT that could NOT be carried across must be reported. Aurora DSQL
        # supports column defaults, so most are preserved (see _column_default_sql) -- but
        # when one has to be dropped, saying nothing would hide a change in INSERT
        # behavior that only surfaces after cut-over. The severity is deliberately split:
        # a NOT NULL column losing its default is a functional break (an application
        # INSERT that omits the column succeeds on MySQL and is REJECTED on the target),
        # whereas a nullable column merely starts defaulting to NULL.
        for column in table.columns:
            if not column.default or column.generated:
                continue
            if column.name == table.auto_increment_column:
                continue
            _emitted, default_warning = _resolve_column_default(column)
            if default_warning is None:
                continue
            if not column.nullable:
                default_warning += (
                    " This column is NOT NULL, so an INSERT that omits it succeeds on "
                    "MySQL but will be REJECTED on Aurora DSQL -- set the value "
                    "explicitly in the application before cutting over."
                )
            warnings.append(
                ConversionWarning(
                    object_name=table.name,
                    column_name=column.name,
                    source_type=column.mysql_type,
                    target_type=column.mysql_type,
                    classification=Classification.MANUAL,
                    message=default_warning,
                )
            )

        for column_def in create.find_all(exp.ColumnDef):
            # Drop any MySQL column COLLATE clause: DSQL/PostgreSQL does not have
            # MySQL collation names (e.g. utf8mb4_general_ci), and the reconstructed
            # type carries it as ``COLLATE '<name>'`` which is invalid PostgreSQL DDL
            # (syntax error at the quoted collation). Dropping it falls back to the
            # database default collation; a warning records the dropped sort/equality
            # semantics. PKs / unique keys under DSQL's default collation may sort
            # differently than under a *_ci collation -- surfaced as MANUAL.
            collate_warning = _strip_column_collation(column_def)
            if collate_warning is not None:
                warnings.append(
                    ConversionWarning(
                        object_name=table.name,
                        column_name=column_def.name,
                        source_type=source_types.get(column_def.name, ""),
                        target_type=source_types.get(column_def.name, ""),
                        classification=Classification.MANUAL,
                        message=collate_warning,
                    )
                )

            data_type = column_def.args.get("kind")
            if not isinstance(data_type, exp.DataType):
                continue
            mapping = map_data_type(data_type)
            if mapping is None:
                continue

            column_def.set("kind", mapping.target)
            if mapping.enum_values is not None:
                _add_enum_check(column_def, mapping.enum_values)

            if mapping.message is not None:
                warnings.append(
                    ConversionWarning(
                        object_name=table.name,
                        column_name=column_def.name,
                        source_type=source_types.get(
                            column_def.name, data_type.sql(dialect=_MYSQL)
                        ),
                        target_type=mapping.target.sql(dialect=_POSTGRES),
                        classification=mapping.classification or Classification.MANUAL,
                        message=mapping.message,
                    )
                )

        # Spatial (and other DSQL-unsupported) columns were retyped to bytea above
        # so the data is preserved as raw bytes; surface that as a MANUAL decision.
        for column_name in bytea_fallback_columns:
            source_type = source_types.get(column_name, "")
            warnings.append(
                ConversionWarning(
                    object_name=table.name,
                    column_name=column_name,
                    source_type=source_type,
                    target_type="bytea",
                    classification=Classification.MANUAL,
                    message=(
                        f"MySQL type '{source_type}' has no Aurora DSQL equivalent; "
                        "preserved as raw bytes (bytea) so the data is not lost "
                        "(geometry is stored as WKB). Spatial indexes/operators are "
                        "unavailable -- edit the target type (e.g. text via WKT) or "
                        "drop the column if the geometry is not needed."
                    ),
                )
            )

        # Apply DSQL structural constraints (Requirements 3.3, 3.5). Foreign keys
        # are never emitted by _build_source_ddl, so removal only requires
        # preserving them as metadata plus a warning.
        #
        # COMPOSITE_KEY is distinct from the auto-increment strategies: it rewrites
        # the KEY COLUMN SET (prepending a high-cardinality leading column) rather
        # than the auto-increment column's type, and applies even when there is no
        # AUTO_INCREMENT column. An invalid leading column must not blank the whole
        # Schema Conversion step, so an invalid choice is isolated as UNSUPPORTED
        # (mirrors the unparsable-table fallback), never raised.
        extra_index_ddls: list[str] = []
        pk_strategy_warnings: list[ConversionWarning] = []
        if options.primary_key_strategy is PrimaryKeyStrategy.COMPOSITE_KEY:
            leading = options.composite_leading_column or ""
            try:
                composite_warning = _apply_composite_key(create, table, leading)
            except CompositeKeyError as exc:
                return _invalid_composite_conversion(table, str(exc))
            if composite_warning is not None:
                pk_strategy_warnings.append(composite_warning)
            # Preserve the original key's uniqueness, which a composite key drops.
            extra_index_ddls.append(_composite_unique_index_ddl(table))
        else:
            # _apply_pk_strategy returns 0..2 warnings: the throughput RECOMMENDATION
            # and (for an identity widening that narrows the range) a LOSS warning.
            pk_strategy_warnings.extend(
                _apply_pk_strategy(create, table, options.primary_key_strategy)
            )
        warnings.extend(pk_strategy_warnings)
        for optional_warning in (
            _no_primary_key_warning(table),
            _foreign_key_warning(table),
            _partitioned_table_warning(table),
            _unsupported_index_type_warning(table),
            _prefix_index_warning(table),
            _too_many_columns_warning(table),
            _too_many_indexes_warning(table, len(extra_index_ddls)),
            _too_many_key_columns_warning(table),
            _oversized_lob_warning(table),
            _generated_column_warning(table),
            _collation_warning(table),
            _on_update_timestamp_warning(table),
        ):
            if optional_warning is not None:
                warnings.append(optional_warning)

        target_ddl = create.sql(dialect=_POSTGRES, pretty=True)
        schema_name, _ = _split_qualified(table.name)
        schema_ddls = (
            [f"CREATE SCHEMA IF NOT EXISTS {_quote_pg_identifier(schema_name)}"]
            if schema_name is not None
            else []
        )
        return TableConversion(
            table=table.name,
            target_ddl=target_ddl,
            schema_ddls=schema_ddls,
            index_ddls=_build_index_ddls(table) + extra_index_ddls,
            preserved_foreign_keys=list(table.foreign_keys),
            warnings=warnings,
        )

    def convert(
        self, inventory: SourceInventory, options: SchemaConvertOptions | None = None
    ) -> SchemaConversionResult:
        """Convert every table in ``inventory`` to DSQL-compatible DDL.

        Triggers and procedural routines are flagged for manual reimplementation
        rather than auto-converted (Requirement 3.7). Warnings from all tables
        and objects are aggregated into the result (Property 6).
        """
        options = options or SchemaConvertOptions()
        conversions = [self.convert_table(table, options) for table in inventory.tables]
        view_conversions = [self.convert_view(view) for view in inventory.views]
        object_warnings = _object_reimplementation_warnings(inventory)
        return SchemaConversionResult.from_tables(
            conversions, object_warnings, views=view_conversions
        )

    def convert_view(self, view: ViewDef) -> ViewConversion:
        """Convert a single MySQL view to a DSQL-compatible ``CREATE VIEW``.

        Aurora DSQL is PostgreSQL-compatible and supports views, so the view's
        captured MySQL definition is transpiled to PostgreSQL via sqlglot and
        re-targeted to the view's (possibly schema-qualified) name, with a
        ``CREATE SCHEMA IF NOT EXISTS`` for its schema. If the definition is
        missing or cannot be parsed/transpiled, the view is surfaced for manual
        reimplementation (``auto_converted=False``, a comment placeholder) with a
        ``MANUAL`` warning -- never silently broken DDL. Table references inside
        the view come from the source definition as-is (best effort); the user
        can edit the converted DDL before applying.
        """
        schema_name, _ = _split_qualified(view.name)
        schema_ddls = (
            [f"CREATE SCHEMA IF NOT EXISTS {_quote_pg_identifier(schema_name)}"]
            if schema_name is not None
            else []
        )
        definition = (view.definition or "").strip()
        if not definition:
            return ViewConversion(
                view=view.name,
                target_ddl=(
                    f"-- View definition for {view.name} was not captured from the "
                    "source; reimplement this view manually for Aurora DSQL."
                ),
                schema_ddls=schema_ddls,
                auto_converted=False,
                warnings=[
                    ConversionWarning(
                        object_name=view.name,
                        classification=Classification.MANUAL,
                        message=(
                            "The view definition was not captured from the source; "
                            "reimplement the view manually on Aurora DSQL."
                        ),
                    )
                ],
            )
        try:
            parsed = sqlglot.parse_one(definition, read=_MYSQL)
            select = parsed.expression if isinstance(parsed, exp.Create) else parsed
            if select is None:
                raise ValueError("no SELECT body parsed from the view definition")
            create = exp.Create(
                this=exp.to_table(view.name), kind="VIEW", expression=select
            )
            target_ddl = create.sql(dialect=_POSTGRES, pretty=True)
        except Exception:  # noqa: BLE001 - any parse/generation failure -> manual
            return ViewConversion(
                view=view.name,
                target_ddl=(
                    f"-- Could not auto-convert view {view.name} for Aurora DSQL; "
                    "reimplement it manually after reviewing the source definition."
                ),
                schema_ddls=schema_ddls,
                auto_converted=False,
                warnings=[
                    ConversionWarning(
                        object_name=view.name,
                        classification=Classification.MANUAL,
                        message=(
                            "The view's SQL could not be automatically converted; "
                            "review and reimplement it manually on Aurora DSQL."
                        ),
                    )
                ],
            )
        return ViewConversion(
            view=view.name,
            target_ddl=target_ddl,
            schema_ddls=schema_ddls,
            auto_converted=True,
            warnings=[],
        )


__all__ = [
    # Re-exported from core.models so existing imports keep working.
    "ConversionNoteKind",
    "ConversionWarning",
    "TableConversion",
    "ViewConversion",
    "PrimaryKeyStrategy",
    "SchemaConvertOptions",
    "ExecutionUnitKind",
    "ExecutionUnit",
    "SchemaConversionResult",
    "SchemaConverter",
    "map_data_type",
    "map_mysql_type",
    "build_source_table_ddl",
    "parse_target_column_types",
    "parse_target_primary_key",
    "CompositeKeyError",
    "validate_composite_leading_column",
]
