# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source DDL reconstruction for the schema converter.

Kept in its own module -- not tangled into ``converter.py``'s MySQL-source logic -- per
the per-engine separation principle. The migration TARGET is Aurora DSQL (PostgreSQL-16
wire), so a PostgreSQL source is near-identity: this rebuilds a PostgreSQL ``CREATE
TABLE`` from the reflected :class:`TableDef` (whose column types are the EXACT PostgreSQL
strings captured by ``PostgresSourceDialect.enrich`` via ``format_type`` -- e.g.
``text[]``, ``numeric(12,2)``, ``timestamp with time zone``, ``uuid``, ``jsonb``). The
converter parses this with ``read="postgres"`` and re-enters the shared DSQL-constraint
phase (FK removal, primary-key strategy, ``CREATE INDEX ASYNC``) unchanged.

It emits columns (name + exact type + NOT NULL) and the primary key. Column DEFAULTs ARE
carried across (like the MySQL path), but NOT through this text: the converter attaches
each one after the parse, as the source's own ``pg_get_expr`` text, so it reaches the target
byte for byte (``converter._apply_pg_column_defaults``). Re-rendering a default through
sqlglot broke common ones (``CURRENT_TIME`` -> ``CURRENT_TIME()``, ``date_part(...)`` ->
an invalid ``EXTRACT(CAST(...) FROM ...)``). A generated column's computed value and a
``serial``/identity ``nextval`` are the exclusions -- for the KEY column ``nextval`` is the
identity mechanism the primary-key strategy governs, not a literal default; on any other
column it is a reported loss (see :func:`pg_column_default_sql`).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Optional, Sequence

import sqlglot

from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.source_dialect import PostgresSourceDialect

if TYPE_CHECKING:
    from dsql_migrator.core.catalog_postgres import StandaloneSequence

_PG = PostgresSourceDialect()

# A serial/identity column's DEFAULT is ``nextval('<seq>'::regclass)`` -- the identity
# mechanism, governed by the primary-key strategy (and advanced by 'Sync identity
# sequences' at cut over), NOT a value to re-emit as a literal DEFAULT.
_PG_NEXTVAL_DEFAULT_RE = re.compile(r"nextval\s*\(", re.IGNORECASE)
# A DEFAULT that IS the sequence's next value (a serial's), as pg_get_expr prints it --
# not an expression that formats one ('ORD-' || lpad(nextval(...)::text, 8, '0')).
_PG_BARE_NEXTVAL_RE = re.compile(
    r"^\s*nextval\s*\(\s*'(?:[^']|'')+'(?:\s*::\s*regclass)?\s*\)\s*$", re.IGNORECASE
)


def pg_is_bare_nextval(default: Optional[str]) -> bool:
    """Whether ``default`` is exactly ``nextval('<sequence>'::regclass)`` (a serial's)."""
    return bool(_PG_BARE_NEXTVAL_RE.match(default or ""))


def pg_column_default_sql(
    column: ColumnDef, *, is_key_column: bool = False
) -> "tuple[Optional[str], Optional[str]]":
    """Return ``(default_sql, drop_reason)`` for a PostgreSQL source column's DEFAULT.

    ``default_sql`` is the text to place after ``DEFAULT`` in the target ``CREATE TABLE``:
    the source default VERBATIM (``pg_get_expr`` output is PostgreSQL already), which the
    converter attaches after its parse so sqlglot never re-renders it -- a re-render turned
    ``CURRENT_TIME`` into the invalid ``CURRENT_TIME()``, dropped the time zone of a
    3-argument ``date_trunc`` and the precision of ``CURRENT_TIMESTAMP(0)``. The only
    rewrite is an enum default, which becomes its plain label (the column becomes text).
    ``None`` means emit no default. ``drop_reason`` is set ONLY
    when a PRESENT default could not be carried (so the caller warns, Property 6); it is
    ``None`` both when there is nothing to carry (no default / generated column) and when
    the default IS carried.

    PostgreSQL -> DSQL is near-identity, so a source default is carried by default (MySQL
    carries them too). The two exclusions -- a generated column's computed value and a
    serial/identity ``nextval`` -- match the MySQL path. A default that does not parse as a
    PostgreSQL expression is dropped (with a reason) rather than emitted: the tool's own
    readers of the applied DDL (column types, key, generated columns -- what Full Load
    follows) parse the whole CREATE TABLE, so one default they cannot read would blind them
    to the entire table.

    A ``nextval`` is never re-emitted, but whether that omission is SILENT now depends on
    ``is_key_column``. For a PRIMARY-KEY column the omission really is governed elsewhere --
    the primary-key strategy turns it into ``GENERATED ... AS IDENTITY``, where a DEFAULT
    would be rejected, and cut-over's "Sync identity sequences" advances it. For any OTHER
    column nothing governs it: it lands on the target with neither identity nor default, so
    a reason is returned. The gate is the primary key rather than
    ``table.auto_increment_column`` on purpose -- that field is set by PostgreSQL
    enrichment, so keying on it would turn an un-enriched inventory's key column into a
    wrongly-worded warning, while the primary key is always present on the ``TableDef``.
    """
    raw = (column.default or "").strip()
    if column.generated:
        return None, None
    # BOTH sequence spellings, treated identically: a ``serial``'s ``nextval`` DEFAULT and a
    # PG10+ ``GENERATED ... AS IDENTITY``. The latter has no pg_attrdef default at all, so
    # testing the default alone silently missed the spelling PostgreSQL 10+ RECOMMENDS --
    # only the legacy one was covered. Checked BEFORE the empty-default return for that
    # reason: an identity column arrives with ``default`` None.
    if raw and not column.identity and _PG_NEXTVAL_DEFAULT_RE.search(raw) and not pg_is_bare_nextval(raw):
        # An expression that FORMATS a sequence value (an order number): no identity can
        # reproduce it, key or not, so nothing governs it -- it must be re-attached.
        sequence = pg_nextval_sequence(raw)
        return None, (
            f"formats a value from sequence {sequence or '(unknown)'} (DEFAULT {raw}), and "
            "that sequence is not created on Aurora DSQL, so the column is created with no "
            "default and an INSERT that omits it gets no value. Create the sequence and "
            "re-attach this DEFAULT after apply, as the sequence's own Step 1 item says (an "
            "identity would not reproduce the formatting)."
        )
    if column.identity or (raw and _PG_NEXTVAL_DEFAULT_RE.search(raw)):
        if is_key_column:
            return None, None
        return None, (
            "takes its value from a sequence (serial / identity), which is not carried "
            "over: its sequence is not created on Aurora DSQL, and the primary-key "
            "strategy generates values only for the key column. The target column is "
            "created with no default, so an INSERT that omits it writes NULL instead of "
            "the next number. Supply the value from the application, or have Aurora DSQL "
            "generate it with an identity: " + pg_identity_fallback([column]) + "."
        )
    if not raw:
        return None, None
    # Validate the default parses as a PostgreSQL expression in isolation, so a malformed
    # one is dropped with a warning instead of leaving the emitted CREATE TABLE unreadable
    # to the applied-DDL readers. (Over realistic pg_get_expr output this SELECT-context
    # check agrees with the DEFAULT-clause context those readers parse.)
    try:
        sqlglot.parse_one(f"SELECT {raw}", read="postgres")
    except sqlglot.errors.SqlglotError:
        return None, (
            f"source default ({raw}) is not a PostgreSQL expression the converter can "
            "carry, so the column is created without a default; set it in the application "
            "or add it with ALTER TABLE after apply."
        )
    # A default that references a type DSQL does not have is itself invalid there, even
    # after the COLUMN is remodelled. The UNSUPPORTED type warning tells the operator to
    # change the column to text; the cast inside the default (`DEFAULT CAST('image' AS
    # ecommerce.media_type)`, or the `'image'::ecommerce.media_type` spelling) survives that
    # edit and the CREATE TABLE still fails -- with nothing having mentioned the default.
    # So drop it and say why: the value is stated in the reason, which is what the operator
    # needs to re-add it once the column has a supported type.
    # A converted ENUM column is now text, so its default -- which PostgreSQL stores as
    # `'pending'::ecommerce.order_status` -- CAN be carried after all: strip the cast to
    # the enum's own type and keep the literal. Before the labels were introspected the
    # column stayed an enum and the default had to be dropped with a "re-add it as a plain
    # text literal" note; the tool can now do exactly that itself.
    if column.type_kind == "enum" and column.enum_labels:
        literal = _pg_enum_default_literal(raw, column.enum_labels)
        if literal is not None:
            return literal, None
    raw, function_reason = _pg_default_functions(raw)
    if function_reason is not None:
        return None, function_reason
    # Judged by what the COLUMN becomes, not by how the default is spelled: an array
    # column becomes jsonb, whatever its DEFAULT looks like (ARRAY['user'::text] has no
    # cast to catch, and is still rejected for a jsonb column).
    substitute = substitute_pg_unsupported_type(column.mysql_type, type_kind=column.type_kind)[0]
    if substitute == "jsonb":
        if _PG_EMPTY_ARRAY_DEFAULT_RE.match(raw):
            return "'[]'::jsonb", None  # what Full Load writes for an empty array
        return None, (
            f"source default ({raw}) is an array, and the column becomes jsonb, so the "
            "default is not carried. Re-add it as JSON after apply (e.g. ALTER TABLE ... "
            "ALTER COLUMN ... SET DEFAULT '[\"user\"]'::jsonb): Full Load writes the "
            "column's arrays as JSON arrays."
        )
    bad_type = _pg_default_unsupported_type(raw)
    if bad_type is not None:
        example = (
            "a number" if (substitute or "").startswith("numeric") else "a plain text literal"
        )
        return None, (
            f"source default ({raw}) casts to '{bad_type}', a PostgreSQL type Aurora DSQL "
            "does not support as a column type, so the default is not carried -- it would "
            "keep failing even after the column itself is remodelled. Re-add it with the "
            f"remodelled type (e.g. {example}) in the DDL or with ALTER TABLE after apply."
        )
    return raw, None


# An empty array DEFAULT as pg_get_expr prints it: '{}'::text[], ARRAY[]::integer[].
_PG_EMPTY_ARRAY_DEFAULT_RE = re.compile(r"^\s*(?:'\{\}'|ARRAY\[\s*\])\s*::\s*[^']+\[\]\s*$", re.IGNORECASE)
# uuid-ossp's random UUID, as a DEFAULT names it (bare when the extension sits on the
# search_path, else schema-qualified): Aurora DSQL has no uuid-ossp, and its built-in
# gen_random_uuid() returns the same thing, a version-4 UUID.
_UUID_V4_DEFAULT_RE = re.compile(
    r'(?<![\w$."])(?:(?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*)\s*\.\s*)?uuid_generate_v4\s*\(\s*\)',
    re.IGNORECASE,
)
# The other uuid-ossp generators (time-based or name-based): no Aurora DSQL equivalent.
_UUID_OSSP_OTHER_RE = re.compile(
    r"(?<![\w$])(uuid_generate_v1mc|uuid_generate_v1|uuid_generate_v3|uuid_generate_v5|"
    r"uuid_nil|uuid_ns_\w+)\s*\(",
    re.IGNORECASE,
)
# A schema-qualified function call; pg_get_expr qualifies a function that is not on the
# search_path, which a built-in (pg_catalog) never needs.
_QUALIFIED_CALL_RE = re.compile(
    r'(?<![\w$."])((?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*))\s*\.\s*((?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*))\s*\('
)
_SQL_STRING_RE = re.compile(r"'(?:[^']|'')*'")


def _pg_default_functions(raw: str) -> "tuple[str, Optional[str]]":
    """``(default, None)`` with uuid-ossp's ``uuid_generate_v4()`` made Aurora DSQL's
    ``gen_random_uuid()``, or ``(raw, reason)`` for a default that calls a function the
    target does not have: another uuid-ossp generator, or a schema-qualified function
    outside pg_catalog (an extension's or the application's own). Carried verbatim, either
    made the CREATE TABLE fail at apply."""
    translated = _UUID_V4_DEFAULT_RE.sub("gen_random_uuid()", raw)
    code = _SQL_STRING_RE.sub("''", translated)  # never a name inside a string literal
    other = _UUID_OSSP_OTHER_RE.search(code)
    if other is not None:
        return raw, (
            f"source default ({raw}) calls {other.group(1)}(), a uuid-ossp function Aurora "
            "DSQL does not have, so the column is created without a default. Generate the "
            "value in the application, or use gen_random_uuid() (a random version-4 UUID) "
            "with ALTER TABLE ... SET DEFAULT after apply if that is acceptable."
        )
    for match in _QUALIFIED_CALL_RE.finditer(code):
        schema = match.group(1).strip('"')
        if schema.lower() != "pg_catalog":
            name = f"{match.group(1)}.{match.group(2)}"
            return raw, (
                f"source default ({raw}) calls {name}(), a function outside pg_catalog that "
                "Aurora DSQL does not have unless you create it, so the column is created "
                "without a default. Generate the value in the application, or create an "
                "equivalent and add the default with ALTER TABLE ... SET DEFAULT after apply."
            )
    return translated, None


# PostgreSQL spellings of bigint, the only type Aurora DSQL allows an identity on.
_PG_BIGINT_TYPES = frozenset({"bigint", "int8"})


# ``nextval('<sequence>'::regclass)`` as PostgreSQL renders a sequence DEFAULT: the literal
# names the sequence, quoted parts as written (an unquoted part is already folded).
_NEXTVAL_RE = re.compile(r"\bnextval\s*\(\s*'((?:[^']|'')+)'(?:\s*::\s*regclass)?\s*\)", re.IGNORECASE)


def pg_nextval_sequence(default: Optional[str]) -> Optional[str]:
    """The sequence a ``nextval(...)`` DEFAULT calls (``schema.name`` or a bare ``name``,
    quotes removed), or ``None``. Pure."""
    match = _NEXTVAL_RE.search(default or "")
    if match is None:
        return None
    literal = match.group(1).replace("''", "'")
    parts = re.findall(r'"((?:[^"]|"")*)"|([^."]+)', literal)
    names = [quoted.replace('""', '"') if quoted else bare for quoted, bare in parts]
    return ".".join(names) if names else None


def pg_identity_fallback(columns: "Sequence[ColumnDef]") -> str:
    """How to have Aurora DSQL generate these sequence-fed columns' values with an identity:
    the clause both Schema Conversion's note on a non-key ``serial`` / identity column and
    Evaluation's ``NON_KEY_SEQUENCE`` finding give, so the two steps say the same thing.

    Column-aware, because the bare "add an identity to the column on the target" could not
    be followed on the most common column of all. Aurora DSQL allows an identity only on a
    ``bigint`` column, and ``ALTER TABLE ... ADD GENERATED ... AS IDENTITY`` needs the
    column NOT NULL already (PostgreSQL refuses it otherwise) -- while Aurora DSQL's ALTER
    TABLE can neither change a column's type nor SET NOT NULL. So only a NOT NULL bigint
    column can get one after apply; any other -- a plain ``serial`` is ``integer`` -- has to
    be declared ``bigint ... AS IDENTITY`` in the target DDL before applying. Either way the
    identity then has to be restarted past the loaded values
    (:data:`~dsql_migrator.core.converter.IDENTITY_RESTART_ADVICE`).
    """
    from dsql_migrator.core.converter import IDENTITY_RESTART_ADVICE, _IDENTITY_CACHE_SIZE

    identity = f"GENERATED BY DEFAULT AS IDENTITY (CACHE {_IDENTITY_CACHE_SIZE})"
    later: list[ColumnDef] = []
    reasons: list[str] = []
    for column in columns:
        if normalize_pg_base_type(column.mysql_type) not in _PG_BIGINT_TYPES:
            reasons.append(f"'{column.name}' ({column.mysql_type}) is not bigint")
        elif column.nullable:
            reasons.append(f"'{column.name}' is nullable")
        else:
            later.append(column)
    parts: list[str] = []
    if later:
        names = ", ".join(f"'{c.name}'" for c in later)
        parts.append(
            f"{names} can get one after apply (ALTER TABLE ... ALTER COLUMN ... ADD "
            f"{identity})"
        )
    if reasons:
        one = len(reasons) == 1
        parts.append(
            f"{', '.join(reasons)}, and Aurora DSQL allows an identity only on a bigint "
            "NOT NULL column and can change neither a column's type nor its NOT NULL after "
            f"apply, so declare {'it' if one else 'them'} bigint {identity} in the target "
            "DDL before applying (an identity column holds no NULL)"
        )
    text = "; ".join(parts) + "; then " + IDENTITY_RESTART_ADVICE
    # CACHE 1 is the source's own default: one value at a time, in order. Said, because the
    # clause above silently traded that for throughput.
    text += (
        f" (CACHE {_IDENTITY_CACHE_SIZE} serves concurrent inserts faster but hands each "
        "session a block, so values arrive out of order with large gaps; CACHE 1 hands them "
        "out one at a time and in order, as the source does -- though an aborted or retried "
        "transaction still uses one up, there as here -- and Aurora DSQL accepts only those "
        "two kinds of size)"
    )
    return text


# Aurora DSQL's sequence rules, from its CREATE SEQUENCE documentation: CACHE must be stated
# and only 1 or >= 65536 is accepted, and a sequence is always bigint (``AS`` accepts only
# BIGINT), so the defaults it falls back to are bigint's.
_BIGINT_MIN = -(2**63)
_BIGINT_MAX = 2**63 - 1


def pg_sequence_next_value(sequence: "StandaloneSequence") -> Optional[int]:
    """The value the source sequence's next ``nextval()`` would return, or ``None`` when that
    cannot be told: the user may not read the sequence, or it is exhausted (at its bound
    with NO CYCLE, so ``nextval()`` already fails on the source).

    ``last_value`` is the last value written to disk, which a cached sequence may have
    reserved ahead of what it handed out, so starting after it can skip values but never
    repeats one. When nothing has been drawn since the sequence was created or reset
    (``is_called`` False) ``last_value`` IS the next value; when it could not be read at
    all, neither is the next one.
    """
    if not sequence.readable or sequence.last_value is None:
        return None
    if sequence.is_called is False:
        return sequence.last_value
    following = sequence.last_value + sequence.increment
    if sequence.min_value <= following <= sequence.max_value:
        return following
    if sequence.cycle:
        return sequence.min_value if sequence.increment > 0 else sequence.max_value
    return None


def dsql_create_sequence_sql(sequence: "StandaloneSequence") -> str:
    """The Aurora DSQL ``CREATE SEQUENCE`` that continues ``sequence`` after its source value.

    Follows the documented syntax, ``CREATE SEQUENCE name CACHE cache [INCREMENT BY ...]
    [MINVALUE ...] [MAXVALUE ...] [CYCLE] [START WITH ...]``. CACHE is 1 when the source
    hands out one value at a time (its own CACHE 1, the PostgreSQL default) and 65536 --
    the smallest cached size Aurora DSQL accepts -- when the source already caches, since
    such an application already tolerates gaps. The source's bounds are kept when they are
    not bigint's defaults, so an ``integer`` sequence keeps failing at 2147483647 instead of
    handing a column a value it cannot hold. When the next value cannot be read the START
    WITH clause holds a placeholder, which fails loudly if pasted as is rather than starting
    at 1 over values the source already used.
    """
    from dsql_migrator.core.converter import _IDENTITY_CACHE_SIZE

    cache = 1 if sequence.cache_size <= 1 else _IDENTITY_CACHE_SIZE
    parts = [f"CREATE SEQUENCE {_PG.quote_table(sequence.name)} CACHE {cache}"]
    if sequence.increment != 1:
        parts.append(f"INCREMENT BY {sequence.increment}")
    lowest, highest = (1, _BIGINT_MAX) if sequence.increment > 0 else (_BIGINT_MIN, -1)
    if sequence.min_value != lowest:
        parts.append(f"MINVALUE {sequence.min_value}")
    if sequence.max_value != highest:
        parts.append(f"MAXVALUE {sequence.max_value}")
    if sequence.cycle:
        parts.append("CYCLE")
    start = pg_sequence_next_value(sequence)
    parts.append(f"START WITH {start}" if start is not None else "START WITH <next value>")
    return " ".join(parts) + ";"


# A type reference inside a DEFAULT, in either PostgreSQL spelling: the `::type` cast and
# the `CAST(... AS type)` form the converter's own re-render produces. The name may be
# schema-qualified and quoted.
_PG_DEFAULT_CAST_RE = re.compile(
    r"(?:::|\bCAST\s*\([^()]*?\bAS\s+)\s*"
    # The type name, with the second words of PostgreSQL's multi-word types (a modifier
    # may sit between: timestamp(6) with time zone), then any array brackets. Reading only
    # the first word took "double precision" for an unsupported "double" (dropping a valid
    # default) and "character varying[]" for a supported "character" (keeping an array
    # default the jsonb column then rejects).
    r"(?P<name>[\w.\"]+)(?:\s*\([^()]*\))?"
    r"(?P<rest>\s+(?:precision|varying|with(?:out)?\s+time\s+zone))?(?:\s*\([^()]*\))?"
    r"(?P<array>(?:\s*\[\s*\])*)",
    re.IGNORECASE,
)


# Casts that appear INSIDE expressions but are never column types, so the column-type
# support test does not apply to them. ``regclass`` is the one that matters: PostgreSQL
# renders a sequence default as ``nextval('s'::regclass)``. That path returns earlier, but
# a default could legitimately cast to one of these for another reason, and dropping it
# would be a false positive on a default DSQL would have accepted.
_PG_EXPRESSION_ONLY_CAST_TYPES = frozenset(
    {"regclass", "regtype", "regproc", "regprocedure", "regoper", "regnamespace", "oid"}
)


# A default that is just one of the enum's own labels, in either cast spelling:
#   'pending'::ecommerce.order_status        |  CAST('pending' AS ecommerce.order_status)
_PG_ENUM_DEFAULT_RE = re.compile(
    r"""^\s*(?:
          '(?P<a>(?:[^']|'')*)'\s*::\s*[\w."]+
        | CAST\s*\(\s*'(?P<b>(?:[^']|'')*)'\s*AS\s+[\w."]+\s*\)
        )\s*$""",
    re.IGNORECASE | re.VERBOSE,
)


def _pg_enum_default_literal(
    raw: str, enum_labels: "Sequence[str]"
) -> "Optional[str]":
    """Return the plain text literal for an enum-typed default, or ``None``.

    Only when the value is one of the type's OWN labels, so an expression that merely
    happens to cast to some type is not rewritten, and a default outside the converted
    CHECK's value set is not silently turned into a value the CHECK would reject.
    """
    match = _PG_ENUM_DEFAULT_RE.match(raw or "")
    if match is None:
        return None
    quoted = match.group("a") if match.group("a") is not None else match.group("b")
    if quoted is None:
        return None
    if quoted.replace("''", "'") not in set(enum_labels):
        return None
    return f"'{quoted}'"


def _pg_default_unsupported_type(raw: str) -> "Optional[str]":
    """The first type a DEFAULT casts to that DSQL does not support, or ``None``. Pure.

    Reuses the same support test the COLUMN types go through, so the two cannot disagree
    about what DSQL accepts. Only reports a type that is genuinely unsupported: a default
    casting to ``VARCHAR``/``numeric``/``timestamp`` is normal (the converter's own
    re-render emits those) and must not be dropped.
    """
    for match in _PG_DEFAULT_CAST_RE.finditer(raw or ""):
        base = match.group("name").strip().strip('"')
        if not base or base.lower() in _PG_EXPRESSION_ONLY_CAST_TYPES:
            continue
        name = base + " ".join([""] + (match.group("rest") or "").split())
        if match.group("array"):
            name += "[]"
        if unsupported_dsql_reason(name) is not None:
            return name
    return None


# PostgreSQL base types Aurora DSQL supports AS COLUMN TYPES, normalized (lower-case,
# type modifiers + whitespace collapsed). Source of truth: the Aurora DSQL User Guide
# "Supported data types" page (numeric / character / date-time / miscellaneous tables).
# NOTE arrays are supported only at QUERY RUNTIME, not as column types (see below), so
# they are NOT in this set. dsql_lint confirms `array_type` is an unfixable error, and
# the docs list geometric/pgvector (and, by omission, network/xml/money/bit/enum/
# composite/range) as unsupported column types.
_DSQL_SUPPORTED_PG_BASE_TYPES = frozenset(
    {
        # Numeric
        "smallint", "int2",
        "integer", "int", "int4",
        "bigint", "int8",
        "real", "float4",
        "double precision", "float8",
        "numeric", "decimal", "dec",
        # Character
        "character", "char",
        "character varying", "varchar",
        "bpchar", "text",
        # Date / time
        "date",
        "time", "time without time zone",
        "time with time zone", "timetz",
        "timestamp", "timestamp without time zone",
        "timestamp with time zone", "timestamptz",
        "interval",
        # Miscellaneous
        "boolean", "bool",
        "bytea", "uuid", "json", "jsonb",
    }
)

# A length/precision/scale modifier anywhere in a format_type string:
# "(50)", "(12,2)", "(6)" -- e.g. numeric(12,2), character varying(50),
# timestamp(6) with time zone.
_TYPE_MODIFIER_RE = re.compile(r"\(\s*\d+\s*(?:,\s*\d+\s*)?\)")

# Aurora DSQL numeric limits, from the SERVICE DOCUMENTATION ("Supported data types":
# "The maximum precision is 1000 and scale can be between -1000 and 1000", default
# numeric(18,6), max storage 510 bytes) and re-confirmed live 2026-09-25 against a real
# cluster: numeric(1000,1000) is accepted, numeric(1001,0) is rejected with "NUMERIC
# precision 1001 must be between 1 and 1000", and a 500-integer-digit + 500-decimal-digit
# value round-tripped EXACT.
#
# These were 38/37 (mirroring converter._DSQL_NUMERIC_*), the limit DSQL enforced when this
# code was written. The service raised it and the constants did not, so a PostgreSQL
# numeric(40,10) -- well within both engines -- was silently narrowed to numeric(38,10) and
# lost digits, while the warning asserted a DSQL maximum that no longer exists. A
# numeric(p,s) beyond 1000 is still REJECTED at CREATE TABLE, so the clamp itself is kept.
_DSQL_NUMERIC_MAX_PRECISION = 1000
_DSQL_NUMERIC_MAX_SCALE = 1000
_NUMERIC_SPEC_RE = re.compile(
    r"^\s*(numeric|decimal|dec)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*$", re.IGNORECASE
)
# A bare numeric with NO precision/scale at all: "numeric", "decimal", "dec".
_BARE_NUMERIC_RE = re.compile(r"^\s*(numeric|decimal|dec)\s*$", re.IGNORECASE)

# Aurora DSQL's default storage for an unconstrained numeric (documented). 18 total
# digits, 6 fractional -> 12 integer digits.
_DSQL_DEFAULT_NUMERIC = "numeric(18,6)"
_DSQL_DEFAULT_NUMERIC_SCALE = 6
_DSQL_DEFAULT_NUMERIC_INT_DIGITS = 12


def unconstrained_numeric_note(pg_type: str) -> "Optional[str]":
    """Warn that a bare PostgreSQL ``numeric``/``decimal`` (no declared precision/scale)
    is stored by Aurora DSQL at its default ``numeric(18,6)``.

    Returns the warning message, or ``None`` when ``pg_type`` is not a bare numeric. A
    bare ``numeric`` is arbitrary-precision on PostgreSQL but the target caps it at 6
    fractional digits (and 12 integer digits), so a value beyond that is rounded (or, for
    the integer part, rejected) on load. :func:`clamp_pg_numeric` covers only a DECLARED
    precision/scale that exceeds DSQL's 1000/1000 limits; this covers the common no-parens
    form, which would otherwise migrate with NO signal that its precision is capped
    (Property 6: no silent precision loss). Values within 12 integer / 6 fractional digits
    are unaffected.
    """
    if _BARE_NUMERIC_RE.match(pg_type or "") is None:
        return None
    return (
        f"PostgreSQL '{pg_type.strip()}' declares no precision/scale, so Aurora DSQL stores "
        f"it at its default {_DSQL_DEFAULT_NUMERIC}: values with more than "
        f"{_DSQL_DEFAULT_NUMERIC_SCALE} fractional digits are rounded, and values needing "
        f"more than {_DSQL_DEFAULT_NUMERIC_INT_DIGITS} integer digits will not fit (rounded "
        "or rejected on load). If this column needs a different precision/scale, set an "
        "explicit numeric(p,s) target in Schema Conversion (Aurora DSQL accepts precision "
        "and scale up to 1000); otherwise "
        f"{_DSQL_DEFAULT_NUMERIC} is used and Validation compares at that scale."
    )


def clamp_pg_numeric(pg_type: str) -> "tuple[str, Optional[str]]":
    """Clamp a PostgreSQL ``numeric(p[,s])`` to what Aurora DSQL accepts.

    Returns ``(type_string, warning)``. When the declared precision or scale (>1000)
    exceeds DSQL's limits the spec is reduced and a warning describes the lost range/
    places; otherwise the type is returned verbatim with ``None``. The MySQL path clamps
    the same way (:func:`~dsql_migrator.core.converter._clamp_numeric_spec`); without this
    a PostgreSQL ``numeric(40,10)`` would be emitted verbatim and DSQL would reject the
    whole ``CREATE TABLE`` at apply time with no prior signal. A bare ``numeric`` (no
    precision) is left untouched here -- DSQL stores it at its default ``numeric(18,6)``;
    that precision cap is surfaced separately by :func:`unconstrained_numeric_note`.
    Non-numeric types pass through.
    """
    match = _NUMERIC_SPEC_RE.match(pg_type or "")
    if match is None:
        return pg_type, None
    base = match.group(1)
    precision = int(match.group(2))
    scale = int(match.group(3)) if match.group(3) is not None else None
    notes: list[str] = []
    if precision > _DSQL_NUMERIC_MAX_PRECISION:
        notes.append(
            f"precision {precision} exceeds the Aurora DSQL maximum of "
            f"{_DSQL_NUMERIC_MAX_PRECISION}"
        )
        precision = _DSQL_NUMERIC_MAX_PRECISION
    if scale is not None and scale > _DSQL_NUMERIC_MAX_SCALE:
        notes.append(
            f"scale {scale} exceeds the Aurora DSQL maximum of {_DSQL_NUMERIC_MAX_SCALE}"
        )
        scale = _DSQL_NUMERIC_MAX_SCALE
    if scale is not None and scale > precision:
        notes.append(f"scale was further reduced to {precision} to fit the precision")
        scale = precision
    if not notes:
        return pg_type, None
    spec = f"{precision}" if scale is None else f"{precision},{scale}"
    clamped = f"{base}({spec})"
    return clamped, (
        f"{pg_type} was reduced to {clamped} for Aurora DSQL ("
        + "; ".join(notes)
        + "); values beyond the reduced precision/scale will be rounded or rejected."
    )


# Aurora DSQL's DECLARED-LENGTH ceilings for the character types, live-verified on a real
# cluster (and documented in "Supported data types in Aurora DSQL"):
#   CREATE ... VARCHAR(65536) -> ProgramLimitExceeded
#       "Datatype limit greater than 65535 bytes not supported for varchar"
#   CREATE ... CHAR(4097)     -> "Datatype limit greater than 4096 bytes not supported for char"
# VARCHAR(65535) / CHAR(4096) are accepted. PostgreSQL allows up to ~1 GB, so a source
# column above these ceilings made the WHOLE CREATE TABLE fail at apply with nothing having
# warned -- and `dsql_lint` does not catch it either (0 diagnostics on VARCHAR(100000)).
_DSQL_MAX_VARCHAR_LENGTH = 65535
_DSQL_MAX_CHAR_LENGTH = 4096

# `character varying(n)`/`varchar(n)` and `character(n)`/`char(n)`, capturing n. Deliberately
# NOT matching a bare spelling (no modifier): an unbounded varchar is already emitted as
# VARCHAR and DSQL applies its default ceiling to the VALUE, which is the oversized-LOB
# check's business, not the DDL's.
_PG_CHAR_SPEC_RE = re.compile(
    r"^\s*(character\s+varying|varchar|character|char|bpchar)\s*\(\s*(\d+)\s*\)\s*$",
    re.IGNORECASE,
)


def clamp_pg_character(pg_type: str) -> "tuple[str, Optional[str]]":
    """Clamp a PostgreSQL character type whose DECLARED LENGTH exceeds DSQL's ceiling.

    Returns ``(type_string, warning)``, mirroring :func:`clamp_pg_numeric`.

    The replacement is ``text``, NOT the ceiling length: emitting ``varchar(65535)`` for a
    source ``varchar(200000)`` would make DSQL REJECT values the source legitimately holds
    (a silent data loss discovered only mid-load), whereas ``text`` accepts up to DSQL's
    1 MiB per-value limit -- strictly more of the source's range, and PostgreSQL's own
    ``varchar(n)`` is ``text`` plus a length check. What IS lost is that length check, and
    for ``character(n)`` the blank padding; the warning says so.

    Types within the ceiling pass through verbatim, as does a bare/unbounded spelling.
    """
    match = _PG_CHAR_SPEC_RE.match(pg_type or "")
    if match is None:
        return pg_type, None
    base = match.group(1).lower()
    length = int(match.group(2))
    is_fixed = base in {"character", "char", "bpchar"}
    ceiling = _DSQL_MAX_CHAR_LENGTH if is_fixed else _DSQL_MAX_VARCHAR_LENGTH
    if length <= ceiling:
        return pg_type, None
    padding = (
        " The blank padding of a fixed-length character column is not reproduced either "
        "(text stores what the source already padded, but comparisons no longer ignore "
        "trailing spaces)."
        if is_fixed
        else ""
    )
    return "text", (
        f"{pg_type} was converted to text: Aurora DSQL does not accept a declared "
        f"{'char' if is_fixed else 'varchar'} length above {ceiling} bytes, so the column "
        "as declared would have made the whole CREATE TABLE fail at apply. text keeps more "
        "of the source's range (values up to Aurora DSQL's 1 MiB per-value limit) than the "
        f"clamped {ceiling}-byte length would, but the length limit itself is no longer "
        f"enforced on the target -- re-add it as a CHECK (length(col) <= {length}) if the "
        f"application relies on it.{padding}"
    )


# The DSQL type each unsupported PostgreSQL type is SUBSTITUTED with, and why that
# substitution is safe on the DATA path (not just the DDL). Every target here is one
# ``_pg_read_expression`` already knows how to read the source value into:
#   an array  -> jsonb    read as CAST(to_jsonb(col) AS text)
#   money     -> numeric  read as CAST(col AS numeric)  (its locale-formatted text is lossy)
#   the rest  -> text     read as CAST(col AS text)     (the type's canonical form)
# Emitting the source type verbatim instead -- which is what the tool used to do, on the
# stated ground that "v1 does not auto-substitute" -- guaranteed a FAILED apply
# ("datatype text[] not supported"), for a remodel the tool itself was recommending and the
# loader was already prepared for. `bytea` is deliberately NOT a fallback: the loader reads
# a bit string as its TEXT, so binding it to bytea stored the ASCII digits of "10101010"
# rather than the byte 0xAA.
_PG_SUBSTITUTE_TARGET = {
    # Declared, not bare: Aurora DSQL stores a bare numeric as numeric(18,6), which refused
    # every amount from 10^12 up. money is an int64 of the locale's minor unit, so 19
    # integer digits (no decimals) to 4 decimals covers every lc_monetary.
    "money": "numeric(23,4)",
    "inet": "text",
    "cidr": "text",
    "macaddr": "text",
    "macaddr8": "text",
    "xml": "text",
    "bit": "text",
    "bit varying": "text",
    "varbit": "text",
    "tsvector": "text",
    "tsquery": "text",
    "point": "text",
    "line": "text",
    "lseg": "text",
    "box": "text",
    "path": "text",
    "polygon": "text",
    "circle": "text",
}


def substitute_pg_unsupported_type(
    pg_type: str,
    *,
    type_kind: "Optional[str]" = None,
) -> "tuple[Optional[str], Optional[str]]":
    """The DSQL type to emit instead of a DSQL-unsupported one, with a warning.

    Returns ``(target_type, note)``, or ``(None, None)`` when the type needs no
    substitution or none is safe. The note states the type CHANGE the application has to
    follow -- the substitution keeps the data, not the type.

    A user-defined kind is excluded: an enum is handled by its own text+CHECK path, and a
    composite / range NAME cannot be read off the type string reliably enough to pick a
    target (a range IS handled, by base name, below).
    """
    if not pg_type:
        return None, None
    if type_kind in ("enum", "composite", "domain"):
        return None, None
    stripped = pg_type.strip()
    if stripped.endswith("[]"):
        return "jsonb", (
            f"Column type {stripped} was converted to jsonb: Aurora DSQL has no array "
            "column type. The values are preserved as a JSON array (Full Load reads them "
            "with to_jsonb), and jsonb is queryable with the JSON operators -- but the "
            "application must stop using array operators/indexing on this column. A child "
            "table keyed by this row's primary key is the alternative if you need to index "
            "or join on the elements."
        )
    base = normalize_pg_base_type(stripped)
    if base in _PG_RANGE_TYPES:
        return "text", (
            f"Column type {stripped} was converted to text: Aurora DSQL has no range "
            "column type. The value is preserved in its canonical form (e.g. '[1,5)'), so "
            "nothing is lost, but range operators (@>, &&, lower(), upper()) no longer "
            "work -- parse it in the application, or split it into two bound columns."
        )
    target = _PG_SUBSTITUTE_TARGET.get(base)
    if target is None:
        return None, None
    detail = (
        "the exact amount is preserved; its locale-formatted text is not used"
        if base == "money"
        else "the value is preserved in its canonical text form"
    )
    return target, (
        f"Column type {stripped} was converted to {target}: Aurora DSQL does not support "
        f"{base} as a column type. {detail[0].upper() + detail[1:]}, but the type changed, "
        "so any operator or function specific to it must be replaced in the application."
    )


def _ddl_column_type(pg_type: str) -> str:
    """The type string to emit in the rebuilt ``CREATE TABLE`` (usually verbatim).

    PostgreSQL -> DSQL is near-identity, so types are emitted as-is. The one exception is
    an ``interval`` carrying a ``(N)`` precision, in either shape:

    - fields-qualified with a precision (``interval second(3)``, ``interval day to
      second(6)``): sqlglot's ``postgres`` reader -- which the converter uses to re-parse
      this DDL -- cannot parse the fields + precision combination at all, so it would
      abort the whole table; and
    - precision-only (``interval(6)`` / ``interval(3)``): sqlglot DOES parse it, but its
      ``postgres`` renderer emits the UNPARSABLE ``INTERVAL 6`` (the ``(N)`` becomes a
      bare integer argument), which DSQL/PostgreSQL then reject with a syntax error.

    Both are fixed by dropping the ``(N)`` fractional-seconds precision (keeping any
    fields qualifier): the renderer emits a plain ``INTERVAL`` (or ``INTERVAL SECOND``),
    which DSQL accepts, and the data is preserved (source values are already rounded to
    the declared precision). Plain ``interval`` (no modifier) is unaffected.
    """
    # Substitute a DSQL-unsupported type with the target the tool recommends FIRST, so the
    # emitted DDL APPLIES instead of failing ("datatype text[] not supported"). It has to
    # precede the bit-varying alias rewrite below, which returns early -- otherwise a bit
    # string kept a type DSQL does not have. The data path already reads the source value
    # into these targets -- see substitute_pg_unsupported_type.
    substituted = substitute_pg_unsupported_type(pg_type)[0]
    if substituted is not None:
        return substituted
    lowered = pg_type.lower()
    if lowered.startswith("interval"):
        return _TYPE_MODIFIER_RE.sub("", pg_type).rstrip()
    if lowered.startswith("bit varying"):
        # sqlglot's postgres reader cannot parse the two-word "bit varying"[(n)]
        # (ParseError at "varying") -- and format_type ALWAYS spells varbit that way, so
        # a real varbit column would abort the whole table via the unparsable fallback and
        # collateral-damage its sibling columns. Emit the equivalent one-word alias
        # "varbit"[(n)], which sqlglot parses. The column is still flagged UNSUPPORTED
        # (bit strings are not a DSQL column type): unsupported_dsql_reason reads the
        # ORIGINAL column.mysql_type, so the surfaced warning still names "bit varying".
        return "varbit" + pg_type[len("bit varying"):]
    # Clamp an over-length character(n)/varchar(n) for the same reason as the numeric
    # clamp below: emit DDL DSQL accepts, and surface the change as a warning.
    clamped_character = clamp_pg_character(pg_type)[0]
    if clamped_character != pg_type:
        return clamped_character
    # Clamp an over-precision numeric(p,s) so the emitted DDL is valid for DSQL (the
    # warning is surfaced separately by the converter -- see convert_table's PG branch).
    return clamp_pg_numeric(pg_type)[0]


def normalize_pg_base_type(pg_type: str) -> str:
    """Reduce a ``format_type`` string to its base type for a support lookup.

    Strips length/precision modifiers wherever they appear and lower-cases/collapses
    whitespace, so ``NUMERIC(12,2)`` -> ``numeric``, ``character varying(50)`` ->
    ``character varying``, ``timestamp(6) with time zone`` -> ``timestamp with time zone``.
    """
    return " ".join(_TYPE_MODIFIER_RE.sub("", pg_type).lower().split())


# The FAITHFUL DSQL remodel target per unsupported PostgreSQL type family, appended to
# the warning so the operator knows WHAT to use instead -- not just "unsupported".
# Chosen for round-trip fidelity, NOT a blanket bytea: a type with a canonical text form
# -> text; a currency -> numeric; an array -> jsonb (queryable) or a child table. bytea
# is only natural for a genuinely binary type (e.g. spatial WKB), so it is not used as a
# general fallback here. The column type still changes, so the application must adapt --
# hence a warning to remodel deliberately rather than a silent auto-substitution.
_PG_UNSUPPORTED_REMODEL = {
    "inet": "text (its canonical address string round-trips losslessly)",
    "cidr": "text (its canonical address string round-trips losslessly)",
    "macaddr": "text",
    "macaddr8": "text",
    "xml": "text",
    "money": "numeric(23,4) (preserves the exact amount; avoid locale-formatted text)",
    # NOT bytea: the loader reads a bit string as its text, so binding it to bytea
    # stored the ASCII digits of "10101010" rather than the byte 0xAA -- silent
    # corruption a green DONE hid, catchable only by a CHECKSUM validation. Advise only
    # what the data path can faithfully produce.
    "bit": "text (the bit string, e.g. '10101010')",
    "bit varying": "text (the bit string)",
    "varbit": "text (the bit string)",
    "tsvector": "text",
    "tsquery": "text",
    "point": "text, or separate numeric columns for the coordinates",
    "line": "text",
    "lseg": "text",
    "box": "text",
    "path": "text",
    "polygon": "text",
    "circle": "text",
    "vector": "jsonb or text (pgvector is an extension Aurora DSQL does not provide)",
}

# Range / multirange types (int4range, tsrange, ... and the PG14+ multirange variants):
# their canonical text form round-trips, or split into lower/upper bound columns.
_PG_RANGE_TYPES = frozenset(
    {
        "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",
        "int4multirange", "int8multirange", "nummultirange", "tsmultirange",
        "tstzmultirange", "datemultirange",
    }
)


def unsupported_dsql_reason(
    pg_type: Optional[str],
    *,
    type_kind: Optional[str] = None,
    enum_labels: "Sequence[str]" = (),
) -> Optional[str]:
    """Return why a PostgreSQL column type is unsupported on Aurora DSQL, else ``None``.

    Arrays (any ``...[]``) are unsupported as column types; otherwise a base type outside
    DSQL's documented supported set (geometric, network, xml, money, bit, enum/composite/
    range, pgvector, ...) is unsupported. Returns a human-readable reason that names the
    FAITHFUL remodel target for the type (e.g. array -> jsonb, inet -> text, money ->
    numeric) so the operator knows what to change it to -- or ``None`` when the type is
    DSQL-supported (int/bigint/numeric/text/varchar/uuid/json[b]/bytea/boolean/date-time/
    interval). PG->DSQL is otherwise near-identity, so supported types pass through
    verbatim. The column is NOT auto-substituted (the app must adapt to the new type);
    the user remodels deliberately -- Property 6 (no silent loss / no silent degradation).
    """
    if not pg_type:
        return None
    if "[]" in pg_type:
        return (
            f"Aurora DSQL does not support array column types ('{pg_type}'). Store the "
            "array as jsonb (queryable with the JSON operators) or in a child table keyed "
            "by this row's primary key, and adapt the application before migrating this "
            "column."
        )
    base = normalize_pg_base_type(pg_type)
    # interval[fields][(p)] is supported (dsql_lint: 0 errors). format_type spells the
    # fields inline ("interval day to second", "interval second(3)"), so a bare-token
    # allowlist can't capture them -- match on the leading token instead.
    if base == "interval" or base.startswith("interval "):
        return None
    if base in _DSQL_SUPPORTED_PG_BASE_TYPES:
        return None
    if base in _PG_RANGE_TYPES:
        target = (
            "text (its canonical form, e.g. '[1,5)'), or two columns for the lower and "
            "upper bounds"
        )
    else:
        target = _PG_UNSUPPORTED_REMODEL.get(base)
    if target is not None:
        return (
            f"Aurora DSQL does not support the PostgreSQL type '{pg_type}' as a column "
            f"type. Store it as {target}, and adapt the application before migrating this "
            "column."
        )
    # A USER-DEFINED type. ``format_type`` returns only its NAME, so the message used to
    # hedge across every kind ("a PostgreSQL enum -> text; a composite type -> ...") and
    # never stated the real reason. With ``type_kind`` from pg_type.typtype it can.
    #
    # Note the wording rule: "enum" is a KIND, not a type name. PostgreSQL has no type
    # spelled ``enum`` (``CREATE TABLE t (c enum)`` -> 'type "enum" does not exist'), so
    # this says "a user-defined enumerated type", never "the type 'enum'".
    if type_kind == "domain":
        # Not unsupported at all: Aurora DSQL DOES support CREATE DOMAIN (live-verified --
        # created, used as a column type, and an out-of-range value rejected by the
        # domain's CHECK). The domain itself still has to be created on the target, which
        # the caller's own DDL does not do yet, so it is reported as an ACTION rather than
        # as a loss.
        return (
            f"'{pg_type}' is a user-defined DOMAIN. Aurora DSQL supports CREATE DOMAIN, so "
            "this column can keep its type -- but the domain does not exist on the target "
            "yet and the converter does not create it, so the CREATE TABLE would be "
            "rejected as-is. Create the domain on Aurora DSQL first (CREATE DOMAIN "
            f"{pg_type} AS <base type> CHECK (...), copying the definition from the source "
            "with \\dD), or replace the column's type with the domain's base type and "
            "re-add its CHECK on the column."
        )
    no_create_type = (
        "Aurora DSQL has no CREATE TYPE, so the type cannot be created on the target and "
        "any column declared with it is rejected"
    )
    if type_kind == "enum":
        labels = ", ".join(enum_labels)
        values = (
            f" Its allowed values, in the type's own sort order, are: {labels}."
            if enum_labels
            else ""
        )
        return (
            f"'{pg_type}' is a user-defined ENUMERATED type (CREATE TYPE ... AS ENUM). "
            f"{no_create_type}. Remodel the column to text with a CHECK over the same "
            "values -- or, since Aurora DSQL supports CREATE DOMAIN, to a domain over text "
            f"carrying that CHECK, which keeps a single named type.{values} Note that enum "
            "ordering is the declaration order above, NOT alphabetical, so an ORDER BY on "
            "this column changes unless you sort on an explicit ranking."
        )
    if type_kind in ("composite", "range", "multirange"):
        how = {
            "composite": "separate columns for its fields, or jsonb",
            "range": "text (its canonical form, e.g. '[1,5)'), or two columns for the "
            "lower and upper bounds",
            "multirange": "jsonb, or a child table with one row per range",
        }[type_kind]
        return (
            f"'{pg_type}' is a user-defined {type_kind.upper()} type. {no_create_type}. "
            f"Store it as {how}, and adapt the application before migrating this column."
        )
    # Kind unknown (an un-enriched inventory), so the honest message names what it can and
    # lists the possibilities rather than asserting one.
    return (
        f"Aurora DSQL does not support the PostgreSQL type '{pg_type}' as a column type. "
        "If it is a user-defined type, Aurora DSQL has no CREATE TYPE, so it cannot be "
        "created there: remodel the column (an enumerated type -> text with a CHECK over "
        "its values; a composite type -> separate columns or jsonb). See the Aurora DSQL "
        "supported data types."
    )


def build_pg_source_ddl(table: TableDef) -> str:
    """Build a PostgreSQL ``CREATE TABLE`` string for ``table`` (columns + PK).

    This is the text the converter PARSES, so it carries only what sqlglot may re-render:
    names, types, ``NOT NULL`` and the primary key. Column DEFAULTs and STORED generated
    expressions are source text sqlglot changes when it re-renders it, so the converter
    attaches them verbatim after the parse instead (``_apply_pg_column_defaults`` /
    ``_apply_pg_generated_columns``).

    Identifiers are double-quoted via the PostgreSQL dialect (injection-safe, and a
    ``schema.table`` name renders as ``"schema"."table"``). Foreign keys and secondary
    indexes are intentionally not emitted (foreign keys are removed for DSQL and
    preserved as metadata by the caller; indexes are rendered separately as ``CREATE
    INDEX ASYNC``). Raises ``ValueError`` if the table has no columns.
    """
    if not table.columns:
        raise ValueError(f"table {table.name!r} has no columns to convert")

    column_clauses: list[str] = []
    for column in table.columns:
        # column.mysql_type holds the EXACT PostgreSQL type string (from enrich's
        # format_type); emit it (near-verbatim; _ddl_column_type only massages a
        # fields+precision interval sqlglot can't parse) so the postgres reader parses it.
        # A user-defined ENUM cannot exist on Aurora DSQL (no CREATE TYPE), so the column
        # is declared `text` here and the allowed values are re-added as a
        # CHECK ... IN (...) after the parse -- the same faithful port MySQL's ENUM has
        # always had. Only possible now that the labels are introspected; before, the
        # domain was simply lost and the DDL named a type that could not be created, so
        # the whole CREATE TABLE failed at apply.
        column_type = (
            "text"
            if column.type_kind == "enum" and column.enum_labels
            else _ddl_column_type(column.mysql_type)
        )
        clause = f"{_PG.quote_identifier(column.name)} {column_type}"
        if not column.nullable:
            clause += " NOT NULL"
        column_clauses.append(clause)

    if table.primary_key:
        pk_columns = ", ".join(
            _PG.quote_identifier(name) for name in table.primary_key
        )
        column_clauses.append(f"PRIMARY KEY ({pk_columns})")

    body = ", ".join(column_clauses)
    return f"CREATE TABLE {_PG.quote_table(table.name)} ({body})"


__all__ = [
    "build_pg_source_ddl",
    "clamp_pg_numeric",
    "dsql_create_sequence_sql",
    "normalize_pg_base_type",
    "pg_column_default_sql",
    "pg_identity_fallback",
    "pg_sequence_next_value",
    "unsupported_dsql_reason",
]
