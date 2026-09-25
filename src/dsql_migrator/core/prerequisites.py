# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prerequisite checks gating Full Load / CDC (read-only).

Before a Full Load or CDC run, the Data Migration sub-flow always runs a
**gate**: it verifies the migration preconditions for the selected tables and
the source/target environment, and reports each check as PASS/FAIL/WARN/SKIP
with an actionable, credential-free remediation. A ``FAIL`` on any ``required``
check makes :attr:`PrerequisiteReport.can_proceed` ``False`` and blocks that mode
(Property 14).

All checks are **read-only** (Property 1): they only run ``SHOW``/catalog reads
and connection probes, never writes. Inputs are gathered through small,
injectable read-only *probes* (:class:`SourceProbe`, :class:`TargetProbe`,
:class:`MskProbe`) so the checker is unit-testable with fakes and never reaches a
real database or MSK in tests. The per-check logic itself lives in small pure
functions (``check_*``) that take already-gathered facts and return a
:class:`PrerequisiteResult`, so each rule is independently testable.

Mode coverage:

- ``FULL_LOAD`` runs the common checks; the CDC-only checks
  (``BINLOG_ROW_FORMAT`` / ``BINLOG_RETENTION`` / ``GTID_MODE`` / ``MSK_AVAILABLE`` /
  ``MSK_CONNECT_AVAILABLE``) are reported as ``SKIP``.
- ``CDC`` runs the common checks plus the CDC-only checks.

Credential confidentiality (Property 7): probe results and remediation strings
never include credential or token values.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Protocol, Sequence

if TYPE_CHECKING:
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

from dsql_migrator.core.models import (
    ConnectionResult,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteCheckRequest,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceType,
    TableDef,
    apply_lob_exclusions,
)

# Privileges required on the source MySQL user, by mode. Full Load needs read
# access; CDC additionally needs the replication privileges Debezium uses.
_FULL_LOAD_GRANTS = ("SELECT",)
_CDC_GRANTS = ("SELECT", "REPLICATION CLIENT", "REPLICATION SLAVE")


class SourceProbe(Protocol):
    """Read-only access to the source database used by prerequisite checks."""

    def reachable(self) -> ConnectionResult:
        """Return whether the source is reachable and login succeeds."""

    def grants(self) -> list[str]:
        """Return ``SHOW GRANTS`` lines for the migration user (raw strings)."""

    def variables(self) -> dict[str, str]:
        """Return relevant ``SHOW VARIABLES`` as a name->value map.

        Keys should be the MySQL variable names (e.g. ``log_bin``,
        ``binlog_format``, ``binlog_row_image``, ``gtid_mode``); values are their
        string values (e.g. ``ON`` / ``ROW`` / ``FULL``).
        """

    def cdc_prerequisites(
        self,
        table_names: Sequence[str],
        *,
        publication_name: str = "",
        slot_name: str = "",
    ) -> "Optional[PostgresCdcFacts]":
        """Return the PostgreSQL CDC readiness facts, or ``None`` for a MySQL source.

        Read-only; delegates to the source dialect. Only consulted for a PostgreSQL
        source in CDC mode (MySQL uses the binlog/GTID variable checks instead).
        """

    def oversized_values(
        self,
        table_name: str,
        columns: "Sequence[str]",
        *,
        limit_bytes: int,
        timeout_seconds: float,
    ) -> "Optional[dict[str, bool]]":
        """Whether any value in each column EXCEEDS ``limit_bytes``. Read-only.

        Returns ``{column: True/False}``, or ``None`` when the answer could not be
        established (the probe is absent, the read failed, or it hit
        ``timeout_seconds``) -- which the check reports as "not determined" rather
        than as a pass, because a silent pass here is exactly the false reassurance
        the check exists to remove.

        OPTIONAL on the protocol: :class:`PrerequisiteChecker` calls it through
        ``getattr`` so a probe that predates it (and every existing test fake)
        simply yields "not determined".
        """


class TargetProbe(Protocol):
    """Read-only access to the target Aurora DSQL used by prerequisite checks."""

    def reachable(self) -> bool:
        """Return whether the DSQL endpoint is reachable (network/TCP)."""

    def iam_auth(self) -> ConnectionResult:
        """Return whether an IAM-token connection + ``SELECT 1`` succeeds."""

    def relation_exists(self, qualified_name: str) -> bool:
        """Return whether the target relation for ``qualified_name`` exists."""

    def required_columns_without_default(
        self, qualified_name: str
    ) -> Optional[Sequence[str]]:
        """Return the target's value-required columns, or ``None`` if unreadable.

        Value-required = ``NOT NULL``, no ``DEFAULT``, not an identity column. Used
        by :func:`check_target_columns_loadable`. ``None`` means the target could not
        be read (missing table / catalog error), which the check treats as "not my
        failure to own" (TARGET_SCHEMA_READY reports it).
        """


class MskProbe(Protocol):
    """Read-only availability probe for the CDC infrastructure (CDC mode only)."""

    def cluster_available(self) -> bool:
        """Return whether the MSK cluster is reachable."""

    def connect_available(self) -> bool:
        """Return whether MSK Connect (for the Debezium source) is available."""


# ---------------------------------------------------------------------------
# Pure per-check functions (each independently unit-testable)
# ---------------------------------------------------------------------------


def check_source_reachable(result: ConnectionResult) -> PrerequisiteResult:
    """PASS when the source connection check succeeded."""
    ok = result.success
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SOURCE_REACHABLE,
        title="Source database is reachable",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Connected to the source." if ok else "Could not connect to the source.",
        remediation=""
        if ok
        else "Verify host/port/security group and source credentials.",
    )


def _parse_privilege_grant(line: str) -> Optional[tuple[str, str]]:
    """Parse a ``GRANT <privs> ON <scope> TO ...`` line into ``(privs, scope)``.

    ``privs`` is the upper-cased privilege list text (e.g. ``SELECT, INSERT``);
    ``scope`` is the object scope with quoting/whitespace stripped (``*.*`` for a
    global grant, ``app.*`` / ``app.orders`` for a scoped one). Returns ``None`` for
    a line that is not a privilege grant -- notably a ROLE-membership grant
    (``GRANT `role`@`host` TO `user`@`host```, which has no ``ON`` clause).
    """
    stripped = line.strip()
    upper = stripped.upper()
    if not upper.startswith("GRANT ") or " ON " not in upper:
        return None
    on_index = upper.index(" ON ")
    privs = stripped[len("GRANT ") : on_index].upper()
    rest = stripped[on_index + len(" ON ") :]
    to_index = rest.upper().find(" TO ")
    scope_text = rest[:to_index] if to_index != -1 else rest
    scope = (
        scope_text.strip()
        .replace("`", "")
        .replace("'", "")
        .replace('"', "")
        .replace(" ", "")
    )
    return privs, scope


def _confers_select(privs_upper: str) -> bool:
    """True when a grant's privilege list confers ``SELECT`` (directly or via ALL)."""
    return (
        "SELECT" in privs_upper
        or "ALL PRIVILEGES" in privs_upper
        or privs_upper.strip() == "ALL"
    )


def _select_grant_scope_state(grants: list[str]) -> str:
    """Classify the SELECT grant scope as ``global`` / ``scoped`` / ``none``.

    ``global`` = SELECT (or ALL) granted on ``*.*``; ``scoped`` = SELECT (or ALL)
    granted only on specific databases/tables; ``none`` = no SELECT-conferring grant
    is visible at all (that is REPLICATION_GRANTS' FAIL to own).
    """
    seen_scoped = False
    for line in grants:
        parsed = _parse_privilege_grant(line)
        if parsed is None:
            continue
        privs_upper, scope = parsed
        if not _confers_select(privs_upper):
            continue
        if scope == "*.*":
            return "global"
        seen_scoped = True
    return "scoped" if seen_scoped else "none"


def _is_role_grant(line: str) -> bool:
    """True when a ``SHOW GRANTS`` line grants a ROLE (no ``ON`` clause).

    A role-membership line is ``GRANT `role`@`host` TO `user`@`host``` -- a ``GRANT
    ... TO ...`` with no ``ON`` scope, distinct from a privilege grant.
    """
    upper = line.strip().upper()
    return upper.startswith("GRANT ") and " TO " in upper and " ON " not in upper


def check_replication_grants(
    grants: list[str],
    mode: MigrationMode,
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> PrerequisiteResult:
    """PASS when the source user holds the privileges required for ``mode``.

    Full Load requires ``SELECT``; CDC additionally requires ``REPLICATION
    CLIENT`` and ``REPLICATION SLAVE``. ``ALL PRIVILEGES`` satisfies any
    requirement. Matching is case-insensitive over the raw ``SHOW GRANTS`` text.

    The CDC replication privileges are MySQL-specific tokens. PostgreSQL CDC uses
    PostgreSQL's own replication readiness instead (the ``REPLICATION`` role
    attribute / ``rds_replication`` membership), checked separately, so a
    PostgreSQL source is only asked for the ``SELECT`` Full Load grant here
    regardless of ``mode`` -- never MySQL's replication privileges.

    Role-granted SELECT (MySQL 8.0+): plain ``SHOW GRANTS`` lists a granted role as a
    ``GRANT `role`@... TO ...`` membership line but does NOT expand that role's
    privileges (only ``SHOW GRANTS ... USING <role>`` does). A user who holds SELECT
    solely through a role would therefore look like it is missing SELECT. Fully
    resolving roles is disproportionate here, so when the ONLY missing required
    privilege is SELECT and the user holds at least one role, the result is DOWNGRADED
    from a hard, blocking FAIL to a non-blocking WARN -- the false "SELECT missing"
    no longer blocks a role-privileged user, while still prompting them to confirm.
    """
    cdc_mysql = mode == MigrationMode.CDC and source_type is SourceType.MYSQL
    required = _CDC_GRANTS if cdc_mysql else _FULL_LOAD_GRANTS
    blob = " ".join(grants).upper()
    has_all = "ALL PRIVILEGES" in blob
    missing = [
        priv for priv in required if not has_all and priv.upper() not in blob
    ]
    ok = not missing

    # Role-granted SELECT: the only missing privilege is SELECT and the user holds a
    # role whose (unexpanded) privileges plain SHOW GRANTS cannot see -> WARN, not FAIL.
    if (
        not ok
        and source_type is SourceType.MYSQL
        and missing == ["SELECT"]
        and any(_is_role_grant(line) for line in grants)
    ):
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.REPLICATION_GRANTS,
            title="Source user has the required privileges",
            status=PrerequisiteStatus.WARN,
            required=False,
            detail=(
                "SELECT was not found in the source user's direct grants, but the user "
                "holds one or more roles. Plain SHOW GRANTS does not expand a role's "
                "privileges, so SELECT may already be available through a role."
            ),
            remediation=(
                "Confirm the migration user's active roles grant SELECT "
                "(SHOW GRANTS FOR CURRENT_USER() USING <role>), or grant SELECT "
                "directly to the user to remove any doubt before loading."
            ),
        )

    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.REPLICATION_GRANTS,
        title="Source user has the required privileges",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="Required privileges present."
        if ok
        else f"Missing privileges: {', '.join(missing)}.",
        remediation=""
        if ok
        else (
            "Use a dedicated least-privilege CDC user granted SELECT, REPLICATION "
            "CLIENT, REPLICATION SLAVE (and RELOAD + LOCK TABLES for the initial "
            "snapshot) rather than an admin account."
            if cdc_mysql
            else "Grant SELECT to a dedicated migration user (avoid an admin account)."
        ),
    )


def check_select_grant_scope(
    grants: list[str],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> Optional[PrerequisiteResult]:
    """Non-blocking WARN when the source SELECT grant is scoped, not global.

    REPLICATION_GRANTS passes as long as ``SELECT`` appears ANYWHERE in the grants,
    ignoring its scope. But a db-/table-scoped SELECT (``GRANT SELECT ON `app`.* ...``)
    means introspection's ``SHOW FULL TABLES`` / ``SHOW SCHEMAS`` are privilege-filtered:
    any table or whole database the user lacks a grant on is silently absent from the
    inventory and would be omitted from the migration. The tool cannot enumerate objects
    it cannot see, so this does not (and cannot) block -- it surfaces a non-blocking WARN
    asking the operator to confirm the migration user can see every object to migrate.

    Returns ``None`` (no notice) when the grant is global (``SELECT``/``ALL`` on ``*.*``)
    or when no SELECT grant is present at all (REPLICATION_GRANTS owns that FAIL). Scoped
    to a MySQL source -- PostgreSQL introspection does not use MySQL's ``SHOW`` statements.
    """
    if source_type is not SourceType.MYSQL:
        return None
    if _select_grant_scope_state(grants) != "scoped":
        return None
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SELECT_GRANT_SCOPE,
        title="Source SELECT grant covers every object to migrate",
        status=PrerequisiteStatus.WARN,
        required=False,
        detail=(
            "The source user's SELECT privilege is scoped to specific databases/tables, "
            "not global (*.*). Introspection lists objects with SHOW FULL TABLES / "
            "SHOW SCHEMAS, which are privilege-filtered, so any table or database the "
            "user cannot SELECT is silently omitted from the inventory."
        ),
        remediation=(
            "Confirm the migration user can SELECT every table and database you intend "
            "to migrate, or grant broader SELECT (e.g. SELECT ON *.*) so nothing is "
            "silently left out of the assessment and load."
        ),
    )


def check_table_primary_key(table: TableDef) -> PrerequisiteResult:
    """PASS when ``table`` has a primary key (required for keying/upsert)."""
    ok = bool(table.primary_key)
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TABLE_PRIMARY_KEY,
        title="Table has a primary key",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail="Primary key present."
        if ok
        else "No primary key on the selected table.",
        remediation=""
        if ok
        else (
            f"Add a primary key to `{table.name}`; CDC keying and idempotent "
            "upsert require it."
        ),
    )


def check_target_dsql_reachable(reachable: bool) -> PrerequisiteResult:
    """PASS when the DSQL endpoint is reachable."""
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_DSQL_REACHABLE,
        title="Target DSQL endpoint is reachable",
        status=PrerequisiteStatus.PASS if reachable else PrerequisiteStatus.FAIL,
        required=True,
        detail="DSQL endpoint reachable." if reachable else "DSQL endpoint unreachable.",
        remediation=""
        if reachable
        else "Check VPC route/NAT/PrivateLink to the DSQL endpoint (port 5432).",
    )


def check_target_iam_auth(result: ConnectionResult) -> PrerequisiteResult:
    """PASS when an IAM-token connection to DSQL succeeds."""
    ok = result.success
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_IAM_AUTH,
        title="Target DSQL IAM authentication works",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="IAM token connection succeeded."
        if ok
        else "IAM token connection failed.",
        remediation=""
        if ok
        else (
            "Attach dsql:DbConnect/DbConnectAdmin to the task or connector "
            "execution role for this cluster."
        ),
    )


def check_target_schema_ready(table: TableDef, exists: bool) -> PrerequisiteResult:
    """PASS when the target table for ``table`` already exists (DDL applied)."""
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_SCHEMA_READY,
        title="Target schema is ready for the table",
        status=PrerequisiteStatus.PASS if exists else PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail="Target table exists." if exists else "Target table does not exist.",
        remediation=""
        if exists
        else f"Apply converted DDL for `{table.name}` in Step 2 (Schema Conversion) first.",
    )


def check_target_columns_loadable(
    table: TableDef, target_required_without_default: Optional[Sequence[str]]
) -> PrerequisiteResult:
    """FAIL when a target column is value-required but the source can't fill it.

    Full Load builds its INSERT column list from the SOURCE table, so a column that
    exists ONLY on the target (e.g. one the user added while editing the target DDL
    in Schema Conversion) is never named in an INSERT. That is fine unless the column
    is ``NOT NULL`` with no default -- then the row has no value for it and the load
    fails with a not-null violation partway through, after the target already holds
    partial data. This check moves that certain failure to a pre-load gate.

    ``target_required_without_default`` is the target's value-required columns (NOT
    NULL, no DEFAULT, not identity) from
    :func:`~dsql_migrator.core.target_introspector.target_required_columns_without_default`.
    The blocking set is those MINUS the source columns: a value-required column that
    also exists on the source is filled by the INSERT and is fine; only ones absent
    from the source are unfillable. ``None`` means the target could not be read
    (missing table or catalog error) -- that is TARGET_SCHEMA_READY's job to report,
    so this check PASSes rather than double-failing on the same cause.

    Nullable or defaulted extra columns are deliberately NOT flagged: verified on a
    live DSQL cluster, the load succeeds and they take NULL or their default.
    """
    if target_required_without_default is None:
        # Unknown target (missing/unreadable) -> not this check's failure to own.
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE,
            title="Target columns can be loaded from the source",
            status=PrerequisiteStatus.PASS,
            required=True,
            target=table.name,
            detail="Target columns not checked (target schema not readable yet).",
            remediation="",
        )
    source_columns = {column.name for column in table.columns}
    unfillable = [
        name for name in target_required_without_default if name not in source_columns
    ]
    if not unfillable:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE,
            title="Target columns can be loaded from the source",
            status=PrerequisiteStatus.PASS,
            required=True,
            target=table.name,
            detail="Every value-required target column is present on the source.",
            remediation="",
        )
    listed = ", ".join(f"`{name}`" for name in unfillable)
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE,
        title="Target columns can be loaded from the source",
        status=PrerequisiteStatus.FAIL,
        required=True,
        target=table.name,
        detail=(
            f"Target has NOT NULL column(s) with no default the source cannot fill: "
            f"{listed}. Full Load inserts only source columns, so the load would fail "
            "with a not-null violation partway through."
        ),
        remediation=(
            f"For `{table.name}`, make each of these columns nullable or give it a "
            "DEFAULT in Step 2 (Schema Conversion), or drop the column if it is not "
            "needed on the target. Columns present on the source are unaffected."
        ),
    )


# Aurora DSQL's documented per-value ceiling for a non-index column. Quoted, not
# probed: a probe once appeared to store 9.5 MiB of text because TOAST compressed it,
# and a measurement must never overrule a documented quota.
DSQL_MAX_VALUE_BYTES = 1024 * 1024

# ... but 1 MiB is NOT the ceiling for every type, and using it for all of them told the
# operator a limit 16x too high on an unbounded varchar. From "Supported data types in
# Aurora DSQL", each figure re-verified live on a real cluster with INCOMPRESSIBLE values:
#   text            1 MiB      (1048577 bytes -> "datatype limit greater than 1048576 ...")
#   bytea           1 MiB      (same, and bytea is NOT compressed)
#   json / jsonb    1 MiB      applied to the COMPRESSED size
#   varchar        65535 bytes (65536 -> "datatype limit greater than 65535 ... varchar")
#   char / bpchar   4096 bytes (4097 -> "value too long for type character(4096)")
# Compression applies to text/varchar/bpchar and json/jsonb, and ONLY in columns that are
# not part of a key -- a key column is always stored uncompressed.
_DSQL_VALUE_LIMIT_BY_BASE_TYPE = {
    "text": DSQL_MAX_VALUE_BYTES,
    "bytea": DSQL_MAX_VALUE_BYTES,
    "json": DSQL_MAX_VALUE_BYTES,
    "jsonb": DSQL_MAX_VALUE_BYTES,
    "character varying": 65535,
    "varchar": 65535,
    "character": 4096,
    "char": 4096,
    "bpchar": 4096,
    # MySQL LOB families, whose own source limits are all at or under DSQL's text ceiling
    # (LONGTEXT/LONGBLOB reach 4 GiB, hence the cap).
    "longtext": DSQL_MAX_VALUE_BYTES,
    "mediumtext": DSQL_MAX_VALUE_BYTES,
    "longblob": DSQL_MAX_VALUE_BYTES,
    "mediumblob": DSQL_MAX_VALUE_BYTES,
    "blob": DSQL_MAX_VALUE_BYTES,
    "tinyblob": DSQL_MAX_VALUE_BYTES,
    "tinytext": DSQL_MAX_VALUE_BYTES,
}

# The types whose 1 MiB ceiling is measured AFTER compression, so an uncompressed
# measurement over the limit is an upper bound rather than a verdict.
# Keyed on the TARGET type each source type converts to: a MySQL LONGTEXT becomes DSQL
# text (compressed), a LONGBLOB becomes bytea (not compressed).
_DSQL_COMPRESSED_BASE_TYPES = frozenset(
    {
        "text",
        "character varying",
        "varchar",
        "character",
        "char",
        "bpchar",
        "json",
        "jsonb",
        "longtext",
        "mediumtext",
        "tinytext",
    }
)


def dsql_value_limit_bytes(column_type: str) -> int:
    """Aurora DSQL's per-value byte ceiling for a column of ``column_type``.

    Falls back to the generic 1 MiB per-column limit for anything not in the table, which
    is the documented database-wide maximum for a non-index column -- so an unknown type
    is never reported with a limit LOWER than the truth.
    """
    base = _base_column_type(column_type)
    return _DSQL_VALUE_LIMIT_BY_BASE_TYPE.get(base, DSQL_MAX_VALUE_BYTES)


def dsql_value_limit_is_compressed(column_type: str) -> bool:
    """Whether ``column_type``'s ceiling applies to the COMPRESSED size (non-key only)."""
    return _base_column_type(column_type) in _DSQL_COMPRESSED_BASE_TYPES


def _base_column_type(column_type: str) -> str:
    """Lower-cased type name with any length/precision modifier stripped."""
    text_value = " ".join((column_type or "").strip().lower().split())
    return text_value.split("(", 1)[0].strip()


def _format_limit(limit_bytes: int) -> str:
    """Render a byte ceiling the way the docs do (1 MiB, or an exact byte count)."""
    if limit_bytes == DSQL_MAX_VALUE_BYTES:
        return "1 MiB"
    return f"{limit_bytes:,} bytes"

# How long the oversized-value probe may run per table before it gives up. The probe
# stops at the FIRST offending row, so a table that HAS one answers fast; proving the
# ABSENCE of one is a full pass over the column, which on a very large table is exactly
# the source scan this project avoids. Timing out yields "not determined", never a pass.
VALUE_SIZE_PROBE_TIMEOUT_SECONDS = 15.0


def check_source_value_size(
    table: TableDef,
    columns: "Sequence[str]",
    oversized: "Optional[Mapping[str, bool]]",
) -> "Optional[PrerequisiteResult]":
    """Grade whether a LOB column already holds a value DSQL cannot store. Pure.

    ``None`` when the table has no LOB column to ask about, so a schema of ordinary
    columns adds no row to the report.

    NON-blocking by design (``required=False``): an oversized value is a decision the
    operator owns -- accept that the row is quarantined, shrink the value, move the
    content to S3 and store a reference, or exclude the column on Data Migration -- and
    the tool must not refuse to start a migration over it. What it must not do is stay
    silent, which is what it did: the Evaluation finding said "check the largest value in
    each column" and nothing in the tool could.

    ``oversized`` maps ONLY the columns the probe actually measured to whether they
    exceeded THEIR ceiling. Two things follow, and both were wrong before:

    * The ceiling is PER TYPE, not a flat 1 MiB (:func:`dsql_value_limit_bytes`) -- text /
      bytea / json / jsonb are 1 MiB, but varchar is 65535 bytes and char 4096, so the old
      single figure overstated an unbounded varchar's headroom 16-fold.
    * A column ABSENT from the mapping was treated as "did not exceed" and reported inside
      a PASS. On PostgreSQL that was reachable in normal use -- ``octet_length(jsonb)``
      does not exist, so a jsonb column's probe failed -- which produced a green
      "No value exceeds ..." naming a column nothing had measured. Unmeasured columns are
      now listed as not-determined, and one unmeasured column downgrades the whole row to
      INFO rather than PASS.
    """
    if not columns:
        return None
    by_name = {column.name: column for column in table.columns}

    def limit_of(name: str) -> int:
        column = by_name.get(name)
        return dsql_value_limit_bytes(column.mysql_type if column else "")

    def describe(names: "Sequence[str]") -> str:
        """`col` (limit) for each, grouping is unnecessary and per-column is clearer."""
        return ", ".join(f"`{name}` ({_format_limit(limit_of(name))})" for name in names)

    title = "No value exceeds the Aurora DSQL per-value limit for its type"
    measured = dict(oversized or {})
    unmeasured = [name for name in columns if name not in measured]
    exceeded = [name for name in columns if measured.get(name)]

    compressed = [name for name in columns if
                  dsql_value_limit_is_compressed((by_name[name].mysql_type
                                                  if name in by_name else ""))]
    compression_note = (
        " For "
        + ", ".join(f"`{name}`" for name in compressed)
        + " the limit applies to the COMPRESSED size (and only outside a key -- a key "
        "column is stored uncompressed), so a highly compressible value may still fit; "
        "this probe measures the uncompressed byte length, an upper bound."
        if compressed
        else ""
    )

    if exceeded:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.SOURCE_VALUE_SIZE,
            title=title,
            status=PrerequisiteStatus.WARN,
            required=False,
            target=table.name,
            detail=(
                f"{describe(exceeded)} already holds at least one value over the limit "
                "shown. Those rows CANNOT be stored on Aurora DSQL: each is quarantined "
                "during Full Load (or dead-lettered during CDC) and reloading cannot fix "
                "it."
                + (
                    f" Not determined for {describe(unmeasured)}."
                    if unmeasured
                    else ""
                )
            ),
            remediation=(
                "Decide before loading: move the content to external storage (e.g. Amazon "
                "S3) and store a reference, shrink the value, or exclude the column on the "
                "Data Migration step so the rest of the row migrates. Proceeding as-is "
                "loads every other row and quarantines these." + compression_note
            ),
        )

    if unmeasured:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.SOURCE_VALUE_SIZE,
            title=title,
            status=PrerequisiteStatus.INFO,
            required=False,
            target=table.name,
            detail=(
                f"Not determined for {describe(unmeasured)}. Proving that no value exceeds "
                "the limit needs a full pass over the column, which was not completed (the "
                "read failed or timed out)."
                + (
                    f" Measured and within the limit: {describe([n for n in columns if n in measured])}."
                    if measured
                    else ""
                )
            ),
            remediation=(
                "Optional: check the largest value yourself (PostgreSQL "
                "`SELECT max(octet_length(col)) FROM table`, casting json/jsonb to text "
                "first; MySQL `SELECT MAX(OCTET_LENGTH(col)) FROM table`). An oversized "
                "value is quarantined during Full Load and dead-lettered during CDC, and "
                "reloading cannot fix it." + compression_note
            ),
        )

    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.SOURCE_VALUE_SIZE,
        title=title,
        status=PrerequisiteStatus.PASS,
        required=False,
        target=table.name,
        detail=f"No value exceeds the limit in {describe(list(columns))}.",
        remediation="",
    )


def _on(value: Optional[str]) -> bool:
    """Return True when a MySQL variable value means 'on' (case-insensitive)."""
    return (value or "").strip().upper() in {"ON", "1"}


def _as_int(value: Optional[str]) -> Optional[int]:
    """Parse a MySQL variable value to int, or None when absent/non-numeric."""
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


# The binlog must be retained at least this long for the Full Load -> CDC handoff:
# CDC resumes from the binlog file:position (or GTID) watermark captured at snapshot
# start, so the source must still hold that binlog when CDC begins consuming. 24h is
# the widely-recommended floor (matches AWS DMS guidance) with margin for the load +
# CDC-stack deploy; 168h (7d) is the recommended set-value.
_MIN_BINLOG_RETENTION_HOURS = 24


def check_binlog_row_format(variables: dict[str, str]) -> PrerequisiteResult:
    """PASS when binary logging is on with ROW format and a FULL row image."""
    log_bin = _on(variables.get("log_bin"))
    row_format = (variables.get("binlog_format") or "").strip().upper() == "ROW"
    full_image = (variables.get("binlog_row_image") or "").strip().upper() == "FULL"
    ok = log_bin and row_format and full_image
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        title="Binary log uses ROW format with full row image",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.FAIL,
        required=True,
        detail="log_bin=ON, binlog_format=ROW, binlog_row_image=FULL."
        if ok
        else "Binary logging is not ROW/FULL or is disabled.",
        remediation=""
        if ok
        else (
            "Set binlog_format=ROW and binlog_row_image=FULL on the source "
            "(RDS parameter group)."
        ),
    )


def check_gtid_mode(variables: dict[str, str]) -> PrerequisiteResult:
    """PASS when GTID mode is on; otherwise a non-blocking ``INFO`` recommendation.

    GTID is **not required** for CDC. Debezium and the watermark-based resume
    (:class:`~dsql_migrator.core.cdc.CdcResumePoint`) fall back to the binlog
    ``file:position`` coordinate when GTID is unavailable, so the gapless Full
    Load -> CDC handoff still works. GTID is only *recommended* because it
    survives source failover/replica promotion (where ``file:position`` can
    break). Its absence is therefore reported as ``INFO`` (an optional
    recommendation, ``required=False``) -- NOT a ``WARN`` (which implies something
    is wrong) and NOT a gating ``FAIL`` (Property 14). Customers without GTID
    enabled are not forced to turn it on and are not shown an alarming warning.
    """
    ok = (variables.get("gtid_mode") or "").strip().upper() == "ON"
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.GTID_MODE,
        title="GTID mode is enabled (recommended)",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.INFO,
        required=False,
        detail="gtid_mode=ON."
        if ok
        else "gtid_mode is not ON; CDC will resume from the binlog file:position watermark.",
        remediation=""
        if ok
        else (
            "Optional: enable GTID mode for more robust CDC resume across source "
            "failover/replica promotion. Not required -- CDC otherwise resumes "
            "from the binlog file:position watermark."
        ),
    )


def check_binlog_retention(variables: dict[str, str]) -> PrerequisiteResult:
    """WARN when the source's binlog retention is too short for the FL -> CDC handoff.

    CDC resumes from the binlog file:position (or GTID) watermark captured at the
    Full Load snapshot point, so the source must still hold that binlog when CDC
    begins. If retention is too short (the classic RDS gotcha: ``binlog retention
    hours`` unset -> RDS purges aggressively), the binlog is gone before CDC starts
    -> a SILENT data gap. This never hard-blocks (WARN / ``required=False``, per
    Property 14) -- a fast load + prompt CDC start may still fit inside a short
    window -- but it is a real, non-obvious risk worth surfacing.

    Retention (hours) is read from, in order: the RDS ``binlog retention hours``
    config (folded into ``variables`` as ``rds_binlog_retention_hours`` by the probe;
    ``"0"`` means the RDS row exists but is unset -> aggressive purge -> risk), else
    the self-managed ``binlog_expire_logs_seconds`` (``0`` = purge DISABLED = binlogs
    kept = safe) or ``expire_logs_days``. Unknown -> non-blocking ``INFO``.
    """
    hours: Optional[float] = None
    unbounded = False  # self-managed automatic purge disabled -> binlogs retained
    rds = variables.get("rds_binlog_retention_hours")
    if rds is not None:
        try:
            hours = float(str(rds).strip())
        except (TypeError, ValueError):
            hours = 0.0  # unparseable RDS value -> treat as risk
    else:
        secs = _as_int(variables.get("binlog_expire_logs_seconds"))
        days = _as_int(variables.get("expire_logs_days"))
        if secs is not None and secs > 0:
            hours = secs / 3600.0
        elif secs == 0:
            unbounded = True
        elif days is not None and days > 0:
            hours = days * 24.0
        elif days == 0:
            unbounded = True

    if unbounded:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.BINLOG_RETENTION,
            title="Binary log retention covers the CDC handoff",
            status=PrerequisiteStatus.PASS,
            required=False,
            detail="Automatic binlog purging is disabled; binlogs are retained.",
        )
    if hours is None:
        return PrerequisiteResult(
            check_id=PrerequisiteCheckId.BINLOG_RETENTION,
            title="Binary log retention covers the CDC handoff",
            status=PrerequisiteStatus.INFO,
            required=False,
            detail=(
                "Could not read the source's binlog retention. Ensure it exceeds the "
                "Full Load duration plus the time to start CDC, or the binlog may be "
                "purged before CDC resumes from the watermark."
            ),
            remediation=(
                f"Confirm retention >= {_MIN_BINLOG_RETENTION_HOURS}h. On RDS: "
                "CALL mysql.rds_set_configuration('binlog retention hours', 168);"
            ),
        )
    ok = hours >= _MIN_BINLOG_RETENTION_HOURS
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.BINLOG_RETENTION,
        title="Binary log retention covers the CDC handoff",
        status=PrerequisiteStatus.PASS if ok else PrerequisiteStatus.WARN,
        required=False,
        detail=(
            f"Binlog retention is ~{hours:.0f}h."
            if ok
            else (
                f"Binlog retention is only ~{hours:.0f}h — the binlog may be purged "
                "before CDC resumes from the Full Load watermark (silent data gap)."
            )
        ),
        remediation=""
        if ok
        else (
            f"Raise retention to >= {_MIN_BINLOG_RETENTION_HOURS}h (168h/7d "
            "recommended). On RDS: "
            "CALL mysql.rds_set_configuration('binlog retention hours', 168); "
            "self-managed: set binlog_expire_logs_seconds accordingly."
        ),
    )


def check_msk_available(available: bool) -> PrerequisiteResult:
    """Report MSK cluster presence as a non-blocking ``INFO`` (not WARN, not FAIL).

    MSK is NOT a CDC prerequisite: in this tool's model the MSK Serverless
    cluster is created when the customer deploys the cdc-stack -- which happens
    AFTER the CDC step produces the connector config, not before. So when the
    cluster is not yet present this is expected, no-action-needed ``INFO`` (not a
    ``WARN`` that implies a problem, and never a required ``FAIL`` that would
    block reaching the CDC step). When the cluster IS already present (e.g. a
    prior deploy) it is reported ``PASS`` as useful confirmation.
    """
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.MSK_AVAILABLE,
        title="MSK cluster is available",
        status=PrerequisiteStatus.PASS if available else PrerequisiteStatus.INFO,
        required=False,
        detail="MSK cluster reachable."
        if available
        else "MSK cluster not deployed yet (created when you deploy the cdc-stack).",
        remediation=""
        if available
        else (
            "Not a prerequisite -- the MSK Serverless cluster is provisioned when "
            "you deploy the cdc-stack, which you do AFTER completing the CDC step "
            "(it produces the connector config to deploy). No action needed here."
        ),
    )


def check_msk_connect_available(available: bool) -> PrerequisiteResult:
    """Report MSK Connect presence as a non-blocking ``INFO`` (not WARN, not FAIL).

    Like :func:`check_msk_available`, MSK Connect (which runs the Debezium source
    and DSQL sink connectors) is created by the cdc-stack deploy that follows the
    CDC step -- so its absence is expected, no-action-needed ``INFO``, not a
    ``WARN`` (no problem here) and not a blocking ``FAIL``. Present (a prior
    deploy) reports ``PASS``.
    """
    return PrerequisiteResult(
        check_id=PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
        title="MSK Connect is available",
        status=PrerequisiteStatus.PASS if available else PrerequisiteStatus.INFO,
        required=False,
        detail="MSK Connect available."
        if available
        else "MSK Connect not deployed yet (created when you deploy the cdc-stack).",
        remediation=""
        if available
        else (
            "Not a prerequisite -- MSK Connect and its connectors are provisioned "
            "when you deploy the cdc-stack after the CDC step. No action needed here."
        ),
    )


def _skipped(
    check_id: PrerequisiteCheckId, title: str, *, reason: str = "mode"
) -> PrerequisiteResult:
    """Build a non-applicable (SKIP) result for a check that did not run.

    ``reason`` says WHY, because the two are not interchangeable and saying the wrong
    one misleads: ``"mode"`` means the check applies to this engine but only in CDC
    (so Full Load previews it as upcoming work), while ``"engine"`` means it never
    applies to this source at all. A PostgreSQL source was shown "Binary log uses ROW
    format ... Not applicable for this mode", which reads as "it WILL apply once you
    switch to CDC" -- PostgreSQL has no binary log in any mode.
    """
    detail = (
        "Not applicable for this source engine."
        if reason == "engine"
        else "Not applicable for this mode."
    )
    return PrerequisiteResult(
        check_id=check_id,
        title=title,
        status=PrerequisiteStatus.SKIP,
        required=True,
        detail=detail,
    )


class PrerequisiteChecker:
    """Runs the prerequisite gate for a mode over the selected tables.

    The source/target/MSK access is supplied as injectable read-only probes, so
    the checker performs no writes (Property 1) and is unit-testable with fakes.
    """

    def __init__(
        self,
        *,
        source_probe: SourceProbe,
        target_probe: TargetProbe,
        msk_probe: Optional[MskProbe] = None,
    ) -> None:
        """Create a checker.

        ``source_probe`` and ``target_probe`` are required. ``msk_probe`` is only
        needed for ``CDC`` mode; in ``FULL_LOAD`` the MSK checks are skipped and a
        missing probe is fine. All probes are read-only.
        """
        self._source = source_probe
        self._target = target_probe
        self._msk = msk_probe

    def _probe_oversized_values(
        self,
        table_name: str,
        columns: "Sequence[str]",
        column_types: "Optional[Mapping[str, str]]" = None,
    ) -> "Optional[dict[str, bool]]":
        """Ask the source whether any value exceeds the limit, or ``None`` if it cannot.

        ``oversized_values`` is OPTIONAL on :class:`SourceProbe` -- resolved through
        ``getattr`` so every probe written before it (and every existing test fake) keeps
        working and simply yields "not determined". Any failure is swallowed to ``None``
        for the same reason: this check is advisory and must never break the gate, and
        ``None`` is reported honestly as not-determined rather than as a pass.
        """
        probe = getattr(self._source, "oversized_values", None)
        if not callable(probe):
            return None
        try:
            types = dict(column_types or {})
            kwargs = {
                "limit_bytes": DSQL_MAX_VALUE_BYTES,
                "timeout_seconds": VALUE_SIZE_PROBE_TIMEOUT_SECONDS,
            }
            # Per-column ceiling and type are passed only when the probe accepts them, so
            # every probe/fake written before they existed keeps working (same reason the
            # whole method is resolved through getattr).
            try:
                accepted = set(inspect.signature(probe).parameters)
            except (TypeError, ValueError):  # pragma: no cover - exotic callable
                accepted = set()
            if "limits" in accepted:
                kwargs["limits"] = {
                    name: dsql_value_limit_bytes(types.get(name, "")) for name in columns
                }
            if "column_types" in accepted:
                kwargs["column_types"] = types
            answer = probe(table_name, list(columns), **kwargs)
        except Exception:  # noqa: BLE001 - advisory read; never break the gate
            return None
        if not isinstance(answer, Mapping):
            return None
        return {str(k): bool(v) for k, v in answer.items()}

    def check(
        self,
        request: PrerequisiteCheckRequest,
        *,
        tables: Sequence[TableDef],
        excluded_columns: Optional[Mapping[str, Iterable[str]]] = None,
        lob_columns: Optional[Mapping[str, Sequence[str]]] = None,
    ) -> PrerequisiteReport:
        """Run all checks for ``request.mode`` over the resolved ``tables``.

        Produces one :class:`PrerequisiteResult` per check (and per selected
        table for ``TABLE_PRIMARY_KEY`` / ``TARGET_SCHEMA_READY``). CDC mode adds
        the binlog/GTID/MSK checks; Full Load reports those as ``SKIP``. Returns a
        :class:`PrerequisiteReport` whose ``can_proceed`` is ``True`` only when no
        required check failed (Property 14). All probe access is read-only.

        ``excluded_columns`` (optional, table name -> column names) is the
        migration-wide oversized-LOB exclusion. Each table is filtered through
        :func:`~dsql_migrator.core.models.apply_lob_exclusions` before the per-table
        checks, so the gate judges the EXACT columns the load will write: an
        excluded column that is ``NOT NULL`` with no default on the target then
        correctly FAILs :func:`check_target_columns_loadable` (it can no longer be
        filled) instead of passing here and failing every batch mid-load. A PK is
        never excluded (the filter guards), so ``check_table_primary_key`` is
        unaffected. Empty/omitted => no filtering (the common case).

        ``lob_columns`` (optional, table name -> column names) are the oversized-LOB
        candidates to ask the per-value size question about. Supplied by the caller
        because the helper that identifies them lives in the UI layer; omitted => the
        size check is not run at all (no row in the report).
        """
        exclusions = excluded_columns or {}
        results: list[Optional[PrerequisiteResult]] = []

        # Source-side checks
        results.append(check_source_reachable(self._source.reachable()))
        grants = self._source.grants()
        results.append(
            check_replication_grants(
                grants,
                request.mode,
                source_type=request.source_type,
            )
        )
        # Non-blocking notice: a db-/table-scoped SELECT grant means introspection's
        # privilege-filtered object listing may silently omit objects the user cannot
        # see. Only added when the grant is actually scoped (None otherwise).
        scope_notice = check_select_grant_scope(
            grants, source_type=request.source_type
        )
        if scope_notice is not None:
            results.append(scope_notice)

        # Per-table checks. Filter each table through the migration-wide LOB
        # exclusion so the pre-load gate sees the columns the load will actually
        # write (never the PK, which the filter preserves).
        effective = {
            table.name: apply_lob_exclusions(table, exclusions.get(table.name))
            for table in tables
        }
        # The at-risk LOB columns are supplied BY THE CALLER, from the same helper that
        # drives the oversized-LOB finding and the exclusion picker, so the three surfaces
        # cannot disagree about which columns are at risk. Passed in rather than derived
        # here because that helper lives in the UI layer and ``core`` must not import it
        # (the dependency is deliberately one-way) -- the same reason ``excluded_columns``
        # is a parameter. Omitted => the size question is simply not asked.
        lob_by_table = {
            name: tuple(cols) for name, cols in (lob_columns or {}).items()
        }

        for table in tables:
            results.append(check_table_primary_key(effective[table.name]))
            # Ask only about columns that SURVIVE the exclusion: a column the operator
            # already excluded is not going to be written, so warning about its size
            # would be noise -- and it is the remedy this very check recommends.
            already_excluded = set(exclusions.get(table.name) or ())
            lob_columns = tuple(
                name
                for name in lob_by_table.get(table.name, ())
                if name not in already_excluded
            )
            if lob_columns:
                results.append(
                    check_source_value_size(
                        table,
                        lob_columns,
                        self._probe_oversized_values(
                            table.name,
                            lob_columns,
                            {c.name: (c.mysql_type or "") for c in table.columns},
                        ),
                    )
                )

        # Target-side checks
        results.append(check_target_dsql_reachable(self._target.reachable()))
        results.append(check_target_iam_auth(self._target.iam_auth()))
        for table in tables:
            eff_table = effective[table.name]
            exists = self._target.relation_exists(table.name)
            results.append(check_target_schema_ready(eff_table, exists))
            # Only meaningful once the table exists; when it does not, pass None so
            # the columns check defers to TARGET_SCHEMA_READY rather than repeating
            # the same "apply DDL first" failure.
            required_without_default = (
                self._target.required_columns_without_default(table.name)
                if exists
                else None
            )
            results.append(
                check_target_columns_loadable(eff_table, required_without_default)
            )

        # CDC-only checks (SKIP in Full Load).
        if request.mode == MigrationMode.CDC and request.source_type is SourceType.POSTGRES:
            # PostgreSQL CDC readiness: logical replication (pgoutput) needs
            # wal_level=logical, a slot-creating role, slot/wal-sender headroom, a writer
            # source, and a usable REPLICA IDENTITY per captured table. The facts are
            # read once (read-only) by the dialect probe; the pure checks live in the
            # engine-separated prerequisites_postgres module. MSK is engine-neutral (PG
            # CDC uses the same MSK pipeline) so it still runs; the MySQL binlog/GTID
            # checks do not apply and are SKIP.
            from dsql_migrator.core import prerequisites_postgres

            # Derive the object names ONLY when the run must find them, so a
            # Full-load-+-CDC probe stays byte-identical (it reads no existence facts and
            # the existence check SKIPs).
            _pub_name = _slot_name = ""
            if not request.provisions_replication and request.cdc_stack_name:
                from dsql_migrator.core import cdc_pg_slot as _pg_names

                _pub_name = _pg_names.pg_publication_name(request.cdc_stack_name)
                _slot_name = _pg_names.pg_slot_name(request.cdc_stack_name)
            facts = self._source.cdc_prerequisites(
                [table.name for table in tables],
                publication_name=_pub_name,
                slot_name=_slot_name,
            )
            # ``facts`` full of Nones is NOT "partially known" -- it is a source whose
            # readiness was never verified, and it used to slip past this gate because the
            # object itself was not None: every check degraded to a non-blocking INFO and
            # the report proceeded. Treated the same as no facts at all.
            if facts is not None and prerequisites_postgres.postgres_cdc_facts_are_unverified(
                facts
            ):
                facts = None
            if facts is None:
                # For a PostgreSQL source the probe returns facts unless it FAILED
                # (unreachable / insufficient privilege). CDC must not start against a
                # source whose logical-replication readiness is unverified, so this is a
                # blocking FAIL -- not the advisory not-supported INFO.
                results.append(
                    prerequisites_postgres.check_postgres_cdc_facts_unavailable()
                )
            else:
                results.extend(
                    prerequisites_postgres.check_postgres_cdc_prerequisites(
                        facts,
                        [effective[table.name] for table in tables],
                        provisions_replication=request.provisions_replication,
                        cdc_start_resnapshots=request.cdc_start_resnapshots,
                        message_key_columns=request.message_key_columns,
                    )
                )
            # MySQL's binlog/GTID rows are NOT listed for a PostgreSQL source, in either
            # mode. They were kept as SKIPs to satisfy a mode-symmetry rule (a check id
            # present in the weaker mode but absent in the stronger one reads as an
            # oversight), and v0.1.498 at least corrected their wording from "not
            # applicable for this MODE" to "...this source ENGINE". But symmetry is about
            # the two MODES of one engine, and for PostgreSQL these are absent from BOTH
            # modes now, so it holds. What remains is simply another engine's requirements
            # on a PostgreSQL operator's screen: three rows that can never apply, beside
            # the six that do. The PG counterpart of the retention risk is its own check
            # (SLOT_WAL_RETENTION), so nothing is lost by omitting them.
            results.append(
                check_msk_available(
                    self._msk.cluster_available() if self._msk else False
                )
            )
            results.append(
                check_msk_connect_available(
                    self._msk.connect_available() if self._msk else False
                )
            )
        elif request.mode == MigrationMode.CDC:
            variables = self._source.variables()
            results.append(check_binlog_row_format(variables))
            results.append(check_binlog_retention(variables))
            results.append(check_gtid_mode(variables))
            results.append(
                check_msk_available(
                    self._msk.cluster_available() if self._msk else False
                )
            )
            results.append(
                check_msk_connect_available(
                    self._msk.connect_available() if self._msk else False
                )
            )
        else:
            # Full Load only: no CDC check RUNS, but they stay visible as SKIP so the
            # operator can preview what switching to CDC will additionally require. That
            # preview has to be the SOURCE ENGINE's requirements -- this branch used to
            # emit MySQL's three binlog/GTID rows unconditionally, so a PostgreSQL user
            # was previewed another engine's checks (labelled "not applicable for this
            # mode", implying they WOULD apply under CDC) and shown none of their own six.
            # The engine split mirrors the CDC branch above, which already splits here.
            is_postgres = request.source_type is SourceType.POSTGRES
            if is_postgres:
                from dsql_migrator.core import prerequisites_postgres

                results.extend(
                    prerequisites_postgres.postgres_cdc_prerequisites_skipped()
                )
            # The MySQL rows stay present for a PostgreSQL source too, but as an ENGINE
            # skip rather than a mode skip: D-1 established that a check id present in
            # one mode and absent in another reads as an oversight, so PG CDC keeps them
            # visible -- and Full Load must match, or the id would vanish going the other
            # way. Only the reason differs by engine.
            if not is_postgres:
                # MySQL only: these preview what switching to CDC will additionally
                # require. A PostgreSQL source gets its OWN six previewed above instead --
                # listing another engine's requirements was the whole defect.
                results.append(
                    _skipped(
                        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
                        "Binary log uses ROW format with full row image",
                    )
                )
                results.append(
                    _skipped(
                        PrerequisiteCheckId.BINLOG_RETENTION,
                        "Binary log retention covers the CDC handoff",
                    )
                )
                results.append(
                    _skipped(PrerequisiteCheckId.GTID_MODE, "GTID mode is enabled")
                )
            results.append(
                _skipped(PrerequisiteCheckId.MSK_AVAILABLE, "MSK cluster is available")
            )
            results.append(
                _skipped(
                    PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
                    "MSK Connect is available",
                )
            )

        # check_source_value_size returns None for a table with no LOB column, so a
        # schema of ordinary columns adds no row at all.
        return PrerequisiteReport.build(
            request.mode, [r for r in results if r is not None]
        )


__all__ = [
    "SourceProbe",
    "TargetProbe",
    "MskProbe",
    "PrerequisiteChecker",
    "check_source_reachable",
    "check_replication_grants",
    "check_select_grant_scope",
    "check_table_primary_key",
    "check_target_dsql_reachable",
    "check_target_iam_auth",
    "check_target_schema_ready",
    "check_binlog_row_format",
    "check_binlog_retention",
    "check_gtid_mode",
    "check_msk_available",
    "check_msk_connect_available",
]
