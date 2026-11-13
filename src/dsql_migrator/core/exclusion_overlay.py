# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The column-exclusion overlay on Aurora DSQL, and the cut-over restore of a DEFAULT (P2).

An excluded column is not dropped from the target: the loader simply never writes it, so
the target column must accept that -- it is made nullable and its DEFAULT removed (the
OVERLAY), and at cut over a dropped DEFAULT is put back. This module holds the whole of
that: the statement builder (:func:`build_overlay_statements`), the facts an applied DDL
gives it (:func:`ddl_column_facts`), the reconnecting per-table unit that applies it and
reads it back (:func:`apply_exclusion_overlay`), and the single-expression check and
statement of the restore (:func:`restore_column_default`).

Split out of :mod:`dsql_migrator.core.schema_applier` (CI-07), whose business is applying
converted DDL; the dependency is one-way -- the overlay asks it for the retrying,
reconnecting unit runner and the error base class, and nothing there imports this module,
so there is no cycle and no re-export shim. Importers (the Full Load, the CDC overlay
pass, the exclusion rules, Cut over) import these names from here.
"""

from __future__ import annotations

import random
import time
from functools import lru_cache
from typing import TYPE_CHECKING, Mapping, Optional, Sequence

from psycopg import sql

from dsql_migrator.core.occ import (
    DEFAULT_BASE_DELAY_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    JitterFunc,
    SleepFunc,
)
from dsql_migrator.core.schema_applier import (
    ConnectionFactory,
    SchemaApplyError,
    _run_unit_reconnecting,
)

if TYPE_CHECKING:
    from dsql_migrator.core.target_introspector import ColumnNullFacts


# DROP NOT NULL, then DROP DEFAULT; and the cut-over restore.
#
# Settled live on Aurora DSQL (P0 F1-F12, P2 LIVE 1): both ALTERs are synchronous O(1)
# metadata changes (no sys.jobs row, no poll), accepted while any index build -- a
# multi-column UNIQUE one included -- is submitted or processing; they are idempotent (F10),
# so a 40001 retry or a whole-unit replay after a lost reply is safe; concurrent DDL on
# different tables fails 40001 (F8), so callers run the units SERIALLY; and DROP NOT NULL
# must come BEFORE DROP DEFAULT (F9: the reverse order made concurrent writers that omit the
# column store the DEFAULT into a still-NOT-NULL column, 67 x 23502).


class OverlayRefusedError(SchemaApplyError):
    """The overlay (or a restore) refused a column before any statement ran."""


class OverlayVerifyError(SchemaApplyError):
    """The read-back after the overlay (or a restore) does not show the intended state."""


_COMPUTED_REFUSAL = "Computed by Aurora DSQL; never written."


class OverlayStatement:
    """One overlay ALTER: ``action`` is ``DROP NOT NULL`` or ``DROP DEFAULT``."""

    __slots__ = ("column", "action", "statement")

    def __init__(self, column: str, action: str, statement: "sql.Composed") -> None:
        self.column = column
        self.action = action
        self.statement = statement

    def text(self) -> str:
        """The statement as shown to the operator (identifiers quoted)."""
        return self.statement.as_string(None)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"OverlayStatement({self.column!r}, {self.action!r})"


def build_overlay_statements(
    table_name: str, facts: "Mapping[str, Optional[ColumnNullFacts]]"
) -> "list[OverlayStatement]":
    """THE statement builder: per column (sorted), DROP NOT NULL, then DROP DEFAULT.

    Only what the facts still need: a nullable column gets no DROP NOT NULL, a column with
    no DEFAULT no DROP DEFAULT; an absent (``None``) column nothing. A column Aurora DSQL
    computes raises :class:`OverlayRefusedError` (the rule's B4 backstop). Fed live facts by
    the overlay and both Start dialogs, and the applied DDL's facts (:func:`ddl_column_facts`)
    by the exclusion popup, so every surface shows the statements that would run.
    """
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    table = _pg_table_identifier(table_name)
    out: list[OverlayStatement] = []
    for column in sorted(facts):
        column_facts = facts[column]
        if column_facts is None:
            continue
        if column_facts.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")
        ident = sql.Identifier(column)
        if column_facts.not_null:
            out.append(OverlayStatement(column, "DROP NOT NULL", sql.SQL(
                "ALTER TABLE {t} ALTER COLUMN {c} DROP NOT NULL").format(t=table, c=ident)))
        if column_facts.default_expr is not None:
            out.append(OverlayStatement(column, "DROP DEFAULT", sql.SQL(
                "ALTER TABLE {t} ALTER COLUMN {c} DROP DEFAULT").format(t=table, c=ident)))
    return out


@lru_cache(maxsize=32768)  # = converter.DDL_CACHE_SIZE
def _generated_columns_folded(create_ddl: str) -> frozenset:
    """The applied DDL's GENERATED columns, cached by text: the popup previews every
    matched table on each render, and the converter's reader is uncached."""
    from dsql_migrator.core.converter import parse_target_generated_columns

    return frozenset(parse_target_generated_columns(create_ddl, folded=True))


def ddl_column_facts(
    create_ddl: str, columns: "Sequence[str]"
) -> "Optional[dict[str, Optional[ColumnNullFacts]]]":
    """The facts :func:`build_overlay_statements` needs, read from an applied DDL.

    Marks identity and generated columns as the live read does (an identity column is
    implicitly NOT NULL and has no DEFAULT), so the preview refuses exactly what the live
    overlay refuses. ``None`` when the DDL cannot be read ("unknown", never "nothing to
    do"); an inner ``None`` is a column the DDL does not declare.
    """
    from dsql_migrator.core.converter import (
        parse_target_column_constraints,
        parse_target_identity_columns,
    )
    from dsql_migrator.core.target_introspector import ColumnNullFacts

    constraints = parse_target_column_constraints(create_ddl)
    if not constraints:
        return None
    generated = _generated_columns_folded(create_ddl)
    identity = parse_target_identity_columns(create_ddl)
    out: dict[str, Optional[ColumnNullFacts]] = {}
    for column in columns:
        if column not in constraints:
            out[column] = None
            continue
        not_null, default_sql = constraints[column]
        is_identity = column in identity
        is_generated = column in generated
        out[column] = ColumnNullFacts(
            not_null=not_null or is_identity,
            default_expr=None if (is_generated or is_identity) else default_sql,
            identity=is_identity,
            generated=is_generated,
        )
    return out


class OverlayOutcome:
    """What one table's overlay unit did.

    ``dropped_defaults`` maps each column whose DEFAULT this unit dropped to the pre-ALTER
    expression (what the ledger records and cut over restores).
    """

    __slots__ = ("dropped_not_null", "dropped_defaults", "already_ok")

    def __init__(self, dropped_not_null, dropped_defaults, already_ok) -> None:
        self.dropped_not_null: tuple[str, ...] = tuple(dropped_not_null)
        self.dropped_defaults: dict[str, str] = dict(dropped_defaults)
        self.already_ok: tuple[str, ...] = tuple(already_ok)

    def __eq__(self, other) -> bool:
        return isinstance(other, OverlayOutcome) and (
            self.dropped_not_null, self.dropped_defaults, self.already_ok
        ) == (other.dropped_not_null, other.dropped_defaults, other.already_ok)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"OverlayOutcome(dropped_not_null={self.dropped_not_null}, "
            f"dropped_defaults={self.dropped_defaults}, already_ok={self.already_ok})"
        )


def _backstop(table_name: str, facts) -> None:
    for column, column_facts in facts.items():
        if column_facts is not None and column_facts.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")


def apply_exclusion_overlay(
    table_name: str,
    planned: "Mapping[str, Optional[ColumnNullFacts]]",
    *,
    expected_defaults: "Optional[Mapping[str, Optional[str]]]" = None,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> OverlayOutcome:
    """Make ``planned``'s present columns nullable with no DEFAULT, as ONE unit.

    ``planned`` is the caller's pre-ALTER facts (``None`` = absent live: skipped).
    ``expected_defaults`` maps a column to the DEFAULT the ledger recorded for it (``None``
    = recorded as having none); ``None`` skips the check. Steps:

    1. the backstop, before any connect -- a computed column raises
       :class:`OverlayRefusedError`;
    2. nothing to issue by ``planned`` (every present column already nullable with no
       DEFAULT): return at once, with NO connection;
    3. otherwise one reconnecting unit on a fresh connection. EVERY attempt first re-reads
       the live facts on its own connection (a lost reply replays safely, F10) and refuses
       before ANY ALTER of that attempt when a live DEFAULT is not the recorded one -- the
       DEFAULT it would drop was never recorded, so cut over could not put it back. It then
       issues what the live catalog still needs, one statement per autocommit transaction
       with 40001 retried per statement, and does ONE read-back on the SAME connection: any
       present column still NOT NULL, still with a DEFAULT, or gone raises
       :class:`OverlayVerifyError`. No poll, no sleep, no sys.jobs.

    Every other error propagates (the caller fails the table).
    """
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    _backstop(table_name, planned)
    present = sorted(c for c, f in planned.items() if f is not None)
    if not build_overlay_statements(table_name, planned):
        return OverlayOutcome((), {}, present)

    issued_not_null: list[str] = []
    issued_defaults: dict[str, str] = {}

    def _read(conn):
        return read_overlay_column_facts(conn, table_name, present)

    def _body(connection, run_one, retry_read):
        _exists, facts = retry_read(_read)
        _backstop(table_name, facts)
        if expected_defaults is not None:
            _refuse_unrecorded_defaults(table_name, facts, expected_defaults, issued_not_null,
                                        issued_defaults)
        for statement in build_overlay_statements(table_name, facts):
            # Counted BEFORE it runs: a lost reply may still have committed it, and the
            # replay's re-read then finds nothing left to issue for it.
            if statement.action == "DROP NOT NULL":
                issued_not_null.append(statement.column)
            else:
                # What THIS statement drops: the live facts it was built from.
                issued_defaults[statement.column] = facts[statement.column].default_expr  # type: ignore[union-attr]
            run_one(connection, statement.statement)
        exists, after = retry_read(_read)
        remaining = []
        for column in present:
            live = after.get(column)
            if not exists or live is None:
                remaining.append(f"{column} (gone)")
            elif live.not_null:
                remaining.append(f"{column} (still NOT NULL)")
            elif live.default_expr is not None:
                remaining.append(f"{column} (still DEFAULT {live.default_expr})")
        if remaining:
            raise OverlayVerifyError(
                f"{table_name}: the overlay did not take effect for "
                + ", ".join(remaining)
            )
        return None

    _run_unit_reconnecting(
        connection_factory,
        _body,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )
    done = set(issued_not_null) | set(issued_defaults)
    return OverlayOutcome(
        sorted(dict.fromkeys(issued_not_null)),
        issued_defaults,
        [c for c in present if c not in done],
    )


def _refuse_unrecorded_defaults(table_name, facts, expected, issued_not_null, issued_defaults):
    """Raise before any ALTER when a live DEFAULT is not the one the ledger recorded."""
    mismatched = [
        f"{column} (live DEFAULT {live.default_expr}, recorded "
        f"{'none' if expected.get(column) is None else expected.get(column)})"
        for column, live in sorted(facts.items())
        if live is not None
        and live.default_expr is not None
        and live.default_expr != expected.get(column)
    ]
    if not mismatched:
        return
    earlier = [f"{c} DROP NOT NULL" for c in dict.fromkeys(issued_not_null)] + [
        f"{c} DROP DEFAULT" for c in issued_defaults
    ]
    changed = (
        "nothing was changed on this table"
        if not earlier
        else "an earlier attempt already issued " + ", ".join(earlier)
    )
    raise OverlayRefusedError(
        f"{table_name}: the DEFAULT to drop is not the recorded one for "
        + ", ".join(mismatched)
        + f"; {changed}. Re-run to record the current DEFAULT first."
    )


def is_single_sql_expression(expr: str) -> bool:
    """True when ``expr`` is ONE SQL expression, safe to splice into a SET DEFAULT.

    The text comes from ``pg_get_expr`` (the ledger) and is still checked, because the
    restore executes it and the ledger is a file anyone with the bucket or the state file
    can edit. The scan is exact because it refuses everything that could make its quote
    tracking wrong, none of which ``pg_get_expr`` emits:

    * a backslash anywhere but inside a PLAIN '...' literal: outside quotes, in a "..."
      identifier, and in a prefixed literal (E'...', U&'...', B'...', X'...') it could
      start an escape. Inside a plain literal it is an ordinary character under
      ``standard_conforming_strings`` (which :func:`restore_column_default` checks on its
      own connection before running such a text), and ``pg_get_expr`` prints every bytea
      constant that way ('\\x00'::bytea);
    * any ``$`` outside quotes (a dollar-quoted body, a ``$1``);
    * ``;``, ``--`` or ``/*`` outside quotes;
    * a ``,`` outside every bracket (a second ALTER TABLE subcommand), and an unbalanced
      ``(`` / ``[``;
    * an empty text or an unterminated quote.

    Quotes are '...' and "..." with the doubled-quote escape.
    """
    text = (expr or "").strip()
    if not text:
        return False
    quote: Optional[str] = None
    plain = False  # the open quote is a '...' literal with no E / U& / B / X prefix
    # Where the last '...' literal closed, and whether it was plain: a quote after
    # whitespace holding a newline CONTINUES that literal in PostgreSQL's lexer, in the
    # same (escape) mode, so it inherits instead of being judged by the char before it.
    closed_at, closed_plain = -1, False
    depth = 0
    i = 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "\\" and not plain:
                return False
            if ch == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    i += 2
                    continue
                if quote == "'":
                    closed_at, closed_plain = i + 1, plain
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            gap = text[closed_at:i] if closed_at >= 0 else None
            if ch == "'" and gap is not None and gap.strip() == "" and ("\n" in gap or "\r" in gap):
                plain = closed_plain
            else:
                before = text[i - 1] if i else " "
                plain = ch == "'" and not (before.isalnum() or before in "_&$")
        elif ch == "\\":
            return False
        elif ch in ("(", "["):
            depth += 1
        elif ch in (")", "]"):
            depth -= 1
            if depth < 0:
                return False
        elif ch in (";", "$") or (ch == "," and depth == 0):
            return False
        elif text.startswith("--", i) or text.startswith("/*", i):
            return False
        i += 1
    return quote is None and depth == 0


def restore_default_statement(table_name: str, column: str, expr: str) -> sql.Composed:
    """THE ``ALTER TABLE .. ALTER COLUMN .. SET DEFAULT (expr)`` a cut-over restore runs
    (and shows). Parenthesized, so any text the single-expression scan missed is a syntax
    error rather than a second subcommand; PostgreSQL does not keep the parentheses."""
    from dsql_migrator.core.validation_sql import _pg_table_identifier

    return (
        sql.SQL("ALTER TABLE {t} ALTER COLUMN {c} SET DEFAULT (").format(
            t=_pg_table_identifier(table_name), c=sql.Identifier(column)
        )
        + sql.SQL(expr)
        + sql.SQL(")")
    )


class RestoreOutcome:
    """``status``: restored (this unit set it) | done (already equal) | diverged | gone."""

    __slots__ = ("status", "live_default")

    def __init__(self, status: str, live_default: Optional[str]) -> None:
        self.status = status
        self.live_default = live_default

    def __eq__(self, other) -> bool:
        return isinstance(other, RestoreOutcome) and (
            self.status, self.live_default) == (other.status, other.live_default)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"RestoreOutcome({self.status!r}, {self.live_default!r})"


def restore_column_default(
    table_name: str,
    column: str,
    expr: str,
    *,
    connection_factory: ConnectionFactory,
    occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
    sleep: SleepFunc = time.sleep,
    jitter: JitterFunc = random.random,
) -> RestoreOutcome:
    """Put back ONE dropped DEFAULT at cut over (``SET DEFAULT`` affects new rows only, F12).

    Refused before any connect unless ``expr`` is a single expression. Then one
    reconnecting unit whose EVERY attempt first re-reads the live column: absent -> gone;
    computed -> refused; the same DEFAULT live -> done (or restored, if an earlier attempt of
    THIS unit set it and only the reply was lost); a different DEFAULT live -> diverged, never
    overwritten; no DEFAULT -> ``SET DEFAULT expr`` and a read-back on the same connection
    that must round-trip byte-identically (F4), else :class:`OverlayVerifyError`.
    """
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    if not is_single_sql_expression(expr):
        raise OverlayRefusedError(
            f"{table_name}.{column}: the recorded DEFAULT is not a single expression, so "
            "it is not restored automatically; run SET DEFAULT by hand."
        )
    issued = {"set": False}

    def _read(conn):
        return read_overlay_column_facts(conn, table_name, [column])

    def _standard_strings(conn):
        cur = conn.cursor()
        try:
            cur.execute("SHOW standard_conforming_strings")
            row = cur.fetchone()
        finally:
            cur.close()
        return str(row[0]).strip().lower() if row else ""

    def _body(connection, run_one, retry_read):
        exists, facts = retry_read(_read)
        live = facts.get(column) if exists else None
        if live is None:
            return RestoreOutcome("gone", None)
        if live.computed:
            raise OverlayRefusedError(f"{table_name}.{column}: {_COMPUTED_REFUSAL}")
        if live.default_expr is not None:
            if live.default_expr == expr:
                return RestoreOutcome("restored" if issued["set"] else "done", expr)
            return RestoreOutcome("diverged", live.default_expr)
        if "\\" in expr and retry_read(_standard_strings) != "on":
            # Off, a backslash in a plain '...' opens an escape: the scan's quote tracking
            # no longer holds, so the text is not run.
            raise OverlayRefusedError(
                f"{table_name}.{column}: the recorded DEFAULT holds a backslash and "
                "standard_conforming_strings is off on this connection, so it is not "
                "restored automatically; run SET DEFAULT by hand."
            )
        issued["set"] = True
        # The read-back below still compares byte-identically (PostgreSQL drops the
        # parentheses the statement adds).
        run_one(connection, restore_default_statement(table_name, column, expr))
        exists, facts = retry_read(_read)
        after = facts.get(column) if exists else None
        if after is None or after.default_expr != expr:
            raise OverlayVerifyError(
                f"{table_name}.{column}: the restored DEFAULT does not round-trip "
                f"(set {expr!r}, read back "
                f"{None if after is None else after.default_expr!r})."
            )
        return RestoreOutcome("restored", expr)

    return _run_unit_reconnecting(
        connection_factory,
        _body,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=occ_base_delay,
        sleep=sleep,
        jitter=jitter,
    )



__all__ = [
    "OverlayRefusedError",
    "OverlayVerifyError",
    "OverlayStatement",
    "OverlayOutcome",
    "RestoreOutcome",
    "build_overlay_statements",
    "ddl_column_facts",
    "apply_exclusion_overlay",
    "is_single_sql_expression",
    "restore_column_default",
    "restore_default_statement",
]
