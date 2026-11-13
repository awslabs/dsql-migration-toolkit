# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reading the converted target DDL script: what its statements are and what they say.

One layer above the quote-aware scan (:mod:`dsql_migrator.core.sql_text`, which this reads
SQL text through) and below the apply engine. It answers the questions Schema Conversion and
Schema Apply ask of a DDL script -- the generated one or the operator's edit -- as TEXT, with
no connection and no apply contract:

- the statements of a script (:func:`split_sql_statements`, :func:`edited_statements`) and
  the ones an edit glued together by losing a ``;`` (:func:`split_glued_statements`,
  :func:`_statement_level`), plus the deferred-foreign-key section a script is split at and
  rejoined by (:func:`split_target_ddl_fk_sections`, :func:`join_target_ddl_fk_sections`,
  :func:`_is_foreign_key_ddl`);
- what one statement is: its short label (:func:`statement_label`, :func:`_describe_ddl` and
  the ``TABLE``/``VIEW``/``INDEX``/``SCHEMA`` kinds), whose object it belongs to and in what
  role (:func:`object_statement_role`, :func:`object_create_kind`,
  :func:`is_object_scaffolding`) -- judged the way the applier's parser and PostgreSQL BOTH
  read a name (:func:`_match_read_alike`, :func:`_stored_parts`);
- merging an object's edited statements back into a script (:func:`merge_object_ddl`, over
  the key definition :func:`_key_definition` reads);
- what a stored DDL says about the primary key the picker chose
  (:func:`identity_from_ddl`, :func:`composite_leading_from_ddl` and the column-clause
  readers under them).

It was split out of ``ui/schema_conversion_apply.py`` (AS-7), which re-exports every name
defined here (private ones too) and which ``ui/schema_conversion.py`` re-exports in turn, so
``from dsql_migrator.ui.schema_conversion_apply import ...`` and ``from
dsql_migrator.ui.schema_conversion import ...`` of any of them keep resolving. The apply
contracts, the OCC-retrying orchestration and the DSQL applier adapter stay there; this
module must not import either of them, and imports no UI module at all. It reads
``core.converter`` (``parse_target_primary_key``) at its top level, so the converter must not
import it.
"""

from __future__ import annotations

import bisect
import re
from typing import Optional, Sequence

from dsql_migrator.core.converter import parse_target_primary_key
from dsql_migrator.core.models import TableDef
from dsql_migrator.core.sql_text import sql_without_comments


# The parsed CREATE kind for an index (matches the core applier's parser). An
# index statement is a leaf: its failure does not abort sibling statements,
# whereas a failed structural statement (SCHEMA/TABLE) aborts dependent indexes.
_INDEX_KIND = "INDEX"

# The parsed CREATE kind for a schema. ``CREATE SCHEMA IF NOT EXISTS`` is
# idempotent scaffolding the core applier always reports as CREATED ("schema
# ensured"), so it must NOT drive a per-object CREATED/SKIPPED verdict -- a
# re-apply of an existing qualified table would otherwise look CREATED just
# because its schema was re-ensured.
_SCHEMA_KIND = "SCHEMA"

# The parsed CREATE kind for a view. A view selecting from a table blocks that
# table's DROP during a destructive REPLACE, so apply-set views are dropped in a
# pre-pass before any table is recreated (see _predrop_dependent_views).
_VIEW_KIND = "VIEW"
# parse_create_object's kind for a table, used to select the REPLACE's table targets.
_TABLE_KIND = "TABLE"


def _describe_ddl(ddl: str) -> tuple[str, Optional[str]]:
    """Return a short ``(label, kind)`` for one DDL statement.

    ``kind`` is the parsed CREATE kind (``TABLE``/``INDEX``/...) or ``None`` when
    the statement cannot be parsed (e.g. a test placeholder); ``label`` is a
    compact human-readable description such as ``TABLE categories`` used in the
    per-statement apply summary.
    """
    from dsql_migrator.core.schema_applier import (
        SchemaApplyError,
        parse_create_object,
    )

    try:
        name, kind = parse_create_object(ddl)
        return f"{kind} {name}", kind
    except SchemaApplyError:
        snippet = " ".join(ddl.split())[:60]
        return (snippet or "statement"), None


def _is_foreign_key_ddl(statement: str) -> bool:
    """Return True when ``statement`` is a preserved-foreign-key ALTER.

    A preserved foreign key is rendered as a post-load ``ALTER TABLE ... ADD
    CONSTRAINT ... FOREIGN KEY ... NOT VALID`` (NOT inlined in ``CREATE TABLE``),
    because the concurrent, cross-table bulk load has no parent-before-child
    ordering. Such a statement is applied by the post-load foreign-key pass, not
    at Schema Apply, so both apply paths -- the deterministic
    :func:`build_apply_objects` (which omits ``foreign_key_ddls``) and the edited
    :func:`override_apply_objects` -- exclude it; it is also the one non-``CREATE``
    statement the Schema Applier's CREATE-only parser would otherwise reject. The
    ``(`` split isolates the leading clause before the referencing column list,
    where both ``ALTER TABLE`` and ``FOREIGN KEY`` always appear (this is the
    single source of truth reused by :func:`_classify_edited_table_conversion`).
    """
    head = sql_without_comments(statement).strip().upper().split("(", 1)[0]
    return head.startswith("ALTER TABLE") and "FOREIGN KEY" in head


def edited_statements(script: str) -> list[str]:
    """The statements of an edited target-DDL script as they are applied: each without its
    comments, and a comment-only one dropped. A note typed above a statement made it one
    comment-led statement, which Schema Apply refused (it reads a CREATE) and Full Load
    filed under no kind -- falling back to the generated DDL without the edit's indexes."""
    return [
        clean
        for statement in split_sql_statements(script)
        if (clean := sql_without_comments(statement).strip())
    ]


# One identifier as the core applier's parser reads it (quoted with "" escapes, or bare), and
# a one- or two-part name of them. Never a shorter reading of a longer name: a bare part ends
# at its last name character, a quoted one at its real closing quote, and no further '.' may
# follow -- ``"shop" . "orders"`` and ``db.shop.orders``, which the applier reads as another
# name than PostgreSQL creates, name nothing here; nor does a bare part the two read apart
# (:func:`_match_read_alike`).
_IDENT_PART = r'(?:"(?:[^"]|"")+"(?!")|[A-Za-z_][\w$]*(?![\w$]))'
_RELATION_NAME = rf"{_IDENT_PART}(?:\.{_IDENT_PART})?(?!\s*\.)"
_OWN_CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:RECURSIVE\s+(?=VIEW\b))?(TABLE|VIEW)\s+"
    rf"(?:IF\s+NOT\s+EXISTS\s+)?({_RELATION_NAME})",
    re.IGNORECASE,
)
_OWN_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?:({_IDENT_PART})\s+)?ON\s+(?:ONLY\s+)?({_RELATION_NAME})",
    re.IGNORECASE,
)
_OWN_FOREIGN_KEY_RE = re.compile(
    rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?({_RELATION_NAME})\s+ADD\s+"
    rf"(?:CONSTRAINT\s+({_IDENT_PART})\s+)?FOREIGN\s+KEY\b",
    re.IGNORECASE,
)
_OWN_SCHEMA_RE = re.compile(
    rf"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT_PART})(?!\s*\.)", re.IGNORECASE
)
_OWN_DROP_RE = re.compile(
    rf"DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_RELATION_NAME})", re.IGNORECASE
)
# object_statement_role's role of a foreign-key ALTER.
_FOREIGN_KEY_ROLE = "FOREIGN KEY"


def _stored_parts(raw_name: str) -> "tuple[str, ...]":
    """A written name's parts as PostgreSQL stores them: quoted as written, unquoted folded,
    each cut to 63 bytes."""
    from dsql_migrator.core.sql_text import _chain_parts
    from dsql_migrator.core.target_introspector import pg_stored_name

    return tuple(pg_stored_name(part) for part in _chain_parts(raw_name)[0])


def _object_stored_parts(object_name: str) -> "tuple[str, str]":
    """``(schema, name)`` of a source object as the converter creates it: split on the first
    dot, an unqualified one in public (where Aurora DSQL's search_path puts it)."""
    from dsql_migrator.core.target_introspector import pg_stored_name

    schema, dot, name = object_name.partition(".")
    if not (dot and schema and name):
        schema, name = "public", object_name
    return pg_stored_name(schema), pg_stored_name(name)


def _names_object(raw_name: str, object_name: str) -> bool:
    """Whether a written relation name is ``object_name`` as PostgreSQL resolves it. A
    one-part name is public's: a bare ``CREATE TABLE orders`` for ``shop.orders`` creates
    ``public.orders``, while the load writes to ``"shop"."orders"``."""
    parts = _stored_parts(raw_name)
    schema, name = _object_stored_parts(object_name)
    if len(parts) == 2:
        return parts == (schema, name)
    return len(parts) == 1 and schema == "public" and parts[0] == name


def _match_read_alike(pattern: "re.Pattern[str]", clean: str) -> "Optional[re.Match[str]]":
    """``pattern``'s match at the start of ``clean``, or ``None`` when PostgreSQL and the
    core applier read a bare name of it apart. PostgreSQL folds only A-Z and the applier
    lower()s every letter, so a bare ``shop.ordersÉ`` is PostgreSQL's "ordersÉ" and the
    applier's "ordersé", which a REPLACE then dropped -- while ``shop.café`` or
    ``shop.order_주문`` is one name to both."""
    from dsql_migrator.core.converter import fold_identifier
    from dsql_migrator.core.sql_text import _SQL_PART_RE

    match = pattern.match(clean)
    if match is None or any(
        bare.lower() != fold_identifier(bare)
        for _quoted, bare in _SQL_PART_RE.findall(match.group(0))
    ):
        return None
    return match


def object_statement_role(statement: str, object_name: str) -> "Optional[tuple[str, str]]":
    """``(role, name)`` when ``statement`` is one of ``object_name``'s own: ``("TABLE" |
    "VIEW", "")`` for its CREATE, ``("INDEX", index name)`` for a CREATE INDEX on it,
    ``("FOREIGN KEY", constraint name)`` for a foreign-key ALTER of it -- a name as
    PostgreSQL stores it, ``""`` when the statement gives none. ``None`` for anything else
    (a SELECT, another object's statement, a CHECK added by ALTER). Comments are ignored."""
    clean = sql_without_comments(statement).strip()
    match = _match_read_alike(_OWN_CREATE_RE, clean)
    if match is not None:
        return (match.group(1).upper(), "") if _names_object(match.group(2), object_name) else None
    match = _match_read_alike(_OWN_INDEX_RE, clean)
    if match is not None:
        if not _names_object(match.group(2), object_name):
            return None
        return _INDEX_KIND, (_stored_parts(match.group(1))[0] if match.group(1) else "")
    match = _match_read_alike(_OWN_FOREIGN_KEY_RE, clean)
    # The same test the apply paths route a foreign key to the post-load pass by.
    if match is not None and _is_foreign_key_ddl(clean):
        if not _names_object(match.group(1), object_name):
            return None
        return _FOREIGN_KEY_ROLE, (_stored_parts(match.group(2))[0] if match.group(2) else "")
    return None


def object_create_kind(script: str, object_name: str) -> Optional[str]:
    """``"TABLE"`` / ``"VIEW"``: what ``script`` creates ``object_name`` as; ``None`` when no
    statement of it does (a not-converted note, a name-collision placeholder)."""
    for statement in edited_statements(script):
        role = object_statement_role(statement, object_name)
        if role is not None and role[0] in (_TABLE_KIND, _VIEW_KIND):
            return role[0]
    return None


def is_object_scaffolding(statement: str, object_name: str) -> bool:
    """Whether ``statement`` only prepares ``object_name``'s CREATE: the ``CREATE SCHEMA [IF
    NOT EXISTS]`` of its schema (a table's generated script has it, and Schema Apply puts the
    generated one first for an edit without it -- :func:`override_apply_objects`) or a
    ``DROP`` of the object itself (an edit is applied in REPLACE mode, which drops it). Each
    as PostgreSQL reads it: the core applier runs a schema ensure as written, and the DROP
    is never taken."""
    clean = sql_without_comments(statement).strip()
    match = _OWN_SCHEMA_RE.match(clean)
    if match is not None:
        return _stored_parts(match.group(1)) == (_object_stored_parts(object_name)[0],)
    match = _OWN_DROP_RE.match(clean)
    return match is not None and _names_object(match.group(1), object_name)


# What may stand between CREATE and the kind it creates: ``CREATE OR REPLACE FUNCTION``,
# ``CREATE UNIQUE INDEX``, ``CREATE TEMP TABLE``, ``CREATE CONSTRAINT TRIGGER``.
_CREATE_MODIFIERS = (
    r"(?:OR\s+REPLACE\s+)?"
    r"(?:(?:UNIQUE|GLOBAL|LOCAL|TEMP|TEMPORARY|UNLOGGED|MATERIALIZED|RECURSIVE|CONSTRAINT)\s+)*"
)
# Any written name, as a notice shows it (not only one the applier reads as PostgreSQL does).
_ANY_NAME_PART = r'(?:"(?:[^"]|"")+"|[^\W\d][\w$]*)'
_ANY_NAME = rf"{_ANY_NAME_PART}(?:\s*\.\s*{_ANY_NAME_PART})*"
_INDEX_LABEL_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+|CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?:(?!ON\b)({_ANY_NAME})\s+)?ON\s+(?:ONLY\s+)?({_ANY_NAME})",
    re.IGNORECASE,
)
_CREATE_LABEL_RE = re.compile(
    rf"CREATE\s+{_CREATE_MODIFIERS}([A-Za-z]+)(?:\s+IF\s+NOT\s+EXISTS)?(?:\s+({_ANY_NAME}))?",
    re.IGNORECASE,
)
_ALTER_OR_DROP_LABEL_RE = re.compile(
    rf"(ALTER|DROP)\s+(TABLE|VIEW|INDEX)\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?({_ANY_NAME})",
    re.IGNORECASE,
)


def statement_label(statement: str) -> str:
    """A statement named for a notice: ``CREATE TABLE shop.order_status`` for a CREATE (its
    modifiers left out: ``CREATE FUNCTION shop.touch`` for a CREATE OR REPLACE FUNCTION;
    ``CREATE INDEX ON shop.customers`` for an unnamed index), ``ALTER TABLE shop.orders`` for
    an ALTER or DROP, else its leading keyword (``SELECT``; a psql ``\\d`` as written), with
    the next one for ``COMMENT ON`` and its kin. A name is read as written and shown as
    PostgreSQL stores it, never through the core parser or the identity regexes, which stop
    short of a name they do not take: ``CREATE TABLE shop.été`` read 'CREATE TABLE shop' and
    ``DROP TABLE IF EXISTS shop.주문`` 'DROP TABLE if'. The first two words read 'CREATE OR'
    for any CREATE OR REPLACE, and the core parser takes an unnamed index's ON for its name."""
    clean = sql_without_comments(statement).strip()
    match = _INDEX_LABEL_RE.match(clean)
    if match is not None:
        if match.group(1):
            return f"CREATE INDEX {'.'.join(_stored_parts(match.group(1)))}"
        return f"CREATE INDEX ON {'.'.join(_stored_parts(match.group(2)))}"
    match = _CREATE_LABEL_RE.match(clean)
    if match is not None:
        # The kind as the core parser reads it: CREATE MATERIALIZED VIEW.
        kind = _describe_ddl(clean)[1] or match.group(1).upper()
        name = ".".join(_stored_parts(match.group(2))) if match.group(2) else ""
        return f"CREATE {kind} {name}".rstrip()
    match = _ALTER_OR_DROP_LABEL_RE.match(clean)
    if match is not None:
        name = ".".join(_stored_parts(match.group(3)))
        return f"{match.group(1)} {match.group(2)} ".upper() + name
    words = clean.split()
    if not words:
        return "a comment"
    if words[0].startswith("\\"):
        return words[0]  # a psql meta-command: \d and \D are two
    if words[0].upper() in ("ALTER", "CREATE", "DROP", "COMMENT", "START") and len(words) > 1:
        return f"{words[0]} {words[1]}".upper()
    return words[0].upper()


# One token as PostgreSQL reads it: a quoted name, a string literal, a word, a number, a sign.
_SQL_TOKEN_RE = re.compile(
    r'"(?P<quoted>(?:[^"]|"")*)"|\'(?:[^\']|\'\')*\'|(?P<word>[^\W\d][\w$]*)|\d[\w.]*|\S'
)


# What PostgreSQL does when a statement leaves it out, as the unquoted words a restatement may
# spell it with: an index's btree and a key's ASC; a foreign key's MATCH SIMPLE, NO ACTION --
# and RESTRICT, which the converter writes as NO ACTION unless the key is DEFERRABLE
# (G152) --, NOT DEFERRABLE and INITIALLY IMMEDIATE. (A key's NULLS LAST, NULLS FIRST
# after DESC, is the other one.)
_KEY_DEFAULTS = (
    ("using", "btree"),
    ("asc",),
    ("match", "simple"),
    ("on", "delete", "no", "action"),
    ("on", "update", "no", "action"),
    ("on", "delete", "restrict"),
    ("on", "update", "restrict"),
    ("not", "deferrable"),
    ("initially", "immediate"),
)
_KEY_DEFAULT_STARTS = frozenset({"nulls", *(default[0] for default in _KEY_DEFAULTS)})


def _key_definition(statement: str) -> "tuple[str, ...]":
    """What an index or foreign-key ALTER of an object creates, its name left out, token by
    token as PostgreSQL reads it (a quoted name as written, an unquoted word folded, comments
    and spacing dropped): UNIQUE or not and what follows ``ON <table>`` -- the key list, a
    predicate -- or what follows ``FOREIGN KEY`` -- the columns, REFERENCES, its actions.

    PostgreSQL's defaults are left out (:data:`_KEY_DEFAULTS`): an unnamed restatement that
    spelled out USING btree or ON DELETE NO ACTION matched nothing, and was added beside the
    statement it restates -- two indexes or constraints on one column. Never more: a changed
    action or predicate is another definition -- and RESTRICT is one on a DEFERRABLE key
    (G172), as the converter keeps it there: PostgreSQL never defers a RESTRICT check, so a
    reply that changed only it to NO ACTION, or back, was taken for the same key and
    dropped."""
    from dsql_migrator.core.converter import fold_identifier

    clean = sql_without_comments(statement).strip()
    match = _OWN_INDEX_RE.match(clean)
    if match is not None:
        unique = re.match(r"CREATE\s+UNIQUE\b", clean, re.IGNORECASE) is not None
        head = ("UNIQUE" if unique else "INDEX",)
    else:
        match = _OWN_FOREIGN_KEY_RE.match(clean)
        if match is None:
            return ()
        head = ()
    tokens = [
        (token.group("quoted").replace('""', '"'), False) if token.group("quoted") is not None
        else (fold_identifier(token.group(0)), True) if token.group("word") is not None
        else (token.group(0), False)
        for token in _SQL_TOKEN_RE.finditer(clean, match.end())
    ]
    # A DEFERRABLE key -- INITIALLY DEFERRED implies it -- keeps RESTRICT apart from NO ACTION.
    deferrable = any(
        bare and (
            (text == "deferrable" and tokens[at - 1 : at] != [("not", True)])
            or (text == "initially" and tokens[at + 1 : at + 2] == [("deferred", True)])
        )
        for at, (text, bare) in enumerate(tokens)
    )
    defaults = tuple(
        d for d in _KEY_DEFAULTS if not (deferrable and d[-1:] == ("restrict",))
    )
    kept: "list[tuple[str, bool]]" = []
    at = 0
    while at < len(tokens):
        default: "tuple[str, ...]" = ()
        if tokens[at][1] and tokens[at][0] in _KEY_DEFAULT_STARTS:
            # The unquoted words from here: a quoted "asc" is a column.
            words = tuple(text if bare else None for text, bare in tokens[at : at + 4])
            nulls = ("nulls", "first" if kept[-1:] == [("desc", True)] else "last")
            default = next((d for d in (*defaults, nulls) if words[: len(d)] == d), ())
        if not default:
            kept.append(tokens[at])
        at += len(default) or 1
    return head + tuple(text for text, _bare in kept)


def merge_object_ddl(
    current: str, statements: Sequence[str], object_name: str
) -> "tuple[str, list[str]]":
    """``current`` -- an object's target-DDL script, edited or generated -- with
    ``statements``, the object's own from an AI reply, put in: each replaces its counterpart
    (its CREATE, the index or foreign key of the same name as PostgreSQL resolves it), one
    that creates what a statement of ``current`` already creates -- the same definition, name
    aside -- is that statement, one without a counterpart is added, and everything else of
    ``current`` is kept as written. So adopting a reply twice changes nothing: an unnamed
    index or foreign key matched nothing, and each click added another copy (two indexes, two
    constraints on one column), as did an unquoted restatement of a mixed-case name.

    Of two statements that create the same, the named one stands, and the current one when
    both are: a foreign key is paired with its metadata by its constraint name (the orphan
    check, a reload's pre-drop), which an unnamed or refolded restatement would lose. Only a
    second copy under the same name, or under none, goes: one of another name that creates
    the same -- MySQL keeps two indexes or foreign keys of one definition -- is kept, and
    said, where it was dropped without a word and no apply path created it; an unnamed one
    (a hand edit's, an earlier adoption's) kept beside it was a second index or constraint.

    Returns the script and what was kept that the reply did not give (an index, a foreign
    key, another statement of the script; never its schema ensure) or gave under another
    name. A reply usually shows only the corrected CREATE TABLE: taken as the whole script,
    it dropped every secondary index and preserved foreign key of the table, which no apply
    path then created."""

    def identity(statement: str) -> "Optional[tuple]":
        # (kind, name, definition) of one of the object's statements, None for any other.
        role = object_statement_role(statement, object_name)
        if role is None:
            return None
        if role[0] in (_TABLE_KIND, _VIEW_KIND):
            return ("CREATE", "", ())
        return (*role, _key_definition(statement))

    pieces = split_sql_statements(current)
    identities = [identity(piece) for piece in pieces]
    names = {(i[0], i[1]) for i in identities if i is not None and i[1]}
    incoming: dict = {}
    for statement in statements:
        ident = identity(statement)
        if ident is None:
            continue
        kind, name, definition = ident
        # By name when the current DDL has its namesake, else by what it creates.
        key = (kind, name) if kind == "CREATE" or (kind, name) in names else (kind, definition)
        incoming.pop(key, None)  # the block's last one wins
        incoming[key] = (name, statement.strip())

    def kept_label(piece: str, ident: "Optional[tuple]") -> Optional[str]:
        if ident is not None and ident[0] in (_INDEX_KIND, _FOREIGN_KEY_ROLE):
            what = "index" if ident[0] == _INDEX_KIND else "foreign key"
            return f"{what} {ident[1]}" if ident[1] else f"an unnamed {what}"
        clean = sql_without_comments(piece).strip()
        if clean and not is_object_scaffolding(clean, object_name):
            return statement_label(clean)
        return None

    merged: list[str] = []
    kept: list[str] = []
    placed: set = set()  # the incoming keys put in, and the names of the pieces put in for them
    for piece, ident in zip(pieces, identities):
        key, stands = None, False
        if ident is not None:
            kind, name, definition = ident
            if (kind, name) in incoming:
                key = (kind, name)
            elif (kind, definition) in incoming:
                key = (kind, definition)
                stands = bool(name) or not incoming[key][0]
                if key in placed and name and (kind, name) not in placed:
                    # What an earlier statement stands for, under another name (MySQL keeps
                    # two indexes or foreign keys of one definition): kept as written. An
                    # unnamed one is a second copy, which goes.
                    key = None
        if key is None:
            merged.append(piece)
            said = True
        elif key in placed:
            continue  # a second copy of a replaced statement would create it twice
        else:
            merged.append(piece if stands else incoming[key][1])
            placed.add(key)
            if name:
                placed.add((kind, name))
            # Kept under its own name where the reply names it otherwise: that is said.
            said = stands and bool(ident[1]) and bool(incoming[key][0])
        label = kept_label(piece, ident) if said else None
        if label:
            kept.append(label)
    created = {(i[0], i[2]) for i in map(identity, merged) if i is not None and i[0] != "CREATE"}
    for key, (_, statement) in incoming.items():
        kind, _, definition = identity(statement)
        if key in placed or (kind, definition) in created:
            continue  # placed, or what it creates is there already
        created.add((kind, definition))
        kinds = [ident[0] if ident else None for ident in map(identity, merged)]
        if key == ("CREATE", ""):
            # After the schema it is created in.
            at = next(
                (
                    i for i, existing in enumerate(merged)
                    if not is_object_scaffolding(existing, object_name)
                ),
                len(merged),
            )
        elif kind == _INDEX_KIND:
            # After the object's last index (or its CREATE); foreign keys come last.
            at = 1 + max(
                (i for i, k in enumerate(kinds) if k is not None and k != _FOREIGN_KEY_ROLE),
                default=len(merged) - 1,
            )
        else:
            at = len(merged)
        merged.insert(at, statement)
    from dsql_migrator.core.sql_text import terminated_sql_statement

    script = "\n\n".join(
        # A comment-only piece (a note at the end of an edit) stays as written.
        terminated_sql_statement(piece) if sql_without_comments(piece).strip() else piece
        for piece in merged
        if piece.strip()
    )
    return script, kept


def identity_from_ddl(table: TableDef, target_ddl: str) -> bool:
    """Return whether the stored target DDL made the AUTO_INCREMENT key an identity.

    The picker keeps NO separate state: it infers the chosen strategy back out of the
    stored (edited) target DDL. This reports ``True`` when that DDL declares the
    table's auto-increment column as ``GENERATED ... AS IDENTITY`` (the
    IDENTITY_WITH_CACHE result), so the picker renders as "Server-generated
    (IDENTITY)". Returns ``False`` when there is no stored DDL, no auto-increment
    column, or the column is a plain key -- i.e. the DDL itself is the single source
    of truth for the picker's state (build -> render -> infer round-trips).

    Detection is a text scan of the column's clause, not a sqlglot parse, ON PURPOSE:
    the converter injects the identity as a raw ``GENERATED ... AS IDENTITY (CACHE
    65536)`` string precisely because sqlglot cannot render/parse the CACHE clause,
    so parsing this DDL would raise on exactly the string we need to detect.
    """
    column = table.auto_increment_column
    if not column or not target_ddl:
        return False
    clause = _column_clause(_first_create_table(target_ddl), column)
    if clause is None:
        return False
    upper = clause.upper()
    return "GENERATED" in upper and "AS IDENTITY" in upper


def _first_create_table(target_ddl: str) -> str:
    """Return the CREATE TABLE statement from a full target script (or the script).

    The stored DDL is the whole script (CREATE TABLE + CREATE INDEX ...), but the
    single-statement parsers here read one statement -- isolate the CREATE TABLE.
    """
    from dsql_migrator.core.sql_text import sql_without_comments

    return next(
        (
            stmt
            for stmt in split_sql_statements(target_ddl)
            if sql_without_comments(stmt).strip().upper().split("(", 1)[0].startswith("CREATE TABLE")
        ),
        target_ddl,
    )


def _column_clause(create_table_ddl: str, column: str) -> Optional[str]:
    """Return the single column-definition clause for ``column`` from a CREATE TABLE.

    The clause is the element of the column list -- split at its top-level commas, so a
    one-line or hand-laid-out CREATE reads like the converter's one-column-per-line one --
    whose leading identifier is the column (quoted, with ``""`` escapes and spaces, or
    bare). Matching on the leading identifier -- not a substring -- keeps a column merely
    NAMED like another from being confused for it. Returns ``None`` when no such column is
    found.
    """
    for element in _column_list_elements(create_table_ddl):
        if _leading_identifier(element) == column:
            return element
    return None


def _leading_identifier(element: str) -> Optional[str]:
    """The identifier an element of a column list starts with (unquoted as written)."""
    text = element.lstrip()
    if text.startswith('"'):
        end = 1
        while end < len(text):
            if text[end] == '"':
                if text[end + 1:end + 2] == '"':
                    end += 2
                    continue
                return text[1:end].replace('""', '"')
            end += 1
        return None
    token = text.split(None, 1)[0] if text.split() else ""
    return token or None


def _column_list_elements(create_table_ddl: str) -> "list[str]":
    """The comma-separated elements of a CREATE TABLE's parenthesised column list (column
    definitions and table constraints), comments dropped and whitespace collapsed -- the
    list's '(' found outside quoted names, strings and comments, and no comma inside
    parentheses, a string literal, a quoted identifier or a comment splitting it."""
    from dsql_migrator.core.sql_text import _sql_skip

    text = create_table_ddl or ""
    elements: list[str] = []
    current: list[str] = []
    depth = 0
    started = False
    i = 0
    while i < len(text):
        ch = text[i]
        skipped = _sql_skip(text, i) if ch in "'\"$-/eE" else None
        if skipped is not None:
            kind, end = skipped
            if started and kind != "comment":
                current.append(text[i:end])
            elif started:
                current.append(" ")
            i = end
            continue
        if not started:
            if ch == "(":
                started = True
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            if depth == 0:
                elements.append("".join(current))
                break
            depth -= 1
        elif ch == "," and depth == 0:
            elements.append("".join(current))
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    return [" ".join(element.split()) for element in elements if element.strip()]


def composite_leading_from_ddl(table: TableDef, target_ddl: str) -> Optional[str]:
    """Infer the composite leading column from a stored target DDL, if composite.

    The picker keeps no separate state: it reads the object's stored (edited)
    target DDL and, when that DDL's parsed primary key is the composite
    ``(leading, original_pk...)``, returns the leading column so the picker renders
    as "Composite key" with the dropdown preset. Returns ``None`` when the DDL's
    key equals the source key (not composite) or cannot be parsed -- i.e. the
    single source of truth for the picker's state is the DDL itself.
    """
    # The stored DDL is the full script (CREATE TABLE + CREATE INDEX ...), but the
    # PK parser reads a single statement -- isolate the CREATE TABLE first.
    create_table = next(
        (
            stmt
            for stmt in edited_statements(target_ddl)
            if stmt.upper().split("(", 1)[0].startswith("CREATE TABLE")
        ),
        target_ddl,
    )
    target_pk = parse_target_primary_key(create_table)
    source_pk = list(table.primary_key)
    if target_pk and target_pk != source_pk and target_pk[1:] == source_pk:
        return target_pk[0]
    return None


# ---------------------------------------------------------------------------
# SQL statement splitting
# ---------------------------------------------------------------------------


def split_sql_statements(text: str) -> list[str]:
    """Split ``text`` into individual SQL statements on ``;`` (empties dropped), never on
    one inside a string literal, quoted identifier, dollar body or comment.

    A single edited target-DDL script is split here so each statement is applied
    as one DDL in its own transaction (Property 2) by the apply path.
    """
    from dsql_migrator.core.sql_text import sql_statement_spans

    return [
        text[start:end].strip()
        for start, end in sql_statement_spans(text)
        if text[start:end].strip()
    ]


# A line that starts a statement of its own. A WITH only as a CTE list (``WITH x AS (``),
# never ``WITH (fillfactor = 70)`` or ``WITH CHECK OPTION``; an ALTER / DROP / CREATE only of
# an object kind, never the ``ALTER COLUMN`` / ``DROP CONSTRAINT`` action of an ALTER TABLE;
# a psql meta-command (``\d``); and the statements no other one goes on with at a line's
# start (ANALYZE, COMMIT, START TRANSACTION, ...). The list is closed: a statement of a kind
# it leaves out (ALTER DEFAULT PRIVILEGES, RESET, COPY, LOCK, ALTER MATERIALIZED VIEW, ...)
# glued after one that lost its ';' is still taken with it, and Schema Apply refuses the
# whole (42601) -- each word added is also a column name a line of a view may start with.
_KIND_WORDS = (
    r"(?:TABLE|VIEW|INDEX|SCHEMA|SEQUENCE|FUNCTION|PROCEDURE|TRIGGER|TYPE|DOMAIN|EXTENSION"
    r"|ROLE|USER|POLICY|RULE|DATABASE)"
)
_CTE_HEAD = (
    rf"WITH\s+(?:RECURSIVE\s+)?{_ANY_NAME_PART}\s*(?:\(\s*\)\s*)?"
    r"AS\s*(?:(?:NOT\s+)?MATERIALIZED\s*)?\("
)
_STATEMENT_START_RE = re.compile(
    r"(?:SELECT|UPDATE|GRANT|REVOKE|EXPLAIN|ANALYZE|VACUUM|SHOW|TRUNCATE|CALL|REFRESH|COMMIT"
    r"|ROLLBACK|INSERT\s+INTO|DELETE\s+FROM|MERGE\s+INTO|COMMENT\s+ON|START\s+TRANSACTION"
    rf"|(?:CREATE\s+{_CREATE_MODIFIERS}|ALTER\s+|DROP\s+){_KIND_WORDS})(?![\w$])|{_CTE_HEAD}|\\",
    re.IGNORECASE,
)
# A line that starts a statement of its own only after one that cannot go on with it. BEGIN,
# SET, VALUES, TABLE and DO also go on a function (BEGIN ATOMIC, SET search_path, RETURNS
# TABLE), an UPDATE (SET), an INSERT (VALUES, ON CONFLICT ... DO) or a rule (DO INSTEAD), so
# they are cut only after the object's own CREATE TABLE, VIEW or INDEX or foreign key, which
# takes none of them -- but the SET of a foreign key's ON DELETE / ON UPDATE, or of an ALTER
# COLUMN in its ALTER TABLE. END also ends a CASE: it is cut only where no CASE is open.
_CLAUSE_START_RE = re.compile(r"(?:END|BEGIN|SET|VALUES|TABLE|DO)(?![\w$])", re.IGNORECASE)
_OWN_STATEMENT_HEAD_RE = re.compile(
    rf"CREATE\s+{_CREATE_MODIFIERS}(?:TABLE|VIEW|INDEX)(?![\w$])", re.IGNORECASE
)
_TAKES_SET_RE = re.compile(
    rf"(?:ON\s+(?:DELETE|UPDATE)|ALTER\s+(?:COLUMN\s+)?{_ANY_NAME_PART})$", re.IGNORECASE
)
# A quoted name, blanked before CASE and END are read: a column named "end" ends no CASE.
_QUOTED_NAME_RE = re.compile(r'"(?:[^"]|"")*"?')
# CASE or END as the keyword. One after '.' or AS is a name (``e.end``, ``AS end``): it is
# read with what precedes it (group 1) and left out.
_CASE_OR_END_RE = re.compile(
    r"(\.\s*|(?<![\w$])AS\s+)?(?<![\w$])(CASE|END)(?![\w$])", re.IGNORECASE
)
# The last word or sign of a statement that goes on with the next line's SELECT, INSERT, ...:
# a view's AS, a set operator, a rule's DO [INSTEAD], a MERGE's THEN, a trigger's BEFORE /
# OR events, FOR UPDATE, ON UPDATE of a foreign key, a list's comma, a GRANT's or REVOKE's
# privilege, a clause still without what it takes -- ``AS SELECT`` then a column named update
# on the next line, which was cut off and left a view of no columns; a foreign key's
# REFERENCES before the table it names.
_GOES_ON_RE = re.compile(
    r"(?:(?<![\w$])(?:AS|UNION|INTERSECT|EXCEPT|ALL|DISTINCT|DO|INSTEAD|ALSO|THEN|ELSE|BEFORE"
    r"|AFTER|OF|OR|AND|NOT|FOR|ON|IN|IS|BY|BEGIN|ATOMIC|EXPLAIN|ANALYZE|VERBOSE|GRANT|REVOKE"
    r"|SELECT|FROM|WHERE|JOIN|HAVING|SET|RETURNING|INTO|USING|WHEN|CASE|REFERENCES)"
    r"|[,(=<>+\-*/|:])$",
    re.IGNORECASE,
)
# A SELECT's DISTINCT ON list, which its columns follow: ``SELECT DISTINCT ON (c.id)`` then
# a column named call on the next line was cut off, and the view adopted had no columns.
_DISTINCT_ON_RE = re.compile(r"(?<![\w$])DISTINCT\s+ON\s*\(\s*\)$", re.IGNORECASE)
# One word of a statement's level text as its last words are read (a SET's ALTER COLUMN
# name, a DISTINCT ON list): a run of anything but space, a quoted name whole.
_WORD_RE = re.compile(r'(?:"(?:[^"]|"")*"?|[^\s"])+')
_NON_SPACE_RE = re.compile(r"\S")
# A head still waiting for what the next line starts: an ALTER's action (``ALTER TABLE t`` /
# ``ALTER type SET NOT NULL``, COLUMN left out), an INSERT's query, what an EXPLAIN explains,
# the GRANT or REVOKE of an ALTER DEFAULT PRIVILEGES.
_OPEN_HEAD_RE = re.compile(
    rf"ALTER\s+(?:MATERIALIZED\s+|FOREIGN\s+)?\w+\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?{_ANY_NAME}"
    rf"|INSERT\s+INTO\s+{_ANY_NAME}(?:\s+AS\s+{_ANY_NAME_PART})?(?:\s*\(\s*\))?"
    r"(?:\s+OVERRIDING\s+\w+\s+VALUE)?"
    r"|EXPLAIN(?:\s+(?:ANALYZE|VERBOSE))*(?:\s*\(\s*\))?"
    rf"|ALTER\s+DEFAULT\s+PRIVILEGES(?:\s+(?:FOR\s+(?:ROLE|USER)|IN\s+SCHEMA)\s+{_ANY_NAME}"
    rf"(?:\s*,\s*{_ANY_NAME})*)*",
    re.IGNORECASE,
)
_CTE_HEAD_RE = re.compile(rf"(?<![\w$]){_CTE_HEAD}", re.IGNORECASE)
_INDENT_RE = re.compile(r"[ \t]*")
# What a CTE list goes on to: once one follows it at its level, the list has its statement.
_CTE_STATEMENT_RE = re.compile(
    r"(?<![\w$])(?:SELECT|INSERT|UPDATE|DELETE|VALUES|TABLE|MERGE)(?![\w$])", re.IGNORECASE
)


def _statement_level(statement: str) -> "tuple[str, list[int]]":
    """``statement`` as read at its own level, every offset kept: a string or dollar literal
    one value (``0``), a comment blank, what is inside brackets blank; and where each line at
    that level starts."""
    from dsql_migrator.core.sql_text import _SQL_SPECIAL_RE, _sql_skip

    out: list[str] = []
    starts: list[int] = []
    depth = 0
    at = 0
    while at < len(statement):
        char = statement[at]
        skipped = _sql_skip(statement, at) if _SQL_SPECIAL_RE.match(statement, at) else None
        if skipped is not None:
            kind, end = skipped
            if depth or kind == "comment":
                out.append(" " * (end - at))
            elif kind == "quoted":
                out.append(statement[at:end].replace("\n", " "))
            else:
                out.append("0".ljust(end - at))
            at = end
            continue
        if char == "(":
            depth += 1
        out.append(char if depth == 0 or (depth == 1 and char in "()") or char == "\n" else " ")
        if char == ")":
            depth = max(depth - 1, 0)
        elif char == "\n" and depth == 0:
            starts.append(at + 1)
        at += 1
    return "".join(out), starts


def _case_keywords(level: str) -> "list[tuple[int, bool]]":
    """The CASE (``True``) and END (``False``) keywords of a statement's level text
    (:func:`_statement_level`), in order by offset -- never a name spelled so: ``"e"."end"``,
    ``e.end``, ``AS end`` or ``"end date"``, a column MySQL lets a table have. Read as an
    END, it cut the view whose CASE ends on a line of its own at that line."""
    blank = _QUOTED_NAME_RE.sub(lambda name: " " * len(name.group(0)), level)
    return [
        (keyword.start(2), keyword.group(2).upper() == "CASE")
        for keyword in _CASE_OR_END_RE.finditer(blank)
        if keyword.group(1) is None
    ]


class _GluedPiece:
    """The statement :func:`split_glued_statements` is reading, each fact about it read once
    as its lines go by: reading the text so far again at every line that starts with END,
    SET, VALUES, ... made a reply of many CASE columns (an END line each) quadratic, on the
    UI's event loop."""

    def __init__(self, level: str, begin: int) -> None:
        self.level = level
        self.begin = begin
        first = _NON_SPACE_RE.search(level, begin)
        self.first = first.start() if first is not None else len(level)
        own = _OWN_STATEMENT_HEAD_RE.match(level, self.first)
        self._own_end = own.end() if own is not None else None
        self._cases = 0  # its CASEs still without their END
        self._foreign_key: Optional[bool] = None  # known once its first bracket is read
        self._bracket_read_to = begin
        self._query: Optional[bool] = None  # whether its last CTE list has its query
        self._cte_read_to = self._query_read_to = begin

    def read_keyword(self, opens: bool) -> None:
        """One of its CASE (``opens``) and END keywords, in order: an END closes the CASE
        last opened, and one with none open closes nothing."""
        self._cases = self._cases + 1 if opens else max(self._cases - 1, 0)

    def takes(self, word: str, end: int, tail: str) -> bool:
        """Whether its text up to ``end`` -- ``tail`` its last words -- goes on with a line
        that starts with ``word`` (:data:`_CLAUSE_START_RE`)."""
        if word == "END":
            return self._cases > 0
        if word == "SET" and _TAKES_SET_RE.search(tail):
            return True
        own = self._own_end is not None and self._own_end <= end
        return not (own or self._is_foreign_key(end))

    def _is_foreign_key(self, end: int) -> bool:
        # By its head up to its first bracket, where FOREIGN KEY's columns open.
        if self._foreign_key is None:
            bracket = self.level.find("(", self._bracket_read_to, end)
            self._bracket_read_to = end
            if bracket < 0:
                return False
            self._foreign_key = _is_foreign_key_ddl(self.level[self.begin : bracket])
        return self._foreign_key

    def awaits_query(self, end: int) -> bool:
        """Whether its text up to ``end`` is a CTE list still without the query it goes on
        to (:data:`_CTE_STATEMENT_RE`)."""
        for cte in _CTE_HEAD_RE.finditer(self.level, self._cte_read_to, end):
            self._query, self._query_read_to = False, cte.end()
        self._cte_read_to = end
        if self._query is False:
            query = _CTE_STATEMENT_RE.search(self.level, self._query_read_to, end)
            self._query, self._query_read_to = query is not None, end
        return self._query is False


def split_glued_statements(statement: str) -> list[str]:
    """``statement`` -- one ``;``-split statement of an AI reply -- cut where a line starts a
    statement of its own. A statement that lost its ``;`` ran into the next one, and the
    whole of it was taken as the object's CREATE: Schema Apply failed it (42601), and Full
    Load's applied DDL carried the verification SELECT or the side table's CREATE after it.

    A line is a cut only at the statement's own level (outside brackets, literals and
    comments), and only when what comes before it is whole: not a view's ``AS``, a set
    operator, the CTE list of the query that follows, an ALTER still without its action, a
    SELECT still without its columns, and the like. A comment just above the line goes with
    it. The text is read once, in linear time (:class:`_GluedPiece`)."""
    level, starts = _statement_level(statement)
    keywords = _case_keywords(level)
    words = [word.start() for word in _WORD_RE.finditer(level)]
    cuts: list[int] = []
    piece = _GluedPiece(level, 0)
    read = 0  # the CASE and END keywords read
    for start in starts:
        head = _INDENT_RE.match(level, start).end()
        clause = _CLAUSE_START_RE.match(level, head)
        if clause is None and not _STATEMENT_START_RE.match(level, head):
            continue
        while read < len(keywords) and keywords[read][0] < head:
            piece.read_keyword(keywords[read][1])
            read += 1
        end = head  # where the statement so far ends, the space before the line left out
        while end > piece.begin and level[end - 1].isspace():
            end -= 1
        if end == piece.begin:
            continue
        # Its last four words: what takes a SET, a DISTINCT ON list.
        tail = level[max(piece.begin, words[max(bisect.bisect_left(words, end) - 4, 0)]) : end]
        if (
            (clause is not None and piece.takes(clause.group(0).upper(), end, tail))
            or _GOES_ON_RE.search(level[max(piece.begin, end - 16) : end])
            or _DISTINCT_ON_RE.search(tail)
            or _OPEN_HEAD_RE.fullmatch(level, piece.first, end)
            or piece.awaits_query(end)  # the query of a CTE list
        ):
            continue
        cut = start
        while cut > piece.begin:
            above = max(level.rfind("\n", piece.begin, cut - 1) + 1, piece.begin)
            if level[above:cut].strip():
                break
            cut = above
        cuts.append(cut)
        piece = _GluedPiece(level, cut)
    bounds = [0, *cuts, len(statement)]
    return [statement[a:b].strip() for a, b in zip(bounds, bounds[1:]) if statement[a:b].strip()]


def _join_sql_statements(statements: Sequence[str]) -> str:
    """Join statements into one script: each ``;``-terminated, blank-line separated.

    Mirrors ``render_target_ddl``'s formatting so a split-then-join round-trips a
    rendered target script unchanged (modulo whitespace).
    """
    from dsql_migrator.core.sql_text import terminated_sql_statement

    return "\n\n".join(terminated_sql_statement(s) for s in statements if s.strip())


def split_target_ddl_fk_sections(script: str) -> tuple[str, str]:
    """Split a rendered target-DDL script into ``(non_fk, fk)`` sub-scripts.

    Separates the editable ``CREATE SCHEMA/TABLE/INDEX`` statements from the
    deferred post-load ``ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`` statements
    so the UI can present the foreign keys in their own read-only "applied after
    Full Load" section instead of intermixed in the editable box (which reads as
    "Schema Apply will create these FKs" when it deliberately will not). Statement
    order within each bucket is preserved and each statement is ``;``-terminated;
    either sub-script may be empty (a table with no FKs -> empty ``fk``). This is a
    PRESENTATION split only: recombining with :func:`join_target_ddl_fk_sections`
    yields the same statement set, so the STORED edited DDL stays a full script and
    the apply path (which already excludes the FK ALTERs, see
    :func:`override_apply_objects`) is unaffected.
    """
    statements = split_sql_statements(script)
    fk = [stmt for stmt in statements if _is_foreign_key_ddl(stmt)]
    # Nothing to separate: return the script VERBATIM so a table with no foreign
    # keys (the common case) is displayed/edited exactly as before -- re-splitting
    # and re-joining would reflow whitespace and could mangle a trailing comment.
    if not fk:
        return script, ""
    non_fk = [stmt for stmt in statements if not _is_foreign_key_ddl(stmt)]
    return _join_sql_statements(non_fk), _join_sql_statements(fk)


def join_target_ddl_fk_sections(non_fk: str, fk: str) -> str:
    """Recombine an edited non-FK script with its read-only FK section into one
    stored target-DDL script (blank-line separated), so ``edited_target_ddls`` stays
    a full script and the deferred FKs still reach the post-load pass. The inverse
    of :func:`split_target_ddl_fk_sections`; an empty part is dropped.
    """
    from dsql_migrator.core.sql_text import terminated_sql_script

    parts = [part.strip() for part in (non_fk, fk) if part.strip()]
    if len(parts) == 2:
        # An edit without its final ';' would glue the first deferred FK ALTER onto it.
        parts[0] = terminated_sql_script(parts[0])
    return "\n\n".join(parts)
