# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The exclusion RULE LANGUAGE: the text an operator types to choose what to exclude.

One rule per line; blank lines ignored; ``#`` starts a comment outside double quotes::

    line       = [ rule ] [ comment ]
    rule       = action WS subject
    action     = "exclude" | "include"                   ; case-insensitive
    subject    = "table" WS table_ref                    ; table rule
               | "type:" type_name { "," type_name }     ; column rule by source type
               | column_ref                              ; column rule by name
    table_ref  = part "." part                           ; schema.table
    column_ref = part "." part "." part                  ; schema.table.column
    part       = bare | quoted
    bare       = 1*( char - ( WS | "." | '"' | "#" | "," ) )   ; "*" inside = wildcard
    quoted     = '"' { char - '"' | '""' } '"'           ; literal: "*" is plain
    type_name  = bare | quoted                           ; no "." and no "*" when bare

This module PARSES: every line becomes a :class:`ParsedRule` or a list of
:class:`RuleError` (E1, syntax), each error carrying one-click fixes as WHOLE rewritten
lines, so the UI's fix is a plain string swap. Every name records its span in the line, so
a later fix can rewrite one name only. Matching, precedence and eligibility build on top.

Pure: no UI, no database, no AWS.
"""

from __future__ import annotations

import difflib
import re
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Callable, Collection, Mapping, Optional, Sequence

from dsql_migrator.core.assessor import _base_type
from dsql_migrator.core.column_exclusion import (
    B1,
    B2,
    B3,
    B4,
    T2,
    T3,
    ExclusionContext,
    ExclusionFinding,
    column_exclusion_verdict,
    column_exclusion_warnings,
    missing_target_columns,
    table_exclusion_findings,
)
from dsql_migrator.core.models import SourceType

if TYPE_CHECKING:
    from dsql_migrator.core.models import ColumnDef, TableDef

_WS = " \t"
# Characters a BARE name part cannot hold (the grammar's bare production).
_BARE_STOP = frozenset(' \t."#,')
_ACTIONS = ("exclude", "include")
_TYPE_PREFIX = "type:"


@dataclass(frozen=True)
class Suggestion:
    """A one-click fix: ``line_text`` is the WHOLE rewritten line."""

    label: str
    line_text: str


@dataclass(frozen=True)
class RuleError:
    """One problem on one line. ``name_index`` is the failing name's position in a
    ``type:`` list (``None`` for every other error). A GLOBAL error (``line`` 0, not tied
    to a line) offers suggestions whose ``line_text`` is a line to APPEND."""

    code: str
    line: int
    message: str
    suggestions: "tuple[Suggestion, ...]" = ()
    name_index: Optional[int] = None


@dataclass(frozen=True)
class RulePart:
    """One name part (or type name): its unquoted ``text``, whether it was quoted, and its
    ``(start, end)`` span in the ORIGINAL line, quotes included."""

    text: str
    quoted: bool
    span: "tuple[int, int]"

    @property
    def wildcard(self) -> bool:
        """``*`` is a wildcard only inside a BARE part; quoted, it is a literal ``*``."""
        return not self.quoted and "*" in self.text


@dataclass(frozen=True)
class ParsedRule:
    """One rule: ``kind`` is ``table`` (2 ``parts``), ``column`` (3 ``parts``) or ``type``
    (``type_names``). ``rule_text`` is the line before its comment, unstripped."""

    line: int
    action: str
    kind: str
    parts: "tuple[RulePart, ...]"
    type_names: "tuple[RulePart, ...]"
    comment: str
    rule_text: str


class _E1(Exception):
    def __init__(self, message: str, suggestions=(), name_index: Optional[int] = None):
        super().__init__(message)
        self.message = message
        self.suggestions = tuple(suggestions)
        self.name_index = name_index


def split_rule_comment(line: str) -> "tuple[str, str]":
    """``(rule_text, comment)``: ``#`` starts a comment only OUTSIDE double quotes (``""``
    inside quotes is one ``"``). An unbalanced quote keeps the whole line as rule text."""
    in_quote = False
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == '"':
            if in_quote and line[i + 1 : i + 2] == '"':
                i += 2
                continue
            in_quote = not in_quote
        elif ch == "#" and not in_quote:
            return line[:i], line[i + 1 :]
        i += 1
    return line, ""


def _skip_ws(text: str, i: int) -> int:
    while i < len(text) and text[i] in _WS:
        i += 1
    return i


def _read_quoted(text: str, i: int) -> "tuple[str, int]":
    """``text[i] == '"'``: the unquoted value and the index after the closing quote."""
    out: list[str] = []
    j = i + 1
    while j < len(text):
        if text[j] == '"':
            if text[j + 1 : j + 2] == '"':
                out.append('"')
                j += 2
                continue
            return "".join(out), j + 1
        out.append(text[j])
        j += 1
    raise _E1('Unbalanced quote: close the quoted name with ".')


def _read_part(text: str, i: int) -> "tuple[RulePart, int]":
    if i < len(text) and text[i] == '"':
        value, end = _read_quoted(text, i)
        if end < len(text) and text[end] not in _WS and text[end] != ".":
            raise _E1(f'Unexpected character after a quoted name: "{text[end]}".')
        return RulePart(value, True, (i, end)), end
    j = i
    while j < len(text) and text[j] not in _BARE_STOP:
        j += 1
    return RulePart(text[i:j], False, (i, j)), j


def _read_ref(text: str, i: int) -> "tuple[list[RulePart], int]":
    """Dot-separated parts from ``i``; an empty part is E1."""
    parts: list[RulePart] = []
    while True:
        part, j = _read_part(text, i)
        if not part.quoted and part.text == "":
            if j < len(text) and text[j] == ",":
                raise _E1(f'Unexpected text after the rule: "{text[j:].strip()}". Use # for a comment.')
            raise _E1('An empty name part: remove the extra ".".')
        parts.append(part)
        if j < len(text) and text[j] == ".":
            i = j + 1
            continue
        return parts, j


def _replace(line: str, start: int, end: int, new: str) -> str:
    return line[:start] + new + line[end:]


def rule_part_token(name: str) -> str:
    """A name part as the rule text writes it: bare when the bare production holds and it
    has no ``*`` (bare ``*`` would be a wildcard) and cannot be read as a keyword; quoted
    otherwise, with ``"`` doubled. ``parse`` of the token returns ``name`` exactly."""
    if (
        name
        and not any(ch in _BARE_STOP or ch == "*" for ch in name)
        and not name.lower().startswith(_TYPE_PREFIX)
    ):
        return name
    return '"' + name.replace('"', '""') + '"'


def type_name_token(key: str) -> str:
    """A type key as the rule text writes it (and as every surface shows it, so what is
    shown can be pasted back): bare when it fits the bare production and has no ``*``;
    quoted otherwise, ``"`` doubled. ``character varying`` -> ``"character varying"``."""
    if key and not any(ch in _BARE_STOP or ch == "*" for ch in key):
        return key
    return '"' + key.replace('"', '""') + '"'


def _parse_types(line: str, i: int) -> "tuple[RulePart, ...]":
    """The ``type:`` list starting at ``i`` (just after the prefix)."""
    if i >= len(line) or line[i] in _WS:
        rest = line[i:].strip()
        if not rest:
            raise _E1("Empty type list: write type:<type>[,<type>...].")
        raise _E1("No spaces in a type list.", [Suggestion("Remove the spaces", _despace_types(line, i))])
    names: list[RulePart] = []
    while True:
        if i >= len(line) or line[i] in _WS:
            if names:
                raise _E1("A trailing comma: remove it or add a type.")
            raise _E1("Empty type list: write type:<type>[,<type>...].")
        if line[i] == ",":
            raise _E1('An empty type name: remove the extra ",".')
        if line[i] == '"':
            value, end = _read_quoted(line, i)
            part = RulePart(value, True, (i, end))
        else:
            j = i
            while j < len(line) and line[j] not in ' \t"#,':
                j += 1
            part = RulePart(line[i:j], False, (i, j))
            index = len(names)
            if "." in part.text:
                fixed = _replace(line, i, j, type_name_token(part.text))
                raise _E1(
                    f'"{part.text}" contains "." -- quote a schema-qualified type: '
                    f'type:"{part.text}".',
                    [Suggestion(f'Use type:"{part.text}"', fixed)],
                    index,
                )
            if "*" in part.text:
                fixed = _replace(line, i, j, type_name_token(part.text))
                raise _E1(
                    'No wildcard in type names: quote it to match a type literally named '
                    f'"{part.text}".',
                    [Suggestion(f"Use type:{type_name_token(part.text)}", fixed)],
                    index,
                )
            end = j
        names.append(part)
        i = end
        if i < len(line) and line[i] == ",":
            i += 1
            if i < len(line) and line[i] in _WS:
                raise _E1("No spaces in a type list.", [Suggestion("Remove the spaces", _despace_types(line, i - 1))])
            continue
        if i < len(line) and line[i] in _WS and _skip_ws(line, i) < len(line) and line[_skip_ws(line, i)] == ",":
            raise _E1("No spaces in a type list.", [Suggestion("Remove the spaces", _despace_types(line, i))])
        return tuple(names)


def _despace_types(line: str, start: int) -> str:
    """``line`` with the whitespace right after ``type:`` and around each ``,`` removed
    (outside quotes); ``start`` is any index inside the type list."""
    prefix_end = line.lower().rfind(_TYPE_PREFIX, 0, start + 1) + len(_TYPE_PREFIX)
    tail = line[prefix_end:]
    out: list[str] = []
    in_quote = False
    for k, ch in enumerate(tail):
        if ch == '"':
            in_quote = not in_quote
        if not in_quote and ch in _WS:
            nxt = tail[k:].lstrip(_WS)[:1]
            if not out or out[-1] == "," or nxt == ",":
                continue
        out.append(ch)
    return line[:prefix_end] + "".join(out)


def _parse_line(number: int, line: str) -> ParsedRule:
    rule_text, comment = split_rule_comment(line)
    i = _skip_ws(rule_text, 0)
    j = i
    while j < len(rule_text) and rule_text[j] not in _WS:
        j += 1
    word = rule_text[i:j]
    if word.lower() not in _ACTIONS:
        close = difflib.get_close_matches(word.lower(), _ACTIONS, n=1, cutoff=0.6)
        raise _E1(
            f'Unknown first word "{word}": a rule starts with exclude or include.',
            [Suggestion(f'Use "{close[0]}"', _replace(line, i, j, close[0]))] if close else [],
        )
    action = word.lower()
    k = _skip_ws(rule_text, j)
    if k >= len(rule_text):
        raise _E1(
            f'Missing what to {action}: write "{action} schema.table.column", '
            f'"{action} table schema.table" or "{action} type:<type>".'
        )
    # "table" is the keyword only when followed by whitespace; a part named table.x is not.
    m = k
    while m < len(rule_text) and rule_text[m] not in _WS:
        m += 1
    kind = "column"
    if rule_text[k:m].lower() == "table" and m < len(rule_text):
        kind = "table"
        k = _skip_ws(rule_text, m)
    elif rule_text[k : k + len(_TYPE_PREFIX)].lower() == _TYPE_PREFIX:
        names = _parse_types(rule_text, k + len(_TYPE_PREFIX))
        end = names[-1].span[1]
        _no_trailing(line, rule_text, end)
        return ParsedRule(number, action, "type", (), names, comment, rule_text)
    parts, end = _read_ref(rule_text, k)
    _no_trailing(line, rule_text, end)
    names = ".".join(rule_part_token(p.text) if p.quoted else p.text for p in parts)
    if kind == "table":
        if len(parts) == 3:
            raise _E1(
                f'"table" takes schema.table (two parts); {names} names a column.',
                [Suggestion(f'Use "{action} {names}"', _replace(line, j, k, " "))],
            )
        if len(parts) != 2:
            raise _E1('"table" takes schema.table (two parts).')
    else:
        if len(parts) == 2:
            fixed = _replace(line, k, k, "table ")
            raise _E1(
                f'Two parts: write "{action} table {names}" for the table, or '
                f"{names}.<column> for a column.",
                [Suggestion(f'Use "{action} table {names}"', fixed)],
            )
        if len(parts) == 1:
            raise _E1("A column rule names schema.table.column (three parts).")
        if len(parts) > 3:
            raise _E1(
                f"{len(parts)} parts: a name has at most three parts (schema.table.column); "
                'quote a name that contains ".".'
            )
    return ParsedRule(number, action, kind, tuple(parts), (), comment, rule_text)


def _no_trailing(line: str, rule_text: str, end: int) -> None:
    """E1 for any text after the subject, with a fix that turns it into a comment (an
    existing comment is kept after it)."""
    text = rule_text[end:].strip()
    if text:
        kept = line[len(rule_text):]  # "" or "#<comment>"
        fixed = line[:end].rstrip() + " # " + text + (" " + kept if kept else "")
        raise _E1(
            f'Unexpected text after the rule: "{text}". Use # for a comment.',
            [Suggestion("Make it a comment", fixed)],
        )


def parse_rules(text: str) -> "tuple[dict[int, ParsedRule], dict[int, list[RuleError]]]":
    """Parse rule text into ``({line: ParsedRule}, {line: [RuleError]})``, lines 1-based.

    Blank and comment-only lines produce neither. Any error on a line means no rule for it.
    """
    rules: dict[int, ParsedRule] = {}
    errors: dict[int, list[RuleError]] = {}
    for number, raw in enumerate((text or "").split("\n"), start=1):
        line = raw[:-1] if raw.endswith("\r") else raw
        rule_text, _comment = split_rule_comment(line)
        if not rule_text.strip():
            continue
        try:
            rules[number] = _parse_line(number, line)
        except _E1 as exc:
            errors[number] = [
                RuleError("E1", number, exc.message, exc.suggestions, exc.name_index)
            ]
    return rules, errors


# --- the type: normalizer ------------------------------------------------------------


def _pg_reduce(text: str) -> str:
    """The PostgreSQL type key of a ``format_type`` string (or of a typed name).

    Every parenthesized typmod OUTSIDE double quotes is removed wherever it sits (a
    negative scale included: ``numeric(5,-2)``); whitespace outside quotes collapses; a
    trailing ``[]`` is kept as a suffix; the rest splits at ``.`` outside quotes and each
    part is unquoted (``""`` is one ``"``) with its case KEPT, then re-joined.
    """
    out: list[str] = []
    in_quote = False
    depth = 0
    for ch in text:
        if ch == '"' and depth == 0:
            in_quote = not in_quote
            out.append(ch)
        elif in_quote:
            out.append(ch)
        elif ch == "(":
            depth += 1
        elif ch == ")" and depth:
            depth -= 1
        elif depth:
            continue
        elif ch in _WS or ch in "\r\n":
            if out and out[-1] != " ":
                out.append(" ")
        else:
            out.append(ch)
    reduced = "".join(out).strip()
    # A typmod before [] leaves "name []": the space goes with the typmod.
    reduced = re.sub(r" +(?=\[\])", "", reduced)
    suffix = ""
    if reduced.endswith("[]"):
        reduced, suffix = reduced[:-2].rstrip(), "[]"
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    quoted = False
    while i < len(reduced):
        ch = reduced[i]
        if ch == '"':
            if quoted and reduced[i + 1 : i + 2] == '"':
                buf.append('"')
                i += 2
                continue
            quoted = not quoted
        elif ch == "." and not quoted:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    parts.append("".join(buf))
    return ".".join(parts) + suffix


def rule_type_name(column: "ColumnDef", source_type: SourceType) -> str:
    """The ONE type key a ``type:`` rule matches, suggests and displays for ``column``.

    * MySQL: the assessor's ``_base_type`` (lower-cased, cut at ``(`` or a space), so
      ``int unsigned`` is ``int`` and ``decimal(10,2) unsigned`` is ``decimal`` -- the same
      cut the oversized-LOB classifier uses.
    * PostgreSQL (``column.mysql_type`` holds ``format_type``): :func:`_pg_reduce` -- every
      typmod outside quotes removed, ``[]`` kept, multi-word names kept, NO lower-casing
      (``format_type`` quotes every name that is not already lower case, so lower-casing
      could only merge case twins: ``"MyType"`` and ``"MYTYPE"`` are distinct keys).

    Two accepted ambiguities: a ``.`` inside one type's own name is not told apart from a
    schema separator; and unquoting gives a user type named like a built-in SQL name the
    built-in's key (``CREATE DOMAIN public."integer"`` prints ``"integer"`` -> ``integer``),
    so one ``type:integer`` line matches both and the per-type count sums them -- a column
    rule separates them. Deliberately NOT ``converter_postgres.normalize_pg_base_type`` (it
    lower-cases and misses a negative scale) and NOT the ``_base_type`` use inside
    ``is_pg_oversized_lob_type``, which only needs the LOB bases.
    """
    declared = column.mysql_type or ""
    if source_type is SourceType.POSTGRES:
        return _pg_reduce(declared)
    return _base_type(declared)


def reduce_typed_name(typed: str, source_type: SourceType) -> str:
    """A TYPED type name reduced the same way as a column's type (step 2 of resolution)."""
    if source_type is SourceType.POSTGRES:
        return _pg_reduce(typed)
    return _base_type(typed)


def resolve_type_name(
    typed: str, keys: "Collection[str]", source_type: SourceType
) -> "Optional[str]":
    """The key a typed ``type:`` name means, or ``None`` (E2): the first step that holds of
    (1) ``typed`` is a key exactly -- first, so a key such as ``my(x)`` can be typed back;
    (2) it reduces (:func:`reduce_typed_name`) to a key; (3) the reduction lower-cased is a
    key (necessarily all lower case: ``type:TEXT`` finds ``text``, while ``MyType`` and
    ``MYTYPE`` never merge). No wildcard, no alias table: on PostgreSQL ``type:varchar``
    never matches ``character varying`` -- only the suggestion hints bridge that."""
    if typed in keys:
        return typed
    reduced = reduce_typed_name(typed, source_type)
    if reduced in keys:
        return reduced
    if reduced.lower() in keys:
        return reduced.lower()
    return None


# Suggestion-ONLY synonyms the source catalog spells differently (E2 "did you mean" and the
# hint note). The matcher never reads this map, and a hint never broadens a match (no MySQL
# bool -> tinyint, no serial).
_TYPE_SUGGESTION_HINTS: "dict[SourceType, dict[str, str]]" = {
    SourceType.POSTGRES: {
        "varchar": "character varying",
        "char": "character", "bpchar": "character",
        "int": "integer", "int4": "integer",
        "int2": "smallint", "int8": "bigint",
        "float4": "real", "float8": "double precision", "double": "double precision",
        "dec": "numeric", "decimal": "numeric",
        "bool": "boolean",
        "timestamp": "timestamp without time zone",
        "timestamptz": "timestamp with time zone",
        "time": "time without time zone", "timetz": "time with time zone",
        "varbit": "bit varying",
        "tinytext": "text", "mediumtext": "text", "longtext": "text",
        "tinyblob": "bytea", "blob": "bytea", "mediumblob": "bytea", "longblob": "bytea",
    },
    SourceType.MYSQL: {
        "integer": "int", "int4": "int",
        "int1": "tinyint", "int2": "smallint",
        "int3": "mediumint", "middleint": "mediumint",
        "int8": "bigint",
        "numeric": "decimal", "dec": "decimal", "fixed": "decimal",
        "float4": "float", "float8": "double",
        "character": "char", "nchar": "char",
        "character varying": "varchar", "nvarchar": "varchar",
    },
}


def hint_for(
    typed: str, source_type: SourceType, keys: "Collection[str]"
) -> "Optional[str]":
    """The hint target key for a typed name, or ``None``: the name lower-cased, whitespace
    collapsed, typmods and a trailing ``[]`` removed (``[]`` re-appended to the target) --
    NOT cut at a space, so MySQL ``character varying`` finds ``varchar``. Withheld when the
    target is not a present key, so a suggestion never lands on another miss."""
    name = re.sub(r"\([^)]*\)", "", typed.lower())
    name = " ".join(name.split())
    suffix = ""
    if name.endswith("[]"):
        name, suffix = name[:-2].rstrip(), "[]"
    target = _TYPE_SUGGESTION_HINTS.get(source_type, {}).get(name)
    if target is None:
        return None
    target += suffix
    return target if target in keys else None


# --- the resolver: matching, include-wins precedence, table provenance ---------------

# Per-line STATUS codes. Distinct from the column warnings W1-W6 (a line status describes
# the line, a warning describes an excluded column), so neither copy nor tests can collide.
LINE_ONLY_UNSELECTED = "only_unselected"  # a warning: every match is on an unselected table
LINE_NO_EFFECT = "no_effect"  # an include that cancels nothing
LINE_ONLY_BLOCKED = "only_blocked"  # every candidate this line names is blocked (B1-B4)

# Short labels for a blocked candidate (the full reason is the verdict's message).
_BLOCKED_LABELS = {
    B1: "primary key", B2: "target key", B3: "referenced by a foreign key", B4: "computed",
}

@dataclass(frozen=True)
class Cancellation:
    """An include line cancelling an exclude line's matches (``kind`` ``table``: table
    names; ``column``: ``(table, column)`` pairs), in scope only. Every one is shown."""

    include_line: int
    exclude_line: int
    kind: str
    items: tuple
    # "L5 include type:varchar re-includes shop.users.password_hash excluded by L2"
    text: str = ""


@dataclass(frozen=True)
class LineStatus:
    """What one rule line did. ``matched``: what an EXCLUDE line puts in effect (stored
    columns, or tables it unselects or keeps unselected); ``keeps``: what an INCLUDE line
    cancels; ``kept_by``: ``(include line, count)`` cancelling this exclude line. Only lines
    with at least one match have a status (a line with none is E2)."""

    line: int
    action: str
    kind: str
    matched: int = 0
    keeps: int = 0
    kept_by: "tuple[tuple[int, int], ...]" = ()
    unselects: "tuple[str, ...]" = ()
    reselects: "tuple[str, ...]" = ()
    codes: "tuple[str, ...]" = ()
    notes: "tuple[str, ...]" = ()
    # (key, columns, tables) per type a type: line resolved, over the tables in scope.
    type_counts: "tuple[tuple[str, int, int], ...]" = ()
    # Candidates of an exclude line that are blocked (never stored), and their labels.
    blocked: int = 0
    blocked_labels: "tuple[str, ...]" = ()


@dataclass(frozen=True)
class BlockedItem:
    """A candidate column that cannot be excluded (B1-B4); reported, never stored."""

    table: str
    column: str
    lines: "tuple[int, ...]"
    findings: "tuple[ExclusionFinding, ...]"
    labels: "tuple[str, ...]"


@dataclass(frozen=True)
class TargetChange:
    """The overlay ALTERs a Save shows for one table (they run at Start), and the DEFAULTs
    Cut over restores. ``note`` replaces the statements when they cannot be known."""

    table: str
    statements: "tuple[str, ...]" = ()
    restores: "tuple[tuple[str, str], ...]" = ()
    note: Optional[str] = None


@dataclass(frozen=True)
class ReviewItem:
    """One row of the popup's Review-all list. ``kind``: ``unselected`` (a table),
    ``excluded``, ``blocked`` or ``kept`` (a column). ``why`` explains it; ``changes`` are
    the target statements for an excluded column; ``warning`` flags a W/T warning."""

    kind: str
    table: str
    column: Optional[str]
    lines: "tuple[int, ...]"
    why: "tuple[str, ...]" = ()
    changes: "tuple[str, ...]" = ()
    warning: bool = False


@dataclass(frozen=True)
class ResolutionDiff:
    """What a Save changes against the stored set: per-table added/removed columns, the
    table moves, the one-line ``text`` and every cancellation (``lines``)."""

    added: "Mapping[str, tuple[str, ...]]"
    removed: "Mapping[str, tuple[str, ...]]"
    unselects: "tuple[str, ...]"
    reselects: "tuple[str, ...]"
    text: str
    lines: "tuple[str, ...]"


@dataclass(frozen=True)
class RuleResolution:
    """The resolved rules. ``columns`` (per table, selected' only) and ``selected_after`` /
    ``rule_excluded_after`` are what a Save stores; everything else explains them.

    ``selected_after`` is ``None`` when the table rules change neither the selection nor
    the provenance: the TableSelection is then left untouched, so a column-only Save keeps
    the automatic selection of tables a later Refresh objects adds."""

    rules: "Mapping[int, ParsedRule]"
    errors: "Mapping[int, tuple[RuleError, ...]]"
    global_errors: "tuple[RuleError, ...]"
    columns: "Mapping[str, frozenset[str]]"
    excluded_by: "Mapping[tuple[str, str], tuple[int, ...]]"
    kept_by: "Mapping[tuple[str, str], tuple[int, ...]]"
    cancellations: "tuple[Cancellation, ...]"
    tables_to_unselect: "tuple[str, ...]"
    tables_to_reselect: "tuple[str, ...]"
    selected_after: "Optional[tuple[str, ...]]"
    rule_excluded_after: "frozenset[str]"
    line_status: "Mapping[int, LineStatus]"
    blocked: "tuple[BlockedItem, ...]" = ()
    warnings: "Mapping[tuple[str, str], tuple[ExclusionFinding, ...]]" = ()
    retained_legacy: "Mapping[str, frozenset[str]]" = ()
    target_changes: "tuple[TargetChange, ...]" = ()
    items: "tuple[ReviewItem, ...]" = ()
    diff: "Optional[ResolutionDiff]" = None

    @property
    def can_save(self) -> bool:
        return not self.errors and not self.global_errors


def _part_matcher(part: RulePart) -> "Callable[[str], bool]":
    """Equality for a quoted or star-free part; otherwise ``*`` is any run of characters
    (including none) within the part. ``_``, ``%`` and ``?`` are literal: never fnmatch."""
    if not part.wildcard:
        text = part.text
        return lambda name: name == text
    pattern = re.compile(".*".join(re.escape(seg) for seg in part.text.split("*")), re.DOTALL)
    return lambda name: pattern.fullmatch(name) is not None


def _names(items: "Sequence[str]", limit: int = 3) -> str:
    shown = ", ".join(items[:limit])
    return shown if len(items) <= limit else f"{shown} and {len(items) - limit} more"


def _plural(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


def _miss(line: int, line_text: str, part: RulePart, level: str, candidates, where: str):
    """E2 at the first failing ``level``, suggesting from that level's names only: a
    case-insensitive equal first, else up to 3 difflib matches (cutoff 0.75)."""
    what = f'{level} matching "{part.text}"' if part.wildcard else f'{level} "{part.text}"'
    message = f"No {what}{' ' + where if where else ''}."
    if part.wildcard:
        return RuleError("E2", line, message)
    picks = [c for c in candidates if c.lower() == part.text.lower()][:3]
    if picks:
        message += (" Names are case-sensitive: did you mean "
                    f"{', '.join(rule_part_token(c) for c in picks)}?")
    else:
        picks = difflib.get_close_matches(part.text, list(candidates), n=3, cutoff=0.75)
    suggestions = []
    for pick in picks:
        fixed = _replace(line_text, part.span[0], part.span[1], rule_part_token(pick))
        suggestions.append(Suggestion(f'Use "{split_rule_comment(fixed)[0].strip()}"', fixed))
    return RuleError("E2", line, message, tuple(suggestions))


def _name_miss(line, rule, line_text, order, split, tables) -> RuleError:
    """The E2 for a table or column rule that matches nothing: schema, then table within
    the matching schemas, then column within the matching tables."""
    matchers = [_part_matcher(p) for p in rule.parts]
    schemas = list(dict.fromkeys(split[n][0] for n in order if n in split))
    hit_schemas = [x for x in schemas if matchers[0](x)]
    if not hit_schemas:
        return _miss(line, line_text, rule.parts[0], "schema", schemas, "")
    in_schemas = set(hit_schemas)
    candidates = [n for n in order if n in split and split[n][0] in in_schemas]
    hit_tables = [n for n in candidates if matchers[1](split[n][1])]
    if not hit_tables:
        where = (f"in schema {rule_part_token(hit_schemas[0])}" if len(hit_schemas) == 1
                 else f"in the {len(hit_schemas)} matching schemas")
        names = list(dict.fromkeys(split[n][1] for n in candidates))
        return _miss(line, line_text, rule.parts[1], "table", names, where)
    where = (f"in {'.'.join(rule_part_token(x) for x in split[hit_tables[0]])}"
             if len(hit_tables) == 1 else f"in the {len(hit_tables)} matching tables")
    names = list(dict.fromkeys(c.name for n in hit_tables for c in tables[n].columns))
    return _miss(line, line_text, rule.parts[2], "column", names, where)


def _type_miss(line, index, typed: RulePart, line_text, key_counts, source_type) -> RuleError:
    """The E2 for one ``type:`` name: the hint, else a case-insensitive equal, else up to 3
    difflib matches, else no suggestion and the types present by column count."""
    message = f"No column of type {type_name_token(typed.text)} in any migratable table."
    hint = hint_for(typed.text, source_type, key_counts)
    if hint is not None:
        picks = [hint]
    else:
        reduced = reduce_typed_name(typed.text, source_type)
        picks = [k for k in sorted(key_counts) if k.lower() == reduced.lower()][:3]
        if picks:
            message += (" Type names are case-sensitive: did you mean "
                        f"{', '.join(type_name_token(k) for k in picks)}?")
        else:
            picks = difflib.get_close_matches(reduced, sorted(key_counts), n=3, cutoff=0.75)
    if not picks:
        ranked = sorted(key_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        listed = ", ".join(f"{type_name_token(k)} ({n})" for k, n in ranked[:8])
        more = f" and {len(ranked) - 8} more" if len(ranked) > 8 else ""
        message += f" Types here: {listed}{more}."
    suggestions = tuple(
        Suggestion(f"Use type:{type_name_token(k)}",
                   _replace(line_text, typed.span[0], typed.span[1], type_name_token(k)))
        for k in picks
    )
    return RuleError("E2", line, message, suggestions, name_index=index)


def _hint_notes(rule, resolved, scope_counts, source_type) -> "list[str]":
    """Notes (never errors) for a type name that matched a key while its hint names a
    DIFFERENT key that is also here; ``bpchar``/``character`` are the same type."""
    notes = []
    on_line = set(resolved.values())
    for typed, key in resolved.items():
        mine = scope_counts.get(key, 0)
        if key == "character" and source_type is SourceType.POSTGRES:
            other = scope_counts.get("bpchar", 0)
            if other and "bpchar" not in on_line:
                notes.append(
                    f"type:{type_name_token(typed)} matched character ({_plural(mine, 'column')})."
                    f" {other} {'column is' if other == 1 else 'columns are'} the same type "
                    "declared without a length, printed bpchar: add type:bpchar to cover them.")
            continue
        target = hint_for(typed, source_type, scope_counts)
        if target is None or target == key or target in on_line or not scope_counts.get(target):
            continue
        other = scope_counts[target]
        if key == "bpchar":
            notes.append(
                f"type:{type_name_token(typed)} matched bpchar ({_plural(mine, 'column')}: "
                f"character declared without a length). The {other} character "
                f"{'column is' if other == 1 else 'columns are'} the same type declared with a "
                "length: add type:character to cover them.")
        else:
            notes.append(
                f"type:{type_name_token(typed)} matched {type_name_token(key)} "
                f"({_plural(mine, 'column')}). {type_name_token(target)} is a different type "
                f"here ({_plural(other, 'column')}): use type:{type_name_token(target)} for "
                "those.")
    return notes


def resolve_exclusion_rules(
    text: str,
    *,
    migratable: "Sequence[TableDef]",
    source_type: SourceType,
    effective_selection: "Sequence[str]",
    rule_excluded_prev: "Collection[str]" = (),
    selection_lock_reason: Optional[str] = None,
    retick_overrides: "Collection[str]" = (),
    context_factory: "Optional[Callable[[Sequence[str], Collection[str]], ExclusionContext]]" = None,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    live_columns: "Optional[Mapping[str, Collection[str]]]" = None,
    legacy: "Optional[Mapping[str, Collection[str]]]" = None,
    stored_columns: "Optional[Mapping[str, Collection[str]]]" = None,
) -> RuleResolution:
    """Resolve rule ``text`` against the ``migratable`` tables (the tree's universe, in its
    order). Include wins and line order never matters: per domain (tables; columns),
    ``final = U(exclude matches) - U(include matches)``; a table include never cancels a
    column exclude.

    Tables (the SAME state as the tree): with ``selected`` the effective selection and
    ``prev_rule`` the tables an earlier Save's rules unselected (``rule_excluded_prev``),
    ``new_rule = final_tables & (selected | prev_rule)`` and
    ``selected' = (selected | prev_rule) - new_rule``. So deleting a rule or adding an
    include re-selects only what the rules removed; a tree-unticked table stays unticked.
    ``retick_overrides`` are rule-excluded tables re-ticked in the tree since the last Save
    (a deliberate override): the Save unselects them again, and says so.

    Column and type rules match over every migratable table (a name matching nothing
    anywhere is E2) but are stored only for ``selected'``. A table name splits at its FIRST
    ``.``, as :func:`~dsql_migrator.core.cdc.format_column_exclude_value` does; a name with
    no ``.`` is never matched (introspection always qualifies).

    While ``selection_lock_reason`` holds, a table line that would change the selection is
    E3, and a re-selection no line causes (a deleted rule) is a global E3. A result that
    would leave no table selected is the global error E5.

    Eligibility: ``context_factory(selected', candidate tables)`` builds the ONE context
    (called once, so B3 is judged against the selection AFTER this Save); a BLOCKED
    candidate is reported and never stored, the rest carry their W1-W6 warnings. E4: a
    ``:`` in the table of a stored column (the session stores ``table:column``), and,
    with ``applied_conversions``, a source column the applied DDL lacks that is not
    excluded -- a certain 42703 only (``live_columns`` lets a column the live target
    still has through), offering the rule, never applying it. ``target_changes`` preview
    the overlay over the stored set minus the ``legacy`` (omit-only) entries it keeps;
    ``diff`` compares with ``stored_columns``. Pure.
    """
    rules, parsed_errors = parse_rules(text)
    source_lines = [raw[:-1] if raw.endswith("\r") else raw for raw in (text or "").split("\n")]
    errors: dict[int, list[RuleError]] = {n: list(es) for n, es in parsed_errors.items()}
    global_errors: list[RuleError] = []
    tables: dict[str, "TableDef"] = {}
    for table in migratable:
        tables.setdefault(table.name, table)
    order = list(tables)
    split = {}
    for name in order:
        schema, sep, rest = name.partition(".")
        if sep:
            split[name] = (schema, rest)
    type_keys: Optional[dict] = None

    def _type_keys() -> dict:
        nonlocal type_keys
        if type_keys is None:
            type_keys = {
                (name, column.name): rule_type_name(column, source_type)
                for name in order if name in split
                for column in tables[name].columns
            }
        return type_keys

    table_hits: dict[int, list[str]] = {}
    column_hits: dict[int, list[tuple[str, str]]] = {}
    type_resolved: dict[int, dict[str, str]] = {}
    for line, rule in sorted(rules.items()):
        line_text = source_lines[line - 1]
        if rule.kind == "type":
            keys = _type_keys()
            key_counts: dict[str, int] = {}
            for key in keys.values():
                key_counts[key] = key_counts.get(key, 0) + 1
            resolved: dict[str, str] = {}
            for index, typed in enumerate(rule.type_names):
                key = resolve_type_name(typed.text, key_counts, source_type)
                if key is None:
                    errors.setdefault(line, []).append(
                        _type_miss(line, index, typed, line_text, key_counts, source_type))
                else:
                    resolved[typed.text] = key
            wanted = set(resolved.values())
            hits = [item for item, key in keys.items() if key in wanted]
            if hits:
                column_hits[line] = hits
                type_resolved[line] = resolved
            continue
        matchers = [_part_matcher(p) for p in rule.parts]
        if rule.kind == "table":
            match_schema, match_table = matchers
            hits_t = [n for n in order if n in split
                      and match_schema(split[n][0]) and match_table(split[n][1])]
            if hits_t:
                table_hits[line] = hits_t
        else:
            match_schema, match_table, match_column = matchers
            hits_c = [(n, column.name) for n in order if n in split
                      and match_schema(split[n][0]) and match_table(split[n][1])
                      for column in tables[n].columns if match_column(column.name)]
            if hits_c:
                column_hits[line] = hits_c
        if line not in table_hits and line not in column_hits:
            errors.setdefault(line, []).append(
                _name_miss(line, rule, line_text, order, split, tables))

    def _by_action(hits_by_line: dict, action: str) -> dict:
        found: dict = {}
        for line, hits in hits_by_line.items():
            if rules[line].action == action:
                for item in hits:
                    found.setdefault(item, []).append(line)
        return found

    excl_t, incl_t = _by_action(table_hits, "exclude"), _by_action(table_hits, "include")
    excl_c, incl_c = _by_action(column_hits, "exclude"), _by_action(column_hits, "include")

    known = set(order)
    selected = set(effective_selection) & known
    prev_rule = set(rule_excluded_prev) & known
    reach = selected | prev_rule
    new_rule = {n for n in excl_t if n not in incl_t} & reach
    after = [n for n in order if n in reach and n not in new_rule]
    after_set = set(after)
    unselect = tuple(n for n in order if n in selected and n not in after_set)
    reselect = tuple(n for n in order if n in after_set and n not in selected)
    changed = after_set != selected or new_rule != prev_rule
    # A provenance entry for a table no longer migratable is kept, not invented or lost.
    rule_excluded_after = frozenset(new_rule | (set(rule_excluded_prev) - known))

    columns: dict[str, set[str]] = {}
    excluded_by: dict = {}
    kept_by: dict = {}
    candidates: dict = {}
    for item, lines in excl_c.items():
        if item[0] not in after_set:
            continue
        if item in incl_c:
            kept_by[item] = tuple(sorted(incl_c[item]))
        else:
            candidates[item] = tuple(sorted(lines))
    ctx = None
    if context_factory is not None:
        ctx = context_factory(after, {t for t, _c in candidates})
    blocked: dict = {}
    warnings: dict = {}
    for item, lines in candidates.items():
        verdict = column_exclusion_verdict(item[0], item[1], ctx) if ctx is not None else None
        if verdict is not None and verdict.blocked:
            labels = tuple(dict.fromkeys(_BLOCKED_LABELS[f.code] for f in verdict.findings))
            blocked[item] = BlockedItem(item[0], item[1], lines, verdict.findings, labels)
            continue
        columns.setdefault(item[0], set()).add(item[1])
        excluded_by[item] = lines
        if ctx is not None:
            found = column_exclusion_warnings(item[0], item[1], ctx)
            if found:
                warnings[item] = found

    cancel: dict = {}
    for kind, hits_by_line, excl, in_scope in (
        ("table", table_hits, excl_t, lambda n: n in reach),
        ("column", column_hits, excl_c, lambda i: i[0] in after_set),
    ):
        for line, hits in hits_by_line.items():
            if rules[line].action != "include":
                continue
            for item in hits:
                if in_scope(item):
                    for exclude_line in excl.get(item, ()):
                        cancel.setdefault((line, exclude_line, kind), []).append(item)
    def _cancel_text(include_line: int, exclude_line: int, kind: str, items) -> str:
        shown = [n if kind == "table" else f"{n[0]}.{n[1]}" for n in items]
        return (f"L{include_line} {rules[include_line].rule_text.strip()} re-includes "
                f"{_names(shown)} excluded by L{exclude_line}")

    cancellations = tuple(
        Cancellation(i, x, kind, tuple(items), _cancel_text(i, x, kind, items))
        for (i, x, kind), items in sorted(cancel.items()))

    def _kept_counts(line: int) -> tuple:
        counts: dict[int, int] = {}
        for (i, x, _kind), items in cancel.items():
            if x == line:
                counts[i] = counts.get(i, 0) + len(items)
        return tuple(sorted(counts.items()))

    unselect_set, reselect_set = set(unselect), set(reselect)
    scope_type_counts: Optional[tuple] = None

    def _scope_type_counts() -> tuple:
        """Per type key: columns and tables over selected' (the scope of "matches N")."""
        nonlocal scope_type_counts
        if scope_type_counts is None:
            counts: dict[str, int] = {}
            in_tables: dict[str, set] = {}
            for (table, _column), key in _type_keys().items():
                if table in after_set:
                    counts[key] = counts.get(key, 0) + 1
                    in_tables.setdefault(key, set()).add(table)
            scope_type_counts = (counts, in_tables)
        return scope_type_counts

    line_status: dict[int, LineStatus] = {}
    for line in sorted(set(table_hits) | set(column_hits)):
        rule = rules[line]
        codes: list[str] = []
        notes: list[str] = []
        if rule.kind == "table":
            hits_t = table_hits[line]
            if rule.action == "exclude":
                effect = [n for n in hits_t if n in new_rule]
                line_unselects = tuple(n for n in effect if n in unselect_set)
                notes.extend(
                    f"L{line} matches {n}, ticked in the tree after your last Save: Save will "
                    "unselect it again"
                    for n in line_unselects if n in retick_overrides
                )
                if not any(n in reach for n in hits_t):
                    codes.append(LINE_ONLY_UNSELECTED)
                status = LineStatus(line, rule.action, rule.kind, matched=len(effect),
                                    kept_by=_kept_counts(line), unselects=line_unselects)
                touched = line_unselects
            else:
                kept = [n for n in hits_t if n in excl_t and n in reach]
                line_reselects = tuple(n for n in kept if n in reselect_set)
                notes.extend(c.text for c in cancellations if c.include_line == line)
                if not kept:
                    codes.append(LINE_NO_EFFECT)
                status = LineStatus(line, rule.action, rule.kind, keeps=len(kept),
                                    reselects=line_reselects)
                touched = line_reselects
            if selection_lock_reason is not None:
                if touched:
                    errors.setdefault(line, []).append(RuleError(
                        "E3", line,
                        f"This rule would change which tables migrate ({_names(touched)}). "
                        f"{selection_lock_reason}",
                    ))
                else:
                    notes.append("No effect while the table set is locked.")
        else:
            hits_c = column_hits[line]
            in_scope = [i for i in hits_c if i[0] in after_set]
            if hits_c and not in_scope:
                codes.append(LINE_ONLY_UNSELECTED)
            inert = []
            for table, _column in hits_c:
                if table in unselect_set and table not in inert:
                    inert.append(table)
            if inert:
                notes.append(f"Inert on {_names(inert)}: a table rule unselects "
                             f"{'it' if len(inert) == 1 else 'them'} in this Save.")
            type_counts: tuple = ()
            if line in type_resolved:
                scope_counts, scope_tables = _scope_type_counts()
                resolved = type_resolved[line]
                type_counts = tuple(
                    (key, scope_counts.get(key, 0), len(scope_tables.get(key, ())))
                    for key in dict.fromkeys(resolved.values()))
                notes.extend(_hint_notes(rule, resolved, scope_counts, source_type))
            if rule.action == "exclude":
                named = [i for i in in_scope if i not in incl_c]
                line_blocked = [blocked[i] for i in named if i in blocked]
                blocked_labels = tuple(dict.fromkeys(
                    label for b in line_blocked for label in b.labels))
                if line_blocked and len(line_blocked) == len(named):
                    codes.append(LINE_ONLY_BLOCKED)
                status = LineStatus(line, rule.action, rule.kind,
                                    matched=len(named) - len(line_blocked),
                                    kept_by=_kept_counts(line), blocked=len(line_blocked),
                                    blocked_labels=blocked_labels)
            else:
                kept_n = sum(1 for i in in_scope if i in excl_c)
                if not kept_n:
                    codes.append(LINE_NO_EFFECT)
                notes.extend(c.text for c in cancellations if c.include_line == line)
                status = LineStatus(line, rule.action, rule.kind, keeps=kept_n)
        line_status[line] = replace(status, codes=tuple(codes), notes=tuple(notes),
                                    type_counts=type_counts if rule.kind != "table" else ())

    if selection_lock_reason is not None:
        cancelled = {n for n in incl_t if n in excl_t}
        unexplained = [n for n in reselect if n not in cancelled]
        if unexplained:
            global_errors.append(RuleError(
                "E3", 0,
                f"Save would re-select {_names(unexplained)}: no table rule unselects "
                f"{'it' if len(unexplained) == 1 else 'them'} any more. "
                f"{selection_lock_reason}",
            ))
    if not after and selected:
        global_errors.append(RuleError(
            "E5", 0, "These rules would leave no table to migrate: keep at least one table "
            "selected."))
    for item, lines in sorted(excluded_by.items()):
        if ":" in item[0]:
            for line in lines:
                errors.setdefault(line, []).append(RuleError(
                    "E4", line,
                    f'{item[0]} contains ":", which a saved column exclusion cannot hold (it is '
                    'stored as table:column). Exclude the whole table, or remove this rule.'))
    if applied_conversions:
        missing = _certain_missing(after, tables, applied_conversions, columns, live_columns)
        if missing:
            global_errors.append(missing)

    retained = {
        t: frozenset(cs & set(legacy.get(t, ()))) for t, cs in columns.items()
        if legacy and cs & set(legacy.get(t, ()))
    }
    target_changes = _target_changes(
        [t for t in order if t in columns], columns, retained, applied_conversions)
    items = _review_items(order, new_rule, table_hits, rules, excluded_by, warnings, blocked,
                          kept_by, target_changes, ctx)
    diff = _diff(columns, stored_columns or {}, unselect, reselect, cancellations)

    return RuleResolution(
        rules=dict(sorted(rules.items())),
        errors={n: tuple(es) for n, es in sorted(errors.items())},
        global_errors=tuple(global_errors),
        columns={t: frozenset(cs) for t, cs in sorted(columns.items())},
        excluded_by=excluded_by,
        kept_by=kept_by,
        cancellations=cancellations,
        tables_to_unselect=unselect,
        tables_to_reselect=reselect,
        selected_after=tuple(after) if changed else None,
        rule_excluded_after=rule_excluded_after,
        line_status=line_status,
        blocked=tuple(blocked[i] for i in sorted(blocked)),
        warnings=warnings,
        retained_legacy=retained,
        target_changes=target_changes,
        items=items,
        diff=diff,
    )


def _certain_missing(after, tables, applied, columns, live) -> "Optional[RuleError]":
    """The global E4 for source columns the applied DDL lacks and the rules do not
    exclude -- only a CERTAIN 42703: a column the live target still has gets through, as
    in missing_column_block_reason. Offers each column's rule, never applies it (D6)."""
    missing = missing_target_columns([tables[n] for n in after], applied, columns)
    if live:
        missing = {t: kept for t, cols in missing.items()
                   if (kept := tuple(c for c in cols if c not in live.get(t, ())))}
    pairs = [(t, c) for t, cols in missing.items() for c in cols]
    if not pairs:
        return None
    shown = [f"{t}.{c}" for t, c in pairs]
    many = len(pairs) > 1
    them = "them" if many else "it"
    suggestions = []
    for t, c in pairs:
        schema, _sep, rest = t.partition(".")
        line = "exclude " + ".".join(rule_part_token(x) for x in (schema, rest, c))
        suggestions.append(Suggestion(f"+ add rule: {line}", line))
    return RuleError(
        "E4", 0,
        f"{_names(shown)} {'are' if many else 'is'} not in your Aurora DSQL "
        f"table{'s' if len(missing) > 1 else ''}; writing {them} would fail (42703, undefined "
        f"column). Add {them} back to the target DDL in Schema Conversion, or exclude {them}.",
        tuple(suggestions),
    )


def _target_changes(tables_in_order, columns, retained, applied) -> "tuple[TargetChange, ...]":
    """The overlay preview over the stored set minus the retained legacy (omit-only)
    entries, through THE builder the Start dialogs use, fed the applied DDL's facts."""
    if applied is None:
        return ()
    from dsql_migrator.core.schema_applier import (
        OverlayRefusedError,
        build_overlay_statements,
        ddl_column_facts,
    )

    changes = []
    for table in tables_in_order:
        cols = sorted(columns[table] - retained.get(table, frozenset()))
        if not cols:
            continue
        ddl = getattr(applied.get(table), "target_ddl", None) or ""
        facts = ddl_column_facts(ddl, cols) if ddl else None
        if facts is None:
            changes.append(TargetChange(table, note=(
                f"Cannot read the Aurora DSQL DDL of {table}: the changes are decided when "
                "the migration starts.")))
            continue
        try:
            statements = build_overlay_statements(table, facts)
        except OverlayRefusedError as exc:  # the B4 backstop; the rule already blocks it
            changes.append(TargetChange(table, note=str(exc)))
            continue
        restores = tuple((c, facts[c].default_expr) for c in cols
                         if facts[c] is not None and facts[c].default_expr is not None)
        changes.append(TargetChange(table, tuple(s.text() for s in statements), restores))
    return tuple(changes)


_KIND_ORDER = {"unselected": 0, "excluded": 1, "blocked": 2, "kept": 3}


def _review_items(order, new_rule, table_hits, rules, excluded_by, warnings, blocked,
                  kept_by, target_changes, ctx) -> "tuple[ReviewItem, ...]":
    """The ONE Review-all list: rule-unselected tables, then each column the rules name."""
    rank = {n: i for i, n in enumerate(order)}
    items: list[ReviewItem] = []
    for table in order:
        if table not in new_rule:
            continue
        lines = tuple(n for n, hits in sorted(table_hits.items())
                      if rules[n].action == "exclude" and table in hits)
        found = table_exclusion_findings(table, ctx) if ctx is not None else ()
        items.append(ReviewItem("unselected", table, None, lines,
                                tuple(f.message for f in found),
                                warning=any(f.code in (T2, T3) for f in found)))
    statements: dict = {}
    for change in target_changes:
        for text in change.statements:
            for column in _columns_named_in(text):
                statements.setdefault((change.table, column), []).append(text)
    for (table, column), lines in excluded_by.items():
        found = warnings.get((table, column), ())
        items.append(ReviewItem("excluded", table, column, lines,
                                tuple(f.message for f in found),
                                tuple(statements.get((table, column), ())), bool(found)))
    for (table, column), item in blocked.items():
        items.append(ReviewItem("blocked", table, column, item.lines,
                                tuple(f.message for f in item.findings)))
    for (table, column), lines in kept_by.items():
        on = ", ".join(f"L{n}" for n in lines)
        items.append(ReviewItem("kept", table, column, lines, (f"Kept by include on {on}.",)))
    items.sort(key=lambda i: (_KIND_ORDER[i.kind], rank.get(i.table, len(rank)), i.table,
                              i.column or ""))
    return tuple(items)


def _columns_named_in(statement: str) -> "tuple[str, ...]":
    """The column an overlay ``ALTER TABLE t ALTER COLUMN "c" ...`` statement names."""
    match = re.search(r'ALTER COLUMN ("(?:[^"]|"")*")', statement)
    return (match.group(1)[1:-1].replace('""', '"'),) if match else ()


def _diff(columns, stored, unselect, reselect, cancellations) -> ResolutionDiff:
    added: dict = {}
    removed: dict = {}
    for table in sorted(set(columns) | set(stored)):
        now = set(columns.get(table, ()))
        before = set(stored.get(table, ()) or ())
        if now - before:
            added[table] = tuple(sorted(now - before))
        if before - now:
            removed[table] = tuple(sorted(before - now))
    n_added = sum(len(c) for c in added.values())
    n_removed = sum(len(c) for c in removed.values())
    text = f"+{_plural(n_added, 'column')}, -{n_removed}"
    if unselect or reselect:
        text += f"; unselects {_plural(len(unselect), 'table')}, re-selects {len(reselect)}"
    return ResolutionDiff(added, removed, tuple(unselect), tuple(reselect), text,
                          (text, *(c.text for c in cancellations)))


__all__ = [
    "LINE_NO_EFFECT",
    "LINE_ONLY_BLOCKED",
    "LINE_ONLY_UNSELECTED",
    "BlockedItem",
    "Cancellation",
    "LineStatus",
    "ParsedRule",
    "ResolutionDiff",
    "ReviewItem",
    "RuleResolution",
    "TargetChange",
    "RuleError",
    "RulePart",
    "Suggestion",
    "hint_for",
    "parse_rules",
    "reduce_typed_name",
    "resolve_exclusion_rules",
    "resolve_type_name",
    "rule_type_name",
    "rule_part_token",
    "split_rule_comment",
    "type_name_token",
]
