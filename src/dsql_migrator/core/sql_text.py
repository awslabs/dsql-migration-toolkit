# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Reading SQL text without parsing it: the quote-aware scan.

A scan, not a parser: it never reads into a string literal, a dollar-quoted body or a
comment, and reads a quoted identifier as the one name it is. It holds:

- :func:`_sql_skip` (the literal, quoted identifier or comment starting at a position)
  and :func:`_mask_sql_literals` (a text with its literals and comments blanked);
- :func:`_scan_sql_names` (the names a view body or a CREATE VIEW reads and puts out,
  :class:`_SqlNames`) with :func:`_chain_parts` (a dotted name's parts as PostgreSQL
  compares them);
- the statement split of a script (:func:`sql_statement_spans`), its text without
  comments (:func:`sql_without_comments`) and the terminators a split -> join -> split
  round trip keeps (:func:`terminated_sql_script`, :func:`terminated_sql_statement`).

It was split out of ``core/converter.py``, which re-exports every name defined here
(private ones too), so ``from dsql_migrator.core.converter import ...`` keeps resolving.
The view ordering that reads it (``views_in_dependency_order``) stays in the converter.
This module must not import the converter: the converter imports it at its top level.
"""

from __future__ import annotations

import functools
import re
from dataclasses import dataclass
from typing import Optional

from dsql_migrator.core.pg_names import pg_fold_unquoted


# ---------------------------------------------------------------------------------------
# Reading SQL text for names: what a view reads, what it outputs, what an index predicate
# compares. Not a parser -- a scan that never reads into a string literal, a dollar-quoted
# body or a comment, so a '"' or a name inside one is no identifier.
# ---------------------------------------------------------------------------------------

_DOLLAR_TAG_RE = re.compile(r"\$(?:[A-Za-z_]\w*)?\$")
# Where something a name scan must step over may start.
_SQL_SPECIAL_RE = re.compile(r"""['"$]|--|/\*|[eE](?=')""")


def _sql_skip(text: str, i: int) -> "Optional[tuple[str, int]]":
    """The literal, quoted identifier or comment starting at ``text[i]``, and its end.

    ``("string", end)`` for ``'...'`` and ``E'...'`` (doubled quotes; backslash escapes in
    an E-string), ``("dollar", end)`` for ``$tag$...$tag$``, ``("comment", end)`` for ``--``
    and (nested) ``/* */``, ``("quoted", end)`` for ``"..."``; ``None`` for anything else.
    An unterminated one runs to the end of the text."""
    n = len(text)
    ch = text[i]
    word_before = i > 0 and (text[i - 1].isalnum() or text[i - 1] in "_$")
    if ch == "'" or (ch in "eE" and text[i + 1 : i + 2] == "'" and not word_before):
        escape = ch in "eE"
        j = i + (2 if escape else 1)
        while j < n:
            if escape and text[j] == "\\":
                j += 2
                continue
            if text[j] == "'":
                if text[j + 1 : j + 2] == "'":
                    j += 2
                    continue
                return "string", j + 1
            j += 1
        return "string", n
    if ch == '"':
        j = i + 1
        while j < n:
            if text[j] == '"':
                if text[j + 1 : j + 2] == '"':
                    j += 2
                    continue
                return "quoted", j + 1
            j += 1
        return "quoted", n
    if ch == "$":
        if word_before:
            return None
        tag = _DOLLAR_TAG_RE.match(text, i)
        if tag is None:
            return None
        close = text.find(tag.group(0), tag.end())
        return "dollar", (n if close < 0 else close + len(tag.group(0)))
    if text.startswith("--", i):
        line_end = text.find("\n", i)
        return "comment", (n if line_end < 0 else line_end)
    if text.startswith("/*", i):
        depth, j = 1, i + 2
        while j < n and depth:
            if text.startswith("/*", j):
                depth, j = depth + 1, j + 2
            elif text.startswith("*/", j):
                depth, j = depth - 1, j + 2
            else:
                j += 1
        return "comment", j
    return None


def _mask_sql_literals(text: str) -> str:
    """``text`` with every string literal, dollar-quoted body and comment blanked to spaces
    (the same length, so offsets hold); quoted identifiers are kept."""
    if not _SQL_SPECIAL_RE.search(text):
        return text
    out: list[str] = []
    pos = 0
    while True:
        match = _SQL_SPECIAL_RE.search(text, pos)
        if match is None:
            out.append(text[pos:])
            return "".join(out)
        start = match.start()
        skipped = _sql_skip(text, start)
        if skipped is None:
            out.append(text[pos : start + 1])
            pos = start + 1
            continue
        kind, end = skipped
        out.append(text[pos:start])
        out.append(text[start:end] if kind == "quoted" else " " * (end - start))
        pos = end


_SQL_NAME_PART = r'"(?:[^"]|"")+"|[^\W\d][\w$]*'
# One token of masked SQL: a dotted name (``app."Orders".id``, ``t.*``), a cast, a bracket,
# a comma, a number or an operator. Anything else (whitespace, ';', ':') is stepped over.
_SQL_TOKEN_RE = re.compile(
    rf"(?P<chain>(?<![\w$])(?:{_SQL_NAME_PART})(?:\s*\.\s*(?:{_SQL_NAME_PART}|\*))*)"
    r"|(?P<cast>::)|(?P<open>\()|(?P<close>\))|(?P<comma>,)|(?P<number>\d[\w.]*)"
    r"|(?P<op>[<>=!~+\-*/%^&|#@?\[\]]+)"
)
_SQL_PART_RE = re.compile(r'"((?:[^"]|"")+)"|([^\W\d][\w$]*)')

# The words that end a FROM clause (and a select list) at their own bracket level. ON,
# USING and WITH (ORDINALITY) are inside a FROM item, so a comma after them still lists
# the next relation.
_FROM_END_WORDS = frozenset(
    {
        "where", "group", "having", "order", "limit", "offset", "fetch", "window",
        "union", "intersect", "except", "returning", "for", "into", "values",
    }
)
_FROM_PREFIX_WORDS = frozenset({"only", "lateral"})


@functools.lru_cache(maxsize=65536)
def _chain_parts(raw: str) -> "tuple[tuple[str, ...], Optional[str]]":
    """A dotted name's parts as PostgreSQL compares them (quoted as written, unquoted
    folded), and the lower-cased word when it is one unquoted part (a keyword candidate)."""
    parts = tuple(
        quoted.replace('""', '"') if quoted else pg_fold_unquoted(bare)
        for quoted, bare in _SQL_PART_RE.findall(raw)
        if quoted or bare
    )
    word = parts[0].lower() if len(parts) == 1 and not raw.startswith('"') else None
    return parts, word


@dataclass
class _SelectItem:
    start: int
    output: Optional[str] = None  # its alias (``AS x``), or the column of a lone ``t.x``
    chains: int = 0  # dotted names at its own level (one, and no alias: a plain column)
    last: Optional[str] = None
    end: int = 0
    skip_paren: bool = False  # the (...) of DISTINCT ON is no expression of the item


@dataclass
class _SqlNames:
    """What :func:`_scan_sql_names` read from one SQL text."""

    names: "set[str]"  # every name part outside literals and comments
    relations: "list[tuple[Optional[str], str]]"  # (schema or None, name) after FROM/JOIN
    items: "list[tuple[Optional[str], int, int]]"  # select items: (output name, start, end)
    masked: str


def _new_frame(*, in_from: bool = False, query: bool = False, start: int = 0) -> dict:
    return {
        "start": start,  # where this bracket level opens (0: the text itself)
        "in_from": in_from,  # inside this level's FROM clause
        "expect": in_from,  # the next name is a relation
        "from_item": in_from,  # this bracket opened where a relation was expected
        "query": query,  # a query level (SELECT/WITH/VALUES), not a function's arguments
        "item": None,
        "as": False,
        "prev": None,  # the previous bare word at this level
        "distinct_on": False,  # the (...) of a DISTINCT ON, closing before the first item
    }


def _scan_sql_names(text: str) -> _SqlNames:
    """The names in a view body or a CREATE VIEW: every identifier outside literals, the
    relations it reads (the name right after ``FROM``/``JOIN``/a FROM-list comma --
    through parenthesised joins, subqueries, ``ON``/``USING`` conditions and ``WITH
    ORDINALITY`` -- until WHERE/GROUP BY/... ends the clause) and its select items with
    their output names, a column alias or a lone column (``timestamp with time zone``,
    ``DISTINCT ON (...)``, ``WITHIN GROUP`` and ``IS DISTINCT FROM`` do not end an item).
    ``CREATE VIEW x AS`` names no relation, so the header never reads as a read of the
    view itself."""
    masked = _mask_sql_literals(text)
    names: set[str] = set()
    relations: list[tuple[Optional[str], str]] = []
    relation_at: list[int] = []  # where each relation is read
    spans: list[tuple[int, int]] = []  # the bracket levels, (open, close)
    # Each bracket level's WITHs: (where, whether RECURSIVE follows), by the level's span.
    level_withs: dict[tuple[int, int], list[tuple[int, bool]]] = {}
    recursive_next = re.compile(r"\s+RECURSIVE\b", re.IGNORECASE)
    items: list[tuple[Optional[str], int, int]] = []
    frames: list[dict] = [_new_frame()]

    def close_item(frame: dict, end: int) -> None:
        item = frame["item"]
        if item is None:
            return
        output = item.output
        if output is None and item.chains == 1:
            output = item.last
        items.append((output, item.start, end))
        frame["item"] = None

    pending_cast = False
    for match in _SQL_TOKEN_RE.finditer(masked):
        kind = match.lastgroup
        frame = frames[-1]
        if kind == "chain":
            parts, word = _chain_parts(match.group("chain"))
            if not parts:
                continue
            if pending_cast:  # a type name, not a reference
                pending_cast = False
                frame["prev"] = word
                continue
            names.update(parts)
            prev, frame["prev"] = frame["prev"], word
            item = frame["item"]
            if word is not None:
                # A WITH opens a query also as the first word of a derived table, where a
                # relation is expected (``FROM (WITH x AS (...) SELECT ...)``): read as
                # WITH ORDINALITY there, its CTE name was taken for a read of a relation.
                if word == "select" or (
                    word == "with" and item is None and (not frame["in_from"] or frame["expect"])
                ):
                    close_item(frame, match.start())
                    frame.update(in_from=False, expect=False, query=True)
                    if word == "select":
                        frame["item"] = _SelectItem(start=match.end())
                    else:
                        frame.setdefault("withs", []).append(
                            (match.start(), recursive_next.match(masked, match.end()) is not None)
                        )
                    continue
                if word == "from" and prev != "distinct" and frame["query"]:
                    close_item(frame, match.start())
                    frame.update(in_from=True, expect=True)
                    continue
                if word in _FROM_END_WORDS and not (word == "group" and prev == "within"):
                    close_item(frame, match.start())
                    frame.update(in_from=False, expect=False)
                    if word == "values":
                        frame["query"] = True
                    continue
                if frame["in_from"]:
                    if word == "join":
                        frame["expect"] = True
                        continue
                    if word in ("on", "using"):
                        frame["expect"] = False
                        continue
                    if word in _FROM_PREFIX_WORDS or word in ("with", "ordinality", "as"):
                        frame["as"] = word == "as"
                        continue
                elif item is not None:
                    if word == "as":
                        frame["as"] = True
                        continue
                    if word in ("distinct", "all"):
                        continue
                    if word == "on" and prev == "distinct":
                        item.skip_paren = True
                        continue
            if frame["in_from"] and frame["expect"]:
                relations.append((parts[-2] if len(parts) >= 2 else None, parts[-1]))
                relation_at.append(match.start())
                frame["expect"] = False
            elif item is not None:
                if frame["as"]:
                    item.output = parts[-1]
                elif match.group("chain").endswith("*"):
                    item.chains += 2  # t.*: many outputs, none of them named t
                else:
                    item.chains += 1
                    item.last = parts[-1]
            frame["as"] = False
            continue
        pending_cast = kind == "cast"
        frame["as"] = False
        frame["prev"] = None
        if kind == "open":
            opened_for_relation = frame["in_from"] and frame["expect"]
            frames.append(
                _new_frame(
                    in_from=opened_for_relation, query=opened_for_relation, start=match.start()
                )
            )
            item = frame["item"]
            if item is not None:
                if item.skip_paren:
                    item.skip_paren = False
                    frames[-1]["distinct_on"] = True
                else:
                    item.chains += 2  # an expression: no longer a lone column
        elif kind == "close":
            if len(frames) > 1:
                close_item(frames[-1], match.start())
                closed = frames.pop()
                spans.append((closed["start"], match.end()))
                level_withs.setdefault(spans[-1], []).extend(closed.get("withs", ()))
                if closed["from_item"]:
                    frames[-1]["expect"] = False
                if closed["distinct_on"] and frames[-1]["item"] is not None:
                    # The first item starts after the DISTINCT ON key, whose names are not
                    # part of what it puts out.
                    frames[-1]["item"].start = match.end()
        elif kind == "comma":
            if frame["in_from"]:
                frame["expect"] = True
            elif frame["item"] is not None:
                close_item(frame, match.start())
                frame["item"] = _SelectItem(start=match.end())
        elif frame["item"] is not None and kind in ("op", "number"):
            frame["item"].chains += 2  # an expression: no longer a lone column
    for frame in frames:
        close_item(frame, len(masked))
        spans.append((frame["start"], len(masked)))
        level_withs.setdefault(spans[-1], []).extend(frame.get("withs", ()))
    # A name a WITH defines (``name AS (``, with an optional column list or MATERIALIZED)
    # is read FROM like a relation, but it is the statement's own: bound to a same-named
    # view or table it made a false edge (a cycle) and a false re-typed read. It is the
    # statement's own only within the bracket level that defines it (and the levels inside
    # it): a nested WITH named like an outer read leaves that read a real relation.
    ctes: dict[str, list[tuple[int, int]]] = {}
    body_ends = {start: end for start, end in spans}
    for m in _CTE_DEFINITION_RE.finditer(masked):
        parts = _chain_parts(m.group(1))[0]
        if not parts:
            continue
        at = m.start(1)
        level = min(
            (span for span in spans if span[0] <= at < span[1]),
            key=lambda span: span[1] - span[0],
            default=(0, len(masked)),
        )
        # Without RECURSIVE a CTE joins the namespace after its own body: its body and the
        # CTEs before it read the real relation of that name. RECURSIVE is the WITH's that
        # opens this CTE list -- the last one at this level before the name -- never a
        # nested WITH RECURSIVE inside an earlier sibling's body.
        withs = [rec for pos, rec in level_withs.get(level, ()) if pos < at]
        recursive = bool(withs) and withs[-1]
        body_end = body_ends.get(m.end() - 1, at)
        ctes.setdefault(parts[-1], []).append(level if recursive else (body_end, level[1]))
    if ctes:
        relations = [
            (schema, bare)
            for (schema, bare), at in zip(relations, relation_at)
            if schema is not None
            or not any(start <= at < end for start, end in ctes.get(bare, ()))
        ]
    return _SqlNames(names=names, relations=relations, items=items, masked=masked)


# ``name [(columns)] AS [NOT] [MATERIALIZED] (``: only a CTE definition reads so (a derived
# table is ``) AS name``, a column alias ``expr AS name``).
# One way to read the blanks between the name and AS (a masked literal is a run of them):
# the optional column list carries its own trailing blanks, never a second \s* beside it.
_CTE_DEFINITION_RE = re.compile(
    r'(?:^|[\s,(])("(?:[^"]|"")+"|[A-Za-z_][\w$]*)\s*(?:\([^()]*\)\s*)?\bAS\s+'
    r"(?:NOT\s+)?(?:MATERIALIZED\s+)?\(",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------------------
# Splitting a script into statements, and its text without comments.
# ---------------------------------------------------------------------------------------

_STATEMENT_SCAN_RE = re.compile(r"""['"$;]|--|/\*|[eE](?=')""")


def sql_statement_spans(text: str) -> "list[tuple[int, int]]":
    """The ``(start, end)`` of each statement of a script: split at each ``;`` outside a
    string literal, a quoted identifier, a dollar-quoted body or a comment -- a carried
    index predicate (``WHERE sep <> ';'``) is one statement, not two broken halves."""
    spans: list[tuple[int, int]] = []
    start = pos = 0
    while True:
        match = _STATEMENT_SCAN_RE.search(text, pos)
        if match is None:
            break
        at = match.start()
        if match.group(0) == ";":
            spans.append((start, at))
            start = pos = at + 1
            continue
        skipped = _sql_skip(text, at)
        pos = at + 1 if skipped is None else skipped[1]
    spans.append((start, len(text)))
    return spans


def sql_without_comments(text: str) -> str:
    """``text`` with its comments removed (``--`` and ``/* */``, outside literals and
    quoted identifiers), everything else kept as written -- a ``/* */`` comment as one
    space, as PostgreSQL reads it (``bigint/* was int */NOT NULL`` is two words)."""
    out, last, pos = [], 0, 0
    while True:
        match = _STATEMENT_SCAN_RE.search(text, pos)
        if match is None:
            break
        at = match.start()
        if match.group(0) == ";":
            pos = at + 1
            continue
        skipped = _sql_skip(text, at)
        if skipped is None:
            pos = at + 1
            continue
        if skipped[0] == "comment":
            out.append(text[last:at])
            if text.startswith("/*", at):
                out.append(" ")
            last = skipped[1]
        pos = skipped[1]
    out.append(text[last:])
    return "".join(out)


def terminated_sql_script(script: str) -> str:
    """``script`` with its last statement terminated when it is not (an edit that omits
    the final ``;``), so text joined after it starts a statement of its own."""
    start, end = sql_statement_spans(script)[-1]
    tail = script[start:end]
    if not sql_without_comments(tail).strip():
        return script
    return script[:start] + terminated_sql_statement(tail)


def terminated_sql_statement(statement: str) -> str:
    """``statement`` with one ``;`` that :func:`sql_statement_spans` splits at -- on its own
    line when the statement's last line is a ``--`` comment, which would swallow it and
    glue the next statement on, so a split -> join -> split round trip keeps every one."""
    body = statement.rstrip().rstrip(";")
    candidate = f"{body};"
    spans = sql_statement_spans(candidate)
    if len(spans) >= 2 and spans[-2][1] == len(body):
        return candidate
    return f"{body}\n;"
