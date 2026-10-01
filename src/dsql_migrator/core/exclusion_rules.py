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
from dataclasses import dataclass
from typing import Optional

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
    ``type:`` list (``None`` for every other error)."""

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


__all__ = [
    "ParsedRule",
    "RuleError",
    "RulePart",
    "Suggestion",
    "parse_rules",
    "rule_part_token",
    "split_rule_comment",
    "type_name_token",
]
