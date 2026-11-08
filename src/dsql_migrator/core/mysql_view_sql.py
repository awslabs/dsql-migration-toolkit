# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""MySQL view and query SQL -> Aurora DSQL (PostgreSQL 16): the expression rewriter.

Schema Conversion converts a MySQL view's body with it: ``SchemaConverter.convert_view``
(which Evaluation's VIEW_NOT_CONVERTED rule also runs) calls :func:`_converted_mysql_view`.
The Query Converter reads and rewrites a MySQL statement with it
(:data:`MYSQL_EXPRESSION_DIALECT`, :func:`rewrite_mysql_expressions`). It holds:

- :class:`_MySqlViewDialect`, the sqlglot MySQL dialect a view or a query is read with;
- the column typing the rewrites consult (:class:`_ColumnTypes`, :func:`table_type_index`,
  :func:`mysql_type_index`, :func:`view_column_types`);
- the call rewrites (:func:`_rewrite_mysql_calls`), the HAVING alias inlining
  (:func:`inline_having_aliases`) and the JSON extract unquoting
  (:func:`unquote_json_extracts`);
- the view conversion itself (:func:`_converted_mysql_view`, cached).

It was split out of ``core/converter.py``, which re-exports every name defined here
(private ones too), so ``from dsql_migrator.core.converter import ...`` keeps resolving.
This module must not import the converter at its top level: the converter imports this
module, and a caller can import this module first. The one name it needs from the
converter, ``map_mysql_type``, is imported where it is called.
"""

from __future__ import annotations

import datetime
import decimal
import functools
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Callable, Optional

import sqlglot
from sqlglot import exp
from sqlglot.dialects.mysql import MySQL as _SqlglotMySQL
from sqlglot.helper import seq_get
from sqlglot.tokens import TokenType

from dsql_migrator.core.models import TableDef, ViewDef
from dsql_migrator.core.mysql_zero_dates import dateless_kind
from dsql_migrator.core.source_time_zone import SourceZone, written_zone_name

_POSTGRES = "postgres"


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


# Calls sqlglot parses into a typed node but writes for PostgreSQL under a name PostgreSQL
# does not have (YEAR(), SUBSTRING_INDEX(), JSON_KEYS(), ...) -- by the MySQL name they
# are reported under -- so the CREATE VIEW fails. The simple ones are rewritten instead
# (:func:`_rewrite_mysql_calls`).
def _node_names(**names: str) -> "dict[type, str]":
    """``{sqlglot node class: MySQL name}`` for the classes this sqlglot has."""
    return {getattr(exp, cls): name for cls, name in names.items() if hasattr(exp, cls)}


_UNTRANSLATED_MYSQL_NODES: "dict[type, str]" = _node_names(
    Week="WEEK",
    TimestampDiff="TIMESTAMPDIFF",
    Elt="ELT",
    SubstringIndex="SUBSTRING_INDEX",
    NumberToStr="FORMAT",
    Hex="HEX",
    Unhex="UNHEX",
    JSONKeys="JSON_KEYS",
    JSONType="JSON_TYPE",
    JSONObjectAgg="JSON_OBJECTAGG",
    JSONSet="JSON_SET",
    JSONRemove="JSON_REMOVE",
    JSONArrayAppend="JSON_ARRAY_APPEND",
    JSONArrayInsert="JSON_ARRAY_INSERT",
    JSONValue="JSON_VALUE",
    Soundex="SOUNDEX",
    ToBase64="TO_BASE64",
    FromBase64="FROM_BASE64",
    BitwiseCount="BIT_COUNT",
    Stuff="INSERT",
    SHA="SHA1",
    SHA2="SHA2",
    CurrentRole="CURRENT_ROLE",
    Collation="COLLATION",
    Compress="COMPRESS",
    StDistance="ST_DISTANCE",
)

# Calls MySQL has no built-in for that sqlglot reads as PostgreSQL's own (a user function
# date_trunc or to_char, written without its schema): the source's, unless this rewrite
# wrote the node (:data:`_PG_WRITTEN`).
_PG_ONLY_NODES: "dict[type, str]" = _node_names(
    DateTrunc="date_trunc",
    TimestampTrunc="date_trunc",
    ToChar="to_char",
    Translate="translate",
    ArrayToString="array_to_string",
)

# MySQL functions sqlglot leaves as written (Anonymous) that PostgreSQL / Aurora DSQL has
# under the same name AND meaning, by argument count (None: any). Not json_array /
# json_arrayagg: PostgreSQL's drop NULL elements (ABSENT ON NULL), MySQL's keep them --
# and not now(): MySQL's is a DATETIME of whole seconds, PostgreSQL's a timestamptz with
# microseconds. They are rewritten instead.
_PG_BUILTIN_ANONYMOUS: "dict[str, Optional[int]]" = {
    "octet_length": None,
    "regexp_substr": None,
    "regexp_instr": None,
    "regexp_count": None,
}
# The meta key marking a call :func:`_rewrite_mysql_calls` writes itself (PostgreSQL's own):
# by the node, not its name -- a MySQL user function named date_trunc or to_char is the
# source's, and still reported. Copies keep it.
_PG_WRITTEN = "pg_written"
# The meta key marking the ROUND that shows an exact value at MySQL's scale (``shown``) --
# "branches" on a CASE / IF whose branches are shown each.
_SHOWN = "mysql_shown"
# The meta key marking the ROUND that shows a COALESCE's value at the scale of its widest
# value (MySQL's IFNULL(price, 0) is 0.00): JSON writes the value itself, as MySQL does.
_WIDENED = "mysql_widened"
# The meta key marking a wall clock of the source's time zone the rewrite wrote (G149 /
# G157, :func:`_zone_clock`): the clock, FROM_UNIXTIME(), a TIMESTAMP read as MySQL reads it.
_ZONE_CLOCK = "zone_clock"
# The note for LENGTH of a value of a type that cannot be told, counted as x || ''.
_UNTYPED_TEXT_NOTE = (
    "LENGTH / CHAR_LENGTH reads a value whose type cannot be told here -- a column of another "
    "view, of a derived table or CTE that computes it, or (in the Query Converter) of a table "
    "Step 1 has not read: it counts the text of x || '' (text for every type, bytes kept "
    "bytes), MySQL's count for text, bytes, integers, decimals and dates, but not for a "
    "TINYINT(1) (a boolean on Aurora DSQL: 'true' / 'false'), a FLOAT (MySQL writes 6 "
    "significant digits), a TIMESTAMP (its text there carries the zone) or a DATETIME / TIME "
    "with fractional digits (trailing zeros dropped)."
)
# The meta key holding how many [0] steps a followed JSON path's value holds, its
# document's included (each step holds its value three times: _MAX_ZERO_STEPS).
_ZERO_STEPS = "mysql_zero_steps"
# The meta key holding ``(shown scale, digits kept)`` of a quotient (an AVG) written as
# MySQL's exact division.
_QUOTIENT_SCALE = "mysql_scale"
# The meta key holding how many GREATEST / LEAST nests a NULL test of the rewrite holds,
# each inside the one before.
_NEST_COPIES = "mysql_nest_copies"
# The meta key marking a join written with a comma (which binds looser than JOIN).
_COMMA_JOIN = "mysql_comma_join"
# The meta key marking an interval amount MySQL read as a number ('decimal' / 'double'):
# sqlglot makes INTERVAL 1.5 DAY's amount the string INTERVAL '1.5' DAY has, and MySQL
# counts the two differently.
_MYSQL_NUMBER = "mysql_number"


def _number_kind(node: exp.Expression) -> str:
    """'double' for a number literal MySQL reads as a DOUBLE (with an exponent), else
    'decimal'."""
    literal = node.this if isinstance(node, exp.Neg) else node
    return "double" if "e" in literal.name.lower() else "decimal"

# EXTRACT field of each MySQL date-part call (an integer on both engines). WEEKOFYEAR is
# the ISO week on both.
_EXTRACT_FIELDS: "dict[type, str]" = {
    exp.Year: "YEAR",
    exp.Month: "MONTH",
    exp.Day: "DAY",
    exp.DayOfMonth: "DAY",
    exp.DayOfYear: "DOY",
    exp.Quarter: "QUARTER",
    exp.Hour: "HOUR",
    exp.Minute: "MINUTE",
    **_node_names(WeekOfYear="WEEK"),
}
# MySQL calls that return an integer, whatever they read: the date parts (before they are
# written as EXTRACT) and the window ranks.
_INTEGER_NODES: "tuple[type, ...]" = (
    *_EXTRACT_FIELDS, exp.DayOfWeek, exp.Second, exp.Week,
    *_node_names(RowNumber="", Rank="", DenseRank="", Ntile=""),
)


def _raw_mysql_call(name: str):
    """A parser builder that keeps ``name(...)`` as MySQL wrote it (an Anonymous call), for
    :func:`_rewrite_mysql_calls` to translate from MySQL's own spelling."""
    return lambda args: exp.Anonymous(this=name, expressions=args)


def _from_unixtime(args: list) -> exp.Expression:
    """FROM_UNIXTIME(x) as sqlglot reads it; with a format, kept as MySQL wrote it."""
    if len(args) > 1:
        return exp.Anonymous(this="FROM_UNIXTIME", expressions=args)
    return exp.UnixToTime(this=seq_get(args, 0))


def _mysql_assignments(holder: exp.Expression) -> None:
    """An UPDATE's or an upsert's SET items as MySQL reads them, in place: SET col = a = b is
    col = (a = b) (col = a = 1 = 0 is col = ((a = 1) = 0)) -- sqlglot reads every equality
    (=, <>, <=>) left to right, ((col = a) = b), the target an operand there."""
    for item in list(holder.expressions or ()):
        spine: list = []
        target = item
        while isinstance(target, _EQUALITIES) and not (
            isinstance(target, exp.EQ) and isinstance(target.this, exp.Column)
        ):
            spine.append(target)
            target = target.this
        if spine and isinstance(target, exp.EQ) and isinstance(target.this, exp.Column):
            spine[-1].set("this", target.expression)  # (a = b), the value
            item.replace(target)
            target.set("expression", item)


class _MySqlViewDialect(_SqlglotMySQL):
    """MySQL as the tool reads a view body or a query.

    * VARIANCE / VAR_POP and STD / STDDEV / STDDEV_POP are the POPULATION statistics MySQL
      computes, VAR_SAMP / STDDEV_SAMP the sample ones -- sqlglot reads VARIANCE and
      VAR_SAMP alike (and STDDEV as the sample one), which no later rewrite can tell apart.
    * DATE_FORMAT / STR_TO_DATE / FROM_UNIXTIME(x, fmt) keep the format MySQL wrote:
      sqlglot re-spells it in its own strftime tokens, which loses ``%%`` and maps tokens
      MySQL does not know.
    * ``CAST(x AS CHAR(n) CHARSET cs)`` keeps its length, which sqlglot drops.
    * An interval's number amount (INTERVAL 1.5 DAY) is marked as one: sqlglot reads it as
      the string of INTERVAL '1.5' DAY, which MySQL counts otherwise.
    * A comma join is marked as one: sqlglot builds the same node for a JOIN without ON,
      which binds as JOIN does.
    * ``%`` / MOD bind as ``*`` and ``/`` do, left to right: sqlglot binds them as ``+``
      and ``-``, so ``a % 3 / 7`` read as ``a % (3 / 7)`` and ``a + b % c`` as ``(a + b) %
      c`` (MySQL stores a view's parentheses, so only a statement read otherwise).
    * ``TIMESTAMP '...'`` is a DATETIME, no time zone: sqlglot reads MySQL's TIMESTAMP as
      the zoned column type. One with a UTC offset ('... 10:00:00+05:00') is the session's
      wall clock at that instant.
    * ``j -> '$.a'`` is JSON_EXTRACT in a function's arguments too: MySQL has no lambdas,
      and sqlglot read it there as one -- written back as PostgreSQL's ``->``, a lookup
      of the key '$.a' (NULL on every row).
    * An UPDATE's or an upsert's ``SET col = a = b`` assigns ``(a = b)`` (``<>`` and ``<=>``
      too): sqlglot reads the equalities left to right, ``((col = a) = b)``, the target an
      operand there.
    Written back as MySQL (the Schema Conversion source pane), each reads as it did."""

    class Parser(_SqlglotMySQL.Parser):
        TERM = {token: node for token, node in _SqlglotMySQL.Parser.TERM.items() if token != TokenType.MOD}
        FACTOR = {**_SqlglotMySQL.Parser.FACTOR, TokenType.MOD: exp.Mod}
        LAMBDAS = {
            token: build for token, build in _SqlglotMySQL.Parser.LAMBDAS.items() if token != TokenType.ARROW
        }
        TYPE_LITERAL_PARSERS = {
            **_SqlglotMySQL.Parser.TYPE_LITERAL_PARSERS,
            exp.DataType.Type.TIMESTAMPTZ: lambda self, this, _: self.expression(exp.Cast(
                this=exp.Cast(this=this, to=exp.DataType.build("TIMESTAMPTZ"))
                if _DATETIME_OFFSET_RE.search(this.name) else this,
                to=exp.DataType.build("DATETIME"),
            )),
        }
        FUNCTIONS = {
            **_SqlglotMySQL.Parser.FUNCTIONS,
            "VARIANCE": exp.VariancePop.from_arg_list,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "VAR_SAMP": exp.Variance.from_arg_list,
            "STD": exp.StddevPop.from_arg_list,
            "STDDEV": exp.StddevPop.from_arg_list,
            "STDDEV_POP": exp.StddevPop.from_arg_list,
            "STDDEV_SAMP": exp.StddevSamp.from_arg_list,
            "DATE_FORMAT": _raw_mysql_call("DATE_FORMAT"),
            "STR_TO_DATE": _raw_mysql_call("STR_TO_DATE"),
            "FROM_UNIXTIME": _from_unixtime,
            # MySQL stores a view's DATEDIFF(a, b) as to_days(a) - to_days(b); sqlglot
            # reads TO_DAYS as a difference from '0000-01-01', a date Aurora DSQL rejects.
            "TO_DAYS": _raw_mysql_call("TO_DAYS"),
        }

        def _parse_types(self, *args: Any, **kwargs: Any) -> Any:
            parsed = super()._parse_types(*args, **kwargs)
            self._mysql_last_type = parsed
            return parsed

        def _parse_interval_span(self, this: Any) -> Any:
            number = _number_kind(this) if this is not None and this.is_number else None
            # MySQL's unit is the word after the amount, always: INTERVAL '3 days' DAY is 3
            # days (the string read by its leading integer), where sqlglot takes the unit
            # inside the string and reads the DAY after it as an alias.
            written = this if this is not None and this.is_string else None
            interval = super()._parse_interval_span(exp.Literal.string("0") if written is not None else this)
            if written is not None:
                interval.set("this", written)
            if number is not None and isinstance(interval.this, exp.Literal):
                interval.this.meta[_MYSQL_NUMBER] = number
            return interval

        def _parse_join(self, *args: Any, **kwargs: Any) -> Any:
            # A comma join, which binds looser than JOIN: sqlglot builds the same node for
            # it as for a JOIN without ON, which MySQL reads as JOIN.
            comma = self._curr is not None and self._curr.token_type == TokenType.COMMA
            join = super()._parse_join(*args, **kwargs)
            if comma and join is not None:
                join.meta[_COMMA_JOIN] = True
            return join

        def _parse_update(self) -> Any:
            update = super()._parse_update()
            _mysql_assignments(update)
            return update

        def _parse_on_conflict(self) -> Any:
            conflict = super()._parse_on_conflict()  # ON DUPLICATE KEY UPDATE's SET items
            if conflict is not None:
                _mysql_assignments(conflict)
            return conflict

        def _parse_convert(self, strict: bool, safe: Any = None) -> Any:
            # CONVERT(x USING cs) reads no type of its own: a CHAR(n) read earlier (a
            # JSON_TABLE column, another cast) is not its length.
            self._mysql_last_type = None
            return super()._parse_convert(strict, safe)

        def build_cast(self, strict: bool, **kwargs: Any) -> exp.Expression:
            # CAST(x AS CHAR(n) CHARSET cs): sqlglot replaces the CHAR(n) it just read
            # with a CHARACTER SET type and drops n -- MySQL's truncation to n characters.
            to = kwargs.get("to")
            last = getattr(self, "_mysql_last_type", None)
            if (
                isinstance(to, exp.DataType)
                and to.this == exp.DataType.Type.CHARACTER_SET
                and isinstance(last, exp.DataType)
                and last.this == exp.DataType.Type.CHAR
                and last.expressions
            ):
                to.set("expressions", [e.copy() for e in last.expressions])
            self._mysql_last_type = None
            return super().build_cast(strict, **kwargs)

    class Generator(_SqlglotMySQL.Generator):
        TRANSFORMS = {
            **_SqlglotMySQL.Generator.TRANSFORMS,
            exp.Variance: lambda self, e: self.func("VAR_SAMP", e.this),
            exp.VariancePop: lambda self, e: self.func("VAR_POP", e.this),
            exp.StddevPop: lambda self, e: self.func("STDDEV_POP", e.this),
            exp.StddevSamp: lambda self, e: self.func("STDDEV_SAMP", e.this),
        }

        def datatype_sql(self, expression: exp.DataType) -> str:
            if expression.this == exp.DataType.Type.CHARACTER_SET and expression.expressions:
                return (
                    f"CHAR({self.expressions(expression, flat=True)}) CHARACTER SET "
                    f"{self.sql(expression, 'kind')}"
                )
            return super().datatype_sql(expression)


def _as_integer(node: exp.Expression) -> exp.Expression:
    return exp.Cast(this=node, to=exp.DataType.build("INT"))


def _any_text(node: exp.Expression) -> exp.Expression:
    """``node || ''``: the text of a value of any type on Aurora DSQL (bytes kept bytes)."""
    if isinstance(node, (exp.Binary, exp.Unary, exp.Predicate)) and not isinstance(node, exp.Paren):
        node = exp.Paren(this=node)
    return exp.DPipe(this=node, expression=exp.Literal.string(""))


def _texted(node: exp.Expression) -> exp.Expression:
    """``node`` as text: a string literal already is, and a concatenation's parts are made
    text (``int || int`` has no operator)."""
    if isinstance(node, exp.Literal) and node.is_string:
        return node
    if isinstance(node, exp.Concat):
        node.set("expressions", [_texted(e) for e in node.expressions])
        return node
    return exp.Cast(this=node, to=exp.DataType.build("TEXT"))


def _innermost_first(tree: exp.Expression, *types: type) -> "list[exp.Expression]":
    """The nodes of ``types`` in ``tree``, each after the ones nested in it: a rewrite that
    copies its arguments then copies arguments already rewritten (outermost first, the
    inner node rewritten later sat in a copy no longer in the tree)."""
    return list(reversed(list(tree.find_all(*types))))


# A DATE_FORMAT / STR_TO_DATE specifier as MySQL writes it -> the PostgreSQL TO_CHAR /
# TO_DATE / TO_TIMESTAMP pattern, for the specifiers with a faithful one (English names,
# as MySQL's default lc_time_names). Checked against MySQL 8.4 and PostgreSQL 17.
_MYSQL_FORMAT_TO_PG: "dict[str, str]" = {
    "a": "Dy", "b": "Mon", "c": "FMMM", "D": "FMDDth", "d": "DD", "e": "FMDD", "f": "US",
    "H": "HH24", "h": "HH12", "I": "HH12", "i": "MI", "j": "DDD", "k": "FMHH24",
    "l": "FMHH12", "M": "FMMonth", "m": "MM", "p": "AM", "r": "HH12:MI:SS AM", "S": "SS",
    "s": "SS", "T": "HH24:MI:SS", "v": "IW", "W": "FMDay", "x": "IYYY", "Y": "YYYY",
    "y": "YY",
}
# Parsing (STR_TO_DATE), MySQL reads the unpadded numeric specifiers like the padded ones
# -- at most two digits -- where an FM field would read every digit that follows.
_MYSQL_PARSE_FORMAT_TO_PG: "dict[str, str]" = {
    **_MYSQL_FORMAT_TO_PG, "c": "MM", "e": "DD", "k": "HH24", "l": "HH12",
}
# MySQL's week numbers other than ISO's, the year for them, its 0-based day of week: no
# PostgreSQL pattern.
_MYSQL_FORMAT_NO_PG = frozenset("UuVXw")
# STR_TO_DATE returns a TIME for a format with only time specifiers (%f counts as one), a
# DATETIME for one with both, a DATE otherwise -- with microseconds when it reads %f
# (MySQL's own rule, get_date_time_result_type).
_MYSQL_TIME_SPECIFIERS = frozenset("HISThiklrsf")
_MYSQL_DATE_SPECIFIERS = frozenset("MVUXYWabcjmvuxyw")


def _mysql_format_specifiers(mysql_format: str) -> "list[str]":
    """The specifier letters of a MySQL format, in order (``%%`` is none)."""
    out: list[str] = []
    i = 0
    while i < len(mysql_format):
        if mysql_format[i] == "%" and i + 1 < len(mysql_format):
            if mysql_format[i + 1] != "%":
                out.append(mysql_format[i + 1])
            i += 2
            continue
        i += 1
    return out


def _mysql_format_to_pg(mysql_format: str, *, parsing: bool = False) -> "tuple[str, list[str]]":
    """``(PostgreSQL pattern, MySQL specifiers it cannot write)`` for a format MySQL wrote.

    Each specifier is mapped; ``%%`` is a ``%``, and a specifier MySQL does not know is the
    character itself, as MySQL prints it. A literal run holding a letter is double-quoted
    (a 'T' or a 'Date: ' is not read as a pattern), and two patterns side by side are kept
    apart with an empty ``""`` (``%d%a`` is DD then Dy, not DDD then y). Parsing
    (STR_TO_DATE), the unpadded numeric specifiers read at most two digits as in MySQL,
    and a ``%Y`` that a separator or the end follows reads as ``YY``, which reads all four
    digits there and two-digit years the way MySQL does (69 -> 2069, 70 -> 1970)."""
    table = _MYSQL_PARSE_FORMAT_TO_PG if parsing else _MYSQL_FORMAT_TO_PG
    out: list[str] = []
    unmapped: list[str] = []
    literal: list[str] = []
    last_pattern = False

    def flush() -> None:
        nonlocal last_pattern
        if literal:
            text = "".join(literal)
            if any(ch.isalpha() or ch in '"\\' for ch in text):
                text = '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'
            out.append(text)
            literal.clear()
            last_pattern = False

    i = 0
    while i < len(mysql_format):
        ch = mysql_format[i]
        if ch == "%" and i + 1 < len(mysql_format):
            spec = mysql_format[i + 1]
            i += 2
            if spec in table:
                flush()
                pattern = table[spec]
                if parsing and spec == "Y" and (
                    i >= len(mysql_format)
                    or not (mysql_format[i].isalnum() or mysql_format[i] == "%")
                ):
                    pattern = "YY"
                if last_pattern:
                    out.append('""')
                out.append(pattern)
                last_pattern = True
            elif spec in _MYSQL_FORMAT_NO_PG:
                # Written as the literal MySQL specifier, so the wrong text is visible, and
                # reported.
                literal.append(f"%{spec}")
                if f"%{spec}" not in unmapped:
                    unmapped.append(f"%{spec}")
            else:
                literal.append(spec)
            continue
        literal.append(ch)
        i += 1
    flush()
    return "".join(out), unmapped


def _pg_format(sqlglot_format: str) -> "tuple[str, list[str]]":
    """:func:`_mysql_format_to_pg` for a format sqlglot made itself (MONTHNAME's), in its
    strftime-like spelling."""
    respelled = (
        sqlglot_format.replace("%B", "%M").replace("%-d", "%e").replace("%A", "%W")
    )
    return _mysql_format_to_pg(respelled)


def _format_leaves(node: exp.Expression) -> "list[exp.Expression]":
    """The values a format expression can take: the branches of an IF / CASE (never their
    conditions), else the expression itself."""
    if isinstance(node, exp.Paren):
        return _format_leaves(node.this)
    if isinstance(node, exp.If):
        leaves = _format_leaves(node.args["true"])
        false = node.args.get("false")
        return leaves + (_format_leaves(false) if false is not None else [])
    if isinstance(node, exp.Case):
        leaves = [leaf for branch in node.args.get("ifs") or () for leaf in _format_leaves(branch.args["true"])]
        default = node.args.get("default")
        return leaves + (_format_leaves(default) if default is not None else [])
    return [node]


_UTC_OFFSET_RE = re.compile(r"^[+-]\d{1,2}:\d{2}$")
# A MySQL datetime literal's UTC offset (8.0.19+: '2024-01-31 10:00:00+05:00').
_DATETIME_OFFSET_RE = re.compile(r"\d:\d\d(?::\d\d(?:\.\d*)?)?[+-]\d\d:\d\d\s*$")
# UTC_DATE / UTC_TIMESTAMP / UTC_TIME, for the classes this sqlglot has.
_UTC_CLOCKS: "tuple[type, ...]" = tuple(_node_names(UtcDate="", UtcTimestamp="", UtcTime=""))
# NOW()'s synonyms as sqlglot reads them from a statement (a view stores each as now()).
_NOW_SYNONYMS: "tuple[type, ...]" = tuple(_node_names(Localtimestamp="", Localtime=""))

# A MySQL character set -> the PostgreSQL encoding its bytes decode from (MySQL's latin1
# is Windows-1252). A charset not listed is reported, never decoded as UTF-8.
_PG_ENCODINGS = {
    "utf8": "UTF8", "utf8mb3": "UTF8", "utf8mb4": "UTF8", "ascii": "UTF8",
    "latin1": "WIN1252", "latin2": "LATIN2", "cp1250": "WIN1250", "cp1251": "WIN1251",
    "cp1256": "WIN1256", "cp1257": "WIN1257", "koi8r": "KOI8R", "koi8u": "KOI8U",
    "greek": "ISO_8859_7", "hebrew": "ISO_8859_8", "latin5": "LATIN5", "latin7": "LATIN7",
    "euckr": "EUC_KR", "sjis": "SJIS", "cp932": "SJIS", "ujis": "EUC_JP",
    "eucjpms": "EUC_JP", "gbk": "GBK", "gb2312": "EUC_CN", "gb18030": "GB18030",
    "big5": "BIG5", "tis620": "WIN874",
}

_COMPARISONS = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike, exp.In)
# MySQL's predicates, each an integer 0 / 1 there -- a boolean on Aurora DSQL.
_PREDICATES = (
    *_COMPARISONS, exp.NullSafeEQ, exp.NullSafeNEQ, exp.Between, exp.Is, exp.Not, exp.And, exp.Or,
    exp.Xor, exp.RegexpLike, exp.Exists,
)
# The numeric functions MySQL reads a string's number in.
_NUMERIC_FUNCTIONS = (exp.Abs, exp.Ceil, exp.Floor, exp.Sign, exp.Pow, exp.Sqrt, exp.Exp, exp.Ln, exp.Log)
_DATE_UNITS = frozenset({"DAY", "WEEK", "MONTH", "QUARTER", "YEAR"})
_NUMERIC_KINDS = frozenset({"integer", "decimal", "float4", "float8"})
_EXACT_KINDS = frozenset({"integer", "decimal"})
_FLOAT_KINDS = frozenset({"float4", "float8"})
_TEMPORAL_KINDS = frozenset({"date", "datetime", "timestamp", "time"})
# The kinds MySQL writes as text where a string is wanted (as_text): all but text and bytes.
_TEXTED_KINDS = _NUMERIC_KINDS | _TEMPORAL_KINDS | {"boolean", "char", "json"}
# The kinds whose values order as their text's bytes do (Aurora DSQL's C collation): MySQL's
# BINARY x orders them as x itself. A number's digits do not (12 before 3).
_BYTE_ORDERED_KINDS = _TEMPORAL_KINDS | {"text", "char", "binary", "boolean"}
# The kinds known not to be text: a string compared with one is converted to it in MySQL.
_UNTEXTED_KINDS = _TEMPORAL_KINDS | {"boolean", "json", "binary"}
# The nodes that are NULL exactly when an operand is, in MySQL and as written for Aurora
# DSQL alike -- a division and a modulo also when the divisor is 0 (:func:`_null_strict`).
_NULL_STRICT = (
    exp.Paren, exp.Neg, exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.IntDiv, exp.Abs,
    exp.Round, exp.Trunc, exp.Floor, exp.Ceil, exp.Concat,
)
# The aggregates that read their argument as a number.
_NUMERIC_AGGREGATES = (
    exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Stddev, exp.StddevPop, exp.StddevSamp, exp.Variance,
    exp.VariancePop,
)
# The operations that read their operands as numbers -- a TINYINT(1) there is its integer:
# arithmetic, the bitwise operators (not ~, MySQL's 64-bit unsigned NOT), the numeric
# functions and aggregates.
_NUMERIC_OPERATIONS = (
    exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.IntDiv, exp.Neg, exp.BitwiseAnd, exp.BitwiseOr,
    exp.BitwiseXor, exp.BitwiseLeftShift, exp.BitwiseRightShift, exp.Abs, exp.Round, exp.Trunc,
    exp.Floor, exp.Ceil, exp.Sign, exp.Pow, exp.Sqrt, exp.Exp, exp.Ln, exp.Log, *_NUMERIC_AGGREGATES,
)
# What a left-deep chain of thousands of operators (a + b + c is ((a + b) + c)) is made of:
# the typing reads walk one in a loop, not a Python frame per operator.
_CHAINED = (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)
# The equality operators, which sqlglot reads left to right as MySQL does ((a = b) = c).
_EQUALITIES = (exp.EQ, exp.NEQ, exp.NullSafeEQ, exp.NullSafeNEQ)
# MySQL's window functions that return a row's value of their argument.
_VALUE_WINDOW_FUNCTIONS = (exp.Lag, exp.Lead, exp.FirstValue, exp.LastValue, exp.NthValue)
# The nodes whose value has their argument's scale, and carries its quotient: -x, SUM, MIN /
# MAX, ANY_VALUE, ABS, NULLIF, a window, a window's row value.
_ARGUMENT_SCALED = (
    exp.Neg, exp.Sum, exp.Min, exp.Max, exp.AnyValue, exp.Abs, exp.Nullif, exp.Window, *_VALUE_WINDOW_FUNCTIONS,
)
# The deepest a typing read walks into an expression: a deeper one is of unknown kind, and
# reported -- each level is a Python frame (a chain of operators is walked in a loop).
_MAX_TYPED_DEPTH = 200
# How many GREATEST / LEAST nests may sit inside each other behind a node that is not
# NULL-strict (IFNULL, IF, CASE, a function): each such level copies the inner nest into
# its own NULL test, doubling it, so deeper ones are reported instead.
_NEST_COPY_LIMIT = 3
# MySQL's composite interval units, by their parts, leftmost first.
_COMPOSITE_UNITS: "dict[str, tuple[str, ...]]" = {
    "YEAR_MONTH": ("year", "month"),
    "DAY_HOUR": ("day", "hour"),
    "DAY_MINUTE": ("day", "hour", "minute"),
    "DAY_SECOND": ("day", "hour", "minute", "second"),
    "DAY_MICROSECOND": ("day", "hour", "minute", "second", "microsecond"),
    "HOUR_MINUTE": ("hour", "minute"),
    "HOUR_SECOND": ("hour", "minute", "second"),
    "HOUR_MICROSECOND": ("hour", "minute", "second", "microsecond"),
    "MINUTE_SECOND": ("minute", "second"),
    "MINUTE_MICROSECOND": ("minute", "second", "microsecond"),
    "SECOND_MICROSECOND": ("second", "microsecond"),
}


def _null_strict(node: exp.Expression) -> bool:
    """Whether ``node`` is NULL exactly when an operand is (arithmetic, ABS, ROUND, FLOOR,
    CONCAT, a cast to a number or text), as MySQL computes it and as Aurora DSQL computes
    what the conversion writes -- a division (NULLIF(divisor, 0)) also for a 0 divisor, and
    NULLIF(x, v) when x is v."""
    if isinstance(node, _NULL_STRICT):
        return True
    if isinstance(node, exp.Nullif):
        return isinstance(node.expression, exp.Literal)
    return isinstance(node, exp.Cast) and _pg_type_kind(node.args.get("to")) in (_NUMERIC_KINDS | {"text"})


def _mysql_quotient_digits(dividend: int, divisor: int) -> int:
    """The fraction digits MySQL's exact division computes with -- compares, multiplies and
    writes in JSON -- truncated, for operands of these fraction digits (do_div_mod): each
    rounded up to whole 9-digit words, plus div_precision_increment (4) less what that
    rounding added, in whole words. 9 for integers (2 / 3 is 0.666666666), and for 1.5 / 7
    too; 18 for 1.25 / 1.5 or a quotient divided again."""
    words1, words2 = 9 * -(-dividend // 9), 9 * -(-divisor // 9)
    increment = max(0, 4 - (words1 - dividend) - (words2 - divisor))
    return 9 * -(-(words1 + words2 + increment) // 9)


def _mysql_interval_parts(text: str, unit: str) -> Optional[str]:
    """A composite interval amount (INTERVAL '1 2' DAY_HOUR) as MySQL reads it, written as
    an Aurora DSQL interval: a '-' before it negates the whole, any characters but digits
    separate the parts, parts left out are the leftmost ('1:30' DAY_MINUTE is 1 hour 30
    minutes), and a microsecond part of fewer than 6 digits is a fraction ('1.5'
    SECOND_MICROSECOND is 1.5 seconds), of 6 or more a count. YEAR_MONTH is its months.
    None for more parts than the unit has, which MySQL reads as NULL."""
    names = _COMPOSITE_UNITS[unit]
    body = text.lstrip()
    negative = body.startswith("-")
    runs = re.findall(r"\d+", body[1:] if negative else body)
    if len(runs) > len(names):
        return None
    values = [0] * (len(names) - len(runs)) + [int(run) for run in runs]
    if names[-1] == "microsecond" and runs and len(runs[-1]) < 6:
        values[-1] *= 10 ** (6 - len(runs[-1]))
    sign = -1 if negative else 1
    if unit == "YEAR_MONTH":
        return str(sign * (values[0] * 12 + values[1]))
    return " ".join(f"{sign * value} {name}" for value, name in zip(values, names))


def _mysql_materializes(query: exp.Expression) -> bool:
    """Whether MySQL materializes a view, derived table or CTE body -- computes its rows
    into a temporary table, each output at its type's scale -- rather than merging it into
    the query that reads it, which then computes with every digit a quotient carries: a
    UNION, GROUP BY, HAVING, DISTINCT, LIMIT, an aggregate or window of its own, no table,
    or an output holding a subquery that is not constant -- one that reads this query's own
    row (MySQL 8.4's merge rule, checked with EXPLAIN; (SELECT MAX(x) FROM t) AS m merges)."""
    while isinstance(query, exp.Subquery):
        query = query.this
    if not isinstance(query, exp.Select):
        return True
    if any(query.args.get(key) for key in ("group", "having", "distinct", "limit", "offset")):
        return True
    if not (query.args.get("from") or query.args.get("from_")):
        return True
    for node in query.walk(prune=lambda n: n is not query and isinstance(n, exp.Query)):
        if isinstance(node, (exp.AggFunc, exp.Window)) or (
            isinstance(node, exp.Anonymous) and (node.name or "").upper() in _ANONYMOUS_AGGREGATES
        ):
            return True
    own = _select_sources(query)
    for projection in query.expressions:
        if projection.find(exp.Query) is None:
            continue
        for column in projection.find_all(exp.Column):
            scope = column.find_ancestor(exp.Select)
            while scope is not None and scope is not query:
                if not column.table or any(_binds(item, column) for item in _select_sources(scope)):
                    break  # read inside the subquery (an unqualified name: by its own FROM)
                scope = scope.find_ancestor(exp.Select)
            if scope is query and (not column.table or any(_binds(item, column) for item in own)):
                return True
    return False


class _QuotientType(str):
    """The MySQL type (decimal(65,4)) the rewrite gives a derived table's or a CTE's column
    computed by a quotient (an AVG): MySQL merges such a body into the query that reads it,
    which shows the quotient at this scale. A column passed through keeps its table's type,
    whose digits are its own."""


def _aggregated(node: exp.Expression) -> Optional[exp.Expression]:
    """The value an aggregate reads (a DISTINCT one's single value); any other node's own
    ``this`` -- None where that is no expression (an Anonymous call's name: JSON_ARRAYAGG(x)
    OVER (...))."""
    value = node.this
    if isinstance(value, exp.Distinct):
        return value.expressions[0] if len(value.expressions) == 1 else None
    return value if isinstance(value, exp.Expression) else None


def _scalar_projection(subquery: exp.Subquery) -> Optional[exp.Expression]:
    """The value a scalar subquery returns: its SELECT's one output."""
    query = subquery.this
    if isinstance(query, exp.Select) and len(query.expressions) == 1:
        return query.expressions[0].unalias()
    return None


def _unshown(node: exp.Expression) -> exp.Expression:
    """An output as it is computed, without the rounding that shows it: MySQL reads an
    alias's own value, every digit. A CASE / IF shown branch by branch is copied with its
    branches unrounded."""
    value = node
    while isinstance(value, exp.Paren):
        value = value.this
    if value.meta.get(_SHOWN) is True:
        return value.this
    if value.meta.get(_SHOWN) != "branches":
        return node
    copy = value.copy()
    copy.meta.pop(_SHOWN, None)
    if isinstance(copy, exp.If):
        holders = [(copy, "true"), (copy, "false")]
    else:
        holders = [(branch, "true") for branch in copy.args.get("ifs") or ()] + [(copy, "default")]
    for holder, key in holders:
        branch = holder.args.get(key)
        if branch is not None:
            holder.set(key, _unshown(branch))
    return copy


def _never_null(node: exp.Expression) -> bool:
    """A value that is never NULL: a literal (not NULL), or a COALESCE that ends in one."""
    if isinstance(node, exp.Neg):
        node = node.this
    if isinstance(node, exp.Coalesce):
        return _never_null((node.expressions or [node.this])[-1])
    return isinstance(node, exp.Literal)


def _integer_literal(node: Optional[exp.Expression]) -> Optional[int]:
    """The value of an integer literal (``2``, ``-1``, and ``-(1)`` as MySQL stores it),
    else None."""
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.Neg):
        value = _integer_literal(node.this)
        return -value if value is not None else None
    if isinstance(node, exp.Literal) and not node.is_string and re.fullmatch(r"\d+", node.name):
        return int(node.name)
    return None


@functools.lru_cache(maxsize=1024)
def _mysql_decimal_scale(mysql_type: Optional[str]) -> Optional[int]:
    """The scale of an exact MySQL column type -- an integer's 0, DECIMAL(p,s)'s s (a
    DECIMAL without one is DECIMAL(10,0)) -- else None."""
    kind = _mysql_kind(mysql_type)
    if kind == "integer":
        return 0
    if kind != "decimal":
        return None
    match = re.match(r"^\w+\s*\(\s*\d+\s*,\s*(\d+)\s*\)", (mysql_type or "").strip())
    return int(match.group(1)) if match else 0


@functools.lru_cache(maxsize=1024)
def _mysql_kind(mysql_type: Optional[str]) -> Optional[str]:
    """What a MySQL column type holds, as the rewrites tell values apart: binary, boolean
    (TINYINT(1), a boolean on Aurora DSQL -- an unsigned one is a smallint there, an
    integer), integer, decimal, float4 / float8 (REAL / DOUBLE PRECISION on Aurora DSQL),
    char (CHAR(n): blank-padded there), date, datetime, timestamp, time, json, else text."""
    if not mysql_type:
        return None
    kind = mysql_type.lower().strip()
    if re.match(r"^(?:tiny|medium|long)?blob\b|^(?:var)?binary\b", kind):
        return "binary"
    if re.match(r"^tinyint\s*\(\s*1\s*\)(?!.*\b(?:unsigned|zerofill)\b)", kind):
        return "boolean"
    if re.match(r"^(?:tiny|small|medium|big)?int\b|^integer\b|^bit\b|^year\b|^serial\b", kind):
        return "integer"
    if re.match(r"^(?:decimal|numeric|dec|fixed)\b", kind):
        return "decimal"
    if re.match(r"^(?:float|real)\b", kind):
        return "float4"
    if re.match(r"^double\b", kind):
        return "float8"
    if re.match(r"^char\b", kind):
        return "char"
    for name in ("datetime", "timestamp", "date", "time", "json"):
        if re.match(rf"^{name}\b", kind):
            return name
    return "text"


def _pg_type_kind(to: object) -> Optional[str]:
    """The kind (see :func:`_mysql_kind`) of a cast's target type."""
    if not isinstance(to, exp.DataType):
        return None
    t = exp.DataType.Type
    kinds = {
        t.DATE: "date", t.TIMESTAMP: "datetime", t.DATETIME: "datetime",
        t.TIMESTAMPTZ: "timestamp", t.TIME: "time", t.BOOLEAN: "boolean",
        t.TINYINT: "integer", t.SMALLINT: "integer", t.INT: "integer", t.BIGINT: "integer",
        t.DECIMAL: "decimal", t.FLOAT: "float4", t.DOUBLE: "float8",
        t.VARBINARY: "binary", t.BINARY: "binary", t.JSON: "json", t.JSONB: "json",
        t.TEXT: "text", t.VARCHAR: "text", t.NVARCHAR: "text", t.CHAR: "text",
        t.CHARACTER_SET: "text", t.UBIGINT: "decimal",
    }
    return kinds.get(to.this)


def table_type_index(tables: "Iterable[TableDef]") -> "dict[str, tuple]":
    """``{name: (name, ((lower-cased column, type), ...))}`` -- one shared entry per table,
    which every view's cache key over it references (a copy per view held one table's whole
    column list once per view, for the life of the server). By the table's exact name: MySQL
    on Linux keeps `Docs` and `docs` apart, and so does Aurora DSQL (the names are quoted),
    and a stored view names each table in the case MySQL keeps it -- `docs` is never `Docs`
    left out of the inventory. Only a Query Converter statement is read in another case
    (:func:`_column_type_resolver`)."""
    return dict(
        (
            table.name,
            (
                table.name,
                tuple((column.name.lower(), (column.mysql_type or "").lower()) for column in table.columns),
            ),
        )
        for table in tables or ()
    )


_MYSQL_QUALIFIED_NAME = re.compile(r"`((?:[^`]|``)+)`\s*\.\s*`((?:[^`]|``)+)`")
_MYSQL_QUOTED_NAME = re.compile(r"`((?:[^`]|``)+)`")


def _mysql_view_reads(name: str, definition: str, views: "Mapping[str, str]") -> "set[str]":
    """The views of ``views`` (by name) a MySQL view's body names, as
    :func:`view_column_types` finds the tables it reads: `db`.`name`, and a bare `name` of
    the view's own schema. A column spelled like a view is a false read, which only orders."""
    schema, _ = _split_qualified(name)
    found = {
        key
        for db, bare in _MYSQL_QUALIFIED_NAME.findall(definition)
        if (key := f"{db.replace('``', '`')}.{bare.replace('``', '`')}") in views
    }
    if schema is not None:
        found.update(
            key
            for bare in _MYSQL_QUOTED_NAME.findall(definition)
            if (key := f"{schema}.{bare.replace('``', '`')}") in views
        )
    found.discard(name)
    return found


def mysql_type_index(
    tables: "Iterable[TableDef]",
    views: "Iterable[ViewDef]" = (),
    zone: "Optional[SourceZone]" = None,
) -> "dict[str, tuple]":
    """:func:`table_type_index`, plus an entry for each MySQL view another view reads (G188):
    its columns typed as its own conversion types them -- one it passes on as the column it
    reads, a computed one by what it computes -- so a view over a view reads a TIMESTAMP
    column, and writes the clock it meets (``zone``: the source's, as convert_view is given
    it), as a view over the table does. Each view is typed after the views it reads, by the
    cached conversion :meth:`SchemaConverter.convert_view` then reuses; one whose body does
    not convert, or whose columns cannot be told, gets no entry and is read untyped."""
    index = table_type_index(tables)
    bodies = {
        view.name: (view.definition or "").strip()
        for view in views or ()
        if not getattr(view, "unsupported_kind", None) and (view.definition or "").strip()
    }
    reads = {name: _mysql_view_reads(name, body, bodies) for name, body in bodies.items()}
    shifted = zone if zone is not None and zone.shifts else None
    done: set[str] = set()
    for start in sorted(set().union(*reads.values()) if reads else ()):
        if start in done:
            continue
        # Depth first, each view after what it reads (MySQL refuses a cycle; a false read's
        # is broken where it closes).
        path, pending = [start], [iter(sorted(reads[start]))]
        while path:
            following = next(pending[-1], None)
            if following is not None:
                if following not in done and following not in path:
                    path.append(following)
                    pending.append(iter(sorted(reads[following])))
                continue
            name = path.pop()
            pending.pop()
            done.add(name)
            body = bodies[name]
            converted = _mysql_view_conversion(
                name,
                body,
                view_column_types(body, index, schema=_split_qualified(name)[0]),
                shifted,
            )
            if converted is not None and converted[4]:
                index[name] = (name, converted[4])
    return index


def _type_source_items(entry: Optional[exp.Expression]) -> "list[exp.Expression]":
    """The sources one FROM entry brings into scope: a table and the tables it joins, a
    derived table, and a parenthesised join's own entries -- MySQL stores every JOIN as
    ``from (a join b on (...))``, nested ones as ``((a join b) join c)``, and one that
    starts with a derived table as ``((select ...) x join b on (...))``, sqlglot hanging b
    on x."""
    out: list = []
    pending = [entry] if isinstance(entry, exp.Expression) else []
    while pending:
        current = pending.pop(0)
        while isinstance(current, exp.Subquery) and not current.args.get("alias") and isinstance(
            current.this, (exp.Table, exp.Subquery)
        ):
            pending[:0] = [
                j.this for j in (current.args.get("joins") or []) if isinstance(j.this, exp.Expression)
            ]
            current = current.this
        out.append(current)
        if isinstance(current, (exp.Table, exp.Subquery)):
            pending.extend(
                j.this for j in (current.args.get("joins") or []) if isinstance(j.this, exp.Expression)
            )
    return out


def _select_sources(select: exp.Select) -> "list[exp.Expression]":
    from_ = select.args.get("from") or select.args.get("from_")
    entries = [from_.this] if from_ is not None and isinstance(from_.this, exp.Expression) else []
    entries += [join.this for join in select.args.get("joins") or () if isinstance(join.this, exp.Expression)]
    return [item for entry in entries for item in _type_source_items(entry)]


def _type_scope(node: exp.Expression) -> Optional[exp.Expression]:
    """The query a column reference is read in: its SELECT, UPDATE or DELETE -- or the
    INSERT whose ON DUPLICATE KEY UPDATE (ON CONFLICT) it sits in."""
    child, parent = node, node.parent
    while parent is not None:
        if isinstance(parent, (exp.Select, exp.Update, exp.Delete)):
            return parent
        if isinstance(parent, exp.Insert) and child is parent.args.get("conflict"):
            return parent
        child, parent = parent, parent.parent
    return None


def _scope_sources(scope: exp.Expression) -> "list[exp.Expression]":
    """The sources a :func:`_type_scope` query reads: a SELECT's FROM and JOINs, the
    table an UPDATE or DELETE writes with what it joins or reads (FROM / USING), the
    table an INSERT writes."""
    if isinstance(scope, exp.Select):
        return _select_sources(scope)
    if isinstance(scope, exp.Insert):
        target = scope.this.this if isinstance(scope.this, exp.Schema) else scope.this
        return [target] if isinstance(target, exp.Table) else []
    entries = [scope.this]
    from_ = scope.args.get("from") or scope.args.get("from_")
    if isinstance(from_, exp.From):
        entries.append(from_.this)
    using = scope.args.get("using")
    entries += using if isinstance(using, list) else []
    return [item for entry in entries if isinstance(entry, exp.Expression) for item in _type_source_items(entry)]


def _binds(
    item: exp.Expression,
    column: exp.Column,
    columns: "Optional[Callable[[exp.Expression], Optional[Mapping[str, object]]]]" = None,
) -> bool:
    """Whether MySQL reads ``column``'s qualifier as ``item`` (find_field_in_table_ref): it
    names the item -- its alias, or an unaliased table's name -- and a database the column
    is written with is the item's own: a derived table, a LATERAL or a CTE has none, so
    `db`.`t`.`col` never reads one. MySQL reads `db`.`alias`.`col` as an aliased table only
    when the table has the column (and stores it `alias`.`col` then): one the inventory
    (``columns``) says lacks it is a source further out."""
    if item.alias_or_name != column.table:
        return False
    db = column.args.get("db")
    if db is not None and (not isinstance(item, exp.Table) or _cte_named(item) is not None):
        return False
    own = item.args.get("db") if isinstance(item, exp.Table) else None
    if not (db is None or own is None or own.name == db.name):
        return False
    if db is not None and columns is not None and isinstance(item, exp.Table) and item.args.get("alias") is not None:
        held = columns(item)
        return held is None or column.name.lower() in held
    return True


def _cte_named(table: exp.Table) -> Optional[exp.CTE]:
    """The CTE of an enclosing WITH a table written without its schema reads: from the end
    of its own body, for a CTE that is not recursive -- inside that body, and in an earlier
    sibling's, the name is the real table."""
    if table.args.get("db") is not None:
        return None
    name = table.name.lower()
    node = table
    while node.parent is not None:
        parent = node.parent
        visible: list = []
        if isinstance(parent, exp.With) and isinstance(node, exp.CTE):
            visible = parent.expressions
            if not parent.args.get("recursive"):
                visible = visible[: next(i for i, cte in enumerate(visible) if cte is node)]
        else:
            with_ = parent.args.get("with") or parent.args.get("with_")
            if isinstance(with_, exp.With) and node is not with_:
                visible = with_.expressions
        found = next((cte for cte in visible if cte.alias.lower() == name), None)
        if found is not None:
            return found
        node = parent
    return None


# The MySQL type a computed column of each kind (see :func:`_mysql_kind`) is typed as.
_KIND_TYPES: "dict[str, str]" = {
    "binary": "blob", "boolean": "tinyint(1)", "integer": "bigint", "float4": "float",
    "float8": "double", "char": "char", "date": "date", "json": "json", "text": "text",
}


class _BodyHolder:
    """What :meth:`_ColumnTypes.projected` reads of a derived table -- its body and alias --
    for a view's body, which keeps its own parent (a Subquery around it would take it)."""

    def __init__(self, body: exp.Expression, alias: "Optional[exp.TableAlias]") -> None:
        self.this = body
        self.args = {"alias": alias}


class _ColumnTypes:
    """The resolver :func:`_column_type_resolver` returns: ``node -> MySQL type``.

    A derived table's or CTE's column is read off the body that defines it -- a computed
    one by :attr:`typed_by`, which the rewrite sets (by what the expression computes: a
    quotient a decimal of its scale)."""

    def __init__(
        self, column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]", *, bare_names: bool
    ) -> None:
        self._types: dict[str, dict[str, str]] = {table: dict(columns) for table, columns in column_types}
        self._bare_names = bare_names
        self._folded: dict[str, list[str]] = {}
        self._bare: dict[str, list[str]] = {}
        self._bare_folded: dict[str, list[str]] = {}
        if bare_names:
            for table in self._types:
                self._folded.setdefault(table.lower(), []).append(table)
                short = table.rsplit(".", 1)[-1]
                self._bare.setdefault(short, []).append(table)
                self._bare_folded.setdefault(short.lower(), []).append(table)
        # Per table reference and per derived table / CTE: a WITH above it, or a body's
        # select list, is read once however many columns ask.
        self._looked_up: dict[int, tuple] = {}
        self._projections: dict[int, tuple] = {}
        # Per (select, position): a computed column is typed once, however many readers
        # ask -- typing it asks about its body's own columns, and so on down a nest of
        # derived tables, which re-typed each level per reader (exponential in the depth).
        self._computed: dict[tuple, tuple] = {}
        self._typing: set = set()
        self.typed_by: "Optional[Callable[[exp.Expression], Optional[str]]]" = None
        self._scopes: dict[int, tuple] = {}
        # Per node, the CTEs a table under it reads by name (see _cte).
        self._visible: dict[int, tuple] = {}

    def __call__(self, node: exp.Expression) -> Optional[str]:
        return self._typed(self._value(node))

    def _cte(self, table: exp.Table) -> Optional[exp.CTE]:
        """:func:`_cte_named`, what each node on the way up makes visible remembered: walked to
        the root per table, a UNION of thousands of branches -- which nests them that deep --
        made typing a statement quadratic in its length."""
        if table.args.get("db") is not None:
            return None
        path: list = []
        node: exp.Expression = table
        visible: dict = {}
        while node.parent is not None:
            cached = self._visible.get(id(node))
            if cached is not None and cached[0] is node:
                visible = cached[1]
                break
            path.append(node)
            node = node.parent
        for node in reversed(path):
            parent = node.parent
            own: list = []
            if isinstance(parent, exp.With) and isinstance(node, exp.CTE):
                own = parent.expressions
                if not parent.args.get("recursive"):
                    own = own[: next(i for i, cte in enumerate(own) if cte is node)]
            else:
                with_ = parent.args.get("with") or parent.args.get("with_")
                if isinstance(with_, exp.With) and node is not with_:
                    own = with_.expressions
            if own:
                inner: dict = {}
                for cte in own:
                    inner.setdefault(cte.alias.lower(), cte)
                for name, cte in visible.items():
                    inner.setdefault(name, cte)
                visible = inner
            self._visible[id(node)] = (node, visible)
        return visible.get(table.name.lower())

    def _one(self, owners: "Optional[list[str]]") -> "Optional[dict[str, str]]":
        return self._types[owners[0]] if owners and len(owners) == 1 else None

    def _table(self, key: str, *, bare: bool) -> "Optional[dict[str, str]]":
        """The inventory's columns of the table ``key`` names, by its exact name -- in a Query
        Converter statement also in another case, and one written without its schema
        (``bare``) by its name alone, each when only one table can be meant."""
        found = self._types.get(key)
        if found is None and self._bare_names:
            found = self._one(self._folded.get(key.lower()))
            if found is None and bare:
                found = self._one(self._bare.get(key) or self._bare_folded.get(key.lower()))
        return found

    def base_columns(self, source: exp.Expression) -> "Optional[Mapping[str, str]]":
        """The inventory's columns of the base table a FROM source names (None for a CTE, a
        derived table, or a table it lacks)."""
        if not isinstance(source, exp.Table) or self._cte(source) is not None:
            return None
        db = source.args.get("db")
        return self._table(f"{db.name}.{source.name}" if db is not None else source.name, bare=db is None)

    def table_types(self, source: Optional[exp.Expression]) -> "Optional[Mapping[str, object]]":
        """The columns a FROM source brings, by lower-cased name: a table's types, a derived
        table's or CTE's as :meth:`projected` reads them; None when they cannot be told
        (another view, a table the inventory lacks)."""
        if isinstance(source, (exp.Subquery, exp.Lateral)):
            return self.projected(source)
        if not isinstance(source, exp.Table):
            return None
        cached = self._looked_up.get(id(source))
        if cached is not None and cached[0] is source:
            return cached[1]
        cte = self._cte(source)
        if cte is not None:
            found: "Optional[Mapping[str, object]]" = self.projected(cte)
        else:
            found = self.base_columns(source)
        self._looked_up[id(source)] = (source, found)
        return found

    def projected(self, holder: exp.Expression) -> "Optional[Mapping[str, object]]":
        """A derived table's or CTE's columns, by the select list of the body that defines
        them: a bare column of a typed source its type, read in the body's own scope;
        ``SELECT *`` its sources' columns; a computed one the (select, position)
        :attr:`typed_by` types when asked. A UNION's where every branch agrees, a recursive
        CTE's by its non-recursive SELECTs (MySQL types it by them). None when its columns
        cannot be told."""
        cached = self._projections.get(id(holder))
        if cached is not None and cached[0] is holder:
            return cached[1]
        self._projections[id(holder)] = (holder, None)  # a body that reads itself tells nothing
        branches: list = []
        pending = [holder.this]
        while pending:
            node = pending.pop()
            while isinstance(node, (exp.Subquery, exp.Paren)):
                node = node.this
            if isinstance(node, exp.SetOperation):
                pending += [node.expression, node.this]
            elif isinstance(node, exp.Select):
                branches.append(node)
            else:
                return None
        if isinstance(holder, exp.CTE) and isinstance(holder.parent, exp.With) and holder.parent.args.get("recursive"):
            leading = []
            for branch in branches:
                if any(self._cte(table) is holder for table in branch.find_all(exp.Table)):
                    break
                leading.append(branch)
            branches = leading
        outputs: list = []
        for branch in branches:
            output = self._select_columns(branch)
            if output is None:
                return None
            outputs.append(output)
        if not outputs:
            return None
        alias = holder.args.get("alias")
        names = (
            [column.name.lower() for column in alias.columns]
            if isinstance(alias, exp.TableAlias) and alias.columns
            else [name for name, _ in outputs[0]]
        )
        columns: dict[str, object] = {}
        for position, name in enumerate(names):
            values = [output[position][1] if position < len(output) else None for output in outputs]
            agreed = len(values) == 1 or all(isinstance(v, str) and v == values[0] for v in values)
            columns.setdefault(name, values[0] if agreed else None)
        self._projections[id(holder)] = (holder, columns)
        return columns

    def outputs(
        self, body: exp.Expression, names: "Sequence[str]" = ()
    ) -> "Optional[tuple[tuple[str, str], ...]]":
        """``((lower-cased column, MySQL type or ""), ...)`` of a view's ``body``, as
        :meth:`projected` reads a derived table's -- named ``names`` when the view lists its
        columns -- for a view that reads this one (G188). None when they cannot be told."""
        alias = (
            exp.TableAlias(this=exp.to_identifier("v"), columns=[exp.to_identifier(n) for n in names])
            if names
            else None
        )
        columns = self.projected(_BodyHolder(body, alias))
        if not columns:
            return None
        return tuple((name, self._typed(value) or "") for name, value in columns.items())

    def _select_columns(self, select: exp.Select) -> "Optional[list[tuple[str, object]]]":
        out: list = []
        for position, projection in enumerate(select.expressions):
            expression = projection.unalias()
            if isinstance(expression, exp.Star) or (
                isinstance(expression, exp.Column) and isinstance(expression.this, exp.Star)
            ):
                qualifier = expression.table if isinstance(expression, exp.Column) else ""
                for source in _select_sources(select):
                    if qualifier and source.alias_or_name != qualifier:
                        continue
                    columns = self.table_types(source)
                    if columns is None:
                        return None
                    out.extend(columns.items())
                continue
            value = self._value(expression) if isinstance(expression, exp.Column) else (select, position)
            out.append((projection.alias_or_name.lower(), value))
        return out

    def _typed(self, value: object) -> Optional[str]:
        if not isinstance(value, tuple):
            return value if isinstance(value, str) else None
        select, position = value
        key = (id(select), position)
        if self.typed_by is None or key in self._typing or position >= len(select.expressions):
            return None
        cached = self._computed.get(key)
        if cached is not None and cached[0] is select:
            return cached[1]
        self._typing.add(key)
        try:
            typed = self.typed_by(select.expressions[position].unalias())
        finally:
            self._typing.discard(key)
        self._computed[key] = (select, typed)
        return typed

    def _scope(self, node: exp.Expression) -> Optional[exp.Expression]:
        """:func:`_type_scope`, remembered for every node on the way up: a column deep in a
        long predicate (thousands of ORs) climbs only to the first node another column has
        climbed, so typing a statement stays linear in its size."""
        path: list = []
        child, parent = node, node.parent
        scope = None
        while parent is not None:
            hit = self._scopes.get(id(child))
            if hit is not None and hit[0] is child:
                scope = hit[1]
                break
            path.append(child)
            if isinstance(parent, (exp.Select, exp.Update, exp.Delete)) or (
                isinstance(parent, exp.Insert) and child is parent.args.get("conflict")
            ):
                scope = parent
                break
            child, parent = parent, parent.parent
        for climbed in path:
            self._scopes[id(climbed)] = (climbed, scope)
        return scope

    def _value(self, node: exp.Expression) -> object:
        if not isinstance(node, exp.Column):
            return None
        name = node.name.lower()
        qualifier = node.table
        db = node.args.get("db")
        scope = self._scope(node)
        while scope is not None:
            listed = _scope_sources(scope)
            if qualifier:
                # EXCLUDED (an upsert's VALUES(col), rewritten) is the row it inserts.
                excluded = isinstance(scope, exp.Insert) and qualifier.upper() == "EXCLUDED"
                for item in listed:
                    if excluded or (db is None and item.alias_or_name == qualifier):
                        found = self.table_types(item)
                    elif (
                        item.alias_or_name != qualifier
                        or not isinstance(item, exp.Table)
                        or item.args.get("alias") is not None
                        or self._cte(item) is not None
                    ):
                        continue
                    elif item.args.get("db") is None:
                        # `db`.`table`.`col` FROM table, in a Query Converter statement: the
                        # session's database, which MySQL takes `db` to be.
                        found = self._table(f"{db.name}.{item.name}", bare=False)
                    elif item.args["db"].name == db.name:
                        found = self.table_types(item)
                    else:
                        continue
                    return found.get(name) if found is not None else None
            elif listed:
                tables = [self.table_types(item) for item in listed]
                holders = [t for t in tables if t is not None and name in t]
                if len(holders) == 1:
                    return holders[0][name]
                if holders or any(t is None for t in tables):
                    return None
            scope = self._scope(scope)
        return None


def _column_type_resolver(
    column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]",
    *,
    bare_names: bool = False,
) -> _ColumnTypes:
    """``node -> MySQL type`` of a column reference, read from ``column_types`` (``(table,
    ((column, type), ...))``, column names lower-cased): through its qualifier -- matched
    exactly, as MySQL (on Linux) and the quoted output both do -- to the source of its own
    or an enclosing query (a SELECT's FROM / JOIN, a parenthesised join included, the table
    an UPDATE / DELETE / ON DUPLICATE KEY UPDATE writes) that MySQL binds it to, and for an
    unqualified column the one source of its query that has it. A column written
    `db`.`table`.`col` is the unaliased table of that database -- in a Query Converter
    statement also one written without it (the session's) -- and one read through an alias
    has lost its database by now (:func:`_drop_alias_db`). A derived table's or a CTE's
    column takes the type of what its body selects (:meth:`_ColumnTypes.projected`). A table
    is found by its exact name -- a stored view keeps MySQL's case; ``bare_names`` (a Query
    Converter statement): also in another case, and one written without its schema by its
    name alone, when only one table can be meant. ``None`` when it cannot be told (another
    view's column, a table the inventory lacks)."""
    return _ColumnTypes(column_types, bare_names=bare_names)


def _drop_alias_db(
    select: exp.Expression,
    columns: "Optional[Callable[[exp.Expression], Optional[Mapping[str, object]]]]" = None,
) -> None:
    """MySQL stores a column read through an ALIASED table (a merged view's) as
    `db`.`alias`.`col`; PostgreSQL refuses that (the alias is no relation of the schema):
    written ``alias.col``. The column is bound as MySQL binds it (:func:`_binds`, with the
    inventory's ``columns``), to the nearest source of that name and database that can hold
    it, so an inner query's alias of another database's table -- or of a table without the
    column -- leaves an outer `db`.`table`.`col`, and PostgreSQL binds that one where MySQL
    does."""
    for column in list(select.find_all(exp.Column)):
        if column.args.get("db") is None or not column.table:
            continue
        scope = _type_scope(column)
        while scope is not None:
            bound = next((item for item in _scope_sources(scope) if _binds(item, column, columns)), None)
            if bound is not None:
                if bound.args.get("alias") is not None:
                    column.set("db", None)
                    column.set("catalog", None)
                break
            scope = _type_scope(scope)


# MySQL's JSON_ARRAYAGG: an aggregate sqlglot keeps as an anonymous call (JSON_AGG once
# written for PostgreSQL).
_ANONYMOUS_AGGREGATES = frozenset({"JSON_ARRAYAGG", "JSON_AGG"})
# Operators that bind tighter than any comparison, IS, IN, BETWEEN or LIKE on both engines.
_ARITHMETIC = (
    exp.Add, exp.Sub, exp.Mul, exp.Div, exp.IntDiv, exp.Mod, exp.DPipe, exp.BitwiseAnd,
    exp.BitwiseOr, exp.BitwiseXor, exp.BitwiseLeftShift, exp.BitwiseRightShift,
)


def _is_aggregate(node: exp.Expression) -> bool:
    """An aggregate call as MySQL counts one: ANY_VALUE, an aggregate to sqlglot, is none --
    MySQL computes it row by row (it only waives ONLY_FULL_GROUP_BY)."""
    return (isinstance(node, exp.AggFunc) and not isinstance(node, exp.AnyValue)) or (
        isinstance(node, exp.Anonymous) and (node.name or "").upper() in _ANONYMOUS_AGGREGATES
    )


def _is_window_function(node: exp.Expression) -> bool:
    """A window's own function (SUM(x) OVER ()), which makes no group -- an aggregate inside
    it (SUM(SUM(x)) OVER ()) does."""
    parent = node.parent
    while isinstance(parent, (exp.RespectNulls, exp.IgnoreNulls)):
        node, parent = parent, parent.parent
    return isinstance(parent, exp.Window) and node.arg_key == "this"


def _put(node: exp.Expression, expression: exp.Expression) -> None:
    """Replace ``node`` with ``expression``, parenthesised when both are operators: bare, the
    alias ``a + b AS s`` in ``HAVING s * 2 > 10`` read a + b * 2 -- arithmetic under a
    comparison excepted, which reads the same either way."""

    def operator(value: Optional[exp.Expression]) -> bool:
        # AND / OR and the JSON arrows are functions to sqlglot too.
        return isinstance(value, (exp.Binary, exp.Unary, exp.Predicate)) and not isinstance(
            value, (exp.Paren, exp.Exists)
        )

    parent = node.parent
    if operator(expression) and operator(parent) and not (
        isinstance(expression, _ARITHMETIC) and isinstance(parent, exp.Predicate)
    ):
        expression = exp.Paren(this=expression)
    node.replace(expression)


def _source_holds(
    column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]" = (),
) -> "Callable[[exp.Expression, str], Optional[bool]]":
    """``(select, column) ->`` whether a FROM source of that select -- or that one source --
    has the (lower-cased) column: a derived table or a CTE by its outputs, a table by
    ``column_types`` (the Evaluation inventory's :func:`table_type_index` entries; a table
    found by its name as :func:`_column_type_resolver` finds a Query Converter statement's).
    ``None`` when it cannot be told -- a table the inventory lacks, a ``*`` output, a
    LATERAL."""
    # The inventory by name, indexed when a lookup first asks for a table, and a table's
    # columns when it is asked for: a statement without HAVING or ANY_VALUE asks for none,
    # however many tables the inventory has. A name in another case, or without its
    # schema, is the one table that has it ("" when more do).
    entries: dict[str, "Sequence[tuple[str, str]]"] = {}
    columns: dict[str, frozenset] = {}
    folded: dict[str, str] = {}
    bare: dict[str, str] = {}
    bare_folded: dict[str, str] = {}
    # Per source: a WITH above it is walked once, however many names ask.
    looked_up: dict[int, tuple] = {}

    def index() -> None:
        if entries or not column_types:
            return
        for table, listed in column_types:
            entries[table] = listed
            short = table.rsplit(".", 1)[-1]
            for names, name in ((folded, table.lower()), (bare, short), (bare_folded, short.lower())):
                names[name] = "" if name in names else table

    def one(owner: Optional[str]) -> "Optional[frozenset]":
        if not owner:
            return None
        if owner not in columns:
            columns[owner] = frozenset(name for name, _type in entries[owner])
        return columns[owner]

    def outputs(node: exp.Expression) -> "Optional[frozenset]":
        names = node.alias_column_names or (
            node.this.named_selects if isinstance(node.this, exp.Query) else []
        )
        return frozenset(name.lower() for name in names) if names and "*" not in names else None

    def source_columns(item: exp.Expression) -> "Optional[frozenset]":
        if isinstance(item, exp.Subquery):
            return outputs(item)
        if not isinstance(item, exp.Table):
            return None
        if _cte_named(item) is not None:
            node = item.parent
            while node is not None:
                with_ = node if isinstance(node, exp.With) else (
                    node.args.get("with_") or node.args.get("with")
                )
                for cte in with_.expressions if isinstance(with_, exp.With) else ():
                    if cte.alias.lower() == item.name.lower():
                        return outputs(cte)
                node = node.parent
            return None
        db = item.args.get("db")
        key = f"{db.name}.{item.name}" if db is not None else item.name
        index()
        found = one(key) if key in entries else one(folded.get(key.lower()))
        if found is None and db is None:
            found = one(bare.get(key) or bare_folded.get(key.lower()))
        return found

    def holds(scope: exp.Expression, name: str) -> Optional[bool]:
        unknown = False
        for item in _select_sources(scope) if isinstance(scope, exp.Select) else (scope,):
            cached = looked_up.get(id(item))
            if cached is None or cached[0] is not item:
                cached = looked_up[id(item)] = (item, source_columns(item))
            if cached[1] is None:
                unknown = True
            elif name in cached[1]:
                return True
        return None if unknown else False

    return holds


# The arguments of a query that hold its sources -- a derived table, a CTE's body, an UPDATE /
# DELETE target's joins -- rather than an expression.
_SOURCE_ARGS = frozenset({"from_", "from", "joins", "with_", "with", "this", "using", "tables"})


def _reading_query(
    column: exp.Column,
    holds: "Callable[[exp.Select, str], Optional[bool]]",
    scope: "Optional[exp.Select]" = None,
    outward: "Optional[Callable[[exp.Select], Optional[exp.Select]]]" = None,
) -> Optional[exp.Select]:
    """The query whose FROM a column reads, searched outward as MySQL binds it: a qualified
    one the nearest with a source of that name; an unqualified one the nearest with a FROM,
    past one whose sources all lack it (``holds``: :func:`_source_holds`) -- a FROM-less
    query has no columns, so (SELECT COUNT(total)) reads the enclosing query's. ``None``
    when no query has it. ``scope`` is the column's own query and ``outward`` a query's
    enclosing one, when the caller has them (a walk of the column's whole expression)."""
    name = column.name.lower()
    if scope is None:
        scope = column.find_ancestor(exp.Select)
    while scope is not None:
        sources = _select_sources(scope)
        if column.table:
            if any(item.alias_or_name == column.table for item in sources):
                return scope
        elif sources and holds(scope, name) is not False:
            return scope
        scope = scope.find_ancestor(exp.Select) if outward is None else outward(scope)
    return None


def _aggregated_in(
    node: exp.Expression, holds: "Callable[[exp.Select, str], Optional[bool]]"
) -> Optional[exp.Select]:
    """The query an aggregate call is computed in, by SQL's rule (MySQL's and PostgreSQL's
    alike): the innermost one whose FROM its columns read -- an enclosing query when it
    reads only that one's, SELECT (SELECT MAX(o.total) FROM p) FROM orders o -- else the one
    it is written in. ``None`` when a column's query cannot be told."""
    read: set = set()
    for column in node.find_all(exp.Column):
        query = _reading_query(column, holds)
        if query is None:
            return None
        read.add(id(query))
    written = node.find_ancestor(exp.Select)
    scope = written
    while scope is not None and id(scope) not in read:
        scope = scope.find_ancestor(exp.Select)
    return scope if scope is not None else written


def _one_group(
    select: exp.Select, holds: "Callable[[exp.Select, str], Optional[bool]]"
) -> Optional[bool]:
    """Whether SQL makes ``select`` one group: by an aggregate written at its own level, or
    one in a subquery computed in it (:func:`_aggregated_in`) -- a window's own function and
    ANY_VALUE make none. ``None`` when an aggregate's query cannot be told."""
    unknown = False
    for node in select.find_all(exp.AggFunc, exp.Anonymous):
        if not _is_aggregate(node) or _is_window_function(node):
            continue
        if node.find_ancestor(exp.Select) is select:
            return True
        computed = _aggregated_in(node, holds)
        if computed is select:
            return True
        unknown = unknown or computed is None
    return None if unknown else False


def _any_value_as_value(
    tree: exp.Expression, holds: "Callable[[exp.Select, str], Optional[bool]]"
) -> None:
    """MySQL's ANY_VALUE(x) as x where nothing groups -- no GROUP BY and no aggregate
    computed in the query PostgreSQL computes its any_value in: there it is no aggregate on
    MySQL, and as one it made the query one group (one row for every row)."""
    flat: dict[int, bool] = {}
    for node in list(tree.find_all(exp.AnyValue)):
        query = _aggregated_in(node, holds)
        if query is None:
            continue
        if id(query) not in flat:
            flat[id(query)] = query.args.get("group") is None and _one_group(query, holds) is False
        if flat[id(query)]:
            _put(node, node.this)


def inline_having_aliases(
    tree: exp.Expression,
    *,
    case_insensitive: bool = True,
    stored: bool = True,
    column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]" = (),
) -> "list[str]":
    """Write a MySQL ``HAVING`` as PostgreSQL / Aurora DSQL read it -- a view's as MySQL
    stores it (``stored``: every column qualified, a bare GROUP BY name the alias it
    resolved to), a Query Converter statement's as written, its tables' columns read from
    ``column_types`` (the Evaluation inventory): they never read an output name in HAVING
    (``column "total" does not exist``). Returns what a HAVING reads that cannot be written
    faithfully (one sentence each).

    A HAVING name is what MySQL makes it -- a GROUP BY column, else a select-list item:
    MySQL reads no other name of the query there. In a subquery it reads any other from the
    enclosing query, as Aurora DSQL does only when no source of the subquery has the name:
    else the HAVING is left as written, and named (t.* beside other sources lists t's
    columns where they can be told; named as either reading where not). A view stores such
    a name qualified, and an enclosing select-list alias bare: that alias's value is written
    in, a correlated reference to Aurora DSQL -- named where it cannot be (an aggregate, a
    shadowed table). A GROUP BY column of that name wins (its documented preference): a
    qualified one -- also the column GROUP BY 1 or a GROUP BY alias points to -- is written
    in the HAVING as that column, qualified (bare, it is ambiguous on PostgreSQL when a
    joined table has the name too). A bare GROUP BY name that is an alias is a FROM column
    when the alias is an aggregate or a window at its own level (MySQL refuses to GROUP BY
    either); stored, when the alias reads a column of its own name at its own level;
    written, when a FROM source of its query has the column (MySQL reads FROM first,
    warning 1052) -- told by the inventory or a derived table's outputs, else by the alias
    being or reading the query's column of that name (``o.status AS status``, an enclosing
    query's too: bare, it reads what MySQL reads either way). Untold, the alias is the group
    when it reads another column of the query outside an aggregate (grouped by the FROM
    column, ONLY_FULL_GROUP_BY refuses it) -- unless a column of its name is in it too: the
    name may be the FROM's key that column depends on, so it stays the column, as 5d955cc
    had it, and both readings are named. An alias of an enclosing query's column in a
    subquery stays the column where it names it, is inlined where not, and is named. Else
    the name is guessed the FROM column, in a subquery (one that can read an enclosing query:
    not a top-level CTE or derived table) written with its one source (bare, a wrong guess
    reads the enclosing query's column of the name on Aurora DSQL) and named beside others.
    Else an alias is inlined, and a select-list column (``o.customer_id``, also behind an
    alias or in parentheses) is written as that column. A qualified HAVING name, one inside
    an aggregate's argument (MySQL reads that from FROM) or in a nested query, is never an
    alias.

    Without GROUP BY or an aggregate computed in the query (:func:`_one_group`), MySQL's
    HAVING filters rows (before windows, DISTINCT and LIMIT), where PostgreSQL makes the
    query one group (``column "t.id" must appear in the GROUP BY clause``): it is written
    as a WHERE, ANDed with the query's own. ANY_VALUE is no aggregate on MySQL: where
    nothing groups, ANY_VALUE(x) is x (:func:`_any_value_as_value`). An alias is its own
    value, every digit (a quotient's output is rounded only to be shown)."""
    holds = _source_holds(column_types)
    notes: list[str] = []

    def key(identifier: object) -> str:
        # MySQL resolves a column alias whatever its case; PostgreSQL folds an unquoted
        # name and keeps a quoted one as written.
        name = getattr(identifier, "name", "") or ""
        if case_insensitive or not getattr(identifier, "quoted", False):
            return name.translate(_ASCII_FOLD_TABLE)
        return name

    def own(root: exp.Expression) -> "Iterable[exp.Expression]":
        # The nodes of root's own query level: a subquery, derived table or CTE in it has
        # aggregates and columns of its own.
        return root.walk(prune=lambda node: node is not root and isinstance(node, exp.Query))

    def read_in(column: exp.Column) -> Optional[exp.Expression]:
        # The aggregate or query a HAVING name is read in (MySQL reads one inside ANY_VALUE
        # as a HAVING name).
        scope = column.find_ancestor(exp.AggFunc, exp.Query, exp.Anonymous)
        while isinstance(scope, (exp.Anonymous, exp.AnyValue)) and not _is_aggregate(scope):
            scope = scope.find_ancestor(exp.AggFunc, exp.Query, exp.Anonymous)
        return scope

    # Per query, the query enclosing it and its GROUP BY's column names: walked once however
    # many aliases and columns ask (rebuilt per alias, 3,000 GROUP BY aliases took seconds).
    enclosing_queries: dict[int, tuple] = {}
    group_names: dict[int, tuple] = {}

    def outward(scope: exp.Select) -> Optional[exp.Select]:
        cached = enclosing_queries.get(id(scope))
        if cached is None or cached[0] is not scope:
            cached = enclosing_queries[id(scope)] = (scope, scope.find_ancestor(exp.Select))
        return cached[1]

    def grouped_names(select: exp.Select) -> "set[str]":
        # The names of the columns a GROUP BY lists (GROUP BY 1: that select-list item's).
        cached = group_names.get(id(select))
        if cached is None or cached[0] is not select:
            names = set()
            count = len(select.expressions)
            for item in select.args["group"].expressions:
                if isinstance(item, exp.Literal) and item.is_int and 0 < int(item.name) <= count:
                    item = select.expressions[int(item.name) - 1].unalias()
                if isinstance(item, exp.Column):
                    names.add(key(item.args.get("this")))
            cached = group_names[id(select)] = (select, names)
        return cached[1]

    def columns_read(select: exp.Select, expression: exp.Expression) -> "list[tuple]":
        # Each column of an alias of ``select`` as (column, its own query, the query whose FROM
        # it reads, whether an aggregate ``select`` computes holds it -- SELECT MAX(o.qty) FROM
        # p: no row's), in one walk of the alias that carries each node's query and aggregates
        # down: walked up per column, a long sum or GREATEST made the analysis quadratic, and
        # cubic inside an aggregate.
        found: list = []
        # Per aggregate, the queries its columns read (None when one cannot be told) and the
        # query it is written in -- :func:`_aggregated_in`'s, from the walk.
        read: dict[int, Optional[set]] = {}
        written: dict[int, Optional[exp.Select]] = {}
        stack: list = [(expression, expression.find_ancestor(exp.Select), ())]
        while stack:
            node, scope, aggregates = stack.pop()
            if isinstance(node, exp.Select):
                scope = node
            elif isinstance(node, exp.Column):
                query = _reading_query(node, holds, scope, outward)
                found.append((node, scope, query, aggregates))
                for aggregate in aggregates:
                    seen = read[id(aggregate)]
                    if seen is not None and query is None:
                        read[id(aggregate)] = None
                    elif seen is not None:
                        seen.add(id(query))
            if _is_aggregate(node) and not _is_window_function(node):
                written[id(node)] = scope
                read[id(node)] = set()
                aggregates = (*aggregates, node)
            for child in node.iter_expressions():
                stack.append((child, scope, aggregates))
        computed: dict[int, Optional[exp.Select]] = {}

        def computed_in(aggregate: exp.Expression) -> Optional[exp.Select]:
            if id(aggregate) not in computed:
                seen = read[id(aggregate)]
                scope = written[id(aggregate)]
                while seen is not None and scope is not None and id(scope) not in seen:
                    scope = outward(scope)
                computed[id(aggregate)] = (
                    None if seen is None else scope if scope is not None else written[id(aggregate)]
                )
            return computed[id(aggregate)]

        return [
            (column, scope, query, any(computed_in(a) is select for a in aggregates))
            for column, scope, query, aggregates in found
        ]

    def from_column(
        select: exp.Select, name: str, expression: exp.Expression, nested: bool
    ) -> "tuple[Optional[bool], str]":
        # Whether MySQL reads a bare GROUP BY name that is also an alias as a FROM column --
        # in a statement None when no source can be told to have it and the alias reads none
        # of the query's columns to tell it by: a guess -- and why MySQL may read it the other
        # way, where it cannot be told ("key", "column", "alias": the HAVING's note names
        # both). ``nested``: the query can read an enclosing one (in_subquery).
        if any(_is_aggregate(node) or isinstance(node, exp.Window) for node in own(expression)):
            return True, ""
        if stored:
            return any(
                isinstance(node, exp.Column) and key(node.args.get("this")) == name
                for node in own(expression)
            ), ""
        found = holds(select, name.lower())
        if found is not None:
            return found, ""
        value = expression
        while isinstance(value, exp.Paren):
            value = value.this
        if isinstance(value, exp.Column) and key(value.args.get("this")) == name:
            # o.status AS status, the enclosing query's o too: bare, the name reads the FROM's
            # column if it has one, as MySQL does, else that column itself.
            return True, ""
        grouped = grouped_names(select)
        others = len(grouped) > (name in grouped)  # another GROUP BY column beside the alias
        enclosing = set()
        scope = outward(select)
        while scope is not None:
            enclosing.add(id(scope))
            scope = outward(scope)
        ungrouped = named = outer = False
        for column, scope, query, aggregated in columns_read(select, expression):
            read = key(column.args.get("this"))
            named = named or read == name
            if query is not select:
                # A nested query's own column, or an enclosing query's: the alias reads one.
                outer = outer or query is not None and id(query) in enclosing
                continue
            if read == name and column.table:
                return True, ""  # o.status AS status: o has the column
            if read == name and nested and scope is select:
                outer = True  # COALESCE(status, 'none') AS status in a subquery: either's
            if read != name and read not in grouped and not aggregated:
                ungrouped = True
        if not ungrouped:
            # An alias of an enclosing query's column (UPPER(o.status) AS status, o.total * 2 AS
            # dbl in a subquery): MySQL reads the FROM's column of the name if it has one, else
            # the alias -- kept as 5d955cc had it (the column when the alias names it), reported.
            # Else -- a constant, an uncorrelated subquery -- a guess.
            if outer:
                return (True, "column") if named else (False, "alias")
            return None, ""
        # Grouped by the FROM column, MySQL (ONLY_FULL_GROUP_BY) refuses an alias that reads
        # another column of the query outside an aggregate: it groups by the alias -- unless
        # that column depends on the FROM column, its key, or on another GROUP BY column, a
        # key (GROUP BY amount, o.id), which cannot be told: then a column of the name
        # anywhere in the alias keeps it the column, as 5d955cc had it -- reported, MySQL
        # may read either (beside another GROUP BY column, a guess).
        if named:
            return (None, "") if others else (True, "key")
        return False, ""

    def in_subquery(select: exp.Select) -> bool:
        # Whether a query can read an enclosing query's columns: a subquery in an expression
        # (an ON's too) or a LATERAL can; a derived table or a CTE's body only when the query
        # it is in can -- at the top level neither has an enclosing query.
        node: exp.Expression = select
        through = False
        while node.parent is not None:
            parent = node.parent
            through = through or isinstance(parent, exp.Lateral) or node.arg_key == "on"
            if isinstance(parent, (exp.Select, exp.Update, exp.Delete)) and (
                through or node.arg_key not in _SOURCE_ARGS
            ):
                return True
            node = parent
        return False

    def read_outside(select: exp.Select, column: exp.Column) -> bool:
        # A name no source of its query has, Aurora DSQL reads from the enclosing query as
        # MySQL does -- unless an enclosing select list aliases it (MySQL may read that).
        if holds(select, column.name.lower()) is not False:
            return False
        name = key(column.args.get("this"))
        scope = select.find_ancestor(exp.Select)
        while scope is not None:
            if any(
                isinstance(p, exp.Alias) and key(p.args.get("alias")) == name
                for p in scope.expressions
            ):
                return False
            scope = scope.find_ancestor(exp.Select)
        return True

    def writable(value: exp.Expression, shadowed: set) -> bool:
        # An enclosing alias's value Aurora DSQL reads the same in a subquery: no aggregate or
        # window of its own (its level would move), every column qualified with a table no
        # source in between shadows.
        if any(_is_aggregate(node) or isinstance(node, exp.Window) for node in own(value)):
            return False
        inner = {id(query) for query in value.find_all(exp.Select)}
        return all(
            column.table
            and (id(_reading_query(column, holds)) in inner or column.table not in shadowed)
            for column in value.find_all(exp.Column)
        )

    def enclosing(select: exp.Select, name: str) -> "tuple[bool, Optional[exp.Expression]]":
        # Whether a view's subquery HAVING name is an enclosing select-list alias (MySQL stores
        # a column it read from a FROM qualified) -- of the nearest query that holds the
        # subquery in its select list, HAVING or ORDER BY, MySQL reads no other's -- and its
        # value where it can be written in.
        shadowed = {item.alias_or_name for item in _select_sources(select)}
        node: exp.Expression = select
        while node.parent is not None:
            parent = node.parent
            if isinstance(parent, exp.Select):
                if node.arg_key in ("expressions", "having", "order"):
                    for projection in parent.expressions:
                        if isinstance(projection, exp.Alias) and key(projection.args.get("alias")) == name:
                            value = _unshown(projection.this)
                            return True, value if writable(value, shadowed) else None
                shadowed |= {item.alias_or_name for item in _select_sources(parent)}
            node = parent
        return False, None

    def label(source: exp.Expression) -> Optional[exp.Identifier]:
        # The name a FROM source's columns are qualified with: its alias, else a table's own.
        alias = source.args.get("alias")
        if isinstance(alias, exp.TableAlias) and isinstance(alias.this, exp.Identifier):
            return alias.this
        if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier):
            return source.this
        return None

    def note(text: str) -> None:
        if text not in notes:
            notes.append(text)

    # A HAVING that filters rows becomes a WHERE once every query is read: until then a
    # subquery in it is still where MySQL read an enclosing alias from.
    filters: list = []
    for select in tree.find_all(exp.Select):
        having = select.args.get("having")
        if having is None:
            continue
        nested = in_subquery(select)
        sources = _select_sources(select)
        aliases = {
            key(projection.args.get("alias")): _unshown(projection.this)
            for projection in select.expressions
            if isinstance(projection, exp.Alias)
        }
        # A select-list column by its name, parenthesised too: an unaliased one, else one
        # behind an alias (MySQL reads that only when no other item has the name).
        listed: dict[str, exp.Column] = {}
        for projection in sorted(select.expressions, key=lambda p: isinstance(p, exp.Alias)):
            value = projection.unalias()
            while isinstance(value, exp.Paren):
                value = value.this
            if isinstance(value, exp.Column):
                listed.setdefault(key(value.args.get("this")), value)
        group = select.args.get("group")
        grouped: set = set()
        qualified: dict[str, exp.Column] = {}
        # A GROUP BY name kept the FROM column though MySQL may read the alias, and why.
        unsure: dict[str, str] = {}
        for item in group.expressions if group is not None else []:
            if isinstance(item, exp.Literal) and item.is_int:
                # GROUP BY 1 groups by that select-list item.
                if 0 < int(item.name) <= len(select.expressions):
                    item = select.expressions[int(item.name) - 1].unalias()
            elif isinstance(item, exp.Column) and not item.table:
                name = key(item.args.get("this"))
                expression = aliases.get(name)
                reading, why = (
                    (True, "") if expression is None else from_column(select, name, expression, nested)
                )
                if why:
                    unsure[name] = why
                if reading is False:
                    # GROUP BY the alias: the group is what it names (o.customer_id AS cid).
                    item = expression
                elif reading is None and nested:
                    # A guessed FROM column in a subquery: bare in HAVING, Aurora DSQL reads the
                    # enclosing query's column of the name when no source has it. With the one
                    # source a wrong guess fails; beside others it is named. (Its GROUP BY
                    # reads the name as MySQL does, a FROM column first.)
                    table = label(sources[0]) if len(sources) == 1 else None
                    if table is not None:
                        item = exp.Column(this=item.this.copy(), table=table.copy())
                    else:
                        unsure[name] = "guess"
            if not isinstance(item, exp.Column):
                continue
            name = key(item.args.get("this"))
            if item.table:
                qualified.setdefault(name, item)
            else:
                grouped.add(name)
        # SELECT * (or t.* of its one source) lists every FROM column: MySQL reads a name
        # there, else in the enclosing query -- where Aurora DSQL reads it too. t.* beside
        # other sources lists t's, as the inventory or a derived table's outputs tell them.
        stars = [
            p for p in select.expressions
            if isinstance(p, exp.Star) or isinstance(p, exp.Column) and isinstance(p.this, exp.Star)
        ]
        every_column = any(isinstance(p, exp.Star) for p in stars) or (
            bool(stars) and len(sources) == 1
        )
        starred = [
            (item, star) for star in stars if isinstance(star, exp.Column)
            for item in sources if item.alias_or_name == star.table
        ]
        outer: list[str] = []
        through: list[tuple] = []
        unread: list[str] = []
        guessed: list[tuple] = []
        for column in list(having.find_all(exp.Column)):
            if column.table or read_in(column) is not select:
                continue
            name = key(column.args.get("this"))
            if name in qualified:
                column.replace(qualified[name].copy())
            elif name in grouped or name in aliases:
                if name in unsure and (column.name, unsure[name]) not in guessed:
                    guessed.append((column.name, unsure[name]))
                if name not in grouped:
                    _put(column, aliases[name].copy())
            elif name in listed:
                column.replace(listed[name].copy())
            else:
                aliased, value = enclosing(select, name) if stored else (False, None)
                if value is not None:
                    _put(column, value.copy())
                elif aliased:
                    if column.name not in unread:
                        unread.append(column.name)
                elif not every_column:
                    lowered = column.name.lower()
                    held = [(item, star) for item, star in starred if holds(item, lowered) is not False]
                    lister = next((star for item, star in held if holds(item, lowered)), None)
                    if lister is not None:
                        column.set("table", lister.args["table"].copy())
                    elif held:
                        # Every starred source that may have it: MySQL reads the one that does.
                        entry = (column.name, tuple(dict.fromkeys(star.table for _item, star in held)))
                        if entry not in through:
                            through.append(entry)
                    elif column.name not in outer and not read_outside(select, column):
                        outer.append(column.name)
        if nested and (outer or through or unread):
            # MySQL reads it from the enclosing query, where Aurora DSQL reads the subquery's
            # own column first; a view stores it qualified, and an alias bare.
            if outer and not stored:
                note(
                    f"MySQL reads {', '.join(outer)} in a subquery's HAVING from the enclosing "
                    "query (a HAVING reads only its own select list and GROUP BY), where Aurora "
                    "DSQL reads the subquery's own column of that name if it has one and makes a "
                    "subquery with HAVING but no GROUP BY one group: it is left as written -- "
                    "qualify the name with the enclosing query's table, in the subquery's WHERE "
                    "when it has neither GROUP BY nor an aggregate."
                )
            for name, stars in through:
                held_by = (
                    f"{stars[0]}.* if {stars[0]} has a column {name}"
                    if len(stars) == 1
                    else f"{' or '.join(f'{star}.*' for star in stars)}, whichever has a column {name}"
                )
                written = " or ".join(f"{star}.{name}" for star in stars)
                note(
                    f"MySQL reads {name} in a subquery's HAVING through {held_by}, else from the "
                    "enclosing query (a HAVING reads only its own select list and GROUP BY), where "
                    "Aurora DSQL makes a subquery with HAVING but no GROUP BY one group: it is left "
                    f"as written -- write it {written}"
                    f"{'' if len(stars) == 1 else ' (the one that has it)'}, or qualified with the "
                    "enclosing query's table, as the tables' columns have it, in the subquery's "
                    "WHERE when it has neither GROUP BY nor an aggregate."
                )
            if unread:
                note(
                    f"MySQL reads {', '.join(unread)} in a subquery's HAVING as the enclosing "
                    "query's select-list alias, which Aurora DSQL cannot read there, and its "
                    "expression cannot be written in as it is (an aggregate or a window, or a "
                    "table a nearer source's name shadows): it is left as written, and fails -- "
                    "write the expression there by hand."
                )
        elif group is None and _one_group(select, holds) is False:
            # A row filter on MySQL: a WHERE, now that it names no output alias.
            filters.append((select, having))
        for name, why in guessed:
            # MySQL's reading untold: a guess beside several sources, a key the alias's columns
            # may depend on, an alias of an enclosing query's column (kept the column when it
            # names it, else its expression).
            tables = " or ".join(item.alias_or_name for item in sources)
            key_of = (
                " when it is a key the alias's own columns depend on (or ONLY_FULL_GROUP_BY is off)"
                if why == "key" else ""
            )
            if why == "alias":
                outcome = (
                    "where the alias reads only the enclosing query: its expression is written in "
                    "the HAVING -- qualify the name with its table instead if one of them has the "
                    "column"
                )
            elif nested:
                outcome = (
                    "where Aurora DSQL's HAVING in a subquery reads a name none of them has from "
                    "the enclosing query: it is left as written -- qualify it with its table, or "
                    "write the alias's expression in the HAVING"
                )
            else:
                outcome = (
                    f"where Aurora DSQL fails when none of them has it ('column \"{name}\" does not "
                    "exist'): it is left as written -- write the alias's expression in the HAVING "
                    "if none has such a column"
                )
            note(
                f"MySQL groups by {name} as the column of {tables} that has it{key_of} (warning "
                f"1052), else by the select-list alias {name}, and its HAVING reads {name} the "
                f"same way, {outcome}, as the tables' columns have it."
            )
    for select, having in filters:
        select.set("having", None)
        select.where(having.this, copy=False)
    _any_value_as_value(tree, holds)
    return notes


_ASCII_FOLD_TABLE = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def _rewrite_mysql_calls(
    select: exp.Expression,
    column_type: "Optional[Callable[[exp.Expression], Optional[str]]]" = None,
    *,
    view: bool = False,
    materialized: bool = False,
    zone: "Optional[SourceZone]" = None,
    on_typed: "Optional[Callable[[], None]]" = None,
) -> "tuple[list[str], list[str], list[str]]":
    """Rewrite in place the MySQL calls sqlglot writes wrongly for PostgreSQL, and return
    ``(untranslated, misformatted, issues)`` -- the calls left that fail on Aurora DSQL,
    the date formats with a specifier it has no pattern for (the result is different
    text), and what else cannot be written faithfully (one sentence each). ``view``: the
    body of a view, whose outputs MySQL shows at their scale only where it materializes
    the view -- else they are computed in the query that reads it; ``materialized``: one
    MySQL materializes whatever its body (ALGORITHM=TEMPTABLE); ``zone``: the source's time
    zone, known and not UTC, which a view's clock is written in (G149) -- NOW() and its
    synonyms and SYSDATE() its wall clock, CURDATE() its date, CURTIME() its time of day,
    where the session's are written otherwise (Aurora DSQL's session zone is UTC unless the
    session sets another) -- and which the view reads the other session-zone values in
    (G157): FROM_UNIXTIME(), a TIMESTAMP read as a wall clock (its date, time, fields,
    text), and that zone's wall clock where it meets a TIMESTAMP (compared as the instant it
    names there). A Query Converter statement gets none: the application's own session runs
    it. ``on_typed`` is called once the statement's columns can be typed and before anything
    is rewritten (a view's own column types are read there, G188).

    A rewrite that depends on what a value holds reads it from the value's static kind
    (:func:`_mysql_kind`): a column's type where ``column_type`` knows it (a view over the
    inventory's tables, a Query Converter statement over the Evaluation inventory), else
    what the expression computes (a cast, a date function, date arithmetic, an aggregate,
    a JSON call, a CASE / COALESCE of them) -- a derived table's or CTE's column by what its
    body selects. Unknown, a value's LENGTH counts the text of ``x || ''`` (right for any
    type but a few, noted), a column in a string function, LIKE or compared as BINARY is
    left as it is (right for text; PostgreSQL refuses a number there) -- in LPAD / RPAD
    and a CONCAT with no text it is ``x || ''`` -- a column's BINARY is ``CAST(x AS
    BYTEA)`` (noted: right for bytes only), ordered by the bytes of its text, grouped as
    itself. Rewritten, faithfully (each checked against MySQL 8.4 and PostgreSQL 17):

    * YEAR/MONTH/DAY/DAYOFMONTH/DAYOFYEAR/QUARTER/HOUR/MINUTE/WEEKOFYEAR / WEEK(x, 3)(x) ->
      EXTRACT as an integer, SECOND(x) its whole seconds, DAYOFWEEK(x) EXTRACT(DOW) + 1,
      DAYNAME(x) TO_CHAR of its date, TO_DAYS from 0001-01-01;
    * DATEDIFF(a, b) -> the day difference of their dates; TIME(x) / TIMESTAMP(x) ->
      casts; NOW() / CURRENT_TIMESTAMP / LOCALTIME / LOCALTIMESTAMP [fsp], CURTIME [fsp]
      and UTC_DATE / UTC_TIME / UTC_TIMESTAMP [fsp] -> the session's / UTC wall clock,
      TRUNCATED to fsp (0 by default) as MySQL does;
    * CAST / CONVERT to CHAR (MySQL stores it with its CHARSET) -> text, or varchar(n)
      with a length (MySQL's CHAR(n) cast truncates and never pads) -- a binary operand's
      bytes decoded (CONVERT_FROM, in the charset's encoding); to BINARY / CHARSET binary
      -> its bytes: compared, x's text (Aurora DSQL compares bytes already); grouped by, x
      itself; ordered by, x itself where its text orders as it does, else the bytes of its
      text -- an output name's by the expression it names, a UNION's or a SELECT
      DISTINCT's x itself (reported); to UNSIGNED -> numeric(20), rounded then a negative
      number wrapped as MySQL does (a string by its leading integer); a recursive CTE's
      recursive values -> cast to its first SELECT's type where that has a modifier (a
      CHAR(n) cast, a VARCHAR(n) or DECIMAL(p,s) column), as MySQL types the column by it;
    * MySQL's exact division (integers and decimals) -> numeric with the digits MySQL
      keeps, truncated, shown at its scale (the dividend's plus 4) where MySQL shows it --
      a statement's output, a text, a view or derived table MySQL materializes -- AVG of
      an exact value as the same division of its SUM by its COUNT, and % / MOD / DIV by 0
      NULL as MySQL's; a FLOAT in double precision (its SUM, real * real); ROUND /
      TRUNCATE(x, d) of a FLOAT / DOUBLE in double precision as MySQL computes it, of an
      integer the integer; a TINYINT(1) or a comparison read as a number as its integer, a
      number assigned to a TINYINT(1) as a boolean; INTERVAL x unit + d as d + INTERVAL x
      unit;
    * GROUP_CONCAT(x) -> STRING_AGG over text (a TINYINT(1) or a comparison as its digit),
      and with DISTINCT x ORDER BY x ->
      ARRAY_TO_STRING(ARRAY_AGG(DISTINCT x ORDER BY x) FILTER (WHERE x IS NOT NULL));
    * CONCAT_WS skipping NULLs as MySQL does, GREATEST/LEAST returning NULL for a NULL
      argument as MySQL does (once per nest, through every NULL-strict node in it), LENGTH
      as bytes and CHAR_LENGTH as characters of the value's text, a number, a date, a
      boolean or a comparison made MySQL's text of it where MySQL wants a string (CONCAT /
      CONCAT_WS -- bytes beside text decoded -- LPAD / RPAD, LEFT / RIGHT, SUBSTRING,
      REPLACE, UPPER / LOWER, TRIM, REVERSE, REPEAT, MD5, ASCII, LOCATE, LIKE, REGEXP,
      GREATEST / LEAST beside a string but a number string below 2, a UNION column beside a
      text or a string PostgreSQL would not read as its values), a COALESCE / CASE /
      IF MySQL makes a string as MySQL reads it where it stands (chosen_as_mysql), a CHAR(n)
      column's padding left out where MySQL leaves it out, a DATE plus days/months a DATE
      (an amount in whole units, as MySQL counts it -- a string by its leading integer, a
      composite unit by its parts; a date string or a two-digit year as MySQL reads it),
      CONVERT_TZ with a UTC offset as an ISO offset -- a literal one, or one read at run
      time -- and a literal zone name tzdata defines as a link as the zone it links to
      (G192, :func:`~dsql_migrator.core.source_time_zone.written_zone_name`);
    * JSON_ARRAY / JSON_ARRAYAGG -> JSON_BUILD_ARRAY / JSON_AGG, which keep NULLs, and
      every JSON value written as MySQL writes it (a DATETIME 'YYYY-MM-DD HH:MM:SS.ffffff',
      a TIME with microseconds, a TINYINT(1) as its number, a comparison as true / false, a
      CHAR(n) without padding, a date beside a date string MySQL's string of them);
      JSON_EXTRACT / -> and
      JSON_UNQUOTE(JSON_EXTRACT(...)) -- MySQL's ->> -- over a path of member names and
      indexes -> the value MySQL's path finds (a member in an object, an index in an array,
      [0] of a value that is no array the value itself), ->> a JSON null as 'null' (NULL
      where a date function reads it; a grouped one as its GROUP BY key), its text -- or a
      JSON value -- meeting a number as the double MySQL reads (noted); another path, and a
      JSON value compared, ordered or grouped, is reported;
    * DATE_FORMAT / FROM_UNIXTIME(x, fmt) -> TO_CHAR, STR_TO_DATE -> TO_DATE / a TIME / a
      TIMESTAMP as MySQL's own rule types it (an empty string NULL, as in MySQL; a
      DATETIME read in the session's time zone), every format written by
      :func:`_mysql_format_to_pg`; a TIME operand of a date format on the current date and
      an integer yyyymmdd one as that date, as MySQL reads them;
    * a JOIN (or INNER JOIN) with no ON -> a CROSS JOIN, which binds as JOIN does (a comma
      stays one), CROSS JOIN ... ON and STRAIGHT_JOIN -> JOIN.
    The statistics are read right by :class:`_MySqlViewDialect` (VAR_SAMP, STDDEV ...)."""
    untranslated: list[str] = []
    issues: list[str] = []
    misformatted: list[str] = []

    def note(name: str) -> None:
        if name.lower() not in {n.lower() for n in untranslated}:
            untranslated.append(name)

    def issue(text: str) -> None:
        if text not in issues:
            issues.append(text)

    def misformat(word: str, tokens: "list[str]") -> None:
        for token in tokens:
            entry = f"{word} ({token})"
            if entry not in misformatted:
                misformatted.append(entry)

    def written(node: exp.Expression) -> exp.Expression:
        """``node``, marked as a call this rewrite writes (:data:`_PG_WRITTEN`)."""
        node.meta[_PG_WRITTEN] = True
        return node

    def pg_call(name: str, *arguments: exp.Expression) -> exp.Expression:
        return written(exp.Anonymous(this=name, expressions=list(arguments)))

    def interval_of(node: exp.Expression) -> Optional[exp.Expression]:
        """What date arithmetic adds -- the INTERVAL of ``d + INTERVAL ...`` or of ``d + n *
        INTERVAL '1 DAY'``, a DATE_ADD / DATE_SUB itself (it holds the unit) -- else None."""
        if isinstance(node, (exp.DateAdd, exp.DateSub)):
            return node
        if not isinstance(node, (exp.Add, exp.Sub)):
            return None
        added = node.expression
        while isinstance(added, exp.Paren):
            added = added.this
        if isinstance(added, exp.Mul):
            added = next((side for side in (added.this, added.expression) if isinstance(side, exp.Interval)), None)
        return added if isinstance(added, exp.Interval) else None

    def interval_unit(node: exp.Expression) -> str:
        interval = interval_of(node)
        unit = interval.args.get("unit") if interval is not None else None
        return (unit.name if isinstance(unit, exp.Expression) else str(unit or "")).upper()

    # The resolver itself (before the cache below wraps it): the rewrite types a derived
    # table's computed columns for it, and _drop_alias_db reads its base tables.
    resolver = column_type if isinstance(column_type, _ColumnTypes) else None
    if column_type is not None:
        # Each column resolved once, however many passes ask: by the node, which the cache
        # keeps alive (so its id is never another node's). Resolution starts after
        # _drop_alias_db below, and no pass moves a column into another query.
        resolve_type, resolved = column_type, {}

        def column_type(node: exp.Expression) -> Optional[str]:  # noqa: F811
            hit = resolved.get(id(node))
            if hit is None or hit[0] is not node:
                hit = resolved[id(node)] = (node, resolve_type(node))
            return hit[1]

    # How deep the typing reads below are nested (a Python frame or two each), and whether
    # one gave up (reported).
    reading = [0]
    too_deep = [False]

    def bounded(read: "Callable[[Any], Any]") -> "Callable[[Any], Any]":
        """``read`` -- a typing read, recursive over an expression -- of unknown result
        (None) past _MAX_TYPED_DEPTH nested reads: a value nested that deep is of unknown
        type, not a RecursionError -- and reported, MySQL's exact arithmetic not applied to
        it. (A chain of operators is walked in a loop: :func:`chain`.)"""

        def guarded(node: Any) -> Any:
            if reading[0] >= _MAX_TYPED_DEPTH:
                too_deep[0] = True
                return None
            reading[0] += 1
            try:
                return read(node)
            finally:
                reading[0] -= 1

        return guarded

    def chain(node: exp.Expression, types: "tuple[type, ...]") -> "tuple[list[exp.Expression], exp.Expression]":
        """The operators of ``types`` down the left of a chain from ``node`` (a + b + c is
        ((a + b) + c)), outermost first, and the operand it starts from -- a chain of
        thousands of operators is read in a loop. A quotient already written and date
        arithmetic end it."""
        operators: list = []
        while True:
            while isinstance(node, exp.Paren):
                node = node.this
            if not isinstance(node, types) or _QUOTIENT_SCALE in node.meta or interval_of(node) is not None:
                return operators, node
            operators.append(node)
            node = node.this

    # The kinds of the choices a pass has written (each its own once, however deep they
    # nest), while that pass runs.
    known_kinds: dict = {}

    @bounded
    def kind_of(node: Optional[exp.Expression]) -> Optional[str]:
        """The static kind of a value (see :func:`_mysql_kind`), or None."""
        if node is None or isinstance(node, exp.Null):
            return None
        known = known_kinds.get(id(node))
        if known is not None and known[0] is node:
            return known[1]
        if _QUOTIENT_SCALE in node.meta:
            return "decimal"  # MySQL's exact division, or an AVG, as written below
        if isinstance(node, exp.Paren):
            return kind_of(node.this)
        if isinstance(node, exp.Column):
            return _mysql_kind(column_type(node)) if column_type is not None else None
        if isinstance(node, exp.Literal):
            if node.is_string:
                return "text"
            if re.fullmatch(r"-?\d+", node.name):
                return "integer"
            return "float8" if "e" in node.name.lower() else "decimal"  # 2.5e0 is a DOUBLE
        if isinstance(node, (exp.Cast, exp.TryCast)):
            return _pg_type_kind(node.args.get("to"))
        clock = _utc_clock_kind(node)
        if clock is not None:
            return {"timestamp": "datetime"}.get(clock, clock)
        if isinstance(node, exp.AtTimeZone):
            # PostgreSQL's: an instant (CURRENT_TIMESTAMP, TO_TIMESTAMP(x) -- MySQL's
            # FROM_UNIXTIME, a DATETIME, read there as the instant it is -- a timestamptz) at a
            # zone is that zone's wall clock, and a wall clock at a zone an instant.
            inner = (
                "timestamp"
                if isinstance(node.this, (exp.CurrentTimestamp, exp.UnixToTime))
                else kind_of(node.this)
            )
            return {"timestamp": "datetime", "datetime": "timestamp"}.get(inner or "")
        if isinstance(node, exp.ConvertTimezone):
            return "datetime"
        if isinstance(node, (exp.TsOrDsToDate, exp.CurrentDate, exp.LastDay, exp.StrToDate)):
            return "date"
        if isinstance(node, (exp.TsOrDsToTimestamp, exp.CurrentTimestamp, exp.UnixToTime, exp.StrToTime, *_NOW_SYNONYMS)):
            return "datetime"
        if isinstance(node, (exp.CurrentTime, exp.Time)):
            return "time"
        if isinstance(node, exp.Anonymous):
            name = (node.name or "").upper()
            return {
                "TO_DATE": "date", "NOW": "datetime", "DATE_TRUNC": "datetime",
                "TO_CHAR": "text", "CONVERT_FROM": "text", "ARRAY_TO_STRING": "text",
                "CONVERT_TO": "binary", "JSON_BUILD_ARRAY": "json", "JSON_AGG": "json",
                "JSON_ARRAY": "json", "JSON_ARRAYAGG": "json", "LOCALTIMESTAMP": "datetime",
                "TO_DAYS": "integer", "OCTET_LENGTH": "integer", "ORD": "integer", "CRC32": "integer",
            }.get(name)
        if isinstance(node, (exp.JSONExtract, exp.JSONObject)):
            return "json"
        if isinstance(node, (exp.JSONExtractScalar, exp.JSONBExtractScalar)):
            return "text"
        if isinstance(node, (exp.Add, exp.Sub, exp.DateAdd, exp.DateSub)) and interval_of(node) is not None:
            base = kind_of(node.this)
            if base == "date" or (base == "text" and _is_date_literal(node.this)):
                return "date" if interval_unit(node) in _DATE_UNITS else "datetime"
            if base == "text" and isinstance(node.this, exp.Literal):
                return "datetime"
            return base if base in ("datetime", "timestamp") else None
        if isinstance(node, _CHAINED):
            operators, start = chain(node, _CHAINED)
            kind = kind_of(start)
            for operator in reversed(operators):
                right = kind_of(operator.expression)
                if not isinstance(operator, exp.Div):
                    kind = combine(kind, right)
                    continue
                # A DOUBLE when an operand is one; of exact operands MySQL's exact division
                # (a pass below writes it, and marks it -- a derived table's column may be
                # typed before); of operands not typed it may well be exact in MySQL
                # (rounded, truncated or made UNSIGNED in numeric, as one).
                kinds = {kind, right}
                kind = "float8" if kinds & _FLOAT_KINDS else "decimal" if kinds <= _EXACT_KINDS else None
            return kind
        if isinstance(node, exp.Neg):
            return kind_of(node.this)
        if isinstance(node, (
            exp.Count, exp.Length, exp.DateDiff, exp.Extract, exp.IntDiv, exp.Sign, exp.StrPosition,
            exp.Ascii, exp.BitwiseCount, *_INTEGER_NODES,
        )):
            return "integer"
        if isinstance(node, _VALUE_WINDOW_FUNCTIONS):
            return kind_of(node.this)  # LAG(x) / FIRST_VALUE(x) ... OVER: an x
        if isinstance(node, exp.Sum):
            kind = kind_of(_aggregated(node))  # a REAL's is summed in double precision, as MySQL's (below)
            return "float8" if kind == "float4" else kind if kind in _NUMERIC_KINDS else None
        if isinstance(node, exp.Abs):
            kind = kind_of(node.this)  # PostgreSQL keeps a REAL's ABS a real
            return kind if kind in _NUMERIC_KINDS else None
        if isinstance(node, (exp.Floor, exp.Ceil)):
            kind = kind_of(node.this)
            return "integer" if kind in _EXACT_KINDS else "float8" if kind in _FLOAT_KINDS else None
        if isinstance(node, exp.Subquery):
            return kind_of(_scalar_projection(node))
        if isinstance(node, exp.Window):
            return kind_of(node.this)
        if isinstance(node, exp.Avg):
            kind = kind_of(_aggregated(node))
            return "decimal" if kind in _EXACT_KINDS else "float8" if kind in _FLOAT_KINDS else None
        if isinstance(node, (exp.Min, exp.Max, exp.AnyValue)):
            return kind_of(_aggregated(node))
        if isinstance(node, (exp.Round, exp.Trunc)):
            kind = kind_of(node.this)
            return "float8" if kind in _FLOAT_KINDS else kind if kind in _EXACT_KINDS else None
        if isinstance(node, (exp.Coalesce, exp.Nullif, exp.Greatest, exp.Least)):
            return common([node.this, *node.expressions] if not isinstance(node, exp.Nullif) else [node.this])
        if isinstance(node, exp.If):
            return common([node.args.get("true"), node.args.get("false")])
        if isinstance(node, exp.Case):
            return common([b.args.get("true") for b in node.args.get("ifs") or ()] + [node.args.get("default")])
        if isinstance(node, (exp.Concat, exp.ConcatWs, exp.GroupConcat, exp.Upper, exp.Lower, exp.Substring, exp.Trim)):
            return "text"
        if isinstance(node, _PREDICATES):
            return "boolean"  # MySQL's 0 / 1, as a TINYINT(1) is
        return None

    def combine(left: Optional[str], right: Optional[str]) -> Optional[str]:
        """The kind of arithmetic over two kinds, as PostgreSQL computes it (a REAL with a
        REAL stays real, with anything else it is double precision)."""
        if left not in _NUMERIC_KINDS or right not in _NUMERIC_KINDS:
            return None
        if left == right == "float4":
            return "float4"
        if {left, right} & _FLOAT_KINDS:
            return "float8"
        return "decimal" if "decimal" in (left, right) else "integer"

    def common(nodes: "list") -> Optional[str]:
        """The kind of a COALESCE / CASE / IF over these values, as PostgreSQL resolves it:
        numbers fold (any double precision -> double, else a REAL -> real), a date string
        beside dates or datetimes is read as one and a time string beside times as one, a
        DATETIME beside a TIMESTAMP is a timestamptz, and a TIME beside a date is MySQL's
        DATETIME (the time on the current date, as chosen_as_mysql writes it)."""
        values = [n for n in nodes if n is not None and not isinstance(n, exp.Null)]
        strings = [n for n in values if isinstance(n, exp.Literal) and n.is_string]
        kinds = {kind_of(n) for n in values if not (isinstance(n, exp.Literal) and n.is_string)}
        if strings and not (
            (kinds and kinds <= {"date", "datetime", "timestamp"} and all(_is_datetime_literal(s) for s in strings))
            or (kinds == {"time"} and all(_is_time_literal(s) for s in strings))
        ):
            kinds.add("text")  # else the date strings are read as the others' type
        if len(kinds) > 1 and kinds <= _TEMPORAL_KINDS:
            return "timestamp" if "timestamp" in kinds else "datetime"
        if len(kinds) > 1 and kinds <= _NUMERIC_KINDS:
            return next(k for k in ("float8", "float4", "decimal") if k in kinds)
        return kinds.pop() if len(kinds) == 1 else None

    def null_checked(node: exp.Expression) -> exp.Expression:
        """The GREATEST / LEAST inside the NULL check written for it (CASE WHEN a IS NULL OR
        ... THEN NULL ELSE GREATEST(a, ...) END), else ``node`` itself."""
        while isinstance(node, exp.Paren):
            node = node.this
        ifs = node.args.get("ifs") if isinstance(node, exp.Case) else None
        if ifs and len(ifs) == 1 and isinstance(ifs[0].args.get("true"), exp.Null) and isinstance(
            node.args.get("default"), (exp.Greatest, exp.Least)
        ):
            return node.args["default"]
        return node

    def string_typed(node: exp.Expression) -> bool:
        """A GREATEST or LEAST mixing a date or a time with a string literal -- seen through
        the NULL check written for it: a STRING in MySQL, a date or a timestamp here (a
        COALESCE / CASE / IF beside a string is written as MySQL's string: chosen_as_mysql)."""
        values = [v for v in choices(null_checked(node)) if v is not None and not isinstance(v, exp.Null)]
        return any(isinstance(v, exp.Literal) and v.is_string for v in values) and any(
            kind_of(v) in _TEMPORAL_KINDS for v in values if not isinstance(v, exp.Literal)
        )

    def each_choice(node: exp.Expression, write: "Callable[[exp.Expression], exp.Expression]") -> None:
        """Each value a COALESCE / CASE / IF / GREATEST / LEAST chooses, as ``write`` gives
        it (each typed while still in the tree)."""
        if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
            node.set("this", write(node.this))
            node.set("expressions", [write(e) for e in node.expressions])
            return
        branches = (node.args.get("ifs") or ()) if isinstance(node, exp.Case) else [node]
        for branch in branches:
            if branch.args.get("true") is not None:
                branch.set("true", write(branch.args["true"]))
        key = "default" if isinstance(node, exp.Case) else "false"
        if node.args.get(key) is not None:
            node.set(key, write(node.args[key]))

    @bounded
    def scales(node: exp.Expression) -> "Optional[tuple[int, int]]":
        """``(shown, kept)`` for an exact (integer or decimal) value: the scale MySQL gives it
        -- what it shows and writes as text: a column's, a literal's, a quotient's or an
        AVG's that of its dividend plus div_precision_increment (4), a product's the sum --
        and the fraction digits it computes with, more for a quotient or an AVG (2 / 3
        shows 0.6667 and keeps 0.666666666: :func:`_mysql_quotient_digits`). None: not
        exact, or not known. Read off the expression's own shape, in one pass."""
        while isinstance(node, exp.Paren):
            node = node.this
        if node is None:
            return None
        if _QUOTIENT_SCALE in node.meta:
            return node.meta[_QUOTIENT_SCALE]
        if isinstance(node, exp.Column):
            scale = _mysql_decimal_scale(column_type(node)) if column_type is not None else None
            return None if scale is None else (scale, scale)
        if isinstance(node, exp.Literal):
            if node.is_string or "e" in node.name.lower():
                return None
            scale = len(node.name.split(".", 1)[1]) if "." in node.name else 0
            return scale, scale
        if isinstance(node, exp.Cast):
            to = node.args.get("to")
            if _pg_type_kind(to) == "integer" or (isinstance(to, exp.DataType) and to.this == exp.DataType.Type.UBIGINT):
                return 0, 0
            if not (isinstance(to, exp.DataType) and to.this == exp.DataType.Type.DECIMAL):
                return None
            params = to.expressions
            scale = int(params[1].name) if len(params) > 1 and params[1].name.isdigit() else 0
            return scale, scale
        if isinstance(node, _ARGUMENT_SCALED):
            return scales(_aggregated(node))
        if isinstance(node, exp.Subquery):
            projection = _scalar_projection(node)
            return scales(projection) if projection is not None else None
        if isinstance(node, exp.Avg):
            dividend = scales(_aggregated(node))
            return None if dividend is None else (min(dividend[0] + 4, 30), _mysql_quotient_digits(dividend[1], 0))
        if isinstance(node, (exp.Add, exp.Sub)) and interval_of(node) is not None:
            return None  # a date
        if isinstance(node, _CHAINED):
            operators, start = chain(node, _CHAINED)
            # A NULL operand (x + NULL) is left out of a sum or a product: () here.
            value = () if isinstance(start, exp.Null) else scales(start)
            for operator in reversed(operators):
                right = () if isinstance(operator.expression, exp.Null) else scales(operator.expression)
                if isinstance(operator, exp.Div):
                    if not value or not right:
                        return None
                    value = min(value[0] + 4, 30), _mysql_quotient_digits(value[1], right[1])
                    continue
                found = [s for s in (value, right) if s != ()]
                if not found or None in found:
                    return None
                if isinstance(operator, exp.Mul):
                    value = min(sum(s[0] for s in found), 30), min(sum(s[1] for s in found), 30)
                else:
                    value = max(s[0] for s in found), max(s[1] for s in found)
            return value or None
        if isinstance(node, (exp.Round, exp.Trunc)):
            decimals = node.args.get("decimals")
            inner = scales(node.this)
            if inner is None:
                return None
            places = 0 if decimals is None else _integer_literal(decimals)
            # MySQL rounds to no more places than the value shows: ROUND(2 / 3, 9) is 0.6667.
            return None if places is None else (max(0, min(places, inner[0])),) * 2
        branches: "list[Optional[exp.Expression]]" = []
        if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
            branches = [node.this, *node.expressions]
        elif isinstance(node, exp.If):
            branches = [node.args.get("true"), node.args.get("false")]
        elif isinstance(node, exp.Case):
            branches = [b.args.get("true") for b in node.args.get("ifs") or ()] + [node.args.get("default")]
        elif kind_of(node) == "integer":
            return 0, 0  # COUNT, LENGTH, DATEDIFF, a date part, FLOOR of an exact value
        found = [scales(b) for b in branches if b is not None and not isinstance(b, exp.Null)]
        if not found or None in found:
            return None
        return max(s[0] for s in found), max(s[1] for s in found)  # type: ignore[index]

    def decimal_scale(node: exp.Expression) -> Optional[int]:
        """The scale MySQL shows an exact value at (:func:`scales`), or None."""
        found = scales(node)
        return found[0] if found is not None else None

    @bounded
    def carries_quotient(node: Optional[exp.Expression]) -> bool:
        """Whether an exact value holds more digits than MySQL shows: a quotient or an AVG
        not rounded since -- also one read out of a derived table or a CTE, typed as such
        (:class:`_QuotientType`, which only their columns are given -- a name written bare
        too), which MySQL merges into the query that reads it."""
        while isinstance(node, exp.Paren):
            node = node.this
        if node is None:
            return False
        if _QUOTIENT_SCALE in node.meta or isinstance(node, (exp.Div, exp.Avg)):
            return True
        if isinstance(node, exp.Column):
            # An unqualified one is the one source of its query that has it, as the resolver
            # bound it: a _QuotientType is only ever a derived table's or a CTE's column.
            mysql_type = column_type(node) if column_type is not None else None
            return isinstance(mysql_type, _QuotientType)
        if isinstance(node, _ARGUMENT_SCALED):
            return carries_quotient(_aggregated(node))
        if isinstance(node, exp.Subquery):
            return carries_quotient(_scalar_projection(node))
        if isinstance(node, (exp.Add, exp.Sub)) and interval_of(node) is not None:
            return carries_quotient(node.this)  # a date
        if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Mod)):
            operators, start = chain(node, (exp.Add, exp.Sub, exp.Mul, exp.Mod))
            return carries_quotient(start) or any(carries_quotient(o.expression) for o in operators)
        if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
            return any(carries_quotient(n) for n in [node.this, *node.expressions])
        if isinstance(node, exp.If):
            return carries_quotient(node.args.get("true")) or carries_quotient(node.args.get("false"))
        if isinstance(node, exp.Case):
            return any(carries_quotient(b.args.get("true")) for b in node.args.get("ifs") or ()) or carries_quotient(
                node.args.get("default")
            )
        return False

    def shown(node: exp.Expression, *, stored: bool = False) -> exp.Expression:
        """An exact value as MySQL shows it, and makes it text: a quotient (an AVG) at its
        scale -- 10 / 3 is 3.3333 -- where Aurora DSQL's numeric shows every digit. MySQL
        computes with the digits it does not show (2 / 3 * 100 is 66.6667, 10 / 3 = 3.3333
        is false), so only an output or a text is rounded. A CASE / IF shows the branch it
        takes as itself (CASE WHEN c THEN 1 / 3 ELSE 0 END is 0), unless its rows are
        ``stored`` in a temporary table first (a materialized view, DISTINCT, a UNION):
        then every value is at the column's scale (0.0000), as COALESCE's always is."""
        value = node
        while isinstance(value, exp.Paren):
            value = value.this
        if isinstance(value, (exp.If, exp.Case)) and not stored and carries_quotient(value):
            if isinstance(value, exp.If):
                holders = [(value, "true"), (value, "false")]
            else:
                holders = [(branch, "true") for branch in value.args.get("ifs") or ()] + [(value, "default")]
            for holder, key in holders:
                branch = holder.args.get(key)
                if branch is not None:
                    holder.set(key, shown(branch))
            value.meta[_SHOWN] = "branches"
            return node
        scale = decimal_scale(node)
        if scale is None or not carries_quotient(node):
            return node
        rounded = exp.Round(this=node, decimals=exp.Literal.number(scale))
        rounded.meta[_SHOWN] = True
        return rounded

    def float_text(kind: Optional[str]) -> None:
        """Note a FLOAT made text: MySQL's text of it has 6 significant digits."""
        if kind == "float4":
            issue(
                "A FLOAT (REAL) value is made text or written in JSON: MySQL writes a FLOAT with "
                "6 significant digits (1234.57), Aurora DSQL with the digits that read back as "
                "the same REAL (1234.5677) -- the text differs (a DOUBLE's is the same). Where "
                "the text matters, round it (ROUND(x, 2)) or cast it (CAST(x AS DECIMAL(10,2)))."
            )

    def as_text(node: exp.Expression, *, kept: bool = False) -> exp.Expression:
        """A value as MySQL makes it text: a CHAR(n) column without its padding, a
        TINYINT(1) (boolean here) or a comparison as its digit, a number or a date as its
        digits -- a quotient as many as MySQL shows (``kept``: the digits it keeps, as a
        UNION's string column stores it), a DATETIME / TIME every fractional digit of its
        precision (PostgreSQL's text drops trailing zeros), a TIMESTAMP without a zone (a
        FLOAT's, which MySQL writes with 6 digits, noted)."""
        kind = kind_of(node)
        float_text(kind)
        if kind == "text":
            return node
        if kind == "boolean":
            node = exp.Cast(this=node, to=exp.DataType.build("INT"))
        elif kind in _EXACT_KINDS and not kept:
            node = shown(node)
        elif kind in ("datetime", "timestamp", "time"):
            digits = fraction_digits(node)
            if digits or kind == "timestamp":
                pattern = "HH24:MI:SS" if kind == "time" else "YYYY-MM-DD HH24:MI:SS"
                if kind == "time":  # TO_CHAR reads a timestamp: on a day of its own
                    node = exp.Paren(this=exp.Add(
                        this=exp.Cast(this=exp.Literal.string("2000-01-01"), to=exp.DataType.build("DATE")),
                        expression=node,
                    ))
                return pg_call("TO_CHAR", node, exp.Literal.string(pattern + (f".FF{digits}" if digits else "")))
        return exp.Cast(this=node, to=exp.DataType.build("TEXT"))

    def unpadded(node: exp.Expression) -> exp.Expression:
        """A CHAR(n) value as MySQL reads it, without its padding."""
        return exp.Cast(this=node, to=exp.DataType.build("TEXT")) if kind_of(node) == "char" else node

    def leading_number(node: exp.Expression, to: str, *, fraction: bool = False) -> exp.Expression:
        """A string as MySQL reads it as a number: its leading digits after spaces and a
        sign ('1.5' is 1, ' -2x' -2, 'AB' and '' 0) -- with a ``fraction`` and an exponent
        ('1.5abc' is 1.5, '2e1' 20, as an INTERVAL ... SECOND reads it) -- NULL for NULL."""
        # No capturing group: PostgreSQL's SUBSTRING(x, pattern) would return only that.
        pattern = r"^[[:space:]]*[+-]?(?:[0-9]+[.]?[0-9]*|[.][0-9]+)(?:[eE][+-]?[0-9]+)?" if fraction else (
            r"^[[:space:]]*[+-]?[0-9]+"
        )
        digits = exp.Cast(
            this=pg_call("SUBSTRING", node.copy(), exp.Literal.string(pattern)), to=exp.DataType.build(to)
        )
        return exp.Case(ifs=[exp.If(
            this=exp.Not(this=exp.Is(this=node, expression=exp.Null())),
            true=exp.Coalesce(this=digits, expressions=[exp.Literal.number(0)]),
        )])

    def as_temporal(node: exp.Expression) -> exp.Expression:
        """A date function's operand as MySQL reads it: a TIME on the current date, an
        integer yyyymmdd as that date."""
        kind = kind_of(node)
        if kind == "time":
            return exp.Paren(this=exp.Add(this=exp.CurrentDate(), expression=node))
        if kind in ("integer", "decimal"):
            return pg_call(
                "TO_DATE", exp.Cast(this=node, to=exp.DataType.build("TEXT")), exp.Literal.string("YYYYMMDD")
            )
        return node

    def truncated_clock(clock: exp.Expression, precision: int) -> exp.Expression:
        """``clock`` (a timestamp) truncated to ``precision`` fractional digits, as MySQL
        truncates NOW(fsp) -- a cast to timestamp(p) would round, half the time into the
        next second (or the next day, at midnight)."""
        if precision >= 6:
            return clock
        if precision == 0:
            return pg_call("DATE_TRUNC", exp.Literal.string("second"), clock)
        if precision == 3:
            return pg_call("DATE_TRUNC", exp.Literal.string("milliseconds"), clock)
        unit = 10 ** (6 - precision)
        micros = exp.Mod(
            this=exp.Extract(this=exp.var("MICROSECONDS"), expression=clock.copy()),
            expression=exp.Literal.number(1000000),
        )
        kept = exp.Mul(
            this=exp.Floor(this=exp.Div(this=micros, expression=exp.Literal.number(unit))),
            expression=exp.Literal.number(unit),
        )
        return exp.Add(
            this=pg_call("DATE_TRUNC", exp.Literal.string("second"), clock),
            expression=exp.Mul(this=kept, expression=exp.Interval(this=exp.Literal.string("1 microsecond"))),
        )

    def fraction_digits(node: Optional[exp.Expression]) -> int:
        """The fractional digits MySQL writes a temporal value with: a column's or a cast's
        precision, date arithmetic's its date's, a choice's widest -- else 0."""
        while isinstance(node, exp.Paren):
            node = node.this
        if isinstance(node, exp.Column):
            declared = (column_type(node) if column_type is not None else None) or ""
            match = re.match(r"^\w+\s*\(\s*(\d+)\s*\)", declared)
            return int(match.group(1)) if match else 0
        if isinstance(node, (exp.Cast, exp.TryCast)):
            to = node.args.get("to")
            params = to.expressions if isinstance(to, exp.DataType) else []
            return int(params[0].name) if params and params[0].name.isdigit() else 0
        if isinstance(node, (exp.Add, exp.Sub, exp.DateAdd, exp.DateSub)) and interval_of(node) is not None:
            return fraction_digits(node.this)
        if isinstance(node, (exp.Min, exp.Max, exp.AnyValue)):
            return fraction_digits(node.this)
        branches = choices(node)
        return max((fraction_digits(b) for b in branches if b is not None), default=0)

    def choices(node: Optional[exp.Expression]) -> "list[Optional[exp.Expression]]":
        """The values a COALESCE / IFNULL, GREATEST / LEAST, IF or CASE chooses between."""
        if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
            return [node.this, *node.expressions]
        if isinstance(node, exp.If):
            return [node.args.get("true"), node.args.get("false")]
        if isinstance(node, exp.Case):
            return [b.args.get("true") for b in node.args.get("ifs") or ()] + [node.args.get("default")]
        return []

    def mysql_type(node: exp.Expression) -> Optional[str]:
        """The MySQL type a derived table's or CTE's computed column has, by what its
        expression computes (:attr:`_ColumnTypes.typed_by`): a quotient a decimal of its
        scale, a DATETIME(6) its precision."""
        kind = kind_of(node)
        if kind == "decimal":
            scale = decimal_scale(node)
            if scale is None:
                return None
            # A body MySQL materializes stores the column at that scale -- and so does the
            # conversion (its outputs are shown): no digit is left to show or to key on.
            body = node.find_ancestor(exp.Select)
            while body is not None and isinstance(body.parent, exp.SetOperation):
                body = body.parent
            holder = body.parent if body is not None else None
            stored = body is not None and (
                isinstance(holder, exp.CTE)
                or (isinstance(holder, exp.Subquery) and holder.args.get("alias") is not None)
            ) and _mysql_materializes(body)
            return (_QuotientType if carries_quotient(node) and not stored else str)(f"decimal(65,{scale})")
        if kind in ("datetime", "timestamp", "time"):
            precision = fraction_digits(node)
            return f"{kind}({precision})" if precision else kind
        return _KIND_TYPES.get(kind or "")

    def zero_steps(document: exp.Expression) -> int:
        """How many [0] steps of a nest of followed JSON paths ``document`` holds (each type
        test written for one is marked with the count so far)."""
        return max((node.meta.get(_ZERO_STEPS, 0) for node in document.walk()), default=0)

    def json_at(document: exp.Expression, steps: "list") -> exp.Expression:
        """The JSON value at ``steps`` (:func:`_json_path_steps`) of ``document``, as MySQL's
        path finds it: a member name only in an object, an index only in an array -- and [0]
        of a value that is no array the value itself (MySQL reads it as an array of one)."""
        if all(isinstance(step, str) and not re.fullmatch(r"\s*[+-]?\d+\s*", step) for step in steps):
            return exp.JSONExtract(this=document, expression=_json_path(steps))
        # The operators: JSON_EXTRACT_PATH reads a name that is a number as an index too.
        value = document
        run: list = []
        zeros = zero_steps(document)
        for step in [*steps, None]:
            if step is not None and not (type(step) is int and step == 0):
                run.append(step)
                continue
            if run:
                value = exp.Paren(this=exp.JSONExtract(this=value, expression=_json_path(run), only_json_types=True))
                run = []
            if step is not None:
                bare = value.this if isinstance(value, exp.Paren) else value
                zeros += 1
                typed = pg_call("JSON_TYPEOF", bare.copy())
                typed.meta[_ZERO_STEPS] = zeros
                value = exp.Case(
                    ifs=[exp.If(
                        this=exp.EQ(this=typed, expression=exp.Literal.string("array")),
                        true=exp.JSONExtract(this=value.copy(), expression=_json_path([0]), only_json_types=True),
                    )],
                    default=bare,
                )
        return value

    def json_text_at(document: exp.Expression, steps: "list") -> exp.Expression:
        """The text of the JSON value at ``steps`` -- a string unquoted -- NULL for a JSON
        null."""
        found = json_at(document, steps)
        if isinstance(found, exp.JSONExtract):
            return exp.JSONExtractScalar(this=found.this, expression=found.expression)
        bare = found.this if isinstance(found, exp.Paren) else found
        return exp.Paren(this=exp.JSONBExtractScalar(this=bare, expression=exp.Literal.string("{}")))

    def standing(node: exp.Expression) -> "tuple[exp.Expression, Optional[exp.Expression]]":
        """``(held, parent)``: ``node`` with the parentheses around it, and what holds them."""
        held, parent = node, node.parent
        while isinstance(parent, exp.Paren):
            held, parent = parent, parent.parent
        return held, parent

    def compared_with(held: exp.Expression, parent: Optional[exp.Expression]) -> "Optional[list]":
        """The values ``held`` is compared with -- =, <, IN, BETWEEN, a simple CASE's WHEN
        values (an IN subquery's: none told) -- or None where it is not compared."""
        if isinstance(parent, exp.Between):
            return [parent.args.get("low"), parent.args.get("high")] if parent.this is held else [parent.this]
        if isinstance(parent, exp.In):
            if parent.args.get("query") is not None:
                return []
            return list(parent.expressions) if parent.this is held else [parent.this]
        if isinstance(parent, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ)):
            return [parent.expression if parent.this is held else parent.this]
        if isinstance(parent, exp.Case) and parent.this is held:
            return [branch.this for branch in parent.args.get("ifs") or ()]
        return None

    def read_as_date(held: exp.Expression, parent: Optional[exp.Expression]) -> bool:
        """Whether a value is read as a date or a time where it stands (the date functions
        written above): DATE / YEAR / DAYOFWEEK / DATE_FORMAT ... of it, EXTRACT, LAST_DAY,
        CONVERT_TZ, a cast to a date or a time, a TIME put on the current date, the date of
        date arithmetic."""
        if isinstance(parent, (exp.TsOrDsToDate, exp.TsOrDsToTimestamp, exp.LastDay, exp.ConvertTimezone)):
            return True
        if isinstance(parent, exp.Extract):
            return held is parent.expression
        if isinstance(parent, (exp.Cast, exp.TryCast)):
            return _pg_type_kind(parent.args.get("to")) in _TEMPORAL_KINDS
        if isinstance(parent, exp.Add) and isinstance(parent.this, exp.CurrentDate):
            return held is parent.expression  # as_temporal's TIME on the current date
        return isinstance(parent, (exp.Add, exp.Sub, exp.DateAdd, exp.DateSub)) and (
            interval_of(parent) is not None and held is parent.this
        )

    def text_context(node: exp.Expression) -> Optional[str]:
        """How MySQL reads a string -- the text of a JSON value (->>), a choice MySQL makes a
        string -- or a JSON value where it stands: 'number' -- compared, IN or BETWEEN with
        numbers, the operand of a simple CASE with number WHENs, in arithmetic, SUM / AVG,
        ROUND / TRUNCATE or a numeric function (ABS, CEIL, SQRT ...); 'remainder' -- MOD /
        DIV, which take a decimal's remainder; 'cast' -- cast to a number; 'plain' -- cast
        to another type, or read by a date function or parsed by STR_TO_DATE (which read a
        JSON null as NULL); 'other' -- compared with a value known not to be text (a date,
        a time, a boolean, json, bytes), or an INTERVAL amount or the date of date
        arithmetic; else None -- shown, made text, compared with text or with a value
        whose type cannot be told (PostgreSQL compares text with text, and reads a
        placeholder's type from it)."""
        held, parent = standing(node)
        if isinstance(parent, exp.Distinct) and isinstance(parent.parent, (exp.Sum, exp.Avg)):
            held, parent = parent, parent.parent  # SUM(DISTINCT x) reads x as SUM(x) does
        if isinstance(parent, exp.Interval) or (
            isinstance(parent, (exp.Add, exp.Sub, exp.DateAdd, exp.DateSub)) and interval_of(parent) is not None
        ):
            return "other"
        if read_as_date(held, parent):
            return "plain"
        if isinstance(parent, (exp.Mod, exp.IntDiv)):
            return "remainder"
        if isinstance(parent, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Neg, *_NUMERIC_FUNCTIONS)):
            return "number"
        if isinstance(parent, (exp.Sum, exp.Avg, exp.Round, exp.Trunc)) and parent.this is held:
            return "number"
        if isinstance(parent, (exp.Cast, exp.TryCast)):
            kind = _pg_type_kind(parent.args.get("to"))
            return "cast" if kind in _NUMERIC_KINDS else None if kind in (None, "text", "json") else "plain"
        if isinstance(parent, exp.Nullif) and isinstance(parent.parent, exp.Anonymous) and (
            parent.parent.meta.get(_PG_WRITTEN) and (parent.parent.name or "").upper() in ("TO_DATE", "TO_TIMESTAMP")
        ):
            return "plain"  # STR_TO_DATE's value: MySQL parses 'null' as no date, NULL
        others = compared_with(held, parent)
        if others is None:
            return None
        kinds = {kind_of(o) for o in others if o is not None and not isinstance(o, exp.Null)}
        if kinds and kinds <= _NUMERIC_KINDS:
            return "number"
        return "other" if kinds & _UNTEXTED_KINDS else None

    # The node classes the statement holds, read in one walk: a pass over a construct it
    # does not hold is skipped -- each pass walks the whole tree, and Evaluation converts
    # thousands of views. No pass needs a class that only an earlier pass writes.
    walked = list(select.walk())
    present = {type(node) for node in walked}

    def nodes(*types: type, innermost: bool = False) -> "list[exp.Expression]":
        if not any(issubclass(cls, types) for cls in present):
            return []
        return _innermost_first(select, *types) if innermost else list(select.find_all(*types))

    if resolver is not None:
        resolver.typed_by = mysql_type
    _drop_alias_db(select, resolver.base_columns if resolver is not None else None)
    if on_typed is not None:
        on_typed()
    unquote_json_extracts(select)
    for join in nodes(exp.Join):
        # A JOIN without ON is MySQL's CROSS JOIN -- MySQL stores JOIN ... CROSS JOIN c as
        # ((a join b on(...)) join c), and a LATERAL join with no ON -- which sqlglot writes
        # as a comma: that binds looser than JOIN (a JOIN b JOIN c ON <a's column> is a, (b
        # JOIN c ...) to PostgreSQL, which cannot see a there), and PostgreSQL refuses it
        # inside parentheses. A comma stays one outside them. MySQL's INNER JOIN without ON
        # is a CROSS JOIN too, and its CROSS JOIN ... ON an inner JOIN. STRAIGHT_JOIN is
        # MySQL's.
        kind = (join.args.get("kind") or "").upper()
        if kind == "STRAIGHT_JOIN" or (kind == "CROSS" and join.args.get("on") is not None):
            join.set("kind", None)
        holder = join.parent
        container = holder.parent if holder is not None else None
        nested = (
            isinstance(container, exp.Subquery)
            and container.this is holder
            and container.args.get("alias") is None
        )
        if (
            (nested or not join.meta.get(_COMMA_JOIN))
            and join.args.get("on") is None
            and not join.args.get("using")
            and (join.args.get("kind") or "").upper() in ("", "INNER")
            and not join.args.get("side")
            and not join.args.get("method")
        ):
            join.set("kind", "CROSS")

    # MySQL adds an interval on either side (INTERVAL 1 DAY + d), and stores a view's as d +
    # INTERVAL 1 DAY: the date first, as the passes below read it.
    for node in [n for n in walked if isinstance(n, exp.Add)]:
        if isinstance(node.this, exp.Interval) and not isinstance(node.expression, exp.Interval):
            interval = node.this
            node.set("this", node.expression)
            node.set("expression", interval)

    def assigned(node: exp.Expression) -> bool:
        """An UPDATE's SET item, or an upsert's (ON DUPLICATE KEY UPDATE, ON CONFLICT)."""
        return isinstance(node, exp.EQ) and node.arg_key == "expressions" and isinstance(
            node.parent, (exp.Update, exp.OnConflict)
        )

    def read_as_number(held: exp.Expression) -> bool:
        """Whether MySQL reads the value ``held`` (with its parentheses) as a number where it
        stands: in arithmetic, a bitwise operator, a numeric function, cast or aggregate, or
        beside a number -- compared (<=> too), IN, BETWEEN, IFNULL / COALESCE / NULLIF /
        GREATEST, a branch of a CASE / IF, a simple CASE's operand or WHEN value."""
        parent = held.parent
        if isinstance(parent, exp.Distinct) and isinstance(parent.parent, _NUMERIC_AGGREGATES):
            parent = parent.parent
        if isinstance(parent, _NUMERIC_OPERATIONS):
            return interval_of(parent) is None  # not date arithmetic
        if isinstance(parent, exp.Cast):
            return _pg_type_kind(parent.args.get("to")) in _NUMERIC_KINDS
        beside: "list" = []
        if isinstance(parent, (exp.Coalesce, exp.Greatest, exp.Least, exp.In)):
            beside = [parent.this, *parent.expressions]
        elif isinstance(
            parent, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ, exp.Nullif)
        ):
            beside = [parent.this, parent.expression]
        elif isinstance(parent, exp.Between):
            beside = [parent.this, parent.args.get("low"), parent.args.get("high")]
        elif isinstance(parent, exp.Case) and held.arg_key == "this":
            beside = [branch.this for branch in parent.args.get("ifs") or ()]  # CASE x WHEN 1
        # A CASE's WHEN branch is an If in its ifs; MySQL's IF() -- an If too -- that is the
        # CASE's ELSE or operand chooses by its own condition, a boolean.
        elif isinstance(parent, exp.If) and held.arg_key == "this" and parent.arg_key == "ifs" and isinstance(
            parent.parent, exp.Case
        ):
            beside = [parent.parent.this]  # a simple CASE's WHEN value, beside its operand
        elif isinstance(parent, (exp.If, exp.Case)) and held.arg_key in ("true", "false", "default"):
            branch = isinstance(parent.parent, exp.Case) and parent.arg_key == "ifs" and held.arg_key == "true"
            case = parent.parent if branch else parent
            beside = (
                [case.args.get("true"), case.args.get("false")] if isinstance(case, exp.If)
                else [b.args.get("true") for b in case.args.get("ifs") or ()] + [case.args.get("default")]
            )
        return any(kind_of(value) in _NUMERIC_KINDS for value in beside if value is not None and value is not held)

    # A TINYINT(1) -- a boolean on Aurora DSQL -- is MySQL's integer 0 / 1, and so is a
    # comparison or a condition (SUM(qty > 2) counts rows): where MySQL reads one as a number
    # (read_as_number) it is written as that integer -- boolean + integer has no operator
    # there, sum(boolean) no function. Elsewhere it stays a boolean: a condition, AND / OR /
    # NOT, IS TRUE, a comparison with another boolean, an output -- and an assignment's
    # target (SET paid = 1), a number assigned to one written as a boolean. Innermost first,
    # each wrapped where it stands.
    holders = (
        exp.Column, exp.If, exp.Case, exp.Coalesce, exp.Nullif, exp.Greatest, exp.Least, exp.Subquery, exp.Window,
    )
    for node in reversed(walked):
        # ANY / SOME / ALL quantify a comparison over a subquery's rows -- no value of their
        # own -- and an IN's subquery is the set it reads (EXISTS (...) is a value: + 0 adds).
        predicate = isinstance(node, (exp.Predicate, exp.Connector, exp.Not, exp.Boolean)) and not isinstance(
            node, (exp.Any, exp.All)
        )
        if not predicate and not (isinstance(node, holders) and kind_of(node) == "boolean"):
            continue
        held = node
        while isinstance(held.parent, exp.Paren):
            held = held.parent
        if (assigned(held.parent) and held is held.parent.this) or (
            isinstance(held.parent, exp.In) and held.arg_key == "query"
        ):
            continue
        if read_as_number(held):
            cast = _as_integer(exp.Null())
            node.replace(cast)
            cast.set("this", node)

    def as_flag(value: exp.Expression) -> None:
        """A number stored in a TINYINT(1) -- a boolean on Aurora DSQL, which has no cast from
        an integer to one -- written where it stands: TRUE / FALSE for a literal, else (v <>
        0)."""
        literal = _integer_literal(value)
        if literal is not None:
            value.replace(exp.Boolean(this=literal != 0))
            return
        flag = exp.NEQ(this=exp.Null(), expression=exp.Literal.number(0))
        value.replace(flag)
        flag.set("this", exp.Paren(this=value) if isinstance(value, exp.Binary) else value)

    for node in [n for n in walked if assigned(n)]:
        if kind_of(node.this) == "boolean" and kind_of(node.expression) == "integer":
            as_flag(node.expression)
    # An INSERT's values the same way, by the columns it names -- else the table's own, in
    # the inventory's order; a table it cannot type is left as written: each VALUES row, and
    # each output of its SELECT (of each UNION branch).
    for insert in nodes(exp.Insert):
        target = insert.this
        table = target.this if isinstance(target, exp.Schema) else target
        columns = (resolver.base_columns(table) if resolver is not None else None) or {}
        names = [c.name.lower() for c in target.expressions] if isinstance(target, exp.Schema) else list(columns)
        flags = [position for position, name in enumerate(names) if _mysql_kind(columns.get(name)) == "boolean"]
        source = insert.expression
        if not flags or not isinstance(source, exp.Expression):
            continue
        rows = (
            [row for row in source.expressions if isinstance(row, exp.Tuple)] if isinstance(source, exp.Values)
            else _output_queries(source)
        )
        for row in rows:
            items = row.expressions
            if len(items) != len(names):
                continue
            # Each typed while all are still in the tree.
            for item in [items[p].unalias() for p in flags if kind_of(items[p].unalias()) == "integer"]:
                as_flag(item)

    # MySQL computes a FLOAT in double precision: PostgreSQL computes real op real in real
    # (with any other operand in double precision) -- and its sum(real) adds in real, losing
    # the cents of 100000.25 + 23456.53 before any later cast. MOD of a FLOAT / DOUBLE is C's
    # fmod in MySQL, which Aurora DSQL has no operator for, nor an exact form of: reported.
    # Innermost first, each operator's kind read once (a chain of thousands stays linear):
    # (f * f) * f is double precision once f * f is.
    arithmetic: dict = {}

    def operand_kind(node: exp.Expression) -> Optional[str]:
        while isinstance(node, exp.Paren):
            node = node.this
        known = arithmetic.get(id(node))
        return known[1] if known is not None and known[0] is node else kind_of(node)

    for node in reversed(walked):
        if not isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Mod)) or interval_of(node) is not None:
            continue
        kinds = [operand_kind(node.this), operand_kind(node.expression)]
        if kinds == ["float4", "float4"] and not isinstance(node, exp.Mod):
            for key in ("this", "expression"):
                node.set(key, exp.Cast(this=node.args[key], to=exp.DataType.build("DOUBLE PRECISION")))
            kinds = ["float8", "float8"]
        if isinstance(node, exp.Mod) and set(kinds) & _FLOAT_KINDS:
            issue(
                "MOD() / % of a FLOAT or DOUBLE is C's fmod in MySQL, which Aurora DSQL has no "
                "operator for (the statement fails there) and no exact form of: x - TRUNC(x / y) "
                "* y in double precision gives fmod's value except where x / y rounds to a whole "
                "number (MOD(1, 0.1e0) is 0.09999999999999995 in MySQL, 0 that way). Rewrite it "
                "-- in numeric (CAST(x AS NUMERIC) % y) where the values are decimals at heart."
            )
        arithmetic[id(node)] = (node, combine(*kinds))
    for node in [n for n in walked if isinstance(n, exp.Sum)]:
        value = _aggregated(node)
        if value is not None and kind_of(value) == "float4":
            # Wrapped where it stands, not copied: the passes below read the nodes as walked
            # (a % b in it divides by NULLIF(b, 0) too).
            cast = exp.Cast(this=exp.Null(), to=exp.DataType.build("DOUBLE PRECISION"))
            value.replace(cast)
            cast.set("this", value)

    # MySQL's a % b, MOD(a, b) and a DIV b are NULL for a 0 divisor, as its division is;
    # Aurora DSQL raises division by zero.
    for node in [n for n in walked if isinstance(n, (exp.Mod, exp.IntDiv))]:
        divisor = node.expression
        bare = divisor.unnest() if isinstance(divisor, exp.Expression) else None
        if not (
            (isinstance(bare, exp.Literal) and bare.is_number and float(bare.name) != 0)
            or (isinstance(divisor, exp.Nullif) and _integer_literal(divisor.expression) == 0)
        ):
            node.set("expression", exp.Nullif(this=divisor, expression=exp.Literal.number(0)))
    # MySQL's SIGN is an integer; PostgreSQL's sign(integer) a double precision.
    for node in [n for n in walked if isinstance(n, exp.Sign)]:
        cast = _as_integer(exp.Null())
        node.replace(cast)
        cast.set("this", node)

    def windowed(node: exp.Expression) -> Optional[exp.Window]:
        """The window ``node`` is the function of (under FILTER / RESPECT NULLS), or None."""
        while isinstance(node.parent, (exp.Filter, exp.RespectNulls, exp.IgnoreNulls)) and node.arg_key == "this":
            node = node.parent
        return node.parent if isinstance(node.parent, exp.Window) and node.arg_key == "this" else None

    def stored_argument(function: exp.Expression) -> None:
        """MySQL keeps a window function's row values in the window's buffer, each at its
        scale: SUM(a / b) OVER () adds 0.3333, not 0.333333333 (a constant is not kept
        there)."""
        value = _aggregated(function)
        if value is not None and value.find(exp.Column) is not None and carries_quotient(value):
            value.replace(shown(value.copy(), stored=True))

    def exact_quotient(
        dividend: exp.Expression, divisor: exp.Expression, found: "tuple[int, int]"
    ) -> exp.Expression:
        """MySQL's exact division as written for Aurora DSQL: DIV(x * 10^k, NULLIF(y, 0)) *
        10^-k, k the digits MySQL keeps (``found``: :func:`scales`)."""
        if kind_of(dividend) == "integer":  # an integer times 10^k overflows int4 / int8
            dividend = exp.Cast(this=dividend, to=exp.DataType.build("NUMERIC"))
        if not (isinstance(divisor, exp.Nullif) and _integer_literal(divisor.expression) == 0):
            divisor = exp.Nullif(this=divisor, expression=exp.Literal.number(0))  # a / NULLIF(b, 0) already is
        shifted = exp.Mul(this=dividend, expression=exp.Literal.number(10 ** found[1]))
        quotient = exp.Mul(
            this=exp.IntDiv(this=shifted, expression=divisor),
            expression=exp.Literal.number(f"{decimal.Decimal(1).scaleb(-found[1]):f}"),
        )
        quotient.meta[_QUOTIENT_SCALE] = found
        return quotient

    # MySQL's exact division: integers and decimals divide to a DECIMAL shown at the
    # dividend's scale plus div_precision_increment (4) -- 10 / 3 is 3.3333 -- never a
    # double, and x / 0 is NULL. It computes with more digits, truncated
    # (_mysql_quotient_digits: 2 / 3 is 0.666666666, so 2 / 3 * 1000000 is 666666.6660): so
    # is it written, exactly, as DIV(x * 10^k, y) * 10^-k -- PostgreSQL's numeric division
    # keeps 16 significant digits, rounded, fewer than MySQL's from a quotient of 10^8 on.
    # shown() gives an output MySQL's scale. An AVG of an exact value is MySQL's division of
    # its SUM by its COUNT (of the DISTINCT values, over the same window), the same exact
    # division -- PostgreSQL's avg is the rounded one. Innermost first: a quotient divided is
    # one too.
    for node in nodes(exp.Div, exp.Avg, innermost=True):
        if node.args.get("typed"):
            continue
        if isinstance(node, exp.Avg):
            if windowed(node) is not None:
                stored_argument(node)
            found = scales(node) if kind_of(_aggregated(node)) in _EXACT_KINDS else None
            if found is None:
                continue
            target = node
            while isinstance(target.parent, (exp.Window, exp.Filter)) and target.arg_key == "this":
                target = target.parent  # the window, never AVG(x) inside it

            def over(function: exp.Expression) -> exp.Expression:
                """``function`` in place of the AVG, under its window."""
                if target is node:
                    return function
                copy = target.copy()
                inner = copy
                while not isinstance(inner, exp.Avg):
                    inner = inner.this
                inner.replace(function)
                return copy

            total = over(exp.Sum(this=node.this.copy()))
            if kind_of(_aggregated(node)) == "integer":  # typed in place: the copy is no column of the tree
                total = exp.Cast(this=total, to=exp.DataType.build("NUMERIC"))
            quotient = exact_quotient(total, over(exp.Count(this=node.this.copy())), found)
            if target is not node:
                # A window's result is kept at its scale before what computes with it:
                # AVG(x) OVER (...) * 1000000000 multiplies 1.3333, not 1.333333333.
                quotient = shown(quotient, stored=True)
            elif isinstance(target.parent, (exp.Div, exp.Mod, exp.IntDiv)) and target.arg_key == "expression":
                quotient = exp.Paren(this=quotient)
            target.replace(quotient)
            continue
        kinds = {kind_of(node.this), kind_of(node.expression)}
        found = scales(node) if kinds <= _EXACT_KINDS else None
        if found is None:
            continue
        quotient = exact_quotient(node.this, node.expression, found)
        if isinstance(node.parent, (exp.Div, exp.Mod, exp.IntDiv)) and node.arg_key == "expression":
            quotient = exp.Paren(this=quotient)
        node.replace(quotient)

    # A window function's result is kept at its scale too, before what computes with it:
    # SUM(1 / 3) OVER () * 1000000000 multiplies 0.3333 (an AVG's is kept above).
    for window in nodes(exp.Window):
        function = window.this
        while isinstance(function, (exp.Filter, exp.RespectNulls, exp.IgnoreNulls)):
            function = function.this
        if not isinstance(function, exp.Func):
            continue  # a WINDOW clause's own definition (WINDOW w AS (...)): a name, no function
        if not isinstance(function, exp.Avg):  # an AVG's argument is kept above
            stored_argument(function)
        if carries_quotient(window):
            window.replace(shown(window.copy(), stored=True))

    # MySQL keys a GROUP BY, a window's PARTITION BY / ORDER BY and a COUNT(DISTINCT ...) on
    # a quotient at its shown scale -- 1 / 3 and 3333 / 10000 are one key, 0.3333 -- where
    # Aurora DSQL keys on the digits it keeps; rewriting the key would change what the
    # select list may read, so it is reported. GROUP BY 1 or an alias keys on the output,
    # shown at that scale on both.
    keys: list = []
    for group in nodes(exp.Group):
        keys += group.expressions
    for window in nodes(exp.Window):
        order = window.args.get("order")
        keys += [*(window.args.get("partition_by") or ()), *(o.this for o in (order.expressions if order else ()))]
    for count in nodes(exp.Count):
        if isinstance(count.this, exp.Distinct):
            keys += count.this.expressions
    if any(carries_quotient(key) and decimal_scale(key) is not None for key in keys):
        issue(
            "A quotient (an AVG) is a GROUP BY, PARTITION BY, window ORDER BY or COUNT(DISTINCT "
            "...) key: MySQL keys it at the scale it shows -- 1 / 3 and 3333 / 10000 are one "
            "key, 0.3333 -- where Aurora DSQL keys it by every digit it keeps, so groups, ranks "
            "or counts can differ. Key it by ROUND(q, 4) (the scale MySQL shows the quotient "
            "at) where that matters."
        )

    # Dates written with a format: from the format MySQL wrote (innermost first, so a
    # STR_TO_DATE inside a DATE_FORMAT is written too).
    for node in nodes(exp.Anonymous, innermost=True):
        name = (node.name or "").upper()
        if name not in ("DATE_FORMAT", "STR_TO_DATE", "FROM_UNIXTIME") or len(node.expressions) != 2:
            continue
        value = node.expressions[0]
        parsing = name == "STR_TO_DATE"
        kinds: set = set()
        dynamic = False
        leaves = _format_leaves(node.expressions[1])
        for leaf in leaves:
            if isinstance(leaf, exp.Literal) and leaf.is_string:
                specifiers = set(_mysql_format_specifiers(leaf.name))
                timed = bool(specifiers & _MYSQL_TIME_SPECIFIERS)
                dated = bool(specifiers & _MYSQL_DATE_SPECIFIERS)
                kinds.add("datetime" if timed and dated else "time" if timed else "date")
                pattern, unmapped = _mysql_format_to_pg(leaf.name, parsing=parsing)
                misformat(name, unmapped)
                leaf.replace(exp.Literal.string(pattern))
            elif not isinstance(leaf, exp.Null):
                dynamic = True
        if dynamic:
            issue(
                f"{name}() is given a format read at run time (a column or an expression), "
                "which Aurora DSQL reads in its own pattern language, not MySQL's: write the "
                "format as a literal."
            )
        fmt = node.expressions[1]
        if name == "DATE_FORMAT":
            node.replace(pg_call("TO_CHAR", exp.TsOrDsToTimestamp(this=as_temporal(value)), fmt))
        elif name == "FROM_UNIXTIME":
            # Under a source zone (G157) the zone's wall clock of the instant, as MySQL
            # formats it there; else the session's (TO_CHAR of a timestamptz).
            instant = exp.UnixToTime(this=value)
            node.replace(pg_call(
                "TO_CHAR", _zone_clock(zone, instant) if zone is not None else instant, fmt
            ))
        else:
            # Typed as MySQL types it: a format chosen at run time is a DATETIME(6). An
            # empty string is NULL to MySQL for a format with a date (TO_DATE reads it as
            # 0001-01-01 BC), midnight for a time-only one -- as TO_TIMESTAMP reads it.
            # Input left over after the format is ignored by both, and a value shorter than
            # it read as far as it goes.
            kind = kinds.pop() if len(kinds) == 1 and len(leaves) == 1 else "datetime"
            text = value if kind_of(value) in ("text", "char", None) else exp.Cast(
                this=value, to=exp.DataType.build("TEXT")
            )
            if kind != "time":
                text = exp.Nullif(this=text, expression=exp.Literal.string(""))
            if not (isinstance(value, exp.Literal) and value.is_string):
                issue(
                    "STR_TO_DATE() returns NULL for a value it cannot read with its format "
                    "(input left over after the format is ignored); Aurora DSQL's TO_DATE / "
                    "TO_TIMESTAMP raise an error for one they cannot read (or an impossible "
                    "date such as 31/02) -- failing every query that reads it -- and read one "
                    "whose separators differ as a date. If every value matches the format "
                    "nothing changes; otherwise guard the call (CASE WHEN value ~ '<pattern>' "
                    "THEN ... END). A date and time is read in the session's time zone, which "
                    "is UTC unless the session sets another: in a zone with daylight saving "
                    "time, a time in the hour the clocks skip comes back an hour later."
                )
            if kind == "date":
                node.replace(pg_call("TO_DATE", text, fmt))
            elif kind == "time":
                node.replace(exp.Cast(this=pg_call("TO_TIMESTAMP", text, fmt), to=exp.DataType.build("TIME")))
            else:
                # TO_TIMESTAMP reads the wall clock in the session's time zone, and the cast
                # gives it back -- the same, but for a time in a daylight-saving gap (noted).
                node.replace(exp.Cast(this=pg_call("TO_TIMESTAMP", text, fmt), to=exp.DataType.build("TIMESTAMP")))
    for node in nodes(exp.Anonymous):
        if (node.name or "").upper() == "TO_DAYS" and len(node.expressions) == 1:
            # Days since MySQL's year 0, of which 0001-01-01 is day 366.
            node.replace(exp.Paren(this=exp.Add(
                this=exp.Sub(
                    this=exp.Cast(this=as_temporal(node.expressions[0]), to=exp.DataType.build("DATE")),
                    expression=exp.Cast(
                        this=exp.Literal.string("0001-01-01"), to=exp.DataType.build("DATE")
                    ),
                ),
                expression=exp.Literal.number(366),
            )))
    for node in nodes(*_EXTRACT_FIELDS):
        node.replace(
            _as_integer(exp.Extract(this=exp.var(_EXTRACT_FIELDS[type(node)]), expression=node.this))
        )
    for node in nodes(exp.Week):
        # MySQL stores WEEKOFYEAR(x) as week(x, 3): the ISO week, EXTRACT's. Its other
        # modes have no Aurora DSQL counterpart and are reported.
        mode = node.args.get("mode")
        if isinstance(mode, exp.Literal) and mode.name == "3":
            node.replace(_as_integer(exp.Extract(this=exp.var("WEEK"), expression=node.this)))
    for node in nodes(exp.Second):
        node.replace(
            _as_integer(exp.Floor(this=exp.Extract(this=exp.var("SECOND"), expression=node.this)))
        )
    for node in nodes(exp.DayOfWeek):
        node.replace(
            exp.Paren(this=exp.Add(
                this=_as_integer(exp.Extract(this=exp.var("DOW"), expression=node.this)),
                expression=exp.Literal.number(1),
            ))
        )
    for node in nodes(exp.DateDiff):
        if node.args.get("unit") is None and node.expression is not None:
            node.replace(exp.Paren(this=exp.Sub(
                this=exp.Cast(this=node.this, to=exp.DataType.build("DATE")),
                expression=exp.Cast(this=node.expression, to=exp.DataType.build("DATE")),
            )))
    for node in nodes(exp.Time):
        node.replace(exp.Cast(this=node.this, to=exp.DataType.build("TIME")))
    for node in nodes(exp.Timestamp):
        if node.args.get("zone") is None:
            node.replace(exp.Cast(this=node.this, to=exp.DataType.build("TIMESTAMP")))
        else:
            note("TIMESTAMP")  # the two-argument form, which adds a time
    for node in nodes(exp.Dayname):
        # Of its date, as MONTHNAME is: a string or a number is a date to MySQL too.
        node.replace(written(exp.func(
            "TO_CHAR", exp.TsOrDsToDate(this=as_temporal(node.this)), exp.Literal.string("FMDay")
        )))

    def precision_of(node: exp.Expression) -> int:
        fsp = node.this if isinstance(node.this, exp.Literal) else (
            node.expressions[0] if isinstance(node, exp.Anonymous) and node.expressions else None
        )
        try:
            return int(fsp.name) if isinstance(fsp, exp.Literal) else 0
        except ValueError:
            return 0

    # The session's / UTC wall clock, truncated to its fsp as MySQL does (NOW() and its
    # synonyms, CURTIME, UTC_*). Each built from a fresh LOCALTIMESTAMP / CURRENT_TIMESTAMP
    # made after this pass, so no rewrite takes it again. Under a source zone (G149) the
    # session's clock becomes that zone's at the end, SYSDATE() with it: it is the time it
    # runs, NOW() the time its statement began, one value in a view's query either way.
    clocks: list = []
    for node in nodes(exp.CurrentTimestamp, exp.CurrentTime, *_NOW_SYNONYMS, *_UTC_CLOCKS):
        clocks.append(node)
    clock_names = ("now", "sysdate") if zone is not None else ("now",)
    clocks += [
        n for n in nodes(exp.Anonymous)
        if (n.name or "").lower() in clock_names and len(n.expressions) <= 1
    ]
    clocks += [n for n in nodes(exp.Column) if _utc_clock_kind(n) is not None]
    for node in clocks:
        precision = precision_of(node)
        utc_kind = _utc_clock_kind(node)
        if utc_kind is not None:
            clock = exp.AtTimeZone(this=exp.CurrentTimestamp(), zone=exp.Literal.string("UTC"))
            if utc_kind == "date":
                node.replace(exp.Cast(this=clock, to=exp.DataType.build("DATE")))
                continue
            truncated = truncated_clock(clock, precision)
            node.replace(
                exp.Cast(this=truncated, to=exp.DataType.build("TIME")) if utc_kind == "time" else truncated
            )
            continue
        truncated = truncated_clock(exp.Localtimestamp(), precision)
        if isinstance(node, exp.CurrentTime):
            node.replace(exp.Cast(this=truncated, to=exp.DataType.build("TIME")))
        else:
            node.replace(truncated)

    def json_keyed(held: exp.Expression, parent: Optional[exp.Expression]) -> bool:
        """Whether a value is compared, ordered, grouped or made distinct where it stands."""
        if compared_with(held, parent) is not None or isinstance(parent, (*_COMPARISONS, exp.Ordered, exp.Group)):
            return True
        if isinstance(parent, (exp.Min, exp.Max, exp.Distinct)):
            return True
        if isinstance(parent, exp.Window):
            return any(key is held for key in parent.args.get("partition_by") or ())
        query = parent.parent if isinstance(parent, exp.Alias) else parent
        listed = held if parent is query else parent
        return isinstance(query, exp.Select) and bool(query.args.get("distinct")) and any(
            projection is listed for projection in query.expressions
        )

    def json_number_note() -> None:
        issue(
            "MySQL reads a JSON value (JSON_EXTRACT / ->) or its text (->>, "
            "JSON_UNQUOTE(JSON_EXTRACT(...))) as a number where it meets one -- compared with a "
            "number, in arithmetic, SUM / AVG, ROUND / TRUNCATE, a numeric function, a cast -- "
            "and it is written so here (CAST(... AS DOUBLE PRECISION), NUMERIC in MOD / DIV). A "
            "text that is not a number reads as its leading digits in MySQL (0 when it has none, "
            "a JSON true 1) and fails the cast on Aurora DSQL; a JSON null, 0 in MySQL, is NULL "
            "here. If the values can be other than numbers, guard the cast."
        )

    def json_null_text(document: exp.Expression, steps: "list") -> exp.Expression:
        """MySQL's ->> as Aurora DSQL computes it: the text of the value, and of a JSON null
        the text 'null' (Aurora DSQL's is NULL)."""
        return exp.Coalesce(
            this=json_text_at(document, steps),
            expressions=[exp.Case(ifs=[exp.If(
                this=exp.Not(this=exp.Is(this=json_at(document.copy(), steps), expression=exp.Null())),
                true=exp.Literal.string("null"),
            )])],
        )

    # MySQL's JSON paths -- JSON_EXTRACT / -> and ->> (JSON_UNQUOTE(JSON_EXTRACT(...))) --
    # as Aurora DSQL follows them; one it cannot follow is reported.
    unfollowed: list = []
    # A ->> a query groups by is written as that GROUP BY key wherever the query reads it
    # (by its text as written): PostgreSQL computes a grouped query's outputs from its keys,
    # and a cast of the bare text is not one of them.
    grouped = {
        (id(group.parent), key.unnest().sql())
        for group in nodes(exp.Group)
        for key in group.expressions
        if isinstance(key.unnest(), exp.JSONExtractScalar)
    }
    # (A ->> a view stores as json_unquote(json_extract(...)) is one only since the walk.)
    as_written = {id(n): (n, n.sql()) for n in select.find_all(exp.JSONExtractScalar)} if grouped else {}
    for node in nodes(exp.JSONExtract, exp.JSONExtractScalar, innermost=True):
        steps = _json_path_steps(node.expression)
        # A [0] step holds its value three times, and a nested call's document is the inner
        # call's value: the steps are counted through the nest (27 copies a level, else).
        zeros = sum(1 for step in steps or () if type(step) is int and step == 0)
        if zeros:
            zeros += zero_steps(node.this)
        if steps is None or node.expressions or zeros > _MAX_ZERO_STEPS:
            # Left as MySQL's own call, which Aurora DSQL refuses -- JSON_EXTRACT_PATH would
            # read such a path as a key, NULL on every row (and sqlglot writes no wildcard) --
            # and reported below, not as a call left untranslated.
            holder = node.parent
            call = written(exp.Anonymous(this="JSON_EXTRACT", expressions=[
                node.this, *(_json_path_text(p) for p in [node.expression, *node.expressions])
            ]))
            node.replace(call)
            if isinstance(holder, exp.Anonymous) and (holder.name or "").upper() == "JSON_UNQUOTE":
                written(holder)
            unfollowed.append(call.sql(dialect=_MySqlViewDialect))
            continue
        document = node.this
        if kind_of(document) != "json":
            # MySQL reads a JSON document from a string too -- and a document whose type
            # cannot be told (another view's column) may be one: the cast leaves a json as it is.
            document = exp.Cast(this=document, to=exp.DataType.build("JSON"))
        held, parent = standing(node)
        context = text_context(node)
        if isinstance(node, exp.JSONExtract):
            if json_keyed(held, parent):
                issue(
                    "A JSON value (JSON_EXTRACT / ->) is compared, ordered, grouped or made "
                    "distinct: MySQL compares JSON values by their JSON type and value (the JSON "
                    "number 5 equals 5, the string \"5\" does not), and Aurora DSQL has no "
                    "comparison of json values -- the statement fails there. Compare, order or "
                    "group by its text instead: x ->> 'k' = 'v', or CAST(x ->> 'k' AS NUMERIC) "
                    "= 5 for a number."
                )
            elif context in ("number", "remainder", "cast"):
                # MySQL reads a JSON number as itself, a string by its text: the double of
                # its unquoted text.
                json_number_note()
                unquoted = json_text_at(document, steps)
                node.replace(unquoted if context == "cast" else exp.Cast(
                    this=unquoted, to=exp.DataType.build("NUMERIC" if context == "remainder" else "DOUBLE PRECISION")
                ))
                continue
            node.replace(json_at(document, steps))
            continue
        query = node.find_ancestor(exp.Select)
        known = as_written.get(id(node))
        text: Optional[exp.Expression] = None
        if context not in (None, "other") and query is not None and known is not None and (
            known[0] is node and (id(query), known[1]) in grouped
        ):
            # Its query groups by it: the GROUP BY key's own form under the reading, a JSON
            # null's 'null' read as the NULL the plain text is.
            text = exp.Nullif(this=json_null_text(document, steps), expression=exp.Literal.string("null"))
        elif context in ("number", "remainder", "cast", "plain"):
            text = json_text_at(document, steps)
        if context in ("number", "remainder", "cast"):
            json_number_note()
            node.replace(text if context == "cast" else exp.Cast(
                this=text, to=exp.DataType.build("NUMERIC" if context == "remainder" else "DOUBLE PRECISION")
            ))
            continue
        if context == "plain":
            node.replace(text)
            continue
        if context == "other":
            issue(
                "The text of a JSON value (->>, JSON_UNQUOTE(JSON_EXTRACT(...))) is compared "
                "with a value that is not text -- a date, a time, a boolean, a json value, bytes "
                "-- or is the date of date arithmetic or an INTERVAL amount: MySQL converts the "
                "text to the type it needs there (a date; an amount by its leading integer), "
                "Aurora DSQL does not and the statement fails. Cast it to that type."
            )
        # MySQL's ->> of a JSON null is the text 'null'; Aurora DSQL's is NULL.
        node.replace(json_null_text(document, steps))

    if unfollowed:
        issue(
            f"MySQL's {', '.join(dict.fromkeys(unfollowed))} (JSON_EXTRACT, -> or ->>) reads "
            "the whole document ('$'), a wildcard (* / **), last, a range (m to n), a negative "
            "index, a path computed at run time or more than three [0] steps, counted through "
            "the calls nested in its document (each one tests the value's type, which MySQL "
            "reads as an array of one when it is none) -- or several paths, whose values MySQL "
            "returns as an array: Aurora DSQL's JSON functions follow one fixed path of member "
            "names and array indexes, and the call is left as MySQL's (which Aurora DSQL "
            "refuses). Rewrite it."
        )

    def textual(node: exp.Expression) -> exp.Expression:
        """``node`` as MySQL makes it text where it wants a string (as_text) -- text, bytes
        and a value of a type that cannot be told stay as they are."""
        return as_text(node) if kind_of(node) in _TEXTED_KINDS else node

    def as_number(value: exp.Expression, to: str) -> exp.Expression:
        """A value of a choice MySQL makes a string, as the number MySQL reads from that
        string where a number is wanted (in double precision): a string literal its leading
        number ('' and 'n/a' are 0, '1.5x' 1.5), a text or a date its leading digits, a
        number itself (a quotient at the scale MySQL writes it, a FLOAT -- 6 digits -- noted)."""
        if isinstance(value, exp.Literal) and value.is_string:
            leading = re.match(r"\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", value.name)
            return exp.Literal.number(f"{decimal.Decimal(leading.group(1) if leading else 0):f}")
        kind = kind_of(value)
        if kind in ("text", "char"):
            return leading_number(value, to, fraction=True)
        if kind in _TEMPORAL_KINDS or kind == "json":
            return leading_number(as_text(value), to, fraction=True)
        if kind == "boolean":
            value = _as_integer(value)
        elif kind in _EXACT_KINDS:
            value = shown(value)
        float_text(kind)
        return exp.Cast(this=value, to=exp.DataType.build(to))

    def widened(node: exp.Coalesce) -> None:
        """A COALESCE of exact numbers as MySQL shows it: each value at the widest scale
        (IFNULL(price, 0) is 0.00 -- MySQL's IF / CASE show the value taken as it is) --
        marked, so JSON writes each value as itself, as MySQL does."""
        scale = decimal_scale(node)
        if not scale or carries_quotient(node):
            return  # a quotient's is shown at its scale already

        def at_widest(value: exp.Expression) -> exp.Expression:
            if isinstance(value, exp.Null) or decimal_scale(value) == scale:
                return value
            rounded = exp.Round(this=value, decimals=exp.Literal.number(scale))
            rounded.meta[_SHOWN] = rounded.meta[_WIDENED] = True
            return rounded
        each_choice(node, at_widest)

    def compared_with_date(held: exp.Expression, parent: Optional[exp.Expression]) -> bool:
        """Whether a value is compared with a date (not a string): MySQL compares a string
        with a DATE, DATETIME or TIMESTAMP as a date -- with a TIME as a string."""
        others = compared_with(held, parent) or []
        return any(
            kind_of(o) in ("date", "datetime", "timestamp") and not (isinstance(o, exp.Literal) and o.is_string)
            for o in others if o is not None
        )

    def read_alike(literals: "list[exp.Expression]", values: "list[exp.Expression]") -> bool:
        """Whether PostgreSQL reads every string literal beside ``values`` as their type
        with the literal's own text, so that MySQL's string of each value is Aurora DSQL's
        text of it: a number literal beside numbers ('0' beside an INT, '2.50' beside a
        DECIMAL, an integer beside a FLOAT / DOUBLE), an ISO date beside DATEs, an ISO date
        and time beside DATETIMEs without fractional digits, each a real date (MySQL's
        '0000-00-00' is none to PostgreSQL). (A TINYINT(1)'s text differs, as do a
        TIMESTAMP's -- its zone -- a fraction's and a quotient's, shown at its scale.)"""
        kinds = {kind_of(value) for value in values}
        if not literals or not kinds or None in kinds:
            return False
        if kinds <= _NUMERIC_KINDS:
            if any(carries_quotient(value) for value in values):
                return False
            # A fraction beside a DECIMAL; beside a FLOAT / DOUBLE an integer of at most 15
            # digits (a double's text of a longer one has an exponent).
            fraction = r"(?:\.[0-9]+)?" if kinds <= _EXACT_KINDS and "decimal" in kinds else ""
            digits = r"[0-9]*" if kinds <= _EXACT_KINDS else r"[0-9]{0,14}"
            pattern = rf"-?(?:0|[1-9]{digits}){fraction}"
            return all(re.fullmatch(pattern, s.name) and not re.fullmatch(r"-0(?:\.0+)?", s.name) for s in literals)
        timed = kinds == {"datetime"} and not any(fraction_digits(value) for value in values)
        if kinds != {"date"} and not timed:
            return False
        pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}" + (" [0-9]{2}:[0-9]{2}:[0-9]{2}" if timed else "")
        for literal in literals:
            try:
                (datetime.datetime if timed else datetime.date).fromisoformat(literal.name)
            except ValueError:
                return False
            if not re.fullmatch(pattern, literal.name):
                return False
        return True

    # Each query's ORDER BY expressions as written, read before the choices below are
    # rewritten (innermost first, an ORDER BY copy of a choice may be texted before the
    # select item it repeats): MySQL stores a view's ORDER BY 1 as the expression.
    ordered_as_written: dict = {}
    for query in nodes(exp.Select):
        order = query.args.get("order")
        if order is not None:
            ordered_as_written[id(query)] = (query, {o.this.unnest().sql() for o in order.expressions})

    def listed_unordered(held: exp.Expression, parent: Optional[exp.Expression]) -> bool:
        """Whether a value is an item of its query's select list -- what a derived table, a
        CTE, a view or an INSERT ... SELECT hands on -- that the query does not order by (nor
        a UNION branch, whose column PostgreSQL types by every branch), or a GROUP BY key of
        such an item: one decision per item, so the select list, the key and an ORDER BY
        copy of it are written alike (PostgreSQL matches them by expression)."""
        item = parent if isinstance(parent, (exp.Alias, exp.Group)) else held
        query = item.parent
        if not isinstance(query, exp.Select) or isinstance(query.parent, exp.SetOperation):
            return False
        written = held.unnest().sql()
        known = ordered_as_written.get(id(query))
        ordered_exprs = known[1] if known is not None and known[0] is query else set()
        if isinstance(parent, exp.Group):
            # A key takes the decision of the select item it repeats.
            for position, projection in enumerate(query.expressions):
                if projection.unalias().unnest().sql() == written:
                    return not ordered(query, position, projection, ordered_exprs, written)
            return written not in ordered_exprs
        position = next((i for i, p in enumerate(query.expressions) if p is item), None)
        if position is None:
            return False
        return not ordered(query, position, item, ordered_exprs, written)

    def ordered(query: exp.Select, position: int, item: exp.Expression, ordered_exprs: set, written: str) -> bool:
        """Whether ``query`` orders by its select item ``item`` at ``position``: by its
        position or its alias -- or, grouped or DISTINCT, by the expression it holds, which
        PostgreSQL then matches to the item (elsewhere an ORDER BY copy is MySQL's string
        on its own, and the item stays the number readers compute with)."""
        alias = item.alias.lower() if isinstance(item, exp.Alias) else ""
        order = query.args.get("order")
        matched = query.args.get("group") is not None or bool(query.args.get("distinct"))
        return (matched and written in ordered_exprs) or any(
            _integer_literal(o.this) == position + 1 or (
                isinstance(o.this, exp.Column) and not o.this.table and alias and o.this.name.lower() == alias
            )
            for o in (order.expressions if order is not None else ())
        )

    # A choice MySQL makes a STRING -- a COALESCE / CASE / IF of a date or a time beside a
    # string (a literal or a text column), of a number beside a string or a date, of a JSON
    # value beside either -- which PostgreSQL types by its other values (IFNULL(d, '') fails
    # there: '' read as a date). Beside strings PostgreSQL reads as its own values with their
    # text (read_alike: IFNULL(end_date, '9999-12-31'), IFNULL(qty, '0')) a date stays a date
    # and a number a number where the choice is shown -- what reads a derived table's, a
    # CTE's, a view's or an INSERT ... SELECT's output then computes with it as MySQL
    # computes with the string. Otherwise it is written as MySQL reads that string where it
    # stands: read as a date (a date function, date arithmetic, compared with a date) it
    # stays one when its strings are dates or times of its sort -- a DATE beside one with a
    # time a timestamp, which keeps it; a string with no digit, which MySQL reads as no date,
    # NULL in a date function and the zero date, below every date, compared ('-infinity');
    # read as a number (arithmetic, SUM / AVG, a numeric function, compared with a number)
    # it is the double MySQL reads from it (as_number). Anywhere else -- shown, made text,
    # in JSON, ordered, compared with text -- it is text: each value as MySQL's text of it,
    # a literal as written. A TIME beside a date is MySQL's DATETIME (the time on the current
    # date), and a COALESCE of exact numbers is shown at its widest scale. GREATEST / LEAST
    # compare a date and a string as dates in MySQL, a number and a string as strings
    # (GREATEST(10, '5') is '5') -- left as PostgreSQL's numbers beside a number string below
    # 2, whose string order is the numbers' (the clamp GREATEST(x, '0')).
    def chosen_as_mysql(node: exp.Expression) -> None:
        values = [v for v in choices(node) if v is not None and not isinstance(v, exp.Null)]
        literals = [v for v in values if isinstance(v, exp.Literal) and v.is_string]
        kinds = {kind_of(v) for v in values if not (isinstance(v, exp.Literal) and v.is_string)}
        dated, numbers = kinds & _TEMPORAL_KINDS, kinds & (_NUMERIC_KINDS | {"boolean"})
        strings = bool(literals or kinds & {"text", "char"})
        alike = read_alike(literals, [v for v in values if not (isinstance(v, exp.Literal) and v.is_string)])
        if isinstance(node, (exp.Greatest, exp.Least)):
            # Compared as strings, a number and a number string below 2 ('0', '1', '0.5') order
            # as the numbers do (checked on 8.4): PostgreSQL's numbers are MySQL's choice there.
            clamped = alike and all(re.fullmatch(r"[01](?:\.[0-9]+)?", s.name) for s in literals)
            if numbers and strings and not dated and not clamped:
                each_choice(node, textual)
            return
        if "time" in dated and len(dated) > 1 and not (strings or numbers):
            each_choice(node, lambda value: exp.Cast(
                this=as_temporal(value), to=exp.DataType.build("TIMESTAMP")
            ) if kind_of(value) == "time" else value)
            return
        if isinstance(node, exp.Coalesce) and kinds and kinds <= _EXACT_KINDS and not literals:
            widened(node)
            return
        json = "json" in kinds
        if not ((strings and (dated or numbers or json)) or (numbers and (dated or json))):
            return
        if dated and alike and text_context(node) not in ("number", "remainder"):
            return  # a date beside dates of its sort: its text is MySQL's string, and a date wherever MySQL reads one
        held, parent = standing(node)
        if dated and not (numbers or json) and (read_as_date(held, parent) or compared_with_date(held, parent)):
            dates = [literal for literal in literals if re.search(r"\d", literal.name)]
            if not kinds & {"text", "char"} and (
                (dated <= {"date", "datetime", "timestamp"} and all(_is_datetime_literal(s) for s in dates))
                or (dated == {"time"} and all(_is_time_literal(s) for s in dates))
            ):
                timed = {
                    kind_of(v) != "date" for v in values if not (isinstance(v, exp.Literal) and v.is_string)
                } | {not _is_date_literal(s) for s in dates}
                if len(timed) > 1 and interval_of(parent) is not None:
                    issue(
                        "A COALESCE / IFNULL / CASE / IF of a date and a date string with a time "
                        "(or of a DATETIME and a date string without one) goes into date "
                        "arithmetic: MySQL makes the choice a string and the arithmetic a string "
                        "whose form follows each row's value ('2024-03-06' for a date row, "
                        "'2024-06-02 10:00:00' for the string with a time); Aurora DSQL computes "
                        "a timestamp, every row with a time ('2024-03-06 00:00:00'). Cast or "
                        "format the result where its text matters."
                    )
                stamped = dated == {"date"} and not all(_is_date_literal(s) for s in dates)
                # A string with no digit is MySQL's zero date compared with a date -- below
                # every date ('' < NOW() holds) -- and no date (NULL) in a date function.
                zero = exp.Cast(this=exp.Literal.string("-infinity"), to=exp.DataType.build(
                    "DATE" if dated == {"date"} and not stamped else "TIMESTAMP"
                )) if dated != {"time"} and compared_with_date(held, parent) else exp.Null()

                def as_date(value: exp.Expression) -> exp.Expression:
                    if any(value is s for s in literals):
                        return value if any(value is s for s in dates) else zero.copy()
                    if stamped and kind_of(value) == "date":
                        return exp.Cast(this=value, to=exp.DataType.build("TIMESTAMP"))
                    return value

                each_choice(node, as_date)
                return
        context = text_context(node)
        if numbers and context in ("number", "remainder"):
            to = "NUMERIC" if context == "remainder" else "DOUBLE PRECISION"
            each_choice(node, lambda value: as_number(value, to))
            return
        if alike and listed_unordered(held, parent):
            return  # shown, as PostgreSQL's numbers: MySQL's text, and numbers to what reads them
        each_choice(node, textual)

    for node in nodes(exp.Coalesce, exp.Case, exp.If, exp.Greatest, exp.Least, innermost=True):
        chosen_as_mysql(node)
        known_kinds[id(node)] = (node, kind_of(node))  # read again by the choice it nests in
    known_kinds.clear()

    def compared_as_date(held: exp.Expression, parent: Optional[exp.Expression]) -> bool:
        """:func:`compared_with_date`, and also a simple CASE's WHEN value (compared with the
        CASE operand) and an argument of NULLIF, GREATEST or LEAST beside a date."""
        if compared_with_date(held, parent):
            return True
        case = parent.parent if isinstance(parent, exp.If) else None
        if isinstance(case, exp.Case) and parent.this is held and any(
            parent is branch for branch in case.args.get("ifs") or ()
        ):
            others = [case.this]  # None for a searched CASE
        elif isinstance(parent, exp.Nullif):
            others = [parent.expression if parent.this is held else parent.this]
        elif isinstance(parent, (exp.Greatest, exp.Least)):
            others = [o for o in (parent.this, *parent.expressions) if o is not held]
        else:
            return False
        return any(
            kind_of(o) in ("date", "datetime", "timestamp") and not (isinstance(o, exp.Literal) and o.is_string)
            for o in others if o is not None
        )

    # A date MySQL stores and Aurora DSQL has no value for -- the zero date, a zero month or
    # day, a day the month does not have -- compared with a date (the soft-delete idiom
    # deleted_at = '0000-00-00 00:00:00', G156; NULLIF(d, '0000-00-00'), a simple CASE's WHEN
    # value, GREATEST / LEAST, a DATE '...' or TIMESTAMP '...' literal, the number 0, G180):
    # Aurora DSQL refuses the literal (22008) -- or a date compared with a number (42883) --
    # and the rows it meant hold what Full Load wrote for such a date.
    for literal in nodes(exp.Literal):
        if not literal.is_string:
            # MySQL reads the number 0 compared with a date as the zero date.
            if re.fullmatch(r"0+", literal.name) and compared_as_date(*standing(literal)):
                issue(
                    "A date is compared with 0, MySQL's number for the zero date: Aurora DSQL "
                    "compares no date with a number (SQLSTATE 42883), and the rows it meant hold "
                    "what Full Load wrote for a zero date -- NULL, the column's DEFAULT or "
                    "1970-01-01 (see Limitations, zero and invalid MySQL dates). Compare with IS "
                    "NULL or that value instead."
                )
            continue
        if dateless_kind(literal.name) is None:
            continue
        held, parent = standing(literal)
        # DATE '...' / TIMESTAMP '...' (or a CAST to one): Aurora DSQL rejects the cast itself.
        typed = isinstance(parent, (exp.Cast, exp.TryCast)) and _pg_type_kind(
            parent.args.get("to")
        ) in ("date", "datetime", "timestamp")
        if typed:
            held, parent = standing(parent)
        if compared_as_date(held, parent):
            issue(
                f"A date is compared with '{literal.name}', a date MySQL stores and Aurora DSQL "
                "has no value for (a zero date, a zero month or day, or a day the month does not "
                "have): Aurora DSQL rejects that literal (SQLSTATE 22008), and the rows it meant "
                "hold what Full Load wrote for such a date -- NULL, the column's DEFAULT or "
                "1970-01-01 (see Limitations, zero and invalid MySQL dates). Compare with IS NULL "
                "or that value instead."
            )
        elif typed:
            issue(
                f"'{literal.name}' is read as a date, one MySQL stores and Aurora DSQL has no "
                "value for (a zero date, a zero month or day, or a day the month does not have): "
                "Aurora DSQL rejects it (SQLSTATE 22008), and the rows such a date meant hold "
                "what Full Load wrote for it -- NULL, the column's DEFAULT or 1970-01-01 (see "
                "Limitations, zero and invalid MySQL dates). Write NULL or that value instead."
            )

    # A UNION column whose branches mix numbers, dates or booleans with strings is MySQL's
    # string, which PostgreSQL types by its branches (integer beside varchar refused, 'N/A'
    # read as a date): each branch's value is MySQL's text of it -- unless every string
    # branch is a literal PostgreSQL reads as the others' type with its own text ('5' beside
    # integers, '2024-01-01' beside DATEs: read_alike), where the column is PostgreSQL's,
    # which what reads it computes with as MySQL computes with the string. (A recursive
    # CTE's column is its first SELECT's type, below.)
    for union in nodes(exp.SetOperation):
        if isinstance(union.parent, exp.SetOperation) or (
            isinstance(union.parent, exp.CTE) and isinstance(union.parent.parent, exp.With)
            and union.parent.parent.args.get("recursive")
        ):
            continue
        branches = _output_queries(union)
        if any(
            isinstance(p.unalias(), exp.Star) or isinstance(p.unalias().this, exp.Star)
            for branch in branches for p in branch.expressions
        ):
            continue  # its columns cannot be lined up
        for position in range(min((len(b.expressions) for b in branches), default=0)):
            kinds = {kind_of(b.expressions[position].unalias()) for b in branches}
            if not (kinds & {"text", "char"} and kinds & (_NUMERIC_KINDS | _TEMPORAL_KINDS | {"boolean"})):
                continue
            values = [b.expressions[position].unalias() for b in branches]
            values = [value for value in values if not isinstance(value.unnest(), exp.Null)]
            literals = [value for value in values if isinstance(value, exp.Literal) and value.is_string]
            if read_alike(literals, [value for value in values if not any(value is s for s in literals)]):
                continue
            for branch in branches:
                projection = branch.expressions[position]
                if kind_of(projection.unalias()) not in _TEXTED_KINDS:
                    continue
                if isinstance(projection, exp.Alias):
                    projection.set("this", as_text(projection.this, kept=True))
                    continue
                outputs = list(branch.expressions)
                texted = as_text(projection, kept=True)
                # Named as it was: CAST(t.d AS TEXT) is "d", a TO_CHAR "to_char".
                if isinstance(projection, exp.Column):
                    texted = exp.Alias(this=texted, alias=projection.this.copy())
                outputs[position] = texted
                branch.set("expressions", outputs)

    # A SELECT DISTINCT's ORDER BY must be an expression of its select list (PostgreSQL
    # refuses it otherwise): a BINARY key is written as the select list writes it -- as the
    # list's BINARY x, or as the x the list holds (reported where x's bytes order otherwise).
    listed_binary: set = set()
    for query in nodes(exp.Select) if exp.Distinct in present else ():
        order = query.args.get("order")
        if not query.args.get("distinct") or order is None:
            continue
        listed = [projection.unalias() for projection in query.expressions]
        for ordered in order.expressions:
            key = ordered.this
            while isinstance(key, exp.Paren):
                key = key.this
            if isinstance(key, exp.Cast) and key in listed:
                listed_binary.add(id(key))

    def ordered_by_bytes(
        node: exp.Expression, operand: exp.Expression, kind: Optional[str], ordered: exp.Ordered
    ) -> None:
        """MySQL's ORDER BY BINARY x: by the bytes of x's text. A text, a date, bytes order as
        themselves on Aurora DSQL (C collation); a number's digits do not (12 before 3) --
        ordered by CONVERT_TO(CAST(x AS TEXT), 'UTF8'), an output name by the expression it
        names (PostgreSQL reads a name there only bare). In a statement MySQL reads a name
        inside BINARY as a column of the query's FROM first -- one the resolver types -- and
        as an output name only when no column has it: a column an output name shadows is
        ordered by the expression, which PostgreSQL reads as the column too (a view stores
        such a column qualified). A UNION's, or a SELECT DISTINCT's that selects x, can be
        ordered only by what it selects: there x itself, reported."""
        owner = ordered.parent.parent if isinstance(ordered.parent, exp.Order) else None
        named = operand.name.lower() if isinstance(operand, exp.Column) and not operand.table else None
        aliased = next((
            p.this for p in (owner.expressions if isinstance(owner, exp.Select) and named else ())
            if isinstance(p, exp.Alias) and p.alias.lower() == named
        ), None)
        shadowed = aliased is not None and kind is not None and not view and not (
            isinstance(aliased, exp.Column) and aliased.name.lower() == named  # s AS s: the column itself
        )
        if shadowed:
            aliased = None
        if isinstance(owner, exp.SetOperation):
            branches = _output_queries(owner)
            names = [p.alias_or_name.lower() for p in branches[0].expressions] if branches else []
            position = names.index(named) if named in names else None
            kinds = {
                kind_of(b.expressions[position].unalias()) if position < len(b.expressions) else None
                for b in branches
            } if position is not None else {None}
        else:
            kinds = {kind_of(aliased) if aliased is not None else kind}
        if kinds <= _BYTE_ORDERED_KINDS and not shadowed:
            node.replace(operand)
            return
        if isinstance(owner, exp.SetOperation) or (isinstance(owner, exp.Select) and owner.args.get("distinct")):
            issue(
                "ORDER BY BINARY x of a UNION, or of a SELECT DISTINCT that selects x, orders by the "
                "bytes of x's text in MySQL -- a number's digits, 12 before 3 -- and Aurora DSQL "
                "orders such a query only by what it selects: it is written ORDER BY x, in x's own "
                "order. For the text's order, select the key as text (CONVERT_TO(CAST(x AS TEXT), "
                "'UTF8') AS k) and order by k, or order an outer query: SELECT * FROM (...) d ORDER "
                "BY CONVERT_TO(CAST(d.x AS TEXT), 'UTF8')."
            )
            node.replace(operand)
            return
        if aliased is not None:
            aliased_keys.append((node, aliased))  # copied once this pass has written the casts in it
            return
        text = exp.Cast(this=operand, to=exp.DataType.build("TEXT")) if kind is None else as_text(operand)
        node.replace(pg_call("CONVERT_TO", text, exp.Literal.string("UTF8")))

    aliased_keys: list = []
    # Casts to text, with a length, from bytes; casts to bytes.
    for node in nodes(exp.Cast, innermost=True):
        to = node.args.get("to")
        if not isinstance(to, exp.DataType):
            continue
        charset = ""
        if to.this == exp.DataType.Type.CHARACTER_SET:
            kind_arg = to.args.get("kind") or ""
            charset = (kind_arg.name if isinstance(kind_arg, exp.Expression) else str(kind_arg)).lower()
        binary = charset == "binary" or to.this in (exp.DataType.Type.BINARY, exp.DataType.Type.VARBINARY)
        operand = node.this
        if binary:
            length = [e for e in to.expressions]
            if length:
                issue(
                    "CAST(x AS BINARY(n)) pads the bytes with 0x00 to n (or cuts them to n) in "
                    "MySQL; the conversion writes the bytes without that length: fix the "
                    "length if it matters."
                )
            held = node.parent if isinstance(node.parent, exp.Paren) else node
            parent = held.parent
            kind = kind_of(operand)
            if isinstance(parent, _COMPARISONS):
                if isinstance(parent, exp.In):
                    expressions = parent.expressions
                    other = expressions[0] if expressions and parent.this is held else parent.this
                else:
                    other = parent.expression if parent.this is held else parent.this
                other_kind = kind_of(other)
                if other_kind != "binary":
                    # MySQL's BINARY x compared: x's text, byte-wise -- which every comparison
                    # on Aurora DSQL already is (C collation): a text as itself, a number, a
                    # date or a boolean as MySQL's text of it ('010' is not 10); beside a
                    # number, as numbers, as MySQL compares a string with one. A value of a
                    # type that cannot be told is compared as itself -- right for text, a
                    # date, a boolean, beside a number, and its index serves the comparison
                    # (x || '' would refuse x = number and read a boolean as 'true'). A
                    # BINARY(n)'s length, noted, is lost either way.
                    if kind in (None, "text", "char", "binary") or other_kind in _NUMERIC_KINDS:
                        node.replace(operand)
                    else:
                        node.replace(as_text(operand))
                    continue
            grouped = isinstance(parent, exp.Group) or (
                isinstance(parent, exp.Window) and any(key is held for key in parent.args.get("partition_by") or ())
            ) or (isinstance(parent, exp.Distinct) and isinstance(parent.parent, exp.AggFunc))
            if grouped:
                # Grouped by its bytes, as Aurora DSQL groups the value itself (C collation): a
                # value groups as its text does -- a json, which has no equality there, by it.
                node.replace(operand if kind != "json" else pg_call(
                    "CONVERT_TO", as_text(operand), exp.Literal.string("UTF8")
                ))
                continue
            if isinstance(parent, exp.Ordered) and id(node) not in listed_binary:
                ordered_by_bytes(node, operand, kind, parent)
                continue
            if kind == "binary":
                node.replace(operand)
            elif kind is None and isinstance(operand, exp.Column):
                # A column of unknown type: no form gives MySQL's bytes for every type.
                issue(
                    "BINARY x / CAST(x AS BINARY) reads a column whose type cannot be told here "
                    "-- one of another view, of a derived table or CTE that computes it, or (in "
                    "the Query Converter) of a table Step 1 has not read: it is written "
                    "CAST(x AS BYTEA), right for a binary column. For a text column write "
                    "CONVERT_TO(x, 'UTF8'), for a number or a date CONVERT_TO(CAST(x AS TEXT), "
                    "'UTF8')."
                )
                node.replace(exp.Cast(this=operand, to=exp.DataType.build("BYTEA")))
            else:
                node.replace(pg_call("CONVERT_TO", as_text(operand), exp.Literal.string("UTF8")))
            continue
        if to.this in (exp.DataType.Type.CHARACTER_SET, exp.DataType.Type.CHAR):
            length = [e.copy() for e in to.expressions]
            target = exp.DataType.build("TEXT") if not length else exp.DataType(
                this=exp.DataType.Type.VARCHAR, expressions=length
            )
            kind = kind_of(operand)
            if kind == "binary":
                # The bytes, read in the cast's character set (MySQL reinterprets them):
                # bytea's own text is its hex escape.
                encoding = _PG_ENCODINGS.get(charset or "utf8mb4")
                if encoding is None:
                    issue(
                        f"CONVERT(... USING {charset}) decodes bytes in a character set Aurora "
                        "DSQL has no encoding for here: rewrite it."
                    )
                    encoding = "UTF8"
                issue(
                    "CONVERT(bytes USING charset) / CAST(bytes AS CHAR) returns NULL in MySQL "
                    "for bytes that are not valid in the charset; Aurora DSQL's CONVERT_FROM "
                    "raises an error instead (a NUL byte included), failing every query that "
                    "reads such a value."
                )
                node.set("this", pg_call("CONVERT_FROM", operand, exp.Literal.string(encoding)))
            elif kind == "boolean":
                node.set("this", exp.Cast(this=operand, to=exp.DataType.build("INT")))
            elif kind in _EXACT_KINDS:
                node.set("this", shown(operand))
            float_text(kind)
            node.set("to", target)
        elif to.this == exp.DataType.Type.UBIGINT:
            node.set("to", exp.DataType.build("NUMERIC(20)"))
            kind = kind_of(operand)
            if kind is None and operand.find(exp.Div) is not None:
                kind = "decimal"  # over a quotient of operands not typed: exact in MySQL, as like as not
            if kind in _NUMERIC_KINDS or kind in ("text", "char"):
                # MySQL rounds first -- a FLOAT / DOUBLE half to even (rint, as PostgreSQL's
                # round(double precision)), a decimal half away from zero; a string is its
                # leading integer ('1.5' is 1) -- and wraps a negative result modulo 2^64:
                # CAST(1 - 2 AS UNSIGNED) is 18446744073709551615, CAST(-0.4 AS UNSIGNED) 0,
                # CAST('-2' AS UNSIGNED) 18446744073709551614. In numeric, which holds 2^64
                # exactly.
                if kind == "integer":
                    rounded = operand
                elif kind in ("text", "char"):
                    rounded = leading_number(operand, "NUMERIC")
                elif kind in _FLOAT_KINDS:
                    rounded = exp.Cast(
                        this=exp.Round(this=exp.Cast(this=operand, to=exp.DataType.build("DOUBLE PRECISION"))),
                        to=exp.DataType.build("NUMERIC"),
                    )
                else:
                    rounded = exp.Round(this=exp.Cast(this=operand, to=exp.DataType.build("NUMERIC")))
                node.set("this", exp.Case(
                    ifs=[exp.If(
                        this=exp.LT(this=rounded.copy(), expression=exp.Literal.number(0)),
                        true=exp.Add(
                            this=rounded.copy(),
                            expression=exp.Literal.number("18446744073709551616"),
                        ),
                    )],
                    default=rounded,
                ))
    for key, aliased in aliased_keys:
        named = aliased.copy() if isinstance(aliased, (exp.Paren, exp.Column, exp.Literal)) else exp.Paren(
            this=aliased.copy()
        )
        key.replace(pg_call(
            "CONVERT_TO", exp.Cast(this=named, to=exp.DataType.build("TEXT")), exp.Literal.string("UTF8")
        ))

    def declared_type(value: exp.Expression) -> Optional[exp.DataType]:
        """The type a recursive CTE's first SELECT gives a column, when it has a modifier
        (VARCHAR(n), NUMERIC(p,s), NUMERIC(20), TIMESTAMP(n), TIME(n)): a cast's, or a
        column's as Schema Conversion writes its table's."""
        if isinstance(value, (exp.Cast, exp.TryCast)):
            to = value.args.get("to")
            return to if isinstance(to, exp.DataType) and to.expressions else None
        mysql = column_type(value) if isinstance(value, exp.Column) and column_type is not None else None
        # The schema converter's type map, imported here: the converter imports this module.
        from dsql_migrator.core.converter import map_mysql_type

        try:
            target = map_mysql_type(mysql)[0] if mysql else ""
        except ValueError:
            return None
        return exp.DataType.build(target, dialect=_POSTGRES) if "(" in target else None

    # MySQL types a recursive CTE's columns by its non-recursive SELECTs -- CAST(x AS
    # CHAR(n)) there is its way to make a column wide enough -- and converts each recursive
    # row's values to those types; PostgreSQL types a column by both terms and refuses a
    # VARCHAR(n), a NUMERIC(p,s), a TIMESTAMP(n) ... beside the recursive term's plain type:
    # there each recursive SELECT's value is cast to the first SELECT's type (a value too
    # long is cut to n, where strict MySQL refuses it). An integer beside a recursive
    # fraction, which MySQL rounds into the integer, is reported.
    for with_ in nodes(exp.With):
        if not with_.args.get("recursive"):
            continue
        for cte in with_.expressions:
            if not isinstance(cte.this, exp.SetOperation):
                continue
            branches = _output_queries(cte.this)
            first = next((
                i for i, branch in enumerate(branches)
                if any(_cte_named(table) is cte for table in branch.find_all(exp.Table))
            ), len(branches))
            anchors, recursive = branches[:first], branches[first:]
            if not anchors or not recursive:
                continue
            for position in range(min(len(branch.expressions) for branch in branches)):
                values = [branch.expressions[position].unalias() for branch in anchors]
                declared = [declared_type(value) for value in values]
                kinds = {kind_of(value) for value in values}
                if kinds == {"integer"} and any(
                    kind_of(branch.expressions[position].unalias()) in ("decimal", "float4", "float8")
                    for branch in recursive
                ):
                    issue(
                        "A recursive CTE's column is an integer in its first SELECT and a fraction "
                        "in its recursive one (n + 0.5): MySQL types the column by the first SELECT "
                        "and rounds each recursive value into it, and Aurora DSQL refuses the "
                        "mismatch -- the statement fails there. Cast the recursive value "
                        "(CAST(... AS SIGNED)) to keep MySQL's rows, or the first SELECT's to a "
                        "DECIMAL to keep the fractions."
                    )
                    continue
                if declared[0] is None or any(d is None or d != declared[0] for d in declared):
                    continue
                for branch in recursive:
                    projection = branch.expressions[position]
                    value = projection.unalias()
                    if isinstance(value, exp.Cast) and value.args.get("to") == declared[0]:
                        continue
                    if isinstance(projection, exp.Alias):
                        projection.set("this", exp.Cast(this=value, to=declared[0].copy()))
                    else:
                        outputs = list(branch.expressions)
                        outputs[position] = exp.Cast(this=projection, to=declared[0].copy())
                        branch.set("expressions", outputs)
    def concatenated(value: exp.Expression) -> exp.Expression:
        """A GROUP_CONCAT value as text: a TINYINT(1) or a comparison as MySQL's digit
        (as_text) -- Aurora DSQL's text of a boolean is 'true' -- else as written."""
        return as_text(value) if kind_of(value) == "boolean" else _texted(shown(value))

    for node in nodes(exp.GroupConcat):
        separator = node.args.get("separator") or exp.Literal.string(",")
        holder, key, value = node, "this", node.this
        ordered = isinstance(value, exp.Order)
        if ordered:
            holder, key, value = value, "this", value.this
        if isinstance(value, exp.Distinct) and ordered:
            # A DISTINCT aggregate's ORDER BY must be its argument: as an array, sorted
            # by the value itself and without NULLs, it is the same list for any type (a
            # CHAR(n) column without its padding, as MySQL reads it; a TINYINT(1) as its
            # integer, which orders as the boolean does).
            orders = [o.this for o in holder.expressions]
            if len(value.expressions) == 1 and len(orders) == 1 and orders[0] == value.expressions[0]:
                first = value.expressions[0]  # typed while still in the tree
                item = _as_integer(first.copy()) if kind_of(first) == "boolean" else unpadded(first).copy()
                ordered_by = holder.expressions[0].copy()
                ordered_by.set("this", item.copy())
                aggregated = exp.ArrayAgg(
                    this=exp.Order(this=exp.Distinct(expressions=[item.copy()]), expressions=[ordered_by])
                )
                node.replace(written(exp.func(
                    "ARRAY_TO_STRING",
                    exp.Filter(this=aggregated, expression=exp.Where(
                        this=exp.Not(this=exp.Is(this=item.copy(), expression=exp.Null()))
                    )),
                    separator.copy(),
                )))
            else:
                issue(
                    "GROUP_CONCAT(DISTINCT x ORDER BY y) orders the distinct values by "
                    "another expression, which a PostgreSQL DISTINCT aggregate cannot do (its "
                    "ORDER BY must be its argument): rewrite it -- for example STRING_AGG over "
                    "a DISTINCT subquery ordered by MIN(y)."
                )
        elif isinstance(value, exp.Distinct):
            value.set("expressions", [concatenated(e) for e in value.expressions])
        elif value is not None:
            holder.set(key, concatenated(value))
    # MySQL makes a number, a date ... text where it wants a string (textual): PostgreSQL
    # has no integer || integer, lpad(integer, ...), btrim(integer), md5(integer) or
    # integer LIKE / ~, and writes a boolean, a comparison, a DATETIME(6) or a TIMESTAMP as
    # text of its own. A column whose type cannot be told (another view's, or a Query
    # Converter statement's before Step 1) is as often a date or a number as text there:
    # inside a string function, where it gets no index use anyway, it is x || ''
    # (untyped_text: text for every type, bytes kept bytes -- LEFT(created_at, 7) is the
    # month); under LIKE / REGEXP CAST(x AS TEXT), which a text column's index still serves
    # for LIKE 'prefix%'; under UPPER / LOWER it stays as written. Bytes beside text in a
    # CONCAT are decoded: as text Aurora DSQL writes their hex.
    def untyped_text(node: exp.Expression) -> exp.Expression:
        """A bare column whose type cannot be told, padded or joined only with values like it:
        x || '', text for every type (bytes kept bytes) -- LPAD(id, 6, '0'), CONCAT(a, b)
        are as often over numbers as over text (a boolean's text, PostgreSQL's 'true', is
        what x || text gave already)."""
        return _any_text(node) if isinstance(node, exp.Column) and kind_of(node) is None else node

    def joined(parts: "list[exp.Expression]") -> "list[exp.Expression]":
        parts = [textual(part) for part in parts]
        if not any(kind_of(part) in ("text", "char") for part in parts):
            parts = [untyped_text(part) for part in parts]
        if any(kind_of(part) == "binary" for part in parts) and not all(kind_of(part) == "binary" for part in parts):
            issue(
                "CONCAT / CONCAT_WS joins bytes (a BLOB / BINARY value) with text: MySQL returns "
                "the bytes, Aurora DSQL decodes them here as UTF-8 (CONVERT_FROM) -- bytes that "
                "are not valid UTF-8 (a NUL byte included) raise an error there, failing every "
                "query that reads such a value."
            )
            parts = [
                pg_call("CONVERT_FROM", part, exp.Literal.string("UTF8")) if kind_of(part) == "binary" else part
                for part in parts
            ]
        return parts

    for node in nodes(exp.ConcatWs):
        node.set("coalesce", True)  # MySQL's CONCAT_WS skips a NULL argument, as PG's does
        node.set("expressions", joined(node.expressions))
    for node in nodes(exp.Concat):
        node.set("expressions", joined(node.expressions))
    for node in nodes(
        exp.Pad, exp.Left, exp.Right, exp.Substring, exp.Replace, exp.Upper, exp.Lower, exp.Like, exp.Trim,
        exp.Reverse, exp.Repeat, exp.MD5, exp.Ascii, exp.StrPosition, exp.RegexpLike,
    ):
        keys = {exp.Pad: ("this", "fill_pattern"), exp.Replace: ("this", "expression", "replacement"),
                exp.Like: ("this", "expression"), exp.Trim: ("this", "expression"),
                exp.StrPosition: ("this", "substr"), exp.RegexpLike: ("this", "expression")}.get(type(node), ("this",))
        for key in keys:
            if node.args.get(key) is not None:
                value = textual(node.args[key])
                if key == "this" and isinstance(node, (exp.Like, exp.RegexpLike)):
                    if isinstance(value, exp.Column) and kind_of(value) is None:
                        value = exp.Cast(this=value, to=exp.DataType.build("TEXT"))
                elif key == "this" and not isinstance(node, (exp.Upper, exp.Lower)):
                    value = untyped_text(value)
                node.set(key, value)
    # GREATEST / LEAST are NULL when an argument is NULL in MySQL; Aurora DSQL's skip a NULL
    # argument. One NULL test per nest, at its outermost call, over the nest's leaves --
    # through every node NULL exactly when an operand is (_null_strict: arithmetic, ABS,
    # ROUND, a cast, CONCAT ...), a division also when its divisor is 0 -- so the test grows
    # with the nest, never doubles; a literal that is not NULL never is, nor a COALESCE that
    # ends in one. A nest behind another node (IFNULL, IF, CASE, a function) is its own,
    # and the outer test copies it: past _NEST_COPY_LIMIT such levels, each doubling the
    # one inside it, the outer calls are left as Aurora DSQL's -- and reported.
    def operands(node: exp.Expression) -> "list[exp.Expression]":
        """The values a NULL-strict node (or a GREATEST / LEAST) reads."""
        if isinstance(node, (exp.Greatest, exp.Least, exp.Concat)):
            found = [node.this, *node.expressions]
        elif isinstance(node, (exp.Round, exp.Trunc)):
            found = [node.this, node.args.get("decimals")]
        elif isinstance(node, exp.Binary) and not isinstance(node, exp.Nullif):
            found = [node.this, node.expression]
        else:
            found = [node.this]  # NULLIF(x, literal): NULL when x is, or equals it (below)
        return [operand for operand in found if isinstance(operand, exp.Expression)]

    def null_when(node: exp.Expression) -> Optional[exp.Expression]:
        """What else makes a NULL-strict node NULL: a 0 divisor (x / 0 is NULL in MySQL, and
        the division is written x / NULLIF(y, 0)), NULLIF(x, v)'s x = v."""
        if isinstance(node, exp.Nullif):
            return exp.EQ(this=node.this.copy(), expression=node.expression.copy())
        if isinstance(node, (exp.Div, exp.Mod, exp.IntDiv)) and not isinstance(node.expression, exp.Nullif):
            divisor = node.expression.unnest()
            if not (isinstance(divisor, exp.Literal) and divisor.is_number and float(divisor.name) != 0):
                return exp.EQ(this=node.expression.copy(), expression=exp.Literal.number(0))
        return None

    for node in nodes(exp.Greatest, exp.Least, innermost=True):
        if node.args.get("ignore_nulls"):
            continue
        above = node.parent
        while above is not None and _null_strict(above):
            above = above.parent
        if isinstance(above, (exp.Greatest, exp.Least)) and not above.args.get("ignore_nulls"):
            continue  # its outermost call tests this one's leaves
        # The nodes between this call and the nests in it, each marked once.
        holders: set = set()
        for inner in node.find_all(exp.Greatest, exp.Least):
            while inner is not None and inner is not node and id(inner) not in holders:
                holders.add(id(inner))
                inner = inner.parent
        tests: list = []
        pending: list = [node]
        while pending:
            current = pending.pop()
            extra = null_when(current)
            if extra is not None:
                tests.append(extra)
            for argument in operands(current):
                nested = isinstance(argument, (exp.Greatest, exp.Least)) and not argument.args.get("ignore_nulls")
                if nested or (id(argument) in holders and _null_strict(argument)):
                    pending.append(argument)
                elif not _never_null(argument):
                    tests.append(argument.copy().is_(exp.Null()))
        if not tests:
            continue
        copies = 1 + max(
            (inner.meta.get(_NEST_COPIES, 0) for test in tests for inner in test.find_all(exp.Case)), default=0
        )
        if copies > _NEST_COPY_LIMIT:
            issue(
                "GREATEST() / LEAST() is nested more than three levels deep behind IFNULL, IF, "
                "CASE or another function: MySQL returns NULL when an argument is NULL, Aurora "
                "DSQL's GREATEST / LEAST skip it, and testing every level would double the "
                "expression per level -- the outer calls are left as Aurora DSQL's. Check their "
                "arguments for NULLs, or rewrite the nest."
            )
            continue
        any_null = tests[0]
        for test in tests[1:]:
            any_null = exp.or_(any_null, test)
        case = exp.Case(ifs=[exp.If(this=any_null, true=exp.Null())])
        case.meta[_NEST_COPIES] = copies
        node.replace(case)
        case.set("default", node)
    for node in nodes(exp.Round, exp.Trunc, exp.Floor, exp.Ceil):
        # MySQL's ROUND / TRUNCATE(x, d) on the number's own value, which PostgreSQL rounds
        # or truncates to d places only as a numeric (round(double precision, integer) does
        # not exist); an integer stays one -- PostgreSQL's round / floor of an integer is a
        # double precision.
        if node.meta.get(_SHOWN) or _QUOTIENT_SCALE in node.meta:
            continue
        decimals = node.args.get("decimals")
        value = node.this
        kind = kind_of(value)
        if decimals is None or isinstance(node, (exp.Floor, exp.Ceil)):
            if kind == "integer":
                node.replace(value)
            elif kind is None and isinstance(node, exp.Round) and value.find(exp.Div) is not None:
                # Over a quotient of operands not typed: exact in MySQL as like as not, so
                # ROUND(5 / 2) is 3 -- half away from zero, as numeric rounds.
                node.set("this", exp.Cast(this=value, to=exp.DataType.build("NUMERIC")))
            continue
        places = _integer_literal(decimals)
        if kind == "integer":
            if places is not None and places >= 0:
                node.replace(value)
            else:
                node.set("this", exp.Cast(this=value, to=exp.DataType.build("NUMERIC")))
                node.replace(exp.Cast(this=node.copy(), to=exp.DataType.build("BIGINT")))
            continue
        if kind == "decimal":
            # Asked for more places than the value has, MySQL keeps its own scale:
            # ROUND(1.5, 3) is 1.5, ROUND(10 / 3, 6) 3.3333.
            scale = decimal_scale(value)
            if places is not None and scale is not None and places > scale:
                node.set("decimals", exp.Literal.number(scale))
            continue
        if kind in _FLOAT_KINDS:
            # MySQL computes it in double precision -- rint(x * 10^d) / 10^d, half to even
            # on the binary product (ROUND(0.125e0, 2) is 0.12, ROUND(2.675e0, 2) 2.68),
            # TRUNCATE toward zero (TRUNCATE(0.29e0, 2) is 0.28) -- to a DOUBLE (2.5, never
            # 2.50): PostgreSQL's round / trunc of a double precision are the same
            # operations. In numeric it would round half away from zero (and a REAL keep 6
            # digits). With d negative: rint(x / 10^-d) * 10^-d.
            value = exp.Cast(this=value, to=exp.DataType.build("DOUBLE PRECISION"))
            whole = exp.Trunc if isinstance(node, exp.Trunc) else exp.Round
            if places is None:
                # Read at run time: the same computation, 10^d in double precision (d the
                # integer MySQL reads it as).
                exponent = decimals if kind_of(decimals) == "integer" else _as_integer(decimals)
                if not isinstance(exponent, (exp.Paren, exp.Column, exp.Literal, exp.Cast)):
                    exponent = exp.Paren(this=exponent)
                ten = exp.Cast(this=exp.Literal.number(10), to=exp.DataType.build("DOUBLE PRECISION"))
                up = exp.Pow(this=ten, expression=exponent.copy())
                down = exp.Pow(this=ten.copy(), expression=exp.Neg(this=exponent.copy()))
                scaled_up = whole(this=exp.Mul(this=value.copy(), expression=up.copy()))
                scaled_down = whole(this=exp.Div(this=value, expression=down.copy(), typed=True))
                node.replace(exp.Case(
                    ifs=[exp.If(
                        this=exp.GTE(this=exponent, expression=exp.Literal.number(0)),
                        true=exp.Div(this=scaled_up, expression=up, typed=True),
                    )],
                    default=exp.Mul(this=scaled_down, expression=down),
                ))
                continue
            power = exp.Literal.number(10 ** abs(places))
            if places == 0:
                node.replace(whole(this=value))
            elif places > 0:
                node.replace(exp.Paren(this=exp.Div(
                    this=whole(this=exp.Mul(this=value, expression=power)), expression=power.copy(), typed=True
                )))
            else:
                node.replace(exp.Paren(this=exp.Mul(
                    this=whole(this=exp.Div(this=value, expression=power, typed=True)), expression=power.copy()
                )))
            continue
        # Of a kind not known (a quotient of operands not typed too): in numeric.
        node.set("this", exp.Cast(this=value, to=exp.DataType.build("NUMERIC")))

    def mysql_amount(amount: exp.Literal, unit: str) -> exp.Literal:
        """A literal interval amount as MySQL counts it: a number to whole units -- half
        away from zero, a DOUBLE half to even (INTERVAL 1.5 DAY is 2 days) -- a string by
        its leading integer (INTERVAL '1.5' DAY and '3 days' DAY are 1 and 3 days, 'AB' 0);
        SECOND keeps its fraction ('1.5abc' SECOND is 1.5 seconds)."""
        if not unit:
            return amount
        text = amount.name.strip()
        number = amount.meta.get(_MYSQL_NUMBER)
        if number is None:
            pattern = r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?" if unit == "SECOND" else r"[+-]?\d+"
            leading = re.match(pattern, text)
            return exp.Literal.string(f"{decimal.Decimal(leading.group()):f}" if leading else "0")
        if unit == "SECOND":
            return amount
        value = decimal.Decimal(text)
        if value == value.to_integral_value():
            return amount
        whole = round(float(value)) if number == "double" else value.to_integral_value(rounding=decimal.ROUND_HALF_UP)
        return exp.Literal.string(str(int(whole)))

    for node in nodes(exp.Add, exp.Sub, exp.DateAdd, exp.DateSub, innermost=True):
        # Date arithmetic, innermost first: an outer one is replaced by a copy holding the
        # inner one, which is then already written.
        interval = interval_of(node)
        if interval is None:
            continue
        unit = interval_unit(node)
        holder, key = (node, "expression") if interval is node else (interval, "this")
        amount = holder.args.get(key)
        literal = amount
        while isinstance(literal, exp.Paren):
            literal = literal.this
        if isinstance(literal, exp.Neg) and isinstance(literal.this, exp.Paren) and literal.this.this.is_number:
            # MySQL stores INTERVAL -1 DAY as interval -(1) day, which sqlglot loses.
            number = literal.this.this
            literal = exp.Literal.string(f"-{number.name}")
            literal.meta[_MYSQL_NUMBER] = _number_kind(number)
        if isinstance(literal, exp.Literal) and unit in _COMPOSITE_UNITS:
            # INTERVAL '1 2' DAY_HOUR: its parts, as MySQL reads them (a number is read as
            # its text, 1.5 DAY_HOUR is 1 day 5 hours), written as Aurora DSQL's '1 day 2
            # hour' -- YEAR_MONTH as its months, which keeps a DATE plus it a DATE.
            parts = None if literal.meta.get(_MYSQL_NUMBER) == "double" else _mysql_interval_parts(literal.name, unit)
            if parts is None:
                issue(
                    f"INTERVAL '{literal.name}' {unit} is not read as MySQL reads it (more parts "
                    f"than {unit} has, which MySQL reads as NULL, or a number with an exponent): "
                    "rewrite the amount."
                )
                continue
            holder.set(key, exp.Literal.string(parts))
            (node if interval is node else interval).set("unit", exp.var("MONTH") if unit == "YEAR_MONTH" else None)
        elif isinstance(literal, exp.Literal):
            holder.set(key, mysql_amount(literal, unit))
        elif amount is not None:
            # A column or an expression: n * INTERVAL '1 DAY' (sqlglot wrote INTERVAL 'n
            # DAY', which PostgreSQL refuses), in whole units as MySQL counts all but SECOND:
            # a number rounded (half away from zero, a DOUBLE half to even), a string read
            # by its leading integer ('1.5' is 1 day, 'AB' 0) -- a SECOND's with its fraction.
            kind = kind_of(amount)
            if unit in _COMPOSITE_UNITS:
                issue(
                    f"INTERVAL ... {unit} is given an amount read at run time: MySQL reads the "
                    f"string's parts ('1 2' DAY_HOUR is 1 day 2 hours), which Aurora DSQL's "
                    "interval arithmetic cannot -- the statement fails there. Rewrite it, for "
                    "example as separate DAY and HOUR intervals."
                )
                continue
            if kind in ("text", "char"):
                amount = leading_number(amount, "NUMERIC" if unit == "SECOND" else "BIGINT", fraction=unit == "SECOND")
            elif unit not in ("SECOND", "") and kind != "integer":
                if kind is None and amount.find(exp.Div) is not None:
                    # Over a quotient of operands not typed: exact in MySQL, as like as not.
                    amount = exp.Cast(this=amount, to=exp.DataType.build("NUMERIC"))
                amount = exp.Round(this=amount)
            if interval is node:
                node.set("expression", amount)
            else:
                if not isinstance(amount, (exp.Column, exp.Paren, exp.Round)):
                    amount = exp.Paren(this=amount)
                interval.replace(exp.Mul(
                    this=amount,
                    expression=exp.Interval(this=exp.Literal.string("1"), unit=interval.args.get("unit")),
                ))
        base = node.this
        if isinstance(base, exp.Literal) and base.is_string:
            # A string MySQL reads as a DATE (or a DATETIME, with a time part), a two-digit
            # year as MySQL reads it.
            node.set("this", exp.Cast(
                this=_four_digit_year(base),
                to=exp.DataType.build("DATE" if _is_date_literal(base) else "TIMESTAMP"),
            ))
        # A DATE plus days, weeks, months or years is a DATE to MySQL, a timestamp to
        # PostgreSQL (``date + interval``) -- whatever computes the date.
        if kind_of(node) == "date" and not (
            isinstance(node.parent, exp.Cast) and _pg_type_kind(node.parent.args.get("to")) == "date"
        ):
            node.replace(exp.Cast(this=node.copy(), to=exp.DataType.build("DATE")))
    for node in nodes(exp.Length):
        # MySQL counts the value's text -- LENGTH its bytes, CHAR_LENGTH its characters: a
        # CHAR(n) column's without its padding, a number's digits, a json's text; a binary
        # value its own bytes.
        operand = node.this
        kind = kind_of(operand)
        characters = not node.args.get("binary")
        if kind in ("binary", "text") or (kind == "char" and characters):  # LENGTH(char) skips the padding
            counted = operand
        elif kind is None:
            # A value of unknown type: x || '' is the text of any type, bytes kept bytes.
            issue(_UNTYPED_TEXT_NOTE)
            counted = _any_text(operand)
        else:
            counted = as_text(operand)
        node.replace(pg_call("LENGTH" if characters else "OCTET_LENGTH", counted))
    for node in nodes(exp.ConvertTimezone):
        for part in ("source_tz", "target_tz"):
            # (Not ``zone``: that is the source's, which the passes below still read.)
            given = node.args.get(part)
            if given is None:
                continue
            if isinstance(given, exp.Literal) and given.is_string:
                if _UTC_OFFSET_RE.match(given.name):
                    node.set(part, exp.Interval(this=exp.Literal.string(given.name)))
                elif written_zone_name(given.name) != given.name:
                    # G192: a tzdata link ('US/Eastern') as the zone it links to, which a
                    # build without the link still has.
                    node.set(part, exp.Literal.string(written_zone_name(given.name)))
                continue
            # Read at run time: an offset turned into the POSIX zone PostgreSQL reads (its
            # sign the other way round), a zone name kept.
            node.set(part, exp.Case(
                ifs=[exp.If(
                    this=exp.RegexpLike(
                        this=given.copy(), expression=exp.Literal.string(r"^[+-][0-9]{1,2}:[0-9]{2}$")
                    ),
                    true=written(
                        exp.func("TRANSLATE", given.copy(), exp.Literal.string("+-"), exp.Literal.string("-+"))
                    ),
                )],
                default=given.copy(),
            ))
    for node in nodes(exp.UnixToTime):
        if zone is not None and node.args.get("format") is None:
            # Under a source zone (G157) the zone's wall clock of the instant, as the clock
            # beside it is (a DATETIME: compared with a TIMESTAMP as the instant again,
            # below), formatted or cast as MySQL formats and casts it there -- once: the
            # one FROM_UNIXTIME(x, fmt) became is at the zone already.
            if not isinstance(node.parent, exp.AtTimeZone):
                node.replace(_zone_clock(zone, node.copy()))
            continue
        if isinstance(node.parent, exp.Anonymous) and (node.parent.name or "").upper() == "TO_CHAR":
            continue
        # FROM_UNIXTIME(x): the session's wall clock, as a DATETIME.
        if node.args.get("format") is None and not isinstance(node.parent, exp.Cast):
            node.replace(exp.Cast(this=node.copy(), to=exp.DataType.build("TIMESTAMP")))
    # A format sqlglot made itself (MONTHNAME's).
    for node in nodes(exp.TimeToStr, exp.StrToTime, exp.StrToDate):
        fmt = node.args.get("format")
        if not (isinstance(fmt, exp.Literal) and fmt.is_string):
            continue
        pattern, unmapped = _pg_format(fmt.name)
        function = (
            "TO_CHAR" if isinstance(node, exp.TimeToStr)
            else "TO_DATE" if isinstance(node, exp.StrToDate) else "TO_TIMESTAMP"
        )
        operand = node.this
        if isinstance(operand, (exp.TsOrDsToDate, exp.TsOrDsToTimestamp)):
            operand.set("this", as_temporal(operand.this))
        node.replace(written(exp.func(function, operand, exp.Literal.string(pattern))))
        misformat("MONTHNAME" if isinstance(node, exp.TimeToStr) else "STR_TO_DATE", unmapped)

    def json_value(node: exp.Expression) -> exp.Expression:
        """A value as MySQL writes it in JSON: a DATETIME as 'YYYY-MM-DD HH:MM:SS.ffffff', a
        TIME with its microseconds, a TINYINT(1) (a boolean on Aurora DSQL) or a choice of
        comparisons as its number -- a comparison standing in the JSON itself as true /
        false, as both write it -- a CHAR(n) without its padding, each value of a COALESCE
        of exact numbers as itself (not at the widest scale it is shown at), through the
        COALESCE / IF / CASE it is chosen by -- but one its query groups by, which must be
        written as its GROUP BY key. A GREATEST / LEAST of a date and a date string is a
        STRING in MySQL: the text of the date it compares as -- and a COALESCE / CASE / IF
        kept a date beside dates of its sort each value's own text (one beside other
        strings is text already: chosen_as_mysql)."""
        kind = kind_of(node)
        float_text(kind)
        if kind in _TEMPORAL_KINDS and string_typed(node):
            chosen = null_checked(node)
            if isinstance(chosen, (exp.Greatest, exp.Least)):
                return as_text(node)
            each_choice(chosen, lambda value: as_text(value) if kind_of(value) in _TEMPORAL_KINDS else value)
            return node
        query = node.find_ancestor(exp.Select)
        group = query.args.get("group") if query is not None else None
        keys = [key.unnest() for key in group.expressions] if group is not None else []
        pending = [node]
        while pending:
            value = pending.pop().unnest()
            if not isinstance(value, (exp.Coalesce, exp.If, exp.Case)):
                continue
            if isinstance(value, exp.Coalesce) and not any(value == key for key in keys):
                each_choice(value, lambda chosen: chosen.this if chosen.meta.get(_WIDENED) else chosen)
            pending.extend(chosen for chosen in choices(value) if chosen is not None)
        if kind in ("datetime", "timestamp"):
            return pg_call("TO_CHAR", node, exp.Literal.string("YYYY-MM-DD HH24:MI:SS.US"))
        if kind == "time":
            return pg_call(
                "TO_CHAR",
                exp.Paren(this=exp.Add(
                    this=exp.Cast(this=exp.Literal.string("2000-01-01"), to=exp.DataType.build("DATE")),
                    expression=node,
                )),
                exp.Literal.string("HH24:MI:SS.US"),
            )
        if kind == "boolean":
            # MySQL writes a comparison or a condition standing in the JSON as true / false,
            # as Aurora DSQL writes a boolean; a TINYINT(1) -- a column, a derived table's
            # comparison column, an IF / CASE / COALESCE of comparisons -- as 1 / 0.
            if isinstance(node.unnest(), _PREDICATES):
                return node
            return exp.Cast(this=node, to=exp.DataType.build("INT"))
        return unpadded(node)

    for node in nodes(exp.Anonymous, innermost=True):
        name = (node.name or "").lower()
        if name == "json_array":
            node.replace(written(exp.func("JSON_BUILD_ARRAY", *[json_value(e) for e in node.expressions], copy=False)))
        elif name == "json_arrayagg" and len(node.expressions) == 1:
            node.replace(written(exp.func("JSON_AGG", json_value(node.expressions[0]), copy=False)))
    for node in nodes(exp.JSONKeyValue):
        value = node.args.get("expression")
        if value is not None:
            node.set("expression", json_value(value))

    # What the statement returns, as MySQL shows it (a quotient at its scale) -- a view's
    # only where MySQL materializes it (_mysql_materializes, or ALGORITHM=TEMPTABLE): a
    # view it merges into the query that reads it is computed there, with every digit a
    # quotient keeps, so it is left unrounded (its own SELECT * shows those digits). And
    # what a derived table or a CTE MySQL materializes holds: each output stored at its
    # scale.
    def show_outputs(query: exp.Select, show: "Callable[[int, exp.Expression], exp.Expression]") -> None:
        outputs = []
        for position, projection in enumerate(list(query.expressions)):
            if isinstance(projection, exp.Alias):
                projection.set("this", show(position, projection.this))
                outputs.append(projection)
                continue
            value = show(position, projection)
            if value is not projection and isinstance(projection, exp.Column) and isinstance(
                projection.this, exp.Identifier
            ):
                # Named as MySQL names it: x.q is q (ROUND(x.q, 4) is round).
                value = exp.Alias(this=value, alias=projection.this.copy())
            outputs.append(value)
        query.set("expressions", outputs)

    def at_scale(value: exp.Expression, scale: int) -> exp.Expression:
        if decimal_scale(value) == scale and not carries_quotient(value):
            return value
        rounded = exp.Round(this=value, decimals=exp.Literal.number(scale))
        rounded.meta[_SHOWN] = True
        return rounded

    def group_key(query: exp.Select, position: int) -> bool:
        """Whether the select list's item at ``position`` is a key of its query's GROUP BY --
        by its position, its alias (a name no FROM column has) or as the same expression:
        MySQL keeps it in the grouping's temporary table, at its column's scale, and Aurora
        DSQL matches the GROUP BY expression only with the whole item. WITH ROLLUP groups by
        sorting, each row's value shown as itself: there only the same expression is one,
        which Aurora DSQL must still match whole."""
        group = query.args.get("group")
        if group is None:
            return False
        rolled_up = bool(group.args.get("rollup"))
        projection = query.expressions[position]
        alias = projection.alias.lower() if isinstance(projection, exp.Alias) else ""
        value = projection.unalias().unnest()
        for key in group.expressions:
            if key.unnest() == value:
                return True
            if rolled_up:
                continue
            if isinstance(key, exp.Literal) and _integer_literal(key) == position + 1:
                return True
            if (
                isinstance(key, exp.Column) and not key.table and alias and key.name.lower() == alias
                and (column_type is None or column_type(key) is None)
            ):
                return True
        return False

    def union_doubles(values: "list[exp.Expression]") -> bool:
        """Whether MySQL types a UNION column with a FLOAT branch a DOUBLE -- Aurora DSQL types
        it real -- by its branches in order: beside a DOUBLE, a DECIMAL (a quotient, an exact
        SUM) or an INT (or BIT) column -- its MIN / MAX too -- wherever they stand; after any
        other integer (a literal, a BIGINT, COUNT(*), arithmetic: a BIGINT in MySQL) that comes
        before the first FLOAT. A TINYINT / SMALLINT / MEDIUMINT / YEAR column, and a BIGINT
        after the FLOAT, keep it a FLOAT."""
        float_seen = False
        for value in values:
            kind = kind_of(value)
            if kind == "float4":
                float_seen = True
                continue
            bare = value.unnest()
            while isinstance(bare, (exp.Window, exp.Min, exp.Max, exp.AnyValue)):
                bare = _aggregated(bare)  # MIN / MAX are their argument's type
            if kind in ("float8", "decimal") or isinstance(bare, exp.Sum):
                return True
            declared = column_type(bare) if column_type is not None and isinstance(bare, exp.Column) else None
            declared = (declared or "").strip().lower()
            if re.match(r"^(?:int|integer|bit)\b", declared) or (
                not float_seen and not re.match(r"^(?:tiny|small|medium)int\b|^year\b", declared)
            ):
                return True
        return False

    top = select.this if isinstance(select, exp.Subquery) else select
    units: "list[tuple[exp.Expression, bool]]" = []
    if not view or materialized or _mysql_materializes(top):
        # A statement's own rows are shown value by value -- through a temporary table
        # (DISTINCT, a UNION, a materialized view) every value is stored at its column's.
        units.append((top, view or isinstance(top, exp.SetOperation) or bool(top.args.get("distinct"))))
    for found in nodes(exp.CTE, exp.Subquery):
        body = found.this
        while isinstance(body, exp.Subquery):
            body = body.this
        derived = isinstance(found, exp.CTE) or found.args.get("alias") is not None
        if derived and isinstance(body, (exp.Select, exp.SetOperation)) and _mysql_materializes(body):
            units.append((body, True))
    for query, stored in units:
        branches = _output_queries(query)  # none for an INSERT, UPDATE or DELETE
        if len(branches) < 2:
            for branch in branches:
                # A CASE / IF the query groups by is stored whole, padded (0.0000).
                show_outputs(branch, lambda position, value: shown(value, stored=stored or (
                    isinstance(value.unnest(), (exp.If, exp.Case)) and group_key(branch, position)
                )))
            continue
        # A UNION column has one type, its branches' largest scale: each branch is stored at
        # it, an integer one too (SELECT 1 / 3 UNION SELECT 1 is 0.3333 and 1.0000) -- a NULL
        # branch has none. With a FLOAT / DOUBLE branch its quotients keep every digit -- a
        # REAL branch is cast to double precision where MySQL types the column a DOUBLE
        # (union_doubles; Aurora DSQL types it real); with a string one it is a string
        # (written by the typing of choices), its quotient left unrounded.
        targets: "dict[int, int]" = {}
        unrounded: set = set()
        doubled: set = set()
        for position in range(min(len(branch.expressions) for branch in branches)):
            values = [branch.expressions[position].unalias() for branch in branches]
            values = [value for value in values if not isinstance(value.unnest(), exp.Null)]
            kinds = {kind_of(value) for value in values}
            if kinds & (_FLOAT_KINDS | {"text", "char"}):
                unrounded.add(position)
                if "float4" in kinds and kinds <= _NUMERIC_KINDS and union_doubles(values):
                    doubled.add(position)
                continue
            found_scales = [decimal_scale(value) for value in values]
            if found_scales and None not in found_scales and any(carries_quotient(value) for value in values):
                targets[position] = max(found_scales)  # type: ignore[type-var]

        def union_value(position: int, value: exp.Expression) -> exp.Expression:
            if position in doubled and kind_of(value) == "float4":
                return exp.Cast(this=value, to=exp.DataType.build("DOUBLE PRECISION"))
            if position in unrounded or isinstance(value.unnest(), exp.Null):
                return value
            return at_scale(value, targets[position]) if position in targets else shown(value, stored=True)

        for branch in branches:
            show_outputs(branch, union_value)

    if zone is not None:
        # G149: the session's clock the passes above read -- the LOCALTIMESTAMP the clock
        # pass wrote, and CURRENT_DATE (CURDATE(), and the date MySQL puts a TIME on) -- as
        # the source zone's, last, so every pass above read the clock it always did.
        for node in list(select.find_all(exp.Localtimestamp)):
            node.replace(_zone_clock(zone))
        for node in list(select.find_all(exp.CurrentDate)):
            node.replace(exp.Cast(this=_zone_clock(zone), to=exp.DataType.build("DATE")))
        # G157: MySQL reads a TIMESTAMP in the session's zone wherever it reads it as a wall
        # clock -- its date, time of day, fields or text -- and compares a wall clock with it
        # there; PostgreSQL makes both conversions in the session's TimeZone (UTC on Aurora
        # DSQL unless the session sets another). Beside the source zone's clock above, a
        # TIMESTAMP compared with NOW() - INTERVAL 1 DAY (a timestamptz with a timestamp) was
        # an offset off, and DATE(ts) = CURDATE() compared a UTC date with the zone's. So a
        # TIMESTAMP read as a wall clock is read at the zone (and marked, as the clock is), and
        # a wall clock of the zone this rewrite wrote that meets a TIMESTAMP -- compared,
        # BETWEEN, IN, a COALESCE / GREATEST / LEAST / NULLIF / IF / CASE of both -- is the
        # instant it names there (the column side untouched, so its index still serves the
        # comparison); in a UNION column the TIMESTAMP is read at the zone instead (below).
        # What the view computes no longer depends on the session's zone. A TIMESTAMP compared
        # with a DATETIME column or a date string, which holds no clock, is compared in the
        # session's zone as before.

        def read_at_zone(value: Optional[exp.Expression]) -> None:
            if value is not None and value.parent is not None and kind_of(value) == "timestamp":
                value.replace(_zone_clock(zone, value.copy()))

        for node in list(select.find_all(exp.Cast, exp.TryCast)):
            if _pg_type_kind(node.args.get("to")) in ("date", "time", "datetime", "text"):
                read_at_zone(node.this)
        for node in list(select.find_all(exp.TsOrDsToDate, exp.TsOrDsToTimestamp, exp.TimeToStr, exp.LastDay)):
            read_at_zone(node.this)
        for node in list(select.find_all(exp.ConvertTimezone)):
            read_at_zone(node.args.get("timestamp"))  # CONVERT_TZ converts the wall clock
        for node in list(select.find_all(exp.Extract)):
            if node.this.name.upper() not in ("EPOCH", "TIMEZONE", "TIMEZONE_HOUR", "TIMEZONE_MINUTE"):
                read_at_zone(node.expression)
        for node in list(select.find_all(exp.Anonymous)):
            if not node.meta.get(_PG_WRITTEN):
                continue
            name = (node.name or "").upper()
            if name == "TO_CHAR" and node.expressions:
                read_at_zone(node.expressions[0])
            elif name == "DATE_TRUNC" and len(node.expressions) == 2:
                read_at_zone(node.expressions[1])

        def met(node: exp.Expression) -> "list[list[Optional[exp.Expression]]]":
            """The values ``node`` compares, or chooses between: each group one type."""
            if isinstance(node, exp.Between):
                return [[node.this, node.args.get("low"), node.args.get("high")]]
            if isinstance(node, exp.In):
                return [] if node.args.get("query") is not None else [[node.this, *node.expressions]]
            if isinstance(node, (exp.Coalesce, exp.Greatest, exp.Least)):
                return [[node.this, *node.expressions]]
            if isinstance(node, exp.Nullif):
                return [[node.this, node.expression]]
            if isinstance(node, exp.If):
                return [[node.args.get("true"), node.args.get("false")]]
            if isinstance(node, exp.Case):
                ifs = node.args.get("ifs") or ()
                results = [*(branch.args.get("true") for branch in ifs), node.args.get("default")]
                if node.this is not None:  # a simple CASE compares its operand with each WHEN
                    return [[node.this, *(branch.this for branch in ifs)], results]
                return [results]
            return [[node.this, node.expression]]

        def zone_wall_clock(value: exp.Expression) -> bool:
            """A DATETIME / DATE value made of a wall clock of the zone this rewrite wrote."""
            return kind_of(value) in ("datetime", "date") and any(n.meta.get(_ZONE_CLOCK) for n in value.walk())

        # A UNION column is a DATETIME to MySQL where a TIMESTAMP meets a DATETIME there (the
        # TIMESTAMP read as its wall clock), a timestamptz to PostgreSQL (the wall clock read
        # in the session's zone): beside the zone's wall clock the TIMESTAMP is read at the
        # zone, so the column is that wall clock, as in MySQL -- compared outside, too.
        for node in _innermost_first(select, exp.SetOperation):
            branches = _output_queries(node)
            for position in range(min((len(branch.expressions) for branch in branches), default=0)):
                values = [branch.expressions[position].unalias() for branch in branches]
                if any(zone_wall_clock(value) for value in values):
                    for value in values:
                        read_at_zone(value)
        for node in _innermost_first(
            select, exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ,
            exp.NullSafeNEQ, exp.Between, exp.In, exp.Coalesce, exp.Greatest, exp.Least,
            exp.Nullif, exp.If, exp.Case,
        ):
            for group in met(node):
                values = [value for value in group if value is not None]
                if not any(kind_of(value) == "timestamp" for value in values):
                    continue
                for value in values:
                    if zone_wall_clock(value):
                        value.replace(_zone_instant(zone, value.copy(), kind_of(value)))

    # A call sqlglot does not know stays an Anonymous function and is emitted as written:
    # a MySQL-only function (FIND_IN_SET, UNIX_TIMESTAMP) or a user one -- unless
    # PostgreSQL has it by that name, or this rewrite wrote it (by the node: a user
    # function named date_trunc is still the source's). A stored function, written with
    # its schema, is always the source's.
    for node in select.find_all(exp.Anonymous):
        name = node.name or ""
        if not isinstance(node.parent, exp.Dot):
            if node.meta.get(_PG_WRITTEN):
                continue
            arity = _PG_BUILTIN_ANONYMOUS.get(name.lower(), -1)
            if arity is None or arity == len(node.expressions):
                continue
        if name:
            note(name)
    for node in nodes(*_UNTRANSLATED_MYSQL_NODES):
        note(_UNTRANSLATED_MYSQL_NODES[type(node)])
    for node in nodes(*_PG_ONLY_NODES):
        if not node.meta.get(_PG_WRITTEN):
            note(_PG_ONLY_NODES[type(node)])
    if too_deep[0]:
        issue(
            f"An expression is nested more than {_MAX_TYPED_DEPTH} levels deep, past what the "
            "conversion types: what depends on its type -- MySQL's exact division and the scale "
            "it shows, ROUND / TRUNCATE, a TINYINT(1) read as a number -- is not written as MySQL "
            "computes it there. Split or simplify the expression."
        )
    return untranslated, misformatted, issues


def _output_queries(tree: exp.Expression) -> "list[exp.Select]":
    """The SELECTs whose rows a statement returns: itself, or each branch of a UNION (a
    chain of thousands, walked without recursion)."""
    found: list = []
    pending = [tree]
    while pending:
        node = pending.pop()
        if isinstance(node, exp.Subquery):
            pending.append(node.this)
        elif isinstance(node, exp.Select):
            found.append(node)
        elif isinstance(node, exp.SetOperation):
            pending += [node.expression, node.this]
    return found


def unquote_json_extracts(tree: exp.Expression) -> None:
    """MySQL's ``->>`` -- which MySQL stores in a view as json_unquote(json_extract(...)) --
    as PostgreSQL's scalar extraction (:class:`exp.JSONExtractScalar`, written out by
    :func:`_rewrite_mysql_calls`): JSON_UNQUOTE(JSON_EXTRACT(col, '$.k')) means the value as
    unquoted text. Only over one path Aurora DSQL can follow as MySQL does
    (:func:`_json_path_steps`): with more, JSON_EXTRACT returns an array of them, and the
    JSON_UNQUOTE is left (and reported) -- as is a ``->>`` a statement writes over any other
    path, written back as JSON_UNQUOTE(JSON_EXTRACT(...))."""
    for node in list(tree.find_all(exp.Anonymous, exp.JSONExtractScalar)):
        if isinstance(node, exp.JSONExtractScalar):
            if not _json_path_steps(node.expression):
                node.replace(exp.Anonymous(
                    this="JSON_UNQUOTE",
                    expressions=[exp.JSONExtract(this=node.this, expression=_json_path_text(node.expression))],
                ))
            continue
        if (node.name or "").upper() != "JSON_UNQUOTE" or len(node.expressions) != 1:
            continue
        inner = node.expressions[0]
        if isinstance(inner, exp.JSONExtract) and not inner.expressions and _json_path_steps(inner.expression):
            node.replace(exp.JSONExtractScalar(this=inner.this, expression=inner.expression))


# How many [0] steps a followed JSON path may hold: each tests the value's type, holding the
# value three times, so the expression grows 3^n with them.
_MAX_ZERO_STEPS = 3


def _json_path_steps(path: Optional[exp.Expression]) -> "Optional[list]":
    """The steps of a literal MySQL JSON path of member names and array indexes -- '$.a',
    '$.a[0]', '$."k y"' -- as names (str) and indexes (int), else None: the whole document
    ('$'), a wildcard, last, a range, a negative index, a path read at run time, or more
    than three [0] steps (each written with a test of the value's type, which holds the
    value three times: three copies per [0])."""
    if not isinstance(path, exp.JSONPath):
        return None
    parts = path.expressions
    if len(parts) < 2 or not isinstance(parts[0], exp.JSONPathRoot):
        return None
    steps: list = []
    for part in parts[1:]:
        step = part.this
        if isinstance(part, exp.JSONPathKey) and isinstance(step, str):
            steps.append(step)
        elif isinstance(part, exp.JSONPathSubscript) and type(step) is int and step >= 0:
            steps.append(step)
        else:
            return None
    return steps if sum(1 for step in steps if step == 0 and type(step) is int) <= _MAX_ZERO_STEPS else None


def _json_path_text(path: exp.Expression) -> exp.Expression:
    """A JSON path as MySQL's own text (a literal, which any engine can render), when sqlglot
    parsed it; any other path as it is."""
    if isinstance(path, exp.JSONPath):
        return exp.Literal.string(path.sql(dialect=_MySqlViewDialect)[1:-1].replace("''", "'"))
    return path


def _json_path(steps: "Sequence[Any]") -> exp.JSONPath:
    """The :class:`exp.JSONPath` of member names and array indexes."""
    return exp.JSONPath(expressions=[exp.JSONPathRoot()] + [
        exp.JSONPathKey(this=step) if isinstance(step, str) else exp.JSONPathSubscript(this=step)
        for step in steps
    ])


def _zone_clock(zone: SourceZone, instant: Optional[exp.Expression] = None) -> exp.Expression:
    """The source zone's wall clock as a node, :meth:`SourceZone.clock_sql`'s expression: an
    instant (the current one, CURRENT_TIMESTAMP, by default) at the zone's name -- or, for a
    fixed offset, its UTC wall clock plus the offset (a zone NAME '+09:00' reads with the
    POSIX sign, UTC-9). Parenthesized, so it stands where a LOCALTIMESTAMP stood whatever
    binds around it, and marked (:data:`_ZONE_CLOCK`)."""
    instant = instant if instant is not None else exp.CurrentTimestamp()
    if zone.kind == "offset":
        clock = exp.Paren(this=exp.Add(
            this=exp.Paren(this=exp.AtTimeZone(this=instant, zone=exp.Literal.string("UTC"))),
            expression=exp.Interval(this=exp.Literal.string(zone.zone)),
        ))
    else:
        clock = exp.Paren(this=exp.AtTimeZone(this=instant, zone=exp.Literal.string(zone.zone)))
    clock.meta[_ZONE_CLOCK] = True
    return clock


def _zone_instant(zone: SourceZone, wall: exp.Expression, kind: str) -> exp.Expression:
    """The instant a wall clock of the source zone names (G157), the inverse of
    :func:`_zone_clock`: ``(wall) AT TIME ZONE 'zone'``, or for a fixed offset ``((wall) -
    INTERVAL 'offset') AT TIME ZONE 'UTC'`` -- a date (``kind`` 'date') at its midnight.
    PostgreSQL makes that conversion in the session's TimeZone where a timestamp meets a
    timestamptz; this makes it in the zone MySQL compared them in, whatever the session's."""
    if kind == "date":
        wall = exp.Cast(this=wall, to=exp.DataType.build("TIMESTAMP"))
    held = wall if isinstance(wall, exp.Paren) else exp.Paren(this=wall)
    if zone.kind == "offset":
        held = exp.Paren(this=exp.Sub(
            this=held, expression=exp.Interval(this=exp.Literal.string(zone.zone))
        ))
        return exp.Paren(this=exp.AtTimeZone(this=held, zone=exp.Literal.string("UTC")))
    return exp.Paren(this=exp.AtTimeZone(this=held, zone=exp.Literal.string(zone.zone)))


def _utc_clock_kind(node: exp.Expression) -> Optional[str]:
    """'date' / 'time' / 'timestamp' for MySQL's UTC_DATE / UTC_TIME / UTC_TIMESTAMP -- a
    call, or the bare word a statement may write (sqlglot reads that as a column)."""
    for name, kind in (("UtcDate", "date"), ("UtcTime", "time"), ("UtcTimestamp", "timestamp")):
        cls = getattr(exp, name, None)
        if cls is not None and isinstance(node, cls):
            return kind
    if (
        isinstance(node, exp.Column)
        and not node.table
        and isinstance(node.this, exp.Identifier)
        and not node.this.quoted
    ):
        return {"utc_date": "date", "utc_time": "time", "utc_timestamp": "timestamp"}.get(node.name.lower())
    return None


def _is_date_literal(node: exp.Expression) -> bool:
    """A string literal MySQL reads as a DATE (no time part): with separators
    ('2024-01-31', '24/1/31'), or MySQL's compact 'YYYYMMDD' (which PostgreSQL reads as
    that date too)."""
    return isinstance(node, exp.Literal) and node.is_string and bool(
        re.fullmatch(
            r"\s*(?:\d{2,4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{4}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01]))\s*", node.name
        )
    )


def _is_datetime_literal(node: exp.Expression) -> bool:
    """A string literal MySQL reads as a DATE or a DATETIME."""
    return _is_date_literal(node) or isinstance(node, exp.Literal) and node.is_string and bool(
        re.fullmatch(
            r"\s*\d{2,4}[-/.]\d{1,2}[-/.]\d{1,2}(?:[ T]\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d{1,6})?)?)?\s*",
            node.name,
        )
    )


def _is_time_literal(node: exp.Expression) -> bool:
    """A string literal MySQL reads as a TIME of the day: 'hh:mm[:ss[.ffffff]]'."""
    return isinstance(node, exp.Literal) and node.is_string and bool(
        re.fullmatch(r"\s*\d{1,2}:\d{1,2}(?::\d{1,2}(?:\.\d{1,6})?)?\s*", node.name)
    )


def _four_digit_year(node: exp.Literal) -> exp.Literal:
    """A date string with a two-digit year written with four, as MySQL reads it (00-69 are
    2000-2069, 70-99 1970-1999): PostgreSQL refuses '24-01-31'."""
    match = re.fullmatch(r"(\s*)(\d{2})([-/.].*)", node.name, flags=re.DOTALL)
    if match is None:
        return node
    year = int(match.group(2))
    return exp.Literal.string(f"{match.group(1)}{year + (2000 if year < 70 else 1900)}{match.group(3)}")


def rewrite_mysql_expressions(
    tree: exp.Expression,
    column_types: "Sequence[tuple[str, Sequence[tuple[str, str]]]]" = (),
) -> "tuple[list[str], list[str], list[str]]":
    """:func:`_rewrite_mysql_calls` over a statement the Query Converter read (with
    :data:`MYSQL_EXPRESSION_DIALECT`): the same MySQL semantics a view gets, with
    ``column_types`` (:func:`table_type_index` entries of the Evaluation inventory) typing
    its columns -- a table it writes without a schema by its name alone."""
    return _rewrite_mysql_calls(tree, _column_type_resolver(column_types, bare_names=True))


MYSQL_EXPRESSION_DIALECT = _MySqlViewDialect


def _mysql_select_options(select: exp.Expression) -> "list[str]":
    """MySQL's SELECT options a view stores (select straight_join high_priority ..., FROM
    `t` PARTITION (`p0`)), which Aurora DSQL has no syntax for: the modifiers left out, in
    place -- they steer MySQL's optimizer and result buffering, not the rows -- and what is
    not the same reported: SQL_CALC_FOUND_ROWS left out, a PARTITION selection left as
    written. A view only: a Query Converter statement loses its modifiers before it is
    parsed, and what sqlglot reads as one there is a backquoted column (SELECT
    `high_priority` FROM t), which must stay."""
    notes: list[str] = []
    found_rows = False
    for node in select.find_all(exp.Select):
        modifiers = node.args.get("operation_modifiers") or []
        found_rows = found_rows or any(m.name.upper() == "SQL_CALC_FOUND_ROWS" for m in modifiers)
        node.set("operation_modifiers", None)
    if found_rows:
        notes.append(
            "SQL_CALC_FOUND_ROWS is MySQL-only and left out: the view returns the same rows. The "
            "FOUND_ROWS() an application reads after it -- how many rows there would be without "
            "the LIMIT -- has no Aurora DSQL form: count them with a separate SELECT COUNT(*) "
            "instead."
        )
    named = ", ".join(dict.fromkeys(
        f"{t.name} {t.args['partition'].sql(dialect=_MySqlViewDialect)}"
        for t in select.find_all(exp.Table)
        if t.args.get("partition")
    ))
    if named:
        notes.append(
            f"{named} is MySQL-only and left as written, so the CREATE VIEW fails on Aurora "
            "DSQL, which has no partitions. MySQL reads only the rows of the partitions named, "
            "so leaving it out takes in the whole table: put the partitions' bounds -- what the "
            "table is partitioned by -- in the view's WHERE instead."
        )
    return notes


def view_column_types(
    definition: str, type_index: "Mapping[str, tuple]", *, schema: Optional[str] = None
) -> "tuple":
    """The column types of the tables a MySQL view body names (MySQL writes each as
    `db`.`table`, and SHOW CREATE VIEW one of the session's database -- the view's own
    ``schema`` in single-database mode -- as `table`): the :func:`table_type_index` entries
    themselves, shared by every view that reads the table -- the key a conversion of the
    view is cached by, beside its body."""
    found: dict[str, tuple] = {}
    for db, name in re.findall(r"`((?:[^`]|``)+)`\s*\.\s*`((?:[^`]|``)+)`", definition or ""):
        entry = type_index.get(f"{db.replace('``', '`')}.{name.replace('``', '`')}")
        if entry is not None:
            found[entry[0]] = entry
    if schema is not None:
        for name in re.findall(r"`((?:[^`]|``)+)`", definition or ""):
            entry = type_index.get(f"{schema}.{name.replace('``', '`')}")
            if entry is not None:
                found[entry[0]] = entry
    return tuple(found[key] for key in sorted(found))


def _converted_mysql_view(
    name: str,
    definition: str,
    column_types: "tuple" = (),
    zone: "Optional[SourceZone]" = None,
) -> "Optional[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...]]]":
    """``(target CREATE VIEW, calls left untranslated, date formats left with a MySQL
    token, what else is not faithful)`` for a MySQL view, or ``None`` when its body does
    not parse or render. Cached (:func:`_mysql_view_conversion`) by name, body, the column
    types of what it reads and the source's time zone its clock is written in (``None``: the
    session's; G149): Schema Conversion and Evaluation (VIEW_NOT_CONVERTED) convert the same
    views."""
    converted = _mysql_view_conversion(name, definition, column_types, zone)
    return None if converted is None else converted[:4]


@functools.lru_cache(maxsize=8192)
def _mysql_view_conversion(
    name: str,
    definition: str,
    column_types: "tuple" = (),
    zone: "Optional[SourceZone]" = None,
) -> "Optional[tuple[str, tuple[str, ...], tuple[str, ...], tuple[str, ...], Optional[tuple]]]":
    """:func:`_converted_mysql_view`, and the view's columns' types: the
    :meth:`_ColumnTypes.outputs` a view over this one is typed by (:func:`mysql_type_index`,
    G188), None when they cannot be told."""
    try:
        parsed = sqlglot.parse_one(definition, read=_MySqlViewDialect)
        select = parsed.expression if isinstance(parsed, exp.Create) else parsed
        if select is None:
            raise ValueError("no SELECT body parsed from the view definition")
        # SHOW CREATE VIEW's text (what the introspector reads) keeps the view's ALGORITHM:
        # MySQL materializes an ALGORITHM=TEMPTABLE view whatever its body.
        properties = parsed.args.get("properties") if isinstance(parsed, exp.Create) else None
        temptable = any(
            isinstance(prop, exp.AlgorithmProperty) and prop.name.upper() == "TEMPTABLE"
            for prop in (properties.expressions if properties is not None else ())
        )
        # SHOW CREATE VIEW writes a table of the session's database bare (`from `t``) -- in
        # single-database mode the view's own, where MySQL resolves it: qualified with it, the
        # body is typed by the inventory and reads that schema's table on Aurora DSQL (bare,
        # the search path's). A CTE's name is no table.
        view_schema, view_name = _split_qualified(name)
        if view_schema:
            for table in list(select.find_all(exp.Table)):
                if table.args.get("db") is None and table.name and _cte_named(table) is None:
                    table.set("db", exp.to_identifier(view_schema, quoted=True))
        resolver = _column_type_resolver(column_types)
        listed = (
            [column.name for column in parsed.this.expressions]
            if isinstance(parsed, exp.Create) and isinstance(parsed.this, exp.Schema)
            else []
        )
        outputs: list = []

        def read_outputs() -> None:
            try:
                typed = resolver.outputs(select, listed)
                # A view MySQL materializes stores each output at its type's scale: what it
                # holds is no longer a quotient where another view reads it.
                if typed is not None and (temptable or _mysql_materializes(select)):
                    typed = tuple((column, str(kind)) for column, kind in typed)
                outputs.append(typed)
            except Exception:  # noqa: BLE001 - the view is then read untyped, not unconverted
                outputs.append(None)

        untranslated, misformatted, issues = _rewrite_mysql_calls(
            select,
            resolver,
            view=True,
            materialized=temptable,
            zone=zone,
            on_typed=read_outputs,
        )
        issues = [*issues, *_mysql_select_options(select)]
        # MySQL stores a HAVING name by its alias (having (`total` > 1000)), which Aurora
        # DSQL never resolves there.
        issues.extend(inline_having_aliases(select))
        # The name is QUOTED, like every table and index name the converter emits:
        # unquoted, Aurora DSQL folded it (``V_Orders`` became ``v_orders``, and a
        # view in schema ``MyApp`` landed in a different schema, ``myapp``), so it
        # could collide with a relation whose name differs only in case.
        create = exp.Create(
            this=exp.Table(
                this=exp.to_identifier(view_name, quoted=True),
                db=(exp.to_identifier(view_schema, quoted=True) if view_schema else None),
            ),
            kind="VIEW",
            expression=select,
        )
        return (
            create.sql(dialect=_POSTGRES, pretty=True),
            tuple(untranslated),
            tuple(misformatted),
            tuple(issues),
            outputs[0] if outputs else None,
        )
    except Exception:  # noqa: BLE001 - any parse/generation failure -> manual
        return None
