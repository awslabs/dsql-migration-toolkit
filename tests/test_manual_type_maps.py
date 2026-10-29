"""The manual's type-mapping tables must equal what the code does.

Parses every markdown table that follows a ``<!-- type-map: mysql -->`` or
``<!-- type-map: postgres -->`` marker in chapter 2 §2.3 (en; ko and ja join when they carry
the same tables), then, for every row, builds a table with an integer key plus one column of
each spelling the row names, and runs the REAL SchemaConverter, CompatibilityAssessor and
(PostgreSQL) CDC classifier on it. No database, no AWS. A converter or assessor change that
alters a mapping fails here until the table says the same.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pytest
import sqlglot
from sqlglot import exp

from dsql_migrator.core import assessor as assessor_mod
from dsql_migrator.core import assessor_postgres as assessor_pg_mod
from dsql_migrator.core import converter as converter_mod
from dsql_migrator.core import converter_postgres as converter_pg_mod
from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.prerequisites_postgres import uncarryable_columns_of

_REPO = Path(__file__).resolve().parents[1]
# The same tables in every language: code spans, classifications, rule ids and "—" are
# language-neutral; only the en CDC phrases are compared word for word.
DOCS = [_REPO / "docs" / "manual" / lang / "02-evaluation-and-schema-conversion.md"
        for lang in ("en", "ko", "ja")]

_MARKER_RE = re.compile(r"^\s*<!--\s*type-map:\s*(mysql|postgres)\s*-->\s*$")
_CODE_RE = re.compile(r"`([^`]+)`")
_RULE_TOKEN_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$")
_CLASSES = ("AUTO", "MANUAL", "UNSUPPORTED")
NOT_CONVERTED = "—"

# en phrase -> prerequisites_postgres reason code (None = carried like Full Load).
CDC_PHRASES = {
    NOT_CONVERTED: None,
    "Arrives NULL": "column_arrives_null",
    "Dead-lettered": "row_dead_letters",
    "Arrives altered": "column_arrives_wrong",
}

# Sample values for the placeholders a source or target type may carry.
_PLACEHOLDERS = {
    "(p,s)": ("(12,2)", {"p": "12", "s": "2"}),
    "(M,D)": ("(10,2)", {"M": "10", "D": "2"}),
    "(fsp)": ("(3)", {"fsp": "3"}),
    "(p)": ("(3)", {"p": "3"}),
    "(n)": ("(10)", {"n": "10"}),
}
_PLACEHOLDER_REGEX = {
    "(p,s)": r"\((?P<p>\d+),\s*(?P<s>-?\d+)\)",
    "(M,D)": r"\((?P<M>\d+),\s*(?P<D>\d+)\)",
    "(fsp)": r"\((?P<fsp>\d+)\)",
    "(p)": r"\((?P<p>\d+)\)",
    "(n)": r"\((?P<n>\d+)\)",
}


@dataclass(frozen=True)
class Probe:
    spelling: str
    kwargs: tuple = ()  # extra ColumnDef fields, as (name, value) pairs

    def column(self) -> ColumnDef:
        return ColumnDef(name="c", mysql_type=self.spelling, **dict(self.kwargs))


def _kw(**kwargs) -> tuple:
    return tuple(sorted(kwargs.items()))


# What a row cannot say in its cells, keyed by the row's code spans (source cell), joined by
# " | ". "extra" adds probes; "replace" probes only these (prose-only rows, and rows whose
# placeholder has a condition).
ROW_PROBES: "dict[tuple[str, str], dict[str, list[Probe]]]" = {
    # --- MySQL: the 5.7 spellings (display widths), the 8.0/8.4 geomcollection, BIT bounds.
    ("mysql", "TINYINT | TINYINT UNSIGNED"): {"extra": [
        Probe("tinyint(4)"), Probe("tinyint(3) unsigned"), Probe("tinyint(1) unsigned")]},
    ("mysql", "SMALLINT"): {"extra": [Probe("smallint(6)")]},
    ("mysql", "SMALLINT UNSIGNED | MEDIUMINT | MEDIUMINT UNSIGNED | INT | INTEGER"): {"extra": [
        Probe("smallint(5) unsigned"), Probe("mediumint(9)"), Probe("mediumint(8) unsigned"),
        Probe("int(11)")]},
    ("mysql", "INT UNSIGNED | BIGINT"): {"extra": [
        Probe("int(10) unsigned"), Probe("int(10) unsigned zerofill"), Probe("bigint(20)")]},
    ("mysql", "BIGINT UNSIGNED"): {"extra": [Probe("bigint(20) unsigned")]},
    ("mysql", "BIT(n)"): {"extra": [Probe(f"bit({n})") for n in (1, 8, 15, 16, 31, 32, 63, 64)]},
    ("mysql", "DECIMAL(p,s) | DECIMAL(p,s) UNSIGNED | NUMERIC | DEC | FIXED"): {"extra": [
        Probe("decimal(10,0)"), Probe("decimal(65,30)"), Probe("decimal(65,0) unsigned")]},
    ("mysql", "FLOAT | FLOAT(M,D) | FLOAT UNSIGNED"): {"extra": [Probe("float(10,2) unsigned")]},
    ("mysql", "DOUBLE | DOUBLE(M,D) | DOUBLE UNSIGNED | REAL | DOUBLE PRECISION"): {"extra": [
        Probe("double(10,2) unsigned")]},
    ("mysql", "DATETIME | DATETIME(fsp)"): {"extra": [Probe("datetime(6)")]},
    ("mysql", "TIMESTAMP | TIMESTAMP(fsp)"): {"extra": [Probe("timestamp(6)")]},
    ("mysql", "TIME | TIME(fsp)"): {"extra": [Probe("time(6)")]},
    ("mysql", "YEAR"): {"extra": [Probe("year(4)")]},
    ("mysql", "CHAR(n)"): {"extra": [Probe("char(1)"), Probe("char(255)")]},
    ("mysql", "VARCHAR(n)"): {"extra": [Probe("varchar(16383)")]},
    ("mysql", "GEOMETRY | POINT | LINESTRING | POLYGON | MULTIPOINT | MULTILINESTRING | "
              "MULTIPOLYGON | GEOMETRYCOLLECTION"): {"extra": [Probe("geomcollection")]},
    # --- PostgreSQL: format_type variants, user-defined kinds, conditional placeholders.
    ("postgres", "numeric(p,s) | decimal(p,s)"): {"extra": [
        Probe("numeric(10,0)"), Probe("numeric(1000,1000)")]},
    ("postgres", "date | time without time zone | timestamp without time zone | "
                 "timestamp with time zone | time | timestamp | timestamptz"): {"extra": [
        Probe("time(3) without time zone"), Probe("timestamp(3) without time zone"),
        Probe("timestamp(0) with time zone")]},
    ("postgres", "time with time zone | timetz"): {"extra": [Probe("time(3) with time zone")]},
    ("postgres", "interval | interval day to second | interval(3)"): {"extra": [
        Probe("interval(3)"), Probe("interval day to second"), Probe("interval second(3)"),
        Probe("interval year to month")]},
    ("postgres", "character varying(n) | character(n)"): {"replace": [
        Probe("character varying(70000)"), Probe("character(5000)")]},
    ("postgres", "CREATE TYPE … AS ENUM"): {"replace": [
        Probe("ch02.mood", _kw(type_kind="enum", enum_labels=("sad", "ok", "happy")))]},
    ("postgres", "CREATE TYPE … AS (…)"): {"replace": [
        Probe("ch02.pair", _kw(type_kind="composite"))]},
    ("postgres", "CREATE DOMAIN"): {"replace": [
        Probe("ch02.email", _kw(type_kind="domain", base_type="text"))]},
    ("postgres", "CREATE TYPE … AS RANGE"): {"replace": [
        Probe("ch02.floatrange", _kw(type_kind="range"))]},
    ("postgres", "integer[] | text[]"): {"extra": [
        Probe("character varying(20)[]"), Probe("numeric(10,2)[]"),
        Probe("timestamp with time zone[]"), Probe("ch02.mood[]")]},
    ("postgres", "citext | hstore | ltree | geometry | geography"): {"extra": [
        Probe("geometry(Point,4326)")]},
}
# Built-in range / multirange types carry a typtype the enrich step records as their kind.
_PG_KIND_BY_NAME = {
    **{name: "range" for name in ("int4range", "int8range", "numrange", "tsrange",
                                  "tstzrange", "daterange")},
    **{name: "multirange" for name in ("int4multirange", "int8multirange", "nummultirange",
                                       "tsmultirange", "tstzmultirange", "datemultirange")},
}


@dataclass
class Row:
    engine: str
    doc: Path
    line: int
    source: str
    target: str
    classification: str
    note: str
    cdc: Optional[str] = None
    tokens: list = field(default_factory=list)  # (code span, is_alias)

    @property
    def key(self) -> str:
        return " | ".join(t for t, _alias in self.tokens)

    @property
    def label(self) -> str:
        return f"{self.doc.parent.name}/{self.doc.name}:{self.line} {self.engine} [{self.key}]"


def _split_cells(line: str) -> list[str]:
    cells, buf, in_code = [], [], False
    for ch in line.strip().strip("|"):
        if ch == "`":
            in_code = not in_code
        if ch == "|" and not in_code:
            cells.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    cells.append("".join(buf).strip())
    return cells


def _tokens(cell: str) -> list:
    """Code spans of a cell, each flagged as an alias when it sits inside parentheses."""
    spans = [(m.start(), m.end(), m.group(1)) for m in _CODE_RE.finditer(cell)]
    masked = list(cell)
    for start, end, _text in spans:
        for i in range(start, end):
            masked[i] = "x"
    depth_at = []
    depth = 0
    for ch in masked:
        if ch == "(":
            depth += 1
        depth_at.append(depth)
        if ch == ")":
            depth -= 1
    return [(text, depth_at[start] > 0) for start, _end, text in spans]


def parse_rows(doc: Path) -> list[Row]:
    rows: list[Row] = []
    lines = doc.read_text(encoding="utf-8").splitlines()
    i = 0
    while i < len(lines):
        marker = _MARKER_RE.search(lines[i])
        if not marker:
            i += 1
            continue
        engine = marker.group(1)
        i += 1
        while i < len(lines) and not lines[i].startswith("|"):
            i += 1
        header = _split_cells(lines[i])
        i += 2  # header + separator
        while i < len(lines) and lines[i].startswith("|"):
            cells = _split_cells(lines[i])
            assert len(cells) == len(header), (doc, i + 1, cells)
            if engine == "mysql":
                source, target, classification, note = cells
                cdc = None
            else:
                source, target, classification, cdc, note = cells
            rows.append(Row(engine, doc, i + 1, source, target, classification, note, cdc,
                            _tokens(source)))
            i += 1
    return rows


def _expand(token: str) -> tuple[str, dict]:
    """A probe spelling for a doc token, and the placeholder values it bound."""
    spelling, bound = token, {}
    for placeholder, (value, binds) in _PLACEHOLDERS.items():
        if placeholder in spelling:
            spelling = spelling.replace(placeholder, value)
            bound.update(binds)
    spelling = re.sub(r",\s*…", "", spelling)
    return spelling, bound


def _pattern(token: str) -> "re.Pattern[str]":
    regex = re.escape(token)
    for placeholder, group in _PLACEHOLDER_REGEX.items():
        regex = regex.replace(re.escape(placeholder), group)
    return re.compile(f"^{regex}$", re.IGNORECASE)


def probes_of(row: Row) -> list[tuple[Probe, dict]]:
    spec = ROW_PROBES.get((row.engine, row.key), {})
    found: list[tuple[Probe, dict]] = []
    if "replace" in spec:
        base = []
    else:
        base = []
        for token, alias in row.tokens:
            if alias:
                continue
            spelling, bound = _expand(token)
            if row.engine == "mysql":
                spelling = spelling.lower()
            kwargs = _kw(type_kind=_PG_KIND_BY_NAME[spelling]) if spelling in _PG_KIND_BY_NAME else ()
            base.append((Probe(spelling, kwargs), bound))
    extra = spec.get("replace", []) + spec.get("extra", [])
    patterns = [_pattern(t) for t, alias in row.tokens if not alias]
    for probe in extra:
        bound = {}
        for pattern in patterns:
            match = pattern.match(probe.spelling)
            if match:
                bound = {k: v for k, v in match.groupdict().items() if v is not None}
                break
        found.append((probe, bound))
    return base + found


def _norm(type_sql: str) -> str:
    create = sqlglot.parse_one(f"CREATE TABLE x (c {type_sql})", read="postgres")
    column = next(create.find_all(exp.ColumnDef))
    return " ".join(column.args["kind"].sql(dialect="postgres").upper().split())


def _base(normalized: str) -> str:
    return normalized.split("(", 1)[0].split(" ", 1)[0]


def _emitted(ddl: str) -> "tuple[Optional[str], bool]":
    """(normalized type of column c, whether c carries a CHECK), or (None, False)."""
    try:
        create = sqlglot.parse_one(ddl, read="postgres")
    except sqlglot.errors.SqlglotError:
        return None, False
    if create is None:
        return None, False
    for column in create.find_all(exp.ColumnDef):
        if column.name == "c":
            kind = column.args.get("kind")
            check = any(
                isinstance(getattr(c, "kind", None), exp.CheckColumnConstraint)
                for c in column.args.get("constraints") or []
            )
            return (" ".join(kind.sql(dialect="postgres").upper().split()) if kind else None,
                    check)
    return None, False


@dataclass
class Outcome:
    type_sql: Optional[str]
    check: bool
    classification: str
    rules: frozenset
    advisory: frozenset
    cdc: Optional[str]


def run(engine: str, probe: Probe) -> Outcome:
    source = SourceType.MYSQL if engine == "mysql" else SourceType.POSTGRES
    key = ColumnDef(name="id", mysql_type="int" if engine == "mysql" else "bigint",
                    nullable=False)
    table = TableDef(name="t" if engine == "mysql" else "ch02.t",
                     columns=[key, probe.column()], primary_key=["id"])
    conversion = SchemaConverter(source_type=source).convert_table(table)
    type_sql, check = _emitted(conversion.target_ddl)
    report = CompatibilityAssessor(source_type=source).assess(SourceInventory(tables=[table]))
    (item,) = [i for i in report.assessed_items if i.object_name == table.name]
    concerns = item.concerns or []
    rules = frozenset(c.rule_id for c in concerns if not c.is_advisory) - {
        "COMPATIBLE", "CDC_UNCARRYABLE_COLUMN"}
    advisory = frozenset(c.rule_id for c in concerns if c.is_advisory)
    cdc = None
    if engine == "postgres":
        reasons = {u.reason for u in uncarryable_columns_of(table) if u.column == "c"}
        assert len(reasons) <= 1
        cdc = next(iter(reasons), None)
        flagged = any(c.rule_id == "CDC_UNCARRYABLE_COLUMN" for c in concerns)
        assert flagged == (cdc is not None), (probe, cdc, flagged)
    return Outcome(type_sql, check, item.classification.value, rules, advisory, cdc)


ROWS = [row for doc in DOCS for row in parse_rows(doc)]


def _known_rule_ids() -> frozenset:
    ids = set()
    for module in (assessor_mod, assessor_pg_mod):
        ids.update(re.findall(r'"([A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+)"',
                              Path(module.__file__).read_text(encoding="utf-8")))
    return frozenset(ids)


KNOWN_RULE_IDS = _known_rule_ids()


@pytest.mark.parametrize("row", ROWS, ids=[r.label for r in ROWS])
def test_row_matches_the_code(row: Row) -> None:
    words = row.classification.split()
    assert words and words[0] in _CLASSES, row.label
    want_class = words[0]
    want_rules = frozenset(t for t, _ in _tokens(row.classification))
    assert want_rules <= KNOWN_RULE_IDS, (row.label, want_rules - KNOWN_RULE_IDS)
    note_rules = frozenset(t for t, _ in _tokens(row.note) if _RULE_TOKEN_RE.match(t))
    assert note_rules <= KNOWN_RULE_IDS, (row.label, note_rules - KNOWN_RULE_IDS)
    not_converted = row.target.strip() == NOT_CONVERTED
    want_types = [t for t, _ in _tokens(row.target)]
    want_check = "CHECK" in row.target
    if not_converted:
        assert want_class == "UNSUPPORTED", row.label
    probes = probes_of(row)
    assert probes, f"{row.label}: no probe (add one to ROW_PROBES)"
    matched: set = set()
    problems = []
    for probe, bound in probes:
        got = run(row.engine, probe)
        where = f"{probe.spelling} {dict(probe.kwargs) or ''}".strip()
        if not not_converted:
            candidates = []
            for doc_type in want_types:
                filled, _ = doc_type, None
                names = re.findall(r"\((?:([a-zA-Z]+)(?:,([a-zA-Z]+))?)\)", doc_type)
                needs = {n for pair in names for n in pair if n}
                if needs - set(bound):
                    if needs:
                        continue
                for name in needs:
                    filled = re.sub(rf"\b{name}\b", bound[name], filled)
                candidates.append((doc_type, filled))
            ok = False
            for doc_type, filled in candidates:
                want = _norm(filled)
                if got.type_sql is None:
                    break
                same = (want == got.type_sql) if "(" in filled else (_base(want) == _base(got.type_sql))
                if same:
                    ok = True
                    matched.add(doc_type)
                    break
            if not ok:
                problems.append(f"{where}: emitted {got.type_sql!r}, doc says {want_types}")
            if got.check != want_check:
                problems.append(f"{where}: CHECK emitted={got.check}, doc says {want_check}")
        if got.classification != want_class:
            problems.append(f"{where}: Evaluation {got.classification}, doc says {want_class}")
        if got.rules != want_rules:
            problems.append(f"{where}: rules {sorted(got.rules)}, doc says {sorted(want_rules)}")
        if not got.advisory <= note_rules:
            problems.append(f"{where}: advisory {sorted(got.advisory - note_rules)} not named in the note")
        if row.engine == "postgres":
            if row.doc.parent.name == "en":
                assert row.cdc in CDC_PHRASES, (row.label, row.cdc)
                if got.cdc != CDC_PHRASES[row.cdc]:
                    problems.append(f"{where}: CDC {got.cdc!r}, doc says {row.cdc!r}")
            elif (got.cdc is None) != (row.cdc == NOT_CONVERTED):
                problems.append(f"{where}: CDC {got.cdc!r}, doc says {row.cdc!r}")
    if not not_converted:
        unmatched = set(want_types) - matched
        if unmatched:
            problems.append(f"doc types no probe produced: {sorted(unmatched)}")
    assert not problems, row.label + "\n  " + "\n  ".join(problems)


def test_every_row_probes_entry_names_a_row() -> None:
    keys = {(r.engine, r.key) for r in ROWS}
    stale = [k for k in ROW_PROBES if k not in keys]
    assert not stale, stale


# Every COLUMN_TYPE base MySQL 5.7 / 8.0 / 8.4 can report (checked on real servers).
MYSQL_COLUMN_TYPE_BASES = frozenset({
    "tinyint", "smallint", "mediumint", "int", "bigint", "bit", "decimal", "float", "double",
    "date", "datetime", "timestamp", "time", "year", "char", "varchar", "binary", "varbinary",
    "tinytext", "text", "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob",
    "enum", "set", "json", "geometry", "point", "linestring", "polygon", "multipoint",
    "multilinestring", "multipolygon", "geometrycollection", "geomcollection",
})
# format_type never prints these spellings (it prints the long form), so no row needs them.
PG_FORMAT_TYPE_ALIASES = frozenset({
    "int2", "int", "int4", "int8", "float4", "float8", "decimal", "dec", "char", "varchar",
    "timetz", "timestamptz", "bool", "time", "timestamp", "varbit",
})
# Explicit, reviewed exemptions from the coverage rule.
PG_COVERAGE_EXEMPT = frozenset({
    "bpchar",  # only a bare `bpchar` declaration prints it; rare
})


def _mysql_base(spelling: str) -> str:
    return spelling.strip().lower().replace("(", " ").split()[0]


def test_every_type_the_converter_knows_has_a_row() -> None:
    mysql_probed = {_mysql_base(p.spelling) for r in ROWS if r.engine == "mysql"
                    for p, _ in probes_of(r)}
    registries = set(converter_mod._SPATIAL_TYPES)
    registries |= {k.name.lower() for k in converter_mod._BLOB_TYPES}
    registries |= {k.name.lower().removeprefix("u") for k in converter_mod._UNSIGNED_WIDENING}
    registries |= {k.name.lower() for k in converter_mod._SIGNED_INT_TARGET}
    assert registries <= MYSQL_COLUMN_TYPE_BASES, registries - MYSQL_COLUMN_TYPE_BASES
    assert MYSQL_COLUMN_TYPE_BASES <= mysql_probed, MYSQL_COLUMN_TYPE_BASES - mysql_probed

    pg_probed = {converter_pg_mod.normalize_pg_base_type(p.spelling)
                 for r in ROWS if r.engine == "postgres" for p, _ in probes_of(r)}
    pg_known = (set(converter_pg_mod._DSQL_SUPPORTED_PG_BASE_TYPES)
                | set(converter_pg_mod._PG_SUBSTITUTE_TARGET)
                | set(converter_pg_mod._PG_RANGE_TYPES)
                | set(converter_pg_mod._PG_UNSUPPORTED_REMODEL))
    missing = pg_known - PG_FORMAT_TYPE_ALIASES - PG_COVERAGE_EXEMPT - pg_probed
    assert not missing, sorted(missing)


def test_every_language_has_the_same_rows() -> None:
    """ko and ja carry the en tables row for row: same source types, Aurora DSQL types and
    classifications (only the notes and the CDC phrases are translated)."""
    by_language: "dict[str, list]" = {}
    for row in ROWS:
        by_language.setdefault(row.doc.parent.name, []).append(
            (row.engine, row.key, row.target.strip(), row.classification.strip()))
    assert set(by_language) == {"en", "ko", "ja"}, sorted(by_language)
    for language in ("ko", "ja"):
        assert by_language[language] == by_language["en"], language
