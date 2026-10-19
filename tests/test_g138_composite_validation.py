# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G138: a table with a composite key is counted and checksummed in keyset pages on each
side, so a large one no longer exceeds Aurora DSQL's 300 s transaction age.

Each side reads its own rows in its own key order (MySQL with the Full Load reader's
cursor, PostgreSQL / Aurora DSQL with a row-value cursor whose last key comes back as text)
and only the per-side totals are compared. A side is paged only by a key it enforces as
unique (Aurora DSQL by its LIVE primary key). The doubles below evaluate the SQL shapes the
pagers emit over Python rows -- no database is reached.
"""

from __future__ import annotations

import hashlib
import random
import re
import unicodedata
from types import SimpleNamespace
from typing import Any, Optional

import pytest

import dsql_migrator.core.validator as v
from dsql_migrator.core.exporter import ExportError, keyset_stream
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    UncheckedForeignKey,
    ValidationMode,
    ValidationReport,
)
from dsql_migrator.core.source_dialect.mysql import MySQLSourceDialect
from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect
from dsql_migrator.core.validation_sql import (
    _mysql_row_token,
    _pg_row_token,
    build_mysql_composite_boundary_sql,
    build_mysql_composite_page_sql,
    build_pg_composite_orphan_page_sql,
    build_pg_composite_page_sql,
    pk_paging_columns,
)
from dsql_migrator.core.validator import (
    PagedReadError,
    ValidationCancelled,
    Validator,
    _from_only,
    _pg_table_identifier,
    render_text_report,
)

_MY = MySQLSourceDialect()
_PG = PostgresSourceDialect()
_SOURCE = SourceConnectionConfig(host="db.example.com", database="app")
_PG_SOURCE = SourceConnectionConfig(
    host="db.example.com", database="app", source_type=SourceType.POSTGRES
)
_TARGET = TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")


# ---------------------------------------------------------------------------
# Row model shared by the doubles
# ---------------------------------------------------------------------------


def _tok(row: dict) -> int:
    """A double's per-row token: equal rows hash equally on both doubles (60 bits)."""
    digest = hashlib.md5(repr(sorted((k, repr(val)) for k, val in row.items())).encode())
    return int(digest.hexdigest()[:15], 16)


def _fold(text: str) -> str:
    """A case- and accent-insensitive collation, as MySQL's utf8mb4_0900_ai_ci compares."""
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().casefold()


def _members(column_type: str) -> tuple[str, list[str]]:
    kind, _, inner = column_type.partition("(")
    return kind.strip().lower(), [m.strip().strip("'") for m in inner.rstrip(")").split(",")]


class _Table:
    """One table of a double: rows (dicts), its key, and the MySQL column types."""

    def __init__(
        self,
        name: str,
        key: tuple[str, ...],
        rows: list[dict],
        *,
        types: Optional[dict[str, str]] = None,
        statistics: Optional[list[tuple]] = None,
    ) -> None:
        self.name = name
        self.key = tuple(key)
        self.rows = [dict(r) for r in rows]
        self.columns = list(self.rows[0]) if self.rows else list(key)
        self.types = types or {c: "int" for c in self.columns}
        self.statistics = statistics

    def tabledef(self, key: Optional[tuple[str, ...]] = None) -> TableDef:
        return TableDef(
            name=self.name,
            columns=[ColumnDef(name=c, mysql_type=self.types.get(c, "int")) for c in self.columns],
            primary_key=list(self.key if key is None else key),
        )


def _my_order(table: _Table, column: str, value: object) -> object:
    """MySQL's sort key for one value: ENUM by member index, SET by bitmask, text folded."""
    kind, members = _members(table.types.get(column, "int"))
    if kind == "enum":
        return ([""] + members).index(value) if value in [""] + members else -1
    if kind == "set":
        return sum(1 << members.index(p) for p in str(value).split(",") if p)
    if isinstance(value, str):
        return _fold(value)
    return value


class _Result:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def scalar(self):
        return self._rows[0][0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def first(self):
        return self._rows[0] if self._rows else None

    def mappings(self):
        return self


class MySqlDouble:
    """A MySQL source: answers the statements Validation and the Full Load reader issue,
    filtering by the dialect's own cursor SQL (translated to Python) in MySQL's order."""

    invalidated = False

    def __init__(self, *tables: _Table) -> None:
        self.tables = {t.name: t for t in tables}
        self.executed: list[tuple[str, dict]] = []
        self.page_sums: list[int] = []

    def execution_options(self, **_kw: object) -> "MySqlDouble":
        return self

    def connect(self) -> "MySqlDouble":
        return self

    def __enter__(self) -> "MySqlDouble":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def dispose(self) -> None:
        return None

    def _table(self, sql: str, params: dict) -> _Table:
        if "information_schema" in sql:
            name = f"{params['schema']}.{params['name']}" if "schema" in params else params["name"]
            return self.tables[name]
        match = re.search(r"FROM `([^`]+)`\.`([^`]+)`", sql)
        return self.tables[f"{match[1]}.{match[2]}"]

    def _filter(self, table: _Table, where: str, params: dict) -> list[dict]:
        rows = table.rows
        if where:
            expr = re.sub(r"CAST\(`([^`]+)` AS UNSIGNED\)", lambda m: f"_num(row, {m[1]!r})", where)
            expr = re.sub(
                r"`([^`]+)` IN \(([^)]*)\)",
                lambda m: "_in(row, {!r}, [{}])".format(
                    m[1], ", ".join(f"p[{n.strip()[1:]!r}]" for n in m[2].split(","))
                ),
                expr,
            )
            expr = re.sub(r"`([^`]+)` > :(\w+)", lambda m: f"_gt(row, {m[1]!r}, p[{m[2]!r}])", expr)
            expr = re.sub(r"`([^`]+)` = :(\w+)", lambda m: f"_eq(row, {m[1]!r}, p[{m[2]!r}])", expr)
            expr = re.sub(r":(\w+)", lambda m: f"p[{m[1]!r}]", expr)
            expr = expr.replace(" AND ", " and ").replace(" OR ", " or ").replace("1 = 0", "False")

            def _label(column, value):
                return _fold(value) if isinstance(value, str) else value

            env = {
                "_num": lambda row, c: _my_order(table, c, row[c]),
                "_gt": lambda row, c, val: _my_order(table, c, row[c]) > _my_order(table, c, val),
                "_eq": lambda row, c, val: _label(c, row[c]) == _label(c, val),
                "_in": lambda row, c, vals: _label(c, row[c]) in [_label(c, x) for x in vals],
            }
            rows = [row for row in rows if eval(expr, env, {"row": row, "p": params})]  # noqa: S307
        return sorted(rows, key=lambda r: tuple(_my_order(table, c, r[c]) for c in table.key))

    def execute(self, statement: object, parameters: object = None) -> _Result:
        sql = str(statement)
        params = dict(parameters or {})
        self.executed.append((sql, params))
        upper = sql.upper()
        if upper.startswith(("START TRANSACTION", "COMMIT", "SET ", "SAVEPOINT", "SHOW ")):
            return _Result([])
        if "@@GLOBAL" in upper:
            return _Result([(None,)])
        if "INFORMATION_SCHEMA.STATISTICS" in upper:
            table = self._table(sql, params)
            parts = table.statistics
            if parts is None:
                parts = [(c, "A", None) for c in table.key]
            return _Result(list(parts))
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            table = self._table(sql, params)
            return _Result([(c, table.types.get(c, "int")) for c in table.columns])
        table = self._table(sql, params)
        where_match = re.search(r" WHERE (.*?) ORDER BY ", sql)
        where = where_match[1] if where_match else ""
        if "SUM(CK_TOK)" in upper:
            page = self._filter(table, where, params)[: int(params["page"])]
            total = sum(_tok(r) for r in page)
            self.page_sums.append(total)
            return _Result([(total, len(page))])
        if "SUM(PAGE_TOK)" in upper:  # a single-column key's checksum page
            page = self._filter(table, where, params)[: int(params["page"])]
            last = page[-1][table.key[0]] if page else None
            return _Result([(sum(_tok(r) for r in page), last, len(page))])
        if " AS PK FROM " in upper:  # a single-column key's reconcile page
            page = self._filter(table, where, params)[: int(params["page"])]
            return _Result([(r[table.key[0]],) for r in page])
        if "LIMIT 1 OFFSET :OFF" in upper:
            rows = self._filter(table, where, params)
            off = int(params["off"])
            return _Result([tuple(rows[off][c] for c in table.key)] if off < len(rows) else [])
        if "LIMIT :BATCH_SIZE" in upper:  # the Full Load reader's page (keyset_stream)
            return _Result(self._filter(table, where, params)[: int(params["batch_size"])])
        if upper.startswith("SELECT COUNT(*)"):
            return _Result([(len(table.rows),)])
        if upper.startswith("SELECT COALESCE(SUM("):  # the whole-table checksum
            return _Result([(sum(_tok(r) for r in table.rows),)])
        raise AssertionError(f"MySqlDouble: unexpected statement {sql[:120]}")


class PgDouble:
    """A PostgreSQL / Aurora DSQL side: a psycopg-style connection whose pages honor the
    row-value cursor, its text-bound last key and C order. ``primary_keys`` is what the
    live catalog says (default: each table's own key; ``()`` for none)."""

    def __init__(
        self,
        *tables: _Table,
        primary_keys: Optional[dict[str, tuple[str, ...]]] = None,
        fail: Optional[dict[str, BaseException]] = None,
    ) -> None:
        self.tables = {t.name: t for t in tables}
        self.primary_keys = primary_keys or {}
        self.fail = fail or {}
        self.executed: list[tuple[str, Any]] = []
        self.page_sums: list[int] = []
        self.closed = False

    def cursor(self) -> "_PgCursor":
        return _PgCursor(self)

    def close(self) -> None:
        self.closed = True

    def rollback(self) -> None:
        return None

    @staticmethod
    def _name(sql: str, after: str = r"FROM (?:ONLY )?") -> str:
        match = re.search(after + r'"([^"]+)"(?:\."([^"]+)")?', sql)
        return f"{match[1]}.{match[2]}" if match[2] else match[1]

    def _parse(self, table: _Table, column: str, text: object) -> object:
        integer = re.match(r"(tiny|small|medium|big)?int", table.types.get(column, "int"))
        return int(text) if integer else text  # the server parses the text into its type

    def _page(self, table: _Table, key: list[str], params: dict, limit: int) -> list[dict]:
        rows = sorted(table.rows, key=lambda r: tuple(r[c] for c in key))
        if "last_0" in params:
            cursor = tuple(self._parse(table, c, params[f"last_{i}"]) for i, c in enumerate(key))
            rows = [r for r in rows if tuple(r[c] for c in key) > cursor]
        elif "last" in params:
            rows = [r for r in rows if r[key[0]] > params["last"]]
        return rows[:limit]

    def resolve(self, statement: Any, params: Optional[dict]) -> Any:
        sql = statement if isinstance(statement, str) else statement.as_string(None)
        params = dict(params or {})
        self.executed.append((sql, params))
        for marker, exc in self.fail.items():
            if marker in sql:
                raise exc
        if "ix.indisprimary" in sql:
            name = ".".join(str(params[k]) for k in ("schema", "table") if k in params)
            if name not in self.tables:
                return []
            key = self.primary_keys.get(name, self.tables[name].key)
            return [(c,) for c in key]
        if "pg_attrdef" in sql:  # read_overlay_column_facts: does the table exist?
            name = ".".join(str(params[k]) for k in ("schema", "table") if k in params)
            return [(None, None, None, None, None)] if name in self.tables else []
        table = self.tables[self._name(sql)]
        limit_match = re.search(r"LIMIT (\d+)", sql)
        limit = int(limit_match[1]) if limit_match else 0
        if "ck_k0" in sql:
            order = sql.rsplit(" LIMIT ", 1)[0].rsplit("ORDER BY ", 1)[1]  # the window's
            key = [re.findall(r'"([^"]+)"', part)[-1] for part in order.split(", ")]
            page = self._page(table, key, params, limit)
            last = [None if not page else str(page[-1][c]) for c in key]
            if "WITH pg AS MATERIALIZED" in sql:
                parent = self.tables[self._name(sql, r"LEFT JOIN ")]
                pairs = re.findall(r'p\."([^"]+)" = pg\."([^"]+)"', sql)
                refs = {tuple(r[ref] for ref, _ in pairs) for r in parent.rows}
                orphans = sum(
                    1 for r in page
                    if all(r[col] is not None for _, col in pairs)
                    and tuple(r[col] for _, col in pairs) not in refs
                )
                return (orphans, len(page), *last)
            flags = re.findall(r'\("([^"]+)" IS NOT NULL\) AS "ck_nn\d+"', sql)
            total = sum(_tok(r) for r in page) if "ck_tok" in sql else 0
            self.page_sums.append(total)
            return (total, len(page), *last, *(sum(r[c] is not None for r in page) for c in flags))
        if "page_tok" in sql:  # a single-column key's checksum page
            key = [re.search(r'SELECT "([^"]+)" AS page_pk', sql)[1]]
            page = self._page(table, key, params, limit)
            return (sum(_tok(r) for r in page), page[-1][key[0]] if page else None, len(page))
        if " AS pk" in sql:  # a single-column key's count / reconcile page
            key = [re.search(r'SELECT "([^"]+)" AS pk', sql)[1]]
            flags = re.findall(r'\("([^"]+)" IS NOT NULL\)', sql)
            return [
                (r[key[0]], *(r[c] is not None for c in flags))
                for r in self._page(table, key, params, limit)
            ]
        if "NOT EXISTS" in sql:  # the single-scan orphan count of a child with no key
            parent = self.tables[self._name(sql, r"SELECT 1 FROM ")]
            pairs = re.findall(r'p\."([^"]+)" = c\."([^"]+)"', sql)
            refs = {tuple(r[ref] for ref, _ in pairs) for r in parent.rows}
            return (sum(
                1 for r in table.rows
                if all(r[col] is not None for _, col in pairs)
                and tuple(r[col] for _, col in pairs) not in refs
            ),)
        if sql.startswith("SELECT COUNT(*)"):
            counted = re.findall(r'COUNT\("([^"]+)"\)', sql)
            return (len(table.rows), *(sum(r[c] is not None for r in table.rows) for c in counted))
        if sql.startswith("SELECT COALESCE(SUM("):
            return (sum(_tok(r) for r in table.rows),)
        raise AssertionError(f"PgDouble: unexpected statement {sql[:120]}")


class _PgCursor:
    def __init__(self, owner: PgDouble) -> None:
        self._owner = owner
        self._result: Any = None

    def execute(self, statement: Any, params: Any = None) -> None:
        self._result = self._owner.resolve(statement, params)

    def fetchone(self) -> Any:
        if isinstance(self._result, list):
            return self._result[0] if self._result else None
        return self._result

    def fetchall(self) -> Any:
        if isinstance(self._result, list):
            return list(self._result)
        return [] if self._result is None else [self._result]

    def close(self) -> None:
        return None


class _PgSqla:
    """The PostgreSQL SOURCE side: a guarded-SQLAlchemy-style connection over a PgDouble
    (``exec_driver_sql`` for the shim's rendered text, ``execute`` for ``text()``)."""

    invalidated = False

    def __init__(self, double: PgDouble, *, inheritance_parent: bool = False) -> None:
        self.double = double
        self.inheritance_parent = inheritance_parent
        self.connection = SimpleNamespace(driver_connection=None)

    def execution_options(self, **_kw: object) -> "_PgSqla":
        return self

    def connect(self) -> "_PgSqla":
        return self

    def __enter__(self) -> "_PgSqla":
        return self

    def __exit__(self, *_exc: object) -> bool:
        return False

    def dispose(self) -> None:
        return None

    def exec_driver_sql(self, sql: str, params: Any = None) -> _Result:
        result = self.double.resolve(sql, params)
        if isinstance(result, list):
            return _Result(result)
        return _Result([] if result is None else [result])

    def execute(self, statement: object, parameters: object = None) -> _Result:
        sql = str(statement)
        if "AS inheritance_parent" in sql:
            return _Result([(self.inheritance_parent,)])
        return _Result([])  # the transaction control / stale-inventory catalog read


def _validator(source: Any, target: Any, **kwargs: Any) -> Validator:
    return Validator(
        source_engine_factory=lambda _cfg: source,
        target_connection_factory=lambda _cfg: target,
        **kwargs,
    )


def _target_statements(double: PgDouble) -> list[str]:
    return [sql for sql, _ in double.executed if "pg_catalog" not in sql]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _order_items(n: int = 23, name: str = "app.order_items") -> _Table:
    rows = [
        {"order_id": 1000 + i // 4, "line_no": i % 4, "qty": i * 3, "note": f"n{i}" if i % 5 else None}
        for i in range(n)
    ]
    return _Table(name, ("order_id", "line_no"), rows,
                  types={"order_id": "bigint unsigned", "line_no": "int", "qty": "int",
                         "note": "varchar(20)"})


def _tickets(n_per: int = 7, name: str = "app.tickets") -> _Table:
    statuses = ("new", "active", "closed")
    rows = [{"status": s, "id": i, "title": f"t{i}"} for s in statuses for i in range(n_per)]
    return _Table(name, ("status", "id"), rows,
                  types={"status": "enum('new','active','closed')", "id": "int",
                         "title": "varchar(60)"})


def _prefs(name: str = "app.prefs") -> _Table:
    rows = [{"user_id": u, "channel": c, "on": (u + len(c)) % 2}
            for u in range(1, 9) for c in ("sms", "email", "push")]
    return _Table(name, ("user_id", "channel"), rows,
                  types={"user_id": "int", "channel": "enum('sms','email','push')", "on": "int"})


def _grants(name: str = "app.grants_set") -> _Table:
    perms = ("read", "write", "read,write", "admin", "read,admin", "")
    rows = [{"id": i, "perms": p, "who": f"w{i}{j}"} for i in range(1, 6) for j, p in enumerate(perms)]
    return _Table(name, ("id", "perms"), rows,
                  types={"id": "int", "perms": "set('read','write','admin')", "who": "varchar(9)"})


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def test_pg_composite_page_sql_first_and_next() -> None:
    table = _order_items().tabledef()
    first = build_pg_composite_page_sql(table, ("order_id", "line_no"), 5000, checksum=True,
                                        first=True).as_string(None)
    nxt = build_pg_composite_page_sql(table, ("order_id", "line_no"), 5000, checksum=True,
                                      first=False).as_string(None)
    assert "WHERE" not in first
    assert 'WHERE ("order_id", "line_no") > (%(last_0)s, %(last_1)s)' in nxt
    for text in (first, nxt):
        assert 'ORDER BY "app"."order_items"."order_id", "app"."order_items"."line_no" LIMIT 5000' in text
        assert '(array_agg("ck_k0"::text ORDER BY "ck_k0", "ck_k1"))[COUNT(*)]' in text
        assert '(array_agg("ck_k1"::text ORDER BY "ck_k0", "ck_k1"))[COUNT(*)]' in text
        assert _pg_row_token(table).as_string(None) + " AS ck_tok" in text
        assert text.startswith("SELECT COALESCE(SUM(ck_tok), 0), COUNT(*), ")


def test_pg_composite_count_page_reads_key_columns_and_proof_flags_only() -> None:
    table = _order_items().tabledef()
    text = build_pg_composite_page_sql(table, ("order_id", "line_no"), 7, checksum=False,
                                       nonnull_columns=("note",), first=False).as_string(None)
    assert "md5(" not in text and "ck_tok" not in text
    assert text.startswith("SELECT 0, COUNT(*), ")
    assert '("note" IS NOT NULL) AS "ck_nn0"' in text
    assert 'COUNT(*) FILTER (WHERE "ck_nn0")' in text


def test_from_only_rewrites_exactly_one_from_of_a_composite_page() -> None:
    table = _order_items().tabledef()
    statement = build_pg_composite_page_sql(table, ("order_id", "line_no"), 9, checksum=True,
                                            first=False)
    text = _from_only(statement, _pg_table_identifier(table.name)).as_string(None)
    assert text.count('FROM ONLY "app"."order_items"') == 1
    assert 'ORDER BY "app"."order_items"."order_id"' in text  # the ORDER BY keeps its name


def test_composite_builders_quote_hostile_identifiers() -> None:
    table = TableDef(
        name='s"x.t`y',
        columns=[ColumnDef(name='k"1', mysql_type="int"), ColumnDef(name="k`2", mysql_type="int")],
        primary_key=['k"1', "k`2"],
    )
    pg = build_pg_composite_page_sql(table, ('k"1', "k`2"), 3, checksum=False, first=False).as_string(None)
    assert '("k""1", "k`2") > (%(last_0)s, %(last_1)s)' in pg
    assert 'FROM "s""x"."t`y"' in pg
    my = build_mysql_composite_page_sql(table, "")
    assert 'FROM `s"x`.`t``y`' in my and "`k\"1`" in my and "`k``2`" in my
    fk = ForeignKeyDef(name="f", columns=['k"1'], referenced_table='p"q.r', referenced_columns=["i d"])
    orphan = build_pg_composite_orphan_page_sql(table.name, fk, ('k"1', "k`2"), 3, first=True).as_string(None)
    assert 'LEFT JOIN "p""q"."r" AS p ON p."i d" = pg."k""1"' in orphan


def test_mysql_composite_builders_embed_the_dialects_cursor_verbatim() -> None:
    table = _order_items().tabledef()
    where, _params = _MY.keyset_after_sql(table, (1000, 2), {})
    page = build_mysql_composite_page_sql(table, where)
    boundary = build_mysql_composite_boundary_sql(table, where)
    order = "ORDER BY `app`.`order_items`.`order_id`, `app`.`order_items`.`line_no`"
    assert f" WHERE {where} {order} LIMIT :page) ckpage" in page
    assert f"SELECT {_mysql_row_token(table)} AS ck_tok FROM `app`.`order_items`" in page
    assert boundary == (
        f"SELECT `order_id`, `line_no` FROM `app`.`order_items` WHERE {where} {order} "
        "LIMIT 1 OFFSET :off"
    )
    assert " WHERE " not in build_mysql_composite_page_sql(table, "")


def test_composite_orphan_page_filters_its_window_by_the_cursor_only() -> None:
    fk = ForeignKeyDef(name="fk_o", columns=["order_id"], referenced_table="app.orders",
                       referenced_columns=["id"])
    text = build_pg_composite_orphan_page_sql("app.order_items", fk, ("order_id", "line_no"),
                                              50, first=False).as_string(None)
    window = text.split(") SELECT ")[0]
    assert 'WHERE ("order_id", "line_no") > (%(last_0)s, %(last_1)s)' in window
    assert "IS NULL" not in window and "IS NOT NULL" not in window  # no orphan predicate
    # The FK column that is also a key column is selected under its own name too.
    assert '"order_id" AS "ck_k0", "line_no" AS "ck_k1", "order_id" FROM' in window
    assert text.endswith('(SELECT (array_agg("ck_k1"::text ORDER BY "ck_k0", "ck_k1"))[COUNT(*)] FROM pg)')


def test_pk_paging_columns() -> None:
    assert pk_paging_columns(_Table("t", ("id",), [{"id": 1}]).tabledef()) == ("id",)
    assert pk_paging_columns(_order_items().tabledef()) == ("order_id", "line_no")
    assert pk_paging_columns(_order_items().tabledef(key=("order_id", "gone"))) == ()
    assert pk_paging_columns(_order_items().tabledef(key=())) == ()


# ---------------------------------------------------------------------------
# The PostgreSQL / Aurora DSQL pager
# ---------------------------------------------------------------------------


def test_pg_pages_add_up_and_bind_the_last_key_back_as_text() -> None:
    table = _order_items(23)
    double = PgDouble(table)
    beats: list[int] = []
    totals = v._composite_pages_pg(double, table.tabledef(), table.key, 5, checksum=True,
                                   on_page=lambda: beats.append(1))
    assert totals.rows == 23
    assert totals.checksum == sum(_tok(r) for r in table.rows)
    pages = [(sql, params) for sql, params in double.executed if "ck_k0" in sql]
    assert len(pages) == 5 == len(beats)  # 5 + 5 + 5 + 5 + 3, one beat per page
    assert pages[0][1] == {}
    ordered = sorted(table.rows, key=lambda r: (r["order_id"], r["line_no"]))
    for index, (_sql, params) in enumerate(pages[1:], start=1):
        last = ordered[index * 5 - 1]
        assert params == {"last_0": str(last["order_id"]), "last_1": str(last["line_no"])}


def test_pg_pages_of_an_exact_multiple_end_on_a_trailing_empty_page() -> None:
    table = _order_items(20)
    double = PgDouble(table)
    totals = v._composite_pages_pg(double, table.tabledef(), table.key, 5, checksum=False)
    assert totals.rows == 20 and totals.checksum == 0
    assert len([sql for sql, _ in double.executed if "ck_k0" in sql]) == 5


class _ScriptedCursor:
    def __init__(self, owner: "_Scripted") -> None:
        self._owner = owner
        self._row: Any = None

    def execute(self, statement: Any, params: Any = None) -> None:
        self._owner.executed.append((statement, params))
        self._row = self._owner.rows.pop(0) if self._owner.rows else None

    def fetchone(self) -> Any:
        return self._row

    def fetchall(self) -> Any:
        return [self._row] if self._row is not None else []

    def close(self) -> None:
        return None


class _Scripted:
    def __init__(self, rows: list) -> None:
        self.rows = list(rows)
        self.executed: list = []

    def cursor(self) -> _ScriptedCursor:
        return _ScriptedCursor(self)


def test_a_pg_page_that_does_not_move_past_its_cursor_raises() -> None:
    conn = _Scripted([(1, 2, "7", "1"), (1, 2, "7", "1"), (1, 2, "7", "1")])
    table = _order_items().tabledef()
    with pytest.raises(PagedReadError, match="did not move past"):
        v._composite_pages_pg(conn, table, table.primary_key, 2, checksum=True)
    assert len(conn.executed) == 2  # stopped at the repeat, never looped


def test_a_stalled_table_is_graded_an_error_not_a_match() -> None:
    table = _order_items(6)

    class _Stalls(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            row = super().resolve(statement, params)
            if isinstance(row, tuple) and "ck_k0" in self.executed[-1][0]:
                return (row[0], row[1], "1000", "1")  # always the same last key
            return row

    source = MySqlDouble(table)
    report = _validator(source, _Stalls(table), reconcile_page_size=2).validate(
        _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM
    )
    item = report.items[0]
    assert item.matched is False and "did not move past" in (item.error or "")


# ---------------------------------------------------------------------------
# The MySQL pager (the Full Load reader's cursor)
# ---------------------------------------------------------------------------


def _whole_table_sums(double: MySqlDouble) -> int:
    return sum(sql.startswith("SELECT COALESCE(SUM(") and "ck_tok" not in sql
               for sql, _ in double.executed)


def _my_pages(double: MySqlDouble) -> list[tuple[str, dict]]:
    out = []
    for sql, params in double.executed:
        if "SUM(ck_tok)" in sql:
            match = re.search(r" WHERE (.*?) ORDER BY ", sql)
            out.append((match[1] if match else "", {k: val for k, val in params.items() if k != "page"}))
    return out


def test_mysql_pages_read_the_boundary_only_for_full_pages() -> None:
    table = _order_items(23)
    double = MySqlDouble(table)
    checksum = v._source_checksum(double, table.tabledef(), 5, dialect=_MY, expected_rows=23)
    assert checksum == str(sum(_tok(r) for r in table.rows))
    pages = [sql for sql, _ in double.executed if "SUM(ck_tok)" in sql]
    boundaries = [p for sql, p in double.executed if "LIMIT 1 OFFSET" in sql]
    assert len(pages) == 5 and len(boundaries) == 4  # the short last page reads none
    assert all(p["off"] == 4 for p in boundaries)
    # One catalog read of the source's PRIMARY index, before the first page.
    assert sum("information_schema.STATISTICS" in sql for sql, _ in double.executed) == 1


def test_mysql_pages_walk_a_key_led_by_an_enum_member_by_member() -> None:
    table = _tickets(7)  # 3 members x 7 rows, page 5
    double = MySqlDouble(table)
    checksum = v._source_checksum(double, table.tabledef(), 5, dialect=_MY, expected_rows=21)
    assert checksum == str(sum(_tok(r) for r in table.rows))
    within = "`status` = :last_0 AND ((`id` > :last_1))"
    assert _my_pages(double) == [
        ("", {}),
        (within, {"last_0": "new", "last_1": 4}),
        ("`status` = :last_0", {"last_0": "active"}),
        (within, {"last_0": "active", "last_1": 4}),
        ("`status` = :last_0", {"last_0": "closed"}),
        (within, {"last_0": "closed", "last_1": 4}),
    ]


@pytest.mark.parametrize("make", [_prefs, _grants, _order_items, _tickets])
@pytest.mark.parametrize("page", [1, 2, 3, 5, 7])
def test_mysql_pages_are_exact_for_enum_set_and_plain_keys(make, page) -> None:
    table = make()
    double = MySqlDouble(table)
    total = v._source_checksum(double, table.tabledef(), page, dialect=_MY,
                               expected_rows=len(table.rows))
    assert total == str(sum(_tok(r) for r in table.rows))


@pytest.mark.parametrize("make", [_prefs, _grants, _order_items, _tickets])
@pytest.mark.parametrize("page", [1, 2, 3, 5, 7])
def test_the_pager_and_the_full_load_reader_take_the_same_cursors(make, page) -> None:
    table = make()
    pager = MySqlDouble(table)
    v._source_checksum(pager, table.tabledef(), page, dialect=_MY, expected_rows=len(table.rows))
    reader = MySqlDouble(table)
    read = list(keyset_stream(reader, table.tabledef(), batch_size=page, dialect=_MY))
    assert len(read) == len(table.rows)
    cursors = []
    for sql, params in reader.executed:
        if "LIMIT :batch_size" in sql:
            match = re.search(r" WHERE (.*?) ORDER BY ", sql)
            cursors.append((match[1] if match else "",
                            {k: val for k, val in params.items() if k != "batch_size"}))
    assert _my_pages(pager) == cursors


def test_mysql_paged_rows_must_equal_the_live_count() -> None:
    table = _order_items(12)
    double = MySqlDouble(table)
    with pytest.raises(PagedReadError, match="read 12 rows, but COUNT\\(\\*\\) .* counts 13"):
        v._source_checksum(double, table.tabledef(), 5, dialect=_MY, expected_rows=13)
    assert v._source_checksum(double, table.tabledef(), 5, dialect=_MY, expected_rows=12)
    counted = MySqlDouble(table)  # no live count from the caller: the pager counts itself
    v._source_checksum(counted, table.tabledef(), 5, dialect=_MY)
    assert any(sql.startswith("SELECT COUNT(*)") for sql, _ in counted.executed)


def test_a_disagreeing_source_count_grades_the_table_not_compared() -> None:
    table = _order_items(12)

    class _Overcounts(MySqlDouble):
        def execute(self, statement: object, parameters: object = None) -> _Result:
            if str(statement).upper().startswith("SELECT COUNT(*)"):
                self.executed.append((str(statement), {}))
                return _Result([(13,)])
            return super().execute(statement, parameters)

    report = _validator(_Overcounts(table), PgDouble(table), reconcile_page_size=5).validate(
        _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM
    )
    item = report.items[0]
    assert item.matched is False
    assert "keyset pages read 12 rows, but COUNT(*) in the same snapshot counts 13" in item.error


@pytest.mark.parametrize(
    "statistics",
    [
        [("order_id", "A", None), ("line_no", "D", None)],  # a DESC key part
        [("order_id", "A", 4), ("line_no", "A", None)],     # a prefix key part
        [("order_id", None, None), ("line_no", None, None)],  # a HASH index
        [("line_no", "A", None), ("order_id", "A", None)],  # not the recorded key
        [],                                                  # not readable
    ],
)
def test_a_mysql_key_the_index_does_not_keep_in_order_keeps_one_statement(statistics) -> None:
    table = _order_items(12)
    table.statistics = statistics
    double = MySqlDouble(table)
    checksum = v._source_checksum(double, table.tabledef(), 5, dialect=_MY, expected_rows=12)
    assert checksum == str(sum(_tok(r) for r in table.rows))
    assert not any("ck_tok" in sql for sql, _ in double.executed)
    assert _whole_table_sums(double) == 1


def test_a_single_precision_float_key_keeps_one_statement_in_validation() -> None:
    rows = [{"f": x / 10, "n": n, "w": n} for x in (7, 9) for n in range(10)]
    table = _Table("app.fl", ("f", "n"), rows, types={"f": "float", "n": "int", "w": "int"})
    double = MySqlDouble(table)
    assert v._source_checksum(double, table.tabledef(), 3, dialect=_MY, expected_rows=20) == str(
        sum(_tok(r) for r in rows))
    assert not any("ck_tok" in sql or "STATISTICS" in sql for sql, _ in double.executed)


def test_a_key_the_dialect_cannot_page_keeps_one_statement() -> None:
    # A row holds an ENUM value the live members do not list: the dialect refuses the cursor
    # (as the Full Load reader stops), and the source has no limit for one exact statement.
    for make, row in ((_tickets, {"status": "archived", "id": 0, "title": "x"}),
                      (_prefs, {"user_id": 1, "channel": "fax", "on": 0})):
        table = make()
        table.rows.append(row)
        double = MySqlDouble(table)
        checksum = v._source_checksum(double, table.tabledef(), 1, dialect=_MY,
                                      expected_rows=len(table.rows))
        assert checksum == str(sum(_tok(r) for r in table.rows))
        assert _whole_table_sums(double) == 1


# ---------------------------------------------------------------------------
# Pages are not comparable across the engines; totals are
# ---------------------------------------------------------------------------


def test_each_side_adds_its_own_pages_up_to_the_same_total() -> None:
    rng = random.Random(138)
    codes = ["a", "B", "Á", "m", "b", "é", "Z", "z", "É", "n"]
    seen: set = set()
    rows = []
    while len(rows) < 240:
        tenant, code = rng.randrange(4), rng.choice(codes) + str(rng.randrange(20))
        if (tenant, _fold(code)) in seen:  # MySQL's PRIMARY KEY is unique under its collation
            continue
        seen.add((tenant, _fold(code)))
        rows.append({"tenant": tenant, "code": code, "v": rng.randrange(10**6)})
    table = _Table("app.comp", ("tenant", "code"), rows,
                   types={"tenant": "int", "code": "varchar(40)", "v": "int"})
    whole = sum(_tok(r) for r in rows)
    differs = 0
    for page in sorted(rng.sample(range(1, 61), 12)):
        my, pg = MySqlDouble(table), PgDouble(table)
        assert v._source_checksum(my, table.tabledef(), page, dialect=_MY,
                                  expected_rows=len(rows)) == str(whole)
        assert v._target_checksum(pg, table.tabledef(), page) == str(whole)
        differs += my.page_sums != pg.page_sums
    assert differs >= 10  # the engines' pages hold different rows, their totals do not


# ---------------------------------------------------------------------------
# End to end through Validator.validate
# ---------------------------------------------------------------------------


def _three_tables() -> list[_Table]:
    return [_order_items(23), _tickets(7), _grants()]


@pytest.mark.parametrize("mode", [ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM])
@pytest.mark.parametrize("workers", [1, 4])
def test_composite_tables_are_paged_on_the_target_and_match(mode, workers) -> None:
    tables = _three_tables()
    source, target = MySqlDouble(*tables), PgDouble(*tables)
    report = _validator(source, target, reconcile_page_size=4).validate(
        _SOURCE, _TARGET, [t.tabledef() for t in tables], mode, max_workers=workers
    )
    assert report.is_match is True, [(i.table, i.error) for i in report.items]
    assert [i.target_row_count for i in report.items] == [23, 21, 30]
    statements = _target_statements(target)
    assert statements and all("LIMIT 4" in sql for sql in statements)
    assert not any(sql.startswith(("SELECT COUNT(*) FROM", "SELECT COALESCE(SUM(md5"))
                   for sql in statements)
    if mode is ValidationMode.CHECKSUM:
        # The checksum pages carry the row count: no separate count-only pass.
        assert not any(sql.startswith("SELECT 0, COUNT(*)") for sql in statements)
        assert all(i.checksum_match for i in report.items)


def test_a_deleted_row_and_a_changed_value_are_both_seen() -> None:
    source_tables = _three_tables()
    target_tables = _three_tables()
    target_tables[0].rows.pop(7)                       # one row missing on Aurora DSQL
    target_tables[1].rows[11]["title"] = "changed"     # one non-key value differs
    report = _validator(MySqlDouble(*source_tables), PgDouble(*target_tables),
                        reconcile_page_size=4).validate(
        _SOURCE, _TARGET, [t.tabledef() for t in source_tables], ValidationMode.CHECKSUM
    )
    items = {i.table: i for i in report.items}
    assert (items["app.order_items"].source_row_count,
            items["app.order_items"].target_row_count) == (23, 22)
    assert items["app.order_items"].matched is False
    tickets = items["app.tickets"]
    assert tickets.row_count_match is True and tickets.checksum_match is False
    assert tickets.matched is False and report.is_match is False
    assert items["app.grants_set"].matched is True


def test_a_postgresql_source_pages_a_composite_checksum_through_the_shim() -> None:
    table = _order_items(23)
    table.types = {c: "integer" if c != "note" else "text" for c in table.columns}
    source = PgDouble(table)
    report = _validator(_PgSqla(source, inheritance_parent=True), PgDouble(table),
                        reconcile_page_size=5).validate(
        _PG_SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM
    )
    assert report.items[0].matched is True, report.items[0].error
    pages = [(sql, params) for sql, params in source.executed if "ck_k0" in sql]
    assert len(pages) == 5
    assert all('FROM ONLY "app"."order_items"' in sql for sql, _ in pages)
    assert all(isinstance(val, str) for _, params in pages for val in params.values())
    # One independent COUNT(*) on the source, which the paged checksum is held to.
    assert sum(sql.startswith("SELECT COUNT(*)") for sql, _ in source.executed) == 1


def test_a_postgresql_source_checksum_is_held_to_its_count() -> None:
    table = _order_items(9)

    class _Overcounts(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            row = super().resolve(statement, params)
            return (10,) if self.executed[-1][0].startswith("SELECT COUNT(*)") else row

    with pytest.raises(PagedReadError, match="read 9 rows, but COUNT"):
        v._source_checksum_for(_PG, _PgSqla(_Overcounts(table)), table.tabledef(), 4)


def test_the_null_proof_rides_on_the_composite_count_pages() -> None:
    table = _order_items(23)
    nulls = sum(r["note"] is None for r in table.rows)
    target = PgDouble(table)
    item = _validator(MySqlDouble(table), target, reconcile_page_size=5,
                      null_proof_columns={table.name: ["note"]}).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.proof_state == "complete"
    assert item.excluded_nonnull_counts == {"note": 23 - nulls}
    flagged = [sql for sql in _target_statements(target) if '"note" IS NOT NULL' in sql]
    assert len(flagged) == 5  # every page of the one count pass, no extra pass


def test_the_null_proof_rides_on_the_composite_checksum_pages() -> None:
    table = _order_items(23)
    target = PgDouble(table)
    item = _validator(MySqlDouble(table), target, reconcile_page_size=5,
                      null_proof_columns={table.name: ["note"]}).validate(
        _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM).items[0]
    assert item.matched is True and item.proof_state == "complete"
    pages = _target_statements(target)
    assert len(pages) == 5 and all("ck_tok" in sql and '"note" IS NOT NULL' in sql
                                   for sql in pages)


class _SqlState(Exception):
    def __init__(self, sqlstate: str, message: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


def test_a_failing_proof_page_reruns_the_count_once_without_the_proof() -> None:
    table = _order_items(23)
    target = PgDouble(table, fail={"IS NOT NULL": _SqlState("42703", "column does not exist")})
    item = _validator(MySqlDouble(table), target, reconcile_page_size=5,
                      null_proof_columns={table.name: ["note"]}).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.matched is True and item.target_row_count == 23
    assert (item.proof_state, item.proof_reason) == ("not_determined", "query_error")


@pytest.mark.parametrize("sqlstate", ["54000", "53200"])
def test_an_aurora_dsql_statement_limit_is_not_rerun_without_the_proof(sqlstate) -> None:
    table = _order_items(23)
    target = PgDouble(table, fail={"ck_k0": _SqlState(sqlstate, "limit exceeded")})
    item = _validator(MySqlDouble(table), target, reconcile_page_size=5,
                      null_proof_columns={table.name: ["note"]}).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.error is not None and item.matched is False
    assert len([sql for sql in _target_statements(target) if "ck_k0" in sql]) == 1


# ---------------------------------------------------------------------------
# Part B: a side is paged only by a key that side enforces as unique
# ---------------------------------------------------------------------------


def _rekeyed() -> tuple[_Table, _Table]:
    """Source keyed ``id`` (9 rows); the target re-keyed ``(lead, id)`` holding a stale copy
    of id 5 under its old lead (10 rows) -- the E11 data."""
    rows = [{"lead": i % 3, "id": i, "v": i * 7} for i in range(1, 10)]
    source = _Table("app.leads", ("id",), rows)
    target = _Table("app.leads", ("id",), rows + [{"lead": 9, "id": 5, "v": 35}])
    return source, target


@pytest.mark.parametrize("page", [2, 4, 5])
def test_a_rekeyed_target_is_paged_by_its_live_key(page) -> None:
    source, target_table = _rekeyed()
    target = PgDouble(target_table, primary_keys={"app.leads": ("lead", "id")})
    # Paged by the SOURCE's key, a duplicate straddling a page boundary goes unseen (page 5
    # ends on the first id 5): the false MATCH Part B closes.
    assert v._bounded_target_count(PgDouble(target_table), target_table.tabledef(), 5,
                                   key=("id",)) == 9
    report = _validator(MySqlDouble(source), target, reconcile_page_size=page).validate(
        _SOURCE, _TARGET, [source.tabledef()], ValidationMode.CHECKSUM
    )
    item = report.items[0]
    assert (item.source_row_count, item.target_row_count) == (9, 10)
    assert item.matched is False
    assert any('("lead", "id") > (%(last_0)s, %(last_1)s)' in sql
               for sql in _target_statements(target))


def test_a_composite_source_key_under_a_wider_target_key_is_not_a_false_match() -> None:
    # The critic's case: source (a, b); target (c, a, b) with a stale (a, b) copy.
    rows = [{"a": i // 3, "b": i % 3, "c": 0, "v": i} for i in range(12)]
    source = _Table("app.ab", ("a", "b"), rows)
    target_table = _Table("app.ab", ("a", "b"), rows + [{"a": 2, "b": 1, "c": 5, "v": 7}])
    target = PgDouble(target_table, primary_keys={"app.ab": ("c", "a", "b")})
    for page in (2, 3, 4, 6):
        item = _validator(MySqlDouble(source), target, reconcile_page_size=page).validate(
            _SOURCE, _TARGET, [source.tabledef()], ValidationMode.CHECKSUM).items[0]
        assert (item.target_row_count, item.matched) == (13, False)


def test_reconcile_does_not_count_a_rekeyed_target_by_the_source_key() -> None:
    source, target_table = _rekeyed()
    target = PgDouble(target_table, primary_keys={"app.leads": ("lead", "id")})
    item = _validator(MySqlDouble(source), target, reconcile_page_size=2).validate(
        _SOURCE, _TARGET, [source.tabledef()], reconcile=True).items[0]
    assert item.reconcile is not None
    assert item.target_row_count == 10 and item.matched is False


def test_an_unreadable_target_key_fails_the_table_closed() -> None:
    table = _order_items(6)
    target = PgDouble(table, fail={"ix.indisprimary": _SqlState("42501", "permission denied")})
    item = _validator(MySqlDouble(table), target).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.matched is False and "permission denied" in item.error
    assert not any("ck_k0" in sql for sql in _target_statements(target))


def test_a_target_without_a_primary_key_is_read_in_one_statement() -> None:
    table = _order_items(6)
    target = PgDouble(table, primary_keys={table.name: ()})
    item = _validator(MySqlDouble(table), target).validate(
        _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM).items[0]
    assert item.matched is True
    checksum, count = _target_statements(target)
    assert checksum.startswith("SELECT COALESCE(SUM(") and "ckpage" not in checksum
    assert count == 'SELECT COUNT(*) FROM "app"."order_items"'


def test_a_keyless_targets_transaction_age_error_says_what_to_do() -> None:
    table = _order_items(6)
    timeout = _SqlState("54000", "transaction age limit of 300s exceeded")
    keyless = PgDouble(table, primary_keys={table.name: ()}, fail={"SELECT COUNT(*)": timeout})
    item = _validator(MySqlDouble(table), keyless).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert item.error.startswith("_SqlState: transaction age limit of 300s exceeded — ")
    assert v.KEYLESS_TABLE_LIMIT_HINT in item.error
    keyed = PgDouble(table, fail={"ck_k0": timeout})  # a keyed table: the error alone
    item = _validator(MySqlDouble(table), keyed).validate(
        _SOURCE, _TARGET, [table.tabledef()]).items[0]
    assert v.KEYLESS_TABLE_LIMIT_HINT not in item.error


# ---------------------------------------------------------------------------
# Orphans
# ---------------------------------------------------------------------------


def _with_fk(table: _Table, *, parent: str = "app.orders", columns=("order_id",)) -> TableDef:
    tabledef = table.tabledef()
    return tabledef.model_copy(update={"foreign_keys": [
        ForeignKeyDef(name="fk_order", columns=list(columns), referenced_table=parent,
                      referenced_columns=["id"])]})


def test_a_composite_child_is_orphan_counted_in_pages() -> None:
    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006) if i != 1003])
    target = PgDouble(child, orders)
    report = _validator(MySqlDouble(child), target, reconcile_page_size=4).validate(
        _SOURCE, _TARGET, [_with_fk(child)], check_orphans=True
    )
    expected = sum(r["order_id"] == 1003 for r in child.rows)
    assert [(f.foreign_key, f.orphan_count) for f in report.orphan_findings] == [
        ("fk_order", expected)]
    orphan_pages = [sql for sql in _target_statements(target) if "WITH pg AS MATERIALIZED" in sql]
    assert len(orphan_pages) == 6 and all("ck_k0" in sql for sql in orphan_pages)
    assert not any("NOT EXISTS" in sql for sql in _target_statements(target))


@pytest.mark.parametrize("workers", [1, 3])
def test_a_failing_orphan_check_marks_only_its_table(workers) -> None:
    child, other = _order_items(23), _tickets(3)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006)])
    target = PgDouble(child, other, orders,
                      fail={"WITH pg AS MATERIALIZED": _SqlState("XX001", "data corrupted")})
    report = _validator(MySqlDouble(child, other), target).validate(
        _SOURCE, _TARGET, [_with_fk(child), other.tabledef(), orders.tabledef()],
        check_orphans=True, max_workers=workers,
    )
    items = {i.table: i for i in report.items}
    marked = items["app.order_items"]
    assert marked.matched is False and marked.source_row_count == 23 == marked.target_row_count
    assert marked.error == ("The orphan check of foreign key fk_order -> app.orders could not "
                            "run: _SqlState: data corrupted")
    assert items["app.tickets"].matched is True and report.is_match is False


@pytest.mark.parametrize("workers", [1, 2])
def test_an_fk_whose_parent_is_not_on_the_target_is_reported_not_checked(workers) -> None:
    child = _order_items(8)

    class _NoParent(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            if isinstance(statement, str) or "app\".\"orders" not in statement.as_string(None):
                return super().resolve(statement, params)
            self.executed.append((statement.as_string(None), params))
            raise _SqlState("42P01", 'relation "app.orders" does not exist')

    other = _tickets(2)
    report = _validator(MySqlDouble(child, other), _NoParent(child, other)).validate(
        _SOURCE, _TARGET, [_with_fk(child), other.tabledef()], check_orphans=True,
        max_workers=workers,
    )
    assert report.is_match is True and all(i.matched for i in report.items)
    assert report.orphan_unchecked == [UncheckedForeignKey(
        table="app.order_items", foreign_key="fk_order", referenced_table="app.orders")]
    text = render_text_report(report)
    assert ("Orphan check: performed (1 foreign key(s) not checked: their parent table is "
            "not on Aurora DSQL)") in text
    assert "- not checked: app.order_items.fk_order -> app.orders" in text


def test_a_stop_during_the_orphan_check_still_stops_the_run() -> None:
    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": 1000}])
    target = PgDouble(child, orders)

    def _stop() -> bool:  # once the first orphan page has run
        return any("WITH pg" in sql for sql, _ in target.executed)

    with pytest.raises(ValidationCancelled):
        _validator(MySqlDouble(child), target, reconcile_page_size=4).validate(
            _SOURCE, _TARGET, [_with_fk(child)], check_orphans=True, should_cancel=_stop)
    assert sum("WITH pg" in sql for sql, _ in target.executed) == 1


def test_the_cut_over_pre_check_pages_a_composite_child_by_its_live_key() -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import _count_orphans

    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006) if i != 1001])
    fk = ForeignKeyDef(name="fk_order", columns=["order_id"], referenced_table="app.orders",
                       referenced_columns=["id"])
    expected = sum(r["order_id"] == 1001 for r in child.rows)
    beats: list[int] = []
    paged = PgDouble(child, orders)
    assert _count_orphans(
        lambda: paged, "app.order_items", fk, 4, on_page=lambda: beats.append(1)
    ) == expected
    pages = [sql for sql, _ in paged.executed if "WITH pg AS MATERIALIZED" in sql]
    assert len(pages) == 6 == len(beats)
    assert sum("ix.indisprimary" in sql for sql, _ in paged.executed) == 1
    assert paged.closed is True  # the count closes the connection it opened (G139)
    keyless = PgDouble(child, orders, primary_keys={"app.order_items": ()})
    assert _count_orphans(lambda: keyless, "app.order_items", fk, 4) == expected
    assert any("NOT EXISTS" in sql for sql, _ in keyless.executed)
    unreadable = PgDouble(child, orders, fail={"ix.indisprimary": _SqlState("42501", "denied")})
    with pytest.raises(_SqlState):
        _count_orphans(lambda: unreadable, "app.order_items", fk, 4)


def test_the_cut_over_pre_check_replays_only_the_page_a_conflict_hit() -> None:
    # G139: a conflict on the third composite page replays that page alone, so neither the
    # live key read nor the pages before it run again (G138 re-ran the count from page 1),
    # and the orphans on the replayed page are counted once.
    from dsql_migrator.ui.data_migration._full_load_engine import _count_orphans

    child = _order_items(23)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1006) if i != 1002])
    calls = {"pages": 0}

    class _OneConflict(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            row = super().resolve(statement, params)
            if "WITH pg" in self.executed[-1][0]:
                calls["pages"] += 1
                if calls["pages"] == 3:
                    raise _SqlState("40001", "OC000 conflict")
            return row

    double = _OneConflict(child, orders)
    fk = ForeignKeyDef(name="fk_order", columns=["order_id"], referenced_table="app.orders",
                       referenced_columns=["id"])
    expected = sum(r["order_id"] == 1002 for r in child.rows)
    assert expected == 4  # all on the third page of 4 rows
    assert _count_orphans(
        lambda: double, "app.order_items", fk, 4, sleep=lambda _s: None
    ) == expected
    pages = [params for sql, params in double.executed if "WITH pg" in sql]
    assert len(pages) == 7  # 6 pages, the third of them twice
    assert pages[2] == pages[3] and pages.count({}) == 1
    walk = pages[:3] + pages[4:]
    assert len({tuple(sorted(p.items())) for p in walk}) == 6  # no other page re-read
    assert sum("ix.indisprimary" in sql for sql, _ in double.executed) == 1


# ---------------------------------------------------------------------------
# A Stop takes effect within one page
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("make", [lambda: _order_items(40), lambda: _Table(
    "app.single", ("id",), [{"id": i, "v": i} for i in range(40)])])
def test_a_stop_takes_effect_within_one_page_of_a_large_table(make) -> None:
    table = make()
    source = MySqlDouble(table)
    pages = {"n": 0}

    class _Counting(PgDouble):
        def resolve(self, statement: Any, params: Any) -> Any:
            pages["n"] += 1
            return super().resolve(statement, params)

    counting = _Counting(table)
    with pytest.raises(ValidationCancelled):
        _validator(source, counting, reconcile_page_size=3).validate(
            _SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM,
            should_cancel=lambda: pages["n"] >= 3)
    assert pages["n"] == 3  # the key read and two pages: stopped at the next page's poll
    assert counting.closed is True


# ---------------------------------------------------------------------------
# A single-precision FLOAT key column: the Full Load reader refuses it
# ---------------------------------------------------------------------------


def test_the_reader_refuses_a_single_precision_float_key_column() -> None:
    table = TableDef(
        name="app.fl",
        columns=[ColumnDef(name="f", mysql_type="float"), ColumnDef(name="n", mysql_type="int")],
        primary_key=["f", "n"],
    )
    with pytest.raises(ExportError, match="key column 'f' is a single-precision FLOAT"):
        _MY.keyset_after_sql(table, (0.7, 3), {})
    for spelling in ("double", "double precision", "real", "decimal(9,3)", "int"):
        same = table.model_copy(update={"columns": [
            ColumnDef(name="f", mysql_type=spelling), ColumnDef(name="n", mysql_type="int")]})
        _MY.keyset_after_sql(same, (0.7, 3), {})  # no refusal
    for spelling in ("float", "FLOAT(7,4)", "float unsigned"):
        assert v.is_single_precision_float(spelling)


def test_a_float_keyed_table_stops_its_load_at_the_second_page() -> None:
    rows = [{"f": x / 10, "n": n} for x in (7, 9) for n in range(10)]
    table = _Table("app.fl", ("f", "n"), rows, types={"f": "float", "n": "int"})
    reader = MySqlDouble(table)
    stream = keyset_stream(reader, table.tabledef(), batch_size=5, dialect=_MY)
    read = [next(stream) for _ in range(5)]  # the first page reads
    assert len(read) == 5
    with pytest.raises(ExportError, match="single-precision FLOAT"):
        next(stream)


# ---------------------------------------------------------------------------
# The report, the screen and the manual
# ---------------------------------------------------------------------------


def _unchecked_report() -> ValidationReport:
    from dsql_migrator.core.models import TableValidationResult

    items = [
        TableValidationResult(table=name, source_row_count=1, target_row_count=1,
                              row_count_match=True, matched=True)
        for name in ("app.a", "app.b")
    ]
    return ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=items, orphan_check_performed=True,
        orphan_unchecked=[UncheckedForeignKey(table=t, foreign_key="fk", referenced_table="app.p")
                          for t in ("app.a", "app.b")],
    )


def test_unchecked_foreign_keys_never_change_the_verdict_and_stay_out_of_old_payloads() -> None:
    report = _unchecked_report()
    assert report.is_match is True
    plain = ValidationReport.build(mode=ValidationMode.ROW_COUNT, items=report.items)
    assert "orphan_unchecked" not in plain.model_dump_json()  # an older build still loads it
    assert ValidationReport.model_validate_json(report.model_dump_json()) == report


def test_a_recheck_replaces_only_its_tables_unchecked_foreign_keys() -> None:
    from dsql_migrator.ui.validation import merge_revalidated

    report = _unchecked_report()
    kept = merge_revalidated(report, [report.items[0]])  # orphans not re-run: left alone
    assert kept.orphan_unchecked == report.orphan_unchecked
    merged = merge_revalidated(report, [report.items[0]], orphan_findings=[],
                               orphan_unchecked=[])
    assert [fk.table for fk in merged.orphan_unchecked] == ["app.b"]


def test_the_orphan_section_names_the_foreign_keys_it_could_not_check() -> None:
    from dsql_migrator.ui.validation import _render_orphans
    from tests.test_ui_validation import _ScreenUi

    ui = _ScreenUi()
    _render_orphans(ui, _unchecked_report())
    body = " ".join(ui.texts)
    assert "2 foreign key(s) not checked" in body
    assert "app.a.fk -> app.p; app.b.fk -> app.p: the parent table is not on Aurora DSQL" in body
    quiet = _ScreenUi()
    _render_orphans(quiet, ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=[], orphan_check_performed=True))
    assert not any("not checked" in t for t in quiet.texts)


_DOCS = "docs/manual/{}/05-validation.md"


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_no_longer_says_a_composite_key_is_read_in_one_statement(lang) -> None:
    from pathlib import Path

    text = (Path(__file__).resolve().parents[1] / _DOCS.format(lang)).read_text(encoding="utf-8")
    for stale in ("composite or missing PK", "composite or missing primary key",
                  "복합/누락 PK", "복합 기본 키이거나", "複合/主キー無し", "複合主キーまたは主キーのない"):
        assert stale not in text, (lang, stale)
    named = {"en": "no primary key", "ko": "기본 키가 없는", "ja": "主キーのない"}[lang]
    assert named in text and "5,000" in text
