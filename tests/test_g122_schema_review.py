"""G122: the final regression round on G112-G117 -- a column named end never ends a CASE, a
DISTINCT ON list or a REFERENCES still takes the line after it, an unnamed copy of a definition
already placed goes again, and a reply's statement is split in linear time."""

from __future__ import annotations

import gc
import time

import pytest

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

from tests.test_g105_schema_review import _converted, _orders_script, _sql
from tests.test_g111_schema_review import _unit

# A column named end -- MySQL does not reserve it (bookings, events) -- however a reply
# spells it, and one named case.
_END_NAMES = (
    '"e"."end"', "e.end", '"end"', 'e."End"', '"week end"', '"end-of-day"', "e.start AS end",
    'e.start AS "end"', "e . end", '"e"."case"', "e.case",
)
# Unreserved in PostgreSQL, so a reply may write a column of the name bare.
_KEYWORD_COLUMNS = (
    "show", "vacuum", "truncate", "call", "refresh", "commit", "rollback", "revoke", "set",
    "values", "begin", "update", "explain",
)
# Statements of many lines PostgreSQL 17 reads as one (each run on it): never cut. A CASE
# whose END starts a line, after a column named end or case.
_CASE_WHOLE = [
    *(
        f"CREATE VIEW shop.v AS\nSELECT e.id, {name},\n  CASE\n    WHEN e.id > 0 THEN 1\n"
        "    ELSE 0\n  END AS flag\nFROM shop.events AS e"
        for name in _END_NAMES
    ),
    *(
        f"CREATE VIEW shop.v AS\nSELECT e.id, {name}, CASE WHEN e.id > 0 THEN 1 ELSE 0\n"
        "END AS flag\nFROM shop.events AS e"
        for name in _END_NAMES
    ),
    'CREATE VIEW shop.v AS\nSELECT\n  CASE\n    WHEN "e"."end" < now() THEN 1\n    ELSE 0\n'
    "  END AS x\nFROM shop.events AS e",
    "CREATE VIEW shop.v AS\nSELECT\n  CASE\n    WHEN e.end < now() THEN 1\n    ELSE 0\n"
    "  END AS x\nFROM shop.events AS e",
    # pg_get_viewdef's own layout
    'CREATE VIEW "shop"."v1" AS\n SELECT id,\n    "end",\n        CASE\n'
    "            WHEN (\"end\" < now()) THEN 'past'::text\n            ELSE 'upcoming'::text\n"
    "        END AS state\n   FROM shop.bookings b",
    'CREATE INDEX ix ON shop.events ("end")\nWHERE CASE WHEN "end" IS NULL THEN true ELSE false\nEND',
    'CREATE VIEW shop.v AS\nSELECT e.id\nFROM shop.events AS e\nWHERE e."end" > now()\n  AND CASE\n'
    "    WHEN e.id > 0 THEN true\n    ELSE false\n  END",
    'CREATE VIEW shop.v AS\nSELECT e.id\nFROM shop.events AS e\nORDER BY e."end",\n  CASE\n'
    "    WHEN e.id > 0 THEN 1\n    ELSE 0\n  END",
    'CREATE VIEW shop.v AS\nSELECT e.id, CASE WHEN e."end" IS NULL THEN 1 END AS a,\n  CASE\n'
    "    WHEN e.id > 0 THEN 1\n  END AS b\nFROM shop.events AS e",
    "CREATE VIEW shop.v AS\nSELECT\n  CASE WHEN e.end IS NULL THEN\n    CASE\n      WHEN e.id > 0 THEN 1\n"
    "    END\n  ELSE 0\n  END AS nest\nFROM shop.events AS e",
]
# A keyword-named column after a DISTINCT ON list, a keyword-named table after REFERENCES.
_LIST_WHOLE = [
    *(
        f"CREATE VIEW shop.v AS\nSELECT DISTINCT ON (k.id)\n  {column}, k.id\nFROM shop.kw AS k"
        for column in _KEYWORD_COLUMNS
    ),
    "CREATE VIEW shop.v AS\nSELECT DISTINCT ON (k.id, (k.a + 1))\n  call\nFROM shop.kw AS k\nORDER BY k.id",
    "CREATE VIEW shop.v AS SELECT DISTINCT ON (\n  k.id\n)\ncommit, k.id FROM shop.kw AS k",
    *(
        f"ALTER TABLE shop.kw ADD CONSTRAINT fk_kw FOREIGN KEY (id) REFERENCES\n  {table} (id) NOT VALID"
        for table in ("commit", "show", "call", "update", "values")
    ),
]


def test_a_column_named_end_never_ends_a_case() -> None:
    """P1 / R1: END was counted against CASE over the text with its quoted names, so the
    "e"."end", e.end or AS end before a CASE whose END starts a line tied the count and the
    END line was cut: the view adopted stopped at ELSE, and Schema Apply failed it (42601)."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    for statement in _CASE_WHOLE:
        assert split_glued_statements(statement) == [statement], statement


def test_a_distinct_on_list_or_a_references_goes_on_with_the_next_line() -> None:
    """R2: DISTINCT ON (...) and REFERENCES were no sign that a statement goes on, so a
    column or table named call, commit, show, ... on the next line -- a statement head since
    G117 -- was cut off, and the view or foreign key adopted ended there (42601)."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    for statement in _LIST_WHOLE:
        assert split_glued_statements(statement) == [statement], statement


def test_an_end_glued_after_a_view_is_still_cut() -> None:
    """The other side of P1: an END only closes an open CASE, in order, so a transaction's END
    glued after a view -- one with a CASE of its own, or a column named case -- is still a
    statement of its own (a column named "case" left a CASE open, and the END was taken)."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    for view in (
        "CREATE VIEW shop.v AS\nSELECT CASE WHEN e.id > 0 THEN 1 END AS a\nFROM shop.events AS e",
        'CREATE VIEW shop.v AS\nSELECT e.id, "e"."case"\nFROM shop.events AS e',
        "CREATE VIEW shop.v AS\nSELECT e.id, e.case, e.end\nFROM shop.events AS e",
        "CREATE VIEW shop.v AS\nSELECT CASE WHEN e.id > 0 THEN 1 END AS a, e.start AS case\nFROM shop.events AS e",
    ):
        for separator in ("\n", "\n\n", "\n-- done\n"):
            pieces = split_glued_statements(view + separator + "END")
            assert pieces == [view, f"{separator.strip()}\nEND".lstrip()], view


def test_a_view_over_a_column_named_end_is_adopted_whole() -> None:
    """P1, end to end: the reply's view was adopted as far as ELSE 'closed', the notice said
    'Not taken from the reply: END.' and Schema Apply failed the view (42601) -- in REPLACE
    mode after dropping the existing one."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, build_view_preview
    from dsql_migrator.ui.schema_conversion_apply import applied_view_ddls

    events = TableDef(
        name="shop.events",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="end", mysql_type="datetime", nullable=True)],
        primary_key=["id"],
    )
    view = ViewDef(
        name="shop.v_events",
        definition="select `e`.`id` AS `id`,`e`.`end` AS `end`,(case when (`e`.`end` is null) "
        "then 'open' else 'closed' end) AS `state` from `shop`.`events` `e`",
    )
    result = _converted(events, views=[view])
    deterministic = build_view_preview(view, result.views[0]).target_ddl
    body = (
        'CREATE VIEW "shop"."v_events" AS\nSELECT\n  "e"."id",\n  "e"."end",\n  CASE\n'
        "    WHEN \"e\".\"end\" IS NULL THEN 'open'\n    ELSE 'closed'\n  END AS \"state\"\n"
        'FROM "shop"."events" AS "e"'
    )
    adoption = adopt_ddl_from_reply(_sql(body + ";"), "shop.v_events", deterministic)
    assert adoption.script == body + ";"
    assert adoption.notice == "Set as shop.v_events's target DDL — review it and Apply."
    assert _unit(result, {"shop.v_events": adoption.script}, "shop.v_events").ddls[-1] == body
    assert applied_view_ddls(result, {"shop.v_events": adoption.script})["shop.v_events"] == body


def test_a_view_after_a_distinct_on_list_is_adopted_whole() -> None:
    """R2: SELECT DISTINCT ON (...) ended in a bracket, no sign it goes on, so a column named
    call on the next line was cut off: the view adopted was 'SELECT DISTINCT ON
    (c.customer_id);' (42601) and the notice listed 'CALL,'."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, build_view_preview

    calls = TableDef(
        name="shop.calls",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="customer_id", mysql_type="int", nullable=True),
                 ColumnDef(name="call", mysql_type="varchar(20)", nullable=True)],
        primary_key=["id"],
    )
    view = ViewDef(
        name="shop.v_last_call",
        definition="select `c`.`customer_id` AS `customer_id`,`c`.`call` AS `call` from `shop`.`calls` `c`",
    )
    result = _converted(calls, views=[view])
    deterministic = build_view_preview(view, result.views[0]).target_ddl
    body = (
        'CREATE VIEW "shop"."v_last_call" AS\nSELECT DISTINCT ON (c.customer_id)\n  call,\n'
        '  c.customer_id\nFROM "shop"."calls" AS c\nORDER BY c.customer_id, c.id DESC'
    )
    adoption = adopt_ddl_from_reply(_sql(body + ";"), "shop.v_last_call", deterministic)
    assert adoption.script == body + ";"
    assert adoption.notice == "Set as shop.v_last_call's target DDL — review it and Apply."


def test_an_unnamed_copy_of_a_definition_already_placed_goes() -> None:
    """R3: a piece matched by definition was kept unless its own name was placed, and an
    unnamed one's never is: the unnamed copy a hand edit or an earlier adoption left beside
    fk_customer survived every adoption -- two FK statements on one column, three with two
    copies, and two indexes for an unnamed index copy. MySQL's duplicates under their own
    names are still kept (G117)."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    result, table, script = _orders_script()
    fk = ('ALTER TABLE "shop"."orders" ADD FOREIGN KEY ("customer_id") REFERENCES '
          '"shop"."customers" ("id") NOT VALID')
    renamed = fk.replace("ADD FOREIGN", 'ADD CONSTRAINT "fk_customer2" FOREIGN')
    index = 'CREATE INDEX ASYNC ON "shop"."orders" ("customer_id")'
    for copies, restated in (([fk], fk), ([fk], renamed), ([fk, fk], fk), ([index], index)):
        stored = script.rstrip() + "\n\n" + "\n\n".join(f"{copy};" for copy in copies)
        reply = _sql(table.target_ddl + ";", restated + ";")
        adoption = adopt_ddl_from_reply(reply, "shop.orders", script, stored)
        applied = applied_table_conversions(result, {"shop.orders": adoption.script})["shop.orders"]
        assert applied.foreign_key_ddls == table.foreign_key_ddls, (copies, restated)
        assert applied.index_ddls == table.index_ddls, (copies, restated)
        assert "unnamed" not in adoption.notice, adoption.notice
        again = adopt_ddl_from_reply(reply, "shop.orders", script, adoption.script)
        assert again.script == adoption.script


def _timed(statement: str) -> float:
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    best = float("inf")
    gc.collect()
    gc.disable()  # a collection costs by the whole suite's heap, not by this input
    try:
        for _ in range(3):
            started = time.perf_counter()
            split_glued_statements(statement)
            best = min(best, time.perf_counter() - started)
    finally:
        gc.enable()
    return best


@pytest.mark.parametrize("shape", ["case columns", "alter column set"])
def test_a_statement_is_split_in_linear_time(shape: str) -> None:
    """R4: every END / SET / VALUES / TABLE / BEGIN / DO line read the statement so far again,
    on the UI's event loop: a reply view of 500 CASE columns took 241 ms per footer tick, and
    4,000 took 15 s (an ALTER TABLE of 4,000 'ALTER COLUMN c SET' lines: 8 s)."""

    def statement(n: int) -> str:
        if shape == "case columns":
            items = ",\n".join(
                f"  CASE\n    WHEN o.c{i} > 0 THEN 'p'\n    ELSE 'n'\n  END AS c{i}" for i in range(n)
            )
            return f'CREATE VIEW "shop"."wide" AS\nSELECT\n{items}\nFROM "shop"."orders" AS o'
        return "ALTER TABLE shop.orders\n" + ",\n".join(
            f"  ALTER COLUMN c{i}\n  SET DEFAULT 0" for i in range(n)
        )

    small, large = statement(250), statement(2000)
    _timed(small)  # warm up
    assert _timed(large) < _timed(small) * 24  # linear: 8x the lines; it was 60x
