"""G117: the review of G106-G111 -- a statement glued after an unterminated CREATE is cut
whatever its kind, and a clause still without what it takes never is; a restatement that
spells out PostgreSQL's defaults is what it restates; an index or foreign key of the same
definition under another name is kept; an edit that ensures another schema still gets its
own; a bare name both readings agree on names the object again; and a notice reads each
name as written."""

from __future__ import annotations

import itertools

from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, IndexDef, TableDef, ViewDef

from tests.test_g105_schema_review import _converted, _customers, _orders_script, _sql
from tests.test_g111_schema_review import _table, _unit

# The object's own statements a reply may leave unterminated, and what may follow them. Each
# glued text -- one of _OWN, a separator, one of _FOLLOWING -- was run on PostgreSQL 17
# (DSQL's ASYNC left out): every one fails as one statement (42601) but a lone BEGIN, COMMIT
# or ROLLBACK after view_union, view_cte or view_case, which PostgreSQL reads as the alias of
# the table the view ends with. The reply meant a statement of its own there too, and the
# view is the same without the alias.
_OWN = {
    "table": 'CREATE TABLE "shop"."orders2" (\n  "id" BIGINT NOT NULL,\n  "status" TEXT,\n  PRIMARY KEY ("id")\n)',
    "table_with": 'CREATE TABLE "shop"."orders2" (\n  "id" BIGINT PRIMARY KEY\n)\nWITH (fillfactor = 70)',
    "table_comment": 'CREATE TABLE "shop"."orders2" (\n  "id" BIGINT PRIMARY KEY\n) -- the table',
    "index": 'CREATE INDEX ASYNC "ix_status" ON "shop"."orders" ("status")',
    "index_where": 'CREATE INDEX ASYNC "ix_open" ON "shop"."orders" ("status")\nWHERE "status" IS NOT NULL',
    "index_literal": "CREATE INDEX ASYNC \"ix_open\" ON \"shop\".\"orders\" (\"status\")\nWHERE \"status\" = 'open'",
    "fk": 'ALTER TABLE "shop"."orders"\n  ADD CONSTRAINT "fk_c" FOREIGN KEY ("customer_id")\n  REFERENCES "shop"."customers" ("id")\n  NOT VALID',
    "fk_cascade": 'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_c" FOREIGN KEY ("customer_id")\n  REFERENCES "shop"."customers" ("id") ON DELETE CASCADE',
    "view": 'CREATE VIEW "shop"."v2" AS\nSELECT "o"."id", "o"."status"\nFROM "shop"."orders" AS "o"\nWHERE "o"."status" IS NOT NULL',
    "view_limit": 'CREATE VIEW "shop"."v2" AS\nSELECT "customer_id", count(*) AS n\nFROM "shop"."orders"\nGROUP BY "customer_id"\nORDER BY n DESC\nLIMIT 10',
    "view_union": 'CREATE VIEW "shop"."v2" AS\nSELECT id FROM shop.orders\nUNION ALL\nSELECT id FROM shop.customers',
    "view_cte": "CREATE VIEW \"shop\".\"v2\" AS\nWITH open AS (\n  SELECT id FROM shop.orders WHERE status = 'open'\n)\nSELECT id FROM open",
    "view_or": 'CREATE VIEW "shop"."v2" AS\nSELECT id FROM shop.orders WHERE status IS NULL OR total > 0',
    "view_in": "CREATE VIEW \"shop\".\"v2\" AS\nSELECT id FROM shop.orders WHERE status IN ('a', 'b')",
    "view_case": "CREATE VIEW \"shop\".\"v2\" AS\nSELECT CASE\n  WHEN status = 'a' THEN 1\n  ELSE 0\nEND AS flag\nFROM shop.orders",
}
_FOLLOWING = {
    "select": 'SELECT count(*) FROM "shop"."orders"',
    "select_lines": "SELECT id,\n       status\nFROM shop.orders\nLIMIT 5",
    "with": "WITH x AS (SELECT 1 AS a)\nSELECT a FROM x",
    "create_table": 'CREATE TABLE "shop"."order_status" ("code" TEXT PRIMARY KEY)',
    "create_index": 'CREATE INDEX ASYNC "ix_c" ON "shop"."orders" ("customer_id")',
    "alter_fk": 'ALTER TABLE "shop"."order_log" ADD CONSTRAINT "fk_l" FOREIGN KEY ("id") REFERENCES "shop"."orders" ("id") NOT VALID',
    "insert": "INSERT INTO shop.order_log (id, note) VALUES (1, 'x')",
    "update": "UPDATE shop.orders SET status = 'x' WHERE id = 1",
    "delete": "DELETE FROM shop.orders WHERE id = 1",
    "comment": "COMMENT ON TABLE shop.orders IS 'orders'",
    "grant": "GRANT SELECT ON shop.orders TO app_user",
    "explain": "EXPLAIN SELECT * FROM shop.orders",
    "drop": "DROP TABLE IF EXISTS shop.order_log",
    "analyze": "ANALYZE shop.orders",
    "vacuum": "VACUUM shop.orders",
    "show": "SHOW search_path",
    "revoke": "REVOKE SELECT ON shop.orders FROM app_user",
    "truncate": "TRUNCATE shop.order_log",
    "call": "CALL shop.p()",
    "refresh": "REFRESH MATERIALIZED VIEW shop.mv",
    "merge": "MERGE INTO shop.order_log l USING shop.orders o ON l.id = o.id WHEN NOT MATCHED THEN INSERT VALUES (o.id, o.status)",
    "begin": "BEGIN",
    "begin_tx": "BEGIN TRANSACTION",
    "start_tx": "START TRANSACTION",
    "commit": "COMMIT",
    "end": "END",
    "rollback": "ROLLBACK",
    "set": "SET search_path = shop",
    "values": "VALUES (1)",
    "table": "TABLE shop.orders",
    "do": "DO $$ BEGIN END $$",
    "psql_d": "\\d shop.orders",
    "psql_dt": '\\dt "shop".*',
}
_SEPARATORS = ("\n\n", "\n", "\n\n-- verify\n", "\n/* check */\n")

# Statements of many lines PostgreSQL 17 reads as one: never cut. A keyword-named column or
# table after a clause that still takes it, each statement head a line may go on with, and the
# clause words that also start statements (BEGIN, SET, VALUES, TABLE, DO, END) going on one.
_WHOLE = [
    "CREATE VIEW v AS SELECT\n  update,\n  id\nFROM t",
    "CREATE VIEW v AS SELECT\n  explain,\n  id\nFROM t",
    "CREATE VIEW v AS SELECT\n  delete\nFROM t",
    "CREATE VIEW v AS SELECT id\nFROM\n  update",
    "CREATE VIEW v AS SELECT id FROM t\nWHERE\n  update > 0",
    "CREATE VIEW v AS SELECT t.id FROM t\nJOIN\n  update u ON u.id = t.id",
    "CREATE VIEW v AS SELECT\n  show,\n  vacuum,\n  truncate,\n  call,\n  refresh,\n  commit,\n  rollback\nFROM t",
    "CREATE VIEW v AS SELECT\n  set,\n  id\nFROM t",
    "UPDATE shop.orders SET\nupdate = 1",
    "INSERT INTO\n  update (a) VALUES (1)",
    "EXPLAIN\nANALYZE SELECT * FROM shop.orders",
    "EXPLAIN\nMERGE INTO shop.order_log l USING shop.orders o ON l.id = o.id WHEN MATCHED THEN DELETE",
    "WITH x AS (SELECT 1 AS id)\nMERGE INTO shop.order_log l USING x ON l.id = x.id\nWHEN MATCHED THEN DELETE",
    "GRANT SELECT,\nTRUNCATE ON shop.orders TO app_user",
    "GRANT\nTRUNCATE ON shop.orders TO app_user",
    "REVOKE\nSELECT ON shop.orders FROM app_user",
    "REVOKE\nTRUNCATE ON shop.orders FROM app_user",
    "ALTER DEFAULT PRIVILEGES IN SCHEMA shop\nGRANT SELECT ON TABLES TO app_user",
    "ALTER DEFAULT PRIVILEGES FOR ROLE app_user IN SCHEMA shop\nREVOKE SELECT ON TABLES FROM app_user",
    "CREATE TRIGGER trg AFTER\nTRUNCATE ON shop.orders EXECUTE FUNCTION shop.f()",
    "CREATE TRIGGER trg AFTER INSERT OR\nTRUNCATE ON shop.orders EXECUTE FUNCTION shop.f()",
    "CREATE TEMP TABLE t2 (a int) ON\nCOMMIT DROP",
    "CREATE SEQUENCE shop.s\nSTART WITH 1",
    "CREATE VIEW shop.v AS SELECT 'a\n\\d' AS x FROM shop.orders",
    "CREATE VIEW shop.v AS\nSELECT CASE\n  WHEN status = 'a' THEN 1\n  ELSE 0\nEND AS flag\nFROM shop.orders",
    "CREATE VIEW shop.v AS\nSELECT CASE WHEN status = 'a' THEN 1 END AS a,\n  CASE\n    WHEN status = 'b' THEN 2\n  END AS b\nFROM shop.orders",
    "CREATE VIEW shop.v AS\nSELECT id, CASE WHEN status = 'a' THEN 1 ELSE 0\nEND\nFROM shop.orders",
    "CREATE PROCEDURE shop.p() LANGUAGE sql\nBEGIN ATOMIC\nEND",
    "CREATE FUNCTION shop.h() RETURNS int LANGUAGE sql\nSET search_path = shop\nAS $$ SELECT 1 $$",
    "CREATE FUNCTION shop.g() RETURNS\nTABLE (a int)\nLANGUAGE sql\nAS $$ SELECT 1 $$",
    "CREATE TABLE shop.t2 AS\nVALUES (1)",
    "CREATE TABLE shop.t2 AS\nTABLE shop.orders",
    "CREATE TABLE shop.t2 AS SELECT 1 AS a\nUNION ALL\nVALUES (2)",
    "CREATE TABLE shop.p1 PARTITION OF shop.orders FOR\nVALUES IN (1)",
    "CREATE VIEW shop.v AS\nWITH x AS (SELECT 1 AS a)\nTABLE x",
    "CREATE VIEW shop.v AS\nWITH RECURSIVE r(n) AS (\n  SELECT 1\n  UNION ALL\n  SELECT n + 1 FROM r WHERE n < 5\n)\nSEARCH DEPTH FIRST BY n\nSET ord\nSELECT n FROM r",
    "ALTER TABLE shop.orders ADD CONSTRAINT fk_c FOREIGN KEY (customer_id) REFERENCES shop.customers (id) ON DELETE\nSET NULL",
    "ALTER TABLE shop.orders ADD CONSTRAINT fk_c FOREIGN KEY (customer_id) REFERENCES shop.customers (id) ON UPDATE\n  SET DEFAULT NOT VALID",
    "ALTER TABLE shop.orders ADD FOREIGN KEY (customer_id) REFERENCES shop.customers (id),\n  ALTER COLUMN status\n  SET NOT NULL",
    "ALTER TABLE shop.orders\n  ALTER COLUMN status\n  SET DEFAULT 'x'",
    "ALTER TABLE shop.orders\n  SET (fillfactor = 70)",
    "INSERT INTO shop.order_log AS l (id, note)\nVALUES (1, 'x')\nON CONFLICT (id)\nDO UPDATE SET note = EXCLUDED.note",
    "INSERT INTO shop.order_log (id, note) VALUES (1, 'x')\nON CONFLICT (id) DO UPDATE\nSET note = EXCLUDED.note",
    "INSERT INTO shop.order_log (id, note) VALUES (1, 'x')\nON CONFLICT (id)\nDO NOTHING",
    "INSERT INTO shop.order_log\nVALUES (1, 'x')",
    "INSERT INTO shop.order_log\nTABLE shop.order_log",
    "INSERT INTO shop.order_log DEFAULT\nVALUES",
    "UPDATE shop.orders\nSET status = 'x'\nFROM shop.customers c\nWHERE c.id = shop.orders.customer_id\nRETURNING shop.orders.id",
    "UPDATE shop.orders AS o\nSET status = 'x'",
    "MERGE INTO shop.order_log l USING shop.orders o ON l.id = o.id\nWHEN MATCHED THEN UPDATE\nSET note = o.status",
    "MERGE INTO shop.order_log l USING shop.orders o ON l.id = o.id\nWHEN NOT MATCHED THEN INSERT (id, note)\nVALUES (o.id, o.status)",
    "CREATE RULE r AS ON UPDATE TO shop.order_log DO ALSO UPDATE shop.orders\nSET status = 'x' WHERE false",
    "CREATE RULE r AS ON INSERT TO shop.order_log\nDO INSTEAD NOTHING",
    "CREATE RULE r AS ON INSERT TO shop.order_log DO INSTEAD INSERT INTO shop.orders (id)\nVALUES (NEW.id)",
    "SELECT 1\nUNION\nVALUES (2)",
    "SELECT 1\nUNION\nTABLE shop.orders",
    "WITH x AS (SELECT 1 AS a)\nVALUES (1)",
    "WITH x AS (SELECT 1 AS a)\nTABLE x",
    "DO $$ BEGIN\nRAISE NOTICE 'x';\nEND $$",
]


def test_a_glued_statement_is_cut_whatever_its_kind() -> None:
    """V1: a line was a cut only before SELECT, WITH, an object kind's CREATE / ALTER / DROP,
    INSERT, UPDATE, DELETE, GRANT, COMMENT ON and EXPLAIN, so an unterminated CREATE and the
    ANALYZE, COMMIT, psql \\d or SET after it stayed one statement, taken as the object's
    CREATE: Schema Apply failed it (42601) and the index kept after it (42P01)."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    for (own_kind, own), (kind, following), separator in itertools.product(
        _OWN.items(), _FOLLOWING.items(), _SEPARATORS
    ):
        pieces = split_glued_statements(own + separator + following)
        assert pieces == [own, f"{separator.strip()}\n{following}".lstrip()], (own_kind, kind)


def test_a_statement_of_many_lines_postgresql_reads_as_one_is_never_cut() -> None:
    """G111-R3 / V1: a SELECT still without its columns went on with nothing, so a column
    named update on the next line was cut off: the view adopted was ``AS SELECT``, which
    PostgreSQL creates with no columns. Nor is any new statement head a cut where a
    statement goes on with it."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    for statement in _WHOLE:
        assert split_glued_statements(statement) == [statement], statement


def test_an_unterminated_create_and_what_follows_it_are_two() -> None:
    """V1, end to end: the ANALYZE, psql \\d, COMMIT or SET after a CREATE without its ';'
    was adopted as part of the CREATE, the notice said nothing, and Full Load's applied
    DDL carried it."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, build_table_preview
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    table = TableDef(
        name="shop.orders", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"], indexes=[IndexDef(name="ix_id", columns=["id"], unique=False)],
    )
    result = _converted(table)
    deterministic = build_table_preview(table, result.tables[0]).target_ddl
    create = 'CREATE TABLE "shop"."orders" (\n  "id" BIGINT NOT NULL,\n  PRIMARY KEY ("id")\n)'
    for block, not_taken in (
        (f'{create}\n\n-- refresh statistics\nANALYZE "shop"."orders";', "ANALYZE"),
        (f'{create}\n\n-- verify\n\\d "shop"."orders"', "\\d"),
        (f"BEGIN;\n{create}\n\nCOMMIT;", "BEGIN and COMMIT"),
        (f"{create}\n\nSET search_path = shop;", "SET"),
    ):
        adoption = adopt_ddl_from_reply(_sql(block), "shop.orders", deterministic)
        assert adoption.notice == (
            "Set as shop.orders's target DDL, keeping index ix_id from its current DDL — review "
            f"it and Apply. Not taken from the reply: {not_taken}."
        )
        unit = _unit(result, {"shop.orders": adoption.script}, "shop.orders")
        assert unit.ddls[1] == create
        applied = applied_table_conversions(result, {"shop.orders": adoption.script})
        assert applied["shop.orders"].target_ddl == create


def test_a_view_over_a_column_named_update_is_adopted_whole() -> None:
    """G111-R3: the reply's view was adopted as 'CREATE VIEW ... AS SELECT', a view of no
    columns that Schema Apply reported CREATED, and the notice listed 'UPDATE,'."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, build_view_preview

    events = TableDef(
        name="shop.events",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="update", mysql_type="datetime", nullable=True)],
        primary_key=["id"],
    )
    view = ViewDef(
        name="shop.v_events",
        definition="select `e`.`update` AS `update`,`e`.`id` AS `id` from `shop`.`events` `e`",
    )
    result = _converted(events, views=[view])
    deterministic = build_view_preview(view, result.views[0]).target_ddl
    body = 'CREATE VIEW "shop"."v_events" AS SELECT\n    update,\n    id\nFROM "shop"."events"'
    adoption = adopt_ddl_from_reply(_sql(body + ";"), "shop.v_events", deterministic)
    assert adoption.script == body + ";"
    assert adoption.notice == "Set as shop.v_events's target DDL — review it and Apply."


def test_a_restatement_that_spells_out_a_default_is_what_it_restates() -> None:
    """V2: the definition was the exact token sequence, so a follow-up restating the unnamed
    index with USING btree, or the generated foreign key unnamed with ON DELETE NO ACTION,
    matched nothing and was added beside it: Schema Apply built the index twice and the FK
    pass added two constraints on one column. A changed definition is still another."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, split_sql_statements
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    result, table, script = _orders_script()
    first = adopt_ddl_from_reply(
        _sql(table.target_ddl + ";", 'CREATE INDEX ASYNC ON "shop"."orders" ("status");'),
        "shop.orders", script,
    ).script
    for index in (
        'CREATE INDEX ASYNC ON "shop"."orders" USING btree ("status");',
        'CREATE INDEX ASYNC ON "shop"."orders" ("status" ASC NULLS LAST);',
        "create index async on shop.orders using BTREE (status asc);",
    ):
        reply = _sql(table.target_ddl + ";", index)
        assert adopt_ddl_from_reply(reply, "shop.orders", script, first).script == first, index
    for index in (
        'CREATE INDEX ASYNC ON "shop"."orders" ("status" DESC);',
        'CREATE INDEX ASYNC ON "shop"."orders" ("status" NULLS FIRST);',
        'CREATE INDEX ASYNC ON "shop"."orders" ("status") WHERE "status" IS NOT NULL;',
        'CREATE INDEX ASYNC ON "shop"."orders" USING hash ("status");',
    ):
        reply = _sql(table.target_ddl + ";", index)
        statements = split_sql_statements(adopt_ddl_from_reply(reply, "shop.orders", script, first).script)
        assert index.rstrip(";") in statements and len(statements) == 6, index

    key = 'ALTER TABLE "shop"."orders" ADD FOREIGN KEY ("customer_id") REFERENCES "shop"."customers" ("id")'
    for spelled, same in (
        ("ON DELETE NO ACTION", True),
        ("MATCH SIMPLE ON UPDATE NO ACTION NOT DEFERRABLE INITIALLY IMMEDIATE", True),
        # The converter writes MySQL's RESTRICT as NO ACTION.
        ("ON DELETE RESTRICT", True),
        ("ON DELETE CASCADE", False),
        ("DEFERRABLE INITIALLY IMMEDIATE", False),
    ):
        reply = _sql(table.target_ddl + ";", f"{key} {spelled} NOT VALID;")
        adoption = adopt_ddl_from_reply(reply, "shop.orders", script)
        applied = applied_table_conversions(result, {"shop.orders": adoption.script})["shop.orders"]
        assert (applied.foreign_key_ddls == table.foreign_key_ddls) is same, spelled


def test_an_index_or_foreign_key_of_one_definition_under_another_name_is_kept() -> None:
    """G111-R2: MySQL keeps two indexes or foreign keys of one definition, and the converter
    emits both. A reply restating one unnamed matched both by definition: the first stood
    and the second was dropped from the script without a word -- Schema Apply no longer
    created it, and the FK pass stopped owning it."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    orders = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="customer_id", mysql_type="int", nullable=False)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_customer", columns=["customer_id"], unique=False),
                 IndexDef(name="ix_customer_dup", columns=["customer_id"], unique=False)],
        foreign_keys=[
            ForeignKeyDef(name=name, columns=["customer_id"], referenced_table="shop.customers",
                          referenced_columns=["id"])
            for name in ("fk_a", "fk_b")
        ],
    )
    result = _converted(_customers(), orders)
    table = next(t for t in result.tables if t.table == "shop.orders")
    script = render_target_ddl(table)
    assert len(table.index_ddls) == 2 and len(table.foreign_key_ddls) == 2
    for restated, said in (
        ('CREATE INDEX ASYNC ON "shop"."orders" ("customer_id");', "index ix_customer_dup"),
        ('CREATE INDEX ASYNC "ix_orders_customer_id" ON "shop"."orders" ("customer_id");',
         "index ix_customer, index ix_customer_dup"),
        ('ALTER TABLE "shop"."orders" ADD FOREIGN KEY ("customer_id") REFERENCES '
         '"shop"."customers" ("id") NOT VALID;', "foreign key fk_b"),
    ):
        reply = _sql(table.target_ddl + ";", restated)
        adoption = adopt_ddl_from_reply(reply, "shop.orders", script)
        applied = applied_table_conversions(result, {"shop.orders": adoption.script})["shop.orders"]
        assert applied.index_ddls == table.index_ddls, restated
        assert applied.foreign_key_ddls == table.foreign_key_ddls, restated
        assert said in adoption.notice, adoption.notice
        again = adopt_ddl_from_reply(reply, "shop.orders", script, adoption.script)
        assert again.script == adoption.script


def test_an_edit_that_ensures_another_schema_still_gets_its_own() -> None:
    """V3: the generated schema ensure went first only for an edit with no CREATE SCHEMA at
    all, so a view edit that ensures its helper's schema, or "Reporting" for reporting,
    failed at Schema Apply: schema "reporting" does not exist."""
    view = ViewDef(name="reporting.v_orders", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")
    result = _converted(_table("shop.orders"), views=[view])
    create = 'CREATE VIEW "reporting"."v_orders" AS SELECT "o"."id" AS "id" FROM "shop"."orders" AS "o"'
    helper = (
        'CREATE SCHEMA IF NOT EXISTS "staging";\n'
        'CREATE VIEW "staging"."base" AS SELECT "o"."id" FROM "shop"."orders" AS "o";\n'
        'CREATE VIEW "reporting"."v_orders" AS SELECT "b"."id" AS "id" FROM "staging"."base" AS "b";'
    )
    ddls = _unit(result, {"reporting.v_orders": helper}, "reporting.v_orders").ddls
    assert ddls[:2] == ('CREATE SCHEMA IF NOT EXISTS "reporting"', 'CREATE SCHEMA IF NOT EXISTS "staging"')
    other_case = f'CREATE SCHEMA IF NOT EXISTS "Reporting";\n{create};'
    assert _unit(result, {"reporting.v_orders": other_case}, "reporting.v_orders").ddls == (
        'CREATE SCHEMA IF NOT EXISTS "reporting"', 'CREATE SCHEMA IF NOT EXISTS "Reporting"', create,
    )
    # Its own schema's ensure, however spelled, is enough.
    own = f"CREATE SCHEMA Reporting;\n{create};"
    assert _unit(result, {"reporting.v_orders": own}, "reporting.v_orders").ddls == (
        "CREATE SCHEMA Reporting", create,
    )


def test_a_bare_name_both_readings_agree_on_names_the_object() -> None:
    """G111-R1: a bare name part was ASCII only, so shop.café, shop.order_주문 or shop.straße
    -- one name to PostgreSQL and the core applier alike -- named nothing: a reply's bare
    CREATE got no button, a reply merged over a hand-typed bare CREATE kept the old one after
    it (REPLACE then recreated the old table), and a view edit's own CREATE VIEW was not
    found (the reload's pre-drop missed it and DROP TABLE failed, 2BP01)."""
    from dsql_migrator.ui.schema_conversion import (
        adopt_ddl_from_reply,
        render_target_ddl,
        split_sql_statements,
    )
    from dsql_migrator.ui.schema_conversion_apply import (
        applied_table_conversions,
        applied_view_ddls,
        object_statement_role,
    )

    for name in ("shop.café", "shop.order_주문", "shop.tbl_注文", "shop.straße"):
        result = _converted(_table(name))
        script = render_target_ddl(next(t for t in result.tables if t.table == name))
        adoption = adopt_ddl_from_reply(_sql(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY);"), name, script)
        assert adoption.script is not None, name
    # Read apart -- PostgreSQL's "ordersÉ", the applier's "ordersé" -- still nothing.
    assert object_statement_role("CREATE TABLE shop.ordersÉ (id int)", "shop.ordersÉ") is None

    name = "shop.café"
    result = _converted(_table(name))
    script = render_target_ddl(result.tables[0])
    stored = 'CREATE SCHEMA IF NOT EXISTS "shop";\n\nCREATE TABLE shop.café (id INT NOT NULL, PRIMARY KEY (id));'
    create = 'CREATE TABLE "shop"."café" ("id" BIGINT NOT NULL, PRIMARY KEY ("id"))'
    adoption = adopt_ddl_from_reply(_sql(create + ";"), name, script, stored)
    assert split_sql_statements(adoption.script) == ['CREATE SCHEMA IF NOT EXISTS "shop"', create]
    assert adoption.notice == "Set as shop.café's target DDL — review it and Apply."
    assert applied_table_conversions(result, {name: adoption.script})[name].target_ddl == create

    view = ViewDef(name="shop.café_v", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")
    result = _converted(_table("shop.orders"), _customers(), views=[view])
    own = "CREATE VIEW shop.café_v AS SELECT o.id FROM shop.orders AS o"
    edit = f"{own};\nCREATE VIEW shop.café_v_count AS SELECT count(*) AS n FROM shop.customers;"
    assert applied_view_ddls(result, {"shop.café_v": edit})["shop.café_v"] == own


def test_a_notice_reads_each_name_as_written() -> None:
    """V4: ALTER and DROP were labelled through the identity regex, which no longer read a
    bare non-ASCII name, so 'DROP TABLE IF EXISTS shop.ordersé' read 'DROP TABLE if' -- and
    a reply's own DROP of shop.straße was listed under that label -- while the core parser,
    tried first, read 'CREATE TABLE shop.été' as 'CREATE TABLE shop'."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import statement_label

    assert statement_label("DROP TABLE IF EXISTS shop.ordersé") == "DROP TABLE shop.ordersé"
    assert statement_label("ALTER TABLE shop.ordersé ADD COLUMN x int") == "ALTER TABLE shop.ordersé"
    assert statement_label("DROP VIEW IF EXISTS shop.주문") == "DROP VIEW shop.주문"
    assert statement_label("CREATE TABLE shop.été (id int)") == "CREATE TABLE shop.été"
    # As PostgreSQL stores it, not as the applier lower()s it.
    assert statement_label("CREATE TABLE shop.ordersÉ (id int)") == "CREATE TABLE shop.ordersÉ"
    assert statement_label("\\d shop.orders") == "\\d"
    assert statement_label("START TRANSACTION") == "START TRANSACTION"

    name = "shop.straße"
    result = _converted(_table(name))
    reply = _sql(f"DROP TABLE IF EXISTS {name};", 'CREATE TABLE "shop"."straße" ("id" BIGINT PRIMARY KEY);')
    adoption = adopt_ddl_from_reply(reply, name, render_target_ddl(result.tables[0]))
    assert adoption.notice == "Set as shop.straße's target DDL — review it and Apply."
