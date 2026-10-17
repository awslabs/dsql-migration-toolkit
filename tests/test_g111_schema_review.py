"""G111: the review of G101-G105 -- a view edit reloads with every view it creates; a reply
statement that lost its ';' is not taken with the next one; a bare name with a non-ASCII
letter names nothing; adopting a reply twice changes nothing; an edit without its schema
ensure is applied after the generated one; and the notice names what it did not take."""

from __future__ import annotations

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef

from tests.test_g105_schema_review import (
    _conversion_chat,
    _converted,
    _customers,
    _orders,
    _orders_script,
    _sql,
)


def _table(name: str) -> TableDef:
    return TableDef(
        name=name, columns=[ColumnDef(name="id", mysql_type="int", nullable=False)], primary_key=["id"]
    )


def _unit(result, edits: dict, name: str):
    from dsql_migrator.ui.schema_conversion_apply import build_apply_objects, override_apply_objects

    (unit,) = [
        o for o in override_apply_objects(build_apply_objects(result), edits, conversion=result)
        if o.object_name == name
    ]
    return unit


def test_a_view_edit_reloads_with_the_helper_view_it_creates() -> None:
    """P1 (G100-9): only the view's own CREATE VIEW reached the reload, so for a view over a
    helper over the reloaded table neither was selected -- the view reads no reloaded table
    itself and the helper was in no map -- and the reload's DROP TABLE was refused (2BP01)."""
    from dsql_migrator.core.column_exclusion import views_referencing
    from dsql_migrator.ui.schema_conversion_apply import applied_view_ddls

    view = ViewDef(name="shop.v_orders", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")
    result = _converted(_table("shop.orders"), _customers(), views=[view])
    helper = 'CREATE VIEW "shop"."v_orders_base" AS SELECT "o"."id" FROM "shop"."orders" AS "o"'
    own = 'CREATE VIEW "shop"."v_orders" AS SELECT "b"."id" FROM "shop"."v_orders_base" AS "b"'
    edit = f'CREATE SCHEMA IF NOT EXISTS "shop";\n-- the helper\n{helper};\n{own};'
    ddls = applied_view_ddls(result, {"shop.v_orders": edit})
    assert ddls["shop.v_orders"] == own and ddls["shop.v_orders_base"] == helper
    # Selected both, each after what it reads: dropped view first, re-created helper first.
    tables = ["shop.orders", "shop.customers"]
    assert views_referencing(ddls, frozenset({"shop.orders"}), known_tables=tables) == [helper, own]

    # A source view of the helper's name keeps its own DDL.
    other = ViewDef(name="shop.v_orders_base", definition="select `c`.`id` AS `id` from `shop`.`customers` `c`")
    result = _converted(_table("shop.orders"), _customers(), views=[view, other])
    ddls = applied_view_ddls(result, {"shop.v_orders": edit})
    assert '"shop"."customers"' in ddls["shop.v_orders_base"]


def test_a_statement_that_lost_its_semicolon_is_not_taken_with_the_next() -> None:
    """P2 / P3 (G100-5, XS-5): the block was split only at ';', so an unterminated CREATE and
    the verification SELECT or the side table's CREATE after it became one statement, taken
    as the object's CREATE -- Schema Apply failed it (42601) and Full Load's applied DDL
    carried the SELECT. The notice did not mention the SELECT at all."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, split_sql_statements
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    result, _table_conversion, script = _orders_script()
    create = 'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, PRIMARY KEY ("id"))'
    verify = _sql(create, "", "-- verify", 'SELECT count(*) FROM "shop"."orders";')
    adoption = adopt_ddl_from_reply(verify, "shop.orders", script)
    assert "SELECT" not in adoption.script
    assert "Not taken from the reply: SELECT." in adoption.notice
    unit = _unit(result, {"shop.orders": adoption.script}, "shop.orders")
    assert create in unit.ddls and not any("SELECT" in ddl for ddl in unit.ddls)
    applied = applied_table_conversions(result, {"shop.orders": adoption.script})["shop.orders"]
    assert applied.target_ddl == create

    side = _sql(create, "", 'CREATE TABLE "shop"."order_status" ("code" TEXT PRIMARY KEY);')
    adoption = adopt_ddl_from_reply(side, "shop.orders", script)
    assert "order_status" not in adoption.script
    assert "Not taken from the reply: CREATE TABLE shop.order_status." in adoption.notice

    # Its own index after it is taken as an index of its own.
    index = 'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders" ("customer_id", "id")'
    adoption = adopt_ddl_from_reply(_sql(create, index + ";"), "shop.orders", script)
    statements = split_sql_statements(adoption.script)
    assert create in statements and index in statements

    # A view's query on the lines after AS stays the view's; the SELECT after it does not.
    view = ViewDef(name="shop.v_orders", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")
    from dsql_migrator.ui.schema_conversion import build_view_preview

    views = _converted(_orders(), _customers(), views=[view])
    deterministic = build_view_preview(view, views.views[0]).target_ddl
    body = (
        'CREATE VIEW "shop"."v_orders" AS\nWITH "open" AS (\n  SELECT "id" FROM "shop"."orders"\n)\n'
        'SELECT "id" FROM "open"\nUNION ALL\nSELECT "id" FROM "shop"."orders"'
    )
    adoption = adopt_ddl_from_reply(
        _sql(body, "", 'SELECT * FROM "shop"."v_orders";'), "shop.v_orders", deterministic
    )
    assert split_sql_statements(adoption.script) == [body]
    assert "Not taken from the reply: SELECT." in adoption.notice


def test_one_statement_on_many_lines_is_never_cut() -> None:
    """P2: a line is a cut only where it starts a statement of its own -- never inside a
    view's query (after AS, a set operator, a CTE list), a CREATE TABLE's WITH (...), an ALTER
    TABLE's actions (COLUMN left out, a column named type), an INSERT's query, a rule's DO."""
    from dsql_migrator.ui.schema_conversion_apply import split_glued_statements

    whole = [
        "CREATE VIEW v AS\nSELECT a FROM t\nUNION ALL\nSELECT b FROM u",
        "CREATE VIEW v AS\nWITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r)\nSELECT n FROM r",
        "CREATE VIEW v AS\nWITH a AS (SELECT 1),\n  b AS (SELECT 2)\nSELECT * FROM a, b",
        "CREATE VIEW v AS SELECT * FROM t\nWITH LOCAL CHECK OPTION",
        "CREATE TABLE t (a int)\nWITH (fillfactor = 70)",
        "CREATE INDEX ASYNC ix ON t (status)\nWHERE status = 'open'",
        'ALTER TABLE "shop"."orders"\n  ALTER type SET NOT NULL',
        "ALTER TABLE t ADD COLUMN a int,\n  DROP CONSTRAINT c",
        "ALTER TABLE t ADD FOREIGN KEY (a) REFERENCES u (id) ON DELETE CASCADE ON\n  UPDATE CASCADE NOT VALID",
        "INSERT INTO t (a, b)\nSELECT a, b FROM u",
        "EXPLAIN (ANALYZE, BUFFERS)\nSELECT * FROM t",
        "CREATE RULE r AS ON INSERT TO t DO INSTEAD\nINSERT INTO u VALUES (NEW.a)",
        "CREATE TRIGGER trg BEFORE INSERT OR\nUPDATE ON t FOR EACH ROW EXECUTE FUNCTION f()",
        "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$\nSELECT 1\n$$",
        "COMMENT ON TABLE t IS 'one\nSELECT in a literal'",
    ]
    for statement in whole:
        assert split_glued_statements(statement) == [statement], statement
    assert split_glued_statements(
        "CREATE VIEW v AS SELECT a FROM t WHERE s = 'x'\n/* then */\nSELECT * FROM v"
    ) == ["CREATE VIEW v AS SELECT a FROM t WHERE s = 'x'", "/* then */\nSELECT * FROM v"]


def test_a_bare_name_with_a_non_ascii_letter_names_nothing() -> None:
    """P4: PostgreSQL folds only A-Z, the core applier lower()s every letter. A bare
    shop.ordersÉ was adopted for shop.ordersÉ, and Schema Apply's REPLACE then dropped
    "shop"."ordersé" -- another table -- before creating it."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import object_statement_role

    name = "shop.ordersÉ"
    result = _converted(_table(name), _table("shop.ordersé"))
    script = render_target_ddl(next(t for t in result.tables if t.table == name))
    bare = adopt_ddl_from_reply(_sql("CREATE TABLE shop.ordersÉ (id BIGINT PRIMARY KEY);"), name, script)
    assert bare.script is None
    quoted = adopt_ddl_from_reply(_sql('CREATE TABLE "shop"."ordersÉ" (id BIGINT PRIMARY KEY);'), name, script)
    assert quoted.script is not None and '"ordersÉ"' in quoted.script
    assert object_statement_role('CREATE INDEX ASYNC ix ON shop.ordersÉ (id)', name) is None
    assert object_statement_role('CREATE INDEX ASYNC ixÉ ON "shop"."ordersÉ" (id)', name) is None


def test_adopting_the_same_reply_twice_changes_nothing(monkeypatch) -> None:
    """G105-R1: an unnamed index or foreign key matched nothing in the current DDL, so each
    click (or a follow-up reply repeating the script) added another copy -- Schema Apply built
    the index twice and the FK pass added the constraint twice. An unnamed restatement of the
    table's own foreign key was added beside it; the named one stands."""
    from dsql_migrator.ui.schema_conversion import split_sql_statements
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    _ui, conv_state, open_chat = _conversion_chat(
        monkeypatch, SourceInventory(tables=[_customers(), _orders()])
    )
    result, table, script = _orders_script()
    chat = open_chat("shop.orders", script)
    reply = _sql(
        table.target_ddl + ";",
        'CREATE INDEX ASYNC ON "shop"."orders" ("status");',
        'ALTER TABLE "shop"."orders" ADD FOREIGN KEY ("customer_id") REFERENCES "shop"."customers" ("id") NOT VALID;',
    )
    chat["footer_action"](reply)
    first = conv_state.get_edited_target_ddl("shop.orders")
    chat["footer_action"](reply)
    assert conv_state.get_edited_target_ddl("shop.orders") == first
    applied = applied_table_conversions(result, {"shop.orders": first})["shop.orders"]
    assert applied.index_ddls == [
        'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders" ("customer_id")',
        'CREATE INDEX ASYNC ON "shop"."orders" ("status")',
    ]
    assert len(applied.foreign_key_ddls) == 1 and '"fk_customer"' in applied.foreign_key_ddls[0]

    # A later reply that names the unnamed index replaces it.
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply

    named = _sql(table.target_ddl + ";", 'CREATE INDEX ASYNC "ix_status" ON "shop"."orders" ("status");')
    statements = split_sql_statements(adopt_ddl_from_reply(named, "shop.orders", script, first).script)
    assert [s for s in statements if '("status")' in s] == [
        'CREATE INDEX ASYNC "ix_status" ON "shop"."orders" ("status")'
    ]


def test_an_unquoted_restatement_of_a_mixed_case_name_is_not_added_beside_it() -> None:
    """G105-R1: the reply restated the generated "IX_Orders_CustomerId" and its foreign key
    unquoted, which PostgreSQL folds to other names, so the merge kept the quoted ones and
    added the folded ones: two indexes and two constraints on one column. The generated ones
    stand (the constraint's name pairs it with its metadata), and the notice says so."""
    from dsql_migrator.core.models import ForeignKeyDef, IndexDef
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import applied_table_conversions

    customers = TableDef(
        name="shop.Customers", columns=[ColumnDef(name="Id", mysql_type="int", nullable=False)],
        primary_key=["Id"],
    )
    orders = TableDef(
        name="shop.Orders",
        columns=[ColumnDef(name="Id", mysql_type="int", nullable=False),
                 ColumnDef(name="CustomerId", mysql_type="int", nullable=False)],
        primary_key=["Id"],
        indexes=[IndexDef(name="IX_Orders_CustomerId", columns=["CustomerId"], unique=False)],
        foreign_keys=[ForeignKeyDef(
            name="FK_Orders_Customers_CustomerId", columns=["CustomerId"],
            referenced_table="shop.Customers", referenced_columns=["Id"],
        )],
    )
    result = _converted(customers, orders)
    table = next(t for t in result.tables if t.table == "shop.Orders")
    reply = _sql(
        table.target_ddl + ";",
        'CREATE INDEX ASYNC IX_Orders_CustomerId ON "shop"."Orders" ("CustomerId");',
        'ALTER TABLE "shop"."Orders" ADD CONSTRAINT FK_Orders_Customers_CustomerId FOREIGN KEY '
        '("CustomerId") REFERENCES "shop"."Customers" ("Id") NOT VALID;',
    )
    adoption = adopt_ddl_from_reply(reply, "shop.Orders", render_target_ddl(table))
    applied = applied_table_conversions(result, {"shop.Orders": adoption.script})["shop.Orders"]
    assert applied.index_ddls == table.index_ddls
    assert applied.foreign_key_ddls == table.foreign_key_ddls
    assert (
        "keeping index IX_Orders_CustomerId and foreign key FK_Orders_Customers_CustomerId "
        "from its current DDL"
    ) in adoption.notice
    # The same reply again: nothing changes.
    again = adopt_ddl_from_reply(reply, "shop.Orders", render_target_ddl(table), adoption.script)
    assert again.script == adoption.script


def test_an_edit_without_its_schema_ensure_is_applied_after_the_generated_one() -> None:
    """G105-R2: a view edit holds only its CREATE VIEW -- the reply's CREATE SCHEMA IF NOT
    EXISTS is not taken, and a hand-typed edit rarely has one -- and Schema Apply ran the edit
    alone, so a view in a schema with no tables failed: schema "reporting" does not exist."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply, build_view_preview

    view = ViewDef(name="reporting.v_orders", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")
    result = _converted(_table("shop.orders"), views=[view])
    deterministic = build_view_preview(view, result.views[0]).target_ddl
    create = 'CREATE VIEW "reporting"."v_orders" AS SELECT "o"."id" AS "id" FROM "shop"."orders" AS "o"'
    reply = _sql('CREATE SCHEMA IF NOT EXISTS "reporting";', "", create + ";")
    adoption = adopt_ddl_from_reply(reply, "reporting.v_orders", deterministic)
    assert adoption.script == create + ";"
    for edit in (adoption.script, create):
        assert _unit(result, {"reporting.v_orders": edit}, "reporting.v_orders").ddls == (
            'CREATE SCHEMA IF NOT EXISTS "reporting"', create,
        )
    # An edit with its own schema ensure is applied as written.
    own = f"CREATE SCHEMA IF NOT EXISTS reporting;\n{create};"
    assert _unit(result, {"reporting.v_orders": own}, "reporting.v_orders").ddls == (
        "CREATE SCHEMA IF NOT EXISTS reporting", create,
    )


def test_the_notice_names_what_it_did_not_take() -> None:
    """G105-R3: the label was the first two words when the core parser rejected the kind, so
    a CREATE OR REPLACE FUNCTION and TRIGGER both read 'CREATE OR' (one label after dedup),
    and an unnamed index on another table read 'CREATE INDEX on'."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply
    from dsql_migrator.ui.schema_conversion_apply import statement_label

    _result, table, script = _orders_script()
    reply = _sql(
        table.target_ddl + ";",
        "CREATE OR REPLACE FUNCTION shop.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$",
        "BEGIN NEW.updated_at := now(); RETURN NEW; END $$;",
        "CREATE OR REPLACE TRIGGER trg_touch BEFORE UPDATE ON shop.orders",
        "  FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();",
        'CREATE INDEX ASYNC ON "shop"."customers" ("id");',
    )
    notice = adopt_ddl_from_reply(reply, "shop.orders", script).notice
    assert notice.endswith(
        "Not taken from the reply: CREATE FUNCTION shop.touch_updated_at, CREATE TRIGGER "
        "trg_touch and CREATE INDEX ON shop.customers."
    )
    assert statement_label("CREATE TEMP TABLE scratch (a int)") == "CREATE TABLE scratch"
    assert statement_label("CREATE UNIQUE INDEX ASYNC ix ON t (a)") == "CREATE INDEX ix"
    assert statement_label("CREATE MATERIALIZED VIEW shop.mv AS SELECT 1") == (
        "CREATE MATERIALIZED VIEW shop.mv"
    )
