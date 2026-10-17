"""G105: the review of G97-G100 -- 'Use as target DDL' merges the reply's statements for its
object into the current target DDL (the table keeps its indexes and foreign keys), reads the
fences the panel renders, is offered only when the click can succeed and only for an object an
apply path creates, takes only this object's statements under its PostgreSQL-resolved name;
each note opens its own chat; a view edit reloads under its own CREATE VIEW; and WITH
RECURSIVE is read from the WITH that opens the CTE list."""

from __future__ import annotations

from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)

_V1 = 'CREATE TABLE "shop"."orders" ("id" INTEGER NOT NULL, PRIMARY KEY ("id"));'
_V2 = 'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, PRIMARY KEY ("id"));'


def _orders() -> TableDef:
    return TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="customer_id", mysql_type="int", nullable=False),
            ColumnDef(name="status", mysql_type="enum('open','closed')", nullable=True),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_customer", columns=["customer_id"], unique=False)],
        foreign_keys=[
            ForeignKeyDef(
                name="fk_customer", columns=["customer_id"],
                referenced_table="shop.customers", referenced_columns=["id"],
            )
        ],
    )


def _customers() -> TableDef:
    return TableDef(
        name="shop.customers",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"],
    )


def _converted(*tables: TableDef, views=(), source_type=SourceType.MYSQL):
    from dsql_migrator.core.converter import SchemaConverter

    return SchemaConverter(source_type=source_type).convert(
        SourceInventory(tables=list(tables), views=list(views))
    )


def _orders_script():
    from dsql_migrator.ui.schema_conversion import render_target_ddl

    result = _converted(_customers(), _orders())
    table = next(t for t in result.tables if t.table == "shop.orders")
    return result, table, render_target_ddl(table)


def _sql(*statements: str) -> str:
    return "```sql\n" + "\n".join(statements) + "\n```"


def _conversion_chat(monkeypatch, inventory: SourceInventory):
    """Render the real Schema Conversion screen (AI on, a NiceGUI double) and hand back the
    double, the session's conversion state, and an opener that returns what the per-object
    chat passes the AI panel (its footer callables)."""
    import sys
    import types

    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.models import (
        AssessmentReport,
        SourceConnectionConfig,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import schema_conversion as sc
    from dsql_migrator.ui.ai_assist import build_ai_assist_config
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore
    from tests.test_ui_data_migration import _ScreenUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    store, eval_store, conv_store = SessionStore(), EvaluationStore(), sc.SchemaConversionStore()
    session = store.get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="a.example", port=3306, database="shop"), None)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1")
    )
    session.set_ai_assist(build_ai_assist_config(enabled=True))
    eval_store.get_or_create("s1").set_result(EvaluationResult(
        inventory=inventory, assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[],
        source_label="MySQL 8.0.39 at a.example:3306/shop", target_endpoint="c.dsql.us-east-1.on.aws",
    ))
    captured: dict = {}
    real = sc._render_browser_and_preview

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(sc, "_render_browser_and_preview", _spy)
    opened: list = []
    content, _runner = sc.build_schema_conversion_screen(
        store, "s1", job_manager=JobManager(), eval_store=eval_store, conv_store=conv_store,
        open_ai_scope=lambda **kwargs: opened.append(kwargs),
    )
    content(lambda: None)

    def open_chat(object_name: str, deterministic: str) -> dict:
        captured["on_ai_chat"](object_name, "SRC", deterministic)
        return opened[-1]

    return ui, conv_store.get_or_create("s1"), open_chat


def test_the_chat_footer_merges_and_is_withheld_where_it_cannot_succeed(monkeypatch) -> None:
    """G100-1 / G100-3 / G100-4 through the real chat opener: the button is shown exactly
    when the click succeeds, the click keeps the table's index and foreign key and says so,
    and a not-converted view gets no button."""
    from dsql_migrator.ui.schema_conversion import _NOT_AUTO_CONVERTED

    view = ViewDef(name="shop.v_missing", definition="")
    ui, conv_state, open_chat = _conversion_chat(
        monkeypatch, SourceInventory(tables=[_customers(), _orders()], views=[view])
    )
    _result, table, script = _orders_script()
    chat = open_chat("shop.orders", script)
    fixed = _sql(table.target_ddl.replace('"status" TEXT', '"status" VARCHAR(6)') + ";")
    assert chat["footer_visible"](fixed) is True
    chat["footer_action"](fixed)
    stored = conv_state.get_edited_target_ddl("shop.orders")
    assert "VARCHAR(6)" in stored and '"ix_customer"' in stored and '"fk_customer"' in stored
    notice, kwargs = ui.notified[-1]
    assert kwargs.get("type") == "positive"
    assert "keeping index ix_customer and foreign key fk_customer" in notice
    # The tool's own script, schema ensure first: offered, and the click takes it.
    assert chat["footer_visible"](_sql(script)) is True
    chat["footer_action"](_sql(script))
    assert ui.notified[-1][1].get("type") == "positive"
    assert conv_state.get_edited_target_ddl("shop.orders").startswith('CREATE SCHEMA IF NOT EXISTS "shop"')
    # A SELECT-only reply: no button.
    assert chat["footer_visible"](_sql('SELECT count(*) FROM "shop"."orders";')) is False

    view_chat = open_chat("shop.v_missing", _NOT_AUTO_CONVERTED)
    view_reply = _sql('CREATE VIEW "shop"."v_missing" AS SELECT 1 AS "id";')
    assert view_chat["footer_visible"](view_reply) is False
    assert conv_state.get_edited_target_ddl("shop.v_missing") is None

    # ... and the chat is told, so its answer says to create the view by hand.
    from dsql_migrator.ui import schema_conversion as sc

    grounding: list = []

    class _Strategist:
        def __init__(self, *_a, **_k) -> None:
            pass

        def stream_chat(self, system, _messages, _on_delta):
            grounding.append(system)

    monkeypatch.setattr(sc, "AssessmentStrategist", _Strategist)
    for opened in (view_chat, chat):
        opened["streamer"]([{"role": "user", "text": "How?"}], lambda _delta: None)
    view_grounding, table_grounding = grounding
    assert "run it on the Aurora DSQL target themselves" in view_grounding
    assert "run it on the Aurora DSQL target themselves" not in table_grounding


def test_a_reply_with_only_the_create_keeps_the_tables_indexes_and_foreign_keys() -> None:
    """G100-1: the block was stored as the whole script, so a reply with the corrected CREATE
    TABLE alone (a focused answer's usual shape) dropped the secondary index and the preserved
    foreign key -- Schema Apply, the post-load index pass, the FK pass and cut over then created
    neither, with no warning."""
    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply
    from dsql_migrator.ui.schema_conversion_apply import (
        applied_table_conversions,
        build_apply_objects,
        override_apply_objects,
    )

    result, table, script = _orders_script()
    fixed = table.target_ddl.replace('"status" TEXT', '"status" VARCHAR(6)') + ";"
    adoption = adopt_ddl_from_reply(_sql(fixed), "shop.orders", script)
    assert adoption.script is not None and "VARCHAR(6)" in adoption.script
    assert "ix_customer" in adoption.notice and "fk_customer" in adoption.notice

    applied = applied_table_conversions(result, {"shop.orders": adoption.script})["shop.orders"]
    assert "VARCHAR(6)" in applied.target_ddl
    assert len(applied.index_ddls) == 1 and len(applied.foreign_key_ddls) == 1
    (unit,) = [
        o for o in override_apply_objects(
            build_apply_objects(result), {"shop.orders": adoption.script}, conversion=result
        )
        if o.object_name == "shop.orders"
    ]
    assert [d.split("(")[0].strip() for d in unit.ddls] == [
        'CREATE SCHEMA IF NOT EXISTS "shop"',
        'CREATE TABLE "shop"."orders"',
        'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders"',
    ]


def test_a_reply_statement_replaces_its_namesake_and_the_rest_of_the_edit_is_kept() -> None:
    """G100-1: an index or foreign key the reply gives replaces the one of the same name, a new
    one is added, and the current target DDL -- the stored edit when there is one -- keeps the
    rest (here the user's own extra index)."""
    from dsql_migrator.ui.schema_conversion import (
        adopt_ddl_from_reply,
        join_target_ddl_fk_sections,
        split_sql_statements,
        split_target_ddl_fk_sections,
    )

    _result, table, script = _orders_script()
    # As the editor stores an edit: the editable part, then the foreign-key section.
    editable, fk_section = split_target_ddl_fk_sections(script)
    stored = join_target_ddl_fk_sections(
        editable + '\n\nCREATE INDEX ASYNC "ix_mine" ON "shop"."orders" ("status");', fk_section
    )
    reply = _sql(
        table.target_ddl + ";",
        'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders" ("customer_id", "id");',
        'CREATE INDEX ASYNC "ix_new" ON "shop"."orders" ("id", "status");',
    )
    adoption = adopt_ddl_from_reply(reply, "shop.orders", script, stored)
    statements = split_sql_statements(adoption.script)
    indexes = [s for s in statements if s.startswith("CREATE INDEX")]
    assert indexes == [
        'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders" ("customer_id", "id")',
        'CREATE INDEX ASYNC "ix_mine" ON "shop"."orders" ("status")',
        'CREATE INDEX ASYNC "ix_new" ON "shop"."orders" ("id", "status")',
    ]
    assert statements[-1].startswith('ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_customer"')
    assert "ix_mine" in adoption.notice and "fk_customer" in adoption.notice
    assert "ix_customer" not in adoption.notice  # replaced, not kept


def test_fences_pair_as_the_panel_renders_them() -> None:
    """G100-2: only a sql-tagged or untagged opener was consumed, so after a ```mysql / ```text /
    ```diff block every fence pairing shifted: the DDL under a 'MySQL before' block got no button,
    and the version the reply says fails was adopted over the final one."""
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply

    mysql_before = (
        "Your MySQL table:\n```mysql\nCREATE TABLE `orders` (`id` int NOT NULL AUTO_INCREMENT,"
        " PRIMARY KEY (`id`)) ENGINE=InnoDB;\n```\nOn Aurora DSQL:\n" + _sql(_V2)
    )
    assert "BIGINT" in (extract_ddl_from_reply(mysql_before, "shop.orders") or "")
    superseded = (
        "First try:\n" + _sql(_V1) + "\nThat fails with:\n```text\nERROR: something\n```\n"
        "Use this instead:\n" + _sql(_V2)
    )
    assert "BIGINT" in (extract_ddl_from_reply(superseded, "shop.orders") or "")
    diff_first = "The change:\n```diff\n- \"id\" INT\n+ \"id\" BIGINT\n```\nFull DDL:\n" + _sql(_V2)
    assert "BIGINT" in (extract_ddl_from_reply(diff_first, "shop.orders") or "")
    assert extract_ddl_from_reply("```psql\n" + _V2 + "\n```", "shop.orders") is not None
    # A MySQL block shown for reference is never the target DDL.
    assert extract_ddl_from_reply("```mysql\n" + _V2 + "\n```", "shop.orders") is None
    # A fence in a list item is read without the item's indent.
    listed = '1. Create it:\n   ```sql\n   CREATE TABLE "shop"."orders" (\n     "id" BIGINT\n   );\n   ```\n'
    assert extract_ddl_from_reply(listed, "shop.orders") == 'CREATE TABLE "shop"."orders" (\n  "id" BIGINT\n);'


def test_the_tools_own_script_is_adoptable_and_the_button_never_refuses() -> None:
    """G100-3: the chat is grounded on the tool's script, which starts with CREATE SCHEMA IF NOT
    EXISTS -- a reply returning it fixed showed the button, and the click refused the schema
    ensure as a forbidden statement (a DROP ... IF EXISTS of the object too)."""
    from dsql_migrator.core.ai_assistant import validate_suggested_sql
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply

    _result, _table, script = _orders_script()
    fixed = script.replace('"status" TEXT', '"status" VARCHAR(6)')
    taken = extract_ddl_from_reply(_sql(fixed), "shop.orders")
    assert taken is not None and validate_suggested_sql(taken).is_safe

    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply

    assert adopt_ddl_from_reply(_sql(fixed), "shop.orders", script).script is not None
    drop_first = _sql('DROP TABLE IF EXISTS "shop"."orders";', _V2)
    adoption = adopt_ddl_from_reply(drop_first, "shop.orders", script)
    assert adoption.script is not None and "DROP" not in adoption.script
    assert "Not taken" not in adoption.notice  # the object's own DROP is the REPLACE's


def test_no_adoption_for_an_object_no_apply_path_creates() -> None:
    """G100-4 / XS-3: for a view the conversion could not convert (definition not captured or
    not parsed, a PostgreSQL materialized view), the click stored an edit that no apply path
    runs, under 'review it and Apply'. No button now, and the chat is told to say so."""
    from dsql_migrator.ui.schema_conversion import (
        adopt_ddl_from_reply,
        build_view_preview,
        conversion_chat_note,
    )

    views = [
        ViewDef(name="shop.v_missing", definition=""),
        ViewDef(name="shop.v_bad", definition="this is not sql((("),
        ViewDef(name="shop.v_ok", definition="select `o`.`id` AS `id` from `shop`.`customers` `o`"),
    ]
    result = _converted(_customers(), views=views)
    reply = '```sql\nCREATE VIEW "shop"."{}" AS SELECT 1 AS "id";\n```'
    for view in views:
        conversion = next(v for v in result.views if v.view == view.name)
        deterministic = build_view_preview(view, conversion).target_ddl
        bare = view.name.split(".", 1)[1]
        adoption = adopt_ddl_from_reply(reply.format(bare), view.name, deterministic)
        note = conversion_chat_note(view.name, deterministic)
        if conversion.auto_converted:
            assert adoption.script is not None and note == ""
        else:
            assert adoption.script is None
            assert "Create it on the Aurora DSQL target yourself" in adoption.notice
            assert "run it on the Aurora DSQL target themselves" in note

    mv = ViewDef(name="public.mv_sales", definition="SELECT 1", unsupported_kind="materialized view")
    pg = _converted(
        TableDef(name="public.sales", columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)],
                 primary_key=["id"]),
        views=[mv], source_type=SourceType.POSTGRES,
    )
    deterministic = build_view_preview(mv, pg.views[0], source_type=SourceType.POSTGRES).target_ddl
    table_reply = '```sql\nCREATE TABLE "public"."mv_sales" ("day" date PRIMARY KEY);\n```'
    assert adopt_ddl_from_reply(table_reply, "public.mv_sales", deterministic).script is None


def test_a_name_collision_placeholder_gets_no_button_and_says_why() -> None:
    from dsql_migrator.core.converter import is_name_collision_placeholder
    from dsql_migrator.ui.schema_conversion import (
        adopt_ddl_from_reply,
        conversion_chat_note,
        render_target_ddl,
    )

    def table(name: str) -> TableDef:
        return TableDef(name=name, columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                        primary_key=["id"])

    a, b = "app." + "t" * 63 + "_a", "app." + "t" * 63 + "_b"
    placeholder = next(t for t in _converted(table(a), table(b)).tables if t.table == a)
    assert is_name_collision_placeholder(placeholder.target_ddl)
    reply = f'```sql\nCREATE TABLE "app"."{"t" * 63}_a" ("id" integer PRIMARY KEY);\n```'
    adoption = adopt_ddl_from_reply(reply, a, render_target_ddl(placeholder))
    assert adoption.script is None and "Shorten the names on the source" in adoption.notice
    assert "shorten the names" in conversion_chat_note(a, render_target_ddl(placeholder))


def test_only_the_objects_own_statements_are_taken_from_a_block() -> None:
    """G100-5 / XS-5: the whole block was adopted when one statement created the object -- a
    verification SELECT then failed the object at Schema Apply after its CREATE ran, and a side
    table's CREATE became the table's applied DDL in Full Load (the last CREATE TABLE won)."""
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply
    from dsql_migrator.ui.schema_conversion_apply import (
        applied_table_conversions,
        build_apply_objects,
        override_apply_objects,
    )

    result, _table, script = _orders_script()
    block = _sql(
        _V2,
        'CREATE INDEX ASYNC "ix_customer" ON "shop"."orders" ("customer_id");',
        "-- verify",
        'SELECT count(*) FROM "shop"."orders";',
        'CREATE TABLE "shop"."order_status" ("code" TEXT NOT NULL, PRIMARY KEY ("code"));',
        'CREATE INDEX ASYNC "ix_code" ON "shop"."order_status" ("code");',
    )
    taken = extract_ddl_from_reply(block, "shop.orders") or ""
    assert "SELECT" not in taken and "order_status" not in taken and "ix_customer" in taken

    from dsql_migrator.ui.schema_conversion import adopt_ddl_from_reply

    adoption = adopt_ddl_from_reply(block, "shop.orders", script)
    assert "SELECT" in adoption.notice and "CREATE TABLE shop.order_status" in adoption.notice
    edits = {"shop.orders": adoption.script}
    (unit,) = [
        o for o in override_apply_objects(build_apply_objects(result), edits, conversion=result)
        if o.object_name == "shop.orders"
    ]
    assert not any("SELECT" in d or "order_status" in d for d in unit.ddls)
    applied = applied_table_conversions(result, edits)["shop.orders"]
    assert applied.target_ddl.startswith('CREATE TABLE "shop"."orders"')


def test_the_name_matches_as_postgresql_resolves_it() -> None:
    """G100-6: the match dropped quotes, case and the schema, so a schema-less CREATE TABLE
    "orders" was adopted for shop.orders: Schema Apply created public.orders, reported
    shop.orders CREATED, and the load wrote to "shop"."orders"."""
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply

    body = ' ("id" BIGINT NOT NULL, PRIMARY KEY ("id"));'
    found = lambda name, obj: extract_ddl_from_reply(_sql(f"CREATE TABLE {name}{body}"), obj)  # noqa: E731
    assert found('"orders"', "shop.orders") is None
    assert found("orders", "shop.orders") is None
    assert found("shop.Orders", "shop.orders") is not None  # unquoted folds to orders
    assert found("shop.Orders", "shop.Orders") is None  # ... so it is not "Orders"
    assert found('"shop"."Orders"', "shop.Orders") is not None
    assert found("orders", "public.orders") is not None  # a bare name is public's
    # Read by Schema Apply as the table "shop": never taken for shop.orders.
    assert found('"shop" . "orders"', "shop.orders") is None
    assert found("db.shop.orders", "shop.orders") is None


def test_a_quoted_name_keeps_its_spaces() -> None:
    """G100-7: whitespace was stripped inside quoted names, so northwind."Order Details" never
    matched its object and got no button."""
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply

    body = ' ("OrderID" BIGINT NOT NULL, PRIMARY KEY ("OrderID"));'
    obj = "northwind.Order Details"
    assert extract_ddl_from_reply(_sql('CREATE TABLE "northwind"."Order Details"' + body), obj)
    assert extract_ddl_from_reply(_sql('CREATE TABLE "northwind"."OrderDetails"' + body), obj) is None


def test_two_notes_on_one_column_open_their_own_chats() -> None:
    """G100-8: a column note's scope was kind:column, so a SET column with a dropped collation
    had two notes in one scope -- the second icon re-focused the first note's chat."""
    from dsql_migrator.core.converter import ConversionWarning
    from dsql_migrator.core.models import Classification
    from dsql_migrator.ui.schema_conversion import _note_scope_key

    notes = [
        ConversionWarning(object_name="shop.t", classification=Classification.MANUAL,
                          column_name="flags", message=text)
        for text in (
            "MySQL collation 'utf8mb4_bin' is dropped; Aurora DSQL uses the default collation.",
            "SET has no lossless mapping in Aurora DSQL; mapped to text.",
        )
    ]
    keys = {_note_scope_key(n) for n in notes}
    assert len(keys) == 2 and all(":flags:" in key for key in keys)


def test_a_view_edit_reloads_under_the_create_view_of_that_view() -> None:
    """G100-9: the FIRST CREATE VIEW of an edit was taken, so an edit that creates a helper view
    first reloaded the helper under the view's name and never re-created the view."""
    from dsql_migrator.ui.schema_conversion_apply import applied_view_ddls

    view = ViewDef(name="shop.v_orders", definition="select `o`.`id` AS `id` from `shop`.`customers` `o`")
    result = _converted(_customers(), views=[view])
    edit = (
        'CREATE SCHEMA IF NOT EXISTS "shop";\n'
        'CREATE VIEW "shop"."v_orders_base" AS SELECT "o"."id" FROM "shop"."customers" AS "o";\n'
        'CREATE VIEW "shop"."v_orders" AS SELECT "b"."id" FROM "shop"."v_orders_base" AS "b";'
    )
    ddl = applied_view_ddls(result, {"shop.v_orders": edit})["shop.v_orders"]
    assert ddl.startswith('CREATE VIEW "shop"."v_orders" AS')


def test_recursive_is_read_from_the_with_that_opens_the_cte_list() -> None:
    """G100-10: WITH RECURSIVE was searched in the raw text from the level start, earlier
    siblings' bodies included: a nested WITH RECURSIVE made a later non-recursive CTE cover its
    own body, and the real read of the table there was dropped (a reload of public.orders then
    did not pre-drop the view, whose DROP blocked the table's)."""
    from dsql_migrator.core.column_exclusion import views_referencing
    from dsql_migrator.core.converter import _scan_sql_names

    body = (
        "WITH nums AS (WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) "
        "SELECT n FROM r), orders AS (SELECT * FROM orders WHERE status = 'open') "
        "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id "
        "JOIN nums ON nums.n = o.id"
    )
    assert _scan_sql_names(body).relations == [(None, "orders"), (None, "customers")]
    ddls = {"public.v_open": 'CREATE VIEW "public"."v_open" AS ' + body}
    assert views_referencing(ddls, frozenset({"public.orders"})) == [ddls["public.v_open"]]


def test_a_derived_table_that_starts_with_with_reads_its_ctes_as_its_own() -> None:
    """G100-10: in a derived table the WITH was read as WITH ORDINALITY, so the CTE's name was
    taken for a read of a relation (and, after G100, no longer covered by the CTE's scope), and
    a WITH RECURSIVE there read a relation named 'recursive'."""
    from dsql_migrator.core.converter import _scan_sql_names

    plain = "SELECT * FROM (WITH x AS (SELECT * FROM t) SELECT * FROM x) d"
    assert _scan_sql_names(plain).relations == [(None, "t")]
    shadow = "SELECT * FROM (WITH orders AS (SELECT * FROM orders) SELECT * FROM orders) d"
    assert _scan_sql_names(shadow).relations == [(None, "orders")]
    recursive = (
        "SELECT * FROM (WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r "
        "WHERE n < 3) SELECT n FROM r) d"
    )
    assert _scan_sql_names(recursive).relations == []
