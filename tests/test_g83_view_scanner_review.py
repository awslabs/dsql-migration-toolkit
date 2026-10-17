"""G83: the review of G74 -- bare relation names, false cycles, quoted names in the reload
closure, and the DISTINCT ON key."""

from __future__ import annotations

import time

from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef, ViewDef


def _table(name: str, *columns: ColumnDef) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False), *columns],
        primary_key=["id"],
    )


def test_a_bare_name_that_is_a_public_table_reads_no_same_named_view() -> None:
    """api.orders (a view named like the public TABLE orders) joins public.order_totals,
    which reads the table orders bare: the false edge made api.orders apply first, and its
    CREATE failed."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion_apply import build_apply_objects

    inventory = SourceInventory(
        tables=[_table("public.orders", ColumnDef(name="customer_id", mysql_type="bigint")),
                _table("public.order_items", ColumnDef(name="order_id", mysql_type="bigint"),
                       ColumnDef(name="amount", mysql_type="numeric(12,2)"))],
        views=[
            ViewDef(name="api.orders", definition=(
                " SELECT o.id, o.customer_id, t.total FROM (orders o "
                "LEFT JOIN order_totals t ON ((t.order_id = o.id)));")),
            ViewDef(name="public.order_totals", definition=(
                " SELECT o.id AS order_id, sum(i.amount) AS total FROM (orders o "
                "JOIN order_items i ON ((i.order_id = o.id))) GROUP BY o.id;")),
        ],
    )
    result = SchemaConverter(source_type=SourceType.POSTGRES).convert(inventory)
    applied = [
        o.object_name for o in build_apply_objects(result)
        if o.object_name in ("api.orders", "public.order_totals")
    ]
    assert applied == ["public.order_totals", "api.orders"]
    units = [u.object_name for u in result.execution_units() if u.sql.lstrip().upper().startswith("CREATE VIEW")]
    assert units == ["public.order_totals", "api.orders"]


def test_a_real_chain_inside_a_false_cycle_keeps_its_order() -> None:
    from dsql_migrator.core.converter import _dependency_order_indices

    views = [
        ("public.v_orders_clean", " SELECT o.id FROM orders o WHERE (NOT o.deleted);"),
        ("reporting.orders", " SELECT s.id FROM reporting.summary_base s;"),
        ("reporting.summary_base", " SELECT c.id FROM v_orders_clean c;"),
    ]
    order = [views[i][0] for i in _dependency_order_indices(views)]
    assert order.index("public.v_orders_clean") < order.index("reporting.summary_base")
    assert order.index("reporting.summary_base") < order.index("reporting.orders")


def test_same_named_tenant_views_cost_no_quadratic_edges() -> None:
    """Every tenant_i.orders reads the shared public table bare: each got an edge to all
    the others (5,000 views: 5 s, ~2.9 GB peak, over the default task memory)."""
    from dsql_migrator.core.converter import SchemaConverter, pg_views_remodelling

    count = 3000
    tables = [_table("public.orders", ColumnDef(name="tenant_id", mysql_type="bigint"),
                     ColumnDef(name="status", mysql_type="app.st", type_kind="enum",
                               enum_labels=("a", "b")))]
    views = [
        ViewDef(name=f"tenant_{i}.orders",
                definition=f" SELECT o.id, o.status FROM orders o WHERE (o.tenant_id = {i});")
        for i in range(count)
    ]
    started = time.perf_counter()
    remodelled = pg_views_remodelling(views, tables)
    SchemaConverter(source_type=SourceType.POSTGRES).convert(
        SourceInventory(tables=tables, views=views)
    )
    assert time.perf_counter() - started < 20
    assert remodelled["tenant_7.orders"].references == (
        ("public.orders", "status", "app.st", "text"),
    )


def test_a_bare_name_binds_to_the_public_relation_even_when_nothing_in_it_is_re_typed() -> None:
    """A bare `orders` is public.orders; with nothing re-typed there it fell back to
    archive.orders (an enum) and the view was graded MANUAL for a table it does not read."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import pg_views_remodelling

    tables = [
        _table("public.orders", ColumnDef(name="status", mysql_type="text")),
        _table("archive.orders", ColumnDef(name="status", mysql_type="archive.order_status",
                                           type_kind="enum", enum_labels=("open", "closed"))),
    ]
    views = [ViewDef(name="public.v_open_orders",
                     definition=" SELECT o.id, o.status FROM orders o WHERE (o.status = 'open'::text);")]
    assert pg_views_remodelling(views, tables)["public.v_open_orders"].references == ()
    items = {i.object_name: i for i in CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=tables, views=views)).items}
    assert items["public.v_open_orders"].classification.value == "AUTO"


def test_the_distinct_on_key_is_not_part_of_the_first_output() -> None:
    from dsql_migrator.core.converter import pg_views_remodelling

    orders = _table("app.orders",
                    ColumnDef(name="status", mysql_type="app.order_status", type_kind="enum",
                              enum_labels=("new", "paid")),
                    ColumnDef(name="created_at", mysql_type="timestamp with time zone"))
    latest = ViewDef(name="app.v_latest_per_status", definition=(
        " SELECT DISTINCT ON (o.status) o.id, o.created_at FROM app.orders o "
        "ORDER BY o.status, o.created_at DESC;"))
    reader = ViewDef(name="app.v_max_id", definition=" SELECT max(l.id) AS top_id FROM app.v_latest_per_status l;")
    assert pg_views_remodelling([latest, reader], [orders])["app.v_max_id"].references == ()


def test_the_reload_closure_finds_quoted_names_with_spaces_and_hyphens() -> None:
    """`Order Details` / `order-items` were never one word of the index: their readers were
    left in place and the table DROP was refused (2BP01)."""
    from dsql_migrator.core.column_exclusion import views_referencing

    ddls = {
        "northwind.v_details": 'CREATE VIEW "northwind"."v_details" AS SELECT d."id" FROM "northwind"."Order Details" AS d',
        "shop.v_items": 'CREATE VIEW "shop"."v_items" AS SELECT i."id" FROM "shop"."order-items" AS i',
        "shop.Order Summary": 'CREATE VIEW "shop"."Order Summary" AS SELECT o."id" FROM "shop"."orders" AS o',
        "shop.v_top": 'CREATE VIEW "shop"."v_top" AS SELECT s."id" FROM "shop"."Order Summary" AS s',
    }
    assert len(views_referencing(ddls, frozenset({"northwind.Order Details"}))) == 1
    assert len(views_referencing(ddls, frozenset({"shop.order-items"}))) == 1
    chain = views_referencing(ddls, frozenset({"shop.orders"}))
    assert [d.split(" AS")[0] for d in chain] == [
        'CREATE VIEW "shop"."Order Summary"', 'CREATE VIEW "shop"."v_top"'
    ]


def test_the_reload_order_treats_the_reloaded_tables_as_tables() -> None:
    from dsql_migrator.core.column_exclusion import views_referencing

    ddls = {
        "api.orders": 'CREATE VIEW "api"."orders" AS SELECT o.id, t.total FROM (orders o LEFT JOIN order_totals t ON ((t.order_id = o.id)))',
        "public.order_totals": 'CREATE VIEW "public"."order_totals" AS SELECT o.id AS order_id FROM orders o',
    }
    create = [d.split(" AS")[0] for d in views_referencing(ddls, frozenset({"public.orders"}))]
    assert create == ['CREATE VIEW "public"."order_totals"', 'CREATE VIEW "api"."orders"']


# --- G90: the review of G79/G83 ---------------------------------------------------------


def test_a_cte_name_is_no_relation() -> None:
    """A CTE named like a table in another schema was bound to it: a false MANUAL grade for
    audit.events' enum, and (named like a public view) a false cycle that applied a view
    before the one it reads."""
    from dsql_migrator.core.converter import _dependency_order_indices, pg_views_remodelling

    tables = [_table("public.raw_events", ColumnDef(name="kind", mysql_type="text")),
              _table("audit.events", ColumnDef(name="kind", mysql_type="audit.event_kind",
                                               type_kind="enum", enum_labels=("a", "b")))]
    view = ViewDef(name="public.v_event_counts", definition=(
        " WITH events AS (SELECT r.id, r.kind FROM raw_events r) "
        "SELECT events.kind, count(*) AS n FROM events GROUP BY events.kind;"))
    assert pg_views_remodelling([view], tables)["public.v_event_counts"].references == ()
    views = [
        ("public.monthly", " SELECT s.id FROM sales_detail s;"),
        ("public.sales_detail", " WITH monthly AS (SELECT o.id FROM orders o) SELECT m.id FROM monthly m;"),
    ]
    order = [views[i][0] for i in _dependency_order_indices(views)]
    assert order == ["public.sales_detail", "public.monthly"]


def test_a_reload_takes_only_the_views_that_read_the_table() -> None:
    """Every tenant's same-named view was dropped and re-created for one tenant's reload."""
    from dsql_migrator.core.column_exclusion import views_referencing

    ddls = {
        f"tenant_{i}.v_open": f'CREATE VIEW "tenant_{i}"."v_open" AS SELECT o.id FROM tenant_{i}.orders o'
        for i in range(50)
    }
    ddls["tenant_7.v_summary"] = 'CREATE VIEW "tenant_7"."v_summary" AS SELECT v.id FROM tenant_7.v_open v'
    chosen = views_referencing(ddls, frozenset({"tenant_7.orders"}))
    assert [d.split(" AS")[0] for d in chosen] == [
        'CREATE VIEW "tenant_7"."v_open"', 'CREATE VIEW "tenant_7"."v_summary"'
    ]


def test_a_double_quote_in_a_literal_does_not_hide_a_reader() -> None:
    from dsql_migrator.core.column_exclusion import views_referencing

    ddls = {
        "northwind.v_clean": 'CREATE VIEW "northwind"."v_clean" AS SELECT d.id, '
                             "replace(d.note, '\"'::text, ''::text) AS note FROM northwind.\"Order Details\" d",
    }
    assert len(views_referencing(ddls, frozenset({"northwind.Order Details"}))) == 1


def test_an_identity_key_is_read_whatever_the_create_holds() -> None:
    """The layout-free splitter missed an identity key after a comment in the column list,
    after a comment holding '(' before the CREATE, and in a table named with '(' -- the
    picker read Keep and the cut-over identity sync could be hidden."""
    from dsql_migrator.ui.schema_conversion_apply import identity_from_ddl

    table = TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                                                  ColumnDef(name="v", mysql_type="int")],
                     primary_key=["id"], auto_increment_column="id")
    key = '"id" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)'
    for ddl in (
        f'CREATE TABLE "shop"."orders" (\n  -- surrogate key\n  {key},\n  "v" BIGINT,\n  PRIMARY KEY ("id")\n)',
        f'CREATE TABLE "shop"."orders" (\n  "v" BIGINT, -- the user\'s value, kept\n  {key},\n  PRIMARY KEY ("id")\n)',
        f'-- (by hand)\nCREATE TABLE "shop"."orders" ({key}, "v" BIGINT, PRIMARY KEY ("id"))',
        f'CREATE TABLE "shop"."orders(2024)" ({key}, "v" BIGINT, PRIMARY KEY ("id"))',
    ):
        assert identity_from_ddl(table, ddl), ddl
    spaced = TableDef(name="s.t", columns=[ColumnDef(name="order id", mysql_type="int", nullable=False)],
                      primary_key=["order id"], auto_increment_column="order id")
    assert identity_from_ddl(spaced, 'CREATE TABLE "s"."t" ("order id" BIGINT NOT NULL '
                                     'GENERATED BY DEFAULT AS IDENTITY (CACHE 65536), PRIMARY KEY ("order id"))')


def test_an_edit_that_only_starts_with_the_refusal_comment_is_kept() -> None:
    from dsql_migrator.core.converter import is_composite_key_placeholder
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    comment = "-- Could not apply a composite primary key to shop.orders: too large"
    assert is_composite_key_placeholder(comment)
    edited = comment + '\nCREATE TABLE "shop"."orders" ("id" INT NOT NULL, PRIMARY KEY ("id"));'
    assert not is_composite_key_placeholder(edited)
    state = SchemaConversionState()
    state.set_edited_target_ddl("shop.orders", edited)
    assert state.get_edited_target_ddl("shop.orders") == edited


def test_a_stored_composite_key_the_estimate_refuses_keeps_its_notes() -> None:
    """Its rebuild was refused, so the table showed the identity default's notes beside a
    composite key nothing generates."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion import TABLE_PREFIX, generate_previews

    table = TableDef(
        name="shop.customers",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="email", mysql_type="varchar(255)", nullable=False)],
        primary_key=["id"], auto_increment_column="id",
    )
    inventory = SourceInventory(tables=[table])
    result = SchemaConverter().convert(inventory)
    stored = ('CREATE TABLE "shop"."customers" ("id" BIGINT NOT NULL, "email" VARCHAR(255) NOT NULL, '
              'PRIMARY KEY ("email", "id"))')
    (preview,) = generate_previews([TABLE_PREFIX + "shop.customers"], inventory, result,
                                   stored_ddl_of={"shop.customers": stored}.get)
    notes = " ".join(w.message for w in preview.warnings)
    assert "With the composite key, 'id' stays" in notes
    assert "KEEPS its value generation" not in notes
