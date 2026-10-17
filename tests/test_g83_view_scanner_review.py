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
