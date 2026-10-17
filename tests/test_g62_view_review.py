"""G62: the review of G44-G55 -- view dependency order, what a flagged view hands on, and
partial-index predicates over a converted enum or a re-typed column."""

from __future__ import annotations

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, views_in_dependency_order
from dsql_migrator.core.models import (
    ColumnDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)

_PG = SourceType.POSTGRES


def _table(name: str, *columns: ColumnDef, **kw) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False), *columns],
        primary_key=["id"],
        **kw,
    )


def _enum(name: str = "status", type_name: str = "app.order_status") -> ColumnDef:
    return ColumnDef(
        name=name, mysql_type=type_name, type_kind="enum",
        enum_labels=["pending", "shipped", "delivered"],
    )


def _order(inventory: SourceInventory, source_type=_PG) -> list[str]:
    result = SchemaConverter(source_type=source_type).convert(inventory)
    return [view.view for view in views_in_dependency_order(result.views)]


def test_a_column_or_alias_named_like_a_view_is_no_dependency() -> None:
    """st.region (a column) and `AS region` (an alias) made v_store_sales 'read' the view
    app.region, which really reads v_store_sales: a false cycle, broken the wrong way, so
    CREATE VIEW app.region failed on every first apply."""
    pg = SourceInventory(
        tables=[_table("app.stores", ColumnDef(name="region", mysql_type="text"))],
        views=[
            ViewDef(name="app.region",
                    definition=" SELECT DISTINCT v_store_sales.region FROM app.v_store_sales;"),
            ViewDef(name="app.v_store_sales",
                    definition=" SELECT st.region AS region, count(*) AS sales "
                               "FROM app.stores st GROUP BY st.region;"),
        ],
    )
    assert _order(pg) == ["app.v_store_sales", "app.region"]
    mysql = SourceInventory(
        tables=[_table("app.stores", ColumnDef(name="region", mysql_type="varchar(20)"))],
        views=[
            ViewDef(name="app.region",
                    definition="select distinct `v_store_sales`.`region` AS `region` "
                               "from `app`.`v_store_sales`"),
            ViewDef(name="app.v_store_sales",
                    definition="select `st`.`region` AS `region` from `app`.`stores` `st`"),
        ],
    )
    assert _order(mysql, SourceType.MYSQL) == ["app.v_store_sales", "app.region"]


def test_another_schemas_table_named_like_a_view_is_no_dependency() -> None:
    """api.orders reads app.customers, which matched the view api.customers by its bare
    name; the cycle breaker then placed api.a_open_orders -- outside the cycle -- first."""
    inventory = SourceInventory(
        tables=[_table("app.orders"), _table("app.customers")],
        views=[
            ViewDef(name="api.a_open_orders", definition=" SELECT o.id FROM api.orders o;"),
            ViewDef(name="api.customers",
                    definition=" SELECT c.id, count(o.id) AS n FROM (app.customers c "
                               "JOIN api.orders o ON ((o.id = c.id))) GROUP BY c.id;"),
            ViewDef(name="api.orders",
                    definition=" SELECT o.id FROM (app.orders o JOIN app.customers c "
                               "ON ((c.id = o.id)));"),
        ],
    )
    order = _order(inventory)
    assert order[0] == "api.orders"
    assert set(order) == {"api.orders", "api.a_open_orders", "api.customers"}


def test_a_false_cycle_is_broken_inside_it_never_before_it() -> None:
    from dsql_migrator.core.converter import _dependency_order

    texts = {
        "s.z": "CREATE VIEW s.z AS SELECT 1 FROM s.x",  # waits on the cycle
        "s.x": "CREATE VIEW s.x AS SELECT 1 FROM s.y",
        "s.y": "CREATE VIEW s.y AS SELECT 1 FROM s.x",
    }
    order = _dependency_order(list(texts), name_of=lambda n: n, text_of=texts.__getitem__)
    assert order[0] == "s.x" and order.index("s.z") > order.index("s.x")


def test_a_quote_inside_a_literal_does_not_hide_the_view_it_reads() -> None:
    inventory = SourceInventory(
        tables=[_table("app.customers", ColumnDef(name="name", mysql_type="text"))],
        views=[
            ViewDef(name="app.a_csv",
                    definition=" SELECT (c.name || '\"'::text) AS inches "
                               "FROM app.\"ActiveCustomers\" c;"),
            ViewDef(name="app.ActiveCustomers",
                    definition=" SELECT customers.id, customers.name FROM app.customers;"),
        ],
    )
    assert _order(inventory) == ["app.ActiveCustomers", "app.a_csv"]


def _graded(inventory: SourceInventory) -> "tuple[dict, dict]":
    converted = {
        v.view: v for v in SchemaConverter(source_type=_PG).convert(inventory).views
    }
    assessed = {
        i.object_name: i for i in CompatibilityAssessor(source_type=_PG).assess(inventory).items
    }
    return converted, assessed


def test_an_aliased_enum_column_carries_its_flag_to_a_view_over_it() -> None:
    """v_orders hands on `status`, but v_furthest names its alias order_state: max() over
    it compares text on Aurora DSQL ('pending' > 'delivered'), and it read AUTO."""
    inventory = SourceInventory(
        tables=[_table("app.orders", _enum(), ColumnDef(name="customer_id", mysql_type="bigint"))],
        views=[
            ViewDef(name="app.v_furthest",
                    definition=" SELECT v_orders.customer_id, max(v_orders.order_state) AS furthest "
                               "FROM app.v_orders GROUP BY v_orders.customer_id;"),
            ViewDef(name="app.v_orders",
                    definition=" SELECT o.id, o.customer_id, o.status AS order_state "
                               "FROM app.orders o;"),
            ViewDef(name="app.v_ids", definition=" SELECT v_orders.id FROM app.v_orders;"),
        ],
    )
    converted, assessed = _graded(inventory)
    (note,) = converted["app.v_furthest"].warnings
    assert "app.v_orders.order_state (app.order_status -> text)" in note.message
    assert assessed["app.v_furthest"].classification.value == "MANUAL"
    # A view over the flagged one that names none of what it carries stays AUTO.
    assert converted["app.v_ids"].warnings == []
    assert assessed["app.v_ids"].classification.value == "AUTO"


def test_a_rewritten_enum_cast_output_carries_its_flag_too() -> None:
    inventory = SourceInventory(
        tables=[
            _table("app.tickets", ColumnDef(name="priority_code", mysql_type="text")),
            _table("app.queue", _enum("level", "app.priority_level")),
        ],
        views=[
            ViewDef(name="app.v_ticket_levels",
                    definition=" SELECT t.id, (t.priority_code)::app.priority_level AS level "
                               "FROM app.tickets t;"),
            ViewDef(name="app.v_worst_first",
                    definition=" SELECT v.id FROM app.v_ticket_levels v ORDER BY v.level DESC;"),
        ],
    )
    converted, assessed = _graded(inventory)
    (note,) = converted["app.v_worst_first"].warnings
    assert "app.v_ticket_levels.level (app.priority_level -> text)" in note.message
    assert assessed["app.v_worst_first"].classification.value == "MANUAL"


def test_propagation_does_not_depend_on_the_order() -> None:
    """A false cycle can still place a reader before the flagged view it reads: the reader
    is read again once the flagged view hands something on."""
    from dsql_migrator.core.converter import pg_views_remodelling

    tables = [_table("app.orders", _enum())]
    views = [
        ViewDef(name="app.a", definition=" SELECT b.order_state AS s FROM app.b b;"),
        ViewDef(name="app.b",
                definition=" SELECT o.status AS order_state FROM app.orders o "
                           "WHERE (EXISTS ( SELECT 1 FROM app.a));"),
    ]
    remodelling = pg_views_remodelling(views, tables)
    assert remodelling["app.b"].flagged
    assert ("app.b", "order_state", "app.order_status", "text") in remodelling["app.a"].references


def test_the_view_order_is_computed_once_per_conversion() -> None:
    """Every Schema Conversion render ordered every view again: ~0.65 s at 5,000 views."""
    from dsql_migrator.core.converter import _view_order_indices

    inventory = SourceInventory(
        tables=[_table("app.t")],
        views=[ViewDef(name=f"app.v{i}", definition=" SELECT t.id FROM app.t;") for i in range(3)],
    )
    views = SchemaConverter(source_type=_PG).convert(inventory).views
    views_in_dependency_order(views)
    hits = _view_order_indices.cache_info().hits
    views_in_dependency_order(views)
    assert _view_order_indices.cache_info().hits == hits + 1


def _index_ddls(table: TableDef) -> list[str]:
    result = SchemaConverter(source_type=_PG).convert(SourceInventory(tables=[table]))
    (converted,) = result.tables
    return list(converted.index_ddls)


def test_a_partial_unique_index_ordering_an_enum_is_not_carried_as_text() -> None:
    """`status > 'shipped'::order_status` became `> 'shipped'::text`: no label sorts after
    'shipped' as text, so the UNIQUE index constrained no row -- with no note."""
    ordered = _table(
        "app.orders", _enum(), ColumnDef(name="customer_id", mysql_type="bigint"),
        indexes=[IndexDef(name="one_delivered", columns=["customer_id"], unique=True,
                          where="(status > 'shipped'::app.order_status)")],
    )
    assert _index_ddls(ordered) == []
    result = SchemaConverter(source_type=_PG).convert(SourceInventory(tables=[ordered]))
    messages = " ".join(w.message for w in result.tables[0].warnings)
    assert "compares app.order_status by order" in messages
    assert "one_delivered is UNIQUE and NOT emitted" in messages
    equal = ordered.model_copy(update={"indexes": [
        IndexDef(name="one_shipped", columns=["customer_id"], unique=True,
                 where="(status = 'shipped'::app.order_status)")
    ]})
    (ddl,) = _index_ddls(equal)
    assert ddl.endswith("WHERE (status = 'shipped'::text)")


def test_an_enum_array_in_a_carried_predicate_becomes_a_text_array() -> None:
    table = _table(
        "app.tasks", _enum("state", "app.task_state"), ColumnDef(name="owner_id", mysql_type="bigint"),
        indexes=[IndexDef(name="ux_owner_active", columns=["owner_id"], unique=True,
                          where="(state = ANY ('{open,doing}'::app.task_state[]))")],
    )
    (ddl,) = _index_ddls(table)
    assert ddl.endswith("WHERE (state = ANY ('{open,doing}'::text[]))")


def test_a_null_test_on_a_re_typed_column_is_carried() -> None:
    """G47 left `UNIQUE (mac) WHERE mac IS NOT NULL` out entirely; NULLs never collide, so
    the predicate holds whatever the column's type."""
    table = _table(
        "app.devices", ColumnDef(name="mac", mysql_type="macaddr"),
        indexes=[IndexDef(name="ux_mac", columns=["mac"], unique=True, where="(mac IS NOT NULL)")],
    )
    (ddl,) = _index_ddls(table)
    assert ddl.startswith('CREATE UNIQUE INDEX ASYNC "ux_mac"')
    assert ddl.endswith("WHERE (mac IS NOT NULL)")
    reads = table.model_copy(update={"indexes": [
        IndexDef(name="ux_mac", columns=["mac"], unique=True,
                 where="(mac <> '00:00:00:00:00:00'::macaddr)")
    ]})
    assert _index_ddls(reads) == []
