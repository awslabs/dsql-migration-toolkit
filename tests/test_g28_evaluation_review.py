# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Edge cases from the review of the Evaluation readiness / limit / parity changes.

Each test names the failure it guards: a name collision that grew the report O(n^2), a
table whose name an index took, a refresh that kept the other cluster's verdict, and the
band and wording edges of the readiness score.
"""

from __future__ import annotations

from dsql_migrator.core.assessor import CompatibilityAssessor, check_table_count
from dsql_migrator.core.converter import (
    SchemaConverter,
    _identifier_length_warning,
    name_collisions,
)
from dsql_migrator.core.models import (
    AssessmentItem,
    AssessmentReport,
    Classification,
    ColumnDef,
    EffortLevel,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
    TargetInventory,
    TargetObjectKind,
    TargetRelation,
    TargetSchemaNode,
    ViewDef,
)
from dsql_migrator.ui.evaluation import (
    EvaluationResult,
    compute_migration_score,
    with_refreshed_catalogs,
)


def _table(name: str, *, indexes=(), columns=None) -> TableDef:
    return TableDef(
        name=name,
        columns=columns
        or [
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="user_id", mysql_type="int"),
            ColumnDef(name="currency", mysql_type="int"),
        ],
        primary_key=["id"],
        indexes=list(indexes),
    )


def _target(tables: dict[str, int] | None = None, views: dict[str, int] | None = None):
    schemas = {}
    for schema, count in (tables or {}).items():
        schemas.setdefault(schema, TargetSchemaNode(name=schema)).tables.extend(
            TargetRelation(schema_name=schema, name=f"x{i}", kind=TargetObjectKind.TABLE)
            for i in range(count)
        )
    for schema, count in (views or {}).items():
        schemas.setdefault(schema, TargetSchemaNode(name=schema)).views.extend(
            TargetRelation(schema_name=schema, name=f"w{i}", kind=TargetObjectKind.VIEW)
            for i in range(count)
        )
    return TargetInventory(schemas=list(schemas.values()))


def _assess(*tables: TableDef, views=(), engine=SourceType.MYSQL, target=None):
    return CompatibilityAssessor(source_type=engine).assess(
        SourceInventory(tables=list(tables), views=list(views)), target_inventory=target
    )


def _item(report: AssessmentReport, name: str) -> AssessmentItem:
    return next(item for item in report.items if item.object_name == name)


# --- NAME_COLLISION -------------------------------------------------------------------


def test_a_name_shared_by_every_table_keeps_each_note_bounded() -> None:
    """One index name on all of 1,000 tables listed the other 999 in every note: O(n^2)
    text in the report, the session snapshot, the HTML export and the AI prompt. The
    indexes are renamed now and each note stays short."""
    tables = [
        _table(f"app.t{i}", indexes=[IndexDef(name="idx_status", columns=["user_id"])])
        for i in range(1000)
    ]
    report = _assess(*tables)
    concerns = [
        concern
        for item in report.items
        for concern in item.concerns
        if concern.rule_id == "NAME_COLLISION"
    ]
    assert len(concerns) == 1000  # every index that shares the name is renamed
    assert all(concern.is_advisory for concern in concerns)
    assert max(len(concern.risk) for concern in concerns) < 1500
    assert "and 996 more" in concerns[0].risk


def test_an_index_that_takes_another_tables_name_is_renamed_so_the_table_is_created() -> None:
    """MySQL names an FK's implicit index after the column, e.g. ``currency`` -> the table
    ``currency``. Applied first, that index took the name and the TABLE was skipped; the
    index is now created under another name."""
    inventory = SourceInventory(
        tables=[
            _table("app.account", indexes=[IndexDef(name="currency", columns=["currency"])]),
            _table("app.currency"),
        ]
    )
    report = _assess(*inventory.tables)
    account, currency = _item(report, "app.account"), _item(report, "app.currency")
    assert currency.classification is Classification.AUTO
    assert "NAME_COLLISION" not in {c.rule_id for c in currency.concerns}
    (rename,) = [c for c in account.concerns if c.rule_id == "NAME_COLLISION"]
    assert rename.is_advisory
    assert "currency is created as account_currency" in rename.risk
    conversion = SchemaConverter().convert(inventory)
    account_ddl = next(t for t in conversion.tables if t.table == "app.account").index_ddls
    assert account_ddl == ['CREATE INDEX ASYNC "account_currency" ON "app"."account" ("currency")']


def test_a_tables_own_index_of_the_same_name_is_renamed() -> None:
    report = _assess(_table("app.users", indexes=[IndexDef(name="users", columns=["user_id"])]))
    (concern,) = [c for c in _item(report, "app.users").concerns if c.rule_id == "NAME_COLLISION"]
    assert concern.is_advisory
    assert "users is created as users_users (the name is also used by table users)" in concern.risk


def test_tables_whose_names_agree_in_63_bytes_are_unsupported() -> None:
    a, b = "t" * 63 + "_a", "t" * 63 + "_b"
    report = _assess(_table(f"app.{a}"), _table(f"app.{b}"))
    for name in (a, b):
        item = _item(report, f"app.{name}")
        assert item.classification is Classification.UNSUPPORTED
        (concern,) = [c for c in item.concerns if c.rule_id == "NAME_COLLISION"]
        assert concern.recommendation.startswith("Shorten the names on the source")


def test_an_index_named_like_a_view_is_renamed_and_the_view_is_left_alone() -> None:
    report = _assess(
        _table("app.orders", indexes=[IndexDef(name="v_orders", columns=["user_id"])]),
        views=[ViewDef(name="app.v_orders", definition="SELECT 1")],
    )
    view = _item(report, "app.v_orders")
    assert "NAME_COLLISION" not in {c.rule_id for c in view.concerns}
    orders = _item(report, "app.orders")
    assert any("v_orders is created as orders_v_orders" in c.risk for c in orders.concerns)


def test_collisions_compare_names_as_stored_on_a_character_boundary() -> None:
    """PostgreSQL cuts a long name on a character boundary: 62 ASCII bytes plus a 2-byte
    character is stored as the 62 bytes, so it collides with a 62-byte name. A raw byte cut
    kept half the character and missed it."""
    stem = "i" * 62
    collisions = name_collisions(
        SourceInventory(
            tables=[
                _table("app.a", indexes=[IndexDef(name=stem + "é", columns=["user_id"])]),
                _table("app.b", indexes=[IndexDef(name=stem, columns=["user_id"])]),
            ]
        )
    )
    assert set(collisions) == {"app.a", "app.b"}
    # The same rule for column names (IDENTIFIER_LENGTH).
    note = _identifier_length_warning(
        TableDef(
            name="app.c",
            columns=[
                ColumnDef(name="id", mysql_type="int"),
                ColumnDef(name=stem + "é", mysql_type="int"),
                ColumnDef(name=stem, mysql_type="int"),
            ],
            primary_key=["id"],
        )
    )
    assert note is not None and note.classification is Classification.UNSUPPORTED


def test_only_indexes_the_conversion_emits_can_collide() -> None:
    """An index over a bytea column is never emitted, so its name collides with nothing."""
    blob = [
        ColumnDef(name="id", mysql_type="int"),
        ColumnDef(name="b", mysql_type="varbinary(16)"),
        ColumnDef(name="user_id", mysql_type="int"),
    ]
    collisions = name_collisions(
        SourceInventory(
            tables=[
                _table("app.a", columns=blob, indexes=[IndexDef(name="ix", columns=["b"])]),
                _table("app.b", indexes=[IndexDef(name="ix", columns=["user_id"])]),
            ]
        )
    )
    assert collisions == {}


def test_a_view_that_shares_a_63_byte_name_with_a_table_gets_a_conversion_note() -> None:
    stem = "v" * 63
    result = SchemaConverter().convert(
        SourceInventory(
            tables=[_table(f"app.{stem}a")],
            views=[ViewDef(name=f"app.{stem}b", definition="SELECT 1 AS one")],
        )
    )
    (view,) = result.views
    (note,) = [w for w in view.warnings if "Names shared within" in w.message]
    assert note.classification is Classification.UNSUPPORTED


def test_an_overlong_name_that_collides_with_nothing_reads_as_advice_consistently() -> None:
    long_name = "c" * 64
    report = _assess(
        _table(
            "app.t",
            columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name=long_name, mysql_type="int")],
        )
    )
    (concern,) = [c for c in _item(report, "app.t").concerns if c.rule_id == "IDENTIFIER_LENGTH"]
    assert concern.is_advisory
    assert "No rename is needed" in concern.risk  # the converter's text no longer says rename
    assert concern.recommendation.startswith("Optional")


# --- BYTEA_KEY / TooManyIndexes on PostgreSQL ------------------------------------------


def test_pg_bytea_secondary_and_unique_indexes_are_gaps() -> None:
    def pg_table(unique: bool) -> TableDef:
        return TableDef(
            name="public.docs",
            columns=[ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="digest", mysql_type="bytea")],
            primary_key=["id"],
            indexes=[IndexDef(name="ix_digest", columns=["digest"], unique=unique)],
        )

    unique = _item(_assess(pg_table(True), engine=SourceType.POSTGRES), "public.docs")
    assert "BYTEA_KEY" in {c.rule_id for c in unique.concerns}
    bytea = next(c for c in unique.concerns if c.rule_id == "BYTEA_KEY")
    assert bytea.classification is Classification.MANUAL
    assert bytea.effort is EffortLevel.MEDIUM
    assert "uniqueness is NOT enforced" in bytea.risk
    plain = _item(_assess(pg_table(False), engine=SourceType.POSTGRES), "public.docs")
    assert next(c for c in plain.concerns if c.rule_id == "BYTEA_KEY").effort is EffortLevel.SIMPLE


def test_pg_too_many_indexes_counts_only_creatable_indexes() -> None:
    columns = [ColumnDef(name="id", mysql_type="bigint")] + [
        ColumnDef(name=f"c{i}", mysql_type="bytea" if i < 2 else "integer") for i in range(24)
    ]
    table = TableDef(
        name="public.wide",
        columns=columns,
        primary_key=["id"],
        indexes=[IndexDef(name=f"i{i}", columns=[f"c{i}"]) for i in range(24)],
    )
    item = _item(_assess(table, engine=SourceType.POSTGRES), "public.wide")
    assert "TOO_MANY_INDEXES" not in {c.rule_id for c in item.concerns}


# --- Database-level limits -------------------------------------------------------------


def test_limit_advice_follows_the_sources_own_size() -> None:
    """1,500 source tables, 600 of them already on the cluster: the source alone needs two
    clusters -- it does not 'fit on its own' because only 900 of its tables are new."""
    source = SourceInventory(tables=[_table(f"app.t{i}") for i in range(1500)])
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="app",
                tables=[
                    TargetRelation(schema_name="app", name=f"t{i}", kind=TargetObjectKind.TABLE)
                    for i in range(600)
                ],
            )
        ]
    )
    (item,) = check_table_count(source, target)
    assert item.object_name == "1,500 tables"
    assert "at least 2 Aurora DSQL clusters" in item.recommendation


def test_a_target_with_tables_and_views_counts_each_against_its_own_limit() -> None:
    report = _assess(
        *[_table(f"app.t{i}") for i in range(10)],
        views=[ViewDef(name=f"app.v{i}", definition="SELECT 1") for i in range(10)],
        target=_target(tables={"other": 995}, views={"other": 4995}),
    )
    database = {i.rule_id: i for i in report.items if i.kind == "DATABASE"}
    assert database["TABLE_COUNT_LIMIT"].object_name == "1,005 tables"
    assert database["VIEW_COUNT_LIMIT"].object_name == "5,005 views"


def test_schemas_on_the_target_can_push_a_multi_database_source_over_the_limit() -> None:
    report = _assess(
        *[_table(f"db{i}.t") for i in range(4)],
        target=_target(tables={f"old{i}": 1 for i in range(7)}),
    )
    rule_ids = {i.rule_id for i in report.items if i.kind == "DATABASE"}
    assert rule_ids == {"SCHEMA_COUNT_LIMIT"}  # the advisory note steps aside


def test_the_source_public_schema_is_not_counted() -> None:
    from dsql_migrator.core.assessor import check_pg_schema_count

    source = SourceInventory(
        tables=[_table("public.t")] + [_table(f"s{i}.t") for i in range(10)]
    )
    assert check_pg_schema_count(source) == []


# --- Readiness score -------------------------------------------------------------------


def _report(*items: AssessmentItem) -> AssessmentReport:
    return AssessmentReport.from_items(list(items))


def _obj(classification, effort=None, *, kind="TABLE", rule_id="R", name=None):
    _obj.n = getattr(_obj, "n", 0) + 1
    return AssessmentItem(
        object_name=name or f"o{_obj.n}",
        rule_id=rule_id,
        classification=classification,
        effort=effort,
        kind=kind,
    )


def test_a_database_item_that_is_not_a_limit_does_not_block() -> None:
    """A PostgreSQL extension or collation note is a DATABASE-level MANUAL item; only the
    limit checks may force 'Blocked'."""
    report = _assess(
        _table("public.t"),
        engine=SourceType.POSTGRES,
    )
    extension = _obj(
        Classification.MANUAL, EffortLevel.MEDIUM, kind="DATABASE", rule_id="PG_EXTENSION_UNSUPPORTED"
    )
    score = compute_migration_score(_report(*report.items, extension))
    assert score.band != "Blocked"
    # Even an UNSUPPORTED DATABASE item that is not one of the limit checks does not.
    odd = _obj(Classification.UNSUPPORTED, EffortLevel.SIGNIFICANT, kind="DATABASE", rule_id="CUSTOM")
    assert compute_migration_score(_report(*report.items, odd)).band != "Blocked"


def test_several_blockers_are_named_in_one_plural_sentence() -> None:
    report = _assess(*[_table(f"db{i}.t{j}") for i in range(11) for j in range(100)])
    score = compute_migration_score(report)
    assert score.band == "Blocked"
    assert score.summary.startswith("This migration exceeds Aurora DSQL database limits: ")
    assert "1,100 tables (Aurora DSQL allows 1,000 tables per database)" in score.summary
    assert "11 schemas (Aurora DSQL allows 10 schemas per database)" in score.summary
    assert "items below" in score.summary


def test_unsupported_objects_in_a_warning_band_keep_the_effort_label() -> None:
    score = compute_migration_score(
        _report(
            *[_obj(Classification.MANUAL, EffortLevel.MEDIUM) for _ in range(5)],
            _obj(Classification.UNSUPPORTED, EffortLevel.MEDIUM),
        )
    )
    assert (score.band, score.tone) == ("Moderate effort", "warning")


# --- Refresh paths recompute the target-dependent verdict ------------------------------


def test_a_target_refresh_recomputes_the_limits_against_the_new_cluster() -> None:
    """Re-stamping the endpoint while keeping the old assessment showed the other cluster's
    verdict as current: 'Ready' for a cluster with no room left."""
    inventory = SourceInventory(tables=[_table(f"app.t{i}") for i in range(700)])
    empty = TargetInventory()
    result = EvaluationResult(
        inventory=inventory,
        assessment=CompatibilityAssessor().assess(inventory, target_inventory=empty),
        target_inventory=empty,
        target_conflicts=[],
        target_endpoint="a.dsql.us-east-1.on.aws",
    )
    assert compute_migration_score(result.assessment).band == "Ready"
    refreshed = with_refreshed_catalogs(
        result,
        inventory=result.inventory,
        target_inventory=_target(tables={"other": 400}),
        target_endpoint="b.dsql.us-east-1.on.aws",
    )
    assert refreshed.target_endpoint == "b.dsql.us-east-1.on.aws"
    assert compute_migration_score(refreshed.assessment).band == "Blocked"
    # And freeing the room clears it again.
    cleared = with_refreshed_catalogs(
        refreshed,
        inventory=refreshed.inventory,
        target_inventory=_target(tables={"other": 100}),
        target_endpoint=refreshed.target_endpoint,
    )
    assert compute_migration_score(cleared.assessment).band == "Ready"


def test_the_cdc_cascade_notice_sees_a_cascade_behind_a_more_severe_finding() -> None:
    """A BINARY(16)-keyed child (BYTEA_KEY governs the item) with an ON DELETE CASCADE FK
    used to drop out of the CDC cascade warning, which matched only the headline rule."""
    from dsql_migrator.core.models import ForeignKeyDef
    from dsql_migrator.ui.data_migration import cdc_cascade_gap_tables

    child = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="binary(16)", nullable=False),
            ColumnDef(name="user_id", mysql_type="int"),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyDef(
                name="fk_user",
                columns=["user_id"],
                referenced_table="shop.users",
                referenced_columns=["id"],
                on_delete="CASCADE",
            )
        ],
    )
    report = _assess(child, _table("shop.users"))
    assert _item(report, "shop.orders").rule_id == "BYTEA_KEY"
    assert cdc_cascade_gap_tables(report) == ["shop.orders"]


def test_names_that_differ_only_in_case_do_not_collide() -> None:
    """The converter quotes every name, so Aurora DSQL keeps ``IDX_A`` and ``idx_a`` apart."""
    collisions = name_collisions(
        SourceInventory(
            tables=[
                _table("app.a", indexes=[IndexDef(name="IDX_A", columns=["user_id"])]),
                _table("app.b", indexes=[IndexDef(name="idx_a", columns=["user_id"])]),
            ]
        )
    )
    assert collisions == {}


# --- Round-2 review ---------------------------------------------------------------------


class _Ui:
    """A permissive NiceGUI stand-in: records label/badge text, accepts everything else."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    class _El:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._El()

    def badge(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return self._El()

    def __getattr__(self, _name):
        return lambda *_a, **_k: self._El()


def _result(inventory: SourceInventory, *, endpoint: str = "a.dsql.us-east-1.on.aws"):
    return EvaluationResult(
        inventory=inventory,
        assessment=CompatibilityAssessor().assess(inventory),
        target_inventory=TargetInventory(),
        target_conflicts=[],
        target_endpoint=endpoint,
    )


def test_a_limit_item_sets_the_band_without_lowering_the_per_object_score() -> None:
    """1,001 clean tables need no per-object work, so the score is 100 'per-object
    readiness' -- and the band is Blocked; the limit item no longer enters the ratio."""
    from dsql_migrator.ui.evaluation import _render_score_card

    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_table(f"app.t{i}") for i in range(1001)])
    )
    score = compute_migration_score(report)
    assert (score.score, score.band) == (100, "Blocked")
    assert score.summary.endswith("the score rates the per-object work only.")
    ui = _Ui()
    _render_score_card(ui, report)
    assert "100 / 100 per-object readiness" in ui.texts


def test_the_unsupported_band_does_not_claim_the_score_excludes_them() -> None:
    report = _assess(
        *[_table(f"app.t{i}") for i in range(9)],
        TableDef(name="app.nopk", columns=[ColumnDef(name="v", mysql_type="int")]),
    )
    score = compute_migration_score(report)
    assert score.band == "Unsupported objects"
    assert "everything else" not in score.summary
    from dsql_migrator.ui.evaluation import _render_score_card

    ui = _Ui()
    _render_score_card(ui, report)
    assert f"{score.score} / 100 readiness" in ui.texts


def test_the_stale_target_notice_sits_at_the_score_and_only_when_one_is_shown() -> None:
    from dsql_migrator.ui.evaluation import _render_result

    stale = _result(SourceInventory(tables=[_table("app.t")]))
    ui = _Ui()
    _render_result(ui, stale, live_target_endpoint="b.dsql.us-east-1.on.aws")
    assert "Readiness was assessed against another cluster" in ui.texts
    current = _Ui()
    _render_result(current, stale, live_target_endpoint="a.dsql.us-east-1.on.aws")
    assert "Readiness was assessed against another cluster" not in current.texts
    # No score card for an empty report, so no notice about "this score" either.
    empty = _Ui()
    _render_result(empty, _result(SourceInventory()), live_target_endpoint="b.dsql.us-east-1.on.aws")
    assert "Readiness was assessed against another cluster" not in empty.texts


def test_a_refresh_grades_the_reread_inventory_with_its_own_engine() -> None:
    """Switching the Connect engine and refreshing used to grade PostgreSQL tables with the
    MySQL rules (inet/array tables read AUTO) and keep the report titled MySQL."""
    result = _result(SourceInventory(tables=[_table("app.t")]))
    pg_inventory = SourceInventory(
        tables=[
            TableDef(
                name="public.hosts",
                columns=[
                    ColumnDef(name="id", mysql_type="bigint"),
                    ColumnDef(name="addr", mysql_type="inet"),
                ],
                primary_key=["id"],
            )
        ]
    )
    refreshed = with_refreshed_catalogs(
        result,
        inventory=pg_inventory,
        target_inventory=result.target_inventory,
        target_endpoint=result.target_endpoint,
        source_type=SourceType.POSTGRES,
    )
    assert refreshed.source_type is SourceType.POSTGRES
    assert _item(refreshed.assessment, "public.hosts").classification is not Classification.AUTO


def test_views_are_quoted_so_names_that_differ_in_case_do_not_collide() -> None:
    """Views used to be emitted unquoted, so Aurora DSQL folded ``V_Orders`` to
    ``v_orders`` (and a view in schema ``MyApp`` landed in ``myapp``). They are quoted now,
    like tables and indexes, and compared exactly."""
    different_case = name_collisions(
        SourceInventory(
            tables=[_table("app.orders", indexes=[IndexDef(name="v_orders", columns=["user_id"])])],
            views=[ViewDef(name="app.V_Orders", definition="SELECT 1")],
        )
    )
    assert different_case == {}
    same_name = name_collisions(
        SourceInventory(
            tables=[_table("app.orders", indexes=[IndexDef(name="V_Orders", columns=["user_id"])])],
            views=[ViewDef(name="app.V_Orders", definition="SELECT 1")],
        )
    )
    assert set(same_name) == {"app.orders", "app.V_Orders"}
    (view,) = SchemaConverter().convert(
        SourceInventory(views=[ViewDef(name="MyApp.V_Orders", definition="SELECT 1 AS one")])
    ).views
    assert view.target_ddl.startswith('CREATE VIEW "MyApp"."V_Orders"')


def test_a_truncation_collision_says_where_the_rows_go() -> None:
    a, b = "t" * 63 + "_a", "t" * 63 + "_b"
    inventory = SourceInventory(
        tables=[
            _table(f"app.{a}", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
            _table(f"app.{b}"),
            _table("app.other", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
        ]
    )
    item = _item(_assess(*inventory.tables), f"app.{a}")
    (concern,) = [c for c in item.concerns if c.rule_id == "NAME_COLLISION"]
    assert concern.classification is Classification.UNSUPPORTED
    assert "mixing their rows" in concern.risk
    assert "creates none of them until the names differ" in concern.risk
    assert concern.recommendation.startswith("Shorten the names on the source")
    # Nothing of either table is applied: both are placeholders the apply skips.
    conversion = SchemaConverter().convert(inventory)
    for name in (a, b):
        table = next(t for t in conversion.tables if t.table == f"app.{name}")
        assert table.target_ddl.startswith("--")
        assert table.index_ddls == [] and table.schema_ddls == []
    # The user_id index clash is not the operator's to fix: the second index is renamed.
    from dsql_migrator.core.converter import index_renames

    assert index_renames(inventory) == {"app.other": {"user_id": "other_user_id"}}


def test_identifier_collisions_give_the_same_fix_in_both_steps() -> None:
    from dsql_migrator.core.converter import IDENTIFIER_COLLISION_REMEDY

    a, b = "c" * 63 + "a", "c" * 63 + "b"
    table = TableDef(
        name="app.t",
        columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name=a, mysql_type="int"), ColumnDef(name=b, mysql_type="int")],
        primary_key=["id"],
    )
    note = _identifier_length_warning(table)
    assert note.message.endswith(IDENTIFIER_COLLISION_REMEDY)
    (concern,) = [c for c in _item(_assess(table), "app.t").concerns if c.rule_id == "IDENTIFIER_LENGTH"]
    assert concern.recommendation == IDENTIFIER_COLLISION_REMEDY


# --- Automatic index renaming ------------------------------------------------------------


def test_a_renamed_index_fits_63_bytes_and_avoids_names_already_taken() -> None:
    from dsql_migrator.core.converter import index_renames
    from dsql_migrator.core.target_introspector import pg_stored_name

    long_table = "orders_" + "x" * 50
    inventory = SourceInventory(
        tables=[
            _table("app.a", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
            _table(f"app.{long_table}", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
            # The plain candidate for the next one, "b_user_id", is already a table name.
            _table("app.b_user_id"),
            _table("app.b", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
        ]
    )
    renames = index_renames(inventory)
    long_name = renames[f"app.{long_table}"]["user_id"]
    assert len(long_name.encode("utf-8")) <= 63 and pg_stored_name(long_name) == long_name
    b_name = renames["app.b"]["user_id"]
    assert b_name != "b_user_id" and b_name.startswith("b_user_id_")
    # Deterministic: the same inventory gives the same names.
    assert index_renames(inventory) == renames


def test_a_primary_key_strategy_rebake_keeps_the_renamed_index_names() -> None:
    """The per-table re-conversion behind the PK-strategy picker used to rebuild the DDL
    without the inventory, bringing the colliding source name back."""
    from dsql_migrator.ui.schema_conversion_apply import (
        build_identity_conversion,
        build_keep_conversion,
    )

    child = TableDef(
        name="shop.payments",
        columns=[ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="user_id", mysql_type="bigint")],
        primary_key=["id"],
        auto_increment_column="id",
        indexes=[IndexDef(name="user_id", columns=["user_id"])],
    )
    renames = {"user_id": "payments_user_id"}
    for build in (build_identity_conversion, build_keep_conversion):
        conversion = build(SchemaConverter(), child, index_names=renames)
        assert conversion.index_ddls == [
            'CREATE INDEX ASYNC "payments_user_id" ON "shop"."payments" ("user_id")'
        ]


# --- Apply safety around shared names (schema-qualified DROP, holder-aware existence) ------


def test_a_replaced_index_is_dropped_in_its_own_tables_schema() -> None:
    """A bare ``DROP INDEX "x"`` resolves through the search_path, so a REPLACE of
    sales.orders' index could drop public.orders' same-named index."""
    from dsql_migrator.core.schema_applier import _build_drop_statement, _parse

    parsed = _parse('CREATE UNIQUE INDEX ASYNC "x" ON "sales"."orders" ("a")')
    assert parsed.lookup_name == "sales.x"
    assert _build_drop_statement(parsed).as_string(None) == 'DROP INDEX IF EXISTS "sales"."x"'
    bare = _parse('CREATE INDEX ASYNC "y" ON "t" ("a")')
    assert bare.lookup_name == "y"


class _HolderOracle:
    """An existence oracle that also says what holds a name (like TargetIntrospector):
    ``object_exists`` case-insensitively, ``relation_holder`` exactly."""

    def __init__(self, holders: dict) -> None:
        self.holders = dict(holders)

    def object_exists(self, name: str) -> bool:
        return name.lower() in {key.lower() for key in self.holders}

    def relation_holder(self, name: str):
        return self.holders.get(name)


class _Duplicate(Exception):
    sqlstate = "42P07"


_RELKIND = {"TABLE": "r", "VIEW": "v", "INDEX": "i", "SEQUENCE": "S"}


def _applier(oracle, *, live=None, duplicate_on=()):
    """A core applier over a recording connection. The live holder query answers from
    ``live`` (default: the oracle's snapshot); a DDL containing any of ``duplicate_on``
    raises 42P07."""
    from dsql_migrator.core.schema_applier import SchemaApplier

    class _Conn:
        autocommit = True

        def __init__(self) -> None:
            self.executed: list[str] = []

        @property
        def ddl(self) -> list[str]:
            return [s for s in self.executed if not s.startswith("SELECT")]

        def cursor(self):
            conn = self

            class _Cur:
                rows: list = []

                def execute(self, statement, params=None):
                    text = statement if isinstance(statement, str) else statement.as_string(None)
                    conn.executed.append(text)
                    if text.startswith("SELECT c.relkind"):
                        holders = oracle.holders if live is None else live
                        held = holders.get(f"{params[0]}.{params[1]}")
                        self.rows = []
                        if held is not None:
                            kind, owner = held
                            on_schema, _, on_table = (owner or "").partition(".")
                            self.rows = [(_RELKIND[kind], on_schema or None, on_table or None)]
                        return
                    if any(marker in text for marker in duplicate_on):
                        raise _Duplicate(f"relation already exists: {text}")

                def fetchall(self):
                    return self.rows

                def close(self):
                    pass

            return _Cur()

        def close(self):
            pass

    connection = _Conn()
    return SchemaApplier(oracle, connection_factory=lambda: connection), connection


def test_a_table_whose_name_an_index_holds_fails_loudly_instead_of_being_skipped() -> None:
    """An earlier version could leave the index ``currency`` of ``account`` under the
    name of the table ``currency``; "already exists" then reported a table that is not
    there."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    applier, connection = _applier(_HolderOracle({"app.currency": ("INDEX", "app.account")}))
    result = applier.apply('CREATE TABLE "app"."currency" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    assert result.status is ApplyStatus.FAILED
    assert "already used on the target by an index on app.account" in result.detail
    assert connection.ddl == []


def test_an_index_held_by_another_tables_index_fails_and_its_own_index_is_skipped() -> None:
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    applier, _ = _applier(_HolderOracle({"app.ix": ("INDEX", "app.other")}))
    taken = applier.apply('CREATE INDEX ASYNC "ix" ON "app"."orders" ("a")', ApplyMode.SKIP_IF_EXISTS)
    assert taken.status is ApplyStatus.FAILED
    assert "Index ix on app.orders was not created" in taken.detail
    own, _ = _applier(_HolderOracle({"app.ix": ("INDEX", "app.orders")}))
    skipped = own.apply('CREATE INDEX ASYNC "ix" ON "app"."orders" ("a")', ApplyMode.SKIP_IF_EXISTS)
    assert skipped.status is ApplyStatus.SKIPPED


def test_names_that_differ_only_in_case_are_two_objects_on_re_apply() -> None:
    """The converter keeps "Currency" and the index currency apart (exact names); the
    applier did not -- it refused one as held by the other, or skipped the missing one."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    both = _HolderOracle(
        {"app.Currency": ("TABLE", None), "app.currency": ("INDEX", "app.account")}
    )
    applier, connection = _applier(both)
    table = applier.apply('CREATE TABLE "app"."Currency" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    index = applier.apply('CREATE INDEX ASYNC "currency" ON "app"."account" ("c")', ApplyMode.SKIP_IF_EXISTS)
    assert (table.status, index.status) == (ApplyStatus.SKIPPED, ApplyStatus.SKIPPED)
    assert connection.ddl == []

    # The table went missing; its twin index must not make it look present.
    only_index, connection = _applier(_HolderOracle({"app.currency": ("INDEX", "app.account")}))
    created = only_index.apply('CREATE TABLE "app"."Currency" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    assert created.status is ApplyStatus.CREATED
    assert connection.ddl == ['CREATE TABLE "app"."Currency" ("id" int PRIMARY KEY)']


def test_a_stale_snapshot_is_checked_on_the_live_catalog_before_refusing() -> None:
    """An earlier REPLACE of the same run (or the user, following the advice) dropped the
    leftover index: the name is free now, so the table is created."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    snapshot = _HolderOracle({"app.currency": ("INDEX", "app.account")})
    applier, connection = _applier(snapshot, live={})
    result = applier.apply('CREATE TABLE "app"."currency" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    assert result.status is ApplyStatus.CREATED
    assert connection.ddl == ['CREATE TABLE "app"."currency" ("id" int PRIMARY KEY)']


def test_already_exists_from_another_tables_index_is_not_skipped_as_this_index() -> None:
    """The snapshot missed it; the CREATE's 42P07 came from orders' index of the same name.
    Skipping reported payments' index as present (a UNIQUE one silently lost)."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    ddl = 'CREATE UNIQUE INDEX ASYNC "user_id" ON "shop"."payments" ("user_id")'
    applier, _ = _applier(
        _HolderOracle({}),
        live={"shop.user_id": ("INDEX", "shop.orders")},
        duplicate_on=['"user_id" ON'],
    )
    result = applier.apply(ddl, ApplyMode.SKIP_IF_EXISTS)
    assert result.status is ApplyStatus.FAILED
    assert "already used on the target by an index on shop.orders" in result.detail

    own, _ = _applier(
        _HolderOracle({}),
        live={"shop.user_id": ("INDEX", "shop.payments")},
        duplicate_on=['"user_id" ON'],
    )
    assert own.apply(ddl, ApplyMode.SKIP_IF_EXISTS).status is ApplyStatus.SKIPPED


def test_a_name_over_63_bytes_matches_its_stored_form() -> None:
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    long_table = "t" * 64
    stored = "t" * 63
    oracle = _HolderOracle(
        {f"app.{stored}": ("TABLE", None), "app.ix_user": ("INDEX", f"app.{stored}")}
    )
    applier, connection = _applier(oracle)
    table = applier.apply(f'CREATE TABLE "app"."{long_table}" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    index = applier.apply(f'CREATE INDEX ASYNC "ix_user" ON "app"."{long_table}" ("u")', ApplyMode.SKIP_IF_EXISTS)
    assert (table.status, index.status) == (ApplyStatus.SKIPPED, ApplyStatus.SKIPPED)
    assert connection.ddl == []


def test_a_doubled_quote_inside_a_name_is_parsed_whole() -> None:
    from dsql_migrator.core.schema_applier import _parse

    parsed = _parse('CREATE INDEX ASYNC "i""x" ON "app"."we""ird" ("a")')
    assert parsed.name == 'i"x'
    assert parsed.on_table == ["app", 'we"ird']
    assert parsed.lookup_name == 'app.i"x'


def test_a_view_an_earlier_version_stored_in_lower_case_is_created_or_replaced() -> None:
    """Up to 0.1.570 ``CREATE VIEW shop.V_Orders`` was unquoted, so the target holds
    ``shop.v_orders``. SKIP creates the quoted view (dependent views read that name) and
    says the old copy is there; REPLACE drops the old copy too."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    ddl = 'CREATE VIEW "shop"."V_Orders" AS SELECT 1'
    oracle = _HolderOracle({"shop.v_orders": ("VIEW", None)})
    applier, connection = _applier(oracle)
    created = applier.apply(ddl, ApplyMode.SKIP_IF_EXISTS)
    assert created.status is ApplyStatus.CREATED
    assert "shop.v_orders, is still on the target" in created.note
    assert connection.ddl == [ddl]

    replacer, connection = _applier(oracle)
    replaced = replacer.apply(ddl, ApplyMode.REPLACE, confirmed=True)
    assert replaced.status is ApplyStatus.CREATED
    # Both names: a replay after the CREATE committed then drops what it created.
    assert connection.ddl == [
        'DROP VIEW IF EXISTS "shop"."v_orders"',
        'DROP VIEW IF EXISTS "shop"."V_Orders"',
        ddl,
    ]

    # A lower-case TABLE of that name is not the view.
    table_twin, connection = _applier(_HolderOracle({"shop.v_orders": ("TABLE", None)}))
    assert table_twin.apply(ddl, ApplyMode.SKIP_IF_EXISTS).status is ApplyStatus.CREATED


def test_an_index_of_the_same_name_in_another_schema_does_not_skip_this_one() -> None:
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    applier, connection = _applier(_HolderOracle({"public.ix": ("INDEX", "public.orders")}))
    result = applier.apply('CREATE INDEX ASYNC "ix" ON "sales"."orders" ("a")', ApplyMode.SKIP_IF_EXISTS)
    assert result.status is ApplyStatus.CREATED
    assert connection.ddl == ['CREATE INDEX ASYNC "ix" ON "sales"."orders" ("a")']


def test_a_transient_catalog_read_at_the_schema_cap_is_retried_not_terminal() -> None:
    from dsql_migrator.core.schema_applier import _schema_present

    class _Transient(Exception):
        sqlstate = "40001"

    class _Conn:
        def cursor(self):
            class _Cur:
                def execute(self, *_a, **_k):
                    raise _Transient("conflict")

                def close(self):
                    pass

            return _Cur()

    import pytest

    with pytest.raises(_Transient):
        _schema_present(_Conn(), "app")


def test_stored_ddl_from_before_the_rename_is_applied_under_the_new_index_name() -> None:
    """A primary-key choice baked into the session before the rename existed still names
    the colliding index; the applied conversion uses the renamed one."""
    from dsql_migrator.ui.schema_conversion import render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import (
        applied_table_conversions,
        build_keep_conversion,
    )

    def child(name: str) -> TableDef:
        return TableDef(
            name=name,
            columns=[ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="user_id", mysql_type="bigint")],
            primary_key=["id"],
            indexes=[IndexDef(name="user_id", columns=["user_id"], unique=True)],
        )

    inventory = SourceInventory(tables=[child("shop.orders"), child("shop.payments")])
    result = SchemaConverter().convert(inventory)
    stale_script = render_target_ddl(build_keep_conversion(SchemaConverter(), child("shop.payments")))
    assert '"user_id" ON' in stale_script  # baked without the renames
    applied = applied_table_conversions(result, {"shop.payments": stale_script})
    assert applied["shop.payments"].index_ddls == [
        'CREATE UNIQUE INDEX ASYNC "payments_user_id" ON "shop"."payments" ("user_id")'
    ]
    # Schema Apply (Step 2) applies the same script under the same name as Full Load.
    from dsql_migrator.ui.schema_conversion_apply import (
        build_apply_objects,
        override_apply_objects,
    )

    objects = override_apply_objects(
        build_apply_objects(result), {"shop.payments": stale_script}, conversion=result
    )
    (payments,) = [obj for obj in objects if obj.object_name == "shop.payments"]
    assert payments.ddls[-1] == (
        'CREATE UNIQUE INDEX ASYNC "payments_user_id" ON "shop"."payments" ("user_id")'
    )


def test_a_rename_never_makes_a_script_name_one_index_twice() -> None:
    """The editor's script already holds the renamed index and the user added one under
    the source name: renaming that too would create ``payments_user_id`` twice."""
    from dsql_migrator.core.converter import rename_index_statements

    script = [
        'CREATE INDEX ASYNC "payments_user_id" ON "shop"."payments" ("user_id")',
        'CREATE INDEX ASYNC "user_id" ON "shop"."payments" ("user_id", "created_at")',
    ]
    assert rename_index_statements(script, {"user_id": "payments_user_id"}) == script


def test_rename_order_does_not_depend_on_inventory_order() -> None:
    """``a.b_c`` and ``a_b.c`` both want ``a_b_c``; who gets the plain name must not flip
    when the inventory order changes (a re-apply would then duplicate one index)."""
    from dsql_migrator.core.converter import index_renames

    def tables():
        return [
            _table("app.a", indexes=[IndexDef(name="b_c", columns=["user_id"])]),
            _table("app.a_b", indexes=[IndexDef(name="c", columns=["user_id"])]),
            _table("app.b_c"),
            _table("app.c"),
        ]

    forward = index_renames(SourceInventory(tables=tables()), is_postgres=False)
    backward = index_renames(SourceInventory(tables=list(reversed(tables()))), is_postgres=False)
    assert forward == backward
    assert forward["app.a"]["b_c"] == "a_b_c"
    assert forward["app.a_b"]["c"].startswith("a_b_c_")


def test_an_edit_never_replaces_the_placeholder_of_names_sharing_a_stored_form() -> None:
    """No target DDL separates tables whose names agree in 63 bytes: a stored edit (a
    pre-upgrade primary-key bake, an AI answer) created one, then the other's load went
    into it."""
    from dsql_migrator.core.converter import is_name_collision_placeholder
    from dsql_migrator.ui.schema_conversion_apply import (
        applied_table_conversions,
        build_apply_objects,
        override_apply_objects,
    )

    a, b = "app." + "t" * 63 + "_a", "app." + "t" * 63 + "_b"
    result = SchemaConverter().convert(SourceInventory(tables=[_table(a), _table(b)]))
    placeholder = next(t for t in result.tables if t.table == a)
    assert is_name_collision_placeholder(placeholder.target_ddl)
    edit = {a: f'CREATE TABLE "app"."{"t" * 63}_a" ("id" integer PRIMARY KEY)'}
    (obj,) = [o for o in override_apply_objects(build_apply_objects(result), edit, conversion=result) if o.object_name == a]
    assert obj.ddls == () and obj.skip_reason
    assert applied_table_conversions(result, edit)[a].target_ddl == placeholder.target_ddl


def test_a_dependent_view_is_matched_on_its_body_not_its_own_name() -> None:
    from dsql_migrator.core.column_exclusion import views_referencing

    ddls = {
        "archive.orders": 'CREATE VIEW "archive"."orders" AS SELECT * FROM "archive"."orders_2024"',
        "shop.v": 'CREATE VIEW "shop"."v" AS SELECT * FROM "shop"."orders"',
    }
    assert views_referencing(ddls, frozenset({"shop.orders"})) == [ddls["shop.v"]]


def test_both_pre_drops_also_drop_the_view_an_earlier_version_left_folded() -> None:
    """A REPLACE / Drop & reload of ``shop.orders`` pre-drops its views; the quoted DROP of
    ``"V_Orders"`` missed the ``v_orders`` 0.1.570 created, whose dependency then blocked
    the table's DROP (2BP01) on every retry."""
    from dsql_migrator.core.schema_applier import drop_object

    ddl = 'CREATE VIEW "shop"."V_Orders" AS SELECT * FROM "shop"."orders"'
    applier, connection = _applier(_HolderOracle({"shop.v_orders": ("VIEW", None)}))
    applier.drop(ddl)
    assert connection.ddl == [
        'DROP VIEW IF EXISTS "shop"."V_Orders"',
        'DROP VIEW IF EXISTS "shop"."v_orders"',
    ]

    # Full Load's introspector-free drop reads the live catalog instead.
    _, live = _applier(_HolderOracle({"shop.v_orders": ("VIEW", None)}))
    drop_object(ddl, connection_factory=lambda: live)
    assert live.ddl == [
        'DROP VIEW IF EXISTS "shop"."V_Orders"',
        'DROP VIEW IF EXISTS "shop"."v_orders"',
    ]

    # Both on the target (a SKIP created the quoted one next to the old copy): the copy
    # still reads the table, so it goes too -- unless it is a source view of its own.
    both = _HolderOracle({"shop.v_orders": ("VIEW", None), "shop.V_Orders": ("VIEW", None)})
    _, dropped = _applier(both)
    drop_object(ddl, connection_factory=lambda: dropped)
    assert dropped.ddl == [
        'DROP VIEW IF EXISTS "shop"."V_Orders"',
        'DROP VIEW IF EXISTS "shop"."v_orders"',
    ]
    _, kept = _applier(both)
    drop_object(
        ddl,
        connection_factory=lambda: kept,
        own_views=frozenset({("shop", "V_Orders"), ("shop", "v_orders")}),
    )
    assert kept.ddl == ['DROP VIEW IF EXISTS "shop"."V_Orders"']


def test_a_full_load_without_schema_conversion_still_emits_the_renamed_indexes() -> None:
    """A library/CLI caller passes no table_conversions; the per-table fallback could not
    see a name another table uses and re-emitted the colliding index names."""
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        BatchedTableMigrator,
        DataMigrationInputs,
        _derived_table_conversion,
        _slim_worker_inputs,
    )

    payments = _table("shop.payments", indexes=[IndexDef(name="user_id", columns=["user_id"])])
    inventory = SourceInventory(
        tables=[
            _table("shop.orders", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
            payments,
        ]
    )
    inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="shop"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        inventory=inventory,
    )
    migrator = BatchedTableMigrator(inputs, exporter=object(), watermark_capturer=object())
    # Only the renames are derived: no conversion is invented for the run (an empty
    # table_conversions still means "nothing applied", e.g. no foreign keys to add).
    assert migrator._inputs.table_conversions == {}
    worker = _slim_worker_inputs(migrator._inputs, "shop.payments")
    assert worker.index_renames == {"shop.payments": {"user_id": "payments_user_id"}}
    assert _derived_table_conversion(worker, payments).index_ddls == [
        'CREATE INDEX ASYNC "payments_user_id" ON "shop"."payments" ("user_id")'
    ]


def test_the_composite_rebake_keeps_the_renamed_index_names_and_the_picker_passes_them() -> None:
    import inspect

    from dsql_migrator.ui import schema_conversion
    from dsql_migrator.ui.schema_conversion_apply import build_composite_conversion

    child = TableDef(
        name="shop.payments",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="tenant", mysql_type="bigint", nullable=False),
            ColumnDef(name="user_id", mysql_type="bigint"),
        ],
        primary_key=["id"],
        auto_increment_column="id",
        indexes=[IndexDef(name="user_id", columns=["user_id"])],
    )
    conversion = build_composite_conversion(
        SchemaConverter(), child, "tenant", index_names={"user_id": "payments_user_id"}
    )
    assert any('"payments_user_id" ON' in ddl for ddl in conversion.index_ddls)
    # The preview passes the whole-inventory renames to the picker.
    source = inspect.getsource(schema_conversion._render_browser_and_preview)
    assert "index_names=conversion_result.index_renames.get(source_table.name)" in source


def test_the_introspector_holder_map_is_exact_and_live_holder_reads_the_catalog() -> None:
    from dsql_migrator.core.target_introspector import (
        TargetIntrospector,
        build_inventory,
        live_relation_holder,
    )

    inventory = build_inventory(
        [("app", "Currency", "BASE TABLE"), ("app", "account", "BASE TABLE")],
        [],
        [("app", "account", "currency", False)],
    )
    introspector = TargetIntrospector()
    introspector._index_inventory(inventory)
    assert introspector.relation_holder("app.Currency") == ("TABLE", None)
    assert introspector.relation_holder("app.currency") == ("INDEX", "app.account")
    assert introspector.relation_holder("app.CURRENCY") is None
    assert introspector.object_exists("app.CURRENCY")  # still case-insensitive

    class _Cur:
        def __init__(self, rows):
            self.rows, self.params = rows, None

        def execute(self, statement, params=None):
            self.params = params

        def fetchall(self):
            return self.rows

        def close(self):
            pass

    class _Conn:
        def __init__(self, rows):
            self.cur = _Cur(rows)

        def cursor(self):
            return self.cur

    index = _Conn([("i", "app", "account")])
    assert live_relation_holder(index, "app", "currency") == ("INDEX", "app.account")
    sequence = _Conn([("S", None, None)])
    assert live_relation_holder(sequence, "app", "currency") == ("SEQUENCE", None)
    free = _Conn([])
    assert live_relation_holder(free, "app", "x" * 70) is None
    assert free.cur.params == ("app", "x" * 63)



class _Unavailable(Exception):
    sqlstate = "XX000"


class _ConnectionLost(Exception):
    sqlstate = "08006"


def _catalog_applier(oracle, catalog: dict, *, fail_after_create: str = "", fail_first_connect=False):
    """A core applier over a STATEFUL catalog: DROP removes, CREATE adds (42P07 when the
    name is taken). ``fail_after_create``: the first CREATE containing it commits, then
    answers XX000 (Aurora DSQL's "may have committed")."""
    import re as _re

    from dsql_migrator.core.schema_applier import SchemaApplier

    state = {"failed": False, "connects": 0}
    executed: list[str] = []

    class _Conn:
        autocommit = True

        def cursor(self):
            class _Cur:
                rows: list = []

                def execute(self, statement, params=None):
                    text = statement if isinstance(statement, str) else statement.as_string(None)
                    executed.append(text)
                    if text.startswith("SELECT c.relkind"):
                        held = catalog.get(f"{params[0]}.{params[1]}")
                        self.rows = [] if held is None else [(_RELKIND[held[0]], None, None)]
                        return
                    match = _re.match(r'DROP \w+ IF EXISTS "([^"]+)"\."([^"]+)"', text)
                    if match:
                        catalog.pop(f"{match.group(1)}.{match.group(2)}", None)
                        return
                    match = _re.match(r'CREATE (VIEW|TABLE) "([^"]+)"\."([^"]+)"', text)
                    if match:
                        key = f"{match.group(2)}.{match.group(3)}"
                        if key in catalog:
                            raise _Duplicate(key)
                        catalog[key] = (match.group(1), None)
                        if fail_after_create and fail_after_create in text and not state["failed"]:
                            state["failed"] = True
                            raise _Unavailable("server unavailable")

                def fetchall(self):
                    return self.rows

                def close(self):
                    pass

            return _Cur()

        def close(self):
            pass

    def factory():
        state["connects"] += 1
        if fail_first_connect and state["connects"] == 1:
            raise _ConnectionLost("connection lost")
        return _Conn()

    applier = SchemaApplier(oracle, connection_factory=factory, sleep=lambda _s: None)
    return applier, executed, factory


def test_a_legacy_view_replace_replays_safely_after_its_create_committed() -> None:
    from dsql_migrator.core.models import ApplyMode, ApplyStatus

    catalog = {"shop.v_orders": ("VIEW", None)}
    ddl = 'CREATE VIEW "shop"."V_Orders" AS SELECT 1'
    applier, _, _ = _catalog_applier(
        _HolderOracle(dict(catalog)), catalog, fail_after_create='"V_Orders"'
    )
    result = applier.apply(ddl, ApplyMode.REPLACE, confirmed=True)
    assert result.status is ApplyStatus.CREATED, result.detail
    assert catalog == {"shop.V_Orders": ("VIEW", None)}


def test_a_lower_case_view_that_is_the_sources_own_is_never_taken_for_a_leftover() -> None:
    """The source has both shop.V_Orders and shop.v_orders; only v_orders is on the target.
    It was skipped as "already there" for V_Orders, or dropped (and never recreated)."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus
    from dsql_migrator.core.schema_applier import converted_names, drop_object

    names = converted_names(["shop.V_Orders", "shop.v_orders"], {})
    ddl = 'CREATE VIEW "shop"."V_Orders" AS SELECT 1'
    oracle = _HolderOracle({"shop.v_orders": ("VIEW", None)})

    applier, connection = _applier(oracle)
    applier.set_converted_names(names)
    assert applier.apply(ddl, ApplyMode.SKIP_IF_EXISTS).status is ApplyStatus.CREATED
    assert connection.ddl == [ddl]

    replacer, connection = _applier(oracle)
    replacer.set_converted_names(names)
    assert replacer.apply(ddl, ApplyMode.REPLACE, confirmed=True).status is ApplyStatus.CREATED
    assert 'DROP VIEW IF EXISTS "shop"."v_orders"' not in connection.ddl
    replacer.drop(ddl)
    assert connection.ddl[-1] == 'DROP VIEW IF EXISTS "shop"."V_Orders"'

    _, live = _applier(oracle)
    drop_object(ddl, connection_factory=lambda: live, own_views=names.views)
    assert live.ddl == ['DROP VIEW IF EXISTS "shop"."V_Orders"']


def test_an_index_an_earlier_version_kept_under_its_source_name_is_created_and_named() -> None:
    """Up to 0.1.570 the first table kept the colliding index's source name. Taking that
    copy for the index skipped a needed index whenever the old name was wrong for it (a
    table that needs the name; two renamed indexes cut to one name), so the renamed index
    is created and the note says the copy now duplicates it."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus
    from dsql_migrator.core.schema_applier import converted_names

    names = converted_names(
        [], {"shop.orders": {"user_id": "orders_user_id"}, "shop.payments": {"user_id": "payments_user_id"}}
    )
    ddl = 'CREATE INDEX ASYNC "orders_user_id" ON "shop"."orders" ("user_id")'
    oracle = _HolderOracle({"shop.orders": ("TABLE", None), "shop.user_id": ("INDEX", "shop.orders")})

    applier, connection = _applier(oracle)
    applier.set_converted_names(names)
    created = applier.apply(ddl, ApplyMode.SKIP_IF_EXISTS)
    assert created.status is ApplyStatus.CREATED
    assert 'now duplicates it; drop it (DROP INDEX "shop"."user_id")' in created.note
    assert connection.ddl == [ddl]

    replacer, connection = _applier(oracle)
    replacer.set_converted_names(names)
    assert replacer.apply(ddl, ApplyMode.REPLACE, confirmed=True).status is ApplyStatus.CREATED
    assert connection.ddl == [
        'DROP INDEX IF EXISTS "shop"."user_id"',
        'DROP INDEX IF EXISTS "shop"."orders_user_id"',
        ddl,
    ]

    # The source-named index on ANOTHER table is that table's, not this one's copy.
    other, connection = _applier(
        _HolderOracle({"shop.user_id": ("INDEX", "shop.payments")})
    )
    other.set_converted_names(names)
    plain = other.apply(ddl, ApplyMode.SKIP_IF_EXISTS)
    assert plain.status is ApplyStatus.CREATED and plain.note == ""


def test_a_table_blocked_by_the_old_copy_of_a_renamed_index_says_exactly_what_to_drop() -> None:
    """account's FK index ``currency`` took the table currency's name under 0.1.570. The
    renamed index is created; the table's failure names the copy and the safe order."""
    from dsql_migrator.core.models import ApplyMode, ApplyStatus
    from dsql_migrator.core.schema_applier import converted_names

    names = converted_names(
        [], {"shop.account": {"currency": "account_currency"}}, table_names=["shop.account", "shop.currency"]
    )
    oracle = _HolderOracle({"shop.account": ("TABLE", None), "shop.currency": ("INDEX", "shop.account")})
    applier, connection = _applier(oracle)
    applier.set_converted_names(names)
    index = applier.apply(
        'CREATE UNIQUE INDEX ASYNC "account_currency" ON "shop"."account" ("currency")',
        ApplyMode.SKIP_IF_EXISTS,
    )
    assert index.status is ApplyStatus.CREATED
    assert "It also holds the name of the table shop.currency" in index.note
    table = applier.apply('CREATE TABLE "shop"."currency" ("id" int PRIMARY KEY)', ApplyMode.SKIP_IF_EXISTS)
    assert table.status is ApplyStatus.FAILED
    assert "the copy an earlier version made of the index this migration creates as account_currency" in table.detail
    assert 'DROP INDEX "shop"."currency"' in table.detail


def test_the_live_folded_view_check_survives_a_transient_connect_failure() -> None:
    from dsql_migrator.core.schema_applier import drop_object

    catalog = {"shop.v_orders": ("VIEW", None)}
    _, executed, factory = _catalog_applier(_HolderOracle({}), catalog, fail_first_connect=True)
    drop_object(
        'CREATE VIEW "shop"."V_Orders" AS SELECT 1',
        connection_factory=factory,
        sleep=lambda _s: None,
    )
    assert [s for s in executed if s.startswith("DROP")] == [
        'DROP VIEW IF EXISTS "shop"."V_Orders"',
        'DROP VIEW IF EXISTS "shop"."v_orders"',
    ]
    assert catalog == {}


def test_the_legacy_skip_note_reaches_the_apply_result() -> None:
    """The core said why it skipped (and that Replace fixes it); the result row and the
    activity log only ever showed the generic "Already existed"."""
    from dsql_migrator.core.models import ApplyResult, ApplyStatus
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.schema_conversion_apply import (
        ApplyMode as UiMode,
        ApplyObject,
        DsqlSchemaApplier,
        run_schema_apply,
    )

    class _Core:
        def apply(self, ddl, mode, confirmed=False):
            return ApplyResult(
                object_name="shop.V_Orders",
                status=ApplyStatus.SKIPPED,
                detail="View shop.V_Orders is on the target as shop.v_orders ...",
                note="View shop.V_Orders is on the target as shop.v_orders ...",
            )

    applier = DsqlSchemaApplier(
        TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        core_factory=lambda target, profile: _Core(),
    )
    (result,) = run_schema_apply(
        [ApplyObject(object_name="shop.V_Orders", ddls=('CREATE VIEW "shop"."V_Orders" AS SELECT 1',))],
        applier=applier,
        mode=UiMode.SKIP_IF_EXISTS,
        confirmed=False,
    )
    assert result.detail.startswith("View shop.V_Orders is on the target as shop.v_orders")


def test_evaluation_matches_the_target_exactly_as_schema_apply_does() -> None:
    """Case-insensitive matching listed shop.Orders as already present next to a different
    shop.orders (Schema Apply then created it) and missed a >63-byte name stored cut."""
    from dsql_migrator.core.target_introspector import relations_existing_on_target

    long_name = "t" * 64
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="shop",
                tables=[
                    TargetRelation(schema_name="shop", name="orders", kind=TargetObjectKind.TABLE),
                    TargetRelation(schema_name="shop", name="t" * 63, kind=TargetObjectKind.TABLE),
                ],
                views=[TargetRelation(schema_name="shop", name="v_orders", kind=TargetObjectKind.VIEW)],
            )
        ]
    )
    assert relations_existing_on_target(["shop.Orders", f"shop.{long_name}"], target) == [
        f"shop.{long_name}"
    ]
    # A view an earlier version created unquoted is not the quoted view (SKIP creates it).
    assert relations_existing_on_target(["shop.V_Orders"], target, views=["shop.V_Orders"]) == []


def test_evaluation_names_source_objects_another_object_blocks_on_the_target() -> None:
    """A target view holding a source table's name was listed as "already exists, choose
    SKIP or REPLACE", and an index holding it was not listed at all; Schema Apply refuses
    both in either mode."""
    from dsql_migrator.core.models import TargetIndexDef
    from dsql_migrator.core.target_introspector import (
        relations_blocked_on_target,
        relations_existing_on_target,
    )

    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="shop",
                tables=[
                    TargetRelation(
                        schema_name="shop",
                        name="account",
                        kind=TargetObjectKind.TABLE,
                        indexes=[TargetIndexDef(name="currency", unique=True)],
                    )
                ],
                views=[TargetRelation(schema_name="shop", name="order_summary", kind=TargetObjectKind.VIEW)],
            )
        ]
    )
    names = ["shop.account", "shop.currency", "shop.order_summary"]
    assert relations_existing_on_target(names, target) == ["shop.account"]
    assert relations_blocked_on_target(names, target) == [
        ("shop.currency", "an index on shop.account"),
        ("shop.order_summary", "a view"),
    ]


def test_tables_sharing_a_stored_name_are_kept_out_of_the_migration() -> None:
    """Schema Conversion creates neither (a placeholder); Data Migration still offered and
    loaded both -- into one relation on a target an earlier version built."""
    from dsql_migrator.ui.data_migration import migratable_table_names
    from dsql_migrator.core.converter import is_name_collision_placeholder, relation_clash_names

    a, b = "app." + "t" * 63 + "_a", "app." + "t" * 63 + "_b"
    inventory = SourceInventory(tables=[_table(a), _table(b), _table("app.ok")])
    conversion = SchemaConverter().convert(inventory)
    placeholders = {
        t.table for t in conversion.tables if is_name_collision_placeholder(t.target_ddl)
    }
    assert relation_clash_names(inventory) == placeholders == {a, b}
    from dsql_migrator.ui.schema_conversion import TABLE_PREFIX

    generated = [TABLE_PREFIX + name for name in (a, b, "app.ok")]
    assert migratable_table_names(inventory, generated, None) == ["app.ok"]


def test_a_placeholder_conversion_is_never_loaded() -> None:
    import pytest

    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        BatchedTableMigrator,
        DataMigrationInputs,
        FullLoadIncompleteError,
    )

    a, b = "app." + "t" * 63 + "_a", "app." + "t" * 63 + "_b"
    inventory = SourceInventory(tables=[_table(a), _table(b)])
    conversion = SchemaConverter().convert(inventory)
    inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        inventory=inventory,
        table_conversions={t.table: t for t in conversion.tables},
    )
    migrator = BatchedTableMigrator(inputs, exporter=object(), watermark_capturer=object())
    with pytest.raises(FullLoadIncompleteError, match="shares its first 63 bytes"):
        migrator.migrate_table(inventory.tables[0])

    # A library caller passes no conversions: each table converts on its own (no
    # placeholder), so the run derives the clash from the inventory instead.
    library = BatchedTableMigrator(
        DataMigrationInputs(
            source_config=SourceConnectionConfig(host="db", database="app"),
            source_password=None,
            target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
            inventory=inventory,
        ),
        exporter=object(),
        watermark_capturer=object(),
    )
    assert library._inputs.name_clash_tables == {a, b}
    with pytest.raises(FullLoadIncompleteError, match="shares its first 63 bytes"):
        library.migrate_table(inventory.tables[1])


def test_a_run_withholds_name_clash_tables_before_any_recreate_or_load() -> None:
    """The thread path loaded both into one relation; the process pre-pass recreated it
    (or aborted the whole run on the placeholder's comment)."""
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        BatchedTableMigrator,
        DataMigrationInputs,
        _migrate_tables_in_parallel,
    )

    a, b = "app." + "t" * 63 + "_a", "app." + "t" * 63 + "_b"
    inventory = SourceInventory(tables=[_table(a), _table(b), _table("app.ok")])
    migrator = BatchedTableMigrator(
        DataMigrationInputs(
            source_config=SourceConnectionConfig(host="db", database="app"),
            source_password=None,
            target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
            inventory=inventory,
        ),
        exporter=object(),
        watermark_capturer=object(),
    )
    loaded: list[str] = []
    migrator.migrate_table = lambda table, **_k: loaded.append(table.name)  # type: ignore[method-assign]

    class _Handle:
        cancelled = False

        def update(self, _mutator):
            pass

        def heartbeat(self):
            pass

    import dsql_migrator.ui.data_migration._full_load_engine as engine

    original = engine.load_config

    def _one_worker():
        cfg = original()
        return cfg.model_copy(update={"full_load_table_parallelism": 1}) if hasattr(cfg, "model_copy") else cfg

    engine.load_config = _one_worker
    errors = ErrorLogStore()
    try:
        _migrate_tables_in_parallel(_Handle(), "job", inventory.tables, migrator, errors)
    finally:
        engine.load_config = original
    assert loaded == ["app.ok"]  # the clash tables never reached a load
    clash_records = [r for r in errors.records("job") if "first 63 bytes" in r.message]
    assert sorted(r.table for r in clash_records) == [a, b]


def test_a_pool_worker_builds_the_same_indexes_as_the_parent_recreate() -> None:
    """A library caller's Drop & reload on the process pool: the parent recreated the table
    (deriving the renamed indexes), and the worker built none of them."""
    from tests.test_ui_data_migration import (
        _FakeExporter,
        _FakeImporter,
        _FakeWatermarkCapturer,
        _watermark,
    )
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        BatchedTableMigrator,
        DataMigrationInputs,
        _effective_conversion,
        _slim_worker_inputs,
    )

    def child(name):
        return TableDef(
            name=name,
            columns=[
                ColumnDef(name="id", mysql_type="bigint", nullable=False),
                ColumnDef(name="user_id", mysql_type="bigint"),
            ],
            primary_key=["id"],
            indexes=[IndexDef(name="user_id", columns=["user_id"])],
        )

    orders, payments = child("shop.orders"), child("shop.payments")
    inventory = SourceInventory(tables=[orders, payments])
    inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="shop"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        inventory=inventory,
        replace_tables=frozenset({"shop.orders", "shop.payments"}),
    )
    parent = BatchedTableMigrator(inputs, exporter=object(), watermark_capturer=object())
    expected = list(_effective_conversion(parent._inputs, orders).index_ddls)
    assert expected == ['CREATE INDEX ASYNC "orders_user_id" ON "shop"."orders" ("user_id")']

    importer = _FakeImporter()
    worker = BatchedTableMigrator(
        _slim_worker_inputs(parent._inputs, orders.name),
        exporter=_FakeExporter(rows_by_table={orders.name: [{"id": 1}]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
        target_counter=lambda _t: 1,
        target_generated_reader=lambda _t: set(),
        target_pk_reader=lambda _t: ["id"],
    )
    worker.migrate_table(orders, pre_recreated=True)
    assert importer.index_ddls_by_table[orders.name] == expected


def test_index_renames_are_derived_only_for_tables_without_an_applied_conversion() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        BatchedTableMigrator,
        DataMigrationInputs,
    )

    inventory = SourceInventory(
        tables=[
            _table("shop.orders", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
            _table("shop.payments", indexes=[IndexDef(name="user_id", columns=["user_id"])]),
        ]
    )
    conversion = SchemaConverter().convert(inventory)

    def build(conversions):
        return BatchedTableMigrator(
            DataMigrationInputs(
                source_config=SourceConnectionConfig(host="db", database="shop"),
                source_password=None,
                target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
                inventory=inventory,
                table_conversions=conversions,
            ),
            exporter=object(),
            watermark_capturer=object(),
        )

    every = build({t.table: t for t in conversion.tables})
    assert every._inputs.index_renames == {}  # the UI's case: nothing to derive
    partial = build({conversion.tables[0].table: conversion.tables[0]})
    assert partial._inputs.index_renames == conversion.index_renames


def test_a_stored_edit_is_renamed_as_written_and_exactly_as_the_apply_paths_rename_it() -> None:
    """The screen shows (and copies) the renamed edit; text in a comment is not a statement
    head, so neither the display nor the apply paths touch it."""
    from dsql_migrator.core.converter import rename_index_script, rename_index_statements
    from dsql_migrator.ui.schema_conversion_apply import split_sql_statements

    script = (
        'CREATE TABLE "shop"."orders" ("id" bigint PRIMARY KEY, "user_id" varchar(50) '
        '-- MySQL KEY became CREATE INDEX ASYNC "user_id" ON "shop"."orders" below\n);\n\n'
        'CREATE UNIQUE INDEX ASYNC "user_id" ON "shop"."orders" ("user_id");'
    )
    renames = {"user_id": "orders_user_id"}
    renamed = rename_index_script(script, renames)
    assert renamed == script.replace(
        'CREATE UNIQUE INDEX ASYNC "user_id"', 'CREATE UNIQUE INDEX ASYNC "orders_user_id"'
    )
    assert split_sql_statements(renamed) == rename_index_statements(
        split_sql_statements(script), renames
    )
    assert rename_index_script(renamed, renames) == renamed


def test_two_databases_whose_names_agree_in_63_bytes_share_one_target_schema() -> None:
    """Aurora DSQL cuts the SCHEMA name too: d...d1.orders and d...d2.orders are one
    relation, so both tables are placeholders (and kept out of the load)."""
    from dsql_migrator.core.converter import is_name_collision_placeholder, relation_clash_names

    one, two = "d" * 63 + "1", "d" * 63 + "2"
    inventory = SourceInventory(tables=[_table(f"{one}.orders"), _table(f"{two}.orders")])
    assert relation_clash_names(inventory) == {f"{one}.orders", f"{two}.orders"}
    conversion = SchemaConverter().convert(inventory)
    assert all(is_name_collision_placeholder(t.target_ddl) for t in conversion.tables)
