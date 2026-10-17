"""G72: the review of G56-G67 -- the AI DBA's facts, grounding, DDL tool and object list."""

from __future__ import annotations

import json
from types import SimpleNamespace


def test_the_dlq_cards_ai_facts_carry_no_row_values(monkeypatch) -> None:
    """The DLQ card's 'Ask AI DBA' grounding sent each record's pk, its SQL and (from a
    pre-v29 sink) the failing row to Bedrock, while the chat's DLQ tool stripped them."""
    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration import _cdc_status

    records = [
        CdcConnectorError(
            table="app.users", error_code="23502",
            message=(
                "DLQ offset=41: sqlstate=23502 null value in column \"name\" violates not-null "
                "constraint\n  Detail: Failing row contains (15, alice@example.com, null). "
                "| pk: id=15 | op: c/r | sql: INSERT INTO app.users (id, email) VALUES (?, ?)"
            ),
        ),
        CdcConnectorError(
            table="app.users", error_code="22P02",
            message='sqlstate=22P02 invalid input syntax for type integer: "4111111111111111"',
        ),
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda *_a: records)
    monkeypatch.setattr(cm, "_cdc_dlq_proven", lambda *_a: {})
    state = SimpleNamespace(cdc_activity=None, migration_type=None, job_id=None)
    facts = cm._cdc_dlq_facts(state, "k")
    for leaked in ("id=15", "alice@example.com", "INSERT INTO", "4111111111111111", "Failing row"):
        assert leaked not in facts, leaked
    assert "violates not-null constraint" in facts and "23502" in facts


def test_the_grounding_does_not_promise_generation_for_every_key() -> None:
    from dsql_migrator.core.assessment_strategist import DSQL_CONSTRAINTS

    assert "kept WITHOUT generation" in DSQL_CONSTRAINTS
    # G84: a numeric key fed by a sequence is narrowed to a bigint identity, not kept.
    assert "narrowed to bigint" in DSQL_CONSTRAINTS
    assert "MySQL" not in DSQL_CONSTRAINTS  # engine-neutral: PostgreSQL prompts carry it


def _executor(eval_result, conv_state=None, source_type=None):
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor

    session = SimpleNamespace(source_config=SimpleNamespace(source_type=source_type or SourceType.MYSQL))
    stores = {
        "session_store": SimpleNamespace(get_or_create=lambda _sid: session, get=lambda _sid: session),
        "evaluation_store": SimpleNamespace(
            get_or_create=lambda _sid: SimpleNamespace(result=eval_result),
            get=lambda _sid: SimpleNamespace(result=eval_result),
        ),
        "schema_conversion_store": SimpleNamespace(
            get_or_create=lambda _sid: conv_state, get=lambda _sid: conv_state
        ),
        "data_migration_store": SimpleNamespace(get_or_create=lambda _sid: None, get=lambda _sid: None),
        "validation_store": SimpleNamespace(get_or_create=lambda _sid: None, get=lambda _sid: None),
    }
    return build_ai_tool_executor(
        session_id="s1", job_manager=None, full_load_rate_eta=lambda *a, **k: (None, None), **stores
    )


def test_the_object_list_is_paged_to_fit_the_tool_bound() -> None:
    """200 entries outgrew the 12,000-character bound: the model got ~140, cut mid-entry,
    under 'listed: 200', and was told to narrow a request that had no paging."""
    from dsql_migrator.core.assessment_strategist import bounded_tool_output
    from dsql_migrator.core.models import AssessmentItem, AssessmentReport, Classification

    items = [
        AssessmentItem(object_name=f"shop.order_line_archive_{i:04d}", kind="TABLE",
                       rule_id="COMPATIBLE", classification=Classification.AUTO,
                       risk="", recommendation="")
        for i in range(300)
    ]
    result = SimpleNamespace(assessment=AssessmentReport.from_items(items))
    ex = _executor(result)
    first = json.loads(ex("list_objects_by_status", {"classification": "AUTO"}))
    raw = ex("list_objects_by_status", {"classification": "AUTO"})
    assert bounded_tool_output(raw) == raw  # never cut
    assert first["listed"] == len(first["objects"]) and first["next_offset"] == first["listed"]
    seen = list(first["objects"])
    offset = first["next_offset"]
    while offset is not None:
        page = json.loads(ex("list_objects_by_status", {"classification": "AUTO", "offset": offset}))
        seen.extend(page["objects"])
        offset = page["next_offset"]
    assert [o["object_name"] for o in seen] == [i.object_name for i in items]


def test_the_ddl_tool_returns_the_edit_as_it_is_applied() -> None:
    """It returned the stored text: a pre-rename index name, and foreign keys the operator
    had turned off -- not what Schema Conversion shows and Apply runs."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import (
        ColumnDef, ForeignKeyDef, IndexDef, SourceInventory, TableDef,
    )
    from dsql_migrator.ui.schema_conversion import SchemaConversionState, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import build_keep_conversion

    def table(name: str, **kw) -> TableDef:
        return TableDef(
            name=name,
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                     ColumnDef(name="status", mysql_type="int"),
                     ColumnDef(name="user_id", mysql_type="int")],
            primary_key=["id"],
            indexes=[IndexDef(name="idx_status", columns=["status"])],
            **kw,
        )

    orders = table("shop.orders", foreign_keys=[ForeignKeyDef(
        name="fk_orders_user", columns=["user_id"], referenced_table="shop.users",
        referenced_columns=["id"],
    )])
    inventory = SourceInventory(tables=[orders, table("shop.users")])
    result = SimpleNamespace(inventory=inventory, assessment=None)
    conv = SchemaConversionState()
    stored = render_target_ddl(build_keep_conversion(SchemaConverter(), orders))
    assert 'INDEX ASYNC "idx_status"' in stored  # stored before the rename map existed
    conv.set_edited_target_ddl("shop.orders", stored)
    ex = _executor(result, conv)
    ddl = json.loads(ex("get_converted_ddl", {"object_name": "shop.orders"}))["target_ddl"]
    assert '"orders_idx_status"' in ddl and 'INDEX ASYNC "idx_status"' not in ddl
    assert "FOREIGN KEY" in ddl
    conv.preserve_foreign_keys = False
    ddl = json.loads(ex("get_converted_ddl", {"object_name": "shop.orders"}))["target_ddl"]
    assert "FOREIGN KEY" not in ddl
