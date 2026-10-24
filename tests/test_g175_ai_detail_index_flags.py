# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G175 (CUX-11): the AI source detail names a UNIQUE ... NULLS NOT DISTINCT index.

G151 keeps NULLS NOT DISTINCT on the target, and the AI tool's source detail carried the
exclusion constraints, temporal and deferrable keys and FK flags of G151/G152 but not the
index flag: asked to write or check a table's target DDL, the assistant saw only
``{"unique": true}`` and could propose a plain unique index that accepts the NULL duplicates
the source refuses. Likewise a PostgreSQL 18 NOT NULL ... NOT VALID column (G167), read as
nullable, is now flagged so the assistant does not take it for an ordinary nullable column.
"""

from __future__ import annotations

import json

from dsql_migrator.core.models import ColumnDef, IndexDef, SourceInventory, TableDef


def _execute(inventory: SourceInventory):  # noqa: ANN202
    from dsql_migrator.ui.ai_tools import build_ai_tool_executor

    class _Obj:
        def __init__(self, **kw) -> None:  # noqa: ANN003
            self.__dict__.update(kw)

    class _Store:
        def __init__(self, obj=None, *, get_returns=None) -> None:  # noqa: ANN001
            self._obj, self._get = obj, get_returns

        def get_or_create(self, _sid):  # noqa: ANN001, ANN202
            return self._obj

        def get(self, _sid):  # noqa: ANN001, ANN202
            return self._get

    return build_ai_tool_executor(
        session_id="s1",
        session_store=_Store(_Obj(target_config=None, target_verified=False,
                                  source_config=None, source_password=None)),
        evaluation_store=_Store(get_returns=_Obj(result=_Obj(inventory=inventory))),
        schema_conversion_store=_Store(_Obj(generated_node_ids=[], apply_results=[])),
        validation_store=_Store(_Obj(result=None)),
        data_migration_store=_Store(_Obj(job_id=None, get_prereq_report=lambda _m: None)),
        job_manager=_Obj(),
        full_load_rate_eta=lambda *_a, **_k: (None, None),
    )


def test_the_source_detail_names_the_flags_when_set() -> None:
    table = TableDef(
        name="public.users",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="tenant", mysql_type="integer", nullable=True,
                           not_null_not_valid=True),
                 ColumnDef(name="email", mysql_type="text")],
        primary_key=["id"],
        indexes=[IndexDef(name="users_tenant_email_key", columns=["tenant", "email"],
                          unique=True, nulls_not_distinct=True),
                 IndexDef(name="users_email_idx", columns=["email"])],
    )
    execute = _execute(SourceInventory(tables=[table]))
    detail = json.loads(execute("get_source_object_detail", {"object_name": "public.users"}))
    indexes = {i["name"]: i for i in detail["indexes"]}
    assert indexes["users_tenant_email_key"]["nulls_not_distinct"] is True
    assert "nulls_not_distinct" not in indexes["users_email_idx"]
    columns = {c["name"]: c for c in detail["columns"]}
    assert columns["tenant"]["not_null_not_valid"] is True
    assert "not_null_not_valid" not in columns["email"]
