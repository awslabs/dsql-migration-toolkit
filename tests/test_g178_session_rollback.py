# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G178: a saved session that names fields this build does not know is restored without them.

The session models are ``extra="forbid"``, so a snapshot saved by a later version -- the
image rolled back after an upgrade -- failed to validate, and both stores ignored it: the
session started fresh, and its next save overwrote the snapshot, losing the Evaluation
result, the edited target DDL and the job, CDC and validation linkage. A payload whose only
problem is unknown fields is now restored without them, their names logged and never their
values; a payload with any other error is still ignored.

Doubles only: no database, no AWS.
"""

from __future__ import annotations

import json
import logging

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    SourceInventory,
    TableDef,
)
from dsql_migrator.core.session_state_store import (
    S3SessionStateStore,
    SessionSnapshot,
    SqliteSessionStateStore,
)

_LOGGER_NAME = "dsql_migrator.core.session_state_store"
_VALUE = "VALUE-NEVER-LOGGED"


def _snapshot() -> SessionSnapshot:
    return SessionSnapshot(
        session_id="later",
        inventory=SourceInventory(
            tables=[
                TableDef(
                    name="shop.orders",
                    columns=[
                        ColumnDef(name="id", mysql_type="int", nullable=False),
                        ColumnDef(name="customer_id", mysql_type="int"),
                    ],
                    primary_key=["id"],
                    indexes=[IndexDef(name="ix_customer", columns=["customer_id"])],
                    foreign_keys=[
                        ForeignKeyDef(
                            name="fk_customer",
                            columns=["customer_id"],
                            referenced_table="shop.customers",
                            referenced_columns=["id"],
                        )
                    ],
                )
            ],
            server_settings={"time_zone": "Asia/Seoul", "sql_mode": "STRICT_TRANS_TABLES"},
        ),
        generated_node_ids=["table:shop.orders"],
        edited_target_ddls={"shop.orders": "CREATE TABLE orders (id integer PRIMARY KEY)"},
        preserve_foreign_keys=False,
        migration_job_id="job-1",
        migration_type="full_load_and_cdc",
    )


def _later_payload(**known_overrides: object) -> str:
    """The snapshot as a later version would save it: unknown fields at three depths."""
    data = json.loads(_snapshot().model_dump_json())
    data.update(known_overrides)
    data["a_field_from_a_later_version"] = _VALUE
    data["inventory"]["a_later_inventory_field"] = {"k": _VALUE}
    data["inventory"]["tables"][0]["indexes"][0]["a_later_index_flag"] = _VALUE
    data["inventory"]["tables"][0]["foreign_keys"][0]["a_later_key_flag"] = True
    return json.dumps(data)


def _sqlite_with(tmp_path, payload: str) -> SqliteSessionStateStore:  # noqa: ANN001
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    store._conn.execute(  # noqa: SLF001 - a row another version wrote
        "INSERT INTO sessions (session_id, payload, updated_at) VALUES (?, ?, ?)",
        ("later", payload, "2026-10-24T00:00:00Z"),
    )
    store._conn.commit()  # noqa: SLF001
    return store


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _S3Holding:
    """An S3 double holding one object another version wrote."""

    def __init__(self, key: str, payload: str) -> None:
        self._objects = {key: payload.encode("utf-8")}

    def get_object(self, *, Bucket, Key):  # noqa: N803
        return {"Body": _Body(self._objects[Key])}


def _s3_with(payload: str) -> S3SessionStateStore:
    return S3SessionStateStore("b", s3_client=_S3Holding("sessions/later.json", payload))


@pytest.mark.parametrize("store_kind", ["sqlite", "s3"])
def test_a_session_saved_by_a_later_version_is_restored_without_the_unknown_fields(
    tmp_path, caplog, store_kind: str  # noqa: ANN001
) -> None:
    payload = _later_payload()
    store = _sqlite_with(tmp_path, payload) if store_kind == "sqlite" else _s3_with(payload)

    with caplog.at_level(logging.WARNING, logger=_LOGGER_NAME):
        loaded = store.load("later")

    # Everything this build knows comes back, as it was saved.
    assert loaded == _snapshot()
    # The dropped fields are named by their path, the list positions left out.
    logged = caplog.text
    for path in (
        "a_field_from_a_later_version",
        "inventory.a_later_inventory_field",
        "inventory.tables.indexes.a_later_index_flag",
        "inventory.tables.foreign_keys.a_later_key_flag",
    ):
        assert path in logged
    assert _VALUE not in logged  # names only, never the values


@pytest.mark.parametrize("store_kind", ["sqlite", "s3"])
def test_a_payload_with_another_error_is_still_ignored(
    tmp_path, store_kind: str  # noqa: ANN001
) -> None:
    # An unknown field next to a value this build cannot read: not restored by halves.
    payload = _later_payload(preserve_foreign_keys="not-a-bool")
    store = _sqlite_with(tmp_path, payload) if store_kind == "sqlite" else _s3_with(payload)

    assert store.load("later") is None  # ignored, no exception


def test_a_snapshot_this_build_saves_round_trips_unchanged(tmp_path) -> None:  # noqa: ANN001
    store = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite"))
    store.save(_snapshot())

    loaded = SqliteSessionStateStore(str(tmp_path / "sessions.sqlite")).load("later")

    assert loaded == _snapshot()
    assert loaded.model_dump_json() == _snapshot().model_dump_json()
