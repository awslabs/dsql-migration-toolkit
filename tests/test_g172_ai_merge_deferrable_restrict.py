# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G172 (K24): an AI reply that changes only a DEFERRABLE key's RESTRICT is not dropped.

The AI merge (``merge_object_ddl``) treats a restatement that spells out a PostgreSQL default
as the statement it restates (G117), and took ON DELETE / ON UPDATE RESTRICT for NO ACTION
-- right for a key that is not deferrable, which the converter writes NO ACTION. G152 keeps
RESTRICT on a DEFERRABLE key, where the two differ (PostgreSQL never defers a RESTRICT check),
so a reply that changed only that action was taken for the current key and silently dropped.
Now RESTRICT is a default only on a key that is not deferrable (INITIALLY DEFERRED implies
DEFERRABLE), mirroring the converter's ``_normalize_fk_action``.
"""

from __future__ import annotations

import pytest

from dsql_migrator.ui.schema_conversion_apply import _key_definition, merge_object_ddl

_HEAD = (
    'ALTER TABLE "rev"."child" ADD CONSTRAINT "child_full" FOREIGN KEY ("a", "b") '
    'REFERENCES "rev"."parent" ("a", "b") MATCH FULL'
)
_UNNAMED = 'ALTER TABLE rev.child ADD FOREIGN KEY (a, b) REFERENCES rev.parent (a, b) MATCH FULL'


@pytest.mark.parametrize(
    "timing", ["DEFERRABLE INITIALLY DEFERRED", "DEFERRABLE INITIALLY IMMEDIATE",
               "INITIALLY DEFERRED", "DEFERRABLE"],
)
def test_restrict_is_another_definition_on_a_deferrable_key(timing: str) -> None:
    restrict = f"{_HEAD} ON DELETE RESTRICT {timing} NOT VALID"
    no_action = f"{_UNNAMED} ON DELETE NO ACTION {timing} NOT VALID"
    assert _key_definition(restrict) != _key_definition(no_action)
    assert _key_definition(f"{_HEAD} ON UPDATE RESTRICT {timing} NOT VALID") != _key_definition(
        f"{_UNNAMED} {timing} NOT VALID"
    )


@pytest.mark.parametrize("timing", ["", "NOT DEFERRABLE", "NOT DEFERRABLE INITIALLY IMMEDIATE"])
def test_restrict_stays_no_action_on_a_key_that_is_not_deferrable(timing: str) -> None:
    assert _key_definition(f"{_HEAD} ON DELETE RESTRICT {timing} NOT VALID") == _key_definition(
        f"{_UNNAMED} ON DELETE NO ACTION {timing} NOT VALID"
    )


def test_the_reply_that_changes_only_restrict_is_not_dropped() -> None:
    table = 'CREATE TABLE "rev"."child" ("a" INT, "b" INT, "id" INT NOT NULL, PRIMARY KEY ("id"));'
    current = f"{table}\n{_HEAD} ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED NOT VALID;"
    reply = f"{_UNNAMED} ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED NOT VALID"
    script, _kept = merge_object_ddl(current, [table.rstrip(";"), reply], "rev.child")
    # Another definition: added beside the current key, visibly, as any changed action is.
    assert reply in script and "ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED" in script
