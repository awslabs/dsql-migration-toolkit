# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A RESTORED PostgreSQL session keeps its engine past the Data Migration screen and Cut over.

An older snapshot with no source host restores the engine WITHOUT a ``source_config``
(``session_persistence.apply_session_snapshot`` -> ``set_restored_source_type``).
tests/test_rr_a2_restored_engine.py pins the Cut over and Data Migration reads; these pin
the rest of the app, each of which still read the engine off ``source_config`` and so took
the restored PostgreSQL session for MySQL.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

from dsql_migrator.core.models import SourceConnectionConfig, SourceType


def _fresh_states():
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    return (
        SessionStore().get_or_create("s1"),
        EvaluationStore().get_or_create("s1"),
        SchemaConversionStore().get_or_create("s1"),
        DataMigrationStore().get_or_create("s1"),
    )


# -- The next save keeps the restored engine --------------------------------------------------


def test_a_restored_postgresql_engine_survives_the_next_save_and_restore() -> None:
    # The first persist poll after a restore re-captures the session. Capture read the
    # engine from a live source_config only, so it saved source_type=None over the old
    # snapshot and the restart after that brought the session back as MySQL.
    from dsql_migrator.ui import session_persistence as sp
    from dsql_migrator.ui.data_migration import session_source_type

    session, ev, cv, dm = _fresh_states()
    sp.apply_session_snapshot(sp.SessionSnapshot(session_id="s1", source_type="postgres"),
                              session, ev, cv, dm)
    assert session.source_config is None  # the premise: a host-less (older) snapshot
    assert session_source_type(session) is SourceType.POSTGRES

    saved = sp.capture_session_snapshot("s1", session, ev, cv, dm)
    assert saved.source_type == "postgres"
    again, ev2, cv2, dm2 = _fresh_states()
    sp.apply_session_snapshot(saved, again, ev2, cv2, dm2)
    assert session_source_type(again) is SourceType.POSTGRES


def test_a_live_source_still_decides_the_saved_engine_and_no_source_saves_none() -> None:
    from dsql_migrator.ui import session_persistence as sp

    session, ev, cv, dm = _fresh_states()
    assert sp.capture_session_snapshot("s1", session, ev, cv, dm).source_type is None
    session.set_source(SourceConnectionConfig(source_type=SourceType.MYSQL, host="h",
                                              database="d", username="u"), None)
    session.set_restored_source_type(SourceType.POSTGRES)  # a stale hint: the live one wins
    assert sp.capture_session_snapshot("s1", session, ev, cv, dm).source_type == "mysql"
