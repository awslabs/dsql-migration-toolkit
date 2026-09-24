

def test_a_unique_violation_is_classified_but_not_called_a_source_schema_change() -> None:
    """23505 is the only signal that a row may have VANISHED from the target.

    An UPDATE to a re-keyed table's leading key column is replicated as delete(old key) +
    insert(new key). The two carry different keys, so they land on different partitions and
    tasks with no ordering; while both key forms coexist the UNIQUE index over the ORIGINAL
    primary key rejects the insert with 23505. The sink treats 23505 as permanent, so the
    insert is dead-lettered with its offset committed -- and if the delete lands afterwards,
    the row is gone from the target with only an unclassified poison row to show for it.
    """
    from dsql_migrator.core.cdc import SchemaDriftKind, classify_schema_drift
    from dsql_migrator.ui.data_migration._cdc_monitoring import (
        _DRIFT_LABELS,
        _TARGET_SIDE_DRIFT_KINDS,
    )

    assert classify_schema_drift("23505") is SchemaDriftKind.UNIQUE_CONFLICT
    kind = SchemaDriftKind.UNIQUE_CONFLICT.value
    # It is NOT a source DDL change, so the banner must not announce one.
    assert kind in _TARGET_SIDE_DRIFT_KINDS
    label, body = _DRIFT_LABELS[kind]
    # The label must not assert the cause -- any unique index raises 23505, including an
    # out-of-band duplicate -- while the body must say the thing a poison row cannot: a row
    # may be MISSING, and what to do about it.
    assert "re-key" not in label.lower()
    assert "23505" in body
    assert "MISSING" in body
    assert "immutable" in body or "never updates" in body

    # The structural SQLSTATEs keep their existing kinds (no reshuffling).
    assert classify_schema_drift("42703") is SchemaDriftKind.ADD_COLUMN
    assert classify_schema_drift("42P01") is SchemaDriftKind.MISSING_TABLE
    # A per-value data exception is still not drift.
    assert classify_schema_drift("22001") is None
