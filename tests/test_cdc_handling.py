

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


def test_the_teardown_estimate_is_engine_aware_and_single_sourced() -> None:
    """Eight UI/doc sites wrote this estimate out, all engine-blind and mutually inconsistent.

    The wall clock is dominated by ONE resource: the in-VPC offset-seeder Lambda, whose
    hyperplane ENIs AWS reclaims asynchronously (measured 18m30s, against MSK Serverless's
    93s -- both recorded in the cdc-stack template). That Lambda's CloudFormation condition
    requires IsMySqlSource AND SeedMode=Lambda, and PostgreSQL is forced to the external-seed
    path, so a PostgreSQL stack never has it. A real PostgreSQL teardown was measured at
    2m07s while every surface promised "~15-25 min" / "up to ~20 min" / "~15-45 min" /
    "~45 min" for the same operation.
    """
    from dsql_migrator.core.cdc import (
        CDC_TEARDOWN_ESTIMATE_NO_SEEDER,
        CDC_TEARDOWN_ESTIMATE_WITH_SEEDER,
        cdc_teardown_estimate,
        cdc_teardown_reason,
        stack_has_seeder_lambda,
    )
    from dsql_migrator.core.models import SourceType

    # The predicate mirrors the template's condition chain, not "is it PostgreSQL".
    assert stack_has_seeder_lambda(SourceType.POSTGRES, "lambda") is False
    assert stack_has_seeder_lambda(SourceType.POSTGRES, None) is False
    assert stack_has_seeder_lambda(SourceType.MYSQL, "external") is False
    assert stack_has_seeder_lambda(SourceType.MYSQL, "lambda") is True
    # Unknown seed mode on MySQL stays conservative: the template default IS lambda.
    assert stack_has_seeder_lambda(SourceType.MYSQL, None) is True

    assert cdc_teardown_estimate(has_seeder_lambda=True) == CDC_TEARDOWN_ESTIMATE_WITH_SEEDER
    assert cdc_teardown_estimate(has_seeder_lambda=False) == CDC_TEARDOWN_ESTIMATE_NO_SEEDER
    # The no-seeder estimate must actually be the short one, or the fix is cosmetic.
    assert CDC_TEARDOWN_ESTIMATE_NO_SEEDER != CDC_TEARDOWN_ESTIMATE_WITH_SEEDER
    assert "2" in CDC_TEARDOWN_ESTIMATE_NO_SEEDER

    # Unknown engine gets a range covering both -- still better than four fixed numbers that
    # disagree with each other.
    both = cdc_teardown_estimate(has_seeder_lambda=None)
    assert "2" in both and "25" in both

    # The reason must only blame the ENIs when the Lambda is actually there.
    assert "network interfaces" in cdc_teardown_reason(has_seeder_lambda=True)
    assert "no in-VPC seeder Lambda" in cdc_teardown_reason(has_seeder_lambda=False)


def test_no_ui_surface_hardcodes_a_teardown_estimate() -> None:
    """Single source of truth, ENFORCED: the numbers drifted precisely because they were
    typed out per site (~15-25 / up to ~20 / ~15-45 / ~45 for one operation, all
    engine-blind).

    Checked over STRING LITERALS via the AST, excluding docstrings -- the history of this
    defect is written into several comments and docstrings, and a plain text scan flagged
    those prose mentions while missing nothing real (verified: the first version of this test
    reported three docstrings and zero rendered strings).
    """
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
    banned = ("15–25 min", "15-25 min", "up to ~20 min", "15–45 min", "15-45 min", "~45 min")
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        if path.name == "cdc.py":  # the single source of truth may name them
            continue
        text = path.read_text(encoding="utf-8")
        if not any(token in text for token in banned):
            continue
        tree = ast.parse(text)
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(
                node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
            ):
                if node.body and isinstance(node.body[0], ast.Expr):
                    first = node.body[0].value
                    if isinstance(first, ast.Constant) and isinstance(first.value, str):
                        docstrings.add(id(first))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Constant) and isinstance(node.value, str)):
                continue
            if id(node) in docstrings:
                continue
            for token in banned:
                if token in node.value:
                    offenders.append(f"{path.name}:{node.lineno} {token!r}")
    assert not offenders, (
        "teardown estimates must come from cdc_teardown_estimate(), not a literal:\n  "
        + "\n  ".join(sorted(set(offenders)))
    )
