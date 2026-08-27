# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the application anti-pattern linter.

Covers per-pattern detection (pessimistic locks, foreign-key dependency,
AUTO_INCREMENT dependency, trigger/stored-routine usage, unsupported MySQL
functions), location reporting, the OCC `40001` retry middleware recommendation,
directory scanning, and report export.

Requirements covered: 7.1, 7.2, 7.3.
"""

from __future__ import annotations

import json

from dsql_migrator.core.linter import (
    AntiPatternReport,
    AntiPatternType,
    AppLinter,
    AppSource,
    export_report,
    render_text_report,
)
from dsql_migrator.core.models import Classification


def _scan_sql(sql: str) -> list:
    """Scan a single inline SQL string with a fresh linter."""
    return AppLinter().scan(AppSource.from_sql(sql))


def _patterns(findings) -> set[AntiPatternType]:
    """Return the set of anti-pattern types present in the findings."""
    return {finding.pattern for finding in findings}


# ---------------------------------------------------------------------------
# Per-pattern detection (Requirement 7.2)
# ---------------------------------------------------------------------------


def test_detects_for_update_pessimistic_lock() -> None:
    findings = _scan_sql("SELECT * FROM t WHERE id = 1 FOR UPDATE")
    assert AntiPatternType.PESSIMISTIC_LOCK in _patterns(findings)
    finding = next(
        f for f in findings if f.pattern is AntiPatternType.PESSIMISTIC_LOCK
    )
    assert finding.classification is Classification.MANUAL
    assert finding.matched_text.upper() == "FOR UPDATE"


def test_detects_lock_in_share_mode_as_pessimistic_lock() -> None:
    findings = _scan_sql("SELECT * FROM t WHERE id = 1 LOCK IN SHARE MODE")
    assert AntiPatternType.PESSIMISTIC_LOCK in _patterns(findings)


def test_detects_for_share_as_pessimistic_lock() -> None:
    findings = _scan_sql("SELECT * FROM t WHERE id = 1 FOR SHARE")
    assert AntiPatternType.PESSIMISTIC_LOCK in _patterns(findings)


def test_foreign_key_is_no_longer_flagged() -> None:
    # Aurora DSQL now supports enforced foreign keys (2026-08), so a FOREIGN KEY in
    # application SQL is no longer an anti-pattern and must NOT be flagged.
    findings = _scan_sql(
        "ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers(id)"
    )
    assert AntiPatternType.FOREIGN_KEY_DEPENDENCY not in _patterns(findings)


def test_detects_auto_increment_dependency() -> None:
    findings = _scan_sql("CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY)")
    assert AntiPatternType.AUTO_INCREMENT_DEPENDENCY in _patterns(findings)


def test_detects_last_insert_id_as_auto_increment_dependency() -> None:
    findings = _scan_sql("SELECT LAST_INSERT_ID()")
    assert AntiPatternType.AUTO_INCREMENT_DEPENDENCY in _patterns(findings)


def test_detects_stored_procedure_call() -> None:
    findings = _scan_sql("CALL recalc_totals(42)")
    assert AntiPatternType.TRIGGER_OR_ROUTINE_USAGE in _patterns(findings)


def test_detects_create_trigger() -> None:
    findings = _scan_sql("CREATE TRIGGER trg_audit BEFORE INSERT ON t FOR EACH ROW SET @x = 1")
    assert AntiPatternType.TRIGGER_OR_ROUTINE_USAGE in _patterns(findings)


def test_detects_unsupported_function() -> None:
    findings = _scan_sql("SELECT GROUP_CONCAT(name) FROM t")
    assert AntiPatternType.UNSUPPORTED_FUNCTION in _patterns(findings)
    finding = next(
        f for f in findings if f.pattern is AntiPatternType.UNSUPPORTED_FUNCTION
    )
    assert finding.matched_text.upper() == "GROUP_CONCAT"
    assert "string_agg" in finding.recommendation.lower()


def test_detects_date_format_unsupported_function() -> None:
    findings = _scan_sql("SELECT DATE_FORMAT(created_at, '%Y') FROM t")
    assert AntiPatternType.UNSUPPORTED_FUNCTION in _patterns(findings)


# ---------------------------------------------------------------------------
# Location reporting (Requirement 7.2)
# ---------------------------------------------------------------------------


def test_finding_reports_file_and_location() -> None:
    sql = "SELECT 1;\nSELECT * FROM t WHERE id = 1 FOR UPDATE;"
    findings = AppLinter().scan(AppSource.from_sql(sql, path="app/query.sql"))
    finding = next(
        f for f in findings if f.pattern is AntiPatternType.PESSIMISTIC_LOCK
    )
    assert finding.file == "app/query.sql"
    assert finding.line == 2
    assert finding.column >= 1


# ---------------------------------------------------------------------------
# OCC retry middleware recommendation (Requirement 7.3)
# ---------------------------------------------------------------------------


def test_pessimistic_lock_recommends_40001_retry_middleware() -> None:
    findings = _scan_sql("SELECT * FROM t WHERE id = 1 FOR UPDATE")
    finding = next(
        f for f in findings if f.pattern is AntiPatternType.PESSIMISTIC_LOCK
    )
    recommendation = finding.recommendation.lower()
    assert "40001" in recommendation
    assert "occ" in recommendation
    assert "retry" in recommendation


# ---------------------------------------------------------------------------
# Clean source and multiple findings
# ---------------------------------------------------------------------------


def test_clean_sql_produces_no_findings() -> None:
    findings = _scan_sql("SELECT id, name FROM t WHERE id = 1")
    assert findings == []


def test_case_insensitive_detection() -> None:
    findings = _scan_sql("select * from t where id = 1 for update")
    assert AntiPatternType.PESSIMISTIC_LOCK in _patterns(findings)


def test_multiple_patterns_in_one_source() -> None:
    sql = (
        "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY);\n"
        "ALTER TABLE t ADD FOREIGN KEY (p) REFERENCES p(id);\n"  # supported now, not flagged
        "SELECT GROUP_CONCAT(x) FROM t WHERE id = 1 FOR UPDATE;\n"
        "CALL do_work();"
    )
    patterns = _patterns(AppLinter().scan(AppSource.from_sql(sql)))
    assert patterns == {
        AntiPatternType.AUTO_INCREMENT_DEPENDENCY,
        AntiPatternType.UNSUPPORTED_FUNCTION,
        AntiPatternType.PESSIMISTIC_LOCK,
        AntiPatternType.TRIGGER_OR_ROUTINE_USAGE,
    }


# ---------------------------------------------------------------------------
# Directory scanning (Requirement 7.1)
# ---------------------------------------------------------------------------


def test_from_directory_scans_matching_files(tmp_path) -> None:
    (tmp_path / "a.sql").write_text("SELECT * FROM t FOR UPDATE", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "b.py").write_text("cur.execute('SELECT LAST_INSERT_ID()')", encoding="utf-8")
    # A non-source file should be ignored.
    (tmp_path / "notes.txt").write_text("SELECT * FROM t FOR UPDATE", encoding="utf-8")

    source = AppSource.from_directory(tmp_path)
    findings = AppLinter().scan(source)

    scanned_files = {finding.file for finding in findings}
    assert any(f.endswith("a.sql") for f in scanned_files)
    assert any(f.endswith("b.py") for f in scanned_files)
    assert not any(f.endswith("notes.txt") for f in scanned_files)


def test_from_directory_missing_path_raises(tmp_path) -> None:
    missing = tmp_path / "does-not-exist"
    try:
        AppSource.from_directory(missing)
    except FileNotFoundError as exc:
        assert "does not exist" in str(exc)
    else:  # pragma: no cover - the call must raise
        raise AssertionError("expected FileNotFoundError for a missing path")


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def test_report_summary_counts_findings_by_pattern() -> None:
    findings = _scan_sql(
        "SELECT GROUP_CONCAT(x) FROM t WHERE id = 1 FOR UPDATE"
    )
    report = AntiPatternReport.from_findings(findings)
    assert report.summary[AntiPatternType.PESSIMISTIC_LOCK] == 1
    assert report.summary[AntiPatternType.UNSUPPORTED_FUNCTION] == 1
    assert report.summary[AntiPatternType.FOREIGN_KEY_DEPENDENCY] == 0
    # Every pattern is present in the summary.
    assert set(report.summary) == set(AntiPatternType)


def test_export_report_json_is_valid_and_round_trips() -> None:
    findings = _scan_sql("SELECT * FROM t FOR UPDATE")
    report = AntiPatternReport.from_findings(findings)
    payload = export_report(report, "json")
    parsed = json.loads(payload)
    assert "findings" in parsed and "summary" in parsed
    assert AntiPatternReport.model_validate(parsed) == report


def test_export_report_text_contains_summary_and_findings() -> None:
    findings = _scan_sql("SELECT * FROM t FOR UPDATE")
    report = AntiPatternReport.from_findings(findings)
    text = export_report(report, "text")
    assert "Application Anti-Pattern Report" in text
    assert "PESSIMISTIC_LOCK" in text
    assert render_text_report(report) == text


def test_export_report_rejects_unknown_format() -> None:
    report = AntiPatternReport.from_findings([])
    try:
        export_report(report, "xml")
    except ValueError as exc:
        assert "unsupported report format" in str(exc)
    else:  # pragma: no cover - the call must raise
        raise AssertionError("expected ValueError for unknown format")
