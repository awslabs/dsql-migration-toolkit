# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Application anti-pattern linter: static analysis of app SQL/source (Req 7).

This module implements the :class:`AppLinter` component (design.md section
"7. App Anti-Pattern Linter"). It statically scans application SQL or a source
directory for patterns that do not carry over to Aurora DSQL and reports each
finding with its file/location and a recommended action (Requirements 7.1, 7.2,
7.3).

Detected patterns (Requirement 7.2):

- **Pessimistic locking** (``FOR UPDATE``, ``FOR SHARE``, ``LOCK IN SHARE
  MODE``): DSQL uses optimistic concurrency control (OCC) and does not support
  pessimistic locks. The recommendation for these findings includes applying the
  ``40001`` (OC000/OC001) retry middleware when moving to OCC (Requirement 7.3).
- **Foreign-key dependency** (``FOREIGN KEY``): DSQL has no foreign keys;
  referential integrity must move to the application.
- **AUTO_INCREMENT dependency** (``AUTO_INCREMENT``, ``LAST_INSERT_ID``): DSQL
  does not provide MySQL auto-increment semantics.
- **Trigger / stored-routine usage** (``CREATE TRIGGER``, ``CREATE
  PROCEDURE``/``FUNCTION``, ``CALL <name>``): DSQL supports neither triggers nor
  stored procedures.
- **Unsupported MySQL functions** (e.g. ``GROUP_CONCAT``, ``DATE_FORMAT``):
  MySQL-specific functions with no direct Aurora DSQL (PostgreSQL 16) equivalent.

Design choices:

- Detection is plain, case-insensitive, line-based regex scanning so it works on
  arbitrary application source files, not only well-formed SQL (the design notes
  that static/regex scanning is acceptable here). It favors over-reporting a
  candidate (which a human reviews) over silently missing a risk; it does not
  strip comments, so a pattern inside a comment is still reported.
- :class:`AppSource` only holds already-read file contents, so :meth:`AppLinter.scan`
  is pure and easy to test. The :meth:`AppSource.from_directory` / :meth:`AppSource.from_sql`
  constructors perform the file I/O for the directory/SQL inputs.
- Findings reuse the shared :class:`~dsql_migrator.core.models.Classification`
  enum (all anti-patterns are ``MANUAL``: they require human review/redesign),
  matching the assessor and query converter conventions.

Security: input SQL/source is treated as untrusted (Requirement 9.4). It is only
read and pattern-matched as text; it is never executed or evaluated.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import Classification

# Default file extensions scanned by :meth:`AppSource.from_directory`. Covers SQL
# plus the common application languages that embed SQL. Kept small and explicit
# so a directory scan is predictable.
DEFAULT_SOURCE_EXTENSIONS: tuple[str, ...] = (
    ".sql",
    ".py",
    ".java",
    ".js",
    ".ts",
    ".go",
    ".rb",
    ".php",
    ".cs",
    ".kt",
    ".scala",
    ".xml",
)


class AntiPatternType(str, Enum):
    """The kinds of application anti-patterns this linter detects (Req 7.2)."""

    PESSIMISTIC_LOCK = "PESSIMISTIC_LOCK"
    FOREIGN_KEY_DEPENDENCY = "FOREIGN_KEY_DEPENDENCY"
    AUTO_INCREMENT_DEPENDENCY = "AUTO_INCREMENT_DEPENDENCY"
    TRIGGER_OR_ROUTINE_USAGE = "TRIGGER_OR_ROUTINE_USAGE"
    UNSUPPORTED_FUNCTION = "UNSUPPORTED_FUNCTION"


class SourceFile(BaseModel):
    """A single application source file (path + already-read text content)."""

    model_config = ConfigDict(extra="forbid")

    path: str = Field(min_length=1, description="File path used as the finding location.")
    content: str = Field(default="", description="Full text content of the file.")


class AppSource(BaseModel):
    """The application source to scan: one or more in-memory source files.

    The model carries already-read content so :meth:`AppLinter.scan` performs no
    I/O. Use :meth:`from_sql` for an inline SQL snippet or :meth:`from_directory`
    to read a directory tree from disk.
    """

    model_config = ConfigDict(extra="forbid")

    files: list[SourceFile] = Field(default_factory=list)

    @classmethod
    def from_sql(cls, sql: str, *, path: str = "<sql>") -> "AppSource":
        """Build a source from a single inline SQL string."""
        return cls(files=[SourceFile(path=path, content=sql)])

    @classmethod
    def from_directory(
        cls,
        root: str | Path,
        *,
        extensions: tuple[str, ...] = DEFAULT_SOURCE_EXTENSIONS,
    ) -> "AppSource":
        """Read all matching text files under ``root`` into an :class:`AppSource`.

        Files are matched by extension (case-insensitive) and read as UTF-8;
        files that cannot be decoded as text are skipped (treated as binary).
        Paths are returned relative to ``root`` and sorted for deterministic
        output. Raises ``FileNotFoundError`` if ``root`` does not exist.
        """
        root_path = Path(root)
        if not root_path.exists():
            raise FileNotFoundError(f"source path does not exist: {root_path}")

        allowed = {ext.lower() for ext in extensions}
        candidates = (
            [root_path]
            if root_path.is_file()
            else sorted(p for p in root_path.rglob("*") if p.is_file())
        )

        files: list[SourceFile] = []
        for candidate in candidates:
            if candidate.suffix.lower() not in allowed:
                continue
            try:
                content = candidate.read_text(encoding="utf-8")
            except (UnicodeDecodeError, OSError):
                # Skip binary or unreadable files rather than failing the scan.
                continue
            try:
                rel = str(candidate.relative_to(root_path))
            except ValueError:
                rel = str(candidate)
            files.append(SourceFile(path=rel, content=content))
        return cls(files=files)


class AntiPatternFinding(BaseModel):
    """A single anti-pattern occurrence with its location and recommendation.

    ``file``/``line``/``column`` pin the location (Requirement 7.2) and
    ``matched_text`` is the exact text that triggered the finding. ``classification``
    reuses the shared :class:`Classification` enum and ``recommendation`` states
    the action to take (Requirement 7.2/7.3).
    """

    model_config = ConfigDict(extra="forbid")

    pattern: AntiPatternType
    file: str = Field(min_length=1, description="File path where the pattern was found.")
    line: int = Field(ge=1, description="1-based line number of the match.")
    column: int = Field(ge=1, description="1-based column of the match start.")
    matched_text: str = Field(min_length=1, description="The matched source text.")
    classification: Classification = Field(
        description="Severity; all anti-patterns are MANUAL (require review)."
    )
    recommendation: str = Field(min_length=1, description="Recommended action (English).")


# A reusable note appended to pessimistic-lock recommendations: moving off
# pessimistic locking means relying on OCC, which requires retrying on 40001
# serialization failures (Requirement 7.3).
_OCC_RETRY_NOTE = (
    "When moving this transaction to OCC, wrap it with the `40001` (OC000/OC001) "
    "retry middleware (see `with_occ_retry`) so serialization failures are retried "
    "idempotently."
)

# MySQL-specific functions with no direct Aurora DSQL (PostgreSQL 16) equivalent.
# Each maps to a short pointer to the PostgreSQL replacement.
_UNSUPPORTED_FUNCTIONS: dict[str, str] = {
    "GROUP_CONCAT": "use string_agg(...)",
    "DATE_FORMAT": "use to_char(...)",
    "STR_TO_DATE": "use to_timestamp(...)/to_date(...)",
    "UNIX_TIMESTAMP": "use extract(epoch from ...)",
    "FROM_UNIXTIME": "use to_timestamp(...)",
    "SLEEP": "remove; there is no DSQL equivalent",
    "GET_LOCK": "use OCC instead of advisory/named locks",
    "RELEASE_LOCK": "use OCC instead of advisory/named locks",
}


@dataclass(frozen=True)
class _PatternDef:
    """A single detector: a compiled regex plus how to describe its findings."""

    pattern: AntiPatternType
    regex: re.Pattern[str]
    matched_text: Callable[[re.Match[str]], str]
    recommendation: Callable[[re.Match[str]], str]
    classification: Classification = Classification.MANUAL


def _unsupported_function_regex() -> re.Pattern[str]:
    """Build a regex matching any unsupported MySQL function call by name."""
    names = "|".join(re.escape(name) for name in _UNSUPPORTED_FUNCTIONS)
    return re.compile(rf"\b({names})\s*\(", re.IGNORECASE)


def _unsupported_function_recommendation(match: re.Match[str]) -> str:
    """Recommendation for an unsupported-function match, naming the replacement."""
    name = match.group(1).upper()
    hint = _UNSUPPORTED_FUNCTIONS.get(name, "replace with a PostgreSQL equivalent")
    return (
        f"`{name}` is a MySQL-specific function with no direct Aurora DSQL "
        f"(PostgreSQL 16) equivalent; {hint}."
    )


def _default_patterns() -> list[_PatternDef]:
    """Return the ordered list of anti-pattern detectors.

    Order is preserved so findings on the same line/column are deterministic.
    """
    return [
        _PatternDef(
            pattern=AntiPatternType.PESSIMISTIC_LOCK,
            regex=re.compile(
                r"\b(FOR\s+UPDATE|FOR\s+SHARE|LOCK\s+IN\s+SHARE\s+MODE)\b",
                re.IGNORECASE,
            ),
            matched_text=lambda m: m.group(0),
            recommendation=lambda m: (
                "Aurora DSQL uses optimistic concurrency control and does not "
                "support pessimistic locking. Remove the lock and rely on OCC. "
                + _OCC_RETRY_NOTE
            ),
        ),
        _PatternDef(
            pattern=AntiPatternType.FOREIGN_KEY_DEPENDENCY,
            regex=re.compile(r"\bFOREIGN\s+KEY\b", re.IGNORECASE),
            matched_text=lambda m: m.group(0),
            recommendation=lambda m: (
                "Aurora DSQL does not support foreign key constraints. Remove the "
                "foreign key and enforce referential integrity in the application "
                "layer."
            ),
        ),
        _PatternDef(
            pattern=AntiPatternType.AUTO_INCREMENT_DEPENDENCY,
            regex=re.compile(r"\b(AUTO_INCREMENT|LAST_INSERT_ID)\b", re.IGNORECASE),
            matched_text=lambda m: m.group(0),
            recommendation=lambda m: (
                "Aurora DSQL does not provide MySQL AUTO_INCREMENT/LAST_INSERT_ID "
                "semantics and monotonic keys cause hot partitions. Use a "
                "UUID/random key, or an identity/sequence, and do not depend on "
                "sequential IDs."
            ),
        ),
        _PatternDef(
            pattern=AntiPatternType.TRIGGER_OR_ROUTINE_USAGE,
            regex=re.compile(
                r"\b(CREATE\s+TRIGGER|CREATE\s+PROCEDURE|CREATE\s+FUNCTION"
                r"|CALL\s+[A-Za-z_]\w*)\b",
                re.IGNORECASE,
            ),
            matched_text=lambda m: m.group(0),
            recommendation=lambda m: (
                "Aurora DSQL does not support triggers or stored procedures. "
                "Reimplement this logic in the application or with event-driven "
                "processing (e.g., EventBridge/Lambda)."
            ),
        ),
        _PatternDef(
            pattern=AntiPatternType.UNSUPPORTED_FUNCTION,
            regex=_unsupported_function_regex(),
            matched_text=lambda m: m.group(1),
            recommendation=_unsupported_function_recommendation,
        ),
    ]


class AppLinter:
    """Statically scans application SQL/source for DSQL anti-patterns (Req 7).

    The linter is stateless. :meth:`scan` walks each source file line by line and
    applies every detector, returning one :class:`AntiPatternFinding` per match
    with its file/location and recommended action.
    """

    def __init__(self, patterns: list[_PatternDef] | None = None) -> None:
        """Create a linter, optionally overriding the detector list."""
        self._patterns = list(patterns) if patterns is not None else _default_patterns()

    def scan(self, source: AppSource) -> list[AntiPatternFinding]:
        """Return every anti-pattern finding in ``source`` (Requirements 7.1-7.3).

        Files are scanned line by line; for each line every detector's regex is
        applied and a finding is emitted per match. Findings are ordered by file,
        then line, then column, then detector declaration order.
        """
        findings: list[AntiPatternFinding] = []
        for source_file in source.files:
            for line_number, line in enumerate(source_file.content.splitlines(), start=1):
                for pattern_def in self._patterns:
                    for match in pattern_def.regex.finditer(line):
                        findings.append(
                            AntiPatternFinding(
                                pattern=pattern_def.pattern,
                                file=source_file.path,
                                line=line_number,
                                column=match.start() + 1,
                                matched_text=pattern_def.matched_text(match),
                                classification=pattern_def.classification,
                                recommendation=pattern_def.recommendation(match),
                            )
                        )
        return findings


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


class AntiPatternReport(BaseModel):
    """An anti-pattern scan result: the findings plus a per-pattern summary."""

    model_config = ConfigDict(extra="forbid")

    findings: list[AntiPatternFinding] = Field(default_factory=list)
    summary: dict[AntiPatternType, int] = Field(default_factory=dict)

    @classmethod
    def from_findings(cls, findings: list[AntiPatternFinding]) -> "AntiPatternReport":
        """Build a report and compute a complete per-pattern count summary.

        Every :class:`AntiPatternType` is present in the summary (0 when absent),
        so consumers never need to guard against missing keys.
        """
        summary = {pattern: 0 for pattern in AntiPatternType}
        for finding in findings:
            summary[finding.pattern] += 1
        return cls(findings=list(findings), summary=summary)


def render_text_report(report: AntiPatternReport) -> str:
    """Render a human-readable text version of an anti-pattern report (English)."""
    lines = ["Application Anti-Pattern Report", "=" * 31, ""]
    lines.append("Summary (findings by pattern):")
    for pattern in AntiPatternType:
        lines.append(f"  {pattern.value}: {report.summary.get(pattern, 0)}")
    lines.append("")
    lines.append(f"Findings ({len(report.findings)}):")
    for finding in report.findings:
        lines.append(
            f"- {finding.file}:{finding.line}:{finding.column} "
            f"[{finding.pattern.value}] '{finding.matched_text}'"
        )
        lines.append(f"    Recommendation: {finding.recommendation}")
    return "\n".join(lines)


def export_report(report: AntiPatternReport, fmt: str = "json") -> str:
    """Export an anti-pattern report as ``"json"`` or ``"text"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable);
    text is a readable summary. Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        return report.model_dump_json(indent=2)
    if normalized == "text":
        return render_text_report(report)
    raise ValueError(f"unsupported report format: {fmt!r} (use 'json' or 'text')")


__all__ = [
    "AntiPatternType",
    "SourceFile",
    "AppSource",
    "AntiPatternFinding",
    "AppLinter",
    "AntiPatternReport",
    "render_text_report",
    "export_report",
    "DEFAULT_SOURCE_EXTENSIONS",
]
