"""Post-migration statistics review (Req 13.3).

After a load completes, the user reviews per-table and per-object migration
statistics in one place and exports them. :class:`MigrationStatsBuilder`
**combines existing outputs** -- it never recomputes anything (Req 13.4):

- rows loaded per table from the :class:`~dsql_migrator.core.models.MigrationJob`
  chunk states,
- source/target row counts and the match verdict from a
  :class:`~dsql_migrator.core.models.ValidationReport` (when validation ran),
- error counts per table from the single
  :class:`~dsql_migrator.core.models.ErrorLogSummary` (Req 13.2 / Property 15),
- per-object apply outcome (CREATED/SKIPPED/FAILED) from the Schema Applier's
  :class:`~dsql_migrator.core.models.ApplyResult` list.

The report renders to JSON / NDJSON / CSV for download (English headers/fields,
credential-free -- Property 7).
"""

from __future__ import annotations

import csv
import io
import json
from typing import Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import (
    ApplyResult,
    ApplyStatus,
    ErrorLogSummary,
    MigrationJob,
    ValidationReport,
)

# CSV columns for the per-table migration statistics export.
_TABLE_CSV_FIELDS = (
    "table",
    "rows_loaded",
    "source_rows",
    "target_rows",
    "matched",
    "errors",
)


class TableMigrationStats(BaseModel):
    """Combined per-table migration statistics (no recomputation -- Req 13.4)."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    rows_loaded: int = Field(default=0, ge=0)
    source_rows: Optional[int] = Field(default=None, ge=0)
    target_rows: Optional[int] = Field(default=None, ge=0)
    matched: Optional[bool] = None
    errors: int = Field(default=0, ge=0)


class ObjectMigrationStats(BaseModel):
    """Per-object schema-apply outcome for the statistics review.

    ``kind`` is best-effort (``OBJECT`` unless richer apply metadata is supplied),
    since :class:`ApplyResult` only carries the object name and status.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    apply_status: ApplyStatus
    kind: str = Field(default="OBJECT", min_length=1)


class MigrationStatsReport(BaseModel):
    """Combined table + object migration statistics, with export rendering."""

    model_config = ConfigDict(extra="forbid")

    tables: list[TableMigrationStats] = Field(default_factory=list)
    objects: list[ObjectMigrationStats] = Field(default_factory=list)

    def render(self, fmt: Literal["json", "ndjson", "csv"] = "json") -> bytes:
        """Serialize the report for download (UTF-8 bytes).

        - ``json``: the full report (tables + objects) as a JSON object.
        - ``ndjson``: one record per line, each tagged with ``record_type``
          (``table`` or ``object``).
        - ``csv``: the per-table statistics with the header
          ``table,rows_loaded,source_rows,target_rows,matched,errors`` (objects
          are JSON/NDJSON only).
        """
        if fmt == "csv":
            return self._render_csv()
        if fmt == "ndjson":
            return self._render_ndjson()
        return self.model_dump_json(indent=2).encode("utf-8")

    def _render_ndjson(self) -> bytes:
        lines: list[str] = []
        for table in self.tables:
            record = {"record_type": "table", **table.model_dump()}
            lines.append(json.dumps(record, default=str))
        for obj in self.objects:
            record = {"record_type": "object", **obj.model_dump()}
            lines.append(json.dumps(record, default=str))
        text = "\n".join(lines)
        if text:
            text += "\n"
        return text.encode("utf-8")

    def _render_csv(self) -> bytes:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(_TABLE_CSV_FIELDS)
        for table in self.tables:
            writer.writerow(
                [
                    table.table,
                    table.rows_loaded,
                    "" if table.source_rows is None else table.source_rows,
                    "" if table.target_rows is None else table.target_rows,
                    "" if table.matched is None else table.matched,
                    table.errors,
                ]
            )
        return buffer.getvalue().encode("utf-8")


class MigrationStatsBuilder:
    """Builds a :class:`MigrationStatsReport` by combining existing outputs."""

    def build(
        self,
        job: MigrationJob,
        *,
        validation_report: Optional[ValidationReport] = None,
        error_summary: Optional[ErrorLogSummary] = None,
        apply_results: Optional[Sequence[ApplyResult]] = None,
    ) -> MigrationStatsReport:
        """Combine job progress, validation, errors, and apply results.

        Nothing is recomputed (Req 13.4): rows come from the job chunks,
        source/target/matched from ``validation_report`` (when present), error
        counts from ``error_summary``, and object outcomes from ``apply_results``.
        """
        val_by_table = {}
        if validation_report is not None:
            val_by_table = {item.table: item for item in validation_report.items}
        errors_by_table = (
            error_summary.errors_by_table if error_summary is not None else {}
        )

        tables = []
        for chunk in job.chunks:
            validation = val_by_table.get(chunk.chunk_id)
            tables.append(
                TableMigrationStats(
                    table=chunk.chunk_id,
                    rows_loaded=chunk.rows_loaded,
                    source_rows=(
                        validation.source_row_count if validation else None
                    ),
                    target_rows=(
                        validation.target_row_count if validation else None
                    ),
                    matched=validation.matched if validation else None,
                    errors=errors_by_table.get(chunk.chunk_id, 0),
                )
            )

        objects = [
            ObjectMigrationStats(
                object_name=result.object_name, apply_status=result.status
            )
            for result in (apply_results or [])
        ]
        return MigrationStatsReport(tables=tables, objects=objects)


__all__ = [
    "TableMigrationStats",
    "ObjectMigrationStats",
    "MigrationStatsReport",
    "MigrationStatsBuilder",
]
