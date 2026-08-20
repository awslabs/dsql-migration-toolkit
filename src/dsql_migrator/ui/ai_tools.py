# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Read-only AI-DBA tool definitions + executor (extracted from ``ui/app.py``).

The persistent AI DBA panel can call these local, read-only "tools" (Anthropic
tool-schema shape) to ground its answers in the migration's REAL, current data --
converted DDL, the assessment, validation results, load status, failure diagnosis
(why CDC is not streaming, prerequisite verdicts, DLQ error text), and a few LIVE
target Aurora DSQL catalog reads. This is NOT an MCP server: they are ordinary
in-process functions the model is told about.

Property 7: a tool only ever returns schema (DDL) / names / counts / verdicts /
credential-free English messages -- NEVER a row value or a credential. A tool must
never raise into the chat; any failure degrades to an error JSON.

The executor is built by :func:`build_ai_tool_executor`, a FACTORY that takes the
app's per-session id, the UI stores, the job manager, and the Full Load rate/ETA
helper, and returns the ``execute(name, args) -> str`` callable the panel calls.
Keeping it a factory (rather than importing the app's module-level stores here)
avoids a circular import with ``ui/app.py`` while leaving the tool bodies unchanged.
"""

from __future__ import annotations

from typing import Callable

# Imported at module level (as in app.py) so the tool bodies reference it as a bare
# name. ``data_migration`` is a leaf relative to app.py, so there is no import cycle.
from dsql_migrator.ui.data_migration import cdc_streaming_started

# The AI DBA's read-only tool schemas. Each name maps to a branch in the executor
# returned by build_ai_tool_executor. Anthropic tool-schema shape.
AI_TOOL_SCHEMAS: list[dict] = [
    {
        "name": "list_converted_tables",
        "description": (
            "List the source object names whose schema has been converted to Aurora "
            "DSQL DDL in this session (Schema Conversion). No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_converted_ddl",
        "description": (
            "Get the source MySQL DDL and the converted Aurora DSQL DDL for one table "
            "or view, by name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "object_name": {"type": "string", "description": "Table or view name."}
            },
            "required": ["object_name"],
        },
    },
    {
        "name": "list_objects_by_status",
        "description": (
            "List assessed source objects, optionally filtered by migration "
            "classification (AUTO, MANUAL, or UNSUPPORTED)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "classification": {
                    "type": "string",
                    "enum": ["AUTO", "MANUAL", "UNSUPPORTED"],
                }
            },
        },
    },
    {
        "name": "get_assessment",
        "description": (
            "Get the compatibility assessment (classification, effort, risk, "
            "recommendation) for one source object by name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"object_name": {"type": "string"}},
            "required": ["object_name"],
        },
    },
    {
        "name": "get_source_object_detail",
        "description": (
            "Get the real STRUCTURE of one SOURCE table (or view) by name: columns "
            "(name/type/nullable/default/collation/generated), primary key, indexes "
            "(incl. prefix lengths + type), foreign keys (incl. on_delete/on_update "
            "cascade), CHECK constraints, and the partitioned flag. Schema only, "
            "never any row data. Use it to name the EXACT offending column/key/FK "
            "behind an assessment finding instead of guessing from DDL text."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"object_name": {"type": "string"}},
            "required": ["object_name"],
        },
    },
    {
        "name": "list_unsupported_objects",
        "description": (
            "List the SOURCE objects Aurora DSQL cannot convert — triggers, stored "
            "procedures/functions (routines), and scheduled events — BY NAME. Their "
            "logic must be reimplemented in the application (or an external scheduler "
            "like Amazon EventBridge Scheduler for events). Use this to name the "
            "objects and advise how to reimplement each. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_validation_summary",
        "description": (
            "Get the latest data-validation result: match verdict, matched/total "
            "tables, and missing/extra row counts. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_validation_mismatches",
        "description": (
            "List the tables that did NOT match in the last data validation — each "
            "with source/target row counts, missing/extra counts, row-count & checksum "
            "match flags, quarantined rows, and any error. Use it to NAME the specific "
            "mismatched tables and reason lag-vs-standing-gap. Counts/verdicts only, "
            "never row data. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_schema_apply_result",
        "description": (
            "List the per-object results of the last Schema Conversion Apply: each "
            "object's status (CREATED / SKIPPED / FAILED) and, for a failure, its error "
            "detail. Use it to NAME the objects whose apply FAILED and why. Names / "
            "statuses / messages only. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_full_load_status",
        "description": (
            "Get Full Load progress (tables done/total/failed, rows loaded) and "
            "whether CDC is streaming. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_failed_full_load_tables",
        "description": (
            "List the Full Load tables that FAILED (each with its latest error message) "
            "and the tables that had rows QUARANTINED (permanently dropped, e.g. a value "
            "over DSQL's ~1 MiB per-value limit). Use it to NAME the specific tables "
            "blocking the migration and to reason about the standing-gap-before-CDC risk. "
            "Names + messages only, never row data. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_cdc_status",
        "description": (
            "Get live CDC (change-data-capture) health: whether the stream is running, "
            "worst replication lag, dead-letter-queue (DLQ) depth with the top poison "
            "tables and their SQLSTATEs, and any detected source SCHEMA DRIFT (kind + "
            "tables). Use it to diagnose CDC silent-data-loss risk (rising DLQ, drift, "
            "stalled sink) and what to fix before cut over. Counts / table names / "
            "SQLSTATEs only, never row data. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_cdc_pipeline_diagnostics",
        "description": (
            "Diagnose WHY CDC is not streaming (or failed to start): the CDC "
            "CloudFormation stack phase + raw status, per-connector state "
            "(RUNNING/FAILED), a confirmed sink stall (connector RUNNING but applying "
            "nothing), the last CDC deploy action + any FAILED deploy stage, the tail "
            "of the deploy log (already-diagnosed, human-actionable failure text — e.g. "
            "partition-quota exhaustion, missing IAM JAAS, bad source creds), and the "
            "Full Load -> CDC watermark handoff (binlog file:pos / GTID presence, "
            "snapshot time, resume mode). Use it when CDC shows not-streaming or a "
            "deploy failed. All CACHED/local state — states/phases/log text only, never "
            "row data or credentials. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_prerequisite_verdicts",
        "description": (
            "Get the latest prerequisite-check verdicts for Full Load and/or CDC — each "
            "check's status (PASS/FAIL/WARN/INFO/SKIP), whether it is required, and its "
            "detail + remediation (e.g. binlog row format, GTID, MSK / MSK Connect "
            "availability, target IAM auth, source reachability, replication grants, "
            "primary keys). Plus can_proceed (blocked only by a required FAIL). Use it "
            "to explain WHY a mode cannot start and exactly how to fix it. Cached from "
            "the last checks the operator ran (empty if none run yet). Verdicts + "
            "English remediation only, never credentials."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "mode": {
                    "type": "string",
                    "enum": ["FULL_LOAD", "CDC"],
                    "description": "Limit to one mode; omit for both.",
                }
            },
        },
    },
    {
        "name": "list_cdc_dlq_samples",
        "description": (
            "List a small SAMPLE of the actual CDC dead-letter-queue (DLQ) error "
            "messages — one per distinct (table, SQLSTATE) — with the error code, the "
            "English error text, and when it occurred. Turns 'SQLSTATE 23505 x5' from "
            "get_cdc_status into the concrete reason + how to fix it. The failing row's "
            "primary-key VALUE is deliberately EXCLUDED (Property 7). Names / SQLSTATEs "
            "/ error text only, never row data. No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "list_target_tables",
        "description": (
            "List the tables and views that currently EXIST on the target Aurora DSQL "
            "cluster (a live, read-only catalog read). No arguments."
        ),
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_target_schema",
        "description": (
            "Get the columns (name, type, nullable) and indexes of one table or view "
            "AS IT EXISTS on the target Aurora DSQL cluster, by name (live catalog "
            "read)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"object_name": {"type": "string"}},
            "required": ["object_name"],
        },
    },
    {
        "name": "count_target_rows",
        "description": (
            "Count the rows currently in one table on the target Aurora DSQL cluster, "
            "by name (a live SELECT COUNT(*); returns only the number, never row data)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"object_name": {"type": "string"}},
            "required": ["object_name"],
        },
    },
]

# Appended to the general chat's system prompt so the model knows it HAS the tools
# and should call them for specifics + present results visually (Markdown table / SQL).
AI_TOOLS_SYSTEM_HINT = (
    "\n\nYou have read-only tools to look up this migration's REAL, current data "
    "(converted DDL, the assessment, validation results, load status), to DIAGNOSE "
    "failures (why CDC is not streaming / a deploy failed, prerequisite verdicts, Full "
    "Load failed/quarantined tables, actual DLQ error messages), plus a few that "
    "read the LIVE target Aurora DSQL cluster (its existing tables, a table's schema, a "
    "table's row count). When the user asks about specific objects or results, CALL the "
    "tools and answer from the actual values -- never guess or answer generically. "
    "Present the result visually: a Markdown table for a list/breakdown, a fenced "
    "```sql block for DDL."
)


def build_ai_tool_executor(
    *,
    session_id: str,
    session_store: object,
    evaluation_store: object,
    schema_conversion_store: object,
    data_migration_store: object,
    validation_store: object,
    job_manager: object,
    full_load_rate_eta: Callable[..., tuple],
) -> Callable[[str, dict], str]:
    """Return the AI DBA's ``execute(name, args) -> str`` tool callable.

    The app wires this once per page from its module-level stores + the Full Load
    rate/ETA helper. The bindings below re-expose those dependencies under the exact
    names the tool bodies use, so the tool logic is identical to when it lived inline
    in ``build_page`` (behavior-preserving extraction).
    """
    # Bind to the names the tool bodies use (identical to app.py's globals), so the
    # executor body is a verbatim move.
    SESSION_STORE = session_store
    EVALUATION_STORE = evaluation_store
    SCHEMA_CONVERSION_STORE = schema_conversion_store
    DATA_MIGRATION_STORE = data_migration_store
    VALIDATION_STORE = validation_store
    JOB_MANAGER = job_manager
    _full_load_rate_eta = full_load_rate_eta

    def _ai_tool_execute(name: str, args: dict) -> str:
        # Run ONE read-only, credential-free AI tool over the UI stores and return a
        # compact JSON string the model can read. Only schema (DDL) / names / counts /
        # verdicts are ever returned -- NEVER row values or credentials (Property 7).
        # A tool must never raise into the chat; any failure degrades to an error JSON.
        import json as _json

        args = args if isinstance(args, dict) else {}
        try:
            if name == "list_converted_tables":
                from dsql_migrator.ui.schema_conversion import selected_object_names

                _ids = SCHEMA_CONVERSION_STORE.get_or_create(
                    session_id
                ).generated_node_ids
                _names = sorted(selected_object_names(_ids)) if _ids else []
                return _json.dumps(
                    {"status": "ok" if _names else "none",
                     "converted": _names, "count": len(_names)}
                )
            if name == "get_converted_ddl":
                from dsql_migrator.core.converter import (
                    SchemaConverter,
                    SchemaConvertOptions,
                )
                from dsql_migrator.ui.schema_conversion import (
                    TABLE_PREFIX,
                    VIEW_PREFIX,
                    generate_previews,
                )

                _obj = str(args.get("object_name", "")).strip()
                if not _obj:
                    return _json.dumps({"status": "error", "message": "object_name required"})
                _ev = EVALUATION_STORE.get_or_create(session_id).result
                _inv = getattr(_ev, "inventory", None) if _ev is not None else None
                if _inv is None:
                    return _json.dumps(
                        {"status": "not_run",
                         "message": "Run Evaluation (Step 1) first to read the source schema."}
                    )
                _res = SchemaConverter().convert(_inv, SchemaConvertOptions())
                _pv = generate_previews(
                    [f"{TABLE_PREFIX}{_obj}", f"{VIEW_PREFIX}{_obj}"],
                    _inv, _res, existence_checker=None,
                )
                _p = next((p for p in _pv if p.object_name == _obj), None)
                if _p is None:
                    return _json.dumps({"status": "not_found", "object_name": _obj})
                return _json.dumps(
                    {"status": "ok", "object_name": _p.object_name,
                     "source_ddl": _p.source_ddl, "target_ddl": _p.target_ddl}
                )
            if name == "list_objects_by_status":
                _ev = EVALUATION_STORE.get(session_id)
                _r = _ev.result if _ev else None
                if _r is None:
                    return _json.dumps({"status": "not_run", "objects": []})
                _items = _r.assessment.items
                _want = str(args.get("classification", "")).strip().upper()
                if _want:
                    _items = [it for it in _items if it.classification.value == _want]
                return _json.dumps(
                    {"status": "ok", "count": len(_items),
                     "objects": [
                         {"object_name": it.object_name, "kind": it.kind,
                          "classification": it.classification.value}
                         for it in _items
                     ]}
                )
            if name == "get_assessment":
                _ev = EVALUATION_STORE.get(session_id)
                _r = _ev.result if _ev else None
                if _r is None:
                    return _json.dumps({"status": "not_run"})
                _obj = str(args.get("object_name", "")).strip().lower()
                _m = next(
                    (it for it in _r.assessment.items if it.object_name.lower() == _obj),
                    None,
                )
                if _m is None:
                    return _json.dumps(
                        {"status": "not_found", "object_name": args.get("object_name")}
                    )
                return _json.dumps(
                    {"status": "ok", "object_name": _m.object_name, "kind": _m.kind,
                     "classification": _m.classification.value,
                     "effort": _m.effort.value if _m.effort else None,
                     "risk": _m.risk, "recommendation": _m.recommendation}
                )
            if name == "get_source_object_detail":
                # Real STRUCTURE of one source table/view (schema only, never rows):
                # columns/types/nullability/defaults/collation, PK, indexes (incl.
                # prefix lengths + type), FKs (incl. cascade actions), CHECKs. Lets a
                # chat name the EXACT offending column/key instead of parsing DDL text.
                _obj = str(args.get("object_name", "")).strip()
                if not _obj:
                    return _json.dumps({"status": "error", "message": "object_name required"})
                _ev = EVALUATION_STORE.get(session_id)
                _r = _ev.result if _ev else None
                _inv = getattr(_r, "inventory", None) if _r is not None else None
                if _inv is None:
                    return _json.dumps(
                        {"status": "not_run",
                         "message": "Run Evaluation (Step 1) first to read the source schema."}
                    )
                _key = _obj.lower()
                _tail = _key.rsplit(".", 1)[-1]

                def _matches(_n: str) -> bool:
                    _nl = _n.lower()
                    return _nl == _key or _nl.rsplit(".", 1)[-1] == _tail

                _t = next((t for t in _inv.tables if _matches(t.name)), None)
                if _t is None:
                    _v = next((v for v in _inv.views if _matches(v.name)), None)
                    if _v is not None:
                        return _json.dumps(
                            {"status": "ok", "object_name": _v.name, "kind": "view",
                             "definition": (_v.definition or "")[:4000]}
                        )
                    return _json.dumps({"status": "not_found", "object_name": _obj})
                return _json.dumps(
                    {"status": "ok", "object_name": _t.name, "kind": "table",
                     "primary_key": list(_t.primary_key),
                     "auto_increment_column": _t.auto_increment_column,
                     "partitioned": _t.partitioned,
                     "columns": [
                         {"name": c.name, "type": c.mysql_type, "nullable": c.nullable,
                          "default": c.default, "collation": c.collation,
                          "generated": c.generated,
                          "auto_update_timestamp": c.auto_update_timestamp}
                         for c in _t.columns
                     ],
                     "indexes": [
                         {"name": i.name, "columns": list(i.columns), "unique": i.unique,
                          "index_type": i.index_type,
                          "prefix_lengths": dict(i.prefix_lengths)}
                         for i in _t.indexes
                     ],
                     "foreign_keys": [
                         {"name": f.name, "columns": list(f.columns),
                          "referenced_table": f.referenced_table,
                          "referenced_columns": list(f.referenced_columns),
                          "on_delete": f.on_delete, "on_update": f.on_update}
                         for f in _t.foreign_keys
                     ],
                     "check_constraints": [
                         {"name": ck.name, "expression": ck.expression}
                         for ck in _t.check_constraints
                     ],
                     "expression_indexes": list(_t.expression_indexes)}
                )
            if name == "list_unsupported_objects":
                _ev = EVALUATION_STORE.get(session_id)
                _r = _ev.result if _ev else None
                _inv = getattr(_r, "inventory", None) if _r is not None else None
                if _inv is None:
                    return _json.dumps(
                        {"status": "not_run",
                         "message": "Run Evaluation (Step 1) first to read the source."}
                    )
                return _json.dumps(
                    {"status": "ok",
                     "triggers": [t.name for t in _inv.triggers],
                     "routines": [r.name for r in _inv.routines],
                     "events": [e.name for e in _inv.events],
                     "note": ("Aurora DSQL supports none of these; reimplement each "
                              "one's logic in the application (an event can move to an "
                              "external scheduler such as Amazon EventBridge Scheduler).")}
                )
            if name == "get_validation_summary":
                from dsql_migrator.ui.validation import summarize_validation

                _rep = VALIDATION_STORE.get_or_create(session_id).result
                if _rep is None:
                    return _json.dumps({"status": "not_run"})
                _s = summarize_validation(_rep)
                return _json.dumps(
                    {"status": "ok", "is_match": _s.is_match,
                     "matched_tables": _s.matched_tables, "total_tables": _s.total_tables,
                     "mismatched_tables": _s.mismatched_tables,
                     "missing_on_target": _s.missing_on_target,
                     "extra_on_target": _s.extra_on_target, "mode": _s.mode}
                )
            if name == "list_validation_mismatches":
                _rep = VALIDATION_STORE.get_or_create(session_id).result
                if _rep is None:
                    return _json.dumps({"status": "not_run"})
                _bad: list[dict] = []
                for _it in _rep.items:
                    if getattr(_it, "matched", True):
                        continue
                    _src = getattr(_it, "source_row_count", None)
                    _tgt = getattr(_it, "target_row_count", None)
                    _have = _src is not None and _tgt is not None
                    _bad.append(
                        {"table": _it.table,
                         "source_row_count": _src, "target_row_count": _tgt,
                         "missing_on_target": max(0, _src - _tgt) if _have else None,
                         "extra_on_target": max(0, _tgt - _src) if _have else None,
                         "row_count_match": getattr(_it, "row_count_match", None),
                         "checksum_match": getattr(_it, "checksum_match", None),
                         "rows_quarantined": getattr(_it, "rows_quarantined", None),
                         "error": (str(getattr(_it, "error", "") or "")[:300]) or None}
                    )
                return _json.dumps(
                    {"status": "ok", "mismatched_count": len(_bad),
                     "mismatches": _bad[:50]}
                )
            if name == "get_schema_apply_result":
                _res = SCHEMA_CONVERSION_STORE.get_or_create(session_id).apply_results
                if not _res:
                    return _json.dumps(
                        {"status": "not_run", "message": "No schema apply has run yet."}
                    )
                return _json.dumps(
                    {"status": "ok",
                     "objects": [
                         {"object_name": r.object_name, "status": r.status.value,
                          "detail": (str(getattr(r, "detail", "") or "")[:300]) or None}
                         for r in _res
                     ],
                     "failed": [
                         r.object_name for r in _res if r.status.value == "FAILED"
                     ]}
                )
            if name == "get_full_load_status":
                from dsql_migrator.ui.data_migration import (
                    _current_job,
                    summarize_progress,
                )

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _job = _current_job(JOB_MANAGER, _dm.job_id)
                _out: dict = {"status": "ok", "full_load": None}
                if _job is not None and hasattr(_job, "chunks"):
                    _fl = summarize_progress(_job)
                    _fl_running = (_fl.in_progress_tables + _fl.pending_tables) > 0
                    _fl_rate, _fl_eta = _full_load_rate_eta(
                        _job, _fl, running=_fl_running
                    )
                    _chunks = list(getattr(_job, "chunks", []) or [])
                    _out["full_load"] = {
                        "total_tables": _fl.total_tables, "done_tables": _fl.done_tables,
                        "failed_tables": _fl.failed_tables,
                        "in_progress_tables": _fl.in_progress_tables,
                        "pending_tables": _fl.pending_tables,
                        "rows_loaded": _fl.rows_loaded,
                        # Skipped = already-present (ON CONFLICT DO NOTHING); quarantined
                        # = permanently DROPPED (a real data gap CDC won't backfill).
                        "rows_skipped": sum(
                            int(getattr(c, "rows_skipped", 0) or 0) for c in _chunks
                        ),
                        "rows_quarantined": sum(
                            int(getattr(c, "rows_quarantined", 0) or 0) for c in _chunks
                        ),
                        "throttled_tables": len(
                            getattr(_job, "throttled_tables", []) or []
                        ),
                        "progress_pct": round(_fl.progress_pct, 1),
                        "running": _fl_running,
                        "rows_per_sec": round(_fl_rate) if _fl_rate else None,
                        "eta": _fl_eta or None,
                    }
                _out["cdc_streaming"] = bool(cdc_streaming_started(_dm, JOB_MANAGER))
                return _json.dumps(_out)
            if name == "list_failed_full_load_tables":
                from dsql_migrator.ui.data_migration import _current_job
                from dsql_migrator.ui.data_migration._cdc_status import (
                    full_load_latest_messages,
                )

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _job = _current_job(JOB_MANAGER, _dm.job_id)
                if _job is None or not hasattr(_job, "chunks"):
                    return _json.dumps(
                        {"status": "not_run", "message": "No Full Load has run yet."}
                    )
                try:
                    _msgs = full_load_latest_messages(
                        getattr(_dm, "error_log", None), _job.job_id
                    )
                except Exception:  # noqa: BLE001
                    _msgs = {}
                _quar_prefix = "quarantined row pk["
                _chunks = list(getattr(_job, "chunks", []) or [])
                # Per-table failure detail: attempts (retries) + what got loaded /
                # skipped / permanently dropped, so the chat can reason lag vs gap.
                _failed = [
                    {"table": c.chunk_id,
                     "error": str(_msgs.get(c.chunk_id, ""))[:500],
                     "attempts": int(getattr(c, "attempts", 0) or 0),
                     "rows_loaded": int(getattr(c, "rows_loaded", 0) or 0),
                     "rows_skipped": int(getattr(c, "rows_skipped", 0) or 0),
                     "rows_quarantined": int(getattr(c, "rows_quarantined", 0) or 0)}
                    for c in _chunks
                    if getattr(c, "status", "") == "FAILED"
                ]
                # Quarantined tables: union of the per-chunk counter (authoritative) and
                # the legacy message-prefix detection, with the dropped-row count.
                _by_id = {c.chunk_id: c for c in _chunks}
                _quar_names = {
                    t for t, m in _msgs.items() if str(m).startswith(_quar_prefix)
                }
                _quar_names |= {
                    c.chunk_id for c in _chunks
                    if int(getattr(c, "rows_quarantined", 0) or 0) > 0
                }
                _quarantined = [
                    {"table": t,
                     "rows_quarantined": int(
                         getattr(_by_id.get(t), "rows_quarantined", 0) or 0
                     )}
                    for t in sorted(_quar_names)
                ]
                return _json.dumps(
                    {"status": "ok", "failed": _failed, "failed_count": len(_failed),
                     "quarantined_tables": _quarantined,
                     "note": ("Failed and quarantined tables are STANDING gaps CDC will "
                              "NOT backfill — resolve them (reload after fixing the "
                              "cause, or accept the gap) before starting CDC.")}
                )
            if name == "get_cdc_status":
                # All reads are LOCAL/cached (the DLQ error log + cached lag + streaming
                # state) -- never a fresh AWS metric fetch per question.
                from collections import Counter

                from dsql_migrator.ui.data_migration._cdc_status import (
                    cdc_dlq_records,
                    cdc_dlq_summary,
                    cdc_error_log_key,
                    cdc_schema_drift_summary,
                )

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _streaming = bool(cdc_streaming_started(_dm, JOB_MANAGER))
                _key = cdc_error_log_key(_dm)
                try:
                    _dlq = cdc_dlq_summary(_dm, _key)
                    _records = list(cdc_dlq_records(_dm, _key))
                    _drift = list(cdc_schema_drift_summary(_dm, _key))
                except Exception:  # noqa: BLE001
                    _dlq, _records, _drift = None, [], []
                _by_table = dict(getattr(_dlq, "errors_by_table", {}) or {})
                _poison = sorted(
                    _by_table.items(), key=lambda kv: kv[1], reverse=True
                )[:10]
                _sqlstates = Counter(
                    str(getattr(r, "error_code", "") or "")
                    for r in _records
                    if getattr(r, "error_code", None)
                ).most_common(5)
                _lag = getattr(_dm, "cdc_replication_lag_by_table", {}) or {}
                _max_lag = max((int(v) for v in _lag.values()), default=None)
                return _json.dumps(
                    {"status": "ok", "streaming": _streaming,
                     "dlq_depth": int(getattr(_dlq, "total_errors", 0) or 0),
                     "poison_tables": [
                         {"table": t, "count": c} for t, c in _poison
                     ],
                     "top_sqlstates": [
                         {"sqlstate": s, "count": c} for s, c in _sqlstates
                     ],
                     "schema_drift": [
                         {"table": d.table, "kind": d.kind, "count": d.count}
                         for d in _drift
                     ],
                     "max_replication_lag_ms": _max_lag,
                     "note": ("DLQ poison rows + detected schema drift are silent "
                              "data-loss risks — resolve before cut over. Common "
                              "SQLSTATEs: 22P02/22001 = data/format, 23505 = duplicate "
                              "key, 23502 = NOT NULL / dropped column.")}
                )
            if name == "get_cdc_pipeline_diagnostics":
                # Why CDC is not streaming / a deploy failed. All CACHED/local state:
                # connector states, sink-stall flag, CFN stack phase, the failed deploy
                # stage + deploy-log tail, and the Full Load -> CDC watermark handoff.
                from dsql_migrator.ui.data_migration import _current_job

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _streaming = bool(cdc_streaming_started(_dm, JOB_MANAGER))
                _view = getattr(_dm, "cdc_status_view", None)
                _conn_states = dict(getattr(_view, "connector_states", {}) or {})
                _act = getattr(_dm, "cdc_activity", None)
                _deploy_job = _current_job(
                    JOB_MANAGER, getattr(_dm, "cdc_deploy_job_id", None)
                )
                _failed_stage = None
                if _deploy_job is not None and hasattr(_deploy_job, "chunks"):
                    _failed_stage = next(
                        (c.chunk_id for c in _deploy_job.chunks
                         if getattr(c, "status", "") == "FAILED"),
                        None,
                    )
                try:
                    _log = _dm.get_cdc_deploy_log()
                except Exception:  # noqa: BLE001
                    _log = []
                _log_lines = [str(m) for _ts, m in _log]
                _err_lines = [ln for ln in _log_lines if "ERROR" in ln.upper()]
                _log_tail = (_err_lines or _log_lines)[-8:]
                _fl_job = _current_job(JOB_MANAGER, getattr(_dm, "job_id", None))
                _wm = getattr(_fl_job, "watermark", None) if _fl_job is not None else None
                _handoff = None
                if _wm is not None:
                    _snap = getattr(_wm, "snapshot_timestamp", None)
                    _handoff = {
                        "binlog_file": getattr(_wm, "binlog_file", None),
                        "binlog_position": getattr(_wm, "binlog_position", None),
                        "has_gtid": bool(getattr(_wm, "gtid_executed", None)),
                        "snapshot_timestamp": _snap.isoformat() if _snap else None,
                    }
                try:
                    _resume_mode = _dm.cdc_start_mode
                except Exception:  # noqa: BLE001
                    _resume_mode = None
                return _json.dumps(
                    {"status": "ok", "streaming": _streaming,
                     "stack_phase": getattr(_dm, "cdc_stack_phase", None),
                     "stack_status": getattr(_dm, "cdc_stack_phase_status", None),
                     "connector_states": _conn_states,
                     "sink_stall_confirmed": bool(
                         getattr(_act, "sink_stall_confirmed", False)
                     ),
                     "deploy_action": getattr(_dm, "cdc_action_kind", None),
                     "failed_deploy_stage": _failed_stage,
                     "deploy_log_tail": _log_tail,
                     "watermark_handoff": _handoff,
                     "resume_mode": _resume_mode,
                     "note": ("If streaming is false: check stack_phase (deployed?), "
                              "connector_states (any FAILED?), failed_deploy_stage + "
                              "deploy_log_tail (why the deploy stopped). "
                              "sink_stall_confirmed = connector RUNNING but nothing "
                              "applied (silent data loss). watermark_handoff is where "
                              "CDC resumes from after Full Load.")}
                )
            if name == "get_prerequisite_verdicts":
                from dsql_migrator.core.models import MigrationMode

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _want = str(args.get("mode", "") or "").upper()
                _modes = (
                    [MigrationMode(_want)]
                    if _want in ("FULL_LOAD", "CDC")
                    else [MigrationMode.FULL_LOAD, MigrationMode.CDC]
                )
                _reports: dict = {}
                for _m in _modes:
                    _rep = _dm.get_prereq_report(_m)
                    if _rep is None:
                        _reports[_m.value] = {"status": "not_run"}
                        continue
                    _reports[_m.value] = {
                        "status": "ok",
                        "can_proceed": bool(_rep.can_proceed),
                        "checks": [
                            {"check_id": str(getattr(r.check_id, "value", r.check_id)),
                             "title": r.title,
                             "status": str(getattr(r.status, "value", r.status)),
                             "required": bool(r.required),
                             "target": r.target,
                             "detail": str(r.detail or "")[:400],
                             "remediation": str(r.remediation or "")[:400]}
                            for r in _rep.results
                        ],
                    }
                return _json.dumps(
                    {"status": "ok", "reports": _reports,
                     "note": ("can_proceed is false only when a REQUIRED check FAILED. "
                              "INFO = expected/no-action (e.g. GTID off, MSK created at "
                              "deploy time). Use remediation for the fix. 'not_run' "
                              "means the operator hasn't run checks for that mode yet.")}
                )
            if name == "list_cdc_dlq_samples":
                from dsql_migrator.ui.data_migration._cdc_status import (
                    cdc_dlq_records,
                    cdc_error_log_key,
                )

                _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
                _key = cdc_error_log_key(_dm)
                try:
                    _records = list(cdc_dlq_records(_dm, _key))
                except Exception:  # noqa: BLE001
                    _records = []
                _seen: set = set()
                _samples: list[dict] = []
                for r in _records:
                    _code = str(getattr(r, "error_code", "") or "")
                    _tbl = str(getattr(r, "table", "") or "")
                    if (_tbl, _code) in _seen:
                        continue
                    _seen.add((_tbl, _code))
                    _occ = getattr(r, "occurred_at", None)
                    # NB: the failing row's primary-key VALUE (r.pk) is intentionally
                    # NOT included -- error text + code only (Property 7).
                    _samples.append(
                        {"table": _tbl, "sqlstate": _code,
                         "message": str(getattr(r, "message", "") or "")[:400],
                         "occurred_at": _occ.isoformat() if _occ else None}
                    )
                    if len(_samples) >= 20:
                        break
                return _json.dumps(
                    {"status": "ok" if _samples else "none",
                     "sample_count": len(_samples), "samples": _samples,
                     "note": ("One sample per (table, SQLSTATE). Dead-lettered rows are "
                              "NOT applied to the target; fix the cause (source data / "
                              "schema drift), then re-load the affected rows if needed.")}
                )
            # --- LIVE target Aurora DSQL reads (read-only; schema/counts only) ------
            if name in ("list_target_tables", "get_target_schema", "count_target_rows"):
                _st = SESSION_STORE.get_or_create(session_id)
                _tc = getattr(_st, "target_config", None)
                if _tc is None or not getattr(_st, "target_verified", False):
                    return _json.dumps(
                        {"status": "not_connected",
                         "message": "Connect and verify the target on Connect first."}
                    )
                from dsql_migrator.ui.evaluation import _default_target_browser_factory

                _profile = getattr(_st, "aws_profile", None)
                _inv = _default_target_browser_factory(_profile).browse(_tc)
                _rels = [
                    r
                    for sch in _inv.schemas
                    for r in (list(sch.tables) + list(sch.views))
                ]
                if name == "list_target_tables":
                    return _json.dumps(
                        {"status": "ok", "count": len(_rels),
                         "objects": [
                             {"name": r.qualified_name, "kind": r.kind.value}
                             for r in _rels
                         ]}
                    )
                _obj = str(args.get("object_name", "")).strip().lower()
                _rel = next(
                    (r for r in _rels
                     if r.name.lower() == _obj or r.qualified_name.lower() == _obj),
                    None,
                )
                if _rel is None:
                    return _json.dumps(
                        {"status": "not_found", "object_name": args.get("object_name"),
                         "message": "No such table/view exists on the target yet."}
                    )
                if name == "get_target_schema":
                    return _json.dumps(
                        {"status": "ok", "name": _rel.qualified_name,
                         "kind": _rel.kind.value,
                         "columns": [
                             {"name": c.name, "type": c.data_type, "nullable": c.nullable}
                             for c in _rel.columns
                         ],
                         "indexes": [
                             {"name": i.name, "unique": i.unique} for i in _rel.indexes
                         ]}
                    )
                # count_target_rows: a live COUNT(*) with catalog-resolved, quoted
                # identifiers (injection-safe) -- returns only the number, no row data.
                from psycopg import sql as _sql

                from dsql_migrator.core.target_connection import DsqlConnector

                _conn = DsqlConnector(_tc, aws_profile=_profile).connect()
                try:
                    with _conn.cursor() as _cur:
                        _cur.execute(
                            _sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                                _sql.Identifier(_rel.schema_name),
                                _sql.Identifier(_rel.name),
                            )
                        )
                        _row = _cur.fetchone()
                finally:
                    try:
                        _conn.close()
                    except Exception:  # noqa: BLE001
                        pass
                return _json.dumps(
                    {"status": "ok", "name": _rel.qualified_name,
                     "row_count": int(_row[0]) if _row else 0}
                )
            return _json.dumps({"status": "error", "message": f"unknown tool {name}"})
        except Exception:  # noqa: BLE001 - a tool must never break the chat
            return _json.dumps({"status": "error", "message": "tool lookup failed"})

    return _ai_tool_execute
