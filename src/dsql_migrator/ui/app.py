# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""NiceGUI application entrypoint.

Run with::

    uv run python -m dsql_migrator.ui.app

This module wires up the UI layer as a sidebar layout: a header, a left
navigation drawer (the preliminary Connect screen plus the four workflow steps —
Evaluation, Schema Conversion, Data Migration, Validation — with their status),
and a main content area that renders the selected screen.

Session credentials entered in the Connect screen are held only in process
memory, scoped per browser session (a stable, cookie-backed browser id), so a
page refresh continues the same session instead of starting over. Credentials
are never persisted to disk, logs, reports, or job state, and are discarded when
the process ends (Property 7 / Requirement 9.2).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dsql_migrator import __version__
from dsql_migrator.config import (
    AppConfig,
    ConnectDefaults,
    load_config,
    load_connect_defaults,
    read_env_file,
)
from dsql_migrator.core.assessment_strategist import (
    AssessmentStrategist,
    build_general_chat_system,
)
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import MigrationContext
from dsql_migrator.ui.connect import build_connect_page
from dsql_migrator.ui.data_migration import (
    DataMigrationStore,
    build_data_migration_screen,
    cdc_streaming_started,
    full_load_run_guard_reason,
    prereq_mode_for_type,
)
from dsql_migrator.ui.evaluation import EvaluationStore, build_evaluation_screen
from dsql_migrator.ui.schema_conversion import (
    SchemaConversionStore,
    build_schema_conversion_screen,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.query_playground import (
    PlaygroundStore,
    build_query_playground_screen,
)
from dsql_migrator.ui.validation import (
    ValidationStore,
    build_cutover_screen,
    build_validation_screen,
    validation_run_guard_reason,
)
from dsql_migrator.ui.workflow import (
    OptionalTool,
    WorkflowStep,
    build_workflow_sidebar,
)

# Stable view key for the Query Playground optional tool (persisted active_view).
_QUERY_PLAYGROUND_VIEW = "query_playground"

# In-memory, per-session connection state. A plain process-memory store keeps
# credentials out of any persisted storage (Property 7).
SESSION_STORE = SessionStore()

# Per-session evaluation inputs/outputs (process memory only).
EVALUATION_STORE = EvaluationStore()

# Per-session schema-conversion inputs/outputs (process memory only).
SCHEMA_CONVERSION_STORE = SchemaConversionStore()

# Per-session data-migration job id / error (process memory only).
DATA_MIGRATION_STORE = DataMigrationStore()

# Per-session validation options / report (process memory only).
VALIDATION_STORE = ValidationStore()

# Per-session Query Playground inputs/outputs (process memory only).
PLAYGROUND_STORE = PlaygroundStore()

# Runs long-running steps (e.g. introspection) off the UI event loop (Req 9.3).
JOB_MANAGER = JobManager()

# Durable per-session workbench state (attached in main()); None until then so
# tests/imports do not touch disk. Holds the latest persisted snapshot signature
# per session to skip redundant writes (large inventories are not re-serialized
# on every poll).
SESSION_STATE_STORE: object | None = None
_LAST_SESSION_SIGNATURE: dict[str, tuple] = {}

# Retention caps to bound durable-store growth across many migrations: keep the
# most recent N completed jobs and N session snapshots; resumable/active jobs are
# never pruned. Pruning runs once at startup.
_KEEP_DONE_JOBS = 100
_KEEP_SESSIONS = 200

# In-process tool-use (function calling) for the general AI chat: read-only,
# credential-free tools the assistant can call to answer specific questions from the
# migration's REAL, current data (converted DDL, assessment, validation, load status)
# instead of guessing. Each name maps to a branch in build_page's `_ai_tool_execute`
# over the UI stores. This is NOT an MCP server -- just local functions the model is
# told about. Anthropic tool-schema shape.
_AI_TOOL_SCHEMAS: list[dict] = [
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
_AI_TOOLS_SYSTEM_HINT = (
    "\n\nYou have read-only tools to look up this migration's REAL, current data "
    "(converted DDL, the assessment, validation results, load status), plus a few that "
    "read the LIVE target Aurora DSQL cluster (its existing tables, a table's schema, a "
    "table's row count). When the user asks about specific objects or results, CALL the "
    "tools and answer from the actual values -- never guess or answer generically. "
    "Present the result visually: a Markdown table for a list/breakdown, a fenced "
    "```sql block for DDL."
)


def build_page(
    config: AppConfig,
    session_id: str,
    connect_defaults: ConnectDefaults | None = None,
) -> None:
    """Build the page content for one session.

    Renders the app as a sidebar layout: a top header, a left navigation drawer
    (the preliminary Connect screen plus the four workflow steps with their
    status), and a main content area that shows the selected screen. Only
    non-secret configuration is surfaced; credential values are never displayed
    in the UI. ``connect_defaults`` optionally prefills the Connect form for
    local development; when ``None`` the form starts blank.
    """
    # The persistent AI panel is built by the shell (build_workflow_sidebar) and its
    # handle handed back here; screens deep-link into it via _open_ai_scope. The
    # holder decouples build order: the screen builders below capture the holder now,
    # and the shell populates it before the first render, so an AI button clicked at
    # render time finds the live handle.
    _ai_panel_holder: dict = {}

    def _open_ai_scope(**kwargs: object) -> object:
        handle = _ai_panel_holder.get("handle")
        return handle.open_scope(**kwargs) if handle is not None else None

    def _ai_post_event(**kwargs: object) -> None:
        # Mirror a MAJOR migration action into the AI panel's activity feed (a
        # deterministic timeline entry the assistant is also made aware of). No-op
        # until the panel is wired or when AI is off (post_event self-gates).
        handle = _ai_panel_holder.get("handle")
        if handle is not None:
            handle.post_event(**kwargs)

    def _full_load_rate_eta(job, prog, *, running):
        # Run-level throughput (rows/sec) + ETA/elapsed for the monitor card, derived
        # from the chunks' start/finish timestamps. Defensive: any missing/odd timing
        # just yields (None, "") so the card simply omits the line. Never raises.
        from datetime import datetime

        from dsql_migrator.ui.data_migration._models import format_duration

        try:
            starts = [
                c.started_at for c in job.chunks
                if getattr(c, "started_at", None) is not None
            ]
            if not starts:
                return None, ""
            start = min(starts)
            now = datetime.now(start.tzinfo) if start.tzinfo else datetime.now()
            elapsed = (now - start).total_seconds()
            rate = (
                prog.rows_loaded / elapsed
                if running and elapsed > 0 and prog.rows_loaded > 0
                else None
            )
            pct = prog.progress_pct
            if running and pct and pct > 0 and elapsed > 0:
                remaining = max(0.0, elapsed / (pct / 100.0) - elapsed)
                return rate, f"~{format_duration(remaining)} left"
            if not running:
                finishes = [
                    c.finished_at for c in job.chunks
                    if getattr(c, "finished_at", None) is not None
                ]
                if finishes:
                    total = (max(finishes) - start).total_seconds()
                    return None, f"{format_duration(total)} elapsed"
            return rate, ""
        except Exception:  # noqa: BLE001 - timing is best-effort, never break the card
            return None, ""

    def _full_load_progress_provider():
        # Live Full Load snapshot for the AI panel's monitor card, polled by a
        # persistent panel timer so it keeps updating across navigation. Credential- and
        # row-free: table counts + failed table NAMES + rows loaded only (Property 7).
        # Returns None when there is no Full Load job (nothing to monitor yet).
        from dsql_migrator.ui.data_migration import _current_job, summarize_progress

        _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
        _job = _current_job(JOB_MANAGER, _dm.job_id)
        if _job is None or not hasattr(_job, "chunks"):
            return None
        _prog = summarize_progress(_job)
        _failed = [
            c.chunk_id for c in _job.chunks if getattr(c, "status", "") == "FAILED"
        ]
        _running = (_prog.in_progress_tables + _prog.pending_tables) > 0
        _rate, _eta = _full_load_rate_eta(_job, _prog, running=_running)
        return {
            "label": "Full Load",
            "running": _running,
            "total": _prog.total_tables,
            "done": _prog.done_tables,
            "failed": _prog.failed_tables,
            "rows": _prog.rows_loaded,
            "failed_objects": _failed,
            "rows_per_sec": _rate,
            "eta": _eta,
        }

    def _ai_context() -> MigrationContext:
        # Credential-free "where you are" for the panel's baseline chip AND the general
        # chat's grounding. We inject as much DETERMINISTIC, non-secret state as the tool
        # has ACTUALLY produced as the operator worked through the steps -- connection
        # coordinates, the assessment verdict, the schema-apply outcome, Full Load /CDC
        # progress, the validation verdict -- so the assistant answers about THIS
        # migration's real state, not generically. Every read is best-effort (a missing
        # or empty store simply omits its line, never raises) and touches only counts /
        # statuses / verdicts / non-secret coordinates -- no row values, PKs, checksums,
        # credentials, or object names (Property 7).
        st = SESSION_STORE.get_or_create(session_id)
        view = st.active_view
        step = "Connect"
        if isinstance(view, str) and view and view.lower() != "connect":
            step = view.replace("_", " ").title()
        mtype = ""
        if st.migration_type_chosen():
            mtype = str(getattr(st.migration_type, "value", "")).replace("_", " ")
        facts: list[str] = []
        # --- The assistant's own Bedrock model (so "which model are you?" is answerable
        # from context, not guessed). Model id + region are non-secret. ---
        _ai = getattr(st, "ai_assist", None)
        if _ai is not None and getattr(_ai, "model_id", ""):
            _m = f"AI DBA: this chat runs on Amazon Bedrock model {_ai.model_id}"
            if getattr(_ai, "region", None):
                _m += f" (region {_ai.region})"
            facts.append(_m)
        # --- Connect: source/target coordinates (session state) ---
        sc = getattr(st, "source_config", None)
        if sc is not None:
            ver = getattr(st, "source_server_version", None)
            src = f"Source: MySQL at {sc.host}"
            if getattr(sc, "database", None):
                src += f" (db {sc.database})"
            if ver:
                src += f", {ver}"
            src += "; verified" if st.source_verified else "; not yet verified"
            facts.append(src)
        tc = getattr(st, "target_config", None)
        if tc is not None:
            tgt = (
                f"Target: Aurora DSQL {tc.cluster_endpoint} in {tc.region} "
                f"(db {tc.database}, role {tc.username})"
            )
            tgt += "; verified" if st.target_verified else "; not yet verified"
            facts.append(tgt)
        # --- Evaluation: assessment verdict (counts only) ---
        try:
            _ev = EVALUATION_STORE.get(session_id)
            _res = _ev.result if _ev is not None else None
            if _res is not None:
                _c = {k.value: n for k, n in _res.assessment.summary.items()}
                _total = sum(_c.values())
                _line = (
                    f"Assessment: {_total} objects — {_c.get('AUTO', 0)} auto, "
                    f"{_c.get('MANUAL', 0)} review, {_c.get('UNSUPPORTED', 0)} unsupported"
                )
                _conf = len(_res.target_conflicts or [])
                if _conf:
                    _line += f", {_conf} target conflict(s)"
                facts.append(_line)
        except Exception:  # noqa: BLE001 - context is best-effort; never break the panel
            pass
        # --- Schema Conversion: bulk-apply outcome (counts only) ---
        try:
            from dsql_migrator.ui.schema_conversion import _summarize_apply
            from dsql_migrator.ui.schema_conversion_apply import ObjectApplyStatus

            _ar = SCHEMA_CONVERSION_STORE.get_or_create(session_id).apply_results
            if _ar is not None:
                _s = _summarize_apply(_ar)
                facts.append(
                    f"Schema apply: {_s[ObjectApplyStatus.CREATED]} created, "
                    f"{_s[ObjectApplyStatus.SKIPPED]} skipped, "
                    f"{_s[ObjectApplyStatus.FAILED]} failed"
                )
        except Exception:  # noqa: BLE001
            pass
        # --- Data Migration: Full Load progress + CDC state (counts/booleans only) ---
        try:
            from dsql_migrator.ui.data_migration import (
                _current_job,
                summarize_progress,
            )

            _dm = DATA_MIGRATION_STORE.get_or_create(session_id)
            _fl_job = _current_job(JOB_MANAGER, _dm.job_id)
            if _fl_job is not None:
                _fl = summarize_progress(_fl_job)
                _line = f"Full Load: {_fl.done_tables}/{_fl.total_tables} tables done"
                if _fl.failed_tables:
                    _line += f", {_fl.failed_tables} failed"
                _line += f", {_fl.rows_loaded:,} rows ({_fl.progress_pct:.0f}%)"
                facts.append(_line)
            if cdc_streaming_started(_dm, JOB_MANAGER):
                _n = len(getattr(_dm, "cdc_connector_names", []) or [])
                facts.append(
                    f"CDC: streaming ({_n} connector{'' if _n == 1 else 's'})"
                )
        except Exception:  # noqa: BLE001
            pass
        # --- Validation: latest verdict (counts only) ---
        try:
            from dsql_migrator.ui.validation import summarize_validation

            _vr = VALIDATION_STORE.get_or_create(session_id).result
            if _vr is not None:
                _vs = summarize_validation(_vr)
                _line = (
                    f"Validation: {'MATCH' if _vs.is_match else 'MISMATCH'} "
                    f"{_vs.matched_tables}/{_vs.total_tables} ({_vs.mode})"
                )
                if not _vs.is_match and (_vs.missing_on_target or _vs.extra_on_target):
                    _line += (
                        f" [missing {_vs.missing_on_target}, extra {_vs.extra_on_target}]"
                    )
                facts.append(_line)
        except Exception:  # noqa: BLE001
            pass
        return MigrationContext(
            current_step=step, migration_type=mtype, summary="\n".join(facts)
        )

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
                    _out["full_load"] = {
                        "total_tables": _fl.total_tables, "done_tables": _fl.done_tables,
                        "failed_tables": _fl.failed_tables,
                        "in_progress_tables": _fl.in_progress_tables,
                        "pending_tables": _fl.pending_tables,
                        "rows_loaded": _fl.rows_loaded,
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
                _failed = [
                    {"table": c.chunk_id, "error": str(_msgs.get(c.chunk_id, ""))[:500]}
                    for c in _job.chunks
                    if getattr(c, "status", "") == "FAILED"
                ]
                _quarantined = sorted(
                    {t for t, m in _msgs.items() if str(m).startswith(_quar_prefix)}
                )
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

    def _general_ai_streamer() -> object:
        # The panel's "ask anything about this migration" streamer, used when it is
        # opened from the header with no specific object scope. Grounded on the current
        # MigrationContext + given READ-ONLY tools (function calling) so it can look up
        # this migration's real data (converted DDL, assessment, validation, load
        # status) on demand and answer with actual values, not generically. Carries the
        # same migration-only guardrail. None when AI is off (the panel stays inert).
        st = SESSION_STORE.get_or_create(session_id)
        if not st.ai_assist.enabled:
            return None
        strategist = AssessmentStrategist(st.ai_assist, aws_profile=st.aws_profile)
        ctx = _ai_context()
        system = (
            build_general_chat_system(
                current_step=ctx.current_step,
                migration_type=ctx.migration_type,
                summary=ctx.summary,
            )
            + _AI_TOOLS_SYSTEM_HINT
        )
        return lambda messages, on_delta: strategist.tool_chat(
            system, messages, on_delta,
            tools=_AI_TOOL_SCHEMAS, execute=_ai_tool_execute,
        )

    # Build each step's (content_builder, runner). These only prepare closures;
    # nothing renders until the sidebar selects and invokes a screen.
    # Step 1 (Evaluation) is the first workflow step after Connect. The migration
    # TYPE (and the CDC-infrastructure deploy) belong to Data Migration, so there is
    # no separate up-front plan screen.
    evaluation_content, evaluation_runner = build_evaluation_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        open_ai_scope=_open_ai_scope,
        ai_post_event=_ai_post_event,
        # Give the per-object/finding guidance chat the same read-only tools the
        # general chat has, so it can answer wider-migration questions too.
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )
    # Step 2 (Schema Conversion): object browsing, DDL preview, query conversion,
    # and target apply.
    # Late-bound navigation: build_workflow_sidebar hands back its ``select``
    # function (below) so the Schema Conversion screen can jump straight to Data
    # Migration when the user clicks "Skip conversion & continue".
    _nav: dict[str, Callable[[object], None]] = {}

    schema_content, schema_runner = build_schema_conversion_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        conv_store=SCHEMA_CONVERSION_STORE,
        on_continue_to_data_migration=lambda: _nav["select"](
            WorkflowStep.FULL_LOAD
        ),
        # Block applying schema while CDC is live: the sink is writing the target
        # tables and a REPLACE would drop them (DDL is not replicated). Probes the
        # same data-migration state that drives the CDC lock.
        cdc_active_check=lambda: cdc_streaming_started(
            DATA_MIGRATION_STORE.get_or_create(session_id), JOB_MANAGER
        ),
        open_ai_scope=_open_ai_scope,
        ai_post_event=_ai_post_event,
        # Give the per-object conversion chat the same read-only tools as Evaluation,
        # so it can name the exact offending column/key and answer wider-migration
        # questions (e.g. "which triggers can't be converted?") on demand.
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )
    # Data Migration is a single step with an inner migration-type selector
    # (Full load only / CDC only / Full load + CDC). One builder serves it; the
    # Full Load phase drives the snapshot run, and in the combined type CDC
    # CDC step opens automatically from the Full Load watermark.
    data_migration_content, data_migration_runner = build_data_migration_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        migration_store=DATA_MIGRATION_STORE,
        conv_store=SCHEMA_CONVERSION_STORE,
        staging_bucket=config.staging_bucket,
        cdc_deploy_role_arn=config.cdc_deploy_role_arn,
        cdc_secret_kms_key_id=config.cdc_secret_kms_key_id,
        validation_store=VALIDATION_STORE,
        open_ai_scope=_open_ai_scope,
        ai_post_event=_ai_post_event,
        # The per-failed-table diagnosis chat can look up the real converted DDL /
        # target schema / source structure to root-cause a schema/DDL failure by name.
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )
    # Step 4 (Validation): compares the migrated target against the source as-of
    # the Step 3 watermark and reports consistency and drift.
    validation_content, validation_runner = build_validation_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        migration_store=DATA_MIGRATION_STORE,
        validation_store=VALIDATION_STORE,
        # Lets a CHECKSUM run resolve the APPLIED target types (Schema-Conversion remaps).
        conversion_store=SCHEMA_CONVERSION_STORE,
        open_ai_scope=_open_ai_scope,
        ai_post_event=_ai_post_event,
        # The mismatch chat can root-cause a divergence against the real converted
        # DDL / target schema / live row counts via the shared read-only tools.
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )
    # Step 6 (Cut over): guidance for switching the application from MySQL to
    # DSQL. The tool cannot perform/verify the cut-over, so this step has no job —
    # the runner marks it DONE on the user's acknowledgement; the content reflects
    # the last validation verdict (clean MATCH -> runbook, else "validate first").
    cutover_content, cutover_runner = build_cutover_screen(
        SESSION_STORE,
        session_id,
        validation_store=VALIDATION_STORE,
        job_manager=JOB_MANAGER,
        ai_post_event=_ai_post_event,
        # The repoint-recipe / "safe to cut over?" chat can consult the real validation,
        # CDC and load state via the shared read-only tools -- never sees secrets.
        open_ai_scope=_open_ai_scope,
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )
    # Optional tool (not a workflow step): the Query Playground — convert a MySQL
    # statement to DSQL and non-destructively test whether it runs on the target.
    query_playground_content = build_query_playground_screen(
        SESSION_STORE,
        session_id,
        playground_store=PLAYGROUND_STORE,
        open_ai_scope=_open_ai_scope,
        ai_post_event=_ai_post_event,
        # The query chat can check its statement against the real converted/target
        # schema (does this table/column exist on the target yet?) via the tools.
        ai_tool_execute=_ai_tool_execute,
        ai_tools=_AI_TOOL_SCHEMAS,
    )

    def schema_run_guard() -> str | None:
        # Disable the bulk Run until the user has selected (ticked) at least one
        # object in the Schema Conversion object browser.
        conv_state = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        if not conv_state.ticked_node_ids:
            return "Select one or more objects in the Object browser first."
        return None

    def validation_run_guard() -> str | None:
        # Disable "Re-run validation" while a per-table re-check owns the single
        # validation job slot (a full run would orphan it and clear the report it
        # is about to merge into).
        return validation_run_guard_reason(
            JOB_MANAGER, VALIDATION_STORE.get_or_create(session_id)
        )

    def data_migration_run_guard() -> str | None:
        # Disable the Full Load Run until the prerequisite checks have been run and
        # all required checks pass (Property 14). The mode is derived from the
        # selected migration type -- exactly as the in-content guard does -- so the
        # two never disagree: hardcoding FULL_LOAD here let the sidebar Run appear
        # enabled for a CDC type whose (superset) checks had not run.
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        eval_state = EVALUATION_STORE.get_or_create(session_id)
        result = eval_state.result
        inventory = result.inventory if result is not None else None
        return full_load_run_guard_reason(
            migration_state,
            inventory,
            prereq_mode=prereq_mode_for_type(migration_state.migration_type),
        )

    # Resume support (Property 4): restore this session's persisted snapshot once
    # per process when the in-memory session is still fresh, and persist a
    # snapshot on each state change. The save is dirty-checked by a cheap
    # signature so a large inventory is never re-serialized on every UI poll.
    def _persist_session() -> None:
        if SESSION_STATE_STORE is None:
            return
        from dsql_migrator.ui.session_persistence import (
            capture_session_snapshot,
            session_signature,
        )

        session = SESSION_STORE.get_or_create(session_id)
        eval_state = EVALUATION_STORE.get_or_create(session_id)
        conv_state = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        validation_state = VALIDATION_STORE.get_or_create(session_id)
        signature = session_signature(
            session, eval_state, conv_state, migration_state, validation_state
        )
        if _LAST_SESSION_SIGNATURE.get(session_id) == signature:
            return
        _LAST_SESSION_SIGNATURE[session_id] = signature
        SESSION_STATE_STORE.save(
            capture_session_snapshot(
                session_id, session, eval_state, conv_state, migration_state,
                validation_state,
            )
        )

    # Restore whenever the in-memory session is still FRESH (uninitialized), not
    # just once per process. A reopened browser tab can hand back a brand-new,
    # empty session object for the same (cookie-stable) session_id; gating on
    # freshness re-hydrates it from the snapshot, while a populated/in-progress
    # session is never clobbered (session_is_fresh is False for it). This is what
    # makes "close the tab, reopen" resume instead of showing a blank workflow.
    if SESSION_STATE_STORE is not None:
        from dsql_migrator.ui.session_persistence import (
            apply_session_snapshot,
            session_is_fresh,
        )

        _session = SESSION_STORE.get_or_create(session_id)
        _eval = EVALUATION_STORE.get_or_create(session_id)
        _conv = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        _mig = DATA_MIGRATION_STORE.get_or_create(session_id)
        _val = VALIDATION_STORE.get_or_create(session_id)
        if session_is_fresh(_session, _eval, _mig):
            _snapshot = SESSION_STATE_STORE.load(session_id)
            if _snapshot is not None:
                apply_session_snapshot(
                    _snapshot, _session, _eval, _conv, _mig, _val
                )
                # Back-compat: CDC was folded into the unified Data Migration nav
                # step (WorkflowStep.FULL_LOAD). A session saved while viewing the
                # old standalone "cdc" nav step would restore an active_view that
                # no longer maps to a step_content entry; redirect it so the page
                # opens the Data Migration step instead of a blank view.
                if _session.active_view == WorkflowStep.CDC.value:
                    _session.set_active_view(WorkflowStep.FULL_LOAD.value)
                # Back-compat: the Migration plan step was retired (its CDC decision
                # is the Data Migration type selector, and its infra deploy moved to
                # that step's Prerequisites sub-step). A session parked on it would
                # restore an active_view with no step_content entry, and _restore_view
                # silently falls back to Connect -- losing the user's place. Send them
                # to Evaluation, which is now the first workflow step.
                if _session.active_view == WorkflowStep.MIGRATION_PLAN.value:
                    _session.set_active_view(WorkflowStep.EVALUATION.value)

    def _reset_session() -> None:
        # "Start over": wipe ALL per-session in-memory state + the durable
        # snapshot for this session. Clears only the tool's workbench -- never any
        # AWS resource. Use reset_in_place (not clear/pop): the workflow screen and
        # its content builders captured these state objects in closures at build
        # time, and Start over does NOT rebuild the page (it just refreshes). A
        # pop+recreate would orphan those captured references -- e.g. re-verifying
        # the connections would update a NEW session object while the nav guard
        # still reads the old (locked) one, so steps never unlock. Resetting the
        # SAME objects in place keeps every closure pointing at the live, wiped
        # state.
        SESSION_STORE.reset_in_place(session_id)
        EVALUATION_STORE.reset_in_place(session_id)
        SCHEMA_CONVERSION_STORE.reset_in_place(session_id)
        DATA_MIGRATION_STORE.reset_in_place(session_id)
        if VALIDATION_STORE is not None:
            try:
                VALIDATION_STORE.reset_in_place(session_id)
            except Exception:  # noqa: BLE001 - reset is best-effort
                pass
        try:
            PLAYGROUND_STORE.reset_in_place(session_id)
        except Exception:  # noqa: BLE001 - reset is best-effort
            pass
        if SESSION_STATE_STORE is not None:
            SESSION_STATE_STORE.delete(session_id)
        _LAST_SESSION_SIGNATURE.pop(session_id, None)

    def _cdc_deployed() -> bool:
        """True when ANY CDC AWS resource exists, so Start over can offer to tear it
        down. Existence -- not health -- is what matters: a connector or stack that
        is FAILED / mid-rollback / half-deployed still bills for MSK / NAT and must
        be offered for teardown just like a running one.

        So this is truthy when either (a) any of MY connectors exist in ANY state
        (``cdc_connector_names`` is populated existence-based by ``_filter_mine``,
        regardless of RUNNING), or (b) the cdc-stack phase is anything other than
        ``absent`` -- i.e. ``running`` / ``infra`` / ``unstable`` (a stuck/rolled-
        back stack is still deployed). Only ``absent`` (no stack at all) is safe.
        This matches the CDC step, which already offers Delete for the ``unstable``
        phase.

        (c) covers the case the first two MISS: a cdc-stack under a name this session
        does not target (``cdc_other_stacks``). Both signals above are scoped to
        ``cdc_stack_name``, so a stack deployed by an earlier session -- or with a
        custom suffix -- left Start over reporting no CDC at all while the Data
        Migration screen simultaneously offered to ATTACH to that very stack. The two
        prompts contradicted each other about the same resource, and the one that
        stayed silent is the one that would have offered to stop the MSK / NAT
        billing. Existence in the account is what matters for a teardown offer, not
        whether this session happens to own the name."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        if getattr(migration_state, "cdc_connector_names", None):
            return True
        phase = getattr(migration_state, "cdc_stack_phase", None)
        if phase is not None and phase != "absent":
            return True
        return bool(getattr(migration_state, "cdc_other_stacks", None))

    def _cdc_teardown_stack_names() -> list:
        """EVERY cdc-stack a Start-over teardown would act on, in teardown order.

        Delegates to the pure :func:`cdc_teardown_stack_names` so the offer
        (:func:`_cdc_deployed`), the dialog's tile listing, and the teardown itself all
        read one resolution -- they must never disagree about which stacks exist.

        This used to resolve a SINGLE name and adopt a discovered stack only when there
        was exactly one; with two or more it fell back to this session's own name, which
        in that branch does not exist. So the dialog offered "Delete all CDC
        infrastructure", the delete found nothing, and the operator kept paying for MSK /
        NAT behind a success toast. Every discovered stack is now returned and torn down.
        """
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_stack_names

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        return cdc_teardown_stack_names(
            own_stack_name=getattr(migration_state, "cdc_stack_name", None),
            stack_phase=getattr(migration_state, "cdc_stack_phase", None),
            connector_names=getattr(migration_state, "cdc_connector_names", None) or [],
            other_stacks=getattr(migration_state, "cdc_other_stacks", None) or [],
        )

    def _cdc_teardown_stack_name() -> Optional[str]:
        """The FIRST cdc-stack a teardown would act on (or the session's own name).

        Kept for the single-stack callers (the orphan-billing caution's custom-name
        wording). Prefer :func:`_cdc_teardown_stack_names` anywhere the full set matters.
        """
        names = _cdc_teardown_stack_names()
        if names:
            return names[0]
        return getattr(
            DATA_MIGRATION_STORE.get_or_create(session_id), "cdc_stack_name", None
        )

    def _cdc_stack_name() -> Optional[str]:
        """Back-compat alias for :func:`_cdc_teardown_stack_name` (same resolution)."""
        return _cdc_teardown_stack_name()

    def _cdc_teardown_in_flight() -> bool:
        """True when a CDC stop/delete is CURRENTLY running, so Start over must not
        race it (a second teardown + a session wipe that hides the running delete,
        unre-discoverable for a custom stack name). Thin session-bound wrapper over
        the pure :func:`cdc_teardown_in_flight` predicate: passes the durable teardown
        marker (which survives the reset and closes the post-Start-over race window),
        the local lifecycle job + kind, and the freshly-probed raw stack status. The
        job/stack signals are refreshed by ``_cdc_probe`` just before the dialog opens.
        """
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        return cdc_teardown_in_flight(
            JOB_MANAGER,
            teardown_job_id=getattr(migration_state, "cdc_teardown_job_id", None),
            deploy_job_id=getattr(migration_state, "cdc_deploy_job_id", None),
            action_kind=getattr(migration_state, "cdc_action_kind", None),
            stack_status=getattr(migration_state, "cdc_stack_phase_status", None),
        )

    def _cdc_teardown_banner() -> Optional[dict]:
        """Info for the persistent 'CDC teardown in progress' banner, or ``None``.

        Reads the durable teardown marker (which survives Start-over's session reset)
        and the JobManager status -- cheap, no AWS call. Returns ``{"kind", "stack"}``
        while the stop/delete job is PENDING/RUNNING so the banner shows on EVERY view
        (including the Connect screen a Start-over → delete lands on); once the job
        while running it returns ``{"state":"running",...}``; when the teardown
        FAILED it returns ``{"state":"failed",...}`` (an actionable banner, marker
        kept so the user can retry/dismiss); when it settled OK (or the job was lost)
        it clears the marker and returns ``None`` so the banner disappears and the
        Start-over guard releases.
        """
        from dsql_migrator.ui.data_migration._cdc_status import (
            cdc_teardown_banner_state,
            stack_status_needs_cleanup as _stack_status_needs_cleanup,
        )

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        job_id = getattr(migration_state, "cdc_teardown_job_id", None)
        if not job_id:
            # No teardown -- but an in-flight INFRASTRUCTURE create also needs the
            # cross-view banner: it runs ~15-20 min and is meant to overlap the Full
            # Load, so once the user leaves the Data Migration screen there would
            # otherwise be no sign it is still going (and they might wait on it).
            # Reuses _cdc_op_in_flight, which already reads the deploy job + kind.
            if _cdc_op_in_flight() == "infra":
                return {
                    "state": "running",
                    "kind": "infra",
                    "stack": getattr(migration_state, "cdc_stack_name", None),
                }
            return None
        state = cdc_teardown_banner_state(JOB_MANAGER, job_id)
        # Self-heal a stale FAILED banner: a DELETE_FAILED freezes both the job record
        # (state=="failed") and the cached stack status, so an out-of-band cleanup
        # (operator finished the delete via the console/CLI) would leave the banner up
        # forever. When we WOULD show failed, do one best-effort read-only live check;
        # only if CloudFormation DEFINITIVELY reports the stack gone do we neutralize
        # BOTH failed sources -- the job-derived state here and the cached status below
        # -- and fall through to the existing clean-completion path (which records the
        # dismissable "deleted" notice and clears the marker). A still-present or
        # ambiguous/errored read leaves the failed banner untouched (never hides a
        # real, still-billing failure). This is the only self-poll-less banner, so the
        # extra read fires only while a failure would be shown.
        would_show_failed = state == "failed" or _stack_status_needs_cleanup(
            getattr(migration_state, "cdc_stack_phase_status", None)
        )
        if would_show_failed and _cdc_teardown_stack_confirmed_gone(migration_state):
            migration_state.set_cdc_stack_phase("absent", status=None)
            if state == "failed":
                state = None
        if state in ("running", "failed"):
            info = {
                "state": state,
                "kind": getattr(migration_state, "cdc_teardown_kind", None),
                "stack": getattr(migration_state, "cdc_teardown_stack", None),
            }
            # With several stacks, say WHICH one of how many is being torn down -- the
            # banner names a single stack, so without this it looked like the only one.
            from dsql_migrator.ui.data_migration._cdc_status import (
                teardown_queue_progress,
            )

            progress = teardown_queue_progress(
                getattr(migration_state, "cdc_teardown_queue", None) or [], job_id
            )
            if progress is not None:
                info["index"], info["total"] = progress
            return info

        # The tracked stack settled -- but with several stacks the others may still be
        # deleting. Re-point the marker at the next unfinished one instead of clearing,
        # which is what made the banner vanish while MSK / NAT kept billing.
        queue = list(getattr(migration_state, "cdc_teardown_queue", None) or [])
        if len(queue) > 1:
            from dsql_migrator.ui.data_migration._cdc_status import (
                next_unfinished_teardown,
            )

            following = next_unfinished_teardown(JOB_MANAGER, queue)
            if following is not None:
                next_job, next_stack, index, total = following
                migration_state.advance_cdc_teardown(next_job, next_stack)
                return {
                    "state": "running",
                    "kind": getattr(migration_state, "cdc_teardown_kind", None),
                    "stack": next_stack,
                    "index": index,
                    "total": total,
                }
        # The job settled OK (DONE) or is unknown (lost across a restart) -- but the
        # JOB finishing is not the same as the STACK being gone. A delete that ended in
        # DELETE_FAILED, or a job whose record vanished with the process, previously
        # cleared the marker here and the banner went silent: the leftover MSK / NAT
        # kept billing with nothing in the UI saying so. So before clearing, check the
        # last probed stack status (cached -- no AWS call on this render path) and keep
        # an actionable failed banner when the stack is still in a failed/rolled-back
        # state.
        if _stack_status_needs_cleanup(
            getattr(migration_state, "cdc_stack_phase_status", None)
        ):
            return {
                "state": "failed",
                "kind": getattr(migration_state, "cdc_teardown_kind", None),
                "stack": getattr(migration_state, "cdc_teardown_stack", None),
            }
        # Everything finished cleanly. Record it as a DISMISSABLE completion notice before
        # clearing the marker: a 15-45 min teardown is meant to be left unattended, and
        # the only completion signal was a toast that a refresh throws away -- so the
        # operator came back to an empty screen with no way to tell whether it finished or
        # never ran. Names every stack, since a multi-stack teardown showed only one.
        from dsql_migrator.ui.data_migration._cdc_status import finished_teardown_stacks

        migration_state.set_cdc_teardown_done(
            kind=getattr(migration_state, "cdc_teardown_kind", None),
            stacks=finished_teardown_stacks(
                getattr(migration_state, "cdc_teardown_queue", None) or [],
                getattr(migration_state, "cdc_teardown_stack", None),
            ),
        )
        migration_state.clear_cdc_teardown()
        return None

    def _cdc_teardown_done() -> Optional[dict]:
        """A finished-but-undismissed teardown for the completion banner, else ``None``."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        done = dict(getattr(migration_state, "cdc_teardown_done", None) or {})
        return done or None

    def _cdc_teardown_done_dismiss() -> None:
        """Close the completion banner (the operator acknowledged the result)."""
        DATA_MIGRATION_STORE.get_or_create(session_id).dismiss_cdc_teardown_done()

    def _cdc_op_in_flight() -> Optional[str]:
        """Kind (``"infra"``/``"start"``) of a NON-teardown CDC lifecycle job still
        running, else ``None``.

        Lets Start over WARN (not hard-block) that a deploy/start will keep running in
        the background after the reset, so an orphaned job is never a silent surprise.
        Teardowns (stop/delete) are the destructive/billing-relevant case and stay
        hard-blocked by :func:`_cdc_teardown_in_flight`; a deploy/start is
        re-discoverable and must not trap a user trying to escape a stuck run.
        """
        from dsql_migrator.ui.data_migration._cdc_status import _current_job

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        job = _current_job(
            JOB_MANAGER, getattr(migration_state, "cdc_deploy_job_id", None)
        )
        kind = getattr(migration_state, "cdc_action_kind", None)
        if (
            job is not None
            and getattr(job, "status", None) in ("PENDING", "RUNNING")
            and kind in ("infra", "start")
        ):
            return kind
        return None

    def _cdc_probe() -> None:
        """Refresh the cached CDC deployment state from a live, read-only AWS probe.

        Called (off the event loop) when the user opens Start over, so the dialog
        reflects the ACTUAL deployed CDC -- and offers the stop/delete tiles --
        regardless of which step the user was on. ``_ensure_cdc_controller`` is
        throttled per session; clear the throttle timestamp first so this explicit
        user action always gets a fresh read. Best-effort and read-only."""
        from dsql_migrator.ui.data_migration._cdc_status import _ensure_cdc_controller

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        migration_state._cdc_discovery_monotonic = None  # bypass the render throttle
        try:
            _ensure_cdc_controller(migration_state, session)
        except Exception:  # noqa: BLE001 - leave cached state; dialog opens regardless
            pass

    def _launch_cdc_teardown(
        migration_state,
        *,
        mode,
        stack_name,
        region,
        role_arn,
        aws_profile,
        cleanup_secret,
        track_in_queue: Optional[list] = None,
    ) -> Optional[str]:
        """Build the deployer and submit a CDC teardown (stop/delete) as a background
        job, recording the durable marker + the retry context ({region, role_arn,
        profile, cleanup_secret}). Shared by the Start-over teardown and the banner's
        one-click "Retry cleanup". Returns the submitted job id (or ``None`` when the
        ownership guard declined to overwrite a different, still-running teardown).
        """
        from dsql_migrator.core.cdc_deployer import (
            build_cdc_stack_deployer,
            run_cdc_delete,
            run_cdc_stop,
        )
        from dsql_migrator.ui.data_migration._cdc_status import (
            should_replace_teardown_marker,
        )

        deployer = build_cdc_stack_deployer(
            region, aws_profile=aws_profile, assume_role_arn=role_arn
        )
        if mode == "delete":

            def work(handle) -> None:
                run_cdc_delete(
                    handle,
                    stack_name=stack_name,
                    deployer=deployer,
                    on_log=lambda _ts, _msg: None,
                    region=region,
                    aws_profile=aws_profile,
                    cleanup_source_secret=cleanup_secret,
                )

        else:

            def work(handle) -> None:
                run_cdc_stop(
                    handle,
                    stack_name=stack_name,
                    deployer=deployer,
                    on_log=lambda _ts, _msg: None,
                )

        job_id = JOB_MANAGER.submit(work)
        # Record it in the caller's queue REGARDLESS of who wins the marker below: with
        # several stacks only the first claims the single marker, so the queue is the only
        # thing that keeps the rest tracked (and lets the banner advance to them).
        if track_in_queue is not None:
            track_in_queue.append((job_id, stack_name))
        # Durable marker (survives the Start-over reset) + the retry context so the
        # banner can re-launch this teardown even after the session is wiped. Ownership
        # guard: don't clobber a DIFFERENT teardown still tracked+running -- keep the
        # first, longer-lived one. This is the normal case for a multi-stack Start-over
        # teardown (stack 2+ is refused), not just a two-tab race.
        if should_replace_teardown_marker(
            JOB_MANAGER, getattr(migration_state, "cdc_teardown_job_id", None), job_id
        ):
            migration_state.set_cdc_teardown(
                job_id,
                kind=mode,
                stack=stack_name,
                ctx={
                    "region": region,
                    "role_arn": role_arn,
                    "profile": aws_profile,
                    "cleanup_secret": cleanup_secret,
                },
            )
            return job_id
        return None

    def _cdc_teardown_on_reset(mode: str) -> None:
        """Submit a CDC teardown as part of Start over (called BEFORE the reset).

        ``mode`` is ``"stop"`` (delete only the 2 MSK connectors, keep MSK/VPC/IAM
        for a fast restart) or ``"delete"`` (tear down the whole cdc-stack). All
        config is captured now (into the durable marker's retry ctx and the job
        closure), so the imminent session reset cannot race it; the teardown runs in
        the background and stays visible/retryable via the persistent banner.
        """
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        target = getattr(session, "target_config", None)
        region = getattr(target, "region", None) if target else None
        if not region and target is not None:
            endpoint = getattr(target, "cluster_endpoint", "") or ""
            if ".dsql." in endpoint and ".on.aws" in endpoint:
                region = endpoint.split(".dsql.")[1].split(".on.aws")[0]
        # Act on EVERY stack the offer was made about -- the dialog now lists them by
        # name, so tearing down only the first would contradict what the operator just
        # read and confirmed. Reading cdc_stack_name directly here (or resolving a single
        # name) targeted a non-existent stack whenever two or more were discovered:
        # delete nothing, leave MSK / NAT billing, report success.
        stack_names = _cdc_teardown_stack_names()
        if not region or not stack_names:
            return
        aws_profile = getattr(session, "aws_profile", None)
        role_arn = getattr(migration_state, "cdc_deploy_role_arn", None)
        cleanup_secret = mode == "delete" and not getattr(
            session, "source_secret_id", None
        )
        # The durable marker tracks ONE teardown, so the banner follows the first stack
        # and the rest are launched behind it. Only the first claims the marker (the
        # ownership guard declines the others while it is still running); each still runs
        # to completion as its own background job. The per-stack plan (including where the
        # shared source-secret cleanup belongs) comes from the pure cdc_teardown_plan.
        from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_plan

        job_id = None
        launched_queue: list[tuple[str, str]] = []
        for stack_name, stack_cleanup_secret in cdc_teardown_plan(
            stack_names, cleanup_secret=cleanup_secret
        ):
            launched = _launch_cdc_teardown(
                migration_state,
                mode=mode,
                stack_name=stack_name,
                region=region,
                role_arn=role_arn,
                aws_profile=aws_profile,
                cleanup_secret=stack_cleanup_secret,
                # Only the first claims the single durable marker; the queue below is
                # what keeps the rest visible.
                track_in_queue=launched_queue,
            )
            if job_id is None:
                job_id = launched
        # Record EVERY launched teardown so the banner can advance to the next unfinished
        # stack when the tracked one settles. Without this the banner followed only the
        # first stack and went silent while the others were still deleting -- MSK / NAT
        # billing on, nothing on screen.
        if launched_queue:
            migration_state.set_cdc_teardown_queue(launched_queue)
        # The teardown runs in the background after the session resets; surface a
        # completion toast (the persistent banner also tracks it). If the marker was
        # not claimed (another teardown already running), poll whatever is tracked.
        from nicegui import ui

        if job_id is None:
            job_id = getattr(migration_state, "cdc_teardown_job_id", None)
        if mode == "delete":
            started = "Deleting CDC infrastructure in the background (~45 min)…"
            done = "CDC infrastructure deleted — MSK/NAT billing stopped."
        else:
            started = "Removing the CDC connectors in the background…"
            done = "CDC connectors removed — you can start a new migration."
        ui.notify(started, type="info", position="top", timeout=6000)  # type: ignore[attr-defined]

        timer_ref: dict = {"t": None}

        def _poll() -> None:
            try:
                status = JOB_MANAGER.get_status(job_id).status
            except Exception:  # noqa: BLE001 - treat an unreadable job as failed
                status = "FAILED"
            if status not in ("DONE", "FAILED", "CANCELLED"):
                return
            if timer_ref["t"] is not None:
                timer_ref["t"].active = False
            if status == "DONE":
                ui.notify(done, type="positive", position="top", timeout=8000)  # type: ignore[attr-defined]
            else:
                ui.notify(  # type: ignore[attr-defined]
                    "CDC teardown did not complete cleanly — a banner with a "
                    "Retry cleanup button stays on screen until it succeeds.",
                    type="warning",
                    position="top",
                    timeout=8000,
                )

        timer_ref["t"] = ui.timer(10.0, _poll)  # type: ignore[attr-defined]

    def _cdc_teardown_retry() -> None:
        """Re-launch the tracked teardown from the durable marker's retry context.

        Backs the failed-teardown banner's one-click "Retry cleanup": rebuilds the
        deployer from the saved region/role/profile (the session may have been wiped
        by Start over) and re-submits the teardown, which retains any stuck resource
        and re-issues the delete (recover_delete_failed). Sets the marker back to
        running so the banner returns to the in-progress state.
        """
        from nicegui import ui

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        stack_name = getattr(migration_state, "cdc_teardown_stack", None)
        kind = getattr(migration_state, "cdc_teardown_kind", None) or "delete"
        ctx = dict(getattr(migration_state, "cdc_teardown_ctx", {}) or {})
        region = ctx.get("region")
        if not stack_name or not region:
            ui.notify(  # type: ignore[attr-defined]
                "Can't retry automatically here — open the Data Migration → CDC step "
                "and use Delete CDC infrastructure.",
                type="warning",
                position="top",
            )
            return
        # Clear first so the ownership guard sees a fresh claim (the old FAILED job is
        # a different, settled one, so it would allow the write anyway).
        migration_state.clear_cdc_teardown()
        _launch_cdc_teardown(
            migration_state,
            mode=kind,
            stack_name=stack_name,
            region=region,
            role_arn=ctx.get("role_arn"),
            aws_profile=ctx.get("profile"),
            cleanup_secret=bool(ctx.get("cleanup_secret")),
        )
        ui.notify("Retrying CDC cleanup…", type="info", position="top")  # type: ignore[attr-defined]

    def _cdc_teardown_dismiss() -> None:
        """Dismiss a FAILED teardown banner (clear the marker only). The AWS resources
        are NOT touched -- the user chooses to stop tracking it here (e.g. they will
        finish cleanup in the console)."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        migration_state.clear_cdc_teardown()

    def _cdc_teardown_stack_confirmed_gone(migration_state) -> bool:
        """Best-effort live check: is the tracked teardown stack DEFINITIVELY gone?

        Self-heals the stale "CDC teardown failed" banner. A DELETE_FAILED freezes the
        job record + cached status at failed, so if the operator finishes cleanup out
        of band (e.g. terminates the ENI-pinning bastion, re-runs delete-stack) the
        banner would otherwise linger forever. Rebuilds a deployer from the durable
        marker's retry ctx (region/role/profile -- the session may have been wiped by
        Start over, exactly as ``_cdc_teardown_retry`` does) and delegates to
        ``teardown_stack_confirmed_gone``, which returns True ONLY on a definitive
        does-not-exist. No ctx/region, or ANY error -> False (keep the banner).
        """
        stack_name = getattr(migration_state, "cdc_teardown_stack", None)
        ctx = dict(getattr(migration_state, "cdc_teardown_ctx", {}) or {})
        region = ctx.get("region")
        if not stack_name or not region:
            return False
        try:
            from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer
            from dsql_migrator.ui.data_migration._cdc_status import (
                teardown_stack_confirmed_gone,
            )

            deployer = build_cdc_stack_deployer(
                region,
                aws_profile=ctx.get("profile"),
                assume_role_arn=ctx.get("role_arn"),
            )
            return teardown_stack_confirmed_gone(deployer, stack_name)
        except Exception:  # noqa: BLE001 - never let the self-heal break the render
            return False

    build_workflow_sidebar(
        SESSION_STORE,
        session_id,
        app_title="MySQL to Aurora DSQL Migration Tool",
        version=__version__,
        connect_builder=lambda go_to_first_step, on_connection_change: (
            build_connect_page(
                SESSION_STORE,
                session_id,
                on_next=go_to_first_step,
                on_connection_change=on_connection_change,
                defaults=connect_defaults,
                open_ai_scope=_open_ai_scope,
                ai_post_event=_ai_post_event,
            )
        ),
        step_content={
            WorkflowStep.EVALUATION: evaluation_content,
            WorkflowStep.SCHEMA_CONVERSION: schema_content,
            WorkflowStep.FULL_LOAD: data_migration_content,
            WorkflowStep.VALIDATION: validation_content,
            WorkflowStep.CUT_OVER: cutover_content,
        },
        runners={
            WorkflowStep.EVALUATION: evaluation_runner,
            WorkflowStep.SCHEMA_CONVERSION: schema_runner,
            WorkflowStep.FULL_LOAD: data_migration_runner,
            WorkflowStep.VALIDATION: validation_runner,
            WorkflowStep.CUT_OVER: cutover_runner,
        },
        run_guards={
            WorkflowStep.SCHEMA_CONVERSION: schema_run_guard,
            WorkflowStep.FULL_LOAD: data_migration_run_guard,
            WorkflowStep.VALIDATION: validation_run_guard,
        },
        on_state_change=_persist_session,
        nav_export=lambda select_fn: _nav.__setitem__("select", select_fn),
        footer_extra=lambda: _render_footer_tools(config.activity_log_path),
        on_reset=_reset_session,
        on_reset_cdc=_cdc_teardown_on_reset,
        cdc_deployed_getter=_cdc_deployed,
        cdc_stack_name_getter=_cdc_stack_name,
        cdc_stack_names_getter=_cdc_teardown_stack_names,
        cdc_teardown_in_flight_getter=_cdc_teardown_in_flight,
        cdc_teardown_banner_getter=_cdc_teardown_banner,
        cdc_teardown_retry=_cdc_teardown_retry,
        cdc_teardown_dismiss=_cdc_teardown_dismiss,
        cdc_teardown_done_getter=_cdc_teardown_done,
        cdc_teardown_done_dismiss=_cdc_teardown_done_dismiss,
        cdc_op_in_flight_getter=_cdc_op_in_flight,
        cdc_probe=_cdc_probe,
        on_ai_panel_ready=lambda handle: _ai_panel_holder.__setitem__("handle", handle),
        ai_context_getter=_ai_context,
        ai_general_streamer_factory=_general_ai_streamer,
        ai_progress_provider=_full_load_progress_provider,
        optional_tools={
            _QUERY_PLAYGROUND_VIEW: OptionalTool(
                view_key=_QUERY_PLAYGROUND_VIEW,
                # "Query Converter", NOT "Query validation": Validation is Step 4's
                # own name for a completely different job (COUNT(*)/checksum/PK
                # reconciliation of migrated DATA), so reusing the word here read as
                # "am I doing Step 4 again?". This screen's always-available core
                # action is CONVERSION -- the target test, AI review and AI DBA tuning
                # are optional extras on top -- and it pairs with Step 2's "Schema
                # Conversion" (schemas there, queries here). The caption still names
                # the test, so nothing is hidden by the narrower title.
                label="Query Converter",
                caption="Optional · Convert & test app queries",
                icon="science",
                content=query_playground_content,
            ),
        },
    )


def _render_footer_tools(activity_log_path: str) -> None:
    """Render the sidebar footer as ONE "Settings" row that opens a modal.

    These three utilities (performance tuning, diagnostics, activity-log download) used
    to sit in the sidebar as two inline ``ui.expansion`` panels plus a button. That put
    a nine-field form into a ~16rem column, and opening one panel shoved the others
    around. They are also all the same KIND of thing -- app-wide runtime settings, none
    of them part of the migration flow -- so they belong behind one entry point rather
    than three competing rows.

    The sidebar now shows a single gear + "Settings"; the modal groups the details into
    labelled categories. The body is built ONCE (not per click), so anything the user
    typed survives closing and reopening.

    One tab per TUNING GROUP (Full Load / Validation / CDC), not a single "Performance"
    tab holding all of them. "Performance" was a category the operator does not think in:
    they arrive wanting to change the Full Load or the CDC sink, and a combined panel made
    them read past the other groups -- while each group's timing caption ("applies to the
    next run" vs "the next Start CDC") sat mid-list where it read as a note on whichever
    field was next. The tabs come from ``tunable_groups()``, so adding a knob in a new
    group grows the tab strip with no change here.
    """
    from nicegui import ui

    from dsql_migrator.config import tunable_groups
    from dsql_migrator.ui.design import render_notice, section_header

    # Material icon per tuning group. A group with no entry falls back to the generic
    # tune glyph, so a newly added group still renders (just without a bespoke icon).
    group_icons = {
        "Full Load": "cloud_upload",
        "Validation": "fact_check",
        "CDC": "stream",
    }

    dialog = ui.dialog().props("persistent")
    with dialog, ui.card().classes("gap-0").style("width: 44rem; max-width: 94vw"):
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            section_header(ui, icon="settings", title="Settings")
            ui.button(icon="close", on_click=dialog.close).props(
                "flat dense round size=sm color=grey-7"
            ).tooltip("Close")
        # Worth keeping -- it prevents a real mistake: an operator who tunes here and
        # walks away would otherwise assume the value persists, and on a Fargate task
        # replacement (or any restart) it silently reverts to the deploy-time default,
        # so a carefully-tuned run behaves differently the next time with no sign why.
        # But as gray micro-text under the title it read as boilerplate and was skipped.
        # Promoted to an info notice (the app's standard treatment for a fact the user
        # must register) and reworded to lead with the consequence, "not permanent",
        # rather than the abstract "app-wide and live".
        #
        # NOT "changes apply to the next run" -- that is only true of the Full Load /
        # Validation groups; each panel states its own timing.
        render_notice(
            ui,
            tone="info",
            header="These settings are not permanent",
            body=(
                "They apply app-wide, take effect without a redeploy, and revert to the "
                "deploy-time defaults whenever the app restarts. To make a value stick, "
                "set its DSQL_MIGRATOR_* environment variable in the deployment."
            ),
        )
        # Tabs, not stacked sections: the categories are unrelated -- you come here to
        # change ONE of them -- so stacking made the reader scroll past the others, and
        # the modal grew with every added knob. Same ui.tabs/tab_panels shape the Schema
        # Conversion screen uses.
        groups = [name for name, _knobs in tunable_groups()]
        with ui.tabs().props("dense align=left").classes("w-full") as tabs:
            group_tabs = [
                ui.tab(name, icon=group_icons.get(name, "tune")) for name in groups
            ]
            diagnostics_tab = ui.tab("Diagnostics", icon="bug_report")
            activity_tab = ui.tab("Activity log", icon="download")
        first_tab = group_tabs[0] if group_tabs else diagnostics_tab
        with ui.tab_panels(tabs, value=first_tab).classes("w-full").style(
            # FIXED height, not a min/max range: with a range the dialog resized on every
            # tab switch (Full Load has three knobs, Validation one), so the card grew and
            # shrank and -- because a centred dialog is positioned from its middle -- the
            # tab strip itself moved under the pointer. Clicking through the tabs made the
            # whole panel jump. A single height keeps the strip anchored so only the
            # content changes. Sized to the tallest panel (Full Load: notice + 4 fields)
            # so nothing scrolls in the normal case; the vh cap keeps a small viewport
            # from pushing the dialog off-screen, and overflow-y auto means a panel that
            # does exceed it scrolls inside instead of resizing the card.
            "height: 26rem; max-height: 68vh; overflow-y: auto"
        ):
            for name, tab in zip(groups, group_tabs):
                with ui.tab_panel(tab).classes("p-0 pt-3"):
                    _render_tuning_group_controls(name)
            with ui.tab_panel(diagnostics_tab).classes("p-0 pt-3"):
                _render_diagnostics_controls()
            with ui.tab_panel(activity_tab).classes("p-0 pt-3"):
                _render_activity_log_download(activity_log_path)

    with ui.item(on_click=dialog.open).props("clickable").classes("rounded-borders"):
        with ui.item_section().props("avatar"):
            ui.icon("settings", color="grey-7")
        with ui.item_section():
            ui.item_label("Settings").classes("text-sm")
            # Short enough to stay on ONE line in the ~16rem sidebar: the full list
            # ("Tuning · diagnostics · activity log") wrapped to two lines and made the
            # row taller than every nav item above it.
            ui.item_label("Tuning · logging").props("caption")


def _render_tuning_group_controls(group: str) -> None:
    """Render ONE tuning group's knobs as an AWS-style (Cloudscape) form section.

    Called once per Settings tab, so each knob category (Full Load / Validation / CDC)
    gets its own tab rather than being stacked under a single "Performance" panel. The
    groups differ in what they affect AND in when a change lands, so they are separate
    destinations, not sections of one list -- you come here to change one category.

    The two timings must not be blurred:

    * **Full Load / Validation** -- the loader and validator call ``load_config()`` on
      every run, so a change lands on the NEXT run of that step.
    * **CDC** -- the value is a cdc-stack CloudFormation PARAMETER, read when Start CDC
      creates/updates the connectors. So it lands at the next Start CDC, and for a
      pipeline already streaming, only after re-running it. "Applies to the next run"
      would promise something that never happens: nothing re-reads it, and a running
      sink keeps its capacity until the connector is updated.

    Each panel therefore leads with its own timing (``group_applies``). Every knob is a
    Cloudscape ``FormField``: visible label, description, and constraint text listing the
    accepted values -- previously the description was hover-only, so the form could not
    be read without hovering each field in turn (and not at all on touch). A knob whose
    legal values are an enum rather than a range (``allowed``, e.g. the template's
    ``AllowedValues: [1,2,4,8]`` for SinkMcuCount) renders as a dropdown; a spinner would
    happily offer 3, which CloudFormation rejects minutes into a billable deploy.
    """
    from nicegui import ui

    from dsql_migrator.config import (
        TuningValueError,
        current_tuning_values,
        group_applies,
        tunable_groups,
        set_tuning_value,
    )
    from dsql_migrator.ui.design import form_field, render_notice

    current = current_tuning_values()
    knobs = dict(tunable_groups()).get(group, ())

    # Lead with WHEN a change takes effect -- the first thing an operator needs in order
    # to trust the field, and the one fact that differs per group.
    ui.label(f"Changes apply to {group_applies(group)}.").classes(
        "text-xs text-gray-500 mb-2"
    )

    # Full Load's two parallelism knobs MULTIPLY into the connection count, which is the
    # one way to misconfigure this panel into a failing run. Scoped to that group: it is
    # meaningless beside a single Validation or CDC field.
    if group == "Full Load":
        render_notice(
            ui,
            tone="info",
            header="Connections ≈ tables in parallel × batches per table",
            body=(
                "Raise these together carefully: the product is how many DSQL "
                "connections a run opens at once."
            ),
        )

    def _on_change(event: object, k) -> None:
        raw = getattr(event, "value", None)
        try:
            applied = set_tuning_value(k.field, raw)
        except TuningValueError as exc:
            ui.notify(str(exc), type="warning", position="top")
            return
        # Name the knob's OWN timing: a CDC knob that reported "the next run" read as
        # though a streaming pipeline would pick it up on its own.
        ui.notify(
            f"{k.label} = {applied} (applies to {k.applies}).",
            type="info",
        )

    with ui.column().classes("gap-3 w-full pt-1"):
        for knob in knobs:
            slot = form_field(
                ui,
                label=knob.short_label,
                description=knob.description,
                constraint=(
                    " / ".join(str(v) for v in knob.allowed)
                    if knob.allowed
                    else f"{knob.minimum}–{knob.maximum}"
                ),
                # Optional info tooltip for a knob whose full guidance (when to raise it,
                # what it costs, when it lands) would not fit the visible description.
                help_text=knob.help_text,
            )
            with slot:
                if knob.allowed:
                    # Enum-valued: offer ONLY the legal values.
                    ui.select(
                        list(knob.allowed),
                        value=current[knob.field],
                        on_change=lambda e, k=knob: _on_change(e, k),
                    ).props("dense outlined options-dense").classes("w-24 text-sm")
                else:
                    ui.number(
                        value=current[knob.field],
                        min=knob.minimum,
                        max=knob.maximum,
                        step=1,
                        format="%d",
                        on_change=lambda e, k=knob: _on_change(e, k),
                    ).props("dense outlined").classes("w-24 text-sm")


def _render_diagnostics_controls() -> None:
    """Render runtime troubleshooting controls in the sidebar footer.

    Deployment is kept parameter-light: the log level and the optional CloudWatch
    mirror are NOT deploy-time inputs but are adjusted here at runtime, so an
    operator can flip INFO<->DEBUG (DEBUG adds failure stacktraces) and start/stop
    mirroring the activity log to stdout (forwarded to CloudWatch on ECS) while
    troubleshooting -- no redeploy. Changes apply app-wide (single-task app) and
    reset to the startup defaults on restart.
    """
    import logging

    from nicegui import ui

    from dsql_migrator.core.activity_log import (
        activity_stdout_enabled,
        configure_activity_stdout_log,
        current_activity_log_level,
        disable_activity_stdout_log,
        set_activity_log_level,
    )

    from dsql_migrator.ui.design import form_field

    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    current = logging.getLevelName(current_activity_log_level())
    if current not in levels:
        current = "INFO"

    # Same "when does this take effect" lead-in as the tuning panels, so every Settings
    # tab opens with the same fact in the same place.
    ui.label("Changes apply immediately.").classes("text-xs text-gray-500 mb-2")

    def _on_level(event: object) -> None:
        value = str(getattr(event, "value", "INFO"))
        set_activity_log_level(getattr(logging, value, logging.INFO))
        ui.notify(f"Log level set to {value}.", type="info")

    def _on_toggle(event: object) -> None:
        if bool(getattr(event, "value", False)):
            configure_activity_stdout_log(level=current_activity_log_level())
            ui.notify(
                "Mirroring activity log to stdout (CloudWatch on ECS).",
                type="info",
            )
        else:
            disable_activity_stdout_log()
            ui.notify("Stopped mirroring activity log to stdout.", type="info")

    # Same Cloudscape FormField rows as the tuning tabs (label + description + control),
    # rather than a floating-label select beside a bare switch: two controls of different
    # shapes in one short panel read as unrelated widgets instead of one form.
    with ui.column().classes("gap-3 w-full pt-1"):
        with form_field(
            ui,
            label="Log level",
            description="DEBUG adds failure stacktraces to the activity log.",
        ):
            ui.select(levels, value=current, on_change=_on_level).props(
                "dense outlined options-dense"
            ).classes("w-24 text-sm")
        with form_field(
            ui,
            label="Mirror to stdout",
            description=(
                "What reaches CloudWatch Logs when the app runs on ECS — the log file "
                "itself lives on ephemeral task storage."
            ),
        ):
            ui.switch(
                value=activity_stdout_enabled(), on_change=_on_toggle
            ).props("dense")


def _render_activity_log_download(activity_log_path: str) -> None:
    """Render a global "Download activity log" button in the sidebar footer.

    Always available so the operator can pull the full UTC, one-line-per-event
    timeline (connection / assessment / schema apply / Full Load / CDC) whenever
    needed, independent of which step is open. Downloads the human-readable text
    rendering; the raw NDJSON file remains on disk for tooling.
    """
    from nicegui import ui

    from dsql_migrator.core.activity_log import read_activity_log

    def _download() -> None:
        data = read_activity_log(activity_log_path, "text")
        if not data:
            ui.notify("No activity has been logged yet.", type="info")
            return
        ui.download(data, "migration_activity.log")

    # This tab is an ACTION, not a set of fields -- so it does not use form_field. Wedging
    # the button into a form row's right-hand control slot made it small and stranded it
    # far from the text it belongs to, with the description wrapping underneath it: the
    # slot is sized for a number input, and right-aligning is what makes a COLUMN of
    # inputs line up, which is meaningless for a single button. Instead: a described
    # section (same label/description/info structure the other tabs read as) with the
    # action beneath it at full button size, left-aligned where reading ends.
    ui.label("Downloads the log as it stands right now.").classes(
        "text-xs text-gray-500 mb-2"
    )
    with ui.column().classes("gap-3 w-full pt-1"):
        with ui.row().classes("items-center gap-1 no-wrap"):
            ui.label("Activity log").classes("text-sm font-medium text-gray-900")
            ui.icon("info_outline").classes(
                "text-gray-400 text-sm cursor-help shrink-0"
            ).tooltip(
                "The human-readable rendering of the audit trail. The raw NDJSON file "
                "stays on disk for tooling.\n\n"
                "On ECS the file lives on ephemeral task storage, so it is lost when the "
                "task is replaced — enable Diagnostics → Mirror to stdout for a durable "
                "copy in CloudWatch Logs."
            )
        ui.label(
            "One UTC line per event across the whole session — connections, assessment, "
            "schema apply, Full Load, CDC — independent of which step is open."
        ).classes("text-xs text-gray-500 leading-snug")
        # The tab's one action: primary-coloured and unstyled-down (no `dense`/`size=sm`),
        # since nothing here competes with it. Names the artifact rather than a bare verb,
        # matching the Full Load error-log button.
        ui.button("Download activity log", on_click=_download, icon="download").props(
            "no-caps color=primary"
        )


def main() -> None:
    """Configure and launch the NiceGUI application."""
    import secrets as _secrets

    from nicegui import app, ui

    # AWS Console / Cloudscape uses sentence-case button labels, but Quasar (the
    # NiceGUI button backend) defaults to ALL-CAPS. Set the default once here so
    # every button in the app reads "Run" / "Deploy" rather than a mix of
    # "RUN" and "Run" -- a single source of truth instead of per-button no-caps.
    ui.button.default_props("no-caps")

    config = load_config()

    # On Fargate the task has credentials (container provider) but no region, so
    # any region-less boto3 client (e.g. the AI-assist Bedrock client when
    # BEDROCK_REGION is blank) would raise NoRegionError. Seed AWS_DEFAULT_REGION
    # from DSQL_MIGRATOR_AWS_REGION (= ${AWS::Region}) when nothing else set it, so
    # every region-less client has a floor. Clients that parse a region from their
    # endpoint (DSQL/Secrets/CDC) are unaffected.
    from dsql_migrator.core.aws_session import ensure_default_region

    ensure_default_region(config.aws_region)

    # Configure the tool's logger with timestamps and the configured level so
    # the tool's messages (e.g. per-table Full Load failures) are timestamped
    # and filterable in the terminal. Done explicitly on the package logger so
    # it applies regardless of how the web server configures the root logger.
    # Honors DSQL_MIGRATOR_LOG_LEVEL.
    _level = getattr(logging, str(config.log_level).upper(), logging.INFO)
    _pkg_logger = logging.getLogger("dsql_migrator")
    _pkg_logger.setLevel(_level)
    if not _pkg_logger.handlers:
        _handler = logging.StreamHandler()
        _handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
        _pkg_logger.addHandler(_handler)
        _pkg_logger.propagate = False

    # Structured activity log (UTC, one JSON line per event), downloadable from
    # the UI: connection tests, assessment, per-object schema apply, per-table
    # Full Load outcomes, and CDC control-plane actions are appended here so the
    # operator has an auditable, time-sortable record of the whole migration.
    from dsql_migrator.core.activity_log import (
        ActivityCategory,
        ActivityStatus,
        configure_activity_file_log,
        configure_activity_stdout_log,
        log_activity,
    )

    # The activity log is an audit convenience, NOT a startup prerequisite: if its
    # path is unwritable (e.g. a non-root container whose WORKDIR /app is read-only
    # and ACTIVITY_LOG_PATH was left at the relative default), opening the rotating
    # file raises PermissionError/OSError. That must NOT crash the app on boot --
    # previously it sent the container into a restart loop and ECS rolled the
    # deploy back as NotStabilized. Fall back to stdout-only logging and continue.
    activity_to_stdout = config.activity_log_to_stdout
    try:
        configure_activity_file_log(config.activity_log_path, level=_level)
    except OSError as exc:
        activity_to_stdout = True  # ensure the audit trail still goes somewhere
        _pkg_logger.warning(
            "activity log file %s is not writable (%s); falling back to stdout-only "
            "activity logging. Set DSQL_MIGRATOR_ACTIVITY_LOG_PATH to a writable "
            "path (e.g. /tmp/migration_activity.log) to keep the rotating file.",
            config.activity_log_path, exc,
        )
    # Optional: also stream activity events to stdout so the container's awslogs
    # driver forwards them to CloudWatch Logs (durable, survives task
    # replacement). Off by default; the rotating file remains the local copy.
    if activity_to_stdout:
        configure_activity_stdout_log(level=_level)
    log_activity(
        ActivityCategory.SYSTEM, "app started", status=ActivityStatus.INFO
    )

    # Durable job state (resumability, Property 4): persist Full Load job
    # snapshots to the configured SQLite file and reload any interrupted jobs so
    # an app restart does not lose progress. Interrupted in-flight jobs are
    # reconciled to FAILED on load so the "retry failed tables" path resumes the
    # unfinished work.
    from dsql_migrator.core.job_store import S3JobStore, SqliteJobStore

    # A DURABLE S3 store (when a bucket is configured -- the container deploy points
    # DSQL_MIGRATOR_JOB_STATE_BUCKET at the tool's managed plugin bucket) survives a
    # Fargate task replacement, so an interrupted Full Load AND the per-table
    # migration monitor resume across a redeploy instead of being lost with the
    # task's EPHEMERAL /tmp SQLite file. Local dev (no bucket) keeps the on-disk
    # SQLite store. Both satisfy the JobStore protocol, so JobManager is unchanged.
    if config.job_state_bucket:
        JOB_MANAGER.attach_store(
            S3JobStore(
                config.job_state_bucket,
                region=config.aws_region,
                aws_profile=config.aws_profile,
            )
        )
    else:
        JOB_MANAGER.attach_store(SqliteJobStore(config.job_state_path))
    # Bound growth: drop all but the most recent completed jobs (resumable/active
    # jobs are never pruned).
    JOB_MANAGER.prune_terminal(_KEEP_DONE_JOBS)

    # Durable per-session state (resumability, Property 4): persist each
    # session's non-secret workbench state so a reconnecting browser resumes
    # where it left off after a restart.
    global SESSION_STATE_STORE
    from dsql_migrator.core.session_state_store import (
        S3SessionStateStore,
        SqliteSessionStateStore,
    )

    # A DURABLE S3 store (when a bucket is configured -- the container deploy points
    # DSQL_MIGRATOR_SESSION_STATE_BUCKET at the tool's managed plugin bucket)
    # survives a Fargate task replacement, so a reconnecting browser resumes its
    # workbench across a redeploy instead of re-running Evaluation. Local dev (no
    # bucket) keeps the on-disk SQLite store. Both satisfy the SessionStateStore
    # protocol, so the save/load/delete/prune call sites are unchanged.
    if config.session_state_bucket:
        SESSION_STATE_STORE = S3SessionStateStore(
            config.session_state_bucket,
            region=config.aws_region,
            aws_profile=config.aws_profile,
        )
    else:
        SESSION_STATE_STORE = SqliteSessionStateStore(config.session_state_path)
    SESSION_STATE_STORE.prune(_KEEP_SESSIONS)

    # Dev convenience: prefill the Connect form from the local .env / environment
    # (gitignored). os.environ takes precedence over the .env file. Source fields
    # reuse DB_*; target fields use TARGET_*.
    project_root = Path(__file__).resolve().parents[3]
    merged_env = {**read_env_file(str(project_root / ".env")), **os.environ}
    connect_defaults = load_connect_defaults(merged_env)

    # Secret used to sign the browser session cookie that backs
    # ``app.storage.browser``. A configured value (DSQL_MIGRATOR_STORAGE_SECRET)
    # keeps the SAME browser id across process restarts AND across closing/
    # reopening the browser, so the persisted session snapshot is found and the
    # user resumes where they left off (e.g. a CDC deploy in flight) instead of
    # landing on a fresh session. Without it a per-process random secret is used,
    # which only survives page refreshes within one running process. Read from the
    # merged env (.env + os.environ, os.environ wins) like the other dev settings;
    # it is a secret, so it is intentionally NOT placed on the log-safe AppConfig.
    storage_secret = (
        merged_env.get("DSQL_MIGRATOR_STORAGE_SECRET") or _secrets.token_urlsafe(32)
    )

    @ui.page("/")
    def _index() -> None:
        # Identify the session by the stable, cookie-backed browser id rather
        # than the per-connection client id, so reloading the page continues the
        # same session (workflow progress, verified connections, results) instead
        # of starting over.
        session_id = app.storage.browser["id"]
        build_page(config, session_id, connect_defaults)

    ui.run(
        host=config.app_host,
        port=config.app_port,
        title="DSQL Migration Tool",
        reload=False,
        show=False,
        storage_secret=storage_secret,
        # Behind an ALB on Fargate the UI WebSocket can briefly drop (load-balancer
        # connection recycling, brief network blips). NiceGUI's default 3s
        # reconnect window is too short to ride those out, so the page would give
        # up and reload -- losing the workbench view mid-deploy even though the
        # job runs server-side. A longer window reconnects to the same task and
        # keeps the long-running CDC/Full-Load session visible.
        reconnect_timeout=60.0,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
