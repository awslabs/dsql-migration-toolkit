# mysql-dsql-migrator — project guide for Claude

A NiceGUI (Python) web tool that migrates Amazon RDS / Aurora **MySQL** to Amazon
**Aurora DSQL** (PostgreSQL-16-compatible, distributed). Heterogeneous migration:
MySQL→PostgreSQL dialect, then PostgreSQL→DSQL constraints. The source is always
read-only. See `README.md` for the architecture and the Full Load + CDC data paths.

## Product principles (optimize for these when in doubt)

These are the recurring, cross-cutting priorities for this project. They override
local convenience and "clever" designs. The points below are what come up most.

1. **Usability / convenience first.** The tool targets an engineer doing a real
   MySQL→DSQL migration. Minimize steps and clicks; provide clear defaults and
   **infer anything that can be inferred** instead of asking. Prefer clear,
   actionable feedback ("what happened, what to do next") over raw errors.
2. **Deployment convenience is paramount.** A fresh `git clone` should be able to
   run and deploy with as little setup as possible. This is why the prebuilt
   connector plugin ZIPs are committed (no Java/Maven toolchain needed to deploy
   CDC), the tool provisions its own S3 bucket / uploads artifacts itself, and CDC
   infra is auto-discovered (the user supplies only what truly can't be inferred,
   e.g. VpcId). Never add a manual setup step the tool could do for the user.
3. **One consistent journey.** All six steps (Migration plan → Evaluation →
   Schema Conversion → Data Migration → Validation → Cut over) must feel like a
   single guided flow, not separate
   screens: the same journey header / progress stepper, the migration-type banner,
   consistent headers, and visible status at every step. When adding or changing a
   screen, match the existing flow's structure and the design system below — a new
   page must not look or behave like a different app.
4. **No bloat.** Build only what the requirement/design calls for. Prefer the
   simplest implementation; don't add options/settings/abstractions "just in case."
   If something seems to add complexity without clear user value, take the simpler
   path and flag the discrepancy rather than building speculatively.
5. **Web-first.** The NiceGUI web UI is the primary interface; everything core is
   reachable from the browser. CLI/library entry points are for automation only.
6. **Decisions evolve with understanding.** Past design choices are not sacred — as
   the app's behavior is better understood, revisit them rather than defending them.
   Re-verify any assumption against the actual code before relying on it.

## Architecture stance: high-performance, TB-scale

This is a **large-database migration tool**: architect every data-path change as if
the source could be **terabyte-scale** with very large individual tables. Performance
and bounded resource use are first-class requirements, not afterthoughts. Concretely:

- **Stream, never materialize.** Source rows are read by **PK keyset** pagination
  (`WHERE pk > :last ORDER BY pk LIMIT :batch_size`, not `OFFSET`) over a server-side
  / streaming cursor and written as they flow (`exporter.py`). Memory stays bounded
  by one page regardless of table size — a whole table is never loaded into RAM. Any
  new processing must preserve this: no "read all rows then process" patterns.
- **Bounded-parallel, idempotent batched load.** Rows stream straight into batched
  `INSERT ... ON CONFLICT` statements (≤ DSQL's per-txn row limit) loaded concurrently
  across a small, **bounded** DSQL connection pool, each batch wrapped in OCC retry
  (`batched_import.py`). Scale throughput by parallelism/batch size, never by
  unbounded fan-out or unbounded memory.
- **Resumable, deterministic units.** Because rows stream in keyset order, batch *i*
  always maps to the same PK range, so batches are stable resumable units — a stop /
  retry re-runs only the unfinished ranges (idempotent, no duplicates). Preserve this
  determinism for any change to chunking or retry.
- **Don't scan the source needlessly.** Watermark row counts are scan-free
  `information_schema` estimates; exact `COUNT(*)`/checksums run only in Validation.
  Avoid adding full-table scans on the hot path — they don't scale to TB.
- **Cost/footprint awareness.** Prefer streaming, batching, and async/after-load index
  builds; flag anything whose time or memory grows unboundedly with row count.

When in doubt on a data-path change, ask "does this still hold at 1 TB / a
billion-row table?" — if it doesn't, redesign it before implementing.

## How to work in this repo

- **Build/verify:** `\.venv/bin/python -m pytest -q` from the repo root. Keep the
  suite green; add tests with any behavior change. Tests never touch a real
  MySQL/DSQL/AWS — seams are injected and UI helpers take a NiceGUI double.
- **Run / restart the UI:** the app runs with `reload=False`, so Python edits do
  NOT hot-reload. Use the `restart-ui` skill (kills :8080, relaunches detached,
  verifies HTTP 200). Don't restart if asked to batch changes for a later restart.
- **Compare source vs target rows:** the `compare-rows` skill / `scripts/compare_rows.py`.
- **Secrets:** credentials live in process memory only (Property 7) — never write
  them to disk, logs, reports, or job state. `.env` is git-ignored.

## UI / AWS-style design system  (apply to every UI change)

The app should read like one **AWS Console** experience. AWS uses the open-source
**Cloudscape Design System**; NiceGUI is Quasar-based so we can't use Cloudscape
directly, but we mirror its *semantics* and *tokens*. The single source of truth is
**`src/dsql_migrator/ui/design.py`** — always reuse it; never re-create these
inline or invent new colors.

- **Notices / alerts:** use `render_notice(ui, tone=..., header=..., body=...)`
  (Cloudscape "Alert"). Never use loose colored text (`ui.label(...).classes("text-red-700")`)
  for a status/verdict — wrap it in a notice box so the box + border + leading
  status icon + bold header carry the severity.
- **Tones** (from `NOTICE_STYLE`) and their meaning:
  - `info` — neutral FYI / recommendation (sky/blue)
  - `success` — completed OK (green)
  - `warning` — be aware / non-blocking issue (amber)
  - `error` — action required / blocking (red)
- **Status chips** (diagram nodes, at-a-glance state): use `BADGE_TONES` /
  `badge_classes(tone)` — tones `ok` / `bad` / `active` / `neutral` / `reconnect`.
- **Card/section headers:** use `section_header(ui, icon=..., title=..., badge=...)`
  (Cloudscape "Container" header band: primary-color glyph + bold title + optional
  right-aligned status badge).
- **One palette only.** Backgrounds `*-50`, borders `*-200`, icons `*-600`, all
  Tailwind. Do NOT mix Quasar numeric shades (`bg-blue-1`, `bg-red-1`) with
  Tailwind shades, and prefer `amber` over `orange` for warnings.
- **Severity calibration** (matches the prerequisite model): things that are
  expected / optional / "no action needed" are `info`, not `warning`. A `warning`
  means a real but non-blocking issue; `error` means it blocks progress. Don't
  alarm the user for normal states (e.g. CDC infra "not yet deployed", or a
  row-count differing from an approximate estimate → `info`).
- **Primary actions** sit on the right of a button row / stepper navigation,
  `color=primary`. Destructive confirmations use `color=negative` and spell out
  the irreversible impact.
- **Text glyphs:** prefer a Material icon (via the notice/badge components) over
  literal `✓`/`⚠`/`ℹ`/`…`/`—` in labels (some fonts render them as tofu boxes).

When adding a new tone/component, add it to `ui/design.py` (with a test in
`tests/test_ui_design.py`) so it stays the single source of truth.

## Domain constraints to respect (Aurora DSQL)

- PostgreSQL-wire, **IAM-token auth** (no password); short-lived tokens.
- **No foreign keys**, **no `TRUNCATE`**, PK required; **≤ 3000 rows / transaction**,
  one DDL per transaction; **optimistic concurrency** (retry on SQLSTATE 40001);
  `CREATE INDEX ASYNC`. Loads are idempotent (`INSERT ... ON CONFLICT`).
- **Full Load** = the tool's own Python bulk loader (NOT a Debezium snapshot).
  **CDC** = Debezium MySQL → MSK (Kafka) → custom DSQL sink; it replicates **row
  data**, not SQL, and does **not** propagate DDL. A binlog/GTID **watermark**
  bridges Full Load → CDC gaplessly.
- Per-table snapshot **row counts on the watermark are approximate** (scan-free
  `information_schema` estimates, to spare the source). Exact `COUNT(*)` /
  checksum comparison is Validation (Step 4) only.

## Safety (non-negotiable)

- Treat AWS resources as **production unless proven otherwise**; prefer
  read/describe/list over modify/delete; use ReadOnly/least-privilege credentials.
- **Never delete/terminate/modify** production resources or disable safety
  protections without explicit user confirmation and a clear impact explanation.
- Inclusive language only (no master/slave, whitelist/blacklist, etc.).

## Wiki Knowledge Base (cross-project reference)

A separate Claude + Obsidian "LLM Wiki" vault holds the owner's compounding
technical knowledge (Text2SQL, Aurora DSQL, MySQL migration, and more as it grows).
It is maintained by the `claude-obsidian` plugin and lives outside this repo.

Path: `~/Documents/LLM Wiki`

When you need background/domain context that is **not already in this repo or the
conversation**, read the wiki in this order (cheap → expensive):

1. `wiki/hot.md` first — recent context, ~500 words.
2. If not enough, `wiki/index.md` — the master catalog of all pages.
3. For a specific area, the relevant `wiki/<section>/_index.md`
   (`domains/`, `concepts/`, `entities/`).
4. Only then open individual `wiki/**/*.md` pages.

Do NOT read the wiki for:
- General Python/SQL/coding questions or language syntax.
- Anything already covered by this repo's code, `README.md`, or the current
  conversation.
- Tasks unrelated to migration/database domain knowledge.

This repo's own files remain the source of truth for how *this tool* works; the wiki
is supplementary domain knowledge. Never copy wiki pages into this repo, and never
modify the wiki from this project (it has its own session/workflow for that).
