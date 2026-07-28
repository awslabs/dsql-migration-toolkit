# Changelog

_Language: **English** | [한국어](CHANGELOG.ko.md) | [日本語](CHANGELOG.ja.md)_

All notable changes to this project are recorded here. This project follows
[semantic versioning](https://semver.org/) (patch releases for bug fixes).

## v0.1.140

### Fixed

- **A failed source read no longer holds its MySQL connection open while the retry
  waits.** The source row streams are generators that dispose their engine in their
  own `finally`, so an abandoned one keeps its connection until it is closed or
  garbage-collected — and the raising frame keeps it referenced. The v0.1.139 retry
  therefore waited out the whole failover backoff (up to 60s) with the dead
  connection still open, then opened another one to re-read. At 16 tables × 8 shards
  that **doubles the source connection count exactly when a just-promoted Aurora
  writer is most fragile**, risking `1040 Too many connections` — which would have
  failed the table outright.
  - `migrate_table` now closes the row streams it created when a load raises, so the
    connection is released as the exception leaves.
  - The retry's backoff wait moved OUT of the `except` block, so the traceback (and
    with it the failed attempt's frames and generator) is dropped before waiting.
  - Verified end-to-end: the connection is now disposed *before* the wait starts.

### Added

- **`Too many connections` on the source is now retried, with its own advice.** MySQL
  1040 / 1203 are self-inflicted and self-clearing (a failover makes every reader
  reconnect at once; slots drain as readers finish), so they are classified as
  transient. The operator hint differs from the failover one, because waiting is not
  the fix: it names `FULL_LOAD_TABLE_PARALLELISM` / `FULL_LOAD_READER_SHARDS` and the
  source's `max_connections`.
- **A clamped reader-shard count now says so.** Concurrent source readers are capped
  at 32 (`table_parallelism × reader_shards`); when that ceiling reduces the
  configured shard count, the log states the old and new values and why, instead of
  silently loading with fewer readers than requested — which looked like the setting
  had no effect.

## v0.1.139

### Added

- **Full Load now survives a source Aurora failover.** A writer promotion (patching,
  an instance replacement, an AZ event) closes every open MySQL connection, so a
  multi-hour load would meet one — and previously the table in flight simply failed
  and waited for someone to press Re-run. Such a table is now **re-read
  automatically** (3 attempts by default, 15s → 30s → 60s backoff to let DNS
  re-point at the promoted writer).
  - The retry deliberately **re-reads the table from a fresh consistent snapshot**
    rather than resuming the dead read at its last primary key. Resuming would splice
    two different MySQL snapshots into one table, leaving it consistent as of no
    single point in time — and the gapless Full Load → CDC handoff depends on each
    table being consistent as of the run's watermark. Already-written rows are skipped
    by the idempotent load, so a retry costs re-read I/O but never duplicates rows.
    (Reader sharding shrinks even that cost: each shard already holds its own
    snapshot, so only the affected shard re-reads.)
  - Only **connection-level** failures retry (MySQL 2013/2006/2003/2002/2055/1053/
    1077/1079/1927 and socket timeouts). A data or schema error fails immediately, as
    before — retrying it would only add delay before the same failure.
  - Applies to the multiprocess load path (the default at scale) as well as the
    single-process one, so a run does not behave differently per worker mode. A retry
    correctly stops treating the target as freshly-emptied, so the re-read cannot
    collide with rows its own failed attempt already wrote.
  - A user **Stop** is honored during the backoff wait, not after it.
  - Tunable: `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS` (1 = off, the previous
    behavior) and `DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS`.

### Changed

- **A dropped source connection now explains itself.** When the retries are
  exhausted, the per-table error no longer reads as a bare
  `OperationalError: (2013, 'Lost connection to MySQL server during query')`. It now
  states that this is usually an Aurora failover, that nothing on the source was
  changed (the load only reads it), and that re-running is safe because the load is
  idempotent and resumes by primary key — filling only what is missing.

## v0.1.138

### Fixed

- **A fully-loaded Full Load table can no longer report as incomplete because the
  source ESTIMATE overcounted.** Per-table `Progress` and the completeness verdict
  divided by / compared against the watermark's scan-free `information_schema` count.
  That estimate comes from InnoDB index sampling and errs in *both* directions, so
  whenever it overcounted, a table the loader had streamed to exhaustion showed e.g.
  **"91%" and counted as mismatched** — implying rows were lost when none were.
  - A `DONE` table is now **100%** by definition: the export streams the table by PK
    keyset until exhausted, so finishing *is* the completeness evidence — it does not
    depend on the estimate agreeing.
  - `complete` reports `True` for a finished table unless the shortfall exceeds the
    estimate's sampling tolerance, so a genuinely truncated load is still flagged
    (and a few-percent discrepancy no longer is).
  - Loading **more** rows than the estimate predicted (the common undercount case) is
    now stated as normal in the Rows tooltip, with the percentage, instead of being
    silently hidden by the 100% cap.

### Changed

- **The Full Load table is explicit that its source figure is approximate.** The
  column header now reads **Rows (target / source est.)** with a new ⓘ tooltip
  explaining the sampling error, why a target exceeding the source is normal, and
  that a finished table is 100% because the loader exhausted it — not because the two
  numbers match. Validation (step 4) remains the exact comparison.

- **The CDC status table no longer flags healthy tables as "target ahead".** Its
  Source rows figure is a scan-free `information_schema` **estimate** (so a large
  production source is never `COUNT(*)`-scanned), but the consistency verdict was
  subtracting the exact target `COUNT(*)` from it and treating any difference as an
  anomaly. InnoDB derives that estimate from index sampling and routinely
  *undercounts* by several percent, so a perfectly healthy target legitimately
  exceeds it — on a live 11-table schema **8 tables showed an amber "target ahead"
  badge** with zero quarantined rows and every stream caught up.
  - The `"target ahead"` verdict is **removed**. A target exceeding an estimated
    source count is the normal case, not an anomaly.
  - Verdicts now lean on the signals that are actually exact and cheap: the DLQ, the
    time-based `ReplicationLagMs`, and the `MAX(pk)` leading edge. A shortfall
    against an *estimate* is only escalated to "rows missing" when it exceeds the
    sampling tolerance, so genuine data loss is still reported while statistics
    noise is not.
  - Equality claims are gated on an **exact** source count (`counts_comparable`);
    `in_sync` now returns "not determinable" rather than a false negative when the
    source figure is an estimate.

### Changed

- **The CDC table is explicit that Source rows is approximate.** The column header
  now reads **Source rows (est.)** with a new ⓘ tooltip explaining the sampling error
  and pointing to Validation (step 4) for the exact comparison; the per-cell `(est.)`
  suffix is gone (it now marks only the unusual *exact* case). The Consistency
  tooltip and the "How to read this table" legend state that green means "nothing
  looks wrong", not a proven exact match.

## v0.1.137

### Added

- **Fast-sweep "verified by row count only" tables can now be deep-checked in place.**
  The footnote that lists tables the fast sweep passed on row count alone previously
  only advised turning Fast sweep off and re-running everything. It now offers
  **Deep-check N count-only table(s)**, which re-compares just those tables with the
  checksum / record reconciliation the run skipped and merges the results into the
  existing report — the same per-table mechanism v0.1.136 added for failing tables.
  This is the one *passing* case where re-validating is genuinely useful, since those
  tables were never proven row-for-row identical.
  - The action is withheld when it would be a no-op: in a `ROW_COUNT`-mode report
    with no reconciliation there is no deeper check to run, so the honest "turn off
    Fast sweep and re-run" advice stands instead of a button that repeats the
    identical count comparison.
  - Otherwise-passing tables still get no re-check button; the affordance appears
    only where it adds a check (failing tables, or count-only fast-sweep tables).

## v0.1.136

### Added

- **Re-check an individual table in Validation instead of re-running everything.**
  When a table fails on row count or checksum, each entry under "Tables needing
  attention" now has a **Re-check** action (plus **Re-check all N tables** for the
  whole failing set). It re-compares only those tables and **merges** the fresh
  result into the existing report, so every other table's verdict — and the overall
  cut-over go/no-go — is kept and updates on its own: fix the last failing table and
  the verdict flips to "Ready for cut-over" without an hour-long full re-run.
  - The re-check reproduces the **original run's options** (comparison mode,
    reconciliation, orphan check) read back from the report itself, so the merged
    report stays internally consistent — and a report restored after a reconnect is
    re-checkable too. The fast sweep is forced **off** for a re-check: the table is
    already known to differ, so its checksum/reconciliation is exactly what should run.
  - The report states the mixed as-of plainly: **"N table(s) re-checked at &lt;time&gt; —
    newer than the rest of this run"**, listing the tables, since the verdict now
    covers two vintages. The disclosure survives a reconnect.
  - A re-check runs on top of the completed step (the step stays **Done**, the report
    stays on screen) with an inline "Re-checking…" state on the affected rows. It
    shares the single validation job slot, so "Re-run validation" is disabled while a
    re-check runs and vice versa — a full re-run can never orphan a re-check or clear
    the report it is about to merge into.
  - A re-check that cannot start (e.g. the short-lived DSQL target token expired since
    the report was produced) reports as its own **"Could not re-check those tables"**
    notice and leaves the existing report untouched — never as "Validation failed".

## v0.1.135

### Fixed

- **Validation no longer false-reports "data differs" for JSON columns after CDC.**
  MySQL `JSON` maps to a Postgres `json` column and the checksum compared raw text:
  MySQL renders a spaced canonical form (`{"k": "v"}`) while a CDC-written row holds
  Debezium's compact serialization (`{"k":"v"}`) — logically-equal data, different
  text, so CDC-touched rows with JSON failed the checksum (Full-Load rows matched).
  JSON is now excluded from the checksum (like FLOAT/DOUBLE); row counts and every
  other column still validate. This was the cause of spurious `customers` / `products`
  / `suppliers` checksum failures.

### Changed (checksum cross-engine hardening)

- **Source MySQL sessions are pinned to UTC** (`SET time_zone='+00:00'` on every
  source engine: connection test, introspection, validation, Full Load stream). MySQL
  `TIMESTAMP` is stored UTC but read in the session's zone; without this a non-UTC
  server/client zone would make `TIMESTAMP` columns drift versus the target's UTC
  rendering in the checksum. (`DATETIME` is a wall-clock and was unaffected.)
- **Validation skips migration-excluded columns** (e.g. the CDC oversized-LOB
  exclusion): a column that was never written to the target is dropped from the
  checksum instead of always "differing" (PK columns are never dropped).

## v0.1.134

### Changed

- **While CDC is streaming, actions that can't apply are now visibly disabled (greyed),
  not just tooltip-warned:**
  - **Start / Re-run Full Load** is now **disabled** during live CDC (it previously
    stayed clickable with only a warning) — running it would collide with the stream.
    The tooltip/hint say to Stop CDC first to re-enable it.
  - **CDC start point** was already read-only when locked but didn't *look* locked —
    the radio choice and the manual GTID/binlog inputs are now clearly **greyed
    (muted + not-allowed cursor)** to match the "Locked" badge.

## v0.1.133

### Changed

- **Inserts / Updates / Deletes cells are now just the coloured count** — the leading
  glyphs (＋ / ✎ / − ) are removed; the column header + green/blue/red colour already
  identify the op. Their header ⓘ tooltips are trimmed to a single plain sentence.

## v0.1.132

### Changed

- **Per-table CDC monitor now has separate Inserts / Updates / Deletes columns**
  (DMS-style), replacing the single combined "Changes since Full Load" cell. Each is
  a **cumulative running total** of what CDC has applied since it started streaming,
  colour-coded (green inserts / blue updates / red deletes).

### Fixed

- **I/U/D counts no longer flicker ("appears then disappears").** The applied-ops read
  is best-effort, so a flaky/empty poll (CloudWatch throttle/timeout, or tables
  momentarily empty) used to overwrite the stored counts with an empty map and blank
  the columns. The counts are cumulative (monotonic), so the poll now **merges** a
  non-empty read into the last-known values and **never wipes** on an empty read —
  the counters stay put and only increase.
- **The per-table header ⓘ tooltips (Stream lag, Consistency, …) no longer close mid-
  hover.** The table used to fully re-render every ~5s poll, tearing down the tooltip.
  The table element + its header tooltips are now built **once** and only the row data
  is swapped **in place** each poll, so a tooltip stays open while you read it.
- **Clearer Stream lag / Consistency explanations** in both the header tooltips and the
  legend (plain-language wording instead of the terse metric definitions).

## v0.1.131

### Fixed

- **Stream lag panel no longer disappears after a session restore of a drained
  pipeline.** The live lag trend is an in-memory rolling buffer that is not persisted,
  so a reconnect re-seeds it from CloudWatch `ReplicationLagMs` — but that metric is
  event-driven, so once the source is quiesced (caught up) there are no recent
  datapoints to seed from, and the chart (which needs ≥2 points) hid the entire panel:
  the operator saw no stream-lag signal at all after reconnecting. The panel now shows
  a **"Caught up — no replication lag in the recent window"** line whenever CDC is live
  but there is no trend to plot, so the metric is always present; it only fully hides
  before streaming starts.

## v0.1.130

### Changed

- **Decluttered the Validation screen text.** The 5-line intro is trimmed to a single
  sentence, and the three status notices (No export watermark / CDC still streaming /
  Comparison in progress) keep their headers but have their bodies cut to the one
  actionable line each — so a combined state (no watermark + CDC active + running) no
  longer stacks into a wall of text. The notices stay (they carry real conditional
  state), just concise.

## v0.1.129

### Changed

- **Change flow reads "idle" once the pipeline drains, absorbing the source
  connector's heartbeat floor.** The source (Debezium) connector never fully goes
  silent — `heartbeat.interval.ms=300000` emits a heartbeat every 5 min, so
  `SourceRecordPollRate` idles at a small floor (~0.03/s on the CloudWatch average)
  rather than 0. The idle threshold was `0.01/s`, so that heartbeat residual kept the
  change-flow line showing "streaming" even after the source was quiesced. Raised the
  threshold to `0.1/s` — above the heartbeat floor, far below any real change traffic
  (typically ≥1/s). The rule still requires BOTH the source-poll AND sink-send rates
  below the threshold, so a stalled sink (source still producing, sink not sending) is
  never mislabelled idle — it correctly stays "streaming".

## v0.1.128

### Fixed

- **Stream lag no longer freezes at the last value after the pipeline drains.** The
  `ReplicationLagMs` metric is event-driven (the sink emits a datapoint only when it
  applies a change), so once the source is quiesced for cut-over the pipeline stops
  emitting — but the reader kept returning the last datapoint still inside its 15-min
  window as the "current" lag, so the Stream lag chart/column sat flat at e.g. 1068 ms
  for up to ~15 minutes even though the source-poll / sink-send rates had correctly
  dropped to idle. The reader now treats a most-recent datapoint older than a freshness
  cutoff (~3 min) as absent, so a drained pipeline reads as **caught up** and the chart
  drops to 0 shortly after the source goes quiet. Reader-side fix (no sink redeploy).

### Changed

- **Decluttered the Data Migration / CDC screens: verbose standing explanations moved
  to hover ⓘ tooltips (or dropped when redundant).** The always-on help paragraphs
  read as noise once the screen is familiar, so the guidance now lives a hover away
  and the views are quieter:
  - **Stream lag** chart caption → an ⓘ next to the title (the title + `lag (ms)` axis
    carry the basics).
  - **Tables to migrate** — the "why only tables (not views/triggers/routines)"
    paragraph → ⓘ on the title; the "Locked — re-run prerequisite checks…" line →
    folded into the lock-icon tooltip; the pre-selection blurb trimmed to
    `Pre-selected: N table(s) already on the target — untick any to skip.`
  - **CDC start point** — the "where streaming begins / Automatic is gapless"
    paragraph → ⓘ on the title; the "CDC has started — locked…" line → folded into
    the **Locked** badge tooltip.
  - **Stop CDC** — the standing "connectors are streaming… Stop removes only the
    connectors…" paragraph removed (the live status shows streaming; the impact is
    already spelled out in the Stop confirmation dialog), with a short reassurance
    tooltip on the button.
  - **Change flow** — the "whether changes are still streaming / watch it drop to
    idle for cutover" paragraph and the "CloudWatch, ~last few min" provenance note
    → folded into one ⓘ on the "Change flow" header, leaving just the state line +
    the source/sink rate gauges.

## v0.1.127

### Changed

- **Per-table CDC monitor now shows a DMS-style change breakdown (I/U/D).** The
  "Net rows since Full Load" column is replaced by **"Changes since Full Load"** —
  three live counters per table: **inserts** (green `add`), **updates** (blue
  `edit`), and **deletes** (red `remove`). This makes UPDATE traffic visible for the
  first time: the old net-rows figure summed inserts − deletes and skipped updates
  entirely, so an update-heavy table looked idle. Still scan-free (no `COUNT(*)`):
  the DSQL sink now emits three CloudWatch metrics — `InsertsApplied` /
  `UpdatesApplied` / `DeletesApplied` (namespace `MysqlDsqlMigrator/CDC`, dimensions
  `Stack` + `Table`) — in place of the single `NetRowsApplied`, and the control plane
  sums each over the window. Net rows stays derivable (inserts − deletes) where still
  needed. Requires the rebuilt sink plugin (`PLUGIN_VERSION` v21 → v22), so a
  **Delete + Deploy** of the CDC infra is needed to pick it up.

## v0.1.126

### Changed

- **CDC Live-status polish (readability + less noise):**
  - **Change flow** rate gauges no longer overflow the Pipeline health card (fixed-
    width bars + inner padding), and the rates are labelled **`rec/s`** (change-event
    records per second — `SourceRecordPollRate` / `SinkRecordSendRate`) instead of a
    bare `/s`.
  - **Connectors** show a colour-coded state **badge** (green "Running", etc.) again
    for at-a-glance health, kept on the compact one-line-per-connector layout.
  - **"CDC behavior & limits"** reference section is **collapsed by default** — it is
    info-only and long, so it no longer adds noise on every visit.
  - The **"Runs on the … cdc-stack"** orientation banner shows **only before the
    cdc-stack is deployed**; once it exists (or the phase is still resolving) it is
    hidden, so it doesn't repeat on every visit or flash on a reconnect.

## v0.1.125

### Fixed

- **The CDC per-table status view (and its live metrics) no longer comes up empty
  when you reconnect to an already-running CDC pipeline.** The per-table set — which
  also scopes the scan-free CDC metrics (net rows, stream lag, and the live lag
  chart) — was derived *only* from a Full Load job's chunks, so a session with no
  Full Load job (reconnected to a running pipeline, or a CDC-only run) showed an
  empty table and no lag/chart even while the pipeline was actively streaming. It now
  falls back to the tables reconciled from the live stack's config.

## v0.1.124

### Changed

- **The Stream lag chart is now a live, in-place time series** (previously it was
  redrawn from scratch on every 5s poll, which flickered). The chart element persists
  and updates in place, so the line extends continuously like a CloudWatch graph. X is
  a **time** axis; Y is lag in **milliseconds**. Its data is a hybrid rolling series —
  seeded from CloudWatch's 1-minute history (so it survives a page reload) then
  extended each ~5s poll with the current worst-across-tables lag (caught-up shown as
  0), bounded to ~15 min. It moved to its own persistent "Stream lag" panel at the top
  of Live status.
- **Change flow (source poll / sink send) is now visual** — two labelled bar gauges on
  a shared scale instead of a plain text line, so you can see at a glance whether the
  sink is keeping up with the source (matched bars) or falling behind.
- **Status badges are unified across the Full Load and CDC statistics tables** — both
  now use the same outline chip with title-case labels. (The Full Load "Status" badge
  was a solid, uppercase chip; it now matches the CDC table's outline style and the
  design system's status-chip convention.)
- **The Live-status "Connectors" list is now minimal** — one compact line per
  connector (status icon + role name + a muted detail; the raw connector id moved to
  a hover tooltip), replacing the previous two-line id + outline-badge treatment.

## v0.1.123

### Added

- **Stream lag over time — a trend line chart in the CDC "Pipeline health" card.**
  The per-table "Stream lag" column shows the *current* lag, but a snapshot can't
  tell you whether the stream is catching up or falling behind — which is exactly
  the cut-over question. The chart plots the **worst end-to-end lag across tables per
  1-minute bucket over the trailing ~15 min** (seconds behind, from the sink's
  `ReplicationLagMs` metric): flat near zero means caught up and safe to cut over; a
  rising line means the pipeline is falling behind. It reuses CloudWatch datapoints
  the per-table read already fetched (no extra state, survives a page reload) and the
  in-app ECharts component (no new dependency). Resolution is ~1 minute (CloudWatch
  Period), so it's a trend, not a per-second readout.

## v0.1.122

### Fixed

- **A failed CloudFormation delete no longer silently strands the CDC
  infrastructure.** The in-VPC seeder Lambda's CloudFormation response now
  **retries** its response PUT (bounded, ~4 attempts) instead of giving up after
  one. A single failed PUT during teardown previously left CloudFormation with no
  response, so it waited its own ~1h custom-resource timeout and the whole cdc-stack
  landed in `DELETE_FAILED` — leaving MSK/NAT billing. Retrying rides out a transient
  S3-gateway egress hiccup while ENIs/routes settle. (Takes effect on freshly
  deployed CDC infrastructure; `PLUGIN_VERSION` bumped to v21.)

### Added

- **The teardown banner now recovers from a `DELETE_FAILED`.** When a CDC teardown
  ends in CloudFormation `DELETE_FAILED`, the persistent banner switches from
  "in progress" to an actionable **"CDC teardown failed — action needed"** state
  (error styling) with a one-click **Retry cleanup** — which re-runs the delete,
  retaining the stuck resource so the rest (MSK/NAT) is removed — and a **Dismiss**.
  The retry works even after Start over has reset the session: the region / deploy
  role / profile it needs are saved with the durable teardown marker.

## v0.1.121

### Fixed

- **A CDC infrastructure teardown now stays visible until it finishes.** When you
  Start over and choose "Delete all CDC infrastructure" (or "Remove connectors,
  keep infrastructure"), the teardown runs in the background while the session
  resets to a fresh Connect screen. Previously nothing indicated it was still
  running, so you couldn't tell whether MSK/NAT were still billing or the
  infrastructure was already gone. A persistent banner now shows on **every**
  screen (Connect included) — "CDC infrastructure teardown in progress…" — and
  clears itself automatically the moment the teardown completes. It also covers a
  teardown started from the CDC step's Delete/Stop buttons, so navigating to
  another step no longer hides it.
- **Start over can no longer race an in-flight CDC teardown.** Resetting was
  already blocked while a stop/delete ran, but a brief window right after
  Start over → delete — before CloudFormation flipped the stack to
  `DELETE_IN_PROGRESS` — could let a second reset slip through and fire a duplicate
  teardown. A durable teardown marker that survives the session reset now closes
  that window.

### Changed

- Start over now **warns** (instead of silently proceeding) when a CDC
  infrastructure deploy or Start CDC job is still running. The reset is still
  allowed — that work is re-discoverable and blocking it would trap a user escaping
  a stuck run — but you're told it keeps running in the background.

## v0.1.120

### Changed

- **CDC Start now creates the source and sink connectors in ONE parallel pass**,
  roughly halving connector-creation wall time. Previously Start ran a serial
  two-pass update — create the source connector, wait for it to reach RUNNING (so
  Debezium auto-created the per-table topics), then create the sink — because a sink
  that starts before its topics exist hits an empty-partition-assignment race. The
  cdc-stack's start-prep custom resource (the seeder Lambda, generalized) now
  **pre-creates the per-table sink topics up front** on every start — with the
  deterministic `<prefix>.<db>.<table>` names and partition count the tool already
  computes — so both connectors depend only on the pre-created topics (not on each
  other) and deploy concurrently. The seeder still seeds the connect-offsets record
  only on a gapless Full-Load handoff (watermark present); topic pre-creation is
  unconditional so CDC-only starts benefit too. Start progress collapses from six
  source-then-sink steps to a single "Waiting for connectors (source + sink)" step;
  per-connector state remains visible in the live connector chips.

## v0.1.119

### Fixed

- **A sharded single large table now loads successfully instead of being marked
  FAILED.** The PK-range shard worker built its result with
  `rows_skipped=result.rows_skipped`, but `BatchedImportResult` has no such
  attribute (it exposes `conflicts`). Every shard raised `AttributeError` at its
  return, was caught, and reported `FAILED` with `rows_loaded=0` — so a big single
  table (which the engine splits into one shard per core) was marked FAILED even
  though all its rows had loaded. Only the sharded path was affected; an unsharded
  table maps `rows_skipped = result.conflicts` correctly, which is why multi-table
  loads (one worker per table, unsharded) were unaffected. The shard worker now maps
  `rows_skipped` from `conflicts` too.
- **A sharded table's failure now records every failed shard's status/rows/message
  to the error log**, not only shards that carried a message — so "one or more
  shards failed" is always diagnosable (previously a shard that failed without a
  message left no cause).

## v0.1.118

### Fixed

- **The `measure_performance` harness now dumps the per-table/-shard/-batch error
  records on a failed run.** A sharded table marks itself `FAILED` when any shard
  fails, but the shard's actual reason is written only to the error log; the perf
  run printed "one or more shards failed" with no cause. It now prints each
  `DATA ERRORS` entry (table/chunk, code, message) alongside the `FAILURE REASON`,
  so a failed run — including a late single-shard failure on a large single-table
  load — is diagnosable from its logs alone.

## v0.1.117

### Fixed

- **DSQL's 10-schema-per-cluster limit now surfaces as an actionable error.** When
  the target cluster is already at its hard cap of 10 schemas, a `CREATE SCHEMA` for
  the migration's schema fails with `program_limit_exceeded` (SQLSTATE 54000,
  "more than 10 schemas not allowed") — even with `IF NOT EXISTS`, because DSQL
  checks the limit before the existence check. This is a hard limit (retrying never
  clears it), so it is translated immediately into a clear message telling the user
  to free a schema (`DROP SCHEMA ... CASCADE`) or use another cluster, instead of an
  opaque driver error. It is deliberately not routed through the OCC/transient retry.
- **The `measure_performance` harness now prints the job's failure reason.** A
  failure that propagated out of `run_full_load` (e.g. the pre-pass schema/DDL error
  above, before any table worker ran) was stored only as the JobManager's captured
  exception; the run printed `status=FAILED` with every table `PENDING` and no
  reason. It now logs `FAILURE REASON: <exception>` so a failed perf run is
  diagnosable from its logs alone.

## v0.1.116

### Fixed

- **Every replace table is now DROP+recreated once, serially, before the parallel
  data load starts** — closing a startup DDL storm at maximum parallelism. Each
  table worker used to recreate its own target inside its process, so at high
  table-parallelism all workers issued `CREATE SCHEMA` / `DROP` / `CREATE` against
  the shared schema catalog at once. DSQL runs one DDL per transaction under
  optimistic concurrency, so those concurrent catalog writes conflict with OC001
  (`SQLSTATE 40001`, "schema has been updated by another transaction") and could
  exhaust the DDL retry budget, failing a table before a single row loaded. The
  DROP+recreate (metadata-only) now runs in the existing pre-pass for **all**
  replace tables, not just sharded ones; workers load into the already-empty target
  without re-running the DDL (they derive the same post-load `CREATE INDEX ASYNC`
  DDLs from the applied conversion). This makes a max-parallelism Full Load start
  deterministically instead of racing the catalog.

## v0.1.115

### Fixed

- **The per-table DROP+recreate connection is now retried on a transient connect
  failure**, closing the last gap that could fail a table during a connection
  storm. In a max-parallelism Full Load (table-parallelism 16, 20 tables), the
  four queued tables start only when the first sixteen finish — which they do
  nearly together, so all four open fresh DSQL connections at once and trip
  DSQL's ~100 new-connections/second limit. `recreate_table` (and the other DDL
  connect paths in `schema_applier`) opened that connection **outside** any retry,
  so the resulting `ConnectionTimeout: connection timeout expired` failed the
  whole table with **0 rows loaded, before a single batch ran** (no OCC retry, no
  give-up log — the failure was outside the batch loop the earlier fixes hardened).
  The connection open is now wrapped in the same transient-connection retry the
  batched loader's pool leases already use, so the connect rides out the storm.
- The transient-connection classifier moved to `core/target_connection.py`
  (`is_transient_connection_error`) so **every** DSQL connect/execute path shares
  one definition — the batched loader's pool leases and the DDL connects alike.
  `batched_import` keeps a back-compat alias.

## v0.1.114

### Changed

- **The OCC/connection retry loop now logs its retries and give-ups**, so a
  batch failure is diagnosable directly instead of inferred from timing.
  `with_occ_retry` was silent; it now logs each retry at DEBUG (attempt N/max, the
  error type + SQLSTATE, and the backoff delay) and, when the budget is exhausted,
  a WARNING with the **attempt count, total elapsed time, and the last error +
  SQLSTATE**. That WARNING is the direct evidence needed to tell apart *"the retry
  budget was too small"* from *"the transient storm lasted longer than the budget"*
  from *"the error wasn't retryable"* — e.g. `occ-retry gave up after 30 attempts
  over 131.4s; last=ConnectionTimeout sqlstate=None`. Purely additive logging; no
  behavior change to the retry itself.

## v0.1.113

### Changed

- **Full Load's per-batch retry budget is now more patient (10 → 20) and
  operator-tunable**, so a batch rides out a longer transient DSQL connection
  storm instead of failing the table. The budget (`occ_max_attempts`) is shared by
  OCC (`40001`) conflicts and the transient connection retries added in
  v0.1.110/112; at high parallelism a connection storm at a load transition (many
  tables finishing → a burst of reconnects) can outlast the old 10-attempt (~20s)
  budget and exhaust it, failing a table with `ConnectionTimeout` even though the
  error was correctly classified as retryable. Raised the default to 20 (~70s of
  exponential-backoff retrying) — a large-scale load runs for hours and will meet
  such a blip — and exposed it as `DSQL_MIGRATOR_FULL_LOAD_OCC_MAX_ATTEMPTS`
  (1–100) for environments that need more. Each retry still leases a fresh
  connection and replays the idempotent batch, so this only adds patience, never
  duplicates.

## v0.1.112

### Fixed

- **Full Load now retries ANY no-SQLSTATE connection error, not just known
  message signatures.** v0.1.110 taught the loader to retry connection drops that
  carry no SQLSTATE, but matched them by a fixed list of libpq/OpenSSL message
  substrings. Under a high-parallelism connection storm (many tables finishing at
  once → hundreds of concurrent connections), DSQL surfaces the drop in *varying*
  forms — "SSL error: unexpected eof", "Network is unreachable", and
  **"connection timeout expired"** — and any message the list didn't contain
  slipped through as a permanent failure (a 1 TB run at 512 connections lost
  tables to `connection timeout expired`). The classifier now treats **any
  psycopg `OperationalError`/`InterfaceError` with `sqlstate=None`** as a transient
  connection failure (a genuine data/constraint error always carries a SQLSTATE),
  gated on the exception type so the tool's own no-SQLSTATE structural errors are
  still never retried. The message-signature list is kept only as a fallback for a
  wrapped/re-raised error whose type was lost.

## v0.1.111

### Fixed

- **DSQL connections are now pinned to IPv4, so a reconnect in an IPv4-only
  network can't fail on the endpoint's unreachable IPv6 address.** Aurora DSQL
  endpoints are dual-stack (A + AAAA records). In an IPv4-only VPC (e.g. an ECS
  task with no IPv6 egress), a reconnect that libpq routes to the IPv6 (AAAA)
  address fails with *"connection to server at … failed: Network is
  unreachable"*. That normally stays hidden — until a transient DSQL event (e.g.
  a brief `XX000 server unavailable`) forces many reconnects at once, at which
  point the IPv6 attempts fail an in-flight Full Load even though IPv4 is
  perfectly reachable (observed: a 1 TB in-VPC load lost tables to IPv6
  `Network is unreachable` right after a DSQL blip). `DsqlConnector.connect` now
  resolves the endpoint's IPv4 address and passes it as `hostaddr` (the DNS name
  stays as `host` for TLS SNI / certificate verification), so every connect and
  reconnect stays on the reachable address family. It falls back to the previous
  host-based resolution when no IPv4 is available (an IPv6-only environment is
  unaffected). Covers all DSQL connections — Full Load, Validation, and probes.

## v0.1.110

### Fixed

- **Full Load now recovers from a mid-query connection drop that carries no
  SQLSTATE (e.g. a TLS teardown), instead of failing the whole table.** The
  batched loader is designed to retry a transient connection drop by leasing a
  fresh connection and replaying the idempotent batch — but `_is_transient_connection_error`
  only recognized drops the server reported with a **SQLSTATE class `08`**. When
  the TLS socket is severed mid-query the server never sends an error code, so
  psycopg raises an `OperationalError` with `sqlstate=None` and a libpq/OpenSSL
  message like *"SSL error: unexpected eof while reading"* / *"server closed the
  connection unexpectedly"*. Those were **mis-classified as permanent** → not
  retried → the batch (and the whole table) failed. This bit hardest under high
  write parallelism (many concurrent connections → DSQL severs some at peak
  pressure): an in-VPC 1 TB load at `table_parallelism=16 × batch_parallelism=32`
  (512 connections) lost 16/20 tables near completion to `SSL error: unexpected
  eof`. The classifier now also treats a **no-SQLSTATE connection-lost error**
  (matched by libpq/OpenSSL drop signatures) as transient, so the loader
  reconnects and retries — the Full Load analogue of the CDC sink's transient
  reconnect. A real data/constraint error (which always carries a SQLSTATE) and a
  structural error with no SQLSTATE that isn't a connection drop are unaffected
  (still surface, never retried forever).

## v0.1.109

### Changed

- **CDC per-table status: "How to read this table" is far easier to scan, and
  each tricky column now explains itself in place.** The legend was a wall of
  small gray bullets where the column name was buried in prose and the
  consistency colors were only described in words. It is now a quiet bordered
  panel of **definition rows** — each term matches a table header, so the mapping
  is obvious — and the Consistency entry renders the **real badge chips**
  (`consistent` / `replicating…` / `rows missing` / `data quarantined`) in the
  exact same colors as the table cells, instead of naming the colors. In addition,
  the three non-obvious column headers (**Net rows since Full Load**, **Stream
  lag**, **Consistency**) now carry an **ⓘ tooltip** with a one-line explanation,
  so help is available right where the eye is. Added a reusable `definition_row`
  to the design system (single source of truth) for the legend layout.

## v0.1.108

### Fixed

- **Skewed CDC workloads no longer serialize a hot table on one sink task —
  Kafka topic partitions are now allocated proportionally to table size.** The
  scaling default spread partitions uniformly, which assumes write load is even
  across tables; when there are many tables (≥ the sink-parallelism cap) it
  collapsed to **1 partition per topic**, and a 1-partition topic can be consumed
  by at most one sink task. So when a few "hot" tables carried most of the writes
  (e.g. a sysbench run hitting 4 of 9 tables), each hot table was streamed by a
  single task while the rest sat idle — pure throughput loss (DSQL was near idle).
  The tool now reads scan-free per-table row-count estimates (the Full Load
  watermark's, or a fresh `information_schema` estimate if CDC infra is deployed
  before Full Load) and gives the larger tables **more partitions** via Debezium
  `topic.creation` groups (2 or 4 partitions for hot tables; 4 is the per-table
  ceiling, where a single table's gain flattens as concurrent DSQL upserts
  contend), so a hot table streams across several tasks in parallel. It is a
  no-op under even load and falls back to the previous uniform default when there
  is no size signal or an explicit `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` override
  is set. Partition counts are fixed at topic creation, so this is decided at CDC
  infra deploy; ordering is unaffected (Debezium keys each record by primary key,
  so a given key always lands on one partition). Requires a fresh CDC infra
  deploy to take effect (existing topics' partition counts are immutable).

## v0.1.107

### Changed

- **Evaluation "Objects by importance" filter is now two clear, category-based
  dropdowns instead of one confusing mixed control.** The old segmented control
  mixed a derived "Needs attention" bucket with per-classification values on one
  axis, which read ambiguously (e.g. "Needs attention" vs. "Review needed"). It
  is replaced by two AWS-Console-style filter dropdowns — **Classification**
  (Automatic / Review needed / Unsupported) and **Estimated manual effort**
  (Simple / Medium / Significant) — the same color-coded categories the summary
  badges already show. The two filters combine (AND), and a **Clear filters**
  link appears when any filter is active. Added a reusable `filter_select` /
  `filter_bar` to the design system (single source of truth) so the dropdowns
  match the Cloudscape "filtering" look.

## v0.1.106

### Fixed

- **CDC infrastructure deploy now self-heals when MSK Serverless rejects an
  auto-selected subnet's availability zone.** MSK Serverless supports only a
  subset of a region's AZs and offers no API to list them, so when the deploy
  auto-selects one NAT-egress subnet per AZ it can pick a subnet in an
  unsupported AZ (e.g. `ap-northeast-2d`), making `MskCluster` fail with
  `CREATE_FAILED … unsupported availability zones: [ap-northeast-2d]` and the
  whole stack roll back. The deployer now detects that specific failure, parses
  the rejected AZ(s) from the stack event, deletes the rolled-back stack,
  re-selects connector subnets with those AZ(s) excluded, and retries the create
  automatically (bounded, so a genuinely stuck deploy still stops). If excluding
  the unsupported AZ(s) leaves fewer than two NAT-egress AZs, it stops with a
  clear message naming the excluded AZ(s) instead of looping. No new inputs —
  the user still supplies only a VpcId.

## v0.1.105

### Added

- **Accurate, time-based CDC replication lag — replacing the imprecise `MAX(pk)`
  "Stream lag".** The old per-table "Stream lag (newest)" compared `MAX(pk)` on each
  side: a count of PK units (not time), insert-only (blind to UPDATE/DELETE lag), and
  only for single-column integer PKs. The DSQL sink now reads each change's **source
  commit time** (Debezium `source.ts_ms`) and emits a per-table **`ReplicationLagMs`**
  CloudWatch metric = apply-wall-clock − source commit time (the worst lag per
  offset-commit window, in milliseconds). The migration monitor's **"Stream lag"**
  column now shows a real time value ("8.5s behind", "2m 10s behind", "caught up"),
  read live and scan-free — accurate for any PK type and reflecting update/delete lag,
  not just the newest insert. It falls back to the `MAX(pk)` leading-edge check
  ("N behind (PK)") only when the time metric is unavailable (older plugin) or the
  counts weren't refreshed. Emission is strictly best-effort (never affects
  replication) and reuses the v18 metric plumbing/IAM (`cloudwatch:PutMetricData`,
  `metrics.stack`) — no new IAM.
- Requires the rebuilt connector plugin (`PLUGIN_VERSION` → `v19`) and a CDC
  re-deploy to take effect; until then the column uses the `MAX(pk)` fallback.

## v0.1.104

### Fixed

- **Query Playground "Test on target" now resolves unqualified table names instead
  of failing with `relation "orders" does not exist` (42P01).** A query written
  against a MySQL database uses unqualified table names (`SELECT * FROM orders`), but
  the migration maps each MySQL database to a same-named PostgreSQL **schema**
  (`ecommerce_demo`), so on DSQL the tables live in that schema — not the default
  `public` search_path the probe ran under, so every unqualified reference was
  rejected. The probe now sets `search_path` to the source database's schema (then
  `public`) before the `EXPLAIN` / dry run, mirroring the MySQL execution context so
  the converted query validates against the migrated tables. No effect when the
  source connection specified no database (search_path unchanged).

## v0.1.103

### Fixed

- **Validation no longer gets stuck on "In progress" with a locked "Re-run
  validation" button when a run finishes while you're on another step.** If you
  clicked Re-run and then navigated away (e.g. to the Data Migration step to Stop
  CDC) while the run was in flight, the poll timer that flips the step to `DONE` was
  torn down with the page — so when the run finished in the background and you
  returned to Validation, the step reconciled to `DONE` *inside the content render*,
  too late for the workflow shell (the step-header badge + Re-run button had already
  drawn the stale "In progress" state, and nothing re-rendered them). Now, whenever
  the in-content reconcile changes the persisted status (finished-while-away or the
  v0.1.102 reconnect case), it schedules a one-shot refresh so the shell re-renders
  with the reconciled status — the completed report shows with an enabled Re-run
  button. (The follow-up render sees `DONE`/`NOT_STARTED`, so it never loops.)

## v0.1.102

### Fixed

- **Validation no longer gets stuck on "In progress" with a permanently-locked
  "Re-run validation" button after a reconnect.** If the browser reconnected right as
  a validation finished (or the session was saved mid-run), the step was restored as
  `IN_PROGRESS` while its completed report was also restored — but the validation job
  id is not persisted, so no live job could ever flip it to `DONE`. The in-content
  reconcile to `DONE` ran too late (after the workflow shell had already drawn the
  stale "In progress" badge + disabled Re-run button, and nothing re-rendered the
  shell), leaving the step showing a finished report under an "In progress" header
  with Re-run locked forever. Session restore now reconciles the step to `DONE` when
  it reads `IN_PROGRESS` but a completed report is present (a report proves the run
  finished) — before the shell renders — so the completed result shows with an
  enabled Re-run button. A genuinely in-flight run has no report (it is cleared at
  run start), so this never hides a live run.

## v0.1.101

### Fixed

- **After deleting the CDC infrastructure, a reconnected session now shows the
  "Deploy CDC infrastructure" action again instead of getting stuck on the old
  "Infrastructure deleted" log.** On reconnect the session restore was re-applying a
  *completed* CDC lifecycle job's link (so the finished delete's stage log kept
  rendering) and the *stale* connector names from before the teardown (so the card
  could misclassify the pipeline) — with no path to redeploy. Restore now skips both
  the finished-job link and the stale connector names when the last CDC action was a
  teardown (`delete` / `stop`), so the card is driven by the fresh read-only AWS
  phase probe: **absent → Deploy CDC infrastructure**, **infra → Start CDC**. The
  stack identity is still restored so the probe knows which stack to check; an
  in-flight teardown is reflected by the probe's live stack status, not a stale job.

## v0.1.100

### Added

- **Durable S3 job store — an interrupted Full Load AND the per-table migration
  monitor now survive a Fargate redeploy.** The JobManager's job state lived in a
  SQLite file on the task's **ephemeral `/tmp`**, so an app redeploy (ECS task
  replacement) wiped it: an interrupted Full Load couldn't resume, and the
  per-table migration monitor — which is keyed to the Full Load job — went **blank**
  after a deploy (the S3 session store kept only the `job_id` linkage, not the job
  itself). New `S3JobStore` persists each job snapshot as a JSON object under
  `jobs/` in the tool's **managed plugin bucket** (same bucket as the session store,
  auto-provisioned — no extra setup), so job/resume state survives a task
  replacement. Wired on Fargate via `DSQL_MIGRATOR_JOB_STATE_BUCKET` → the managed
  bucket; local dev keeps the on-disk SQLite store (both satisfy the `JobStore`
  protocol, so the JobManager is unchanged).
- **Scale-safe writes (no PUT storm).** The Full Load drain persists on every
  progress tick, which is cheap for local SQLite but would flood S3 on a large
  table. Since only chunk/job **status transitions** matter for resume (a non-`DONE`
  chunk is re-run whole; sub-chunk progress is display-only and an interrupted chunk
  is reconciled to `FAILED` on reload), `S3JobStore` PUTs immediately on a status
  signature change and throttles pure-progress writes to ≤ 1 PUT / 5 s — bounding
  PUTs to ~the number of status transitions regardless of row count. Best-effort (an
  S3 error never breaks the live migration); no new IAM (the task role's existing
  bucket `/*` grant covers the `jobs/` prefix). Template + code only — no
  connector/plugin change; takes effect on the next app redeploy.

## v0.1.99

### Fixed

- **The per-table net-rows monitor now works in single-database mode, not just
  cluster mode.** The DSQL sink always emits the `NetRowsApplied` metric's `Table`
  dimension **schema-qualified** (`db.table`, e.g. `ecommerce_demo.orders`), but in
  single-database mode the tool addresses tables by **bare** name (`orders`) — so the
  monitor's exact-dimension CloudWatch lookup missed and the "Net rows since Full
  Load" column silently fell back to the `COUNT(*)`-based figure. The reader now
  `ListMetrics`-discovers the `Table` dimension values the sink actually published for
  the stack and matches each requested table by exact name, else by an **unambiguous
  bare** table name — so the scan-free column works in both cluster (already-qualified)
  and single-database (bare) naming, without assuming the qualification scheme.
  Ambiguous bare matches (the same table name under two schemas) are skipped rather
  than risk misattributing rows (that table falls back to the COUNT). Grants the app
  task role `cloudwatch:ListMetrics` (Resource `*` — the API has no resource-level
  scoping). Reader + IAM only (no connector/plugin change): a deploy updates the role
  and ships the reader — no plugin rebuild or CDC re-deploy needed.

## v0.1.98

### Added

- **Per-table "Net rows since Full Load" is now scan-free — sourced from a sink
  metric, not a `COUNT(*)`.** The DSQL sink connector now emits a per-table
  `NetRowsApplied` CloudWatch metric (namespace `MysqlDsqlMigrator/CDC`, dimensions
  `Stack` + `Table`): each commit records inserts − deletes (an insert is +1, an
  update 0, a delete −1), so summing the metric gives the net rows CDC has applied
  to each table since it started streaming. The per-table migration-status monitor
  reads this on the existing ~5 s CDC poll and shows it directly, so the "Net rows
  since Full Load" column no longer needs any `COUNT(*)` on the source or target —
  it stays light and never scans the (potentially billion-row) source. While CDC is
  streaming the per-table table now re-renders on that poll (reading the stored
  metric, no network), so the column updates **live** instead of only when you click
  "Refresh source/target counts" (which still runs the exact source/target
  `COUNT(*)` — those columns are unchanged). Emission is strictly best-effort in the
  sink (a metric failure never affects replication or offset commits), and the
  column falls back to the old `target − Full Load` figure when the metric is
  unavailable (older plugin, or the sink not yet emitting). The figure is a live
  progress monitor, not the authoritative reconciliation: it can slightly over-count
  if Kafka Connect redelivers an already-applied batch (at-least-once), so the exact
  source-vs-target verdict remains Validation (Step 4).
- Requires the rebuilt connector plugin (`PLUGIN_VERSION` → `v18`) and a CDC
  re-deploy to take effect; until then the monitor uses the `COUNT(*)`-based
  fallback. Template change grants the sink's connector-execution role
  `cloudwatch:PutMetricData` scoped by a namespace condition; the app task role's
  `cloudwatch:GetMetricData` (added in v0.1.97) reads it back.

## v0.1.97

### Fixed

- **The live CDC pipeline-health throughput now actually populates.** The UI reads
  the connectors' `AWS/KafkaConnect` CloudWatch metrics (`SinkRecordSendRate`,
  `SourceRecordPollRate`, running/errored task counts) for the change-flow panel,
  but the app task role was never granted `cloudwatch:GetMetricData` — so every read
  failed with `AccessDenied` and was swallowed best-effort, leaving the throughput
  showing blank/unknown. Grant `cloudwatch:GetMetricData` (Resource `*` — the API
  has no resource-level scoping) so the panel shows real send/poll rates. This is a
  lightweight, **source-scan-free** CDC-activity signal; it also readies the role to
  read the per-table net-rows custom metric added next. Template-only IAM change —
  a deploy updates the role (no image rebuild).

## v0.1.96

### Fixed

- **Start/Stop CDC no longer fails with `AccessDeniedException` on
  `kafkaconnect:ListConnectors`.** The CDC-deploy role granted `ListConnectors` but
  scoped it to a connector ARN (`connector/mysql-dsql-cdc-*/*`) — yet `ListConnectors`
  is an **account-level** list operation, authorized against `.../v1/connectors`, so
  the ARN scope granted nothing. The deployer lists connectors to read source/sink
  state during the two-pass Start CDC (and Stop), so that read hit AccessDenied and
  the operation errored ("could not read … state"). It became visible once v0.1.86
  made the connector-state read raise (instead of silently returning `None`).
  `ListConnectors` is now granted on `Resource: "*"` in its own statement (matching
  the task role's discovery grant); the other connector operations stay scoped to the
  `mysql-dsql-cdc-*` family. Requires an app-stack deploy to update the role (no image
  rebuild).

## v0.1.95

### Fixed

- **Hardened the S3 session store so a snapshot serialization error can't break the
  UI.** In `S3SessionStateStore.save()` the `model_dump_json()` serialization ran
  just outside the `try`/`except` that guarantees the store never raises to its
  caller, so a (dormant, but possible) serialization failure could escape and break
  the live UI request that persists session state. Moved the serialization inside
  the guard so `save()` honors its best-effort contract in all cases — no behavior
  change on the normal path. (Surfaced by the v0.1.93 change's own adversarial
  review.)

## v0.1.94

### Fixed

- **Stopping CDC no longer reports a false "Stack operation timed out".** Stop CDC
  blanks `MskBootstrapServers`, which removes the connectors — but it also used to
  tear down the in-VPC offset-seeder Lambda, and reclaiming that Lambda's Hyperplane
  ENIs takes ~20–40 min, well past the control plane's 10-minute stop wait. So the
  stop reported a failure even though the connectors were already removed (CDC was
  actually stopped) and the stack reached `UPDATE_COMPLETE` on its own minutes
  later. The cdc-stack template now keeps the seeder Lambda (+ its role) deployed
  across a stop via a new `DeploySeederFunction` condition (gated on the seeder key
  + watermark, independent of `MskBootstrapServers`); only the fast
  `OffsetSeedResource` invoker is removed on stop. Stop cleanup is then just the two
  connectors + the invoker (all quick), so the stack settles well inside the
  timeout; the VPC-Lambda ENI teardown now happens only on a full stack delete
  (whose timeout already accommodates it). Takes effect once the updated template is
  deployed — i.e. from the next Start CDC.

## v0.1.93

### Added

- **Durable per-session resume across a redeploy (S3-backed session store).** A
  reconnecting browser resumes its per-session workbench (workflow progress, the
  Step-1 Evaluation result, Schema Conversion choices, the CDC start point / adopted
  stack) instead of re-running Evaluation. That snapshot previously lived in a local
  SQLite file on the container's **ephemeral** disk, so a Fargate **task
  replacement** (any redeploy) wiped it — the operator had to redo Evaluation after
  every deploy. A new `S3SessionStateStore` (implementing the existing
  `SessionStateStore` protocol) writes each non-secret snapshot to the tool's managed
  plugin bucket (`mysql-dsql-migrator-plugins-<account>-<region>`, auto-provisioned —
  no new parameter or customer setup) under a `sessions/` prefix, so resume now
  survives a redeploy. Selected automatically on the Fargate deploy via a new
  `DSQL_MIGRATOR_SESSION_STATE_BUCKET` (the template points it at the managed
  bucket); local dev keeps the SQLite path. Non-secret state only (Property 7 — the
  source DB password is re-entered on Connect); persistence is best-effort (a
  transient S3 error is logged and never breaks the UI). The task role gains
  `s3:DeleteObject` for session delete/prune.

## v0.1.92

### Fixed

- **Adopting an existing CDC pipeline now reconciles its table set, so the CDC
  step reflects the running pipeline instead of "no tables selected".** When a
  session attaches to a pre-existing cdc-stack ("Attach to &lt;stack&gt;", e.g.
  after a session reset) — or the pipeline was otherwise started out of band —
  the session held no Full Load watermark and no in-session table selection, so
  the CDC step showed "Select at least one table before starting CDC", built its
  config preview from an empty set, and could not populate the per-table status,
  even though the pipeline was actively replicating. The render-time stack probe
  now reads the live stack's `TableIncludeList` (the source connector's
  `table.include.list`, i.e. each table's name) and reconciles it onto the
  session; `_cdc_tables_for_config` uses it as a final fallback (after an
  in-session watermark or selection). So an adopted/out-of-band pipeline resolves
  exactly which tables it is replicating — the "select a table" warning clears,
  the config preview and per-table status reflect reality — while a normal
  in-session Full Load → Start CDC flow is unchanged. Re-adopting a different
  stack clears the previous reconciled set (the fresh probe repopulates it).

## v0.1.91

### Fixed

- **The CDC "Deploy log" no longer snaps shut every few seconds while a
  lifecycle job runs.** The live CDC panel re-renders on a ~5s poll to stream
  new deploy-log lines and connector status. The "Deploy log" expansion's
  open/closed state was held in a local variable of the panel's render function,
  so each full re-render recreated it as collapsed — a log the operator expanded
  to watch a Start/Stop/Deploy would close itself a few seconds later. The
  open/closed state now lives on the session-scoped migration state, so it
  survives every level of re-render (both the inner refreshable and the outer
  panel poll) and stays open until the operator closes it.

## v0.1.90

### Fixed

- **The "CDC is running, can't apply schema" block on Schema Conversion is now
  actionable instead of a dead end.** When a CDC pipeline is already streaming into
  the target, applying schema conversion is (correctly) blocked — the sink is
  writing those tables and DDL is not replicated, so a REPLACE would drop or
  corrupt them. Previously this only surfaced as a transient toast shown when you
  clicked Apply, telling you to "stop CDC first" — but Data Migration (the only
  place CDC can be stopped) is prerequisite-locked behind Schema Conversion, so
  there was no way forward from that screen. Schema Conversion now shows a
  **persistent warning notice** at the top of the step whenever CDC is live,
  explaining that the target schema is **already applied** (CDC is streaming to it)
  and offering the one safe path: **"Skip conversion & continue to Data
  Migration"** — which both proceeds and unlocks Data Migration, where CDC can be
  stopped if the schema genuinely needs to change. The on-Apply toast now carries
  the same actionable guidance (Skip to continue, or stop CDC in Data Migration to
  change the schema).

## v0.1.89

### Fixed

- **The "attach to existing CDC infrastructure" banner now appears on the Migration
  Plan step, where CDC is actually chosen.** v0.1.88 added the banner but only on
  the Data Migration step's migration-type selector; the **Migration Plan** step is
  a separate screen (where you answer "Include CDC? — Yes, keep in sync"), and it
  did not surface the banner — so selecting keep-in-sync there still dropped you
  into the fresh "deploy CDC infrastructure" flow (and the "already exists" error)
  for a pipeline that already exists. The Migration Plan's CDC-infrastructure
  section now shows the **"Attach to &lt;stack&gt;"** banner when an existing
  `mysql-dsql-cdc-*` pipeline is discovered under a different stack name (the phase
  probe that already runs on that step also populates the discovery). Attaching
  points the session at that stack; the next probe recognizes it as deployed and
  shows "CDC infrastructure ready". The Data Migration surfacing is kept as well.

### Added

- **Existing CDC infrastructure is now surfaced on the Migration Plan, not only
  deep in the CDC step.** The previous release added account-wide CDC discovery and
  an "attach to existing" choice, but that affordance only rendered inside the
  active CDC sub-step — which a session reset makes hard to reach (you must pass the
  earlier steps first). Now, the moment the plan includes CDC, a banner beside the
  migration-type choice names any existing `mysql-dsql-cdc-*` pipeline with an
  **"Attach to &lt;stack&gt;"** action, so you adopt it right where you are —
  without navigating to the CDC sub-step and hitting a duplicate-deploy risk. The
  discovery already runs at plan time (it is gated on the plan including CDC); this
  just surfaces its result where the user is. Attaching remains read/attach-only,
  and deploying a deliberate second pipeline (a different stack-name suffix) is
  still available from the CDC step — so this is a choice, never a hard block.

### Added

- **The CDC screen now discovers existing CDC infrastructure and offers to attach
  to it, instead of blindly re-deploying.** The tool tracked which CDC stack a
  migration uses in session state only; a single-task app restart (an ECS/Fargate
  task replacement) loses that, so a reconnected session defaulted to a fresh
  "deploy CDC infrastructure" flow even when a CDC pipeline was already running
  under a different stack name — risking a second, billable Amazon MSK cluster. The
  CDC step now scans the account for `mysql-dsql-cdc-*` stacks and, when one exists
  that the session doesn't target, surfaces it with a primary **"Attach to
  &lt;stack&gt;"** action (fresh deploy is de-emphasized behind an expansion).
  Attaching re-reads the pipeline's live state from AWS (running / provisioning /
  infra), so a running pipeline lands straight on its monitoring view; it never
  mutates the stack or connectors — starting over remains the explicit **Stop CDC**
  (connectors only, keeps MSK) or **Delete CDC infrastructure** path. Requires the
  CDC-deploy role to have `cloudformation:ListStacks` (added to the app stack);
  discovery is best-effort and simply shows nothing if the grant is absent.

### Fixed

- **CDC no longer stalls silently when it can't read a connector's state; it
  surfaces the cause.** When starting CDC, the tool waits for the source connector
  to reach `RUNNING` before it requests the sink connector. That wait read the
  connector state through a helper that swallowed **every** error (credential
  expiry, throttling, a transient network blip) to `None` — indistinguishable from
  "still creating" — so a read failure made the wait log "creating…" forever: the
  sink was never requested, the deploy appeared stuck, and no error was shown.
  Recovering then required restarting the app task, which on Fargate wipes the
  in-progress session (all workflow steps had to be redone). Now the state read
  **propagates** errors; the `RUNNING`-wait tolerates a few consecutive transient
  read failures and then fails with the **actual cause**, and fails **immediately**
  on a non-recoverable credential/authorization error with a "retry Start CDC"
  hint. A genuinely-absent connector still reads as `None` (unchanged), so normal
  "still provisioning" polling is unaffected.

## v0.1.85

### Fixed

- **CDC failed to deploy: the privileged CDC-deploy role was missing CloudWatch
  alarm permissions.** v0.1.84 added a per-connector CloudWatch alarm (on
  `ErroredTaskCount`) to the CDC stack, but the app's `cdc-deploy` role was not
  granted `cloudwatch:PutMetricAlarm` / `DeleteAlarms` / `DescribeAlarms`. Starting
  CDC therefore failed while creating the alarm with an `AccessDenied` error, and
  the CDC stack rolled back (its rollback then also failed on
  `cloudwatch:DeleteAlarms`), so no connectors were created. The role now has the
  scoped alarm permissions (alarm ARNs in the CDC stack family). **Redeploy the
  app-stack to pick up the permission, then retry Start CDC** (no new image build
  is required — this is an IAM-only template change).

## v0.1.84

### Fixed

- **CDC sink survives a transient DSQL connectivity blip instead of dying
  (connector rebuilt, `PLUGIN_VERSION` v17).** On a transient failure — OCC
  retry budget exhausted, or a connection torn down by DSQL's 1-hour idle close,
  IAM-token expiry, or an MSK Connect worker recycle — the sink re-raised a plain
  `ConnectException`, which Kafka Connect's `WorkerSinkTask` treats as **fatal**:
  it kills the task, the offset never advances, and CDC stalls until a human
  restarts the connector. The sink now throws `RetriableException` for these
  transient cases, which `WorkerSinkTask` catches and redelivers (pause + retry
  the same batch) so the pipeline self-heals across a reconnect. Apply is
  idempotent, so replaying the same offsets is safe. The transient-vs-permanent
  classification is unchanged; a genuine poison row still goes to the DLQ.
- **Gapless resumability on a low-traffic source: the Debezium source connector
  now sets `heartbeat.interval.ms`.** Debezium only advances its committed binlog
  offset when it emits a record. If the captured tables are idle while other
  tables churn the binlog, the committed offset can fall behind the live binlog
  head; if source binlog retention then purges past it, a restart cannot resume
  (a gap → forced re-Full-Load). A periodic heartbeat keeps the offset advancing.
  `heartbeat.action.query` is deliberately not set — it would write to the
  read-only source; emitting the heartbeat record is enough for MySQL.

### Added

- **CloudWatch alarms surface a failed CDC connector automatically.** Each
  connector (Debezium source, DSQL sink) now has an alarm on the
  `AWS/KafkaConnect` `ErroredTaskCount` metric, so a task that errors out is
  visible without a human watching the console — previously, recovery waited
  entirely on someone noticing a FAILED connector, and a long gap could exceed
  source binlog retention. The alarms are always created (visible in CloudWatch);
  set the new optional `AlarmNotificationTopicArn` parameter to an SNS topic ARN
  to also be notified. No SNS wiring is required to deploy (the default is empty).

## v0.1.83

### Fixed

- **AI assist works outside US regions: the default Bedrock model is now a
  region-agnostic `global.*` profile.** The code-level default model id was
  `us.anthropic.claude-sonnet-4-6`, a US-geography cross-region-inference profile
  that `InvokeModel` rejects from a non-US region. An operator deploying in, e.g.,
  ap-northeast-2 (Seoul) who enabled AI assist and left the model id blank (the
  natural path) got a failure. The default is now
  `global.anthropic.claude-sonnet-4-6` (reachable from any commercial region),
  matching the CloudFormation template's own recommendation. Set `BEDROCK_MODEL_ID`
  to override. (Found by a region-portability audit; the deploy templates, region
  derivation, STS/token region, and S3 endpoint/LocationConstraint handling were
  all already region-correct — only this code default was US-locked.)

## v0.1.82

### Fixed

- **AI assist: expired/invalid AWS credentials now give an actionable message.**
  An expired-session or invalid-signature error (`ExpiredTokenException`,
  `InvalidSignatureException`, `InvalidClientTokenId`, …) was misclassified as a
  generic "unavailable"/"unknown", telling the user only that the workflow
  continues without AI — with no hint to re-authenticate. Such errors are now
  classified as `ACCESS_DENIED` on both the suggestion and "Verify AI access"
  paths, and both messages now mention re-authenticating if credentials/session
  expired.
- **Cluster-wide schema read: cross-schema foreign-key targets are now
  schema-qualified.** When reflecting an entire cluster (multiple schemas), a
  table name was qualified `schema.table` but its foreign key's referenced table
  stayed unqualified, so a downstream orphan-check / DDL query resolved the parent
  against the search_path (or a wrong same-named table in another schema). The FK
  target is now qualified with the FK's own `referred_schema` (or the reflected
  schema for a same-schema FK), matching how table names are qualified.

### Changed

- **AI assist hardening.** The Bedrock client now sets bounded connect/read
  timeouts (10s / 60s) so a hung connection can't leave an "AI is writing…" /
  "Verifying…" state spinning forever (a stalled socket surfaces as a
  classified network/timeout error). "Verify AI access" now also catches an
  error while *building* the client (e.g. no resolvable region) and reports it
  as an actionable result instead of letting the exception reach the UI. The
  persistent AI-status line in the connection screen now carries its verdict
  severity via the design-system palette instead of plain gray text.
- **Source overview: report the Aurora writer's instance class, not a reader's.**
  For an Aurora cluster endpoint the source-metadata lookup now resolves the
  writer via `DescribeDBClusters` (`IsClusterWriter`) instead of taking an
  arbitrary cluster member, so an asymmetric writer/reader topology no longer
  mislabels the source capacity (best-effort; falls back to the first member).
- **Schema apply: `CREATE SCHEMA` self-heals a duplicate-object race.** A `42P07`
  on schema creation is now absorbed as `CREATED` (the schema is present),
  matching the table/view/index self-heal path, instead of a spurious `FAILED`.

## v0.1.81

### Fixed

- **Evaluation: `TINYINT(1)`, `BIT(n)`, and `YEAR` are no longer reported as
  fully auto-compatible.** The compatibility assessor had no rule for these three
  types, so a table whose only notable column was one of them was classified
  `AUTO` / `COMPATIBLE` with zero findings — even though the schema converter maps
  all three to a *different* DSQL type with changed semantics (`MANUAL`), and a
  `TINYINT(1)` value outside `{0,1}` aborts Full Load. Evaluation therefore showed
  "fully compatible, no risk" for a table that could fail at load, contradicting
  the assessor's own "nothing is silently treated as compatible" guarantee. New
  `TINYINT_BOOLEAN` / `BIT_TYPE` / `YEAR_TYPE` rules now surface each as `MANUAL`
  with the specific risk, matching the converter's classification.

### Changed

- **Evaluation: spatial columns are now `MANUAL`, not `UNSUPPORTED`.** Spatial
  types (`GEOMETRY`, `POINT`, `POLYGON`, …) were classified `UNSUPPORTED` with a
  "substitute or redesign the column" recommendation, implying the table was
  blocked. But the converter already auto-substitutes each spatial column to
  `bytea` (raw WKB bytes preserved end-to-end through Full Load and CDC), so the
  table migrates. The new `SPATIAL_TYPE` rule reclassifies these as `MANUAL`
  (review whether raw `bytea` suffices; spatial operators/indexes are lost),
  which no longer sends users to redesign a table the tool already migrates.

## v0.1.80

### Changed

- **UI: statuses use the design-system palette instead of ad-hoc glyphs/colors.**
  A design-system consistency pass across the Data Migration and Evaluation
  screens:
  - The Full Load "CDC is streaming" warning card and the CDC consistency /
    stream-lag columns no longer embed literal `✓`/`✗`/`⚠` glyphs (a
    tofu-box risk on fonts lacking them). Severity is carried by the existing
    colored notice box / status badge; the health-table legend was reworded to
    describe the colored badges rather than glyphs.
  - Busy buttons (Fetch current position, Start CDC, Apply to target) now show
    the in-progress state by disabling + swapping the label (e.g. "Applying…")
    instead of the Quasar `loading` prop the design system forbids.
  - Warning/disruptive cues use the design system's amber rather than orange
    (Stop CDC buttons, the score gauge, effort/conflict badges).
  - Removed a dead, never-rendered `_format_complete_cell` helper.

## v0.1.79

### Fixed

- **App shell: a step-render crash now shows a red error notice, not a blue
  info one.** The top-level "step could not be displayed" fallback called
  `render_notice(tone="negative")`, but `negative` is not a defined notice tone,
  so it silently fell back to the calm blue `info` styling for what is actually
  the most alarming state in the app (an unhandled rendering exception). It now
  uses `tone="error"` (red), matching the severity.

### Housekeeping

- Open-source release hygiene: removed an internal session handoff note and
  internal author/repo identifiers from the talk decks, and replaced dangling
  citations of internal (unpublished) design/spec documents in the connector
  sources, CloudFormation template, and CDC read-models with inline summaries.
  Added `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md`.

## v0.1.78

### Fixed

- **Schema Conversion: `DOUBLE(M,D)` now emits valid DSQL DDL.** A MySQL
  `DOUBLE(M,D)` column (e.g. `DOUBLE(10,2)`) fell through the type mapper (it parses
  to sqlglot kind `DOUBLE`, which the `UDOUBLE`/`FLOAT` special cases both miss),
  so it rendered as a two-argument `FLOAT(10, 2)`. PostgreSQL/DSQL `double precision`
  takes no arguments, so this was a syntax error that failed the **entire** table's
  `CREATE TABLE` at apply time. `DOUBLE(M,D)` now maps to a plain `double precision`
  (the `(M,D)` display spec carries no storage meaning), matching the existing
  `FLOAT(M,D) -> real` handling.
- **Validation: large `BIGINT UNSIGNED` / `DECIMAL` values no longer produce false
  checksum mismatches.** The PostgreSQL-side `to_char` numeric mask provided only 18
  integer digit positions, but the MySQL side renders via `CAST(... AS DECIMAL(65,
  scale))` and `BIGINT UNSIGNED` is stored as `numeric(20, 0)`. Any integer magnitude
  at or above ~10^18 (e.g. `18446744073709551615`) overflowed the mask, so `to_char`
  emitted the overflow indicator (`####...`) instead of the digits — making a
  byte-identical value report a **checksum MISMATCH** and potentially blocking
  cut-over on a false alarm. The mask now spans the full 65-digit `DECIMAL(65,0)`
  integer range.

## v0.1.77

### Fixed

- **CDC survives a source reboot without manual intervention.** When the source
  RDS/Aurora instance rebooted (maintenance patch, failover, instance-class change),
  the Debezium source connector hit a retriable binlog error, restarted once, failed
  the restart with "Error reading MySQL variables: Communications link failure"
  (source still booting), and — because `errors.retry.timeout` defaulted to `0` (no
  retry) — Kafka Connect **killed the task permanently** ("will not recover until
  manually restarted"), a silent stall (`SourceRecordWriteRate=0`) needing a
  Stop/Start to recover. The source connector now sets `errors.retry.timeout=600000`
  (10 min) + `errors.retry.delay.max.ms=60000` — mirroring the sink — so it keeps
  reattempting across the reboot window and resumes from the committed binlog offset
  once the source is back (gapless, no human intervention). Observed and fixed after
  a 2→8 vCPU source scale-up reboot on 2026-07-08.

### Changed

- **CDC: sink MCU is now sized separately from the source (`SinkMcuCount`).** The
  sink became CPU-bound once the per-row round-trips were removed (plugin v16: ~80%
  CPU / ~21,000 rows/s at 4 MCU), while the single-task source has spare CPU. A new
  `SinkMcuCount` CFn parameter (default 4) lets the sink scale independently;
  `ConnectorMcuCount` now applies to the source only. Measured: sink 4→8 MCU took
  throughput ~21,000 → ~26,200 rows/s and CPU 80% → ~34%.

## v0.1.76

### Changed

- **CDC sink: fetch parameter metadata once per statement (plugin `v16`) — ~9.7×
  sink throughput.** `bind()` called `getParameterMetaData()` for every change
  event; on pgjdbc that is a server-side Parse/Describe round-trip, so the sink was
  issuing roughly one read-only transaction *per applied row* — confirmed by DSQL's
  `ReadOnlyTransactions` metric sitting at ~115,000/min (≈ 60× the write rate) while
  `OccConflicts` was flat 0. That hidden round-trip, not server-side write
  contention, was the real ceiling — it was cancelling most of the v13/v15 batching
  gains. The metadata is identical for every row of a given SQL, so it is now
  fetched once per prepared statement and passed into `bind()`. Measured DSQL apply
  rate rose from ~1,925 to **~18,672 rows/s** (8 partitions/tasks); read-only
  transactions dropped ~150× and sink CPU rose 10% → ~65%. Sink-jar change only
  (`PLUGIN_VERSION` → `v16`).

## v0.1.75

### Changed

- **CDC sink: multi-row INSERT rewrite (plugin `v15`) — +30% sink throughput.** The
  sink's JDBC URL now enables pgjdbc `reWriteBatchedInserts=true`, so a batch of
  single-row `INSERT`s is collapsed into one multi-row
  `INSERT ... VALUES (..),(..) ON CONFLICT ..` statement — turning N execute
  round-trips into 1. Because DSQL is latency-bound, this lifted measured sink
  throughput from ~1,500 to ~1,925 rows/s (8 partitions/tasks), cross-checked by
  the DSQL apply rate. To make the rewrite safe, `applyChunkBatched` first dedupes
  each same-SQL run to one row per primary key (last image wins — idempotent,
  order-preserving); without it a rewritten multi-row `ON CONFLICT` would reject a
  duplicate conflict key ("cannot affect row a second time"). Sink-jar change only
  (`PLUGIN_VERSION` → `v15`).

## v0.1.74

### Changed

- **CDC connector scaling is now inferred, not hardcoded.** The tool computes the
  per-table topic partition count, sink `tasks.max`, and MSK Connect MCU count from
  the number of captured tables (`compute_cdc_scaling_defaults`) and passes them at
  cdc-stack create. It picks the smallest partitions-per-topic that brings total
  sink parallelism (`partitions × tables`) up to a ceiling of 8 — e.g. 1 table → 8
  partitions, 4 tables → 2 each, ≥8 tables → 1 each — because the sink is
  DSQL-write-latency-bound and scales sublinearly past that point. Partition count
  is set at create because it is irreversible (a topic's partitions can only be
  raised). Previously `topic.creation.default.partitions` was hardcoded to `4`; it
  is now the `TopicDefaultPartitions` CFn parameter. Advanced operators can override
  the inference with `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS`,
  `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX`, and `DSQL_MIGRATOR_CDC_MCU_COUNT`. Manual §7.2
  (CDC) documents the model.

## v0.1.73

### Changed

- **CDC source throughput tuning (plugin `v14`).** After the v0.1.72 sink batching,
  the bottleneck moved to the Debezium source (~2,000 rec/s at ~12% CPU —
  produce/queue-bound, not binlog-parse-bound). New CFn parameters expose the
  source pipeline knobs so a redeploy can widen it: `SourceMaxBatchSize` (8192) and
  `SourceMaxQueueSize` (32768) drain more binlog events per streaming iteration, and
  `SourceProducerBatchSize` (256 KiB), `SourceProducerLingerMs` (20), and
  `SourceProducerCompression` (`lz4`) enlarge and compress the Kafka produce batch.
  The producer knobs are set as `producer.*` in the **source worker config** — MSK
  Connect rejects per-connector `.override.` keys — so the immutable worker config
  is renamed via a `PLUGIN_VERSION` bump to `v14` (no connector JAR changed).

## v0.1.72

### Changed

- **CDC sink throughput: batched apply (plugin `v13`).** The DSQL sink connector
  now coalesces each maximal run of *consecutive* change events that render to the
  same SQL into a single JDBC `executeBatch()` instead of a per-row
  `executeUpdate()`. DSQL is latency-bound — each statement is a distributed
  round-trip, and the sink task was observed running at ~5% CPU / ~550 rec/s
  (round-trip-bound, not compute-bound). Collapsing per-row round-trips into
  batched sends is the primary throughput lever. Apply **order is preserved**:
  only contiguous identical-SQL events group, so an upsert followed by a delete on
  the same PK still applies in arrival order, and a run breaks on any
  table/column-set/kind change. Poison-row isolation, OCC retry, and idempotent
  replay are unchanged (a permanent failure still falls back to record-by-record
  apply). Bumps `PLUGIN_VERSION` to `v13`.
- **CDC sink `consumer.max.poll.records` now defaults to 3000.** New
  `SinkMaxPollRecords` CFn parameter (default 3000, set in the sink worker config).
  The Kafka default (500) capped how many records reach one `put()` call — and
  thus how many the connector can batch into one ≤3000-row DSQL transaction —
  leaving the batched apply under-filled. Matching it to the transaction limit lets
  a full poll fill one round-trip.
- **CDC throughput defaults raised for large-scale sources:** `ConnectorMcuCount`
  4, `SinkTasksMax` 4, and per-table `topic.creation.default.partitions` 4, so the
  sink can consume a data topic across 4 partitions in parallel out of the box
  (effective sink concurrency is capped by the partition count). The app stack also
  now allows 8/16 vCPU task sizes.

### Fixed

- **CDC: non-GTID sources reliably fall back to file:position mode.** Debezium is
  now told to exclude all GTIDs (`gtid.source.excludes=.*`) and not filter DML on a
  missing GTID (`gtid.source.filter.dml.events=false`), so a source with
  `gtid_mode=OFF` (e.g. RDS MySQL where GTID can't be enabled) captures changes via
  binlog file:position instead of producing zero records.

### UI

- **Start CDC gives immediate feedback:** the button shows a loading state and a
  toast on click, rather than appearing unresponsive while the deploy request is
  in flight.
- **Interrupted CDC stages show a FAILED icon** once the job has ended, instead of
  remaining stuck on an in-progress spinner.

## v0.1.71

### Fixed

- **CDC: `SnapshotMode` now actually reaches the CloudFormation template.**
  v0.1.70 computed the correct mode in Python but the cdc-stack template had
  `snapshot.mode: recovery` hardcoded — the fix never reached the deployed
  connector. Added a `SnapshotMode` CFn parameter and wired it through
  `build_cdc_stack_params` / `build_cdc_infra_params`. Start CDC now also passes
  the updated template (not `UsePreviousTemplate`) so new parameters are
  recognized by stacks deployed before this version. This is the real fix for
  the "Could not find existing redo log information" connector failure.

- **CDC: source DB port is now read from the session's source config.** Previously
  always defaulted to 3306, causing connector timeout failures when the source
  runs on a non-standard port.

## v0.1.70

### Fixed

- **CDC: `snapshot.mode` now correctly uses `schema_only` for new connectors.**
  Previously hardcoded to `recovery`, which requires a pre-existing schema-history
  topic. Now `recovery` is used only when a real Full Load watermark (with binlog
  coordinates) exists; all other cases — manual start, session reset, CDC-only
  flow — use `schema_only`. Eliminates the "Could not find existing redo log
  information" connector failure.

- **CDC: pre-flight subnet NAT egress check prevents 10-minute silent failures.**
  MSK Connect assigns private IPs only — subnets without NAT gateway egress
  cannot reach Secrets Manager. Both user-supplied and auto-discovered subnets
  are now verified before deploy submission. Also re-verifies discovered subnets
  at deploy time to catch race conditions (e.g. another stack's NAT deleted
  between diagnosis and deploy).

- **CDC: prerequisites button locked during CDC deploy/start.** The Check button
  was only disabled during Full Load; now it's also disabled while a CDC stack
  operation is in flight.

## v0.1.69

### Added

- **CDC: "Fetch current position" button in Manual start-point mode.** When no
  Full Load watermark is available (CDC-only flow), the Manual start-point form
  now includes a "Fetch current position" button that queries `SHOW MASTER STATUS`
  on the source and auto-fills the GTID and binlog fields. Eliminates the need to
  manually run SQL on the source and copy-paste coordinates.

## v0.1.68

### Changed

- **Full Load: multi-process parallelism (GIL bypass).** Tables now load in
  separate OS processes via `ProcessPoolExecutor`, giving each table (or shard)
  its own Python GIL and its own CPU core. Large tables with a single integer
  primary key are automatically split into PK-range shards across multiple
  processes. All work units — whole-table workers and shard workers — share one
  bounded pool. Measured on ECS Fargate 8 vCPU:
  - 4 tables mixed (tp=8): **34,800 rows/s** at CPU 561% (was 12,277 at 110%)
  - Single 33.6M-row table sharded (tp=8): **51,000 rows/s** at CPU 777%
  - 200GB table estimate: **~2.5 hours** (was ~46 hours, **18× faster**)
  - Backward-compatible: test doubles automatically use the thread fallback.

## v0.1.67

### Changed

- **Full Load single-table throughput optimizations (GIL-aware).** Five changes
  that compound to reduce GIL hold time and network round-trips:
  1. MySQL keyset page size raised from 1,000 to 5,000 rows — 5× fewer source
     round-trips per table (the dominant bottleneck).
  2. `build_insert_statement` SQL template cached per batch shape — eliminates
     ~40,000 object allocations per batch (99.99% cache hit on large tables).
  3. `_iter_batches` byte estimation made lazy — samples the first row of each
     batch and only checks per-row near the 8 MiB budget, eliminating 90%+ of
     `_estimate_row_bytes` calls for normal-width tables.
  4. `_flatten_params` converted to list comprehension (~40% faster in CPython).
  5. `convert_row` passthrough fast path — columns that need no type conversion
     (int, varchar, numeric, text) skip `convert_value` entirely via a
     precomputed frozenset lookup.

### Fixed

- **"Retry unfinished tables" button now gives immediate visual feedback.**
  The button shows "Checking target…" with a hourglass icon and disables
  itself while probing the target, then shows a toast on retry start. Previously
  the slow probe ran without visible feedback so the UI felt unresponsive.
- **Per-object "Apply to target" in Schema Conversion now detects existing tables**
  and shows a Replace/Skip dialog (previously silent SKIP due to unwired
  existence checker; now resolved from the target inventory).
- **"Keep integer PK" renamed to "Keep source PK"** — the label was misleading
  for tables with non-integer primary keys.
- **"Apply converted to target" renamed to "Apply all to target"** — clearer.

## v0.1.66

### Changed

- **Migration overview diagram redesigned as a single unified panel.** The three
  separate bordered cards (Source / Migration Tool / Aurora DSQL) are now
  borderless column segments inside one shared surface. Status indicators use a
  lighter dot + text pattern (Cloudscape "StatusIndicator") instead of bordered
  chip badges, flow connectors are simpler dashed arrows with plain text captions,
  and the overall chrome is significantly reduced while preserving all information
  (endpoint, engine, region, connection state). Adds a reusable
  `render_status_dot` component to the design system (`ui/design.py`).

## v0.1.65

### Changed

- **Applying a single object that already exists now asks how to handle it,
  instead of silently skipping.** In Schema Conversion (Step 2), clicking a
  per-object **Apply to target** for a table that already exists on the target
  (and whose DDL you did not edit) previously just reported `SKIPPED` and left the
  target unchanged — easy to miss, and there was no way to change your mind from
  that button. It now opens a **Replace / Skip / Cancel** dialog so the choice is
  explicit at the moment you apply. This matters when you *revert* a choice — e.g.
  switch a table back from a composite key to the integer key: SKIP would have left
  the old composite table in place, whereas Replace drops and recreates it with the
  new DDL. (Editing an object's DDL, or the global REPLACE mode, still routes
  through the existing destructive-replace confirmation.)
- **The per-object Apply to target button now shows it is working.** While the
  apply runs (a target round-trip that can take a moment, or waits on the
  confirmation dialog), the button switches to a disabled, loading spinner state
  and returns to normal when the apply finishes — so a slow apply no longer looks
  like a dead click. The busy state is always cleared, even if the apply fails.

## v0.1.64

### Added

- **Opt-in per-table composite primary key (write hot-partition fix).** Aurora
  DSQL stores rows in primary-key order, so a monotonic `AUTO_INCREMENT` key
  funnels every insert into one partition — a write hot partition that caps
  throughput. Schema Conversion (Step 2) now offers a per-table **primary key**
  picker: keep the integer key (default, unchanged) or switch to a **composite
  key** that prepends a high-cardinality column you choose (e.g.
  `(customer_id, id)`) so writes spread across partitions. The source MySQL schema
  is never changed — only the DSQL target key. The tool only offers NOT NULL,
  non-key columns as the leading column, validates the result against DSQL's key
  limits (≤ 8 columns, ≤ 1 KiB), and emits a `CREATE UNIQUE INDEX ASYNC` on the
  original key so its uniqueness is preserved. A notice at selection time spells
  out the consequence: after cutover the application's queries, joins, and upserts
  must use the new composite key, and the leading column must be immutable.
  - **Full Load** loads a composite table correctly: the idempotent
    `INSERT ... ON CONFLICT` now keys on the **target** primary key (previously it
    always used the source key), so a changed key no longer mismatches the target
    constraint. Appending into an existing table whose target key differs is
    refused with a clear message (reload fresh to apply the new key first).
  - **CDC** replicates a composite table with no connector/plugin change: the
    Debezium source is re-keyed via `message.key.columns` so each change record's
    key matches the target composite key, and the sink's idempotent upsert/delete
    apply against it unchanged. CDC start refuses to proceed only if a composite
    key column was also chosen for LOB exclusion (it must be captured to build the
    key), with an actionable message.

## v0.1.63

### Changed

- **Full Load can read a large table with multiple concurrent readers (reader
  range sharding).** The single keyset reader is CPU-bound (per-row type
  conversion) and tops out near one core, so a big table's read is now optionally
  split into K disjoint primary-key ranges streamed concurrently, all feeding the
  one write pool. Off by default (`DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS=1`); only
  applies to a table with a single **integer** PK and at least
  `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` (default 1,000,000) estimated rows —
  composite/non-integer PKs and smaller tables always use one reader. Bounded so
  total source readers (`table_parallelism × shards`) stay within a safe ceiling.
  Sharding is **not** applied on a clean replace load (plain INSERT, no CDC), whose
  single consistent snapshot must be preserved; it is limited to the idempotent
  existing-data/CDC path where the watermark + idempotent re-load make per-shard
  snapshot skew safe. No change to resumability, OCC handling, or the write side.

## v0.1.62

### Changed

- **Full Load reads ahead of the write pool (bounded prefetch queue).** The source
  reader now fills a bounded queue from a dedicated background thread, so reading
  page N+1 overlaps writing page N instead of the two running serially. Memory
  stays bounded (the queue is capped at ~2× the write parallelism), the load order
  is unchanged (batches still map to fixed PK ranges), and the reader thread is
  joined on stop/cancel so nothing leaks. On by default; a measurement seam
  (`DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0`) can disable it to reproduce the previous
  path for A/B benchmarking. No change to load correctness or resumability.

## v0.1.61

### Changed

- **Simpler Full Load progress table.** Trimmed from 9 columns to 6 so it reads at
  a glance and stops wrapping: "Rows on target" and "Source rows" are merged into
  one **Rows (target / source)** column with large counts abbreviated
  (`1.18M / 33.6M`) and the exact figures + new/already-there breakdown moved to a
  hover tooltip; the **Errors** column is folded into **Attempts** (e.g. `5 · 1 err`);
  the redundant **Complete** column is dropped (Status + Progress already show it);
  and the **Time** header no longer wraps. Small counts still show in full with
  thousands separators.

## v0.1.60

### Changed

- **Prerequisite checks can't be re-run while a Full Load is in progress.** You
  could previously go back to the Prerequisites step and click "Check" mid-load.
  It was harmless (the checks are read-only and never touch the running job — a
  fresh result only applies to the *next* run), but pointless and confusing: a
  newly-failing check would show a red "blocked" verdict while the load kept
  running, and it added avoidable read load on the source. The Check button is now
  disabled while a Full Load is IN_PROGRESS, with a short note explaining the
  checks apply to the next run and don't affect the running load — matching how
  the migration-type selector already locks during a run. Stop the load to re-run
  checks.

## v0.1.59

### Changed

- **Full Load "Failure details" is cleaner and no longer shifts on long errors.**
  Removed the per-row "Reload" button from each failure — retrying is now driven
  solely by the single "Retry unfinished tables" checklist below, so there's one
  consistent way to retry instead of two competing controls. Each failure row now
  has a stable layout (table name + wrapping error message on the left, the
  "AI Assist" action pinned to the right) so a long error message no longer pushes
  the buttons onto a second line or misaligns them between rows. Quarantined-row
  entries keep their own "Reload" (a quarantined table is DONE, not "unfinished",
  so the retry checklist doesn't cover it).

## v0.1.58

### Fixed

- **The Full Load progress table no longer jumps back to page 1 while a load is
  running.** The per-table table refreshes every ~1.5 s, and each refresh rebuilt
  it from scratch — so paging to page 2+ snapped you back to page 1 on the next
  tick. The chosen page is now preserved across refreshes (and clamped so a
  shrinking table can't strand you on a now-empty page).

## v0.1.57

### Fixed

- **Tables left PENDING by a failed run can now be retried (were stranded).** If a
  Full Load ended in failure *before* some tables were even attempted, those
  tables stayed `PENDING` (not `FAILED`) — and the recovery UI keyed only off
  `FAILED` chunks, so it showed no "Retry" action and the only escape was a full
  "Re-run Full Load". Recovery now targets every **unfinished** table (FAILED *or*
  PENDING): the retry row appears whenever a terminated run has unfinished tables,
  the button reads "Retry unfinished tables (N)", and the checklist lists each one
  with its reason (its error, or "Not loaded yet — the previous run ended first."
  for a PENDING table). Already-loaded (DONE) tables are still kept and never
  re-run needlessly. (This is the recovery path for the v0.1.56 crash: after
  updating, click Retry unfinished tables to resume the PENDING ones.)

## v0.1.56

### Fixed

- **"Drop & reload" no longer crashes the whole run with a `SchemaApplier`
  TypeError.** Choosing Drop & reload for a table whose dependent views had to be
  dropped/recreated raised `TypeError: SchemaApplier.__init__() missing 1 required
  positional argument: 'introspector'`, which aborted the entire Full Load and
  wiped the per-table progress view (showing only "Migration failed"). The
  dependent-view pre-drop/recreate now uses introspector-free DDL helpers
  (`drop_object` / `recreate_table`) instead of constructing a `SchemaApplier`
  incorrectly, so a clean reload succeeds. Additionally, the optional view
  pre-drop/recreate passes are now defensive: any unexpected failure there is
  logged and skipped rather than failing the run — so a view-handling problem can
  never again wipe your Full Load progress (a table that truly can't drop still
  surfaces as a normal per-table failure you can act on).
- **Full Load step no longer errors when CDC is live and the run is startable.**
  Fixed a `NameError` (a stale `cdc_live` reference) that broke the Full Load
  step's render in the specific case where CDC is streaming and the Start/Re-run
  button is enabled.

## v0.1.55

### Changed

- **"Retry failed tables" now lets you pick which failed tables to retry.** The
  retry dialog lists each failed table with a checkbox (all pre-checked) and its
  failure reason, so you can uncheck the ones you're not ready to retry yet (e.g.
  a source value you haven't fixed, or a dependency you haven't resolved) and
  retry only the rest. Confirming retries just the checked subset — and the
  read-only "already has data" probe and the Append-vs-Drop choice are scoped to
  that subset too. Confirm is disabled when nothing is checked. Retrying all (the
  common case) is unchanged: leave everything checked and confirm. The per-table
  "Reload" shortcut is unchanged.

## v0.1.54

### Changed

- **"Retry failed tables" and per-table "Reload" now offer the same Drop-vs-Append
  choice as Start.** Previously the choice was only made on the initial Start Full
  Load and a retry silently reused it, so you couldn't switch to a clean reload
  after a failed append (short of a full Start over). Retry and Reload now run the
  same read-only probe (scoped to just the tables being retried) and open the same
  confirm dialog, so you pick **Append** or **Drop & reload** at retry time too.
- **The Drop & reload choice now spells out that it uses your edited schema.** The
  dialog notes that a drop & reload recreates each table from your **applied Schema
  Conversion — including any edits you made there** — and rebuilds its secondary
  indexes after loading, so it's clear a schema change is honored on a clean
  reload (it already was; this just makes it visible).

## v0.1.53

### Fixed

- **Full Load now lets you choose Drop-vs-Append for tables that already have
  data — and a retry keeps that choice.** Previously, if a selected target table
  already held rows, the tool decided for you (DROP+recreate on the first run),
  and a **retry silently reverted to append**, reporting "0 new + N already there"
  over stale data — so a failed load could look clean without actually refreshing
  anything. The Start Full Load dialog now asks, once for the run: **Append**
  (keep existing rows, load only the missing ones — idempotent, the default) or
  **Drop & reload** (DROP and recreate each table first, for a clean load). The
  choice is stored, so **retry and per-table Reload follow the same behavior**
  instead of quietly changing it.
- **"Drop & reload" no longer fails when a view depends on the table.** A
  dependent view (e.g. `customer_order_summary`) used to block `DROP TABLE` with
  `DependentObjectsStillExist`, leaving the old rows in place. The drop path now
  drops the dependent views first (a run-level pre-pass, since a view can span
  several tables loaded in parallel) and **recreates them after the load**, so a
  clean reload succeeds and your views survive — without a blunt `DROP … CASCADE`.
  Suppressed while CDC is streaming (a DROP would race the live sink).

## v0.1.52

### Added

- **AI Assist on each failed Full Load table.** Every table in the Full Load
  "Failure details" list now has an "AI Assist" button next to "Reload". It opens
  the AI chat drawer and explains that specific failure's likely cause and how to
  fix it — grounded in the actual error text (e.g. a `DependentObjectsStillExist`
  drop conflict from a dependent view, or a transient `InternalError_: server
  unavailable`) **and in this migration's situation**: the migration type
  (Full-Load-only vs Full Load + CDC), whether the table was a DROP+recreate of an
  existing target, and whether CDC is already streaming. So the guidance is
  specific to your migration, not generic, and points at the right recovery
  (fix a schema dependency, fix a source value, or just Reload a transient). Opt-in
  — the button is enabled only when AI Assist is turned on at Connect; otherwise it
  shows a disabled affordance pointing there. Reuses the existing chat-drawer /
  Bedrock stack (no new credentials path).

## v0.1.51

### Fixed

- **Prerequisites section no longer collapses when you click "Check" in a
  reconnected session.** After an app restart you may need to (re-)run the
  prerequisite checks. Expanding the Prerequisites section and clicking Check
  used to collapse it immediately — the click triggers a re-render, and the
  section only stayed open when it was the "active" sub-step, which after a
  reconnect is a later step. It now stays expanded while it is the actionable
  section (its checks are running, or it still blocks the run), so the running
  spinner and the results remain visible.

## v0.1.50

### Changed

- **Schema Conversion object browser matches the "Tables to migrate" styling.**
  The Step 2 source/target browsers now use the same look as the Step 3 table
  picker: white, bordered scroll panels and connector-less trees. Each source
  table leaf shows the same primary-key indicator (green check when the table has
  a primary key, amber warning when it has none, which Aurora DSQL requires) with
  a legend under the filter. Views/triggers/routines carry no PK indicator (they
  have no primary key). Selection and DDL-generation behavior is unchanged.

## v0.1.49

### Fixed

- **"Tables to migrate" filter now works, and the primary-key icons have a
  legend.** The name filter box above the table tree rendered but did nothing —
  it wasn't bound to the tree — so typing filtered nothing; it's now wired to the
  tree's filter (typing narrows to matching table leaves). Added a small legend
  under the header explaining the per-table icons: a green check means the table
  has a primary key, an amber warning means it has none (which Aurora DSQL
  requires).

## v0.1.48

### Changed

- **"Tables to migrate" picker: back to the schema tree, with the modern
  styling kept.** Reverted the flat data table (v0.1.47) to the schema → Tables →
  leaf object-browser tree, but wrapped in the same AWS/Cloudscape styling — a
  name filter, Select all / Unselect all, and a live "N of M selected" counter
  above a white, bordered scroll panel. Each table leaf now shows a small primary-
  key indicator (a green check, or an amber warning when the table has no primary
  key, which Aurora DSQL requires); other metadata columns from the table view
  were dropped to keep the tree light. The PK indicator is a client-side Quasar
  slot, so it adds no per-node work. Selection behavior and the locked (dimmed,
  non-interactive) state are unchanged.

## v0.1.47

### Changed

- **"Tables to migrate" picker restyled as a compact AWS Console (Cloudscape)
  data table.** Step 3's table picker was a schema → Tables → leaf tree with a
  checkbox at every level. It's now a flat, sortable data table with a single
  checkbox column and one row per table, showing more at a glance: schema,
  column count, whether the table has a primary key (a green check, or an amber
  warning when absent since DSQL requires one), secondary-index count, and a
  "exists"/"new" target status chip. A name filter and a live "N of M selected"
  counter sit above it. Fewer checkboxes, higher information density, same
  selection behavior — the ticked set still drives Full Load / CDC / prerequisite
  checks, and the picker still locks (dimmed, non-interactive) once checks have
  run or CDC is live.

## v0.1.46

### Changed

- **Clearer "re-run prerequisites" message after an app restart.** If you had
  already cleared the Data Migration prerequisites but hadn't started the Full
  Load yet, an app restart used to gate the run behind the same blunt "Run the
  prerequisite checks first" prompt shown to a first-time user — reading as if
  your progress was lost. The checks still must re-run (they're read-only, and
  the source connection is re-established on reconnect so a stale result can't be
  trusted), but the message now names the situation: "Reconnected — re-run the
  prerequisite checks to resume. They're read-only and quick; your progress
  wasn't lost, but the results aren't kept across an app restart." A genuine
  first-time user still sees the original prompt. Detected from the persisted
  active sub-step (only reachable once checks passed), so the two cases can't be
  confused.

## v0.1.45

### Changed

- **Performance-tuning control restyled as a compact AWS Console (Cloudscape)
  form.** The sidebar "Performance tuning" panel no longer stacks four bare
  number inputs. It now opens with a one-line info Alert (applies to the next
  run; live/app-wide, resets on restart; connections ≈ tables × batches), then
  lays the knobs out as grouped form fields under "Full Load" / "Validation"
  section subheaders. Each knob is a single dense row — label, an info glyph
  whose tooltip carries the longer description, the allowed range, and a
  bounded number input — so the whole panel stays tight in the narrow sidebar.
  The knob metadata (group / label / description / range) all lives in
  `config.py` so the UI and the validation messages share one source of truth.
  No behavior change to what the knobs do.

## v0.1.44

### Fixed

- **"Start Full Load" can't be double-clicked into two confirm dialogs.** Opening
  the confirm runs a ~1–2 s read-only probe (which target tables already hold data)
  before the dialog appears; a fast double-click used to open the dialog twice. The
  handler now drops a second click while the probe is in flight (re-entrancy guard)
  and shows a busy cue — the clicked button disables and reads "Checking…" with an
  hourglass icon, restoring when the dialog opens. Applies to both the initial
  Start and the terminal Re-run buttons.

## v0.1.43

### Changed

- **Deploy-log timestamps now show the `UTC` zone.** Each CDC deploy/teardown log
  line reads `HH:MM:SS UTC - …` (was zone-less `HH:MM:SS - …`), making it
  unambiguous and consistent with the downloaded activity log, CloudWatch, and
  CloudFormation events — all UTC.

## v0.1.42

### Fixed

- **CDC stack-name field alignment.** The fixed `mysql-dsql-cdc-` prefix is now
  rendered inside the input via Quasar's `prefix` prop (baseline-aligned with the
  typed suffix, like `$` before an amount) instead of as a separate left label that
  floated out of line with the field's own label. A one-line helper below shows the
  resulting full stack name.

## v0.1.41

### Changed

- **CDC stack-name field is now suffix-only, so a custom name can't be silently
  rejected.** The mandatory `mysql-dsql-cdc-` prefix is shown as a fixed, read-only
  addon and you edit only the suffix (e.g. `orders` → `mysql-dsql-cdc-orders`).
  Previously, typing a name without the prefix (e.g. `abcde`) was rejected and
  reverted to `mysql-dsql-cdc-stack` with a warning — confusing, since the prefix is
  required by the deploy role's IAM scope. Now `abcde` simply becomes the valid
  `mysql-dsql-cdc-abcde`; only an illegal-charset suffix is rejected.

## v0.1.40

### Changed

- **"Start over" shows a "Checking…" busy state while it probes CDC.** Opening
  Start over runs a ~1–2 s read-only AWS probe (to decide whether to offer the CDC
  stop/delete tiles); the button now disables and swaps to "Checking…" with an
  hourglass icon during that probe, then restores when the dialog opens — a visible
  cue that also prevents a double-open. (Label/icon swap, matching the app's busy
  idiom, not Quasar's `loading` prop which artifacts on flat buttons.)

## v0.1.39

### Fixed

- **Start over no longer warns about "CDC keeps billing" when CDC is already gone.**
  If a fresh live probe confirms no CDC infrastructure exists (e.g. you just
  finished deleting the stack), the Start-over dialog no longer shows the
  "resetting does not delete CDC infrastructure — MSK/NAT keep billing" caution,
  which was misleading about infra that is already torn down. The warning still
  appears when the probe is inconclusive (a hedge) and, of course, when CDC really
  is deployed (that path shows the stop/delete tiles instead).

## v0.1.38

### Changed

- **The CDC card shows a clear "being deleted" state during teardown.** While the
  cdc-stack is `DELETE_IN_PROGRESS`, the pipeline card previously read as a vague
  "Busy" / "cdc-stack needs cleanup — wait for the current operation". It now shows
  a **"Deleting…"** badge and a reassuring notice — *"CDC infrastructure is being
  deleted (~15–25 min — the in-VPC Lambda's network interfaces take time to detach);
  MSK / NAT billing stops once it completes"* — and keeps polling so it flips to
  "Not deployed" on its own. A settled-but-stuck stack (`ROLLBACK_COMPLETE` /
  `DELETE_FAILED`) still shows the "needs cleanup — delete then redeploy" guidance.
  (New pure helper `cdc_unstable_message` drives badge + notice from one place.)

## v0.1.37

### Fixed

- **"Start over" no longer races an in-flight CDC teardown.** After choosing to
  stop/delete the CDC pipeline during Start over, the CloudFormation stack is
  `DELETE_IN_PROGRESS` for ~15–25 min — during which the header "Start over" button
  stayed clickable, and because the reset had already wiped the session, a second
  attempt no longer recognized the running teardown (confusing, and for a custom
  stack name a risk of orphaned MSK/NAT billing). Start over is now **blocked while
  a CDC stop/delete is actually in flight**: the dialog explains that a teardown is
  running and offers only Close (no RESET). Detection is narrow — a live
  `*_IN_PROGRESS` stack status or a PENDING/RUNNING stop/delete job — so a settled
  but stuck stack (`ROLLBACK_COMPLETE` / `DELETE_FAILED`) can still be reset and
  cleaned up. The `run_cdc_delete` already-deleting backstop is unchanged.

## v0.1.36

### Added

- **Runtime performance tuning from the UI.** A new **Performance tuning** control
  in the sidebar footer (next to Diagnostics) lets an operator retune the four Full
  Load / Validation parallelism knobs (`FULL_LOAD_TABLE_PARALLELISM`,
  `FULL_LOAD_BATCH_PARALLELISM`, `FULL_LOAD_BATCH_ROWS`, `VALIDATE_MAX_WORKERS`)
  **between runs without a redeploy or restart** — the loader and validator re-read
  the config on every run, so a value set here applies to the next Full Load /
  Validation. Each field is bounded by the same limits as the config (single source
  of truth), app-wide (single-task app), and resets to the deploy/startup values on
  restart. Set the task-definition `environment` for values you want to persist;
  use this control to experiment live.

## v0.1.35

### Fixed

- **AI assist now deploys in non-US regions (e.g. Seoul / ap-northeast-2).** The
  `BedrockModelId` deploy parameter accepted only `us.` inference profiles, and the
  task-role `bedrock:InvokeModel` scope was derived by splitting on `"us."` and
  hard-coded to the US member regions (us-east-1/2, us-west-2) — so AI assist could
  not be enabled outside the US (a non-`us.` id was rejected at parameter
  validation, and the derived IAM scope was wrong for other geographies). The
  parameter now also offers `global.` profiles (portable to any region), the
  foundation-model id is derived by splitting on `"anthropic."` (present in every
  `us.`/`global.`/`apac.` profile id), and the foundation-model ARN is scoped
  region-agnostically (region `*`, exact model id) instead of enumerating per-geo
  member regions. Still least-privilege — the `*` is only the region field; the
  model id stays exact and the resource is never a blanket `*`.
- **CDC deploy no longer opens `0.0.0.0/0` egress to the source DB by default.** At
  CDC-infra deploy the tool now auto-discovers the source DB's security group (RDS
  `DescribeDBInstances`, read-only) and scopes the connector's egress-to-source
  rule to it, so the stack stops falling back to an open source-port egress on
  every UI deploy. Best effort — a non-RDS host or missing `rds:DescribeDBInstances`
  leaves it empty (documented fallback, unchanged).
- **CDC sink log corrected + dead in-memory S3 CSV export removed.** The sink's
  `start()` advisory now states a permanently-rejected record with no DLQ **fails
  the task** (the actual behavior), not "logged and skipped"; and an unreachable,
  whole-file-in-memory S3 CSV export path was deleted (the shipping path streams
  page-bounded). No behavior change to the live data path.

### Changed

- **Default container image bumped to `0.1.34`.** The app-stack default
  `ContainerImageUri` still pointed at `0.1.31` while the shipped release was newer,
  so a fresh deploy ran a stale image.

### Docs

- **Japanese (日本語) manual + docs**, with a 3-way English / 한국어 / 日本語
  language switcher across the manual, README, deployment guide, and changelog.
- **Natural-Korean pass** over the Korean manual (fluency + terminology
  consistency), a rewritten testing chapter, and a new measured-results section in
  the performance chapter.
- **Architecture diagrams as PNGs** embedded in the README (the full topology is
  click-to-enlarge); the editable `.drawio` sources are no longer shared.
- **Deployment guide**: the AWS CLI example now enables AI assist inline
  (`EnableAiAssist` / `BedrockRegion` / `BedrockModelId`); Apache-2.0 `LICENSE`
  copyright line filled; internal working documents removed from the repo.

## v0.1.34

### Added

- **AI DBA query tuning in the Query Playground.** After a converted `SELECT`
  passes "Test on target", a new **Tune with AI DBA** action opens the shared AI
  chat drawer and rewrites the query for Aurora DSQL efficiency — grounded on the
  query's REAL captured EXPLAIN plan and DPU cost, and on Aurora DSQL's own
  execution model (the primary key *is* the table, filter pushdown through the
  three filter layers, `Full Scan` vs. `Index`/`Index Only Scan`, and DPU as the
  cost unit). It explains what it changed and why it is cheaper on DSQL, and is
  explicitly steered away from vanilla-PostgreSQL tuning advice that does not
  apply to DSQL. Each proposed rewrite has a **Test rewrite on target** action
  that re-runs it read-only on the target and has the AI report the measured
  before/after DPU improvement in the same chat. Opt-in (AI off by default),
  advisory only — nothing is auto-applied, and the measured DPU (not the model's
  prose) is the proof of improvement.

## v0.1.33

### Fixed

- **"Start over" now reliably offers to tear down a deployed CDC pipeline,
  regardless of which step you were on.** The reset dialog decides whether to show
  the stop/delete choices from the detected CDC deployment, but that detection was
  only refreshed when the CDC step had been opened — so starting over from another
  step (or a reconnected session) could fall back to a passive "resetting does not
  delete CDC infrastructure" warning with no teardown action. Start over now runs a
  read-only AWS probe when it opens, so it reflects the real deployed state.
- **Teardown is offered for CDC resources in ANY state, not just running ones.** A
  connector that is failed/still provisioning, a stuck or rolled-back cdc-stack, or
  an infrastructure-only stack (the MSK cluster + NAT with no connectors yet) all
  still bill — but were not always offered for teardown. Existence, not health, now
  drives the offer, matching the CDC step (which already exposes Delete for a
  stuck/unstable stack).
- **A custom cdc-stack name is named explicitly in the Start-over warning.** If you
  deployed CDC under a custom stack name (the CDC step's "Advanced — CDC stack
  name", e.g. for a second parallel migration), a fresh session cannot re-discover
  it (it reverts to the default name). The warning now names the exact stack so you
  know precisely what to delete (in the tool or the AWS console).
- **Deleting CDC infrastructure no longer submits a doomed delete against a stack
  that is mid-operation.** If a CloudFormation operation was still running, the
  delete raced it and could fail opaquely. Delete now stops with a clear
  wait-and-retry message when an operation is in flight (and, if a deletion is
  already underway, simply waits for it) — while still deleting stable, failed, and
  rolled-back stacks as before.

## v0.1.32

### Fixed

- **Validation checksums no longer false-mismatch on NULL-bearing rows.** The
  per-row checksum joined columns with a `'\0'` NUL sentinel for NULLs, but that
  byte renders differently on each engine (a single NUL on MySQL vs. the two-char
  string `0x5C30` under PostgreSQL's `standard_conforming_strings`, DSQL's
  default), so any row containing a NULL hashed differently on source and target
  and was reported as a spurious difference. The sentinel is now the plain text
  `<NULL>` (also avoiding NUL, which is invalid in PG text), so identical data
  hashes identically on both engines.
- **Validation checksums now agree on binary and BIT columns.** MySQL rendered
  `BINARY`/`VARBINARY`/`BLOB` (and spatial) as raw bytes while the target side
  used hex, and `BIT` was compared as raw bits vs. an integer — both produced a
  guaranteed cross-engine mismatch even when the stored data was identical. Binary
  columns are now hashed as lower-case hex on both sides (`LOWER(HEX(…))` on MySQL
  to match PG `encode(…, 'hex')`), and `BIT(n)` is compared as its integer value
  (`CAST(… AS UNSIGNED)` vs. `::text`).
- **Out-of-range MySQL `TIME` values now fail loudly instead of corrupting the
  target column.** MySQL `TIME` spans `-838:59:59..838:59:59`, but a DSQL `time`
  column only holds `00:00:00..23:59:59.999999`. A value outside that range had no
  `time` representation and would silently bind as an interval (or a non-time text
  cell), corrupting the column. Full Load now raises a clear `ValueConversionError`
  naming the column and value and pointing to the fix (remap the target type to
  `interval`/`text` in Schema Conversion, or restrict the source values), matching
  the existing `TINYINT(1)`-out-of-range guard — data is never silently mangled.

## v0.1.31

### Fixed

- **Validation is now reachable during a CDC-only run (no more "Complete Data
  Migration first").** The Data Migration step only ever reached DONE via a
  finished Full Load, so a CDC-only plan — or a reconnected session that never ran
  Full Load locally — left Validation permanently locked even though CDC was
  actively replicating to the target. When CDC is streaming, the Data Migration
  step is now treated as DONE for downstream gating (new pure
  `data_migration_step_after_cdc`; only promotes, never downgrades a terminal
  DONE/FAILED).

### Known issues

- **Object browser can still show "everything selected" (locked) for a
  reconnected CDC-only session.** When CDC is live but this session has no Full
  Load watermark and no locally-confirmed table selection (e.g. reconnected after
  starting fresh from Connect), the tool cannot resolve the real streamed table set
  from local state and the locked browser falls back to the target-existing
  default. Fully fixing this needs reading the deployed connector's actual
  table set (`describe_connector`) off the event loop during CDC status discovery —
  tracked as a follow-up. (v0.1.30 already fixed the common case where the
  watermark/selection is known.)

## v0.1.30

### Fixed

- **Data Migration object browser no longer shows "everything selected" while CDC
  is live.** When the picker is locked (CDC streaming), a reconnect fell back to the
  generic "everything on the target" default and ticked every table — misrepresenting
  what CDC is actually replicating (and frozen, so it couldn't be corrected). The
  locked browser now reflects the REAL streamed set (the CDC connectors' table set,
  from the Full Load watermark / confirmed selection) instead of the target-existing
  default.
- **Schema Conversion "Apply to target" is now blocked while CDC is running.**
  Applying schema during live CDC — especially a destructive REPLACE, which DROPs and
  recreates the table — would corrupt or truncate the tables the sink is actively
  writing (Debezium does not propagate DDL), risking data loss / a broken pipeline.
  Both the bulk apply and the per-object inline apply now stop with a warning telling
  the operator to stop CDC first. (Guarded by a CDC-status probe injected from the
  app; when unavailable, apply is unaffected.)

## v0.1.29

### Added / Changed

- **Schema Conversion: one-click copy for the Source and Target DDL.** Each DDL
  block now has a copy-to-clipboard icon — on the side-by-side Source/Target diff
  (per-side, in the header bar) and on the non-editable view/trigger/routine preview
  (next to each "Source DDL" / "Target DDL" label). A positive toast confirms the
  copy; if the browser clipboard is unavailable (e.g. non-HTTPS) it falls back to a
  calm "select and copy from the block" note.

## v0.1.28

### Fixed

- **CDC teardown auto-recovers a `DELETE_FAILED` stack blocked by the offset-seeder
  Lambda's leftover ENIs.** The offset-seeder runs in-VPC (it must — MSK Serverless
  bootstrap is VPC-private, so nothing outside the VPC can produce the gapless seed
  record), and a VPC Lambda leaves AWS-managed hyperplane ENIs behind that AWS
  reclaims only asynchronously (minutes to tens of minutes). While they linger,
  deleting the connector subnets / security group fails and the whole stack lands
  in `DELETE_FAILED` — previously a dead-end that required manually deleting the
  ENIs and re-running delete-stack from the CLI (hit repeatedly this session), while
  MSK/NAT kept billing. `run_cdc_delete` now detects `DELETE_FAILED`, deletes the
  leftover *detached* (`available`) ENIs pinning the failed subnets/SG, and
  re-issues the delete (retaining anything still stuck) so teardown completes.
  In-use ENIs (still being reclaimed) are left alone; best-effort throughout.
  (This is the practical resolution of the offset-seeder ENI known-issue: the
  Lambda cannot move out of the VPC, so the tool now heals the teardown instead.)

## v0.1.27

### Fixed

- **CDC deploy auto-recovers a wedged `UPDATE_ROLLBACK_FAILED` cdc-stack instead of
  dead-ending.** A connector `UpdateConnector` that fails leaves the connector
  not-RUNNING, and CloudFormation's own rollback then also fails on that resource
  ("only valid for RUNNING"), parking the stack in `UPDATE_ROLLBACK_FAILED` — a
  state from which no further update can be submitted (previously it required a
  manual `continue-update-rollback` from the CLI). `discover_stack` now detects
  that state and continues the rollback while skipping the stuck resource(s), so
  the stack returns to `UPDATE_ROLLBACK_COMPLETE` and the next Start/Retry proceeds.
  Best-effort: if the recovery call itself errors, the normal "not a stable state"
  error is surfaced.

## v0.1.26

### Fixed

- **CDC UI: surface the "no tables selected" guard, and stop retries snapping back
  to Prerequisites.** Following the v0.1.25 backend guard, the CDC step now shows a
  clear "select at least one table" notice (instead of the config preview crashing
  or a deploy failing minutes later at connector-create), and Start CDC blocks with
  the same message before submitting a job. The early "provision infrastructure"
  deploy still allows an empty selection (`build_sink_config(..., allow_empty=True)`)
  because it creates no connector yet — `SinkTopics` is filled at Start CDC.
- **CDC sub-step no longer collapses to Prerequisites on a retry / re-render once
  connectors are deployed.** The active-sub-step resolver had nothing persisting
  "cdc", so any re-render (a CDC retry, a reconnect) fell back to
  full_load/prerequisites and yanked the user off the live CDC view. When the plan
  includes CDC and connectors exist, the CDC sub-step is now pinned and persisted.

## v0.1.25

### Fixed

- **CDC start now fails fast when no tables are selected, instead of deploying a
  broken sink.** `build_sink_config` raises if the table list is empty: a Kafka
  Connect sink requires a non-empty topic list, so an empty selection produced
  `SinkTopics=""` and MSK Connect rejected the connector at `POST /connectors`
  with an opaque HTTP 400 minutes into the deploy (see v0.1.24 notes). The guard
  turns that into an early, actionable error ("select at least one table") before
  any slow/billable deploy is attempted. (The *source* config is unchanged — an
  empty `table.include.list` is valid there and means "all tables".)

## v0.1.24

### Fixed

- **CDC connector deploy: complete the CdcDeployRole / task-role IAM so a connector
  actually reaches RUNNING.** Creating an MSK Connect connector exercises a chain of
  permissions that were incrementally missing; each one failed the connector CREATE
  (or left the UI stuck) until added. Verified end to end against a live cdc-stack —
  the Debezium source connector now reaches RUNNING. The additions:
  - `ec2:CreateNetworkInterface` / `DescribeNetworkInterfaces` / `DeleteNetworkInterface`
    on **CdcDeployRole** — MSK Connect places the connector's ENIs using the *caller's*
    credentials (confirmed via CloudTrail: `CreateNetworkInterface` invoked by
    `kafkaconnect.amazonaws.com` but authorized against the deploy role), not the
    connector's ServiceExecutionRole or the MSK Connect service-linked role. (The ENI
    grant mistakenly added to the cdc-stack `ConnectorExecutionRole` was removed —
    the service execution role does not need it.)
  - CloudWatch Logs *delivery* actions (`logs:CreateLogDelivery`, `ListLogDeliveries`,
    `PutResourcePolicy`, …) on CdcDeployRole — the connector enables CloudWatch worker-
    log delivery, set up via the vended-logs delivery API using the deploy role; without
    them the connector went to FAILED with `InvalidInput.WorkerLogsError` and no worker
    logs were ever written.
  - `kafkaconnect:DescribeConnectorOperation` / `ListConnectorOperations` on
    CdcDeployRole, scoped to **both** the `connector/*` and `connector-operation/*`
    ARN shapes — UpdateConnector is asynchronous and its poll is authorized against
    either ARN; a CDC retry rolled the stack back without both.
  - `kafkaconnect:ListConnectors` / `DescribeConnector` on the **task role** itself —
    the app polls connector state to drive the CDC UI (and to advance from the source
    pass to the sink pass). Without it the AccessDenied was silently swallowed and a
    connector that was actually RUNNING showed "creating…" forever.
- **DSQL sink connector reaches RUNNING — the full source→MSK→sink→DSQL pipeline is
  now verified end to end.** The sink had been failing `POST /connectors` with HTTP
  400 once IAM/infra was complete; root cause was an **empty `SinkTopics`** parameter
  (a Kafka Connect sink requires `topics`/`topics.regex`, so a blank value is
  rejected at registration). `SinkTopics` was empty because the two-pass Start never
  populated it (see the UI known-issue below); with it set to
  `<TopicPrefix>.<db>.<table>` the sink connector creates and runs.

### Known issues

- **UI: "Retry CDC" can reset the view to Prerequisites without running the deploy,**
  the source→sink two-pass does not resume after a long stack cleanup, and a Start
  that skips table selection leaves `SinkTopics`/`TableIncludeList` empty (the source
  tolerates it — captures all tables — but the sink then fails `POST /connectors`
  with HTTP 400). A follow-up UX/guardrail pass should block a CDC start when no
  tables are selected and surface the empty-topics condition before deploy rather
  than at connector-create time.
  _Update: the empty-table start is now blocked and the CDC view is kept on retry
  (v0.1.26); the two-pass resume after a long cleanup is the remaining piece._

## v0.1.23

### Added / Changed

- **The "before you start CDC" notice is friendlier and better-timed.** It now
  shows which tables will stream right at the Start button (e.g. "Will stream 3
  tables: …"), so "finalize your selection" is verifiable at a glance instead of
  asking the user to scroll up. The MSK-capacity caution is a calm info tip on the
  first start after a fresh deploy (the happy path — no alarm), and only escalates
  to a warning once connectors have actually existed before (a prior start/stop or
  a restored run), which is when repeated create/delete really begins consuming
  MSK's non-reclaimed capacity. Wording is plain-language ("MSK's limited capacity
  that isn't freed up again") instead of "partition quota … exhaust … force a full
  teardown".

## v0.1.22

### Fixed

- **CDC connector deploy no longer fails with "Access denied for operation
  'AWS::KafkaConnect::Connector'".** `kafkaconnect:CreateConnector` has no
  resource-level support (the connector ARN doesn't exist at create time), but the
  CdcDeployRole scoped it to a `connector/mysql-dsql-cdc-*` ARN, so the
  DebeziumSourceConnector CREATE was denied. It (plus create-time `TagResource`) is
  now granted on `Resource: "*"`, like the sibling CreateCustomPlugin /
  CreateWorkerConfiguration; the other connector operations stay scoped.
- **CDC connector deploy no longer fails with "not authorized to perform
  ec2:CreateNetworkInterface".** MSK Connect assumes the connector's
  ServiceExecutionRole to place the connector's ENIs in the connector subnets, but
  that role (`ConnectorExecutionRole` in cdc-stack) lacked the EC2 network-interface
  permissions. Added the MSK-Connect `EC2NetworkAccess` set
  (`ec2:CreateNetworkInterface` / `DescribeNetworkInterfaces` / `DeleteNetworkInterface`
  + attach/detach/permission, `Resource: "*"`), so the connector can create/clean up
  its ENIs. (These two were latent — earlier CDC failures stopped before the connector
  CREATE stage, so the connector had never actually been created before.)

### Added / Changed

- **After a Full-load-only run completes, the Full Load step now suggests CDC.** A
  Full-load-only migration has no CDC phase (no "Continue to CDC" button), so when
  it finishes an info notice explains how to add continuous replication: change the
  migration type to "CDC only" (streams from this Full Load's watermark onto the
  already-loaded target, no re-snapshot), noting the CDC infrastructure may need
  deploying first.

## v0.1.21

### Added / Changed

- **Migration Plan now asks a single "Include CDC?" question instead of the full
  three-way migration-type tiles.** The step's only durable effect is whether CDC
  streaming infrastructure (MSK, ~15-20 min) is provisioned early, so it asks
  exactly that (Yes / No) rather than overstating the commitment — the type is
  freely changeable on Data Migration, and Full Load always runs. No →
  `FULL_LOAD_ONLY`, Yes → `FULL_LOAD_AND_CDC`; the finer Full Load + CDC vs
  CDC-only choice stays on the Data Migration step (re-selecting Yes no longer
  clobbers a CDC-only choice). The underlying `migration_type` enum, sub-steps,
  prerequisites, and session snapshots are unchanged.
- **The "Migration type:" banner is hidden on the Migration Plan step** (still
  shown on every later step for continuity). On the plan step the "Include CDC?"
  control is the source of truth, so a three-value banner ("Full load + CDC")
  above the two-value decision was redundant and read as conflicting.

## v0.1.20

### Fixed

- **Aurora DSQL connection no longer times out on an IPv4-only Fargate task.** The
  DSQL cluster endpoint is dual-stack (DNS returns both an A and an AAAA record),
  but a Fargate task on an IPv4-only subnet/ENI (no IPv6 CIDR, no IPv6 SG egress)
  has no route to the IPv6 address. glibc could return the AAAA first, so the
  driver (psycopg/libpq) blocked on the unreachable IPv6 until `connect_timeout`,
  surfacing in the UI as "Connection failed: connection timeout expired" even
  though IPv4:5432 was reachable. The container image now prefers IPv4 for all
  outbound name resolution (`/etc/gai.conf`: `precedence ::ffff:0:0/96 100`), so
  `getaddrinfo` returns the reachable IPv4 address first and the connection
  succeeds. Harmless on a genuine dual-stack task (IPv4 is simply tried first).
- **CDC source-secret re-provisioning no longer fails with AccessDenied after a
  teardown.** The task role's `provision-cdc-source-secret` policy was missing
  `secretsmanager:RestoreSecret`, but the upsert restores a same-named secret that a
  prior teardown scheduled for deletion (recovery window) before writing the new
  value. Re-provisioning the CDC source secret after a delete now succeeds; the
  action stays scoped to the `mysql-dsql-migrator/cdc/*` prefix.

### Added / Changed

- **Deploy guide + stack-details form clarifications.** "Specify stack details"
  now leads with a required-fields table and a one-line self-signed certificate
  command; the desktop-browser access combo (`AlbScheme=internet-facing` + public
  `AlbSubnetIds` + `AllowedIngressCidr=<your-ip>/32`) is called out; and
  `HttpsEgressCidr` is documented as "keep the `0.0.0.0/0` default" (tighten only
  with PrivateLink). `ServiceSubnetIds` guidance notes you may reuse the ALB
  subnets + `AssignPublicIp=ENABLED` when the VPC has no private/NAT subnets.

## v0.1.19

### Fixed

- **Validation no longer shows a completed run as "in progress" (then "not
  started" on refresh).** The IN_PROGRESS→DONE flip is driven by a poll timer that
  only runs on the Validation screen, so navigating away mid-run (e.g. to Data
  Migration) left the step stuck IN_PROGRESS after the job finished, and the
  orphaned-status reconcile then discarded the completed report as "not started".
  Now, when a run actually finished (a report exists) but the step is a stale
  IN_PROGRESS with no live job, it reconciles to **DONE** and shows the report.

### Added / Changed

- **CDC lifecycle + connector state-transition activity logging.** Control-plane
  actions (deploy / start / stop / delete CDC infrastructure) and connector
  RUNNING/FAILED transitions are now appended to the activity log as discrete
  milestones (de-duplicated; continuous lag/throughput stays in the live panel, not
  the log).
- **Cut over: the "Steps to cut over" 1–6 runbook is larger and easier to read**
  (the critical guidance was too small) — scoped to the cut-over runbook only.
- **Deploy guide: a complete teardown order.** The Teardown section now lists the
  full decommission sequence — remove the costly **cdc-stack** first (via "Start
  over → Delete all CDC infrastructure", or a manual `delete-stack`), then the
  app-stack, then the build-stack, and verify no `mysql-dsql-*` stacks / Route 53
  records / build bucket remain — so no resources or cost are left behind.

## v0.1.18

### Fixed

- **A Full Load re-run now drops + recreates the confirmed tables before CDC has
  started, even in the "Full load + CDC" pattern.** The DROP+recreate was disabled
  whenever the pattern was Full-load-+-CDC (so a "Re-run all tables" before CDC
  started merged idempotently instead of reloading fresh, leaving prior rows as
  "already there"). The suppression is now gated on CDC **actually streaming**: a
  re-run before CDC starts drops + recreates the confirmed tables (clean reload),
  and only an actively-streaming CDC pipeline forces the safe idempotent
  `SKIP_EXISTING` load (no DROP) to avoid racing the live sink. The Start-Full-Load
  confirmation only shows the "will be DROPPED" warning when the drop will actually
  happen (CDC not live). (Re-loading without a DROP never duplicates rows — it is
  `INSERT ... ON CONFLICT (PK) DO NOTHING` — but it could leave rows deleted from
  the source; a clean reload removes that ambiguity.)

## v0.1.17

### Fixed

- **The "Start / Re-run Full Load" confirmation dialog no longer vanishes after a
  few seconds.** It was built inside the periodically re-rendered content and
  opened via a one-shot flag, so the ~1.5 s progress-poll re-render tore it down
  right after it appeared. It is now created and opened in the top-level client
  context on demand, so it stays up until you Confirm or Cancel.

## v0.1.16

### Fixed

- **A Full Load re-run no longer reverts a customized target schema.** The
  per-object **edited target DDL** (e.g. a `TINYINT(1)` → `smallint` remap) is now
  persisted in the durable session snapshot and restored on reconnect/restart.
  Previously the edit lived only in memory, so after a restart a "Re-run all
  tables" recreated the table from the deterministic conversion (e.g. reverting
  `smallint` back to `boolean`) and the out-of-range value failed to load again.
  The re-run's DROP+recreate now uses the customized DDL.

> Note: restoration matches by session id, so set `DSQL_MIGRATOR_STORAGE_SECRET`
> to keep the session (and its edits) stable across restarts. A container
> redeploy uses fresh ephemeral storage, so re-apply the edit after one.

## v0.1.15

### Fixed

- **Schema Conversion: "Apply to target" now reliably shows its REPLACE confirmation.**
  The confirmation dialog was built inside the per-object editor's (nested) slot, so
  it often never rendered as a page overlay — the button looked unresponsive. It is
  now created in the top-level client context and always appears.
- **Schema Conversion: a slow apply no longer crashes with "parent slot deleted".**
  Post-await UI feedback (notify / refresh) now re-enters the originating client and
  is best-effort, so a slot torn down during a slow apply can't raise.
- **The UI version (top-right) now reflects the real released version.** `__version__`
  is read from the installed package metadata instead of a hardcoded value, so each
  built image shows its true version.

### Added / Changed

- **Schema Conversion & Data Migration: Select all / Unselect all** in both object
  browsers for fast bulk selection.
- **Schema Conversion: "Generate DDL for selected" locks after generating** and
  re-enables after "Reset all", so a regeneration is always obvious (a second click
  no longer silently re-runs the same scope).
- **Data Migration: clearer pre-selection caption** — states how many tables are
  pre-selected and why (already present on the target), with the Select all/Unselect
  all controls.
- **Quarantined rows are reframed, not treated as a table failure.** A table that
  loaded but had to permanently drop a row a hard DSQL limit rejects (e.g. a value
  over the ~1 MiB per-value limit) is shown as "Done — quarantined" (amber), separate
  from real, retryable failures (red).
- **Per-table Reload.** Re-run Full Load for exactly one table (even a DONE one) —
  e.g. after fixing an oversized source value so a previously-quarantined row loads —
  keeping the other tables as-is.
- **Accept quarantined rows & continue (CDC override).** When a Full Load is
  incomplete ONLY because of permanently-quarantined rows, you can acknowledge the gap
  and unblock CDC without re-running; the gap is still reported in Validation. A
  retryable real failure still blocks (the override can never mask a recoverable
  failure).

## v0.1.14

### Fixed

- **Schema Conversion: an edit now reliably applies via REPLACE (it was sometimes
  still skipped).** v0.1.13 gated the auto-REPLACE on a UI-side existence check that
  could be stale or unavailable, so an edited object could still come back
  "SKIPPED — already existed; left unchanged". Applying an edited object now always
  routes through the REPLACE confirmation (REPLACE's `DROP ... IF EXISTS` safely
  handles an object that does not exist yet), so the edit lands once confirmed.
- **Schema Conversion: applying no longer collapses the open Generated-DDL panels.**
  The post-apply re-render now preserves each expansion's open/closed state per
  object instead of folding everything.

### Notes

- UI fix; ships in the `:0.1.14` image.

## v0.1.13

### Changed

- **Schema Conversion: applying an EDITED object that already exists now uses
  REPLACE (with confirmation) instead of silently skipping.** Previously, after
  editing a converted DDL (e.g. remapping a column's type) and clicking "Apply to
  target" in the default SKIP mode, an already-existing target object was left
  untouched -- the edit silently did not take effect, and the only feedback was a
  brief SKIPPED toast (it looked like "nothing happened"). The per-object Apply now
  detects an edit to an existing object and routes it through the REPLACE
  confirmation dialog ("DROP and recreate …"), so the change actually lands once
  confirmed. A non-edited existing object is still skipped (idempotent); an edited
  object that does not yet exist is created normally.

### Notes

- UI/behavior change; ships in the `:0.1.13` image.

## v0.1.12

### Changed

- **DSQL-unsupported source columns are now PRESERVED as `bytea` -- never blocked
  or silently NULLed -- across BOTH Full Load and CDC.** A table with a MySQL
  spatial column (geometry/point/…) previously failed Schema Conversion entirely
  (an UNSUPPORTED, read-only comment placeholder). Now:
  - **Schema Conversion** maps the spatial column to `bytea` and produces a real,
    editable `CREATE TABLE` (classified MANUAL with a "preserved as raw bytes
    (WKB)" note). You can still edit it to `text` (WKT), drop the column, or keep
    `bytea`.
  - **Full Load** reads the column via `ST_AsBinary(col)` -> WKB bytes -> `bytea`.
  - **CDC**: the custom DSQL sink converts Debezium's geometry logical type
    (`io.debezium.data.geometry.Geometry`/`Geography`/`Point`) to its WKB bytes ->
    `bytea` -- the **same bytes** Full Load writes (FL/CDC parity; SRID dropped on
    both paths, plain WKB). An unexpected shape is bound as-is so it fails loudly
    to the DLQ -- it is never silently NULLed.
  - The shared write contract (`converter.DSQL_WRITE_CONTRACT_CASES`) records
    geometry -> `bytea` so the Full Load (Python) and CDC (Java) write paths stay
    in lockstep.

### Notes

- The DSQL sink connector plugin must be rebuilt/republished for the CDC geometry
  handling to take effect on a live pipeline; it ships with the next image + plugin
  build.

## v0.1.11

### Changed

- **Full Load value conversion now follows the applied target schema.** The value
  converter previously re-derived each column's target type from the *source*
  MySQL type, so a column remapped in Schema Conversion (e.g. `TINYINT(1)` ->
  `smallint` instead of `boolean`) was ignored and a non-0/1 value failed the whole
  table. Full Load now converts each value to match the *applied* target type
  (parsed from the converted/edited DDL), so a remapped `smallint`/`integer` column
  loads non-0/1 values as integers; a genuine boolean column is unaffected.
- **A fresh/replace re-load preserves a custom-remapped target schema.** The
  fresh-load recreate step now DROPs+recreates from the applied (edited) DDL rather
  than a deterministic re-derivation, so a user remap is not silently clobbered on a
  full re-load.

### Fixed

- The boolean value-conversion conflict message now guides the user to remap the
  column's target type to `smallint`/`integer` in Schema Conversion (now effective)
  and retry the table, instead of only suggesting a source-side change.

### Notes

- No new container image is published yet (batched with v0.1.10). Locally, restart
  the UI to pick it up; on ECS it ships with the next image build.

## v0.1.10

### Fixed

- **Schema Conversion preview: an object that cannot be auto-converted is labeled
  "Unsupported" and shows no "Apply to target" button.** A table with a specific
  placeholder (e.g. MySQL spatial types) was previously shown as just
  "N warning(s)", stayed editable, and offered an Apply button (which would
  no-op / SKIP). The preview now (1) surfaces the conversion severity
  ("Unsupported" / "Review needed") in the object header, and (2) treats **any**
  non-`CREATE` placeholder -- not only the generic not-converted note -- as
  not-auto-converted: shown read-only with the redesign reason and the
  AI-suggestion option, and never offered for apply. Complements v0.1.9, which
  already SKIPs such objects on the apply path.

## v0.1.9

### Fixed

- **Schema Conversion: a table that cannot be auto-converted is now SKIPPED, not
  FAILED.** Applying a table the converter could not auto-convert -- e.g. one with
  MySQL spatial/geometry columns, which Aurora DSQL has no type for -- produced a
  confusing `SchemaApplyError: target DDL must be a CREATE TABLE/VIEW/MATERIALIZED
  VIEW/INDEX statement`, because the converter emits a comment placeholder (not a
  `CREATE`) for it. Such a table is now reported **SKIPPED** with the redesign
  reason (matching its assessment) and is never sent to the applier; the other
  selected tables apply normally.

## v0.1.8

### Fixed

- **CDC offset-seeder (gapless Full Load -> CDC handoff) can now deploy.** When CDC
  is deployed with a Full Load watermark (`SeedOffset`), the cdc-stack creates an
  in-VPC offset-seeder Lambda plus its own IAM role, invoked by a custom resource.
  The assumed `CdcDeployRole` lacked the permissions to do this, so the deploy would
  fail with `AccessDenied` and roll back. Added to `CdcDeployRole`:
  - `lambda:*` lifecycle (`CreateFunction`/`DeleteFunction`/`InvokeFunction`/…) on
    `function:mysql-dsql-cdc-*`;
  - broadened the IAM role-management scope from `*-ConnectorExecutionRole-*` to
    `role/mysql-dsql-cdc-*` so it also covers the auto-named offset-seeder role;
  - `iam:PassRole` to `lambda.amazonaws.com` (in addition to MSK Connect).
- **CDC infrastructure: MSK Serverless cluster creation.** Creating the MSK
  Serverless cluster validates the VPC under the caller's credentials, so the
  assumed `CdcDeployRole` also needs `ec2:DescribeVpcAttribute` (and
  `ec2:DescribeAvailabilityZones`); without them the deploy failed with `You are
  not authorized to perform DescribeVpcAttribute` and rolled back.
- **CDC infrastructure: connector role creation + rollback cleanup.**
  `logs:DescribeLogGroups` (which CloudFormation calls to resolve a LogGroup `Arn`
  for `!GetAtt`) has no resource-level support, so it is now its own statement scoped
  to the account/region log groups rather than pinned to the connector log group;
  and the MSK Serverless cluster delete requires `kafka:DeleteCluster` (there is no
  `DeleteClusterV2`) -- without it rollback/teardown left the cluster orphaned.
- **Removed dead Glue Schema Registry permissions** from the deploy role: the
  pipeline uses the built-in JSON converter (since v0.1.5) and creates no Glue
  registry, so the `glue:*` grants were unused.

### Notes

- Deploy-template only (app-stack IAM); **no container image change** — the published
  `:0.1.7` image is unchanged and remains the default.

## v0.1.7

### Fixed

- **CDC infrastructure now deploys (cdc-stack).** Deploying the cdc-stack via the
  assumed `CdcDeployRole` failed and rolled back due to missing IAM permissions and
  a template bug. Fixed:
  - `CdcDeployRole` IAM: stage the oversize template in the plugin bucket
    (`s3:PutObject`/`GetObject`); MSK Connect plugin + worker-configuration tag
    permissions (`kafkaconnect:TagResource`/`ListTagsForResource`/`UntagResource`)
    with `Resource: "*"` for the create actions (which have no resource-level
    support); and VPC endpoint permissions (`ec2:CreateVpcEndpoint`, …).
  - `cdc-stack.yaml`: removed an invalid `!GetAtt ConnectorS3Endpoint.PrefixListId`
    (`AWS::EC2::VPCEndpoint` has no such attribute), and shortened a security-group
    rule description to satisfy EC2's <256-char / restricted-charset rule.

### Changed

- Default `ContainerImageUri` -> the published `:0.1.7` image.

> Note: the CDC **infrastructure** path is validated end-to-end; connector start
> ("Start CDC") and the offset-seeder (watermark/gapless handoff) paths are being
> hardened separately.

## v0.1.6

### Fixed

- **CDC infrastructure deploy works on the published image.** The cdc-stack
  CloudFormation template (`deploy/cdc-stack/cdc-stack.yaml`), which "Deploy CDC
  infrastructure" reads at runtime, was not bundled in the container image (the
  Dockerfile did not copy it and `.dockerignore` excluded `deploy/`), so a clean
  image failed with "Could not read the cdc-stack template". The template is now
  bundled in the image.

### Changed

- Default `ContainerImageUri` bumped to the published `:0.1.6` image (so a fresh
  deploy includes the CDC-template fix).

## v0.1.5

### Changed

- **CDC deploy cost estimate is shown per hour, not per month**, matching the
  tool's temporary (cut-over duration) use of the CDC pipeline. **Glue is removed**
  from the listed cost drivers — the pipeline does not use Glue.

## v0.1.4

### Fixed

- **Schema Conversion no longer blanks on unsupported spatial types.** A table
  using a MySQL spatial type (`POINT`, `LINESTRING`, `POLYGON`, …) previously
  raised a `sqlglot` `ParseError` that aborted the entire Schema Conversion step.
  The failure is now isolated per table: the affected table is classified
  `UNSUPPORTED` with a clear reason (naming the spatial column) and the remaining
  tables still convert.
- **"Deploy CDC infrastructure" button on the Migration plan step now works.** The
  click was a silent no-op because the async confirm-dialog/deploy handlers were
  invoked without `await` (the coroutine was never awaited). The handlers are now
  awaited, so the confirmation dialog opens and the deploy starts.

### Changed

- **app-stack networking guardrail.** `AllowedIngressCidr` guidance is clarified
  (internet-facing ALB → set your own public IP as `x.x.x.x/32`), and a new
  `SourceReachabilityRequired` rule requires at least one of
  `SourceDbSecurityGroupId` / `SourceDbCidr` so the task always has egress to the
  source DB (prevents a silent "can't connect to source" after deploy).
- **AI assist model selection.** `BedrockModelId` is now a curated Anthropic
  dropdown, and the task role's `bedrock:InvokeModel` scope is auto-derived from
  the chosen model. `BedrockModelArns` becomes an optional override.
- **`CertificateArn` test path documented.** The deployment guide (EN/KO) was
  tidied: clearer optional sections, and the public-IP / test-cert prerequisites
  are surfaced up front.

## v0.1.3

- Prior published baseline (ECR Public image `:0.1.3`).
