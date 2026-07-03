# Changelog

_Language: **English** | [한국어](CHANGELOG.ko.md) | [日本語](CHANGELOG.ja.md)_

All notable changes to this project are recorded here. This project follows
[semantic versioning](https://semver.org/) (patch releases for bug fixes).

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
