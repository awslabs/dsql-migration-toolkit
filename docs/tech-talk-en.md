---
marp: true
theme: default
paginate: true
title: MySQL to DSQL Migrator — Migration Architecture & Data Path Deep Dive
class: dense
style: |
  /* dense applied to every slide via frontmatter class: dense. To exempt one slide, put <!-- _class: other --> at its top. */
  section.dense { font-size: 21px; }
  section.dense h1 { font-size: 30px; }
  section.dense h2 { font-size: 22px; }
  section.dense table { font-size: 19px; }
  section.dense pre { font-size: 16px; }
  section.dense li { line-height: 1.3; }
---

<!--
Talk deck (for DB experts, English). Render with Marp / reveal-md, or just read as Markdown.
Slides split on a horizontal rule (three hyphens); speaker notes are HTML comment blocks.
Timing (20-min talk + 5-min demo): intro 2 · architecture 4 · Evaluation/Schema 3 · Full Load 4 · CDC 5 · Validation/AI 2 · closing (hot partition) 1 (+buffer).
-->

<style scoped>
section h1 { font-size: 60px; }
section h2 { font-size: 34px; }
</style>

# MySQL to DSQL Migrator
## Migration Architecture & Data Path Deep Dive

Speaker: dalyoung@ · 2026-07-08

Gitlab - https://gitlab.aws.dev/dalyoung/mysql-dsql-migration-tool-public

<!--
- (verbal) Internal tech share · 20-min talk + 5-min demo · audience: DB experts.
- This tool is a web app that migrates RDS/Aurora MySQL to Aurora DSQL (PostgreSQL-16 compatible, distributed).
- Today is about "how it works," not "what it does" — especially the internals of the architecture, Full Load, and CDC.
- DSQL is distributed, so it intentionally omits features that don't scale horizontally (FKs, triggers, synchronous indexes) → this is a "heterogeneous migration," not an "upgrade."
-->

---

# Three lenses for today

1. **This is a heterogeneous migration** — not an upgrade
   - MySQL → PostgreSQL dialect → DSQL constraints, a 2-hop conversion

2. **Two data paths converge on DSQL**
   - Full Load (one-time bulk) + optional CDC (continuous streaming)
   - **Full Load is NOT a Debezium snapshot** — it's the tool's own Python bulk loader

3. **The tool is a control plane**
   - It only configures, bulk-loads, watermarks, and monitors
   - Data-integrity principle: **loud failure over silent loss**

<!--
- These three lenses run through the whole talk. Especially remember (2) "Full Load ≠ Debezium snapshot" and (3) "loud fail over silent loss."
- The goal is NOT fully-automated zero-downtime: assess feasibility → automate what's deterministic → clearly surface where human work is required.
-->

---

# Why this tool — MySQL ≠ Aurora DSQL

| | **RDS/Aurora MySQL (source)** | **Aurora DSQL (target)** |
|---|---|---|
| Engine family | MySQL | **PostgreSQL(-16) dialect/compatible** → heterogeneous |
| Architecture | Single-node storage (heap) | **Distributed · storage partitioned by PK** |
| Foreign keys | Supported | **None** (enforce in app layer) |
| Triggers · stored procs | Supported | **None** |
| Index creation | Synchronous | **`CREATE INDEX ASYNC`** (backfilled after load) |
| Transactions | Large txns OK | **≤3000 rows · 1 DDL · 1 MiB/value · ≤5 min** |
| Concurrency control | Lock-based | **Optimistic (OCC) — retry on 40001** |
| Auth | Password | **Short-lived IAM token (~15 min)** |
| PK | Optional | **Required** (AUTO_INCREMENT causes hot partitions) |

> A plain dump/restore or a stock JDBC loader can't clear these constraints → you need **a tool that understands DSQL**

<!--
- This slide answers "why this tool." Each row is the rationale for a later slide:
  distributed + PK partitioning → hot partition / PK strategy, ≤3000 rows → batched loader, OCC 40001 → statement-level retry, IAM token → custom sink, no FK → preserve-in-report.
- One-liner: mysqldump or a generic tool's "full load" does not handle DSQL's batch limit, OCC, IAM token, or type differences.
-->

---

# Architecture (at a glance) — two data paths converge on DSQL

![w:1000](../deploy/architecture-aws-simple.png)

- **Migration Tool** (ECS Fargate · web UI) reads the source (**convert + bulk load**) and writes to DSQL = Full Load
- **CDC pipeline** (dashed box) runs on managed MSK Connect: Debezium → MSK → **custom DSQL sink**
- **Offset-seeder Lambda** bridges Full Load → CDC gaplessly

<!--
- Big picture first. Top path = Full Load (convert + bulk load), bottom box = optional CDC.
- Emphasize the "CDC pipeline runs on managed MSK Connect — no servers owned" label: we don't operate compute.
- Next slide zooms into the full production architecture (networking/IAM/security).
-->

---

# Architecture (full) — app-stack + cdc-stack

![w:1080](../deploy/architecture-aws.png)

- **app-stack** (always): ALB (optional Cognito) · ECS Fargate (ECR Public) · Secrets Manager · (optional) Bedrock
- **cdc-stack** (optional, VPC-private): MSK + MSK Connect (Debezium source + custom sink) · Offset-seeder Lambda · S3 Gateway VPC endpoint
- **Control plane vs data plane**: the tool only configures/bulk-loads/watermarks/monitors; the sink runs on managed MSK Connect

<!--
- Why single task (desiredCount=1)? It's a control plane, no state to shard (no-bloat). Rolling image replacement means brief downtime → managed via the next state-layers slide.
- Source MySQL is customer-owned, outside both stacks. The browser only renders the UI (not on the data path).
- Security: Cognito is off by default; the template forcibly blocks the "internet-exposed (0.0.0.0/0)" combo. Least-privilege IAM (task/execution role split).
- If short on time, cover this slide in two lines: "left = app-stack always, right VPC box = cdc-stack optional."
-->

---

# State management — where each layer lives and its lifetime

| Layer | Storage location | On task replacement |
|---|---|---|
| **① Credentials** | Per-session, **in process memory only** | Gone (never persisted anyway) |
| **② Workbench/job state** | Local SQLite (`/tmp`, ephemeral) | Gone → re-run after reconnect |
| **③ Migrated data · schema** | **DSQL itself** | Preserved (unaffected) |

- **Property 7**: credentials are **never** written to disk, logs, reports, or job state. Discarded when the session ends.
- Rolling task replacement → ①② vanish, ③ survives → **auto-recovers on reconnect via a read-only re-run of Evaluation**.
- The session-cookie signing secret is auto-created by the stack (no operator input; not a DB credential).

<!--
- Appeal point for DB experts: "credentials never touch disk" is an enforced rule (code-review gate).
- Since ③ is DSQL, data/schema stay safe even if the task dies. On reconnect, the tool introspects the target to restore state.
-->

---

# Deployment model — deploy with minimal setup

- Image published to **ECR Public** → no build needed
- Connector plugin artifacts are **committed** → no Java/Maven toolchain needed
- The tool **creates its own S3 bucket** → uploads artifacts itself
- CDC infra is **auto-discovered** → you only enter what can't be inferred (VpcId, etc.)
- Region is **derived from the DSQL endpoint** (`…dsql.ap-northeast-2.on.aws` → `ap-northeast-2`)

**Least-privilege IAM split**
- task role: `dsql:DbConnect(+Admin)`, read-only `GetCluster`, source-secret-scoped `secretsmanager:GetSecretValue`
- `bedrock:InvokeModel` **only when AI is enabled** (scoped to allowed model ARNs)

<!--
- Deployment convenience is the top design principle. Goal: "fresh clone + minimal setup."
- Cross-region migration is unsupported — the CDC data plane must reach the source privately from inside the DSQL region's VPC. Single-region assumption.
-->

---

# 6-step migration workflow

```
Connect → 1.Migration plan → 2.Evaluation → 3.Schema Conversion
        → 4.Data Migration → 5.Validation → 6.Cut over
```

- Each step has independent status (not started / in progress / done / failed), runnable and re-runnable on its own
- **Migration plan**'s only lasting effect: whether to pre-provision CDC infra
  - The exact mode (Full+CDC vs CDC only) is chosen later in Data Migration, and is **reversible**
- **Cut over** is a human action → the tool provides only a runbook (no Run action)

<!--
- A guided flow, not a forced wizard. If a prior step is incomplete, the UI only advises.
- Today's deep dive covers 2·3 (Evaluation/Schema), 4 (Full Load/CDC), and 5 (Validation).
-->

---

# Steps 2·3: Evaluation & Schema Conversion
## Learn what DSQL will reject — before moving any data

**Per-object 3-way classification** — every source object is judged AUTO / MANUAL / UNSUPPORTED
- No rule matches → default AUTO. Multiple rules match → **take the strictest grade**: UNSUPPORTED > MANUAL > AUTO
- But **every matched rule's reason and recommendation is recorded together** in the report, so no finding is buried

**Rule-based 2-hop conversion** (`sqlglot`)
```
MySQL DDL → [sqlglot: MySQL→PostgreSQL dialect] → [DSQL constraint layer] → DSQL DDL
                                                   drop FK · index→ASYNC
                                                   split DDL per txn · type mapping
```
- The deterministic conversion **always runs first**; AI (Bedrock) only augments MANUAL/UNSUPPORTED (applied after review + approval)

<!--
- Core message: a "deterministic gate" between connecting and moving data — predictable conversion, not trial and error.
- For an Aurora MySQL user, this step is where you learn "what DSQL won't accept" — before a single row moves, read-only and cheap.
- AI does not replace the deterministic path, only augments it (off by default), and must pass review-only + an explicit approval gate to reach the target.
-->

---

# Key Schema Conversion decisions

| Item | Handling |
|---|---|
| **Type mapping** | TINYINT(1)→boolean, BIT(n)→int, ENUM/SET→text+CHECK, BLOB→bytea, DATETIME→timestamp |
| **Foreign keys** | DSQL has no FK → removed from DDL but **preserved in the report** + "enforce in app layer" advice |
| **Secondary indexes** | `CREATE INDEX ASYNC` (async backfill after load) — FULLTEXT/SPATIAL are UNSUPPORTED |
| **DDL apply** | **1 DDL per txn**, safe retry on 40001/OC001 (idempotent) |
| **PK strategy** | keep integer / UUID / cached identity / **composite PK (new)** |

- **Loss transparency**: even an unparseable view is not silently dropped — it becomes a **placeholder + MANUAL flag**
- **fail-loud**: a TINYINT(1) value outside `{0,1}` is not silently coerced to true — it aborts that table's Full Load

<!--
- FK "preserve-in-report" is the emblem of loss transparency: it's surfaced, not vanished.
- The reason for PK strategy (hot partition) is detailed in the Full Load deep dive; here it's just "you have choices."
- DSQL hard limits: ≤255 columns/table, ≤1000 tables/DB, 1 DB/cluster, DECIMAL ≤38, 1 MiB/value.
-->

---

# Step 4A: Inside the Full Load engine (1/2)
## Not a Debezium snapshot — a dedicated Python bulk loader

**Read: PK keyset pagination (not OFFSET)**
```sql
SELECT <cols> FROM <table>
WHERE pk > :last ORDER BY pk LIMIT 1000        -- composite PK uses row-value tuple comparison
-- START TRANSACTION WITH CONSISTENT SNAPSHOT (InnoDB REPEATABLE READ), server-side cursor
```
- OFFSET re-scans the head every page → O(n²) on the source. keyset is an index seek, only ~1000 rows in flight per page
- **Memory is bounded to one page regardless of table size** — the whole table is never held in RAM
- Single consistent snapshot → safe even as the live source changes. **PK required** (else UNSUPPORTED)

**Write: batched `INSERT ... ON CONFLICT`**
- batch size = min(≤2000 rows / **≤3000 hard cap**, params ≤65535, bytes ≤8 MiB)
- idempotent (ON CONFLICT) → loading the same batch repeatedly causes no dupes. With CDC concurrent, "skip existing"

<!--
- Why a dedicated loader: a generic tool's full load is internally JDBC INSERTs with no DSQL-specific OCC handling.
- Inside the consistent snapshot, the watermark is captured in the same transaction → the snapshot point and binlog coordinates align exactly (the basis for the later CDC handoff).
- Caveat: the REPEATABLE READ snapshot stays open for the whole table read, so on a write-heavy source it can block InnoDB undo purge (History List Length).
-->

---

# Step 4A: Inside the Full Load engine (2/2)
## Resumability · parallelism · failure isolation

**Deterministic resume**: rows stream in keyset (PK) order → **batch i = always the same PK range**
→ a batch is a stable resume unit. Stop/retry re-runs **only the unfinished ranges** (no dupes)

**Parallelism model (v0.1.68+): multi-process — not threads**
- `table_parallelism` (worker processes, default 4 · match to vCPU) × `batch_parallelism` (DSQL connections inside a process, default 8)
- **`ProcessPoolExecutor`**: each table (or shard) runs in **its own OS process = its own GIL and CPU core**
- **Large single-integer-PK tables → auto-split into PK-range shards**, scheduled alongside whole-table workers in one pool
- concurrent DSQL connections ≈ table_par × batch_par (well within the 10,000-conn / 100-new-per-sec cluster limits)

**OCC retry is per-statement** (not the whole batch)
- 40001 (OC000 data / OC001 schema) → retry **only the conflicting `INSERT`** with backoff+jitter, up to 10×
- Resubmitting the whole batch re-pays for the 99% non-conflicting rows + a wide key range = livelock risk

**Two failure-isolation paths** (intentionally different)
- **Row quarantine**: DSQL rejects a row via SQLSTATE (>1 MiB / constraint) → binary-split the batch to isolate just that row (**PK + reason only, values never logged**), load the rest, mark the run failed
- **Table-fatal**: lossless conversion impossible (e.g. TINYINT(1)=2) → `ValueConversionError` (no SQLSTATE) → loudly abort that table's load

<!--
- Per-statement retry is this tool's signature. The custom CDC sink mirrors it later.
- Row-quarantine vs table-fatal: the former is DSQL rejecting (has SQLSTATE), the latter happens during read/convert before asking DSQL (no SQLSTATE). Neither is passed over silently.
- Indexes are created last, after load, as CREATE INDEX ASYNC (building during load makes every INSERT pay index-maintenance cost).
-->

---

# Full Load performance — the GIL wall and breaking it (measured)

**Finding #1: CPU-bound, not network-bound**
- The source reader converts MySQL→DSQL types per row in **pure Python (holds the GIL)**
- ThreadPool era: CPU pinned at **~110% (1 core) on any vCPU** = the GIL signature. Reader sharding (threads) also **~0%**

**Breakthrough #2: multi-process (v0.1.68) — `ThreadPool → ProcessPoolExecutor`**
- Each table/shard gets **its own process = its own GIL and core**. Large integer-PK tables **auto-split into PK-range shards**

| Approach (8 vCPU Fargate) | rows/s | CPU | 200GB estimate |
|---|---|---|---|
| ThreadPool (v0.1.67, prior) | 12,277 | 110% | ~12 h |
| ProcessPool, 4 tables mixed, tp=8 | **34,800** | 561% | ~5 h |
| ProcessPool, single large table sharded, tp=8 | **51,000** | 777% | **~2.5 h (18×)** |

→ **Optimal setting: `table_parallelism = vCPU count`** (the loader auto-shards large tables)
→ Around tp=8 the bottleneck moves from **CPU → DSQL server write capacity** (~67K rows/s peak)

<!--
- This is the biggest update since the last tech-talk. It used to end at "the GIL is the wall, just add CPU" — now multi-process breaks that wall.
- Core narrative: threads can't use the vCPUs because of the GIL → each process has its own GIL so cores are actually used → 18× (200GB 46h→2.5h).
- Uses the spawn context; each worker builds its own MySQL engine + DSQL connection pool (no inter-process row transfer). Test doubles auto-use the thread fallback (backward-compatible).
- Replace path: for an empty table, plain INSERT (no ON CONFLICT) removes OCC contention → 41K–51K sustained, 67K peak.
-->

---

# Parallelism is a throttle, not a throughput dial — the OCC guardrail

**Once multi-process clears the CPU wall, the next wall is OCC (server write contention)**

**Parallelism guardrail (measured)**: doubling to 128 connections (32→128) → only **+5%** throughput,
while the retried-batch rate rose **9.6%→12.8%** (a monotonic PK piles writes into the same key range)

**OCC storm**: many concurrent writers + an **already-populated** target + `ON CONFLICT` → livelock risk
(observed: CPU maxed, zero progress). **Fix**: plain `INSERT` into an empty target; on the replace path DROP+recreate first

**Source load** is a separate lever: `table_parallelism` = concurrent source-read pressure → start low (2–4), ramp with headroom

→ In short, bottlenecks surface in layers: **① CPU (solved by multi-process) → ② OCC (PK strategy · empty target) → ③ IAM token / TLS cold-start**

<!--
- The old "parallelism +5%, retries 9.6→12.8%" measurement still holds — it's now repositioned as the story *after* clearing the CPU wall.
- The OCC storm is a real landmine we hit when adding multi-process: 32 writers ON CONFLICT into a populated target → 8+ min at 0 rows. Fixed by plain INSERT into an empty target.
- Memory ≈ table_par × batch_par × ~8 MiB (independent of table size). The Fargate CPU/memory pairing (8 vCPU→16 GiB) already covers it.
-->

---

# Performance case study — Composite PK A/B (in-VPC, measured)

**Experiment**: `orders`+`payments`, only the PK strategy varies → keep (integer) vs composite `(customer_id, id)`
(orders = treatment / payments has no `customer_id` so keeps its integer PK = control)

| Condition | keep overall rows/s | composite overall rows/s | CPU |
|---|---|---|---|
| **0.5 vCPU · bp8** | 4,270 | 4,243 (**0.99x**) | ~50% (1-core wall) |
| **4 vCPU · bp16** | **10,055** | **10,088 (1.00x)** | 109~111% |

- **composite made no difference in either condition.** DSQL `CommitLatency`: both **p50 ~47ms · p99 60~120ms** → **no hot-partition long tail**
- This A/B was measured in the **thread (single-process)** era → the wall was client CPU, so the server-distribution lever (composite) had nothing to gain

> **Lesson: measure the bottleneck before optimizing.** This workload's wall was **client CPU**, not server writes. So the answer wasn't composite — it was **multi-process (breaking the CPU wall)**. Composite only pays off under much higher write concurrency or a genuine monotonic-PK hotspot.

<!--
- Make clear this A/B predates multi-process (thread era) — that's why CPU was the wall, and the real fix for that wall was the later multi-process work.
- The control (payments) moved identically to the treatment (orders) → not environment noise, a genuine no-effect.
- Where composite shines: (a) after first removing the client CPU bottleneck (via multi-process), (b) at much higher write concurrency where a monotonic PK heats one partition.
-->

---

# Step 4B: CDC pipeline topology

![w:960](../deploy/architecture-cdc-pipeline.png)

- CDC is **optional**. Full Load copies existing rows; CDC applies every subsequent insert/update/delete → minimal downtime
- One topic per table + **PK keying** → all changes for a row stay **ordered within one partition**
- Schema is carried by the runtime's built-in JSON converter (no separate schema registry needed)

<!--
- If a short freeze is acceptable, Full Load alone suffices. CDC is for large/continuous migrations.
- The tool does not run this pipeline in-process → it's just a control plane (see two slides ahead).
-->

---

# Why a custom sink — the core design decision

**Standard managed JDBC sink vs custom sink**

| | Standard JDBC sink | **Custom DSQL sink** |
|---|---|---|
| OCC (40001) retry | **per batch** | **per statement** |
| High-contention large-scale CDC | throughput **collapses** | retry only the conflicting stmt, batch proceeds |
| IAM short-lived token | ✗ | 15-min token refreshed with 2-min headroom |
| ≤3000-row batches | ✗ | one commit per chunk |

> "Java is a consequence of the runtime, not a preference."

- Managed MSK Connect = managed Kafka Connect → the plugin must be a **JVM jar**
- We **mirror** the token generation / OCC retry / DSQL dialect logic from Python `core/` into Java
- **A bounded cross-language duplication is the price of the managed runtime** — enforced by a write-contract parity test

<!--
- The basis is "Decision Change 8." A standard JDBC sink retries 40001 per batch → replays all 3000 rows → wide key range → livelock.
- A shared parity test forces Full Load (Python) and the CDC sink (Java) to follow the same type mapping → the same source row lands identically via either path.
- CDC specifics: BIGINT UNSIGNED needs precise mode, JSON is wrapped as a PGobject, GEOMETRY extracts .wkb.
-->

---

# Gapless handoff — carrying Full Load through to CDC

**Gapless is enforced at both ends of the pipeline — the start point and the apply**

**① Entry (start point): begin streaming exactly where Full Load ended**
- Record the **watermark** (binlog position + GTID) at the moment Full Load takes its snapshot
- When CDC starts, **before** the source connector comes up, an in-VPC Lambda seeds that watermark as Debezium's start offset
  - Why a Lambda: the MSK Serverless bootstrap address is VPC-private, so the app can't seed it directly
- As a result Debezium reads **from "the first change after the snapshot," not "now"** → no head loss
  - `snapshot.mode=recovery`: it doesn't re-read rows; it rebuilds only the schema history, then resumes from the seeded offset

**② Exit (apply): safe even if the same change is applied twice**
- The sink applies via PK-based `ON CONFLICT` upsert / PK delete → **no dupes on retry/replay** (idempotent)
- If the connection drops, that offset is replayed → net result is **exactly-once (effectively-once)**

⚠️ **Must-check prerequisite**: the binlog the watermark points to **must still exist at CDC start**
- Aurora MySQL retains binlog 24h by default, but CDC stack deploy alone takes 15~20 min → **raise retention before starting** (e.g. 7 days). If it's gone, re-run Full Load for a fresh watermark

<!--
- Core framing: gapless is enforced not at one point but at two layers — Lambda (entry) prevents head loss, sink idempotency (exit) prevents mid-stream loss.
- The actual loss we hit was an "exit" bug: misclassifying a connection drop as poison advanced the offset past unapplied rows → fixed by reclassifying in isTransient (retry). The Lambda (entry) was fine all along.
- The prerequisite (binlog retention) is the most commonly missed thing in the field. binlog_format=ROW and binlog_row_image=FULL are also required.
-->

---

# CDC data integrity — no silent loss

**transient vs permanent classification is the retry/DLQ criterion**
- **transient** (retry, not DLQ'd): OCC 40001, connection drop (idle close / token expiry / worker replacement)
  - detect a dead/half-open connection → reconnect with a fresh token → re-apply the same offset (PK idempotent, no dupes)
- **permanent** (DLQ quarantine): type mismatch, constraint violation, missing target column (un-propagated source ALTER), oversized value
- if neither is possible → **do not silently skip; fail the task loudly**

**1 MiB per-value limit — 3 bands**
- ≤1 MiB normal / 1–8 MiB the sink measures before write and **quarantines to DLQ** (raise Kafka limit 4→8 MiB) / >8 MiB **drop at capture via `column.exclude.list`**

**The DLQ is viewed in CloudWatch, not Kafka**
- the quarantine reason carries **only the SQL template (column names + `?`)** — never row values or credentials → the tool parses it into the UI as "per-table Quarantined + downloadable error log"

<!--
- "CDC replicates data, not schema" (include.schema.changes=false). Source DDL changes don't propagate → re-apply them directly to DSQL first. Until then, non-matching rows are DLQ-quarantined (not silently gone).
- Not mistaking a connection drop for a poison row is the key — that was the earlier data-loss mode.
- Composite PK: re-keyed at the source via message.key.columns → a row's changes stay ordered in one partition, and the sink builds ON CONFLICT(pk...)/DELETE WHERE correctly.
-->

---

# Step 5: Validation — the only place a final verdict is issued

Full Load/watermark row counts are **scan-free estimates** (to spare the source). The exact verdict is **Validation only**.

**Per-table, 3 escalating rigor levels** (cost↑)
1. **Row count** = exact source vs target `COUNT(*)` (cheap)
2. **Checksum** = order-independent per-table checksum computed identically on both sides → catches "same count, different values" (reads every row)
   - Logic: per row, integerize the first 60 bits of `MD5(columns)` → `SUM` over the whole table (order-independent) → compare source=target. Cross-engine normalization (NULL sentinel, matching type rendering) makes same data = same hash; FLOAT excluded
3. **Reconciliation** = sorted merge of the full PK set on both sides → pinpoints `missing_on_target`/`extra_on_target` (**single integer PK only**)

**Verdict AND-chain**: a table is matched = (COUNT equal) AND (checksum equal) AND (PK set consistent)
→ report is_match = (∀ tables matched) AND (orphan==0). Without evidence, it reports "not deeply checked" (never a false match)

**Live-source drift correction** (the source keeps changing during validation too)
- Compare the snapshot-time GTID (watermark) with the source GTID **now** → see if the source has advanced
- If it has, "source > target" is attributed to **new data added since the snapshot, not a migration bug**
- → filter out "explained differences" (new activity · intended quarantine · not-yet-converged CDC), and flag only the **unexplained missing rows** as real problems

<!--
- Target-short diagnosis: (a) drift, (b) intended quarantine (>1 MiB), (c) not-yet-converged CDC delete → if explained by these three, it's healthy. Unexplained missing/extra PKs are the real targets.
- Diff samples show only PK + checksum tokens, never row values. There are also read-only CLIs (compare_rows.py / cdc_consistency_check.py, exit-0 for shell gating).
-->

---

# AI DBA & Query Playground (optional)
## Evidence-based — proven by measured DPU

- AI assist is **opt-in** (off by default), **control-plane only**, **never on the data path**
- Query conversion uses the **same sqlglot engine** as schema + AUTO/MANUAL/UNSUPPORTED + anti-pattern tagging (`SELECT ... FOR UPDATE`, etc.)
- **Safe execution on the target**: SELECT→EXPLAIN (ANALYZE is read-only), DDL→dry-run + **ROLLBACK**, DML→**blocked**

**DSQL query-tuning rules (different from generic PG advice)**
- **The PK is the table** — PK-ordered B-tree, no heap. No index → not a Seq Scan but a **Full Scan**
- **compute↔storage split** → every row that crosses incurs **DPU**. **Pushing filters down** is the key lever
- Filter 3 layers: Query Processor Filter (worst) → Storage Filter (INCLUDE) → **Index Condition (best)**
- Excluded: VACUUM/REINDEX/fillfactor/planner GUCs/lowering `cost=` (don't fit DSQL)

<!--
- The "Tune with AI DBA" button appears only after the converted SELECT passes Test on target — so the AI grounds on the real plan, not a guess.
- Proof loop: EXPLAIN ANALYZE feeds the before/after DPU delta back into the chat → the measurement, not the model's claim, is the evidence. If there's no improvement it says so. Not auto-applied (human-review gate).
-->

---

# Closing thoughts: hot partitions and application query changes

**DSQL partitions storage by PK** → a monotonic PK (AUTO_INCREMENT/timestamp) piles writes into one partition (hot partition). Remedies: UUID / cached identity / **composite PK `(high-cardinality leading col, original PK)`**.

**But — the hot partition isn't always the bottleneck (measured)**
- keep vs composite A/B (orders+payments): **no throughput difference at either 0.5 vCPU·bp8 or 4 vCPU·bp16 (0.99~1.00x)**
- DSQL CommitLatency: both **p50 ~47ms / p99 60~120ms** — no multi-second hot-partition long tail
- The bottleneck is **client CPU, not server writes** (§Full Load) → the real fix wasn't composite, it was **multi-process** (breaking the GIL wall, 18×)
- Only after multi-process clears the CPU wall and you push to tp=8 does the bottleneck move to **DSQL server writes** — that's when composite starts to pay off

**Composite PK's real cost: the application's queries change**
- Once the PK is `(customer_id, id)`, the app's reads/joins/**upserts must use the new composite key**, and the leading column must be **immutable**
- A separate `UNIQUE INDEX ASYNC` is needed to preserve the original key's uniqueness; CDC needs `message.key.columns` re-keying

> **Conclusion**: bottlenecks come in layers — **CPU (solved by multi-process) → OCC/hot partition → server writes**. A hot-partition remedy (changing the PK) is worth it **only after you clear the CPU wall and actually hit the server write wall**. Measure first, and weigh composite together with its **query-change cost**.

<!--
- Honest diagnosis: the composite PK feature works correctly but gained 0 in this workload — because the bottleneck was client CPU, not the server. The real fix for that GIL wall was multi-process (18×).
- Message: "hot partitions are real, but measure before applying a remedy. Bottlenecks layer as CPU→OCC→server writes. And composite PK isn't free — app queries change."
- Where composite shines: (a) after removing the client CPU bottleneck via multi-process, (b) at much higher write concurrency where a monotonic PK heats a partition.
-->

---

# Demo (5 min)

## Now we run the actual tool

- The 6-step workflow in the UI: Connect → Evaluation/Schema Conversion → Full Load → Validation
- See on screen what we discussed:
  - 3-way (AUTO/MANUAL/UNSUPPORTED) report · side-by-side DDL diff
  - Full Load progress (per-table rows/s) · failure isolation
  - Validation verdict · (optional) AI DBA proof loop

**Q&A welcome during and after the demo**

<!--
- Start the demo. (Run in an internal test environment — the launch method is intentionally not in the deck.)
- If short on time, just showing the Full Load progress screen + Validation verdict conveys the core.
-->

---

# Thank you / References

- Manual: `docs/manual/en/` (chapters 0–11, per-step detail)
- Deployment guide: `deploy/DEPLOYMENT.md`
- Custom sink: `connectors/dsql-sink/`

**Three-line summary**
1. Heterogeneous migration — deterministic-first, surfaces where human work is needed
2. Full Load (streaming bulk, **multi-process to bypass the GIL → 200GB 46h→2.5h, 18×**) + CDC (custom sink, statement-OCC, gapless) → DSQL
3. Loud failure over silent loss — credentials in memory only, verdict only in Validation

<!--
- Wrap up. Invite questions.
-->
