# 7. Performance, tuning, and why this design

_Language: **English** | [한국어](../ko/07-performance-and-tuning.md) | [日本語](../ja/07-performance-and-tuning.md)_

> **Prev:** [6. Limitations](06-limitations.md)

This chapter explains **why** the tool's data path is built the way it is —
grounded in how Aurora DSQL actually works — and **how** to tune its parallelism
for your workload. If you're evaluating whether to trust this tool for a
large-scale migration, this is the technical case.

> Every design choice below maps to a documented Aurora DSQL behavior or limit.
> Sources: [DSQL quotas & limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html),
> [Concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html),
> [PostgreSQL→DSQL migration guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html),
> [Asynchronous indexes](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html),
> [Primary keys](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html).

---

## 7.1 Why this design (the technical case)

### Statement-level OCC retry, not batch-level

Aurora DSQL is **lock-free**: it never takes row locks, and detects write
conflicts **at commit time**, returning `SQLSTATE 40001` (a serialization
failure — `OC000` data conflict or `OC001` schema conflict). The losing
transaction must **re-run**, and AWS explicitly notes that with OCC, "applications
[must] exercise this logic **more frequently**" than with lock-based databases.

A stock JDBC sink retries the **whole batch** on `40001`. At large scale with
bounded parallelism that is the wrong unit: re-submitting all ~3000 rows
re-pays the read/write work for the 99%+ that never conflicted, and the larger
key range a transaction spans, the more likely *another* worker touches it before
the retry commits — pushing a busy load toward livelock. This tool retries at the
**statement level**: only the conflicting `INSERT … ON CONFLICT` re-runs, so each
conflict is local and bounded. This is exactly why a custom DSQL sink connector
exists instead of a stock JDBC sink (see [Chapter 4 §4.1](04-cdc-and-dsql-constraints.md#41-the-pipeline)).

### Primary-key strategy — avoid hot partitions

DSQL **partitions and distributes storage by primary key**, and the docs are
explicit: **"Choose a random primary key… Avoid patterns that increase contention
on single keys."** A MySQL `AUTO_INCREMENT` PK is monotonic — every insert targets
the same "rightmost" key range, so during a high-throughput load all workers
converge on one partition, spiking the OCC conflict rate and creating a write
hot-spot **even when rows don't logically conflict**.

The tool surfaces this at **Evaluation** (`AUTO_INCREMENT` → `MANUAL`) and offers
PK strategies in **Schema Conversion** — keep the integer PK, convert to **UUID**,
or use an **identity column with caching** — so you can spread writes across the
key range. This is a DSQL-specific concern that a same-engine (MySQL→MySQL)
migration never has to think about.

#### The composite-key option — the one lever that moves the server-side wall

Why this option exists: in our in-VPC measurements, a large-table Full Load
plateaued at roughly the same throughput no matter how much we tuned the
**client** — a read-ahead prefetch queue, sharding the read across PK ranges, and
raising batch size / write parallelism each moved throughput by ~0%. The wall was
not the client; DSQL's `CommitLatency` sat at a healthy ~50 ms p50 but spiked to
**several seconds (p99) and tens of seconds (max)** in a periodic long tail, while
the OCC conflict rate stayed near zero. That long tail is the signature of a write
**hot partition**: with a monotonic `AUTO_INCREMENT` key, every insert lands in the
same rightmost key range, so one partition serializes the writes even though the
rows never logically conflict. A hot partition is a *server-side* limit, so only a
change that spreads writes across partitions can move it — and the composite key is
that change.

So Schema Conversion offers a fourth, **per-table** strategy: switch a table's
target primary key to a **composite key** that prepends a high-cardinality column
you choose ahead of the original key — e.g. `(customer_id, id)` instead of `(id)`.
Because DSQL stores rows in primary-key order, leading with `customer_id` scatters
inserts across many key ranges (one per customer) instead of funneling them to a
single rightmost partition. Key points:

- **The source MySQL schema is never changed** — only the DSQL target key. This is
  a target-side migration decision, not a schema redesign you push back to MySQL.
- **The original key's uniqueness is preserved.** The tool emits a
  `CREATE UNIQUE INDEX ASYNC` on the original key alongside the composite PK, so a
  lookup or constraint on the old key still holds.
- **The tool validates the choice** against DSQL's key limits before you apply it
  (the leading column must be `NOT NULL`, not already part of the key, and the
  composite key must stay within ≤ 8 columns and ≤ 1 KiB), and spells out the
  consequence at selection time: **after cutover the application's queries, joins,
  and upserts must use the new composite key, and the leading column must be
  immutable** (DSQL primary keys cannot be updated in place).
- **Full Load and CDC both handle it.** Full Load's idempotent
  `INSERT ... ON CONFLICT` keys on the target composite key; CDC needs **no
  connector or plugin change** — the Debezium source is re-keyed (via
  `message.key.columns`) so each change record's key matches the target composite
  key, and the sink's upsert/delete apply against it unchanged.

Reach for this when a table's writes are demonstrably hot-partitioned (a
`CommitLatency` long tail under load with low OCC) **and** it has a natural
high-cardinality grouping column. If the load is bounded by something else — client
CPU, or write round-trip latency that isn't a hot partition — a composite key buys
nothing, because there is no server-side wall for it to move; measure first
(see [§7.5](#75-a-measured-example--one-run-that-backs-71-and-72)).

### Batched loads sized to DSQL's transaction envelope

DSQL enforces a hard per-transaction envelope: **≤ 3000 rows**, **≤ 10 MiB of
modified data**, **≤ 5 minutes**, one DDL per transaction. The loader batches to
**≤ 3000 rows** and an **8 MiB** byte budget (margin under the 10 MiB ceiling),
clamped further by the 65,535 bind-parameter limit. Two pitfalls this avoids:

- A **row-by-row** loader pays DSQL's per-transaction overhead (and the per-write
  DPU minimum) on *every row* — multiples more expensive than amortizing it across
  a batch.
- A **whole-table-in-one-transaction** loader simply **cannot succeed** — it hits
  the 3000-row ceiling (and the 5-minute limit on large tables) and fails outright.

### Indexes built *after* the load, asynchronously

DSQL offers `CREATE INDEX ASYNC` for **non-blocking** index builds. The tool loads
data **first**, then builds secondary indexes with `CREATE INDEX ASYNC` (one DDL
per transaction). Building indexes during the load would make every `INSERT` also
pay write cost for each secondary-index entry — including for rows a later CDC
change overwrites before cut-over — and add a uniqueness read to every write.
Deferring pays that cost once, over the stable dataset.

### A bulk loader for the copy, streaming CDC for the cut-over

DSQL is shared-nothing and serverless — there is no PostgreSQL logical-replication
slot to target, and a generic tool's "full load" is JDBC `INSERT` under the hood
with **no** DSQL-specific OCC handling. The tool's purpose-built loader is keyset
streaming (resumable, bounded memory), DSQL-envelope-aware batching, statement
-level OCC retry, and PK remapping in one path. For the cut-over, **Debezium → MSK
→ custom sink** decouples the source binlog from the apply: Kafka durably buffers
changes so the sink can fall behind during an OCC retry burst **without** losing
events or stalling the source's binlog rotation.

### Reading ahead of the write pool (bounded prefetch queue)

A single source table streams from **one** MySQL connection: pulling the next
keyset page issues the next `SELECT … WHERE pk > :last LIMIT :page` plus per-row
type conversion. If that read ran inline on the same thread that dispatches
writes, page *N+1*'s read would only start **after** page *N*'s batch was
submitted — read and write serialized, and the bounded write pool would sit idle
during each read. The loader instead runs a **dedicated reader thread** that fills
a **bounded queue** (depth ≈ 2× the write parallelism), so page *N+1* is read
**while** page *N*'s `INSERT … ON CONFLICT` batches are still draining. The bound
preserves the streaming memory guarantee (the reader blocks once the queue is
full — it can't race unboundedly ahead), and load order is unchanged (batches
still map to fixed PK ranges, so a stop/retry is still deterministic).

This overlap helps most exactly where a real migration runs — **in-VPC, with
adequate task CPU** — where the write side (network round-trips to DSQL, already
issued in parallel) is what the reader can hide behind. On a measured in-VPC 4 vCPU
run it was **~19% faster** than with the queue disabled. It is on by default; a
measurement seam (`DSQL_MIGRATOR_FULL_LOAD_PREFETCH=0`) can disable it to reproduce
the pre-prefetch path for A/B benchmarking. On a **CPU-starved** task (see §7.2) or
a **high-RTT** link where the read isn't the thing on the critical path, the gain
shrinks toward zero — which is why the tool leans on adequate CPU rather than this
optimization alone.

### Short-lived IAM token refresh in long-running CDC

DSQL uses **IAM-token auth only** (no static password), tokens are **short-lived**
(~15 min), and **connections time out after 60 minutes**. A long-running CDC sink
that cached one token would fail to *reconnect* after a pool eviction or the 60-min
timeout — a failure that looks like a network error but is really an expired token.
The custom sink mints a **fresh token per new connection** (15-min TTL, 2-min
refresh margin), so hours-long CDC never stalls on auth.

---

## 7.2 Tuning parallelism

All four migration phases run with sensible bounded defaults; you can raise them
for throughput on big hardware or lower them to protect a busy source. **Each
phase is tuned differently** — Full Load and Validation via the app's environment
variables, CDC via CloudFormation parameters.

### Full Load

| Setting (env var) | Default | Bound | Effect |
|---|---|---|---|
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | 4 | ≤ 16 | How many tables load concurrently. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | 8 | ≤ 32 | In-flight `INSERT … ON CONFLICT` batches per table. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | Rows per batched write (hard-capped at DSQL's 3000-row limit). |
| `DSQL_MIGRATOR_FULL_LOAD_PREFETCH` | `1` (on) | on/off | Read-ahead prefetch queue (§7.1). **Leave on** — set `0` only to reproduce the pre-prefetch path for A/B benchmarking. |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `1` (off) | ≤ 8 | Reader range sharding: split a **large single-integer-PK** table's read into K concurrent readers (§7.1). **Off by default and rarely worth turning on** — see the caveat below. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | 1000000 | — | A table must have at least this many (estimated) rows to be sharded; smaller tables always use one reader. |

> **When does reader sharding actually help? Rarely — and it was ~0% in our
> in-VPC test.** The per-row type conversion is pure Python and holds the GIL, so K
> reader *threads* still can't exceed ~one core of conversion throughput — thread
> sharding overlaps the readers' I/O waits (which the prefetch queue already does)
> but cannot add a second core of convert. In a measured in-VPC 4-vCPU run,
> `reader_shards=4` gave **~0% over one reader** and CPU stayed pinned at ~1 core,
> because that load was bounded by **DSQL write round-trip latency**, not by the
> reader. Turn it up only if you have *evidence* a run is read/convert-starved (the
> reader can't keep the write pool busy); when the wall is the write side, raise
> **`BATCH_PARALLELISM`** instead. Total source readers = `table_parallelism ×
> reader_shards`, clamped to a safe ceiling to protect the source's connection limit.

> **Connection-quota guardrail.** Total concurrent DSQL connections ≈
> `table_parallelism × batch_parallelism` (the default 4 × 8 = 32). DSQL allows up
> to **10,000 connections per cluster** but only **100 new connections/second**,
> so keep the product comfortably within quota and don't set both knobs to their
> maximum without reason. Raising parallelism also raises the **OCC collision
> rate** on hot key ranges — pair it with a good PK strategy (above).

### Validation

| Setting (env var) | Default | Bound | Effect |
|---|---|---|---|
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | 4 | ≤ 32 | How many tables are compared concurrently (each on its own read-only source + target connection). `1` = sequential. |

Bounded at 32 to protect the source from too many concurrent scans.

### CDC (data plane)

**The tool infers CDC scaling for you — there is nothing to set in the UI.** The
connectors run on managed MSK Connect, and their scaling knobs are computed from
the **number of captured tables** (plus, for a skewed workload, each table's
estimated size — see below). Each table is its own Kafka topic, and the sink
consumes topic **partitions** in parallel (one sink task per partition), so total
sink parallelism = `partitions-per-topic × tables`. The tool picks the smallest
partition count that brings that product up to a sink-parallelism ceiling, then
stops:

| Captured tables | partitions / topic | `SinkTasksMax` | Effective parallelism |
|---|---|---|---|
| 1 | 8 | 8 | 8 |
| 2 | 4 | 8 | 8 |
| 3 | 3 | 8 | 8 |
| 4 | 2 | 8 | 8 |
| ≥ 8 | 1 | 8 | 8 |

**Skewed workloads — size-proportional partitions.** The table above assumes load
is spread **evenly** across tables. It often isn't: a few "hot" tables can carry
most of the writes. With many tables the uniform rule gives **1 partition each**,
and a 1-partition topic is consumed by **at most one sink task** — so a hot table
would be streamed by a single task while the rest sit idle (pure throughput loss,
even when DSQL is near idle). To avoid that, the tool reads **scan-free per-table
row-count estimates** (the Full Load watermark's, or a fresh `information_schema`
estimate when CDC infra is deployed before Full Load) and gives the larger tables
**more partitions — 2 or 4** — via Debezium `topic.creation` groups, so a hot
table streams across several sink tasks in parallel. 4 is the per-table ceiling (a
single table's gain flattens past ~4 partitions as concurrent DSQL upserts to it
contend). This is **automatic**: it activates only for a genuinely skewed,
many-table capture, is a **no-op under even load**, and falls back to the uniform
default when there is no size signal (or you set
`DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS`). Ordering is unaffected — Debezium keys each
record by primary key, so all changes for a row still land on one partition.
Because a topic's partition count is fixed at creation, this is decided at CDC
infra deploy; changing it needs a fresh CDC deploy.

Why inferred and hidden, not a UI field:

- **Partition count is irreversible.** A topic's partitions can only be *raised*,
  never lowered — and in practice raising them means recreating the MSK cluster. A
  wrong value is permanent, so it must be right at create time, not fiddled with.
- **A CDC change is a 15–20 min connector redeploy**, not a cheap retune loop like
  Full Load — so there is no fast experiment cycle to expose.
- **The knobs interact and MCUs cost money** — a free-form combination is easy to
  get wrong and can silently over-bill.

The ceiling exists because the sink is **DSQL-write-latency-bound**: measured
throughput scales with partitions **sublinearly** (4 → 8 partitions gave ~1.4×,
not 2×) as concurrent upserts to one table start to contend inside DSQL. Beyond the
ceiling, more partitions mostly add MCU cost without throughput. The source side is
single-task per MySQL server (one binlog stream) but is **not** the bottleneck —
with the shipped producer tuning it sustains tens of thousands of records/sec. As
always, the real ceiling under load is OCC on hot primary keys — **PK strategy
matters most** (see §7.1).

**Overriding the inference (advanced).** If you have a reason to depart from the
inferred values, set these environment variables before the tool deploys the
cdc-stack (blank/invalid values fall back to the smart default):

| Environment variable | Overrides | Notes |
|---|---|---|
| `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` | partitions per per-table topic | **Irreversible** once the topic exists. Also **forces a uniform count** — disables the size-proportional allocation above. |
| `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX` | sink connector `tasks.max` | Capped in effect by the partition count. |
| `DSQL_MIGRATOR_CDC_MCU_COUNT` | MSK Connect MCUs per worker, **source** connector | Must be one of 1 / 2 / 4 / 8. |

Two related cdc-stack parameters are fixed, not inferred: `SourceTasksMax` = 1
(MySQL is single-task per server) and `SinkBatchMaxRows` = 3000 (DSQL's
per-transaction row limit — **do not exceed 3000**).

#### The sink's consumer timeouts (why they are not Kafka's defaults)

The sink is a Kafka **consumer group** member, and one `put()` call can legitimately
take minutes: it is handed up to `SinkMaxPollRecords` (3000) records, and if any row
in a chunk hits a permanent SQL error the connector re-applies that chunk **row by
row — one DSQL transaction each** so the poison row is isolated and the healthy rows
still land. At ~50 ms per round-trip that single call can run ~450 s, and OCC retry
adds more.

On Kafka's defaults that is fatal in a way that **looks healthy**: exceeding
`max.poll.interval.ms` (default 300000, 5 min) makes the group coordinator eject the
consumer and revoke its partitions, after which offsets can never be committed. And
because the sink runs with `errors.tolerance=all` and Connect only *warns* on a failed
commit, the task never dies — it logs `Commit of offsets timed out` on a loop while
the connector still reports **RUNNING** and replication is permanently stopped. The
cdc-stack therefore sets these on the sink worker config:

| Parameter | Default | Kafka default | Why |
|---|---|---|---|
| `SinkMaxPollIntervalMs` | 900000 (15 min) | 300000 | Must exceed the worst-case `put()`, i.e. the 3000-row row-by-row fallback. **Raise it if you raise `SinkMaxPollRecords`.** |
| `SinkSessionTimeoutMs` | 60000 | 45000 | Heartbeats come from a background thread, so this only absorbs worker GC/scheduling pauses — not apply time. |
| `SinkHeartbeatIntervalMs` | 20000 | 3000 | Kafka requires it below the session timeout (≤ ⅓ is the guidance): three missed heartbeats before eviction. |
| `SinkOffsetFlushTimeoutMs` | 120000 | 5000 | The commit path calls the task's `flush()`, which emits the monitor metrics to CloudWatch over the NAT gateway; 5 s is too tight for that. |

These are all settable because MSK Connect's **worker**-level allowlist includes
`consumer.max.poll.interval.ms`, `consumer.session.timeout.ms` and
`consumer.heartbeat.interval.ms`. What MSK Connect does not support is the
per-*connector* `.override.` form (it excludes
`connector.client.config.override.policy`) — which is why they live in the worker
config. Changing them replaces the worker configuration, so it needs a
`PLUGIN_VERSION` bump and a **Delete + Deploy** of the CDC infrastructure; Start CDC
alone will not pick it up.

> **The other direction bounds it more cheaply.** Lowering `SinkMaxPollRecords`
> (e.g. to 500–1000) caps how long one `put()` can ever run, at no throughput cost:
> a `put()` is applied in `SinkBatchMaxRows`-sized chunks, so a smaller poll still
> fills whole DSQL transactions — it just hands the task less work per call. Both are
> cdc-stack CloudFormation parameters (not app settings), so either change is a
> redeploy.

#### Sink compute (the knob to reach for first)

The sink's compute is sized **separately** from the source: `ConnectorMcuCount`
applies to the source connector, `SinkMcuCount` (default 4) to the sink. The sink
is CPU-bound under heavy load once its apply path was optimized, whereas the
single-task source has spare CPU — so raise the **sink's** MCUs, not the source's,
if the sink can't keep up. See the appendix (§12) for the measured curve.

Unlike everything else in this section, this one is in the UI:
**Settings → Performance → CDC → "Sink compute (MCU)"** (or
`DSQL_MIGRATOR_CDC_SINK_MCU_COUNT`). Only 1 / 2 / 4 / 8 are offered — those are the
MSK Connect API's valid values (`mcuCount`), so 8 MCUs per worker is the ceiling.
1 MCU = 1 vCPU + 4 GiB.

**When it takes effect.** This is *not* like the Full Load knobs, which the loader
re-reads on its next run. It is a cdc-stack CloudFormation parameter, applied when
Start CDC creates or updates the sink connector:

| Pipeline state | Effect of changing it |
|---|---|
| No CDC deployed yet | Used by the next infrastructure deploy and Start CDC. |
| Infra deployed, not started | Used by the next Start CDC. |
| **Already streaming** | **Nothing changes until you run Start CDC again.** |

Re-running Start CDC on a streaming pipeline is safe for this purpose: connector
`Capacity` is an in-place update (no replacement), so the sink is resized rather
than recreated — it does **not** consume MSK partition quota the way a table-set
change does, and there is no gap in replication.

> **Source reboots are handled automatically.** The source connector sets
> `errors.retry.timeout=600000` (10 min), so a source RDS/Aurora reboot
> (maintenance, failover, resize) is absorbed: the connector retries across the
> reboot and resumes from the committed binlog offset with no gap and no operator
> action — you do **not** need to Stop/Start CDC after a reboot.

### On AWS (ECS Fargate) — yes, all of this is tunable there too

The Full Load and Validation knobs are ordinary `DSQL_MIGRATOR_*` **environment
variables**, read at run time by the app. In a Fargate deployment they are set in
the **ECS task definition's container `environment` block** (the same place the
template already sets `DSQL_MIGRATOR_LOG_LEVEL`, the `/tmp` state paths, etc.) —
add the keys above to `deploy/cloudformation.yaml`'s container environment (or your
own task definition) and redeploy. The CDC knobs are cdc-stack CloudFormation
parameters, passed when the tool deploys the cdc-stack.

> **Retune between runs without redeploying.** For iterating on these Full Load /
> Validation values, open **Settings** in the sidebar footer and go to the
> **Performance** tab: the loader and validator re-read the config on every run,
> so a value you set there applies to the **next** Full Load / Validation
> immediately — no task-definition edit or redeploy. It is app-wide (single-task
> app) and resets to the deploy/startup values on restart, so set the
> task-definition `environment` for the values you want to **persist** across
> restarts, and use the UI control to **experiment** live.

Also size the **Fargate task CPU** (`ContainerCpu`) generously — **Full Load is
CPU-bound**, not network-bound. The source reader converts every row's MySQL type
to its DSQL form in Python (per-cell, GIL-held), so throughput scales with CPU:
in a measured payments+orders load, **4 vCPU ran ~3.8× faster than the 0.5 vCPU
(512) default** on the same data. Use **0.5–1 vCPU for evaluation**, but **2–4 vCPU
for a real large-scale Full Load**. Beyond ~4 vCPU returns diminish for a single large
table — the reader is one thread and tops out near one core, so the next lever is
sharding the read across PK ranges (a future enhancement), not more vCPU.

**Memory** (`ContainerMemory`) is a Fargate **hard limit** — exceed it and the kernel
OOM-kills the task with *no app shutdown* (only a CloudWatch spike + an ELB timeout).
It is bounded by the buffered pipeline, not by table size: roughly `table_parallelism ×
(prefetch_depth + batch_parallelism) × per-batch-bytes`, **summed across the worker
processes** (Full Load runs one process per table at `table_parallelism > 1`, and
Fargate counts the whole task cgroup). The per-batch byte cap is ~8 MiB, but **wide rows
— large `TEXT`/`BLOB` values — fill a batch to that cap**, so the default `512` CPU /
`1024` MiB has OOM-killed a real load running with concurrent source writes. Size it:
**`1024` MiB is fine for evaluation; use ≥ `2048` for a real Full Load, and ≥ `8192`
(with CPU ≥ `2048`) when tables carry large LOBs or you raise the parallelism knobs.**
Raising it is a redeploy — Fargate does not auto-scale a task's memory. The app logs a
memory high-water and an ~80%-of-limit pressure warning (also to the activity log, which
survives the task) so an approaching OOM is visible before the kill, and you can
right-size from evidence. See [DEPLOYMENT.md → Task sizing](../../../deploy/DEPLOYMENT.md#task-sizing--containercpu--containermemory)
for the full CPU↔memory pairing table.

> **Local runs** read the same environment variables — set them in your shell or
> `.env` before launching `mysql-dsql-migrator ui`.

---

## 7.3 Minimizing impact on the source

Full Load reads your production source, so the natural worry is: **"loading many
tables at once means a heavy read — will it hurt the source?"** The design already
keeps that read light (keyset streaming, no `OFFSET` re-scans, one ~1000-row page
in flight per table, no global lock, scan-free `information_schema` counts instead
of `COUNT(*)`, and implicit back-pressure — the next page is only read after the
current one is loaded). What's left to manage is **concurrent read pressure**, and
it scales almost entirely with **one lever**.

### The one lever: table parallelism

`DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` (default **4**, max **16**) is *how many
tables are read from the source at once* — one streaming source connection per
concurrent table. It is the dial on concurrent source read pressure. `BATCH_PARALLELISM`
and `BATCH_ROWS` raise **DSQL write** pressure, not source read load; leave them
unless DSQL is the bottleneck. (The source read **page size is fixed at 1000 rows**
and is not tunable — table parallelism is your only source-read throttle.)

> Set it under **Settings → Performance** to experiment between runs, or as the env
> var to persist it (see §7.2).

### Start conservative, then tune under observation

Treat parallelism as a **throttle, not a throughput dial** — ramp it against measured
headroom, not the number of tables:

1. **Start low (2–4)** even on a large instance. Finishing slower is far cheaper than
   causing a production latency incident.
2. **Watch the source** over a sustained 5–10 min window (not a single spike).
3. **Raise in modest steps** only when there's clear headroom, and re-observe.
   (Parallelism is read once per run, so a change applies to the **next** run.)

**Signals to watch** (Amazon RDS/Aurora — CloudWatch + Performance Insights):

- **Green light to raise:** `CPUUtilization` well below the line, `ReadLatency` flat
  as `ReadIOPS` rises, `DiskQueueDepth` low, `BurstBalance` / `EBSIOBalance%` /
  `CPUCreditBalance` near 100%, Aurora `BufferCacheHitRatio` steady (~99%).
- **Back off now:** `CPUUtilization` sustained > ~85–90%; `ReadLatency` inflating 2–5×
  while IOPS plateaus; any burst/credit balance **trending toward 0** (throttle
  *before* it hits zero — recovery is slow); `FreeableMemory` near zero / `SwapUsage`;
  Aurora `BufferCacheHitRatio` dropping (your cold scans are evicting production's hot
  pages); and above all, **your application's own query latency rising** — the ultimate
  back-off trigger regardless of instance metrics.

> [!note] No built-in rate limiter
> The tool has **no throughput/QPS rate limiting** — source read pressure scales
> linearly with table parallelism. Lowering the knob is your control. Schedule the
> bulk load **off-peak** and avoid overlapping the instance's backup/maintenance
> window (a snapshot plus full scans is the worst case for gp2 `BurstBalance`).

### Pre-flight headroom checklist

Baseline these over a representative peak, then confirm there's room to add the load:
**CPU** headroom; **storage** — gp2 `BurstBalance` (a sustained scan can drain it to
zero and collapse to baseline IOPS — the most common way to hurt production), plus
`ReadIOPS`/`ReadThroughput`/`ReadLatency`/`DiskQueueDepth` (Aurora: `VolumeReadIOPS`);
**buffer pool** `BufferCacheHitRatio`; **connections** `DatabaseConnections` vs
`max_connections` (16-way + your app pool must stay under the limit with margin);
**free space / memory**. For a busy RDS source, gp3 with headroom IOPS removes the gp2
burst cliff that most often bites full scans.

### Large tables on a write-heavy source

Each table's read runs in a `REPEATABLE READ` snapshot held open **for that table's
entire read**. On a write-heavy source that keeps InnoDB's undo history (**History
List Length**) from being purged — undo/disk growth and slower reads — and with 16
tables in flight the **oldest** open read view sets the purge horizon. Mitigations:
lower parallelism (fewer open read views); **shard the biggest tables by PK range**
into separate, shorter runs (there's no page-size knob, so shortening any one
snapshot means splitting the table set); and monitor History List Length, provisioning
free-space headroom before starting so undo growth can't trip a storage-full condition.

### What about reading from a read replica?

Pointing Full Load at an **RDS read replica / Aurora reader endpoint** moves all the
scan IOPS, CPU, and buffer-pool churn off the primary — the strongest way to protect
production, and it lets you push parallelism harder.

- ✅ **Full-Load-only migrations: a read replica is a good option.** The loader only
  does read-only keyset `SELECT`s in a consistent snapshot; when CDC is off, the
  watermark is captured but unused. (Expect the snapshot to reflect a slightly older
  point due to replica lag — normal.)
- ⛔ **If you are also running CDC, do NOT read from a replica — use the primary
  (writer).** The tool captures the CDC handoff **watermark** (binlog file/position,
  GTID, `server_uuid`) from the same source you connect to, and the CDC connector
  streams from that same host. A replica's binlog coordinates live in a **different
  namespace** than the primary's, and RDS replicas often have binlog **disabled or
  unretained** — so a replica-sourced watermark can silently produce a **CDC data
  gap** (changes between the snapshot point and CDC start are lost). For any Full
  Load + CDC migration, connect to the **primary/writer**, keep it as your single
  source, and set **binlog retention to outlive the entire Full Load**
  (`CALL mysql.rds_set_configuration('binlog retention hours', N)`).

> [!tip] Quick recommendation for a busy production source
> **Full-Load-only:** read from a **read replica** (gp3, up-sized for the window if
> needed); start parallelism at 4–8 and ramp on clear headroom. **Full Load + CDC:**
> read from the **primary/writer** (never a replica), start parallelism at **2–4** and
> ramp only on clear headroom, run **off-peak**, and set binlog retention to outlast
> the load. Either way, shard the few largest tables into PK-range runs.

---

## 7.4 Tuning an individual query

Beyond the parallelism knobs above, you can tune an individual query against Aurora
DSQL's distributed execution model — where the primary key *is* the table, filter
pushdown drives cost, and **DPU** (not PostgreSQL's `cost=`) is the unit. The
optional **Query Converter** converts a MySQL query, probes it read-only with
`EXPLAIN` / `EXPLAIN ANALYZE`, and — with AI assist on — an **AI DBA** rewrites it
for DSQL efficiency and re-tests the rewrite to prove the DPU improvement.

> See [Chapter 9 — The Query Converter and the AI DBA](09-query-validation.md) for the
> full workflow.

---

## 7.5 A measured example — one run that backs §7.1 and §7.2

Below is one run on live infrastructure, done to check whether the design rationale
above actually shows up in practice. Read it **as an illustration of the method, not
as a performance spec or guarantee** — anyone can reproduce it in their own
environment with `scripts/measure_performance.py`.

> [!note] The conditions these numbers came from
> RDS MySQL 8.0.42 source + Aurora DSQL target + MSK, `us-east-1`, a single run.
> **The hardware (source RDS class, Fargate/local CPU and memory, DSQL warm state,
> network RTT) was not pinned**, so absolute throughput and lag are
> environment-dependent and will differ elsewhere. This run also used an
> `AUTO_INCREMENT` integer-PK schema — the **hot-partition worst case** described in
> §7.1 — whereas the **UUID / cached-identity PK** this chapter recommends lowers
> contention. Your numbers will differ.

**Full Load — raising parallelism raises contention faster than throughput (the
§7.2 guardrail).** Doubling both the table and batch parallelism (4×8 = 32 → 8×16 =
128 connections) bought only **~+5%** throughput, while the **share of batches that
hit at least one retry** rose by about a third (≈ 9.6% → 12.8%). Even for rows that
don't logically conflict, writes converge on the same key range of a monotonic PK —
exactly the hot-partition effect in §7.1. The takeaway: **fix the PK strategy before
turning parallelism up blindly.**

**CDC replication lag — a latency floor at the default sizing.** Source commit →
visible on DSQL measured at roughly p50 0.8s / p95 1.3s under a sustained load, and
p50 0.5s / p95 0.7s under a burst. This is a **good-case floor at the default CDC
sizing (MCU=1, one worker, a single-column table)**; it can grow with many tables or
a high change rate. Scale it with the §7.2 CDC parameters when you need throughput.

> **Reproduce it:** `scripts/measure_performance.py full-load` (throughput +
> contention) and `scripts/measure_performance.py cdc-lag` (replication lag) give you
> the same metrics against your own source/target. Note that `full-load` **drops and
> recreates** the target tables (needs `--yes`) so use it **only on a non-production
> target**, and `cdc-lag` needs an active CDC pipeline.

---

**Next:** [8. Testing and verification →](08-testing-and-verification.md)
