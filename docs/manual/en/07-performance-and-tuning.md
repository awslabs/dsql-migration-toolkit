# 7. Performance, tuning, and why this design

_Language: **English** | [한국어](../ko/07-performance-and-tuning.md) | [日本語](../ja/07-performance-and-tuning.md)_

> **Prev:** [6. Limitations](06-limitations.md)

The defaults fit Aurora DSQL's limits and keep the load on your source modest, so **most
migrations need no tuning**. Read [§7.2](#72-tuning-parallelism) if a load is slower than
you need, [§7.3](#73-minimizing-impact-on-the-source) if the source serves production
traffic, and [§7.1](#71-what-the-tool-does-for-you-on-aurora-dsql) for what the tool
already does to fit Aurora DSQL.

> Every design choice below maps to a documented Aurora DSQL behavior or limit. Sources:
> [DSQL quotas & limits](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html),
> [Concurrency control](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-concurrency-control.html),
> [PostgreSQL→DSQL migration guide](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-migration-guide.html),
> [Asynchronous indexes](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-create-index-async.html),
> [Primary keys](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-primary-keys.html).

---

## 7.1 What the tool does for you on Aurora DSQL

Each row pairs a documented Aurora DSQL behavior or limit with what the tool does about
it:

| Aurora DSQL behavior | What the tool does |
|---|---|
| Write conflicts are detected at commit, and the losing transaction fails with `40001` (there are no row locks) | Keeps every write transaction small and idempotent (a Full Load batch or CDC chunk is at most 3,000 rows, written by primary key), so a re-run is cheap and safe |
| A transaction holds at most 3,000 rows and 10 MiB, runs at most 5 minutes, and carries one DDL | Batches to at most 3,000 rows and an 8 MiB byte budget, and runs one DDL per transaction |
| `CREATE INDEX ASYNC` builds an index without blocking | Loads the data first, then builds secondary indexes once over the stable data, instead of paying an index write on every `INSERT` (including rows a later CDC change overwrites) |
| IAM tokens last about 15 minutes, and connections end after 60 | Mints a fresh token for every new connection, so hours-long CDC never stalls on authentication |
| There is no logical-replication slot to target | Copies with its own bulk loader (keyset streaming: resumable, bounded memory), then streams changes through Debezium → MSK → a custom sink; Kafka buffers the changes, so the sink can fall behind during a conflict burst without losing events |

**When a write fails.** After a `40001` conflict the tool re-runs the **whole
transaction** with exponential backoff and jitter, so workers that collided do not retry
in lockstep. A dropped connection or an expired IAM token is reconnected and retried too.
So is Aurora DSQL's `XX000` ("server unavailable"), a temporary service condition. If
retrying outlasts its budget, the Full Load batch fails (use **Retry unfinished tables**;
no row is quarantined for it) and the sink has Kafka Connect redeliver its batch (no row
is dead-lettered for it); if it keeps recurring, contact AWS Support. Any other error is
not retried: the Full Load splits the batch down to the rejected row and quarantines it,
the sink sends it to the DLQ, and every healthy row still lands.

### Primary-key strategy — avoid hot partitions

Aurora DSQL **partitions and distributes storage by primary key**, and AWS's guidance is
explicit: **"Choose a random primary key… Avoid patterns that increase contention on
single keys."** A MySQL `AUTO_INCREMENT` key is monotonic, so every insert targets the
same rightmost key range: during a fast load all workers converge on one partition, and
the OCC conflict rate and write latency climb **even though the rows never logically
conflict**.

The tool flags such keys at **Evaluation** (`AUTO_INCREMENT` → `MANUAL`) and lets you
choose a strategy per table in **Schema Conversion**: a cached identity column (the
default for a single-column key that `bigint` holds), keeping the integer key, or a
composite key
([Choose the primary-key strategy](02-evaluation-and-schema-conversion.md#choose-the-primary-key-strategy)).

#### The composite-key option for a hot partition

Schema Conversion can put a high-cardinality column you choose in front of a table's
original key, such as `(customer_id, id)` instead of `(id)`, so inserts scatter across
many key ranges instead of one rightmost partition.

Reach for it when a table's writes are demonstrably hot-partitioned (`CommitLatency` shows
a long tail under load, seconds at p99 and tens of seconds at the maximum, while the OCC
conflict rate stays near zero) **and** the table has a natural high-cardinality grouping
column. In our in-VPC measurements, tuning the client (read-ahead, sharding the read,
bigger batches, more write parallelism) moved a large-table Full Load by about 0%; only
spreading the writes across key ranges moves a server-side hot partition. If the load is
bound by something else, such as client CPU or write round-trip latency, a composite key
buys nothing: measure first (see
[§7.5](#75-a-measured-example--one-run-that-backs-71-and-72)).

- **The source is never changed**, only the Aurora DSQL target key. A
  `CREATE UNIQUE INDEX ASYNC` keeps the original key unique, so lookups and constraints on
  it still hold.
- **Your application changes with it.** After cut over, its queries, joins and upserts
  must use the composite key, and the leading column must never change (Aurora DSQL
  primary keys cannot be updated in place). The list of leading columns offers only
  columns that pass Aurora DSQL's key limits (`NOT NULL`, an indexable type, and at most 8
  columns and 1 KiB in total).
- **Full Load and CDC both handle it**, on both engines. CDC needs infrastructure deployed
  by this version ([Chapter 6](06-limitations.md)). On a **PostgreSQL** source it also
  needs **`REPLICA IDENTITY FULL` on the table and every partition for the whole run**,
  because a `DELETE` otherwise does not carry the added key column: the prerequisites
  block Start without it, and the CDC monitor re-checks it while CDC runs
  ([§5.6](05-validation.md#56-re-keyed-postgresql-tables-and-cut-over)).

---

## 7.2 Tuning parallelism

Raise a setting when a phase is too slow and the hardware has room; lower it to protect a
busy source. **Full Load** and **Validation** are tuned with environment variables or in
**Settings** (§7.6); **CDC** is sized for you.

### Full Load

| Setting (env var) | Default | Bound | Effect |
|---|---|---|---|
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | 4 | ≤ 16 | How many tables load concurrently. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | 8 | ≤ 32 | In-flight `INSERT … ON CONFLICT` batches per table. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | Rows per batched write (hard-capped at DSQL's 3000-row limit). |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `1` (off) | ≤ 8 | Reader range sharding: split a **large** table's read by its integer leading primary-key column into K ranges, each read by its own worker process ([§3.8](03-full-load.md#38-parallel-loading)). PostgreSQL: any run. MySQL: Full load + CDC only, for a table whose unique keys all include that column. Also *Reader shards per large table* in **Settings → Full Load**. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | 1000000 | — | A table must have at least this many (estimated) rows to be split; smaller tables use one reader. |

> **Reader sharding: when it helps, and what it costs.** It helps when a few large tables
> dominate a load and pool slots would otherwise idle: one reader converts rows on one
> core, while each range runs in its own process on its own core (about **3.8× at K = 4**
> on an 8M-row PostgreSQL load). K is capped at 32 ÷ `TABLE_PARALLELISM`. **The
> trade-off:** K > 1 turns idle slots into concurrent reads of the same table for its
> whole read, so the source read rate, the DSQL connections and the memory rise up to
> `TABLE_PARALLELISM`-fold for that stretch. On a source still serving production traffic,
> pair it with the `Threads_running` throttle (`FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`,
> §7.3). When the wall is the write side, raise **`BATCH_PARALLELISM`** instead. On MySQL
> a table is split only in Full load + CDC ([§3.8](03-full-load.md#38-parallel-loading)).

> **Connection-quota guardrail.** Total concurrent DSQL connections ≈
> `table_parallelism × batch_parallelism` (the default 4 × 8 = 32). DSQL allows up to
> **10,000 connections per cluster** but only **100 new connections/second**, so keep the
> product comfortably within quota and don't set both knobs to their maximum without
> reason. Raising parallelism also raises the **OCC collision rate** on hot key ranges —
> pair it with a good PK strategy (above).

### Validation

| Setting (env var) | Default | Bound | Effect |
|---|---|---|---|
| `DSQL_MIGRATOR_VALIDATE_MAX_WORKERS` | 4 | ≤ 32 | How many tables are compared concurrently (each on its own read-only source + target connection). `1` = sequential. |

Bounded at 32 to protect the source from too many concurrent scans.

### CDC (data plane)

**The tool sizes CDC for you; there is nothing to set in the UI.** Each captured table is
its own Kafka topic, and the sink runs one task per topic partition, so sink parallelism =
partitions per topic × tables. The tool picks the smallest partition count that brings
that product up to a ceiling of 8 sink tasks and stops there:

| Captured tables | partitions / topic | `SinkTasksMax` | Effective parallelism |
|---|---|---|---|
| 1 | 8 | 8 | 8 |
| 2 | 4 | 8 | 8 |
| 3 | 3 | 8 | 8 |
| 4 | 2 | 8 | 8 |
| ≥ 8 | 1 | 8 | 8 |

**Skewed workloads get more partitions for the big tables.** The rule above assumes load
is spread evenly. With many tables it gives each topic one partition, and one partition is
consumed by at most one sink task, so a hot table would stream through a single task while
the rest sit idle. The tool therefore reads scan-free per-table row-count estimates (the
Full Load watermark's, or a fresh `information_schema` estimate if CDC infrastructure is
deployed before Full Load) and gives larger tables 2 or 4 partitions. This is automatic:
it applies only to a genuinely skewed capture and does nothing under even load. Because a
topic's partition count is fixed at creation, changing it needs a fresh CDC deploy.

Per-row ordering holds for every change that keeps the primary key (Debezium keys each
record by primary key). Two kinds of change span partitions: an `UPDATE` that changes the
primary key, and a unique value passed from one row to another. On a table with a unique
index besides its primary key, CDC infrastructure deployed by this version holds such a
change until the other row's change has applied: up to 5 minutes for each held change.
Other partitions keep flowing, and the wait shows as Stream lag on its table. CDC
infrastructure deployed by an earlier version dead-letters the change at once; Delete and
Deploy CDC infrastructure to pick up the wait ([Chapter 6](06-limitations.md)).

**Overriding the inference (advanced).** There is rarely a reason to: beyond 8 sink tasks,
more partitions mostly add MCU cost without throughput (4 → 8 partitions gave about 1.4×,
not 2×), because the sink is bound by Aurora DSQL write latency. If you do, set these
environment variables before the tool deploys the cdc-stack (blank or invalid values fall
back to the inferred default):

| Environment variable | Overrides | Notes |
|---|---|---|
| `DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS` | partitions per per-table topic | **Irreversible** once the topic exists. Also **forces a uniform count** — disables the size-proportional allocation above. |
| `DSQL_MIGRATOR_CDC_SINK_TASKS_MAX` | sink connector `tasks.max` | Capped in effect by the partition count. |
| `DSQL_MIGRATOR_CDC_MCU_COUNT` | MSK Connect MCUs per worker, **source** connector | Must be one of 1 / 2 / 4 / 8. |

#### Sink compute (the knob to reach for first)

If the sink cannot keep up, raise the **sink's** compute, not the source's: under heavy
load the sink is CPU-bound, while the single-task source has spare CPU. Set it in
**Settings → CDC → "Sink compute (MCU)"** (or `DSQL_MIGRATOR_CDC_SINK_MCU_COUNT`); 1, 2, 4
or 8 MCUs per worker are offered (1 MCU = 1 vCPU + 4 GiB; the default is 4). The measured
curve is in the appendix (§12).

**When it takes effect.** Unlike the Full Load knobs, it is applied when Start CDC creates
or updates the sink connector:

| Pipeline state | Effect of changing it |
|---|---|
| No CDC deployed yet | Used by the next infrastructure deploy and Start CDC. |
| Infra deployed, not started | Used by the next Start CDC. |
| **Already streaming** | **Nothing changes until the next Start CDC: Stop CDC, then Start CDC.** |

While CDC streams, the CDC step offers only **Stop CDC**, so a new sink capacity takes
effect when you Stop CDC and Start CDC again. The connectors are then re-created with it
(each connector create/delete uses MSK Serverless partitions), and the changes the sink
had not applied at the Stop are applied from the CDC topics, so that restart is gapless
only while the topics still retain them
([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)).

> **Source reboots are handled automatically.** The source connector retries for 10
> minutes, so a source RDS/Aurora reboot (maintenance, failover, resize) is absorbed: the
> connector retries across the reboot and resumes from the committed binlog offset with no
> gap and no operator action — you do **not** need to Stop/Start CDC after a reboot.

### On AWS (ECS Fargate)

Set the Full Load and Validation variables in the task definition's container
`environment` block (in `deploy/cloudformation.yaml`, or your own task definition) and
redeploy. The CDC settings are passed when the tool deploys the cdc-stack.

> **Retune between runs without redeploying.** Open **Settings** (sidebar footer) and go
> to the **Full Load / Validation** tab: the loader and validator re-read the config on
> every run, so a value set there applies to the **next** run immediately. It is app-wide
> and resets on restart, so put the values you want to keep in the task definition and use
> **Settings** to experiment.

Size the task for the load: **Full Load is CPU-bound** (the source reader converts every
row's MySQL types to their Aurora DSQL form in Python), and **memory is a hard limit**.

| Setting | Evaluation | A real large-scale Full Load |
|---|---|---|
| `ContainerCpu` | 0.5–1 vCPU (`512`–`1024`) | 2–4 vCPU (`2048`–`4096`) |
| `ContainerMemory` | `1024` MiB | ≥ `2048` MiB; ≥ `8192` MiB (with CPU ≥ `2048`) for large LOBs or raised parallelism |

- **CPU.** In a measured payments+orders load, 4 vCPU ran about **3.8× faster** than the
  0.5 vCPU (`512`) default on the same data. Beyond about 4 vCPU a single large table
  gains little (its reader is one thread), so split the read instead: *Reader shards per
  large table* (§7.2; MySQL in Full load + CDC only).
- **Memory.** Exceeding `ContainerMemory` OOM-kills the task with no app shutdown (you see
  only a CloudWatch spike and an ELB timeout). Wide rows (large `TEXT`/`BLOB` values) fill
  a batch to its ~8 MiB cap, so the default `512` CPU / `1024` MiB has OOM-killed a real
  load with concurrent source writes. The app logs a memory high-water mark and an
  ~80%-of-limit warning (also to the activity log, which survives the task), so you can
  right-size from evidence.

Raising either is a redeploy: Fargate does not auto-scale a task's memory.
[DEPLOYMENT.md → Task sizing](../../../deploy/DEPLOYMENT.md#task-sizing--containercpu--containermemory)
has the full CPU↔memory pairing table.

> **Local runs** read the same environment variables — set them in your shell or `.env`
> before launching `mysql-dsql-migrator ui`.

---

## 7.3 Minimizing impact on the source

**The source read is already light:** keyset streaming (no `OFFSET` re-scans), one page in
flight per table, no global lock, and scan-free row-count estimates instead of `COUNT(*)`.
What is left to manage is concurrent read pressure, and one setting controls almost all of
it.

### The one lever: table parallelism

`DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` (default **4**, max **16**) sets how many
tables are read from the source at once, one streaming connection each.
`BATCH_PARALLELISM` and `BATCH_ROWS` raise Aurora DSQL write pressure, not source read
load, so leave them unless Aurora DSQL is the bottleneck. An opt-in `Threads_running` cap
is a second safety valve (see the note below). Set the parallelism under **Settings → Full
Load** to experiment between runs, or as the environment variable to persist it (§7.2).

### Start conservative, then tune under observation

Treat parallelism as a **throttle, not a throughput dial** — ramp it against measured
headroom, not the number of tables:

1. **Start low (2–4)** even on a large instance. Finishing slower is far cheaper than
   causing a production latency incident.
2. **Watch the source** over a sustained 5–10 min window (not a single spike).
3. **Raise in modest steps** only when there's clear headroom, and re-observe.
   (Parallelism is read once per run, so a change applies to the **next** run.)

**Signals to watch** (Amazon RDS/Aurora — CloudWatch + Performance Insights):

- **Green light to raise:** `CPUUtilization` well below the line, `ReadLatency` flat as
  `ReadIOPS` rises, `DiskQueueDepth` low, `BurstBalance` / `EBSIOBalance%` /
  `CPUCreditBalance` near 100%, Aurora `BufferCacheHitRatio` steady (~99%).
- **Back off now:** `CPUUtilization` sustained > ~85–90%; `ReadLatency` inflating 2–5×
  while IOPS plateaus; any burst/credit balance **trending toward 0** (throttle *before*
  it hits zero — recovery is slow); `FreeableMemory` near zero / `SwapUsage`; Aurora
  `BufferCacheHitRatio` dropping (your cold scans are evicting production's hot pages);
  and above all, **your application's own query latency rising** — the ultimate back-off
  trigger regardless of instance metrics.

> [!note] Source-load safety valve There is **no QPS rate limiter**, but there **is** an
> opt-in concurrency throttle: `DSQL_MIGRATOR_FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`
> (default `0` = off, also settable in **Settings → Full Load**) pauses new source reads
> while the source's global `Threads_running` exceeds the ceiling — a gh-ost-style
> `--max-load` guard. Otherwise, lowering table parallelism is your control. Schedule the
> bulk load **off-peak** and avoid overlapping the instance's backup/maintenance window (a
> snapshot plus full scans is the worst case for gp2 `BurstBalance`).

### Pre-flight headroom checklist

Baseline these over a representative peak, then confirm there's room to add the load:
**CPU** headroom; **storage** — gp2 `BurstBalance` (a sustained scan can drain it to zero
and collapse to baseline IOPS — the most common way to hurt production), plus
`ReadIOPS`/`ReadThroughput`/`ReadLatency`/`DiskQueueDepth` (Aurora: `VolumeReadIOPS`);
**buffer pool** `BufferCacheHitRatio`; **connections** `DatabaseConnections` vs
`max_connections` (16-way + your app pool must stay under the limit with margin); **free
space / memory**. For a busy RDS source, gp3 with headroom IOPS removes the gp2 burst
cliff that most often bites full scans.

### Large tables on a write-heavy source

Each table's read runs in a `REPEATABLE READ` snapshot held open for that table's whole
read, even on a MySQL source configured `READ-COMMITTED` (the tool sets its own sessions
to `REPEATABLE READ`,
[§3.2](03-full-load.md#32-streaming-export--bounded-memory-at-any-table-size)); Validation
does the same for each table it compares. On a write-heavy source an open read view keeps
InnoDB from purging undo history (**History List Length**), so undo and disk usage grow
and reads slow down, and with many tables in flight the **oldest** read view sets the
purge horizon. To limit it:

- Lower the parallelism, so fewer read views are open.
- Split the biggest tables by primary-key range into separate, shorter runs. In Full load
  + CDC, *Reader shards per large table* does this within one run, so each read view lasts
  only while its own range is read, at the cost of concurrent reads of that table (§7.2).
- Monitor History List Length, and provision free-space headroom before you start so undo
  growth cannot trip a storage-full condition.

### What about reading from a read replica?

Pointing Full Load at an **RDS read replica / Aurora reader endpoint** moves all the scan
IOPS, CPU, and buffer-pool churn off the primary — the strongest way to protect
production, and it lets you push parallelism harder.

|  | Full Load only | Full Load + CDC |
|---|---|---|
| **Read from** | A **read replica** (gp3, up-sized for the window if needed) | The **primary/writer**, never a replica |
| **Start parallelism at** | 4–8, then ramp only on clear headroom | 2–4, then ramp only on clear headroom |
| **Also** | The snapshot reflects a slightly older point (replica lag): normal | Run off-peak, and set binlog retention to outlast the entire Full Load |

**Why not a replica with CDC?** The tool captures the CDC handoff **watermark** (binlog
file/position, GTID, `server_uuid`) from the same source you connect to, and the CDC
connector streams from that same host. A replica's binlog coordinates live in a
**different namespace** than the primary's, and RDS replicas often have binlog disabled or
unretained, so a replica-sourced watermark can silently produce a **CDC data gap**
(changes between the snapshot point and CDC start are lost). Set the retention with
`CALL mysql.rds_set_configuration('binlog retention hours', N)`. Either way, shard the few
largest tables into PK-range runs.

---

## 7.4 Tuning an individual query

Beyond the parallelism knobs above, you can tune an individual query against Aurora DSQL's
distributed execution model — where the primary key *is* the table, filter pushdown drives
cost, and **DPU** (not PostgreSQL's `cost=`) is the unit. The optional **Query Converter**
converts a MySQL query, probes it read-only with `EXPLAIN` / `EXPLAIN ANALYZE`, and — with
AI assist on — an **AI DBA** rewrites it for DSQL efficiency and re-tests the rewrite to
prove the DPU improvement.

> See [Chapter 9 — The Query Converter and the AI DBA](09-query-validation.md) for the
> full workflow.

---

## 7.5 A measured example — one run that backs §7.1 and §7.2

Below is one run on live infrastructure, done to check whether the design rationale above
actually shows up in practice. Read it **as an illustration of the method, not as a
performance spec or guarantee** — anyone can reproduce it in their own environment with
`scripts/measure_performance.py`.

> [!note] The conditions these numbers came from RDS MySQL 8.0.42 source + Aurora DSQL
> target + MSK, `us-east-1`, a single run on hardware that was not pinned (source RDS
> class, Fargate/local CPU and memory, DSQL warm state, network RTT), so absolute
> throughput and lag are environment-dependent. The run also used an `AUTO_INCREMENT`
> integer-PK schema, the **hot-partition worst case** from §7.1; the cached identity key
> this chapter recommends lowers contention.

**Full Load — raising parallelism raises contention faster than throughput (the §7.2
guardrail).** Doubling both the table and batch parallelism (4×8 = 32 → 8×16 = 128
connections) bought only **~+5%** throughput, while the **share of batches that hit at
least one retry** rose by about a third (≈ 9.6% → 12.8%). Even for rows that don't
logically conflict, writes converge on the same key range of a monotonic PK — exactly the
hot-partition effect in §7.1. The takeaway: **fix the PK strategy before turning
parallelism up blindly.**

**CDC replication lag — a latency floor at the default sizing.** Source commit → visible
on DSQL measured at roughly p50 0.8s / p95 1.3s under a sustained load, and p50 0.5s / p95
0.7s under a burst. This is a **good-case floor at the default CDC sizing (MCU=1, one
worker, a single-column table)**; it can grow with many tables or a high change rate.
Scale it with the §7.2 CDC parameters when you need throughput.

> **Reproduce it:** `scripts/measure_performance.py full-load` (throughput + contention)
> and `scripts/measure_performance.py cdc-lag` (replication lag) give you the same metrics
> against your own source/target. Note that `full-load` **drops and recreates** the target
> tables (needs `--yes`) so use it **only on a non-production target**, and `cdc-lag`
> needs an active CDC pipeline.

---

## 7.6 The Settings dialog (tuning, diagnostics, activity log)

The knobs above are also editable **in the UI** without a restart: the **Settings** entry
(sidebar footer) opens a tabbed dialog with —

- **Full Load / CDC / Validation** tabs — the tunable knobs from §7.2 (table & batch
  parallelism, batch rows, the source-load throttle, CDC sink MCU, validation workers),
  each applied to the **next** run of that step.
- an **Activity log** tab — everything about the one activity/audit log in one place: set
  the app **log level** (DEBUG / INFO / WARNING / ERROR), toggle **"Mirror to CloudWatch
  Logs"** so the log is also streamed to the container's stdout (forwarded to CloudWatch
  on ECS — a durable copy that survives task replacement), and **download** the session's
  activity log (the same feed the AI DBA reads).

Changes made in the dialog **revert on restart**; to persist one, set its
`DSQL_MIGRATOR_*` environment variable (listed in §7.2) instead.

---

**Next:** [8. Testing and verification →](08-testing-and-verification.md)
