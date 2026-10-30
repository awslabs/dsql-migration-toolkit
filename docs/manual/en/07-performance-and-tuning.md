# 7. Performance, tuning, and why this design

_Language: **English** | [한국어](../ko/07-performance-and-tuning.md) | [日本語](../ja/07-performance-and-tuning.md)_

> **Prev:** [6. Limitations](06-limitations.md)

The defaults fit Aurora DSQL's limits and keep the load on your source modest, so **most
migrations need no tuning**: a large table is already read and written in primary-key
ranges, and the batches in flight are capped where Aurora DSQL stops gaining from more.
For a large run, set *Tables in parallel* to the task's vCPU count
([§7.2](#72-tuning-parallelism)); if the source serves production traffic, start at 2–4
instead and raise it while you watch the source
([§7.3](#73-minimizing-impact-on-the-source)). What the tool already does to fit Aurora
DSQL is in [§7.1](#71-what-the-tool-does-for-you-on-aurora-dsql).

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
| Storage is partitioned by primary key, so a primary-key-ordered stream of inserts goes to one key range at a time: measured, about 20,000 rows/s whatever the batch size or the batches in flight | Reads a large table in primary-key ranges, so its writes go to several key ranges at once, and caps the batches in flight at 2 per key range (at least 4 per table or part) and 128 per run: more would only wait (§7.2) |
| `CREATE INDEX ASYNC` builds an index in the background | Creates a table's secondary indexes while it is empty, before any row: Schema Conversion's apply does, and a **Drop & reload** does again and waits up to 10 minutes for the builds before loading. Measured, an index builds far faster on the empty table, so the load and its index builds end sooner (§7.5) |
| IAM tokens last about 15 minutes, and connections end after 60 | Renews the token before it expires, and at once if Aurora DSQL refuses it, so an hours-long load or CDC stream never stalls on authentication |
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
single keys."** A MySQL `AUTO_INCREMENT` key is monotonic, so every new row targets the
same rightmost key range: the application's inserts after cut over (and CDC's while it
streams them) converge on one partition, and write latency climbs **even though the rows
never logically conflict**. The Full Load spreads a large table's writes over key ranges
itself ([§7.2](#72-tuning-parallelism)).

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
column. Measured on Aurora DSQL, bigger batches and more batches in flight do not move a
hot key range (one took about 20,000 rows/s whatever was in flight); only spreading the
writes across key ranges does. The Full Load does that for a large table by reading it in
key ranges (§7.2); a composite key does it for every write to the table, the
application's after cut over and CDC's included. If the load is bound by something else,
such as client CPU, a composite key buys nothing: measure first (see
[§7.5](#75-measured-on-live-infrastructure--what-backs-71-and-72)).

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
**Settings** (§7.6); **CDC** is sized for you. For a Full Load the setting that matters is
*Tables in parallel*: the key ranges and the batches in flight are already sized from the
measurements in §7.5.

### Full Load

| Setting (env var) | Default | Bound | Effect |
|---|---|---|---|
| `DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM` | 4 | ≤ 16 | How many tables, or parts of a large table, load at once: one worker process and one source connection each. Set it to the task's vCPU count for a large run. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM` | 8 | ≤ 32 | Batches in flight per table or part, each on its own Aurora DSQL connection. The tool also caps them at 2 per key range (at least 4 per table or part) and 128 per run, so for a table read in one or two key ranges a value above 4 changes nothing. |
| `DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS` | 2000 | ≤ 3000 | Rows per batched write (hard-capped at DSQL's 3000-row limit). |
| `DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS` | `0` (automatic) | ≤ 32 | How many primary-key ranges (K) a **large** table is read and written in ([§3.8](03-full-load.md#38-parallel-loading)). `0`: one range per 2,500,000 estimated rows from 5,000,000, at most 32; `1`: off; `2`–`32`: exactly that many. Needs *Tables in parallel* above 1 and an integer leading primary-key column. Also *Reader shards per large table* in **Settings → Full Load**. |
| `DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS` | 1000000 | — | A table must have at least this many (estimated) rows to be split (automatic also needs 5,000,000); smaller tables use one reader. |

> **Key ranges: why they help, and what they cost.** Aurora DSQL writes one key range of a
> table at about 20,000 rows/s whatever the batch size or the batches in flight, so a
> large table loads faster with its writes spread over several key ranges (a 150M-row
> PostgreSQL table in 32 ranges loaded at about 205,000 rows/s, §7.5). How the ranges are
> read depends on the source:
>
> - **PostgreSQL:** the ranges share one snapshot and are read by as many worker processes
>   as the table's share of the run's estimated rows (at most *Tables in parallel*).
> - **MySQL:** one reader session reads every range from one snapshot, so the table takes
>   one source connection and one worker process, which keeps about one core busy: a huge
>   MySQL table loads at one core's pace (about 25,000–55,000 rows/s in our runs,
>   depending on the host).
>   In **Full load + CDC** from a binlog start point, an explicit value of 2 or more reads
>   it in separate-snapshot parts instead, one process each, if every unique key includes
>   the leading key column (150M rows in 16 parts: about 164,000 rows/s overall, against
>   about 55,500 steady in one session). The copy is then consistent only once CDC
>   replays it, so the tool refuses the routes that would skip that replay: unticking the
>   table, switching to Full load only, or starting CDC from a later point
>   ([§3.8](03-full-load.md#38-parallel-loading)).
>
> **The trade-off:** on PostgreSQL, and for MySQL's separate-snapshot parts, K > 1 turns
> idle slots into concurrent reads of the same table for its whole read, so the source
> read rate, the DSQL connections and the memory rise up to `TABLE_PARALLELISM`-fold for
> that stretch, and a PostgreSQL source keeps the shared snapshot until the last of them
> has finished. On a source still serving production traffic, pair it with the
> `Threads_running` throttle (`FULL_LOAD_MAX_SOURCE_THREADS_RUNNING`, §7.3). A higher
> *Batches per table* does not speed up a key range (§7.5): to take a huge MySQL table
> past one core, use the separate-snapshot parts above.

> **Connection-quota guardrail.** A run's Aurora DSQL connections are at most its batches
> in flight, which the tool caps at 2 per key range (at least 4 per table or part) and 128
> per run, plus one per running table or part. DSQL allows up to **10,000 connections per
> cluster** but only **100 new connections/second**, so the caps keep a run well within the
> quota. More batches in flight than the key ranges absorb add only commit latency and
> Aurora DSQL compute cost (`ComputeDPU`), not rows/s: 4 per key range instead of 2 about
> doubled both at the same rows/s (§7.5).

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
> to its **Full Load** or **Validation** tab: the loader and validator re-read the config
> on every run, so a value set there applies to the **next** run immediately. It is
> app-wide and resets on restart, so put the values you want to keep in the task
> definition and use **Settings** to experiment.

Size the task for the load, and set *Tables in parallel* to its vCPU count: each table,
or part of a large table, that loads at once is one worker process. **Memory is a hard
limit.**

| Setting | Evaluation | A real large-scale Full Load |
|---|---|---|
| `ContainerCpu` | 0.5–1 vCPU (`512`–`1024`) | 4–16 vCPU (`4096`–`16384`) |
| `ContainerMemory` | `1024` MiB | ≥ `8192` MiB at 4 vCPU, ≥ `32768` at 16 vCPU (Fargate's minimum for that CPU) |

- **A MySQL load is CPU-bound on the task.** Reading and converting MySQL rows keeps about
  one core busy per running table or part, so throughput grows with the vCPUs up to
  *Tables in parallel* (16 tables of 20M rows on 16 vCPU: about 193,000 rows/s, every core
  busy).
- **A PostgreSQL load is bound by Aurora DSQL.** Its values pass through almost unchanged,
  so the task needs few cores (about 3.5 at 336,000 rows/s), and the throughput comes from
  the key ranges (above).
- **The host varies.** Fargate places a task on hosts with different CPUs, and the same
  MySQL load ran up to about 1.8× faster on one task than on another: judge a change over
  more than one run.
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
tables, or parts of a large table, are read from the source at once, one streaming
connection each (plus one on PostgreSQL while a table shares its snapshot).
`BATCH_PARALLELISM` and `BATCH_ROWS` change only the Aurora DSQL side, not the source read
load. An opt-in `Threads_running` cap is a second safety valve (see the note below). Set
the parallelism under **Settings → Full Load** to experiment between runs, or as the
environment variable to persist it (§7.2).

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
- Expect a large table's read view to last for its whole read: the automatic split reads
  every range in one session, from one snapshot. In Full load + CDC from a binlog start
  point, an explicit *Reader shards per large table* of 2 or more shortens it, so each
  read view lasts only while its own range is read, at the cost of concurrent reads of
  that table (§7.2) and of the refusals in [§3.8](03-full-load.md#38-parallel-loading).
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
`CALL mysql.rds_set_configuration('binlog retention hours', N)`.

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

## 7.5 Measured on live infrastructure — what backs §7.1 and §7.2

These runs check that the design above shows up in practice. Read them **as an
illustration of the method, not as a performance spec or guarantee**: the source, the
task's host and Aurora DSQL's state all move the numbers, and you can reproduce them in
your own environment with `scripts/measure_performance.py` (below).

> [!note] The conditions these numbers came from Seoul (`ap-northeast-2`), the tool on one
> ECS Fargate task of 16 vCPU / 120 GB, Aurora MySQL and Aurora PostgreSQL sources on
> `db.r8g.16xlarge`, and every setting at its default except *Tables in parallel* 16
> (Reader shards automatic unless a row says otherwise, Batches per table 8).

| Load | Result |
|---|---|
| PostgreSQL, 16 tables × 20M rows | about 336,000 rows/s overall, 600,000 in its steady phase |
| PostgreSQL, one table of 150M rows (32 ranges) | about 205,000 rows/s overall |
| MySQL, 16 tables × 20M rows | about 193,000 rows/s overall, the task's 16 vCPU saturated |
| MySQL, one table of 150M rows, Full load only (one session) | about 55,500 rows/s steady, one process on one core (stopped after 25 minutes, 75M rows in) |
| MySQL, the same table in Full load + CDC, Reader shards 16 | about 164,000 rows/s overall, beside an earlier copy's DROP (below) |

What they, and smaller runs on a 4 vCPU task with `db.r7g.large` sources, show:

- **One key range is the wall, not the batches.** One primary-key-ordered stream into a
  table took about 20,000 rows/s with 8 or 32 batches in flight and with 3,000-row batches
  alike; more in flight only raised Aurora DSQL's commit latency (26 ms at a light load,
  0.7 s at 8 in flight, 3 s at 32) and its compute cost (`ComputeDPU`, about 4×) at the
  same rows/s. An 80M-row PostgreSQL table read in 31–32 key ranges, two per process,
  loaded at about 150,000 rows/s with 2 batches in flight per range as with 4, at about
  half the commit latency (1.4–1.6 s against 2.8 s) and half the `ComputeDPU` (`TotalDPU`
  per row about 5% lower); so the tool keeps 2 per key range and at least 4 per table or
  part: one key range reached its 20,000 rows/s with 4. Spread over 4 key ranges, a
  10M-row table loaded at about 37,000–40,000 rows/s overall (54,000–60,000 at its peak),
  and two such tables, one per engine, at 127,000 together and still rising. Against the
  ~20,000 rows/s of one key range, the 150M-row table above loaded about 10× faster on
  PostgreSQL and 8× on MySQL in parts (overall), and 2.8× on MySQL in one session
  (steady).
- **Secondary indexes go on the empty table.** With 25 secondary indexes over 8.5M rows
  (MySQL), a Drop & reload that created them before the rows ended in 512 s end to end,
  against 873 s with the indexes built after the load. The rows loaded more slowly into
  the indexed tables (485 s against 262 s), but the builds took 13–41 s on the empty
  tables and 10–11 minutes after a load.
- **A fresh table warms up.** Aurora DSQL starts a new table on one partition and splits
  it as it grows, so a load's first minute or two runs at about one key range's rate:
  judge a short test by its steady phase.
- **An earlier copy's DROP keeps running.** Dropping a loaded table, as a Drop & reload
  does, is a background job on Aurora DSQL: it ran for minutes, and for over an hour for a
  75M-row copy, beside whatever loaded next.

**CDC replication lag — a latency floor.** Measured separately (RDS MySQL 8.0 source, MSK,
`us-east-1`, a sink of 1 MCU and one worker, a single-column table): source commit →
visible on DSQL at roughly p50 0.8s / p95 1.3s under a sustained load, and p50 0.5s / p95
0.7s under a burst. It can grow with many tables or a high change rate; scale it with the
§7.2 CDC parameters when you need throughput.

> **Reproduce it:** `scripts/measure_performance.py full-load` reports the throughput (per
> table and per time window), the write path's retries and latencies, and when Aurora DSQL
> finished the index builds; `--table-parallelism`, `--reader-shards` and `--cdc-mode` set
> up the rows above, and `scripts/measure_performance.py cdc-lag` measures replication lag
> (it needs an active CDC pipeline). `full-load` **drops and recreates** the target tables
> (needs `--yes`), so use it **only on a non-production target**. For a fair comparison,
> drop the previous copy first and let Aurora DSQL finish its background jobs
> (`--wait-sys-jobs` waits for them before the run starts).

---

## 7.6 The Settings dialog (tuning, diagnostics, activity log)

The knobs above are also editable **in the UI** without a restart: the **Settings** entry
(sidebar footer) opens a tabbed dialog with —

- **Full Load**, **CDC** and **Validation** tabs — the tunable knobs from §7.2, each tab
  opening with when its changes apply (*Changes apply to the next run.*; on the CDC tab,
  *the next Start CDC*):
  - **Full Load**: *Tables in parallel*, *Batches per table*, *Rows per batch*, *Reader
    shards per large table* and *Source-load throttle (max Threads_running)*, under the
    notice *Batches in flight are capped at 2 per key range (at least 4 per table or part)
    and 128 per run*;
  - **CDC**: *Sink compute (MCU)*;
  - **Validation**: *Tables in parallel*.
- an **Activity log** tab — everything about the one activity/audit log in one place: set
  the app **log level** (DEBUG / INFO), toggle **"Mirror to CloudWatch Logs"** so the log
  is also streamed to the container's stdout (forwarded to CloudWatch on ECS — a durable
  copy that survives task replacement), and **download** the session's activity log (the
  same feed the AI DBA reads).

Changes made in the dialog **revert on restart**; to persist one, set its
`DSQL_MIGRATOR_*` environment variable (listed in §7.2) instead.

---

**Next:** [8. Testing and verification →](08-testing-and-verification.md)
