# 7. Performance, tuning, and why this design

_Language: **English** | [한국어](../ko/07-performance-and-tuning.md)_

> **Prev:** [6. Limitations](06-limitations.md)

This chapter explains **why** the tool's data path is built the way it is —
grounded in how Aurora DSQL actually works — and **how** to tune its parallelism
for your workload. If you're evaluating whether to trust this tool for a
TB-scale migration, this is the technical case.

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

A stock JDBC sink retries the **whole batch** on `40001`. At TB scale with
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

CDC parallelism is set on the **cdc-stack CloudFormation parameters**, not app env
(the connectors run on managed MSK Connect, not in the app):

| CloudFormation parameter | Default | Effect |
|---|---|---|
| `SinkTasksMax` | 2 | Sink connector write parallelism (**capped by the topic partition count**). |
| `SourceTasksMax` | 1 | Debezium source tasks — MySQL is effectively single-task per server; leave at 1. |
| `ConnectorMcuCount` | 1 | MSK Connect compute units (MCUs) per worker (1/2/4/8). |
| `ConnectorWorkerCount` | 1 | MSK Connect workers per connector. |
| `SinkBatchMaxRows` | 3000 | Rows per DSQL write transaction in the sink (**do not exceed 3000**). |

CDC throughput scales with **MSK partitions × sink `tasks.max` × worker MCU/count**,
ultimately bounded by the partition count; the real ceiling under load is OCC on
hot primary keys — again, PK strategy matters most.

### On AWS (ECS Fargate) — yes, all of this is tunable there too

The Full Load and Validation knobs are ordinary `DSQL_MIGRATOR_*` **environment
variables**, read at run time by the app. In a Fargate deployment they are set in
the **ECS task definition's container `environment` block** (the same place the
template already sets `DSQL_MIGRATOR_LOG_LEVEL`, the `/tmp` state paths, etc.) —
add the keys above to `deploy/cloudformation.yaml`'s container environment (or your
own task definition) and redeploy. The CDC knobs are cdc-stack CloudFormation
parameters, passed when the tool deploys the cdc-stack. Also size the **Fargate
task CPU/memory** (`ContainerCpu` / `ContainerMemory`) to match the parallelism:
~1 vCPU / 2 GiB is a reasonable starting point for a multi-table Full Load, since
memory is bounded by `table_parallelism × batch_parallelism × ~8 MiB` of row
buffers, not by table size.

> **Local runs** read the same environment variables — set them in your shell or
> `.env` before launching `mysql-dsql-migrator ui`.

---

## 7.3 Tuning an individual query

Beyond the parallelism knobs above, you can tune an individual query against Aurora
DSQL's distributed execution model — where the primary key *is* the table, filter
pushdown drives cost, and **DPU** (not PostgreSQL's `cost=`) is the unit. The
optional **Query Playground** converts a MySQL query, probes it read-only with
`EXPLAIN` / `EXPLAIN ANALYZE`, and — with AI assist on — an **AI DBA** rewrites it
for DSQL efficiency and re-tests the rewrite to prove the DPU improvement.

> See [Chapter 9 — Query validation and the AI DBA](09-query-validation.md) for the
> full workflow.

---

## 7.4 A measured example — one run that backs §7.1 and §7.2

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
