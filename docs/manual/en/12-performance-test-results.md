# Appendix: Performance test results

_Language: **English** | [한국어](../ko/12-performance-test-results.md) | [日本語](../ja/12-performance-test-results.md)_

> **Prev:** [11. Customer FAQ](11-customer-faq.md)

This appendix documents the throughput measurements taken during development for
both data paths — **Full Load** (the tool's Python bulk loader) and **CDC** (the
Debezium → MSK → custom DSQL sink pipeline) — showing how each optimization stage
contributed to the final performance. All measurements were run on **ECS Fargate**
(and, for CDC, managed **MSK Connect**) in the same VPC as the source RDS MySQL and
target Aurora DSQL (sub-ms network RTT).

---

## Test environment

| Component | Configuration |
|---|---|
| **ECS Fargate** | 8 vCPU (8192 CPU units), 16 GB memory |
| **Source** | Aurora MySQL (RDS), `customers_sample` schema |
| **Target** | Aurora DSQL, us-east-1 |
| **Tables** | `order_items` (33.6M rows), `orders` (8.5M), `payments` (8.4M), `customers` (small) |
| **Measurement** | `scripts/measure_performance.py full-load` with CloudWatch progress monitoring |

---

## Evolution of Full Load throughput

### Stage 1: Baseline (ThreadPool, GIL-bound)

The initial implementation used `ThreadPoolExecutor` for table-level parallelism.
Due to Python's GIL, only one CPU core was utilized regardless of vCPU count.

| Configuration | rows/s | CPU | Notes |
|---|---|---|---|
| 0.5 vCPU, tp=4, bp=8, page=1000 | 4,243 | 50% | Original default settings |
| 4 vCPU, tp=4, bp=8, page=1000 | 9,732 | 113% | More vCPU → only scheduling relief |
| 8 vCPU, tp=2, bp=8, page=5000 | 12,277 | 110% | Code optimizations (v0.1.67) |

**Diagnosis:** CPU locked at ~110% (1 core) on any vCPU count = GIL signature.

### Stage 2: Code optimizations (v0.1.67, still GIL-bound)

Optimizations that reduce GIL hold time per row:

| Optimization | Effect |
|---|---|
| MySQL keyset page size 1000 → 5000 | 5× fewer source round-trips |
| `build_insert_statement` SQL template cached | ~40K object allocations eliminated per batch |
| `_iter_batches` lazy byte estimation | 90%+ of `_estimate_row_bytes` calls eliminated |
| `_flatten_params` list comprehension | ~40% faster parameter serialization |
| `convert_row` passthrough fast path | Most columns skip `convert_value` entirely |

**Result:** +41% improvement (4,243 → 6,000 rows/s at 0.5 vCPU). Still GIL-bound.

### Stage 3: Multi-process parallelism (v0.1.68)

`ThreadPoolExecutor` replaced with `ProcessPoolExecutor` — each worker process
has its own GIL, its own MySQL connection, and its own DSQL connection pool.

| Test | Tables | tp | rows/s | CPU | vs baseline |
|---|---|---|---|---|---|
| A: ThreadPool | 2 (orders, payments) | 2 | 12,277 | 110% | 1× |
| B: ProcessPool Phase 1 | 2 (orders, payments) | 2 | 22,365 | 207% | **1.82×** |
| C: ProcessPool Phase 1 | 4 (all) | 4 | 32,270 | 311% | **2.63×** |
| D: ProcessPool + PK shard | 1 (order_items) | 4 | 41,000 | 415% | **3.34×** |
| E: ProcessPool + PK shard | 1 (order_items) | 8 | 51,000 | 777% | **4.15×** |
| F: Unified pool (old, no shard in mixed) | 4 (all) | 4 | 19,500 | 179% | 1.59× |
| **G: Unified pool + auto shard** | **4 (all)** | **8** | **34,800** | **561%** | **2.83×** |

### Stage 4: Replace path optimization

When loading into a freshly DROP+recreated table (empty target), the loader uses
plain `INSERT` (no `ON CONFLICT`) which eliminates OCC contention entirely:

| Test | rows/s (sustained) | rows/s (peak) | CPU |
|---|---|---|---|
| ProcessPool + shard, SKIP_EXISTING (append) | 35,000 | 35,333 | 439% |
| ProcessPool + shard, NONE (replace/empty target) | **41,000–51,000** | **67,000** | 777% |

---

## Large-scale validation: 1TB multi-table Full Load (16 vCPU, composite PK)

The sections above traced the optimization evolution at 8 vCPU. This section is a
large-scale validation that actually ran a **~1TB dataset to completion** at
**maximum parallelism (16 vCPU)** (2026-07, us-east-1). The deployed tool was driven
by **automated scripting (ECS RunTask)**, not by clicking the UI.

### Test environment (1TB)

| Component | Setting |
|---|---|
| **ECS Fargate** | 16 vCPU (16384 CPU units), 32 GB |
| **Source** | Aurora MySQL `db.r7g.8xlarge` (temporarily upsized for the test) |
| **Target** | Aurora DSQL, us-east-1 |
| **Dataset** | `dsql_test_multi` — 20 tables × 45.78M rows = **915.7M rows (≈ 1.07TB)** |
| **Loader settings** | composite PK (`dist_key`) on all 20, `TABLE_PARALLELISM=16`, `BATCH_PARALLELISM=32`, batch-rows 3000 |

### Result

| Metric | Value |
|---|---|
| **Completion** | **20/20 tables, 0 failures** |
| **Total wall time** | **8,851.5s = 2h27m32s** |
| **Average throughput** | 103,455 rows/s |
| **OCC 40001 retries** | **0** (composite PK removed hot-partition contention entirely) |
| **Bottleneck** | CPU (16 vCPU saturated) |

- **Confirmed at scale that a composite PK removes the hot-partition bottleneck.** A
  monotonic AUTO_INCREMENT PK funnels writes into the trailing partition and provokes
  OCC contention; prepending a high-cardinality column spreads the writes, so 40001
  retries were **exactly 0**.

### Tail penalty — the "more tables than parallelism" imbalance

The 8,851s includes a tail cost from the **20 tables / 16 slots** imbalance:

| Phase | What | Throughput |
|---|---|---|
| front-16 parallel | 16 tables = 732.6M rows in ~5,562s (16 cores saturated) | **~131K rows/s** ← true max parallel |
| back-4 tail | remaining 4 tables use only 4 of 16 slots, +~3,290s (12 cores idle) | ~21K rows/s (combined) |

- **Balanced (tables ≤ `TABLE_PARALLELISM`) this would be ~131K rows/s → 915.7M rows
  ≈ ~1h56m.** The tail imbalance added ~31 minutes.
- **Lesson:** when the table count is ≤ vCPU, set `TABLE_PARALLELISM` to **at least the
  table count** to avoid serializing the tail. Large tables are auto-split by the
  loader (PK sharding) to fill the remaining cores.

### Two connection storms exposed at max-parallelism startup/transition (v0.1.115 / v0.1.116)

With 16 workers starting and transitioning at once, two storm classes that smaller
tests never surfaced appeared and were each fixed. Both stem from **DSQL's ~100
new-connections/second limit** and its **one-DDL-per-transaction + OCC** model:

| | BUG-A (connection storm) | BUG-B (DDL catalog storm) |
|---|---|---|
| When | front-16 finish → back-4 start (transition) | right at startup (16 workers start together) |
| Cause | per-table DROP+recreate **connection open** was outside any retry → `ConnectionTimeout` under a new-connection burst | 16 workers issue DDL against the same schema catalog at once → OC001 (40001) contention exhausts the DDL retry budget |
| Symptom | that table fails with rows=0, 0 OCC batches, no give-up log | `SerializationFailure: schema has been updated by another transaction` |
| Fix | **v0.1.115** — wrap every DSQL connection open in the transient-connection retry | **v0.1.116** — DROP+recreate all replace tables in a **serial pre-pass** before spawning workers (workers no longer re-run the DDL) |

After both fixes, a rerun **completed 20/20 with 0 failures**, validating them in
production. Lesson: **as you scale out in parallel, concurrent connection and DDL
initiation (startup/transition) hit a limit before row loading itself does** — every
DSQL connection open and every DDL must tolerate the rate limit / OCC.

### Variant: a single huge table (1TB in one table)

Same 16 vCPU, but the data lives in **one table** `big_events` (915.7M rows ≈ 1.07TB,
BIGINT AUTO_INCREMENT PK) instead of 20. The engine detects the integer PK and
auto-splits the table into **16 PK-range shards, one per core**.

| Metric | Value |
|---|---|
| **Completion** | all 915.7M rows loaded |
| **Total wall time** | **~2h10m** (faster than the 20-table 2h27m — 16 even shards, no tail penalty) |
| **Throughput** | ~16K at first → **~120–150K rows/s** after ramp (CPU saturated) |
| **OCC 40001** | 0 |

- **Key insight — a fresh single table's partition warm-up.** A just-created DSQL
  table starts with one partition, so even with 16 shards writing at once, the writes
  initially funnel into that single partition and the load **starts slow (~16K
  rows/s)**. As DSQL **splits the table into more partitions under load**, throughput
  climbs ~16K → 46K → 97K → **120–150K (CPU-saturated)**. The multi-table load never
  hit this because **20 tables = 20× the initial partitions** — spreading data across
  tables gives DSQL write parallelism from the start.
- On this path we found and fixed a **shard result-aggregation bug** (it referenced a
  non-existent `result.rows_skipped`, wrongly marking a fully-loaded single table
  `FAILED`) in **v0.1.119** (`rows_skipped` now maps from `conflicts`). Multi-table
  loads (one worker per table, unsharded) were unaffected.

---

## CDC throughput

CDC is a different pipeline with a different bottleneck. Full Load is a Python
process that is **CPU/GIL-bound**; CDC is `Debezium (source) → MSK topic → custom
DSQL sink`, where the sink is **DSQL-write-latency-bound**. These measurements
(2026-07-08) drove the shipped connector code (`dsql-sink` plugin) and the smart
defaults in [§7.2](07-performance-and-tuning.md#72-tuning-parallelism).

### Parameters that affect CDC throughput

| Parameter | Where | Effect |
|---|---|---|
| `topic.creation.default.partitions` | cdc-stack (inferred) | Sink's unit of parallelism — one sink task consumes one partition. **Irreversible** (raise-only). |
| `SinkTasksMax` | cdc-stack (inferred) | Sink connector write parallelism; effective value capped by the partition count. |
| `ConnectorMcuCount` | cdc-stack (default 2, env-overridable) | MSK Connect compute units per worker (1/2/4/8). |
| `SinkBatchMaxRows` | cdc-stack (3000, fixed) | Rows per DSQL write transaction (DSQL's hard limit). |
| `consumer.max.poll.records` | sink worker config | Records handed to one `put()` — bounds how many the sink can coalesce into one JDBC `executeBatch`. |
| `max.batch.size` / `max.queue.size` | source connector | Binlog events drained per streaming iteration / reader→producer queue depth. |
| `producer.batch.size` / `linger.ms` / `compression.type` | source worker config | Size, fill-delay, and compression of the Kafka produce batch. |

The connector-scaling knobs (partitions / `SinkTasksMax`) are **inferred from the
captured-table count** and not exposed in the UI; `ConnectorMcuCount` is a fixed
default (`CDC_DEFAULT_MCU_COUNT` = 2, env-overridable), not derived from the table
count — see [§7.2 → CDC](07-performance-and-tuning.md#72-tuning-parallelism).

### Test environment (CDC)

| Component | Configuration |
|---|---|
| **Source connector** | Debezium MySQL on MSK Connect, `ConnectorMcuCount`=4 |
| **Sink connector** | custom `dsql-sink`, `SinkTasksMax` scaled 4→8 |
| **Workload** | 4 ECS tasks bulk-inserting into `customers_sample.orders` (~20,000 rows/s into the source) |
| **Measurement** | CloudWatch `AWS/KafkaConnect` `SourceRecordWriteRate` / `SinkRecordSendRate`, cross-checked by the DSQL target row-count delta |

### Evolution of CDC throughput

| Stage | Config | Sink rows/s | Sink CPU | Bottleneck | vs baseline |
|---|---|---|---|---|---|
| 1: single partition | 1 partition / 1 task | 292 | — | partition count = 1 (no parallelism) | 1× |
| 2: partitioned | 4 partitions / 4 tasks | ~550 | 5% | sink applied **one row per round-trip** | 1.9× |
| 3: batched apply (**plugin v13**) | 4 partitions / 4 tasks | ~1,165 | 7% | source (under-tuned producer) | **4.0×** |
| 4: source tuning (**plugin v14**) | 8 partitions / 8 tasks | ~1,500 | 6.5% | hidden per-row metadata round-trip | **5.1×** |
| 5: multi-row rewrite (**plugin v15**) | 8 partitions / 8 tasks | ~1,925 | ~10% | hidden per-row metadata round-trip | **6.6×** |
| 6: metadata once/statement (**plugin v16**) | 8 partitions / 8 tasks | **~18,672** | ~65% | source / workload feed | **64×** |

Four code/config changes did most of the work:

- **Plugin v13 — batched sink apply.** The sink coalesces each maximal run of
  consecutive same-SQL change events into one JDBC `executeBatch()` instead of a
  per-row `executeUpdate()`. Because DSQL is latency-bound (each statement is a
  distributed round-trip; the task ran at ~5% CPU), collapsing per-row round-trips
  into batched sends **doubled** sink throughput (~550 → ~1,165 rows/s). Also raised
  `consumer.max.poll.records` 500 → 3000 so a full poll fills one ≤3000-row
  transaction.
- **Plugin v14 — source producer tuning.** Larger batch/queue + `lz4`-compressed
  producer batches took the source from ~1,940 → **~31,000 rec/s (16×)**, proving
  the source was never the real ceiling — it was under-batched. This exposed the
  sink→DSQL write as the true final bottleneck.
- **Plugin v15 — multi-row INSERT rewrite.** Enabling pgjdbc
  `reWriteBatchedInserts=true` collapses each `executeBatch` into a single multi-row
  `INSERT ... VALUES (..),(..) ON CONFLICT` — N execute round-trips → 1 — lifting the
  sink from ~1,500 → **~1,925 rows/s (+30%)**. Made safe by deduping each same-SQL
  run to one row per PK first (a rewritten multi-row `ON CONFLICT` rejects a
  duplicate conflict key).
- **Plugin v16 — fetch parameter metadata once per statement.** The real ceiling
  turned out **not** to be DSQL-side write contention (as v14/v15 assumed) but a
  hidden client round-trip: `bind()` called `getParameterMetaData()` for every row,
  and on pgjdbc that is a server Parse/Describe — one read-only transaction *per
  applied row*. DSQL's `ReadOnlyTransactions` sat at ~115,000/min (≈60× the write
  rate) while `OccConflicts` was flat **0**, disproving the contention theory.
  Fetching the metadata once per prepared statement took the sink from ~1,925 →
  **~18,672 rows/s (≈9.7×)**, cut read-only transactions ~150×, and lifted sink CPU
  10% → ~65% (now doing real work, not waiting on round-trips).

**On the (disproven) contention theory.** Under v14/v15 the sink plateaued near
~1,500–1,925 rows/s and scaling 4→8 partitions gave only ~1.4×, which *looked* like
DSQL-side write contention. It was not: `OccConflicts` was 0 throughout. The plateau
was the per-row metadata round-trip above; once removed, the same 8 partitions ran
~9.7× faster and the bottleneck moved to the source / workload feed (~20,000 rows/s).
The lesson: **low sink CPU + sublinear partition scaling does not prove server-side
contention** — a hidden client round-trip produces the same symptoms. DSQL's
`OccConflicts` / `ReadOnlyTransactions` metrics settle it directly. A composite,
partition-spreading PK helps only once `OccConflicts` actually rises — not here.

### Sizing the sink independently of the source

Once the per-row round-trip was gone (v16) the sink became **CPU-bound** (~80% at 4
MCU / ~21,000 rows/s), while the single-task Debezium source has spare CPU. So the
sink's MSK Connect compute is a separate knob (`SinkMcuCount`, default 4) from the
source's (`ConnectorMcuCount`). Raising the sink to **8 MCU** took it to ~26,000
rows/s at ~34% CPU. Beyond that the source (a single binlog reader) and the source
DB's own capacity become the limit — a small source instance can even bottleneck CDC
because it serves the write workload *and* Debezium's binlog read at once (a 2 vCPU
source ran at 93% CPU and capped the pipeline until it was scaled up).

### Surviving a source reboot (resilience, not throughput)

A production CDC pipeline runs for weeks and **will** see the source reboot
(maintenance patch, failover, instance-class change). The Debezium source connector
sets `errors.retry.timeout` to 10 minutes so a reboot is absorbed automatically: the
binlog stream is cut, the task retries across the reboot window, and once the source
is back it **resumes from the committed binlog offset — gapless, no operator
action**. (With the Kafka Connect default of `0` the task would be killed on the
first failed restart and stall silently at `SourceRecordWriteRate=0` until a manual
Stop/Start — verified fixed by rebooting the source mid-stream and watching the sink
catch up with no gap.)

---

## Key findings

### Full Load

1. **GIL is the ceiling in Python-based data pipelines.** Even with I/O-releasing
   C extensions (psycopg3), the per-row Python conversion dominates and serializes
   on one core.

2. **ProcessPoolExecutor with `spawn` context is the correct GIL bypass.** Each
   worker builds its own MySQL engine + DSQL connector — no cross-process row
   transfer needed (only progress counters via IPC).

3. **OCC contention scales with concurrent writers on existing data.** 32 writers
   hitting the same rows with `ON CONFLICT DO NOTHING` can livelock. Plain INSERT
   into an empty table (after DROP+recreate) eliminates this entirely.

4. **Throughput ceiling shifts from CPU to DSQL write capacity at tp=8.** Beyond
   ~8 concurrent writer processes, the bottleneck moves from Python CPU to DSQL
   server-side write throughput (~67K rows/s peak observed).

5. **Optimal configuration:** set `TABLE_PARALLELISM` = vCPU count. The loader
   automatically shards large tables and allocates pool slots.

### CDC

6. **The sink was latency-bound: every avoided round-trip is a win.** At low CPU the
   sink was waiting on DSQL round-trips, not computing — so the lever is *fewer*
   round-trips (batched `executeBatch`, multi-row rewrite, and — the big one —
   killing the per-row metadata round-trip), not more compute.

7. **A hidden per-row round-trip was the real ceiling, ~9.7× when removed.**
   `getParameterMetaData()` per row is a server Parse/Describe on pgjdbc; hoisting it
   to once-per-statement took the sink ~1,925 → ~18,672 rows/s. Batching (v13/v15)
   only paid off fully once this was gone.

8. **The source was never the ceiling.** Producer tuning (batch/queue/`lz4`) took
   it 16× to ~31,000 rec/s; a single Debezium task per MySQL server is plenty.

9. **Low CPU + sublinear partition scaling ≠ server-side contention.** The 4→8
   partition ~1.4× plateau looked like DSQL write contention but wasn't —
   `OccConflicts` was 0; the cause was the per-row round-trip. Always check DSQL's
   `OccConflicts` / `ReadOnlyTransactions` before blaming the server. A composite,
   partition-spreading PK helps only once `OccConflicts` actually rises.

10. **Partition count is irreversible**, so the tool infers it at create time from
    the captured-table count rather than exposing a UI knob that could be set wrong
    permanently.

---

## Reproducing these measurements

```bash
AWS_REGION=us-east-1 \
DB_HOST=<rds-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
TARGET_ENDPOINT=<dsql-cluster-endpoint> \
MEASURE_SCHEMA=customers_sample \
MEASURE_TABLES="order_items orders payments customers" \
TABLE_PARALLELISM=8 \
BATCH_PARALLELISM=8 \
deploy/run_measure_on_fargate.sh
```

See [`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh)
for the full A/B measurement harness and
[`scripts/measure_performance.py`](../../../scripts/measure_performance.py) for
the in-process throughput + OCC reporting tool.

For CDC, deploy the cdc-stack and start a steady insert workload on the source,
then read the pipeline rates from CloudWatch `AWS/KafkaConnect`
(`SourceRecordWriteRate` on the `-debezium-source` connector, `SinkRecordSendRate`
on the `-dsql-sink` connector), cross-checking against the DSQL target's
`COUNT(*)` delta over a fixed interval.

---

**Prev:** [11. Customer FAQ](11-customer-faq.md)
