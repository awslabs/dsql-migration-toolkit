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

## Summary: 200GB single-table load time estimate

| Version | Approach | rows/s | 200GB estimate | Improvement |
|---|---|---|---|---|
| Pre-v0.1.67 | ThreadPool, page=1000 | ~4,000 | ~46 hours | — |
| v0.1.67 | ThreadPool, code optimizations | ~6,000 | ~31 hours | 1.5× |
| v0.1.67 | ThreadPool, 8 vCPU | ~15,000 | ~12 hours | 3.8× |
| **v0.1.68** | **ProcessPool, tp=4, 8 vCPU** | **~41,000** | **~4 hours** | **10×** |
| **v0.1.68** | **ProcessPool, tp=8, 8 vCPU** | **~51,000** | **~2.5 hours** | **18×** |

> Estimates assume ~300 bytes/row average. Actual times vary by row width, network
> latency, DSQL cluster load, and OCC collision rate.

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
| `ConnectorMcuCount` | cdc-stack (inferred) | MSK Connect compute units per worker (1/2/4/8). |
| `SinkBatchMaxRows` | cdc-stack (3000, fixed) | Rows per DSQL write transaction (DSQL's hard limit). |
| `consumer.max.poll.records` | sink worker config | Records handed to one `put()` — bounds how many the sink can coalesce into one JDBC `executeBatch`. |
| `max.batch.size` / `max.queue.size` | source connector | Binlog events drained per streaming iteration / reader→producer queue depth. |
| `producer.batch.size` / `linger.ms` / `compression.type` | source worker config | Size, fill-delay, and compression of the Kafka produce batch. |

The connector-scaling knobs (partitions / `SinkTasksMax` / `ConnectorMcuCount`) are
**inferred from the captured-table count** and not exposed in the UI — see
[§7.2 → CDC](07-performance-and-tuning.md#72-tuning-parallelism).

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
| 4: source tuning (**plugin v14**) | 8 partitions / 8 tasks | ~1,500 | 6.5% | DSQL write contention | **5.1×** |

Two code/config changes did most of the work:

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

At 8 partitions the sink reached ~1,500 rows/s (DSQL apply cross-checked at 1,484
rows/s) — but scaling 4→8 gave only **~1.4× (sublinear)**: concurrent upserts to one
table begin to contend inside DSQL. This is exactly why the smart default caps
effective parallelism at 8.

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

6. **The sink is latency-bound, not CPU-bound.** At ~5–7% CPU the sink was waiting
   on DSQL round-trips, not computing — so the lever is *fewer, larger* writes
   (batched `executeBatch`), not more compute.

7. **Batching per-row round-trips is the biggest single CDC win** (~550 → ~1,165
   rows/s from plugin v13 alone).

8. **The source was never the ceiling.** Producer tuning (batch/queue/`lz4`) took
   it 16× to ~31,000 rec/s; a single Debezium task per MySQL server is plenty.

9. **Sink parallelism scales sublinearly.** 4 → 8 partitions gave ~1.4×, not 2×, as
   concurrent upserts to one table contend inside DSQL — so effective parallelism is
   capped at 8 in the smart default.

10. **Partition count is irreversible**, so the tool infers it at create time from
    the captured-table count rather than exposing a UI knob that could be set wrong
    permanently.

---

## Reproducing these measurements

```bash
AWS_REGION=us-east-1 \
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
