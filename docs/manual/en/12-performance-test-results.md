# Appendix: Performance test results

_Language: **English** | [한국어](../ko/12-performance-test-results.md) | [日本語](../ja/12-performance-test-results.md)_

> **Prev:** [11. Customer FAQ](11-customer-faq.md)

This appendix records throughput measurements of both data paths — **Full Load** (the
tool's bulk loader) and **CDC** (Debezium → MSK → the custom DSQL sink). Read the numbers
as an illustration of what drives throughput and what to expect, **not as a guarantee**:
your hardware, schema, source and Aurora DSQL's warm state will differ. The runs used
**ECS Fargate** (and, for CDC, managed **MSK Connect**) in the same VPC as the source and
Aurora DSQL (sub-millisecond network RTT).

---

## At a glance

| Path | Setup | Result |
|---|---|---|
| Full Load, 4 tables | 8 vCPU, MySQL source | ~32,000 rows/s |
| Full Load, one table split across 8 processes | 8 vCPU, MySQL source, empty target | 41,000–51,000 rows/s sustained (~67,000 peak) |
| Full Load, ~1 TB in 20 tables | 16 vCPU, MySQL source, composite key | 915.7M rows in 2h27m (average ~103K rows/s) |
| Full Load, ~1 TB in 20 tables | 16 vCPU, PostgreSQL source, composite key | 860M rows in 1h42m (average ~140K rows/s) |
| Full Load, one huge table (865M rows) | 16 vCPU, PostgreSQL source, default settings | ~20–23K rows/s (single reader) |
| CDC | default sink sizing (4 MCU) / 8 MCU | ~21,000 rows/s / ~26,000 rows/s |

---

## Full Load

### What drives throughput (8 vCPU)

| Component | Configuration |
|---|---|
| **ECS Fargate** | 8 vCPU (8192 CPU units), 16 GB memory |
| **Source** | Aurora MySQL (RDS), `customers_sample` schema |
| **Target** | Aurora DSQL, us-east-1 |
| **Tables** | `order_items` (33.6M rows), `orders` (8.5M), `payments` (8.4M), `customers` (small) |
| **Measurement** | `scripts/measure_performance.py full-load` with CloudWatch progress monitoring |

| Configuration | rows/s | CPU |
|---|---|---|
| Threads in one process (single core), 2 tables | 12,277 | 110% (one core) |
| One process per table, 2 tables | 22,365 | 207% |
| One process per table, 4 tables | 32,270 | 311% |
| One large table split across 8 processes, empty target (plain `INSERT`) | 41,000–51,000 sustained, ~67,000 peak | 777% |
| Same table, appending into existing rows (`ON CONFLICT`) | ~35,000 | 439% |

- **Full Load is CPU-bound, not network-bound.** One Python process converts rows on one
  core, so throughput scales with the number of parallel processes: set table parallelism
  close to the vCPU count (§7.2).
- **Loading into an empty target is the fastest path.** After **Drop & reload** the loader
  uses a plain `INSERT` with no `ON CONFLICT`, which removes optimistic-concurrency
  contention entirely; appending into existing rows ran roughly 15–30% slower.
- **Beyond about 8 concurrent writer processes the limit moves from CPU to DSQL write
  capacity** (about 67K rows/s peak observed).

### Large scale: about 1 TB (16 vCPU)

| | MySQL source | PostgreSQL source |
|---|---|---|
| **Source** | Aurora MySQL `db.r7g.8xlarge` (temporarily upsized) | Aurora PostgreSQL 16.14 `db.r7g.8xlarge` (temporarily upsized) |
| **Dataset** | 20 tables × 45.78M rows = 915.7M rows (≈ 1.07 TB) | 20 tables × 43M rows = 860M rows (≈ 1.02 TB) |
| **Loader settings** | composite PK (`dist_key`) on all 20 tables, `TABLE_PARALLELISM=16`, `BATCH_PARALLELISM=32`, batch rows 3000 | the same |
| **Completion** | 20/20 tables, 0 failures | 20/20 tables, 0 failures |
| **Total wall time** | 2h27m32s | 1h42m31s |
| **Average throughput** | ~103K rows/s | ~140K rows/s |
| **While 16 tables ran in parallel** | ~131K rows/s (16 vCPU saturated) | ~194K rows/s (about 12 of 16 vCPU) |
| **OCC `40001` retries** | 0 | 0 |

Both runs used the same Fargate size and the same Aurora DSQL cluster, driven by automated
scripting (ECS RunTask) rather than the UI.

- **A composite primary key removes the hot-partition bottleneck.** A monotonic
  `AUTO_INCREMENT` key funnels every insert into the trailing key range; prepending a
  high-cardinality column spread the writes, and the retries were exactly 0 (§7.1).
- **Keep the table count within the table parallelism.** With 20 tables on 16 slots the
  last 4 tables ran alone (about 21K rows/s) and added about 31 minutes; with table
  parallelism at least the table count, the MySQL run would have taken about 1h56m.
- **A PostgreSQL source loads faster at lower CPU.** Its values pass through almost
  unchanged, so the per-row CPU cost is lower than with MySQL's dialect conversion; MySQL
  was CPU-saturated, PostgreSQL was not.
- **Split big data over several tables where you can.** A freshly created DSQL table
  starts with one partition and splits under load, so a single-table load starts slow
  (about 16K rows/s) and climbs to 120–150K rows/s; 20 tables start with 20 times the
  partitions.

### One huge table

With the current tool a single large table loads with **one reader** by default, so
concurrent readers never read different snapshots of a source that is being written to.
Measured on an 865M-row PostgreSQL table, one reader reached about 20–23K rows/s (about 12
hours for the whole table); forcing 4 shards on a static source reached about 42K rows/s
and was write-bound, not CPU-bound. For one huge table, see *Reader shards per large
table* ([§3.8](03-full-load.md#38-parallel-loading),
[§7.2](07-performance-and-tuning.md#72-tuning-parallelism)).

---

## CDC throughput

CDC is a different pipeline with a different bottleneck. Full Load is **CPU-bound**; CDC
is `Debezium (source) → MSK topic → custom DSQL sink`, where the sink is
**DSQL-write-latency-bound** (each statement is a distributed round-trip). The tool infers
the scaling parameters from the number of captured tables
([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)).

| Component | Configuration |
|---|---|
| **Source connector** | Debezium MySQL on MSK Connect, `ConnectorMcuCount`=4 |
| **Sink connector** | custom `dsql-sink`, `SinkTasksMax` scaled 4→8 |
| **Workload** | 4 ECS tasks bulk-inserting into `customers_sample.orders` (~20,000 rows/s into the source) |
| **Measurement** | CloudWatch `AWS/KafkaConnect` `SourceRecordWriteRate` / `SinkRecordSendRate`, cross-checked by the DSQL target row-count delta |

- **The sink was latency-bound, so every avoided round-trip was a win.** Early versions
  applied one row per round-trip and sat at 300–2,000 rows/s with low CPU; batching the
  apply and removing a hidden per-row metadata round-trip took the sink to about 18,700
  rows/s on 8 partitions.
- **Size the sink independently of the source.** The sink is then CPU-bound (about 80% at
  4 MCU and ~21,000 rows/s), while the single-task Debezium source has spare CPU.
  `SinkMcuCount` (default 4) is a separate knob from the source's `ConnectorMcuCount`: **8
  MCU** reached about 26,000 rows/s at ~34% CPU.
- **Beyond that the source side limits the pipeline.** One binlog reader and the source
  database's own capacity: a 2 vCPU source ran at 93% CPU and capped the pipeline until it
  was scaled up. With the shipped producer tuning the source itself sustained about 31,000
  records/s.
- **Low sink CPU plus sublinear partition scaling does not prove server-side contention.**
  Here a hidden client round-trip produced the same symptoms; DSQL's `OccConflicts` was 0
  throughout. Check `OccConflicts` and `ReadOnlyTransactions` before blaming the server.
- **A source reboot is absorbed automatically** — the pipeline resumes from the committed
  binlog offset with no gap ([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)).

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

See [`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh) for
the full A/B measurement harness and
[`scripts/measure_performance.py`](../../../scripts/measure_performance.py) for the
in-process throughput + OCC reporting tool.

For CDC, deploy the cdc-stack and start a steady insert workload on the source, then read
the pipeline rates from CloudWatch `AWS/KafkaConnect` (`SourceRecordWriteRate` on the
`-debezium-source` connector, `SinkRecordSendRate` on the `-dsql-sink` connector),
cross-checking against the DSQL target's `COUNT(*)` delta over a fixed interval.

---

**Prev:** [11. Customer FAQ](11-customer-faq.md)

**Next:** [13. Excluding tables and columns →](13-excluding-tables-and-columns.md)
