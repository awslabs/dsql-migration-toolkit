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
| Full Load, 16 tables × 20M rows | 16 vCPU, PostgreSQL source | ~336,000 rows/s overall (~600,000 steady) |
| Full Load, 16 tables × 20M rows | 16 vCPU, MySQL source | ~193,000 rows/s overall (~226,000 steady) |
| Full Load, one table of 150M rows | 16 vCPU, PostgreSQL source | ~205,000 rows/s overall (~280,000 steady) |
| Full Load, one table of 150M rows | 16 vCPU, MySQL source | ~55,500 rows/s steady by default (one session); ~164,000 rows/s overall in Full load + CDC with *Reader shards per large table* 16 |
| CDC | default sink sizing (4 MCU) / 8 MCU | ~21,000 rows/s / ~26,000 rows/s |

---

## Full Load

### The maximum-size runs (16 vCPU)

| Component | Configuration |
|---|---|
| **ECS Fargate** | 16 vCPU, 120 GB memory (the largest task) |
| **Sources** | Aurora MySQL and Aurora PostgreSQL, `db.r8g.16xlarge` |
| **Target** | Aurora DSQL, `ap-northeast-2` |
| **Loader settings** | *Tables in parallel* 16, everything else at its default (*Reader shards per large table* automatic, *Batches per table* 8, *Rows per batch* 2000) |
| **Measurement** | `scripts/measure_performance.py full-load`, run as a one-off ECS task |

| Run | Rows | Time | Overall | Steady | What limited it |
|---|---|---|---|---|---|
| PostgreSQL, 16 tables | 320M | 15m53s | ~336K rows/s | ~600K rows/s | Aurora DSQL (the task used about 3.5 cores) |
| PostgreSQL, one table: 32 ranges on 16 processes | 150M | 12m12s | ~205K rows/s | ~280K rows/s | Aurora DSQL |
| MySQL, 16 tables | 320M | 27m | ~193K rows/s | ~226K rows/s | the task's CPU (all 16 vCPU busy) |
| MySQL, one table, Full load only: one session | 75M of 150M (stopped at 25 min) | — | — | ~55.5K rows/s | one core |
| MySQL, one table, Full load + CDC: 16 separate-snapshot parts | 150M | 15m12s | ~164K rows/s | ~226K rows/s | the task's CPU, beside the previous copy's DROP |

### What drives throughput

- **Key ranges, not batches in flight.** Aurora DSQL writes one key range of a table at
  about 20,000 rows/s whatever is in flight, so a large table goes faster only with its
  writes spread over key ranges, which the loader does by default
  ([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)). Against one range per
  table, the 150M-row table loaded about 10× faster on PostgreSQL, 8× on MySQL in parts
  and 2.8× on MySQL in one session.
- **A MySQL load is bound by the task's CPU, a PostgreSQL load by Aurora DSQL.** Reading
  and converting MySQL rows keeps about one core busy per table or part loading at once;
  PostgreSQL values pass through, so 16 tables loaded at 336K rows/s on about 3.5 cores.
  Set *Tables in parallel* to the task's vCPU count.
- **One huge MySQL table loads at one core's pace by default.** One session reads its
  ranges from one snapshot. In Full load + CDC from a binlog start point, an explicit
  *Reader shards per large table* reads it in separate-snapshot parts on several cores,
  consistent once CDC replays it ([§3.8](03-full-load.md#38-parallel-loading)).
- **A fresh table warms up.** Aurora DSQL starts a new table on one partition and splits
  it as it grows, so a load's first minute or so is slower than its steady rate.
- **The host and earlier jobs move the numbers.** The same MySQL load ran up to about 1.8×
  faster on one Fargate host than on another, and dropping the previous 75M-row copy kept
  a background job running on Aurora DSQL for over an hour, beside the next load.

---

## CDC throughput

CDC is a different pipeline with a different bottleneck: `Debezium (source) → MSK topic
→ custom DSQL sink`, where the sink is **DSQL-write-latency-bound** (each statement is a
distributed round-trip). The tool infers the scaling parameters from the number of
captured tables ([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)).

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
AWS_REGION=<region> APP_STACK=<app-stack> \
DB_HOST=<source-host> DB_PORT=3306 DB_USER=admin DB_PASSWORD=<pw> \
MEASURE_SCHEMA=<schema> MEASURE_TABLES="<table> <table>" \
TABLE_PARALLELISM=16 VARIANTS=auto \
deploy/run_measure_on_fargate.sh
```

[`deploy/run_measure_on_fargate.sh`](../../../deploy/run_measure_on_fargate.sh) runs the
measurement as a one-off Fargate task in the source's VPC; `VARIANTS=auto` measures the
loader's defaults, and the script lists the other variants.
[`scripts/measure_performance.py`](../../../scripts/measure_performance.py) is the
measurement itself: it reports the throughput per table and per time window, the write
path's retries and latencies, and when Aurora DSQL finished the index builds. Its header
shows the usage (`--reader-shards`, `--cdc-mode`, …), and `PERF_SOURCE_ENGINE=postgres`
points it at a PostgreSQL source. It drops and recreates the target tables, so use it
**only on a non-production target**, and drop an earlier copy first and let Aurora DSQL
finish its background jobs (`--wait-sys-jobs`) before you compare runs.

For CDC, deploy the cdc-stack and start a steady insert workload on the source, then read
the pipeline rates from CloudWatch `AWS/KafkaConnect` (`SourceRecordWriteRate` on the
`-debezium-source` connector, `SinkRecordSendRate` on the `-dsql-sink` connector),
cross-checking against the DSQL target's `COUNT(*)` delta over a fixed interval.

---

**Prev:** [11. Customer FAQ](11-customer-faq.md)

**Next:** [13. Excluding tables and columns →](13-excluding-tables-and-columns.md)
