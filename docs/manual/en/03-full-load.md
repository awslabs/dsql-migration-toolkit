# 3. How Full Load works

_Language: **English** | [한국어](../ko/03-full-load.md) | [日本語](../ja/03-full-load.md)_

> **Prev:** [2. Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md)

**Full Load** is the tool's own bulk copy of your existing rows from source MySQL
into Aurora DSQL. It is **not** a Debezium snapshot — it is a purpose-built loader
that streams data and respects DSQL's constraints. It runs from the **Data
Migration** step after you've selected tables.

> **Mental model:** the loader reads your source one PK-ordered page at a time and
> writes it into DSQL in small, idempotent, retryable batches — bounded in memory
> no matter how big the table is, and resumable if interrupted.

---

## 3.1 The big picture

```
Source MySQL                         Aurora DSQL
  │  keyset page (PK > last, LIMIT)      ▲  batched INSERT ... ON CONFLICT
  │  streaming server-side cursor        │  (≤3000 rows, ≤8 MiB, OCC retry)
  └──────────►  convert types  ──────────┘
       (read-only, consistent snapshot)
```

1. **Capture a watermark** (a consistent point in the source's history).
2. **Stream rows out** of the source by primary-key pages.
3. **Convert** each value to its DSQL form as it flows.
4. **Load** rows into DSQL in bounded, idempotent batches, concurrently.
5. Build secondary **indexes** afterward.

The source is read **read-only** inside a consistent snapshot; the loader never
modifies it.

---

## 3.2 Streaming export — bounded memory at any table size

The loader reads with **keyset pagination on the primary key**, not `OFFSET`:

```sql
SELECT <cols> FROM <table>
WHERE pk > :last           -- (composite PKs use a row-value tuple comparison)
ORDER BY pk
LIMIT :batch_size          -- DEFAULT_BATCH_SIZE = 5000
```

It advances `:last` to the last row of each page until a short page signals the
end. The query runs over a **server-side / streaming cursor** inside a
`START TRANSACTION WITH CONSISTENT SNAPSHOT` (InnoDB repeatable read), so:

- a whole table is **never loaded into RAM** — memory stays bounded by one page;
- the read is a **single consistent snapshot** even as the live source changes.

**A primary key is required.** A table with no PK cannot be keyset-paginated and
is rejected (it's also flagged `UNSUPPORTED` back in Evaluation, because DSQL
requires a PK too). Single-column and composite PKs are both supported — and a
composite key is not just tolerated but is the tool's recommended fix for a write
hot partition on a monotonic key (see [Chapter 7 §7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions)).

> **Worried about read load on a busy production source?** Loading many tables at
> once is bounded by one lever (table parallelism) and there are concrete steps to
> keep the source healthy — see
> [Chapter 7 §7.3 — Minimizing impact on the source](07-performance-and-tuning.md#73-minimizing-impact-on-the-source).

---

## 3.3 Type conversion on the fly

As rows stream, each value is converted to the form DSQL stores. This mirrors the
Schema Conversion mapping (so the column types and the values agree). A few
examples MySQL users should know:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → integer (decoded from the source bytes).
- `DATETIME` → `timestamp` normalized to UTC; `TIMESTAMP` → `timestamptz`.
- `BLOB`/`BINARY`/`VARBINARY` family → `bytea`.

The full mapping (and how DSQL constraints like "no foreign keys" are handled)
lives in [Chapter 2 §2.3](02-evaluation-and-schema-conversion.md#23-mysql--dsql-type-and-constraint-handling-reference)
and the Schema Conversion step.

---

## 3.4 Batched, idempotent, bounded-parallel load

Rows flow straight into **multi-row `INSERT ... ON CONFLICT`** statements loaded
**concurrently** across a small, bounded DSQL connection pool. Every limit here
is a real DSQL constraint, handled for you:

| Constraint | What the loader does | Default / cap |
|---|---|---|
| ≤ 3000 rows per transaction | Caps batch row count | `DEFAULT_BATCH_ROWS = 2000`, hard cap `3000` |
| Bind-parameter limit | Clamps batch to fit | `MAX_STATEMENT_PARAMETERS = 65535` (`65535 // columns`) |
| Per-write-txn size | Splits wide rows | `MAX_BATCH_BYTES = 8 MiB` |
| Optimistic concurrency (`40001`) | Retries the batch with backoff + jitter — also on a transient connection drop | up to 20 attempts |
| Bounded resource use | Limited concurrent batches | `DEFAULT_PARALLELISM = 8` per table, `4` tables at once |

**Idempotent by construction.** Loads use `INSERT ... ON CONFLICT`, so re-running
a batch never creates duplicates. When CDC is running alongside, the loader uses a
"skip existing" mode so it **never overwrites a newer CDC-applied row**.

**Indexes come last.** Secondary indexes are created **after** all data batches
succeed, each as `CREATE INDEX ASYNC` in its own single-DDL transaction (DSQL
allows one DDL per transaction and builds indexes asynchronously).

---

## 3.5 The watermark — the bridge to CDC

Before loading, the tool captures a **watermark**: a consistent point in the
source's binary log, recorded inside the same consistent-snapshot transaction.
It contains:

- **binlog file + position**,
- the **GTID set** (`@@GLOBAL.gtid_executed`) and `server_uuid`,
- a **UTC snapshot timestamp**, and
- **approximate** per-table row counts (scan-free `information_schema` estimates,
  so the source isn't full-scanned just to count).

The watermark is what makes a later **gapless handoff to CDC** possible: CDC
starts streaming changes from exactly the point the snapshot ended — no gap, no
overlap (see [Chapter 4](04-cdc-and-dsql-constraints.md)). If your RDS settings
restrict `SHOW MASTER STATUS`, the binlog/GTID fields degrade gracefully to
empty, and you simply don't get the CDC handoff (Full Load still works).

> The watermark's row counts are **approximate on purpose** (to spare the
> source). Exact `COUNT(*)` and checksums are the job of **Validation**
> ([Chapter 5](05-validation.md)), not Full Load.

---

## 3.6 How failures are isolated — and the one that isn't

This is the part to understand well, because the two failure behaviors are
deliberately different.

### Per-row quarantine (the table keeps loading)

If **DSQL rejects a specific row** at apply time (an error that carries a
SQLSTATE — e.g. a value over DSQL's 1 MiB limit, a constraint violation), the
loader does **not** fail the table. It binary-splits the batch down to the single
offending row, **quarantines** that row (recording its **primary key and the
reason — never its values**), and loads the rest. Quarantined rows show up in a
downloadable error log.

### Table-fatal (the whole table stops)

If a value **cannot be converted at all without silent data loss** — the
canonical case is a `TINYINT(1)` column mapped to DSQL `boolean` that holds a
value outside `{0, 1}` (e.g. `2`) — the exporter raises a `ValueConversionError`.
This has **no SQLSTATE** (it happens while reading/converting, before DSQL is
even asked), so it is **not** a per-row quarantine: it stops that table's load
**loudly**. This is intentional — the tool refuses to flatten `2` to `true` and
silently corrupt your data. Fix the source data (or exclude the column) and
re-run.

> **Why "loud" beats "silent":** DSQL's `boolean` can't represent `2`. The tool
> chooses a visible, fixable failure over a quiet, wrong value.

### The run-level verdict

A table that quarantined **any** rows is reported as a **failure** (incomplete),
and the run does not report success — so an incomplete load is never mistaken for
a clean one. The downloadable error log lists exactly which PKs were quarantined
and why.

---

## 3.7 Resumability

Because rows stream in **keyset (PK) order**, batch *i* always maps to the same
PK range on every run. That makes batches **stable, deterministic resumable
units**:

- a completed batch is recorded as `DONE`;
- a stop / retry re-runs only the unfinished ranges;
- `INSERT ... ON CONFLICT` makes any re-run idempotent (no duplicates).

The "retry failed tables" path resets only the failed tables to pending, **reuses
the original watermark**, and keeps the completed tables — so you never redo work
or lose the CDC handoff point.

> **Run Full Load from the command line (optional).** The same bulk loader is
> available as a CLI script — `scripts/run_full_load.py` (plan first, then `--yes`;
> optional `--clean` and `--watermark-out` to capture the CDC watermark) — for
> automation or a large-scale run without the web UI. See
> [`scripts/README.md`](../../../scripts/README.md); the source is read-only and the
> load is idempotent, exactly as in the UI.

---

## 3.8 Multi-process parallelism (GIL bypass)

Python's GIL (Global Interpreter Lock) limits a single process to one CPU core
regardless of how many threads it uses. Since Full Load's per-row type conversion
and batch assembly are CPU-bound Python, the loader was previously capped at
~15,000 rows/s even on an 8 vCPU Fargate task.

Starting in v0.1.68, the loader uses **`ProcessPoolExecutor`** — each table (or
table shard) loads in its own OS process with its own GIL and its own CPU core.

### How it works

- **Small tables** (non-shardable or below the row threshold): 1 worker process
  each — same as before, just in a separate process instead of a thread.
- **Large tables** whose leading PK column is an integer (a single integer PK, or a
  composite PK with an integer leading column such as `(tenant_id, id)`): split into K
  PK-range shards, each loaded by its own worker process from a disjoint slice of
  the source. This reader-range sharding is **off by default** (`full_load_reader_shards=1`);
  enable it with `FULL_LOAD_READER_SHARDS` > 1, and it only engages on a
  **CDC-coexisting run** — a standalone Full Load / REPLACE always uses one reader per
  table.
- **All work units share one bounded pool** — `table_parallelism` controls the
  total number of concurrent worker processes.

```
ProcessPoolExecutor(max_workers=table_parallelism)
  ├─ customers (small)        → 1 worker
  ├─ orders (9M, int PK)      → 2 shard workers
  ├─ payments (9M, int PK)    → 2 shard workers
  └─ order_items (33.6M, int PK) → 3 shard workers
                                    ───────────────
                                    8 workers = 8 cores
```

### Tuning it

For a large migration, set the worker count to the task's vCPU count — the loader
distributes the pool slots between whole-table workers and shard workers itself. The
knobs (`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`, `FULL_LOAD_READER_SHARDS`), their limits, and how
they interact with source load live together in
[Chapter 7 §7.2 — Tuning parallelism](07-performance-and-tuning.md#72-tuning-parallelism);
the measured throughput this design achieves (and the ThreadPool baseline it replaced) is
in [Appendix: Performance test results](12-performance-test-results.md).

---

**Next:** [4. CDC and DSQL constraints →](04-cdc-and-dsql-constraints.md)
