# 3. How Full Load works

_Language: **English** | [한국어](../ko/03-full-load.md) | [日本語](../ja/03-full-load.md)_

> **Prev:** [2. Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md)
>
> **Leaving tables or columns out?** See [13. Excluding tables and columns](13-excluding-tables-and-columns.md): an excluded column loads as NULL on Aurora DSQL.

**Full Load** is the tool's own bulk copy of your existing rows from the source
database — RDS / Aurora **MySQL** or RDS / Aurora **PostgreSQL** — into Aurora
DSQL. It is **not** a Debezium snapshot — it is a purpose-built loader
that streams data and respects DSQL's constraints. It runs from the **Data
Migration** step after you've selected tables.

> **Mental model:** the loader reads your source one PK-ordered page at a time and
> writes it into DSQL in small, idempotent, retryable batches — bounded in memory
> no matter how big the table is, and resumable if interrupted.

---

## 3.1 The big picture

```
Source (MySQL / PostgreSQL)          Aurora DSQL
  │  keyset page (PK > last, LIMIT)      ▲  idempotent batched INSERT
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
modifies it. A MySQL source — and a PostgreSQL Full-Load-only migration — is never
written. The single exception is **PostgreSQL CDC**: at the watermark point (§3.5)
the tool creates, and at teardown drops, a logical replication slot and a
publication scoped to exactly the migrated tables (`FOR TABLE ONLY`: a table's
inheritance children are not added with it, just as the Full Load reads only the table's
own rows, `FROM ONLY`, see
[§6.1](06-limitations.md#61-aurora-dsql-feature-limits-your-schema-must-fit-these); a
partitioned table's partitions are).
It also sets `REPLICA IDENTITY FULL` on a re-keyed table and repairs the publication's
`publish_via_partition_root`: at the Full Load on a publication it reuses (before it
creates the slot), at each Start CDC (if there is no publication yet, as on a first
CDC only Start or one after a Full-load-only run, it creates an empty one with that
setting and the connector adds its tables), and before it adds a table selected after
the Full Load. Such a table (Full load + CDC, before CDC starts) joins the publication
just before its copy, then the tool takes one transaction ID to wait for the
transactions open at that moment. All of these are autocommit, restricted to a small
allowlist, and audited. They are the only writes the tool ever makes to any source.
Before it creates the publication (or adds tables to it) at the watermark, it checks
that every table it would add keeps a usable `REPLICA IDENTITY` (and, on PostgreSQL 18,
that no re-keyed table has a generated column, which the `FULL` it needs would break);
otherwise the run stops before it changes anything on the source and names the table and
the fix for its cause. It also
stops before any write if a replication slot of the same name is in use, belongs to
another database or is not a logical `pgoutput` slot (for example another
migration's, under the same CDC stack name); an inactive one in this database is
taken as this stack's earlier slot and replaced. If the Full Load's
`publish_via_partition_root` repair fails while a selected table is partitioned, the
run stops before it creates the slot and names the `ALTER PUBLICATION` statement to
run as the publication's owner; at Start CDC a repair that fails, or cannot run because
the session has no source password, does not stop the Start, and its log names the
same statement. A table selected after the Full Load gets the same check before it is
added: without one it is not loaded, and its row says why
([13.7](13-excluding-tables-and-columns.md)).

---

## 3.2 Streaming export — bounded memory at any table size

The loader reads with **keyset pagination on the primary key**, not `OFFSET`:

```sql
SELECT <cols> FROM <table>
WHERE pk > :last           -- composite PKs: explicit lexicographic OR-expansion (PK-index-friendly on MySQL 5.7+ and on PostgreSQL)
ORDER BY pk
LIMIT :batch_size          -- DEFAULT_BATCH_SIZE = 5000
```

It advances `:last` to the last row of each page until a short page signals the
end. The query runs over a **server-side / streaming cursor** inside a single
repeatable-read snapshot transaction (MySQL: `START TRANSACTION WITH CONSISTENT
SNAPSHOT` on InnoDB; PostgreSQL: a `REPEATABLE READ` transaction), so:

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
examples a **MySQL source** produces:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → integer (decoded from the source bytes).
- `DATETIME` → `timestamp` normalized to UTC; `TIMESTAMP` → `timestamptz`.
- `BLOB`/`BINARY`/`VARBINARY` family → `bytea`.

For a **PostgreSQL source** there is **no** MySQL-style value translation during
Full Load: both ends are PostgreSQL-16 wire (psycopg on both sides), so
DSQL-supported values pass through unchanged. PostgreSQL type handling — unsupported
types flagged for remodel, `numeric` precision clamping, no `DEFAULT` / serial /
identity emitted — happens in **Schema Conversion**, not as an on-the-fly value
transform.

The full mapping (and how DSQL constraints like the 1 MiB per-value limit are handled)
lives in [Chapter 2 §2.3](02-evaluation-and-schema-conversion.md#23-mysql--dsql-type-and-constraint-handling-reference)
and the Schema Conversion step.

---

## 3.4 Batched, idempotent, bounded-parallel load

Rows flow straight into **idempotent batched INSERTs** loaded **concurrently**
across a small, bounded DSQL connection pool. Every limit here is a real DSQL
constraint, handled for you:

| Constraint | What the loader does | Default / cap |
|---|---|---|
| ≤ 3000 rows per transaction | Caps batch row count | `DEFAULT_BATCH_ROWS = 2000`, hard cap `3000` |
| Bind-parameter limit | Clamps batch to fit | `MAX_STATEMENT_PARAMETERS = 65535` (`65535 // columns`) |
| Per-write-txn size | Splits wide rows | `MAX_BATCH_BYTES = 8 MiB` |
| Optimistic concurrency (`40001`) | Retries the batch with backoff + jitter — also on a transient connection drop and on Aurora DSQL's temporary `XX000` (server unavailable) | up to 20 attempts |
| Bounded resource use | Limited concurrent batches | `DEFAULT_PARALLELISM = 8` per table, `4` tables at once |

**Idempotent by construction.** A clean load inserts into a freshly-recreated
empty target (a plain multi-row `INSERT`); when data may already be present (a
retry, or CDC running alongside) the loader switches to a **"skip existing"** mode
— insert, and on a pre-existing key skip rather than overwrite — so a re-run never
creates duplicates and **never overwrites a newer CDC-applied row**. (The tool
deliberately avoids DSQL's *multi-row* `ON CONFLICT DO NOTHING`, which was observed
to silently drop non-conflicting rows.)

**Identity sequences are advanced after a complete load.** If a table's key was
converted to a cached identity column (`GENERATED BY DEFAULT AS IDENTITY`), the
tool runs `ALTER TABLE … RESTART WITH max(pk)+1` once the load finishes, so the
application's **first insert after cut-over can't collide** with a migrated key. It
runs **only on a complete** (or accepted-gap) load — never on a partial/failed one;
plain-integer keys have no sequence and are untouched.

**Indexes come last.** Secondary indexes are created **after** all data batches
succeed, each as `CREATE INDEX ASYNC` in its own single-DDL transaction (DSQL allows
one DDL per transaction and builds indexes asynchronously). An index statement
Aurora DSQL answers with `XX000` ("server unavailable"), or whose connection drops
while it runs, is retried the same way. If a retry then finds an index of that name
already there, the tool does not count it as created, because index names are per
schema in Aurora DSQL and it could be another table's: the Full Load panel and the
error log show it as **not confirmed** (not as missing), with a note that the
earlier attempt may have created it, so check the target before you add it. The
panel's notice says a constraint is missing only when a `UNIQUE` index is known not
to have been created; when some indexes are missing and others not confirmed, it
names both.

---

## 3.5 The watermark — the bridge to CDC

Before loading, the tool captures a **watermark**: a consistent point in the
source's change stream, recorded inside the same consistent-snapshot transaction.
Every watermark carries a **UTC snapshot timestamp** and **approximate** per-table
row counts (scan-free `information_schema` estimates, so the source isn't
full-scanned just to count). The engine-specific coordinate differs:

- **MySQL** — the **binlog file + position**, plus the **GTID set**
  (`@@GLOBAL.gtid_executed`), the source's **`gtid_mode`** and `server_uuid`. These are
  read via `SHOW BINARY LOG STATUS` (MySQL 8.2+/8.4, where `SHOW MASTER STATUS` was
  removed) with a fallback to `SHOW MASTER STATUS` (≤ 8.0.x). On a `gtid_mode=ON` source
  CDC starts after the watermark's GTID set; otherwise at its file + position. The
  watermark also records a short fingerprint of each migrated table's column
  definitions, read just before the position, so Start CDC can tell whether a table
  changed since (see [Chapter 6](06-limitations.md)).
- **PostgreSQL** — a **WAL LSN** (`Watermark.wal_lsn`), not a binlog coordinate.
  At the consistency point the tool creates a **publication** (`FOR TABLE` the exact
  migrated tables, never `FOR ALL TABLES`) and a `pgoutput` **logical replication
  slot**, and records the slot's returned consistent LSN together with the slot and
  publication names. That LSN pins WAL and is what makes the gapless
  `snapshot.mode=never` CDC handoff possible. There is no GTID / `server_uuid`
  concept on PostgreSQL, and no `SHOW BINARY LOG STATUS` — the LSN comes from the
  created slot.

The watermark is what makes a later **gapless handoff to CDC** possible: CDC starts
streaming changes from exactly the point the snapshot ended — no gap, no overlap
(see [Chapter 4](04-cdc-and-dsql-constraints.md)). If the CDC prerequisites aren't
met — on MySQL, binary logging disabled or the privilege restricted; on PostgreSQL,
`wal_level` not `logical`, or the slot / publication can't be created (missing
replication privilege, or a standby endpoint) — the coordinate degrades gracefully
and you simply don't get the CDC handoff (Full Load still works).

> The watermark's row counts are **approximate on purpose** (to spare the
> source). Exact `COUNT(*)` and checksums are the job of **Validation**
> ([Chapter 5](05-validation.md)), not Full Load.

### A new start point needs new CDC topics

Capturing a **new** watermark is a decision about CDC too, so on a **Full load + CDC**
migration whose CDC stack has **streamed** — stopped after a Start, its connectors in place,
or a Start that CloudFormation rolled back — every action that takes one is **refused**:
**Start Full Load**, **Re-run Full Load** and **Re-run all tables** are disabled, and the page
header's **Run** starts nothing. Stop CDC (and a rolled-back Start) removes the sink without draining it, so its
consumer group would re-apply changes older than the new snapshot over the reloaded tables,
while the source skips the changes between the old stop point and the new snapshot —
silently: only a CHECKSUM validation would show it. The notice names the route instead:
**Delete CDC infrastructure** and deploy it again (the new stack starts with empty topics),
then **Re-run Full Load** choosing **Drop & reload** (on PostgreSQL that creates the
replication slot again at its own snapshot point), then **Start CDC**.

- **The page header's Run is gated the same way**, because it takes a new start point just
  like Start / Re-run Full Load. On a stack that has streamed it starts nothing: a warning
  toast names the same route, no job is created, no watermark is captured (on PostgreSQL the
  replication slot is not replaced), and the step status and the previous Full Load record
  stay as they were. That covers a stack that is still **streaming** too.
- **While CDC is live**, the Full Load actions keep their own gate first: *Stop CDC first
  (CDC step → Stop CDC), then Delete CDC infrastructure and deploy it again before a new
  Full Load.* The confirm dialog of **Retry unfinished tables**, **Reload specific tables**
  or a per-table **Reload**, which keep the original start point, says instead to Stop CDC
  first, run it, then Start CDC again: CDC then continues from where it stopped, gapless
  only while the CDC topics still retain what the sink had not applied (their retention
  period on MSK).
- **Until the CDC state has been read** in this session — just after the screen opens, or
  after an app restart — or when it could not be read, a new start point **waits** rather
  than guessing: the notice says so and points at **Re-check CDC state** on the CDC step
  (fix the AWS access first if the read failed).
- **Not gated:** **Retry unfinished tables**, **Reload specific tables**, a per-table
  **Reload** and **Change & reload** all keep the **original** start point, so CDC continues
  from where it stopped — like any restart after Stop CDC, gapless only while the CDC topics
  still retain the changes the sink had not applied
  ([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)) — and the
  reloaded tables converge. The Full Load step does not gate a **Full load only** or **CDC
  only** migration, but switching to one is no way around this gate (next entry).
- **Start CDC refuses a newer start point (MySQL).** On a MySQL source, Start CDC refuses to
  move a CDC stack that has streamed forward to a start point newer than the one it last
  started from (or, once it is stopped, to any start point when its last Start offered none),
  whatever the migration type: for example the watermark of a Re-run Full Load made in
  **Full load only** after switching away from Full load + CDC, or a later **Manual** point.
  The start-point card and the Start CDC card show *This CDC stack cannot start from a newer
  start point* with the route — the one above, or with CDC only, **Delete CDC
  infrastructure** and deploy it again, then **Start CDC** on the new stack — and Start CDC
  is disabled; the Start CDC dialog, a click on Start or Retry CDC, and the Start CDC job, on
  the stack as it reads it before changing anything (*Start CDC blocked: This CDC stack has
  streamed, so Start CDC does not move it forward to a newer start point …*), refuse it too.
  A refused Manual point also says when setting the start point back to **Automatic** would
  continue the stream. A start point at or before that one, or none, continues the stream
  and is not refused. After a first Start that CloudFormation rolled back, the stack records
  only that it rolled back a Start: the rollback restores its parameters, so the start point
  that Start offered is kept only in the app's memory. While the app holds it, a start point
  later than that one is refused and a retry from the same one is not. After an app restart
  (or for a Start made in another session) no start point can be checked against it, so
  every start point is refused on that stack, that same one included (*CloudFormation rolled
  back this stack's last Start, and the start offset that Start offered is no longer known
  …*), with the same route. The CDC only route starts the new stack from the Full Load's
  watermark, so after a Full load only re-run that appended to rows already on the target,
  run it again choosing **Drop & reload** before that Start (an append keeps those rows as
  they were, and CDC from a watermark carries only the changes after it). On PostgreSQL
  there is no such
  refusal: Start CDC continues from the stack's own replication slot whatever the Full Load's
  watermark says, and a Full load only run leaves that slot alone, so CDC continues from
  where it stopped if the stack's source connector streamed (otherwise Debezium snapshots
  every selected table first), as in any restart after Stop CDC (above).
- If the stack starts streaming between the click and the job starting, the engine refuses on
  the job thread before it reads or writes anything: the run is recorded as failed with
  *FullLoadRefusedError* and the same one-line reason.

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

A temporary Aurora DSQL condition is not a rejection: a `40001`, a dropped
connection and `XX000` ("server unavailable") retry the whole batch (§3.4), and a
batch whose retries run out fails — the table can be retried with **Retry
unfinished tables**, and no row is quarantined for it.

### Table-fatal (the whole table stops)

If a value **cannot be written without silently changing it**, the exporter raises a
`ValueConversionError`:

- a `TINYINT(1)` mapped to `boolean` holding a value outside `{0,1}` (e.g. `2`; MySQL
  source only, where values are translated during export);
- a value with more decimal places than its `numeric(p,s)` column keeps, or a fraction
  for an integer column, on both engines. This is detected from the source column's
  live type once per table read, so a table whose types match pays nothing. It arises
  when the source type was widened after Step 1 or a narrower type was chosen in Schema
  Conversion. A PostgreSQL `numeric` without precision into a bare `numeric` target is
  not refused (Aurora DSQL's documented `numeric(18,6)` default), and a `FLOAT`/`DOUBLE`
  source value for a `numeric` column is not checked and is still rounded;
- a MySQL `TIME` outside 00:00:00–23:59:59.999999.

This has **no SQLSTATE** (it happens while reading/converting, before DSQL is even
asked), so it is **not** a per-row quarantine: it stops that table's load **loudly**.
This is intentional — the tool refuses to flatten `2` to `true` or round `1.2345` to
`1.23` and silently corrupt your data. The message names the column, its type and the
rule — never the value. Give the column a type that holds the values in Schema
Conversion (re-run Step 1 first if the source type changed, then re-apply with
**Replace**), or fix the source data / exclude the column, then retry the table.

> **Why "loud" beats "silent":** DSQL's `boolean` can't represent `2`. The tool
> chooses a visible, fixable failure over a quiet, wrong value.

### The run-level verdict

A table that quarantined **any** rows is reported as a **failure** (incomplete),
and the run does not report success — so an incomplete load is never mistaken for
a clean one — **unless you explicitly choose "Accept quarantined rows & continue,"**
which completes the run with the gap acknowledged (still surfaced in Validation). A
retryable table failure always blocks. The downloadable error log lists exactly
which PKs were quarantined and why.

---

## 3.7 Resumability

Because rows stream in **keyset (PK) order**, batch *i* always maps to the same
PK range on every run. That makes batches **stable, deterministic resumable
units**:

- a completed batch is recorded as `DONE`;
- a stop / retry re-runs only the unfinished ranges;
- `INSERT ... ON CONFLICT` makes re-running a batch idempotent (no duplicates).

The "retry failed tables" path resets only the failed tables to pending, **reuses
the original watermark**, and keeps the completed tables — so you never redo work
or lose the CDC handoff point.

A table selected after the load (Full load only lets the table set change; see
[13.7](13-excluding-tables-and-columns.md)) is loaded by the same retry, but the
original watermark does not cover it, so the CDC handoff holds only for the original
set: the migration type stays on Full load only until Re-run Full Load takes a fresh
watermark over the current set (or you untick the tables selected since). Choose
**Drop & reload** for that re-run: CDC then starts from its watermark, and an *Append*
keeps the rows the earlier load wrote as they were, without the source's updates and
deletes since.

Unticking tables does not lock the type unless CDC has already started for this migration: CDC captures the loaded tables still selected. In Full load + CDC you can untick tables after the load until CDC starts. They are left as is on Aurora DSQL, CDC does not capture them, the watermark still covers every captured table (the handoff stays gapless), and a failure only in unticked tables no longer holds the step.

In Full load + CDC you can also tick a table after the load, until CDC starts. Retry unfinished tables loads it under the original watermark, after the checks in [13.7](13-excluding-tables-and-columns.md) (CDC never started, the Aurora DSQL table is empty or dropped and reloaded, and CDC can capture the table with no gap: on MySQL its definition is unchanged since the watermark; on PostgreSQL it has a usable replica identity and joins CDC's publication before its copy). The watermark then covers it, CDC captures it from that watermark with the other tables, and the handoff stays gapless for it too. On MySQL, a table the Full Load recorded no start-point definition for (a Full Load from an earlier version, or a table converted after the load) is loaded by Re-run all tables instead, choosing **Drop & reload**: it takes a fresh start point for every table, and an append would keep the rows already loaded as they were. Full load only is unchanged: a load there never extends the watermark.

> **Run Full Load from the command line (optional).** The same bulk loader is
> available as a CLI script — `scripts/run_full_load.py` (plan first, then `--yes`;
> optional `--clean` and `--watermark-out` to capture the CDC watermark) — for
> automation or a large-scale run without the web UI. See
> [`scripts/README.md`](../../../scripts/README.md); the source is read-only (the
> sole exception is the PostgreSQL-CDC slot and publication noted in §3.1) and the
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
