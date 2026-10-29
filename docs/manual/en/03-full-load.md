# 3. How Full Load works

_Language: **English** | [한국어](../ko/03-full-load.md) | [日本語](../ja/03-full-load.md)_

> **Prev:** [2. Evaluation and Schema Conversion](02-evaluation-and-schema-conversion.md)
>
> **Leaving tables or columns out?** See [13. Excluding tables and columns](13-excluding-tables-and-columns.md): an excluded column loads as NULL on Aurora DSQL.

**Full Load** is the tool's own bulk copy of your existing rows from the source database —
RDS / Aurora **MySQL** or RDS / Aurora **PostgreSQL** — into Aurora DSQL. It is **not** a
Debezium snapshot. It runs from the **Data Migration** step after you've selected tables.

> **Mental model:** the loader reads your source one PK-ordered page at a time and writes
> it into DSQL in small, idempotent, retryable batches — bounded in memory no matter how
> big the table is, and resumable if interrupted.

**Shortest path:** on the **Data Migration** step, select the tables and click **Start
Full Load**, watch the progress table, and if a table is quarantined or has failed, follow
[§3.6](#36-how-failures-are-isolated--and-the-one-that-isnt). Next come CDC
([Chapter 4](04-cdc-and-dsql-constraints.md)), if you chose it, and Validation
([Chapter 5](05-validation.md)).

---

## 3.1 The big picture

```
Source (MySQL / PostgreSQL)              Aurora DSQL
  │  one primary-key page at a time      ▲  small, retryable batches
  └──────────►  convert values  ─────────┘
       (read-only, consistent snapshot)
```

1. **Capture a watermark** (a consistent point in the source's history).
2. **Stream rows out** of the source by primary-key pages (a large table's in several
   primary-key ranges, so its writes spread, §3.8).
3. **Convert** each value to its DSQL form as it flows.
4. **Load** rows into DSQL in bounded, idempotent batches, concurrently, into tables that
   already carry their secondary **indexes** (§3.4).

**What the tool writes to your source.** The source is read **read-only** inside a
consistent snapshot; the loader never modifies it. Beyond those reads only CDC touches a
source, and only like this:

| Run | What the tool writes to the source |
|---|---|
| MySQL **Full load + CDC** on a source not running `gtid_mode=ON` | One **`FLUSH BINARY LOGS`**, right after recording CDC's start point and before any table is read. It needs `RELOAD`, writes no data, locks no table and moves the binlog to a new file (§3.5). |
| PostgreSQL **CDC** | A logical replication slot and a publication scoped to exactly the migrated tables, created at the watermark (§3.5) and dropped at teardown; `REPLICA IDENTITY FULL` on a re-keyed table; and the publication's `publish_via_partition_root` setting, where it needs repair. |
| Any other run | Nothing beyond reads. |

All of these are autocommit, restricted to a small allowlist, and audited. They are the
only writes the tool ever makes to any source.

On PostgreSQL the tool checks before it writes. Before it creates the publication (or adds
tables to it) at the watermark, it checks that every table it would add keeps a usable
`REPLICA IDENTITY` (and, on PostgreSQL 18, that no re-keyed table has a generated column,
which the `FULL` it needs would break); otherwise the run stops before it changes anything
on the source and names the table and the fix for its cause. It also stops before any
write if a replication slot of the same name is in use, belongs to another database or is
not a logical `pgoutput` slot (for example another migration's, under the same CDC stack
name); an inactive one in this database is taken as this stack's earlier slot and
replaced. If the publication's `publish_via_partition_root` cannot be set for a
partitioned table, the run stops before it creates the slot and names the
`ALTER PUBLICATION` statement to run as the publication's owner; at Start CDC a repair
that fails, or cannot run because the session has no source password, does not stop the
Start, and its log names the same statement. A table selected after the Full Load gets the
same check before it is added: without one it is not loaded, and its row says why
([13.7](13-excluding-tables-and-columns.md)).

---

## 3.2 Streaming export — bounded memory at any table size

The loader reads each table by **primary-key pages**, not `OFFSET`: every page starts
after the last key of the previous one, in key order, until a short page signals the end.
The pages stream from the source inside a single consistent snapshot, so:

- a whole table is **never loaded into RAM** — memory stays bounded by one page;
- the read is a **single consistent snapshot** even as the live source changes (a MySQL
  table read in parts: one snapshot per part, [§3.8](#38-parallel-loading)).

**MySQL reads at `REPEATABLE READ`, whatever the source's own setting.** The tool sets it
on its own sessions only (the server's `transaction_isolation` is left as it is, and no
privilege is needed), so even a source configured `READ-COMMITTED` gives each table one
consistent snapshot. Each table read therefore holds one InnoDB read view until it ends:
on a write-heavy source the History List Length grows while a large table is read
([§7.3](07-performance-and-tuning.md#large-tables-on-a-write-heavy-source)).

**A primary key is required.** A table with no PK cannot be paged and is rejected (it's
also flagged `UNSUPPORTED` back in Evaluation, because DSQL requires a PK too).
Single-column and composite PKs are both supported — and a composite key is not just
tolerated but is the tool's recommended fix for a write hot partition on a monotonic key
(see
[Chapter 7 §7.1](07-performance-and-tuning.md#primary-key-strategy--avoid-hot-partitions)).

A key column that cannot be paged exactly stops its table with a message that names the
fix, instead of loading it with rows missing: a MySQL `ENUM` or `SET` key column whose
members cannot be read exactly or that the source compares as equal (for example under a
case-insensitive collation), or a single-precision MySQL `FLOAT` key column **followed by
another key column** in a table that needs more than one page (change it to `DOUBLE` or
`DECIMAL` on the source, or give the table another primary key, then retry the table).
`TIME` and `BIT` key columns read like any other. After you add an `ENUM` or `SET` member
on the source, re-run Evaluation and Schema Conversion so the target's `CHECK` lists it.

> **Worried about read load on a busy production source?** Loading many tables at once is
> bounded by one lever (table parallelism) and there are concrete steps to keep the source
> healthy — see
> [Chapter 7 §7.3 — Minimizing impact on the source](07-performance-and-tuning.md#73-minimizing-impact-on-the-source).

---

## 3.3 Type conversion on the fly

As rows stream, each value is converted to the form DSQL stores. This mirrors the Schema
Conversion mapping (so the column types and the values agree). A few examples a **MySQL
source** produces:

- `TINYINT(1)` → DSQL **boolean** (`0/1` → `false/true`).
- `BIT(n)` → integer.
- `DATETIME` → `timestamp`, its wall clock copied as it is (no time zone is applied or
  shifted; a source in another zone keeps that zone's wall clock, `SOURCE_TIME_ZONE` in
  Chapter 2); `TIMESTAMP` → `timestamptz`, the instant.
- A zero or invalid `DATE` / `DATETIME` / `TIMESTAMP` (`0000-00-00`, `2024-00-15`, and
  `2024-02-31` under `ALLOW_INVALID_DATES`), which Aurora DSQL has no value for → the
  value the CDC connector writes for a zero date: NULL, the column's `DEFAULT` or
  `1970-01-01` (`1970-01-01` too for a day the month does not have, at which CDC stops
  instead), as [Chapter 6 §6.2](06-limitations.md#62-migration-process-limits) details.
  The table then gets one WARNING line in the activity log with how many, per column
  (never the values). A primary-key column's are left as stored, and so are a unique
  key's, which Aurora DSQL rejects: that row is quarantined.
- `BLOB`/`BINARY`/`VARBINARY` family → `bytea`.

For a **PostgreSQL source** there is **no** MySQL-style value translation during Full
Load: both ends speak PostgreSQL, so DSQL-supported values pass through unchanged.
PostgreSQL type handling — unsupported types flagged for remodel, `numeric` precision
clamping — happens in **Schema Conversion**, not as an on-the-fly value transform.

The full mapping (and how DSQL constraints like the 1 MiB per-value limit are handled)
lives in
[Chapter 2 §2.3](02-evaluation-and-schema-conversion.md#23-type-mapping-reference) and the
Schema Conversion step.

---

## 3.4 Batched, idempotent, bounded-parallel load

Rows flow straight into **idempotent batched INSERTs**, loaded **concurrently** across a
small, bounded DSQL connection pool. The tool sizes every batch to Aurora DSQL's
per-transaction limits for you, and retries a batch that hits an optimistic-concurrency
conflict, a dropped connection or a temporary "server unavailable" with backoff (up to 20
attempts).

**Idempotent by construction.** A clean load inserts into a freshly-recreated empty
target; when data may already be present (a retry, or an append to a table that already
holds rows) the loader switches to a **"skip existing"** mode — insert, and on a
pre-existing key skip rather than overwrite — so a re-run never creates duplicates and
**never overwrites a newer CDC-applied row**. What it cannot tell apart is a row CDC has
just deleted and one that was never there, so no Full Load runs while CDC streams into the
target, and Start CDC waits for a running one
([§3.5](#35-the-watermark--the-bridge-to-cdc)): a row the source deleted while its table
was being copied would otherwise be written back.

**Identity sequences are advanced after a complete load.** If a table's key was converted
to a cached identity column (`GENERATED BY DEFAULT AS IDENTITY`), the tool restarts its
sequence after the largest migrated key once the load finishes, so the application's
**first insert after cut-over can't collide** with a migrated key. It runs **only on a
complete** (or accepted-gap) load — never on a partial/failed one; plain-integer keys have
no sequence and are untouched.

**Secondary indexes come first.** On Aurora DSQL an index builds far faster on an empty
table than over loaded rows
([§7.5](07-performance-and-tuning.md#75-measured-on-live-infrastructure--what-backs-71-and-72)),
so a table's secondary indexes exist before its rows: Schema Conversion's apply creates
them with the table, and a **Drop & reload** creates them again on the recreated table,
then waits up to 10 minutes for Aurora DSQL to build them before the first row loads. A
table the run recreates later, or an index it could not submit before the rows, gets its
indexes after every table's data has loaded (a Stop still submits the indexes of the
tables already loaded). With the indexes in place, a row that a `UNIQUE` index other than
the primary key rejects is quarantined (§3.6), and the rest of the table loads.

A failed index statement is retried; if the retry finds an index of that name already
there, the tool does not count it as created (index names are per schema in Aurora DSQL,
so it could be another table's) and the Full Load panel shows it as **not confirmed**, not
as missing: check the target before you add it. The panel says a constraint is missing
only when a `UNIQUE` index is known not to have been created.

Aurora DSQL builds each index in the background, and a Full Load's reported time ends when
its indexes are submitted, not built. The Data Migration step shows the builds:

| The step shows | What it means | What to do |
|---|---|---|
| *Waiting for Aurora DSQL to build the secondary indexes of N recreated table(s) before loading* | A Drop & reload put the indexes on the empty tables and waits for their builds | Nothing: the load starts once they are built, or after 10 minutes with any build still running left building |
| *Aurora DSQL is building secondary indexes: n of m built* | The data is loaded; a query that needs an index is slower until it is built | Nothing: you can go on to the next step |
| *Secondary indexes built: all m* | Every index the run submitted is built | — |
| *UNIQUE index build failed: a constraint is missing*, or *Secondary index builds failed* | Aurora DSQL could not build the index; a failed `UNIQUE` one is a missing constraint | Fix the cause the notice names (for `UNIQUE`, remove the duplicates), drop the index if the table still lists it, and create it again, before cutting over |
| *Secondary indexes not created yet (N table(s))* | The run ended before it submitted those tables' indexes (a stop or an app restart during that step, or Aurora DSQL not answering); no data is missing | **Retry unfinished tables**: it submits them without reloading any data |

---

## 3.5 The watermark — the bridge to CDC

Before loading, the tool captures a **watermark**: the point in the source's change stream
that CDC starts from, recorded before any table is read. Every watermark also carries a
**UTC snapshot timestamp** and **approximate** per-table row counts (estimates that spare
the source a full scan). The start point itself differs by engine:

- **MySQL** — the **binlog file + position**, plus the **GTID set**, the source's
  **`gtid_mode`** and `server_uuid`. On a `gtid_mode=ON` source CDC starts after the
  watermark's GTID set; otherwise at its file + position. The watermark also records a
  short fingerprint of each migrated table's column definitions, so Start CDC can tell
  whether a table changed since (see [Chapter 6](06-limitations.md)).
- **PostgreSQL** — a **WAL LSN**, not a binlog coordinate. At the consistency point the
  tool creates a **publication** (`FOR TABLE` the exact migrated tables, never
  `FOR ALL TABLES`) and a `pgoutput` **logical replication slot**, and records the slot's
  consistent LSN together with the slot and publication names. That LSN pins WAL and is
  what makes the gapless CDC handoff possible. There is no GTID on PostgreSQL.

The watermark is what makes a later **gapless handoff to CDC** possible. Every table is
read from its own consistent snapshot, opened after the watermark, and CDC replays every
change after the watermark; a change both the snapshot and CDC carry is applied twice,
which the idempotent upsert / delete by primary key makes harmless (see
[Chapter 4](04-cdc-and-dsql-constraints.md)). So the watermark must never cover a
transaction the snapshots cannot see. On MySQL the tool makes sure of that:

- On a `gtid_mode=ON` source nothing is needed: the GTID set lists a transaction only
  after it has committed.
- A binlog file + position can cover a transaction that is in the binlog but not yet
  committed. A **Full load + CDC** run therefore runs **`FLUSH BINARY LOGS`** right after
  recording the position, before any table is read: before MySQL switches to a new binlog
  file it waits until every transaction already in the binlog has committed (normally
  milliseconds, during which new commits wait too). It needs the **RELOAD** privilege,
  locks no table, and is recorded in the activity log. On Aurora MySQL with **enhanced
  binlog** the tool does not rely on that wait, so a Full load + CDC run there needs
  `gtid_mode=ON` (or enhanced binlog turned off). The prerequisite **CDC start point can
  be made exact** checks both before each Full load + CDC run.
- A transaction prepared with `XA PREPARE` before the watermark and committed after the
  tables are read is copied by neither, so the run waits up to 30 s for the prepared XA
  transactions it finds and stops if one is still prepared. On MySQL 8.0.19 and later this
  check needs `XA_RECOVER_ADMIN`; without it the run logs a warning and goes on.

A **Full load only** run issues no `FLUSH`: it copies the snapshots and nothing after
them, so stop writes to the source first if the copy must include every change. On a
source not running `gtid_mode=ON` its watermark (like one recorded by an earlier version
of the tool) is therefore not made exact, so a later Start CDC from it shows *This start
point was recorded without waiting for in-flight commits*; CDC only re-runs the Full Load
through Full load + CDC for this reason. If the CDC prerequisites aren't met — on MySQL,
binary logging disabled or the privilege restricted; on PostgreSQL, `wal_level` not
`logical`, or the slot / publication can't be created (missing replication privilege, or a
standby endpoint) — you simply don't get the CDC handoff, and Full Load still works. The
exception is a MySQL **Full load + CDC** run whose binlog position cannot be made exact
(no `RELOAD`, enhanced binlog without `gtid_mode=ON`, or an XA transaction still
prepared): it stops before reading any table and names the remedy, so fix the cause and
start it again.

> The watermark's row counts are **approximate on purpose** (to spare the source). Exact
> `COUNT(*)` and checksums are the job of **Validation** ([Chapter 5](05-validation.md)),
> not Full Load.

### A new start point needs new CDC topics

Capturing a **new** watermark is a decision about CDC too, so on a **Full load + CDC**
migration whose CDC stack has **streamed** — stopped after a Start, its connectors in
place, or a Start that CloudFormation rolled back — every action that takes one is
**refused**: **Start Full Load**, **Re-run Full Load** and **Re-run all tables** are
disabled, and the page header's **Run** starts nothing. Stop CDC (and a rolled-back Start)
removes the sink without draining it, so its consumer group would re-apply changes older
than the new snapshot over the reloaded tables, while the source skips the changes between
the old stop point and the new snapshot — silently: only a CHECKSUM validation would show
it. The notice names the route instead: **Delete CDC infrastructure** and deploy it again
(the new stack starts with empty topics), then **Re-run Full Load** choosing **Drop &
reload** (on PostgreSQL that creates the replication slot again at its own snapshot
point), then **Start CDC**.

What each Full Load action does depends on the CDC state:

| CDC state | New start point: **Start Full Load**, **Re-run Full Load**, **Re-run all tables**, the header's **Run** | Original start point: **Retry unfinished tables**, **Reload specific tables**, a per-table **Reload**, **Change & reload** |
|---|---|---|
| Not streamed yet | Allowed | Allowed |
| **Live** (streaming) | Disabled: *Stop CDC first (CDC step → Stop CDC), then Delete CDC infrastructure and deploy it again before a new Full Load.* | Disabled: Stop CDC first, run it, then Start CDC again. CDC continues from where it stopped, gapless only while the CDC topics still retain what the sink had not applied (their retention period on MSK). |
| A **Start**, **Stop** or **Delete CDC infrastructure** in progress | Disabled: wait for it to finish (the CDC step shows its progress). | Disabled: wait for it to finish. |
| **Stopped** after it streamed | Refused: use the route above. | Allowed: CDC continues from where it stopped (as above), and the reloaded tables converge. |

The Full Load step does not gate a **Full load only** or **CDC only** migration, but
switching to one is no way around this gate: on MySQL, Start CDC refuses too (below).
Until the CDC state has been read in this session — just after the screen opens, or after
an app restart — or when it could not be read, a new start point **waits** rather than
guessing: the notice says so and points at **Re-check CDC state** on the CDC step (fix the
AWS access first if the read failed).

**Start CDC refuses a newer start point (MySQL).** On a MySQL source, Start CDC refuses to
move a CDC stack that has streamed forward to a start point newer than the one it last
started from (or, once it is stopped, to any start point when its last Start offered
none), whatever the migration type: for example the watermark of a Re-run Full Load made
in **Full load only** after switching away from Full load + CDC, or a later **Manual**
point. The start-point card and the Start CDC card show *This CDC stack cannot start from
a newer start point* with the route (the one above, or with CDC only, **Delete CDC
infrastructure** and deploy it again, then **Start CDC** on the new stack), and Start CDC
is disabled. A start point at or before that one, or none, continues the stream and is not
refused.

After a first Start that CloudFormation rolled back, the same rule applies against the
start point that Start offered. When that point cannot be found either, every start point
is refused on that stack (*This CDC stack's last start point is not known*), with the same
route.

The CDC only route starts the new stack from the Full Load's watermark, so when the
refused point is that watermark (Automatic), the route has one condition: if that Full
Load appended to rows already on the target, run it again choosing **Drop & reload**
before that Start. CDC only has no Full Load step, so switch the migration type to Full
load + CDC for that run, whose Full Load makes the CDC start point exact (an append keeps
those rows as they were, and CDC from a watermark carries only the changes after it). On
the new stack that Start is not refused, and the CDC step states the condition before it
when this session shows that the Full Load may have appended to an earlier copy
([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)).

On PostgreSQL there is no such refusal: Start CDC continues from the stack's own
replication slot whatever the Full Load's watermark says, and a Full load only run leaves
that slot alone, so CDC continues from where it stopped if the stack's source connector
streamed (otherwise Debezium snapshots every selected table first), as in any restart
after Stop CDC (above).

---

## 3.6 How failures are isolated — and the one that isn't

This is the part to understand well, because the two failure behaviors are deliberately
different.

### Per-row quarantine (the table keeps loading)

If **Aurora DSQL rejects a specific row** at apply time (for example a value over its 1
MiB limit, or a constraint violation such as a duplicate value for a `UNIQUE` index other
than the primary key), the loader does **not** fail the table. It isolates
the offending row, **quarantines** it (recording its **primary key and the reason — never
its values**), and loads the rest. Quarantined rows show up in a downloadable error log.

A temporary Aurora DSQL condition is not a rejection: a concurrency conflict, a dropped
connection or "server unavailable" retries the whole batch (§3.4), and a batch whose
retries run out fails — the table can be retried with **Retry unfinished tables**, and no
row is quarantined for it.

### Table-fatal (the whole table stops)

If a value **cannot be written without silently changing it**, the table's load stops
**loudly**, with a message that names the column, its type and the rule — never the value:

- a `TINYINT(1)` mapped to `boolean` holding a value outside `{0,1}` (e.g. `2`; MySQL
  source only);
- a value with more decimal places than its `numeric(p,s)` column keeps, or a fraction for
  an integer column, on both engines. It arises when the source type was widened after
  Step 1 or a narrower type was chosen in Schema Conversion. A `FLOAT`/`DOUBLE` source
  value for a `numeric` column is not checked and is still rounded;
- a MySQL `TIME` outside 00:00:00–23:59:59.999999.

This is intentional, and it is **not** a per-row quarantine, because the problem is found
while reading, before Aurora DSQL is even asked. The tool refuses to flatten `2` to `true`
or round `1.2345` to `1.23` and silently corrupt your data: a visible, fixable failure
beats a quiet, wrong value.

### The run-level verdict

A table that quarantined **any** rows is reported as a **failure** (incomplete), and the
run does not report success — so an incomplete load is never mistaken for a clean one —
**unless you explicitly choose "Accept quarantined rows & continue,"** which completes the
run with the gap acknowledged (still surfaced in Validation). A retryable table failure
always blocks. The downloadable error log lists exactly which PKs were quarantined and
why.

### What to do

| What happened | What to do |
|---|---|
| A row was quarantined | Download the error log (primary keys and reasons, never values), then fix those rows or choose **Accept quarantined rows & continue**, which completes the run with the gap acknowledged (Validation still shows it). |
| A table failed after its retries (a temporary Aurora DSQL condition) | **Retry unfinished tables**; no row is quarantined for it. |
| A table stopped on a value that cannot be written without changing it | Give the column a type that holds the values in Schema Conversion (re-run Step 1 first if the source type changed, then re-apply with **Replace**), or fix the source data or exclude the column, then retry the table. |
| The run was interrupted (stop, crash, task replacement) | **Retry unfinished tables**: finished tables are kept, unfinished ones reload from their start, and the original watermark (the CDC handoff point) is reused ([§3.7](#37-resumability)). |
| *Secondary indexes not created yet* (no data is missing) | **Retry unfinished tables**: it submits only those tables' missing indexes, without reloading any data (§3.4). |
| You want a fresh copy and a new start point | **Re-run Full Load**, choosing **Drop & reload**. On a Full load + CDC migration whose CDC stack has streamed, first see [A new start point needs new CDC topics](#a-new-start-point-needs-new-cdc-topics). |

---

## 3.7 Resumability

The unit an interrupted Full Load resumes from is the **table**, not a PK range:

- a finished table is recorded as `DONE` and kept;
- a stop, a crash or a task replacement leaves each unfinished table to be **reloaded from
  its start**, in the "skip existing" mode (§3.4) or, with **Drop & reload**, recreated
  first, so a very large table interrupted near its end is read again in full.

The **Retry unfinished tables** path resets only the unfinished tables to pending,
**reuses the original watermark**, and keeps the completed tables — so you never reload a
finished table or lose the CDC handoff point.

**Ticking or unticking tables after the load.** In **Full load only** the table set can
change; in **Full load + CDC** it can until CDC starts for the first time. The rules and
the checks are in [13.7](13-excluding-tables-and-columns.md). A table selected after the
load is loaded by the same retry, but the original watermark does not cover it, so the CDC
handoff holds only for the original set: the migration type stays on Full load only until
Re-run Full Load takes a fresh watermark over the current set (or you untick the tables
selected since). Choose **Drop & reload** for that re-run: CDC then starts from its
watermark, and an *Append* keeps the rows the earlier load wrote as they were, without the
source's updates and deletes since. In Full load + CDC, a table ticked after the load is
loaded by Retry unfinished tables under the original watermark once CDC can capture it
with no gap since the Full Load; on MySQL a table the Full Load recorded no start-point
definition for (a Full Load from an earlier version, or a table converted after the load)
is loaded by Re-run all tables instead, choosing **Drop & reload**: it takes a fresh start
point for every table, and an append would keep the rows already loaded as they were. A
table unticked after the load is left as is on Aurora DSQL and no longer holds the step.

> **Run Full Load from the command line (optional).** The same bulk loader is available as
> a CLI script — `scripts/run_full_load.py` (plan first, then `--yes`; optional `--clean`
> and `--watermark-out` to capture the CDC watermark) — for automation or a large-scale
> run without the web UI; see [`scripts/README.md`](../../../scripts/README.md). The
> source is read-only (with `--watermark-out` the run is a Full load + CDC one, so unless
> the source runs `gtid_mode=ON` it issues `FLUSH BINARY LOGS` right after the watermark,
> which needs `RELOAD`, §3.5) and the load is idempotent, exactly as in the UI.

---

## 3.8 Parallel loading

Tables load in parallel, each in its own worker process, so a Full Load can use every vCPU
of the task it runs on. `table_parallelism` (*Tables in parallel*) sets how many workers
run at once. A large table is also read in primary-key ranges, because Aurora DSQL writes
one key range of a table at about 20,000 rows/s: spread over several, its writes go faster
([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)).

### How it works

- **Small tables** (below the split threshold, or without an integer leading primary-key
  column): one worker each, with one reader.
- **Large tables** whose leading PK column is an integer (a single integer PK, or a
  composite PK with an integer leading column such as `(tenant_id, id)`) are read in K
  primary-key ranges. *Reader shards per large table* (**Settings → Full Load**,
  `FULL_LOAD_READER_SHARDS`) sets K: `0` = automatic, the default: one range per
  2,500,000 estimated rows from 5,000,000 rows, at most 32 (10M rows: 4 ranges); `1` =
  off; `2`–`32` = exactly that many (below). The estimates are the start point's scan-free
  ones, and nothing is split when `TABLE_PARALLELISM` is 1.
  - **PostgreSQL**: every range imports one shared snapshot, so the copy is one point in
    time, as with one reader. The table is read by as many workers as its share of the
    run's estimated rows (at most `TABLE_PARALLELISM`), each reading a block of its
    ranges.
  - **MySQL**: one reader session reads every range from one snapshot, a page of each in
    turn, so the copy is one point in time, as with one reader, in Full load only and Full
    load + CDC alike. The table takes one source connection and one worker.
- **An explicit 2 to 32** gives exactly that many ranges to a table of at least
  `SHARD_MIN_ROWS` estimated rows, read the same way, except for separate-snapshot parts
  on MySQL:
  - **MySQL**: only in **Full load + CDC** whose start point has a binlog file and
    position, and only a table whose every unique key on Aurora DSQL includes the leading
    key column: each range is then a part, read by its own worker from its own snapshot.
    MySQL cannot share a snapshot, so a row whose key changes during the read can be
    copied twice or not at all; CDC, started from the start point taken before any read
    ([§3.5](#35-the-watermark--the-bridge-to-cdc)), replays every change since then and
    corrects that. Any other MySQL table is read by one session, as above.
  - A MySQL part is never re-read in place after a lost source connection: the table is
    *Failed* with that reason (*range 2 of 4*), and **Retry unfinished tables** loads it
    again. A table read by one session is read again whole from a fresh snapshot, and a
    PostgreSQL range re-reads its shared snapshot.
- **All work units share one bounded pool**: each table, or part of one, takes one worker
  slot, so at most `table_parallelism` units run at once, the ones that read the most rows
  first.

### A table read in parts needs CDC's replay (MySQL)

This applies only to the separate-snapshot parts above, which take an explicit *Reader
shards per large table* of 2 or more: a run with the default setting reads no table in
parts. Until CDC has replayed it, a MySQL table read in parts is torn, so every route that
would take it out of that replay is refused with the table's name and the way that ends
the refusal:

- **Unticking it** in the table list after the load, or a table rule in *Exclude tables &
  columns* that unselects it: its checkbox is locked, with the reason on the lock icon.
- **Switching the migration type to Full load only** after the load (from CDC only too:
  there the route goes through Full load + CDC, whose Full Load step reloads it).
- **Start CDC from a later start point**: a Manual position later than the Full Load's
  watermark, typed or from *Fetch current position*. Automatic, or a Manual point at or
  before the watermark, replays every read and is never refused, nor is a Start that
  resumes a stopped stream.
- **Deploy CDC infrastructure, Start CDC and Retry CDC**, when the table cannot be
  deselected: they name it as one that cannot be deselected and do not offer Full load
  only.

**Ending the refusal.** Set *Reader shards per large table* to 0 (automatic) or 1 (off) in
Settings and reload the table (one reader session, one snapshot) through the control the
Full Load step shows for it — **Retry unfinished tables** while it is unfinished,
**Reload specific tables** once every table has loaded — then do the operation. Full load
only and a later start point have a second way: once CDC has replayed the table, run
Validation in **CHECKSUM** mode with *Fast sweep* off: a match proves the copy and ends
the refusal. Unticking the table has only the reload, before CDC starts: the replay needs
CDC started, and from CDC's first Start the table set is fixed (only Start over changes
it, [§13.7](13-excluding-tables-and-columns.md)).

The table stays marked until one of two things clears the mark: a load that recreates it
(a **Drop & reload**), or a Validation whose **CHECKSUM** comparison matches it and
started after the table's last load ended. So a Retry or Reload that reads with one reader
drops and reloads such a table whatever the Append / Drop & reload choice says (the dialog
says why). A re-run of every table (**Re-run Full Load**, or **Re-run all tables**) drops
and reloads such a table too, whatever the choice and the Reader shards setting: it takes
a new start point, and CDC from that point never replays the changes made before it.

A Start CDC, even one that streamed, does not end the refusals: the tool cannot tell from
it whether CDC replayed the table. After a Start that never streamed (the worker refused
it, or a connector failed), start CDC again from **Automatic** (or a Manual point at or
before the Full Load's watermark): that is the replay, and it is never refused. Once CDC
has caught up, the CHECKSUM match ends the Full load only and later-start-point refusals.

### Tuning it

For a large migration, set *Tables in parallel* to the task's vCPU count: whole-table
units and range units queue for the same slots. The split itself needs no setting. The
knobs (`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`,
`FULL_LOAD_READER_SHARDS`), their limits, and how they interact with source load live
together in
[Chapter 7 §7.2 — Tuning parallelism](07-performance-and-tuning.md#72-tuning-parallelism);
the measured throughput this design achieves is in
[Appendix: Performance test results](12-performance-test-results.md).

---

**Next:** [4. CDC and DSQL constraints →](04-cdc-and-dsql-constraints.md)
