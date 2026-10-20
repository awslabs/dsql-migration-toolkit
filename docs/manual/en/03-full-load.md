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
WHERE pk > :last           -- composite PKs: the cursor the source's PK index serves (below)
ORDER BY pk
LIMIT :batch_size          -- DEFAULT_BATCH_SIZE = 5000
```

It advances `:last` to the last row of each page until a short page signals the
end. The query runs over a **server-side / streaming cursor** inside a single
repeatable-read snapshot transaction (MySQL: `START TRANSACTION WITH CONSISTENT
SNAPSHOT` on InnoDB; PostgreSQL: a `REPEATABLE READ` transaction), so:

- a whole table is **never loaded into RAM** — memory stays bounded by one page;
- the read is a **single consistent snapshot** even as the live source changes (a MySQL
  table read in parts: one snapshot per part, [§3.8](#38-multi-process-parallelism-gil-bypass)).

**MySQL reads at `REPEATABLE READ`, whatever the source's own setting.** Every MySQL
source session the tool opens first runs `SET SESSION TRANSACTION ISOLATION LEVEL
REPEATABLE READ`. It is a session setting: the server's `transaction_isolation` is left as
it is, and no privilege is needed. MySQL ignores `WITH CONSISTENT SNAPSHOT` below
`REPEATABLE READ` (warning 138), so on a source configured `READ-COMMITTED` each page
would otherwise read the rows committed when it runs, and a row whose key changed during
the read could be copied twice or not at all. The watermark capture and Validation's
per-table reads open the same kind of snapshot. On such a source each table read therefore
holds one InnoDB read view until it ends, as on a `REPEATABLE READ` source: on a
write-heavy source the History List Length grows while a large table is read
([§7.3](07-performance-and-tuning.md#large-tables-on-a-write-heavy-source)).

**How the cursor is written.** It follows the order the primary-key index keeps, in
the form that index serves as a range. A composite key on MySQL uses the explicit
lexicographic OR-expansion `(k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR …`
(MySQL 5.7 serves no row-value comparison from an index); on PostgreSQL it uses the
row-value comparison `(k0, k1, …) > (:last_0, :last_1, …)`, an index condition on the
primary key and on each partition's (there the OR-expansion is only a filter: every
page would re-read the key from its start).

A MySQL `ENUM` or `SET` key column is paged in the order MySQL sorts it — by member
index or bitmask, not by label:

- A key that **starts with an `ENUM`** column is read one member at a time, in
  declaration order: each page is `col = <member>`, or that member and the rest of
  the key after the cursor, and the read moves on to the next member when a page
  comes back short — only a short page on the last member ends the table. Every
  page is an index lookup or range, even when the primary key holds every column of
  the table.
- A later `ENUM` key column is paged by the members declared after the cursor's
  (`col IN (…)`), and a `SET` key column by its bitmask (`CAST(col AS UNSIGNED) > n`,
  exact for all 64 members). No index serves the bitmask, so a key that starts with
  a `SET` column re-reads the key from its start on every page.
- The members are read from the source's catalog (`information_schema.COLUMNS`) when
  each table's read starts, so a member added or reordered after Evaluation is read
  in the right order. The target's `CHECK` still lists the members Evaluation saw:
  re-run Evaluation and Schema Conversion after you add a member.
- The read stops the table instead of loading it with rows missing when the members
  cannot be read exactly (for example a member holding NUL, or one declared twice),
  when a key value is not one of them, or when the source compares two members as
  equal (for example under a case-insensitive collation), so that a page returns rows
  the read has already passed.

A single-precision MySQL `FLOAT` key column **followed by another key column** stops the
read of a table that needs more than one page: MySQL returns its value rounded to 6
significant digits, which does not compare equal to the stored float, so a cursor on it
would pass the rest of that value's rows by, silently. Change the column to `DOUBLE` or
`DECIMAL` on the source, or give the table another primary key, then retry the table. As
the last (or only) key column it is read as usual: two keys that differ only past their
6th significant digit read back as the same value and cannot both be stored on Aurora
DSQL (Validation shows the row-count difference), and when a page ends on a value MySQL
returns below the stored one, the next page starts at the rows that read back as that
value — rows already loaded, or ones Aurora DSQL would hold as the same key — and the
read skips them (with an `ENUM` or `SET` key column too, it stops the table instead, as for
members the collation calls equal). A `TIME` key column (negative values included) and a `BIT` key column
are read in key order like any other: the cursor binds them as the duration text and the
integer that MySQL compares exactly. A PostgreSQL `real` key column is too: the cursor
binds its value as text, which PostgreSQL reads as the stored `real`.

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
retry, or an append to a table that already holds rows) the loader switches to a
**"skip existing"** mode — insert, and on a pre-existing key skip rather than
overwrite — so a re-run never creates duplicates and **never overwrites a newer
CDC-applied row**. (The tool deliberately avoids DSQL's *multi-row* `ON CONFLICT DO
NOTHING`, which was observed to silently drop non-conflicting rows.) What it cannot
tell apart is a row CDC has just deleted and one that was never there, so no Full
Load runs while CDC streams into the target, and Start CDC waits for a running one
([§3.5](#35-the-watermark--the-bridge-to-cdc)): a row the source deleted while its
table was being copied would otherwise be written back.

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
  Full Load.* **Retry unfinished tables**, **Reload specific tables** and a per-table
  **Reload**, which keep the original start point, are disabled too, and a confirm dialog
  already open for one cannot be confirmed. They say instead to Stop CDC first, run it, then
  Start CDC again: CDC then continues from where it stopped, gapless only while the CDC
  topics still retain what the sink had not applied (their retention period on MSK). A load
  beside the streaming sink inserts only the rows missing on the target, so a row the source
  deletes while its table is being copied could reach Aurora DSQL after CDC applied that
  delete and stay there, an extra row that only a COUNT or CHECKSUM validation shows. For the
  same reason the page header's **Run** starts nothing while a Start CDC is still in flight,
  and Start CDC waits while a Full Load runs
  ([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)). While a
  **Start CDC**, **Stop CDC** or **Delete CDC infrastructure** is still in progress, the Full
  Load step's actions stay disabled, but the reason they give is to wait for it to finish (the
  CDC step shows its progress), not to Stop CDC first. The page header's **Run** starts
  nothing then either; on a stack that has streamed it names the delete-and-redeploy route
  above instead of the wait. Once a Stop or a Delete has finished, a Retry or a Reload runs;
  once a Start has CDC streaming, Stop CDC comes first again.
- **Until the CDC state has been read** in this session — just after the screen opens, or
  after an app restart — or when it could not be read, a new start point **waits** rather
  than guessing: the notice says so and points at **Re-check CDC state** on the CDC step
  (fix the AWS access first if the read failed).
- **Not gated by the new start point** (while CDC is live they are disabled too, as above):
  **Retry unfinished tables**, **Reload specific tables**, a per-table **Reload** and
  **Change & reload** all keep the **original** start point, so CDC continues
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
  and is not refused. After a first Start that CloudFormation rolled back, the rollback
  restores the stack's parameters, so they no longer hold the start point that Start
  offered, but the stack's CloudFormation events still do (the offset seeder's properties):
  the app judges the next Start against that point as it recorded it, or, after an app
  restart (or for a Start made in another session), as it reads it from those events. A
  start point later than that one is refused, a retry from the same one is not, and an
  earlier one is refused at the click on Start CDC, or by the Start CDC job when this
  session had not read that point yet (*This start point cannot be applied*: that offset
  stays seeded, and the seeder never rewinds). When the events do not give that
  point either — they cannot be read, for example, or that Start seeded Kafka in the app
  instead of through the offset-seeder Lambda, as the single EC2 host does
  ([§1.4](01-setup.md#14-run-on-a-single-ec2-host-from-source)) — no start point can be
  checked against it, so every start point is refused on that stack, that same one
  included (*This CDC stack's last start point is not known*: *CloudFormation rolled back
  this stack's last Start, and the start offset that Start offered is no longer known …*),
  with the same route. The CDC only route starts the new stack from the Full Load's
  watermark, so when the refused point is that watermark (Automatic), the refusal adds that
  route's condition: if that Full Load appended to rows already on the target, run it again
  choosing **Drop & reload** before that Start — CDC only has no Full Load step, so switch
  the migration type to Full load only for that run, then back to CDC only (an append keeps
  those rows as they were, and CDC from a watermark carries only the changes after it). In
  the disabled Start's tooltip and the job's error, the route reads *Delete CDC infrastructure and
  deploy it again, then Start CDC (if the Full Load appended to rows already on the target,
  re-run it choosing 'Drop & reload' first)*. On the new stack that Start is not refused,
  and the CDC step states the condition before it when this session shows that the Full
  Load may have appended to an earlier copy
  ([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)). On
  PostgreSQL there is no such refusal: Start CDC continues from the stack's own replication
  slot whatever the Full Load's watermark says, and a Full load only run leaves that slot
  alone, so CDC continues from
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

The unit an interrupted Full Load resumes from is the **table**, not a PK range:

- a finished table is recorded as `DONE` and kept;
- a stop, a crash or a task replacement leaves each unfinished table to be **reloaded
  from its start**, in the "skip existing" mode (§3.4) or, with **Drop & reload**,
  recreated first, so a very large table interrupted near its end is read again in full;
- `INSERT ... ON CONFLICT` makes re-running a batch idempotent (no duplicates).

The **Retry unfinished tables** path resets only the unfinished tables to pending,
**reuses the original watermark**, and keeps the completed tables — so you never reload
a finished table or lose the CDC handoff point.

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
  composite PK with an integer leading column such as `(tenant_id, id)`): their read can be
  split into K primary-key ranges, each loaded by its own worker process from a disjoint
  slice of the source. This reader-range sharding is **off by default**
  (`full_load_reader_shards=1`; *Reader shards per large table* in **Settings → Full Load**),
  and it only engages on this multi-process path (`TABLE_PARALLELISM` above 1), for a table
  of at least `SHARD_MIN_ROWS` estimated rows. K is the setting, at most 32 ÷
  `TABLE_PARALLELISM`.
  - **PostgreSQL**: in any run. Every range imports one shared snapshot, so the copy is one
    point in time, as with one reader.
  - **MySQL**: only in **Full load + CDC** whose start point has a binlog file and position,
    and only a table whose every unique key on Aurora DSQL includes the leading key column
    (the primary key, each UNIQUE constraint and UNIQUE index of the applied DDL and, for an
    append, each unique index of the live table). MySQL cannot share a snapshot, so each range
    reads its own: a row whose key changes during the read can be copied twice or not at all,
    and a transaction across two ranges half applied, with equal row counts. CDC, started
    after the load from the start point taken before any read
    ([§3.5](#35-the-watermark--the-bridge-to-cdc)), replays every change since then and
    corrects them, as it corrects tables read at different times. A unique key without the
    leading column (a `UNIQUE (email)`) would see a row's two copies, so such a table keeps
    one reader. **Full load only** reads every MySQL table with one reader: nothing would
    correct a split copy.
  - An append table whose applied primary key differs from the source's is split only while
    the live table's key equals the applied one; otherwise it loads as one unit, which
    recreates an empty table with the applied key or refuses the load, as with one reader.
  - A MySQL range is never re-read in place after a lost source connection: a second
    snapshot in one range could break a unique key that includes the leading column (a
    `(tenant_id, email)`). The range fails, the table is *Failed* with that reason (*range 2
    of 4*), and **Retry unfinished tables** loads it again. A PostgreSQL range re-reads its
    shared snapshot.
  - With Reader shards and `TABLE_PARALLELISM` both above 1, the activity log says, for each
    table large enough to split (an integer leading key column and at least `SHARD_MIN_ROWS`
    estimated rows), *read in K primary-key ranges* or *read with one reader* and why, and
    the Full Load table shows *read in K parts* under a split table's name.
- **All work units share one bounded pool** — `table_parallelism` controls the
  total number of concurrent worker processes; each range takes one slot.

```
ProcessPoolExecutor(max_workers=table_parallelism)     e.g. 8
  ├─ customers (small)             → 1 work unit
  ├─ orders (9M, int PK)           → K = 4 range units
  └─ order_items (33.6M, int PK)   → K = 4 range units
                                     at most 8 units run at once,
                                     one source reader each
```

### A table read in parts needs CDC's replay (MySQL)

Until CDC has replayed it, a MySQL table read in parts is torn, so every route that would take
it out of that replay is refused with the table's name and the way that ends the refusal.
Set *Reader shards per large table* to 1 in Settings and reload the table (one reader, one
snapshot) through the control the Full Load step shows for it — **Retry unfinished tables**
while it is unfinished, **Reload specific tables** once every table has loaded — then do the
operation. Full load only and a later start point have a second way: once CDC has replayed
the table, run Validation in **CHECKSUM** mode with *Fast sweep* off: a match proves the copy
and ends the refusal. Unticking the table has only the reload, before CDC starts: the replay
needs CDC started, and from CDC's first Start the table set is fixed (only Start over changes
it, [§13.7](13-excluding-tables-and-columns.md)). The control follows the tables the step
lists now: a table ticked since the load and still to load counts as unfinished, one unticked
since does not count.

- **Unticking it** in the table list after the load, or a table rule in *Exclude tables &
  columns* that unselects it: its checkbox is locked, with the reason on the lock icon.
- **Switching the migration type to Full load only** after the load (from CDC only too: there
  the route goes through Full load + CDC, whose Full Load step reloads it).
- **Start CDC from a later start point**: a Manual position, typed or from *Fetch current
  position*, later than the Full Load's watermark — the purged-binlog remedy's manual route
  too, and the start-point and GTID remedies that suggest *Fetch current position* say so.
  Automatic, or a Manual point at or before the watermark, replays every read and is never
  refused, nor is a Start that resumes a stopped stream; the refusal names that way first.
- Before the load, the *Too many tables for one CDC pipeline* route (load every table, then
  untick the ones CDC does not need) says so when Reader shards and `TABLE_PARALLELISM` are
  both above 1. After the load, the same hold on Deploy CDC infrastructure, Start CDC and
  Retry CDC names such a table as one that cannot be deselected and does not offer Full
  load only.

The table stays marked until one of two things clears the mark: a load that recreates it (a
**Drop & reload**), or a Validation whose **CHECKSUM** comparison matches it and started after
the table's last load ended — a ROW_COUNT run, or a CHECKSUM that *Fast sweep* skipped because
the counts agreed, does not. An Append, a retry that fails or stops, or a load that reads it
as one unit keeps its rows from the parts on Aurora DSQL, so it keeps the mark; a table whose
parts provably wrote no row (none ran, or none read a row before it stopped or failed) is not
marked. So a Retry or Reload that reads with one reader drops and reloads such a table, with
rows or empty, whatever the Append / Drop & reload choice says (the dialog says why); with
Reader shards still above 1, the reload reads it in parts again and an Append is kept. A
re-run of every table (**Re-run Full Load**, or **Re-run all tables**) drops and reloads such
a table too, whatever the choice and the Reader shards setting: it takes a new start point,
and CDC from that point never replays the changes made before it, so an Append would keep the
rows read in parts as they are.

A Start CDC, even one that streamed, does not end the refusals: the tool cannot tell from it
whether CDC replayed the table. After a Start that never streamed (the worker refused it, or a
connector failed), start CDC again from **Automatic** (or a Manual point at or before the Full
Load's watermark): that is the replay, and it is never refused. Once CDC has caught up, the
CHECKSUM match ends the Full load only and later-start-point refusals; from the first Start
on, the table set is fixed anyway ([§13.7](13-excluding-tables-and-columns.md)).

### Tuning it

For a large migration, set the worker count to the task's vCPU count — whole-table units and
range units queue for the same slots. Reader shards help when a few large tables would leave
slots idle, at the cost of concurrent reads of the same table
([§7.2](07-performance-and-tuning.md#72-tuning-parallelism)). The
knobs (`TABLE_PARALLELISM`, `BATCH_PARALLELISM`, `SHARD_MIN_ROWS`, `FULL_LOAD_READER_SHARDS`), their limits, and how
they interact with source load live together in
[Chapter 7 §7.2 — Tuning parallelism](07-performance-and-tuning.md#72-tuning-parallelism);
the measured throughput this design achieves (and the ThreadPool baseline it replaced) is
in [Appendix: Performance test results](12-performance-test-results.md).

---

**Next:** [4. CDC and DSQL constraints →](04-cdc-and-dsql-constraints.md)
