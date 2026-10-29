# 6. Limitations

_Language: **English** | [한국어](../ko/06-limitations.md) | [日本語](../ja/06-limitations.md)_

> **Prev:** [5. Validation](05-validation.md)

These are the **real, enforced** limits to plan around — most come from Aurora DSQL itself
(it's distributed and intentionally omits features that don't scale horizontally), a few
from how the tool is deployed. None of them are surprises at runtime: the tool flags them
in **Evaluation**, handles them where it can, and fails **loudly** where it can't.

---

## 6.1 Aurora DSQL feature limits (your schema must fit these)

> **Bottom line for schema design:** push large blobs, server-side logic, and
> very-high-precision numerics out of the database and into the application *before* you
> rely on DSQL for them, and keep any cascade fan-out within DSQL's per-transaction limit.
> Evaluation tells you exactly which objects need this.

Aurora DSQL does not support, or caps, the items below. Evaluation flags each one (the
finding is in parentheses), and
[§2.4](02-evaluation-and-schema-conversion.md#24-findings-reference) says what to do about
it.

**Blocks the migration** (**Unsupported**)

| Aurora DSQL | What happens, and what to do |
|---|---|
| A primary key is required (`NO_PRIMARY_KEY`) | A table without one can't be migrated: Full Load pages by primary key and CDC applies changes by key. |
| No triggers, stored procedures, functions or scheduled events (`TRIGGER_UNSUPPORTED`, `PROC_PLPGSQL`, `EVENT_UNSUPPORTED`) | Server-side logic doesn't move: reimplement it in the application (events → EventBridge/Lambda). |
| No row-level security (PostgreSQL source; `PG_ROW_LEVEL_SECURITY`) | After cut over every role that can read or write the table reaches every row, so enforce each policy's condition in the application. The migration user must bypass the policies (a superuser, `BYPASSRLS`, or the table's owner, except under `FORCE ROW LEVEL SECURITY`), or the table is refused rather than copied as a subset (prerequisite check `ROW_SECURITY`). |
| At most 255 columns per table, and 1,000 tables, 10 schemas and 5,000 views per database (`TOO_MANY_COLUMNS`, `TABLE_COUNT_LIMIT`, `SCHEMA_COUNT_LIMIT`, `VIEW_COUNT_LIMIT`) | Beyond these the `CREATE` fails (`54011` for columns, `54000` for the database limits). The database counts include what the target cluster already holds, and any of them makes the readiness score read **Blocked**. |
| At most 8 columns in a primary key or index; a MySQL source allows 16 (`TOO_MANY_KEY_COLUMNS`) | A wider primary key is rejected (error `54011`), so nothing loads; a wider index is left out and flagged **Review needed**, rather than emitting DDL that would fail after the load. |

**Behaves differently after cut over** (**Review needed**)

| Aurora DSQL | What happens, and what to do |
|---|---|
| Source `CHECK` constraints (`CHECK_CONSTRAINT_DROPPED`) | Not translated (for example a MySQL `CHECK`, 8.0.16+): dropped from the DDL. Re-add a DSQL-compatible `CHECK` or enforce the rule in the application. The `CHECK … IN (...)` generated for an enum is unaffected, but a value added to the source enum later is dead-lettered with `23514` until you replace it ([§4.2](04-cdc-and-dsql-constraints.md#detecting-and-handling-source-schema-drift)). |
| No `EXCLUDE` constraints, `WITHOUT OVERLAPS` keys or `PERIOD` (PostgreSQL source; `PG_EXCLUSION_CONSTRAINT`, `PG_TEMPORAL_KEY`, `PG_FK_PERIOD`) | Aurora DSQL has none of them, so after cut over a row that breaks such a rule is accepted. An `EXCLUDE` constraint and a `FOREIGN KEY … PERIOD` are not created, and a `WITHOUT OVERLAPS` key becomes an ordinary key (refusing only an exact duplicate). Enforce the rule in the application (make concurrent writers conflict on a row they both update), or change the design; before loading a large table with a `WITHOUT OVERLAPS` primary key, add a btree index on its key columns on the source. |
| No deferrable unique keys (PostgreSQL source; `PG_DEFERRABLE_KEY`) | Aurora DSQL does not support deferrable unique keys, so the key is checked at once: a write that briefly gives two rows one key value (swapping, renumbering) can fail where the source accepted it. With CDC, a `DEFERRABLE` primary key is also no `REPLICA IDENTITY`: a table left at `REPLICA IDENTITY DEFAULT` fails the CDC prerequisite check until you re-create the key `NOT DEFERRABLE` or set the table `REPLICA IDENTITY FULL`. |
| Sequences and identity columns are `bigint` (`PG_STANDALONE_SEQUENCE`, `NON_KEY_SEQUENCE`) | A sequence is always `bigint` with `CACHE` 1 or ≥ 65536, and an identity is allowed only on a `bigint` column (whose type cannot change after apply). A sequence no column owns is not created, and a non-key `serial`/identity column loses its generation: [§2.4](02-evaluation-and-schema-conversion.md#24-findings-reference) says how to keep them (for a column, declare it `bigint … AS IDENTITY` in the target DDL before applying). |
| No table inheritance (PostgreSQL source) | A parent and each of its child tables migrate as separate tables, each only if selected. The Full Load and Validation read a parent's own rows only, as CDC captures them, so a parent whose rows all live in its children (inheritance used as partitioning) loads none, and a child needs a primary key of its own (PostgreSQL does not inherit one). The prerequisite check warns about each such parent, and Start CDC refuses to snapshot it (*CDC cannot snapshot a table that has inheritance child tables*, [§4.3](04-cdc-and-dsql-constraints.md)). With CDC only, untick the parent (each child still migrates as a table of its own); with Full load + CDC, CDC starts from the Full Load's snapshot point, with no snapshot. |
| Spatial and PostgreSQL-only types (`SPATIAL_TYPE`, `PG_UNSUPPORTED_TYPE`) | A MySQL source's spatial columns become `bytea` (raw WKB is preserved). A PostgreSQL type with no Aurora DSQL equivalent gets a stand-in that keeps the value (arrays → `jsonb`, `money` → `numeric(23,4)`, geometric, network, enum and the rest → `text`), graded **Review needed**: the data migrates and the application adapts ([§2.3](02-evaluation-and-schema-conversion.md#23-type-mapping-reference)). A composite type, a domain and pgvector have no stand-in and stay **Unsupported**. |
| FULLTEXT and SPATIAL indexes (`UNSUPPORTED_INDEX_TYPE`) | Not emitted and flagged **Review needed**: the table and its data migrate, but the search the index served must move outside the database (for example Amazon OpenSearch Service). |

**Size and count caps**

| Aurora DSQL | What happens, and what to do |
|---|---|
| 1 MiB per column value, 2 MiB per row, 10 MiB per write transaction (`OVERSIZED_LOB`) | A column value over 1 MiB can't be stored, whatever the type: `54000 ERROR: maximum column size exceeded`; nor can a row over 2 MiB (`maximum row size exceeded`; [Aurora DSQL quotas](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)). Oversized values are **quarantined** per row (the error log in Full Load, the **DLQ** in CDC), and a column that can exceed the Kafka message cap must be **excluded at capture**. Binary and document columns (MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`) are flagged `OVERSIZED_LOB` as a loss, an unbounded PostgreSQL `text`/`varchar` as a recommendation. |
| At most 24 indexes per table: the primary key counts, so 23 secondary; a MySQL source allows 64 (`TOO_MANY_INDEXES`) | Schema Conversion keeps 23 and leaves the rest out, naming them in its note, so the table loads without them and a query that relied on one falls back to a scan. Flagged **Review needed** at planning time: choose which indexes to keep before you apply. |
| At most 1 KiB combined key size, for each primary key and index | Checked on the **value at `INSERT`/`UPDATE`**, not on the DDL, so only rows whose actual key is too long fail (error `54000` `key size too large`). Conversion warns when the *declared* widths could exceed it (4 bytes per character for `utf8mb4`), naming the key and its worst-case size, but never blocks. |

**Different by design** (the tool handles these)

| Aurora DSQL | What happens, and what to do |
|---|---|
| Foreign keys are enforced, with runtime costs (`PG_FK_NOT_ENFORCED`, `PG_FK_PERIOD`) | DML on the related tables pays **extra reads**, a concurrent conflict is a retryable `40001`, and a `CASCADE`/`SET NULL`/`SET DEFAULT` action counts toward the **3000-row/txn** limit (a cascade touching more than 3000 rows fails): prefer `NO ACTION`/`RESTRICT` for unbounded child counts. The tool applies the keys after the load as `NOT VALID` ([Foreign keys](02-evaluation-and-schema-conversion.md#foreign-keys)); you can strip them instead (`preserve_foreign_keys=False`) and enforce integrity in the application. A PostgreSQL key keeps its `MATCH FULL` and `DEFERRABLE` / `INITIALLY DEFERRED`; a PostgreSQL 18 key declared `NOT ENFORCED` is not created, since Aurora DSQL would enforce it, and neither is a `FOREIGN KEY … PERIOD`. |
| No native partitioning (`PARTITIONED_TABLE`, advice only) | Aurora DSQL distributes rows itself: Schema Conversion drops the partitioning and the partitions become one table (with CDC, pause partition maintenance until cut over). |
| No `TRUNCATE`, one DDL per transaction, optimistic concurrency, IAM-token authentication (no password, short-lived tokens) | The tool handles all of these: it drops and re-creates instead of `TRUNCATE`, runs single-DDL units, retries `40001` everywhere, and mints and refreshes IAM tokens (also for the CDC sink). |

---

## 6.2 Migration-process limits

### Start points, region and source prerequisites

- **A new CDC start point needs new CDC topics first.** Wherever this chapter sends you to
  **Re-run Full Load with "Drop & reload"** (or **Re-run all tables**), or to a newer
  **Manual** start point, to give CDC a newer start point, that start point is **refused**
  while this migration's CDC stack has streamed — stopped after a Start, its connectors in
  place, or a Start CloudFormation rolled back. **Delete CDC infrastructure** and deploy
  it again first, then the re-run (or the Manual point), then **Start CDC**
  ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). Otherwise that
  stack's sink would re-apply changes older than the new start point over the reloaded
  tables.

  | | What happens |
  |---|---|
  | **Full load + CDC** | The Full Load step refuses the re-run. |
  | **MySQL source** | Start CDC refuses a start point newer than the one the stack last started from, whatever the migration type, so a detour through **Full load only** does not get around it. |
  | **Full load only** | Full load only has no CDC step: on a detour through it, switch the migration type first, then delete and deploy on the CDC step. |
  | **PostgreSQL source** | On a PostgreSQL source Start CDC has no such refusal: it continues from the stack's own replication slot, which a Full load only run leaves as it is, so CDC continues from where it stopped if the stack's source connector streamed, gapless only while the CDC topics still retain what the sink had not applied, and the reloaded tables converge. Otherwise Debezium snapshots every selected table first, and a row deleted on the source between the reload and that snapshot stays on the target: Validation reports it as an EXTRA row. |
  | **Reload specific tables**, **Retry unfinished tables** | They keep the original start point, so this refusal does not apply to them (while CDC streams they wait for Stop CDC, like every Full Load). |

  A Manual point is a new start point: the changes between where CDC stopped and it are
  not replicated, on the new stack too, so reload or correct those tables and validate
  with CHECKSUM, as each remedy below says.

- **Single region — no cross-region migration.** The tool works in any region where DSQL
  is available, but **source and target must be in the same region**, and the optional CDC
  pipeline runs in a single region/VPC. Cross-region is not supported.

- **CDC has source-side prerequisites (engine-specific).** The prerequisite gate **blocks
  CDC** until they are met; **Full Load alone needs none of this.** How to set them up:
  [§1.6](01-setup.md#16-prepare-the-source-for-cdc).
  - **MySQL**: Binary logging in **ROW format with a full row image**
    (`binlog_format=ROW`, `binlog_row_image=FULL`), a user with **replication privileges**
    (plus `RELOAD` for a Full load + CDC run unless the source runs `gtid_mode=ON`), and
    **binlog retention raised** so the log at the Full Load watermark survives until CDC
    starts (Aurora MySQL keeps binlogs only 24 h by default).
  - **PostgreSQL**: `wal_level=logical`, a **replication-privileged user**, the cluster
    **WRITER** (not a standby/reader), and a usable **`REPLICA IDENTITY`** on every
    replicated table (every partition of a partitioned one): `DEFAULT` needs a valid
    primary key that is not `DEFERRABLE`, `USING INDEX` a valid unique index, and `FULL`
    works for any table except, on PostgreSQL 18, one with a generated column (PostgreSQL
    18 refuses `UPDATE`/`DELETE` on a published table whose identity includes a generated
    column the publication does not publish, so there neither `FULL` nor a key with a
    generated column will do). Without one, the CDC prerequisite check fails the table and
    the Full Load stops before it changes anything on the source; each refusal names the
    fix, with the statement to run. The **logical replication slot** the tool creates pins
    the WAL instead of a retention window — watch `wal_status`, because an inactive slot
    can fill the source disk.

- **PostgreSQL source, re-keyed tables:** a table CDC re-keys — Schema Conversion's
  **Composite key**, or an edited target key that adds a column outside the source primary
  key — needs **`REPLICA IDENTITY FULL` on the table and on every partition, for the whole
  CDC run**, not only at Start (a partition created later starts at `DEFAULT`). Below
  FULL, a `DELETE` on that table can leave its row on Aurora DSQL with no error and no
  dead letter, and an `UPDATE` that changes the added key column is dead-lettered and
  leaves the old row. The prerequisites block Start. On PostgreSQL 18 a re-keyed table
  with a generated column cannot keep FULL (its `UPDATE`/`DELETE` would fail on the
  source, see above), so the prerequisites, the Full Load and the load of a table selected
  after the Full Load refuse it before any write: set it back to **Keep source PK** in
  Schema Conversion (for such a table the CDC monitor's notice, below, names that fix
  instead of FULL: then reload the table, or, with CDC only, migrate it again with **Start
  over**). While the Data Migration, Validation or Cut over step is open, the tool
  re-reads it at most every 2 minutes: a red notice appears in the CDC monitor, and Cut
  over is held until it is FULL again and the table matches by CHECKSUM (see
  [§5.6](05-validation.md#56-re-keyed-postgresql-tables-and-cut-over)). After an app
  restart the re-check needs the source password again (reconnect on the Connect step). A
  drop and its reversal between two re-checks are not seen, so keep FULL in place.

### Primary-key changes and unique values

- **Primary-key changes and unique values moving between rows (both engines).** A
  primary-key `UPDATE` reaches CDC as a delete of the old key plus an insert of the new
  key, which can land on different partitions of a topic with more than one (fewer than 8
  captured tables, or a large table given 2 or 4) and apply in either order; a unique
  value passed from one row to another (an email address moved to another account) can do
  the same. On a table with a unique index besides its primary key, the insert can then
  meet the old row, which Aurora DSQL rejects at once (`23505`).
  - **CDC infrastructure deployed by this version** (sink plugin v53 or later): The sink
    **holds** that change, and every later change on its partition, until the other row's
    change has applied, for up to **5 minutes**; other partitions keep flowing and the
    table's Stream lag shows the wait. On a table that is not re-keyed, when a later
    change of the same row replaces it entirely, the change is skipped instead.
  - **Still dead-lettered** (*a unique constraint rejected the row*, with the index name):
    The change that frees the value is more than 5 minutes behind (very uneven partition
    lag during a long catch-up); the target already holds the value from outside the
    stream; two changes wait on each other (a value passed along several rows, or a
    PostgreSQL transaction that swaps unique values under a `DEFERRABLE` unique
    constraint); an older change replayed after a restart or a re-read of the stream meets
    values its own later change already wrote; or, on a re-keyed (Composite key) table,
    the row's own later change already made the conflict moot (the target ends up right,
    but the change waits the 5 minutes and is dead-lettered).

  Either way Cut over holds the table until it matches by CHECKSUM (the CDC step then
  shows it as *Verified by CHECKSUM*). **Remedy:** Stop CDC, reload the table on the Full
  Load step with **Drop & reload** (CDC only: correct the rows on Aurora DSQL by hand),
  Start CDC, then validate it. A source that dropped or relaxed the `UNIQUE` index gives
  the same dead letter (CDC replicates no DDL): drop the index on Aurora DSQL too, and
  with Full load + CDC re-run Step 1 (Evaluation) first, so the reload does not rebuild
  it.

- **Re-keyed tables on CDC infrastructure deployed by an earlier version** keep their
  change records keyed on the composite key (the Start CDC log says so). An `UPDATE` of
  the leading key column — or a delete and re-insert of a row with another leading value —
  is then replicated as a delete of the old key plus an insert of the new one, which can
  apply in either order. If the insert comes first it is dead-lettered (*a unique
  constraint rejected the row*) and the delete removes the row, so it is missing until the
  table is reloaded (on any CDC infrastructure deployed by an earlier version a unique
  conflict is dead-lettered at once, without the wait above); Cut over holds the table
  until it matches by CHECKSUM. **Delete CDC infrastructure** and deploy it again to key
  these tables on the source primary key and to pick up the wait. That also deletes the
  stream position: on MySQL, Start CDC then re-reads the binlog from the Full Load
  watermark (CDC only: it begins at the source's current position, so changes made after
  the last one Aurora DSQL applied are not replicated); on PostgreSQL it drops the
  replication slot, so the next Start reads every table again (Full load + CDC: choose
  **"Re-snapshot every table instead"** when the Start CDC dialog offers it) and a row the
  source deleted meanwhile stays on Aurora DSQL. See "Starting CDC again after a delete"
  and "The cost of deleting a CDC-only stack" in
  [Chapter 13](13-excluding-tables-and-columns.md#why-the-set-locks).

### Source schema changes while CDC runs

- **CDC replicates data, not DDL.** Schema changes on the source during CDC are **not**
  propagated to DSQL — you make the matching change on Aurora DSQL yourself:

  | Source change | What you do on Aurora DSQL |
  |---|---|
  | A column added or renamed | **Fix target schema…** |
  | A column that now allows NULL | `ALTER … DROP NOT NULL` |
  | A type or key change | Recreate the table with CDC stopped (**Replace** + **Drop & reload**; CDC only has its own repair). |

  Schema Conversion's Apply, Replace included, is blocked while CDC streams (see
  [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
  Some changes alter what CDC writes without any dead letter (a dropped nullable column, a
  widened type, a renamed table, …), so while the CDC monitor, Validation or Cut over step
  is open the tool re-reads the captured tables' definitions at most every 5 minutes and
  holds Cut over on a difference from the Step 1 inventory (see
  [§5.7](05-validation.md#57-source-schema-changes-and-cut-over)). What that re-check
  cannot see:

  - a change undone between two reads, or one made while no CDC monitor, Validation or Cut
    over page is open (it is seen at the next one);
  - a same-shape `DROP TABLE` + `CREATE TABLE` (MySQL has no stable table identity to
    compare), or a PostgreSQL enum gaining a value (`ALTER TYPE … ADD VALUE`);
  - defaults, comments, collations, indexes and generated-column expressions (not
    compared);
  - a change already in the Step 1 inventory when the tool first checked the table (a Step
    1 re-run before CDC ran becomes the baseline);
  - after an app restart, anything until you reconnect the source on the Connect step (the
    password is kept in memory only); and, on a very large MySQL 5.7 catalog the bounded
    read cannot finish, the CDC monitor and Cut over report the failed read.

- **PostgreSQL: DDL while CDC is stopped.** Nothing replays the DDL, because `pgoutput`
  carries none. The source schema re-check above reports a changed key or NULL rule at its
  next read, but it cannot tell you that, when CDC continues, the backlog may already have
  been applied with the table's *current* key and NULL rule, as if the change were already
  in force for earlier changes too. Give it the same follow-up on Aurora DSQL as a live
  change, and validate the table with CHECKSUM before Cut over.

- **A source `TRUNCATE` is not applied by CDC.** Aurora DSQL has no `TRUNCATE`, so CDC
  dead-letters it and the CDC step names the table. Cut over is held for that table until
  you remove the extra rows (Drop & reload for Full Load + CDC, a manual delete for CDC
  only) and it matches by CHECKSUM (see
  [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
  On MySQL, `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` — and on PostgreSQL,
  dropping, detaching or truncating a partition, or attaching one that already holds rows
  — produce no change event at all; only Validation shows the extra (or missing) rows.
  Pause partition maintenance until cut over.

- **MySQL: a schema change between a CDC start point and the Start that uses it breaks the
  stream.** A Start that begins from an earlier binlog position — the first Start from the
  Full Load watermark or a Manual binlog start point, and the first Start after **Delete
  CDC infrastructure** and a new deploy, which reads again from the **original** watermark
  — replays the binlog against the tables' **current** definitions, matching each row's
  values to columns by position. A column added, dropped or renamed in between stops the
  connector on every Start (the CDC step names the cause — in the Start CDC error, or on
  the connector's Pipeline health row if it stops after starting,
  [§4.6](04-cdc-and-dsql-constraints.md#pipeline-health-and-change-flow)); a change that
  keeps the column count (a type change other than a length or a DECIMAL precision, a
  reorder, a dropped column replaced by another, a character-set conversion) writes
  shifted or NULL values with **no error at all**.
  - **The check.** The Full Load watermark, and **Fetch current position** for a Manual
    point, record a short per-table fingerprint of the captured tables (column order,
    types, NULL rules, character sets and primary key). Before such a Start, Start CDC
    compares the tables with it and **refuses**, naming each changed table (it needs the
    source connection, so re-test it on the Connect step if it refuses for that). It only
    **warns** when the start point has no fingerprint: one recorded by an earlier version
    of the tool, a position typed by hand, or one whose definitions could not be read. A
    new length of a `CHAR`, `VARCHAR` or `VARBINARY` column, or a new `DECIMAL` precision
    at the same scale, decodes the same and is not refused; the source schema re-check
    ([§4.2](04-cdc-and-dsql-constraints.md#changes-that-dead-letter-nothing-the-source-schema-re-check))
    still reports it, and the Aurora DSQL column may need widening. Widening an integer
    type or appending an `ENUM` value is still refused.
  - **What it cannot see.** A column rename on a table CDC does **not** capture, and a
    change made while the connectors are being created or before an automatic task restart
    (a run started from an earlier position stays exposed until Stop CDC). The source
    schema re-check reports a retype or a replaced column and holds Cut over, but neither
    check sees a reorder or a character-set conversion, so validate the table with
    CHECKSUM after such a change.
  - **Way forward.** Starting again, Retry CDC and Debezium's "recovery mode" never help —
    they replay the same rows. Move the start point past the change:
    - Stop CDC. On a stack that has streamed (a restart after Stop CDC, or a connector
      that stopped mid-run), **Delete CDC infrastructure** and deploy it again first: you
      get new, empty topics, and until then the Full Load step refuses a new start point
      and Start CDC refuses a newer one.
    - Re-run Step 1 (Evaluation) and update the changed tables in Schema Conversion, so
      the reload recreates and reads the new definition — a table whose DDL you edited, or
      whose key strategy you chose, keeps that edit (redo or reset it), and a dropped or
      renamed column must be dropped on Aurora DSQL first or the table re-applied with
      Replace.
    - **Full load + CDC:** **Re-run Full Load with "Drop & reload"** (the default *Append*
      keeps existing rows, so updates and deletes made in between would be lost). The new
      run's watermark is past the change, and Start CDC on the new stack (or the stack's
      first Start) starts from it. **CDC only:** there is no Full Load to re-run: update
      the changed tables on Aurora DSQL, then choose **Manual**, **Fetch current
      position** and **Use this start point**, and Start CDC. Changes to the captured
      tables made before that point are then not replicated: reload them with "Drop &
      reload" (Full load + CDC) or correct them on Aurora DSQL (CDC only), and validate
      with CHECKSUM. A reload switches the CDC start point back to Automatic, so choose
      Manual again (the fetched point is kept) before Start CDC.
  - **A restart after Stop CDC is different.** It continues from its recorded position and
    **replays the recorded schema history**, so a DDL made while CDC was stopped is
    applied in order, like a change made while it streams, and needs the same follow-up
    ([Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
    If that history is missing, the Start fails saying so. Stop CDC (or **Clean up
    leftover connector**) if the connectors are still listed — a Start or Retry CDC on
    connectors that exist keeps their snapshot mode and fails the same way — and the next
    Start rebuilds it after the same check. That check compares the tables with the
    original start point, so a change made even before the Stop refuses that Start.

### MySQL `gtid_mode`

- **MySQL with `gtid_mode=ON`: CDC starts after the start point's GTID set.** Debezium
  positions by GTID only when the source's `gtid_mode` is exactly `ON`: it then streams
  every transaction that is not in the GTID set recorded with the start point and ignores
  the start point's binlog `file:position` (with `OFF`, `OFF_PERMISSIVE` or
  `ON_PERMISSIVE` it uses the `file:position`). On an `ON` source the GTID set therefore
  *is* the start point, so Start CDC reads `gtid_mode` first — in the Start dialog and
  again before it changes anything (a refusal there fails the Start with *Start CDC
  blocked: …*, and the CDC step shows the whole notice, with its fix, under the failed
  Start) — and refuses a start point it cannot use:

  | Start point | What to do |
  |---|---|
  | **No GTID set** (a position you typed, or a Manual start point saved by version 0.1.565 or earlier, which comes back without its GTID set): MySQL would resend its whole retained binary log, re-applying changes made before the start point | **Fetch current position** records the position and the GTID set together. For a position you type, also enter the GTID set the source had executed at exactly that position (for example the one your dump recorded); a Manual point saved by 0.1.565 or earlier needs fetching again. |
  | **Recorded while `gtid_mode` was not `ON`** (also a restart after Stop CDC of a stack started from such a point, or from a start point without a GTID set, and any start of a stack whose last start ran while `gtid_mode` was not `ON`) | See the next entry. |
  | **A GTID set the source has not executed** (it was read from another server, or entered incorrectly), **or one the source has purged binary logs after** | Give CDC a new start point — **Re-run Full Load with "Drop & reload"**, or **Manual** → **Fetch current position** → **Use this start point** (changes to the captured tables between the two points are then not replicated: reload them with "Drop & reload" — CDC only: correct them on Aurora DSQL — and validate with CHECKSUM) — raising the binlog retention first if it ran out. If checking the GTID set against the source fails instead (the connection dropped, a read timed out), nothing is known to be wrong with the start point: check that the source is reachable from the tool, then Start CDC again. |
  | **`gtid_mode` cannot be read** (this session has no source connection — the password is kept in memory only, so an app restart clears it — or the read failed) | Re-test the source connection on the Connect step, then Start CDC again. |

  On the Manual card, editing the position after **Fetch current position** drops the
  fetched GTID set (it was read at the fetched position) — also when the position is left
  empty or mistyped — and editing the GTID set drops the recorded table definitions, so
  the start-point check above then only warns. A GTID set you enter must be a MySQL GTID
  set (`<server UUID>:<interval>`, comma-separated, for example
  `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100`): Start CDC refuses any other text — quotes
  or a `SET` statement copied with it, say — on every `gtid_mode`.

- **MySQL: decide `gtid_mode` before the Full Load and keep it until cut over.** A start
  point recorded while `gtid_mode` was not `ON` cannot start CDC once the source is `ON`:
  MySQL cannot send the changes written without a GTID by GTID, so it stops the connector
  on them or skips them with no error. Keep the start point by setting `gtid_mode` back to
  `ON_PERMISSIVE` until cut over (CDC then streams by `file:position`), or give CDC a new
  start point — for a restart after Stop CDC (a stack that has streamed), after **Delete
  CDC infrastructure** and a new deploy first: **Re-run Full Load with "Drop & reload"**,
  or **Manual** → **Fetch current position** → **Use this start point**, then reload the
  tables changed in between with "Drop & reload" (CDC only: correct them on Aurora DSQL),
  Start CDC and validate with CHECKSUM. A new stack has no committed offset, so CDC then
  starts exactly at its start point — the one you hold if `gtid_mode` was already `ON`
  when it was recorded and has stayed `ON` since, else a new start point as above.
  - **What Start CDC refuses.** A start from such a start point, and a restart after Stop
    CDC of a stack started from it or from a start point without a GTID set (it streamed
    by `file:position` and holds no GTID set to continue from), even when the session no
    longer holds the start point (an attached stack, a reset session). Start CDC also
    records on the CDC stack the source's `gtid_mode` each time it starts the source
    connector (Stop CDC keeps the record), and on an `ON` source it refuses any start of a
    stack whose last start ran while `gtid_mode` was not `ON`: a restart after Stop CDC,
    and also a new start point, because that stack streamed by `file:position` from that
    start and the GTID set in its committed offset stopped advancing there.
  - **Do not change `gtid_mode` while CDC streams.** The change takes effect at the
    connector's next automatic restart, can stop it or skip changes with no error, and the
    tool cannot intercept it: Stop CDC first.
  - **Earlier versions.** A stack last started by version 0.1.566 or earlier holds no
    record until a start reads the mode, and start points recorded by 0.1.565 or earlier
    do not record it: keep such a source not `ON` until cut over, or give CDC a new start
    point on a new stack (Delete CDC infrastructure and deploy it again first).
- **MySQL with `gtid_mode=ON`: pipelines started by version 0.1.565 or earlier.** Those
  versions emptied the GTID set Debezium starts from, so on a `gtid_mode=ON` source MySQL
  re-sent its whole retained binary log at every Start and every automatic connector
  restart. Writes converged (the sink is idempotent), but older values were re-applied,
  unique-key conflicts could be dead-lettered, and an unparseable DDL anywhere in the
  retained log — even before the start point — stopped every Start. A running pipeline
  keeps that behaviour until one **Stop CDC** → **Start CDC**, which then continues from
  where it stopped: the changes the sink had not applied at the Stop come from the CDC
  topics, so this is gapless only while the topics still retain them
  ([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)). A
  pipeline started from a Manual point (including CDC only) can fail that Start with *the
  source has purged binary logs CDC's GTID set does not cover*: Stop CDC, set the source's
  `gtid_mode` to `ON_PERMISSIVE` until cut over (a parameter-group change on RDS and
  Aurora), then Start CDC — it resumes from where it stopped by `file:position`, with no
  gap while the CDC topics still retain the changes the sink had not applied when CDC
  stopped (if the binlog retention also ran out, that Start fails instead). Or give CDC a
  new start point, as the message says: this stack has streamed, so first **Delete CDC
  infrastructure** and deploy it again, then **Re-run Full Load with "Drop & reload"** for
  all tables (CDC only: **Manual** → **Fetch current position** → **Use this start
  point**, then correct the tables changed in between on Aurora DSQL), then Start CDC and
  validate with CHECKSUM. Validate any pipeline that ran on a `gtid_mode=ON` source with
  an earlier version with CHECKSUM before cut over.

### A MySQL DDL statement CDC cannot parse

- **MySQL: a DDL statement Debezium cannot parse, anywhere on the source server, stops
  CDC.** Debezium parses every DDL statement in the binary log with its MySQL grammar —
  for every database and table on the server, not only the captured ones, including stored
  routines, triggers, views and account statements — before it filters anything, and the
  tool's configuration stops rather than skip one it cannot parse. The source connector
  stops just before the statement and its position cannot move past it: Retry CDC, and
  Start CDC after Stop CDC, stop on it again. The CDC step names the cause — in the Start
  CDC error, or on the connector's Pipeline health row with an error notice once it has
  started ([§4.6](04-cdc-and-dsql-constraints.md#pipeline-health-and-change-flow)) — and
  shows the beginning of the statement with quoted values hidden. Only a statement from
  the start point on stops CDC.
  - **Way forward.** Stop CDC, then **Delete CDC infrastructure** and deploy it again: the
    stack has streamed, so a new start point needs new, empty topics (until then the Full
    Load step refuses a new start point, and Start CDC a newer one). Then give CDC a start
    point after the statement: **Re-run Full Load with "Drop & reload"** for all tables
    (Full load + CDC; re-run Step 1 Evaluation and Schema Conversion first if the
    statement changed a captured table), or **Manual** → **Fetch current position** →
    **Use this start point**; changes made between where CDC stopped and that point are
    then not replicated, so reload those tables with "Drop & reload" (CDC only: correct
    them on Aurora DSQL) and validate with CHECKSUM. A reload switches the start point
    back to Automatic, so choose Manual again before you **Start CDC** on the new stack.
    Do not use the *binlog reader near position* from the connector log as a start point:
    it is where the failing statement begins.
  - **When moving the start point does not help.** If a table's own definition uses syntax
    the grammar does not support, every Start that builds the schema from the source (the
    first Start, a Start from a new start point, a Start that rebuilds a missing schema
    history) stops while reading it, whatever the start point: change that definition on
    the source, then Stop CDC and Start CDC, or migrate that server with Full load only. A
    statement that runs again (a schema-migration tool, a scheduled job) stops CDC each
    time it runs.
  - **No skip option.** The tool offers no way to skip a statement CDC cannot parse: a
    skipped statement could leave later rows written to the wrong columns with no error.
    PostgreSQL sources are not affected: their change stream carries no DDL.

### Foreign keys

- **Cascading FK actions don't replicate over CDC.** Server-side
  `ON DELETE/UPDATE CASCADE` (and `SET NULL`/`SET DEFAULT`) actions fire *inside* the
  source engine and may not be captured by the change stream, so CDC can't apply them —
  the child rows the source cascaded are left behind on the target (for a MySQL source,
  InnoDB fires them without ever writing them to the binlog). The tool re-creates the
  foreign key on DSQL **at cut over** (never during replication): it is still added at
  once (`NOT VALID`), but Aurora DSQL's **validation of the loaded rows fails** on those
  orphaned rows — the cut-over screen names the key, counts the rows and lists them, and
  for a MySQL source **I've cut over** waits until the key validates or you choose to cut
  over without that check — and the **Validation orphan check** reports them too, rather
  than letting them silently diverge. Evaluation flags such tables **MANUAL**; replace the
  automatic action with explicit child-row statements in the application, and **quiesce
  source writes before the final cut-over comparison** so the divergence is caught.
- **A foreign key Aurora DSQL could not validate stays `NOT VALID`.** It is in place and
  enforced for every new write: an insert, or an update that changes the key, naming a
  missing parent is refused (`23503`), and so is deleting or re-keying a parent row that
  is still referenced (`NO ACTION` / `RESTRICT`; `CASCADE` and `SET NULL` act as usual).
  The rows already there are not checked again: updating a non-key column of such a row,
  deleting it, or pointing it at an existing parent works — re-saving it as a delete plus
  a re-insert does not, like any new orphan. The key is not proven until the rows are
  fixed (in batches of at most 3,000 rows per transaction) and **Validate again**
  succeeds.

### Capacity and cost

- **One CDC pipeline captures a bounded number of tables: about 80–160 with typical
  names.** CloudFormation limits a parameter value to 4,096 bytes, and the CDC stack
  receives the captured tables (the table list and the sink's topic list) in parameters,
  so one pipeline captures at most about 4,097 ÷ (L + 11) tables, where L is the average
  length in bytes of a qualified table name (`database.table` on MySQL, `schema.table` on
  PostgreSQL): about 155 tables with 15-byte names, 130 with 20, 100 with 30 and 80 with 40.
  Evaluation allows 1,000 tables, and Full load only has no such limit.
  - **Deploy CDC infrastructure and Start CDC refuse a larger capture** before anything is
    uploaded or created (*Too many tables for one CDC pipeline*, naming the parameter and
    its size). Until you click Deploy, the deploy form warns when the parameters could not
    fit at worst (*Possibly too many tables for one CDC pipeline*).
  - **With Full load + CDC, the Full Load is not held.** Before the load, the Full Load
    step warns (*Too many tables for one CDC pipeline*, or *Possibly too many tables for
    one CDC pipeline*) and says about how many of the selected tables one pipeline
    captures. You can still load every table and then, before CDC starts, untick the
    tables CDC does not need to capture (see "Changing the table set after a Full Load
    (Full load + CDC)" in [Chapter 13](13-excluding-tables-and-columns.md)): they stay on
    Aurora DSQL as loaded, and CDC and Validation skip them. A MySQL table the load reads
    in separate-snapshot parts (an explicit *Reader shards per large table* of 2 or more,
    [§3.8](03-full-load.md#38-parallel-loading)) is the exception: it cannot be unticked
    until it is reloaded with one reader (Drop & reload).
  - Otherwise capture fewer tables in this CDC pipeline (deselect them in the table
    picker), or use Full load only.
- **MySQL: a table read in parts is consistent only once CDC replays it.** With an
  explicit *Reader shards per large table* of 2 or more, a Full load + CDC load from a
  binlog start point reads a large MySQL table in primary-key ranges, each from its own
  snapshot ([§3.8](03-full-load.md#38-parallel-loading)). Until CDC, started from the Full
  Load's start point, has replayed the table, a row whose key changed during the read can
  be on Aurora DSQL twice or not at all, and a transaction across two ranges half applied,
  with equal row counts: a ROW_COUNT validation does not see it, a CHECKSUM does. So every
  route that would leave such a table without that replay (unticking it, Full load only, a
  Start CDC from a later start point) is refused until the table is reloaded with one
  reader (Drop & reload: an Append keeps those rows); Full load only and the later start
  point are also allowed once a Validation in CHECKSUM mode matches it. Unticking needs
  the reload before CDC starts: from CDC's first Start the table set is fixed. No table is
  read this way by default, nor in a Full load only run, nor a table whose unique key does
  not include the leading key column: those are read by one reader session from one
  snapshot.
- **CDC is billable while deployed.** The streaming pipeline (MSK Serverless + MSK
  Connect, plus a NAT gateway if created) costs money for as long as it runs. Tear down
  the cdc-stack after cut-over. Full Load alone provisions no streaming infrastructure.

### Values Aurora DSQL cannot hold

- **A value its target column cannot hold exactly is refused, never converted.** This
  covers:
  - a number other than 0 or 1 for a `boolean` column (a MySQL `TINYINT(1)` holding `2`,
    or, on CDC, a PostgreSQL `boolean` changed to an integer type);
  - more decimal places than a `numeric(p,s)` column keeps;
  - a fraction for an integer column.

  It usually happens when a source type was widened after Step 1, or a narrower type was
  chosen in Schema Conversion. Full Load stops that table (table-fatal). CDC dead-letters
  the row and shows a banner; CDC infrastructure deployed by an earlier version of the
  tool still writes the changed value until it is deleted and deployed again. Give the
  column a type that holds the values in Schema Conversion (re-apply with **Replace**) and
  reload, or clean the source data. A bare `numeric` target (Aurora DSQL's default
  `numeric(18,6)`, warned in Schema Conversion) still rounds a PostgreSQL unconstrained
  `numeric` to 6 decimal places, by design. Not checked: `FLOAT`/`DOUBLE` values in a
  `numeric` column.

- **Dates and times before 1582-10-15, and MySQL years 0–99, on CDC infrastructure
  deployed by an earlier version.** Full Load stores a `DATETIME`, `timestamp`,
  `timestamptz`, `DATE` or `date` as it is, including MySQL's documented minimum
  `1000-01-01` and the `0001-01-01` that .NET's `DateTime.MinValue` and Go's zero
  `time.Time` write; so does CDC from sink plugin v54.
  - An earlier sink wrote a `DATETIME`, `timestamp` or `timestamptz` before 1582-10-15
    days off, in the Julian calendar (`1000-01-01` as `0999-12-27`, `0001-01-01` as
    `0001-01-03`), and a `DATE` or `date` in the ten days the Gregorian reform skipped,
    1582-10-05 to 1582-10-14, ten days later (`1582-10-10` as `1582-10-20`); an earlier
    MySQL source read a year 0–99 of a `DATETIME` or `DATE` as 2000–2069 or 1970–1999
    (`0001-01-01` as `2001-01-01`). Nothing was dead-lettered and ROW_COUNT still matches,
    so only a CHECKSUM shows it. **Delete CDC infrastructure**, deploy it again, then
    reload such tables (Drop & reload) and validate with CHECKSUM.
  - On a PostgreSQL source, though, the rows a Debezium snapshot copies (every CDC only
    Start, a re-snapshot or a Manual start point; [§4.3](04-cdc-and-dsql-constraints.md))
    are read through JDBC before they reach the sink, so with any sink plugin a `date` or
    `timestamp` from 1582-10-05 to 1582-10-14 lands ten days later and a BC one loses its
    era (`0044-03-15 BC` as `0044-03-15`), and a `timestamptz` before 1582-10-15 lands
    days off, read in the Julian calendar (`1000-01-01` five days later, `0001-01-01` two
    days earlier). Migrate such tables with **Full load + CDC** (its slot hand-off takes
    no snapshot) and validate them with CHECKSUM.
- **Zero and invalid MySQL dates.** MySQL can store dates Aurora DSQL has no value for:
  the zero date `0000-00-00`, a date with a zero month or day (`2024-00-15`), the zero
  `TIMESTAMP`, and, under `ALLOW_INVALID_DATES`, a day the month does not have
  (`2024-02-31`). They get in where `sql_mode` lacks strict mode, `NO_ZERO_DATE` or
  `NO_ZERO_IN_DATE`, or a column's `DEFAULT` is the zero date; Evaluation's `ZERO_DATES`
  item ([Chapter 2 §2.1](02-evaluation-and-schema-conversion.md)) lists the date columns
  and a query per table that counts the rows holding one. Full Load writes each such value
  as the bundled CDC connector (Debezium) writes it from the binary log, so a row is the
  same whichever path wrote it:
  - a zero date, or a zero year, month or day, in a `DATE` or `DATETIME` column becomes
    NULL where the column allows NULL; in a `NOT NULL` column it becomes the column's
    `DEFAULT` when that is a date (not `CURRENT_TIMESTAMP` or another expression), or else
    `1970-01-01` (`1970-01-01 00:00:00` in a `DATETIME`);
  - the zero `TIMESTAMP` becomes `1970-01-01 00:00:00` UTC, whether or not the column
    allows NULL;
  - a day the month does not have becomes `1970-01-01` (`1970-01-01 00:00:00`), the value
    Debezium substitutes for a date it cannot read (the bundled connector stops at one
    instead, below).

  A table that held any gets one WARNING line in the activity log with how many, per
  column, and what they became (never the values). Validation renders the source's values
  the same way before it checksums them, so a table whose only differences are such values
  matches, and any other difference still shows. What does not carry over:

  - **CDC stops at a day the month does not have.** The source connector cannot read a
    binary-log change to a row that holds one — in any table the binary log records, not
    only the migrated ones — so it stops at that change, and at the same change again
    after every restart. Correcting such a row is itself such a change, so correct them
    before CDC's start point is recorded: before the Full Load in **Full load + CDC**,
    before **Start CDC** in **CDC only**. Corrected after that point, give CDC a newer
    start point (**Re-run Full Load** with "Drop & reload") before **Start CDC**; once CDC
    has stopped at one, it continues only from a newer start point (the first item of this
    section).
  - **A primary key that holds such a date.** Full Load leaves a key column's value as
    stored: written as a fixed date (`1970-01-01`, or the column's `DEFAULT`), rows whose
    keys hold one would fall onto one key — each other's, or the key of the row that holds
    that date. Aurora DSQL rejects the stored value (`22008`), so such a row is
    quarantined and Validation reports the difference. CDC writes a change to such a row
    at the fixed date instead, where it can overwrite or delete the row whose key that is.
    Correct those keys on the source before the Full Load.
  - **A unique key over such a column.** Full Load leaves the value of a unique key's
    column as stored too: written as a fixed date, different zero or invalid dates could
    become one value, or the value another row holds, and the load would fail on the
    duplicate. Aurora DSQL rejects the stored value (`22008`), so such a row is
    quarantined and Validation reports the difference. CDC writes a change to such a row
    with the fixed date instead, and dead-letters it when another row holds that value
    (`23505`). Correct those values on the source first.
  - **The application's SQL.** Aurora DSQL rejects such a date in an `INSERT`, an `UPDATE`
    or a `WHERE` (`22008`), and the rows a comparison with one found — the soft-delete
    idiom `deleted_at = '0000-00-00 00:00:00'`, or `deleted_at = 0`, since MySQL reads the
    number 0 compared with a date as the zero date (Aurora DSQL compares no date with a
    number, `42883`) — now hold NULL, the column's `DEFAULT` or `1970-01-01`, as above.
    Before cut over, change the code that writes such dates, and compare with `IS NULL` or
    that value instead. Schema Conversion reports a view that compares with one (its
    `CREATE VIEW` would fail), and the Query Converter a statement that does.

### PostgreSQL specifics

- **PostgreSQL: CDC does not carry most extension types.** The bundled CDC connector
  (Debezium) recognises a type an extension defines by its name alone, and it knows six:
  `citext`, `hstore`, `ltree`, `isbn`, and PostGIS `geometry` and `geography` (and arrays
  of them). It leaves a column of any other extension type out of every change event: for
  example pgvector's `vector`, `halfvec` and `sparsevec`, `cube`, `seg`, `lquery`, or an
  `isn` type other than `isbn`. A row CDC inserts then gets NULL in that column (or its
  `DEFAULT`) and an update leaves the column as it was, with nothing dead-lettered. If the
  column is `NOT NULL` on Aurora DSQL with no `DEFAULT`, Aurora DSQL refuses every insert
  and update CDC writes to the table instead (`23502`, dead-lettered). Full Load carries
  the column, so only the rows that arrive through CDC differ (with CDC only, the
  snapshot's rows too), and a Validation CHECKSUM shows it. Evaluation
  (`CDC_UNCARRYABLE_COLUMN`) names such columns of the common extensions, and the CDC
  prerequisite check (*Every column's type can be carried by CDC*) reads the source
  catalog for all of them, as for the built-in types CDC cannot carry
  ([§4.2](04-cdc-and-dsql-constraints.md#detecting-and-handling-source-schema-drift)).
  Decide before you deploy CDC: exclude the column
  ([Chapter 13](13-excluding-tables-and-columns.md)), give it a type CDC carries on the
  source (`text`), leave the table out and migrate it with Full load only, or accept the
  gap and reload the table with **Drop & reload**, its writes stopped, before Cut over. A
  primary-key column of such a type, or of any other type CDC cannot carry, is worse: CDC
  leaves it out of each change's key too, so it applies the table's changes by the other
  key columns alone, and a DELETE removes every target row that shares them. A key column
  cannot be excluded, so the CDC prerequisite check holds the migration for such a table
  (in **Full load + CDC**, before the Full Load): leave the table out and migrate it with
  Full load only, or give the key column a type CDC carries on the source. A table whose
  primary key you change in Schema Conversion to one that leaves the column out is not
  held: CDC keys its changes by that key, and the column is treated like any other column
  of its type (except that a source primary-key column cannot be excluded).
- **PostgreSQL 17/18 sources — four version-specific edges (none block a migration).** PG
  13–18 are supported; 17 and 18 add four things to keep in mind:
  - **`interval` `infinity` / `-infinity`** (new in PG17): Aurora DSQL's `interval` input
    rejects these values, so a row carrying an *infinite* `interval` is **quarantined**
    during Full Load (visible in the quarantine count) and surfaced by Validation as a
    count/checksum mismatch. CDC, however, carries it as a **zero** interval (`00:00:00`),
    silently, and only a CHECKSUM validation shows it. Finite intervals are unaffected,
    and so is `date`/`timestamp`/`timestamptz` `infinity`/`-infinity`: Full Load and CDC
    (sink plugin v54 and later, streamed or snapshot) both store it as `infinity`. **What
    to do:** Remodel infinite-`interval` rows (e.g. to a finite sentinel) before migrating
    the column or starting CDC. On CDC infrastructure deployed by an earlier version,
    **Delete CDC infrastructure**, deploy it again, then reload such tables (Drop &
    reload, or the re-snapshot the next Start offers) and validate with CHECKSUM, even if
    no task stopped.
  - **VIRTUAL generated columns** (the DEFAULT kind on PG18): Aurora DSQL supports
    `GENERATED ALWAYS AS (expr) **STORED**` and maintains the value itself, so a STORED
    column is **preserved** (a write must simply omit the column, which Full Load and the
    CDC sink do). A **VIRTUAL** column is created as an ordinary column and flagged
    **MANUAL/LOSS**: Full Load materializes the value the source computed (the target
    starts correct) but nothing maintains it afterward, and a VIRTUAL column cannot be
    logically replicated at all, so CDC does not carry it. **What to do:** Recompute it in
    the application before cut over. Decide before **apply**: DSQL can DROP an expression
    from a column but never ADD one.
  - **`NOT NULL … NOT VALID`** (new in PG18): The column may still hold NULLs from before
    the constraint, which the source has not checked: it is created without `NOT NULL` on
    Aurora DSQL, so Full Load loads those rows, and flagged **MANUAL/LOSS**
    (`PG_NOT_NULL_NOT_VALID`). After cut over the target accepts a NULL there, and Aurora
    DSQL cannot add `NOT NULL` to the column later. **What to do:** To keep the rule, give
    those rows a value and run `ALTER TABLE … VALIDATE CONSTRAINT` on the source before
    Schema Conversion applies the table, then re-run Evaluation.
  - **CDC connector coverage**: CDC is tested end to end on PG17 and 18 too, although the
    bundled Debezium PostgreSQL connector's own tested matrix tops out at PG16. **What to
    do:** On PG18 also confirm `idle_replication_slot_timeout` is `0` (disabled) so a
    quiet period can't auto-invalidate the CDC slot.

---

## 6.3 Deployment limits (the AWS-hosted form)

- **Single-task control plane.** The tool deploys as **one** ECS Fargate task, and you
  should not raise it above one. On the AWS-hosted deploy both **job and session state are
  kept durable in a managed S3 bucket** (`jobs/` + `sessions/` prefixes), so a task
  replacement (deploy, crash) **resumes** rather than losing progress — a resume lands on
  the last recorded status transition, not the exact in-flight batch. The single-task cap
  is about **single-writer orchestration** (coordinating CDC deploys and in-memory session
  state), **not** state durability; local SQLite on ephemeral storage is only the
  local-dev fallback.
- **No built-in app authentication.** The app relies on the ALB's **optional Cognito**
  gate. Enable Cognito for any internet-facing deployment — the deploy template blocks the
  unsafe internet-facing-without-Cognito combination.
- **Credentials are per-session and in-memory only.** They are never persisted; a restart
  means re-entering source/target connection details.

---

**Next:** [7. Performance and tuning →](07-performance-and-tuning.md)
