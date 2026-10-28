# 4. How CDC works, and how DSQL constraints are handled

_Language: **English** | [한국어](../ko/04-cdc-and-dsql-constraints.md) | [日本語](../ja/04-cdc-and-dsql-constraints.md)_

> **Prev:** [3. Full Load](03-full-load.md)
>
> **Leaving columns out of CDC?** See [13.8 The CDC capture list](13-excluding-tables-and-columns.md#138-the-cdc-capture-list).

**CDC (Change Data Capture)** is the **optional** streaming pipeline for a
near-zero-downtime cut-over. After Full Load copies the existing rows, CDC keeps DSQL
continuously up to date with every new insert/update/delete on the source — so you can cut
over with minimal downtime instead of taking a long outage.

You only need CDC for **large-scale or continuous** migrations. For a one-shot cut-over
where a short freeze is acceptable, Full Load alone is enough.

---

## 4.1 The pipeline

<p align="center">
  <img src="../../images/architecture-cdc-pipeline.png" alt="CDC pipeline: source change log (MySQL binlog / PostgreSQL WAL via a logical replication slot) → Debezium source connector → Amazon MSK (per-table topics keyed by PK + DLQ) → custom DSQL sink connector → Aurora DSQL" width="900">
</p>

The source's change log flows through three stages. Both connectors run on **managed MSK
Connect**; the tool runs **no sink compute of its own**: it is the control plane (it
builds the configs, sets the resume point and monitors).

- **The Debezium source connector (MySQL or PostgreSQL)** reads the source's change log —
  the binary log for MySQL, or the write-ahead log (WAL) through a logical replication
  slot for PostgreSQL. The source stays read-only, with two sanctioned exceptions: for a
  PostgreSQL source the tool creates (and at teardown drops) its logical slot and a
  publication scoped to exactly the migrated tables, and on a MySQL source not running
  `gtid_mode=ON` a Full load + CDC run issues one `FLUSH BINARY LOGS` right after
  recording the start point ([§3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
- **Amazon MSK (Kafka)** is the durable backbone: **one topic per table**, keyed by
  primary key (so all changes for a row stay ordered on one partition), plus a dead-letter
  (DLQ) topic, a **heartbeat** topic that keeps the committed source position advancing
  while the source is idle and, on MySQL only, a Debezium **schema-history** topic. A
  restart is gapless only while the CDC topics still retain the changes the sink had not
  applied (§4.3).
- **The custom DSQL sink connector** — a Java Kafka Connect plugin this project owns —
  applies the changes to DSQL. A stock JDBC sink cannot authenticate with DSQL's
  short-lived IAM tokens, nor handle its optimistic-concurrency conflicts
  (`SQLSTATE 40001`) or a row it rejects (§4.4). A CDC stack keeps the plugins it was
  deployed with, so while it runs an older sink plugin than this version of the tool
  bundles, the CDC step says so (*This CDC infrastructure runs an older sink plugin*);
  **Delete CDC infrastructure** and a new deploy register the bundled one.

---

## 4.2 CDC replicates *data*, not *schema* — important

CDC replicates **row-level data changes** (insert / update / delete), not SQL statements
or **DDL**: a source `ALTER TABLE`, `CREATE` or `DROP` is **not** propagated, and the
Aurora DSQL schema stays as **Schema Conversion** left it.

**If you change the source schema while CDC runs**, make the matching change on Aurora
DSQL yourself (**Fix target schema…** on the drift banner does it for an added or renamed
column), but not with Schema Conversion: while CDC streams, its Apply, **Replace**
included, is blocked (Replace drops the table and the rows already loaded with it, which
only a **Drop & reload** brings back). Adding a nullable column on Aurora DSQL before the
source is safe.

Until the target matches, rows it cannot take are **dead-lettered** to the **DLQ** and
never retried, and Cut over stays locked for each such table until it passes a CHECKSUM
after its last dead-lettered record
([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). Some changes raise no dead
letter at all: see
[Changes that dead-letter nothing](#changes-that-dead-letter-nothing-the-source-schema-re-check)
below.

### Detecting and handling source schema drift

You don't have to notice a source `ALTER` yourself: when dead-lettered records show the
source shape changed, the CDC step shows a **"Source schema change detected"** banner.
Aurora DSQL has no `ALTER COLUMN … TYPE` or `SET NOT NULL` and cannot change a primary key
in place, so each row below gives the repair it does support. The tool never drops or
rewrites target data by itself.

| Banner row | SQLSTATE | Usual cause | What to do on Aurora DSQL |
|---|---|---|---|
| column added or renamed at the source | `42703` | `ADD COLUMN` / `RENAME COLUMN` | **Fix target schema…** (below) |
| a NOT NULL column got no value ("column dropped at the source" once the re-check confirms it) | `23502`, column not sent | `DROP COLUMN`, an excluded column, or on PostgreSQL an update that left a large (TOASTed) value unchanged | `DROP COLUMN` for a dropped column, before **Drop & reload** (which refuses while it exists; a primary-key column cannot be dropped), else `ALTER COLUMN … DROP NOT NULL` |
| a NULL sent for a NOT NULL column ("NULL rule changed at the source" once confirmed) | `23502`, NULL sent | the source column now allows NULL | `ALTER COLUMN … DROP NOT NULL` (keeps the values; it cannot be made NOT NULL again) |
| column type changed at the source | `42804` (`42883` on a delete) | an incompatible type change | recreate: **Replace** + **Drop & reload** |
| a value its Aurora DSQL column cannot hold exactly | `22001`, `22003`, plus the sink's refusals | a widened source type or a narrower Schema Conversion type | the same recreate |
| a CHECK constraint rejected the row | `23514` | a value added to a MySQL `ENUM` or a PostgreSQL enum (both ported to `text` + `CHECK`) | `DROP CONSTRAINT`, then `ADD CONSTRAINT … CHECK (…) NOT VALID` with the name the reason gives, one statement per transaction |
| the change records' key no longer matches the Aurora DSQL primary key (red: every insert and update is lost) | `42P10` | the source primary key changed, or the table was re-applied with another key | restore the key, or recreate (re-run Step 1, **Replace**, **Drop & reload**; a re-keyed table also needs **Delete CDC infrastructure** and a new deploy) |

**CDC only** has no Full Load step to reload from: where a row says **Drop & reload**, fix
the table on Aurora DSQL by hand and correct the rows set aside meanwhile (for a type
change, add a column of the new type, copy the values across, drop the old column and
rename the new one; at most 3,000 rows per transaction), or use **Start over**.

**Fix target schema…** (for an added or renamed column) shows the exact statements and
applies them only when you approve (one DDL per transaction); CDC then resumes for that
table.

- A **new column** is added (nullable, no default). A **renamed column** is renamed,
  keeping its values, when it looks like a rename (for example the Step 1 inventory has
  the old name at the same position with the same type, or only the letter case differs);
  otherwise it is offered as an added column, with a warning naming the Aurora DSQL column
  the source no longer has. If the dialog guessed wrong, cancel and make the change by
  hand.
- **Not added:** a **generated** column, a type Aurora DSQL cannot hold (the dialog says
  how to add it by hand) and a column your exclusions keep out
  ([§13.8](13-excluding-tables-and-columns.md#138-the-cdc-capture-list)): its rows keep
  being dead-lettered until the capture list changes. An **ENUM** column is added as
  `text` without its `CHECK`; the warning gives the statement that adds it.
- **A renamed, removed or changed primary-key or CDC key column** is left alone: CDC
  writes by that key and Aurora DSQL cannot change a primary key in place, so the dialog
  gives the recreate steps (the last row above) instead.

**To backfill the rows already set aside:**

- **Full Load + CDC:** re-run Step 1 (Evaluation), Stop CDC, reload the table with **Drop
  & reload** (it recreates the new key, type, NULL rule and ENUM values unless you edited
  its DDL in Schema Conversion), Start CDC, then re-check it in Validation (CHECKSUM is
  automatic for it).
- **CDC only:** correct the rows on Aurora DSQL by hand (at most 3,000 rows per
  transaction) or use **Start over**: the dead-letter queue card names each record's
  primary key but shows a key value only when it is an integer or a UUID (any other is
  withheld), so use Start over for a table whose key values it withholds.

A target table that no longer exists (`42P01`), a unique-key conflict (`23505`) and a
value the sink cannot bind (`07006`) get a banner row too (*Stop CDC and reload these
tables*; with **CDC only**, *Stop CDC and repair these tables*): the same repair, once you
have confirmed the table exists on the target (re-apply it in Schema Conversion if not). A
table dropped on the target also lost every row applied before the drop, which no dead
letter lists: use Start over for it. A `23505` row names the unique index and its likely
causes (see §4.4 and §4.6); if the source dropped or relaxed that `UNIQUE` index, which
CDC does not replicate, drop it on Aurora DSQL too.

Any other dead letter gets no banner row and shows up only in the dead-letter queue card,
with its SQLSTATE where it has one:

- a value too large for Aurora DSQL ([§4.5](#45-the-1-mib-per-value-limit-and-the-dlq));
- a value the sink cannot convert, for example a PostgreSQL `timetz[]`, `bytea[]` or
  unconstrained `numeric[]` column. Evaluation names such columns in advance
  (`CDC_UNCARRYABLE_COLUMN`, with the columns that arrive NULL instead, such as
  `interval[]`, `money[]`, `tsvector` and most extensions' types like pgvector's `vector`;
  [§6.2](06-limitations.md#62-migration-process-limits));
- a change record with no key after the source primary key was dropped (MySQL or
  PostgreSQL): the source schema re-check reports it as "primary key changed at the
  source";
- any other rejection (for example `22P02`, `22007`/`22008`, `22021`, `23503` or `54000`).

After any source DDL to a table, validate that table with CHECKSUM.

**A value its column cannot hold exactly** (MySQL or PostgreSQL source) is refused, not
written: the sink checks each number against its Aurora DSQL column, so it never silently
stores a number other than 0 or 1 as `true`/`false` in a `boolean` column (a MySQL
`TINYINT(1)` can hold up to 127 with no source `ALTER` at all), or rounds one to fewer
decimal places or to a whole number. The row is dead-lettered, and the CDC step shows a
**"Rows set aside: a value does not fit its Aurora DSQL column"** banner naming the table
and column (never the value); Aurora DSQL itself rejects a string longer than a
`varchar`/`char` column (`22001`) or a number outside an integer or `numeric` column's
range (`22003`), with the same banner row. Not checked, and still rounded: a
`FLOAT`/`DOUBLE` value for a `numeric` column, and a PostgreSQL `numeric` declared without
precision (stored as `numeric(18,6)`, as Schema Conversion warns). Aurora DSQL cannot
change a column's type in place, so recreate the table with a type that holds the values
(**Replace** with, say, `smallint` for a `TINYINT(1)`, then **Drop & reload**; **CDC
only**: see the note under the banner table).

**A source `TRUNCATE`** (MySQL or PostgreSQL source) is not replicated: Aurora DSQL has no
`TRUNCATE` statement, and the event names no row to delete. The sink dead-letters it
instead (one dead letter per truncated table), the CDC step shows a **"Source TRUNCATE was
not replicated"** banner naming the table, later changes keep streaming, and the target
keeps the rows the source removed (Validation reports them as extra rows). To repair it:

- **Full Load + CDC:** Stop CDC, reload the table on the Full Load step with **Drop &
  reload** (offered only while CDC is stopped; **Append** only adds missing rows, so it
  cannot remove the extra ones), Start CDC, then re-check the table in Validation.
- **CDC only:** there is no Full Load step to reload from. Stop CDC, delete the rows the
  source no longer has from the Aurora DSQL table yourself, at most 3,000 rows per
  transaction, then Start CDC and re-check the table in Validation. For a table with a
  single-column integer key, Validation's **Reconcile every record** counts the extra rows
  and lists a sample of their keys.

A table your workload truncates routinely (a staging or queue table) raises this on every
`TRUNCATE`: leave it out of the migration if you don't need its data on Aurora DSQL.
Partition maintenance (a MySQL `TRUNCATE PARTITION` or `DROP PARTITION`; on PostgreSQL
dropping, detaching or truncating a partition, or attaching one that already holds rows)
changes rows with no change event, so CDC reports nothing and only Validation shows the
extra (or missing) rows: repair such a table the same way, or pause partition maintenance
until cut over.

**Lag while a table drifts.** After Aurora DSQL rejects one of a table's rows because the
target lacks a column, the table or its schema, the key changed, a column's type changed,
or a `NOT NULL` column got no value, the sink applies that table's later changes one row
per transaction (the other tables stay in batches but share the sink, so their stream lag
can still grow while you repair it). After the repair, the table goes back to batches as
soon as one of its inserts or updates is applied, or 60 seconds after its last rejection.
A value-level rejection (a unique or `CHECK` violation, a value too long or out of range,
a value that can't be bound) does not switch the table to one row per transaction, but
each change batch that holds such a row is still replayed one row per transaction, the
other tables' rows in it included.

### Changes that dead-letter nothing: the source schema re-check

Some source DDL changes what CDC writes without Aurora DSQL rejecting a single row, so no
dead letter and no banner reveal it:

- a **dropped column** whose target column is nullable or has a DEFAULT: new rows get NULL
  or the DEFAULT, updated rows keep the old value;
- a **changed type** the target still accepts with every value intact (a wider `VARCHAR`
  or integer type), or a column changed to `FLOAT`/`DOUBLE` on a `numeric(p,s)` target
  (for example `DECIMAL(10,2)` → `DOUBLE`), whose values are rounded with no dead letter
  because that case is not checked (any other value it would round or store as
  `true`/`false` is dead-lettered,
  [see above](#detecting-and-handling-source-schema-drift));
- a **dropped or renamed table** (or one moved to another schema): CDC captures tables by
  the names it was deployed with, so the target table stops changing (on MySQL a table
  re-created under the old name streams into it as is; on PostgreSQL, after a rename,
  changes reach the old target only until the connector restarts);
- on PostgreSQL, an **added column** of a type CDC cannot convert (for example
  `interval[]` or `money[]`), or a **generated** column: its values never arrive;
- a **NULL-rule** change, or a **primary-key** change.

So while CDC runs, the tool re-reads the definitions of the captured tables on the source
(read-only, no table data, at most every 5 minutes while the CDC monitor, Validation or
Cut over is open) and compares them with the Step 1 (Evaluation) inventory the migration
was built from: whether each table still exists, its column names (exactly, case
included), types, NULL rules and primary key. Excluded columns, generated columns CDC does
not capture, defaults, comments, collations and indexes are not compared, and a column you
added with **Fix target schema…** counts as part of the definition (one it renamed does
not: the check keeps reporting the rename, and holding Cut over, until you re-run Step 1).
A difference is shown on the CDC step ([§4.6](#the-source-schema-re-check)) and holds Cut
over for that table until the source matches the Step 1 inventory again and the table then
matches by CHECKSUM ([§5.7](05-validation.md#57-source-schema-changes-and-cut-over)).

---

## 4.3 The gapless Full Load → CDC handoff

This is the bit designed to ensure **no missed changes and no duplicates** between the
bulk load and the stream.

### How it works

1. **Full Load records a watermark** at the snapshot point: on MySQL the binlog
   file:position, the GTID set and the source's `gtid_mode`; on PostgreSQL the WAL LSN
   returned when the logical replication slot is created, together with the slot and
   publication names
   ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
2. **Start CDC resumes at exactly that watermark**, with the first change after the
   snapshot — not "now", and without re-reading any row. On MySQL the tool seeds the
   connector's start offset with the file:position and the GTID set (with `gtid_mode=ON`
   Debezium starts after exactly that GTID set) and refuses a start point it cannot use
   ([Chapter 6](06-limitations.md)). On PostgreSQL the slot created at the Full Load's
   consistency point *is* the watermark, so nothing is seeded.
3. **The sink applies every change idempotently by primary key**, so an overlap or a retry
   cannot create a duplicate.

A Manual start point seeds its binlog file:position and the GTID set saved with it; one
saved by version 0.1.565 or earlier comes back without its GTID set, so on a
`gtid_mode=ON` source use **Fetch current position** again, or re-enter the GTID set
executed at exactly that position, before Start CDC.

> **This describes a `Full load + CDC` run.** After **Full load only**, a later CDC start
> is still lossless but works differently. On **PostgreSQL** a slot cannot be created at a
> past position, so the source connector creates its own slot and **re-snapshots every
> selected table first** (the source is read a second time). On **MySQL** the binary log
> keeps history, so CDC starts at the watermark with no re-read — unless the stack has
> streamed: Start CDC then refuses the newer start point, so set the migration type to CDC
> only, then on its CDC step Delete CDC infrastructure and deploy it again before Start
> CDC (Full load only has no CDC step;
> [Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). Either way CDC
> carries only the changes after the watermark, so if that Full load only run appended to
> rows an earlier load left on the target, first Re-run Full Load choosing **Drop &
> reload**. Without `gtid_mode=ON` the MySQL watermark is a binlog position recorded
> without waiting for in-flight commits: validate in CHECKSUM mode once CDC has caught up,
> or, for an exact start point, re-run the Full Load through Full load + CDC. See
> [Chapter 10 §10.1](10-conclusion.md) for choosing between them.

### Before you start CDC

- **MySQL — keep the binlog.** The binlog at the watermark must still exist when CDC
  starts. Aurora MySQL keeps binlogs 24 h by default and deploying the CDC stack takes
  ~15–20 min, so **raise binlog retention before you start**
  ([§1.6](01-setup.md#16-prepare-the-source-for-cdc)). If it is gone, re-run Full Load
  with **Drop & reload** to capture a fresh watermark (on a stack that has streamed, after
  Delete CDC infrastructure and a new deploy).
- **PostgreSQL — keep the slot healthy.** The slot pins the WAL from the moment it is
  created, so the prerequisite is `wal_level=logical`, not binlog retention. If the slot
  falls too far behind (`wal_status='lost'`), Start CDC blocks (see *When Start CDC is
  blocked* below).
- **Let the Full Load finish.** While a Full Load runs, the CDC step shows *Start CDC
  waits for the Full Load to finish* and Start CDC is disabled. The other way round, the
  Full Load step runs nothing while CDC streams: Stop CDC first.
- **Did the Full Load append?** If it found rows already on the target (*already there*;
  for example a re-run with the default *Append*), the start-point card shows *Gapless
  only if this Full Load did not append*. An append keeps those rows as they were and CDC
  carries only the changes after the watermark, so Re-run Full Load choosing **Drop &
  reload** before Start CDC (with CDC only, switch the migration type to Full load + CDC
  for that run). The notice does not block Start, can also show after a copy that did not
  append, and goes away after a Drop & reload. The Full Load dialog warns in advance (*CDC
  from this run's new start point needs 'Drop & reload' …*).
- **Changing the table set after the Full Load?** See
  [§13.7](13-excluding-tables-and-columns.md).

### Starting again

A **restart after Stop CDC** continues from the last recorded position and replays the
recorded schema history; no row is re-read. Stop CDC removes the connectors without
draining the sink, so the changes the sink had not applied reach Aurora DSQL only from the
CDC topics: **a restart is gapless only while the topics still retain them** (their
retention period on MSK, which Start CDC does not check). The **Stop CDC** dialog and the
*Ready to resume CDC* card say so.

| Situation | What Start CDC does | What you do |
|---|---|---|
| Restart after Stop CDC, or after a Start that CloudFormation rolled back | Continues from the last recorded position (PostgreSQL: the slot's position). A source connector that never completed its snapshot snapshots every selected table again. | Nothing, while the topics still retain the unapplied changes. Once their retention period has passed: Delete CDC infrastructure and deploy it again, then Re-run Full Load with **Drop & reload**, then Start CDC (CDC only: Delete CDC infrastructure and deploy it again, then Start CDC, which snapshots every selected table). |
| A newer start point (Re-run Full Load or a newer Manual point) on a stack that has streamed | Refuses it (*This CDC stack cannot start from a newer start point*): its topics may hold changes no sink applied. | Delete CDC infrastructure and deploy it again (new, empty topics), then Re-run Full Load or set the Manual point, then Start CDC. |
| The first Start at a new start point (MySQL) | Rebuilds the schema history from the current source tables, without re-reading any row, after checking that the captured tables are unchanged since the start point ([Chapter 6](06-limitations.md)). | Nothing. |

### When Start CDC is blocked (PostgreSQL source)

Start CDC checks the publication, the replication slot and its position, and inheritance
child tables before it creates any connector. A block names its own fix first, then the
route below.

| What you see | What to do |
|---|---|
| *Replication slot invalidated*, *CDC's replication slot is not at this Full Load's start point*, a missing publication or slot, or a publication that misses a selected table | Take the route below. |
| *CDC's publication does not publish every change type* | As the publication's owner, run `ALTER PUBLICATION … SET (publish = 'insert, update, delete')` on the source, then take the route below: changes committed while the publication was narrowed are not brought back. |
| A slot of that name exists but CDC cannot use it (another database's, or not a logical `pgoutput` slot) | Drop it, or give this migration its own CDC stack name; then the route below (with CDC only, Start CDC again). |
| *CDC cannot snapshot a table that has inheritance child tables* | Re-run Full Load with **Drop & reload** (on a stack whose connectors were started before, Delete CDC infrastructure and deploy it again first; with CDC only, which has no Full Load step, switch the migration type to Full load + CDC first), or untick the parent while the table picker can still change the set. |
| *CDC's publication and replication slot could not be checked* | The tool cannot read the source (for example a Secrets Manager source after an app restart), so Start CDC is not blocked but these checks do not run. Test the source connection on the Connect step, then open Start CDC again so they run. |

**The route.** It depends on whether the stack's connectors were ever started:

- A stack whose connectors were **never started**: Re-run Full Load with **Drop &
  reload**, which creates the slot again at its own snapshot point, then Start CDC (with
  CDC only, which has no Full Load step, switch the migration type to Full load + CDC
  first); or **Re-snapshot every table instead**, which the Start CDC dialog offers only
  when a Start that re-snapshots would itself pass its checks (otherwise it says
  *Re-snapshot every table is not offered here: a Start that re-snapshots is refused too.*
  and names the route that works).
- A stack whose connectors were **started before** (stopped since, or its last Start
  rolled back): the CDC topics may still hold changes no sink applied and Debezium skips
  the snapshot on later Starts, so no re-snapshot is offered and the route is new topics
  first: Delete CDC infrastructure and deploy it again, then Re-run Full Load with **Drop
  & reload**, then Start CDC. With CDC only, which has no Full Load step, Delete CDC
  infrastructure and deploy it again, then Start CDC on the new stack, which snapshots
  every selected table (a row deleted on the source in between stays on Aurora DSQL;
  Validation reports it as an EXTRA row).

**Resume from the Full Load's slot instead**, on the start point card, undoes a recorded
re-snapshot (*Manual re-snapshots instead of resuming from this load's slot*). The slot's
name derives from the CDC stack name, so give each migration of one source its own CDC
stack name.

---

## 4.4 How the sink handles DSQL's constraints in the data path

DSQL is distributed and PostgreSQL-compatible, so the sink can't behave like a classic
MySQL/JDBC writer. Here is what it does for each constraint:

| DSQL constraint | How the custom sink handles it |
|---|---|
| **IAM-token auth (no password)** | Uses a short-lived IAM token as the password over TLS and **refreshes it before expiry**, so long-running CDC never stalls on an expired token. |
| **Optimistic concurrency (no locks)** | On `SQLSTATE 40001` it re-runs the conflicting **transaction** with exponential backoff + jitter (up to 10 attempts): one chunk of ≤3000 rows (safe because apply is idempotent), or one row while it applies row by row to isolate a rejected row. Each re-run starts on a clean transaction (the failed attempt is rolled back first), because Aurora DSQL can also report `40001` while a statement runs, not only at commit. |
| **Temporary service condition (`XX000` "server unavailable")** | Aurora DSQL documents `XX000` as temporary, so the sink handles it like a `40001`: it rolls the failed attempt back and re-runs the **transaction** with the same backoff (the two share the 10-attempt budget). If it outlasts that budget, the sink reconnects and Kafka Connect redelivers the batch until Aurora DSQL answers again, so such a row is **never dead-lettered**. While it lasts, the CDC step reports the sink as stalled ("The sink is not applying changes") and the sink connector log repeats `DSQL apply failed (transient sqlstate=XX000); Connect will retry`; the stream resumes on its own once Aurora DSQL answers. If `XX000` keeps recurring, contact AWS Support. |
| **Unique indexes besides the primary key (`23505`)** | Aurora DSQL checks a unique index at once, so a change can be rejected while another row still holds its value: an `UPDATE` of the primary key (Debezium replicates it as a delete of the old key and an insert of the new key, which can travel on different partitions, so the insert can arrive first) or a unique value passed from one row to another. The sink (plugin v53 or later) holds such a change, and every later change on its partition, until the change that frees the value has applied: it retries with a growing wait (500 ms up to 10 s) while every other partition keeps applying, for at most 5 minutes (longer if an Aurora DSQL outage interrupts the wait; [§6.2](06-limitations.md#62-migration-process-limits) has the details). The 5 minutes apply to each held change. The wait shows as Stream lag on its table (§4.6). On a table that is not re-keyed, if a later change of the same row replaces it entirely (a plain delete, or an image of the same columns), the conflicting change is skipped instead (never dead-lettered). On a re-keyed (Composite key) table nothing is skipped. A skipped change leaves its row's values on the target as they were until its replacement applies, so a change of another row that needs a value the skipped change would have freed can still be dead-lettered after its 5 minutes ([§6.2](06-limitations.md#62-migration-process-limits) lists the cases). Past the 5 minutes it is dead-lettered with SQLSTATE `23505` and the index name (*a unique constraint rejected the row*), and, like every dead-lettered table, Cut over holds the table until it matches by CHECKSUM ([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). |
| **≤ 3000 rows per transaction** | Applies in chunks of ≤ 3000 rows and at most an 8 MiB byte budget, one `commit()` per chunk. Every text, JSON or binary value counts at its size, so a batch of large JSON documents is split well under Aurora DSQL's 10 MiB limits on a transaction and on one message. |
| **No UPDATE-by-statement / replays** | Every change is an **idempotent upsert or delete keyed by PK** (`INSERT ... ON CONFLICT (pk) DO UPDATE`, `DELETE ... WHERE pk = ?`, and Kafka tombstones). Re-applying the same event is safe. |
| **Connections drop (idle close / token expiry / worker recycle)** | Detects a dead/half-open connection, reconnects with a fresh token, and **replays the same offsets** (safe because applies are idempotent) rather than dropping records. Connectivity errors are treated as transient and retried, not mistaken for poison rows. So is a connection Aurora DSQL refuses to open — its IAM token (`28xxx`), too many open connections (`53300`) or the connection rate (`53400`): the batch is retried with a newly signed token and nothing is dead-lettered. |
| **Foreign keys (applied at cut over, after the drain)** | No foreign key exists on the target while the sink streams: it applies changes across tables in no parent-before-child order, so an enforced key would reject a child row that arrives first (`23503`, dead-lettered). The keys are added at cut over, after the final drain: `NOT VALID` at once (seconds; every new write is enforced from then on), and Aurora DSQL then validates the rows already loaded in the background ([Chapter 2](02-evaluation-and-schema-conversion.md)). A source `ON DELETE` / `ON UPDATE` action never reaches the stream (InnoDB applies it inside the engine), so the child rows it changed can fail that validation: for a MySQL source **I've cut over** waits until such a key has validated, or until you choose to cut over without that check. |

A CDC stack keeps the plugin it was deployed with: where an older sink plugin behaves
differently from this table, the CDC step says so (*This CDC infrastructure runs an older
sink plugin*) until you **Delete CDC infrastructure** and deploy it again.

---

## 4.5 The 1 MiB per-value limit, and the DLQ

DSQL rejects a **single value larger than ~1 MiB** (a `TEXT`/`bytea` value). The pipeline
handles oversized values in **three bands**:

| Value size | What happens |
|---|---|
| **≤ 1 MiB** | Applied normally. |
| **1 MiB – 8 MiB** | The sink measures each value **before writing** and **quarantines** the oversized one to the **DLQ** (it can never be applied), while the rest of the record's table keeps flowing. |
| **> 8 MiB** | Cannot enter Kafka at all. These must be **excluded at capture**, so the oversized LOB column never reaches the pipeline. You exclude it with a rule in **Exclude tables & columns** (under the table list on the Data Migration step); its *Oversized-LOB candidates* section lists the same columns Evaluation flags `OVERSIZED_LOB`. Rows the tool writes hold NULL in an excluded column; rows already on Aurora DSQL before CDC started keep their values (CDC never writes an excluded column), and Validation counts them. |

### What gets dead-lettered

Beyond oversized values, the DLQ isolates any record DSQL **permanently** rejects — a type
mismatch, a constraint violation, a missing target column (e.g. after an un-propagated
source `ALTER`) — and any value the sink refuses because its Aurora DSQL column would
change it ([§4.2](#detecting-and-handling-source-schema-drift)). Transient failures (OCC
`40001`, Aurora DSQL's temporary `XX000` "server unavailable", connection drops) are
**retried**, not dead-lettered. A rejection a source schema change explains gets a banner
row with its repair (§4.2); a per-value data error with no schema meaning (for example
`22P02`) is listed with its SQLSTATE only. A value-level rejection (a unique or `CHECK`
violation, a value too long or out of range) makes the rows near it apply one per
transaction, and a unique violation (`23505`) of an insert or update is first held up to 5
minutes for the change that frees the value (§4.4) and dead-lettered only if it outlasts
that.

### Where the DLQ surfaces — CloudWatch, not a Kafka topic you read

The sink logs each quarantined record to its **CloudWatch** connector log group, and the
tool's monitoring parses those lines into the UI (per-table "Quarantined" counts and a
single downloadable error log). The logged reason includes the **SQL template** (column
names + `?` placeholders) — **never row values or credentials**. A record that can be
neither applied nor dead-lettered makes the task **fail loudly** rather than silently
skip.

### What CDC does with later changes to a row that Full Load quarantined

If Full Load quarantined a row (its oversized value could not be written), the row is
**absent from the target** while still present on the source. What happens when that same
row changes later depends on the operation:

| Source operation on the missing row | What CDC does | Result |
|---|---|---|
| `DELETE` | `DELETE … WHERE pk = ?` matches **0 rows**; the sink does not treat that as an error, so it is applied and committed silently. | **Correct.** The intended end state — "the row is not on the target" — already holds. |
| `UPDATE` that shrinks the value below 1 MiB | Debezium sends the **full after-image** and the sink writes it as an upsert; with no existing row, the row is **inserted**. | **The gap heals itself.** No action needed. |
| `UPDATE` where the value is still oversized | The sink measures values **before** writing, so the record is quarantined to the **DLQ** for the same reason. | Gap persists, and it is **visible** (DLQ depth / "Quarantined" in the monitor). |
| `INSERT` of a *different* row | Unaffected. | Normal. |

A quarantine gap is not self-announcing after Full Load: if the source never touches that
row again, CDC has nothing to replicate, and a 0-row delete is indistinguishable from a
normal replay. **Validation (Step 4)** is what reports the gap, which is why the Full Load
completeness verdict points you there. The practical guidance is unchanged: fix the
oversized source value and **Reload** that table with CDC stopped (Stop CDC, Reload, then
Start CDC again: the Full Load step runs no reload while CDC streams,
[Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)) before cutting
over, or exclude the oversized LOB column at capture (§4.5) so it never enters the
pipeline.

---

## 4.6 Monitoring CDC progress (the migration monitor)

While CDC runs, the Data Migration screen shows a live per-table monitor. It refreshes on
its own (no manual reload) and is **scan-free** on the source — it never runs a `COUNT(*)`
against your production database. The columns:

| Column | What it means |
|---|---|
| **Full Load rows** | Rows the one-shot snapshot loaded into the table. |
| **Inserts** / **Updates** / **Deletes** | Cumulative CDC inserts, updates and deletes applied to this table since Full Load. |
| **Quarantined** | Change events set aside to the **DLQ** (permanently rejected rows: bad type, oversized > 1 MiB value, constraint / schema drift). |
| **Source rows (est.)** | Scan-free catalog **estimate**. **Target rows** — exact DSQL count. |
| **Stream lag** | How far the target is behind the source **in time** (see below). |
| **Consistency** | A colored badge: green *consistent* = counts match · *replicating…* = catching up · red *rows missing* = the newest change landed but rows went missing mid-stream · red *data quarantined* = the DLQ has un-applied events. |

### Stream lag — a real time measure, not a row count

**Stream lag** is the end-to-end replication delay: for each change, the sink records
**apply time − the source commit time**, and the monitor shows the **worst (maximum) lag**
over the recent window (not the latest single value) as a duration: `caught up`
(sub-second / the stream has drained), `8.5s behind`, `2m 10s behind`, `1h 4m behind`.
Being a true **time** lag, it is accurate for **any** primary-key type and reflects
update/delete lag too, not just the newest insert. A change the sink **holds on a unique
index** (§4.4) counts as lag on its table while it waits, and the lag drops once it
applies or is dead-lettered; there is no per-partition lag. When the time metric isn't
available yet (an older sink plugin, or no datapoint yet), the monitor shows
`N behind (PK)` or `caught up`, which works only for a single-column integer PK.

Lag is emitted strictly **best-effort** and never affects replication. Use "Stream lag →
caught up", at least 5 minutes after the source's last write (a held change's lag can take
a minute or two to show), as the signal that it's safe to proceed to
[cut-over](10-conclusion.md).

### Pipeline health and change flow

Above the per-table table, a **pipeline-health card** shows each connector's state and a
**"change flow"** status line — *Streaming*, *No changes flowing — idle*, or **"Sink
stalled — changes are NOT reaching DSQL"** — with two rec/s gauges (source poll vs sink
send), so you can tell an idle stream from a stuck one at a glance. A **live stream-lag
chart** plots the worst end-to-end lag over time.

A confirmed stall's notice points to the sink connector log. Do not cut over until the
sink send rate recovers. What a repeating line means:

| Log line | Means | What to do |
|---|---|---|
| `Commit of offsets timed out` | the sink consumer was ejected from its group | Stop CDC, then Start CDC to rejoin it |
| `DSQL apply failed (transient sqlstate=XX000)` | a temporary Aurora DSQL condition | The sink resumes on its own; contact AWS Support if it lasts |
| `DSQL apply failed (transient sqlstate=…)` with `08xxx`, `57xxx` or `null` | the sink cannot reach the cluster | Check that the cluster still exists, that the connector subnets reach its endpoint on port 5432 (security group, NAT gateway or Aurora DSQL VPC endpoint and its DNS), and that the connector's IAM role may still connect |
| the same, with `28xxx`, `53300` or `53400` | Aurora DSQL refused to open the connection | `28xxx` is its IAM token (check that the connector's IAM role may still connect), `53300` the cluster's limit of open connections (close idle ones elsewhere or raise the quota), `53400` its rate of new connections; the sink resumes once one opens |
| the same, with `40001` | something else is writing the same rows on Aurora DSQL | Stop it until cut over |

When a connector's task stops, its row turns red. If the connector's CloudWatch worker log
shows a failure the tool recognizes (a MySQL DDL statement Debezium cannot parse, a
schema-history problem, a PostgreSQL publication it cannot create, an unreachable source,
…), the row reads *Stopped —* followed by the cause, and an error notice under the
connectors says what to do. Otherwise the row reads *Stopped — a task is not running.
Restart required.* and the log group `/msk-connect/<stack>-cdc` has the details. The
lifecycle card then says CDC is not streaming and offers **Stop CDC**: Retry CDC leaves
connectors that exist as they are, so it cannot restart a stopped one. After Stop CDC, the
cause stays on the Start CDC card until the next Start.

### Inspecting the DLQ

The **DLQ inspector** lists dead-lettered records — **Time / Table / SQLSTATE / Reason** —
paged and filterable, with a per-table breakdown and a depth badge, so you can triage
poison rows without leaving the UI. **"Download CDC error log"** exports them as NDJSON.
(The reason carries the SQL template, never row values — see §4.5.) The record list lives
in memory and is not kept across an app restart; the per-table count, latest time and
drift kinds are, and they are what Cut over checks
([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)).

### Re-keyed PostgreSQL tables: the REPLICA IDENTITY re-check

A table CDC re-keys (a composite target key that adds a column outside the source primary
key) needs its source `REPLICA IDENTITY` to be **FULL** on the table and every partition.
Below FULL, a `DELETE` does not carry the added column and can leave its row on Aurora
DSQL with no error and no dead letter, and an `UPDATE` that changes only the added column
is dead-lettered (*a unique constraint rejected the row*) and leaves the old row. So the
monitor re-reads `REPLICA IDENTITY` (read-only, at most every 2 minutes, never on a MySQL
source) and shows these notices:

| Notice | What it means | What to do |
|---|---|---|
| **Red — "Deletes on re-keyed tables can be lost"** | A table (or one of its partitions) is below FULL. Cut over is held, and deletes made before the fix are not replayed. | Run `ALTER TABLE <table> REPLICA IDENTITY FULL` on the source — on each partition too, since the parent does not propagate and a new partition starts at `DEFAULT` — then choose **Check again**. On PostgreSQL 18, for a table with a generated column, the notice names **Keep source PK** instead of FULL (see [Chapter 6](06-limitations.md)). |
| **Amber — "REPLICA IDENTITY is FULL again"** | The fix is in, but deletes made while it was below FULL may be missing. Validation compares the table by CHECKSUM automatically, and Cut over waits for a match started after the fix ([§5.6](05-validation.md#56-re-keyed-postgresql-tables-and-cut-over)), unless you chose **Continue without this check** there. | If the table differs: let CDC catch up, Stop CDC, reload it on the Full Load step with **Drop & reload**, Start CDC, and re-check it. CDC only has no Full Load step: delete the extra rows on Aurora DSQL by hand instead. The same notice, headed "REPLICA IDENTITY was below FULL on re-keyed tables", appears when CDC stops re-keying a table that was below FULL (the table was set back to **Keep source PK** in Schema Conversion and CDC redeployed, or the source primary key now covers the composite key): deletes lost before then still need the CHECKSUM. |
| **Info — "Can't re-check REPLICA IDENTITY on the source"** | The source password is kept in memory only, so after an app restart the tool cannot connect. | Reconnect on the Connect step; keep these tables at FULL meanwhile. |
| **Amber — "Can't re-check REPLICA IDENTITY on the source"** | The last read failed; the tool tries again in 2 minutes. | Keep these tables at FULL meanwhile. |

### The source schema re-check

The monitor re-reads the captured tables' definitions on the source (see
[§4.2](#changes-that-dead-letter-nothing-the-source-schema-re-check)) and shows these
notices, beside the dead-letter queue card rather than in it:

| Notice | What it means | What to do |
|---|---|---|
| **Red — "Source schema changed on N captured table(s)"** | One line per table says what changed (a column dropped, added or renamed; a type, NULL rule or primary key changed; a table not found) and what that does to the rows CDC writes. Cut over is held. A change the dead-letter banner already reports the same way (with the rows it rejected) is listed only in that banner; Cut over is held either way. | Realign the table once the source has its final definition (below), then choose **Check again**. |
| **Amber — "The source schema changed while CDC ran"** | The source matches the Step 1 inventory again (or a Step 1 re-run changed a table's definition, or a table left the inventory), but values CDC wrote meanwhile may differ. | Validation compares these tables by CHECKSUM automatically; Cut over waits for a match started after that time ([§5.7](05-validation.md#57-source-schema-changes-and-cut-over)). A table no longer in the Step 1 inventory cannot be compared: acknowledge it on the Cut over step with **Continue without this check**. |
| **Info — "Can't re-check the source schema"** | The source password is kept in memory only, so after an app restart the tool cannot connect. | Reconnect on the Connect step. |
| **Amber — "Can't re-check the source schema"** | The last read failed, or found none of the captured tables; the tool tries again in 5 minutes. | Avoid DDL on migrated tables meanwhile. |

To realign a table once the source has its final definition:

| What changed | How to realign |
|---|---|
| A column added or renamed | **Fix target schema…** (offered unless the dead-letter banner already offers it for that table): it adds a new column, or renames a renamed one so it keeps its values; re-run Step 1 afterwards so Validation compares the column. It changes nothing for a key change or a renamed primary-key or CDC key column, and it adds neither a generated column nor one your exclusions keep out (the dialog says why and what to do). |
| A column dropped, or a type or NULL rule changed | Undo it on the source, or: **Full Load + CDC** — re-run Step 1 (the check then compares with it); where Aurora DSQL must change too, let CDC catch up (Stream lag: caught up), Stop CDC, drop a removed column on Aurora DSQL first (Drop & reload does not destroy a target column the new definition no longer has), reload the table with **Drop & reload**, then Start CDC. A changed column type needs the new type in Schema Conversion first (Aurora DSQL cannot change a column's type in place). Run no other DDL on migrated tables while CDC is stopped. **CDC only** — change the Aurora DSQL table by hand where it must follow the source, correct the rows CDC wrote since the change (they are not replayed), then re-run Step 1. |
| A primary key | Restore the key on the source, or (Full Load + CDC) re-run Step 1 and reload the table with **Drop & reload**, which recreates it with the new key. If a key column was renamed or removed, first re-apply the table in Schema Conversion with **Replace** (Drop & reload refuses to destroy the old key column, and Aurora DSQL cannot drop it). CDC only: **Start over**. For a renamed CDC key column (Composite key) the notice offers no Fix and gives the recreate steps itself. |
| A table renamed or dropped | CDC captures a fixed table list, so it cannot be realigned in place: rename it back on the source, or use **Start over** to migrate it under its new name. If you dropped it on purpose, re-run Step 1: it then leaves the check, and Cut over asks you to acknowledge it. |

---

**Next:** [5. Validation →](05-validation.md)
