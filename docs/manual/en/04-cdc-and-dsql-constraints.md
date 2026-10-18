# 4. How CDC works, and how DSQL constraints are handled

_Language: **English** | [한국어](../ko/04-cdc-and-dsql-constraints.md) | [日本語](../ja/04-cdc-and-dsql-constraints.md)_

> **Prev:** [3. Full Load](03-full-load.md)
>
> **Leaving columns out of CDC?** See [13.8 The CDC capture list](13-excluding-tables-and-columns.md#138-the-cdc-capture-list).

**CDC (Change Data Capture)** is the **optional** streaming pipeline for a
near-zero-downtime cut-over. After Full Load copies the existing rows, CDC keeps
DSQL continuously up to date with every new insert/update/delete on the source —
so you can cut over with minimal downtime instead of taking a long outage.

You only need CDC for **large-scale or continuous** migrations. For a one-shot
cut-over where a short freeze is acceptable, Full Load alone is enough.

---

## 4.1 The pipeline

<p align="center">
  <img src="../../images/architecture-cdc-pipeline.png" alt="CDC pipeline: source change log (MySQL binlog / PostgreSQL WAL via a logical replication slot) → Debezium source connector → Amazon MSK (per-table topics keyed by PK + DLQ) → custom DSQL sink connector → Aurora DSQL" width="900">
</p>

- **The Debezium source connector (MySQL or PostgreSQL)** reads the source's
  change log — the binary log for MySQL, or the write-ahead log (WAL) through a
  logical replication slot for PostgreSQL — and emits change events. The source
  stays read-only, with the one sanctioned exception that for a PostgreSQL source
  the tool creates (and at teardown drops) its logical slot and a publication
  scoped to exactly the migrated tables.
- **Amazon MSK (Kafka)** is the durable backbone: **one topic per table**, keyed
  by primary key (so all changes for a row stay ordered on one partition), plus a
  dead-letter (DLQ) topic, a Debezium **schema-history** topic (built by `recovery`
  at a new start point and replayed on a restart — **MySQL source only**; the
  PostgreSQL/pgoutput connector has no schema-history topic), and a **heartbeat**
  topic that keeps the committed source position advancing during idle windows — the
  binlog offset for MySQL, and for PostgreSQL the slot's confirmed LSN (which also
  stops WAL accumulating) — so a restart's source position is not left behind by an idle
  window (the restart is gapless only while the CDC topics still retain the changes the
  sink had not applied: §4.3).
- **The custom DSQL sink connector** — a Java Kafka Connect plugin this project
  owns — applies the changes to DSQL. Both connectors run on **managed MSK
  Connect**; the tool runs **no sink compute of its own**, it is the control
  plane (it builds the configs, establishes the resume point — for MySQL it seeds
  the Kafka start offset, for PostgreSQL it creates the logical replication slot
  and publication — and monitors).

**Why a *custom* sink and not a stock JDBC sink?**

A stock JDBC sink cannot authenticate with DSQL's short-lived IAM tokens and has no
DSQL-specific handling of optimistic-concurrency conflicts (`SQLSTATE 40001`) or of
a row DSQL rejects. The custom sink re-runs a conflicting **transaction** itself
(one chunk of ≤3000 rows, or one row) with exponential backoff and full jitter,
sends a rejected row to the DLQ while the rest of its chunk still lands, and handles
DSQL's short-lived IAM tokens and reconnects (details in §4.4).

---

## 4.2 CDC replicates *data*, not *schema* — important

CDC replicates **row-level data changes** (insert / update / delete). It does
**not** replicate SQL statements or **DDL**. Concretely:

- The sink applies only row changes (DML). On a MySQL source the connector also
  runs with `include.schema.changes=false`, so no schema-change events reach Kafka.
  On a PostgreSQL source the `pgoutput` stream carries no DDL at all; Debezium
  learns a table's new shape from the stream's Relation message that precedes the
  next change to it.
- Source **DDL** (`ALTER TABLE`, `CREATE`, `DROP`, …) is **not** propagated to
  DSQL.

The DSQL target schema is fixed when the **Schema Conversion** step runs. **If you
change the source schema while CDC is running**, make the matching change on
Aurora DSQL yourself, but not with Schema Conversion while CDC streams: its Apply,
**Replace** included, is blocked then. Replace drops and recreates the table, so
the rows already loaded would be gone and later changes would land in an empty
table until you reload it with **Drop & reload**. What to run depends on the
change (see the table below):

- for an added or renamed column, use **Fix target schema…** on the drift banner,
  or run the same `ALTER` by hand on Aurora DSQL;
- for a source column that now allows NULL, or one you will drop that is
  `NOT NULL` on Aurora DSQL, run `ALTER TABLE … ALTER COLUMN … DROP NOT NULL` on
  Aurora DSQL (before the source drop);
- for a type or key change, recreate the table with CDC stopped: **Replace** in
  Schema Conversion, then **Drop & reload**, as the table below describes (with CDC
  only, follow its CDC-only repair).

Only additive changes are safe to make on Aurora DSQL before the source: the sink
names its columns, so it ignores an extra nullable target column. Until the target
matches, rows that cannot be written in the target's shape (e.g. referencing a new
column) are dead-lettered to the **DLQ** and never retried, and Cut over stays locked until each such table passes
a CHECKSUM after its last dead-lettered record
([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). But some changes are
applied **silently**, with no dead letter at all — see
[Changes that dead-letter nothing](#changes-that-dead-letter-nothing-the-source-schema-re-check)
below.

### Detecting and handling source schema drift

You don't have to notice a source `ALTER` yourself. When dead-lettered records
indicate the source shape changed, the tool **classifies the drift** by SQLSTATE and the
record's shape (table below) and shows a **"Source schema change detected"** banner on the
CDC step. For an added or renamed column it offers
**"Fix target schema…"**: it reads the table's live definition on both sides, shows the
exact statements, and applies them only when you approve them (one DDL per transaction);
CDC then resumes for that table.

- A **new column** is added (`ALTER TABLE … ADD COLUMN`, nullable, no default).
- A **renamed column** is renamed (`ALTER TABLE … RENAME COLUMN`) and keeps its values
  when it looks like a rename: the Step 1 inventory has the old name at the same
  position with the same type, the source schema re-check saw the rename, or the names
  differ only in letter case. The catalog cannot tell a rename from a dropped column plus
  an added one of the same type at the same position (an added column goes at the end
  of the table), so if the source did that, cancel and add the column by hand: the
  dialog shows the statement. Otherwise it is offered as an added column, with a
  warning naming the Aurora DSQL column the source does not have; if that is the old
  name, cancel and rename it by hand.
- **Nothing changes for a table whose primary-key or CDC key column was renamed, removed
  or changed.** CDC writes and deletes those rows by that key, so an added column would
  not repair it: writes would keep failing and deletes would silently match no row.
  Aurora DSQL cannot change a primary key in place or drop a primary-key column, so the
  dialog gives the recreate steps instead: Stop CDC, re-run Step 1, re-apply the table in
  Schema Conversion with **Replace**, reload it with **Drop & reload**, Start CDC (CDC
  only: restore the key on the source or use **Start over**). A table CDC re-keys
  (Composite key) also needs **Delete CDC infrastructure** and a new deploy.
- **A column your exclusions keep out is not added**: a new column a saved exclusion
  rule matches, an excluded column the source renamed, or any new column while an
  excluded column is gone from the source. CDC still sends it and its rows keep being
  dead-lettered until the capture list changes, even when the dialog adds or renames
  other columns of that table; see
  [§13.8](13-excluding-tables-and-columns.md#138-the-cdc-capture-list).
- A **generated** column, or one of a type Aurora DSQL cannot hold, is not added; the
  dialog says how to add it by hand.
- An **ENUM** column is added as `text` without its `CHECK` (Aurora DSQL's `ADD COLUMN`
  takes no constraint); the warning gives the `ADD CONSTRAINT … CHECK (…) NOT VALID`
  statement that adds it.

Every rejection in the table below, and a target table that no longer exists (`42P01`),
a unique-key conflict (`23505`; its row names the unique index and the causes it can
have — a primary-key change or a unique value moved between rows, see §4.4, a
composite-key table's leading-column update, see §4.6, and a source that dropped or
relaxed that `UNIQUE` index, which CDC does not replicate: drop it on Aurora DSQL too)
or a value the sink cannot bind (`07006`), gets a banner row the same way, for you to
resolve by hand (the tool never drops or rewrites target data automatically); a source
`TRUNCATE` gets its own (below). Any other dead letter gets no banner row of its own and
shows up only in the dead-letter queue card:

- a rejection with any other SQLSTATE (for example `22P02`, `22007`/`22008`, `22021`,
  `23503` or `54000`), listed with that SQLSTATE;
- a value too large for Aurora DSQL
  ([§4.5](#45-the-1-mib-per-value-limit-and-the-dlq));
- a value the sink cannot convert, for example a PostgreSQL `timetz[]`, `bytea[]` or
  unconstrained `numeric[]` column (Evaluation names such columns in advance,
  `CDC_UNCARRYABLE_COLUMN`, with the columns that arrive NULL instead: `interval[]`,
  `money[]`, `tsvector`, `tsquery`, and the geometric types `box`, `circle`, `line`, `lseg`,
  `path`, `polygon`); a scalar `point` is written as its own text `(x,y)` since sink plugin
  v54;
- a change record with no key after the source primary key was dropped (MySQL or
  PostgreSQL). The record itself is not classified, but the source schema re-check
  reports the dropped key as "primary key changed at the source".

The source schema re-check below reports a change that Aurora DSQL accepts silently,
but only within its limits ([§6.2](06-limitations.md#62-migration-process-limits)).
After any source DDL to a table, validate that table with CHECKSUM.

Aurora DSQL has no `ALTER COLUMN … TYPE` or `SET NOT NULL` and cannot change a primary
key, so each banner row gives the repair it does support:

| Banner row | SQLSTATE | Usual cause | Repair on Aurora DSQL |
|---|---|---|---|
| column added or renamed at the source | `42703` | `ADD COLUMN` / `RENAME COLUMN` | **Fix target schema…** |
| a NOT NULL column got no value ("column dropped at the source" once the re-check confirms it) | `23502`, column not sent | `DROP COLUMN`, an excluded column, or on PostgreSQL an update that left a large (TOASTed) value unchanged | `DROP COLUMN` for a dropped column, before **Drop & reload** (which refuses while it exists; a primary-key column cannot be dropped), else `ALTER COLUMN … DROP NOT NULL` |
| a NULL sent for a NOT NULL column ("NULL rule changed at the source" once confirmed) | `23502`, NULL sent | the source column now allows NULL | `ALTER COLUMN … DROP NOT NULL` (keeps the values; it cannot be made NOT NULL again) |
| column type changed at the source | `42804` (`42883` on a delete) | an incompatible type change | recreate: **Replace** + **Drop & reload** (CDC only: add, copy, drop and rename a column by hand, or **Start over**) |
| a value its Aurora DSQL column cannot hold exactly | `22001`, `22003`, plus the sink's refusals | a widened source type or a narrower Schema Conversion type | the same recreate |
| a CHECK constraint rejected the row | `23514` | a value added to a MySQL `ENUM` or a PostgreSQL enum (both ported to `text` + `CHECK`) | `DROP CONSTRAINT`, then `ADD CONSTRAINT … CHECK (…) NOT VALID` with the name the reason gives, one statement per transaction |
| the change records' key no longer matches the Aurora DSQL primary key (red: every insert and update is lost) | `42P10` | the source primary key changed, or the table was re-applied with another key | restore the key, or recreate (re-run Step 1, **Replace**, **Drop & reload**; a re-keyed table also needs **Delete CDC infrastructure** and a new deploy); CDC only: restore the key (then correct the rows set aside meanwhile by hand) or **Start over** |

To backfill the rows already set aside: with **Full Load + CDC**, re-run Step 1
(Evaluation), Stop CDC, reload the table with **Drop & reload** (it recreates the new
key, type, NULL rule and ENUM values unless you edited its DDL in Schema Conversion),
Start CDC, then re-check it in Validation (CHECKSUM is automatic for it); with **CDC
only**, correct them on Aurora DSQL by hand or use **Start over**: the dead-letter queue
card names each record's primary key but shows a key value only when it is an integer or a
UUID (any other is withheld), so use Start over for a table whose key values it withholds.
Other data errors (`22P02`, `22007`/`22008`,
`22021`, …) are listed in the dead-letter queue card with their SQLSTATE only; like
every dead-lettered table, Cut over holds the table until it matches by CHECKSUM.
For a target table that no longer exists, a unique-key conflict or a value the sink
cannot bind, the banner's notice reads *Stop CDC and reload these tables*; with **CDC
only** it reads *Stop CDC and repair these tables*: Stop CDC, confirm the table exists
on the target (re-apply it in Schema Conversion if it does not), correct the
dead-lettered rows on Aurora DSQL by hand (at most 3,000 rows per transaction), then
Start CDC and run Validation, or use **Start over**, as for a table whose key values the
card withholds. A table dropped on the target also
lost every row applied before the drop, which no dead letter lists: use Start over for
it.

**A value its column cannot hold exactly** (MySQL or PostgreSQL source) is refused, not
written. Before writing, the sink checks each number against its Aurora DSQL column,
whose type it reads once per table (and again every 10 minutes). Without this check,
three kinds of value would change with no error:

- a number other than 0 or 1 for a `boolean` column, which would be stored as `true` or
  `false`. A MySQL `TINYINT(1)` can hold up to 127 with no source `ALTER` at all; a
  `TINYINT(1)` widened to `INT`, or a PostgreSQL `boolean` changed to an integer type,
  does the same;
- a number with more decimal places than a `numeric(p,s)` column keeps (for example
  after `DECIMAL(10,2)` → `DECIMAL(12,4)`), which would be rounded;
- a fractional number for an integer column (for example after `INT` →
  `DECIMAL(10,2)` or `DOUBLE`), which would be rounded to a whole number.

The row is dead-lettered instead, and the CDC step shows a **"Rows set aside: a value
does not fit its Aurora DSQL column"** banner naming the table and column. The
dead-letter reason names the column and its type, never the value. A value that fits is
written unchanged (`1.2300` into `numeric(10,2)` stores `1.23`). Aurora DSQL itself
rejects a string longer than a `varchar`/`char` column (`22001`) or a number outside an
integer or `numeric` column's range (`22003`); those rows get the same banner row and the
same repair. A PostgreSQL `numeric`
declared without precision is stored at Aurora DSQL's default `numeric(18,6)`, as Schema
Conversion warns, and is still rounded to 6 decimal places. A `FLOAT`/`DOUBLE` value for
a `numeric` column is not checked and is still rounded. Aurora DSQL cannot change a
column's type in place, so the repair is to recreate the table:

- **Full Load + CDC:** let CDC catch up, Stop CDC, re-run Step 1 if the source type
  changed, re-apply the table in Schema Conversion with **Replace** and a type that holds
  the values (edit a `TINYINT(1)` to `smallint`), reload it with **Drop & reload**, Start
  CDC, then re-check it in Validation.
- **CDC only:** Stop CDC and give the column such a type by hand (add a column of that
  type, copy the values across at most 3,000 rows per transaction, drop the old column
  and rename the new one), correct the set-aside rows, then Start CDC; or use **Start
  over**.

Cut over is held for the table until it matches by CHECKSUM
([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). CDC infrastructure deployed
by an earlier version of the tool does not have this check: until you **Delete CDC
infrastructure** and deploy it again, it stores such values as `true`/`false`, or
rounded.

**A source `TRUNCATE`** (MySQL or PostgreSQL source) is not a schema change, and CDC
cannot apply it either. Aurora DSQL has no `TRUNCATE` statement, and deleting every
target row because of one event that names no row would be unbounded and could also
remove rows inserted after it, so the tool never does that. Instead the sink
dead-letters the event: one dead letter per truncated table. The CDC step shows a
**"Source TRUNCATE was not replicated"** banner naming the table, later changes to the
table keep streaming, the target keeps the rows the source removed (Validation reports
them as extra rows), and Cut over is held for that table until it matches by CHECKSUM
([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). To repair it:

- **Full Load + CDC:** Stop CDC, reload the table on the Full Load step with **Drop &
  reload** (offered only while CDC is stopped; **Append** only adds missing rows, so it
  cannot remove the extra ones), Start CDC, then re-check the table in Validation.
- **CDC only:** there is no Full Load step to reload from. Stop CDC, delete the rows the
  source no longer has from the Aurora DSQL table yourself, at most 3,000 rows per
  transaction, then Start CDC and re-check the table in Validation. For a table with a
  single-column integer key, Validation's **Reconcile every record** counts the extra
  rows and lists a sample of their keys.

If the `TRUNCATE` ran after the Full Load started but before it copied that table, the
target may already match; the CHECKSUM re-check then clears the hold with no reload. A
table your workload truncates routinely (a staging or queue table) raises this on every
`TRUNCATE`; leave it out of the migration if you don't need its data on Aurora DSQL. Not
covered: on MySQL, `ALTER TABLE … TRUNCATE PARTITION` and `ALTER TABLE … DROP PARTITION`
— and on PostgreSQL, dropping, detaching or truncating a partition (a `TRUNCATE` on a
partition is not replicated at all) — remove rows without any change event, and attaching a
partition that already holds rows adds them the same way, so CDC reports nothing and only
Validation shows the extra (or missing) rows; repair such a table the same way, or pause
partition maintenance until cut over. A MySQL pipeline that was already
streaming when you upgraded from an earlier version of the tool starts reporting
`TRUNCATE` after one Stop CDC → Start CDC.

**Lag while a table drifts.** After Aurora DSQL rejects one of a table's rows because
the target lacks a column, the table or its schema, the key changed, a column's type
changed, or a `NOT NULL` column got no value, the sink applies that table's later changes
one row per transaction and keeps applying every other table in batches. The drift then
no longer pushes every table's changes through one-row transactions, but the other tables
still share the sink with the drifted one, so their stream lag can still grow while you
repair it, and grows more the busier the drifted table is. Every row of the drifted table
is still tried on Aurora DSQL, and a row is dead-lettered only if Aurora DSQL rejects it.
After the repair (**Fix target schema…** or your own fix), the table goes back to batches
as soon as one of its inserts or updates is applied, or 60 seconds after its last
rejection. A value-level rejection (a unique or `CHECK` violation, a value too long or out
of range, a value that can't be bound) does not switch the table to one row per
transaction: each change batch that holds such a row is still replayed one row per
transaction, the other tables' rows in it included.

### Changes that dead-letter nothing: the source schema re-check

Some source DDL changes what CDC writes without Aurora DSQL rejecting a single row, so
no dead letter and no banner reveal it:

- a **dropped column** whose target column is nullable or has a DEFAULT: new rows get
  NULL or the DEFAULT, updated rows keep the old value;
- a **changed type** the target still accepts with every value intact (a wider
  `VARCHAR` or integer type). A value it would round or store as `true`/`false` is
  dead-lettered instead ([see above](#detecting-and-handling-source-schema-drift)); CDC
  infrastructure deployed by an earlier version of the tool still writes it changed until
  you delete and redeploy it;
- a column changed to `FLOAT`/`DOUBLE` on a `numeric(p,s)` target (for example
  `DECIMAL(10,2)` → `DOUBLE`): its values are rounded to the target's scale with no dead
  letter, because that case is not checked;
- a **dropped or renamed table** (or one moved to another schema): CDC captures tables
  by the names it was deployed with, so the target table stops changing. On MySQL a
  table re-created under the old name streams into it as is; on PostgreSQL, after a
  rename, changes reach the old target only until the connector restarts;
- on PostgreSQL, an **added column** of a type CDC cannot convert (for example
  `interval[]` or `money[]`), or a **generated** column: its values never arrive;
- a **NULL-rule** change, or a **primary-key** change.

So while CDC runs, the tool re-reads the definitions of the captured tables on the
source — one read-only catalog query for all of them (`information_schema` on MySQL,
`pg_catalog` on PostgreSQL; no table data is read), at most every 5 minutes while the
CDC monitor, Validation or Cut over is open — and compares them with the Step 1
(Evaluation) inventory the migration was built from: whether each table still exists,
its column names (exactly, case included), types, NULL rules and primary key. Excluded
columns and generated columns CDC does not capture are left out, and a column you added
with **Fix target schema…** counts as part of the definition (a column it renamed does
not: the check keeps reporting the rename, and holding Cut over, until you re-run Step
1); defaults, comments,
collations and indexes are not compared (a change there does not change a CDC row). A
difference is shown on the CDC step ([§4.6](#the-source-schema-re-check)) and holds Cut
over for that table until the source matches the Step 1 inventory again and the table
then matches by CHECKSUM ([§5.7](05-validation.md#57-source-schema-changes-and-cut-over)).

---

## 4.3 The gapless Full Load → CDC handoff

This is the bit designed to ensure **no missed changes and no duplicates** between
the bulk load and the stream.

1. Full Load captured a **watermark** at the snapshot point — for MySQL the binlog
   file:position, the GTID set and the source's `gtid_mode`; for PostgreSQL the WAL LSN
   returned when the logical replication slot is created, recorded together with the
   slot and publication names
   ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
2. When you start CDC, the tool establishes the resume point at exactly that
   watermark. **For a MySQL source** it **seeds the connector's start offset** (an
   in-VPC Lambda writes the offset record before the source connector starts). The
   seeded offset carries the file:position and the GTID set: with `gtid_mode=ON`
   Debezium starts after exactly that GTID set (the transactions the Full Load already
   copied); with `OFF`, `OFF_PERMISSIVE` or `ON_PERMISSIVE`, at the file:position. Start
   CDC reads `gtid_mode` first and refuses a start point it cannot use
   ([Chapter 6](06-limitations.md)). A Manual start point seeds its binlog file:position
   and the GTID set saved with it; one saved by version 0.1.565 or earlier comes back
   without its GTID set (those versions kept a fetched set beside any position typed
   after it), so on a `gtid_mode=ON` source use **Fetch current position** again, or
   re-enter the GTID set executed at exactly that position, before Start CDC.
   **For a PostgreSQL source** there is no offset-seeder Lambda; instead, at the
   Full Load consistency point the tool creates the logical replication slot (and a
   publication for the tables the Full Load loaded), and the slot's returned
   consistent WAL LSN *is* the watermark — CDC resumes directly from that
   pre-created slot, so nothing is written to Kafka's connect-offsets. Either way
   Debezium begins streaming the **first change after the snapshot** — not from
   "now," and not by re-reading the data.

   > **This describes a `Full load + CDC` run.** It is the slot created *during* that
   > load that makes the handoff free of a re-read, and a PostgreSQL slot cannot be
   > created at a past position. If you ran **`Full load only`** and come back to CDC
   > later, streaming is still lossless but the source connector creates its own slot,
   > sets the publication to the selected tables and **re-snapshots every selected
   > table first** — the data is read a second time (on a CDC stack whose connectors were
   > started before, Start CDC instead continues from that stack's own slot if its source
   > connector streamed (otherwise Debezium snapshots every selected table first): gapless
   > only while the CDC topics still retain what the sink had not applied). See
   > [Chapter 10 §10.1](10-conclusion.md)
   > for choosing between them. On MySQL the binary log retains history with no
   > consumer, so a later CDC start on a CDC stack that has never streamed starts from
   > the watermark with no re-read. On one that has streamed, Start CDC refuses that newer
   > start point: set the migration type to CDC only, then on its CDC step Delete CDC
   > infrastructure and deploy it again before Start CDC (Full load only has no CDC step;
   > [Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). Either way
   > CDC starts from the watermark and carries only the changes after it, so if that Full
   > load only run appended to rows an earlier load left on the target, first Re-run Full
   > Load choosing **Drop & reload**.

   **After a Full Load that may have appended to an earlier copy**, the CDC step states the Drop & reload condition before a Start that begins at that Full Load's watermark and is not refused. It does so when this session shows that the Full Load found rows already on the target (*already there* in its row counts on the Full Load step) on a table's first attempt (*Attempts* 1), or that Start CDC was submitted in this migration, from an earlier start point, before that Full Load ran and the Full Load found any — for example a re-run with the default *Append*, made before CDC was ever started or after it had streamed. The start-point card then shows *Gapless only if this Full Load did not append*: if that Full Load appended to them, Re-run Full Load choosing **Drop & reload** before Start CDC (CDC only has no Full Load step: on MySQL switch the migration type to Full load only for that run, then back to CDC only; on PostgreSQL switch it to Full load + CDC and re-run it there, which creates the slot again at its own snapshot point), because an append keeps those rows as they were and CDC from a watermark carries only the changes after it. The card no longer calls that start gapless, the Start CDC dialog shows the same notice without blocking Start, and the Start's activity-log line adds *(from the Full Load watermark; gapless only if that Full Load did not append to rows already on the target)* after its start point. On PostgreSQL, when the start point is set to re-snapshot (*Manual re-snapshots instead of resuming from this load's slot*) or the Full Load did not finish, the card's re-snapshot notice says the same of the resume from that Full Load's slot it gives as the way back, and names the Drop & reload re-run for the case where that Full Load appended. A first copy into empty tables finds no rows already there. A Drop & reload re-run recreates each table it loads, so the only rows it finds already there are ones it loaded itself (in a retry, or in a re-read after a dropped source connection). Retry unfinished tables and Reload specific tables keep that Full Load's watermark and continue each table's attempt count from the earlier run, so the rows a table they load again finds give no first-attempt sign, and a table they leave as it was keeps its own. So a Retry or Reload that loads again every table that showed the sign ends the notice, even when that Full Load appended. The notice can also show after a copy that did not append (its wording is conditional): when a table's first attempt re-read rows after a dropped source connection, when Aurora DSQL answered a batch that had committed with a temporary error (XX000) or the connection dropped after the commit, so that the batch's retry found its own rows, or when a multi-process load did not count one of a retried table's attempts. Rows found on a first attempt keep the notice for as long as that Full Load is the latest one, also after a Start CDC that failed before it updated the stack and after an Attach to an existing stack; otherwise the notice ends at the next Start CDC submission, from any start point, and after an Attach. Start over ends it in both cases.

   Before such a Full Load, its confirm dialog on the Full Load step names Drop & reload too: in Full load + CDC, where selected tables already hold rows, the dialog of Start Full Load, Re-run Full Load or Re-run all tables (each takes a new start point) does not mark *Append* recommended, and a line under the choice reads *CDC from this run's new start point needs 'Drop & reload' …*, while *Append* stays the default. Retry unfinished tables and Reload specific tables keep the start point, and their choice is unchanged.

   If you untick tables after the Full Load (Full load + CDC, before CDC starts; [§13.7](13-excluding-tables-and-columns.md)), CDC captures only the tables still selected and resumes from the same watermark or slot, so the handoff stays gapless for them. On PostgreSQL, when CDC resumes from that slot, the publication still lists the unticked tables (the tool does not remove them on the source) and the connector discards their changes; a Start that re-snapshots instead lets the connector set the publication to the captured tables. A Re-run Full Load over the smaller set reuses the existing publication.

   A table you tick after the Full Load (Full load + CDC, before CDC starts) is loaded first, by Retry unfinished tables after the checks in [§13.7](13-excluding-tables-and-columns.md), and CDC then captures it from the same watermark or slot (a MySQL table the Full Load recorded no start-point definition for needs Re-run all tables, with **Drop & reload**, instead); Start CDC waits until it is loaded. On MySQL, CDC replays every change made to it since the watermark over the newer copy. On PostgreSQL the table joins CDC's publication just before its copy starts, and CDC streams every change made to it from then on over that copy. Either way it converges; a change that briefly conflicts on a unique index can be dead-lettered, and Cut over then waits for a CHECKSUM validation.
3. **For a MySQL source**, a Start at a **new start point** — the first Start (from the
   Full Load watermark or a Manual point), or the first Start after Delete CDC
   infrastructure — runs the source connector with **`snapshot.mode=recovery`**:
   because an offset is already seeded, Debezium rebuilds its internal **schema
   history** from the **current source tables** (so it can decode binlog events)
   **without re-reading any row data**, then resumes from the seeded offset. Start
   CDC first checks that the captured tables have not changed since that start point
   ([Chapter 6](06-limitations.md)). On a stack that has streamed, Start CDC refuses a
   newer Full Load or Manual point (*This CDC stack cannot start from a newer start
   point*) until the infrastructure is deployed again: its topics may hold changes no sink
   applied, which its sink would write over anything loaded since, while the source would
   skip every change between where it stopped and the new point
   ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). After a first
   Start that CloudFormation rolled back, it refuses a later point than that Start offered,
   after an app restart too (the rollback restores the stack's parameters, but the app reads
   that point from the stack's CloudFormation events), and every start point when those
   events do not give it either.
   A **restart after Stop CDC** runs
   **`snapshot.mode=schema_only`** instead: it replays the recorded schema history
   and applies any DDL in binlog order. Such a restart, like any automatic connector
   restart, continues from the position Debezium recorded — on `gtid_mode=ON`, after
   the GTID set it last committed. Stop CDC removes both connectors without draining the
   sink, so the changes the sink had not applied when CDC stopped reach Aurora DSQL on that
   restart only from the CDC topics, re-applied from the sink's last commit (idempotent, so
   nothing is duplicated): the restart is gapless only while the topics still retain them —
   their retention period on MSK. The *Ready to resume CDC* card and the *Resuming from the
   last streamed position* start-point card state that condition; neither promises *no gap*
   or that nothing is re-applied. **For a PostgreSQL source**, the Debezium
   pgoutput connector has no schema-history topic: on the gapless path it runs with
   **`snapshot.mode=never`** (the logical slot already holds the start LSN and the
   rows are loaded), while a stand-alone / manual start uses **`initial`** (snapshot
   then stream). Debezium PostgreSQL can only resume from the slot's own
   position — it cannot seek an arbitrary WAL LSN. A Start after Stop CDC continues from
   the slot's position (where streaming stopped, or the Full Load's start point if its
   source connector never streamed); with `initial`, though, a source connector that never completed
   its snapshot (its task failed, or CDC was stopped, first) snapshots every selected
   table again — a full re-read of the source. Stop CDC removes the connectors without
   draining them, so the changes the sink had not applied by then reach Aurora DSQL only
   from the CDC topics: such a Start is gapless only while the topics still retain them,
   which Start CDC does not check. Once their retention period on MSK has passed, it would
   skip them, so the Start CDC card (*Ready to resume CDC*) and the start-point card
   (*Resuming from this stack's replication slot*) name the route to take instead: that of
   a stack whose connectors were started before (new topics first; see the PostgreSQL note
   below). The **Stop CDC** dialog states the same condition before you stop — for either
   source — and on PostgreSQL it names that route for after the retention period. A Start that snapshots is refused for a captured table that has inheritance
   (`INHERITS`) child tables (*CDC cannot snapshot a table that has inheritance child
   tables*), because Debezium's snapshot reads it with its children's rows
   ([Chapter 6](06-limitations.md)). If the tool can read the source but not which
   captured tables have any, it refuses such a Start too (*Whether a captured table has
   inheritance child tables could not be read*): check that the tool can reach the source,
   then Start CDC again. If it cannot read the source at all — for example a Secrets
   Manager source after an app restart, before you test the connection again — Start CDC
   is not blocked, but its PostgreSQL pre-start checks do not run (the publication, the
   replication slot and its position, and the inheritance read): the Start CDC dialog
   warns *CDC's publication and replication slot could not be checked*, with the reason,
   and the activity log records *PostgreSQL publication and slot checks not run*
   (WARNING), from the dialog and again from the Start CDC job when it goes ahead without
   them. Test the source connection on the Connect step, then open Start CDC again so they
   run.

The result: every change between the snapshot and "now" is applied exactly once.
Because the sink's applies are **idempotent** (keyed by PK), even an overlap or a
retry can't create duplicates.

> **MySQL: the binlog at the watermark must still exist when CDC starts.** This
> handoff only works if the source hasn't purged the binary log at the watermark
> position yet. RDS/Aurora purge binlogs aggressively by default (Aurora MySQL
> keeps them 24 h), and deploying the CDC stack takes ~15–20 min, so **raise binlog
> retention before you start** — see [§1.1](01-setup.md#11-prerequisites). If the
> segment is already gone, CDC can't resume gaplessly and you'd re-run Full Load with
> **Drop & reload** to capture a fresh watermark (an append would keep the rows already
> loaded as they were) — on a stack that has streamed, after Delete CDC infrastructure
> and a new deploy, because the Full Load step refuses a new start point until then
> ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)).
>
> **PostgreSQL: the logical replication slot pins the WAL for you.** From the moment the
> slot is created it holds the required WAL, so nothing is purged out from under you — the
> prerequisite is `wal_level=logical` (RDS/Aurora `rds.logical_replication`), not binlog
> retention. But the slot must stay healthy: if it falls too far behind and Postgres
> reports `wal_status='lost'` (slot invalidated), gapless resume from it is broken, and
> Start CDC blocks and names the way out (below): with Full load + CDC, re-run Full Load
> with **Drop & reload** to capture a fresh watermark, after Delete CDC infrastructure and
> a new deploy if the stack's connectors were started before. The CDC monitor's slot panel reports the same slot
> as *Replication slot invalidated* and names that route in full: Delete CDC infrastructure
> and deploy it again, then Re-run Full Load with **Drop & reload**, then Start CDC; with CDC
> only, Delete CDC infrastructure and deploy it again, then Start CDC on the new stack, which
> snapshots every selected table (that type has no Full Load step to re-run). The slot's name
> derives from the CDC stack name, so two migrations of one source that use the same stack name
> share it. The Full Load
> never drops a slot of that name that is in use or belongs to another database (it stops
> before any write instead). So where a slot of that name exists but CDC cannot use it —
> another database's, or not a logical `pgoutput` slot — every block names that slot's own fix
> first (drop it, or give this migration its own CDC stack name), ahead of any Full Load
> re-run, which would refuse such a slot before any write; the block for a missing slot says
> the same, and such a slot is reported before a captured table's inheritance children, whose
> own fix is that very re-run. On a CDC stack's first Start — while the stack records no
> Start of its connectors, not even one whose CloudFormation update rolled back, and MSK
> Connect lists none of its connectors — Start CDC refuses to resume from a slot that is
> no longer at the Full Load's start point (*CDC's replication slot is not at this Full
> Load's start point*: another run with the same CDC stack name re-created or read it, and
> resuming would skip the changes in between): re-snapshot every table, or Re-run Full
> Load with **Drop & reload** (under its own CDC stack name if another migration shares
> this one) — with CDC only, after switching the migration type to Full load + CDC. The routes for a stack
> whose connectors were never started are below. A stack whose connectors were started
> before — stopped since, with its connectors still in place (as for Retry CDC; connectors
> of the stack that MSK Connect lists count whatever the stack's status), or with a last
> Start whose CloudFormation update rolled back (any `UPDATE_ROLLBACK_*` status: complete,
> in progress, failed or cleanup in progress) — is never judged on the slot's position:
> its own source connector may have moved the slot. A Full Load that would take a new start
> point is refused on such a stack until the CDC infrastructure is deleted and deployed again
> ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)), so nothing here
> describes a resume from a slot a Full Load re-created after this stack had streamed. The check
> reads only what the stack records
> and which of its connectors MSK Connect lists, so a refused Start, a Start that failed
> before it updated the stack, an app restart and Start over all leave the next Start
> judged the same way. It cannot see another migration that uses the same CDC stack name
> and re-creates the slot after this stack has streamed, or after this stack's first Start
> rolled back, so give each migration its own CDC stack name.
>
> **On a stack whose connectors were started before**, the CDC topics may still hold
> changes that no sink applied: Stop CDC and a rolled-back Start remove the connectors
> without draining them, and this stack's sink resumes from its last commit, so a Full
> Load re-run on the old topics would have those changes applied over the reloaded tables.
> A re-snapshot cannot be relied on there either: once the source connector has streamed,
> Debezium skips the snapshot on every later Start. So on such a stack a block for a
> missing publication, a publication that misses a selected table, or a missing,
> invalidated or unusable slot says so and names one route, new topics first: Delete CDC
> infrastructure and deploy it again (the new stack starts with empty topics), then Re-run
> Full Load with **Drop & reload** (it creates the slot again at its own snapshot point),
> then Start CDC; with CDC only, Delete CDC infrastructure and deploy it again, then Start
> CDC on the new stack, which snapshots every selected table (a row deleted on the source
> in between stays on Aurora DSQL; Validation reports it as an EXTRA row). For the same
> reason the inheritance block (*CDC cannot snapshot a table that has inheritance child
> tables*), whose own fix is a Full load + CDC run, ends there by asking you to Delete CDC
> infrastructure and deploy it again before that Full Load. The block for a slot of that
> name it cannot use names the route as a step after its own fix (drop that slot, or use a
> different CDC stack name). No **Re-snapshot every table instead** is offered on such a
> stack. The diagnosis of a source connector that fails with Debezium's *Publication
> autocreation is disabled* (read only on a stack whose connectors were started) names the
> same route for both migration types, after Stop CDC if the CDC step still shows the
> connectors.
>
> **After a Start that CloudFormation rolled back**, the next Start continues from the
> position that Start recorded, if it recorded one: its source connector may have
> streamed, and committed its position, before the rollback deleted it, which is why such
> a Start counts. Otherwise, with `snapshot.mode=never`, it continues from the slot's
> confirmed position, with no snapshot; with `initial` (Manual included), from where an
> earlier run stopped streaming. With `initial`, though, Debezium continues without a
> snapshot only from a recorded position whose initial snapshot had completed: if that
> Start (or the earlier run) had not completed its initial snapshot, or nothing was
> recorded, it snapshots every selected table first, then streams from the slot. So the
> Start CDC card (*Ready to start CDC again*), the start-point card above it (*Starting
> again after a rolled-back Start*, which offers no start point or re-snapshot there) and
> the Start's activity-log line say just that: with `initial`, they promise neither a
> snapshot nor its absence. The changes that Start streamed exist only in the CDC topics,
> which outlive the rollback, and the next sink applies them from there: starting CDC
> again is gapless only while the topics still retain them, which Start CDC does not
> check. Once their retention period on MSK has passed since that Start, use Delete CDC
> infrastructure and deploy it again, then Re-run Full Load with **Drop & reload**, then
> Start CDC (with CDC only, Delete CDC infrastructure and deploy it again: its first Start
> CDC snapshots every selected table). If CDC had been **stopped** before that rolled-back
> Start, the cards say so and count that period from the **Stop** instead (*since CDC was
> stopped*): the changes the sink had not applied when CDC stopped are the oldest in the
> topics, so they expire first. One case goes unchecked: on a stack whose first
> Start rolled back, a Start that sends `snapshot.mode=initial` does not check that the
> slot exists. An absent slot there is normally one the failed connector never created,
> and that Start snapshots with no gap; but if that connector streamed and the slot was
> dropped since, that Start skips every change made in between.
>
> Before a block offers **Re-snapshot every table instead**, the Start CDC dialog, as it
> opens, runs the checks of the Start that remedy would unlock (`snapshot.mode=initial`)
> and offers it only if they pass. It is offered only on a stack whose connectors were
> never started, for a missing publication, a missing slot or a slot that is not at the
> Full Load's start point. If that Start would be refused for the block's own reason, the
> block is shown with no second notice and ends with the route that works there; if for
> another — for example a captured table has inheritance child tables, or a slot of that
> name exists but cannot be used (an invalidated one, say) — the dialog shows that refusal
> too, introduced by *Re-snapshot every table is not offered here: a Start that
> re-snapshots is refused too.* It then names the route that works there. After an
> inheritance refusal (or a failed read of them), that is Re-run Full Load with
> **Drop & reload** instead, which creates the slot again at its own snapshot point, so
> the Start CDC after it takes no snapshot; with CDC only, which has no Full Load step,
> switch the migration type to Full load + CDC first. After the refusal of a slot of that
> name it cannot use, it is a step after that refusal's own fix (drop that slot, or use a
> different CDC stack name): then Re-run Full Load with **Drop & reload**, then Start CDC
> (with CDC only, Start CDC again). A publication that misses a selected table is refused
> for its own reason (the Start that re-snapshots grades the publication's coverage too),
> so its block ends with its route: Re-run Full Load with **Drop & reload**, which adds
> each missing selected table to the publication and creates the slot again at its own
> snapshot point, then Start CDC (with CDC only, Delete CDC infrastructure and deploy it
> again: it drops the publication, so the new stack's Start CDC can create one over the
> selected tables). The Start CDC job runs the publication and slot checks again before it
> creates any connector (Retry CDC opens no dialog). On a stack never started, its refusal
> for a publication that misses a selected table, or for a slot of that name it cannot
> use, ends with the same route; for a missing publication, a missing slot or a slot that
> is not at the Full Load's start point it names both ways out: Re-snapshot every table
> instead (the Start CDC dialog offers it when the Start that re-snapshots passes its own
> checks), or Re-run Full Load with **Drop & reload**, then Start CDC (with CDC only,
> after switching the migration type to Full load + CDC). If those checks cannot read the
> source, the remedy stays, and the dialog warns *The re-snapshot route could not be
> checked*; the activity log records *PostgreSQL re-snapshot route check not run*
> (WARNING). The publication and slot checks did run then, and the Start CDC job runs the
> re-snapshot's own checks before it creates any connector (it goes ahead without them if
> it cannot read the source either). *CDC's publication and replication slot could not be
> checked* is shown only when the publication and slot checks themselves cannot read the
> source (above).
>
> **A publication restricted to a subset of INSERT/UPDATE/DELETE** blocks Start CDC too
> (*CDC's publication does not publish every change type*), and that block names its own fix
> first: as the publication's owner, run `ALTER PUBLICATION … SET (publish = 'insert,
> update, delete')` on the source. That alone is not enough, and the block says so: pgoutput
> decides what to publish from the publication as it stood when each change committed, so
> setting the list back does not bring back the changes committed while it was narrowed, and
> a Start that continues from the slot would still skip them. The route follows that fix —
> on a stack whose connectors were never started, Re-run Full Load with **Drop & reload**,
> then Start CDC (with CDC only, switch the migration type to Full load + CDC first); on one
> whose connectors were started before, new topics first, as above. **Re-snapshot every
> table instead** is never offered for it, because a re-snapshot — like a Full Load
> re-run — reuses the publication exactly as it is.
>
> **The inheritance block's second fix is named only where the UI can carry it out.** *…or
> untick `<the parent>`: each child table migrates as a table of its own* appears only while
> the table picker can still change the set. Once a Start has been submitted the set is
> locked, so the Start CDC job's own refusal never names it and only the Start from a Full
> Load's snapshot point is left. And when such a Start snapshots only because **Re-snapshot
> every table instead** was taken over a slot the Full Load recorded, the block names the way
> back: **Resume from the Full Load's slot instead**, a control on the CDC start point card
> that sets the start point back to Automatic — so a recorded re-snapshot never loops on the
> same block. The control appears in exactly that state, and the re-snapshot notice (*Manual
> re-snapshots instead of resuming from this load's slot*) names it too.

> **Why `recovery` at a new start point and `schema_only` on a restart? (MySQL
> source)** With a seeded offset present, Debezium takes its "resume" path and
> expects an existing schema-history topic, which a new start point does not have
> yet. `recovery` builds that history from the live database without re-snapshotting
> rows — exactly the "I already loaded the data myself, just resume from this offset"
> situation — provided the tables have not changed since the offset, hence the
> pre-start check. On a restart the history already exists and is replayed instead: a
> rebuild would read the older rows with today's definitions. If the history is
> missing, that Start fails loudly and the next Start (after Stop CDC, if its
> connectors are still listed) uses `recovery` after the same check. **This does not apply to a PostgreSQL source:** PG has no schema-history
> topic, so Debezium simply resumes from the pre-created logical slot with
> `snapshot.mode=never` (or does an `initial` snapshot for a stand-alone start).

---

## 4.4 How the sink handles DSQL's constraints in the data path

DSQL is distributed and PostgreSQL-compatible, so the sink can't behave like a
classic MySQL/JDBC writer. Here is what it does for each constraint:

| DSQL constraint | How the custom sink handles it |
|---|---|
| **IAM-token auth (no password)** | Generates a short-lived IAM token (admin or standard) and uses it as the JDBC password over TLS; **refreshes before expiry** (15-min token, 2-min refresh margin) so long-running CDC never stalls on an expired token. |
| **Optimistic concurrency (no locks)** | On `SQLSTATE 40001` it re-runs the conflicting **transaction** with exponential backoff + jitter (up to 10 attempts): one chunk of ≤3000 rows (safe because apply is idempotent), or one row while it applies row by row to isolate a rejected row. Each re-run starts on a clean transaction: the failed attempt is rolled back first, because Aurora DSQL can also report `40001` while a statement runs (`OC001`, after a schema change elsewhere on the cluster), not only at commit. CDC infrastructure deployed by an earlier version of the tool re-runs without that rollback: until you **Delete CDC infrastructure** and deploy it again, such a `40001` can dead-letter a healthy row (`25P02`). |
| **Temporary service condition (`XX000` "server unavailable")** | Aurora DSQL documents `XX000` as temporary: the connection stays usable, and the whole transaction, `COMMIT` included, is to be run again. The sink handles it exactly like a `40001`: it rolls the failed attempt back and re-runs the **transaction** (one chunk, or one row) with the same exponential backoff + jitter, and the two share the 10-attempt budget. If it outlasts that budget, the sink reconnects and Kafka Connect redelivers the batch until Aurora DSQL answers again, so such a row is **never dead-lettered**. CDC infrastructure deployed by an earlier version of the tool dead-letters such a row instead (`XX000`) until you **Delete CDC infrastructure** and deploy it again. An `XX000` at commit can mean the commit landed. Every write is a PK-keyed upsert or delete, so the re-run is idempotent per primary key; on a table with a secondary `UNIQUE` index, though, it can be rejected (`23505`) for an intermediate image the landed chunk already replaced. If the commit had landed, the re-run's intermediate image is replaced by the row's later change in the same batch and skipped, on a table that is not re-keyed; any remaining conflict waits like any held change (the row below) and is dead-lettered only if it outlasts the 5-minute wait, and then, like every dead-lettered table, Cut over holds the table until it matches by CHECKSUM ([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)). Earlier infrastructure dead-letters it at once. While it lasts, the CDC step reports the sink as stalled ("The sink is not applying changes") and the sink connector log repeats `DSQL apply failed (transient sqlstate=XX000); Connect will retry`; the stream resumes on its own once Aurora DSQL answers. If `XX000` keeps recurring, contact AWS Support. |
| **Unique indexes besides the primary key (`23505`)** | Aurora DSQL checks a unique index at once (only foreign keys can be deferred), so a change can be rejected while another row still holds its value. That is what an `UPDATE` of the primary key meets: Debezium replicates it as a delete of the old key and an insert of the new key, keyed differently, so the two can travel on different partitions and the insert can arrive first; a unique value passed from one row to another can do the same. The sink (plugin v53 or later) holds such a change, and every later change on its partition, until the change that frees the value has applied: it is tried again with a growing wait (500 ms up to 10 s) while every other partition keeps applying, for at most 5 minutes from its first rejection — or from the last transient failure (a dropped connection, or an `XX000` or `40001` that outlasted its retries) of the sink task that holds it, if that is later, so an Aurora DSQL outage that task sees does not use up the wait; one that only delays the freeing change on another sink task can. The wait shows as Stream lag on its table (§4.6). The 5 minutes apply to each held change: conflicts on one partition are waited out one after another, during a catch-up too. On a table that is not re-keyed, if a later change of the same row replaces it entirely (a plain delete, or an image of the same columns) — in the same batch, or in its partition's next batch, which the sink reads (without applying it) before it pauses the partition — the conflicting change is skipped instead (never dead-lettered). On a re-keyed (Composite key) table nothing is skipped. Past the 5 minutes it is dead-lettered with SQLSTATE `23505` and the index name (*a unique constraint rejected the row*). CDC infrastructure deployed by an earlier version of the tool dead-letters the conflicting change at once, and the old row's delete then removes the row, until you **Delete CDC infrastructure** and deploy it again ([Chapter 6](06-limitations.md)). A skipped change leaves its row's values on the target as they were until its replacement applies. So a change of another row that needs a value the skipped change would have freed (a unique value passed along several rows) waits for that replacement, and is dead-lettered after its 5 minutes when the replacement is queued behind it on its partition, or itself waits for a value that change's row still holds (then both are dead-lettered); like every dead-lettered table, Cut over then holds the table until it matches by CHECKSUM. |
| **≤ 3000 rows per transaction** | Applies in chunks of ≤ 3000 rows (default batch 3000) and at most an 8 MiB byte budget, one `commit()` per chunk. Every text, JSON or binary value counts at its size, so a batch of large JSON documents is split well under Aurora DSQL's 10 MiB limits on a transaction and on one message (a statement never spans two chunks). CDC infrastructure deployed by an earlier version of the tool counted a JSON value (MySQL `JSON`, PostgreSQL `json`/`jsonb` or an array) as a few bytes: a burst of large documents could build a statement over the message limit, which Aurora DSQL refuses (`08P01`, *invalid message length*), and the sink then retried that batch forever, shown as a stalled sink, until you **Delete CDC infrastructure** and deploy it again. |
| **No UPDATE-by-statement / replays** | Every change is an **idempotent upsert or delete keyed by PK**: `INSERT ... ON CONFLICT (pk) DO UPDATE` for inserts/updates, `DELETE ... WHERE pk = ?` for deletes (and Kafka tombstones). Re-applying the same event is safe. |
| **Connections drop (idle close / token expiry / worker recycle)** | Detects a dead/half-open connection, reconnects with a fresh token, and **replays the same offsets** (safe because applies are idempotent) rather than dropping records. Connectivity errors are treated as transient and retried, not mistaken for poison rows. So is a connection Aurora DSQL refuses to open — its IAM token (`28xxx`), too many open connections (`53300`) or the connection rate (`53400`): the batch is retried with a newly signed token and nothing is dead-lettered. CDC infrastructure deployed by an earlier version of the tool dead-letters every row it tries while such a refusal lasts, until you **Delete CDC infrastructure** and deploy it again. |

---

## 4.5 The 1 MiB per-value limit, and the DLQ

DSQL rejects a **single value larger than ~1 MiB** (a `TEXT`/`bytea` value). The
pipeline handles oversized values in **three bands**:

| Value size | What happens |
|---|---|
| **≤ 1 MiB** | Applied normally. |
| **1 MiB – 8 MiB** | The sink measures each value **before writing** and **quarantines** the oversized one to the **DLQ** (it can never be applied), while the rest of the record's table keeps flowing. To let such a record even traverse Kafka to be dead-lettered, the per-table topic and client limits are raised (default 4 MiB, max 8 MiB). |
| **> 8 MiB** | Cannot enter Kafka at all. These must be **excluded at capture**: Debezium `column.exclude.list` drops the oversized LOB column so it never reaches the pipeline. You exclude it with a rule in **Exclude tables & columns** (under the table list on the Data Migration step); its *Oversized-LOB candidates* section lists the same columns Evaluation flags `OVERSIZED_LOB`. Rows the tool writes hold NULL in an excluded column; rows already on Aurora DSQL before CDC started keep their values (CDC never writes an excluded column), and Validation counts them. |

### What gets dead-lettered

Beyond oversized values, the DLQ isolates any record DSQL **permanently** rejects
— a type mismatch, a constraint violation, a missing target column (e.g. after an
un-propagated source `ALTER`) — and any value the sink refuses because its Aurora DSQL
column would change it ([§4.2](#detecting-and-handling-source-schema-drift)). Transient
failures (OCC `40001`, Aurora DSQL's temporary `XX000` "server unavailable", connection
drops) are **retried**, not dead-lettered. A rejection
a source schema change explains gets a banner row with its repair (§4.2); a per-value data
error with no schema meaning (for example `22P02`) is listed with its SQLSTATE only.
While a table's rows are being dead-lettered because the target lacks a column, a table
or the key, or a column's type changed, the other tables are still applied in batches
(see §4.2); a value-level rejection (a unique or `CHECK` violation, a value too long or
out of range) still makes the rows near it apply one per transaction. A unique violation
(`23505`) of an insert or update is first held up to 5 minutes for the change that frees
the value (§4.4) and dead-lettered only if it outlasts that.

### Where the DLQ surfaces — CloudWatch, not a Kafka topic you read

The sink logs each quarantined record to its **CloudWatch** connector log group,
and the tool's monitoring parses those lines into the UI (per-table
"Quarantined" counts and a single downloadable error log). The logged reason
includes the **SQL template** (column names + `?` placeholders) — **never row
values or credentials** — so you can see the exact statement shape DSQL rejected
without exposing data. A record that can be neither applied nor dead-lettered
makes the task **fail loudly** rather than silently skip — visible over silent
loss.

### What CDC does with later changes to a row that Full Load quarantined

If Full Load quarantined a row (its oversized value could not be written), the row
is **absent from the target** while still present on the source. What happens when
that same row changes later depends on the operation:

| Source operation on the missing row | What CDC does | Result |
|---|---|---|
| `DELETE` | `DELETE … WHERE pk = ?` matches **0 rows**. The sink does not treat a 0-row delete as an error, so it is applied and committed silently. | **Correct.** The intended end state — "the row is not on the target" — already holds. Treating this as a failure would break idempotency: a replayed or retried delete must stay safe. |
| `UPDATE` that shrinks the value below 1 MiB | Debezium sends the **full after-image** and the sink writes `INSERT … ON CONFLICT (pk) DO UPDATE`. With no existing row, `ON CONFLICT` never fires and the row is **inserted**. | **The gap heals itself.** No action needed. |
| `UPDATE` where the value is still oversized | The sink measures values **before** writing, so the record is quarantined to the **DLQ** for the same reason. | Gap persists, and it is **visible** (DLQ depth / "Quarantined" in the monitor). |
| `INSERT` of a *different* row | Unaffected. | Normal. |

Two consequences worth planning around:

- **A quarantine gap is not self-announcing after Full Load.** If the source never
  touches that row again, the gap simply persists — CDC has nothing to replicate.
  It is **Validation (Step 4)** that reports it, which is why the Full Load
  completeness verdict points you there.
- **A 0-row delete is indistinguishable from a normal replay.** The sink cannot tell
  "this delete found nothing because the row was quarantined" from "this delete was
  simply reprocessed". That is a deliberate trade-off in favour of idempotency;
  gap accounting belongs to Validation, not to the delete path.

The practical guidance is unchanged: fix the oversized source value and **Reload**
that table before cutting over, or exclude the oversized LOB column at capture
(§4.5) so it never enters the pipeline.

---

## 4.6 Monitoring CDC progress (the migration monitor)

While CDC runs, the Data Migration screen shows a live per-table monitor. It
refreshes on its own (no manual reload) and is **scan-free** on the source — it
never runs a `COUNT(*)` against your production database. The columns:

| Column | What it means |
|---|---|
| **Full Load rows** | Rows the one-shot snapshot loaded into the table. |
| **Inserts** | Cumulative CDC inserts applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Updates** | Cumulative CDC updates applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Deletes** | Cumulative CDC deletes applied to this table since Full Load — a **non-negative** per-table running count reported live by the sink (scan-free). |
| **Quarantined** | Per-table count of change events set aside to the **DLQ** — permanently-rejected rows (bad type, oversized > 1 MiB value, constraint / schema-drift), reported live by the sink. |
| **Source rows (est.)** | Scan-free catalog **estimate** (`information_schema.tables` for MySQL; `pg_class.reltuples` for PostgreSQL). **Target rows** — exact DSQL count. |
| **Stream lag** | How far the target is behind the source **in time** (see below). |
| **Consistency** | A colored badge: green *consistent* = counts match · *replicating…* = catching up · red *rows missing* = the newest change landed but rows went missing mid-stream · red *data quarantined* = the DLQ has un-applied events. |

### Stream lag — a real time measure, not a row count

**Stream lag** is the end-to-end replication delay: for each change, the sink
records **apply time − the source commit time** (Debezium `source.ts_ms`) and
emits it as a per-table `ReplicationLagMs` CloudWatch metric; the monitor shows
the **worst (maximum) lag** observed over the recent window, not the latest single
value.

- Reads as a **duration**: `caught up` (sub-second / the stream has drained),
  `8.5s behind`, `2m 10s behind`, `1h 4m behind`.
- It is a true **time** lag, so it is accurate for **any** primary-key type and
  reflects update/delete lag too — not just the newest insert.
- A change the sink **holds on a unique index** (§4.4) counts as lag on its table
  while it waits — when it is held, on each retry and at each offset commit (about
  once a minute) — with no insert, update or delete counted for it until it applies;
  the lag drops once it applies or is dead-lettered. There is no per-partition lag:
  the changes queued behind it on its partition show only through it.
- **Fallback:** when the time metric isn't available yet (an older sink plugin, or
  the metric hasn't emitted a datapoint), the monitor falls back to a `MAX(pk)`
  **leading-edge** check and shows `N behind (PK)` (how many PK units the target's
  newest key trails the source's) or `caught up`. This fallback only works for a
  single-column integer PK; the time-based value above is the preferred, general
  measure.

Lag is emitted strictly **best-effort** and never affects replication. Use "Stream
lag → caught up", at least 5 minutes after the source's last write (a held change's
lag can take a minute or two to show), as the signal that it's safe to proceed to
[cut-over](10-conclusion.md).

### Pipeline health and change flow

Above the per-table table, a **pipeline-health card** shows each connector's state
and a **"change flow"** status line — *Streaming*, *No changes flowing — idle*, or
**"Sink stalled — changes are NOT reaching DSQL"** — with two rec/s gauges (source
poll vs sink send), so you can tell an idle stream from a stuck one at a glance. A
**live stream-lag chart** plots the worst end-to-end lag over time, distinguishing
"caught up" from a confirmed sink stall.

A confirmed stall's notice points to the sink connector log. A repeating
`Commit of offsets timed out` means the sink consumer was ejected from its group: Stop
CDC and Start CDC to rejoin it. A repeating `DSQL apply failed (transient sqlstate=…)`
means the sink keeps retrying writes that fail, and the SQLSTATE says why: `XX000` is a
temporary Aurora DSQL condition (the sink resumes on its own; contact AWS Support if it
lasts); `08xxx`, `57xxx` or `null` means the sink cannot reach the cluster (check that
the cluster still exists, that the connector subnets reach its endpoint on port 5432 —
security group, NAT gateway or Aurora DSQL VPC endpoint and its DNS — and that the
connector's IAM role may still connect); `28xxx`, `53300` or `53400` means Aurora DSQL
refused to open the connection (`28xxx` its IAM token: check that the connector's IAM
role may still connect; `53300` the cluster's limit of open connections: close idle ones
elsewhere or raise the quota; `53400` its rate of new connections), and the sink resumes
once one opens; `40001` means something else is writing the same rows on Aurora DSQL
(stop it until cut over). Do not cut over until the sink send rate recovers.

When a connector's task stops, its row turns red. If the connector's CloudWatch worker
log shows a failure the tool recognizes — a MySQL DDL statement Debezium cannot parse, a
schema-history problem, a PostgreSQL publication it cannot create, an unreachable
source, and others — the row reads *Stopped —* followed by the cause, and an error
notice under the connectors says what to do. The tool reads only the newest few hundred
lines of that log, only while a connector is stopped and at most once a minute, and
keeps what it found until the connector runs again or the next Start; quoted values in
a DDL statement are hidden. Otherwise the row reads *Stopped — a task is not running.
Restart required.* and the log group `/msk-connect/<stack>-cdc` has the details. The
lifecycle card then says CDC is not streaming and offers **Stop CDC**: Retry CDC leaves
connectors that exist as they are, so it cannot restart a stopped one. After Stop CDC,
the cause stays on the Start CDC card until the next Start.

### Inspecting the DLQ

An interactive **DLQ inspector** lists dead-lettered records — **Time / Table /
SQLSTATE / Reason** — paged and filterable, with a per-table breakdown and a depth
badge, so you can triage poison rows without leaving the UI. **"Download CDC error
log"** exports them as NDJSON. (The reason carries the SQL template, never row
values — see §4.5.) The record list lives in memory and is not kept across an app
restart; the per-table count, latest time and drift kinds are, and they are what Cut
over checks ([§5.5](05-validation.md#55-cdc-dead-letters-and-cut-over)).

### Re-keyed PostgreSQL tables: the REPLICA IDENTITY re-check

A table CDC re-keys (a composite target key that adds a column outside the source
primary key) needs its source `REPLICA IDENTITY` to be **FULL** on the table and every
partition. CDC infrastructure deployed by this version keys such a table's change records
on the **source** primary key, so every change to a row stays in order, and the sink writes
them by the composite key (an `UPDATE` of the added column becomes a delete of the old row
and an insert of the new one, in one transaction). Below FULL a `DELETE` does not carry the
added column: the sink then removes the row by its source primary key, but only once Aurora
DSQL has built the table's unique index on that key (Composite key creates it with
`CREATE UNIQUE INDEX ASYNC` together with the table at Schema Conversion's Apply, or after
the load when **Drop & reload** recreates the table; Aurora DSQL builds it in the
background). Until then (for example while it builds after a Drop & reload), and always on
CDC infrastructure deployed by an earlier version, which keys the records on the composite
key, the delete leaves its row on Aurora DSQL with no error and no dead letter. An `UPDATE`
that changes only the added column arrives with no old image: once that index is built the
sink replaces the row holding its source primary key with the new one; before then, and
always on earlier infrastructure, it is dead-lettered (*a unique constraint rejected the row*)
and leaves the old row. So the monitor re-reads `REPLICA IDENTITY` (one read-only catalog
query for all such tables, at most every 2 minutes, never on a MySQL source) and shows these
notices:

- **Red — "Deletes on re-keyed tables can be lost":** a table (or one of its
  partitions) is below FULL. Run `ALTER TABLE <table> REPLICA IDENTITY FULL` on the
  source — on each partition too, since the parent does not propagate and a new
  partition starts at `DEFAULT` — then choose **Check again** (on PostgreSQL 18, for a
  table with a generated column, it names **Keep source PK** instead of FULL, and a failed
  re-read keeps that; after an app restart, until the tool reads the source again, it
  names FULL and adds that such a table needs **Keep source PK** instead; see
  [Chapter 6](06-limitations.md)). Cut over is held. Deletes made before the fix are not
  replayed. If the re-read itself fails, the notice
  says so: if the source credentials or network access changed, reconnect the source on
  the Connect step.
- **Amber — "REPLICA IDENTITY is FULL again":** the fix is in, but deletes made while
  it was below FULL may be missing. Validation compares the table by CHECKSUM
  automatically; Cut over waits for a match started after the fix
  ([§5.6](05-validation.md#56-re-keyed-postgresql-tables-and-cut-over)), unless you
  chose **Continue without this check** there. If the table differs: let CDC catch up,
  Stop CDC, reload it on the Full Load step with **Drop & reload**, Start CDC, and
  re-check it. CDC only has no Full Load step: delete the extra rows on Aurora DSQL by
  hand instead. The same notice, headed "REPLICA IDENTITY was below FULL on re-keyed
  tables", appears when CDC stops re-keying a table that was below FULL (the table was set
  back to **Keep source PK** in Schema Conversion and CDC redeployed, or the source primary
  key now covers the composite key — keying its change records on the source primary key
  does not count): deletes lost before then still need the CHECKSUM.
- **Info — "Can't re-check REPLICA IDENTITY on the source":** the source password is
  kept in memory only, so after an app restart the tool cannot connect until you
  reconnect on the Connect step. Keep these tables at FULL meanwhile.
- **Amber — "Can't re-check REPLICA IDENTITY on the source":** the last read failed;
  the tool tries again in 2 minutes. Keep these tables at FULL meanwhile.

Each change (below FULL, FULL again, no longer re-keyed) is written once to the activity
log.

### The source schema re-check

The monitor re-reads the captured tables' definitions on the source (see
[§4.2](#changes-that-dead-letter-nothing-the-source-schema-re-check)) and shows these
notices, beside the dead-letter queue card rather than in it:

- **Red — "Source schema changed on N captured table(s)":** one line per table says
  what changed (a column dropped, added or renamed; a type, NULL rule or primary key
  changed; a table not found) and what that does to the rows CDC writes. Cut over is
  held. A table whose change the dead-letter banner already reports the same way (a
  column added, dropped or retyped, a changed primary key, or a relaxed NULL rule when
  the banner names every relaxed column, with the rows it rejected) is listed only in
  that banner; Cut over is held either way. To realign a table once the source has its
  final definition:
  - **Full Load + CDC:** re-run Step 1 (Evaluation) — the check then compares with it.
    Where the Aurora DSQL table must change too: let CDC catch up (Stream lag: caught
    up), Stop CDC, rename a renamed column on Aurora DSQL (**Fix target schema…** does
    it) and drop a removed one there first (Drop & reload does not destroy a target
    column the new definition no longer has; a primary-key column cannot be dropped on
    Aurora DSQL, so re-apply that table in Schema Conversion with **Replace** instead;
    Drop & reload recreates a column whose type changed with its type from the re-run
    Step 1, unless you edited that table's DDL in Schema Conversion, where the edit wins:
    change the column's type there first, as Aurora DSQL cannot change a column's type in
    place), reload the table on the Full Load step with **Drop & reload**, then Start
    CDC. Run no other DDL on migrated tables while CDC is stopped.
  - **CDC only:** change the Aurora DSQL table by hand where it must follow the source,
    correct the rows CDC wrote since the change (they are not replayed), then re-run
    Step 1.
  - Or undo the change on the source.
  - A **renamed or dropped table** cannot be realigned in place: CDC captures a fixed
    table list, so rename it back on the source, or use **Start over** to migrate it
    under its new name. If you dropped it on purpose, re-run Step 1: it then leaves the
    check, and Cut over asks you to acknowledge it.
  - Aurora DSQL cannot change a **primary key** in place: restore the key on the
    source, or (Full Load + CDC) re-run Step 1 and reload the table with **Drop &
    reload**, which recreates it with the new key; if a key column was renamed or
    removed, first re-apply the table in Schema Conversion with **Replace** (Drop &
    reload refuses to destroy the old key column, and Aurora DSQL cannot drop it); for
    CDC only, use **Start over**.
  - For an **added or renamed column** the notice offers **Fix target schema…** (unless
    the dead-letter banner already offers it for that table). It adds a new column, or
    renames a renamed one so it keeps its values; an added column counts as part of the
    definition from then on, while a renamed one stays reported until you re-run Step 1.
    Re-run Step 1 too, so Validation compares the column. It changes nothing for a key
    change or a renamed primary-key or CDC key column, and it does not add a generated
    column or a column your exclusions keep out; the dialog says why and what to do. For
    a renamed CDC key column (Composite key) the notice offers no Fix and gives the
    recreate steps itself.
  - Choose **Check again** after a change. If the re-read itself fails, or finds none of
    the captured tables (a reconnect to another database or user), the notice says so.
- **Amber — "The source schema changed while CDC ran":** the source matches the Step 1
  inventory again (or a Step 1 re-run changed a table's definition, or a table left the
  inventory), but values CDC wrote meanwhile may differ. Validation compares these
  tables by CHECKSUM automatically; Cut over waits for a match started after that time
  ([§5.7](05-validation.md#57-source-schema-changes-and-cut-over)). A table no longer in
  the Step 1 inventory cannot be compared: acknowledge it on the Cut over step with
  **Continue without this check**.
- **Info — "Can't re-check the source schema":** the source password is kept in memory
  only, so after an app restart the tool cannot connect until you reconnect on the
  Connect step.
- **Amber — "Can't re-check the source schema":** the last read failed, or found none
  of the captured tables; the tool tries again in 5 minutes. Avoid DDL on migrated
  tables meanwhile.

Each change is written to the activity log once — one line for all the tables a read
found changed the same way.

---

**Next:** [5. Validation →](05-validation.md)
