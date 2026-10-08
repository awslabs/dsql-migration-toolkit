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
  stops WAL accumulating) — so a restart can still resume gaplessly.
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

Every rejection in the table below, and a target table that no longer exists
(`42P01`), a unique-key conflict (`23505`) or a value the sink cannot bind (`07006`),
gets a banner row the same way, for you to resolve by hand (the tool never drops or
rewrites target data automatically); a source `TRUNCATE` gets its own (below). Any
other dead letter gets no banner row of its own and shows up only in the dead-letter
queue card:

- a rejection with any other SQLSTATE (for example `22P02`, `22007`/`22008`, `22021`,
  `23503` or `54000`), listed with that SQLSTATE;
- a value too large for Aurora DSQL
  ([§4.5](#45-the-1-mib-per-value-limit-and-the-dlq));
- a value the sink cannot convert, for example a PostgreSQL `timetz[]`, `bytea[]` or
  unconstrained `numeric[]` column;
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
only**, correct them on Aurora DSQL by hand (the dead-letter queue card lists each
primary key) or use **Start over**. Other data errors (`22P02`, `22007`/`22008`,
`22021`, …) are listed in the dead-letter queue card with their SQLSTATE only; like
every dead-lettered table, Cut over holds the table until it matches by CHECKSUM.

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
remove rows without any change event, so CDC reports nothing and only Validation shows
the extra rows; repair such a table the same way. A MySQL pipeline that was already
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
   > later, streaming is still lossless but the source connector creates its own
   > publication and slot and **re-snapshots every selected table first** — the data is
   > read a second time. See [Chapter 10 §10.1](10-conclusion.md) for choosing between
   > them. On MySQL the binary log retains history with no consumer, so a later CDC
   > start resumes from the watermark with no re-read.

   If you untick tables after the Full Load (Full load + CDC, before CDC starts; [§13.7](13-excluding-tables-and-columns.md)), CDC captures only the tables still selected and resumes from the same watermark or slot, so the handoff stays gapless for them. On PostgreSQL, when CDC resumes from that slot, the publication still lists the unticked tables (the tool does not remove them on the source) and the connector discards their changes; a Start that re-snapshots instead lets the connector set the publication to the captured tables. A Re-run Full Load over the smaller set reuses the existing publication.

   A table you tick after the Full Load (Full load + CDC, before CDC starts) is loaded first, by Retry unfinished tables after the checks in [§13.7](13-excluding-tables-and-columns.md), and CDC then captures it from the same watermark or slot (a MySQL table the Full Load recorded no start-point definition for needs Re-run all tables instead); Start CDC waits until it is loaded. On MySQL, CDC replays every change made to it since the watermark over the newer copy. On PostgreSQL the table joins CDC's publication just before its copy starts, and CDC streams every change made to it from then on over that copy. Either way it converges; a change that briefly conflicts on a unique index can be dead-lettered, and Cut over then waits for a CHECKSUM validation.
3. **For a MySQL source**, a Start at a **new start point** — the first Start, a
   Start after a newer Full Load or Manual point, or the first Start after Delete CDC
   infrastructure — runs the source connector with **`snapshot.mode=recovery`**:
   because an offset is already seeded, Debezium rebuilds its internal **schema
   history** from the **current source tables** (so it can decode binlog events)
   **without re-reading any row data**, then resumes from the seeded offset. Start
   CDC first checks that the captured tables have not changed since that start point
   ([Chapter 6](06-limitations.md)). A **restart after Stop CDC** runs
   **`snapshot.mode=schema_only`** instead: it replays the recorded schema history
   and applies any DDL in binlog order. Such a restart, like any automatic connector
   restart, continues from the position Debezium recorded — on `gtid_mode=ON`, after
   the GTID set it last committed. **For a PostgreSQL source**, the Debezium
   pgoutput connector has no schema-history topic: on the gapless path it runs with
   **`snapshot.mode=never`** (the logical slot already holds the start LSN and the
   rows are loaded), while a stand-alone / manual start uses **`initial`** (snapshot
   then stream). Debezium PostgreSQL can only resume from the slot's own
   position — it cannot seek an arbitrary WAL LSN.

The result: every change between the snapshot and "now" is applied exactly once.
Because the sink's applies are **idempotent** (keyed by PK), even an overlap or a
retry can't create duplicates.

> **MySQL: the binlog at the watermark must still exist when CDC starts.** This
> handoff only works if the source hasn't purged the binary log at the watermark
> position yet. RDS/Aurora purge binlogs aggressively by default (Aurora MySQL
> keeps them 24 h), and deploying the CDC stack takes ~15–20 min, so **raise binlog
> retention before you start** — see [§1.1](01-setup.md#11-prerequisites). If the
> segment is already gone, CDC can't resume gaplessly and you'd re-run Full Load to
> capture a fresh watermark.
>
> **PostgreSQL: the logical replication slot pins the WAL for you.** From the
> moment the slot is created it holds the required WAL, so nothing is purged out
> from under you — the prerequisite is `wal_level=logical` (RDS/Aurora
> `rds.logical_replication`), not binlog retention. But the slot must stay healthy:
> if it falls too far behind and Postgres reports `wal_status='lost'` (slot
> invalidated), gapless resume is broken and you must re-run Full Load to capture a
> fresh watermark.

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
| **Optimistic concurrency (no locks)** | On `SQLSTATE 40001` it re-runs the conflicting **transaction** with exponential backoff + jitter (up to 10 attempts): one chunk of ≤3000 rows (safe because apply is idempotent), or one row while it applies row by row to isolate a rejected row. |
| **≤ 3000 rows per transaction** | Applies in chunks of ≤ 3000 rows (default batch 3000), one `commit()` per chunk. |
| **No UPDATE-by-statement / replays** | Every change is an **idempotent upsert or delete keyed by PK**: `INSERT ... ON CONFLICT (pk) DO UPDATE` for inserts/updates, `DELETE ... WHERE pk = ?` for deletes (and Kafka tombstones). Re-applying the same event is safe. |
| **Connections drop (idle close / token expiry / worker recycle)** | Detects a dead/half-open connection, reconnects with a fresh token, and **replays the same offsets** (safe because applies are idempotent) rather than dropping records. Connectivity errors are treated as transient and retried, not mistaken for poison rows. |

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
failures (OCC `40001`, connection drops) are **retried**, not dead-lettered. A rejection
a source schema change explains gets a banner row with its repair (§4.2); a per-value data
error with no schema meaning (for example `22P02`) is listed with its SQLSTATE only.
While a table's rows are being dead-lettered because the target lacks a column, a table
or the key, or a column's type changed, the other tables are still applied in batches
(see §4.2); a value-level rejection (a unique or `CHECK` violation, a value too long or
out of range) still makes the rows near it apply one per transaction.

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
- **Fallback:** when the time metric isn't available yet (an older sink plugin, or
  the metric hasn't emitted a datapoint), the monitor falls back to a `MAX(pk)`
  **leading-edge** check and shows `N behind (PK)` (how many PK units the target's
  newest key trails the source's) or `caught up`. This fallback only works for a
  single-column integer PK; the time-based value above is the preferred, general
  measure.

Lag is emitted strictly **best-effort** and never affects replication. Use "Stream
lag → caught up" as the signal that it's safe to proceed to
[cut-over](10-conclusion.md).

### Pipeline health and change flow

Above the per-table table, a **pipeline-health card** shows each connector's state
and a **"change flow"** status line — *Streaming*, *No changes flowing — idle*, or
**"Sink stalled — changes are NOT reaching DSQL"** — with two rec/s gauges (source
poll vs sink send), so you can tell an idle stream from a stuck one at a glance. A
**live stream-lag chart** plots the worst end-to-end lag over time, distinguishing
"caught up" from a confirmed sink stall.

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
  partition starts at `DEFAULT` — then choose **Check again**. Cut over is held.
  Deletes made before the fix are not replayed. If the re-read itself fails, the notice
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
