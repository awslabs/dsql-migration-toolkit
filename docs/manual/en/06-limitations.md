# 6. Limitations

_Language: **English** | [한국어](../ko/06-limitations.md) | [日本語](../ja/06-limitations.md)_

> **Prev:** [5. Validation](05-validation.md)

These are the **real, enforced** limits to plan around — most come from Aurora
DSQL itself (it's distributed and intentionally omits features that don't scale
horizontally), a few from how the tool is deployed. None of them are surprises at
runtime: the tool flags them in **Evaluation**, handles them where it can, and
fails **loudly** where it can't.

---

## 6.1 Aurora DSQL feature limits (your schema must fit these)

| Limit | Consequence | Tool behavior |
|---|---|---|
| **Foreign keys (enforced, with runtime caveats)** | DSQL supports **enforced** foreign keys, but DML on referenced/referencing tables incurs **extra reads**, a concurrent conflict is a retryable `40001`, and a `CASCADE`/`SET NULL`/`SET DEFAULT` action counts toward the **3000-row/txn** limit (a cascade touching > 3000 rows fails). | Preserved and re-created as a post-load `ALTER TABLE … ADD CONSTRAINT` (Full Load applies it after the data lands; a CDC migration defers it to cut over). Prefer `NO ACTION`/`RESTRICT` for unbounded child cardinality, or strip them (`preserve_foreign_keys=False`) and enforce integrity in the app. |
| **Source `CHECK` constraints** | A source `CHECK` constraint is not translated to the target (e.g. a MySQL `CHECK`, 8.0.16+). | Dropped from the DDL and flagged **MANUAL** — re-add a DSQL-compatible `CHECK` by hand or enforce it in the app. (The `CHECK … IN (...)` the tool generates for a MySQL `ENUM` or a PostgreSQL enum, both converted to `text`, is unaffected. A value added to the source type later is dead-lettered with `23514` until you replace that `CHECK`; see [§4.2](04-cdc-and-dsql-constraints.md#detecting-and-handling-source-schema-drift).) |
| **Primary key required** | A PK-less table can't be migrated. | Flagged **UNSUPPORTED**; also blocks Full Load (keyset export needs a PK). |
| **No triggers / stored procedures / functions / scheduled events** | Server-side logic doesn't move. | Flagged **UNSUPPORTED** — reimplement in the application (events → EventBridge/Lambda). |
| **No native partitioning** | DSQL auto-distributes data itself. | Partitioned tables flagged **MANUAL** (drop the partitioning). |
| **1 MiB per-column limit** | A single column value over **1 MiB** can't be stored — ANY type (MySQL `TEXT`/`BLOB`/`JSON`, PostgreSQL `text`/`bytea`/`json`): `54000 ERROR: maximum column size exceeded`. A whole **row** is capped at **2 MiB** (`maximum row size exceeded`), and one write transaction at 10 MiB. ([Aurora DSQL quotas](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)) | Oversized values are **quarantined** per row to the error log (Full Load) / **DLQ** (CDC); columns that can exceed the Kafka message cap must be **excluded at capture**. Columns that can hold an oversized value are flagged `OVERSIZED_LOB`: a binary/document column (MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`) as a LOSS, since choosing that type is the intent to store something large; an unbounded PostgreSQL `text`/`varchar` as a RECOMMENDATION, since that is also the idiomatic spelling for an ordinary short string. A column a CHECK limits to a finite set of literals is not flagged at all. |
| **`DECIMAL` precision > 1000** | Higher precision unsupported (DSQL documents maximum precision 1000, scale -1000..1000). | Evaluation flags it **UNSUPPORTED** (`NUMERIC_PRECISION`); if the converted DDL is applied anyway, Schema Conversion **clamps to the maximum** (lossy) with a warning. MySQL caps `DECIMAL` at 65,30, so this is a PostgreSQL-source edge only. |
| **Spatial / geometry types** | For a MySQL source, substituted to `bytea` (raw WKB preserved end-to-end through Full Load and CDC); a PostgreSQL source is **not** auto-substituted. | MySQL source: the converter auto-substitutes each column to `bytea` and the assessor flags the table **MANUAL** for review. PostgreSQL source: geometric types (point/line/lseg/box/path/polygon/circle) are flagged **UNSUPPORTED** at both Evaluation (`PG_UNSUPPORTED_TYPE`) and Schema Conversion, with a suggested remodel to `text` — you must remodel before the DDL applies. (Other PG-only types that need manual remodeling rather than auto-substitution: arrays, network `inet`/`cidr`/`macaddr`, `xml`, `money`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange, composite, pgvector.) |
| **FULLTEXT / SPATIAL indexes** | Not supported. | Flagged **UNSUPPORTED**. |
| **≤ 255 columns per table, ≤ 1000 tables per database** | Beyond these, unsupported. | Flagged **UNSUPPORTED** (`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT`). |
| **≤ 24 indexes per table** (the PK counts, so ≤ 23 secondary; a MySQL source allows 64) | The excess `CREATE INDEX ASYNC` fails **after** Full Load has written every row. | Flagged **MANUAL** (`TOO_MANY_INDEXES`) at planning time, with a matching Schema Conversion note. |
| **≤ 8 columns in a primary key or index** (a MySQL source allows 16) | A wider key fails with error 54011. | Flagged **UNSUPPORTED** for a wide PK (the `CREATE TABLE` is rejected, so nothing loads) or **MANUAL** for a wide index (`TOO_MANY_KEY_COLUMNS`). Conversion **omits** the wide index rather than emitting DDL guaranteed to fail post-load, and names it in the notes. |
| **≤ 1 KiB combined key size** (each PK, each index) | Checked on the **value at `INSERT`/`UPDATE`**, not on the DDL — so only rows whose actual key is too long fail (error 54000 `key size too large`). | Conversion warns (**MANUAL** recommendation) when the *declared* widths could exceed it — counting 4 bytes/char, as a MySQL source's utf8mb4 can use — naming the key and its worst-case size. Never blocks: wide declared types holding short values migrate fine. |
| **One database per cluster** | DSQL organizes by schema, not multiple databases. | For a MySQL source, a multi-database source is flagged **MANUAL** (each MySQL database maps to a DSQL schema — consolidate into schemas, or split clusters). A PostgreSQL source connects to a single database whose non-system schemas already map directly to DSQL schemas (qualified `schema.table`), so there is no multi-database consolidation step. |
| **No `TRUNCATE`; one DDL per transaction; optimistic concurrency** | Different write/DDL semantics than a conventional single-node RDBMS (MySQL or PostgreSQL source). | Handled transparently: DROP+recreate instead of TRUNCATE, single-DDL units, `40001` retry everywhere. |
| **IAM-token auth (no password); short-lived tokens** | No static DB password. | The tool (and the CDC sink) mint and refresh IAM tokens automatically. |

> **Bottom line for schema design:** push large blobs, server-side logic, and
> very-high-precision numerics out of the database and into the application *before*
> you rely on DSQL for them, and keep any cascade fan-out within DSQL's
> per-transaction limit. Evaluation tells you exactly which objects need this.

---

## 6.2 Migration-process limits

- **Single region — no cross-region migration.** The tool works in any region
  where DSQL is available, but **source and target must be in the same region**,
  and the optional CDC pipeline runs in a single region/VPC. Cross-region is not
  supported.
- **CDC has source-side prerequisites (engine-specific).** The prerequisite gate
  **blocks CDC** until they are met; **Full Load alone needs none of this.**
  - **MySQL source:** binary logging on in **ROW format with a full row image**
    (`binlog_format=ROW`, `binlog_row_image=FULL`) and a user with **replication
    privileges**. You must also **raise binlog retention** so the log at the Full
    Load watermark survives until CDC starts (Aurora MySQL keeps binlogs only 24 h
    by default). On RDS/Aurora these are set via parameter groups and
    `mysql.rds_set_configuration`, not `my.cnf` — see
    [§1.1](01-setup.md#11-prerequisites).
  - **PostgreSQL source:** `wal_level=logical` (RDS/Aurora: set the static
    `rds.logical_replication=1` in a custom DB/cluster parameter group, then reboot;
    self-managed: set `wal_level=logical` and restart); a **replication-privileged
    user** (superuser, the `REPLICATION` role attribute, or `rds_replication` membership
    on RDS/Aurora); the source must be the cluster **WRITER**, not a standby/reader
    (`pg_is_in_recovery()=false`); and every replicated table must have a usable
    **`REPLICA IDENTITY`**: `DEFAULT` needs a valid primary key that is not `DEFERRABLE`
    (PostgreSQL keeps `DEFAULT` after the key is dropped, so the check reads the
    source's catalog), `USING INDEX` a valid unique index, and `FULL` always works; on a
    partitioned table every partition needs one. Without one, every `UPDATE`/`DELETE` on
    the table would fail on the publisher once it is published, so the CDC prerequisite
    check fails it and the Full Load stops before it changes anything on the source (no
    publication, no slot). Each refusal names the fix for its cause: a new primary key
    repairs only `DEFAULT`; `NOTHING`, or a `USING INDEX` whose index is gone, changes
    only with `ALTER TABLE … REPLICA IDENTITY` (`FULL`, or `DEFAULT` with a primary key).
    There is no binlog-retention analog: the tool creates a
    **logical replication slot** and a **publication scoped to exactly the migrated
    tables** at the Full Load consistency point, so WAL is pinned by the slot instead of
    by a retention window (watch `wal_status` — an inactive slot can fill the source
    disk).
    Replication-slot / `max_wal_senders` headroom is checked (non-blocking).
  - **PostgreSQL source, re-keyed tables:** a table CDC re-keys — Schema
    Conversion's **Composite key**, or an edited target key that adds a column outside
    the source primary key — needs **`REPLICA IDENTITY FULL` on the table and on every
    partition, for the whole CDC run**, not only at Start (a partition created later
    starts at `DEFAULT`). Below FULL, a `DELETE` on that table can leave its row on
    Aurora DSQL with no error and no dead letter — until Aurora DSQL has built the
    table's unique index on the source primary key, and always on CDC infrastructure
    deployed by an earlier version — and in the same cases an `UPDATE` that changes the
    added key column is dead-lettered and leaves the old row. The prerequisites block
    Start. While the Data Migration, Validation or Cut over step is open, the tool
    re-reads it at most every 2 minutes: a red notice appears in the CDC monitor, and Cut
    over is held until it is FULL again and the table matches by CHECKSUM
    (see [§5.6](05-validation.md#56-re-keyed-postgresql-tables-and-cut-over)). After an
    app restart the re-check needs the source password again (reconnect on the Connect
    step). A drop and its reversal between two re-checks are not seen, so keep FULL in
    place.
- **Primary-key changes and unique values moving between rows.** Debezium replicates an
  `UPDATE` of a primary key as a delete of the old key plus an insert of the new key; the
  two are keyed differently, so they can land on different partitions (any topic with more
  than one: fewer than 8 captured tables, or a large table given 2 or 4) and apply in
  either order. A unique value passed from one row to another (an email address moved to
  another account) can do the same. On a table with a unique index besides its primary
  key, the insert can then meet the old row, which Aurora DSQL rejects at once (`23505`).
  CDC infrastructure deployed by this version (sink plugin v53 or later) holds that change,
  and every later change on its partition, until the other row's change has applied, for
  up to 5 minutes, while other partitions keep flowing; when the partition's next batch
  carries a later change of the same row that replaces it entirely, the change is skipped
  instead. The change is still dead-lettered (*a unique constraint rejected the row*, with
  the index name) when the change that frees the value is more than 5 minutes behind (very
  uneven partition lag during a long catch-up), when an older change of a row replayed
  after a restart or a re-read of the stream (such as the one after Delete and Deploy CDC
  infrastructure) meets values its own later change already wrote and that later change is
  more than one batch further on its partition, when the target already holds the value
  from outside the stream, when a
  PostgreSQL transaction swaps unique values between rows under a `DEFERRABLE` unique
  constraint (each change waits on the other, so both are dead-lettered after 5 minutes),
  and for the composite-key cause of a re-keyed table (below and §4.6). On CDC
  infrastructure deployed by an earlier version the insert is dead-lettered at once and the
  old row's delete then removes the row, so it is missing until the table is reloaded.
  Either way Cut over holds the table until it matches by CHECKSUM, and once it does the
  CDC step shows it as *Reloaded and verified*. Remedy: Stop CDC, reload the table on the
  Full Load step with **Drop & reload**, Start CDC, then validate it. To pick up the wait,
  Delete CDC infrastructure and deploy it again, which also deletes the stream position
  (see the next entry). Both engines.
- **Re-keyed tables on CDC infrastructure deployed by an earlier version** keep their
  change records keyed on the composite key (the Start CDC log says so). An `UPDATE` of
  the leading key column — or a delete and re-insert of a row with another leading
  value — is then replicated as a delete of the old key plus an insert of the new one,
  which can apply in either order when a topic has more than one partition (fewer than
  8 captured tables, or a large table given 2 or 4). If the insert comes first it is
  dead-lettered (*a unique constraint rejected the row*) and the delete removes the row,
  so it is missing until the table is reloaded; Cut over holds the table until it
  matches by CHECKSUM. Delete CDC infrastructure and deploy it again to key these tables
  on the source primary key. That also deletes the stream position: on MySQL, Start CDC
  then re-reads the binlog from the Full Load watermark (CDC only: it begins at the
  source's current position, so changes made after the last one Aurora DSQL applied are
  not replicated); on PostgreSQL it drops the replication slot, so the next Start reads
  every table again (Full load + CDC: choose **"Re-snapshot every table instead"**) and a
  row the source deleted meanwhile stays on Aurora DSQL. See "Starting CDC again after a
  delete" and "The cost of deleting a CDC-only stack" in
  [Chapter 13](13-excluding-tables-and-columns.md#why-the-set-locks). Both engines. (A
  primary-key change on any table with another unique index: the entry above.)
- **CDC replicates data, not DDL.** Schema changes on the source during CDC are
  **not** propagated to DSQL — you make the matching change on Aurora DSQL yourself:
  **Fix target schema…** for an added or renamed column, an `ALTER … DROP NOT NULL`
  for a column that now allows NULL, and for a type or key change a recreate with CDC
  stopped (**Replace** + **Drop & reload**; CDC only has its own repair). Schema
  Conversion's Apply, Replace included, is blocked while CDC streams
  (see [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
  Some of them change what CDC writes without any dead letter (a dropped nullable
  column, a widened type, a renamed table, …), so while the CDC monitor, Validation or
  Cut over step is open the tool re-reads the captured tables' definitions at most every
  5 minutes and holds Cut over on a difference from the Step 1 inventory
  (see [§5.7](05-validation.md#57-source-schema-changes-and-cut-over)). What that
  re-check cannot see:
  - a change undone between two reads, or one made while no CDC monitor, Validation or
    Cut over page is open (it is seen at the next one);
  - a same-shape `DROP TABLE` + `CREATE TABLE` (MySQL has no stable table identity to
    compare), or a PostgreSQL enum gaining a value (`ALTER TYPE … ADD VALUE`);
  - defaults, comments, collations, indexes and generated-column expressions (not
    compared);
  - a change already in the Step 1 inventory when the tool first checked the table (a
    Step 1 re-run before CDC ran becomes the baseline);
  - after an app restart, anything until you reconnect the source on the Connect step
    (the password is kept in memory only);
  - a very large MySQL 5.7 catalog the bounded read cannot finish (8 s on the server,
    10 s on the socket): the CDC monitor and Cut over then report the failed read.
- **PostgreSQL: DDL while CDC is stopped.** Nothing replays the DDL, because `pgoutput`
  carries none. The source schema re-check above reports a changed key or NULL rule at
  its next read, but it cannot tell you that, when CDC continues, the backlog may
  already have been applied with the table's *current* key and NULL rule, as if the
  change were already in force for earlier changes too. Give it the same follow-up on
  Aurora DSQL as a live change, and validate the table with CHECKSUM before Cut over.
- **A source `TRUNCATE` is not applied by CDC.** Aurora DSQL has no `TRUNCATE`, so CDC
  dead-letters it and the CDC step names the table. Cut over is held for that table
  until you remove the extra rows (Drop & reload for Full Load + CDC, a manual delete for
  CDC only) and it matches by CHECKSUM (see
  [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
  On MySQL, `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` produce no change event
  at all; only Validation shows the extra rows. A MySQL pipeline that was already
  streaming when you upgraded from an earlier version of the tool detects `TRUNCATE`
  only after one Stop CDC → Start CDC.
- **MySQL: a schema change between a CDC start point and the Start that uses it.** A
  MySQL Start that seeds a new start position runs Debezium in `snapshot.mode=recovery`:
  the first Start from the Full Load watermark or a Manual binlog start point (a MySQL
  CDC-only first start needs one unless the session holds a Full Load watermark), a Start
  after **Re-run Full Load** or a newer Manual point, and the first Start after **Delete
  CDC infrastructure** and a new deploy — which reads again from the **original**
  watermark. Under `recovery` Debezium rebuilds its schema history from the tables'
  **current** definitions, then reads the binlog from that earlier position, matching
  each row's values to columns by position. A schema change (`ALTER TABLE`, …) to a
  captured table committed in between therefore breaks the stream: an added or dropped
  column stops the connector on every Start, a renamed column stops it too, and a change
  that keeps the column count (a type change other than a length or a DECIMAL precision,
  a reorder, a dropped column replaced by another, a character-set conversion) writes
  shifted or NULL values with **no error at all**.
  - **The check.** The Full Load watermark, and **Fetch current position** for a Manual
    point, record a short per-table fingerprint of the captured tables (column order,
    types, NULL rules, character sets and primary key) just before reading the position.
    Before such a Start, Start CDC compares the tables with it and **refuses**, naming
    each changed table. The check reads the source, so it needs the source connection
    (it refuses until you re-test it on the Connect step). It only **warns** when the
    start point has no fingerprint: one recorded by an earlier version of the tool, a
    position typed by hand, or one whose definitions could not be read. A new length of
    a `CHAR`, `VARCHAR` or `VARBINARY` column, or a new `DECIMAL` precision at the same
    scale, decodes the same and is not refused; the source schema re-check
    ([§4.2](04-cdc-and-dsql-constraints.md#changes-that-dead-letter-nothing-the-source-schema-re-check))
    still reports it, and the Aurora DSQL column may need widening. Widening an integer
    type or appending an `ENUM` value is still refused: the fingerprint cannot tell it
    from a narrowing or a reorder, which do decode wrongly.
  - **What it cannot see.** A column rename on a table CDC does **not** capture
    (Debezium tracks every table's definition, so it fails replaying it), a change made
    while the connectors are being created, or one made before an automatic task
    restart while that run is in `recovery` mode. A connector created in recovery keeps
    that mode until Stop CDC, so this window lasts for the whole run, not just its first
    minutes. A restart after Stop CDC normally resumes in `schema_only` and is not
    affected; if its schema history is missing, that Start rebuilds it in `recovery`
    (see "A restart after Stop CDC is different" below), and the window applies to the
    run it creates. A rename, or an added or dropped column, stops the
    connector, and the CDC step names the cause — in the Start CDC error, or on the
    connector's Pipeline health row if it stops after starting
    ([§4.6](04-cdc-and-dsql-constraints.md#pipeline-health-and-change-flow)). A change
    that keeps the column count does not: the older rows are written with shifted or
    NULL values and no error. The source schema re-check
    ([§4.2](04-cdc-and-dsql-constraints.md#changes-that-dead-letter-nothing-the-source-schema-re-check))
    reports a retype or a replaced column and holds Cut over. Neither check sees a
    reorder or a character-set conversion, so validate the table with CHECKSUM after such
    a change.
  - **Starting again, Retry CDC and Debezium's "recovery mode" never help** — they replay
    the same rows. Move the start point past the change: Stop CDC; refresh the source
    inventory (re-run Step 1 Evaluation) and update the changed tables in Schema
    Conversion, so the reload both recreates and reads the new definition — a table
    whose DDL you edited, or whose key strategy you chose, keeps that edit (redo or reset
    it), and a dropped or renamed column must be dropped on Aurora DSQL first or the
    table re-applied with Replace (Drop & reload refuses to destroy a target column the
    new definition no longer has); then **Re-run Full Load with "Drop & reload"** (the
    default *Append* keeps existing rows, so updates and deletes made in between would
    be lost; *Reload specific tables* and *Retry* keep the original watermark). The new
    run's watermark is past the change, so Start CDC moves the offset forward to it. A
    CDC-only run has no Full Load to re-run: update the changed tables on Aurora DSQL,
    then choose **Manual**, **Fetch current position** and **Use this start point** on
    the same stack (Start never rewinds a stopped stack's offset, but it does advance
    it; on a stopped stack the start-point card offers this under **Manual — start from
    a later binlog position instead**). Changes to the captured tables made before that
    point are then not replicated: reload them with "Drop & reload" (Full load + CDC) or
    correct them on Aurora DSQL (CDC only), and validate with CHECKSUM. A reload
    switches the CDC start point back to Automatic, so choose Manual again (the fetched
    point is kept) before Start CDC.
  - **A restart after Stop CDC is different.** It continues from its recorded position
    and **replays the recorded schema history** (`snapshot.mode=schema_only`), so a DDL
    made while CDC was stopped is applied in order, like a change made while it streams,
    and needs the same follow-up
    ([Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
    If that history is missing, the Start fails saying so. Stop CDC (or **Clean up
    leftover connector**) if the connectors are still listed — a Start or Retry CDC on
    connectors that exist keeps their snapshot mode and fails the same way — and the
    next Start rebuilds it (`recovery`) after the same check. That check compares the
    tables with the original start point, so a change made even before the Stop refuses
    that Start.
- **MySQL with `gtid_mode=ON`: CDC starts after the start point's GTID set.** Debezium
  positions by GTID only when the source's `gtid_mode` is exactly `ON`: it then streams
  every transaction that is not in the GTID set recorded with the start point and
  ignores the start point's binlog `file:position` (with `OFF`, `OFF_PERMISSIVE` or
  `ON_PERMISSIVE` it uses the `file:position`). On an `ON` source the GTID set therefore
  *is* the start point, so Start CDC reads `gtid_mode` first — in the Start dialog and
  again before it changes anything — and refuses a start point it cannot use:
  - **No GTID set.** Debezium would make MySQL send its whole retained binary log again,
    re-applying changes made before the start point. **Fetch current position** records
    the position and the GTID set together; for a position you type, also enter the GTID
    set the source had executed at exactly that position (for example the one your dump
    recorded). A Manual start point saved by version 0.1.565 or earlier comes back
    without its GTID set (those versions kept a fetched set beside any position typed
    after it): fetch it again, or re-enter the set executed at exactly that position.
  - **Recorded while `gtid_mode` was not `ON`** (also a restart after Stop CDC of a stack
    started from such a point, or from a start point without a GTID set, and any start of
    a stack whose last start ran while `gtid_mode` was not `ON`): see the next bullet.
  - **A GTID set the source has not executed** (it was read from another server, or
    entered incorrectly), **or one the source has purged binary logs after**: give CDC a
    new start point — **Re-run Full Load with "Drop & reload"**, or **Manual** → **Fetch
    current position** → **Use this start point** (changes to the captured tables between
    the two points are then not replicated: reload them with "Drop & reload" — CDC only:
    correct them on Aurora DSQL — and validate with CHECKSUM) — raising the binlog
    retention first if it ran out. If checking the GTID set against the source fails
    instead (the connection dropped, a read timed out), nothing is known to be wrong with
    the start point: check that the source is reachable from the tool, then Start CDC
    again.
  - **`gtid_mode` cannot be read** while the start point has no GTID set, or was recorded
    while `gtid_mode` was not `ON`, or while Start CDC has to record it — before every
    start of a stack that holds a `gtid_mode` record, and before a stack's first start
    (this session has no source connection — the password is kept in memory only, so an
    app restart clears it — or the read failed): re-test the source connection on the
    Connect step, then Start CDC again.

  On the Manual card, editing the position after **Fetch current position** drops the
  fetched GTID set (it was read at the fetched position) — also when the position is
  left empty or mistyped — and editing the GTID set drops the recorded table definitions,
  so the start-point check above then only warns. A GTID set you enter must be a MySQL
  GTID set (`<server UUID>:<interval>`, comma-separated, for example
  `3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100`): Start CDC refuses any other text — quotes
  or a `SET` statement copied with it, say — on every `gtid_mode`, because Debezium would
  write it into every offset it commits and stop at its next restart.
- **MySQL: decide `gtid_mode` before the Full Load and keep it until cut over.** A start
  point recorded while `gtid_mode` was not `ON` cannot start CDC once the source is `ON`:
  MySQL cannot send the changes written without a GTID by GTID, so it stops the
  connector on them or skips them with no error. Start CDC refuses that start, and also
  a restart after Stop CDC of a stack started from such a point, or from a start point
  without a GTID set (it streamed by `file:position` and holds no GTID set to continue
  from). Start CDC reads the start offset from the stack's own record of its last
  Start, so this holds even when the session no longer holds the start point (an
  attached stack, a reset session). Keep the start point by setting `gtid_mode` back to
  `ON_PERMISSIVE` until cut over (CDC then streams by `file:position`), or give CDC a new
  start point: **Re-run Full Load with "Drop & reload"**, or **Manual** → **Fetch current
  position** → **Use this start point**, then reload the tables changed in between with
  "Drop & reload" (CDC only: correct them on Aurora DSQL) and validate with CHECKSUM.
  Changing `gtid_mode` while CDC streams takes effect at the connector's next automatic
  restart, can stop it or skip changes with no error, and the tool cannot intercept it:
  Stop CDC first. Start CDC also records on the CDC stack the source's `gtid_mode` each
  time it starts the source connector (Stop CDC keeps the record). On an `ON` source it
  refuses any start of a stack whose last start ran while `gtid_mode` was not `ON` — a
  restart after Stop CDC, and also a new start point, because the offset seeder keeps
  the committed offset whenever the new point is not later than it and Start CDC cannot
  see that offset. That stack streamed by `file:position` from that start, so the GTID
  set in its committed offset stopped advancing there. Keep `gtid_mode` `ON_PERMISSIVE`
  until cut over, or **Delete CDC infrastructure**, deploy it again and Start CDC: a new
  stack has no committed offset, so CDC then starts exactly at its start point — the one
  you hold if `gtid_mode` was already `ON` when it was recorded and has stayed `ON`
  since, else a new start point as above. A stack whose last start ran while `gtid_mode`
  was `ON` continues by GTID, even when the session no longer holds its start point. The
  record cannot see a `gtid_mode` change while CDC streams (Stop CDC first, as above),
  nor a Start that failed and was rolled back (the stack then keeps the record of the
  start before it, so start again before you change `gtid_mode`). A stack last started by
  version 0.1.566 or earlier holds no record until a start reads the mode, and until
  then — that start included — Start CDC judges it only by what is recorded: the mode
  recorded with the start point, and the start offset (with its GTID set) the stack was
  last started from, not how the stack streamed since. So for such a stack a Start after
  `gtid_mode` is back to `ON` is not refused if the stack was started from a point
  recorded while it was `ON` and then streamed while it was not `ON` (for example
  `ON_PERMISSIVE` after a *changes written without a GTID* failure), was started from a
  GTID set typed while it was not `ON`, or was last started with no start point to offer
  (a restart from a session that held none): it continues after the GTID set it holds
  and re-sends changes already applied, or stops on the ones written without a GTID, or
  skips them with no error. Keep such a source not `ON` until cut over, or give CDC a
  new start point as above. Start points recorded by version 0.1.565 or earlier do not
  record the mode: if `gtid_mode` was turned `ON` since, set a new one.
- **MySQL with `gtid_mode=ON`: pipelines started by version 0.1.565 or earlier.** Those
  versions emptied the GTID set Debezium starts from, so on a `gtid_mode=ON` source MySQL
  re-sent its whole retained binary log at every Start, every Start after Stop and every
  automatic connector restart. Writes converged (the sink is idempotent), but older
  values were re-applied, unique-key conflicts could be dead-lettered, and an unparseable
  DDL anywhere in the retained log — even before the start point — stopped every Start. A
  running pipeline keeps that behaviour until one **Stop CDC** → **Start CDC**, which then
  resumes exactly where it stopped. A pipeline started from a Manual point (including CDC
  only) can fail that Start with *the source has purged binary logs CDC's GTID set does
  not cover*: those versions recorded a GTID set without the transactions the source had
  purged at its start, although the source may still hold every change since CDC
  stopped. Stop CDC, set the source's `gtid_mode` to `ON_PERMISSIVE` until cut over (a
  parameter-group change on RDS and Aurora), then Start CDC: Debezium resumes from where
  it stopped by `file:position`, with no gap (if the binlog retention also ran out, that
  Start fails instead). Or give CDC a new start point as the message says. Such a Start
  is refused before anything changes when its Manual point has no GTID set; follow the
  message. If such a Start failed part-way, the topics may hold re-sent older changes
  and the sink may have applied some: **Delete CDC infrastructure**, deploy again,
  **Re-run Full Load with "Drop & reload"** for all tables (CDC only: **Manual** →
  **Fetch current position** → **Use this start point**, then correct the tables on
  Aurora DSQL — rows the sink re-applied keep their older values until rewritten), Start
  CDC, then validate with CHECKSUM. Validate any pipeline that ran on a `gtid_mode=ON`
  source with an earlier version with CHECKSUM before cut over.
- **MySQL: a DDL statement Debezium cannot parse, anywhere on the source server, stops
  CDC.** Debezium parses every DDL statement in the binary log with its MySQL grammar
  (Debezium 2.7) — for every database and table on the server, not only the captured
  ones, including stored routines, triggers, views and account statements — before it
  filters anything, and the tool's configuration stops rather than skip one it cannot
  parse. The source connector stops just before the statement and its position cannot
  move past it: Retry CDC, and Start CDC after Stop CDC, stop on it again. The CDC step
  names the cause — in the Start CDC error, or on the connector's Pipeline health row
  with an error notice once it has started
  ([§4.6](04-cdc-and-dsql-constraints.md#pipeline-health-and-change-flow)) — and shows
  the beginning of the statement with quoted values hidden. Only a statement from the
  start point on stops CDC (version 0.1.565 and earlier also re-read the retained binary
  log from its beginning on a `gtid_mode=ON` source, so an older statement could stop it
  too — see above).
  - **Way forward.** Stop CDC, then give CDC a start point after the statement:
    **Re-run Full Load with "Drop & reload"** for all tables (Full load + CDC; re-run
    Step 1 Evaluation and Schema Conversion first if the statement changed a captured
    table), or **Manual** → **Fetch current position** → **Use this start point**;
    changes made between where CDC stopped and that point are then not replicated, so
    reload those tables with "Drop & reload" (CDC only: correct them on Aurora DSQL) and
    validate with CHECKSUM. A reload switches the start point back to Automatic, so
    choose Manual again before Start CDC. Do not use the *binlog reader near position*
    from the connector log as a start point: it is where the failing statement begins.
  - **When moving the start point does not help.** A Start that builds Debezium's
    schema from the source — the first Start, a Start from a new start point, a Start
    that rebuilds a missing schema history — reads the current definition of every
    table on the server. If a table's own definition uses syntax the grammar does not
    support, every such Start stops while reading it, whatever the start point: change
    that definition on the source, then Stop CDC and Start CDC, or migrate that server
    with Full load only. A statement that runs again (a schema-migration tool, a
    scheduled job) stops CDC each time it runs.
  - **No skip option.** The tool does not offer Debezium's
    `schema.history.internal.skip.unparseable.ddl`: a skipped statement can leave
    Debezium's table definitions silently wrong, so later rows could be written to the
    wrong columns with no error, and the same setting also skips a column rename
    Debezium cannot replay (above), turning that stop into wrong values. PostgreSQL
    sources are not affected: their change stream carries no DDL.
- **Cascading FK actions don't replicate over CDC.** Server-side `ON DELETE/UPDATE
  CASCADE` (and `SET NULL`/`SET DEFAULT`) actions fire *inside* the source engine
  and may not be captured by the change stream, so CDC can't apply them — the child
  rows the source cascaded are left behind on the target (for a MySQL source, InnoDB
  fires them without ever writing them to the binlog). Because the tool re-creates
  the foreign key on DSQL **at
  cut over** (never during replication), those orphaned rows then **block the
  `ADD CONSTRAINT`** and are reported by the **Validation orphan check**, rather
  than silently diverging. Evaluation flags such tables **MANUAL**; replace the
  automatic action with explicit child-row statements in the application, and
  **quiesce source writes before the final cut-over comparison** so the divergence
  is caught.
- **CDC is billable while deployed.** The streaming pipeline (MSK Serverless +
  MSK Connect, plus a NAT gateway if created) costs money for as long as it runs.
  Tear down the cdc-stack after cut-over. Full Load alone provisions no streaming
  infrastructure.
- **A value its target column cannot hold exactly is refused, never converted.** This
  covers:
  - a number other than 0 or 1 for a `boolean` column (a MySQL `TINYINT(1)` holding
    `2`, or, on CDC, a PostgreSQL `boolean` changed to an integer type);
  - more decimal places than a `numeric(p,s)` column keeps;
  - a fraction for an integer column.

  It usually happens when a source type was widened after Step 1, or a narrower type
  was chosen in Schema Conversion. Full Load stops that table (table-fatal). CDC
  dead-letters the row and shows a banner; CDC infrastructure deployed by an earlier
  version of the tool still writes the changed value until it is deleted and deployed
  again. Give the column a type that holds the values in Schema Conversion (re-apply
  with **Replace**) and reload, or clean the source data. A bare `numeric` target
  (Aurora DSQL's default `numeric(18,6)`, warned in Schema Conversion) still rounds a
  PostgreSQL unconstrained `numeric` to 6 decimal places, by design. Not checked:
  `FLOAT`/`DOUBLE` values in a `numeric` column.
- **PostgreSQL 17/18 sources — three version-specific edges (none block a
  migration).** PG 13–18 are supported; 17 and 18 add three things to keep in mind:
  - **`interval` `infinity`/`-infinity` (new in PG17).** Aurora DSQL is
    PostgreSQL-16-compatible and its `interval` input parser rejects these values, so
    a row carrying an *infinite* `interval` is **quarantined** during Full Load
    (visible in the quarantine count) and surfaced by Validation as a count/checksum
    mismatch — never silently altered. Finite intervals and `timestamp`/`timestamptz`
    `infinity` are unaffected. Remodel such rows (e.g. to a finite sentinel) before
    migrating the column.
  - **VIRTUAL generated columns (the DEFAULT kind on PG18).** Aurora DSQL supports
    `GENERATED ALWAYS AS (expr) **STORED**` and maintains the value itself, so a STORED
    column is **preserved** — nothing to recompute and no drift (a write must simply omit
    the column, which Full Load and the CDC sink do; DSQL rejects a supplied value). A
    **VIRTUAL** column is the limitation: DSQL does not accept it, so it is created as an
    ordinary column and flagged **MANUAL/LOSS**. Full Load materializes the value the
    source computed (the target starts correct) but nothing maintains it afterward, and a
    VIRTUAL column cannot be logically replicated at all, so CDC does not carry it.
    Recompute it in the application before cut over. Decide before **apply** either way:
    DSQL can DROP an expression from a column but never ADD one.
  - **CDC connector coverage.** The bundled Debezium PostgreSQL connector's tested
    matrix tops out at PG16. `pgoutput` streaming still works on 17/18 (the logical
    replication protocol is unchanged and the driver connects over the stable wire
    protocol), but treat 17/18 CDC as **best-effort** and validate it end to end
    (Full Load watermark → slot resume → DSQL sink → Validation checksum) before
    relying on it in production. On PG18 also confirm `idle_replication_slot_timeout`
    is `0` (disabled) so a quiet period can't auto-invalidate the CDC slot.

---

## 6.3 Deployment limits (the AWS-hosted form)

- **Single-task control plane.** The tool deploys as **one** ECS Fargate task, and
  you should not raise it above one. On the AWS-hosted deploy both **job and session
  state are kept durable in a managed S3 bucket** (`jobs/` + `sessions/` prefixes),
  so a task replacement (deploy, crash) **resumes** rather than losing progress — a
  resume lands on the last recorded status transition, not the exact in-flight
  batch. The single-task cap is about **single-writer orchestration** (coordinating
  CDC deploys and in-memory session state), **not** state durability; local SQLite
  on ephemeral storage is only the local-dev fallback.
- **No built-in app authentication.** The app relies on the ALB's **optional
  Cognito** gate. Enable Cognito for any internet-facing deployment — the deploy
  template blocks the unsafe internet-facing-without-Cognito combination.
- **Credentials are per-session and in-memory only.** They are never persisted; a
  restart means re-entering source/target connection details.

---

**Next:** [7. Performance and tuning →](07-performance-and-tuning.md)
