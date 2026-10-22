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
| **Foreign keys (enforced, with runtime caveats)** | DSQL supports **enforced** foreign keys, but DML on referenced/referencing tables incurs **extra reads**, a concurrent conflict is a retryable `40001`, and a `CASCADE`/`SET NULL`/`SET DEFAULT` action counts toward the **3000-row/txn** limit (a cascade touching > 3000 rows fails). | Preserved and re-created as a post-load `ALTER TABLE … ADD CONSTRAINT … NOT VALID`: added at once, then validated by Aurora DSQL against the rows already loaded, in the background (the **Apply foreign keys** action after a Full Load; at cut over for a CDC migration). Prefer `NO ACTION`/`RESTRICT` for unbounded child cardinality, or strip them (`preserve_foreign_keys=False`) and enforce integrity in the app. |
| **Source `CHECK` constraints** | A source `CHECK` constraint is not translated to the target (e.g. a MySQL `CHECK`, 8.0.16+). | Dropped from the DDL and flagged **MANUAL** — re-add a DSQL-compatible `CHECK` by hand or enforce it in the app. (The `CHECK … IN (...)` the tool generates for a MySQL `ENUM` or a PostgreSQL enum, both converted to `text`, is unaffected. A value added to the source type later is dead-lettered with `23514` until you replace that `CHECK`; see [§4.2](04-cdc-and-dsql-constraints.md#detecting-and-handling-source-schema-drift).) |
| **Primary key required** | A PK-less table can't be migrated. | Flagged **UNSUPPORTED**; also blocks Full Load (keyset export needs a PK). |
| **No triggers / stored procedures / functions / scheduled events** | Server-side logic doesn't move. | Flagged **UNSUPPORTED** — reimplement in the application (events → EventBridge/Lambda). |
| **No row-level security** (PostgreSQL source) | A table's policies (`CREATE POLICY`, `ENABLE`/`FORCE ROW LEVEL SECURITY`) do not exist on Aurora DSQL: after cut over every role that can read or write the table reaches every row. | Flagged **UNSUPPORTED** (`PG_ROW_LEVEL_SECURITY`) with each policy's `USING`/`WITH CHECK` text — enforce the condition in the application. The source is read with `row_security = off` (as `pg_dump` does), so a migration user the policies apply to is refused on that table instead of copying and validating a subset; it has to be a superuser, a role with `BYPASSRLS`, or the table's owner (not for `FORCE ROW LEVEL SECURITY`), which the prerequisite checks verify (`ROW_SECURITY`). |
| **Sequences and identity columns are `bigint`** | Aurora DSQL supports both, but a sequence is always `bigint` with `CACHE` 1 or ≥ 65536, and an identity is allowed only on a `bigint` column (whose type cannot change after apply). | A PostgreSQL sequence no column owns is not created by Schema Conversion: Evaluation lists it **MANUAL** (`PG_STANDALONE_SEQUENCE`) with the `CREATE SEQUENCE` to run, and you move it past the source's final value at cut over (`setval`) — the tool does not sync it. A non-key `serial`/identity column loses its generation (`NON_KEY_SEQUENCE`); to keep it, declare the column `bigint … AS IDENTITY` in the target DDL before applying. |
| **No native partitioning** | DSQL auto-distributes data itself. | Schema Conversion drops the partitioning, and a partitioned table gets a recommendation (what depended on its partitions, and pausing partition maintenance under CDC), not a **MANUAL** gap; nothing on the source is changed. Nor is there table inheritance: a PostgreSQL inheritance (`INHERITS`) parent and each of its child tables migrate as separate tables, each only if selected. The Full Load, Validation and the CDC monitor's `MAX(pk)` check read a parent's own rows only (`FROM ONLY`), as CDC captures them, so a parent whose rows all live in its children (inheritance used as partitioning) loads none, and on Aurora DSQL its table holds only its own rows; a child needs a primary key of its own (PostgreSQL does not inherit one). In every migration type the prerequisite check warns about each such parent (*Inheritance child tables are migrated as tables of their own*: only the parent's own rows, and each child migrates as a table of its own if it is selected), and it fails the parent when this migration's Start CDC would snapshot it (for example every CDC only Start, a re-snapshot or a Manual start point; [§4.3](04-cdc-and-dsql-constraints.md)), and Start CDC refuses that snapshot, as does Deploy CDC infrastructure with CDC only (*CDC cannot snapshot a table that has inheritance child tables*): Debezium's snapshot would copy the children's rows into the parent's table, where CDC never changes them. If the tool cannot read the source at all, Start CDC does not refuse the snapshot but warns that its PostgreSQL checks did not run ([§4.3](04-cdc-and-dsql-constraints.md)). With CDC only, untick the parent (each child still migrates as a table of its own); with Full load + CDC, CDC starts from the Full Load's snapshot point, with no snapshot. |
| **1 MiB per-column limit** | A single column value over **1 MiB** can't be stored — ANY type (MySQL `TEXT`/`BLOB`/`JSON`, PostgreSQL `text`/`bytea`/`json`): `54000 ERROR: maximum column size exceeded`. A whole **row** is capped at **2 MiB** (`maximum row size exceeded`), and one write transaction at 10 MiB. ([Aurora DSQL quotas](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/CHAP_quotas.html)) | Oversized values are **quarantined** per row to the error log (Full Load) / **DLQ** (CDC); columns that can exceed the Kafka message cap must be **excluded at capture**. Columns that can hold an oversized value are flagged `OVERSIZED_LOB`: a binary/document column (MySQL `MEDIUMBLOB`/`LONGBLOB`/`MEDIUMTEXT`/`LONGTEXT`/`JSON`, PostgreSQL `bytea`/`json`/`jsonb`) as a LOSS, since choosing that type is the intent to store something large; an unbounded PostgreSQL `text`/`varchar` as a RECOMMENDATION, since that is also the idiomatic spelling for an ordinary short string. A column a CHECK limits to a finite set of literals is not flagged at all. |
| **`DECIMAL` precision > 1000** | Higher precision unsupported (DSQL documents maximum precision 1000, scale -1000..1000). | Evaluation flags it **UNSUPPORTED** (`NUMERIC_PRECISION`); if the converted DDL is applied anyway, Schema Conversion **clamps to the maximum** (lossy) with a warning. Neither source can exceed it (MySQL caps `DECIMAL` at 65,30, and PostgreSQL's own `numeric` limit is the same 1000), so this is only a safety net. |
| **Spatial / geometry types** | For a MySQL source, substituted to `bytea` (raw WKB preserved end-to-end through Full Load and CDC); a PostgreSQL source's geometric types are re-typed to `text`. | MySQL source: the converter auto-substitutes each column to `bytea` and the assessor flags the table **MANUAL** for review. PostgreSQL source: geometric types (point/line/lseg/box/path/polygon/circle) are re-typed to `text` (their canonical text), like the other PG-only types with a stand-in — arrays → `jsonb`, network `inet`/`cidr`/`macaddr`, `xml`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange → `text`, `money` → `numeric(23,4)`, an enum → `text` with a `CHECK` — each graded **MANUAL** at both Evaluation (`PG_UNSUPPORTED_TYPE`) and Schema Conversion: the data migrates, the application adapts to the new type. A composite type, a domain and pgvector have no stand-in and stay **UNSUPPORTED**: remodel before the DDL applies. |
| **FULLTEXT / SPATIAL indexes** | Not supported. | Not emitted, and flagged **MANUAL** (`UNSUPPORTED_INDEX_TYPE`): the table and its data migrate, the search the index served must move outside the database (e.g. Amazon OpenSearch Service). |
| **≤ 255 columns per table; ≤ 1,000 tables, ≤ 10 schemas, ≤ 5,000 views per database** | Beyond these, the `CREATE` fails (`54011` for columns, `54000` for the database limits). | Flagged **UNSUPPORTED** (`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT` / `SCHEMA_COUNT_LIMIT` / `VIEW_COUNT_LIMIT`). The database-level counts include what the target cluster already holds, and any of them makes the readiness score read **Blocked**. |
| **≤ 24 indexes per table** (the PK counts, so ≤ 23 secondary; a MySQL source allows 64) | The excess `CREATE INDEX ASYNC` fails **after** Full Load has written every row. | Flagged **MANUAL** (`TOO_MANY_INDEXES`) at planning time, with a matching Schema Conversion note. |
| **≤ 8 columns in a primary key or index** (a MySQL source allows 16) | A wider key fails with error 54011. | Flagged **UNSUPPORTED** for a wide PK (the `CREATE TABLE` is rejected, so nothing loads) or **MANUAL** for a wide index (`TOO_MANY_KEY_COLUMNS`). Conversion **omits** the wide index rather than emitting DDL guaranteed to fail post-load, and names it in the notes. |
| **≤ 1 KiB combined key size** (each PK, each index) | Checked on the **value at `INSERT`/`UPDATE`**, not on the DDL — so only rows whose actual key is too long fail (error 54000 `key size too large`). | Conversion warns (**MANUAL** recommendation) when the *declared* widths could exceed it — counting 4 bytes/char, as a MySQL source's utf8mb4 can use — naming the key and its worst-case size. Never blocks: wide declared types holding short values migrate fine. |
| **One database per cluster** | DSQL organizes by schema, not multiple databases. | For a MySQL source, each MySQL database becomes its own DSQL schema automatically (flagged `RECOMMENDED`); more than 10 databases exceed the schema limit above (`SCHEMA_COUNT_LIMIT`). A PostgreSQL source connects to a single database whose non-system schemas already map directly to DSQL schemas (qualified `schema.table`), so there is no multi-database consolidation step. |
| **No `TRUNCATE`; one DDL per transaction; optimistic concurrency** | Different write/DDL semantics than a conventional single-node RDBMS (MySQL or PostgreSQL source). | Handled transparently: DROP+recreate instead of TRUNCATE, single-DDL units, `40001` retry everywhere. |
| **IAM-token auth (no password); short-lived tokens** | No static DB password. | The tool (and the CDC sink) mint and refresh IAM tokens automatically. |

> **Bottom line for schema design:** push large blobs, server-side logic, and
> very-high-precision numerics out of the database and into the application *before*
> you rely on DSQL for them, and keep any cascade fan-out within DSQL's
> per-transaction limit. Evaluation tells you exactly which objects need this.

---

## 6.2 Migration-process limits

- **A new CDC start point needs new CDC topics first.** Wherever this chapter sends you to
  **Re-run Full Load with "Drop & reload"** (or **Re-run all tables**), or to a newer
  **Manual** start point, to give CDC a newer start point, that start point is **refused**
  while this migration's CDC stack has streamed — stopped after a Start, its connectors in
  place, or a Start CloudFormation rolled back. The Full Load step refuses the re-run (Full
  load + CDC), and on a MySQL source Start CDC refuses a start point newer than the one the
  stack last started from, whatever the migration type, so a detour through **Full load
  only** does not get around it. **Delete CDC infrastructure** and deploy it again first,
  then the re-run (or the Manual point), then **Start CDC**
  ([Chapter 3 §3.5](03-full-load.md#35-the-watermark--the-bridge-to-cdc)). Otherwise that
  stack's sink would re-apply changes older than the new start point over the reloaded
  tables, while the source skips the changes between where CDC stopped and the new point.
  Full load only has no CDC step: on a detour through it, switch the migration type first,
  then delete and deploy on the CDC step. On MySQL, after a first Start that
  CloudFormation rolled back, the rollback restores the stack's parameters, so Start CDC
  judges the next Start against the start point that Start offered as the app recorded it
  or, after an app restart, as the stack's CloudFormation events still record it: it
  refuses a later point than that one (a retry from it is not refused), and every start
  point on that stack when neither gives it (for example after a restart on the single EC2
  host, which seeds Kafka in the app).
  On a PostgreSQL source Start CDC has no such refusal: it continues from the stack's own
  replication slot, which a Full load only run leaves as it is, so CDC continues from where
  it stopped if the stack's source connector streamed, gapless only while the CDC topics
  still retain what the sink had not applied, and the reloaded tables converge. Otherwise
  Debezium snapshots every selected table first, and a row deleted on the source between
  the reload and that snapshot stays on the target: Validation reports it as an EXTRA row.
  **Reload specific tables** and **Retry unfinished tables** keep the original start point,
  so this refusal does not apply to them (while CDC streams they wait for Stop CDC, like
  every Full Load). A Manual point is a new start point: the changes between where CDC
  stopped and it are not replicated, on the new stack too, so reload or correct those
  tables and validate with CHECKSUM, as each remedy below says.
- **Single region — no cross-region migration.** The tool works in any region
  where DSQL is available, but **source and target must be in the same region**,
  and the optional CDC pipeline runs in a single region/VPC. Cross-region is not
  supported.
- **CDC has source-side prerequisites (engine-specific).** The prerequisite gate
  **blocks CDC** until they are met; **Full Load alone needs none of this.**
  - **MySQL source:** binary logging on in **ROW format with a full row image**
    (`binlog_format=ROW`, `binlog_row_image=FULL`) and a user with **replication
    privileges** — plus `RELOAD` for a Full load + CDC run unless the source runs
    `gtid_mode=ON`, which Aurora MySQL with enhanced binlog requires for such a run. You
    must also **raise binlog retention** so the log at the Full
    Load watermark survives until CDC starts (Aurora MySQL keeps binlogs only 24 h
    by default). On RDS/Aurora these are set via parameter groups and
    `mysql.rds_set_configuration`, not `my.cnf` — see
    [§1.1](01-setup.md#11-prerequisites).
  - **PostgreSQL source:** `wal_level=logical` (RDS/Aurora: set the static
    `rds.logical_replication=1` in a custom DB/cluster parameter group, then reboot;
    self-managed: set `wal_level=logical` and restart); a **replication-privileged user**
    (superuser, the `REPLICATION` role attribute, or `rds_replication` membership on
    RDS/Aurora); the source must be the cluster **WRITER**, not a standby/reader
    (`pg_is_in_recovery()=false`); and every replicated table must have a usable
    **`REPLICA IDENTITY`**: `DEFAULT` needs a valid primary key that is not `DEFERRABLE`
    (PostgreSQL keeps `DEFAULT` after the key is dropped, so the check reads the source's
    catalog), `USING INDEX` a valid unique index, and `FULL` works for any table except,
    on PostgreSQL 18, one with a generated column: PostgreSQL 18 refuses `UPDATE`/`DELETE`
    on a published table whose identity includes a generated column the publication does
    not publish (CDC's publishes none), so there neither `FULL` nor a key with a generated
    column will do. On a partitioned table every partition needs one. Without one, every
    `UPDATE`/`DELETE` on the table would fail on the publisher once it is published, so
    the CDC prerequisite check fails it and the Full Load stops before it changes anything
    on the source (no publication, no slot). Each
    refusal names the fix for its cause (the prerequisite check also gives the statement,
    with the table's name quoted): a new primary key repairs only `DEFAULT`; a
    `DEFERRABLE` key is fixed by adding it again `NOT DEFERRABLE` (PostgreSQL cannot
    change it in place) or by `FULL`; `NOTHING`, or a `USING INDEX` whose index is gone,
    changes only with `ALTER TABLE … REPLICA IDENTITY` (`FULL`, or `DEFAULT` with a
    primary key); and no fix offers `FULL` for a table PostgreSQL 18 would refuse it on.
    There is no binlog-retention analog: the tool creates a **logical replication slot**
    and a **publication scoped to exactly the migrated tables** at the Full Load
    consistency point, so WAL is pinned by the slot instead of by a retention window
    (watch `wal_status` — an inactive slot can fill the source disk). Replication-slot /
    `max_wal_senders` headroom is checked (non-blocking).
  - **PostgreSQL source, re-keyed tables:** a table CDC re-keys — Schema
    Conversion's **Composite key**, or an edited target key that adds a column outside
    the source primary key — needs **`REPLICA IDENTITY FULL` on the table and on every
    partition, for the whole CDC run**, not only at Start (a partition created later
    starts at `DEFAULT`). Below FULL, a `DELETE` on that table can leave its row on
    Aurora DSQL with no error and no dead letter — until Aurora DSQL has built the
    table's unique index on the source primary key, and always on CDC infrastructure
    deployed by an earlier version — and in the same cases an `UPDATE` that changes the
    added key column is dead-lettered and leaves the old row. The prerequisites block
    Start. On PostgreSQL 18 a re-keyed table with a generated column cannot keep FULL
    (its `UPDATE`/`DELETE` would fail on the source, see above), so the prerequisites, the
    Full Load and the load of a table selected after the Full Load refuse it before any
    write: set it back to **Keep source PK** in Schema Conversion (for such a table the CDC
    monitor's notice, below, names that fix instead of FULL: then reload the table, or, with
    CDC only, migrate it again with **Start over**). While the Data
    Migration, Validation or Cut over step is open, the tool
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
  CDC infrastructure deployed by this version (sink plugin v53 or later) holds that
  change, and every later change on its partition, until the other row's change has
  applied, for up to 5 minutes (counted again from a later transient failure of the sink
  task that holds it, so an Aurora DSQL outage that task sees does not use them up; one
  that only delays the other row's change on another sink task can), while other
  partitions keep flowing and its table's Stream lag shows the wait. The 5 minutes apply
  to each held change, so conflicts on one partition are waited out one after another:
  several of them during a catch-up delay that partition by about 5 minutes each. On a
  table that is not re-keyed, when the same batch or the partition's next batch carries
  a later change of the same row that replaces it entirely, the change is skipped
  instead. The change is still
  dead-lettered (*a unique constraint rejected the row*, with the index name) when the
  change that frees the value is more than 5 minutes behind (very uneven partition lag
  during a long catch-up), when an older change of a row replayed after a restart or a
  re-read of the stream (such as the one after Delete and Deploy CDC infrastructure) meets
  values its own later change already wrote and that later change is more than one batch
  further on its partition, when the target already holds the value from outside the
  stream, when the change needs a value that a skipped change of another row would have
  freed and that row's replacing change is queued behind it on its partition, or itself
  waits for a value the change's own row holds (then both are dead-lettered; a unique
  value passed along several rows, such as X changing a to b while b is still taken, Z
  then changing z to a and X changing again: X's first change is skipped, and X keeps a
  on the target until its last change applies), when a PostgreSQL transaction swaps
  unique values between rows under a `DEFERRABLE` unique constraint (each change waits on
  the other, so both are dead-lettered, after 5 minutes, or about 10 when the two share a
  partition), on a
  re-keyed (Composite key) table when the row's own later change already made the conflict
  moot (a primary-key change and its reversal, a replayed older image, the re-run of an
  `XX000` commit that landed: nothing is skipped there, so it waits the 5 minutes and is
  dead-lettered, though the target ends up right), and for the composite-key cause of a
  re-keyed table (below and §4.6). On CDC infrastructure deployed by an earlier version
  the insert is dead-lettered at once and the old row's delete then removes the row, so it
  is missing until the table is reloaded. Either way Cut over holds the table until it
  matches by CHECKSUM, and once it does the CDC step shows it as *Verified by CHECKSUM*;
  if an open source-schema or REPLICA IDENTITY finding still holds it at Cut over, that
  notice names the finding, which keeps its own fix (no reload is needed for the dead
  letters). Remedy: Stop
  CDC, reload the table on the Full Load step with **Drop & reload** (CDC only: correct
  the rows on Aurora DSQL by hand), Start CDC, then validate it. A source that dropped or
  relaxed the `UNIQUE` index gives the same dead letter (CDC replicates no DDL): drop the
  index on Aurora DSQL too, and with Full load + CDC re-run Step 1 (Evaluation) first, so
  the reload does not rebuild it. To pick up the wait, Delete CDC infrastructure and
  deploy it again, which also deletes the stream position (see the next entry). Both
  engines.
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
  every table again (Full load + CDC: choose **"Re-snapshot every table instead"** when
  the Start CDC dialog offers it) and a row the source deleted meanwhile stays on Aurora
  DSQL. See "Starting CDC again after a delete" and "The cost of deleting a CDC-only
  stack" in
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
  On MySQL, `ALTER TABLE … TRUNCATE PARTITION` / `DROP PARTITION` — and on PostgreSQL,
  dropping, detaching or truncating a partition, or attaching one that already holds rows —
  produce no change event at all; only Validation shows the extra (or missing) rows. Pause partition maintenance until cut over. A MySQL pipeline that was already
  streaming when you upgraded from an earlier version of the tool detects `TRUNCATE`
  only after one Stop CDC → Start CDC.
- **MySQL: a schema change between a CDC start point and the Start that uses it.** A
  MySQL Start that seeds a new start position runs Debezium in `snapshot.mode=recovery`:
  the first Start from the Full Load watermark or a Manual binlog start point (a MySQL
  CDC-only first start needs one unless the session holds a Full Load watermark), and the
  first Start after **Delete CDC infrastructure** and a new deploy — which reads again from
  the **original** watermark. On a stack that has streamed, the new start point of a
  **Re-run Full Load**, or a newer Manual point, is refused until the infrastructure is
  deployed again (§6.2, first entry). Under `recovery` Debezium rebuilds its schema
  history from the tables'
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
    the same rows. Move the start point past the change. Stop CDC. On a stack that has
    streamed (a restart after Stop CDC, or a connector that stopped mid-run), **Delete CDC
    infrastructure** and deploy it again first: you get new, empty topics, and until then
    the Full Load step refuses a new start point and Start CDC refuses a newer one.
    Refresh the source inventory (re-run Step 1 Evaluation) and update the changed tables
    in Schema Conversion, so the reload both recreates and reads the new definition — a
    table whose DDL you edited, or whose key strategy you chose, keeps that edit (redo or
    reset it), and a dropped or renamed column must be dropped on Aurora DSQL first or the
    table re-applied with Replace (Drop & reload refuses to destroy a target column the
    new definition no longer has); then **Re-run Full Load with "Drop & reload"** (the
    default *Append* keeps existing rows, so updates and deletes made in between would
    be lost; *Reload specific tables* and *Retry* keep the original watermark). The new
    run's watermark is past the change, and Start CDC on the new stack (or the stack's
    first Start) starts from it. A CDC-only run has no Full Load to re-run: update the
    changed tables on Aurora DSQL, then — after Delete CDC infrastructure and a new deploy
    if the stack has streamed — choose **Manual**, **Fetch current position** and **Use
    this start point**, then Start CDC. Changes to the captured tables made before that
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
  again before it changes anything (a refusal there fails the Start with *Start CDC
  blocked: …*, and the CDC step shows the whole notice, with its fix, under the failed
  Start; the activity log records it whole too, one longer than a log line as
  consecutive `(1/2) …`, `(2/2) …` lines) — and refuses a start point it cannot use:
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
  start point — for a restart after Stop CDC (a stack that has streamed), after **Delete
  CDC infrastructure** and a new deploy first: **Re-run Full Load with "Drop & reload"**,
  or **Manual** → **Fetch current position** → **Use this start point**, then reload the
  tables changed in between with "Drop & reload" (CDC only: correct them on Aurora DSQL),
  Start CDC and validate with CHECKSUM.
  Changing `gtid_mode` while CDC streams takes effect at the connector's next automatic
  restart, can stop it or skip changes with no error, and the tool cannot intercept it:
  Stop CDC first. Start CDC also records on the CDC stack the source's `gtid_mode` each
  time it starts the source connector (Stop CDC keeps the record). On an `ON` source it
  refuses any start of a stack whose last start ran while `gtid_mode` was not `ON` — a
  restart after Stop CDC, and also a new start point, because Start CDC does not take a
  start point newer than where the stack last started (§6.2, first entry) and the offset
  seeder keeps the committed offset whenever the new point is not later than it. That
  stack streamed by `file:position` from that start, so the GTID set in its committed
  offset stopped advancing there. For a restart, keep `gtid_mode` `ON_PERMISSIVE` until
  cut over; or **Delete CDC infrastructure**, deploy it again and Start CDC: a new
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
  new start point on a new stack (Delete CDC infrastructure and deploy it again first),
  as above. Start points recorded by version 0.1.565 or earlier do not record the mode:
  if `gtid_mode` was turned `ON` since, set a new one (on a stack that has streamed,
  after Delete CDC infrastructure and a new deploy).
- **MySQL with `gtid_mode=ON`: pipelines started by version 0.1.565 or earlier.** Those
  versions emptied the GTID set Debezium starts from, so on a `gtid_mode=ON` source MySQL
  re-sent its whole retained binary log at every Start, every Start after Stop and every
  automatic connector restart. Writes converged (the sink is idempotent), but older
  values were re-applied, unique-key conflicts could be dead-lettered, and an unparseable
  DDL anywhere in the retained log — even before the start point — stopped every Start. A
  running pipeline keeps that behaviour until one **Stop CDC** → **Start CDC**, which then
  continues from where it stopped: the changes the sink had not applied at the Stop come
  from the CDC topics, so this is gapless only while the topics still retain them
  ([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)). A
  pipeline started from a Manual point (including CDC
  only) can fail that Start with *the source has purged binary logs CDC's GTID set does
  not cover*: those versions recorded a GTID set without the transactions the source had
  purged at its start, although the source may still hold every change since CDC
  stopped. Stop CDC, set the source's `gtid_mode` to `ON_PERMISSIVE` until cut over (a
  parameter-group change on RDS and Aurora), then Start CDC: Debezium resumes from where
  it stopped by `file:position`, with no gap while the CDC topics still retain the
  changes the sink had not applied when CDC stopped (their retention period on MSK); if
  the binlog retention also ran out, that Start fails instead. Or give CDC a new start
  point, as the message says: this stack has streamed, so first **Delete CDC
  infrastructure** and deploy it again, then **Re-run Full Load with "Drop & reload"** for
  all tables (CDC only: **Manual** → **Fetch current position** → **Use this start
  point**, then correct the tables changed in between on Aurora DSQL), then Start CDC and
  validate with CHECKSUM. Such a Start
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
  - **Way forward.** Stop CDC, then **Delete CDC infrastructure** and deploy it again: the
    stack has streamed, so a new start point needs new, empty topics (until then the Full
    Load step refuses a new start point, and Start CDC a newer one). Then give CDC a start
    point after the statement: **Re-run Full Load with "Drop & reload"** for all tables
    (Full load + CDC; re-run Step 1 Evaluation and Schema Conversion first if the
    statement changed a captured table), or **Manual** → **Fetch current position** →
    **Use this start point**; changes made between where CDC stopped and that point are
    then not replicated, so reload those tables with "Drop & reload" (CDC only: correct
    them on Aurora DSQL) and validate with CHECKSUM. A reload switches the start point back
    to Automatic, so choose Manual again before you **Start CDC** on the new stack. Do not
    use the *binlog reader near position* from the connector log as a start point: it is
    where the failing statement begins.
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
  fires them without ever writing them to the binlog). The tool re-creates the
  foreign key on DSQL **at cut over** (never during replication): it is still added at
  once (`NOT VALID`), but Aurora DSQL's **validation of the loaded rows fails** on those
  orphaned rows — the cut-over screen names the key, counts the rows and lists them,
  and for a MySQL source **I've cut over** waits until the key validates or you choose
  to cut over without that check — and the **Validation orphan check** reports them
  too, rather than letting them silently diverge. Evaluation flags such tables
  **MANUAL**; replace the automatic action with explicit child-row statements in the
  application, and **quiesce source writes before the final cut-over comparison** so the
  divergence is caught.
- **A foreign key Aurora DSQL could not validate stays `NOT VALID`.** It is in place and
  enforced for every new write: an insert, or an update that changes the key, naming a
  missing parent is refused (`23503`), and so is deleting or re-keying a parent row that
  is still referenced (`NO ACTION` / `RESTRICT`; `CASCADE` and `SET NULL` act as usual).
  The rows already there are not checked again: updating a non-key column of such a row,
  deleting it, or pointing it at an existing parent works — re-saving it as a delete plus
  a re-insert does not, like any new orphan. The key is not proven until the rows are
  fixed (in batches of at most 3,000 rows per transaction) and **Validate again**
  succeeds.
- **One CDC pipeline captures a bounded number of tables: about 80–160 with typical names.**
  The CDC stack receives the captured tables in single CloudFormation parameters: the table
  list (`TableIncludeList`), the sink's topic list (`SinkTopics`, one `dsqlcdc.<table>` per
  table) and, when the deploy sizes partitions by table size (once one table holds at least
  twice the average row count), the per-topic partition map (`SinkTopicPartitions`, one
  `dsqlcdc.<table>:<count>` per table) and the lists of the tables given more partitions.
  CloudFormation limits a parameter value to 4,096 bytes, so one pipeline captures at most
  about 4,097 ÷ (L + 11) tables, where L is the average length in bytes of a qualified table
  name (`database.table` on MySQL, `schema.table` on PostgreSQL): about 155 tables with
  15-byte names, 130 with 20, 100 with 30 and 80 with 40 (about 4,097 ÷ (L + 9) when no
  table holds twice the average row count, since the deploy then sends no partition map).
  Evaluation allows 1,000 tables, and Full load only has no such limit.
  - **Deploy CDC infrastructure and Start CDC refuse a larger capture** before anything is
    uploaded or created (*Too many tables for one CDC pipeline*, naming the parameter and
    its size). The deploy judges its partition map on the row estimates the Full Load
    recorded or, without them (CDC only, or a deploy before the Full Load), on the source's
    own estimates, which it reads when you click it. Until then the deploy form shows what
    the map would need at worst, as a warning (*Possibly too many tables for one CDC
    pipeline*). Start CDC on a deployed stack judges only the two lists it sends.
  - **With Full load + CDC, the Full Load is not held.** Before the load, the Full Load step
    warns (*Too many tables for one CDC pipeline*, or *Possibly too many tables for one CDC
    pipeline* when only the partition map at its worst would not fit, as on the deploy form)
    and says about how many of the selected tables one pipeline captures. You can still load
    every table and then, before CDC starts, untick the tables CDC does not need to capture
    (see "Changing the table set after a Full Load (Full load + CDC)" in
    [Chapter 13](13-excluding-tables-and-columns.md)): they stay on Aurora DSQL as loaded, and
    CDC and Validation skip them. A MySQL table the load reads in parts (*Reader shards per
    large table* above 1, [§3.8](03-full-load.md#38-multi-process-parallelism-gil-bypass)) is
    the exception: it cannot be unticked until it is reloaded with one reader (Drop &
    reload).
  - Otherwise capture fewer tables in this CDC pipeline (deselect them in the table
    picker), or use Full load only.
- **MySQL: a table read in parts is consistent only once CDC replays it.** With *Reader
  shards per large table* above 1, a Full load + CDC load splits a large MySQL table's read
  into primary-key ranges, each from its own snapshot
  ([§3.8](03-full-load.md#38-multi-process-parallelism-gil-bypass)). Until CDC, started from
  the Full Load's start point, has replayed the table, a row whose key changed during the read
  can be on Aurora DSQL twice or not at all, and a transaction across two ranges half applied,
  with equal row counts: a ROW_COUNT validation does not see it, a CHECKSUM does. So every
  route that would leave such a table without that replay (unticking it, Full load only, a
  Start CDC from a later start point) is refused until the table is reloaded with one reader
  (Drop & reload: an Append keeps those rows); Full load only and the later start point are
  also allowed once a Validation in CHECKSUM mode matches it. Unticking needs the reload
  before CDC starts: from CDC's first Start the table set is fixed. A Start CDC, even one
  that streamed, does not end the refusals; after one that never streamed, start CDC again
  from Automatic.
  A table whose unique key does not include the leading key column, and every table of a Full
  load only run, keeps one reader.
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
- **Dates and times before 1582-10-15, and MySQL years 0–99, on CDC infrastructure
  deployed by an earlier version.** Full Load stores a `DATETIME`, `timestamp`,
  `timestamptz`, `DATE` or `date` as it is, including MySQL's documented minimum
  `1000-01-01` and the `0001-01-01` that .NET's `DateTime.MinValue` and Go's zero
  `time.Time` write; so does CDC from sink plugin v54. An earlier sink wrote a `DATETIME`,
  `timestamp` or `timestamptz` before 1582-10-15 days off, in the Julian calendar
  (`1000-01-01` as `0999-12-27`, `0001-01-01` as `0001-01-03`), and a `DATE` or `date` in
  the ten days the Gregorian reform skipped, 1582-10-05 to 1582-10-14, ten days later
  (`1582-10-10` as `1582-10-20`); an earlier MySQL source read a year 0–99 of a `DATETIME`
  or `DATE` as 2000–2069 or 1970–1999 (`0001-01-01` as `2001-01-01`). Nothing was
  dead-lettered and ROW_COUNT still matches, so only a CHECKSUM shows it; on a key column
  the update inserted a second row and the delete matched nothing.
  **Delete CDC infrastructure**, deploy it again, then reload such tables (Drop & reload)
  and validate with CHECKSUM. On a PostgreSQL source, though, the rows a Debezium snapshot
  copies (every CDC only Start, a re-snapshot or a Manual start point;
  [§4.3](04-cdc-and-dsql-constraints.md)) are read through JDBC before they reach the sink, so
  with any sink plugin a `date` or `timestamp` from 1582-10-05 to 1582-10-14 lands ten days
  later and a BC one loses its era (`0044-03-15 BC` as `0044-03-15`), and a `timestamptz`
  before 1582-10-15 lands days off, read in the Julian calendar (`1000-01-01` five days later,
  `0001-01-01` two days earlier). Migrate such tables with **Full load + CDC** (its slot
  hand-off takes no snapshot) and validate them with CHECKSUM.
- **PostgreSQL 17/18 sources — three version-specific edges (none block a
  migration).** PG 13–18 are supported; 17 and 18 add three things to keep in mind:
  - **`interval` `infinity`/`-infinity` (new in PG17).** Aurora DSQL is
    PostgreSQL-16-compatible and its `interval` input parser rejects these values, so
    a row carrying an *infinite* `interval` is **quarantined** during Full Load
    (visible in the quarantine count) and surfaced by Validation as a count/checksum
    mismatch. CDC, however, carries it as a **zero** interval (`00:00:00`), silently:
    Debezium's driver reads `infinity` as 0, and only a CHECKSUM validation shows it.
    Remodel infinite-`interval` rows (e.g. to a finite sentinel) before migrating the
    column or starting CDC. Finite intervals are unaffected, and so is
    `date`/`timestamp`/`timestamptz` `infinity`/`-infinity`: Full Load and CDC (sink
    plugin v54 and later, streamed or snapshot) both store it as `infinity`. An earlier
    plugin stopped its sink task at the first such `timestamptz` value, stored a
    `timestamp` `+infinity` as the year 294247, and dead-lettered `-infinity` and a date
    infinity: **Delete CDC infrastructure**, deploy it again, then reload such tables
    (Drop & reload, or the re-snapshot the next Start offers) and validate with CHECKSUM,
    even if no task stopped.
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
