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
| **Source `CHECK` constraints** | A source `CHECK` constraint is not translated to the target (e.g. a MySQL `CHECK`, 8.0.16+). | Dropped from the DDL and flagged **MANUAL** — re-add a DSQL-compatible `CHECK` by hand or enforce it in the app. (The `CHECK … IN (...)` the tool *generates* for a MySQL `ENUM` is unaffected; for a PostgreSQL source, `ENUM` types are converted to `text`, so no tool-generated `CHECK` is emitted.) |
| **Primary key required** | A PK-less table can't be migrated. | Flagged **UNSUPPORTED**; also blocks Full Load (keyset export needs a PK). |
| **No triggers / stored procedures / functions / scheduled events** | Server-side logic doesn't move. | Flagged **UNSUPPORTED** — reimplement in the application (events → EventBridge/Lambda). |
| **No native partitioning** | DSQL auto-distributes data itself. | Partitioned tables flagged **MANUAL** (drop the partitioning). |
| **1 MiB per-value limit** | A single large text/binary value over ~1 MiB can't be stored (MySQL `TEXT`/`BLOB`, PostgreSQL `text`/`bytea`). | 1–8 MiB values **quarantined** to the error log / DLQ; > 8 MiB columns must be **excluded at capture**. Large LOB columns flagged `OVERSIZED_LOB` (MANUAL). |
| **`DECIMAL` precision > 38** | Higher precision unsupported. | Evaluation flags it **UNSUPPORTED** (`NUMERIC_PRECISION`); if the converted DDL is applied anyway, Schema Conversion **clamps to `numeric(38,37)`** (lossy) with a warning — scale is also capped at 37. |
| **Spatial / geometry types** | For a MySQL source, substituted to `bytea` (raw WKB preserved end-to-end through Full Load and CDC); a PostgreSQL source is **not** auto-substituted. | MySQL source: the converter auto-substitutes each column to `bytea` and the assessor flags the table **MANUAL** for review. PostgreSQL source: geometric types (point/line/lseg/box/path/polygon/circle) are flagged **UNSUPPORTED** at both Evaluation (`PG_UNSUPPORTED_TYPE`) and Schema Conversion, with a suggested remodel to `text` — you must remodel before the DDL applies. (Other PG-only types that need manual remodeling rather than auto-substitution: arrays, network `inet`/`cidr`/`macaddr`, `xml`, `money`, `bit`/`varbit`, `tsvector`/`tsquery`, range/multirange, `enum`, composite, pgvector.) |
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
    user** (superuser, the `REPLICATION` role attribute, or `rds_replication`
    membership on RDS/Aurora); the source must be the cluster **WRITER**, not a
    standby/reader (`pg_is_in_recovery()=false`); and every replicated table must
    have a usable **`REPLICA IDENTITY`** (a PK gives the default, else
    `ALTER TABLE … REPLICA IDENTITY FULL` or an index identity — `NOTHING` is
    refused and errors `UPDATE`/`DELETE` on the publisher). There is no
    binlog-retention analog: the tool creates a **logical replication slot** and a
    **publication scoped to exactly the migrated tables** at the Full Load
    consistency point, so WAL is pinned by the slot instead of by a retention window
    (watch `wal_status` — an inactive slot can fill the source disk).
    Replication-slot / `max_wal_senders` headroom is checked (non-blocking).
- **CDC replicates data, not DDL.** Schema changes on the source during CDC are
  **not** propagated to DSQL — you must apply equivalent DDL to DSQL yourself
  (see [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
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
- **TINYINT(1) out-of-range is table-fatal (MySQL source).** A `TINYINT(1)` value
  outside `{0,1}` stops that table's Full Load loudly rather than silently
  flattening to `true`. Clean the source data (or exclude the column) and re-run.
  A PostgreSQL source has a native `boolean` type, so this MySQL-specific
  `TINYINT(1)`→`boolean` coercion and its table-fatal guard do not apply.
- **PostgreSQL 17/18 sources — three version-specific edges (none block a
  migration).** PG 13–18 are supported; 17 and 18 add three things to keep in mind:
  - **`interval` `infinity`/`-infinity` (new in PG17).** Aurora DSQL is
    PostgreSQL-16-compatible and its `interval` input parser rejects these values, so
    a row carrying an *infinite* `interval` is **quarantined** during Full Load
    (visible in the quarantine count) and surfaced by Validation as a count/checksum
    mismatch — never silently altered. Finite intervals and `timestamp`/`timestamptz`
    `infinity` are unaffected. Remodel such rows (e.g. to a finite sentinel) before
    migrating the column.
  - **VIRTUAL generated columns (the DEFAULT kind on PG18).** DSQL has no generated
    columns, so a `GENERATED ALWAYS AS (expr)` column — STORED **or** VIRTUAL — is
    created as an ordinary column and flagged **MANUAL/LOSS** in Schema Conversion.
    Full Load materializes the computed value (the target starts correct), but nothing
    maintains it afterward and CDC does **not** replicate generated columns (VIRTUAL
    columns cannot be logically replicated at all). Recompute the value in the
    application before cut over.
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
