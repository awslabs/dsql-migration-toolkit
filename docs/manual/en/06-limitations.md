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
| **No foreign keys** | Referential integrity is your application's job on DSQL. | FK definitions removed from DDL but preserved in the report; flagged **MANUAL**. |
| **Primary key required** | A PK-less table can't be migrated. | Flagged **UNSUPPORTED**; also blocks Full Load (keyset export needs a PK). |
| **No triggers / stored procedures / functions / scheduled events** | Server-side logic doesn't move. | Flagged **UNSUPPORTED** — reimplement in the application (events → EventBridge/Lambda). |
| **No native partitioning** | DSQL auto-distributes data itself. | Partitioned tables flagged **MANUAL** (drop the partitioning). |
| **1 MiB per-value limit** | A single `TEXT`/`BLOB` value over ~1 MiB can't be stored. | 1–8 MiB values **quarantined** to the error log / DLQ; > 8 MiB columns must be **excluded at capture**. Large LOB columns flagged `OVERSIZED_LOB` (MANUAL). |
| **`DECIMAL` precision > 38** | Higher precision unsupported. | Flagged **UNSUPPORTED** (`NUMERIC_PRECISION`). |
| **Spatial / geometry types** | Not supported. | Flagged **UNSUPPORTED**. |
| **FULLTEXT / SPATIAL indexes** | Not supported. | Flagged **UNSUPPORTED**. |
| **≤ 255 columns per table, ≤ 1000 tables per database** | Beyond these, unsupported. | Flagged **UNSUPPORTED** (`TOO_MANY_COLUMNS` / `TABLE_COUNT_LIMIT`). |
| **≤ 24 indexes per table** (the PK counts, so ≤ 23 secondary; MySQL allows 64) | The excess `CREATE INDEX ASYNC` fails **after** Full Load has written every row. | Flagged **MANUAL** (`TOO_MANY_INDEXES`) at planning time, with a matching Schema Conversion note. |
| **≤ 8 columns in a primary key or index** (MySQL allows 16) | A wider key fails with error 54011. | Flagged **UNSUPPORTED** for a wide PK (the `CREATE TABLE` is rejected, so nothing loads) or **MANUAL** for a wide index (`TOO_MANY_KEY_COLUMNS`). Conversion **omits** the wide index rather than emitting DDL guaranteed to fail post-load, and names it in the notes. |
| **One database per cluster** | DSQL organizes by schema, not multiple databases. | A multi-database source is flagged **MANUAL** (consolidate into schemas, or split clusters). |
| **No `TRUNCATE`; one DDL per transaction; optimistic concurrency** | Different write/DDL semantics than MySQL. | Handled transparently: DROP+recreate instead of TRUNCATE, single-DDL units, `40001` retry everywhere. |
| **IAM-token auth (no password); short-lived tokens** | No static DB password. | The tool (and the CDC sink) mint and refresh IAM tokens automatically. |

> **Bottom line for schema design:** push relational integrity, large blobs,
> server-side logic, and very-high-precision numerics out of the database and into
> the application *before* you rely on DSQL for them. Evaluation tells you exactly
> which objects need this.

---

## 6.2 Migration-process limits

- **Single region — no cross-region migration.** The tool works in any region
  where DSQL is available, but **source and target must be in the same region**,
  and the optional CDC pipeline runs in a single region/VPC. Cross-region is not
  supported.
- **CDC has source-side prerequisites (binlog).** CDC requires the source to have
  binary logging on in **ROW format with a full row image** (`binlog_format=ROW`,
  `binlog_row_image=FULL`) and a user with **replication privileges** — the
  prerequisite gate **blocks CDC** until these are met. You must also **raise
  binlog retention** so the log at the Full Load watermark survives until CDC
  starts (Aurora MySQL keeps binlogs only 24 h by default). On RDS/Aurora these are
  set via parameter groups and `mysql.rds_set_configuration`, not `my.cnf` — see
  [§1.1](01-setup.md#11-prerequisites). (Full Load alone needs none of this.)
- **CDC replicates data, not DDL.** Schema changes on the source during CDC are
  **not** propagated to DSQL — you must apply equivalent DDL to DSQL yourself
  (see [Chapter 4 §4.2](04-cdc-and-dsql-constraints.md#42-cdc-replicates-data-not-schema--important)).
- **CDC is billable while deployed.** The streaming pipeline (MSK Serverless +
  MSK Connect, plus a NAT gateway if created) costs money for as long as it runs.
  Tear down the cdc-stack after cut-over. Full Load alone provisions no streaming
  infrastructure.
- **TINYINT(1) out-of-range is table-fatal, by design.** A `TINYINT(1)` value
  outside `{0,1}` stops that table's Full Load loudly rather than silently
  flattening to `true`. Clean the source data (or exclude the column) and re-run.

---

## 6.3 Deployment limits (the AWS-hosted form)

- **Single-task control plane.** The tool deploys as **one** ECS Fargate task. Its
  job/session state is local SQLite on the task's **ephemeral storage**, so a task
  replacement (deploy, crash) loses in-flight job state — you reconnect and re-run
  the read-only steps. Don't scale to more than one task without moving that state
  to a shared store; for zero-loss resume you can point the job/session state
  paths at a shared EFS mount (see [`deploy/DEPLOYMENT.md`](../../../deploy/DEPLOYMENT.md)).
- **No built-in app authentication.** The app relies on the ALB's **optional
  Cognito** gate. Enable Cognito for any internet-facing deployment — the deploy
  template blocks the unsafe internet-facing-without-Cognito combination.
- **Credentials are per-session and in-memory only.** They are never persisted; a
  restart means re-entering source/target connection details.

---

**Next:** [7. Performance and tuning →](07-performance-and-tuning.md)
