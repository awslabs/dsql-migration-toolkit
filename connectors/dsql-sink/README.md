# Custom DSQL Sink Connector

> **STATUS: implemented + offline unit-tested + plugin jar builds.**
> The connector logic is complete and verified at the unit/build level
> (`mvn test` → the offline unit tests pass; `mvn package` → shaded MSK Connect plugin jar).
> Before a production deploy, validate the runtime characteristics on a live
> MSK Connect + Aurora DSQL environment: IAM token rotation, OCC `40001`
> throughput under contention, ≤3,000-row batching / 1-hour reconnect, and
> effectively-once delivery, deploying to an account via `deploy/cdc-stack`.

## Why a custom connector (and why Java)

A stock managed JDBC sink cannot authenticate with DSQL's short-lived IAM tokens
and has no DSQL-specific handling of OCC conflicts (`SQLSTATE 40001`) or of a row
DSQL rejects (this connector sends it to the DLQ while the rest of its chunk still
lands). This connector applies Debezium change events to Aurora DSQL while
handling DSQL's constraints directly:

- **IAM short-lived tokens (~15 min)** — auto-refresh + reconnect before the
  1-hour connection timeout (`DsqlIamTokenProvider`).
- **OCC `40001`** — the conflicting **transaction** (one chunk of ≤3,000
  changes, or one row in the record-by-record fallback) is re-run with
  exponential backoff + full jitter (`OccRetry`), mirroring the Python
  `core/occ.py` policy. Each re-run starts on a clean transaction: what the
  failed attempt wrote is rolled back first, because Aurora DSQL can report
  `40001` while a statement runs (`OC001`: a schema change elsewhere on the
  cluster, such as an asynchronous index build finishing), not only at commit.
- **Server unavailable `XX000`** — Aurora DSQL's temporary service condition is
  retried exactly like a `40001` (`OccRetry.retriesInProcess`: the same
  `occ.max.retries` budget, the failed attempt rolled back first, backoff +
  jitter, the same session). If it outlasts the budget, the task drops its
  connection and throws a `RetriableException`, so Kafka Connect redelivers the
  batch: it is never dead-lettered. A COMMIT that fails with `XX000` may have
  committed; the PK-keyed upsert/delete re-run is idempotent per key, though a
  secondary `UNIQUE` index can reject (`23505`) an image the landed chunk already
  replaced: that image is skipped when a later change of the same row in the
  batch replaces it, and otherwise held like any unique conflict (below). The exception Connect logs on a redelivery carries the SQLSTATE and
  exception classes only (pgjdbc renders a failed batch entry with its bound
  values).
- **Unique conflict `23505` (plugin v53)** — Debezium replicates an `UPDATE` of
  the primary key as a delete of the old key and an insert of the new key; the
  two are keyed differently, so they can land on different partitions and apply
  in either order, and Aurora DSQL checks a unique index at once (only foreign
  keys can be deferred). So an insert or update a unique index besides the
  primary key rejects is not dead-lettered at once: on a table that is not
  re-keyed it is skipped when a later change of the same row on the same
  partition in the batch replaces it entirely (a plain delete, or an upsert of
  the same column set), and otherwise its partition is held — the task writes
  nothing at or after it on that partition (the tail is fetched again); on a
  table that is not re-keyed it first reads the partition's next batch without
  applying it, and a later change of the same row there that replaces it the
  same way makes it moot (the partition is rewound past it: a primary-key change
  and its reversal, or an older image replayed after a restart); otherwise it
  asks Connect to rewind the partition to it (`SinkTaskContext.offset`) and pause it,
  caps the committed offset at it (`preCommit`), and tries it alone at the end of
  each put (500 ms, doubling to 10 s, with `context.timeout` bounding an idle
  poll) while every other partition keeps applying. Once it applies, the
  partition is rewound past it and resumed; a conflict that outlasts 5 minutes
  from its first rejection (a value the target already holds, a freeing
  change further behind, or a replayed older image whose newer image is more
  than one batch behind on its own partition) is dead-lettered with its real reason. A rebalance
  drops the hold after its capped commit. Not held: deletes, `delete.enabled=false`,
  and the composite-key cause of a re-keyed table (an `UPDATE` of its leading
  key column whose old key is unknown). The cool-down is never armed by it.
  Plugin v52 and earlier dead-letter the change at once.
- **≤ 3,000 rows / transaction** — small idempotent batches (`Batches` +
  `DsqlSinkTask`).
- **PK-keyed idempotent upsert/delete** — `INSERT ... ON CONFLICT (pk) DO
  UPDATE` for Debezium `c/r/u`, `DELETE` for `d` and tombstones (`DsqlDialect`).

**Java is a consequence of the runtime, not a preference.** The CDC runtime is
managed **MSK Connect** = managed **Apache Kafka Connect**, whose connector
plugins are JVM (`SinkConnector`/`SinkTask`) classes packaged as JVM jars. There
is no Python connector plugin on this runtime, so choosing the managed Kafka
Connect runtime dictates a JVM/Java connector. The token-generation, OCC-retry,
and DSQL-dialect logic is
therefore mirrored from the Python `core/` into this Java subproject — a small,
bounded cross-language duplication that is the price of the managed runtime.

## Relationship to the Python control plane

The Python tool (control plane) builds this connector's config
(`CdcPipelineOrchestrator.build_sink_config`), seeds the Debezium start offset
from the Full Load watermark (gapless), monitors status, and surfaces DLQ/task
errors — it does **not** run this connector in-process. This connector is the
**data plane**, run by managed MSK Connect.

## File map

| File | Purpose |
|---|---|
| `pom.xml` | Maven build; Kafka Connect (provided) + PostgreSQL JDBC + AWS SDK v2 `dsql` + JUnit 5; shaded uber-jar for the MSK Connect custom plugin |
| `DsqlSinkConnector.java` | `SinkConnector` entrypoint; distributes config to tasks |
| `DsqlSinkConnectorConfig.java` | Connector config: `dsql.cluster.endpoint`/`dsql.region`/`dsql.database`/`dsql.username`/`delete.enabled`/`batch.size`/`occ.*`/`metrics.*`/`target.key.columns` (re-keyed tables keyed on the source primary key, with the composite key the sink writes each by — the cdc-stack `SinkKeyColumns` parameter; absent = none) |
| `DsqlSinkTask.java` | `SinkTask`: parse → batch (≤3,000) → OCC/`XX000`-retried upsert/delete → commit; IAM-token reconnect; a unique conflict (`23505`) held on its partition for up to 5 min until the earlier change that frees the value applies; failures re-thrown for the Connect DLQ |
| `DebeziumEvents.java` | Maps a Debezium envelope `SinkRecord` to a `ChangeEvent` (record key = PK; `c/r/u`→upsert, `d`/tombstone→delete). A table listed in `target.key.columns` whose record is keyed on the SOURCE primary key is written by its composite key instead: an `UPDATE` of a composite-key column becomes delete-old + insert-new in one transaction, a delete uses the exact composite key of its before-image (or the source key once its unique index is valid), and its tombstone is skipped |
| `ChangeEvent.java` | Parsed change event (table, delete flag, columns/values, pk columns/values) |
| `DsqlDialect.java` | Pure DSQL SQL builders: identifier quoting, `INSERT ... ON CONFLICT` upsert, `DELETE` |
| `Batches.java` | Partitions events into ≤`batch.size` chunks (rows weighted: a move counts 2) |
| `SourceKeyIndex.java` | Per-task cache of whether a re-keyed table's unique index on its source key is valid (one `pg_index` read per table; 10 min when valid, 60 s otherwise), so a delete is keyed on the source key only when that is a single-row lookup |
| `OccRetry.java` | Re-runs a transaction (a chunk, or one row) that failed with a `40001` conflict or Aurora DSQL's `XX000` server unavailable, with backoff + jitter (mirrors `core/occ.py`), rolling back the failed attempt first; injectable sleeper/jitter for tests |
| `DsqlIamTokenProvider.java` | DSQL IAM token cache + refresh policy (mirrors `core/target_connection.py`); injectable token source + clock for tests |
| `AwsDsqlTokenSource.java` | The only class touching the AWS SDK: `DsqlUtilities` admin/non-admin token generation |
| `src/test/java/...` | JUnit 5 tests: dialect SQL, OCC retry (incl. a `40001` or `XX000` raised while a statement runs or at COMMIT, against a double with PostgreSQL's aborted-transaction semantics; a COMMIT that landed re-runs to the same rows for every write shape), the unique-conflict hold (`DsqlSinkUniqueHoldTest`: a primary-key change across partitions, per-key order, a genuine conflict's dead letter, redelivery, rebalance), batch partition, Debezium parse, token refresh |

## Build and test

```bash
# Requires JDK 17+ and Maven.
mvn -B -ntp test       # runs the offline unit tests (no AWS/DSQL needed)
mvn -B -ntp package    # produces target/dsql-sink-connector-*.jar (shaded)
```

The shaded jar is the MSK Connect custom plugin artifact (uploaded to S3 by
`deploy/cdc-stack`).

## What the offline tests cover (and what they don't)

Covered without AWS/DSQL: DSQL dialect SQL generation (upsert/delete), OCC
`40001` / `XX000` transaction-retry policy (each attempt rolled back before the next),
≤3,000-row batch partitioning, Debezium envelope → upsert/delete parsing, and
the IAM-token refresh trigger.

**Not** covered here (requires live MSK Connect + Aurora DSQL validation):
real IAM token rotation across >15 min / >1 h inside an MSK Connect
worker, OCC throughput/conflict rate under target parallelism, and
at-least-once + idempotent → effectively-once convergence under a real change
stream. Tune `batch.size` / `tasks.max` / partition count / key distribution
from the spike results before production deploy.

## Per-record apply logging (debug only — NOT implemented)

> **Status: design note, intentionally not built.** The sink does **not** log
> per-record applies today (only `reportOrThrow` logs DLQ/dropped records at
> WARN/ERROR). This section is the recipe for adding it *when a specific CDC
> data-consistency bug needs row-level tracing on the target side* — so it can be
> turned on deliberately rather than carried as always-on overhead. Until then,
> use the Python-side row-consistency tools, which need no connector change:
> `scripts/cdc_consistency_check.py` (source↔DSQL reconciliation + the exact PKs
> missing on the target), Step 4 Validation's row-level diff
> (`DSQL_MIGRATOR_VALIDATE_ROW_DIFF_SAMPLE_SIZE`), and the Full Load data-path
> trace (`DSQL_MIGRATOR_LOG_LEVEL=DEBUG`).

**Why a connector-config gate, not a log level.** MSK Connect (managed Kafka
Connect) exposes **no** dynamic `/admin/loggers` REST endpoint, and its worker
`connect-log4j.properties` is not operator-editable, so you **cannot** raise the
sink's log4j level after deploy. The only externally controllable switch is a
**connector-config property**, toggled by editing the `DsqlSinkConnector`
`ConnectorConfiguration` block in `deploy/cdc-stack/cdc-stack.yaml` and running
`update_stack` (the same path that already sets `delete.enabled`).

**One-time change (requires a rebuild + redeploy):**

1. Add a config key in `DsqlSinkConnectorConfig.java` next to `DELETE_ENABLED`:
   ```java
   public static final String APPLY_LOG_ENABLED = "apply.log.enabled";
   // in CONFIG_DEF:
   .define(APPLY_LOG_ENABLED, Type.BOOLEAN, false, Importance.LOW,
           "Dev-only: log table+op+PK+outcome per applied record. Default false.")
   ```
2. Read it in `DsqlSinkTask.start(...)` into a `boolean applyLog` field, and emit
   the line in `executeOne(...)` (DsqlSinkTask.java:272) **after** `ps.executeUpdate()`:
   ```java
   int n = ps.executeUpdate();
   if (applyLog) {                              // INFO so it shows under MSK Connect's default level
     log.info("apply table={} op={} pk={} rows={}",
              event.table(), event.isDelete() ? "delete" : "upsert",
              event.pkValues(), n);             // PK + op + outcome ONLY
   }
   ```
3. Rebuild + redeploy the plugin: `mvn -B -ntp package` → re-zip
   `connectors/plugins/dsql-sink-plugin.zip` → bump `PLUGIN_VERSION` in
   `src/dsql_migrator/core/s3_provision.py` (MSK Connect plugins/worker-configs are
   immutable, custom-named; the version token forces a clean replacement) → deploy.

**After that one-time change, toggling needs NO rebuild** — flip the config in
`cdc-stack.yaml` and `update_stack`:
```yaml
# DsqlSinkConnector → ConnectorConfiguration (next to delete.enabled)
apply.log.enabled: "true"    # turn on for a debugging window
# ...set back to "false" and update_stack again when done
```

**Constraints to respect (Property 7 + cost):**
- Log **table + op + PK + outcome only — never `event.values()`/`event.columns()`**
  (row payload can contain PII). PK itself can be sensitive for natural keys, so
  this stays default-off and dev-gated.
- `op` is **`upsert` or `delete` only** — Debezium `c/r/u` all collapse to an
  idempotent upsert, so insert-vs-update is not distinguishable here.
- A per-record line at CDC throughput inflates the CloudWatch log group
  (`ConnectorLogGroup`) and ingestion cost; keep it on only for a short window,
  and prefer a per-`put()` summary tally if steady-state observability is wanted.
- Lines land in CloudWatch Logs (`ConnectorLogGroupName` stack output), and with
  `tasks.max > 1` they interleave across tasks — key any analysis on table+PK.
