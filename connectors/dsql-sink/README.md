# Custom DSQL Sink Connector (Task 23.2)

> **STATUS: implemented + offline unit-tested + plugin jar builds.**
> The connector logic is complete and verified at the unit/build level
> (`mvn test` → 20 tests pass; `mvn package` → shaded MSK Connect plugin jar).
> Before a production deploy, validate the runtime characteristics on a live
> MSK Connect + Aurora DSQL environment: IAM token rotation, OCC `40001`
> throughput under contention, ≤3,000-row batching / 1-hour reconnect, and
> effectively-once delivery, deploying to an account via `deploy/cdc-stack`.

## Why a custom connector (and why Java)

The standard managed JDBC sink retries OCC (`SQLSTATE 40001`) at **batch**
granularity, which collapses throughput under high-contention large-scale CDC:
a single `40001` replays the whole batch, so a wide key range keeps colliding and
throughput degrades toward a livelock. This connector applies Debezium change
events to Aurora DSQL while handling DSQL's constraints directly:

- **IAM short-lived tokens (~15 min)** — auto-refresh + reconnect before the
  1-hour connection timeout (`DsqlIamTokenProvider`).
- **OCC `40001`** — **statement-level** retry with backoff + jitter
  (`OccRetry`), mirroring the Python `core/occ.py` policy.
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
| `DsqlSinkConnectorConfig.java` | Connector config: `dsql.cluster.endpoint`/`dsql.region`/`dsql.database`/`dsql.username`/`delete.enabled`/`batch.size`/`occ.*` |
| `DsqlSinkTask.java` | `SinkTask`: parse → batch (≤3,000) → OCC-retried upsert/delete → commit; IAM-token reconnect; failures re-thrown for the Connect DLQ |
| `DebeziumEvents.java` | Maps a Debezium envelope `SinkRecord` to a `ChangeEvent` (record key = PK; `c/r/u`→upsert, `d`/tombstone→delete) |
| `ChangeEvent.java` | Parsed change event (table, delete flag, columns/values, pk columns/values) |
| `DsqlDialect.java` | Pure DSQL SQL builders: identifier quoting, `INSERT ... ON CONFLICT` upsert, `DELETE` |
| `Batches.java` | Partitions events into ≤`batch.size` chunks |
| `OccRetry.java` | Statement-level `40001` retry with backoff + jitter (mirrors `core/occ.py`); injectable sleeper/jitter for tests |
| `DsqlIamTokenProvider.java` | DSQL IAM token cache + refresh policy (mirrors `core/target_connection.py`); injectable token source + clock for tests |
| `AwsDsqlTokenSource.java` | The only class touching the AWS SDK: `DsqlUtilities` admin/non-admin token generation |
| `src/test/java/...` | JUnit 5 tests: dialect SQL, OCC retry, batch partition, Debezium parse, token refresh |

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
`40001` statement-retry policy, ≤3,000-row batch partitioning, Debezium
envelope → upsert/delete parsing, and the IAM-token refresh trigger.

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
