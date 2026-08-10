// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Implemented; the deterministic apply logic (event parse, dialect, batching,
 * OCC retry) is offline unit-tested. Validate the JDBC reconnect/commit behavior
 * against a live MSK Connect + DSQL run before a production deploy: see README.md.
 */
package dev.dsqlmigrator.connect;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;

/**
 * Applies Debezium change events to Aurora DSQL with idempotent PK upsert/delete,
 * statement-level OCC retry, &lt;=3,000-row batches, and IAM-token reconnect.
 *
 * <p><b>Error handling (heterogeneous-engine policy).</b>
 * A change event that DSQL permanently rejects (type mismatch, constraint
 * violation, oversized value, missing object, …) MUST NOT crash the task or stall
 * the pipeline. Such a record is isolated to the dead-letter queue via the
 * Connect {@link ErrantRecordReporter} and the rest of the batch is still applied;
 * only transient failures are retried:
 *
 * <ul>
 *   <li><b>OCC 40001</b> — serialization failure: statement-level retry
 *       ({@link OccRetry}). Safe because apply is idempotent.</li>
 *   <li><b>Connectivity</b> — closed/expired connection (1h DSQL timeout, token
 *       expiry): reconnect and retry.</li>
 *   <li><b>Permanent</b> — any other SQLSTATE: do NOT retry; report the offending
 *       record to the DLQ and continue.</li>
 * </ul>
 *
 * <p>The fast path applies a whole batch in one transaction. On a permanent
 * failure the batch is replayed record-by-record so the exact poison row is
 * identified and quarantined while every healthy row still lands. Re-applying a
 * healthy row is safe (idempotent {@code ON CONFLICT} upsert / PK delete).
 */
public class DsqlSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(DsqlSinkTask.class);

  /** PostgreSQL/DSQL serialization-failure SQLSTATE (the only retryable class). */
  private static final String OCC_SQLSTATE = "40001";

  /**
   * DSQL rejects a single TEXT/bytea value larger than 1 MiB ("datatype limit
   * greater than 1048576 bytes not supported"). Such a value can never be applied,
   * so the sink quarantines it to the DLQ from a copy held in memory <em>before</em>
   * attempting any DSQL write — see {@link #oversizedColumn}.
   *
   * <p><b>Why the dead-letter actually works.</b> A record only reaches
   * this task if the broker accepted it, and the MSK Serverless broker caps a
   * message at 8 MiB, so any record we see is &le;8 MiB and CAN be produced to the
   * DLQ — provided the Kafka client limits are raised above the 1 MiB client
   * default (worker config {@code producer.max.request.size} /
   * {@code consumer.*.fetch.bytes}; see the {@code deploy/cdc-stack/cdc-stack.yaml}
   * {@code MaxMessageBytes} parameter). Without that, the SOURCE producer throws
   * {@code RecordTooLargeException} and the row never arrives — a stall this guard
   * cannot see. The two go together: raised limits make the row reachable, this
   * guard keeps a per-value-oversized one from being sent to DSQL.
   *
   * <p><b>The hard limit is prevention.</b> A value &gt;8 MiB can't enter Kafka at
   * all, so it must be removed at capture (Debezium {@code column.exclude.list},
   * driven by the Evaluation {@code OVERSIZED_LOB} rule). This guard handles the
   * 1-8 MiB band (quarantine); prevention handles the &gt;8 MiB band.
   */
  static final int DSQL_MAX_VALUE_BYTES = 1024 * 1024; // 1 MiB

  private DsqlSinkConnectorConfig config;
  private DsqlIamTokenProvider tokenProvider;
  private Connection connection;
  private ErrantRecordReporter dlqReporter; // null if the runtime has no DLQ wired

  // --- Per-table applied-ops monitor metrics (best-effort CloudWatch) --------
  // The UI reads these instead of COUNT(*)-ing the source: the sink knows exactly
  // how many inserts / updates / deletes it applied per table (a DMS-style
  // change breakdown), a source-scan-free signal costing three running counters +
  // at most one PutMetricData per offset-commit window (queued, not inline -- see
  // scheduleEmit). NOTE updates are counted too -- the old single "net rows" counter
  // (inserts - deletes) skipped them entirely (netRowDelta 0), so an update-heavy table
  // looked idle. Emission is strictly best-effort: a CloudWatch error is logged and
  // NEVER breaks apply, and it never runs on the offset-commit path.
  private static final String METRIC_NAMESPACE = "MysqlDsqlMigrator/CDC";
  private static final String METRIC_INSERTS = "InsertsApplied";
  private static final String METRIC_UPDATES = "UpdatesApplied";
  private static final String METRIC_DELETES = "DeletesApplied";
  // End-to-end replication lag: apply-wall-clock - event's source commit time
  // (source.ts_ms). Time-based (milliseconds) and PK-agnostic, unlike the UI's
  // MAX(pk) leading-edge check. Per table we keep the WORST (max) lag seen since the
  // last emit and publish it as a gauge; the reader takes Maximum over a trailing
  // window ("worst recent lag"). Idle windows (no events) emit nothing = caught up.
  private static final String METRIC_REPLICATION_LAG = "ReplicationLagMs";
  private boolean metricsEnabled;
  private String metricsStack = "";
  private final Map<String, LongAdder> insertsByTable = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> updatesByTable = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> deletesByTable = new ConcurrentHashMap<>();
  // Per-table worst replication lag (ms) since the last emit (reset on emit).
  private final Map<String, AtomicLong> lagByTable = new ConcurrentHashMap<>();
  private volatile CloudWatchClient cloudWatch; // built lazily on first emit
  // Emission runs OFF the Connect worker thread. flush() is called on the offset-COMMIT
  // path, which Connect bounds by offset.flush.timeout.ms; PutMetricData is a network
  // call (its first invocation also resolves credentials/endpoints, and in the cdc-stack
  // it egresses via NAT with no monitoring VPC endpoint), so doing it inline let a slow
  // CloudWatch consume the commit budget and surface as "Commit of offsets timed out" --
  // a monitor degrading replication, which best-effort metrics must never do.
  // Single-threaded so emissions stay ordered and at most one is in flight; the
  // sum-then-reset in emitMetrics is what makes handing the window off safe (each datum
  // is a delta owned by exactly one emission).
  private ExecutorService metricsExecutor;
  // Set while an emission is queued/running: skip queueing another (a slow CloudWatch
  // must not build a backlog of windows). The skipped counts are not lost -- they stay
  // in the counters and roll into the next window.
  private final AtomicBoolean emitInFlight = new AtomicBoolean(false);

  @Override
  public String version() {
    return "0.1.0-SNAPSHOT";
  }

  /**
   * Configure the metrics half of {@link #start} only.
   *
   * <p>Extracted so a unit test can exercise the emission plumbing (which is where the
   * offset-commit hazard lives) without a Connect {@code context} — {@link #start}
   * reads {@code context.errantRecordReporter()}, which no offline test can provide.
   */
  void startMetrics(Map<String, String> props) {
    this.config = new DsqlSinkConnectorConfig(props);
    // Net-rows metric is on only when a Stack dimension was supplied (the cdc-stack
    // connector config sets it); otherwise stay silent (e.g. local/unit runs).
    this.metricsStack = config.metricsStack();
    this.metricsEnabled = config.metricsEnabled() && !metricsStack.isEmpty();
    if (metricsEnabled) {
      // Daemon thread: it must never keep the JVM (or a task shutdown) waiting.
      this.metricsExecutor =
          Executors.newSingleThreadExecutor(
              r -> {
                Thread t = new Thread(r, "dsql-sink-metrics");
                t.setDaemon(true);
                return t;
              });
    }
  }

  @Override
  public void start(Map<String, String> props) {
    startMetrics(props);
    this.tokenProvider =
        new DsqlIamTokenProvider(config.clusterEndpoint(), config.region(), config.username());
    // The errant-record reporter is wired only when errors.deadletterqueue.* /
    // errors.tolerance=all are configured. On older runtimes context.
    // errantRecordReporter() may not exist (NoSuchMethodError) or may return null.
    try {
      this.dlqReporter = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      this.dlqReporter = null;
    }
    if (dlqReporter == null) {
      log.warn(
          "No ErrantRecordReporter available; a permanently-rejected record will "
              + "FAIL THE TASK (not be silently skipped), because skipping would "
              + "advance the offset past unwritten data. Configure errors.tolerance="
              + "all + errors.deadletterqueue.topic.name to quarantine such records "
              + "to the DLQ instead of stalling.");
    }
  }

  /**
   * Open (or reopen) the JDBC connection using a fresh IAM token. DSQL closes
   * connections after 1 hour; a closed/expired connection is transparently
   * reopened so the task survives long runs (spike H1).
   */
  private Connection connection() throws SQLException {
    if (connection != null && !connection.isClosed() && connection.isValid(2)) {
      return connection;
    }
    // reWriteBatchedInserts=true: pgjdbc collapses a batch of single-row INSERTs
    // into one multi-row "INSERT ... VALUES (..),(..),.. ON CONFLICT .." statement,
    // turning N execute round-trips into 1. DSQL is latency-bound, so this is a
    // large win on top of executeBatch's pipelining. It is SAFE here only because
    // applyChunkBatched dedupes each same-SQL run to one row per PK first: a
    // rewritten multi-row upsert with a duplicate conflict key would otherwise fail
    // with "ON CONFLICT DO UPDATE command cannot affect row a second time".
    String url =
        String.format(
            "jdbc:postgresql://%s:5432/%s?sslmode=require&reWriteBatchedInserts=true",
            config.clusterEndpoint(), config.database());
    Properties props = new Properties();
    props.setProperty("user", config.username());
    props.setProperty("password", tokenProvider.currentToken()); // short-lived IAM token
    Connection conn = DriverManager.getConnection(url, props);
    conn.setAutoCommit(false); // batch within a transaction (<=3000 rows)
    this.connection = conn;
    return conn;
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      return;
    }
    // Keep each event paired with its originating SinkRecord so a poison row can
    // be reported to the DLQ with full Connect context.
    List<Applicable> batch = new ArrayList<>(records.size());
    for (SinkRecord record : records) {
      ChangeEvent event;
      try {
        event = DebeziumEvents.parse(record);
      } catch (DataException e) {
        // Unparseable/poison envelope: quarantine the record, keep going.
        reportOrThrow(record, e);
        continue;
      }
      if (event.isDelete() && !config.deleteEnabled()) {
        continue;
      }
      // Size guard: a value over DSQL's 1 MiB limit can neither be applied nor
      // dead-lettered through Kafka, so quarantine it here (before any DSQL
      // write) rather than let it stall the partition.
      String oversized = oversizedColumn(event);
      if (oversized != null) {
        reportOrThrow(
            record,
            new DataException(
                "Value for column '" + oversized + "' exceeds DSQL's "
                    + DSQL_MAX_VALUE_BYTES + "-byte limit; quarantined "
                    + "(exclude oversized LOB columns at capture)."));
        continue;
      }
      batch.add(new Applicable(record, event));
    }
    if (batch.isEmpty()) {
      return;
    }
    for (List<Applicable> chunk : Batches.partition(batch, config.batchSize())) {
      applyBatch(chunk);
    }
  }

  /**
   * Apply one &lt;=batchSize chunk in a single transaction, retrying transient
   * failures. On a permanent SQL failure, fall back to record-by-record apply so
   * the poison row is isolated to the DLQ and the healthy rows still commit.
   */
  private void applyBatch(List<Applicable> chunk) {
    try {
      OccRetry.withRetry(
          () -> {
            Connection conn = connection();
            applyChunkBatched(conn, chunk);
            conn.commit();
            return null;
          },
          config.maxRetries(),
          config.retryBackoffMs());
      recordOps(chunk); // committed: count the chunk's per-table applied ops
    } catch (SQLException e) {
      rollbackQuietly();
      if (isTransient(e)) {
        // OCC budget exhausted or connectivity issue: discard the (possibly
        // half-open) connection so the next attempt truly reconnects -- isValid()
        // can pass on a stale socket -- then re-raise as a RetriableException so
        // Connect redelivers the whole batch (not a poison row). Apply is
        // idempotent, so a replay of the same offsets is safe.
        //
        // RetriableException (NOT ConnectException): WorkerSinkTask.deliverMessages()
        // catches RetriableException and pauses+redelivers the SAME batch on the next
        // poll; a plain ConnectException falls through to its fatal catch and KILLS
        // the task (offset never advances, CDC stalled until a manual restart). This
        // is retried INDEFINITELY until DSQL recovers -- it is NOT bounded by
        // errors.retry.timeout (that only wraps the conversion stage, never put()).
        // Do not "fix" this into a bounded/fail-fast retry: that would dead-letter
        // healthy rows during a routine reconnect (idle close / token expiry / worker
        // recycle) -- the exact data-loss mode this path exists to prevent.
        discardConnection();
        throw transientRetryException(e);
      }
      // Permanent failure somewhere in the batch — find and isolate the poison
      // row(s); apply the rest. Single-row chunk → that row is the poison.
      if (chunk.size() == 1) {
        Applicable only = chunk.get(0);
        reportOrThrow(only.record(), only.event(), e);
        return;
      }
      applyRecordByRecord(chunk);
    }
  }

  /**
   * Replay a chunk one row at a time (each in its own transaction). Healthy rows
   * commit; a row that hits a permanent error is quarantined to the DLQ; a
   * transient error re-raises so Connect retries.
   */
  private void applyRecordByRecord(List<Applicable> chunk) {
    for (Applicable a : chunk) {
      try {
        OccRetry.withRetry(
            () -> {
              Connection conn = connection();
              executeOne(conn, a.event());
              conn.commit();
              return null;
            },
            config.maxRetries(),
            config.retryBackoffMs());
        recordOps(a.event()); // committed this row: count its applied op
      } catch (SQLException e) {
        rollbackQuietly();
        if (isTransient(e)) {
          // Transient: reconnect + re-raise as RetriableException so Connect
          // redelivers (see applyBatch for the full rationale). Not fatal.
          discardConnection();
          throw transientRetryException(e);
        }
        reportOrThrow(a.record(), a.event(), e);
      }
    }
  }

  /**
   * Return the name of the first column whose value exceeds DSQL's per-value
   * byte limit, or {@code null} if all values fit. Only string/byte[] values are
   * size-bounded (TEXT/bytea); numbers/booleans/timestamps are small. The UTF-8
   * byte length is what DSQL measures, so a String is sized by its encoded bytes.
   */
  private static String oversizedColumn(ChangeEvent event) {
    if (event.isDelete()) {
      return null; // deletes carry only PK values, never a large payload
    }
    List<String> cols = event.columns();
    List<Object> vals = event.values();
    for (int i = 0; i < vals.size(); i++) {
      Object v = vals.get(i);
      int bytes;
      if (v instanceof String s) {
        bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
      } else if (v instanceof byte[] b) {
        bytes = b.length;
      } else {
        continue;
      }
      if (bytes > DSQL_MAX_VALUE_BYTES) {
        return i < cols.size() ? cols.get(i) : ("column[" + i + "]");
      }
    }
    return null;
  }

  /**
   * Apply a chunk in one transaction, coalescing every maximal run of
   * consecutive events that render to the SAME SQL into a single
   * {@link PreparedStatement#executeBatch()}. DSQL is a distributed store where
   * each statement round-trip dominates apply cost (the task is latency-bound,
   * not CPU-bound), so collapsing N per-row round-trips into one batched send is
   * the primary throughput lever.
   *
   * <p><b>Ordering is preserved.</b> Only <em>contiguous</em> same-SQL events are
   * grouped, so a delete that follows an upsert on the same PK (or vice-versa)
   * still executes in arrival order — batching never reorders apply. A run breaks
   * whenever the rendered SQL changes (different table, column set, or
   * upsert↔delete), so mixed streams degrade gracefully to smaller batches, never
   * to incorrect order.
   *
   * <p><b>Why {@code executeBatch}, not multi-row {@code VALUES}.</b> A rewritten
   * multi-row {@code INSERT ... ON CONFLICT} would reject a chunk that carries the
   * same PK twice ("ON CONFLICT DO UPDATE command cannot affect row a second
   * time"); plain {@code executeBatch} runs each statement independently, so
   * intra-chunk duplicate PKs stay safe while still pipelining the round-trips.
   */
  private void applyChunkBatched(Connection conn, List<Applicable> chunk) throws SQLException {
    int i = 0;
    while (i < chunk.size()) {
      String sql = renderSql(chunk.get(i).event());
      int end = runEnd(chunk, i); // exclusive end of the maximal same-SQL run
      // Dedupe the run to the LAST event per PK. Within one same-SQL run every
      // event is the same kind (all upsert or all delete) on the same table, so
      // collapsing repeated PKs to their final image is apply-equivalent (the
      // last write wins under ON CONFLICT / PK delete) AND order-preserving. This
      // is what makes reWriteBatchedInserts safe: the rewritten multi-row upsert
      // can never carry a duplicate conflict key.
      List<Applicable> run = dedupeRunByPk(chunk, i, end);
      try (PreparedStatement ps = conn.prepareStatement(sql)) {
        java.sql.ParameterMetaData meta = paramMetaOrNull(ps); // once per statement
        for (Applicable a : run) {
          bind(ps, a.event(), meta);
          ps.addBatch();
        }
        if (run.size() == 1) {
          ps.executeUpdate();
        } else {
          ps.executeBatch();
        }
      }
      i = end;
    }
  }

  /**
   * Collapse a same-SQL run {@code chunk[start, end)} to one entry per primary key,
   * keeping the LAST occurrence (its final after-image / delete). Preserves the
   * relative order of the surviving rows. Pure and package-private for unit tests.
   *
   * <p>Correctness: a run is a maximal block of consecutive events that render to
   * identical SQL, so all are the same operation kind on the same table. Applying
   * only the last write for a PK is equivalent to applying every write in order
   * because the apply is idempotent last-write-wins (ON CONFLICT upsert / PK
   * delete). Deduping here is REQUIRED for {@code reWriteBatchedInserts}: a
   * rewritten multi-row {@code ON CONFLICT} rejects a duplicate conflict key
   * ("cannot affect row a second time").
   */
  static List<Applicable> dedupeRunByPk(List<Applicable> chunk, int start, int end) {
    // LinkedHashMap keyed by PK: a re-put keeps the key's FIRST-seen position but
    // overwrites the value, so each surviving row is the LAST image for its PK and
    // the run's relative order is preserved. (Order among distinct PKs is in fact
    // apply-irrelevant within a same-SQL run, but keeping it stable is clearer.)
    java.util.LinkedHashMap<List<Object>, Applicable> byPk = new java.util.LinkedHashMap<>();
    for (int j = start; j < end; j++) {
      Applicable a = chunk.get(j);
      byPk.put(a.event().pkValues(), a);
    }
    return new ArrayList<>(byPk.values());
  }

  /**
   * Exclusive end index of the maximal run of consecutive events, starting at
   * {@code start}, that render to the same SQL as {@code chunk[start]}. Pure and
   * package-private so the run-grouping (which preserves apply order) is unit
   * tested without a live JDBC connection.
   */
  static int runEnd(List<Applicable> chunk, int start) {
    String sql = renderSql(chunk.get(start).event());
    int j = start + 1;
    while (j < chunk.size() && renderSql(chunk.get(j).event()).equals(sql)) {
      j++;
    }
    return j;
  }

  /** Render the upsert/delete SQL for a change event (its batching group key). */
  static String renderSql(ChangeEvent event) {
    return event.isDelete()
        ? DsqlDialect.deleteSql(event.table(), event.pkColumns())
        : DsqlDialect.upsertSql(event.table(), event.columns(), event.pkColumns());
  }

  /** Execute one change event (upsert or delete) on the open connection. */
  private void executeOne(Connection conn, ChangeEvent event) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(renderSql(event))) {
      bind(ps, event, paramMetaOrNull(ps));
      ps.executeUpdate();
    }
  }

  // Canonical dashed UUID (8-4-4-4-12 hex). A CHAR(36)/VARCHAR UUID PK is a
  // surrogate whose value is safe to log; an arbitrary string PK (e.g. an email or
  // account number) is a natural key that may be PII and must be withheld.
  private static final Pattern UUID_RE =
      Pattern.compile(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  /**
   * True if a PK value is a SURROGATE key whose value is safe to emit to the log
   * (integers and UUIDs), false for a natural key whose value is withheld
   * (Property 7). Decided on the value's runtime type -- NOT the Connect
   * {@code Schema.Type} -- because Debezium encodes {@code BIGINT UNSIGNED} as a
   * {@link BigDecimal} (Connect {@code BYTES}), which a type-based check would
   * wrongly withhold. Package-private + static so a unit test can assert it
   * without a live pipeline.
   */
  static boolean isSurrogate(Object value) {
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof BigInteger) {
      return true;
    }
    // Whole-number decimal (unsigned BIGINT and NUMERIC(n,0)) is a surrogate; a
    // fractional decimal is not a key we render.
    if (value instanceof BigDecimal bd) {
      return bd.scale() <= 0;
    }
    // A UUID string is a surrogate; any other string is treated as a natural key.
    return value instanceof String s && UUID_RE.matcher(s).matches();
  }

  /**
   * Render parallel PK column/value lists as {@code col=value} (surrogate) /
   * {@code col=<withheld>} (natural key), joined by {@code ", "}. Returns {@code ""}
   * for empty or length-mismatched lists so the caller emits nothing. Static +
   * package-private for unit testing.
   */
  static String formatPk(List<String> cols, List<Object> vals) {
    if (cols == null || vals == null || cols.isEmpty() || cols.size() != vals.size()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < cols.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      Object v = vals.get(i);
      sb.append(cols.get(i)).append('=').append(isSurrogate(v) ? String.valueOf(v) : "<withheld>");
    }
    return sb.toString();
  }

  /**
   * Render a Connect record key {@link Struct} (the PK, {@code pk.mode=record_key})
   * the same way as {@link #formatPk(List, List)}. Returns {@code ""} when the key
   * is absent or not a Struct.
   */
  static String formatPk(Object key) {
    if (!(key instanceof Struct struct)) {
      return "";
    }
    List<String> cols = new ArrayList<>();
    List<Object> vals = new ArrayList<>();
    for (Field field : struct.schema().fields()) {
      cols.add(field.name());
      vals.add(struct.get(field));
    }
    return formatPk(cols, vals);
  }

  /** Wrap a formatted PK as a {@code " | pk: ..."} log suffix; {@code ""} when empty. */
  private static String pkSuffix(String formattedPk) {
    return formattedPk.isEmpty() ? "" : " | pk: " + formattedPk;
  }

  /**
   * Quarantine a permanently-rejected record to the DLQ and continue. If no
   * reporter is wired, log and skip rather than killing the task (the pipeline
   * keeps moving; the loss is visible in the log).
   */
  private void reportOrThrow(SinkRecord record, Exception cause) {
    // No ChangeEvent on this path (the envelope did not parse, or the size guard
    // fired before batching), so read the PK straight off the Connect record key.
    quarantine(record, cause, cause.getMessage() + pkSuffix(formatPk(record.key())));
  }

  /**
   * Quarantine a row whose DSQL write failed, annotating the reason with the
   * rendered SQL TEMPLATE (column names only; every value is a {@code ?}
   * placeholder, so no row values or credentials are emitted — Property 7). The
   * SQL is re-rendered here, on the quarantine path only (not the hot path), so a
   * reader of the DLQ log / UI sees the exact statement shape DSQL rejected
   * (e.g. an {@code INSERT} referencing a column the target lacks) without a
   * separate lookup. Re-rendering is a few microseconds and runs only per poison
   * row, so it does not affect steady-state throughput.
   */
  private void reportOrThrow(SinkRecord record, ChangeEvent event, SQLException cause) {
    String sql;
    try {
      sql =
          event.isDelete()
              ? DsqlDialect.deleteSql(event.table(), event.pkColumns())
              : DsqlDialect.upsertSql(event.table(), event.columns(), event.pkColumns());
    } catch (RuntimeException e) {
      sql = null; // never let SQL rendering mask the real failure
    }
    // PK BEFORE the SQL template: the Python parser truncates the surfaced message
    // at a fixed length, so a long template must not push the (more actionable) PK
    // out of the window. Source the PK from the event -- it is already
    // type-converted and covers the delete before-image fallback where the record
    // key is empty.
    String pk = pkSuffix(formatPk(event.pkColumns(), event.pkValues()));
    String reason =
        sql == null
            ? cause.getMessage() + pk
            : cause.getMessage() + pk + " | sql: " + sql;
    quarantine(record, cause, reason);
  }

  /**
   * Report a record to the DLQ (or log-and-skip if none) with a final reason. The
   * ORIGINAL {@code cause} is handed to the DLQ reporter so the Kafka DLQ message
   * keeps its accurate {@code __connect.errors.exception.*} headers; the enriched
   * {@code reason} (which may carry the SQL template) goes to the worker log,
   * which is what the tool parses for the UI / activity log.
   */
  private void quarantine(SinkRecord record, Exception cause, String reason) {
    if (dlqReporter == null) {
      // No DLQ wired: a record that can neither be applied NOR quarantined must
      // NOT be silently skipped -- that advances the offset past unwritten data
      // (the observed data-loss mode). Fail the task loudly instead; the operator
      // configures errors.tolerance=all + errors.deadletterqueue.topic.name to
      // quarantine, or fixes the poison row. Loud stall beats silent loss.
      throw new ConnectException(
          "Record cannot be applied to DSQL and no DLQ is configured to quarantine it "
              + "(topic=" + record.topic() + ", partition=" + record.kafkaPartition()
              + ", offset=" + record.kafkaOffset() + "): " + reason,
          cause);
    }
    // The reporter returns a Future that completes when the DLQ produce is
    // acknowledged. We do NOT await it here (that would serialize the hot path),
    // but a produce failure must not be swallowed: log it via the callback so a
    // failed quarantine is visible rather than looking like a clean skip.
    java.util.concurrent.Future<Void> ack = dlqReporter.report(record, cause);
    log.warn(
        "Quarantined record to DLQ (topic={}, partition={}, offset={}): {}",
        record.topic(), record.kafkaPartition(), record.kafkaOffset(), reason);
    if (ack != null) {
      // Observe the produce result on the producer's callback thread; a failure
      // here means the row is neither applied nor durably dead-lettered.
      try {
        java.util.concurrent.CompletableFuture
            .runAsync(() -> {
              try {
                ack.get();
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              } catch (java.util.concurrent.ExecutionException ee) {
                log.error(
                    "DLQ produce FAILED for quarantined record (topic={}, partition={}, "
                        + "offset={}); the record was not durably dead-lettered: {}",
                    record.topic(), record.kafkaPartition(), record.kafkaOffset(),
                    ee.getCause() == null ? ee.toString() : ee.getCause().toString());
              }
            });
      } catch (RuntimeException ignored) {
        // Never let the observation wiring itself break the apply path.
      }
    }
  }

  /**
   * Wrap a transient {@link SQLException} as a Kafka Connect {@link RetriableException}
   * so {@code WorkerSinkTask} redelivers the batch instead of killing the task. Call
   * this ONLY when {@link #isTransient(SQLException)} is true -- it is not a
   * general-purpose wrapper (wrapping a permanent error would make Connect retry a
   * poison row forever). Pure + package-private so a unit test can assert the surfaced
   * exception TYPE without a live JDBC connection.
   */
  static RetriableException transientRetryException(SQLException e) {
    return new RetriableException(
        "DSQL apply failed (transient sqlstate=" + e.getSQLState() + "); Connect will retry", e);
  }

  /**
   * Transient = retryable: OCC serialization failure or a connectivity drop.
   *
   * <p><b>Why this is broader than SQLSTATE 08.</b> A connection torn down out
   * from under an in-flight statement -- DSQL's 1h idle close, IAM-token expiry,
   * or (the case that caused observed data loss) an MSK Connect worker
   * replacement that recycles the JDBC connection -- does NOT reliably surface as
   * SQLSTATE class {@code 08}. The pgjdbc driver raises such failures with a
   * {@code null} SQLSTATE ("This connection has been closed.") or a class
   * {@code 57} operator-intervention state ({@code 57P01} admin shutdown, etc.).
   * Treating those as <em>permanent</em> routed whole batches of healthy rows to
   * {@link #quarantine} during a worker bounce, advancing the offset past rows
   * that were never applied (a contiguous gap, no DLQ). They are connectivity
   * failures, so they are transient: rethrow so Connect replays the same offsets
   * after a reconnect (apply is idempotent, so a replay is safe).
   */
  // Package-private for unit testing (DsqlSinkTaskTest); semantically internal.
  boolean isTransient(SQLException e) {
    if (OCC_SQLSTATE.equals(e.getSQLState())) {
      return true;
    }
    // A closed/aborted connection often arrives with no SQLSTATE, or as a
    // dedicated connection-exception subclass -- treat both as transient.
    if (e instanceof java.sql.SQLNonTransientConnectionException
        || e instanceof java.sql.SQLRecoverableException
        || e instanceof java.sql.SQLTransientConnectionException) {
      return true;
    }
    String state = e.getSQLState();
    if (state == null) {
      // pgjdbc "connection has been closed" frequently carries no SQLSTATE.
      return true;
    }
    // 08 = connection exception; 57 = operator intervention (admin shutdown /
    // connection terminated) -- both mean "reconnect and retry", not "poison row".
    return state.startsWith("08") || state.startsWith("57");
  }

  /**
   * Bind statement parameters: delete binds PK values, upsert binds column values.
   *
   * <p>MySQL {@code TINYINT(1)} is the boolean convention and the schema converter
   * maps it to a DSQL {@code boolean} column, but Debezium serializes it as a plain
   * integer (INT16) with no logical schema name, so it arrives as a {@code Short}/
   * {@code Integer}. Binding that via {@code setObject} into a {@code boolean}
   * column fails ("column is of type boolean but expression is of type smallint")
   * and DLQs every such row. We therefore consult the statement's parameter
   * metadata and convert a numeric bind to a {@link Boolean} (0 -&gt; false, non-0
   * -&gt; true) when the target parameter is {@code BIT}/{@code BOOLEAN}. This mirrors
   * the Full Load value converter's TINYINT(1)-&gt;boolean handling so both data
   * paths agree.
   */
  private static void bind(
      PreparedStatement ps, ChangeEvent event, java.sql.ParameterMetaData meta)
      throws SQLException {
    List<Object> binds = event.isDelete() ? event.pkValues() : event.values();
    for (int i = 0; i < binds.size(); i++) {
      Object value = binds.get(i);
      if (value instanceof Number && meta != null && isBooleanParam(meta, i + 1)) {
        ps.setObject(i + 1, ((Number) value).longValue() != 0L);
      } else {
        ps.setObject(i + 1, value);
      }
    }
  }

  /**
   * Fetch a statement's parameter metadata ONCE, tolerating drivers that can't
   * describe params before execute. Hoisted out of {@link #bind} because on pgjdbc
   * {@code getParameterMetaData()} issues a server-side Parse/Describe round-trip;
   * calling it per row (as the old bind did) generated one extra round-trip per
   * change event — the dominant cost on a latency-bound sink (it showed up as
   * ~1 read-only transaction per applied row in DSQL's TotalTransactions metric).
   * The parameter types are identical for every row of a given SQL, so one fetch
   * per statement is all that is needed.
   */
  private static java.sql.ParameterMetaData paramMetaOrNull(PreparedStatement ps) {
    try {
      return ps.getParameterMetaData();
    } catch (SQLException ignored) {
      return null; // fall back to plain setObject binding
    }
  }

  /** True when prepared-statement parameter {@code idx} (1-based) targets boolean. */
  private static boolean isBooleanParam(java.sql.ParameterMetaData meta, int idx) {
    try {
      int type = meta.getParameterType(idx);
      return type == java.sql.Types.BOOLEAN || type == java.sql.Types.BIT;
    } catch (SQLException ignored) {
      return false;
    }
  }

  private void rollbackQuietly() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.rollback();
      }
    } catch (SQLException ignored) {
      // best-effort
    }
  }

  /**
   * Drop the cached connection after a transient/connectivity failure so the next
   * {@link #connection()} opens a fresh one. {@code isValid(2)} can return true on
   * a half-open socket, so a connection that just failed must not be reused -- we
   * close it best-effort and null the field to force a reconnect on retry.
   */
  private void discardConnection() {
    Connection stale = this.connection;
    this.connection = null;
    if (stale != null) {
      try {
        stale.close();
      } catch (SQLException ignored) {
        // best-effort; the field is already cleared so the next call reconnects
      }
    }
  }

  /**
   * Called by Connect on each offset commit: hand the per-table deltas accumulated
   * since the last emit to the background metrics thread, then return immediately.
   *
   * <p><b>This must not block.</b> Connect calls {@code flush()} inside the offset
   * commit, which it bounds by {@code offset.flush.timeout.ms}; a blocking
   * PutMetricData here could exhaust that budget and produce a repeating "Commit of
   * offsets timed out" — i.e. a best-effort monitor degrading replication. So the
   * network call is queued, never awaited (see {@link #scheduleEmit}).
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    scheduleEmit();
  }

  @Override
  public void stop() {
    // Task teardown, no longer on the commit path -- emit the final window INLINE so
    // counts accumulated since the last commit are not lost to a daemon thread dying
    // with the JVM. Bounded by the SDK's own timeouts and swallowed like every other
    // emission, so a slow CloudWatch delays only this shutdown, never replication.
    shutdownMetricsExecutor();
    emitMetrics();
    closeCloudWatchQuietly();
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException ignored) {
      // best-effort
    }
  }

  // --- Per-table applied-ops metric helpers (best-effort; never affect apply) ---

  /** Count every applied event by op kind (insert/update/delete) into its table. */
  private void recordOps(List<Applicable> chunk) {
    if (!metricsEnabled) {
      return;
    }
    for (Applicable a : chunk) {
      recordOps(a.event());
    }
  }

  private void recordOps(ChangeEvent event) {
    if (!metricsEnabled) {
      return;
    }
    // Count the applied op by KIND (inserts / updates / deletes). Unlike the old net
    // counter (which added netRowDelta and skipped delta==0), updates ARE counted --
    // they were previously invisible in "net rows".
    final Map<String, LongAdder> counter =
        event.isDelete()
            ? deletesByTable
            : event.isInsert() ? insertsByTable : updatesByTable;
    counter.computeIfAbsent(event.table(), t -> new LongAdder()).increment();
    // End-to-end replication lag for this just-applied event: how long from the
    // source commit (source.ts_ms) to now (apply/commit time). Keep the WORST lag
    // per table since the last emit. Clamp to >= 0 (source/target clock skew can
    // make a fresh event read slightly negative). Only when source.ts_ms was present.
    long src = event.sourceTsMs();
    if (src > 0L) {
      long lag = Math.max(0L, System.currentTimeMillis() - src);
      lagByTable.computeIfAbsent(event.table(), t -> new AtomicLong(0L)).accumulateAndGet(lag, Math::max);
    }
  }

  /**
   * Append one COUNT {@link MetricDatum} per table (dimensions Stack + Table) for the
   * counts accumulated in {@code byTable} since the last emit, then reset each counter
   * ({@code sumThenReset}). A table with a zero count this window emits nothing.
   */
  private void emitOpCounter(
      List<MetricDatum> data, Map<String, LongAdder> byTable, String metricName) {
    for (Map.Entry<String, LongAdder> e : byTable.entrySet()) {
      long count = e.getValue().sumThenReset();
      if (count == 0) {
        continue;
      }
      data.add(
          MetricDatum.builder()
              .metricName(metricName)
              .dimensions(
                  Dimension.builder().name("Stack").value(metricsStack).build(),
                  Dimension.builder().name("Table").value(e.getKey()).build())
              .value((double) count)
              .unit(StandardUnit.COUNT)
              .build());
    }
  }

  /**
   * Emit the per-table applied-ops counters ({@code InsertsApplied} /
   * {@code UpdatesApplied} / {@code DeletesApplied}) and the worst replication lag
   * ({@code ReplicationLagMs}) accumulated since the last emit, then reset. A failure
   * is logged and swallowed: the metrics are a monitor, so emission must never fail
   * replication.
   *
   * <p><b>Runs on the background metrics thread</b> (queued by {@link #scheduleEmit}),
   * concurrently with the Connect worker's {@code put()} — it is deliberately NOT on
   * the offset-commit path. That is safe because each counter is read-and-cleared
   * atomically ({@link LongAdder#sumThenReset()} / {@link AtomicLong#getAndSet}): an
   * apply that increments during an emission lands either in this window or the next,
   * never in both and never lost. {@link #scheduleEmit} keeps at most one emission in
   * flight, so two windows cannot interleave their resets. The exception is
   * {@link #stop()}, which emits inline after the executor is shut down.
   *
   * <p>Package-private and overridable so a test can substitute a slow emission and
   * assert that {@code flush()} does not wait for it.
   */
  void emitMetrics() {
    if (!metricsEnabled) {
      return;
    }
    List<MetricDatum> data = new ArrayList<>();
    // Applied-ops counters: one COUNT datum per (table, kind) for the counts since the
    // last emit, then reset. A table/kind idle this window emits nothing.
    emitOpCounter(data, insertsByTable, METRIC_INSERTS);
    emitOpCounter(data, updatesByTable, METRIC_UPDATES);
    emitOpCounter(data, deletesByTable, METRIC_DELETES);
    // Replication lag (gauge): the worst apply-time lag per table since the last
    // emit, then reset. getAndSet(0) reads-and-clears so an idle next window (no
    // events) emits no datapoint (= caught up). Same Stack+Table dimensions and the
    // same PutMetricData request as the net-rows datums above.
    for (Map.Entry<String, AtomicLong> e : lagByTable.entrySet()) {
      long lagMs = e.getValue().getAndSet(0L);
      if (lagMs <= 0L) {
        continue;
      }
      data.add(
          MetricDatum.builder()
              .metricName(METRIC_REPLICATION_LAG)
              .dimensions(
                  Dimension.builder().name("Stack").value(metricsStack).build(),
                  Dimension.builder().name("Table").value(e.getKey()).build())
              .value((double) lagMs)
              .unit(StandardUnit.MILLISECONDS)
              .build());
    }
    if (data.isEmpty()) {
      return;
    }
    try {
      // PutMetricData accepts up to 1000 datums/request; the captured-table count is
      // far below that, so one request per commit window suffices.
      cloudWatch()
          .putMetricData(
              PutMetricDataRequest.builder().namespace(METRIC_NAMESPACE).metricData(data).build());
    } catch (RuntimeException ex) {
      // Drop this window (do NOT re-accumulate: avoids unbounded growth if CloudWatch
      // stays unreachable); the next window re-reports its own delta.
      log.warn(
          "Could not emit CDC monitor metrics (net-rows / replication-lag; best-effort; "
              + "replication unaffected): {}",
          ex.toString());
    }
  }

  /**
   * Queue one {@link #emitMetrics()} on the background thread, or skip it.
   *
   * <p>Skips when an emission is already queued/running: a slow CloudWatch must not
   * build a backlog of windows. Nothing is lost by skipping — the counters are not
   * read until the emission runs, so the skipped window's counts simply roll into the
   * next one. Package-private so a test can drive it with an injected executor.
   */
  void scheduleEmit() {
    if (!metricsEnabled) {
      return;
    }
    ExecutorService executor = metricsExecutor;
    if (executor == null) {
      emitMetrics(); // no executor (e.g. a unit test): behave as before, inline
      return;
    }
    if (!emitInFlight.compareAndSet(false, true)) {
      return; // one already pending; its window will include these counts
    }
    try {
      executor.execute(
          () -> {
            try {
              emitMetrics();
            } finally {
              emitInFlight.set(false);
            }
          });
    } catch (RejectedExecutionException e) {
      // Executor already shut down (task stopping) -- drop this window; stop() emits
      // the final one inline.
      emitInFlight.set(false);
    }
  }

  /** Stop accepting new emissions and give an in-flight one a bounded moment to finish. */
  private void shutdownMetricsExecutor() {
    ExecutorService executor = metricsExecutor;
    this.metricsExecutor = null;
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      // Bounded: a hung PutMetricData must not hold up task shutdown.
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private CloudWatchClient cloudWatch() {
    CloudWatchClient client = cloudWatch;
    if (client == null) {
      synchronized (this) {
        client = cloudWatch;
        if (client == null) {
          client = CloudWatchClient.builder().region(Region.of(config.region())).build();
          cloudWatch = client;
        }
      }
    }
    return client;
  }

  private void closeCloudWatchQuietly() {
    CloudWatchClient client = cloudWatch;
    if (client != null) {
      try {
        client.close();
      } catch (RuntimeException ignored) {
        // best-effort
      }
      cloudWatch = null;
    }
  }

  /** A change event paired with the SinkRecord it came from (for DLQ reporting). */
  static final class Applicable {
    private final SinkRecord record;
    private final ChangeEvent event;

    Applicable(SinkRecord record, ChangeEvent event) {
      this.record = record;
      this.event = event;
    }

    SinkRecord record() {
      return record;
    }

    ChangeEvent event() {
      return event;
    }
  }
}
