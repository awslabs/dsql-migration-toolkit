// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Implemented; the deterministic apply logic (event parse, dialect, batching,
 * OCC retry) is offline unit-tested. Validate the JDBC reconnect/commit behavior
 * against a live MSK Connect + DSQL run before a production deploy: see README.md.
 */
package dev.dsqlmigrator.connect;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  @Override
  public String version() {
    return "0.1.0-SNAPSHOT";
  }

  @Override
  public void start(Map<String, String> props) {
    this.config = new DsqlSinkConnectorConfig(props);
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
    } catch (SQLException e) {
      rollbackQuietly();
      if (isTransient(e)) {
        // OCC budget exhausted or connectivity issue: discard the (possibly
        // half-open) connection so the next attempt truly reconnects -- isValid()
        // can pass on a stale socket -- then re-raise so Connect retries/backs off
        // the whole batch (not a poison row). Apply is idempotent, so a replay of
        // the same offsets is safe.
        discardConnection();
        throw new ConnectException("DSQL apply failed (transient sqlstate=" + e.getSQLState() + ")", e);
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
      } catch (SQLException e) {
        rollbackQuietly();
        if (isTransient(e)) {
          discardConnection();
          throw new ConnectException(
              "DSQL apply failed (transient sqlstate=" + e.getSQLState() + ")", e);
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

  /**
   * Quarantine a permanently-rejected record to the DLQ and continue. If no
   * reporter is wired, log and skip rather than killing the task (the pipeline
   * keeps moving; the loss is visible in the log).
   */
  private void reportOrThrow(SinkRecord record, Exception cause) {
    quarantine(record, cause, cause.getMessage());
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
    String reason =
        sql == null ? cause.getMessage() : cause.getMessage() + " | sql: " + sql;
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

  @Override
  public void stop() {
    try {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
    } catch (SQLException ignored) {
      // best-effort
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
