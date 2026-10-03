// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * The per-table drift cool-down (G10): once Aurora DSQL rejects a table's row for a STRUCTURAL
 * reason (a column the target lacks, a dropped table, a changed key or type), that table's later
 * rows are applied one per transaction while every other table keeps batching -- instead of
 * every chunk that holds one of its rows paying a failed batch + rollback + a one-row replay of
 * the WHOLE chunk. The cool-down only chooses the path: every row is still executed, and a row
 * is dead-lettered only when its own statement fails.
 *
 * The harness is a deterministic JDBC double that fails writes by SQL, counts
 * prepare/execute/commit/rollback, and tracks which rows were actually committed; a recording
 * ErrantRecordReporter stands in for the Connect DLQ; the clock is injected.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkDriftCooldownTest {

  private static final String HEALTHY = "orders"; // table a
  private static final String DRIFTED = "drifted"; // table t
  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

  // --- Records ----------------------------------------------------------------

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("v", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("db", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("after", ROW)
          .field("source", SOURCE)
          .build();

  private static long offset;

  private static SinkRecord row(String table, String op, long id, String v) {
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", op)
            .put("source", new Struct(SOURCE).put("db", "shop").put("table", table));
    if (!"d".equals(op)) {
      envelope.put("after", new Struct(ROW).put("id", id).put("v", v));
    }
    return new SinkRecord(
        "dsqlcdc.shop." + table, 0, KEY, new Struct(KEY).put("id", id), ENVELOPE, envelope,
        offset++);
  }

  private static SinkRecord insert(String table, long id) {
    return row(table, "c", id, "v" + id);
  }

  /** 1,000 inserts, every 10th (i % 10 == 9) from the drifted table, the rest healthy. */
  private static List<SinkRecord> mixedPut(long firstId) {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      records.add(insert(i % 10 == 9 ? DRIFTED : HEALTHY, firstId + i));
    }
    return records;
  }

  // --- The JDBC double ----------------------------------------------------------

  /**
   * Fails a write by its SQL ({@link #failure}, {@code null} = succeeds), records
   * {@code prepare:<tag>}, {@code batch:<tag>}, {@code update:<tag>}, {@code commit},
   * {@code rollback}, {@code close} in order, and keeps the rows that were really committed
   * (a rolled-back transaction's rows are discarded). {@code <tag>} is "upsert:"/"delete:"
   * plus the table.
   */
  private static final class FakeDsql {
    final List<String> calls = new ArrayList<>();
    final List<List<Object>> committed = new ArrayList<>(); // [table, id, v?] per row
    private final List<List<Object>> pending = new ArrayList<>();
    Function<String, SQLException> failure = sql -> null;

    static String table(String sql) {
      return sql.contains("\"" + DRIFTED + "\"") ? DRIFTED : HEALTHY;
    }

    static String tag(String sql) {
      return (sql.startsWith("DELETE") ? "delete:" : "upsert:") + table(sql);
    }

    int count(String call) {
      return (int) calls.stream().filter(call::equals).count();
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private Object execute(String sql, List<List<Object>> rows, Object result)
        throws SQLException {
      SQLException e = failure.apply(sql);
      if (e != null) {
        throw e;
      }
      for (List<Object> r : rows) {
        List<Object> applied = new ArrayList<>();
        applied.add(table(sql));
        applied.addAll(r);
        pending.add(applied);
      }
      return result;
    }

    private PreparedStatement statement(String sql) {
      List<Object> current = new ArrayList<>();
      List<List<Object>> batch = new ArrayList<>();
      return proxy(
          PreparedStatement.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "getParameterMetaData", "close" -> null;
                case "setObject" -> {
                  current.add(a[1]);
                  yield null;
                }
                case "addBatch" -> {
                  batch.add(new ArrayList<>(current));
                  current.clear();
                  yield null;
                }
                case "executeUpdate" -> {
                  calls.add("update:" + tag(sql));
                  yield execute(sql, batch.isEmpty() ? List.of(current) : batch, 1);
                }
                case "executeBatch" -> {
                  calls.add("batch:" + tag(sql));
                  yield execute(sql, batch, new int[batch.size()]);
                }
                case "toString" -> "FakeStatement";
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }

    Connection connection() {
      return proxy(
          Connection.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "isClosed" -> false;
                case "isValid" -> true;
                case "prepareStatement" -> {
                  calls.add("prepare:" + tag((String) a[0]));
                  yield statement((String) a[0]);
                }
                case "commit" -> {
                  calls.add("commit");
                  committed.addAll(pending);
                  pending.clear();
                  yield null;
                }
                case "rollback" -> {
                  calls.add("rollback");
                  pending.clear();
                  yield null;
                }
                case "close" -> {
                  calls.add("close");
                  yield null;
                }
                case "toString" -> "FakeDsql";
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }

    long committedRows(String table) {
      return committed.stream().filter(r -> table.equals(r.get(0))).count();
    }
  }

  /** Fails every INSERT/upsert of the drifted table with {@code state}; deletes succeed. */
  private static Function<String, SQLException> driftedUpserts(String state) {
    return sql ->
        sql.startsWith("INSERT") && FakeDsql.table(sql).equals(DRIFTED)
            ? new SQLException("column \"new_col\" of relation \"drifted\" does not exist", state)
            : null;
  }

  /** A stand-in for the Connect DLQ: keeps every reported record and its cause. */
  private static final class RecordingReporter implements ErrantRecordReporter {
    final List<SinkRecord> records = new ArrayList<>();
    final List<Throwable> causes = new ArrayList<>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      records.add(record);
      causes.add(error);
      return CompletableFuture.completedFuture(null);
    }
  }

  /** A task with an injected clock and no guarded target columns (no catalog round trips). */
  private static final class ClockedTask extends DsqlSinkTask {
    long now = 1_000 * SECOND;

    @Override
    long nanoTime() {
      return now;
    }

    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }
  }

  private static Map<String, String> props(int batchSize) {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.BATCH_SIZE, String.valueOf(batchSize));
    return props;
  }

  private static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  private static ClockedTask task(FakeDsql dsql, RecordingReporter dlq, int batchSize)
      throws Exception {
    ClockedTask task = new ClockedTask();
    task.startMetrics(props(batchSize));
    setField(task, "connection", dsql.connection());
    if (dlq != null) {
      setField(task, "dlqReporter", dlq);
    }
    return task;
  }

  // --- (a) the measured effect ------------------------------------------------------------

  @Test
  void mixedPutWithOneDriftedTableBatchesTheHealthyTableAfterTheFirstRejection()
      throws Exception {
    // Measured on this harness, 1,000-row put, batch.size 100, every 10th row from a table
    // whose INSERT fails 42703:
    //   before G10: 900 commits + 110 rollbacks (each chunk = failed batch + rollback + 100
    //               one-row transactions, healthy rows included)
    //   after  G10:  99 commits + 101 rollbacks (chunk 1 unchanged; chunks 2-10 = one batched
    //               commit of the 90 healthy rows + 10 one-row attempts of the drifted table)
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);

    task.put(mixedPut(0));

    assertEquals(99, dsql.count("commit"));
    assertEquals(101, dsql.count("rollback"));
    assertEquals(900, dsql.committedRows(HEALTHY));
    assertEquals(0, dsql.committedRows(DRIFTED));
    assertEquals(100, dlq.records.size());
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    // Healthy rows went one per transaction only in chunk 1 (90 of them); from chunk 2 on they
    // are batched: exactly one batched execute per chunk (+1 for chunk 1's failed attempt,
    // which got through its first healthy run before the drifted run failed).
    assertEquals(90, dsql.count("update:upsert:" + HEALTHY));
    assertEquals(1 + 9, dsql.count("batch:upsert:" + HEALTHY));
    // Every drifted row was still executed: chunk 1's failed batched run + 100 per-row tries.
    assertEquals(1 + 100, dsql.count("update:upsert:" + DRIFTED));

    // A second put of the same shape pays no failed batch at all: 10 x (1 + 10).
    dsql.calls.clear();
    task.put(mixedPut(10_000));
    assertEquals(10, dsql.count("commit"));
    assertEquals(100, dsql.count("rollback"));
    assertEquals(10, dsql.count("batch:upsert:" + HEALTHY));
    assertEquals(0, dsql.count("update:upsert:" + HEALTHY));
    assertEquals(1800, dsql.committedRows(HEALTHY));
    assertEquals(200, dlq.records.size());
  }

  // --- (b) the drifted rows keep their exact reason ---------------------------------------

  @Test
  void driftedRowsKeepTheirExactReason() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(mixedPut(0));
    assertEquals(100, dlq.records.size());

    for (int i = 0; i < dlq.records.size(); i++) {
      SinkRecord r = dlq.records.get(i);
      assertTrue(r.topic().endsWith("." + DRIFTED), r.topic());
      assertInstanceOf(SQLException.class, dlq.causes.get(i));
      assertEquals("42703", ((SQLException) dlq.causes.get(i)).getSQLState());
    }
    // The enriched reason (the log line the Python monitor parses) is byte-identical on all
    // three paths: a single-row chunk (unchanged), the chunk replay before the table is armed,
    // and the cooled per-row path. A task with NO reporter throws the reason in its
    // ConnectException message, after the "(topic=..., offset=...): " prefix.
    for (int i : new int[] {0, 50, 99}) { // one from chunk 1's replay, two from cooled chunks
      SinkRecord r = dlq.records.get(i);
      String single = reasonOf(newTask(null), List.of(r));
      assertTrue(single.startsWith("sqlstate=42703 "), single);
      assertTrue(single.contains("pk: id=" + ((Struct) r.key()).getInt64("id")), single);
      assertTrue(single.contains("op: c/r"), single);
      assertTrue(single.contains(" | sql: INSERT"), single);

      String replay = reasonOf(newTask(null), List.of(insert(HEALTHY, 1), r));
      assertEquals(single, replay);

      RecordingReporter arming = new RecordingReporter();
      ClockedTask cooled = newTask(arming);
      cooled.put(List.of(insert(DRIFTED, 777_777L)));
      assertEquals(Set.of("shop." + DRIFTED), cooled.driftCooledTables());
      setField(cooled, "dlqReporter", null);
      assertEquals(single, reasonOf(cooled, List.of(insert(HEALTHY, 1), r)));
    }
  }

  private static ClockedTask newTask(RecordingReporter dlq) throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    return task(dsql, dlq, 100);
  }

  private static String reasonOf(ClockedTask task, List<SinkRecord> put) {
    ConnectException e = assertThrows(ConnectException.class, () -> task.put(put));
    String m = e.getMessage();
    return m.substring(m.indexOf("): ") + 3);
  }

  // --- (c) Fix target schema ends it, nothing dead-lettered after the fix ------------------

  @Test
  void fixTargetSchemaEndsTheCooldownWithNoFurtherDeadLetter() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(List.of(insert(HEALTHY, 1), insert(DRIFTED, 2), insert(HEALTHY, 3)));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    int deadLettered = dlq.records.size();
    assertEquals(1, deadLettered);

    dsql.failure = sql -> null; // 'Fix target schema...' added the column
    dsql.calls.clear();
    task.put(
        List.of(insert(HEALTHY, 10), insert(DRIFTED, 11), insert(HEALTHY, 12), insert(DRIFTED, 13)));
    assertTrue(task.driftCooledTables().isEmpty());
    assertEquals(deadLettered, dlq.records.size());
    assertEquals(2, dsql.committedRows(DRIFTED));

    // The next put batches the drifted table again: one execute per same-SQL run, one commit,
    // no per-row prepare of a single row, no rollback.
    dsql.calls.clear();
    task.put(
        List.of(insert(DRIFTED, 20), insert(DRIFTED, 21), insert(HEALTHY, 22), insert(HEALTHY, 23)));
    assertEquals(
        List.of(
            "prepare:upsert:" + DRIFTED, "batch:upsert:" + DRIFTED,
            "prepare:upsert:" + HEALTHY, "batch:upsert:" + HEALTHY,
            "commit"),
        dsql.calls);
    assertEquals(deadLettered, dlq.records.size());
  }

  // --- (d) TTL on the injected clock -------------------------------------------------------

  @Test
  void cooldownTtlOnTheInjectedClock() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    long t0 = task.now;
    task.put(List.of(insert(DRIFTED, 1)));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());

    // Only deletes of t follow (they succeed under column drift, and do not end it).
    task.now = t0 + 59 * SECOND;
    dsql.calls.clear();
    task.put(List.of(insert(HEALTHY, 2), row(DRIFTED, "d", 3, null), insert(HEALTHY, 4)));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    assertEquals(0, dsql.count("rollback"));
    assertEquals(1, dsql.count("batch:upsert:" + HEALTHY));
    assertEquals(1, dsql.count("update:delete:" + DRIFTED));

    // Exactly at the TTL the entry has expired (expiry is "<= now"): a healthy-only put
    // expires it and does not re-arm it.
    task.now = t0 + DsqlSinkTask.DRIFT_COOLDOWN_NANOS;
    task.put(List.of(insert(HEALTHY, 50), insert(HEALTHY, 51)));
    assertTrue(task.driftCooledTables().isEmpty());

    // Just past the TTL: the next mixed chunk tries a batch first again (1 rollback), replays
    // per row, and re-arms because t still fails.
    task.now = t0 + DsqlSinkTask.DRIFT_COOLDOWN_NANOS + 1;
    dsql.calls.clear();
    task.put(List.of(insert(HEALTHY, 5), insert(DRIFTED, 6), insert(HEALTHY, 7)));
    assertEquals(
        List.of(
            "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY,
            "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback", // the batch
            "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY, "commit",
            "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback",
            "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY, "commit"),
        dsql.calls);
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
  }

  // --- (e) sustained drift refreshes the expiry ---------------------------------------------

  @Test
  void sustainedDriftRefreshesTheExpiry() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(List.of(insert(HEALTHY, 0), insert(DRIFTED, 1)));
    int firstRollbacks = dsql.count("rollback");
    assertEquals(2, firstRollbacks); // the failed batch + the replayed t row
    for (int i = 1; i <= 4; i++) { // every 50 s, 200 s in all
      task.now += 50 * SECOND;
      dsql.calls.clear();
      task.put(List.of(insert(HEALTHY, 10L * i), insert(DRIFTED, 10L * i + 1)));
      // Never a failed batched attempt again: one batched a commit + one per-row t rollback.
      assertEquals(
          List.of(
              "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY, "commit",
              "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback"),
          dsql.calls);
      assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    }
  }

  // --- (f) transient errors never arm it ------------------------------------------------------

  @Test
  void transientErrorsNeverArm() throws Exception {
    List<SQLException> transients =
        List.of(
            new SQLException("serialization failure", "40001"),
            new SQLException("This connection has been closed."),
            new SQLException("connection failure", "08006"),
            new SQLException("admin shutdown", "57P01"),
            new SQLTransientConnectionException("timed out", "42703")); // subclass wins
    for (SQLException failure : transients) {
      FakeDsql dsql = new FakeDsql();
      dsql.failure = sql -> FakeDsql.table(sql).equals(DRIFTED) ? failure : null;
      RecordingReporter dlq = new RecordingReporter();
      ClockedTask task = task(dsql, dlq, 100);
      assertThrows(
          RetriableException.class,
          () -> task.put(List.of(insert(HEALTHY, 1), insert(DRIFTED, 2))),
          String.valueOf(failure.getSQLState()));
      assertTrue(task.driftCooledTables().isEmpty(), String.valueOf(failure.getSQLState()));
      assertTrue(dlq.records.isEmpty());
      // A following healthy put takes the hot path.
      FakeDsql next = new FakeDsql();
      setField(task, "connection", next.connection());
      task.put(List.of(insert(HEALTHY, 3), insert(HEALTHY, 4)));
      assertEquals(
          List.of("prepare:upsert:" + HEALTHY, "batch:upsert:" + HEALTHY, "commit"), next.calls);
    }
  }

  // --- (g) per-value codes do not arm it -------------------------------------------------------

  @Test
  void perValueCodesDoNotArm() throws Exception {
    for (String state : List.of("23505", "22P02", "23514", "23503", "07006")) {
      FakeDsql dsql = new FakeDsql();
      dsql.failure = driftedUpserts(state);
      RecordingReporter dlq = new RecordingReporter();
      ClockedTask task = task(dsql, dlq, 100);
      task.put(List.of(insert(HEALTHY, 1), insert(DRIFTED, 2), insert(HEALTHY, 3)));
      assertEquals(1, dlq.records.size(), state);
      assertTrue(task.driftCooledTables().isEmpty(), state);
      // Today's fallback on the next put too: batch -> rollback -> per row for every row.
      dsql.calls.clear();
      task.put(List.of(insert(HEALTHY, 4), insert(DRIFTED, 5), insert(HEALTHY, 6)));
      assertEquals(
          List.of(
              "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY,
              "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback",
              "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY, "commit",
              "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback",
              "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY, "commit"),
          dsql.calls,
          state);
      assertEquals(2, dlq.records.size(), state);
    }
  }

  // --- (h) a delete success under column drift does not clear it ----------------------------

  @Test
  void deleteSuccessUnderColumnDriftDoesNotClear() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(List.of(insert(DRIFTED, 1)));
    task.put(List.of(row(DRIFTED, "d", 2, null), insert(DRIFTED, 3)));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    assertEquals(2, dlq.records.size()); // both upserts, really rejected
    assertEquals(1, dsql.committedRows(DRIFTED)); // the delete

    // Armed by 42P01 (the table is gone: deletes fail too), a delete success after the fix
    // DOES prove the table is back.
    FakeDsql gone = new FakeDsql();
    gone.failure =
        sql ->
            FakeDsql.table(sql).equals(DRIFTED)
                ? new SQLException("relation \"drifted\" does not exist", "42P01")
                : null;
    RecordingReporter dlq2 = new RecordingReporter();
    ClockedTask task2 = task(gone, dlq2, 100);
    task2.put(List.of(insert(DRIFTED, 1)));
    assertEquals(Set.of("shop." + DRIFTED), task2.driftCooledTables());
    gone.failure = sql -> null;
    task2.put(List.of(row(DRIFTED, "d", 2, null)));
    assertTrue(task2.driftCooledTables().isEmpty());
    assertEquals(1, dlq2.records.size());
  }

  @Test
  void deleteSuccessEndsACooldownArmedByAMissingSchemaOrAFailedDelete() throws Exception {
    // 3F000: the schema is gone, so every statement fails; any applied row proves it is back.
    FakeDsql noSchema = new FakeDsql();
    noSchema.failure =
        sql ->
            FakeDsql.table(sql).equals(DRIFTED)
                ? new SQLException("schema \"shop\" does not exist", "3F000")
                : null;
    ClockedTask task = task(noSchema, new RecordingReporter(), 100);
    task.put(List.of(insert(DRIFTED, 1)));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    noSchema.failure = sql -> null;
    task.put(List.of(row(DRIFTED, "d", 2, null)));
    assertTrue(task.driftCooledTables().isEmpty(), "3F000");

    // A rejected DELETE (e.g. a renamed key column): a later applied delete proves the repair.
    FakeDsql renamedKey = new FakeDsql();
    renamedKey.failure =
        sql ->
            sql.startsWith("DELETE") && FakeDsql.table(sql).equals(DRIFTED)
                ? new SQLException("column \"id\" does not exist", "42703")
                : null;
    ClockedTask task2 = task(renamedKey, new RecordingReporter(), 100);
    task2.put(List.of(row(DRIFTED, "d", 1, null)));
    assertEquals(Set.of("shop." + DRIFTED), task2.driftCooledTables());
    renamedKey.failure = sql -> null;
    task2.put(List.of(row(DRIFTED, "d", 2, null)));
    assertTrue(task2.driftCooledTables().isEmpty(), "failed delete");
  }

  @Test
  void allCooledChunkPaysNoEmptyBatchedTransaction() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    ClockedTask task = task(dsql, new RecordingReporter(), 100);
    task.put(List.of(insert(DRIFTED, 1)));
    dsql.calls.clear();
    task.put(List.of(row(DRIFTED, "d", 2, null), insert(DRIFTED, 3)));
    // Only the per-row attempts: no leading commit of an empty healthy batch.
    assertEquals(
        List.of(
            "prepare:delete:" + DRIFTED, "update:delete:" + DRIFTED, "commit",
            "prepare:upsert:" + DRIFTED, "update:upsert:" + DRIFTED, "rollback"),
        dsql.calls);
  }

  @Test
  void cooldownStartLogNamesTheRealEndCondition() {
    String columnDrift = DsqlSinkTask.driftCooldownStartMessage(false);
    assertTrue(columnDrift.contains("until one of its inserts or updates is applied"));
    assertTrue(columnDrift.contains("a delete does not prove the repair"));
    String anyRow = DsqlSinkTask.driftCooldownStartMessage(true);
    assertTrue(anyRow.contains("until one of its rows is applied"));
    for (String line : List.of(columnDrift, anyRow)) {
      assertTrue(line.startsWith("Table {}: Aurora DSQL rejected a row for a schema reason"));
      assertTrue(line.endsWith(", or for 60 s with no further rejection. Other tables keep batching."));
      assertEquals(2, line.split("\\{\\}", -1).length - 1); // table + SQLSTATE, no values
      assertFalse(line.contains("Quarantined record") || line.contains("Dropping unapplicable"));
    }
  }

  // --- (i) no permanent errors: the identical call sequence --------------------------------

  @Test
  void noPermanentErrorsGivesTheIdenticalCallSequence() throws Exception {
    FakeDsql dsql = new FakeDsql();
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 3);
    task.put(
        List.of(insert(HEALTHY, 1), insert(DRIFTED, 2), insert(DRIFTED, 3), insert(HEALTHY, 4)));
    task.put(List.of(insert(DRIFTED, 5), insert(DRIFTED, 6)));
    assertEquals(
        List.of(
            // put 1, chunk [a1, t2, t3]
            "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY,
            "prepare:upsert:" + DRIFTED, "batch:upsert:" + DRIFTED,
            "commit",
            // put 1, chunk [a4]
            "prepare:upsert:" + HEALTHY, "update:upsert:" + HEALTHY,
            "commit",
            // put 2, chunk [t5, t6]
            "prepare:upsert:" + DRIFTED, "batch:upsert:" + DRIFTED,
            "commit"),
        dsql.calls);
    assertTrue(task.driftCooledTables().isEmpty());
    assertTrue(dlq.records.isEmpty());
  }

  // --- (j) per-table order on the per-row path ----------------------------------------------

  @Test
  void orderingPerTableIsPreserved() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(List.of(insert(DRIFTED, 99))); // arm
    dsql.failure = sql -> null; // so the cooled rows' order is observable in what commits
    // Keep t cooled through this put: the first applied t upsert ends it, but the REST of the
    // chunk still finishes on the per-row path, in arrival order.
    dsql.committed.clear();
    task.put(
        List.of(
            row(DRIFTED, "c", 7, "v1"),
            row(HEALTHY, "c", 1, "a1"),
            row(DRIFTED, "u", 7, "v2"),
            row(HEALTHY, "u", 1, "a2"),
            row(DRIFTED, "u", 7, "v3")));
    List<List<Object>> t =
        dsql.committed.stream().filter(r -> DRIFTED.equals(r.get(0))).toList();
    assertEquals(
        List.of(List.of(DRIFTED, 7L, "v1"), List.of(DRIFTED, 7L, "v2"), List.of(DRIFTED, 7L, "v3")),
        t);
    // The healthy half was batched and deduped to its last image per PK.
    List<List<Object>> a =
        dsql.committed.stream().filter(r -> HEALTHY.equals(r.get(0))).toList();
    assertEquals(List.of(List.of(HEALTHY, 1L, "a2")), a);
  }

  // --- (k) a transient error on the cooled half redelivers the put ---------------------------

  @Test
  void transientInTheCooledHalfRedeliversThePut() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    RecordingReporter dlq = new RecordingReporter();
    ClockedTask task = task(dsql, dlq, 100);
    task.put(List.of(insert(DRIFTED, 1))); // arm
    dsql.failure =
        sql ->
            FakeDsql.table(sql).equals(DRIFTED)
                ? new SQLException("connection failure", "08006")
                : null;
    List<SinkRecord> put = List.of(insert(HEALTHY, 2), insert(DRIFTED, 3));
    dsql.calls.clear();
    assertThrows(RetriableException.class, () -> task.put(put));
    assertTrue(dsql.calls.contains("close"), dsql.calls.toString()); // connection discarded
    assertEquals(1, dsql.committedRows(HEALTHY)); // the healthy half committed first
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());

    // Connect redelivers the same put: a is applied idempotently again, t still cooled.
    dsql.failure = driftedUpserts("42703");
    setField(task, "connection", dsql.connection());
    task.put(put);
    assertEquals(2, dsql.committedRows(HEALTHY));
    assertEquals(Set.of("shop." + DRIFTED), task.driftCooledTables());
    assertEquals(2, dlq.records.size());
  }

  // --- (l) no DLQ reporter: still fails loud ---------------------------------------------------

  @Test
  void dlqReporterNullStillFailsLoud() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.failure = driftedUpserts("42703");
    ClockedTask task = task(dsql, null, 100);
    ConnectException e =
        assertThrows(
            ConnectException.class,
            () -> task.put(List.of(insert(HEALTHY, 1), insert(DRIFTED, 2))));
    assertTrue(e.getMessage().contains("sqlstate=42703"), e.getMessage());
    // And on the cooled path too.
    assertThrows(
        ConnectException.class, () -> task.put(List.of(insert(HEALTHY, 3), insert(DRIFTED, 4))));
  }

  // --- (m) the arming set -------------------------------------------------------------------

  @Test
  void isStructuralDriftTable() {
    for (String s : List.of("42703", "42P01", "3F000", "42P10", "42804", "42883", "23502")) {
      assertTrue(DsqlSinkTask.isStructuralDrift(s), s);
    }
    for (String s :
        List.of(
            "23505", "23514", "22001", "22003", "22P02", "07006", "23503", "54000", "40001",
            "25P02", "08006")) {
      assertFalse(DsqlSinkTask.isStructuralDrift(s), s);
    }
    assertFalse(DsqlSinkTask.isStructuralDrift(null));
  }
}
