// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Re-keyed tables keyed on the SOURCE primary key in the change stream (G15).
 *
 * A table Schema Conversion re-keyed has the Aurora DSQL primary key (lead, id) and a unique
 * index on the source key (id). Listed in target.key.columns, its change records are keyed on
 * {id}, so every change to one row stays on one partition, in order. The sink writes it by
 * the composite key: an UPDATE of `lead` is ONE record and becomes delete-old + insert-new in
 * one transaction; a delete is keyed on the exact composite key of its before-image, or on the
 * source key once the unique index on it is valid; its tombstone is skipped.
 *
 * The harness is a stateful JDBC double of that one table -- PK (lead, id), UNIQUE (id) -- that
 * records every statement with its binds, applies DELETE and INSERT ... ON CONFLICT the way
 * PostgreSQL does (a unique violation on the other key is SQLSTATE 23505), and keeps committed
 * and in-flight rows apart, so a rollback is visible. UNIQUE (id) can be switched off (no
 * source-key index yet, or an edited DDL without one), so a write that should have removed a
 * row shows up as a duplicate instead of being repaired by the row-by-row replay of a 23505.
 * The unique-index catalog read and the clock are injected.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkRekeyTest {

  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
  private static final String ORDERS = "app.orders"; // re-keyed: PK (lead, id), UNIQUE (id)
  private static final String ITEMS = "app.items"; // not re-keyed: PK (id)
  private static final String TARGET_KEYS = "app\\.orders:lead,id";

  private static final String DELETE_EXACT =
      "DELETE FROM \"app\".\"orders\" WHERE \"lead\" = ? AND \"id\" = ?";
  private static final String DELETE_BY_SOURCE_KEY =
      "DELETE FROM \"app\".\"orders\" WHERE \"id\" = ?";
  // The delete of an UPDATE whose old key is unknown: spares a row already at the new lead.
  private static final String DELETE_UNLESS_AT =
      "DELETE FROM \"app\".\"orders\" WHERE \"id\" = ? AND NOT (\"lead\" = ?)";
  private static final String UPSERT_COMPOSITE =
      "INSERT INTO \"app\".\"orders\" (\"lead\", \"id\", \"v\") VALUES (?, ?, ?) "
          + "ON CONFLICT (\"lead\", \"id\") DO UPDATE SET \"v\" = EXCLUDED.\"v\"";

  // --- Records ----------------------------------------------------------------

  private static final Schema SOURCE_KEY =
      SchemaBuilder.struct().name("app.orders.Key").field("id", Schema.INT32_SCHEMA).build();
  private static final Schema COMPOSITE_KEY =
      SchemaBuilder.struct()
          .name("app.orders.Key")
          .field("lead", Schema.INT32_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("app.orders.Value")
          .field("lead", Schema.INT32_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("v", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("connector", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("before", ROW)
          .field("after", ROW)
          .field("source", SOURCE)
          .build();

  private static long offset;

  private static Struct row(int lead, int id, String v) {
    return new Struct(ROW).put("lead", lead).put("id", id).put("v", v);
  }

  /** A change record of {@code table} keyed {@code {id}} (the source key) or {@code {lead,id}}. */
  private static SinkRecord record(
      String table, Struct key, String op, Struct before, Struct after) {
    String name = table.substring(table.indexOf('.') + 1);
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", op)
            .put(
                "source",
                new Struct(SOURCE)
                    .put("connector", "postgresql")
                    .put("schema", "app")
                    .put("table", name));
    if (before != null) {
      envelope.put("before", before);
    }
    if (after != null) {
      envelope.put("after", after);
    }
    return new SinkRecord(
        "dsqlcdc." + table, 0, key.schema(), key, ENVELOPE, envelope, offset++);
  }

  private static Struct sourceKey(int id) {
    return new Struct(SOURCE_KEY).put("id", id);
  }

  private static Struct compositeKey(int lead, int id) {
    return new Struct(COMPOSITE_KEY).put("lead", lead).put("id", id);
  }

  private static SinkRecord tombstone(String table, Struct key) {
    return new SinkRecord("dsqlcdc." + table, 0, key.schema(), key, null, null, offset++);
  }

  // --- The JDBC double --------------------------------------------------------------

  /** One executed statement: its SQL and its binds, in order. */
  record Executed(String sql, List<Object> binds) {}

  /**
   * A stateful double of {@code app.orders} (PK (lead, id), UNIQUE (id) unless
   * {@link #uniqueSourceKey} is off); any other table's writes are only recorded. {@link #calls}
   * logs prepare/update/batch/commit/rollback/close; {@link #executed} every statement with its
   * binds. {@link #failure} fails a statement by (SQL, binds), {@code null} = it runs.
   */
  private static final class FakeDsql {
    final List<String> calls = new ArrayList<>();
    final List<Executed> executed = new ArrayList<>();
    private List<Map<String, Object>> committed = new ArrayList<>();
    private List<Map<String, Object>> working = new ArrayList<>();
    BiFunction<String, List<Object>, SQLException> failure = (sql, binds) -> null;
    // Off: no unique index on the source key (not built yet, or an edited DDL without one).
    boolean uniqueSourceKey = true;
    // G22: the next N commits answer Aurora DSQL's XX000 "server unavailable" -- after
    // committing when unavailableCommitLands (the documented uncertain outcome), else after
    // ending the transaction with nothing applied.
    int commitsUnavailable;
    boolean unavailableCommitLands;

    private static final Pattern WHERE_COLUMN = Pattern.compile("\"([^\"]+)\" = \\?");
    private static final Pattern INSERT_COLUMNS =
        Pattern.compile("^INSERT INTO \\S+ \\(([^)]*)\\)");
    private static final Pattern CONFLICT_COLUMNS = Pattern.compile("ON CONFLICT \\(([^)]*)\\)");

    void seed(int lead, int id, String v) {
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("lead", lead);
      r.put("id", id);
      r.put("v", v);
      committed.add(r);
      working = copy(committed);
    }

    /** The committed rows of app.orders as [lead, id, v], in insertion order. */
    List<List<Object>> rows() {
      List<List<Object>> out = new ArrayList<>();
      for (Map<String, Object> r : committed) {
        out.add(java.util.Arrays.asList(r.get("lead"), r.get("id"), r.get("v")));
      }
      return out;
    }

    int count(String call) {
      return (int) calls.stream().filter(call::equals).count();
    }

    private static List<Map<String, Object>> copy(List<Map<String, Object>> rows) {
      List<Map<String, Object>> out = new ArrayList<>();
      for (Map<String, Object> r : rows) {
        out.add(new LinkedHashMap<>(r));
      }
      return out;
    }

    private static List<String> names(String quotedList) {
      List<String> out = new ArrayList<>();
      Matcher m = Pattern.compile("\"([^\"]+)\"").matcher(quotedList);
      while (m.find()) {
        out.add(m.group(1));
      }
      return out;
    }

    private static boolean matches(Map<String, Object> row, Map<String, Object> wanted) {
      for (Map.Entry<String, Object> e : wanted.entrySet()) {
        if (!java.util.Objects.equals(row.get(e.getKey()), e.getValue())) {
          return false;
        }
      }
      return true;
    }

    private static Map<String, Object> project(Map<String, Object> row, List<String> cols) {
      Map<String, Object> out = new LinkedHashMap<>();
      for (String c : cols) {
        out.put(c, row.get(c));
      }
      return out;
    }

    private void apply(String sql, List<Object> binds) throws SQLException {
      executed.add(new Executed(sql, List.copyOf(binds)));
      SQLException e = failure.apply(sql, binds);
      if (e != null) {
        throw e;
      }
      if (!sql.contains("\"orders\"")) {
        return; // another table: recorded only
      }
      if (sql.startsWith("DELETE")) {
        // WHERE k = ? [AND NOT (c = ? ...)]: the row matched by k, unless it holds every c.
        String where = sql.substring(sql.indexOf(" WHERE "));
        int not = where.indexOf(" AND NOT (");
        Matcher m = WHERE_COLUMN.matcher(not < 0 ? where : where.substring(0, not));
        Map<String, Object> wanted = new LinkedHashMap<>();
        int i = 0;
        while (m.find()) {
          wanted.put(m.group(1), binds.get(i++));
        }
        Map<String, Object> spared = new LinkedHashMap<>();
        if (not >= 0) {
          Matcher u = WHERE_COLUMN.matcher(where.substring(not));
          while (u.find()) {
            spared.put(u.group(1), binds.get(i++));
          }
        }
        working.removeIf(r -> matches(r, wanted) && (spared.isEmpty() || !matches(r, spared)));
        return;
      }
      Matcher cols = INSERT_COLUMNS.matcher(sql);
      Matcher conflict = CONFLICT_COLUMNS.matcher(sql);
      assertTrue(cols.find() && conflict.find(), sql);
      List<String> columns = names(cols.group(1));
      List<String> arbiter = names(conflict.group(1));
      Map<String, Object> row = new LinkedHashMap<>();
      for (int i = 0; i < columns.size(); i++) {
        row.put(columns.get(i), binds.get(i));
      }
      Map<String, Object> key = project(row, arbiter);
      for (Map<String, Object> existing : working) {
        if (matches(existing, key)) { // ON CONFLICT (arbiter) DO UPDATE
          for (String c : columns) {
            if (!arbiter.contains(c)) {
              existing.put(c, row.get(c));
            }
          }
          return;
        }
      }
      List<List<String>> uniques =
          uniqueSourceKey
              ? List.of(List.of("lead", "id"), List.of("id"))
              : List.of(List.of("lead", "id"));
      for (List<String> unique : uniques) {
        Map<String, Object> u = project(row, unique);
        for (Map<String, Object> existing : working) {
          if (matches(existing, u)) {
            throw new SQLException(
                "duplicate key value violates unique constraint \"ux_orders_id\"", "23505");
          }
        }
      }
      working.add(row);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
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
                  calls.add("update");
                  for (List<Object> binds : batch.isEmpty() ? List.of(current) : batch) {
                    apply(sql, binds);
                  }
                  yield 1;
                }
                case "executeBatch" -> {
                  calls.add("batch");
                  for (List<Object> binds : batch) {
                    apply(sql, binds);
                  }
                  yield new int[batch.size()];
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
                  calls.add("prepare");
                  yield statement((String) a[0]);
                }
                case "commit" -> {
                  if (commitsUnavailable > 0) {
                    commitsUnavailable--;
                    calls.add("commit!XX000");
                    if (unavailableCommitLands) {
                      committed = copy(working); // it committed; the client got an error
                    } else {
                      working = copy(committed); // the failed COMMIT ended the transaction
                    }
                    throw new SQLException("server unavailable", "XX000");
                  }
                  calls.add("commit");
                  committed = copy(working);
                  yield null;
                }
                case "rollback" -> {
                  calls.add("rollback");
                  working = copy(committed);
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

  /**
   * A task with an injected clock, no guarded target columns, a scripted unique-index read
   * ({@code Boolean} or an {@code SQLException} to throw; the last answer repeats) and no
   * CloudWatch.
   */
  private static final class RekeyTask extends DsqlSinkTask {
    long now = 1_000 * SECOND;
    final Deque<Object> indexAnswers = new ArrayDeque<>();
    int indexLoads;

    RekeyTask index(Object... answers) {
      indexAnswers.addAll(List.of(answers));
      return this;
    }

    @Override
    long nanoTime() {
      return now;
    }

    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }

    @Override
    boolean loadSourceKeyIndex(String schema, String table, List<String> columns)
        throws SQLException {
      indexLoads++;
      assertEquals("app", schema);
      assertEquals("orders", table);
      assertEquals(List.of("id"), columns);
      Object next = indexAnswers.size() > 1 ? indexAnswers.poll() : indexAnswers.peek();
      if (next instanceof SQLException e) {
        throw e;
      }
      return Boolean.TRUE.equals(next);
    }

    @Override
    void emitMetrics() {
      // never CloudWatch in a unit test
    }
  }

  private static Map<String, String> props(String targetKeys, int batchSize) {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.BATCH_SIZE, String.valueOf(batchSize));
    props.put(DsqlSinkConnectorConfig.METRICS_STACK, "test-stack");
    if (targetKeys != null) {
      props.put(DsqlSinkConnectorConfig.TARGET_KEY_COLUMNS, targetKeys);
    }
    return props;
  }

  private static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  @SuppressWarnings("unchecked")
  private static long applied(DsqlSinkTask task, String counter, String table) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(counter);
    field.setAccessible(true);
    LongAdder adder = ((Map<String, LongAdder>) field.get(task)).get(table);
    return adder == null ? 0L : adder.sum();
  }

  private static RekeyTask task(FakeDsql dsql, RecordingReporter dlq, String targetKeys)
      throws Exception {
    return task(dsql, dlq, targetKeys, 1000);
  }

  private static RekeyTask task(
      FakeDsql dsql, RecordingReporter dlq, String targetKeys, int batchSize) throws Exception {
    RekeyTask task = new RekeyTask();
    task.startMetrics(props(targetKeys, batchSize));
    setField(task, "connection", dsql.connection());
    if (dlq != null) {
      setField(task, "dlqReporter", dlq);
    }
    return task;
  }

  /** The UPDATE of the leading column: key {id:2}, before (5,2,a), after (9,2,b). */
  private static SinkRecord leadUpdate() {
    return record(ORDERS, sourceKey(2), "u", row(5, 2, "a"), row(9, 2, "b"));
  }

  // --- The move -------------------------------------------------------------------------------

  @Test
  void aMoveDeletesTheOldKeyAndInsertsTheNewOneInOneTransaction() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false);

    task.put(List.of(leadUpdate()));

    // Today (no source keying) the record parses as an upsert ON CONFLICT ("id") -- a key no
    // primary key matches -- so this fails without the fix.
    assertEquals(
        List.of(
            new Executed(DELETE_EXACT, List.of(5, 2)),
            new Executed(UPSERT_COMPOSITE, List.of(9, 2, "b"))),
        dsql.executed);
    assertEquals(1, dsql.count("commit"));
    assertEquals(0, dsql.count("rollback"));
    assertEquals(List.of(List.of(9, 2, "b")), dsql.rows());
    // A move counts ONCE, as an update: it replicates one source UPDATE.
    assertEquals(1L, applied(task, "updatesByTable", ORDERS));
    assertEquals(0L, applied(task, "deletesByTable", ORDERS));
    assertEquals(0L, applied(task, "insertsByTable", ORDERS));
    assertEquals(1, task.indexLoads);
  }

  @Test
  void aMoveDeletesBySourceKeyOnceTheIndexIsValid() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);

    task.put(List.of(leadUpdate()));

    assertEquals(
        List.of(
            new Executed(DELETE_BY_SOURCE_KEY, List.of(2)),
            new Executed(UPSERT_COMPOSITE, List.of(9, 2, "b"))),
        dsql.executed);
    assertEquals(List.of(List.of(9, 2, "b")), dsql.rows());
  }

  @Test
  void aMoveAppliesEvenWithDeletesDisabled() throws Exception {
    // delete.enabled=false drops DELETE events; a move is an UPDATE, so both halves run.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = new RekeyTask().index(false);
    Map<String, String> props = props(TARGET_KEYS, 1000);
    props.put(DsqlSinkConnectorConfig.DELETE_ENABLED, "false");
    task.startMetrics(props);
    setField(task, "connection", dsql.connection());

    task.put(List.of(leadUpdate(), record(ORDERS, sourceKey(3), "d", row(5, 3, "z"), null)));

    assertEquals(
        List.of(
            new Executed(DELETE_EXACT, List.of(5, 2)),
            new Executed(UPSERT_COMPOSITE, List.of(9, 2, "b"))),
        dsql.executed);
  }

  @Test
  void aFailingMoveRollsBackItsDelete() throws Exception {
    // The insert half fails with a permanent error: the delete half must not survive.
    BiFunction<String, List<Object>, SQLException> failNewRow =
        (sql, binds) ->
            sql.startsWith("INSERT") && sql.contains("\"orders\"") && binds.get(0).equals(9)
                ? new SQLException("duplicate key value violates unique constraint", "23505")
                : null;

    // A single-record chunk: the chunk IS the row, quarantined after one rollback.
    FakeDsql single = new FakeDsql();
    single.seed(5, 2, "a");
    single.failure = failNewRow;
    RecordingReporter dlq = new RecordingReporter();
    RekeyTask task = task(single, dlq, TARGET_KEYS).index(false);
    task.put(List.of(leadUpdate()));
    assertEquals(List.of(List.of(5, 2, "a")), single.rows());
    assertEquals(0, single.count("commit"));
    assertEquals(1, dlq.records.size());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());

    // A mixed chunk: replayed one row per transaction, where the move's delete and insert
    // share ONE transaction -- the healthy row commits, the move rolls back as a whole.
    FakeDsql mixed = new FakeDsql();
    mixed.seed(5, 2, "a");
    mixed.failure = failNewRow;
    RecordingReporter dlq2 = new RecordingReporter();
    RekeyTask task2 = task(mixed, dlq2, TARGET_KEYS).index(false);
    SinkRecord insert = record(ORDERS, sourceKey(7), "c", null, row(1, 7, "n"));
    task2.put(List.of(insert, leadUpdate()));
    assertEquals(List.of(List.of(5, 2, "a"), List.of(1, 7, "n")), mixed.rows());
    assertEquals(1, dlq2.records.size());
    // The replayed move: delete then insert on one connection, then ONE rollback, no commit.
    List<String> tail = mixed.calls.subList(mixed.calls.size() - 5, mixed.calls.size());
    assertEquals(List.of("prepare", "update", "prepare", "update", "rollback"), tail);

    // The reason (the log line the control plane parses) names the upsert and the op.
    FakeDsql loud = new FakeDsql();
    loud.seed(5, 2, "a");
    loud.failure = failNewRow;
    RekeyTask noDlq = task(loud, null, TARGET_KEYS).index(false);
    ConnectException e =
        assertThrows(ConnectException.class, () -> noDlq.put(List.of(leadUpdate())));
    assertTrue(e.getMessage().contains("sqlstate=23505"), e.getMessage());
    assertTrue(e.getMessage().contains("op: u"), e.getMessage());
    assertTrue(e.getMessage().contains(" | sql: INSERT INTO \"app\".\"orders\""), e.getMessage());
    assertTrue(e.getMessage().contains("pk: lead=9, id=2"), e.getMessage());
  }

  @Test
  void aMoveCountsOnceAsAnUpdate() throws Exception {
    // Also on the one-row-per-transaction path (a chunk with a poison row is replayed).
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    dsql.failure =
        (sql, binds) ->
            sql.contains("\"items\"") ? new SQLException("check violation", "23514") : null;
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false);
    Schema itemKey = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    SinkRecord poison = record(ITEMS, new Struct(itemKey).put("id", 1), "c", null, row(0, 1, "x"));

    task.put(List.of(poison, leadUpdate()));

    assertEquals(List.of(List.of(9, 2, "b")), dsql.rows());
    assertEquals(1L, applied(task, "updatesByTable", ORDERS));
    assertEquals(0L, applied(task, "deletesByTable", ORDERS));
  }

  @Test
  void movesWeighTwoRows() throws Exception {
    // 2,999 upserts and one move modify 3,001 rows: one chunk of them would break Aurora DSQL's
    // 3,000-row transaction limit, so the move goes to a second chunk (one commit each).
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS, 3000).index(false);
    Schema itemKey = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    List<SinkRecord> put = new ArrayList<>();
    for (int i = 0; i < 2999; i++) {
      put.add(record(ITEMS, new Struct(itemKey).put("id", i), "c", null, row(0, i, "x")));
    }
    put.add(leadUpdate());

    task.put(put);

    assertEquals(2, dsql.count("commit"));
    assertEquals(List.of(List.of(9, 2, "b")), dsql.rows());
  }

  @Test
  void wideMovesSplitUnderTheByteBudget() throws Exception {
    // Nine moves with ~1 MiB after-images modify ~9 MiB: put() sizes each by its after-image
    // and old key, so they commit in two transactions (8 + 1), not one over the 8 MiB budget.
    FakeDsql dsql = new FakeDsql();
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS, 3000).index(false);
    String wide = "x".repeat(1024 * 1024 - 1024);
    List<SinkRecord> put = new ArrayList<>();
    for (int id = 0; id < 9; id++) {
      dsql.seed(5, id, "a");
      put.add(record(ORDERS, sourceKey(id), "u", row(5, id, "a"), row(9, id, wide)));
    }

    task.put(put);

    assertEquals(2, dsql.count("commit"));
    assertEquals(9, dsql.rows().size());
    assertTrue(dsql.rows().stream().allMatch(r -> r.get(0).equals(9)), "every row moved");
  }

  @Test
  void everyMoveInABatchedChunkDeletesItsOwnOldRow() throws Exception {
    // Two moves of different rows, behind a plain insert and ahead of a later write of a moved
    // key, in ONE committed chunk. With no unique index on id (still building), a move whose
    // delete were dropped would leave its old row beside the new one: nothing would fail and
    // send the chunk to the row-by-row replay that repairs it.
    FakeDsql dsql = new FakeDsql();
    dsql.uniqueSourceKey = false;
    dsql.seed(5, 2, "a");
    dsql.seed(5, 3, "c");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false);

    task.put(
        List.of(
            record(ORDERS, sourceKey(7), "c", null, row(1, 7, "n")),
            leadUpdate(),
            record(ORDERS, sourceKey(3), "u", row(5, 3, "c"), row(9, 3, "d")),
            record(ORDERS, sourceKey(2), "u", row(9, 2, "b"), row(9, 2, "e"))));

    assertEquals(
        List.of(
            new Executed(UPSERT_COMPOSITE, List.of(1, 7, "n")),
            new Executed(DELETE_EXACT, List.of(5, 2)),
            new Executed(DELETE_EXACT, List.of(5, 3)),
            // The moves' upserts and the later write of (9, 2) are one same-SQL run, deduped
            // to the last image per key.
            new Executed(UPSERT_COMPOSITE, List.of(9, 2, "e")),
            new Executed(UPSERT_COMPOSITE, List.of(9, 3, "d"))),
        dsql.executed);
    assertEquals(1, dsql.count("commit"));
    assertEquals(0, dsql.count("rollback"));
    assertEquals(
        List.of(List.of(1, 7, "n"), List.of(9, 2, "e"), List.of(9, 3, "d")), dsql.rows());
  }

  @Test
  void distinctRowsMovedTogetherBatchTheirDeletesThenTheirUpserts() throws Exception {
    // A bulk UPDATE of the leading column: one batch of deletes, then one of upserts, in one
    // transaction -- not two single-row round trips per move.
    for (boolean indexValid : new boolean[] {false, true}) {
      FakeDsql dsql = new FakeDsql();
      List<SinkRecord> put = new ArrayList<>();
      List<Executed> deletes = new ArrayList<>();
      List<Executed> upserts = new ArrayList<>();
      for (int id = 0; id < 50; id++) {
        dsql.seed(5, id, "a");
        put.add(record(ORDERS, sourceKey(id), "u", row(5, id, "a"), row(9, id, "b")));
        deletes.add(
            indexValid
                ? new Executed(DELETE_BY_SOURCE_KEY, List.of(id))
                : new Executed(DELETE_EXACT, List.of(5, id)));
        upserts.add(new Executed(UPSERT_COMPOSITE, List.of(9, id, "b")));
      }
      RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS, 3000).index(indexValid);

      task.put(put);

      assertEquals(List.of("prepare", "batch", "prepare", "batch", "commit"), dsql.calls);
      List<Executed> expected = new ArrayList<>(deletes);
      expected.addAll(upserts);
      assertEquals(expected, dsql.executed);
      assertEquals(50, dsql.rows().size());
      assertTrue(dsql.rows().stream().allMatch(r -> r.get(0).equals(9) && r.get(2).equals("b")));
      assertEquals(50L, applied(task, "updatesByTable", ORDERS));
      assertEquals(0L, applied(task, "deletesByTable", ORDERS));
    }
  }

  @Test
  void theSameRowMovedTwiceInOnePutKeepsItsOrder() throws Exception {
    // id 2 moves 5 -> 9, then 9 -> 4, around a move of id 3 and its delete. The second move of
    // id 2 starts a new group: its delete runs after the first move's insert -- else, with no
    // unique index on id to reject the second insert, (9, 2) would survive beside (4, 2).
    FakeDsql dsql = new FakeDsql();
    dsql.uniqueSourceKey = false;
    dsql.seed(5, 2, "a");
    dsql.seed(5, 3, "c");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false);

    task.put(
        List.of(
            leadUpdate(),
            record(ORDERS, sourceKey(3), "u", row(5, 3, "c"), row(9, 3, "d")),
            record(ORDERS, sourceKey(2), "u", row(9, 2, "b"), row(4, 2, "x")),
            record(ORDERS, sourceKey(3), "d", row(9, 3, "d"), null),
            tombstone(ORDERS, sourceKey(3))));

    assertEquals(
        List.of(
            new Executed(DELETE_EXACT, List.of(5, 2)),
            new Executed(DELETE_EXACT, List.of(5, 3)),
            new Executed(UPSERT_COMPOSITE, List.of(9, 2, "b")),
            new Executed(UPSERT_COMPOSITE, List.of(9, 3, "d")),
            new Executed(DELETE_EXACT, List.of(9, 2)),
            new Executed(UPSERT_COMPOSITE, List.of(4, 2, "x")),
            new Executed(DELETE_EXACT, List.of(9, 3))),
        dsql.executed);
    assertEquals(List.of(List.of(4, 2, "x")), dsql.rows());
  }

  // --- An UPDATE with no before-image -----------------------------------------------------------

  /** An UPDATE of {@code id} with NO before-image (REPLICA IDENTITY DEFAULT), new image given. */
  private static SinkRecord blindUpdate(int lead, int id, String v) {
    return record(ORDERS, sourceKey(id), "u", null, row(lead, id, v));
  }

  @Test
  void anUpdateWithoutABeforeImageReplacesTheRowBySourceKeyOnceTheIndexIsValid() throws Exception {
    // The live REPLICA IDENTITY DEFAULT failure: UPDATE lead 9 -> 11 of id 3 arrives with no
    // before-image, so a plain upsert of (11, 3) hits the unique index on id beside (9, 3) --
    // 23505, dead-lettered, the old row kept. Once that index is valid the row holding id 3 is
    // deleted and the new image inserted, in one transaction. An UPDATE that left lead alone
    // (id 4) cannot be told apart in the record, so the delete spares a row already at the new
    // lead and the upsert updates it in place.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(9, 3, "r3");
    dsql.seed(9, 4, "r4");
    RecordingReporter dlq = new RecordingReporter();
    RekeyTask task = task(dsql, dlq, TARGET_KEYS).index(true);

    task.put(List.of(blindUpdate(11, 3, "r3"), blindUpdate(9, 4, "r4b")));

    assertEquals(
        List.of(
            new Executed(DELETE_UNLESS_AT, List.of(3, 11)),
            new Executed(DELETE_UNLESS_AT, List.of(4, 9)),
            new Executed(UPSERT_COMPOSITE, List.of(11, 3, "r3")),
            new Executed(UPSERT_COMPOSITE, List.of(9, 4, "r4b"))),
        dsql.executed);
    assertEquals(List.of("prepare", "batch", "prepare", "batch", "commit"), dsql.calls);
    assertEquals(List.of(List.of(9, 4, "r4b"), List.of(11, 3, "r3")), dsql.rows());
    assertTrue(dlq.records.isEmpty());
    // Each replicates one source UPDATE.
    assertEquals(2L, applied(task, "updatesByTable", ORDERS));
    assertEquals(0L, applied(task, "deletesByTable", ORDERS));
    assertEquals(0L, applied(task, "insertsByTable", ORDERS));
    assertEquals(1, task.indexLoads);
  }

  @Test
  void anUpdateWithoutABeforeImageStaysAPlainUpsertUntilTheIndexIsValid() throws Exception {
    // Without a valid unique index on id a delete by it would be a full scan: the UPDATE is
    // the plain upsert it always was (right whenever lead did not change).
    FakeDsql dsql = new FakeDsql();
    dsql.seed(9, 4, "r4");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false);

    task.put(List.of(blindUpdate(9, 4, "r4b")));

    assertEquals(List.of(new Executed(UPSERT_COMPOSITE, List.of(9, 4, "r4b"))), dsql.executed);
    assertEquals(List.of(List.of(9, 4, "r4b")), dsql.rows());
    assertEquals(1, task.indexLoads);
  }

  @Test
  void anUpdateWithoutABeforeImageWeighsTwoRowsOnlyOnceItIsAMove() throws Exception {
    // batch.size 2: two replaced UPDATEs modify four rows, so they commit one per chunk; as
    // plain upserts (index not valid) they are two rows, one chunk.
    for (boolean indexValid : new boolean[] {false, true}) {
      FakeDsql dsql = new FakeDsql();
      dsql.seed(9, 3, "a");
      dsql.seed(9, 4, "b");
      RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS, 2).index(indexValid);

      task.put(List.of(blindUpdate(9, 3, "c"), blindUpdate(9, 4, "d")));

      assertEquals(indexValid ? 2 : 1, dsql.count("commit"), "indexValid=" + indexValid);
      assertEquals(List.of(List.of(9, 3, "c"), List.of(9, 4, "d")), dsql.rows());
    }
  }

  @Test
  void anUpdateMissingAnUnchangedToastValueIsNeverReplaced() throws Exception {
    // v is an unchanged TOASTed value the reselector could not fill: Debezium's placeholder,
    // omitted from the image. A delete + re-insert would lose v; the partial upsert keeps it.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(9, 3, "big");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);

    task.put(List.of(blindUpdate(9, 3, DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER)));

    assertEquals(
        List.of(
            new Executed(
                "INSERT INTO \"app\".\"orders\" (\"lead\", \"id\") VALUES (?, ?) "
                    + "ON CONFLICT (\"lead\", \"id\") DO NOTHING",
                List.of(9, 3))),
        dsql.executed);
    assertEquals(List.of(List.of(9, 3, "big")), dsql.rows());
    assertEquals(0, task.indexLoads); // not a candidate: the index is never read for it
  }

  @Test
  void aReplaceKeepsAColumnTheImageDoesNotCarryUnlessTheRowMoves() throws Exception {
    // v is left out of capture (column.exclude.list drops it from the record), yet a row
    // already on Aurora DSQL holds a value there. An UPDATE that keeps lead must not delete
    // and re-insert that row -- it would lose v: the delete spares it and the upsert updates it
    // in place. A row that really moves is re-inserted, as every move is, without v.
    Schema narrow =
        SchemaBuilder.struct()
            .name("app.orders.Value")
            .field("lead", Schema.INT32_SCHEMA)
            .field("id", Schema.INT32_SCHEMA)
            .optional()
            .build();
    Schema envelope =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", narrow)
            .field("after", narrow)
            .field("source", SOURCE)
            .build();
    java.util.function.BiFunction<Integer, Integer, SinkRecord> narrowUpdate =
        (lead, id) -> {
          Struct value =
              new Struct(envelope)
                  .put("op", "u")
                  .put("after", new Struct(narrow).put("lead", lead).put("id", id))
                  .put(
                      "source",
                      new Struct(SOURCE)
                          .put("connector", "postgresql")
                          .put("schema", "app")
                          .put("table", "orders"));
          Struct key = sourceKey(id);
          return new SinkRecord(
              "dsqlcdc." + ORDERS, 0, key.schema(), key, envelope, value, offset++);
        };
    FakeDsql dsql = new FakeDsql();
    dsql.seed(9, 3, "kept");
    dsql.seed(9, 4, "moved");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);

    task.put(List.of(narrowUpdate.apply(9, 3), narrowUpdate.apply(11, 4)));

    assertEquals(
        java.util.Arrays.asList(
            List.of(9, 3, "kept"), java.util.Arrays.asList(11, 4, null)),
        dsql.rows());
  }

  @Test
  void theSameRowUpdatedTwiceWithoutABeforeImageKeepsItsOrder() throws Exception {
    // id 3: lead 9 -> 11 -> 12 in one put. The second UPDATE of the row starts a new group, so
    // its delete runs after the first insert -- else, with no unique index on id enforced
    // here to reject it, (11, 3) would survive beside (12, 3).
    FakeDsql dsql = new FakeDsql();
    dsql.uniqueSourceKey = false;
    dsql.seed(9, 3, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);

    task.put(List.of(blindUpdate(11, 3, "b"), blindUpdate(12, 3, "c")));

    assertEquals(
        List.of(
            new Executed(DELETE_UNLESS_AT, List.of(3, 11)),
            new Executed(UPSERT_COMPOSITE, List.of(11, 3, "b")),
            new Executed(DELETE_UNLESS_AT, List.of(3, 12)),
            new Executed(UPSERT_COMPOSITE, List.of(12, 3, "c"))),
        dsql.executed);
    assertEquals(List.of(List.of(12, 3, "c")), dsql.rows());
  }

  @Test
  void aReplacedUpdateKeepsBothHalvesOnTheRowByRowReplay() throws Exception {
    // A poison row sends the chunk to the one-row-per-transaction replay: the replaced UPDATE
    // runs its delete and its insert in one transaction there too.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(9, 3, "a");
    dsql.failure =
        (sql, binds) ->
            sql.contains("\"items\"") ? new SQLException("check violation", "23514") : null;
    RecordingReporter dlq = new RecordingReporter();
    RekeyTask task = task(dsql, dlq, TARGET_KEYS).index(true);
    Schema itemKey = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    SinkRecord poison = record(ITEMS, new Struct(itemKey).put("id", 1), "c", null, row(0, 1, "x"));

    task.put(List.of(poison, blindUpdate(11, 3, "b")));

    assertEquals(List.of(List.of(11, 3, "b")), dsql.rows());
    assertEquals(1, dlq.records.size());
    assertSame(poison, dlq.records.get(0));
    List<String> tail = dsql.calls.subList(dsql.calls.size() - 5, dsql.calls.size());
    assertEquals(List.of("prepare", "update", "prepare", "update", "commit"), tail);
    assertEquals(1L, applied(task, "updatesByTable", ORDERS));
  }

  /** A move of {@code table} keyed {column: key} (any value, null included), lead 5 -> 9. */
  private static DsqlSinkTask.Applicable move(String table, String column, Object key) {
    ChangeEvent upsert =
        ChangeEvent.upsert(
            table,
            List.of("lead", "id", "v"),
            java.util.Arrays.asList(9, key, "b"),
            List.of("lead", "id"),
            java.util.Arrays.asList(9, key),
            0L);
    return new DsqlSinkTask.Applicable(
        null,
        upsert
            .withSourceKey(List.of(column), java.util.Arrays.asList(key))
            .withMovedFrom(java.util.Arrays.asList(5, key)));
  }

  private static DsqlSinkTask.Applicable move(Object key) {
    return move(ORDERS, "id", key);
  }

  /** withMoveDeletes' output as letters: D a delete, M a move's upsert, o anything else. */
  private static String shape(DsqlSinkTask.Applicable... chunk) {
    StringBuilder out = new StringBuilder();
    for (DsqlSinkTask.Applicable a : DsqlSinkTask.withMoveDeletes(List.of(chunk))) {
      out.append(a.event().isDelete() ? 'D' : a.event().isMove() ? 'M' : 'o');
    }
    return out.toString();
  }

  @Test
  void aMoveGroupEndsAtAnythingThatCouldBeTheSameRow() {
    // Different rows: all the deletes, then all the upserts.
    assertEquals("DDDMMM", shape(move(1), move(2), move(3)));
    // The same row twice keeps its order -- also when the keys are equal only by value: bytes
    // by content, numbers by value (an INT key widened to BIGINT, a NUMERIC scale), text
    // without CHAR padding.
    assertEquals("DMDM", shape(move(2), move(2)));
    assertEquals("DMDM", shape(move(new byte[] {1, 2}), move(new byte[] {1, 2})));
    assertEquals("DDMM", shape(move(new byte[] {1, 2}), move(new byte[] {1, 3})));
    assertEquals("DMDM", shape(move(5), move(5L)));
    assertEquals(
        "DMDM",
        shape(move(new java.math.BigDecimal("5.0")), move(new java.math.BigDecimal("5.00"))));
    assertEquals("DDMM", shape(move(5), move(6L)));
    assertEquals("DMDM", shape(move("a"), move("a  ")));
    assertEquals("DDMM", shape(move("a"), move("b")));
    // A key of another kind (its column's type changed), a null component, other key columns,
    // another table, or anything that is not a move ends the group.
    assertEquals("DMDM", shape(move(5), move("6")));
    assertEquals("DMDM", shape(move(2), move(null)));
    assertEquals("DMDM", shape(move(null), move(3)));
    assertEquals("DMDM", shape(move(2), move(ORDERS, "code", 3)));
    assertEquals("DMDM", shape(move(2), move("app.other", "id", 3)));
    DsqlSinkTask.Applicable plain =
        new DsqlSinkTask.Applicable(
            null,
            ChangeEvent.upsert(
                ORDERS,
                List.of("lead", "id", "v"),
                List.of(1, 7, "n"),
                List.of("lead", "id"),
                List.of(1, 7),
                0L));
    assertEquals("DMoDM", shape(move(2), plain, move(3)));
    // No move at all: the chunk itself.
    List<DsqlSinkTask.Applicable> noMove = List.of(plain, plain);
    assertSame(noMove, DsqlSinkTask.withMoveDeletes(noMove));
  }

  @Test
  void movesConvergeToTheSourceInAnyPerRowOrder() throws Exception {
    // Random per-row histories -- inserts, moves, plain updates, deletes with tombstones,
    // re-inserts with another lead -- spread over partitions by source key (per-row order kept,
    // cross-row order shuffled), random batch sizes and put boundaries, with and without the
    // unique index on id: the target always ends equal to the source, with nothing
    // dead-lettered and no duplicate row.
    java.util.Random random = new java.util.Random(20261004L);
    for (int run = 0; run < 400; run++) {
      boolean unique = random.nextBoolean();
      boolean indexValid = unique && random.nextBoolean();
      int partitions = 1 + random.nextInt(3);
      FakeDsql dsql = new FakeDsql();
      dsql.uniqueSourceKey = unique;
      Map<Integer, int[]> source = new java.util.TreeMap<>(); // id -> {lead, version}
      for (int id = 0; id < 6; id++) {
        if (random.nextBoolean()) {
          int lead = random.nextInt(3);
          source.put(id, new int[] {lead, 0});
          dsql.seed(lead, id, "v0");
        }
      }
      List<List<SinkRecord>> byPartition = new ArrayList<>();
      for (int p = 0; p < partitions; p++) {
        byPartition.add(new ArrayList<>());
      }
      int version = 1;
      for (int op = 0, ops = 1 + random.nextInt(30); op < ops; op++) {
        int id = random.nextInt(6);
        List<SinkRecord> partition = byPartition.get(id % partitions);
        int[] now = source.get(id);
        if (now == null) {
          int lead = random.nextInt(3);
          partition.add(record(ORDERS, sourceKey(id), "c", null, row(lead, id, "v" + version)));
          source.put(id, new int[] {lead, version++});
          continue;
        }
        Struct before = row(now[0], id, "v" + now[1]);
        switch (random.nextInt(3)) {
          case 0 -> { // a move, with or without another change
            int lead = (now[0] + 1 + random.nextInt(2)) % 3;
            int v = random.nextBoolean() ? version++ : now[1];
            partition.add(record(ORDERS, sourceKey(id), "u", before, row(lead, id, "v" + v)));
            source.put(id, new int[] {lead, v});
          }
          case 1 -> { // a plain update
            partition.add(
                record(ORDERS, sourceKey(id), "u", before, row(now[0], id, "v" + version)));
            source.put(id, new int[] {now[0], version++});
          }
          default -> { // a delete and its tombstone
            partition.add(record(ORDERS, sourceKey(id), "d", before, null));
            partition.add(tombstone(ORDERS, sourceKey(id)));
            source.remove(id);
          }
        }
      }
      List<SinkRecord> stream = new ArrayList<>();
      byPartition.forEach(stream::addAll);
      RecordingReporter dlq = new RecordingReporter();
      RekeyTask task =
          task(dsql, dlq, TARGET_KEYS, 1 + random.nextInt(12)).index(indexValid);
      int cut = random.nextInt(stream.size() + 1);
      task.put(new ArrayList<>(stream.subList(0, cut)));
      task.put(new ArrayList<>(stream.subList(cut, stream.size())));

      List<List<Object>> want = new ArrayList<>();
      source.forEach((id, s) -> want.add(List.of(s[0], id, "v" + s[1])));
      List<List<Object>> got = new ArrayList<>(dsql.rows());
      got.sort(java.util.Comparator.comparing(r -> (Integer) r.get(1)));
      String context = "run " + run + " unique=" + unique + " valid=" + indexValid;
      assertEquals(want, got, context);
      assertTrue(dlq.records.isEmpty(), () -> context + " " + dlq.causes);
    }
  }

  @Test
  void anUnchangedKeyUpdateIsAPlainUpsert() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);

    task.put(List.of(record(ORDERS, sourceKey(2), "u", row(5, 2, "a"), row(5, 2, "c"))));

    assertEquals(List.of(new Executed(UPSERT_COMPOSITE, List.of(5, 2, "c"))), dsql.executed);
    assertEquals(List.of(List.of(5, 2, "c")), dsql.rows());
    assertEquals(0, task.indexLoads); // nothing to delete: no catalog read
  }

  // --- Deletes and tombstones ------------------------------------------------------------------

  @Test
  void aSourceKeyedDeleteUsesTheExactKeyUntilTheSourceKeyIndexIsValid() throws Exception {
    // The live REPLICA IDENTITY DEFAULT shape: key {id: 2}, the before-image's lead holds the
    // column DEFAULT (7), not the row's 5. The exact key matches nothing until the unique index
    // on id is valid; then the delete is keyed on id and removes the row.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    dsql.seed(5, 3, "b");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(false, true);
    long t0 = task.now;

    task.put(List.of(record(ORDERS, sourceKey(2), "d", row(7, 2, null), null)));
    assertEquals(List.of(new Executed(DELETE_EXACT, List.of(7, 2))), dsql.executed);
    assertEquals(2, dsql.rows().size()); // silently missed -- still below FULL, index building
    task.now = t0 + 59 * SECOND;
    task.put(List.of(record(ORDERS, sourceKey(3), "d", row(7, 3, null), null)));
    assertEquals(1, task.indexLoads); // within the 60 s not-valid TTL: no re-read

    dsql.executed.clear();
    task.now = t0 + 61 * SECOND;
    task.put(List.of(record(ORDERS, sourceKey(2), "d", row(7, 2, null), null)));
    assertEquals(List.of(new Executed(DELETE_BY_SOURCE_KEY, List.of(2))), dsql.executed);
    assertEquals(List.of(List.of(5, 3, "b")), dsql.rows());
    assertEquals(2, task.indexLoads);
    // Valid: trusted for 10 minutes.
    task.now = t0 + 61 * SECOND + TimeUnit.MINUTES.toNanos(9);
    task.put(List.of(record(ORDERS, sourceKey(3), "d", row(7, 3, null), null)));
    assertEquals(2, task.indexLoads);
    assertTrue(dsql.rows().isEmpty());
    // Every applied delete is counted, as for any table (the two misses included).
    assertEquals(4L, applied(task, "deletesByTable", ORDERS));
  }

  @Test
  void aSourceKeyedTombstoneIsNotApplied() throws Exception {
    FakeDsql dsql = new FakeDsql();
    RecordingReporter dlq = new RecordingReporter();
    RekeyTask task = task(dsql, dlq, TARGET_KEYS).index(true);

    task.put(List.of(tombstone(ORDERS, sourceKey(2))));

    assertTrue(dsql.calls.isEmpty(), dsql.calls.toString());
    assertTrue(dlq.records.isEmpty());
    assertEquals(0, task.indexLoads);
    // A delete and its tombstone: one statement.
    task.put(
        List.of(
            record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null),
            tombstone(ORDERS, sourceKey(2))));
    assertEquals(List.of(new Executed(DELETE_BY_SOURCE_KEY, List.of(2))), dsql.executed);
  }

  @Test
  void aLegacyKeyedRecordOfAListedTableAppliesAsToday() throws Exception {
    // A record still keyed on the composite key (an older pipeline's in-flight record) parses
    // exactly as before: same SQL, same binds, no index read, and its tombstone is applied.
    List<SinkRecord> put =
        List.of(
            record(ORDERS, compositeKey(5, 2), "c", null, row(5, 2, "a")),
            record(ORDERS, compositeKey(5, 2), "d", row(5, 2, "a"), null));
    List<SinkRecord> tombstoneOnly = List.of(tombstone(ORDERS, compositeKey(5, 2)));
    FakeDsql listed = new FakeDsql();
    RekeyTask withConfig = task(listed, new RecordingReporter(), TARGET_KEYS).index(true);
    withConfig.put(put);
    withConfig.put(tombstoneOnly);
    FakeDsql plain = new FakeDsql();
    RekeyTask without = task(plain, new RecordingReporter(), null);
    without.put(put);
    without.put(tombstoneOnly);

    assertEquals(plain.executed, listed.executed);
    assertEquals(plain.calls, listed.calls);
    assertEquals(
        List.of(
            "INSERT INTO \"app\".\"orders\" (\"lead\", \"id\", \"v\") VALUES (?, ?, ?) "
                + "ON CONFLICT (\"lead\", \"id\") DO UPDATE SET \"v\" = EXCLUDED.\"v\"",
            DELETE_EXACT,
            DELETE_EXACT),
        listed.executed.stream().map(Executed::sql).toList());
    assertEquals(0, withConfig.indexLoads);
  }

  @Test
  void anUnlistedTableRendersTheSameSqlAsBefore() throws Exception {
    Schema itemKey = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
    List<SinkRecord> put =
        List.of(
            record(ITEMS, new Struct(itemKey).put("id", 1), "c", null, row(0, 1, "x")),
            record(ITEMS, new Struct(itemKey).put("id", 1), "u", row(0, 1, "x"), row(4, 1, "y")),
            record(ITEMS, new Struct(itemKey).put("id", 2), "d", row(0, 2, "z"), null),
            tombstone(ITEMS, new Struct(itemKey).put("id", 2)));
    FakeDsql none = new FakeDsql();
    task(none, new RecordingReporter(), "").put(put);
    FakeDsql other = new FakeDsql();
    RekeyTask otherTask = task(other, new RecordingReporter(), TARGET_KEYS).index(true);
    otherTask.put(put);
    FakeDsql absent = new FakeDsql();
    task(absent, new RecordingReporter(), null).put(put);

    assertEquals(absent.executed, none.executed);
    assertEquals(absent.calls, none.calls);
    assertEquals(absent.executed, other.executed);
    assertEquals(absent.calls, other.calls);
    // The create and update dedupe to one upsert, the delete and its tombstone to one delete.
    assertEquals(2, other.executed.size());
    assertEquals(0, otherTask.indexLoads);
  }

  // --- The unique-index read ------------------------------------------------------------------

  @Test
  void aTransientIndexReadRedeliversAndAPermanentOneFallsBackToTheExactKey() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task =
        task(dsql, new RecordingReporter(), TARGET_KEYS)
            .index(new SQLException("This connection has been closed."));
    SinkRecord delete = record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null);
    RetriableException redelivered =
        assertThrows(RetriableException.class, () -> task.put(List.of(delete)));
    assertInstanceOf(SQLException.class, redelivered.getCause());
    assertTrue(dsql.calls.contains("close"), dsql.calls.toString()); // connection discarded
    assertTrue(dsql.executed.isEmpty()); // nothing written: Connect redelivers the batch

    FakeDsql next = new FakeDsql();
    next.seed(5, 2, "a");
    RekeyTask denied =
        task(next, new RecordingReporter(), TARGET_KEYS)
            .index(new SQLException("permission denied for table pg_index", "42501"));
    denied.put(List.of(delete));
    assertEquals(List.of(new Executed(DELETE_EXACT, List.of(5, 2))), next.executed);
    assertTrue(next.rows().isEmpty()); // FULL before-image: the exact key finds the row
    assertEquals(Set.of(ORDERS), denied.sourceKeyWarnedTables());
    // Retried after 60 s, still one WARN for the table.
    denied.now += 61 * SECOND;
    denied.put(List.of(record(ORDERS, sourceKey(3), "d", row(5, 3, "b"), null)));
    assertEquals(2, denied.indexLoads);
    assertEquals(Set.of(ORDERS), denied.sourceKeyWarnedTables());
  }

  @Test
  void aServerUnavailableIndexReadIsRetriedAndASustainedOneRedelivers() throws Exception {
    // G22: an XX000 on the unique-index read is retried like a 40001, and one that never
    // clears is redelivered. FAILS WITHOUT G22: it counted as NOT VALID for 60 s, so this
    // REPLICA IDENTITY DEFAULT delete (the before-image's lead is the column DEFAULT, 7) was
    // keyed on the exact composite key and silently matched nothing.
    SQLException unavailable = new SQLException("server unavailable", "XX000");
    SinkRecord delete = record(ORDERS, sourceKey(2), "d", row(7, 2, null), null);
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task =
        task(dsql, new RecordingReporter(), TARGET_KEYS).index(unavailable, true);
    task.put(List.of(delete));
    assertEquals(List.of(new Executed(DELETE_BY_SOURCE_KEY, List.of(2))), dsql.executed);
    assertTrue(dsql.rows().isEmpty());
    assertEquals(2, task.indexLoads);
    assertTrue(task.sourceKeyWarnedTables().isEmpty());

    FakeDsql next = new FakeDsql();
    next.seed(5, 2, "a");
    RekeyTask sustained = task(next, new RecordingReporter(), TARGET_KEYS).index(unavailable);
    RetriableException e =
        assertThrows(RetriableException.class, () -> sustained.put(List.of(delete)));
    assertEquals("XX000", ((SQLException) e.getCause()).getSQLState());
    assertTrue(next.executed.isEmpty()); // nothing written: Connect redelivers the batch
    assertEquals(10, sustained.indexLoads); // the occ.max.retries budget
    assertTrue(sustained.sourceKeyWarnedTables().isEmpty()); // never "deletes are off"
  }

  @Test
  void theWarningNamesTheTableAndSqlstateOnly() {
    String line = DsqlSinkTask.SOURCE_KEY_INDEX_WARN;
    assertEquals(
        "Source-key deletes are off for table {}: its Aurora DSQL unique indexes could not be "
            + "read (sqlstate={}); its deletes use the composite primary key and the read is "
            + "retried every 60 s",
        line);
    assertEquals(2, line.split("\\{\\}", -1).length - 1);
  }

  @Test
  void a42P01RejectionInvalidatesTheCachedIndexState() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);
    task.put(List.of(record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null)));
    assertEquals(1, task.indexLoads);

    // The target table was dropped and recreated: Aurora DSQL rejects a write with 42P01.
    dsql.failure =
        (sql, binds) ->
            sql.startsWith("INSERT") ? new SQLException("relation does not exist", "42P01") : null;
    task.put(List.of(record(ORDERS, sourceKey(4), "c", null, row(5, 4, "d"))));
    dsql.failure = (sql, binds) -> null;
    task.put(List.of(record(ORDERS, sourceKey(3), "d", row(5, 3, "b"), null)));
    assertEquals(2, task.indexLoads); // re-read well within the 10-minute TTL
  }

  @Test
  void aCachedIndexStateIsReReadForADifferentSourceKey() {
    // The source primary key changed while CDC runs: records once keyed {id} are now keyed
    // {lead}, still a strict subset of (lead, id). The VALID state cached for the index on {id}
    // must not answer for {lead}, which has no unique index of its own: the delete would be
    // keyed on a column set no valid unique index covers (every row sharing the value, or a
    // scan when no index leads with it).
    List<List<String>> loads = new ArrayList<>();
    SourceKeyIndex index =
        new SourceKeyIndex(
            (table, columns) -> {
              loads.add(List.copyOf(columns));
              return columns.equals(List.of("id"));
            });
    assertTrue(index.valid(ORDERS, List.of("id"), 0));
    assertTrue(index.valid(ORDERS, List.of("id"), SECOND)); // cached
    assertFalse(index.valid(ORDERS, List.of("lead"), 2 * SECOND)); // well inside the VALID TTL
    assertEquals(List.of(List.of("id"), List.of("lead")), loads);
  }

  @Test
  void loadSourceKeyIndexReadsTheNamedTableAndMatchesTheColumnSet() throws Exception {
    // The real catalog read over a result-set double: bound schema then table, committed
    // before any chunk transaction; valid only for a VALID unique index on exactly {id}.
    List<String> calls = new ArrayList<>();
    Map<Integer, Object> params = new HashMap<>();
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[] {"orders_pkey", true, new String[] {"lead", "id"}});
    rows.add(new Object[] {"ux_orders_id", false, new String[] {"id"}}); // still building
    Connection conn = catalogConnection(calls, params, rows);
    DsqlSinkTask task = new DsqlSinkTask();
    setField(task, "connection", conn);
    assertFalse(task.loadSourceKeyIndex("app", "Orders", List.of("id")));
    assertEquals("app", params.get(1));
    assertEquals("Orders", params.get(2));
    assertEquals(List.of("prepare:" + SourceKeyIndex.CATALOG_SQL, "commit"), calls);

    rows.set(1, new Object[] {"ux_orders_id", true, new String[] {"id"}});
    assertTrue(task.loadSourceKeyIndex("app", "Orders", List.of("id")));
    // A set match, not an order match; a different set never matches.
    assertTrue(SourceKeyIndex.validOn(true, new String[] {"b", "a"}, Set.of("a", "b")));
    assertFalse(SourceKeyIndex.validOn(true, new String[] {"a"}, Set.of("a", "b")));
    assertFalse(SourceKeyIndex.validOn(false, new String[] {"a"}, Set.of("a")));
    assertFalse(SourceKeyIndex.validOn(true, null, Set.of("a")));
  }

  private static Connection catalogConnection(
      List<String> calls, Map<Integer, Object> params, List<Object[]> rows) {
    return FakeDsql.proxy(
        Connection.class,
        (self, m, a) ->
            switch (m.getName()) {
              case "isClosed" -> false;
              case "isValid" -> true;
              case "prepareStatement" -> {
                calls.add("prepare:" + a[0]);
                yield FakeDsql.proxy(
                    PreparedStatement.class,
                    (ps, pm, pa) ->
                        switch (pm.getName()) {
                          case "setString" -> {
                            params.put((Integer) pa[0], pa[1]);
                            yield null;
                          }
                          case "executeQuery" -> resultSet(rows);
                          case "close" -> null;
                          default -> throw new UnsupportedOperationException(pm.getName());
                        });
              }
              case "commit", "rollback" -> {
                calls.add(m.getName());
                yield null;
              }
              default -> throw new UnsupportedOperationException(m.getName());
            });
  }

  private static ResultSet resultSet(List<Object[]> rows) {
    java.util.Iterator<Object[]> it = new ArrayList<>(rows).iterator();
    Object[][] current = new Object[1][];
    return FakeDsql.proxy(
        ResultSet.class,
        (self, m, a) ->
            switch (m.getName()) {
              case "next" -> {
                current[0] = it.hasNext() ? it.next() : null;
                yield current[0] != null;
              }
              case "getBoolean" -> (Boolean) current[0][(int) a[0] - 1];
              case "getArray" -> {
                Object[] names = (Object[]) current[0][(int) a[0] - 1];
                yield FakeDsql.proxy(
                    java.sql.Array.class,
                    (arr, am, aa) ->
                        switch (am.getName()) {
                          case "getArray" -> names;
                          default -> throw new UnsupportedOperationException(am.getName());
                        });
              }
              case "close" -> null;
              default -> throw new UnsupportedOperationException(m.getName());
            });
  }

  @Test
  void aNullSourceKeyComponentNeverBecomesTheDeleteKey() throws Exception {
    // A `col = NULL` predicate matches nothing: such a delete keeps the exact composite key.
    Schema nullableKey =
        SchemaBuilder.struct()
            .name("app.orders.Key")
            .field("id", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    RekeyTask task = task(dsql, new RecordingReporter(), TARGET_KEYS).index(true);
    Struct nullKey = new Struct(nullableKey).put("id", null);
    task.put(List.of(record(ORDERS, nullKey, "d", row(5, 2, "a"), null)));
    assertEquals(List.of(new Executed(DELETE_EXACT, List.of(5, 2))), dsql.executed);
    assertEquals(0, task.indexLoads);
    ChangeEvent parsed =
        DebeziumEvents.parse(
            record(ORDERS, nullKey, "d", row(5, 2, "a"), null),
            Map.of(ORDERS, List.of("lead", "id")));
    assertNull(parsed.sourceKeyValues().get(0));
  }

  // --- A case-only twin of a re-keyed table ---------------------------------------------------

  private static final Schema TWIN_ROW =
      SchemaBuilder.struct()
          .name("app.Orders.Value")
          .field("id", Schema.INT32_SCHEMA)
          .field("v", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema TWIN_ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("before", TWIN_ROW)
          .field("after", TWIN_ROW)
          .field("source", SOURCE)
          .build();

  /** A change record of app."Orders" -- PK (id), no `lead`, NOT re-keyed -- keyed {id}. */
  private static SinkRecord twinRecord(String op, Struct before, Struct after) {
    Struct envelope =
        new Struct(TWIN_ENVELOPE)
            .put("op", op)
            .put(
                "source",
                new Struct(SOURCE)
                    .put("connector", "postgresql")
                    .put("schema", "app")
                    .put("table", "Orders"));
    if (before != null) {
      envelope.put("before", before);
    }
    if (after != null) {
      envelope.put("after", after);
    }
    return new SinkRecord(
        "dsqlcdc.app.Orders", 0, SOURCE_KEY, sourceKey(1), TWIN_ENVELOPE, envelope, offset++);
  }

  @Test
  void anUnlistedCaseTwinIsWrittenExactlyAsWithoutTargetKeys() throws Exception {
    // app.Orders differs from the listed app.orders only in case (PostgreSQL; MySQL with
    // lower_case_table_names=0): a different table, so it is written by its own key.
    Struct a = new Struct(TWIN_ROW).put("id", 1).put("v", "a");
    Struct b = new Struct(TWIN_ROW).put("id", 1).put("v", "b");
    List<SinkRecord> records =
        List.of(
            twinRecord("c", null, a),
            twinRecord("u", a, b),
            twinRecord("d", b, null),
            tombstone("app.Orders", sourceKey(1)));

    FakeDsql plain = new FakeDsql();
    RecordingReporter plainDlq = new RecordingReporter();
    task(plain, plainDlq, null).put(records);
    FakeDsql listed = new FakeDsql();
    RecordingReporter dlq = new RecordingReporter();
    RekeyTask rekeyed = task(listed, dlq, TARGET_KEYS);
    rekeyed.put(records);

    assertTrue(plainDlq.records.isEmpty());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertFalse(plain.executed.isEmpty());
    assertEquals(plain.executed, listed.executed);
    assertEquals(0, rekeyed.indexLoads);
  }

  // --- Below REPLICA IDENTITY FULL ------------------------------------------------------------

  @Test
  void aLeadingColumnUpdateWithoutABeforeImageIsDeadLetteredUntilTheIndexIsValid()
      throws Exception {
    // PostgreSQL below FULL sends no old tuple for an UPDATE that leaves the source key alone,
    // so an UPDATE of `lead` alone arrives as an upsert of the new composite key. Until the
    // unique index on id is valid it stays one, which that index rejects (23505) while the old
    // row exists: dead-lettered, the old row kept -- not a silent miss. Once valid, the row
    // holding id 4 is replaced (anUpdateWithoutABeforeImageReplacesTheRowBySourceKey...). The
    // composite-keyed path of CDC infrastructure deployed by an earlier version (Debezium keys
    // it on the new composite key) is dead-lettered the same way.
    for (boolean indexValid : new boolean[] {false, true}) {
      FakeDsql dsql = new FakeDsql();
      dsql.seed(5, 4, "a");
      RecordingReporter dlq = new RecordingReporter();
      task(dsql, dlq, TARGET_KEYS)
          .index(indexValid)
          .put(List.of(record(ORDERS, sourceKey(4), "u", null, row(9, 4, "b"))));
      if (indexValid) {
        assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
        assertEquals(List.of(List.of(9, 4, "b")), dsql.rows());
      } else {
        assertEquals(1, dlq.records.size());
        assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
        assertEquals(List.of(List.of(5, 4, "a")), dsql.rows());
      }
    }
    FakeDsql legacy = new FakeDsql();
    legacy.seed(5, 4, "a");
    RecordingReporter dlq = new RecordingReporter();
    task(legacy, dlq, null)
        .put(List.of(record(ORDERS, compositeKey(9, 4), "u", null, row(9, 4, "b"))));
    assertEquals(1, dlq.records.size());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertEquals(List.of(List.of(5, 4, "a")), legacy.rows());
  }

  // --- G22: an XX000 at COMMIT (Aurora DSQL's uncertain outcome) ------------------------------
  //
  // Aurora DSQL documents that a transaction whose COMMIT answered XX000 "server unavailable"
  // MIGHT have committed. The sink re-runs the whole transaction either way, so every write
  // shape must end in the same rows whether that first COMMIT landed or not.

  /**
   * Runs {@code put} with its first COMMIT answering XX000, once after it landed and once after
   * it did not, and checks both end at {@code expected}, nothing dead-lettered, one re-run.
   */
  private static void assertConvergesThroughAnUncertainCommit(
      boolean indexValid,
      java.util.function.Consumer<FakeDsql> seed,
      List<SinkRecord> put,
      List<List<Object>> expected)
      throws Exception {
    for (boolean landed : new boolean[] {true, false}) {
      FakeDsql dsql = new FakeDsql();
      seed.accept(dsql);
      dsql.commitsUnavailable = 1;
      dsql.unavailableCommitLands = landed;
      RecordingReporter dlq = new RecordingReporter();
      task(dsql, dlq, TARGET_KEYS).index(indexValid).put(put);
      String context = "landed=" + landed + " indexValid=" + indexValid;
      assertEquals(expected, dsql.rows(), context);
      assertTrue(dlq.records.isEmpty(), () -> context + " " + dlq.causes);
      assertEquals(1, dsql.count("commit!XX000"), context);
      assertEquals(1, dsql.count("commit"), context);
    }
  }

  @Test
  void g22anUpsertConvergesThroughAnUncertainCommit() throws Exception {
    assertConvergesThroughAnUncertainCommit(
        false,
        dsql -> dsql.seed(5, 2, "a"),
        List.of(
            record(ORDERS, sourceKey(4), "c", null, row(5, 4, "n")),
            record(ORDERS, sourceKey(2), "u", row(5, 2, "a"), row(5, 2, "b"))),
        List.of(List.of(5, 2, "b"), List.of(5, 4, "n")));
  }

  @Test
  void g22deletesConvergeThroughAnUncertainCommit() throws Exception {
    for (boolean indexValid : new boolean[] {false, true}) {
      assertConvergesThroughAnUncertainCommit(
          indexValid,
          dsql -> {
            dsql.seed(5, 2, "a");
            dsql.seed(5, 3, "b");
          },
          List.of(
              record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null),
              tombstone(ORDERS, sourceKey(2))),
          List.of(List.of(5, 3, "b")));
    }
  }

  @Test
  void g22aMoveConvergesThroughAnUncertainCommit() throws Exception {
    for (boolean indexValid : new boolean[] {false, true}) {
      assertConvergesThroughAnUncertainCommit(
          indexValid,
          dsql -> dsql.seed(5, 2, "a"),
          List.of(leadUpdate()),
          List.of(List.of(9, 2, "b")));
    }
  }

  @Test
  void g22aReplaceWithAnUnknownOldKeyConvergesThroughAnUncertainCommit() throws Exception {
    // The spared delete (DELETE ... AND NOT (lead = ?)) spares the row the landed attempt
    // already moved, and the one an UPDATE kept in place.
    assertConvergesThroughAnUncertainCommit(
        true,
        dsql -> {
          dsql.seed(9, 3, "r3");
          dsql.seed(9, 4, "r4");
        },
        List.of(blindUpdate(11, 3, "r3"), blindUpdate(9, 4, "r4b")),
        List.of(List.of(9, 4, "r4b"), List.of(11, 3, "r3")));
  }

  /** What one run of a put left: the rows, the dead-lettered offsets, the COMMIT counts. */
  private record Outcome(
      Set<List<Object>> rows, List<Long> deadLettered, int commits, int uncertain) {}

  private static Outcome runOnce(List<SinkRecord> put, boolean indexValid, boolean landed)
      throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, 2, "a");
    dsql.seed(5, 4, "d");
    if (landed) {
      dsql.commitsUnavailable = 1;
      dsql.unavailableCommitLands = true;
    }
    RecordingReporter dlq = new RecordingReporter();
    task(dsql, dlq, TARGET_KEYS).index(indexValid).put(put);
    List<Long> offsets = new ArrayList<>();
    for (SinkRecord r : dlq.records) {
      offsets.add(r.kafkaOffset());
    }
    return new Outcome(
        Set.copyOf(dsql.rows()), offsets, dsql.count("commit"), dsql.count("commit!XX000"));
  }

  @Test
  void everyWriteShapeReRunAfterALandedCommitEndsInTheSameState() throws Exception {
    // Every shape, with and without a valid unique index on id (Aurora DSQL enforces that
    // index even while it builds), run twice: cleanly, and with its COMMIT landing but
    // answering XX000 so the whole transaction re-runs on the state it already wrote. Both end
    // in the same rows. The one difference allowed is pre-existing (any Connect redelivery of
    // a committed chunk does the same) and comes from a secondary UNIQUE index -- here the one
    // on id -- not from re-keying: the re-run replays earlier images on rows that already hold
    // their final ones, an earlier image whose unique value the committed state gives to
    // another row is rejected (23505), the chunk falls back to one row per transaction, and
    // that record is dead-lettered. A table that is not re-keyed does the same (an insert, a
    // delete, then a re-insert of one unique value in one chunk). For the shapes below the
    // record is a superseded image and the row still ends correct; that is no general
    // guarantee -- rows that pass a unique value along within one chunk can lose a row's last
    // image, which is why Cut over waits for a CHECKSUM of every dead-lettered table.
    Map<String, List<SinkRecord>> shapes = new LinkedHashMap<>();
    shapes.put("insert", List.of(record(ORDERS, sourceKey(7), "c", null, row(1, 7, "n"))));
    shapes.put(
        "update in place",
        List.of(record(ORDERS, sourceKey(2), "u", row(5, 2, "a"), row(5, 2, "c"))));
    shapes.put("move", List.of(leadUpdate()));
    shapes.put("delete", List.of(record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null)));
    shapes.put(
        "delete + tombstone",
        List.of(
            record(ORDERS, sourceKey(2), "d", row(5, 2, "a"), null),
            tombstone(ORDERS, sourceKey(2))));
    shapes.put("unknown old key (spared delete)", List.of(blindUpdate(9, 2, "b")));
    shapes.put("unknown old key, unmoved", List.of(blindUpdate(5, 2, "c")));
    shapes.put(
        "two moves of different rows",
        List.of(leadUpdate(), record(ORDERS, sourceKey(4), "u", row(5, 4, "d"), row(9, 4, "e"))));
    shapes.put(
        "the same row moved twice",
        List.of(leadUpdate(), record(ORDERS, sourceKey(2), "u", row(9, 2, "b"), row(4, 2, "x"))));
    shapes.put(
        "update then move",
        List.of(
            record(ORDERS, sourceKey(2), "u", row(5, 2, "a"), row(5, 2, "c")),
            record(ORDERS, sourceKey(2), "u", row(5, 2, "c"), row(9, 2, "b"))));
    shapes.put(
        "insert then delete",
        List.of(
            record(ORDERS, sourceKey(7), "c", null, row(1, 7, "n")),
            record(ORDERS, sourceKey(7), "d", row(1, 7, "n"), null),
            tombstone(ORDERS, sourceKey(7))));
    for (Map.Entry<String, List<SinkRecord>> shape : shapes.entrySet()) {
      List<SinkRecord> put = shape.getValue();
      for (boolean indexValid : new boolean[] {false, true}) {
        String context = shape.getKey() + " indexValid=" + indexValid;
        Outcome clean = runOnce(put, indexValid, false);
        Outcome landed = runOnce(put, indexValid, true);
        assertEquals(clean.rows(), landed.rows(), context);
        if (clean.commits() > 0) {
          assertEquals(1, landed.uncertain(), context); // the re-run really happened
        }
        for (long offset : landed.deadLettered()) {
          if (clean.deadLettered().contains(offset)) {
            continue; // dead-lettered either way: not the re-run's doing
          }
          int at = -1;
          for (int i = 0; i < put.size(); i++) {
            if (put.get(i).kafkaOffset() == offset) {
              at = i;
            }
          }
          assertTrue(at >= 0, context);
          Object key = put.get(at).key();
          boolean superseded = false;
          for (int i = at + 1; i < put.size(); i++) {
            // A later change of the same row (a tombstone is not one: it follows its delete).
            superseded |= put.get(i).value() != null && key.equals(put.get(i).key());
          }
          assertTrue(superseded, context + ": the re-run dead-lettered a row's last image");
        }
      }
    }
  }
}
