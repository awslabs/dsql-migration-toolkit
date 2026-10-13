// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * The shared harness of the G25 unique-hold tests: app.customers (PK id, UNIQUE email named
 * uk_email) as a stateful JDBC double -- DELETE and INSERT ... ON CONFLICT (id) behave as on
 * PostgreSQL, a unique violation is a PSQLException naming its constraint (as Aurora DSQL reports
 * it), committed and in-flight rows are kept apart -- plus record builders, a recording DLQ and a
 * task with a fake clock. The same double as DsqlSinkUniqueHoldTest's, which keeps its own copy;
 * this one also records the order in which writes COMMITTED and carries source.ts_ms.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

final class HoldHarness {

  private HoldHarness() {}

  static final long SECOND = TimeUnit.SECONDS.toNanos(1);
  static final String TOPIC = "dsqlcdc.app.customers";
  static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
  static final TopicPartition P2 = new TopicPartition(TOPIC, 2);
  static final String HUGE = "x".repeat(DsqlSinkTask.DSQL_MAX_VALUE_BYTES + 1);

  // --- Records ----------------------------------------------------------------------------------

  private static final Schema KEY =
      SchemaBuilder.struct().name("app.customers.Key").field("id", Schema.INT32_SCHEMA).build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("app.customers.Value")
          .field("id", Schema.INT32_SCHEMA)
          .field("email", Schema.OPTIONAL_STRING_SCHEMA)
          .field("bio", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("connector", Schema.STRING_SCHEMA)
          .field("schema", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
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

  static Struct row(int id, String email, String bio) {
    return new Struct(ROW).put("id", id).put("email", email).put("bio", bio);
  }

  /** A Debezium change; {@code sourceTsMs} 0 = no source.ts_ms. */
  static SinkRecord change(
      TopicPartition tp, long offset, int id, String op, Struct before, Struct after,
      long sourceTsMs) {
    Struct source =
        new Struct(SOURCE).put("connector", "postgresql").put("schema", "app")
            .put("table", "customers");
    if (sourceTsMs > 0L) {
      source.put("ts_ms", sourceTsMs);
    }
    Struct envelope = new Struct(ENVELOPE).put("op", op).put("source", source);
    if (before != null) {
      envelope.put("before", before);
    }
    if (after != null) {
      envelope.put("after", after);
    }
    Struct key = new Struct(KEY).put("id", id);
    return new SinkRecord(tp.topic(), tp.partition(), KEY, key, ENVELOPE, envelope, offset);
  }

  static SinkRecord create(TopicPartition tp, long offset, int id, String email, String bio) {
    return change(tp, offset, id, "c", null, row(id, email, bio), 0L);
  }

  static SinkRecord update(TopicPartition tp, long offset, int id, String email, String bio) {
    return change(tp, offset, id, "u", null, row(id, email, bio), 0L);
  }

  static SinkRecord delete(TopicPartition tp, long offset, int id) {
    return change(tp, offset, id, "d", row(id, null, null), null, 0L);
  }

  static SinkRecord tombstone(TopicPartition tp, long offset, int id) {
    Struct key = new Struct(KEY).put("id", id);
    return new SinkRecord(tp.topic(), tp.partition(), KEY, key, null, null, offset);
  }

  static String name(SinkRecord r) {
    return "P" + r.kafkaPartition() + "@" + r.kafkaOffset();
  }

  // --- The JDBC double --------------------------------------------------------------------------

  /** A unique violation as Aurora DSQL reports it: SQLSTATE 23505 and the constraint name. */
  static SQLException uniqueViolation(String constraint) {
    return new PSQLException(
        new ServerErrorMessage(
            "SERROR\0C23505\0Mduplicate key value violates unique constraint \""
                + constraint + "\"\0Dthis detail carries a value\0n" + constraint + "\0"));
  }

  static final class FakeDsql {
    final List<String> calls = new ArrayList<>();
    final List<String> executed = new ArrayList<>(); // every attempted "upsert:<id>"/"delete:<id>"
    final List<String> committedWrites = new ArrayList<>(); // the writes that committed, in order
    private final List<String> pendingWrites = new ArrayList<>();
    private Map<Object, Map<String, Object>> committed = new LinkedHashMap<>();
    private Map<Object, Map<String, Object>> working = new LinkedHashMap<>();
    BiFunction<String, List<Object>, SQLException> failure = (sql, binds) -> null;
    int commitsUnavailable; // the next N commits land, then answer XX000 (G22)

    private static final Pattern INSERT_COLUMNS =
        Pattern.compile("^INSERT INTO \\S+ \\(([^)]*)\\)");

    void seed(int id, String email, String bio) {
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("id", id);
      r.put("email", email);
      r.put("bio", bio);
      committed.put(id, r);
      working = copy(committed);
    }

    /** The committed rows as "id:email:bio", by id. */
    List<String> rows() {
      List<String> out = new ArrayList<>();
      committed.values().stream()
          .sorted((a, b) -> Integer.compare((Integer) a.get("id"), (Integer) b.get("id")))
          .forEach(r -> out.add(r.get("id") + ":" + r.get("email") + ":" + r.get("bio")));
      return out;
    }

    boolean has(int id) {
      return committed.containsKey(id);
    }

    long count(String what) {
      return executed.stream().filter(what::equals).count();
    }

    private static Map<Object, Map<String, Object>> copy(Map<Object, Map<String, Object>> rows) {
      Map<Object, Map<String, Object>> out = new LinkedHashMap<>();
      rows.forEach((k, v) -> out.put(k, new LinkedHashMap<>(v)));
      return out;
    }

    private void apply(String sql, List<Object> binds) throws SQLException {
      SQLException injected = failure.apply(sql, binds);
      if (sql.startsWith("DELETE")) {
        executed.add("delete:" + binds.get(0));
        if (injected != null) {
          throw injected;
        }
        working.remove(binds.get(0));
        pendingWrites.add("delete:" + binds.get(0));
        return;
      }
      Matcher m = INSERT_COLUMNS.matcher(sql);
      assertTrue(m.find(), sql);
      List<String> columns = new ArrayList<>();
      Matcher q = Pattern.compile("\"([^\"]+)\"").matcher(m.group(1));
      while (q.find()) {
        columns.add(q.group(1));
      }
      Map<String, Object> image = new LinkedHashMap<>();
      for (int i = 0; i < columns.size(); i++) {
        image.put(columns.get(i), binds.get(i));
      }
      Object id = image.get("id");
      executed.add("upsert:" + id);
      if (injected != null) {
        throw injected;
      }
      Map<String, Object> existing = working.get(id);
      Map<String, Object> next =
          existing == null ? new LinkedHashMap<>() : new LinkedHashMap<>(existing);
      if (existing == null) {
        next.put("id", id);
        next.put("email", null);
        next.put("bio", null);
      }
      next.putAll(image);
      Object email = next.get("email");
      if (email != null) {
        for (Map<String, Object> other : working.values()) {
          if (!other.get("id").equals(id) && email.equals(other.get("email"))) {
            throw uniqueViolation("uk_email");
          }
        }
      }
      working.put(id, next);
      pendingWrites.add("upsert:" + id);
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
                  committed = copy(working);
                  committedWrites.addAll(pendingWrites);
                  pendingWrites.clear();
                  if (commitsUnavailable > 0) {
                    commitsUnavailable--;
                    calls.add("commit!XX000"); // it landed; the client got an error
                    throw new SQLException("server unavailable", "XX000");
                  }
                  calls.add("commit");
                  yield null;
                }
                case "rollback" -> {
                  calls.add("rollback");
                  working = copy(committed);
                  pendingWrites.clear();
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

  static final class RecordingReporter implements ErrantRecordReporter {
    final List<SinkRecord> records = new ArrayList<>();
    final List<Throwable> causes = new ArrayList<>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      records.add(record);
      causes.add(error);
      return CompletableFuture.completedFuture(null);
    }

    List<String> names() {
      List<String> out = new ArrayList<>();
      records.forEach(r -> out.add(name(r)));
      return out;
    }
  }

  /**
   * A task on the double: a fake clock, a recording context, no catalog reads, no CloudWatch.
   * A dropped connection (the task discards it after a transient failure) reconnects to the
   * same double, as DSQL would.
   */
  static class HoldTask extends DsqlSinkTask {
    long now = 1_000 * SECOND;
    final RecordingSinkTaskContext ctx = new RecordingSinkTaskContext();
    FakeDsql dsql;

    @Override
    long nanoTime() {
      return now;
    }

    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }

    @Override
    void emitMetrics() {
      // never CloudWatch in a unit test
    }

    void advance(long nanos) {
      now += nanos;
    }

    /** The reconnect a real task does on its next statement after a transient failure. */
    @Override
    public void put(java.util.Collection<SinkRecord> records) {
      reconnectIfDropped();
      super.put(records);
    }

    void reconnectIfDropped() {
      try {
        if (get(this, "connection") == null) {
          setField(this, "connection", dsql.connection());
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  static Object get(DsqlSinkTask task, String name) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(task);
  }

  static HoldTask task(FakeDsql dsql, RecordingReporter dlq, Map<String, String> extra)
      throws Exception {
    return task(new HoldTask(), dsql, dlq, extra);
  }

  static <T extends HoldTask> T task(
      T task, FakeDsql dsql, RecordingReporter dlq, Map<String, String> extra) throws Exception {
    task.dsql = dsql;
    task.initialize(task.ctx);
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.BATCH_SIZE, "1000");
    props.putAll(extra);
    task.startMetrics(props);
    setField(task, "connection", dsql.connection());
    if (dlq != null) {
      setField(task, "dlqReporter", dlq);
    }
    return task;
  }

  static HoldTask task(FakeDsql dsql, RecordingReporter dlq) throws Exception {
    return task(dsql, dlq, Map.of());
  }

  static Map<TopicPartition, OffsetAndMetadata> offsets(Object... pairs) {
    Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put((TopicPartition) pairs[i], new OffsetAndMetadata((Long) pairs[i + 1]));
    }
    return out;
  }
}
