// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * A table name holding a '.' and Debezium's tombstones, through put() (plugin v57).
 *
 * The sink names its target from the event's own source block and reads that name back at its
 * FIRST '.', so MySQL `shop`.`orders.v2` is "shop"."orders.v2" and PostgreSQL app."app.orders"
 * is "app"."app.orders" -- never app.orders. A change it cannot name exactly (a '.' in the
 * database or schema name, no source.table) is dead-lettered with nothing written. Every
 * tombstone is skipped: the op=d before it applies the source DELETE, and a tombstone's table
 * could only be read from its topic, whose segments a '.' in a name makes ambiguous.
 *
 * The JDBC double records every statement with its binds, and every Connection call that
 * matters; the column-type read and CloudWatch are stubbed.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkTombstoneAndDottedNameTest {

  // --- Records ----------------------------------------------------------------------------------

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("connector", Schema.OPTIONAL_STRING_SCHEMA)
          .field("db", Schema.OPTIONAL_STRING_SCHEMA)
          .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
          .field("table", Schema.OPTIONAL_STRING_SCHEMA)
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

  /** A MySQL source block (database {@code db}, no schema). */
  private static Struct mysql(String db, String table) {
    return new Struct(SOURCE).put("connector", "mysql").put("db", db).put("table", table);
  }

  /** A PostgreSQL source block (database {@code db}, schema {@code schema}). */
  private static Struct postgres(String db, String schema, String table) {
    return new Struct(SOURCE)
        .put("connector", "postgresql").put("db", db).put("schema", schema).put("table", table);
  }

  /** A change of row {@code id}: op c/u carry an after-image, op d a before-image. */
  private static SinkRecord change(String topic, long offset, String op, Struct source, long id) {
    Struct envelope = new Struct(ENVELOPE).put("op", op).put("source", source);
    envelope.put("d".equals(op) ? "before" : "after", new Struct(ROW).put("id", id).put("name", "a"));
    return new SinkRecord(topic, 0, KEY, new Struct(KEY).put("id", id), ENVELOPE, envelope, offset);
  }

  /** Debezium's tombstone of row {@code id}: its key, no value. */
  private static SinkRecord tombstone(String topic, long offset, long id) {
    return new SinkRecord(topic, 0, KEY, new Struct(KEY).put("id", id), null, null, offset);
  }

  // --- The JDBC double and the task -------------------------------------------------------------

  /** One executed statement: its SQL and its binds, in order. */
  record Executed(String sql, List<Object> binds) {}

  private static final class FakeDsql {
    final List<String> calls = new ArrayList<>(); // prepare/commit/rollback/close
    final List<Executed> executed = new ArrayList<>();

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
                case "setNull" -> {
                  current.add(null);
                  yield null;
                }
                case "addBatch" -> {
                  batch.add(new ArrayList<>(current));
                  current.clear();
                  yield null;
                }
                case "executeUpdate" -> {
                  for (List<Object> binds : batch.isEmpty() ? List.of(current) : batch) {
                    executed.add(new Executed(sql, List.copyOf(binds)));
                  }
                  yield 1;
                }
                case "executeBatch" -> {
                  for (List<Object> binds : batch) {
                    executed.add(new Executed(sql, List.copyOf(binds)));
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
                case "commit", "rollback", "close" -> {
                  calls.add(m.getName());
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

  /** A task whose column-type read records the {schema, table} it was asked for. */
  private static final class Task extends DsqlSinkTask {
    final List<List<String>> typeReads = new ArrayList<>();

    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      typeReads.add(List.of(schema, table));
      return Map.of();
    }

    @Override
    void emitMetrics() {
      // never CloudWatch in a unit test
    }
  }

  private final FakeDsql dsql = new FakeDsql();
  private final RecordingReporter dlq = new RecordingReporter();

  private Task task() throws Exception {
    Task task = new Task();
    task.initialize(new RecordingSinkTaskContext());
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.METRICS_STACK, "test-stack"); // count applied ops
    task.startMetrics(props);
    setField(task, "connection", dsql.connection());
    setField(task, "dlqReporter", dlq);
    return task;
  }

  private static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, LongAdder> applied(DsqlSinkTask task, String counter)
      throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(counter);
    field.setAccessible(true);
    return (Map<String, LongAdder>) field.get(task);
  }

  /** The offset Connect commits for {@code topic}'s partition 0 after this put. */
  private static long committed(DsqlSinkTask task, String topic, long current) {
    TopicPartition tp = new TopicPartition(topic, 0);
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(current));
    return task.preCommit(offsets).get(tp).offset();
  }

  // --- A table name holding a '.' ---------------------------------------------------------------

  @Test
  void aDeleteAndItsTombstoneOfADottedTableDeleteFromThatTableOnly() throws Exception {
    // PostgreSQL app."app.orders" next to app.orders, whose row 2 must survive. FAILS WITHOUT THE
    // FIX (plugin v57): the op=d ran DELETE FROM "app"."app"."orders" (split at every '.') and
    // the tombstone, its table read from the topic's last two segments, ran
    // DELETE FROM "app"."orders" -- a row of ANOTHER table deleted, with no error.
    String topic = "dsqlcdc.app.app.orders";
    Task task = task();

    task.put(
        List.of(
            change(topic, 0, "d", postgres("testdb", "app", "app.orders"), 2L),
            tombstone(topic, 1, 2L)));

    assertEquals(
        List.of(new Executed("DELETE FROM \"app\".\"app.orders\" WHERE \"id\" = ?", List.of(2L))),
        dsql.executed);
    assertTrue(
        dsql.executed.stream().noneMatch(e -> e.sql().contains("\"app\".\"orders\"")),
        dsql.executed::toString);
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    // One source DELETE, counted once, under its own table.
    assertEquals(1L, applied(task, "deletesByTable").get("app.app.orders").sum());
    assertNull(applied(task, "deletesByTable").get("app.orders"));
    assertEquals(2L, committed(task, topic, 2L)); // committed past the delete AND the tombstone
  }

  @Test
  void aMysqlChangeToADottedTableIsWrittenToThatTableAndItsValuesAreChecked() throws Exception {
    // FAILS WITHOUT THE FIX (plugin v57): `shop`.`orders.v2` was written as
    // "shop"."orders"."v2" (rejected by every write) and its column types were never read
    // (the value-fit guard failed open for it).
    Task task = task();

    task.put(List.of(change("dsqlcdc.shop.orders.v2", 0, "c", mysql("shop", "orders.v2"), 1L)));

    assertEquals(
        List.of(
            new Executed(
                "INSERT INTO \"shop\".\"orders.v2\" (\"id\", \"name\") VALUES (?, ?) "
                    + "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"",
                List.of(1L, "a"))),
        dsql.executed);
    assertEquals(List.of(List.of("shop", "orders.v2")), task.typeReads);
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aChangeWhoseDatabaseNameHoldsADotIsDeadLetteredWithNothingWritten() throws Exception {
    // FAILS WITHOUT THE FIX (plugin v57): database my.db's table t was written to "my"."db"."t".
    // The tool does not support a '.' in a database or schema name, so this can only be a
    // database Evaluation reported Unsupported (DOTTED_SCHEMA_NAME) and capture still reached.
    Task task = task();
    SinkRecord record = change("dsqlcdc.my.db.t", 0, "c", mysql("my.db", "t"), 1L);

    task.put(List.of(record));

    assertTrue(dsql.executed.isEmpty(), dsql.executed::toString);
    assertTrue(task.typeReads.isEmpty(), task.typeReads::toString);
    assertEquals(List.of(record), dlq.records);
    String reason = dlq.causes.get(0).getMessage();
    assertTrue(
        reason.startsWith("Cannot apply a change to table my.db.t: its database or schema name,"
            + " my.db, holds a '.'"),
        reason);
    assertTrue(reason.contains("DOTTED_SCHEMA_NAME"), reason);
    assertEquals(1L, committed(task, "dsqlcdc.my.db.t", 1L)); // dead-lettered, then committed

    // A PostgreSQL source names its schema, so a '.' in its DATABASE name changes nothing.
    task.put(List.of(change("dsqlcdc.app.t", 0, "c", postgres("my.app", "app", "t"), 1L)));
    assertEquals(1, dsql.executed.size());
    assertTrue(dsql.executed.get(0).sql().startsWith("INSERT INTO \"app\".\"t\" "),
        dsql.executed::toString);
    assertEquals(1, dlq.records.size());
  }

  @Test
  void aChangeWithNoSourceTableIsDeadLetteredNotWrittenToTheTopicsTable() throws Exception {
    // FAILS WITHOUT THE FIX (plugin v57): the table was read from the topic's last two segments
    // (app.users here); for dsqlcdc.app.app.orders that named app.orders -- another table.
    Task task = task();
    SinkRecord record = change("dsqlcdc.app.users", 0, "c", mysql("app", null), 1L);

    task.put(List.of(record));

    assertTrue(dsql.executed.isEmpty(), dsql.executed::toString);
    assertEquals(List.of(record), dlq.records);
    assertTrue(
        dlq.causes.get(0).getMessage().startsWith(
            "Cannot resolve the target table: the change event has no source.table"),
        dlq.causes.get(0).getMessage());
  }

  // --- Tombstones -------------------------------------------------------------------------------

  @Test
  void aTombstoneOnItsOwnWritesNothingAndIsCommitted() throws Exception {
    // FAILS WITHOUT THE FIX (plugin v57): a tombstone of a table that is not re-keyed was applied
    // as a second DELETE by the key of the op=d before it, of the table read from its topic.
    Task task = task();

    task.put(List.of(tombstone("dsqlcdc.app.users", 0, 7L)));

    assertTrue(dsql.calls.isEmpty(), dsql.calls::toString); // no statement, no transaction
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertTrue(applied(task, "deletesByTable").isEmpty());
    assertEquals(1L, committed(task, "dsqlcdc.app.users", 1L));
  }
}
