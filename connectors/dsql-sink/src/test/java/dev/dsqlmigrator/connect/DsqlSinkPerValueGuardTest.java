// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * The pre-write per-value guard (DsqlSinkTask.oversizedColumn) against Aurora DSQL's documented
 * per-type limits ("Supported data types in Aurora DSQL",
 * https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html):
 * a bytea or text value over 1 MiB is refused before any write, but a json / jsonb value -- and
 * a PostgreSQL array, which the sink renders as jsonb -- is sent as it is. Aurora DSQL compresses
 * large json and jsonb values and "the 1 MiB limit applies to the compressed size", so a
 * compressible document well past 1 MiB is stored, as Full Load stores it. A guard that sized a
 * PGobject would dead-letter, on CDC only, a document Full Load loads. The chunk byte budget
 * still counts the JSON text (G126): the two use one sizer, only the guard's TYPES differ.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

class DsqlSinkPerValueGuardTest {

  private static final int LIMIT = DsqlSinkTask.DSQL_MAX_VALUE_BYTES;

  /** A highly compressible JSON document of about {@code bytes} UTF-8 bytes. */
  private static String document(int bytes) {
    return "{\"k\": \"" + "x".repeat(bytes - 9) + "\"}";
  }

  private static PGobject pg(String type, String value) throws Exception {
    PGobject pg = new PGobject();
    pg.setType(type);
    pg.setValue(value);
    return pg;
  }

  /** A PostgreSQL text[] of {@code bytes} or more, rendered as jsonb the way the sink binds it. */
  private static PGobject arrayAsJsonb(int bytes) {
    List<String> elements = new ArrayList<>();
    for (int total = 0; total < bytes; total += 64) {
      elements.add("y".repeat(60));
    }
    return DebeziumTypeConverter.pgArrayAsJsonb(elements, null);
  }

  private static ChangeEvent upsertOf(Object value) {
    return ChangeEvent.insert(
        "app.docs", List.of("id", "payload"), Arrays.asList(1L, value), List.of("id"),
        List.of(1L), 0L);
  }

  // --- The guard, per type --------------------------------------------------------------------

  @Test
  void aJsonJsonbOrArrayValueOverOneMebibyteIsNotRefusedByTheGuard() throws Exception {
    // FAILS on a guard that sizes a PGobject (the review's first JS-3 proposal): these values
    // are well past 1 MiB as text, compress far below it, and Aurora DSQL stores them.
    PGobject json = pg("json", document(LIMIT + LIMIT / 2));
    PGobject jsonb = pg("jsonb", document(2 * LIMIT));
    PGobject array = arrayAsJsonb(LIMIT + LIMIT / 4);
    assertEquals("jsonb", array.getType());
    for (PGobject value : List.of(json, jsonb, array)) {
      assertTrue(DsqlSinkTask.encodedBytes(value) > LIMIT, value.getType());
      assertNull(DsqlSinkTask.oversizedColumn(upsertOf(value)), value.getType());
    }
  }

  @Test
  void aTextOrByteaValueOverOneMebibyteIsStillRefused() {
    assertEquals("payload", DsqlSinkTask.oversizedColumn(upsertOf("x".repeat(LIMIT + 1))));
    assertEquals("payload", DsqlSinkTask.oversizedColumn(upsertOf(new byte[LIMIT + 1])));
    // Text is sized by its UTF-8 bytes: fewer than 1 Mi characters, 3 bytes each.
    String hangul = "한".repeat(LIMIT / 3 + 1);
    assertTrue(hangul.length() < LIMIT);
    assertEquals("payload", DsqlSinkTask.oversizedColumn(upsertOf(hangul)));
    // At the limit itself, and for any value that is not text or bytea, nothing is refused.
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf("x".repeat(LIMIT))));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(new byte[LIMIT])));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(null)));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(42L)));
    // A delete carries only its key: never refused.
    assertNull(
        DsqlSinkTask.oversizedColumn(
            ChangeEvent.delete("app.docs", List.of("id"), List.of("x".repeat(LIMIT + 1)), 0L)));
  }

  @Test
  void theChunkBudgetStillCountsTheJsonText() throws Exception {
    // G126: the chunk byte budget sizes a PGobject by its text, and the guard's narrower TYPES
    // must not narrow it -- the budget is what keeps a chunk under 10 MiB.
    String doc = document(LIMIT + LIMIT / 2);
    long docBytes = doc.getBytes(StandardCharsets.UTF_8).length;
    PGobject json = pg("json", doc);
    assertEquals(docBytes, DsqlSinkTask.encodedBytes(json));
    assertEquals(docBytes, DsqlSinkTask.encodedBytes(pg("jsonb", doc)));
    PGobject array = arrayAsJsonb(LIMIT);
    assertEquals(
        array.getValue().getBytes(StandardCharsets.UTF_8).length,
        DsqlSinkTask.encodedBytes(array));
    assertEquals(9L, DsqlSinkTask.encodedBytes("한글abc"));
    assertEquals(7L, DsqlSinkTask.encodedBytes(new StringBuilder("abcdefg")));
    assertEquals(5L, DsqlSinkTask.encodedBytes(new byte[5]));
    // Not a sized value: null, a scalar, a PGobject with no text.
    assertEquals(-1L, DsqlSinkTask.encodedBytes(null));
    assertEquals(-1L, DsqlSinkTask.encodedBytes(42L));
    assertEquals(-1L, DsqlSinkTask.encodedBytes(new PGobject()));
    long rowBytes = DsqlSinkTask.estimateModifiedBytes(upsertOf(json));
    assertTrue(rowBytes >= docBytes && rowBytes < docBytes + 64, "row bytes: " + rowBytes);
  }

  // --- Through put(): what reaches Aurora DSQL and what is dead-lettered -----------------------

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field(
              "doc",
              SchemaBuilder.string().name(DebeziumTypeConverter.JSON_TYPE).optional().build())
          .field("body", Schema.OPTIONAL_STRING_SCHEMA)
          .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
          .field(
              "tags", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
          .optional()
          .build();
  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("connector", Schema.STRING_SCHEMA)
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

  /** An insert of {@code id} whose only non-key value is {@code column} = {@code value}. */
  private static SinkRecord insert(String connector, long id, String column, Object value) {
    Struct after = new Struct(ROW).put("id", id).put(column, value);
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", "c")
            .put("after", after)
            .put("source", new Struct(SOURCE).put("connector", connector).put("db", "app")
                .put("table", "docs"));
    return new SinkRecord(
        "dsqlcdc.app.docs", 0, KEY, new Struct(KEY).put("id", id), ENVELOPE, envelope, id);
  }

  /** A JDBC double that keeps the bound rows of every committed statement. */
  private static final class RecordingDsql {
    final List<List<Object>> committed = new ArrayList<>();
    private final List<List<Object>> pending = new ArrayList<>();

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private PreparedStatement statement() {
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
                  pending.addAll(batch.isEmpty() ? List.of(current) : batch);
                  yield 1;
                }
                case "executeBatch" -> {
                  pending.addAll(batch);
                  yield new int[batch.size()];
                }
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
                case "prepareStatement" -> statement();
                case "commit" -> {
                  committed.addAll(pending);
                  pending.clear();
                  yield null;
                }
                case "rollback" -> {
                  pending.clear();
                  yield null;
                }
                case "close" -> null;
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }

    List<Object> committedIds() {
      List<Object> ids = new ArrayList<>();
      for (List<Object> row : committed) {
        ids.add(row.get(0));
      }
      return ids;
    }
  }

  /** A task with no guarded target columns (no catalog round trips). */
  private static final class UncheckedTask extends DsqlSinkTask {
    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }
  }

  private static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  @Test
  void putWritesLargeJsonAndArraysAndRefusesLargeTextAndByteaBeforeAnyWrite() throws Exception {
    // FAILS on a guard that sizes a PGobject: the MySQL JSON document and the PostgreSQL array
    // were dead-lettered as "exceeds DSQL's 1048576-byte limit" -- on CDC only, since Full Load
    // sends them and Aurora DSQL stores them compressed.
    List<String> tags = new ArrayList<>();
    for (int i = 0; i < LIMIT / 40; i++) {
      tags.add("z".repeat(60));
    }
    List<SinkRecord> records =
        List.of(
            insert("mysql", 1, "doc", document(LIMIT + LIMIT / 2)),
            insert("mysql", 2, "body", "x".repeat(LIMIT + 1)),
            insert("mysql", 3, "blob", new byte[LIMIT + 1]),
            insert("postgresql", 4, "tags", tags));
    RecordingDsql dsql = new RecordingDsql();
    List<Object> deadLettered = new ArrayList<>();
    List<String> reasons = new ArrayList<>();
    ErrantRecordReporter dlq =
        (record, error) -> {
          deadLettered.add(record.kafkaOffset());
          reasons.add(error.getMessage());
          return CompletableFuture.completedFuture(null);
        };
    UncheckedTask task = new UncheckedTask();
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    task.startMetrics(props);
    setField(task, "connection", dsql.connection());
    setField(task, "dlqReporter", dlq);

    task.put(records);

    assertEquals(List.of(1L, 4L), dsql.committedIds());
    PGobject doc = (PGobject) dsql.committed.get(0).get(1);
    assertEquals("json", doc.getType());
    assertTrue(DsqlSinkTask.encodedBytes(doc) > LIMIT);
    PGobject array = (PGobject) dsql.committed.get(1).get(4);
    assertEquals("jsonb", array.getType());
    assertTrue(DsqlSinkTask.encodedBytes(array) > LIMIT);
    assertEquals(List.of(2L, 3L), deadLettered);
    assertTrue(reasons.get(0).startsWith("Value for column 'body' exceeds DSQL's 1048576-byte"));
    assertTrue(reasons.get(1).startsWith("Value for column 'blob' exceeds DSQL's 1048576-byte"));
  }
}
