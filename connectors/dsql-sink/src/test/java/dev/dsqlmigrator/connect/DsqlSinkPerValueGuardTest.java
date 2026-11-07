// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * The pre-write per-value guard (DsqlSinkTask.oversizedColumn) against Aurora DSQL's per-type
 * limits ("Supported data types in Aurora DSQL",
 * https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html,
 * and a live measurement for text): only a bytea value over 1 MiB is refused before any write.
 * A text value, a json / jsonb value and a PostgreSQL array (which the sink renders as jsonb) are
 * sent as they are. Aurora DSQL limits json and jsonb by their compressed size (documented) and
 * text the same way (measured 2026-11-07: a 1.5 MiB JSON-shaped text value stored in 807,991
 * bytes, while 1.05 MiB of random characters was refused), so a compressible value well past
 * 1 MiB is stored, as Full Load stores it; bytea is limited by its raw size. A guard that sized a
 * String or a PGobject would dead-letter, on CDC only, a value Full Load loads. The chunk byte
 * budget still counts the raw text (G126): the two use one sizer, only the guard's TYPES differ.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

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
  void aTextValueOverOneMebibyteIsNotRefusedByTheGuard() {
    // FAILS on the v55 guard, which refused a String whose UTF-8 bytes passed 1 MiB: Aurora DSQL
    // limits text by its stored, compressed size (measured), so these are stored when they
    // compress below 1 MiB -- as Full Load stores them -- and Aurora DSQL decides when not.
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf("x".repeat(LIMIT + 1))));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(document(3 * LIMIT))));
    // Fewer than 1 Mi characters but over 1 MiB of UTF-8 bytes (3 bytes each): not refused either.
    String hangul = "한".repeat(LIMIT / 3 + 1);
    assertTrue(hangul.length() < LIMIT);
    assertTrue(DsqlSinkTask.encodedBytes(hangul) > LIMIT);
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(hangul)));
  }

  @Test
  void aByteaValueOverOneMebibyteIsStillRefused() {
    // bytea is limited by its RAW size (measured: 1.5 MiB of zero bytes was refused too).
    assertEquals("payload", DsqlSinkTask.oversizedColumn(upsertOf(new byte[LIMIT + 1])));
    // Beside a larger text value in the same row, the bytea column is the one named.
    ChangeEvent both =
        ChangeEvent.insert(
            "app.docs", List.of("id", "body", "blob"),
            Arrays.asList(1L, "x".repeat(2 * LIMIT), new byte[LIMIT + 1]), List.of("id"),
            List.of(1L), 0L);
    assertEquals("blob", DsqlSinkTask.oversizedColumn(both));
    // At the limit itself, and for any value that is not bytea, nothing is refused.
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(new byte[LIMIT])));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(null)));
    assertNull(DsqlSinkTask.oversizedColumn(upsertOf(42L)));
    // A delete carries only its key: never refused.
    assertNull(
        DsqlSinkTask.oversizedColumn(
            ChangeEvent.delete("app.docs", List.of("id"), List.of(new byte[LIMIT + 1]), 0L)));
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

  /**
   * A JDBC double that keeps the bound rows of every committed statement, and refuses a row the
   * way Aurora DSQL refuses a value over its limit when {@link #refuse} names a type for it.
   */
  private static final class RecordingDsql {
    final List<List<Object>> committed = new ArrayList<>();
    private final List<List<Object>> pending = new ArrayList<>();
    // The type a row's statement is refused for (54000), or null: the row is accepted.
    Function<List<Object>, String> refuse = row -> null;

    private void execute(List<List<Object>> rows) throws SQLException {
      for (List<Object> row : rows) {
        String type = refuse.apply(row);
        if (type != null) {
          throw valueLimit(type);
        }
      }
      pending.addAll(rows);
    }

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
                  execute(batch.isEmpty() ? List.of(current) : batch);
                  yield 1;
                }
                case "executeBatch" -> {
                  execute(batch);
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
  void putWritesLargeTextJsonAndArraysAndRefusesOnlyLargeByteaBeforeAnyWrite() throws Exception {
    // FAILS on a guard that sizes a String or a PGobject: the text value, the MySQL JSON
    // document and the PostgreSQL array were dead-lettered as "exceeds DSQL's 1048576-byte
    // limit" -- on CDC only, since Full Load sends them and Aurora DSQL stores them compressed.
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

    assertEquals(List.of(1L, 2L, 4L), dsql.committedIds());
    PGobject doc = (PGobject) dsql.committed.get(0).get(1);
    assertEquals("json", doc.getType());
    assertTrue(DsqlSinkTask.encodedBytes(doc) > LIMIT);
    assertEquals("x".repeat(LIMIT + 1), dsql.committed.get(1).get(2)); // the text, as it is
    PGobject array = (PGobject) dsql.committed.get(2).get(4);
    assertEquals("jsonb", array.getType());
    assertTrue(DsqlSinkTask.encodedBytes(array) > LIMIT);
    assertEquals(List.of(3L), deadLettered);
    assertTrue(reasons.get(0).startsWith("Value for column 'blob' exceeds DSQL's 1048576-byte"));
  }

  // --- Aurora DSQL's own rejection: the reason names the column when it can --------------------

  /** Aurora DSQL's refusal of one value over its 1 MiB limit (measured): it names the TYPE. */
  private static SQLException valueLimit(String type) {
    return new PSQLException(
        new ServerErrorMessage(
            "SERROR\0C54000\0Mdatatype limit greater than 1048576 bytes not supported for "
                + type + "\0"));
  }

  private static final String TEXT_LIMIT =
      "datatype limit greater than 1048576 bytes not supported for text";

  private static ChangeEvent row(List<String> columns, Object... values) {
    return ChangeEvent.insert(
        "app.docs", columns, Arrays.asList(values), List.of("id"), List.of(values[0]), 0L);
  }

  @Test
  void aValueLimitRejectionNamesTheOneCandidateColumnOfItsType() throws Exception {
    // FAILS before T1 part 2: the reason of a row Aurora DSQL refused carried only the TYPE, so
    // the in-app recovery could not pre-tick the column to stop capturing.
    String big = "x".repeat(2 * LIMIT);
    ChangeEvent text = row(List.of("id", "title", "body"), 1L, "small", big);
    String note = DsqlSinkTask.valueLimitNote(text, "54000", TEXT_LIMIT);
    assertEquals(
        " -- Value for column 'body' (text, " + (2 * LIMIT) + " bytes sent) exceeds DSQL's "
            + "1048576-byte limit as stored. This row cannot be stored as-is and is never "
            + "retried: exclude the column at capture and keep the value outside the database.",
        note);
    // The server's spelling may lead with a capital.
    assertTrue(
        DsqlSinkTask.valueLimitNote(text, "54000", "Datatype limit greater than 1048576 bytes "
            + "not supported for text").contains("column 'body'"));
    // json: the one PGobject over 1 MiB, even beside a larger text value.
    ChangeEvent json =
        row(List.of("id", "doc", "body"), 1L, pg("json", document(2 * LIMIT)), big);
    assertTrue(
        DsqlSinkTask.valueLimitNote(json, "54000", TEXT_LIMIT.replace("text", "json"))
            .startsWith(" -- Value for column 'doc' (json, "));
    // jsonb: the sink binds a PostgreSQL jsonb document as a json PGobject (Aurora DSQL casts
    // it), so a json PGobject is a jsonb candidate too.
    ChangeEvent jsonb = row(List.of("id", "doc"), 1L, pg("json", document(2 * LIMIT)));
    assertTrue(
        DsqlSinkTask.valueLimitNote(jsonb, "54000", TEXT_LIMIT.replace("text", "jsonb"))
            .startsWith(" -- Value for column 'doc' (jsonb, "));
    // bytea: a byte[] (the guard refuses one over 1 MiB before any write; this is the rule).
    ChangeEvent bytea = row(List.of("id", "blob"), 1L, new byte[LIMIT + 1]);
    assertTrue(
        DsqlSinkTask.valueLimitNote(bytea, "54000", TEXT_LIMIT.replace("text", "bytea"))
            .startsWith(" -- Value for column 'blob' (bytea, " + (LIMIT + 1) + " bytes sent)"));
  }

  @Test
  void aValueLimitRejectionNamesNoColumnUnlessExactlyOneCandidateHasItsTypeAndSize()
      throws Exception {
    String big = "x".repeat(2 * LIMIT);
    // Two text values over 1 MiB: either could be the one refused, so none is named.
    ChangeEvent two = row(List.of("id", "a", "b"), 1L, big, big);
    assertEquals("", DsqlSinkTask.valueLimitNote(two, "54000", TEXT_LIMIT));
    // No text value over 1 MiB (a stored size over 1 MiB implies a raw size over 1 MiB).
    ChangeEvent none = row(List.of("id", "a", "doc"), 1L, "x".repeat(LIMIT), pg("json", big));
    assertEquals("", DsqlSinkTask.valueLimitNote(none, "54000", TEXT_LIMIT));
    // A json and a jsonb (array) value both over 1 MiB: a jsonb rejection names neither.
    ChangeEvent docs =
        row(List.of("id", "doc", "tags"), 1L, pg("json", big), arrayAsJsonb(2 * LIMIT));
    assertEquals(
        "", DsqlSinkTask.valueLimitNote(docs, "54000", TEXT_LIMIT.replace("text", "jsonb")));
    // A text value is no json or bytea candidate, and a byte[] no text one.
    ChangeEvent text = row(List.of("id", "body"), 1L, big);
    assertEquals(
        "", DsqlSinkTask.valueLimitNote(text, "54000", TEXT_LIMIT.replace("text", "json")));
    assertEquals(
        "", DsqlSinkTask.valueLimitNote(text, "54000", TEXT_LIMIT.replace("text", "bytea")));
    ChangeEvent bytes = row(List.of("id", "blob"), 1L, new byte[2 * LIMIT]);
    assertEquals("", DsqlSinkTask.valueLimitNote(bytes, "54000", TEXT_LIMIT));
    // Any other 54000 (the transaction, row or key size), another limit (varchar's 65535
    // bytes), another SQLSTATE, no message, or a delete: no column named.
    for (String other :
        List.of(
            "transaction size limit 10485760 exceeded",
            "maximum row size exceeded",
            "key size too large",
            "datatype limit greater than 65535 bytes not supported for varchar")) {
      assertEquals("", DsqlSinkTask.valueLimitNote(text, "54000", other), other);
    }
    assertEquals("", DsqlSinkTask.valueLimitNote(text, "22001", TEXT_LIMIT));
    assertEquals("", DsqlSinkTask.valueLimitNote(text, null, TEXT_LIMIT));
    assertEquals("", DsqlSinkTask.valueLimitNote(text, "54000", null));
    assertEquals(
        "",
        DsqlSinkTask.valueLimitNote(
            ChangeEvent.delete("app.docs", List.of("id"), List.of(1L), 0L), "54000",
            TEXT_LIMIT));
  }

  @Test
  void aTextValueAuroraDsqlRefusesIsDeadLetteredAloneWithItsColumnNamed() throws Exception {
    // Through put(): the text value goes to Aurora DSQL (the guard no longer refuses it); the
    // 54000 fails the batch, the per-row replay applies the healthy rows and dead-letters the
    // refused one, and its reason names the column, the type and the size -- never the value.
    String refused = "R" + "x".repeat(2 * LIMIT - 1); // stands for 2 MiB that do not compress
    List<SinkRecord> records =
        List.of(
            insert("mysql", 1, "doc", document(LIMIT + LIMIT / 2)),
            insert("mysql", 2, "body", refused),
            insert("mysql", 3, "body", "small"));
    RecordingDsql dsql = new RecordingDsql();
    dsql.refuse = r -> refused.equals(r.get(2)) ? "text" : null;
    List<Object> deadLettered = new ArrayList<>();
    List<Throwable> causes = new ArrayList<>();
    ErrantRecordReporter dlq =
        (record, error) -> {
          deadLettered.add(record.kafkaOffset());
          causes.add(error);
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

    assertEquals(List.of(1L, 3L), dsql.committedIds());
    assertEquals(List.of(2L), deadLettered);
    // The Kafka dead letter keeps Aurora DSQL's own error.
    assertEquals("54000", ((SQLException) causes.get(0)).getSQLState());

    // The logged reason (what the control plane parses), read from the loud no-DLQ failure.
    UncheckedTask loud = new UncheckedTask();
    loud.startMetrics(props);
    setField(loud, "connection", dsql.connection());
    ConnectException e =
        assertThrows(ConnectException.class, () -> loud.put(List.of(records.get(1))));
    String reason = e.getMessage();
    assertTrue(
        reason.contains(
            ": sqlstate=54000 " + TEXT_LIMIT + " -- Value for column 'body' (text, "
                + (2 * LIMIT) + " bytes sent) exceeds DSQL's 1048576-byte limit as stored."),
        reason.substring(0, Math.min(reason.length(), 600)));
    assertTrue(reason.contains(" | pk: id=2 | op: c/r | sql: INSERT INTO "), reason);
    assertFalse(reason.contains("RXX") || reason.contains("xxxx"), "no value in the reason");
  }
}
