// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * A unique-index conflict waits on its own partition (plugin v53, G23).
 *
 * Debezium replicates an UPDATE of the primary key as a delete of the old key and an insert of
 * the new key; the records are keyed by different keys, so they can land on different
 * partitions and reach the sink in either order. Aurora DSQL checks a unique index at once, so
 * on a table with a unique index besides its primary key the new key's insert fails (23505)
 * while the old row still holds the value. Before v53 that insert was dead-lettered and the
 * old row's delete then applied: the row was missing. Since v53 the insert -- and everything
 * after it on its partition -- waits until the old row's delete has applied, for up to 5
 * minutes, while every other partition keeps applying.
 *
 * The harness is a stateful JDBC double of app.customers (PK id, UNIQUE email named
 * uk_email): DELETE and INSERT ... ON CONFLICT (id) behave as on PostgreSQL, a unique violation
 * is a PSQLException naming its constraint (as Aurora DSQL reports it), committed and in-flight
 * rows are kept apart, and a recording SinkTaskContext and a fake clock stand in for Connect.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

class DsqlSinkUniqueHoldTest {

  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
  private static final String TOPIC = "dsqlcdc.app.customers";
  private static final TopicPartition P0 = new TopicPartition(TOPIC, 0);
  private static final TopicPartition P1 = new TopicPartition(TOPIC, 1);
  private static final TopicPartition P2 = new TopicPartition(TOPIC, 2);
  // Debezium's PostgreSQL placeholder for an unchanged TOASTed value: the sink omits the column.
  private static final String UNAVAILABLE = "__debezium_unavailable_value";

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

  private static Struct row(int id, String email, String bio) {
    return new Struct(ROW).put("id", id).put("email", email).put("bio", bio);
  }

  private static SinkRecord change(
      TopicPartition tp, long offset, int id, String op, Struct before, Struct after) {
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", op)
            .put(
                "source",
                new Struct(SOURCE)
                    .put("connector", "postgresql")
                    .put("schema", "app")
                    .put("table", "customers"));
    if (before != null) {
      envelope.put("before", before);
    }
    if (after != null) {
      envelope.put("after", after);
    }
    Struct key = new Struct(KEY).put("id", id);
    return new SinkRecord(tp.topic(), tp.partition(), KEY, key, ENVELOPE, envelope, offset);
  }

  /** The insert half of a primary-key change (Debezium's CREATE keyed by the new key). */
  private static SinkRecord create(TopicPartition tp, long offset, int id, String email, String bio) {
    return change(tp, offset, id, "c", null, row(id, email, bio));
  }

  private static SinkRecord update(TopicPartition tp, long offset, int id, String email, String bio) {
    return change(tp, offset, id, "u", null, row(id, email, bio));
  }

  /** The delete half of a primary-key change (keyed by the old key). */
  private static SinkRecord delete(TopicPartition tp, long offset, int id) {
    return change(tp, offset, id, "d", row(id, null, null), null);
  }

  private static SinkRecord tombstone(TopicPartition tp, long offset, int id) {
    Struct key = new Struct(KEY).put("id", id);
    return new SinkRecord(tp.topic(), tp.partition(), KEY, key, null, null, offset);
  }

  // --- The JDBC double ----------------------------------------------------------------------------

  /** A unique violation as Aurora DSQL reports it: SQLSTATE 23505 and the constraint name. */
  static SQLException uniqueViolation(String constraint) {
    return new PSQLException(
        new ServerErrorMessage(
            "SERROR\0C23505\0Mduplicate key value violates unique constraint \""
                + constraint + "\"\0Dthis detail carries a value\0n" + constraint + "\0"));
  }

  private static final class FakeDsql {
    final List<String> calls = new ArrayList<>();
    final List<String> executed = new ArrayList<>(); // "upsert:<id>" / "delete:<id>"
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
      Map<String, Object> next = existing == null ? new LinkedHashMap<>() : new LinkedHashMap<>(existing);
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

  private static final class HoldTask extends DsqlSinkTask {
    long now = 1_000 * SECOND;
    final RecordingSinkTaskContext ctx = new RecordingSinkTaskContext();
    final List<Object> countedOps = new ArrayList<>(); // the pk of each op counted one by one

    @Override
    void recordOps(ChangeEvent event) {
      countedOps.add(event.pkValues().get(0));
      super.recordOps(event);
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
    void emitMetrics() {
      // never CloudWatch in a unit test
    }

    void advance(long nanos) {
      now += nanos;
    }
  }

  private static void setField(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  private static HoldTask task(FakeDsql dsql, RecordingReporter dlq, Map<String, String> extra)
      throws Exception {
    HoldTask task = new HoldTask();
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

  private static HoldTask task(FakeDsql dsql, RecordingReporter dlq) throws Exception {
    return task(dsql, dlq, Map.of());
  }

  private static Map<TopicPartition, OffsetAndMetadata> offsets(Object... pairs) {
    Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      out.put((TopicPartition) pairs[i], new OffsetAndMetadata((Long) pairs[i + 1]));
    }
    return out;
  }

  private static long committedOffset(
      Map<TopicPartition, OffsetAndMetadata> offsets, TopicPartition tp) {
    return offsets.get(tp).offset();
  }

  // --- The live repro ---------------------------------------------------------------------------

  @Test
  void aPrimaryKeyChangeWhoseInsertArrivesFirstInOnePutLosesNoRow() throws Exception {
    // UPDATE customers SET id = 100005 WHERE id = 5, email unique: partition 1's insert of the
    // new key arrives before partition 0's delete of the old key. FAILS ON v52: the insert was
    // dead-lettered (23505 on uk_email) and the delete then applied -- the row was gone.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);

    task.put(
        List.of(
            create(P1, 10, 100005, "a", "b5"), delete(P0, 20, 5), tombstone(P0, 21, 5)));

    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertEquals(List.of("100005:a:b5"), dsql.rows());
    // Resolved in the same put, before its partition was ever paused: rewound past it only.
    assertEquals(List.of("offset:" + TOPIC + "-1:11"), task.ctx.calls);
    assertTrue(task.heldOffsets().isEmpty());
    // Until the rewind to 11 has run (the next put), a commit cannot pass it.
    assertEquals(11L, committedOffset(task.preCommit(offsets(P1, 11L, P0, 22L)), P1));
    assertEquals(11L, committedOffset(task.preCommit(offsets(P1, 30L, P0, 22L)), P1));
    task.put(List.of(update(P1, 11, 100005, "a", "b6")));
    assertEquals(List.of("100005:a:b6"), dsql.rows());
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P1, 12L, P0, 22L);
    assertSame(current, task.preCommit(current)); // no hold: the default, the same map
  }

  @Test
  void aHeldPartitionWaitsAcrossPutsAndResumesAfterTheOldRowsDelete() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);

    // Put N: the new key's insert, then a later partial UPDATE of it (bio unchanged, so not a
    // full image) on the same partition. The old row's delete is still in partition 0.
    task.put(List.of(create(P1, 10, 100005, "a", "b5"), update(P1, 11, 100005, "c", UNAVAILABLE)));

    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    // The insert: once in the batch, once alone, once probed -- the later update never.
    assertEquals(3, dsql.count("upsert:100005"));
    assertEquals(List.of("5:a:b5"), dsql.rows());
    // Not paused yet: the next poll of partition 1 is read for a later change of the row.
    assertEquals(List.of("timeout:500"), task.ctx.calls);
    Map<TopicPartition, OffsetAndMetadata> committed =
        task.preCommit(offsets(P1, 12L, P0, 7L));
    assertEquals(10L, committedOffset(committed, P1)); // never past the held change
    assertEquals(7L, committedOffset(committed, P0)); // another partition: unclamped

    // Put N+1: the old row's delete, and partition 1's next record (another row, which does
    // not replace the held one): never written; partition 1 is rewound to the hold and paused.
    // The hold is not due yet (500 ms), so nothing probes.
    task.ctx.calls.clear();
    task.put(List.of(delete(P0, 20, 5), create(P1, 12, 77, "q", "r")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertEquals(List.of(), dsql.rows());
    assertEquals(0, dsql.count("upsert:77"));
    assertEquals(
        List.of("offset:" + TOPIC + "-1:10", "pause:" + TOPIC + "-1",
            "offset:" + TOPIC + "-1:10", "timeout:500"),
        task.ctx.calls);

    // The poll timeout brings an empty put: the probe applies, the partition is rewound past
    // it and resumed.
    task.ctx.calls.clear();
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("100005:a:b5"), dsql.rows());
    assertEquals(
        List.of("offset:" + TOPIC + "-1:11", "resume:" + TOPIC + "-1"), task.ctx.calls);

    // Connect fetches again from 11: the partial UPDATE applies after its insert, then 12.
    task.put(List.of(update(P1, 11, 100005, "c", UNAVAILABLE), create(P1, 12, 77, "q", "r")));
    assertEquals(List.of("77:q:r", "100005:c:b5"), dsql.rows());
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P1, 13L, P0, 21L);
    assertSame(current, task.preCommit(current));
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void withoutAHoldTheTaskNeverCallsTheContext() throws Exception {
    FakeDsql dsql = new FakeDsql();
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(create(P0, 1, 1, "a", "x"), create(P1, 1, 2, "b", "y"), delete(P0, 2, 1)));
    task.put(List.of());
    assertEquals(List.of(), task.ctx.calls);
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P0, 3L, P1, 2L);
    assertSame(current, task.preCommit(current));
    assertEquals(List.of("prepare", "batch", "prepare", "update", "commit"), dsql.calls);
  }

  @Test
  void laterChangesOfTheHeldRowNeverApplyBeforeIt() throws Exception {
    // The held insert sets email and bio; a later partial UPDATE sets bio only. Applied out of
    // order the update would be overwritten by the older insert's bio.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "old");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(
        List.of(
            create(P1, 10, 100005, "a", "old"),
            change(P1, 11, 100005, "u", null, row(100005, UNAVAILABLE, "new"))));
    assertEquals(3, dsql.count("upsert:100005")); // batch, alone, probe; never the update
    task.put(List.of(delete(P0, 3, 5)));
    task.advance(SECOND);
    task.put(List.of());
    task.put(List.of(change(P1, 11, 100005, "u", null, row(100005, UNAVAILABLE, "new"))));
    assertEquals(List.of("100005:a:new"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aLaterDeleteOfTheSameRowReplacesAConflictingInsert() throws Exception {
    // insert k, update k (partial), delete k on one partition while the old row still holds
    // the email: the insert is moot (the delete replaces it), the partial update applies, the
    // delete removes the row -- the source's final state, no hold, no dead letter.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(
        List.of(
            create(P1, 10, 100005, "a", "b5"),
            update(P1, 11, 100005, "z", UNAVAILABLE),
            delete(P1, 12, 100005)));
    assertTrue(task.heldOffsets().isEmpty());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertEquals(List.of("5:a:b5"), dsql.rows());
    assertEquals(List.of(), task.ctx.calls);
  }

  // --- A genuine conflict -----------------------------------------------------------------------

  @Test
  void aGenuineConflictIsDeadLetteredWithItsRealReasonAfterTheWait() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x"); // another row really holds the email; no delete is coming
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());

    // Tries: 500 ms, then doubling to a 10 s cap; an early empty put tries nothing.
    List<Long> timeouts = new ArrayList<>();
    timeouts.add(task.ctx.lastTimeout);
    long probes = dsql.count("upsert:100005");
    task.put(List.of());
    assertEquals(probes, dsql.count("upsert:100005")); // not due yet
    long waited = 0;
    while (dlq.records.isEmpty()) {
      long step = TimeUnit.MILLISECONDS.toNanos(task.ctx.lastTimeout);
      task.advance(step);
      waited += step;
      task.put(List.of());
      if (dlq.records.isEmpty()) {
        timeouts.add(task.ctx.lastTimeout);
      }
      assertTrue(waited <= DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS + 10 * SECOND, "bounded");
    }
    assertEquals(List.of(500L, 1000L, 2000L, 4000L, 8000L, 10000L, 10000L), timeouts.subList(0, 7));
    assertTrue(waited >= DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS);
    assertEquals(1, dlq.records.size());
    assertEquals(10L, dlq.records.get(0).kafkaOffset());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(11L, task.ctx.rewinds.get(P1));
    assertFalse(task.ctx.paused.contains(P1));
    // Later records apply.
    task.put(List.of(create(P1, 11, 8, "b", "z")));
    assertEquals(List.of("7:a:x", "8:b:z"), dsql.rows());

    // The reason (the log line the control plane parses), with no DLQ wired: unchanged.
    FakeDsql loud = new FakeDsql();
    loud.seed(7, "a", "x");
    HoldTask noDlq = task(loud, null);
    noDlq.put(List.of(create(P1, 10, 100005, "a", "y")));
    noDlq.advance(DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS + SECOND);
    ConnectException e = assertThrows(ConnectException.class, () -> noDlq.put(List.of()));
    String reason = e.getMessage().substring(e.getMessage().indexOf("): ") + 3);
    assertTrue(reason.startsWith("sqlstate=23505 duplicate key value violates unique "
        + "constraint \"uk_email\" [constraint: uk_email] | pk: id=100005 | op: c/r | sql: "
        + "INSERT INTO \"app\".\"customers\""), reason);
    assertFalse(reason.contains("this detail carries a value"), reason);
  }

  @Test
  void aUniqueConflictNeverArmsTheDriftCoolDown() throws Exception {
    assertFalse(DsqlSinkTask.isStructuralDrift("23505"));
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    assertTrue(task.driftCooledTables().isEmpty());
    task.advance(DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS + SECOND);
    task.put(List.of());
    assertEquals(1, dlq.records.size());
    assertTrue(task.driftCooledTables().isEmpty());
  }

  // --- G22: a landed XX000 commit re-run ------------------------------------------------------

  @Test
  void aReRunAfterALandedCommitSkipsTheReplacedImageInsteadOfDeadLetteringIt() throws Exception {
    // Row 1 takes email x, row 9 is deleted, row 1 moves to y and row 2 takes x -- one chunk.
    // Its COMMIT lands but answers XX000, so it re-runs on the state it already wrote: row 1's
    // first image (x) meets row 2 (x). FAILS ON v52: that image was dead-lettered.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "w", "b");
    dsql.seed(9, "q", "b");
    dsql.commitsUnavailable = 1;
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(
        List.of(
            update(P0, 1, 1, "x", "b"),
            delete(P0, 2, 9),
            update(P0, 3, 1, "y", "b"),
            create(P0, 4, 2, "x", "b")));
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("1:y:b", "2:x:b"), dsql.rows());
    assertEquals(1, dsql.calls.stream().filter("commit!XX000"::equals).count());
  }

  @Test
  void aPartialLaterImageDoesNotReplaceAFullOne() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(create(P1, 10, 100005, "a", "b5"), update(P1, 11, 100005, "a", UNAVAILABLE)));
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // held, not skipped
  }

  @Test
  void aChainOfPrimaryKeyChangesOnThreePartitionsConverges() throws Exception {
    // 5 -> 100005 -> 200005, email unique, delivered in the worst order: the last insert
    // first, the middle row's insert and delete, then the first row's delete.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(
        List.of(
            create(P2, 0, 200005, "a", "b"),
            create(P1, 0, 100005, "a", "b"),
            delete(P1, 1, 100005),
            tombstone(P1, 2, 100005)));
    assertEquals(Map.of(P2, 0L), task.heldOffsets());
    task.put(List.of(delete(P0, 0, 5), tombstone(P0, 1, 5)));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("200005:a:b"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  // --- The look-ahead: a hold never waits on its own partition's later change of the row -------

  @Test
  void aPrimaryKeyChangeAndItsReversalNeverWaitOnTheirOwnPartition() throws Exception {
    // 5 -> 100005 -> 5, email unique. Partition 0 (delete 5, create 5) is ahead, so the target
    // already holds 5:a when partition 1's create(100005) arrives at the end of a poll; the
    // delete(100005) that makes it moot is in partition 1's next poll. FAILED BEFORE THE
    // LOOK-AHEAD: the partition was paused at once, that delete never arrived, and after a
    // 5-minute stall the create was dead-lettered.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(delete(P0, 0, 5), tombstone(P0, 1, 5), create(P0, 2, 5, "a", "b")));
    assertEquals(List.of("5:a:b"), dsql.rows());
    task.put(List.of(create(P1, 0, 100005, "a", "b")));
    assertEquals(Map.of(P1, 0L), task.heldOffsets());
    assertFalse(task.ctx.paused.contains(P1)); // read one poll further first

    task.put(List.of(delete(P1, 1, 100005), tombstone(P1, 2, 100005)));
    assertTrue(task.heldOffsets().isEmpty());
    assertFalse(task.ctx.paused.contains(P1));
    assertEquals(1L, task.ctx.rewinds.get(P1)); // fetched again past the moot create
    assertEquals(0, dsql.count("delete:100005")); // nothing of the look-ahead is written
    task.put(List.of(delete(P1, 1, 100005), tombstone(P1, 2, 100005)));
    assertEquals(List.of("5:a:b"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aReplayedOlderImageWhoseNewerImageIsInTheNextPollIsNotDeadLettered() throws Exception {
    // A restart replays partition 1 from an older commit: the target already holds row 1 = b
    // and row 2 = a. update(1, a) arrives alone in a poll, its newer image update(1, b) in the
    // next. FAILED BEFORE THE LOOK-AHEAD: a 5-minute stall, then a dead letter.
    FakeDsql dsql = new FakeDsql();
    dsql.seed(1, "b", "x");
    dsql.seed(2, "a", "x");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(update(P1, 0, 1, "a", "x")));
    assertEquals(Map.of(P1, 0L), task.heldOffsets());
    task.put(List.of(update(P1, 1, 1, "b", "x")));
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(1L, task.ctx.rewinds.get(P1));
    task.put(List.of(update(P1, 1, 1, "b", "x")));
    assertEquals(List.of("1:b:x", "2:a:x"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    Map<TopicPartition, OffsetAndMetadata> current = offsets(P1, 2L);
    assertSame(current, task.preCommit(current));
  }

  @Test
  void aLookAheadThatFindsNoLaterChangeOfTheRowPausesAndStillDeadLettersAGenuineConflict()
      throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x"); // another row really holds the email
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    // The next poll carries another row on that partition: it does not replace the held one.
    task.put(List.of(create(P1, 11, 8, "b", "z")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(task.ctx.paused.contains(P1));
    assertEquals(10L, task.ctx.rewinds.get(P1));
    assertEquals(0, dsql.count("upsert:8"));
    task.advance(DsqlSinkTask.UNIQUE_HOLD_BUDGET_NANOS + SECOND);
    task.put(List.of());
    assertEquals(1, dlq.records.size());
    assertEquals(10L, dlq.records.get(0).kafkaOffset());
    assertEquals("23505", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertEquals(11L, task.ctx.rewinds.get(P1));
    assertFalse(task.ctx.paused.contains(P1));
    task.put(List.of(create(P1, 11, 8, "b", "z")));
    assertEquals(List.of("7:a:x", "8:b:z"), dsql.rows());
  }

  // --- Ordering and the superseded rule ---------------------------------------------------------

  @Test
  void aHeldPartitionsTailInALaterChunkOfTheSamePutWaits() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "old");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, Map.of(DsqlSinkConnectorConfig.BATCH_SIZE, "2"));
    // Chunk 1 holds P1@10; chunk 2 is P1@11, the held row's partial update.
    task.put(
        List.of(
            create(P1, 10, 100005, "a", "old"),
            create(P0, 1, 50, "o", "p"),
            update(P1, 11, 100005, "a", UNAVAILABLE)));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    assertEquals(3, dsql.count("upsert:100005")); // batch, alone, probe: never the tail
    task.put(List.of(delete(P0, 3, 5)));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    task.put(List.of(update(P1, 11, 100005, "a", UNAVAILABLE)));
    assertEquals(List.of("50:o:p", "100005:a:old"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void anEarlierDeleteOfTheRowDoesNotReplaceItsReinsert() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    dsql.seed(100005, "z", "old");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(delete(P1, 9, 100005), create(P1, 10, 100005, "a", "new")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // held, never skipped
    task.put(List.of(update(P0, 3, 5, "q", "b5")));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("5:q:b5", "100005:a:new"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void anEarlierSameColumnUpsertDoesNotReplaceALaterInsert() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(update(P1, 9, 100005, "m", "x"), update(P1, 10, 100005, "a", "new")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
  }

  // --- Refusals, other codes and side effects ----------------------------------------------------

  @Test
  void aRecordRefusedOnAnotherPartitionNeverSkipsTheHeldPartitionsTail() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    String huge = "x".repeat(DsqlSinkTask.DSQL_MAX_VALUE_BYTES + 1);
    // P1@10 is held; P0@11 (the same offset as P1's tail) is refused before any write.
    task.put(List.of(create(P1, 10, 100005, "a", "b5"), create(P0, 11, 300, "e", huge)));
    assertEquals(1, dlq.records.size());
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    task.put(List.of(delete(P0, 12, 5)));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    // P1's tail is fetched again: its healthy offset 11 applies.
    task.put(List.of(create(P1, 11, 301, "f", "g")));
    assertEquals(List.of("301:f:g", "100005:a:b5"), dsql.rows());
    assertEquals(1, dlq.records.size());
  }

  @Test
  void aProbeRejectedForAnotherReasonIsDeadLetteredAtOnce() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(7, "a", "x");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    task.put(List.of(create(P1, 10, 100005, "a", "y")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    dsql.failure =
        (sql, binds) -> sql.startsWith("INSERT") ? new SQLException("check", "23514") : null;
    task.advance(SECOND);
    task.put(List.of());
    assertEquals(1, dlq.records.size());
    assertEquals("23514", ((SQLException) dlq.causes.get(0)).getSQLState());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(11L, task.ctx.rewinds.get(P1));
    assertFalse(task.ctx.paused.contains(P1));
  }

  @Test
  void aResolvedHeldChangeIsCountedOnceAndEndsItsTablesCoolDown() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    // A column-drift rejection puts the table on the one-row path (a delete never ends it).
    dsql.failure =
        (sql, binds) -> sql.startsWith("INSERT") ? new SQLException("no column", "42703") : null;
    task.put(List.of(create(P0, 1, 6, "c", "d")));
    assertEquals(1, dlq.records.size());
    assertFalse(task.driftCooledTables().isEmpty());
    dsql.failure = (sql, binds) -> null;
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    task.put(List.of(delete(P0, 2, 5)));
    assertFalse(task.driftCooledTables().isEmpty());
    task.countedOps.clear();
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of(100005), task.countedOps); // the probe's applied op, counted once
    assertTrue(task.driftCooledTables().isEmpty());
    assertEquals(1, dlq.records.size());
  }

  @Test
  void aPutOfOnlySkippedRecordsStillTriesADueHoldAndNeverParsesAHeldTail() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    String huge = "x".repeat(DsqlSinkTask.DSQL_MAX_VALUE_BYTES + 1);
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    task.put(List.of(create(P1, 12, 77, "q", "r"))); // the look-ahead poll: paused
    assertTrue(task.ctx.paused.contains(P1));
    // A redelivered held tail that would be refused before any write: skipped unparsed.
    task.put(List.of(create(P1, 11, 300, "e", huge)));
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    task.put(List.of(delete(P0, 3, 5)));
    task.advance(SECOND);
    task.ctx.calls.clear();
    // Only held-tail records: nothing to apply, but the due hold is tried and released.
    task.put(List.of(create(P1, 11, 300, "e", huge), create(P1, 12, 77, "q", "r")));
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of("offset:" + TOPIC + "-1:11", "resume:" + TOPIC + "-1"), task.ctx.calls);
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
    // Fetched again from 11: the oversized row is dead-lettered exactly once, then 77 applies.
    task.put(List.of(create(P1, 11, 300, "e", huge), create(P1, 12, 77, "q", "r")));
    assertEquals(1, dlq.records.size());
    assertEquals(List.of("77:q:r", "100005:a:b5"), dsql.rows());
  }

  // --- What is not held ---------------------------------------------------------------------------

  @Test
  void withDeletesOffAUniqueConflictIsDeadLetteredAtOnce() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq, Map.of(DsqlSinkConnectorConfig.DELETE_ENABLED, "false"));
    task.put(List.of(create(P1, 10, 100005, "a", "b")));
    assertEquals(1, dlq.records.size());
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(List.of(), task.ctx.calls);
  }

  @Test
  void deletesAndOtherCodesAreNeverHeld() throws Exception {
    for (String state : List.of("23505", "23502", "23514", "42703")) {
      FakeDsql dsql = new FakeDsql();
      dsql.seed(5, "a", "b");
      dsql.failure =
          (sql, binds) ->
              ("23505".equals(state) ? sql.startsWith("DELETE") : sql.startsWith("INSERT"))
                  ? ("23505".equals(state)
                      ? uniqueViolation("uk_email")
                      : new SQLException("rejected", state))
                  : null;
      RecordingReporter dlq = new RecordingReporter();
      HoldTask task = task(dsql, dlq);
      task.put(
          List.of("23505".equals(state) ? delete(P0, 1, 5) : create(P0, 1, 6, "c", "d")));
      assertEquals(1, dlq.records.size(), state);
      assertTrue(task.heldOffsets().isEmpty(), state);
      assertEquals(List.of(), task.ctx.calls, state);
    }
  }

  // --- Redelivery, refusals, rebalance --------------------------------------------------------

  @Test
  void aTransientFailureOfTheProbeKeepsTheHoldAndARedeliverySkipsTheHeldTail() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    List<SinkRecord> batch =
        List.of(
            create(P0, 3, 50, "o", "p"),
            create(P1, 10, 100005, "a", "b5"),
            update(P1, 11, 100005, "c", UNAVAILABLE));
    task.put(batch);
    assertEquals(Map.of(P1, 10L), task.heldOffsets());

    dsql.failure = (sql, binds) -> new SQLException("connection lost", "08006");
    task.advance(SECOND);
    // G25: the probe's transient failure no longer fails the put (every write of it already
    // committed): the hold survives and is tried again after the first wait.
    task.put(List.of());
    assertEquals(Map.of(P1, 10L), task.heldOffsets()); // the hold survives
    assertEquals(500L, task.ctx.lastTimeout);
    dsql.failure = (sql, binds) -> null;
    setField(task, "connection", dsql.connection()); // the reconnect

    // The same batch again: row 50 replays, the held tail is skipped unparsed.
    dsql.executed.clear();
    task.advance(SECOND);
    task.put(batch);
    assertEquals(1, dsql.count("upsert:50"));
    assertEquals(1, dsql.count("upsert:100005")); // only the end-of-put probe, still 23505
    assertEquals(Map.of(P1, 10L), task.heldOffsets());
    // A redelivery brings nothing past what the put that made the hold carried: the
    // look-ahead stays open, so the partition is not paused yet.
    assertFalse(task.ctx.paused.contains(P1));

    task.put(List.of(delete(P0, 4, 5)));
    task.advance(10 * SECOND);
    task.put(List.of());
    task.put(List.of(update(P1, 11, 100005, "c", UNAVAILABLE)));
    assertEquals(List.of("50:o:p", "100005:c:b5"), dsql.rows());
    assertTrue(dlq.records.isEmpty(), () -> String.valueOf(dlq.causes));
  }

  @Test
  void aRecordRefusedBeforeAnyWriteAheadOfAHoldIsDeadLetteredOnce() throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    RecordingReporter dlq = new RecordingReporter();
    HoldTask task = task(dsql, dlq);
    String huge = "x".repeat(DsqlSinkTask.DSQL_MAX_VALUE_BYTES + 1);
    task.put(List.of(create(P1, 10, 100005, "a", "b5"), create(P1, 11, 300, "e", huge)));
    assertEquals(1, dlq.records.size()); // the oversized row, refused before any write
    task.put(List.of(delete(P0, 1, 5)));
    task.advance(SECOND);
    task.put(List.of());
    assertTrue(task.heldOffsets().isEmpty());
    // The released cap holds the commit exactly at the refused offset: it is kept.
    assertEquals(11L, committedOffset(task.preCommit(offsets(P1, 12L)), P1));
    // Fetched again from 11: not dead-lettered a second time, even on a second re-fetch.
    task.put(List.of(create(P1, 11, 300, "e", huge), create(P1, 12, 301, "f", "g")));
    task.put(List.of(create(P1, 11, 300, "e", huge)));
    assertEquals(1, dlq.records.size());
    assertEquals(List.of("301:f:g", "100005:a:b5"), dsql.rows());
    // Once the commit passes it, it is forgotten.
    task.preCommit(offsets(P1, 13L));
    task.put(List.of(create(P1, 11, 300, "e", huge)));
    assertEquals(2, dlq.records.size());
  }

  @Test
  void aRebalanceDropsTheHoldAfterItsCappedCommitAndResumesTheReassignedPartition()
      throws Exception {
    FakeDsql dsql = new FakeDsql();
    dsql.seed(5, "a", "b5");
    HoldTask task = task(dsql, new RecordingReporter());
    task.put(List.of(create(P1, 10, 100005, "a", "b5")));
    assertEquals(10L, committedOffset(task.preCommit(offsets(P1, 11L)), P1));
    task.put(List.of(create(P1, 11, 6, "c", "d"))); // the look-ahead poll: now paused
    assertTrue(task.ctx.paused.contains(P1));
    assertEquals(10L, committedOffset(task.preCommit(offsets(P1, 12L)), P1));

    task.close(List.of(P1));
    assertTrue(task.heldOffsets().isEmpty());
    assertEquals(15L, committedOffset(task.preCommit(offsets(P1, 15L)), P1));

    task.ctx.calls.clear();
    task.ctx.assigned.add(P1);
    task.open(List.of(P1));
    assertEquals(List.of("resume:" + TOPIC + "-1"), task.ctx.calls);
    task.ctx.calls.clear();
    task.open(List.of(P1)); // no hold paused it any more: nothing
    assertEquals(List.of(), task.ctx.calls);
  }

  @Test
  void theHoldLogLinesCarryNoValue() {
    assertNotNull(DsqlSinkTask.UNIQUE_HOLD_START);
    for (String line :
        List.of(
            DsqlSinkTask.UNIQUE_HOLD_START,
            DsqlSinkTask.UNIQUE_HOLD_RESOLVED,
            DsqlSinkTask.UNIQUE_HOLD_EXPIRED,
            DsqlSinkTask.UNIQUE_SUPERSEDED)) {
      // Placeholders are table, index, partition, offset, durations and counts only.
      assertTrue(line.split("\\{}", -1).length - 1 <= 4, line);
    }
    assertEquals("uk_email", DsqlSinkTask.constraintName(uniqueViolation("uk_email")));
    assertEquals("(unnamed)", DsqlSinkTask.constraintName(new SQLException("dup", "23505")));
    assertEquals(500L, TimeUnit.NANOSECONDS.toMillis(DsqlSinkTask.holdWaitNanos(1)));
    assertEquals(10_000L, TimeUnit.NANOSECONDS.toMillis(DsqlSinkTask.holdWaitNanos(40)));
  }
}
