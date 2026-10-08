// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * OCC retry on an ABORTED transaction (G21). Aurora DSQL can report a 40001 -- OC001, the
 * session's cached catalog is behind after a DDL, GRANT or REVOKE elsewhere on the cluster --
 * when a statement RUNS, not only at COMMIT. In PostgreSQL semantics the failed statement
 * leaves the transaction aborted: every later statement fails with 25P02 until ROLLBACK, and
 * pgjdbc's isValid() still answers true, so the task keeps the connection. The double below
 * models exactly that (and pgjdbc's client-side no-op rollback when no transaction is open);
 * the doubles of the other sink tests keep executing after an error, which is why they never
 * saw the retry run on an aborted transaction.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkOccRollbackTest {

  // --- Records: shop.orders(id bigint, flag boolean <- MySQL TINYINT(1), an INT16) ----------

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("flag", Schema.OPTIONAL_INT16_SCHEMA)
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

  private static SinkRecord insert(String table, long id) {
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", "c")
            .put("source", new Struct(SOURCE).put("db", "shop").put("table", table))
            .put("after", new Struct(ROW).put("id", id).put("flag", (short) 1));
    return new SinkRecord(
        "dsqlcdc.shop." + table, 0, KEY, new Struct(KEY).put("id", id), ENVELOPE, envelope,
        offset++);
  }

  // --- The JDBC double: PostgreSQL aborted-transaction semantics, as pgjdbc sees them --------

  static final class AbortingDsql {
    enum Tx { IDLE, OPEN, ABORTED }

    Tx tx = Tx.IDLE;
    final List<String> calls = new ArrayList<>();
    final List<Long> committed = new ArrayList<>(); // committed ids
    private final List<Long> pending = new ArrayList<>();
    private int executes;
    private int describes;
    Predicate<Integer> conflictOnExecute = n -> false; // execute #n raises 40001 (OC001)
    Predicate<Integer> unavailableOnExecute = n -> false; // execute #n raises XX000
    Predicate<Integer> conflictOnDescribe = n -> false; // describe #n raises 40001 (OC001)
    Predicate<Integer> unavailableOnDescribe = n -> false; // describe #n raises XX000
    int commitConflicts; // the next N commits raise 40001 (OC000)
    int commitsUnavailable; // the next N commits raise XX000 ...
    boolean unavailableCommitLands; // ... after committing (Aurora DSQL's uncertain outcome)
    Predicate<Long> rejected = id -> false; // rows Aurora DSQL rejects for good (23505)
    SQLException rollbackFailure; // thrown once, by the first rollback past the skipped ones
    int rollbacksBeforeFailure; // rollbacks that still succeed before rollbackFailure
    boolean closed;

    @SuppressWarnings("unchecked")
    static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static SQLException aborted() {
      return new SQLException(
          "current transaction is aborted, commands ignored until end of transaction block",
          "25P02");
    }

    /** Aurora DSQL's temporary service condition; the session stays open (G22). */
    static SQLException serverUnavailable() {
      return new SQLException("server unavailable", "XX000");
    }

    /** A failed statement inside a transaction aborts it (PostgreSQL). */
    private SQLException fail(String call, SQLException e) {
      calls.add(call + "!" + e.getSQLState());
      tx = Tx.ABORTED;
      return e;
    }

    private Object execute(String op, List<List<Object>> rows, Object result)
        throws SQLException {
      if (tx == Tx.ABORTED) {
        throw fail(op, aborted());
      }
      tx = Tx.OPEN; // pgjdbc sends BEGIN before the first statement (autocommit off)
      int n = ++executes;
      if (conflictOnExecute.test(n)) {
        throw fail(op, new SQLException("schema has been updated by another transaction "
            + "(OC001)", "40001"));
      }
      if (unavailableOnExecute.test(n)) {
        throw fail(op, serverUnavailable());
      }
      for (List<Object> row : rows) {
        // A Number bound to the boolean column is what PostgreSQL rejects (42804) -- the
        // sink converts 0/1 to a Boolean only when it has the parameter metadata.
        if (!(row.get(1) instanceof Boolean)) {
          throw fail(op, new SQLException("column \"flag\" is of type boolean but expression "
              + "is of type smallint", "42804"));
        }
        if (rejected.test((Long) row.get(0))) {
          throw fail(op, new SQLException("duplicate key value violates unique constraint",
              "23505"));
        }
      }
      calls.add(op);
      for (List<Object> row : rows) {
        pending.add((Long) row.get(0));
      }
      return result;
    }

    private ParameterMetaData describe() throws SQLException {
      if (tx == Tx.ABORTED) {
        throw fail("describe", aborted());
      }
      int n = ++describes;
      SQLException e =
          conflictOnDescribe.test(n)
              ? new SQLException("schema has been updated by another transaction (OC001)",
                  "40001")
              : unavailableOnDescribe.test(n) ? serverUnavailable() : null;
      if (e != null) {
        if (tx == Tx.OPEN) {
          throw fail("describe", e); // inside the transaction: it is now aborted
        }
        // pgjdbc sends it with no BEGIN: nothing is aborted
        calls.add("describe!" + e.getSQLState());
        throw e;
      }
      return proxy(
          ParameterMetaData.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "getParameterType" -> ((Integer) a[0]) == 2 ? Types.BOOLEAN : Types.BIGINT;
                case "getParameterCount" -> 2;
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }

    private PreparedStatement statement() {
      List<Object> current = new ArrayList<>();
      List<List<Object>> batch = new ArrayList<>();
      return proxy(
          PreparedStatement.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "getParameterMetaData" -> describe();
                case "close" -> null;
                case "setObject" -> {
                  current.add(a[1]);
                  yield null;
                }
                case "addBatch" -> {
                  batch.add(new ArrayList<>(current));
                  current.clear();
                  yield null;
                }
                case "executeUpdate" ->
                    execute("update", batch.isEmpty() ? List.of(current) : batch, 1);
                case "executeBatch" -> execute("batch", batch, new int[batch.size()]);
                case "toString" -> "AbortingStatement";
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
                case "isClosed" -> closed;
                case "isValid" -> !closed; // pgjdbc: true in an aborted transaction (25P02)
                case "prepareStatement" -> {
                  calls.add("prepare");
                  yield statement();
                }
                case "commit" -> {
                  if (tx == Tx.IDLE) {
                    yield null; // pgjdbc sends nothing
                  }
                  if (tx == Tx.ABORTED) {
                    // PostgreSQL turns COMMIT of an aborted transaction into ROLLBACK,
                    // with no error (pgjdbc 42.7.12 raises nothing).
                    calls.add("commit(rolled back)");
                    pending.clear();
                    tx = Tx.IDLE;
                    yield null;
                  }
                  if (commitConflicts > 0) {
                    commitConflicts--;
                    calls.add("commit!40001");
                    pending.clear();
                    tx = Tx.IDLE; // a failed COMMIT ends the transaction
                    throw new SQLException("change conflicts with another transaction (OC000)",
                        "40001");
                  }
                  if (commitsUnavailable > 0) {
                    commitsUnavailable--;
                    calls.add("commit!XX000");
                    if (unavailableCommitLands) {
                      committed.addAll(pending); // it committed; the client got an error
                    }
                    pending.clear();
                    tx = Tx.IDLE; // a failed COMMIT ends the transaction
                    throw serverUnavailable();
                  }
                  calls.add("commit");
                  committed.addAll(pending);
                  pending.clear();
                  tx = Tx.IDLE;
                  yield null;
                }
                case "rollback" -> {
                  if (rollbackFailure != null) {
                    if (rollbacksBeforeFailure > 0) {
                      rollbacksBeforeFailure--; // this one still succeeds
                    } else {
                      SQLException e = rollbackFailure;
                      rollbackFailure = null;
                      calls.add("rollback!" + e.getSQLState());
                      throw e; // the transaction is left as it was
                    }
                  }
                  calls.add(tx == Tx.IDLE ? "rollback(no-op)" : "rollback");
                  pending.clear();
                  tx = Tx.IDLE;
                  yield null;
                }
                case "close" -> {
                  calls.add("close");
                  closed = true;
                  yield null;
                }
                case "toString" -> "AbortingDsql";
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }
  }

  static final class Dlq implements ErrantRecordReporter {
    final List<Throwable> causes = new ArrayList<>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      causes.add(error);
      return CompletableFuture.completedFuture(null);
    }

    List<String> states() {
      List<String> out = new ArrayList<>();
      for (Throwable t : causes) {
        out.add(t instanceof SQLException e ? e.getSQLState() : t.getClass().getSimpleName());
      }
      return out;
    }
  }

  /** No guarded target columns: no catalog round trip in these tests. */
  static final class Task extends DsqlSinkTask {
    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }
  }

  private static Task task(AbortingDsql dsql, Dlq dlq) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    Task task = new Task();
    task.startMetrics(props);
    set(task, "connection", dsql.connection());
    set(task, "dlqReporter", dlq);
    return task;
  }

  private static void set(DsqlSinkTask task, String name, Object value) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(task, value);
  }

  private static Object get(DsqlSinkTask task, String name) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(name);
    field.setAccessible(true);
    return field.get(task);
  }

  // --- A 40001 when a statement runs: the retry starts on a clean transaction ---------------

  @Test
  void aOneRowChunkWhoseStatementRaises40001IsAppliedNotDeadLettered() throws Exception {
    // FAILS WITHOUT THE FIX: the retry ran on the aborted transaction, got 25P02 (not
    // retried), and the healthy row went to the DLQ.
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnExecute = n -> n == 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update!40001", "rollback", "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aMultiRowChunkCommitsOnTheBatchedPathAfterAStatement40001() throws Exception {
    // FAILS WITHOUT THE FIX: the batched retry got 25P02, so the chunk fell back to one row
    // per transaction (three commits instead of one).
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnExecute = n -> n == 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3)));
    assertEquals(List.of(1L, 2L, 3L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "batch!40001", "rollback", "prepare", "batch", "commit"), dsql.calls);
  }

  @Test
  void aRowOfTheOneRowPerTransactionPathWhoseStatementRaises40001IsApplied() throws Exception {
    // Row 2 is rejected for good (23505), so the chunk falls back to one row per transaction;
    // there, row 1's statement (execute #2) raises 40001 once. FAILS WITHOUT THE FIX: healthy
    // row 1 was dead-lettered with 25P02 next to row 2.
    AbortingDsql dsql = new AbortingDsql();
    dsql.rejected = id -> id == 2L;
    dsql.conflictOnExecute = n -> n == 2;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3)));
    assertEquals(List.of(1L, 3L), dsql.committed);
    assertEquals(List.of("23505"), dlq.states());
    assertEquals(
        List.of("prepare", "batch!23505", "rollback",
            "prepare", "update!40001", "rollback", "prepare", "update", "commit",
            "prepare", "update!23505", "rollback",
            "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aCommitTime40001StillRetriesAndItsRollbackSendsNothing() throws Exception {
    // Unchanged behaviour: a COMMIT-time conflict already ended the transaction; the rollback
    // before the retry is pgjdbc's client-side no-op.
    AbortingDsql dsql = new AbortingDsql();
    dsql.commitConflicts = 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update", "commit!40001", "rollback(no-op)", "prepare", "update",
            "commit"),
        dsql.calls);
  }

  @Test
  void aConflictThatNeverClearsIsRedeliveredNeverDeadLettered() throws Exception {
    // FAILS WITHOUT THE FIX: the second attempt got 25P02 (permanent), so a one-row chunk
    // was dead-lettered after ONE conflict instead of being redelivered after the budget.
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnExecute = n -> true;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    RetriableException e =
        assertThrows(RetriableException.class, () -> task.put(List.of(insert("orders", 1))));
    assertEquals("40001", ((SQLException) e.getCause()).getSQLState());
    assertEquals(List.of(), dlq.states());
    assertEquals(10, dsql.calls.stream().filter("update!40001"::equals).count()); // the budget
    assertEquals(10, dsql.calls.stream().filter("rollback"::equals).count()); // 9 + the caller's
    assertEquals("close", dsql.calls.get(dsql.calls.size() - 1)); // reconnect on redelivery
    assertNull(get(task, "connection"));
  }

  @Test
  void aPermanentErrorIsNotRetriedAndRollsBackOnce() throws Exception {
    // Unchanged behaviour: any other error is the caller's, with one rollback.
    AbortingDsql dsql = new AbortingDsql();
    dsql.rejected = id -> true;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of("prepare", "update!23505", "rollback"), dsql.calls);
    assertEquals(List.of("23505"), dlq.states());
  }

  // --- A 40001 from the parameter describe is retried too ------------------------------------

  @Test
  void aDescribe40001IsRetriedSoTheBooleanStillBindsAsBoolean() throws Exception {
    // FAILS WITHOUT THE FIX: the describe's 40001 was swallowed, the 0/1 bound as a number,
    // Aurora DSQL rejected it (42804) and the healthy row was dead-lettered -- and its table
    // put on the drift cool-down for a type change that never happened.
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnDescribe = n -> n == 1;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    task.put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertTrue(task.driftCooledTables().isEmpty());
    assertEquals(
        List.of("prepare", "describe!40001", "rollback(no-op)", "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aDescribe40001InsideTheTransactionIsRolledBackAndTheChunkRetriedBatched()
      throws Exception {
    // Two tables -> two statement groups; the second group's describe runs inside the open
    // transaction and aborts it. FAILS WITHOUT THE FIX: swallowed, the next execute got
    // 25P02, and the chunk fell back to one row per transaction.
    AbortingDsql dsql = new AbortingDsql();
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    dsql.conflictOnDescribe = n -> n == 2; // group 2's describe, inside the transaction
    task.put(List.of(insert("orders", 1), insert("items", 2)));
    assertEquals(List.of(1L, 2L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update", "prepare", "describe!40001", "rollback", "prepare",
            "update", "prepare", "update", "commit"),
        dsql.calls);
  }

  // --- The rollback itself failing: the next attempt opens a new session ---------------------

  @Test
  void aRollbackThatFailsDropsTheConnectionSoTheRetryReconnects() throws Exception {
    AbortingDsql dsql = new AbortingDsql();
    dsql.rollbackFailure = new SQLException("An I/O error occurred while sending", "08006");
    Task task = task(dsql, new Dlq());
    task.rollbackBeforeRetry();
    assertNull(get(task, "connection"));
    assertEquals(List.of("rollback!08006", "close"), dsql.calls);
  }

  @Test
  void aRollbackThatFailsOnTheRetryPathReconnectsInsteadOfDeadLettering() throws Exception {
    // The retry path must step through rollbackBeforeRetry, not rollbackQuietly: a rollback
    // that fails while the session stays open (XX000 "server unavailable": Aurora DSQL keeps
    // the connection) leaves the transaction aborted, so a retry on that session would get
    // 25P02 and dead-letter the healthy row. FAILS if the call site passes rollbackQuietly.
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnExecute = n -> n == 1;
    dsql.rollbackFailure = new SQLException("server unavailable", "XX000");
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    AtomicInteger reconnects = offline(task);
    assertThrows(IllegalStateException.class, () -> task.put(List.of(insert("orders", 1))));
    assertEquals(List.of("prepare", "update!40001", "rollback!XX000", "close"), dsql.calls);
    assertEquals(1, reconnects.get()); // the retry opened a new session
    assertEquals(List.of(), dlq.states());
  }

  @Test
  void aRollbackThatFailsOnTheOneRowPerTransactionRetryPathReconnectsToo() throws Exception {
    // The same on the record-by-record path: row 2 is rejected for good (23505), so the chunk
    // falls back to one row per transaction (the chunk's own rollback still succeeds); there,
    // row 1's statement (execute #2) raises 40001 and the rollback before its retry fails.
    // FAILS if that call site passes rollbackQuietly (healthy row 1 dead-lettered, 25P02).
    AbortingDsql dsql = new AbortingDsql();
    dsql.rejected = id -> id == 2L;
    dsql.conflictOnExecute = n -> n == 2;
    dsql.rollbackFailure = new SQLException("server unavailable", "XX000");
    dsql.rollbacksBeforeFailure = 1; // the chunk's rollback after its 23505
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    AtomicInteger reconnects = offline(task);
    assertThrows(
        IllegalStateException.class,
        () -> task.put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3))));
    assertEquals(
        List.of("prepare", "batch!23505", "rollback",
            "prepare", "update!40001", "rollback!XX000", "close"),
        dsql.calls);
    assertEquals(1, reconnects.get());
    assertEquals(List.of(), dlq.states());
  }

  // --- G22: Aurora DSQL's XX000 "server unavailable" is retried, never dead-lettered ----------
  //
  // Aurora DSQL documents XX000 as a temporary service condition: the connection stays
  // usable, and the whole transaction, COMMIT included, is to be retried with backoff and
  // jitter. Before G22 it was permanent, so a one-row chunk (or each row of the one-row
  // fallback that met it) was dead-lettered although healthy.

  @Test
  void aOneRowChunkWhoseStatementRaisesXX000IsAppliedNotDeadLettered() throws Exception {
    // FAILS WITHOUT G22: the XX000 was not retried and the healthy row went to the DLQ.
    AbortingDsql dsql = new AbortingDsql();
    dsql.unavailableOnExecute = n -> n == 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update!XX000", "rollback", "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aMultiRowChunkCommitsOnTheBatchedPathAfterAStatementXX000() throws Exception {
    // FAILS WITHOUT G22: the chunk fell back to one row per transaction (three commits).
    AbortingDsql dsql = new AbortingDsql();
    dsql.unavailableOnExecute = n -> n == 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3)));
    assertEquals(List.of(1L, 2L, 3L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "batch!XX000", "rollback", "prepare", "batch", "commit"), dsql.calls);
  }

  @Test
  void aCommitTimeXX000ReRunsTheWholeTransaction() throws Exception {
    // The COMMIT failed and did not land: the whole transaction, COMMIT included, re-runs.
    // FAILS WITHOUT G22: the row was dead-lettered.
    AbortingDsql dsql = new AbortingDsql();
    dsql.commitsUnavailable = 1;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update", "commit!XX000", "rollback(no-op)", "prepare", "update",
            "commit"),
        dsql.calls);
  }

  @Test
  void aCommitTimeXX000ThatLandedIsReRunHarmlessly() throws Exception {
    // Aurora DSQL's uncertain outcome: the COMMIT landed but answered XX000. The re-run writes
    // the same row again -- an idempotent upsert -- and nothing is dead-lettered.
    AbortingDsql dsql = new AbortingDsql();
    dsql.commitsUnavailable = 1;
    dsql.unavailableCommitLands = true;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L, 1L), dsql.committed); // applied twice: the same upsert
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update", "commit!XX000", "rollback(no-op)", "prepare", "update",
            "commit"),
        dsql.calls);
  }

  @Test
  void aServerUnavailableThatNeverClearsIsRedeliveredNeverDeadLettered() throws Exception {
    // FAILS WITHOUT G22: the healthy row was dead-lettered at the first XX000.
    AbortingDsql dsql = new AbortingDsql();
    dsql.unavailableOnExecute = n -> true;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    RetriableException e =
        assertThrows(RetriableException.class, () -> task.put(List.of(insert("orders", 1))));
    assertEquals("XX000", ((SQLException) e.getCause()).getSQLState());
    assertEquals(List.of(), dlq.states());
    assertEquals(10, dsql.calls.stream().filter("update!XX000"::equals).count()); // the budget
    assertEquals(10, dsql.calls.stream().filter("rollback"::equals).count()); // 9 + the caller's
    assertEquals("close", dsql.calls.get(dsql.calls.size() - 1)); // reconnect on redelivery
    assertNull(get(task, "connection"));
  }

  @Test
  void aSustainedXX000OnTheOneRowPathIsRedeliveredNotDeadLettered() throws Exception {
    // Row 2 is rejected for good (23505), so the chunk falls back to one row per transaction;
    // there, every statement meets XX000. FAILS WITHOUT G22: healthy row 1 (and row 3) were
    // dead-lettered with XX000 next to row 2.
    AbortingDsql dsql = new AbortingDsql();
    dsql.rejected = id -> id == 2L;
    dsql.unavailableOnExecute = n -> n >= 2;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    assertThrows(
        RetriableException.class,
        () -> task.put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3))));
    assertEquals(List.of(), dlq.states());
    assertEquals(List.of(), dsql.committed);
  }

  @Test
  void xx000And40001ShareOneBudget() throws Exception {
    // One occ.max.retries budget for both (the ~21 s per-unit bound the poll interval relies
    // on), then the transient path.
    AbortingDsql dsql = new AbortingDsql();
    dsql.conflictOnExecute = n -> n % 2 == 1;
    dsql.unavailableOnExecute = n -> n % 2 == 0;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    assertThrows(RetriableException.class, () -> task.put(List.of(insert("orders", 1))));
    assertEquals(10, dsql.calls.stream().filter(c -> c.startsWith("update!")).count());
    assertEquals(List.of(), dlq.states());
  }

  @Test
  void aDescribeXX000IsRetriedSoTheBooleanStillBindsAsBoolean() throws Exception {
    // FAILS WITHOUT G22: the describe's XX000 was swallowed, the 0/1 bound as a number, Aurora
    // DSQL rejected it (42804) and the healthy row was dead-lettered -- and its table put on
    // the drift cool-down for a type change that never happened.
    AbortingDsql dsql = new AbortingDsql();
    dsql.unavailableOnDescribe = n -> n == 1;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    task.put(List.of(insert("orders", 1)));
    assertEquals(List.of(1L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertTrue(task.driftCooledTables().isEmpty());
    assertEquals(
        List.of("prepare", "describe!XX000", "rollback(no-op)", "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aDescribeXX000InsideTheTransactionIsRolledBackAndTheChunkRetriedBatched()
      throws Exception {
    // The second statement group's describe runs inside the open transaction and aborts it.
    // FAILS WITHOUT G22: swallowed, the next execute got 25P02 and the chunk fell back to one
    // row per transaction.
    AbortingDsql dsql = new AbortingDsql();
    dsql.unavailableOnDescribe = n -> n == 2;
    Dlq dlq = new Dlq();
    task(dsql, dlq).put(List.of(insert("orders", 1), insert("items", 2)));
    assertEquals(List.of(1L, 2L), dsql.committed);
    assertEquals(List.of(), dlq.states());
    assertEquals(
        List.of("prepare", "update", "prepare", "describe!XX000", "rollback", "prepare",
            "update", "prepare", "update", "commit"),
        dsql.calls);
  }

  @Test
  void aFailedErrorPathRollbackDropsTheSessionSoAHealthyRowIsNotDeadLettered()
      throws Exception {
    // Row 2 is rejected for good (23505) and the chunk's rollback fails while Aurora DSQL
    // keeps the session open (XX000), so that session still holds the aborted transaction.
    // The one-row-per-transaction fallback must start on a new session. FAILS WITHOUT G22:
    // rollbackQuietly kept the session, so healthy row 1 was dead-lettered with 25P02.
    assertAFailedErrorPathRollbackDropsTheSession(AbortingDsql.serverUnavailable());
  }

  @Test
  void anyFailedErrorPathRollbackDropsTheSessionWhateverItsSqlState() throws Exception {
    // Not only XX000: a rollback that fails with any other server error, or with no SQLSTATE,
    // may leave the session holding the aborted transaction too -- and pgjdbc's isValid()
    // still answers true in one, so connection() would reuse it. FAILS if the drop is
    // narrowed to XX000: healthy row 1 is dead-lettered with 25P02.
    assertAFailedErrorPathRollbackDropsTheSession(new SQLException("io error", "58030"));
    assertAFailedErrorPathRollbackDropsTheSession(new SQLException("rollback failed"));
  }

  private static void assertAFailedErrorPathRollbackDropsTheSession(SQLException failure)
      throws Exception {
    AbortingDsql dsql = new AbortingDsql();
    dsql.rejected = id -> id == 2L;
    dsql.rollbackFailure = failure;
    Dlq dlq = new Dlq();
    Task task = task(dsql, dlq);
    AtomicInteger reconnects = offline(task);
    assertThrows(
        IllegalStateException.class,
        () -> task.put(List.of(insert("orders", 1), insert("orders", 2), insert("orders", 3))));
    assertEquals(
        List.of("prepare", "batch!23505", "rollback!" + failure.getSQLState(), "close"),
        dsql.calls);
    assertEquals(1, reconnects.get()); // the fallback's first row opened a new session
    assertEquals(List.of(), dlq.states());
  }

  /**
   * Give the task a token provider whose token source counts its calls and throws, so a
   * reconnect (connection() asks for a token before DriverManager) is observed offline.
   */
  private static AtomicInteger offline(DsqlSinkTask task) throws Exception {
    AtomicInteger reconnects = new AtomicInteger();
    set(task, "tokenProvider", new DsqlIamTokenProvider("h", "r", "u",
        (h, r, u) -> {
          reconnects.incrementAndGet();
          throw new IllegalStateException("offline");
        },
        Instant::now));
    return reconnects;
  }
}
