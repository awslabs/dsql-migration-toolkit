// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * G25: a describe failure inside a move's transaction is reported with its own cause, not as
 * the 25P02 it leads to.
 *
 * pgjdbc sends getParameterMetaData() with QUERY_SUPPRESS_BEGIN, so a failed describe aborts
 * nothing at a transaction's start, but inside an open transaction -- a move's upsert after its
 * delete -- it aborts the transaction; the sink falls back to plain binding (not a 40001 / XX000)
 * and the execute then fails with 25P02, which names no cause. Before G25 such a move under ADD
 * COLUMN drift was dead-lettered as 25P02: no ADD_COLUMN drift kind, no cool-down. The double
 * below models exactly that; a column the target lacks (ADD COLUMN drift at the source) is
 * rejected with 42703 by any statement that names it.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class MoveDescribeCauseTest {

  private static final Schema KEY =
      SchemaBuilder.struct().name("app.orders.Key").field("id", Schema.INT32_SCHEMA).build();
  // The source added column "w"; the Aurora DSQL target lacks it.
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("app.orders.Value")
          .field("lead", Schema.INT32_SCHEMA)
          .field("id", Schema.INT32_SCHEMA)
          .field("w", Schema.OPTIONAL_STRING_SCHEMA)
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

  /** An UPDATE of the added key column lead (1 -> 2) of source row id: a MOVE. */
  private static SinkRecord move(int id) {
    Struct env =
        new Struct(ENVELOPE)
            .put("op", "u")
            .put("source",
                new Struct(SOURCE).put("connector", "postgresql").put("schema", "app")
                    .put("table", "orders"))
            .put("before", new Struct(ROW).put("lead", 1).put("id", id))
            .put("after", new Struct(ROW).put("lead", 2).put("id", id).put("w", "new"));
    return new SinkRecord("dsqlcdc.app.orders", 0, KEY, new Struct(KEY).put("id", id),
        ENVELOPE, env, offset++);
  }

  /** PostgreSQL aborted-transaction semantics as pgjdbc sees them; column "w" does not exist. */
  static final class Pg {
    enum Tx { IDLE, OPEN, ABORTED }

    Tx tx = Tx.IDLE;
    final List<String> calls = new ArrayList<>();

    @SuppressWarnings("unchecked")
    static <T> T proxy(Class<T> type, InvocationHandler h) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, h);
    }

    private SQLException fail(String call, SQLException e) {
      calls.add(call + "!" + e.getSQLState());
      tx = Tx.ABORTED;
      return e;
    }

    private static SQLException aborted() {
      return new SQLException(
          "current transaction is aborted, commands ignored until end of transaction block",
          "25P02");
    }

    private static SQLException undefinedColumn() {
      return new SQLException("column \"w\" of relation \"orders\" does not exist", "42703");
    }

    private ParameterMetaData describe(String sql) throws SQLException {
      if (tx == Tx.ABORTED) {
        throw fail("describe", aborted());
      }
      if (sql.contains("\"w\"")) {
        if (tx == Tx.OPEN) {
          throw fail("describe", undefinedColumn()); // inside the transaction: aborts it
        }
        calls.add("describe!42703"); // sent with no BEGIN: nothing is aborted
        throw undefinedColumn();
      }
      return null; // no metadata: plain binding (the values here need no conversion)
    }

    private Object execute(String sql, Object result) throws SQLException {
      String op = sql.startsWith("DELETE") ? "delete" : "upsert";
      if (tx == Tx.ABORTED) {
        throw fail(op, aborted());
      }
      tx = Tx.OPEN; // pgjdbc sends BEGIN before the first statement (autocommit off)
      if (sql.contains("\"w\"")) {
        throw fail(op, undefinedColumn());
      }
      calls.add(op);
      return result;
    }

    private PreparedStatement statement(String sql) {
      List<Object> batch = new ArrayList<>();
      return proxy(
          PreparedStatement.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "getParameterMetaData" -> describe(sql);
                case "setObject", "close" -> null;
                case "addBatch" -> {
                  batch.add(Boolean.TRUE);
                  yield null;
                }
                case "executeUpdate" -> execute(sql, 1);
                case "executeBatch" -> execute(sql, new int[batch.size()]);
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                case "toString" -> "PgStatement";
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }

    Connection connection() {
      return proxy(
          Connection.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "isClosed" -> false;
                case "isValid" -> true; // pgjdbc: true in an aborted transaction
                case "prepareStatement" -> statement((String) a[0]);
                case "commit" -> {
                  calls.add(tx == Tx.ABORTED ? "commit(rolled back)" : "commit");
                  tx = Tx.IDLE;
                  yield null;
                }
                case "rollback" -> {
                  calls.add("rollback");
                  tx = Tx.IDLE;
                  yield null;
                }
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                case "toString" -> "Pg";
                default -> throw new UnsupportedOperationException(m.getName());
              });
    }
  }

  static final class Dlq implements ErrantRecordReporter {
    final List<String> states = new ArrayList<>();

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      states.add(error instanceof SQLException e ? e.getSQLState() : error.toString());
      return CompletableFuture.completedFuture(null);
    }
  }

  /** No catalog round trips: no guarded column types; the source-key index is valid. */
  static final class Task extends DsqlSinkTask {
    @Override
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table) {
      return Map.of();
    }

    @Override
    boolean loadSourceKeyIndex(String schema, String table, List<String> columns) {
      return true;
    }
  }

  private static Task task(Pg pg, Dlq dlq) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.TARGET_KEY_COLUMNS, "app\\.orders:lead,id");
    Task task = new Task();
    task.startMetrics(props);
    for (String[] f : new String[][] {{"connection"}, {"dlqReporter"}}) {
      java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField(f[0]);
      field.setAccessible(true);
      field.set(task, f[0].equals("connection") ? pg.connection() : dlq);
    }
    return task;
  }

  @Test
  void aMoveWhoseUpsertDescribeFailsReportsTheRealCauseNot25P02() throws Exception {
    // FAILS WITHOUT THE FIX: the move's delete ran, the upsert's describe (42703) aborted the
    // transaction and was swallowed, and the row was dead-lettered as 25P02 -- no ADD COLUMN
    // drift kind, no cool-down.
    Pg pg = new Pg();
    Dlq dlq = new Dlq();
    Task task = task(pg, dlq);
    task.put(List.of(move(7)));
    assertEquals(List.of("42703"), dlq.states);
    assertTrue(task.driftCooledTables().contains("app.orders"), task.driftCooledTables().toString());
    assertEquals(
        List.of("delete", "describe!42703", "upsert!25P02", "rollback"), pg.calls);
  }

  @Test
  void aChunkOfMovesFallsBackRowByRowAndEachReportsTheRealCause() throws Exception {
    // FAILS WITHOUT THE FIX: both moves were dead-lettered as 25P02.
    Pg pg = new Pg();
    Dlq dlq = new Dlq();
    Task task = task(pg, dlq);
    task.put(List.of(move(7), move(8)));
    assertEquals(List.of("42703", "42703"), dlq.states);
    assertTrue(task.driftCooledTables().contains("app.orders"), task.driftCooledTables().toString());
  }
}
