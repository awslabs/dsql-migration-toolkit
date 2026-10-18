// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Aurora DSQL refuses a protocol message over 10 MiB (SQLSTATE 08P01 "invalid message length",
 * a connection error, so the sink redelivers it), and pgjdbc rewrites a run of same-SQL upserts
 * into one multi-row statement (reWriteBatchedInserts). A statement never spans two chunks, and
 * a chunk is bounded by its estimated bytes (DsqlSinkTask.MAX_BATCH_BYTES): the estimate must
 * count every value a statement carries. Before G126 a JSON value (a PGobject) counted as a
 * 16-byte scalar, one default fetch of 100 KiB documents became one chunk that pgjdbc sent as a
 * 12.5 MiB statement (128 rows), and Connect redelivered the same batch forever while nothing on
 * the task committed again. The double below sees one batch execution as one message, which
 * bounds every statement pgjdbc can rewrite it into.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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

class DsqlSinkMessageLimitTest {

  /** Aurora DSQL's documented maximum message size (08P01 above it). */
  private static final long DSQL_MESSAGE_LIMIT = 10L * 1024 * 1024;

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema JSON =
      SchemaBuilder.string().name(DebeziumTypeConverter.JSON_TYPE).optional().build();
  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("payload", JSON)
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

  private static SinkRecord jsonInsert(long id, String document) {
    Struct envelope =
        new Struct(ENVELOPE)
            .put("op", "c")
            .put("after", new Struct(ROW).put("id", id).put("payload", document))
            .put("source", new Struct(SOURCE).put("db", "app").put("table", "events"));
    return new SinkRecord(
        "dsqlcdc.app.events", 0, KEY, new Struct(KEY).put("id", id), ENVELOPE, envelope, id);
  }

  /**
   * A JDBC double of Aurora DSQL's message limit: one statement execution is one message,
   * sized as the sum of its bound values (a PGobject's text and a String by their UTF-8 bytes,
   * a byte[] by its length, 8 bytes for any other value) -- more than 10 MiB fails with 08P01,
   * as Aurora DSQL answers; otherwise its rows are kept until the commit.
   */
  private static final class LimitedDsql {
    long largestMessage;
    int commits;
    int committedRows;
    private int pendingRows;

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static long size(Object v) {
      if (v instanceof org.postgresql.util.PGobject pg) {
        return pg.getValue() == null ? 0 : pg.getValue().getBytes(StandardCharsets.UTF_8).length;
      }
      if (v instanceof String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
      }
      return v instanceof byte[] b ? b.length : 8;
    }

    private Object execute(List<List<Object>> rows, Object result) throws SQLException {
      long message = 0;
      for (List<Object> row : rows) {
        for (Object v : row) {
          message += size(v);
        }
      }
      largestMessage = Math.max(largestMessage, message);
      if (message > DSQL_MESSAGE_LIMIT) {
        throw new SQLException("FATAL: invalid message length", "08P01");
      }
      pendingRows += rows.size();
      return result;
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
                case "executeUpdate" -> execute(batch.isEmpty() ? List.of(current) : batch, 1);
                case "executeBatch" -> execute(batch, new int[batch.size()]);
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
                  commits++;
                  committedRows += pendingRows;
                  pendingRows = 0;
                  yield null;
                }
                case "rollback" -> {
                  pendingRows = 0;
                  yield null;
                }
                case "close" -> null;
                case "hashCode" -> System.identityHashCode(self);
                case "equals" -> self == a[0];
                default -> throw new UnsupportedOperationException(m.getName());
              });
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
  void aFetchOfLargeJsonDocumentsCommitsUnderTheMessageLimit() throws Exception {
    // What one default sink fetch (4 MiB of lz4-compressed records) delivered in the repro: 158
    // inserts of ~100 KiB JSON documents, 15.4 MiB of JSON. FAILS WITHOUT G126: estimated at 32
    // bytes a row they formed one chunk, too large a message for Aurora DSQL (08P01), so put()
    // threw a RetriableException on every redelivery and nothing ever committed.
    String document = "{\"k\": \"" + "x".repeat(100 * 1024 - 9) + "\"}";
    List<SinkRecord> records = new ArrayList<>();
    for (long id = 1; id <= 158; id++) {
      records.add(jsonInsert(id, document));
    }
    LimitedDsql dsql = new LimitedDsql();
    List<SinkRecord> deadLettered = new ArrayList<>();
    ErrantRecordReporter dlq =
        (record, error) -> {
          deadLettered.add(record);
          return CompletableFuture.completedFuture(null);
        };
    UncheckedTask task = new UncheckedTask();
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0");
    props.put(DsqlSinkConnectorConfig.BATCH_SIZE, "3000"); // the cdc-stack's SinkBatchMaxRows
    task.startMetrics(props);
    setField(task, "connection", dsql.connection());
    setField(task, "dlqReporter", dlq);

    task.put(records);

    assertEquals(158, dsql.committedRows);
    assertTrue(dsql.commits >= 2, "commits: " + dsql.commits); // the byte budget split it
    assertTrue(
        dsql.largestMessage <= DsqlSinkTask.MAX_BATCH_BYTES, "message: " + dsql.largestMessage);
    assertEquals(List.of(), deadLettered);
  }
}
