// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Unit tests for DsqlSinkTask's transient-vs-permanent error classification.
 *
 * Regression guard for the observed CDC data-loss mode: a connection torn down
 * by an MSK Connect worker replacement (or DSQL idle close / token expiry) was
 * mis-classified as a PERMANENT error and routed to quarantine, advancing the
 * Kafka offset past healthy rows that were never applied (a contiguous gap, no
 * DLQ). isTransient() must treat all connectivity-failure shapes as retryable so
 * Connect replays the same offsets after a reconnect (apply is idempotent).
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;

import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DsqlSinkTaskTest {

  private final DsqlSinkTask task = new DsqlSinkTask();

  @Test
  void occSerializationFailureIsTransient() {
    assertTrue(task.isTransient(new SQLException("conflict", "40001")));
  }

  @Test
  void serverUnavailableIsTransientButTheCorruptionCodesAreNot() {
    // G22: Aurora DSQL's XX000 "server unavailable" is a temporary service condition. Once its
    // in-process budget runs out it is redelivered, never dead-lettered. FAILS WITHOUT G22:
    // XX000 was permanent, so a healthy row went to the DLQ during a short service blip.
    assertTrue(task.isTransient(new SQLException("server unavailable", "XX000")));
    // XX001 (data corrupted) and XX002 (index corrupted) share the class but are not
    // transient: redelivering them would stall the stream forever.
    assertFalse(task.isTransient(new SQLException("data corrupted", "XX001")));
    assertFalse(task.isTransient(new SQLException("index corrupted", "XX002")));
  }

  @Test
  void connectionExceptionClass08IsTransient() {
    assertTrue(task.isTransient(new SQLException("connection failure", "08006")));
    assertTrue(task.isTransient(new SQLException("conn does not exist", "08003")));
  }

  @Test
  void operatorInterventionClass57IsTransient() {
    // 57P01 admin_shutdown / 57P02 crash_shutdown / 57P03 cannot_connect_now --
    // emitted when the server terminates the connection (e.g. worker recycle).
    assertTrue(task.isTransient(new SQLException("admin shutdown", "57P01")));
    assertTrue(task.isTransient(new SQLException("terminating connection", "57P02")));
  }

  @Test
  void nullSqlStateIsTransient() {
    // pgjdbc "This connection has been closed." frequently carries no SQLSTATE.
    assertTrue(task.isTransient(new SQLException("This connection has been closed.")));
  }

  @Test
  void connectionExceptionSubclassesAreTransient() {
    assertTrue(task.isTransient(new SQLNonTransientConnectionException("closed", (String) null)));
    assertTrue(task.isTransient(new SQLRecoverableException("recoverable", (String) null)));
  }

  @Test
  void genuinePermanentErrorIsNotTransient() {
    // A real poison row (type mismatch, missing column, etc.) must stay permanent
    // so it is quarantined to the DLQ rather than retried forever.
    assertFalse(task.isTransient(new SQLException("column does not exist", "42703")));
    assertFalse(task.isTransient(new SQLException("not null violation", "23502")));
    // DSQL per-value limit rejection is permanent (handled by the oversized guard
    // pre-write, but if it reaches here it must not loop).
    assertFalse(task.isTransient(new SQLException("datatype limit exceeded", "54000")));
    // 25P02 is the symptom of an earlier failure in the same transaction (e.g. a later
    // statement group's describe failing with 42703 under drift; G25 reports that describe
    // failure in its place). Permanent, the row is dead-lettered with its real cause;
    // transient, the same deterministic failure would be redelivered forever.
    // G21 removed the OCC cause (rollback before a retry); it did not make 25P02 retryable.
    assertFalse(task.isTransient(new SQLException(
        "current transaction is aborted, commands ignored until end of transaction block",
        "25P02")));
  }

  @Test
  void sqlStateTagPrefixesTheStateForDriftClassification() {
    // The quarantine reason is prefixed with "sqlstate=<state> " so the control
    // plane can classify a source-schema-drift rejection (42703 = ADD COLUMN,
    // 23502 = DROP COLUMN of a NOT NULL target col, 42804 = TYPE CHANGE) purely
    // from the CloudWatch log line -- pgjdbc's getMessage() does not carry it.
    assertEquals("sqlstate=42703 ", DsqlSinkTask.sqlStateTag(
        new SQLException("column \"new_col\" does not exist", "42703")));
    assertEquals("sqlstate=23502 ", DsqlSinkTask.sqlStateTag(
        new SQLException("not null violation", "23502")));
    assertEquals("sqlstate=42804 ", DsqlSinkTask.sqlStateTag(
        new SQLException("datatype mismatch", "42804")));
  }

  @Test
  void safeCauseMessageDropsTheServerDetailThatCarriesRowValues() throws Exception {
    // pgjdbc's getMessage() is ServerErrorMessage.toString(), which appends the server's
    // DETAIL -- and for a not-null violation DETAIL is the FAILING ROW. The quarantine
    // reason is logged to CloudWatch, where the row is not otherwise present, so the
    // reason must carry the primary message only (Property 7). Built from the wire format
    // pgjdbc parses: field-type byte + value, NUL-separated.
    String wire = "SERROR\u0000C23502\u0000M"
        + "null value in column \"email\" of relation \"customers\" violates not-null constraint"
        + "\u0000DFailing row contains (42, alice@example.com, 555-0100)."
        + "\u0000ncustomers_email_not_null\u0000";
    ServerErrorMessage server = new ServerErrorMessage(wire);
    PSQLException cause = new PSQLException(server);

    // Precondition: the raw driver message really does leak the row (else this test proves
    // nothing) ...
    assertTrue(cause.getMessage().contains("alice@example.com"),
        "expected pgjdbc to append DETAIL to getMessage(); it did not: " + cause.getMessage());

    String safe = DsqlSinkTask.safeCauseMessage(cause);
    assertFalse(safe.contains("alice@example.com"), "row value leaked into the reason: " + safe);
    assertFalse(safe.contains("Failing row"), "DETAIL leaked into the reason: " + safe);
    // ... while the diagnostic value is kept: the primary message and the constraint name.
    assertTrue(safe.contains("violates not-null constraint"), safe);
    assertTrue(safe.contains("customers_email_not_null"), safe);
  }

  @Test
  void safeCauseMessageFallsBackToTheDriverMessageForClientSideFailures() {
    // A connection drop or token expiry has no ServerErrorMessage and carries no row data,
    // so the message is used as-is rather than being blanked.
    assertEquals("This connection has been closed.",
        DsqlSinkTask.safeCauseMessage(new SQLException("This connection has been closed.")));
    assertEquals("", DsqlSinkTask.safeCauseMessage(null));
  }

  @Test
  void sqlStateTagIsEmptyWhenDriverGaveNoState() {
    // A null/empty SQLSTATE (e.g. "This connection has been closed.") must yield no
    // tag rather than "sqlstate=null ", so the parser does not mis-read a code.
    assertEquals("", DsqlSinkTask.sqlStateTag(new SQLException("connection closed")));
    assertEquals("", DsqlSinkTask.sqlStateTag(new SQLException("empty state", "")));
  }

  @Test
  void transientRetryExceptionIsRetriable() {
    // A transient SQLException must surface as a Kafka Connect RetriableException so
    // WorkerSinkTask redelivers the batch instead of killing the task. Assert on
    // RetriableException specifically -- NOT ConnectException (its supertype), which
    // would pass trivially and not guard against a revert to plain ConnectException
    // (the fatal type that killed the task on transient connectivity blips).
    SQLException closed = new SQLException("This connection has been closed.");
    assertInstanceOf(RetriableException.class, DsqlSinkTask.transientRetryException(closed));
  }

  @Test
  void transientRetryExceptionCarriesSqlStateAndAValueFreeCause() {
    // G22: Connect logs a RetriableException with its cause chain at ERROR on every
    // redelivery, so the cause keeps the SQLSTATE, the exception class and the stack trace --
    // never the driver message.
    SQLException adminShutdown = new SQLException("admin shutdown", "57P01");
    RetriableException wrapped = DsqlSinkTask.transientRetryException(adminShutdown);
    assertEquals(
        "DSQL apply failed (transient sqlstate=57P01); Connect will retry",
        wrapped.getMessage());
    SQLException cause = assertInstanceOf(SQLException.class, wrapped.getCause());
    assertEquals("57P01", cause.getSQLState());
    assertEquals("java.sql.SQLException", cause.getMessage());
    assertNull(cause.getCause());
    assertArrayEquals(adminShutdown.getStackTrace(), cause.getStackTrace());
  }

  @Test
  void theRedeliveredCauseCarriesNoRowValue() {
    // pgjdbc renders a failed batch entry WITH its bound values into the BatchUpdateException
    // message (logServerErrorDetail defaults to true; reWriteBatchedInserts makes it the whole
    // multi-row INSERT), and Connect prints the RetriableException's cause chain on every
    // redelivery. FAILS WITHOUT G22: the raw exception was the cause, row values included.
    java.sql.BatchUpdateException batch =
        new java.sql.BatchUpdateException(
            "Batch entry 0 INSERT INTO \"shop\".\"customers\" (\"id\", \"email\") VALUES "
                + "('42', 'alice@example.com') was aborted: ERROR: server unavailable",
            "XX000",
            0,
            new int[] {-3},
            new SQLException("ERROR: server unavailable", "XX000"));
    RetriableException wrapped = DsqlSinkTask.transientRetryException(batch);
    java.io.StringWriter printed = new java.io.StringWriter();
    wrapped.printStackTrace(new java.io.PrintWriter(printed));
    assertFalse(printed.toString().contains("alice@example.com"), printed.toString());
    assertFalse(printed.toString().contains("INSERT INTO"), printed.toString());
    SQLException cause = (SQLException) wrapped.getCause();
    assertEquals("XX000", cause.getSQLState());
    assertEquals(
        "java.sql.BatchUpdateException caused by java.sql.SQLException", cause.getMessage());
  }

  @Test
  void aValueFreeCopyNamesTheRootCauseKeepsTheVendorCodeAndEndsOnACycle() {
    // pgjdbc's mid-batch I/O drop is three levels deep: BatchUpdateException -> PSQLException
    // (08006) -> the socket error. The redelivery ERROR names the ROOT class, the actionable
    // part, and the copy keeps the vendor code (pgjdbc itself always sets 0, so this pins the
    // copy's contract). FAILS if the walk stops at the direct cause or the code is dropped.
    SQLException io =
        new SQLException(
            "An I/O error occurred while sending to the backend.",
            "08006",
            new java.net.SocketException("Connection reset"));
    java.sql.BatchUpdateException batch =
        new java.sql.BatchUpdateException(
            "Batch entry 0 INSERT INTO \"shop\".\"customers\" (\"id\", \"email\") VALUES "
                + "('42', 'alice@example.com') was aborted",
            "08006",
            7,
            new int[] {-3},
            io);
    SQLException copy = DsqlSinkTask.valueFreeCopy(batch);
    assertEquals("java.sql.BatchUpdateException caused by java.net.SocketException",
        copy.getMessage());
    assertEquals("08006", copy.getSQLState());
    assertEquals(7, copy.getErrorCode());
    java.io.StringWriter printed = new java.io.StringWriter();
    DsqlSinkTask.transientRetryException(batch).printStackTrace(new java.io.PrintWriter(printed));
    assertFalse(printed.toString().contains("alice@example.com"), printed.toString());

    // A cyclic cause chain must not spin the task thread inside put(): the walk is bounded.
    // (Run on another thread: a plain @Timeout only interrupts a loop that never checks.)
    SQLException a = new SQLException("a", "XX000");
    SQLException b = new SQLException("b", "XX000");
    a.initCause(b);
    b.initCause(a);
    SQLException cyclic =
        assertTimeoutPreemptively(
            java.time.Duration.ofSeconds(5), () -> DsqlSinkTask.valueFreeCopy(a));
    assertEquals("XX000", cyclic.getSQLState());
  }

  // --- DLQ log PK enrichment (surrogate value shown, natural key withheld) -----

  @Test
  void isSurrogateForIntegerTypes() {
    // Every integral PK type Debezium can produce is a surrogate whose value is safe.
    assertTrue(DsqlSinkTask.isSurrogate((byte) 1));
    assertTrue(DsqlSinkTask.isSurrogate((short) 1));
    assertTrue(DsqlSinkTask.isSurrogate(14));
    assertTrue(DsqlSinkTask.isSurrogate(14L));
    assertTrue(DsqlSinkTask.isSurrogate(BigInteger.valueOf(14)));
    // BIGINT UNSIGNED arrives as a whole-number BigDecimal (Connect BYTES/Decimal).
    assertTrue(DsqlSinkTask.isSurrogate(new BigDecimal("42")));
  }

  @Test
  void isSurrogateForUuidStringOnly() {
    // A canonical UUID string PK is a surrogate; any other string is a natural key.
    assertTrue(DsqlSinkTask.isSurrogate("f47ac10b-58cc-4372-a567-0e02b2c3d479"));
    assertFalse(DsqlSinkTask.isSurrogate("user@example.com"));
    assertFalse(DsqlSinkTask.isSurrogate("ACME-000123"));
    // A numeric-LOOKING natural key is still a string -> withheld.
    assertFalse(DsqlSinkTask.isSurrogate("1234567890"));
  }

  @Test
  void isSurrogateForNonKeyValues() {
    // Fractional decimals and binary are never rendered as a key value.
    assertFalse(DsqlSinkTask.isSurrogate(new BigDecimal("42.5")));
    assertFalse(DsqlSinkTask.isSurrogate(new byte[] {1, 2, 3}));
    assertFalse(DsqlSinkTask.isSurrogate(Boolean.TRUE));
    assertFalse(DsqlSinkTask.isSurrogate(null));
  }

  @Test
  void formatPkRendersSurrogateValueAndWithholdsNaturalKey() {
    assertEquals("id=14", DsqlSinkTask.formatPk(List.of("id"), List.of(14L)));
    assertEquals(
        "email=<withheld>", DsqlSinkTask.formatPk(List.of("email"), List.of("a@b.com")));
    // Composite PK: decided per column.
    assertEquals(
        "email=<withheld>, region=2",
        DsqlSinkTask.formatPk(List.of("email", "region"), List.of("a@b.com", 2)));
  }

  @Test
  void formatPkEmptyOrMismatchedListsYieldEmpty() {
    assertEquals("", DsqlSinkTask.formatPk(List.of(), List.of()));
    assertEquals("", DsqlSinkTask.formatPk(List.of("id"), List.of()));
    assertEquals("", DsqlSinkTask.formatPk(null, null));
  }

  @Test
  void formatPkFromStructKeyRendersAndNullYieldsEmpty() {
    Schema keySchema =
        SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
    Struct key = new Struct(keySchema).put("id", 7L);
    assertEquals("id=7", DsqlSinkTask.formatPk(key));
    // A null or non-Struct key contributes nothing.
    assertEquals("", DsqlSinkTask.formatPk((Object) null));
    assertEquals("", DsqlSinkTask.formatPk("not-a-struct"));
  }

  @Test
  void opSuffixTagsTheDmlOperationThatDecidesTheRecovery() {
    // The quarantine reason must carry the DML op, because it is what splits the two
    // recoveries. CDC replicates STATE: a quarantined INSERT or UPDATE converges by
    // re-reading the source's CURRENT row, so the intermediate value is not needed. A
    // quarantined DELETE is the opposite -- the row is gone from the source, so no source
    // query can reveal that the target still holds it, and it must be deleted on the target.
    // The op was parsed into a local and never logged: an operator grepped 2,705 sink log
    // lines for it and found zero.
    assertEquals(
        " | op: d",
        DsqlSinkTask.opSuffix(ChangeEvent.delete("app.orders", List.of("id"), List.of(1L), 0L)));
    assertEquals(
        " | op: c/r",
        DsqlSinkTask.opSuffix(
            ChangeEvent.insert(
                "app.orders", List.of("id"), List.of(1L), List.of("id"), List.of(1L), 0L)));
    assertEquals(
        " | op: u",
        DsqlSinkTask.opSuffix(
            ChangeEvent.upsert(
                "app.orders", List.of("id"), List.of(1L), List.of("id"), List.of(1L), 0L)));
    // A tombstone APPLIES as a delete, so it reports as one -- with a marker, because
    // Debezium sends it alongside the op=d envelope for a single source DELETE.
    assertEquals(
        " | op: d (tombstone)",
        DsqlSinkTask.opSuffix(
            ChangeEvent.tombstone("app.orders", List.of("id"), List.of(1L), 0L)));
    // No event (the envelope did not parse) -> no op, never a guess.
    assertEquals("", DsqlSinkTask.opSuffix(null));
  }

  // --- The pre-write value-fit guard (ValueFit) and its bind backstop ----------

  @Test
  void aDurationBoundIntoATimeColumnIsRefusedNotWrapped() throws Exception {
    // With the value-fit check off, an interval '30:00:00' bound to a time column went
    // through PostgreSQL's assignment cast and landed as 06:00:00.
    org.postgresql.util.PGobject duration = new org.postgresql.util.PGobject();
    duration.setType("interval");
    duration.setValue("30:00:00");
    java.sql.ParameterMetaData meta =
        (java.sql.ParameterMetaData)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {java.sql.ParameterMetaData.class},
                (proxy, method, args) ->
                    "getParameterType".equals(method.getName())
                        ? ((Integer) args[0] == 1 ? java.sql.Types.TIME : java.sql.Types.OTHER)
                        : null);
    assertTrue(DsqlSinkTask.isDurationIntoTime(duration, meta, 1));
    assertFalse(DsqlSinkTask.isDurationIntoTime(duration, meta, 2)); // an interval column
    assertFalse(DsqlSinkTask.isDurationIntoTime("30:00:00", meta, 1));
  }

  @Test
  void bindValueCoercesOnlyAnExactZeroOrOne() {
    // The old bind turned ANY number bound to a boolean parameter into `longValue() != 0`, so
    // a MySQL TINYINT(1) holding 2 was written as true and 0.5 as false -- silently. Now only
    // an exact 0/1 is coerced; anything else is bound as itself, so Aurora DSQL rejects it
    // (42804) instead of storing a value the source never had.
    Short two = (short) 2;
    assertSame(two, DsqlSinkTask.bindValue(two, true, false));
    BigDecimal half = new BigDecimal("0.5");
    assertSame(half, DsqlSinkTask.bindValue(half, true, false));
    assertEquals(Boolean.TRUE, DsqlSinkTask.bindValue((short) 1, true, false));
    assertEquals(Boolean.FALSE, DsqlSinkTask.bindValue((short) 0, true, false));
    assertEquals(Boolean.TRUE, DsqlSinkTask.bindValue(new BigDecimal("1.0"), true, false));
    // BIT(1) -> integer coercion is unchanged.
    assertEquals(1, DsqlSinkTask.bindValue(Boolean.TRUE, false, true));
    assertEquals(0, DsqlSinkTask.bindValue(Boolean.FALSE, false, true));
    // Every other value is the ORIGINAL object (never a rescaled copy).
    BigDecimal amount = new BigDecimal("1.23");
    assertSame(amount, DsqlSinkTask.bindValue(amount, false, false));
  }

  /** A task whose catalog read is scripted (a Map, null, or an SQLException to throw). */
  private static final class ScriptedCatalogTask extends DsqlSinkTask {
    private final Deque<Object> answers = new ArrayDeque<>();
    int loads;

    ScriptedCatalogTask answer(Object... next) {
      for (Object a : next) {
        answers.add(a == null ? NULL : a);
      }
      return this;
    }

    private static final Object NULL = new Object();

    @Override
    @SuppressWarnings("unchecked")
    Map<String, ValueFit.TargetType> loadTargetTypes(String schema, String table)
        throws SQLException {
      loads++;
      Object next = answers.size() > 1 ? answers.poll() : answers.peek();
      if (next instanceof SQLException e) {
        throw e;
      }
      return next == NULL || next == null ? null : (Map<String, ValueFit.TargetType>) next;
    }
  }

  private static Map<String, String> sinkProps() {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.RETRY_BACKOFF_MS, "0"); // OCC retries sleep 0 ms
    return props;
  }

  private static final Schema ORDER_KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
  private static final Schema ORDER_ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("flag", Schema.OPTIONAL_INT16_SCHEMA)
          .optional()
          .build();
  private static final Schema ORDER_SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("db", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .optional()
          .build();
  private static final Schema ORDER_ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("after", ORDER_ROW)
          .field("source", ORDER_SOURCE)
          .build();

  @Test
  void putRefusesAValueTheTargetWouldChangeBeforeAnyWrite() {
    ScriptedCatalogTask task =
        new ScriptedCatalogTask()
            .answer(Map.of("flag", ValueFit.TargetType.fromCatalog("boolean", null, null)));
    task.startMetrics(sinkProps()); // no start(): no token provider, no DLQ reporter
    Struct envelope =
        new Struct(ORDER_ENVELOPE)
            .put("op", "c")
            .put("after", new Struct(ORDER_ROW).put("id", 1L).put("flag", (short) 97))
            .put("source", new Struct(ORDER_SOURCE).put("db", "shop").put("table", "orders"));
    SinkRecord record =
        new SinkRecord(
            "dsqlcdc.shop.orders", 0, ORDER_KEY, new Struct(ORDER_KEY).put("id", 1L),
            ORDER_ENVELOPE, envelope, 0L);

    // No DLQ reporter is wired, so the refusal fails loudly rather than skipping. It must be
    // the QUARANTINE that throws: had the row reached applyBatch, connection() would have hit
    // the missing token provider (a NullPointerException, not a ConnectException).
    ConnectException e = assertThrows(ConnectException.class, () -> task.put(List.of(record)));
    String message = e.getMessage();
    assertTrue(
        message.contains("Value does not fit target column 'flag' of type boolean"), message);
    assertTrue(message.contains("pk: id=1"), message);
    assertTrue(message.contains("op: c/r"), message);
    assertFalse(message.contains("97"), "the value leaked into the reason: " + message);
    assertEquals(1, task.loads);
  }

  @Test
  void valueFitRefusalReReadsAStaleEntryBeforeRefusing() {
    // The target column was recreated WIDER out of band (numeric(10,2) -> numeric(12,4)). A
    // refusal from an entry older than 30 s re-reads the table first, so a value that now
    // fits is not dead-lettered on a stale type.
    ScriptedCatalogTask task =
        new ScriptedCatalogTask()
            .answer(
                Map.of("amount", ValueFit.TargetType.fromCatalog("numeric", 10, 2)),
                Map.of("amount", ValueFit.TargetType.fromCatalog("numeric", 12, 4)));
    task.startMetrics(sinkProps());
    ChangeEvent event =
        ChangeEvent.upsert(
            "shop.orders", List.of("id", "amount"), List.of(1L, new BigDecimal("1.2345")),
            List.of("id"), List.of(1L), 0L);
    long t0 = 5_000_000_000L;
    String reason = task.valueFitRefusal(null, event, t0);
    assertNotNull(reason);
    assertTrue(reason.contains("numeric(10,2)"), reason);
    // Within 30 s of the read: refused again, no re-read.
    assertNotNull(task.valueFitRefusal(null, event, t0 + TimeUnit.SECONDS.toNanos(5)));
    assertEquals(1, task.loads);
    // Past 30 s: re-read before refusing; the wider column holds the value.
    assertNull(task.valueFitRefusal(null, event, t0 + TimeUnit.SECONDS.toNanos(31)));
    assertEquals(2, task.loads);
    // A delete is never refused and never reads the catalog.
    ChangeEvent delete =
        ChangeEvent.delete("shop.other", List.of("id"), List.of(new BigDecimal("5.5")), 0L);
    assertNull(task.valueFitRefusal(null, delete, t0));
    assertEquals(2, task.loads);
  }

  @Test
  void catalogReadFailureSemantics() {
    // Transient (no SQLSTATE, as a dropped connection reports it; or class 08): redelivered,
    // exactly like the apply path -- never a dead letter, never a failed task.
    ScriptedCatalogTask closed =
        new ScriptedCatalogTask().answer(new SQLException("This connection has been closed."));
    closed.startMetrics(sinkProps());
    assertThrows(RetriableException.class, () -> closed.readTargetTypes("shop.orders"));
    ScriptedCatalogTask dropped =
        new ScriptedCatalogTask().answer(new SQLException("connection failure", "08006"));
    dropped.startMetrics(sinkProps());
    assertThrows(RetriableException.class, () -> dropped.readTargetTypes("shop.orders"));

    // Permanent (e.g. no privilege): fail OPEN -- the table goes unchecked, as before the
    // guard existed, and the task keeps running.
    ScriptedCatalogTask denied =
        new ScriptedCatalogTask().answer(new SQLException("permission denied", "42501"));
    denied.startMetrics(sinkProps());
    assertNull(denied.readTargetTypes("shop.orders"));
    assertNull(denied.readTargetTypes("shop.orders"));

    // OCC 40001 is retried, then the read succeeds.
    Map<String, ValueFit.TargetType> types =
        Map.of("flag", ValueFit.TargetType.fromCatalog("boolean", null, null));
    ScriptedCatalogTask conflicted =
        new ScriptedCatalogTask().answer(new SQLException("conflict", "40001"), types);
    conflicted.startMetrics(sinkProps());
    assertEquals(types, conflicted.readTargetTypes("shop.orders"));
    assertEquals(2, conflicted.loads);

    // A target name that is not schema.table is not guarded and not read.
    ScriptedCatalogTask unqualified = new ScriptedCatalogTask().answer(types);
    unqualified.startMetrics(sinkProps());
    assertNull(unqualified.readTargetTypes("orders"));
    assertEquals(0, unqualified.loads);
  }

  @Test
  void aServerUnavailableCatalogReadIsRetriedThenRedeliveredNeverFailedOpen() {
    // G22: an XX000 on the column-type read is retried like a 40001, and one that never
    // clears is redelivered. FAILS WITHOUT G22: it FAILED OPEN -- the table's values were
    // written unchecked for 60 s (extra decimal places or a fraction for an integer column
    // rounded silently) because of a temporary service condition.
    Map<String, ValueFit.TargetType> types =
        Map.of("flag", ValueFit.TargetType.fromCatalog("boolean", null, null));
    ScriptedCatalogTask once =
        new ScriptedCatalogTask().answer(new SQLException("server unavailable", "XX000"), types);
    once.startMetrics(sinkProps());
    assertEquals(types, once.readTargetTypes("shop.orders"));
    assertEquals(2, once.loads);

    ScriptedCatalogTask sustained =
        new ScriptedCatalogTask().answer(new SQLException("server unavailable", "XX000"));
    sustained.startMetrics(sinkProps());
    RetriableException e =
        assertThrows(RetriableException.class, () -> sustained.readTargetTypes("shop.orders"));
    assertEquals("XX000", ((SQLException) e.getCause()).getSQLState());
    assertEquals(10, sustained.loads); // the occ.max.retries budget, then redelivered
  }

  @Test
  void isVariableScaleSourceReadsTheAfterSchema() {
    Schema variableScale =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL)
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .optional()
            .build();
    Schema row =
        SchemaBuilder.struct()
            .name("Row")
            .field("v", variableScale)
            .field("d", Decimal.builder(2).optional().build())
            .optional()
            .build();
    Schema env = SchemaBuilder.struct().name("Envelope").field("after", row).build();
    SinkRecord record =
        new SinkRecord("dsqlcdc.app.t", 0, null, null, env, new Struct(env), 0L);
    assertTrue(DsqlSinkTask.isVariableScaleSource(record, "v"));
    assertFalse(DsqlSinkTask.isVariableScaleSource(record, "d"));
    assertFalse(DsqlSinkTask.isVariableScaleSource(record, "missing"));
    assertFalse(
        DsqlSinkTask.isVariableScaleSource(
            new SinkRecord("dsqlcdc.app.t", 0, null, null, null, null, 0L), "v"));
    assertFalse(DsqlSinkTask.isVariableScaleSource(null, "v"));
  }

  @Test
  void theDefaultNumericExemptionIsOnlyForAnUnconstrainedSourceColumn() {
    // Through valueFitRefusal with a REAL record, so the record -> isVariableScaleSource wiring
    // is what decides. An unconstrained PostgreSQL numeric (VariableScaleDecimal) into Aurora
    // DSQL's default numeric(18,6) rounds by design; a CONSTRAINED source decimal (a MySQL
    // DECIMAL(18,6) -> numeric(18,6) later widened to DECIMAL(20,8)) must still be refused.
    ScriptedCatalogTask task =
        new ScriptedCatalogTask()
            .answer(
                Map.of(
                    "v", ValueFit.TargetType.fromCatalog("numeric", null, null),
                    "d", ValueFit.TargetType.fromCatalog("numeric", 18, 6)));
    task.startMetrics(sinkProps());
    Schema variableScale =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL)
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .optional()
            .build();
    Schema row =
        SchemaBuilder.struct()
            .name("Row")
            .field("id", Schema.INT64_SCHEMA)
            .field("v", variableScale)
            .field("d", Decimal.builder(8).optional().build())
            .optional()
            .build();
    Schema env = SchemaBuilder.struct().name("Envelope").field("after", row).build();
    SinkRecord record = new SinkRecord("dsqlcdc.app.t", 0, null, null, env, new Struct(env), 0L);

    ChangeEvent unconstrained =
        ChangeEvent.upsert(
            "app.t", List.of("id", "v"), List.of(1L, new BigDecimal("1.1234567")),
            List.of("id"), List.of(1L), 0L);
    assertNull(task.valueFitRefusal(record, unconstrained, 1L));
    ChangeEvent constrained =
        ChangeEvent.upsert(
            "app.t", List.of("id", "d"), List.of(1L, new BigDecimal("1.12345678")),
            List.of("id"), List.of(1L), 0L);
    String reason = task.valueFitRefusal(record, constrained, 1L);
    assertNotNull(reason);
    assertTrue(reason.contains("'d'"), reason);
    assertTrue(reason.contains("6 decimal places"), reason);
    assertFalse(reason.contains("12345678"), reason);
  }

  // --- The real catalog read (loadTargetTypes) over a JDBC double -------------

  /**
   * A JDBC double (java.lang.reflect.Proxy): the catalog statement answers {@link #catalogRows}
   * (or throws {@link #catalogFailure}); any other statement is a write that succeeds (or
   * throws {@link #writeFailure}), and its parameter describe answers no metadata (or throws
   * {@link #describeFailure}). Records the Connection calls that matter.
   */
  private static final class FakeJdbc {
    final List<String> calls = new ArrayList<>(); // prepare/commit/rollback/close, in order
    final Map<Integer, Object> params = new HashMap<>(); // the catalog query's parameters
    List<Object[]> catalogRows = List.of();
    SQLException catalogFailure;
    SQLException writeFailure;
    SQLException describeFailure; // thrown by a write statement's getParameterMetaData
    int catalogReads;

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler);
    }

    private static Object objectMethod(Object self, String name, Object[] args) {
      return switch (name) {
        case "toString" -> "FakeJdbc";
        case "hashCode" -> System.identityHashCode(self);
        case "equals" -> self == args[0];
        default -> throw new UnsupportedOperationException(name);
      };
    }

    private ResultSet resultSet() {
      Iterator<Object[]> rows = catalogRows.iterator();
      Object[][] current = new Object[1][];
      boolean[] lastNull = new boolean[1];
      return proxy(
          ResultSet.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "next" -> {
                  current[0] = rows.hasNext() ? rows.next() : null;
                  yield current[0] != null;
                }
                case "getString" -> (String) current[0][(int) a[0] - 1];
                case "getInt" -> {
                  Object v = current[0][(int) a[0] - 1];
                  lastNull[0] = v == null;
                  yield v == null ? 0 : (Integer) v;
                }
                case "wasNull" -> lastNull[0];
                case "close" -> null;
                default -> objectMethod(self, m.getName(), a);
              });
    }

    private PreparedStatement catalogStatement() {
      return proxy(
          PreparedStatement.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "setString" -> {
                  params.put((Integer) a[0], a[1]);
                  yield null;
                }
                case "executeQuery" -> {
                  catalogReads++;
                  if (catalogFailure != null) {
                    throw catalogFailure;
                  }
                  yield resultSet();
                }
                case "close" -> null;
                default -> objectMethod(self, m.getName(), a);
              });
    }

    private PreparedStatement writeStatement() {
      return proxy(
          PreparedStatement.class,
          (self, m, a) ->
              switch (m.getName()) {
                case "getParameterMetaData" -> {
                  if (describeFailure != null) {
                    throw describeFailure;
                  }
                  yield null;
                }
                case "setObject", "addBatch", "close" -> null;
                case "executeUpdate" -> {
                  if (writeFailure != null) {
                    throw writeFailure;
                  }
                  yield 1;
                }
                case "executeBatch" -> {
                  if (writeFailure != null) {
                    throw writeFailure;
                  }
                  yield new int[0];
                }
                default -> objectMethod(self, m.getName(), a);
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
                  boolean catalog = ValueFit.CATALOG_SQL.equals(a[0]);
                  calls.add(catalog ? "prepareCatalog" : "prepareWrite");
                  yield catalog ? catalogStatement() : writeStatement();
                }
                case "commit", "rollback", "close" -> {
                  calls.add(m.getName());
                  yield null;
                }
                default -> objectMethod(self, m.getName(), a);
              });
    }
  }

  /** Hand the task an open connection (connection() reuses one that is open and valid). */
  private static void inject(DsqlSinkTask task, Connection connection) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField("connection");
    field.setAccessible(true);
    field.set(task, connection);
  }

  private static Connection injected(DsqlSinkTask task) throws Exception {
    java.lang.reflect.Field field = DsqlSinkTask.class.getDeclaredField("connection");
    field.setAccessible(true);
    return (Connection) field.get(task);
  }

  @Test
  void loadTargetTypesReadsTheNamedTableInExactCaseAndCommits() throws Exception {
    FakeJdbc jdbc = new FakeJdbc();
    jdbc.catalogRows =
        List.of(
            new Object[] {"Flag", "boolean", null, null},
            new Object[] {"amount", "numeric", 10, 2},
            new Object[] {"note", "text", null, null});
    DsqlSinkTask task = new DsqlSinkTask();
    inject(task, jdbc.connection());

    Map<String, ValueFit.TargetType> types = task.loadTargetTypes("shop", "Orders");
    // Bound in order: schema, then table (swapped, every table reads 0 rows and goes unchecked).
    assertEquals("shop", jdbc.params.get(1));
    assertEquals("Orders", jdbc.params.get(2));
    // Keyed by the exact column_name: DsqlDialect quotes identifiers, so ValueFit looks a
    // column up by its exact case. An unguarded type (text) is left out.
    assertEquals(
        Map.of(
            "Flag", ValueFit.TargetType.fromCatalog("boolean", null, null),
            "amount", ValueFit.TargetType.fromCatalog("numeric", 10, 2)),
        types);
    // The read transaction is ended before any chunk transaction starts.
    assertEquals(List.of("prepareCatalog", "commit"), jdbc.calls);

    // A table the role cannot see reads 0 rows: unknown (null), never "no guarded columns".
    FakeJdbc empty = new FakeJdbc();
    DsqlSinkTask unseen = new DsqlSinkTask();
    inject(unseen, empty.connection());
    assertNull(unseen.loadTargetTypes("shop", "orders"));
    assertEquals(List.of("prepareCatalog", "commit"), empty.calls);

    // A failed read is rolled back and rethrown as is (readTargetTypes classifies it).
    FakeJdbc denied = new FakeJdbc();
    denied.catalogFailure = new SQLException("permission denied", "42501");
    DsqlSinkTask refused = new DsqlSinkTask();
    inject(refused, denied.connection());
    SQLException thrown =
        assertThrows(SQLException.class, () -> refused.loadTargetTypes("shop", "orders"));
    assertSame(denied.catalogFailure, thrown);
    assertTrue(denied.calls.contains("rollback"), denied.calls.toString());
    assertFalse(denied.calls.contains("commit"), denied.calls.toString());
  }

  @Test
  void aTransientCatalogFailureDiscardsTheConnectionAndIsRedelivered() throws Exception {
    // Same contract as the apply path: a connection that just failed must not be reused
    // (isValid can pass on a half-open socket), and Connect must redeliver the batch.
    FakeJdbc jdbc = new FakeJdbc();
    jdbc.catalogFailure = new SQLException("This connection has been closed.");
    DsqlSinkTask task = new DsqlSinkTask();
    task.startMetrics(sinkProps());
    Connection connection = jdbc.connection();
    inject(task, connection);
    assertThrows(RetriableException.class, () -> task.readTargetTypes("shop.orders"));
    assertTrue(jdbc.calls.contains("close"), jdbc.calls.toString());
    assertNull(injected(task));
  }

  @Test
  void aMissingTargetColumnMakesTheNextPutReReadTheColumnTypes() throws Exception {
    // 42703 (and 42P01/3F000) means the target was recreated or altered, so the cached types
    // may be stale: the quarantine drops them and the next put() reads them again, instead of
    // trusting them for up to 10 minutes.
    FakeJdbc jdbc = new FakeJdbc();
    jdbc.catalogRows =
        List.of(new Object[] {"id", "bigint", 64, 0}, new Object[] {"flag", "boolean", null, null});
    jdbc.writeFailure = new SQLException("column \"flag\" does not exist", "42703");
    DsqlSinkTask task = new DsqlSinkTask();
    task.startMetrics(sinkProps()); // no start(): no DLQ reporter, so a quarantine throws
    inject(task, jdbc.connection());
    Struct envelope =
        new Struct(ORDER_ENVELOPE)
            .put("op", "u")
            .put("after", new Struct(ORDER_ROW).put("id", 1L).put("flag", (short) 1))
            .put("source", new Struct(ORDER_SOURCE).put("db", "shop").put("table", "orders"));
    SinkRecord record =
        new SinkRecord(
            "dsqlcdc.shop.orders", 0, ORDER_KEY, new Struct(ORDER_KEY).put("id", 1L),
            ORDER_ENVELOPE, envelope, 0L);

    ConnectException first = assertThrows(ConnectException.class, () -> task.put(List.of(record)));
    assertTrue(first.getMessage().contains("sqlstate=42703"), first.getMessage());
    assertEquals(1, jdbc.catalogReads);
    assertTrue(jdbc.calls.contains("prepareWrite"), jdbc.calls.toString()); // it was written
    assertThrows(ConnectException.class, () -> task.put(List.of(record)));
    assertEquals(2, jdbc.catalogReads);
  }

  @Test
  void aDescribeFailureThatIsNotAnOccConflictFallsBackToPlainBinding() throws Exception {
    // G21/G22 rethrow only a 40001 or an XX000 from the parameter describe (OccRetry re-runs
    // it); any other describe failure -- a driver or server that cannot describe parameters
    // before execute -- must still fall back to plain setObject binding and apply the row.
    // FAILS if paramMetaOrNull rethrows it: the healthy row is then treated as poison (no DLQ
    // reporter here, so the put throws; with one it would be dead-lettered).
    assertADescribeFailureFallsBackToPlainBinding(
        new SQLException("parameter metadata not available", "0A000"));
  }

  @Test
  void aDescribeFailureWithNoSqlStateFallsBackToPlainBinding() throws Exception {
    // A describe failure with no SQLSTATE (a driver limitation) falls back to plain binding
    // like any other that is not a 40001 or an XX000. FAILS if paramMetaOrNull rethrows it:
    // isTransient(null) is true, so put() throws a RetriableException and Connect redelivers
    // the same healthy batch forever, never applying it.
    assertADescribeFailureFallsBackToPlainBinding(
        new SQLException("parameter metadata not available"));
  }

  private static void assertADescribeFailureFallsBackToPlainBinding(SQLException failure)
      throws Exception {
    FakeJdbc jdbc = new FakeJdbc();
    jdbc.describeFailure = failure;
    DsqlSinkTask task = new DsqlSinkTask();
    task.startMetrics(sinkProps()); // no start(): no DLQ reporter, so a quarantine throws
    inject(task, jdbc.connection());
    Struct envelope =
        new Struct(ORDER_ENVELOPE)
            .put("op", "u")
            .put("after", new Struct(ORDER_ROW).put("id", 1L).put("flag", (short) 1))
            .put("source", new Struct(ORDER_SOURCE).put("db", "shop").put("table", "orders"));
    SinkRecord record =
        new SinkRecord(
            "dsqlcdc.shop.orders", 0, ORDER_KEY, new Struct(ORDER_KEY).put("id", 1L),
            ORDER_ENVELOPE, envelope, 0L);

    task.put(List.of(record));
    // The catalog read (no rows: unchecked), then the row written and committed.
    assertEquals(List.of("prepareCatalog", "commit", "prepareWrite", "commit"), jdbc.calls);
  }
}
