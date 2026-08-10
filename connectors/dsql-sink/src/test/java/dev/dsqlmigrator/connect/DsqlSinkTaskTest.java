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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

class DsqlSinkTaskTest {

  private final DsqlSinkTask task = new DsqlSinkTask();

  @Test
  void occSerializationFailureIsTransient() {
    assertTrue(task.isTransient(new SQLException("conflict", "40001")));
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
  void transientRetryExceptionCarriesSqlStateAndCause() {
    SQLException adminShutdown = new SQLException("admin shutdown", "57P01");
    RetriableException wrapped = DsqlSinkTask.transientRetryException(adminShutdown);
    assertTrue(wrapped.getMessage().contains("57P01"));
    assertSame(adminShutdown, wrapped.getCause());
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
}
