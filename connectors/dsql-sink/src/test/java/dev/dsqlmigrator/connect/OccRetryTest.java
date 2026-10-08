// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.Test;

class OccRetryTest {

  private static final OccRetry.Sleeper NO_SLEEP = millis -> {};
  private static final LongUnaryOperator NO_JITTER = bound -> 0L;

  private static SQLException occ() {
    return new SQLException("serialization failure", OccRetry.OCC_SQLSTATE);
  }

  @Test
  void retriesOnConflictThenSucceeds() throws SQLException {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger rollbacks = new AtomicInteger();
    String result =
        OccRetry.withRetry(
            () -> {
              if (calls.incrementAndGet() < 3) {
                throw occ();
              }
              return "ok";
            },
            rollbacks::incrementAndGet,
            10,
            50L,
            NO_SLEEP,
            NO_JITTER);
    assertEquals("ok", result);
    assertEquals(3, calls.get());
    assertEquals(2, rollbacks.get()); // one before each re-run
  }

  @Test
  void eachRetryRollsBackBeforeTheBackoffAndTheNextAttempt() throws SQLException {
    // G21: a 40001 raised while a statement runs leaves the transaction aborted, so the
    // next attempt must start on a clean one.
    List<String> events = new ArrayList<>();
    AtomicInteger calls = new AtomicInteger();
    OccRetry.withRetry(
        () -> {
          events.add("work");
          if (calls.incrementAndGet() == 1) {
            throw occ();
          }
          return null;
        },
        () -> events.add("rollback"),
        10,
        50L,
        millis -> events.add("sleep"),
        NO_JITTER);
    assertEquals(List.of("work", "rollback", "sleep", "work"), events);
  }

  @Test
  void anOtherErrorAfterAConflictPropagatesWithNoFurtherRollback() {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger rollbacks = new AtomicInteger();
    SQLException other = new SQLException("duplicate key", "23505");
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () ->
                OccRetry.withRetry(
                    () -> {
                      if (calls.incrementAndGet() == 1) {
                        throw occ();
                      }
                      throw other;
                    },
                    rollbacks::incrementAndGet,
                    10,
                    50L,
                    NO_SLEEP,
                    NO_JITTER));
    assertSame(other, thrown);
    assertEquals(1, rollbacks.get()); // between the attempts only: the caller owns the rest
  }

  @Test
  void nonConflictPropagatesImmediately() {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger rollbacks = new AtomicInteger();
    SQLException other = new SQLException("relation does not exist", "42P01");
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () ->
                OccRetry.withRetry(
                    () -> {
                      calls.incrementAndGet();
                      throw other;
                    },
                    rollbacks::incrementAndGet,
                    10,
                    50L,
                    NO_SLEEP,
                    NO_JITTER));
    assertSame(other, thrown);
    assertEquals(1, calls.get());
    assertEquals(0, rollbacks.get()); // not a conflict: the caller's error path rolls back
  }

  @Test
  void exhaustsBudgetAndReraisesLastConflict() {
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger rollbacks = new AtomicInteger();
    assertThrows(
        SQLException.class,
        () ->
            OccRetry.withRetry(
                () -> {
                  calls.incrementAndGet();
                  throw occ();
                },
                rollbacks::incrementAndGet,
                3,
                50L,
                NO_SLEEP,
                NO_JITTER));
    assertEquals(3, calls.get());
    assertEquals(2, rollbacks.get()); // never after the last attempt
  }

  // --- G22: Aurora DSQL's XX000 "server unavailable" is retried like a 40001 -----------------

  private static SQLException unavailable() {
    return new SQLException("server unavailable", OccRetry.SERVER_UNAVAILABLE_SQLSTATE);
  }

  @Test
  void serverUnavailableIsRetriedLikeAConflict() throws SQLException {
    // FAILS WITHOUT G22: an XX000 propagated at once (no retry), so the caller dead-lettered a
    // healthy row for a temporary service condition.
    List<String> events = new ArrayList<>();
    AtomicInteger calls = new AtomicInteger();
    String result =
        OccRetry.withRetry(
            () -> {
              events.add("work");
              if (calls.incrementAndGet() <= 2) {
                throw unavailable();
              }
              return "ok";
            },
            () -> events.add("rollback"),
            10,
            50L,
            millis -> events.add("sleep"),
            NO_JITTER);
    assertEquals("ok", result);
    assertEquals(
        List.of("work", "rollback", "sleep", "work", "rollback", "sleep", "work"), events);
  }

  @Test
  void serverUnavailableAndConflictsShareOneBudget() {
    // One budget for both: an XX000 does not reset or extend the 40001 count (the per-unit
    // bound the poll-interval analysis relies on).
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger rollbacks = new AtomicInteger();
    SQLException thrown =
        assertThrows(
            SQLException.class,
            () ->
                OccRetry.withRetry(
                    () -> {
                      throw calls.incrementAndGet() % 2 == 1 ? unavailable() : occ();
                    },
                    rollbacks::incrementAndGet,
                    4,
                    50L,
                    NO_SLEEP,
                    NO_JITTER));
    assertEquals(4, calls.get());
    assertEquals(3, rollbacks.get());
    assertEquals(OccRetry.OCC_SQLSTATE, thrown.getSQLState()); // the last error is re-raised
  }

  @Test
  void onlyXX000ItselfIsRetriedNotTheCorruptionCodes() {
    // XX001 (data corrupted) and XX002 (index corrupted) share XX000's class but are not
    // transient; neither is an aborted transaction (25P02) or a dropped connection (08006,
    // the caller's reconnect path).
    for (String state : List.of("XX001", "XX002", "25P02", "08006")) {
      AtomicInteger calls = new AtomicInteger();
      AtomicInteger rollbacks = new AtomicInteger();
      SQLException other = new SQLException("not retried in process", state);
      SQLException thrown =
          assertThrows(
              SQLException.class,
              () ->
                  OccRetry.withRetry(
                      () -> {
                        calls.incrementAndGet();
                        throw other;
                      },
                      rollbacks::incrementAndGet,
                      10,
                      50L,
                      NO_SLEEP,
                      NO_JITTER),
              state);
      assertSame(other, thrown, state);
      assertEquals(1, calls.get(), state);
      assertEquals(0, rollbacks.get(), state);
    }
    assertTrue(OccRetry.retriesInProcess(unavailable()));
    assertTrue(OccRetry.retriesInProcess(occ()));
    assertFalse(OccRetry.retriesInProcess(new SQLException("data corrupted", "XX001")));
    assertFalse(OccRetry.retriesInProcess(new SQLException("This connection has been closed.")));
  }

  @Test
  void backoffIsExponentialAndCapped() {
    assertEquals(100L, OccRetry.backoffCapMs(1, 50L));
    assertEquals(200L, OccRetry.backoffCapMs(2, 50L));
    assertEquals(OccRetry.MAX_BACKOFF_MS, OccRetry.backoffCapMs(20, 50L));
  }
}
