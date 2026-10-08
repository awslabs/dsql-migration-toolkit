// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

  @Test
  void backoffIsExponentialAndCapped() {
    assertEquals(100L, OccRetry.backoffCapMs(1, 50L));
    assertEquals(200L, OccRetry.backoffCapMs(2, 50L));
    assertEquals(OccRetry.MAX_BACKOFF_MS, OccRetry.backoffCapMs(20, 50L));
  }
}
