package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
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
    String result =
        OccRetry.withRetry(
            () -> {
              if (calls.incrementAndGet() < 3) {
                throw occ();
              }
              return "ok";
            },
            10,
            50L,
            NO_SLEEP,
            NO_JITTER);
    assertEquals("ok", result);
    assertEquals(3, calls.get());
  }

  @Test
  void nonConflictPropagatesImmediately() {
    AtomicInteger calls = new AtomicInteger();
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
                    10,
                    50L,
                    NO_SLEEP,
                    NO_JITTER));
    assertSame(other, thrown);
    assertEquals(1, calls.get());
  }

  @Test
  void exhaustsBudgetAndReraisesLastConflict() {
    AtomicInteger calls = new AtomicInteger();
    assertThrows(
        SQLException.class,
        () ->
            OccRetry.withRetry(
                () -> {
                  calls.incrementAndGet();
                  throw occ();
                },
                3,
                50L,
                NO_SLEEP,
                NO_JITTER));
    assertEquals(3, calls.get());
  }

  @Test
  void backoffIsExponentialAndCapped() {
    assertEquals(100L, OccRetry.backoffCapMs(1, 50L));
    assertEquals(200L, OccRetry.backoffCapMs(2, 50L));
    assertEquals(OccRetry.MAX_BACKOFF_MS, OccRetry.backoffCapMs(20, 50L));
  }
}
