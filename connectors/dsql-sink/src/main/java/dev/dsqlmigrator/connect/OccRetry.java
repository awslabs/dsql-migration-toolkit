/*
 * Task 23.2 — custom Aurora DSQL Kafka Connect sink connector.
 * Implemented and offline unit-tested. Live throughput under contention
 * (cdc-connector-spike.md, H2) is still pending: see README.md.
 */
package dev.dsqlmigrator.connect;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/**
 * Statement-level OCC retry for DSQL serialization failures (SQLSTATE 40001).
 *
 * <p>Mirrors the Python {@code core/occ.py} policy: a unit of work that raises
 * {@code 40001} is retried up to {@code maxAttempts} with exponential backoff +
 * full jitter (capped); any other SQL error propagates immediately, and once
 * the attempt budget is exhausted the last {@code 40001} is re-raised. Because
 * the connector applies idempotent PK-keyed upsert/delete, retrying a
 * conflicted statement is safe (Property 5 analog). Spike H2 measures
 * throughput under contention.
 *
 * <p>The sleeper and jitter source are injectable (package-private overload) so
 * the retry policy is unit-tested instantly and deterministically without real
 * waiting.
 */
public final class OccRetry {

  /** PostgreSQL/DSQL serialization-failure SQLSTATE. */
  public static final String OCC_SQLSTATE = "40001";

  /** Cap on a single backoff sleep (ms), mirroring core/occ.py's 5s cap. */
  static final long MAX_BACKOFF_MS = 5_000L;

  private OccRetry() {}

  @FunctionalInterface
  public interface SqlWork<T> {
    T run() throws SQLException;
  }

  /** Injectable sleep seam (real: {@link Thread#sleep(long)}). */
  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** Production entry point: real sleep, real jitter. */
  public static <T> T withRetry(SqlWork<T> work, int maxAttempts, long baseBackoffMs)
      throws SQLException {
    return withRetry(work, maxAttempts, baseBackoffMs, Thread::sleep, OccRetry::fullJitter);
  }

  /**
   * Test/seam overload. {@code jitter} maps an exclusive upper bound to a delay
   * in {@code [0, bound)}; {@code sleeper} performs the wait.
   */
  static <T> T withRetry(
      SqlWork<T> work,
      int maxAttempts,
      long baseBackoffMs,
      Sleeper sleeper,
      LongUnaryOperator jitter)
      throws SQLException {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be at least 1");
    }
    int attempt = 0;
    while (true) {
      try {
        return work.run();
      } catch (SQLException e) {
        attempt++;
        if (!OCC_SQLSTATE.equals(e.getSQLState()) || attempt >= maxAttempts) {
          throw e;
        }
        long capped = backoffCapMs(attempt, baseBackoffMs);
        long delay = jitter.applyAsLong(capped + 1);
        try {
          sleeper.sleep(delay);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
  }

  /** Exponential backoff base*2^attempt, capped at {@link #MAX_BACKOFF_MS}. */
  static long backoffCapMs(int attempt, long baseBackoffMs) {
    long shift = Math.min(attempt, 16);
    long exp = baseBackoffMs * (1L << shift);
    return Math.min(exp, MAX_BACKOFF_MS);
  }

  private static long fullJitter(long boundExclusive) {
    if (boundExclusive <= 0L) {
      return 0L;
    }
    return ThreadLocalRandom.current().nextLong(boundExclusive);
  }
}
