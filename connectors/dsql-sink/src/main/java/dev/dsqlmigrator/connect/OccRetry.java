// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Implemented and offline unit-tested. Validate live throughput under contention
 * before a production deploy: see README.md.
 */
package dev.dsqlmigrator.connect;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/**
 * OCC retry of a whole transaction on DSQL serialization failures (SQLSTATE 40001).
 *
 * <p>Mirrors the Python {@code core/occ.py} policy: a unit of work that raises
 * {@code 40001} is retried up to {@code maxAttempts} with exponential backoff +
 * full jitter (capped); any other SQL error propagates immediately, and once
 * the attempt budget is exhausted the last {@code 40001} is re-raised. Because
 * the connector applies idempotent PK-keyed upsert/delete, re-running a
 * conflicted transaction (the sink's whole chunk, or one row's) is safe
 * (Property 5 analog). Spike H2 measures throughput under contention.
 *
 * <p><b>Every attempt starts on a clean transaction.</b> After a {@code 40001}, and only when
 * another attempt follows, the caller's {@code rollback} runs before the backoff. Aurora DSQL
 * reports a write conflict (OC000) at COMMIT, which already ended the transaction (the rollback
 * then sends nothing), but a schema conflict (OC001: the session's cached catalog is behind
 * after a DDL, GRANT or REVOKE elsewhere on the cluster, such as an asynchronous index build
 * finishing) can come when a statement runs -- and that failed statement leaves the transaction
 * aborted, so without the rollback every statement of the next attempt failed with 25P02,
 * which is not retried. The retry stays on the same session, whose catalog Aurora DSQL
 * refreshes. After the last attempt, and for any other error, nothing runs here: the caller's
 * error path rolls back.
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

  /**
   * Production entry point: real sleep, real jitter. {@code rollback} must leave the
   * connection with no open transaction; it runs between attempts only.
   */
  public static <T> T withRetry(
      SqlWork<T> work, Runnable rollback, int maxAttempts, long baseBackoffMs)
      throws SQLException {
    return withRetry(
        work, rollback, maxAttempts, baseBackoffMs, Thread::sleep, OccRetry::fullJitter);
  }

  /**
   * Test/seam overload. {@code jitter} maps an exclusive upper bound to a delay
   * in {@code [0, bound)}; {@code sleeper} performs the wait.
   */
  static <T> T withRetry(
      SqlWork<T> work,
      Runnable rollback,
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
        rollback.run(); // the next attempt starts on a clean transaction
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
