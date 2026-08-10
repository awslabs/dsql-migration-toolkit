// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Regression guard: the CloudWatch monitor must never run on the offset-commit path.
 *
 * Connect calls SinkTask.flush() inside the offset commit and bounds that commit by
 * offset.flush.timeout.ms. Emitting the per-table monitor metrics INLINE there meant a
 * slow PutMetricData (its first call also resolves credentials/endpoints, and in the
 * cdc-stack it egresses via NAT with no monitoring VPC endpoint) could consume the whole
 * commit budget and surface as a repeating "Commit of offsets timed out" -- a
 * best-effort monitor degrading replication. flush() must hand the emission to the
 * background thread and return immediately.
 */
package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DsqlSinkMetricsOffCommitPathTest {

  /** Config with metrics ENABLED (a Stack dimension is what turns emission on). */
  private static Map<String, String> metricsEnabledProps() {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    props.put(DsqlSinkConnectorConfig.METRICS_STACK, "test-stack");
    return props;
  }

  /**
   * A task whose emission blocks until released, so a test can prove flush() did not
   * wait for it. Overrides the emit seam rather than talking to CloudWatch.
   */
  private static final class BlockingEmitTask extends DsqlSinkTask {
    final CountDownLatch emitStarted = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    volatile int emitCount = 0;

    @Override
    void emitMetrics() {
      emitCount++;
      emitStarted.countDown();
      try {
        release.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  void flushDoesNotWaitForTheCloudWatchEmission() throws Exception {
    BlockingEmitTask task = new BlockingEmitTask();
    task.startMetrics(metricsEnabledProps());
    try {
      long before = System.nanoTime();
      task.flush(Collections.emptyMap());
      long elapsedMs = (System.nanoTime() - before) / 1_000_000L;

      // flush() returned while the emission is still blocked -- that is the property.
      assertTrue(
          task.emitStarted.await(5, TimeUnit.SECONDS), "emission should run in the background");
      assertTrue(
          elapsedMs < 1_000L,
          "flush() must not block on the emission; took " + elapsedMs + " ms");
    } finally {
      task.release.countDown();
      task.stop();
    }
  }

  @Test
  void aSlowEmissionDoesNotQueueAWindowPerCommit() throws Exception {
    // A slow CloudWatch must not build a backlog: while one emission is in flight,
    // further flushes are skipped. Nothing is lost -- the counters are only read when
    // an emission runs, so skipped counts roll into the next window.
    BlockingEmitTask task = new BlockingEmitTask();
    task.startMetrics(metricsEnabledProps());
    try {
      for (int i = 0; i < 20; i++) {
        task.flush(Collections.emptyMap());
      }
      assertTrue(task.emitStarted.await(5, TimeUnit.SECONDS));
      assertEquals(1, task.emitCount, "only one emission should be in flight");
    } finally {
      task.release.countDown();
      task.stop();
    }
  }

  @Test
  void stopEmitsTheFinalWindowInline() throws Exception {
    // stop() is teardown, not the commit path, so the last window is emitted inline --
    // otherwise counts since the last commit would die with the daemon thread.
    BlockingEmitTask task = new BlockingEmitTask();
    task.startMetrics(metricsEnabledProps());
    task.release.countDown(); // do not block; we only care that it runs
    task.stop();
    assertEquals(1, task.emitCount, "stop() must emit the final window");
  }

  @Test
  void emissionIsInlineAndSilentWhenMetricsAreDisabled() {
    // No Stack dimension -> metrics off -> no executor, no emission, no throw.
    BlockingEmitTask task = new BlockingEmitTask();
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    task.startMetrics(props);
    task.flush(Collections.emptyMap());
    assertEquals(0, task.emitCount, "metrics disabled: nothing should be emitted");
  }
}
