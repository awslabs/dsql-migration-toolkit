// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTaskContext;

/**
 * A stand-in for Connect's sink task context: records every rewind, pause, resume and poll
 * timeout the task asks for (the unique-conflict hold, plugin v53), in order.
 */
final class RecordingSinkTaskContext implements SinkTaskContext {
  final List<String> calls = new ArrayList<>();
  final Map<TopicPartition, Long> rewinds = new HashMap<>(); // the last rewind per partition
  final Set<TopicPartition> paused = new HashSet<>();
  final Set<TopicPartition> assigned = new HashSet<>();
  long lastTimeout = -1L;

  static String name(TopicPartition tp) {
    return tp.topic() + "-" + tp.partition();
  }

  @Override
  public Map<String, String> configs() {
    return Map.of();
  }

  @Override
  public void offset(Map<TopicPartition, Long> offsets) {
    offsets.forEach(this::offset);
  }

  @Override
  public void offset(TopicPartition tp, long offset) {
    calls.add("offset:" + name(tp) + ":" + offset);
    rewinds.put(tp, offset);
  }

  @Override
  public void timeout(long timeoutMs) {
    calls.add("timeout:" + timeoutMs);
    lastTimeout = timeoutMs;
  }

  @Override
  public Set<TopicPartition> assignment() {
    return assigned;
  }

  @Override
  public void pause(TopicPartition... partitions) {
    for (TopicPartition tp : partitions) {
      calls.add("pause:" + name(tp));
      paused.add(tp);
    }
  }

  @Override
  public void resume(TopicPartition... partitions) {
    for (TopicPartition tp : partitions) {
      calls.add("resume:" + name(tp));
      paused.remove(tp);
    }
  }

  @Override
  public void requestCommit() {
    calls.add("requestCommit");
  }
}
