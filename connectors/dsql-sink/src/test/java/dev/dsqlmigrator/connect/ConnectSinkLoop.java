// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * A model of how Kafka Connect 3.7's WorkerSinkTask drives one sink task (the version the
 * cdc-stack pins), over in-memory partition logs, so a test can follow what Connect does with
 * what the task asks of its context (RecordingSinkTaskContext):
 *
 *   poll():    rewind() first -- seek each partition the task asked for, and set its current
 *              and last-committed offset to it; then fetch from the partitions that are not
 *              paused; convertMessages() records origOffsets[tp] = offset + 1 and appends to
 *              messageBatch (origOffsets is NOT cleared by a poll); then deliverMessages().
 *   deliver:   put(a copy of messageBatch). Returned: currentOffsets.putAll(origOffsets), both
 *              cleared, and if every partition was paused for a redelivery, resume all but the
 *              ones the task paused. RetriableException: messageBatch and origOffsets are KEPT
 *              (the same batch is handed back after the next poll) and, the first time, every
 *              partition is paused (pauseAll) so that poll fetches nothing.
 *   commit():  preCommit(current offsets); a task offset is taken when it is <= the current one
 *              (else that partition keeps its last commit); closing also calls close().
 *
 * The task's pause()/resume() calls reach the consumer during put(); they are replayed from the
 * context's call log right after put() returns or throws, before Connect's own pauseAll or
 * resume, which gives the same consumer state at the next poll.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

final class ConnectSinkLoop {
  // "Kafka": the partition logs and the group's committed offsets (they survive a task).
  final Map<TopicPartition, TreeMap<Long, SinkRecord>> log = new LinkedHashMap<>();
  final Map<TopicPartition, Long> committed = new HashMap<>();
  final List<String> trace = new ArrayList<>();
  int maxPerPartition = Integer.MAX_VALUE; // records one poll fetches from each partition
  // An empty consumer.poll() waits out its timeout -- the task's context.timeout, else the
  // commit deadline (defaultWaitMs here) -- before put() runs: the test's clock.
  java.util.function.LongConsumer clock = ms -> {};
  long defaultWaitMs = 60_000L;
  int lastFetched;

  // One task incarnation.
  SinkTask task;
  RecordingSinkTaskContext ctx;
  private final Map<TopicPartition, Long> position = new LinkedHashMap<>();
  private final Set<TopicPartition> consumerPaused = new HashSet<>();
  private final List<SinkRecord> messageBatch = new ArrayList<>();
  private final Map<TopicPartition, Long> origOffsets = new HashMap<>();
  final Map<TopicPartition, Long> currentOffsets = new HashMap<>();
  private final Map<TopicPartition, Long> lastCommitted = new HashMap<>();
  private boolean pausedForRedelivery;
  private int callsSeen;

  private final Map<TopicPartition, Long> logStart = new HashMap<>();

  /** A partition whose first offset is {@code start}; its records are appended over time. */
  void declare(TopicPartition tp, long start) {
    log.computeIfAbsent(tp, k -> new TreeMap<>());
    logStart.put(tp, start);
  }

  /** Records produced since: the next poll can fetch them. */
  void append(SinkRecord... records) {
    for (SinkRecord r : records) {
      TopicPartition tp = new TopicPartition(r.topic(), r.kafkaPartition());
      if (!log.containsKey(tp)) {
        declare(tp, r.kafkaOffset());
      }
      log.get(tp).put(r.kafkaOffset(), r);
    }
  }

  /** Assign every partition of the log to {@code task} from the committed offsets. */
  void start(SinkTask task, RecordingSinkTaskContext ctx) {
    this.task = task;
    this.ctx = ctx;
    position.clear();
    consumerPaused.clear();
    messageBatch.clear();
    origOffsets.clear();
    currentOffsets.clear();
    lastCommitted.clear();
    pausedForRedelivery = false;
    callsSeen = ctx.calls.size();
    for (TopicPartition tp : log.keySet()) {
      long from = committed.getOrDefault(tp, logStart.get(tp));
      position.put(tp, from);
      currentOffsets.put(tp, from);
      lastCommitted.put(tp, from);
      ctx.assigned.add(tp);
    }
    task.open(new ArrayList<>(log.keySet()));
  }

  /** One WorkerSinkTask.poll(); true when put() returned, false on a RetriableException. */
  boolean poll() {
    // rewind()
    for (Map.Entry<TopicPartition, Long> e : ctx.rewinds.entrySet()) {
      position.put(e.getKey(), e.getValue());
      currentOffsets.put(e.getKey(), e.getValue());
      lastCommitted.put(e.getKey(), e.getValue());
      trace.add("rewind " + label(e.getKey()) + " -> " + e.getValue());
    }
    ctx.rewinds.clear();
    long timeoutMs = ctx.lastTimeout;
    ctx.lastTimeout = -1L;
    int fetched = 0;
    // consumer.poll(): a paused partition returns nothing
    for (Map.Entry<TopicPartition, TreeMap<Long, SinkRecord>> e : log.entrySet()) {
      TopicPartition tp = e.getKey();
      if (consumerPaused.contains(tp)) {
        continue;
      }
      long pos = position.get(tp);
      int taken = 0;
      for (SinkRecord r : e.getValue().tailMap(pos, true).values()) {
        if (taken++ == maxPerPartition) {
          break;
        }
        messageBatch.add(r); // convertMessages()
        origOffsets.put(tp, r.kafkaOffset() + 1);
        pos = r.kafkaOffset() + 1;
        fetched++;
      }
      position.put(tp, pos);
    }
    lastFetched = fetched;
    if (fetched == 0) {
      clock.accept(timeoutMs > 0 ? timeoutMs : defaultWaitMs);
    }
    // deliverMessages()
    List<String> names = new ArrayList<>();
    messageBatch.forEach(r -> names.add(HoldHarness.name(r)));
    try {
      task.put(new ArrayList<>(messageBatch));
      replayPauses();
      currentOffsets.putAll(origOffsets);
      origOffsets.clear();
      messageBatch.clear();
      if (pausedForRedelivery) {
        consumerPaused.retainAll(ctx.paused); // resumeAll(), but what the task paused
        pausedForRedelivery = false;
      }
      trace.add("put " + names + " ok");
      return true;
    } catch (RetriableException e) {
      replayPauses();
      if (!pausedForRedelivery) {
        pausedForRedelivery = true;
        consumerPaused.addAll(position.keySet()); // pauseAll()
      }
      trace.add("put " + names + " RetriableException");
      return false;
    }
  }

  private void replayPauses() {
    List<String> calls = ctx.calls;
    for (int i = callsSeen; i < calls.size(); i++) {
      String call = calls.get(i);
      for (TopicPartition tp : log.keySet()) {
        if (call.equals("pause:" + RecordingSinkTaskContext.name(tp))) {
          consumerPaused.add(tp);
        } else if (call.equals("resume:" + RecordingSinkTaskContext.name(tp))) {
          consumerPaused.remove(tp);
        }
      }
    }
    callsSeen = calls.size();
  }

  /** WorkerSinkTask.commitOffsets(): the group's committed offsets afterwards. */
  Map<TopicPartition, Long> commit(boolean closing) {
    Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
    currentOffsets.forEach((tp, o) -> toCommit.put(tp, new OffsetAndMetadata(o)));
    Map<TopicPartition, OffsetAndMetadata> provided;
    try {
      provided = task.preCommit(new HashMap<>(toCommit));
    } finally {
      if (closing) {
        task.close(new ArrayList<>(toCommit.keySet()));
      }
    }
    Map<TopicPartition, Long> committable = new HashMap<>(lastCommitted);
    for (Map.Entry<TopicPartition, OffsetAndMetadata> e : provided.entrySet()) {
      TopicPartition tp = e.getKey();
      if (committable.containsKey(tp) && e.getValue().offset() <= toCommit.get(tp).offset()) {
        committable.put(tp, e.getValue().offset());
      }
    }
    if (!committable.equals(lastCommitted)) {
      committed.putAll(committable);
      lastCommitted.putAll(committable);
    }
    trace.add((closing ? "closing commit " : "commit ") + flat(committed));
    return committedOffsets();
  }

  /** Where the next owner of each partition starts: its committed offset, else its start. */
  Map<TopicPartition, Long> committedOffsets() {
    Map<TopicPartition, Long> out = new HashMap<>();
    for (TopicPartition tp : log.keySet()) {
      out.put(tp, committed.getOrDefault(tp, logStart.get(tp)));
    }
    return out;
  }

  /** Poll and commit until every partition is drained, nothing is held and no rewind waits. */
  void drain(int maxPolls) {
    for (int i = 0; i < maxPolls; i++) {
      poll();
      commit(false);
      boolean drained = true;
      for (Map.Entry<TopicPartition, TreeMap<Long, SinkRecord>> e : log.entrySet()) {
        if (!e.getValue().isEmpty() && position.get(e.getKey()) <= e.getValue().lastKey()) {
          drained = false;
        }
      }
      if (drained && ((DsqlSinkTask) task).heldOffsets().isEmpty() && ctx.rewinds.isEmpty()) {
        return;
      }
    }
    throw new AssertionError("did not drain: " + trace);
  }

  /** The task dies (a crash, a worker recycle): nothing more is committed. */
  void crash() {
    trace.add("crash");
    task = null;
  }

  long position(TopicPartition tp) {
    return position.get(tp);
  }

  private static String label(TopicPartition tp) {
    return "P" + tp.partition();
  }

  static String flat(Map<TopicPartition, Long> m) {
    Map<String, Long> out = new TreeMap<>();
    m.forEach((tp, o) -> out.put(label(tp), o));
    return out.toString();
  }

  static Collection<SinkRecord> none() {
    return List.of();
  }
}
