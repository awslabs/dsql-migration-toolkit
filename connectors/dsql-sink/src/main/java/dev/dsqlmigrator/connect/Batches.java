// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Partitions a list into transaction-sized chunks.
 *
 * <p>DSQL bounds a write transaction on BOTH axes: at most 3,000 rows AND at most
 * 10 MiB of modified data. Chunking by row count alone (the {@link #partition(List, int)}
 * overload) lets a wide / JSON-heavy 3,000-row chunk blow past the 10 MiB byte limit and
 * collapse to the slow one-row-per-transaction fallback (or error), so
 * {@link #partition(List, int, long, ToLongFunction)} additionally flushes a chunk once
 * adding the next item would push its estimated modified bytes past the byte budget --
 * mirroring the Full Load loader's {@code MAX_BATCH_BYTES} bound in
 * {@code core/batched_import.py}.
 */
final class Batches {

  private Batches() {}

  static <T> List<List<T>> partition(List<T> items, int size) {
    if (size < 1) {
      throw new IllegalArgumentException("size must be at least 1");
    }
    List<List<T>> chunks = new ArrayList<>();
    for (int i = 0; i < items.size(); i += size) {
      chunks.add(new ArrayList<>(items.subList(i, Math.min(i + size, items.size()))));
    }
    return chunks;
  }

  /**
   * Partition {@code items} into chunks bounded by row count AND estimated payload bytes.
   *
   * <p>A chunk is flushed when it reaches {@code size} rows OR when adding the next item
   * would push the chunk's estimated modified bytes past {@code maxBytes} (headroom under
   * DSQL's 10 MiB per-write-transaction limit). A non-empty chunk always keeps at least one
   * item -- a single event cannot be split, and it is independently bounded by DSQL's 2 MiB
   * row limit plus the sink's 1 MiB per-value guard -- so an oversized lone item forms its
   * own chunk rather than being dropped. {@code sizer} estimates one item's modified bytes
   * cheaply (see {@code DsqlSinkTask#estimateModifiedBytes}); it is summed once per item, so
   * a size-skewed run (small first row, large later rows) still splits correctly.
   */
  static <T> List<List<T>> partition(
      List<T> items, int size, long maxBytes, ToLongFunction<T> sizer) {
    return partition(items, size, maxBytes, sizer, item -> 1);
  }

  /**
   * {@link #partition(List, int, long, ToLongFunction)} where an item may modify more than one
   * row: {@code size} bounds the chunk's summed {@code rowWeight}, not its item count. A move on
   * a re-keyed table (G15) is a delete of the old row plus an insert of the new one in the same
   * transaction, so it weighs 2 -- a chunk of 3,000 weighted rows never exceeds DSQL's 3,000
   * rows per transaction. A chunk is flushed BEFORE an item whose weight would push it past
   * {@code size}, and AFTER one that brings it to {@code size}; a non-empty chunk always keeps
   * at least one item, so a lone heavy item forms its own chunk. With every weight 1 this is
   * exactly the unweighted overload.
   */
  static <T> List<List<T>> partition(
      List<T> items,
      int size,
      long maxBytes,
      ToLongFunction<T> sizer,
      ToIntFunction<T> rowWeight) {
    if (size < 1) {
      throw new IllegalArgumentException("size must be at least 1");
    }
    if (maxBytes < 1) {
      throw new IllegalArgumentException("maxBytes must be at least 1");
    }
    List<List<T>> chunks = new ArrayList<>();
    List<T> chunk = new ArrayList<>();
    long chunkBytes = 0;
    long chunkWeight = 0;
    for (T item : items) {
      long itemBytes = sizer.applyAsLong(item);
      int weight = Math.max(rowWeight.applyAsInt(item), 1);
      // Flush BEFORE appending when this item would push a non-empty chunk over the byte
      // budget or the row limit (a single oversized item still forms its own chunk -- it
      // cannot be split).
      if (!chunk.isEmpty() && (chunkBytes + itemBytes > maxBytes || chunkWeight + weight > size)) {
        chunks.add(chunk);
        chunk = new ArrayList<>();
        chunkBytes = 0;
        chunkWeight = 0;
      }
      chunk.add(item);
      chunkBytes += itemBytes;
      chunkWeight += weight;
      if (chunkWeight >= size) {
        chunks.add(chunk);
        chunk = new ArrayList<>();
        chunkBytes = 0;
        chunkWeight = 0;
      }
    }
    if (!chunk.isEmpty()) {
      chunks.add(chunk);
    }
    return chunks;
  }
}
