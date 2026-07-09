// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
import java.util.List;

/** Partitions a list into fixed-size chunks (DSQL caps a transaction at 3,000 rows). */
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
}
