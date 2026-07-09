// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class BatchesTest {

  @Test
  void partitionsEvenly() {
    List<List<Integer>> chunks = Batches.partition(List.of(1, 2, 3, 4), 2);
    assertEquals(2, chunks.size());
    assertEquals(List.of(1, 2), chunks.get(0));
    assertEquals(List.of(3, 4), chunks.get(1));
  }

  @Test
  void partitionsWithRemainder() {
    List<List<Integer>> chunks = Batches.partition(List.of(1, 2, 3, 4, 5), 2);
    assertEquals(3, chunks.size());
    assertEquals(List.of(5), chunks.get(2));
  }

  @Test
  void emptyInputYieldsNoChunks() {
    assertTrue(Batches.partition(List.<Integer>of(), 3).isEmpty());
  }

  @Test
  void rejectsNonPositiveSize() {
    assertThrows(IllegalArgumentException.class, () -> Batches.partition(List.of(1), 0));
  }
}
