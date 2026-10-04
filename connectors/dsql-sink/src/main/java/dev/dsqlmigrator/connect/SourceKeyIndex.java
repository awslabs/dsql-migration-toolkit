// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Whether a re-keyed table's source-key unique index is valid: pure cache, offline
 * unit-tested (DsqlSinkRekeyTest). The JDBC catalog read that feeds it lives in DsqlSinkTask
 * and ports a live-verified query (core/target_introspector.unique_index_state).
 */
package dev.dsqlmigrator.connect;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

/**
 * May a source-keyed table's deletes be keyed on its SOURCE primary key?
 *
 * <p>A re-keyed table's Aurora DSQL primary key is the composite {@code (leading, source
 * key)}; Schema Conversion's Composite key also creates a unique index on the source key with
 * {@code CREATE UNIQUE INDEX ASYNC} -- with the table at Schema Conversion's Apply, or after the
 * load when Full Load recreates the table (Drop &amp; reload) -- and Aurora DSQL builds it in the
 * background. A {@code DELETE ... WHERE <source key> = ?} is a single-row delete through that
 * index -- but only once it is VALID: while it builds (live-verified: the index row exists with
 * {@code indisvalid = false}) or when it is absent (an edited DDL), the same statement scans the
 * whole table. So the sink keys a delete (or a move's delete) on the source key only when this
 * cache reports a valid unique index on exactly that column SET, and otherwise on the exact
 * composite key from the before-image -- never a full scan.
 *
 * <p>One catalog read per table, cached: {@link #VALID_TTL_NANOS} once valid,
 * {@link #NOT_VALID_TTL_NANOS} while not valid or unknown (so a finished build is picked up
 * within a minute), re-read when the requested columns differ, and dropped by
 * {@link #invalidate} when Aurora DSQL reports the table or a column missing. Bounded by the
 * captured tables. Not thread-safe: {@code put()} is single-threaded per task.
 */
final class SourceKeyIndex {

  /**
   * The unique indexes of one {@code schema.table}, each with its validity and KEY columns in
   * order. A port of {@code core/target_introspector.unique_index_state} (live-verified on
   * Aurora DSQL), with the names bound as parameters and the column names cast to text so the
   * driver returns a {@code String[]}.
   */
  static final String CATALOG_SQL =
      "SELECT i.relname, ix.indisvalid, "
          + "ARRAY(SELECT a.attname::text FROM pg_catalog.pg_attribute a "
          + "JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) "
          + "ON a.attnum = k.attnum "
          + "WHERE a.attrelid = t.oid AND k.ord <= ix.indnkeyatts "
          + "ORDER BY k.ord) AS key_columns "
          + "FROM pg_catalog.pg_index ix "
          + "JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid "
          + "JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid "
          + "JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace "
          + "WHERE ix.indisunique AND n.nspname = ? AND t.relname = ?";

  /** How long a VALID index is trusted. */
  static final long VALID_TTL_NANOS = TimeUnit.MINUTES.toNanos(10);

  /** How soon a not-valid (building, absent) or unreadable index is read again. */
  static final long NOT_VALID_TTL_NANOS = TimeUnit.SECONDS.toNanos(60);

  private record Entry(List<String> columns, boolean valid, long loadedAt, long lifeNanos) {}

  private final BiFunction<String, List<String>, Boolean> loader;
  private final Map<String, Entry> entries = new HashMap<>();

  /**
   * @param loader {@code (schema.table, source-key columns)} -> {@code TRUE} when a valid
   *     unique index has exactly that column set, {@code FALSE} when none does, {@code null}
   *     when unknown (unreadable); it may throw a {@code RetriableException}, which caches
   *     nothing
   */
  SourceKeyIndex(BiFunction<String, List<String>, Boolean> loader) {
    this.loader = loader;
  }

  /** Whether {@code table}'s deletes may be keyed on {@code columns}. At most one read per TTL. */
  boolean valid(String table, List<String> columns, long nowNanos) {
    Entry entry = entries.get(table);
    if (entry != null
        && entry.columns().equals(columns)
        && nowNanos - entry.loadedAt() < entry.lifeNanos()) {
      return entry.valid();
    }
    boolean valid = Boolean.TRUE.equals(loader.apply(table, columns));
    entries.put(
        table,
        new Entry(
            List.copyOf(columns), valid, nowNanos, valid ? VALID_TTL_NANOS : NOT_VALID_TTL_NANOS));
    return valid;
  }

  /** Forget {@code table}'s state (Aurora DSQL reported it, or a column of it, missing). */
  void invalidate(String table) {
    entries.remove(table);
  }

  /**
   * True when one catalog row is a VALID unique index whose key columns are exactly the SET
   * {@code wanted} (order need not match: any unique index on that set makes the delete a
   * single-row lookup). Pure; package-private for tests.
   */
  static boolean validOn(boolean indisvalid, Object[] keyColumns, Set<String> wanted) {
    if (!indisvalid || keyColumns == null || keyColumns.length != wanted.size()) {
      return false;
    }
    Set<String> have = new HashSet<>();
    for (Object column : keyColumns) {
      if (column == null) {
        return false;
      }
      have.add(column.toString());
    }
    return have.equals(wanted);
  }
}
