// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Maps a Debezium envelope SinkRecord to a ChangeEvent. Offline unit-tested
 * with synthetic Connect Structs; the exact envelope shape produced by the
 * deployed Debezium + Glue converter config is confirmed in the spike.
 */
package dev.dsqlmigrator.connect;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Parses a Debezium change event into a {@link ChangeEvent}.
 *
 * <p>The record key carries the primary-key columns ({@code pk.mode=record_key}).
 * The value is the Debezium envelope Struct ({@code op}, {@code before},
 * {@code after}, {@code source}); a {@code null} value is a tombstone. Mapping:
 * {@code c/r/u} (or any op with an after-image) becomes an upsert from the
 * after-image; {@code d} and tombstones become a delete keyed by the PK
 * (falling back to the before-image when the message has no key).
 */
final class DebeziumEvents {

  /**
   * Debezium's default sentinel for an UNCHANGED, out-of-line (TOASTed) column value on a
   * PostgreSQL UPDATE. Under {@code REPLICA IDENTITY DEFAULT} the WAL omits a TOASTed
   * column that the UPDATE did not change, so Debezium substitutes this placeholder in the
   * after-image ({@code unavailable.value.placeholder}, which the source connector leaves
   * at its default). Binding it would OVERWRITE the real value with the sentinel, so such a
   * column is dropped from the upsert (see {@link #extractAfterImage}) -> {@code ON CONFLICT
   * DO UPDATE} leaves the existing DSQL value intact. This is a PostgreSQL-only concern
   * (MySQL has no TOAST), so {@link #parse} gates the drop on a PostgreSQL source and the
   * guard is never even evaluated for a MySQL source.
   */
  static final String TOAST_UNAVAILABLE_PLACEHOLDER = "__debezium_unavailable_value";

  // The bytea form of the placeholder (a bytea column carries the sentinel as the UTF-8
  // bytes of the string, since the source connector uses the default string placeholder).
  private static final byte[] TOAST_UNAVAILABLE_PLACEHOLDER_BYTES =
      TOAST_UNAVAILABLE_PLACEHOLDER.getBytes(StandardCharsets.UTF_8);

  private DebeziumEvents() {}

  static ChangeEvent parse(SinkRecord record) {
    List<String> pkColumns = new ArrayList<>();
    List<Object> pkValues = new ArrayList<>();
    extractStruct(record.key(), pkColumns, pkValues);

    Object value = record.value();
    if (value == null) {
      // Tombstone -> delete by key. No envelope, so no source.ts_ms (lag unknown).
      return buildDelete(tableFromTopic(record.topic()), pkColumns, pkValues, 0L);
    }
    if (!(value instanceof Struct envelope)) {
      throw new DataException(
          "Unsupported record value type; expected a Debezium Struct envelope");
    }

    String op = optString(envelope, "op");
    Struct source = optStruct(envelope, "source");
    String table = resolveTable(envelope, record.topic());
    // Source commit time for the end-to-end replication-lag metric (now - ts at
    // apply). 0 when the source block omits ts_ms -> lag simply not recorded.
    long sourceTsMs = optLong(source, "ts_ms");
    // The TOAST unavailable-value omission (see extractAfterImage) is a PostgreSQL-only
    // concern, so gate it on the origin engine (Debezium's source.connector). A MySQL --
    // or unknown -- source keeps byte-identical pre-Phase-D behavior: every after-image
    // column is bound verbatim, even one whose value happens to equal the sentinel string
    // (MySQL has no TOAST, so the sentinel is only ever real user data there).
    boolean pgSource = "postgresql".equals(optString(source, "connector"));
    Struct after = optStruct(envelope, "after");

    if ("d".equals(op) || after == null) {
      if (pkColumns.isEmpty()) {
        // No message key: fall back to the before-image for the PK.
        Struct before = optStruct(envelope, "before");
        if (before != null) {
          extractStruct(before, pkColumns, pkValues);
        }
      }
      return buildDelete(table, pkColumns, pkValues, sourceTsMs);
    }

    List<String> columns = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    extractAfterImage(after, columns, values, pgSource);
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build upsert for table " + table + ": record has no key (pk) fields");
    }
    // Classify for the net-rows monitor metric: c (create) / r (snapshot read) are
    // inserts (+1 to the target row count); u (update) is an upsert that leaves the
    // count unchanged (net 0). Both apply identically (idempotent ON CONFLICT upsert).
    boolean isInsert = "c".equals(op) || "r".equals(op);
    return isInsert
        ? ChangeEvent.insert(table, columns, values, pkColumns, pkValues, sourceTsMs)
        : ChangeEvent.upsert(table, columns, values, pkColumns, pkValues, sourceTsMs);
  }

  private static ChangeEvent buildDelete(
      String table, List<String> pkColumns, List<Object> pkValues, long sourceTsMs) {
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build DELETE for table " + table + ": no primary key in record key or before-image");
    }
    return ChangeEvent.delete(table, pkColumns, pkValues, sourceTsMs);
  }

  private static void extractStruct(Object maybeStruct, List<String> names, List<Object> values) {
    if (!(maybeStruct instanceof Struct struct)) {
      return;
    }
    for (Field field : struct.schema().fields()) {
      names.add(field.name());
      // Convert the Debezium-encoded value to its canonical DSQL-target form
      // (e.g. MicroTimestamp Long -> java.sql.Timestamp) so it matches the Full
      // Load bulk loader's encoding before it is bound. The field's schema name
      // carries the Debezium logical type that drives the conversion.
      values.add(DebeziumTypeConverter.convert(field.schema().name(), struct.get(field)));
    }
  }

  /**
   * Extract an UPSERT after-image, dropping any column whose value is the PostgreSQL TOAST
   * unavailable-value placeholder (see {@link #TOAST_UNAVAILABLE_PLACEHOLDER}) when
   * {@code dropToastPlaceholder} is set (i.e. a PostgreSQL source -- see {@link #parse}).
   * For a MySQL source {@code dropToastPlaceholder} is false and every column is bound
   * verbatim, byte-identical to the pre-Phase-D path (MySQL has no TOAST, so the sentinel
   * there would only ever be genuine user data that must NOT be dropped).
   *
   * <p>An omitted column is simply absent from the rendered {@code INSERT ... ON CONFLICT
   * DO UPDATE SET ...}, so its existing DSQL value is preserved (a partial upsert) instead
   * of being overwritten with the sentinel. The primary key is never TOASTed, so it is
   * never dropped -- and the {@code ON CONFLICT} target comes from the record key, not this
   * after-image, so dropping a non-key column cannot affect conflict resolution.
   *
   * <p><b>Tradeoff:</b> if a placeholder-bearing UPDATE ever targets a PK that does not yet
   * exist in DSQL, the {@code ON CONFLICT} inserts a row with that column left at its
   * default (NULL) rather than the true value. Under the gapless handoff the row always
   * exists (the slot resumes after the Full Load consistency point), and Validation would
   * surface any residual gap -- a bounded, detectable gap is strictly better than silently
   * writing the sentinel string into the column.
   *
   * <p><b>Known v1 limitation (unchanged TOASTed {@code numeric}):</b> Debezium substitutes
   * a DETECTABLE placeholder only for string- and bytes-schema'd columns (text/varchar/
   * char/json/jsonb and bytea) -- the realistic large-value types. For an unchanged TOASTed
   * {@code numeric} its value converter yields a plain {@code NULL} (not the sentinel), which
   * this method cannot distinguish from a genuine {@code NULL} update, so such a column is
   * NOT omitted and the upsert would overwrite the real value with NULL. This requires a
   * {@code numeric} large enough to be stored out-of-line (thousands of digits, &gt;~2 KiB) --
   * effectively unreachable for real data (business numbers never reach that size; "very
   * large values" are text/blob, which ARE handled). The robust fix is Debezium's
   * {@code ReselectColumnsPostProcessor} (re-query the unavailable column from the source by
   * PK), to be enabled on the source connector and validated live in Phase F.
   */
  private static void extractAfterImage(
      Object after, List<String> names, List<Object> values, boolean dropToastPlaceholder) {
    if (!(after instanceof Struct struct)) {
      return;
    }
    for (Field field : struct.schema().fields()) {
      Object raw = struct.get(field);
      if (dropToastPlaceholder && isToastPlaceholder(raw)) {
        continue; // unchanged TOAST value: omit so the existing target value is kept
      }
      names.add(field.name());
      values.add(DebeziumTypeConverter.convert(field.schema().name(), raw));
    }
  }

  /**
   * True when a raw after-image value is Debezium's TOAST unavailable-value placeholder --
   * as the sentinel string (text/varchar/char/json/jsonb columns) or its UTF-8 bytes (a
   * bytea column). Checked on the RAW value BEFORE type conversion so a {@code json}
   * sentinel is caught before it would be wrapped in a {@code PGobject}. Only consulted for
   * a PostgreSQL source (see {@link #extractAfterImage}), so a MySQL row that happens to
   * carry this exact string is never affected.
   */
  private static boolean isToastPlaceholder(Object raw) {
    if (raw instanceof String s) {
      return TOAST_UNAVAILABLE_PLACEHOLDER.equals(s);
    }
    if (raw instanceof byte[] b) {
      return Arrays.equals(b, TOAST_UNAVAILABLE_PLACEHOLDER_BYTES);
    }
    return false;
  }

  /**
   * Resolve the schema-qualified target table ({@code schema.table}).
   *
   * <p>Debezium's {@code source} block carries the namespace separately from the
   * table: a MySQL source puts the database (which is the schema) in {@code db},
   * a PostgreSQL-style source in {@code schema}. The namespace MUST be preserved
   * so a captured {@code cdc_monitor.heartbeat} lands in DSQL's {@code cdc_monitor}
   * schema — the same schema-qualified target the Full Load writes to. Dropping it
   * would silently route streamed changes to {@code public} (DSQL's default
   * {@code search_path}), splitting one table across two schemas and colliding any
   * two source databases that share a table name.
   */
  private static String resolveTable(Struct envelope, String topic) {
    Struct source = optStruct(envelope, "source");
    if (source != null) {
      String table = optString(source, "table");
      if (table != null && !table.isEmpty()) {
        String schema = optString(source, "schema");
        if (schema == null || schema.isEmpty()) {
          schema = optString(source, "db");
        }
        return (schema != null && !schema.isEmpty()) ? schema + "." + table : table;
      }
    }
    return tableFromTopic(topic);
  }

  /**
   * Fall back to the topic name when {@code source.table} is absent. Per-table
   * topics are {@code <prefix>.<db>.<table>} (see cdc-stack.yaml), so the last two
   * dotted segments are {@code db.table} — the schema-qualified target. Keep both;
   * keeping only the last segment would drop the schema (see {@link #resolveTable}).
   */
  private static String tableFromTopic(String topic) {
    if (topic == null || topic.isEmpty()) {
      throw new DataException("Cannot resolve target table: empty topic and no source.table");
    }
    String[] parts = topic.split("\\.");
    if (parts.length >= 2) {
      return parts[parts.length - 2] + "." + parts[parts.length - 1];
    }
    return topic;
  }

  private static String optString(Struct struct, String field) {
    if (struct.schema().field(field) == null) {
      return null;
    }
    Object value = struct.get(field);
    return value == null ? null : value.toString();
  }

  private static Struct optStruct(Struct struct, String field) {
    if (struct.schema().field(field) == null) {
      return null;
    }
    Object value = struct.get(field);
    return value instanceof Struct nested ? nested : null;
  }

  /** Read an epoch-millis long field (e.g. source.ts_ms); 0 when absent/null. Null-safe. */
  private static long optLong(Struct struct, String field) {
    if (struct == null || struct.schema().field(field) == null) {
      return 0L;
    }
    Object value = struct.get(field);
    return value instanceof Number number ? number.longValue() : 0L;
  }
}
