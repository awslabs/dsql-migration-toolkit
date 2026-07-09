// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * Maps a Debezium envelope SinkRecord to a ChangeEvent. Offline unit-tested
 * with synthetic Connect Structs; the exact envelope shape produced by the
 * deployed Debezium + Glue converter config is confirmed in the spike.
 */
package dev.dsqlmigrator.connect;

import java.util.ArrayList;
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

  private DebeziumEvents() {}

  static ChangeEvent parse(SinkRecord record) {
    List<String> pkColumns = new ArrayList<>();
    List<Object> pkValues = new ArrayList<>();
    extractStruct(record.key(), pkColumns, pkValues);

    Object value = record.value();
    if (value == null) {
      // Tombstone -> delete by key.
      return buildDelete(tableFromTopic(record.topic()), pkColumns, pkValues);
    }
    if (!(value instanceof Struct envelope)) {
      throw new DataException(
          "Unsupported record value type; expected a Debezium Struct envelope");
    }

    String op = optString(envelope, "op");
    String table = resolveTable(envelope, record.topic());
    Struct after = optStruct(envelope, "after");

    if ("d".equals(op) || after == null) {
      if (pkColumns.isEmpty()) {
        // No message key: fall back to the before-image for the PK.
        Struct before = optStruct(envelope, "before");
        if (before != null) {
          extractStruct(before, pkColumns, pkValues);
        }
      }
      return buildDelete(table, pkColumns, pkValues);
    }

    List<String> columns = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    extractStruct(after, columns, values);
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build upsert for table " + table + ": record has no key (pk) fields");
    }
    return ChangeEvent.upsert(table, columns, values, pkColumns, pkValues);
  }

  private static ChangeEvent buildDelete(
      String table, List<String> pkColumns, List<Object> pkValues) {
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build DELETE for table " + table + ": no primary key in record key or before-image");
    }
    return ChangeEvent.delete(table, pkColumns, pkValues);
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
}
