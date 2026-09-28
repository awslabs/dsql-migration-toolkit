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
import org.apache.kafka.connect.data.Schema;
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
 * (falling back to the before-image when the message has no key); {@code t} (a source
 * TRUNCATE) has no correct apply on Aurora DSQL and is dead-lettered by name (see
 * {@link #truncateNotApplicable}).
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
    Object value = record.value();
    Struct envelope = (value instanceof Struct) ? (Struct) value : null;
    Struct source = envelope != null ? optStruct(envelope, "source") : null;
    // Determine the origin engine (Debezium's source.connector) EARLY so PostgreSQL-specific
    // value handling applies to the key/before PK extraction too, not just the after-image:
    //   - the TOAST unavailable-value omission (see extractAfterImage), and
    //   - PG bit/varbit rendered as the bit-string text (see convertField / pgBitString).
    // A MySQL -- or unknown/tombstone -- source keeps byte-identical prior behavior (MySQL
    // BIT stays an integer; the sentinel string is only ever real user data there).
    boolean pgSource = source != null && "postgresql".equals(optString(source, "connector"));

    List<String> pkColumns = new ArrayList<>();
    List<Object> pkValues = new ArrayList<>();
    extractStruct(record.key(), pkColumns, pkValues, pgSource);

    if (value == null) {
      // Tombstone -> delete by key. No envelope, so no source.ts_ms (lag unknown). Flagged
      // as a tombstone so the applied-ops metric counts one source DELETE ONCE: Debezium
      // emits an op=d envelope AND a tombstone for the same row, and counting both reported
      // DeletesApplied=2 for one deleted row. The APPLY is unchanged -- still an idempotent
      // DELETE by key.
      return buildTombstone(tableFromTopic(record.topic()), pkColumns, pkValues);
    }
    if (envelope == null) {
      throw new DataException(
          "Unsupported record value type; expected a Debezium Struct envelope");
    }

    String op = optString(envelope, "op");
    String table = resolveTable(envelope, record.topic());
    // Source commit time for the end-to-end replication-lag metric (now - ts at
    // apply). 0 when the source block omits ts_ms -> lag simply not recorded.
    long sourceTsMs = optLong(source, "ts_ms");
    Struct after = optStruct(envelope, "after");

    // A source TRUNCATE, BEFORE the after-image branch below -- it has no row image at all,
    // so it would otherwise be mistaken for a delete. Verified against the SHIPPED jars
    // (connectors/plugins/debezium-postgres-plugin.zip): the op code is "t"
    // (io.debezium.data.Envelope$Operation.TRUNCATE in debezium-core-2.7.4.Final.jar),
    // Envelope.truncate(source, ts) sets ONLY op/source/ts_ms/ts_us/ts_ns -- so both
    // before and after are null -- and PostgresChangeRecordEmitter.emitTruncateRecord passes
    // a null key, which EventDispatcher$StreamingChangeRecordReceiver turns into a record
    // with a null keySchema AND a null key. pgoutput emits ONE such record per truncated
    // table on that table's own topic, so `table` above already names the diverged table.
    //
    // Without this branch the record fell through `after == null` into buildDelete and
    // dead-lettered as "no primary key in record key or before-image": loud, but naming the
    // wrong problem, so the operator could not tell a TRUNCATE from a broken REPLICA
    // IDENTITY. Unreachable unless the source connector is configured to emit truncates
    // (Debezium's `skipped.operations` defaults to "t", i.e. truncate skipped), which the
    // cdc-stack does only for the PostgreSQL source -- so this is inert for MySQL.
    if ("t".equals(op)) {
      throw truncateNotApplicable(table);
    }

    if ("d".equals(op) || after == null) {
      if (pkColumns.isEmpty()) {
        // No message key: fall back to the before-image for the PK.
        Struct before = optStruct(envelope, "before");
        if (before != null) {
          extractStruct(before, pkColumns, pkValues, pgSource);
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

  /**
   * The named dead-letter for a source {@code TRUNCATE} (op {@code t}).
   *
   * <p>Aurora DSQL has no {@code TRUNCATE} statement, so there is NO correct apply -- and the
   * sink must not invent one: a blanket {@code DELETE FROM <table>} is unbounded (the 3,000
   * row / 10 MiB per-transaction limits make it unexecutable on a large table) and would
   * destroy target rows on the strength of a single un-keyed event. So the event is
   * quarantined with its cause named, which is what the operator actually needs: the
   * divergence is already created (the source dropped its rows, the target kept them) and the
   * remedy is per-TABLE, not per-row.
   *
   * <p>The wording is load-bearing in two directions. It states what happened, that no apply
   * exists, which way source and target now differ, and the recovery -- and it is also the
   * string the control plane classifies on: {@code cdc_dlq.parse_dlq_log_message} matches
   * "Cannot apply a source TRUNCATE" to assign the synthetic {@code SOURCE_TRUNCATE} code
   * (there is no SQLSTATE -- nothing was sent to DSQL), which
   * {@code cdc.classify_schema_drift} maps to the {@code source-truncate} banner kind. Keep
   * the leading phrase byte-identical if this message is ever reworded.
   */
  private static DataException truncateNotApplicable(String table) {
    return new DataException(
        "Cannot apply a source TRUNCATE of table "
            + table
            + ": Aurora DSQL has no TRUNCATE statement, so there is no correct apply and this"
            + " event is quarantined rather than guessed at. The target still holds the rows"
            + " the source truncated, so the two now differ by exactly those rows (Validation"
            + " reports them as extra). Recovery: reload this table on the Full Load step with"
            + " 'Drop & reload', which recreates it and re-reads the source -- an upsert-only"
            + " reload cannot remove rows the source no longer has.");
  }

  private static ChangeEvent buildDelete(
      String table, List<String> pkColumns, List<Object> pkValues, long sourceTsMs) {
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build DELETE for table " + table + ": no primary key in record key or before-image");
    }
    requireNoNullKeyComponent(table, pkColumns, pkValues, "DELETE");
    return ChangeEvent.delete(table, pkColumns, pkValues, sourceTsMs);
  }

  /**
   * Refuse a delete whose key has a NULL component, instead of applying it to zero rows.
   *
   * <p>The emptiness checks above ask "is there a key at all"; this asks whether the key can
   * actually locate a row. The delete SQL renders {@code "col" = ?} per component, so a NULL
   * makes the predicate UNKNOWN and the statement removes nothing -- and the affected-row
   * count is not inspected, so the record was acknowledged, its offset committed, and the
   * delete lost with no error, no log and no dead-letter. Live-observed on a PostgreSQL
   * source: a deleted row stayed on the target while rows from the same transaction that were
   * keyed on the source primary key deleted correctly.
   *
   * <p>The cause is upstream of the sink -- a table re-keyed onto the target primary key whose
   * source {@code REPLICA IDENTITY} is DEFAULT, so the added key column is absent from the
   * DELETE before-image -- and the tool now blocks that configuration in its prerequisites.
   * This is the backstop: a key the sink cannot use must be LOUD (dead-lettered, like any
   * other unusable record) rather than silently dropped, because a silent drop is
   * indistinguishable from success and the operator has no way to find it.
   */
  private static void requireNoNullKeyComponent(
      String table, List<String> pkColumns, List<Object> pkValues, String opName) {
    for (int i = 0; i < pkColumns.size(); i++) {
      if (i >= pkValues.size() || pkValues.get(i) == null) {
        throw new DataException(
            "Cannot build "
                + opName
                + " for table "
                + table
                + ": key column '"
                + pkColumns.get(i)
                + "' is null, so the target row cannot be located and the "
                + opName
                + " would be applied to 0 rows. On a PostgreSQL source this means the"
                + " UPDATE/DELETE before-image does not carry that column: run"
                + " ALTER TABLE "
                + table
                + " REPLICA IDENTITY FULL.");
      }
    }
  }

  private static ChangeEvent buildTombstone(
      String table, List<String> pkColumns, List<Object> pkValues) {
    if (pkColumns.isEmpty()) {
      throw new DataException(
          "Cannot build DELETE for table "
              + table
              + ": no primary key in record key or before-image");
    }
    requireNoNullKeyComponent(table, pkColumns, pkValues, "DELETE");
    return ChangeEvent.tombstone(table, pkColumns, pkValues, 0L);
  }

  /**
   * Read one field's RAW value without letting the schema's default stand in for a NULL.
   *
   * <p>{@code Struct.get(Field)} SUBSTITUTES the schema's default value when the slot is
   * null (verified in connect-api 3.7.0's bytecode), and Debezium's
   * {@code TableSchemaBuilder.addField} sets that default from the SOURCE COLUMN's own
   * {@code DEFAULT} clause. So for any nullable column that has a default, a genuine source
   * NULL arrived here as the default and was written to the target as though the row held
   * it -- with no error, no dead letter and no log line. Live-reproduced on Aurora
   * PostgreSQL 17.7: a row whose column IS NULL landed on Aurora DSQL holding the default.
   * This affected BOTH source engines.
   *
   * <p>{@code getWithoutDefault} returns the slot verbatim. It is the correct reading for a
   * Debezium sink because the record and its schema always come from the same schema
   * version, so a null slot means the source value genuinely IS NULL -- and Debezium puts
   * the real value even when it happens to EQUAL the default, so nothing that used to work
   * starts failing. It cannot throw for a field obtained from {@code struct.schema()
   * .fields()}, which is the only way this is called.
   *
   * <p>This also fixes the KEY path. A re-keyed table's key schema carries the same source
   * default, so {@code Struct.get} fabricated a non-NULL key component out of a before-image
   * that never had one: {@link #requireNoNullKeyComponent} could never fire, and the DELETE
   * went out with an invented key, applied to 0 rows and was acknowledged -- the source row
   * gone, the target's copy kept. Reading the slot verbatim restores the NULL, so that
   * existing guard fires and dead-letters with its already-actionable message.
   */
  private static Object rawFieldValue(Struct struct, Field field) {
    return struct.getWithoutDefault(field.name());
  }

  private static void extractStruct(
      Object maybeStruct, List<String> names, List<Object> values, boolean pgSource) {
    if (!(maybeStruct instanceof Struct struct)) {
      return;
    }
    for (Field field : struct.schema().fields()) {
      names.add(field.name());
      // Convert the Debezium-encoded value to its canonical DSQL-target form
      // (e.g. MicroTimestamp Long -> java.sql.Timestamp) so it matches the Full
      // Load bulk loader's encoding before it is bound.
      values.add(convertField(field, rawFieldValue(struct, field), pgSource));
    }
  }

  /**
   * Convert one field value to its DSQL-target form. Delegates to
   * {@link DebeziumTypeConverter#convert(String, Object)} except for a PostgreSQL source's
   * {@code io.debezium.data.Bits} (bit/varbit), which PostgreSQL treats as a bit STRING and
   * the Full Load path writes as such -- so it is rendered via
   * {@link DebeziumTypeConverter#pgBitString} (using the field schema's declared length)
   * rather than the MySQL-BIT integer mapping. MySQL Bits keeps the integer mapping.
   */
  private static Object convertField(Field field, Object raw, boolean pgSource) {
    Schema fieldSchema = field.schema();
    if (pgSource && DebeziumTypeConverter.BITS_TYPE.equals(fieldSchema.name())) {
      return DebeziumTypeConverter.pgBitString(raw, fieldSchema);
    }
    // A PostgreSQL ARRAY column. Schema Conversion stores it as jsonb (DSQL has no array
    // type) and the Full Load wrote PostgreSQL's own to_jsonb text, so render the same --
    // see DebeziumTypeConverter.pgArrayAsJsonb. Gated on ARRAY rather than on a schema NAME
    // because Debezium builds these with SchemaBuilder.array(...) and never names them,
    // which is precisely why convert() passed the List straight through to setObject and
    // every write for the table dead-lettered with SQLSTATE 07006.
    //
    // MySQL is untouched: its value converters emit no array schema at all (SET becomes a
    // String, JSON becomes io.debezium.data.Json), so this branch is unreachable there --
    // and it is gated on pgSource regardless.
    //
    // The ELEMENT schema (valueSchema()) is passed too: it carries the Debezium logical-type
    // name that decides each element's to_jsonb spelling. Without it the renderer saw only the
    // raw Java class and wrote a bare JSON number for timestamp[]/date[]/time[] where to_jsonb
    // writes a quoted ISO string -- valid jsonb, accepted by DSQL, silently unequal to the
    // Full Load bytes.
    if (pgSource && fieldSchema.type() == Schema.Type.ARRAY && raw != null) {
      return DebeziumTypeConverter.pgArrayAsJsonb(raw, fieldSchema.valueSchema());
    }
    return DebeziumTypeConverter.convert(fieldSchema.name(), raw);
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
      Object raw = rawFieldValue(struct, field);
      if (dropToastPlaceholder && isToastPlaceholder(raw)) {
        continue; // unchanged TOAST value: omit so the existing target value is kept
      }
      names.add(field.name());
      values.add(convertField(field, raw, dropToastPlaceholder));
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
