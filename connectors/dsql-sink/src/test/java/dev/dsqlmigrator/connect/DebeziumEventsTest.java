// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class DebeziumEventsTest {

  private static final Schema KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();

  private static final Schema ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.STRING_SCHEMA)
          .optional()
          .build();

  private static final Schema SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("db", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .optional()
          .build();

  private static final Schema ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("before", ROW)
          .field("after", ROW)
          .field("source", SOURCE)
          .build();

  private static Struct key(long id) {
    return new Struct(KEY).put("id", id);
  }

  private static Struct row(long id, String name) {
    return new Struct(ROW).put("id", id).put("name", name);
  }

  private static Struct source(String table) {
    return new Struct(SOURCE).put("db", "app").put("table", table);
  }

  private static SinkRecord record(Struct key, Struct value, String topic) {
    return new SinkRecord(
        topic,
        0,
        key == null ? null : key.schema(),
        key,
        value == null ? null : value.schema(),
        value,
        0L);
  }

  @Test
  void insertBecomesUpsert() {
    Struct env =
        new Struct(ENVELOPE).put("op", "c").put("after", row(1L, "Alice")).put("source", source("users"));
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.app.users"));
    assertFalse(event.isDelete());
    // Schema-qualified: source.db (MySQL database = schema) is preserved so the
    // change lands in the same schema the Full Load and Schema Conversion target.
    assertEquals("app.users", event.table());
    assertEquals(List.of("id", "name"), event.columns());
    assertEquals(List.of(1L, "Alice"), event.values());
    assertEquals(List.of("id"), event.pkColumns());
    assertEquals(List.of(1L), event.pkValues());
    // op "c" is an insert -> +1 net; classified as an insert for the applied-ops metrics.
    assertEquals(1, event.netRowDelta());
    assertTrue(event.isInsert());
    assertFalse(event.isUpdate());
    assertFalse(event.isDelete());
    // No ts_ms on this source block -> 0 (replication-lag not recorded).
    assertEquals(0L, event.sourceTsMs());
  }

  @Test
  void sourceTsMsParsedForReplicationLag() {
    // source.ts_ms (source commit time) is carried onto the ChangeEvent so the sink
    // can compute end-to-end replication lag = now - source.ts_ms at apply time.
    Schema srcSchema =
        SchemaBuilder.struct()
            .name("Source")
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .optional()
            .build();
    Schema envSchema =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", ROW)
            .field("source", srcSchema)
            .build();
    Struct src =
        new Struct(srcSchema).put("db", "app").put("table", "users").put("ts_ms", 1_700_000_000_000L);
    Struct env = new Struct(envSchema).put("op", "u").put("after", row(1L, "Alice")).put("source", src);
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.app.users"));
    assertEquals(1_700_000_000_000L, event.sourceTsMs());
  }

  @Test
  void updateBecomesUpsert() {
    Struct env =
        new Struct(ENVELOPE).put("op", "u").put("after", row(1L, "Alice2")).put("source", source("users"));
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.app.users"));
    assertFalse(event.isDelete());
    assertEquals(List.of(1L, "Alice2"), event.values());
    // op "u" is an update -> net 0; classified as an update for the applied-ops metrics.
    assertEquals(0, event.netRowDelta());
    assertTrue(event.isUpdate());
    assertFalse(event.isInsert());
  }

  @Test
  void deleteOpBecomesDelete() {
    Struct env =
        new Struct(ENVELOPE).put("op", "d").put("before", row(5L, "Bob")).put("source", source("users"));
    ChangeEvent event = DebeziumEvents.parse(record(key(5L), env, "dsqlcdc.app.users"));
    assertTrue(event.isDelete());
    assertEquals("app.users", event.table());
    assertEquals(List.of("id"), event.pkColumns());
    assertEquals(List.of(5L), event.pkValues());
    // op "d" is a delete -> -1 net; classified as a delete for the applied-ops metrics.
    assertEquals(-1, event.netRowDelta());
    assertTrue(event.isDelete());
    assertFalse(event.isInsert());
    assertFalse(event.isUpdate());
  }

  @Test
  void tombstoneBecomesDeleteWithQualifiedTableFromTopic() {
    // No value (tombstone) -> resolve schema-qualified target from the topic:
    // <prefix>.<db>.<table> = dsqlcdc.app.users -> app.users (schema preserved).
    ChangeEvent event = DebeziumEvents.parse(record(key(7L), null, "dsqlcdc.app.users"));
    assertTrue(event.isDelete());
    assertEquals("app.users", event.table());
    assertEquals(List.of(7L), event.pkValues());
  }

  @Test
  void schemaFieldTakesPrecedenceOverDb() {
    // A PostgreSQL-style source exposes the namespace in source.schema; when both
    // are present, schema wins.
    Schema pgSource =
        SchemaBuilder.struct()
            .name("Source")
            .field("db", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .optional()
            .build();
    Schema pgEnvelope =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", ROW)
            .field("source", pgSource)
            .build();
    Struct src =
        new Struct(pgSource).put("db", "inventory").put("schema", "cdc_monitor").put("table", "heartbeat");
    Struct env = new Struct(pgEnvelope).put("op", "c").put("after", row(1L, "tick")).put("source", src);
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.inventory.heartbeat"));
    assertEquals("cdc_monitor.heartbeat", event.table());
  }

  // --- PostgreSQL TOAST unavailable-value placeholder omission (Phase D) -------

  // A row with a large text column and a bytea column that may carry the TOAST sentinel.
  private static final Schema TOAST_ROW =
      SchemaBuilder.struct()
          .name("ToastRow")
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .field("body", Schema.OPTIONAL_STRING_SCHEMA)
          .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
          .optional()
          .build();

  // A source block carrying the origin engine in source.connector. The TOAST omission is
  // gated on connector == "postgresql", so these tests must supply it.
  private static final Schema ENGINE_SOURCE =
      SchemaBuilder.struct()
          .name("Source")
          .field("connector", Schema.STRING_SCHEMA)
          .field("db", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .optional()
          .build();

  private static final Schema TOAST_ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("after", TOAST_ROW)
          .field("source", ENGINE_SOURCE)
          .build();

  private static Struct engineSource(String connector, String table) {
    return new Struct(ENGINE_SOURCE).put("connector", connector).put("db", "app").put("table", table);
  }

  @Test
  void unchangedToastTextColumnOmittedFromUpsert() {
    // On a PostgreSQL UPDATE, an unchanged TOASTed text column arrives as the sentinel.
    // The sink must OMIT it (so ON CONFLICT DO UPDATE keeps the existing value), not bind
    // the literal "__debezium_unavailable_value" over the real one.
    Struct after =
        new Struct(TOAST_ROW)
            .put("id", 1L)
            .put("name", "Alice")
            .put("body", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER) // unchanged TOAST value
            .put("blob", null);
    Struct env =
        new Struct(TOAST_ENVELOPE)
            .put("op", "u")
            .put("after", after)
            .put("source", engineSource("postgresql", "docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.app.docs"));

    // "body" is dropped; the changed columns remain (id + name + the explicit NULL blob).
    assertFalse(event.columns().contains("body"), "sentinel TOAST column omitted");
    assertEquals(List.of("id", "name", "blob"), event.columns());
    assertEquals(List.of(1L, "Alice"), List.of(event.values().get(0), event.values().get(1)));
    assertEquals(List.of("id"), event.pkColumns()); // conflict target still the PK
  }

  @Test
  void unchangedToastByteaColumnOmittedFromUpsert() {
    // A bytea column carries the sentinel as its UTF-8 bytes; it too must be omitted.
    byte[] sentinelBytes =
        DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    Struct after =
        new Struct(TOAST_ROW)
            .put("id", 2L)
            .put("name", "Bob")
            .put("body", "real text")
            .put("blob", sentinelBytes); // unchanged TOAST bytea
    Struct env =
        new Struct(TOAST_ENVELOPE)
            .put("op", "u")
            .put("after", after)
            .put("source", engineSource("postgresql", "docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(2L), env, "dsqlcdc.app.docs"));

    assertFalse(event.columns().contains("blob"), "sentinel bytea column omitted");
    assertEquals(List.of("id", "name", "body"), event.columns());
  }

  @Test
  void realValuesEqualToPlaceholderStringAreNotFalselyOmitted() {
    // A genuine value that merely resembles the sentinel prefix is retained (only the EXACT
    // sentinel is dropped), and a normal update keeps every column.
    Struct after =
        new Struct(TOAST_ROW)
            .put("id", 3L)
            .put("name", "__debezium_unavailable_value_but_real") // not the exact sentinel
            .put("body", "hello")
            .put("blob", new byte[] {0x01, 0x02});
    Struct env =
        new Struct(TOAST_ENVELOPE)
            .put("op", "u")
            .put("after", after)
            .put("source", engineSource("postgresql", "docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(3L), env, "dsqlcdc.app.docs"));
    assertEquals(List.of("id", "name", "body", "blob"), event.columns());
  }

  @Test
  void mysqlSourceNeverDropsAColumnEqualToTheSentinel() {
    // Byte-identical guard: the TOAST omission is gated on a PostgreSQL source. A MySQL row
    // whose text (or bytea) value EXACTLY equals the Debezium sentinel is genuine user data
    // (MySQL has no TOAST), so it MUST be retained and bound verbatim -- both on op=c and
    // op=u -- exactly as the pre-Phase-D path did.
    byte[] sentinelBytes =
        DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    for (String op : new String[] {"c", "u"}) {
      Struct after =
          new Struct(TOAST_ROW)
              .put("id", 9L)
              .put("name", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER) // genuine MySQL value
              .put("body", "text")
              .put("blob", sentinelBytes); // genuine MySQL bytes
      Struct env =
          new Struct(TOAST_ENVELOPE)
              .put("op", op)
              .put("after", after)
              .put("source", engineSource("mysql", "docs"));
      ChangeEvent event = DebeziumEvents.parse(record(key(9L), env, "dsqlcdc.app.docs"));
      assertEquals(List.of("id", "name", "body", "blob"), event.columns(), "op=" + op);
      assertEquals(DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER, event.values().get(1), "op=" + op);
    }
  }

  @Test
  void unqualifiedWhenSourceHasNoSchemaOrDb() {
    // Defensive: a source block with only a table name yields the bare table
    // (DSQL resolves it against the connection search_path).
    Schema bareSource =
        SchemaBuilder.struct().name("Source").field("table", Schema.STRING_SCHEMA).optional().build();
    Schema bareEnvelope =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", ROW)
            .field("source", bareSource)
            .build();
    Struct src = new Struct(bareSource).put("table", "users");
    Struct env = new Struct(bareEnvelope).put("op", "c").put("after", row(1L, "Alice")).put("source", src);
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "users"));
    assertEquals("users", event.table());
  }
}
