// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

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
  void aValueTheConverterCannotReadIsDeadLetteredNotATaskKiller() {
    // put() dead-letters a DataException; anything else escaped it and killed the task.
    Schema rowSchema =
        SchemaBuilder.struct()
            .name("Row")
            .field("id", Schema.INT64_SCHEMA)
            .field(
                "valid_to",
                SchemaBuilder.string().name(DebeziumTypeConverter.ZONED_TIMESTAMP).optional().build())
            .optional()
            .build();
    Schema envSchema =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", rowSchema)
            .field("source", SOURCE)
            .build();
    Struct current =
        new Struct(envSchema)
            .put("op", "c")
            .put("after", new Struct(rowSchema).put("id", 1L).put("valid_to", "infinity"))
            .put("source", source("periods"));
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), current, "dsqlcdc.app.periods"));
    assertEquals("infinity", assertInstanceOf(PGobject.class, event.values().get(1)).getValue());
    Struct garbled =
        new Struct(envSchema)
            .put("op", "c")
            .put("after", new Struct(rowSchema).put("id", 2L).put("valid_to", "not a time"))
            .put("source", source("periods"));
    DataException e =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(record(key(2L), garbled, "dsqlcdc.app.periods")));
    assertTrue(e.getMessage().contains("valid_to"), e.getMessage());
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
    // Flagged as a tombstone: it APPLIES like any delete, but Debezium sends an op=d
    // envelope AND a tombstone for one source DELETE, so counting both reported
    // DeletesApplied=2 for a single deleted row. The flag is what lets the metric skip it
    // without changing the apply.
    assertTrue(event.isTombstone());
  }

  @Test
  void anOpDeleteIsNotATombstone() {
    // The negative control for the flag: without it, excluding tombstones from the metric
    // would also exclude real deletes and DeletesApplied would report 0 instead of 2.
    Struct env =
        new Struct(ENVELOPE)
            .put("op", "d")
            .put("before", row(5L, "Bob"))
            .put("source", source("users"));
    ChangeEvent event = DebeziumEvents.parse(record(key(5L), env, "dsqlcdc.app.users"));
    assertTrue(event.isDelete());
    assertFalse(event.isTombstone());
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

  // --- Tier-3 TOAST regression guards -----------------------------------------

  @Test
  void multiToastBothStringAndByteaColumnsDropped() {
    // A single UPDATE can leave MULTIPLE TOASTed columns unchanged: a text sentinel AND a
    // bytea sentinel in one row -> BOTH omitted, the genuinely-changed column retained.
    byte[] sentinelBytes =
        DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    Struct after = new Struct(TOAST_ROW).put("id", 1L)
        .put("name", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER)  // text sentinel
        .put("body", "real")
        .put("blob", sentinelBytes);                                // bytea sentinel
    Struct env = new Struct(TOAST_ENVELOPE)
        .put("op", "u").put("after", after).put("source", engineSource("postgresql", "docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(1L), env, "dsqlcdc.app.docs"));
    assertEquals(List.of("id", "body"), event.columns());  // both sentinels dropped
  }

  @Test
  void allNonPkToastSentinelsYieldDoNothingUpsert() {
    // When EVERY non-PK column is an unchanged TOAST value, only the PK survives -> the
    // upsert must render DO NOTHING (idempotent no-op), never an empty/invalid SET.
    byte[] sentinelBytes =
        DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    Struct after = new Struct(TOAST_ROW).put("id", 2L)
        .put("name", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER)
        .put("body", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER)
        .put("blob", sentinelBytes);
    Struct env = new Struct(TOAST_ENVELOPE)
        .put("op", "u").put("after", after).put("source", engineSource("postgresql", "docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(2L), env, "dsqlcdc.app.docs"));
    assertEquals(event.pkColumns(), event.columns());  // only the PK remains
    String sql = DsqlDialect.upsertSql(event.table(), event.columns(), event.pkColumns());
    assertTrue(sql.trim().endsWith("DO NOTHING"), sql);
  }

  @Test
  void pgSourceBitColumnRendersBitStringWhileMysqlKeepsInteger() {
    // Live DLQ-test finding: a PG bit/varbit column must land as the bit-string (matching
    // Full Load's psycopg write), not the MySQL-BIT integer -- gated on source.connector so
    // MySQL stays byte-identical (BIT -> integer). B'11011011' = little-endian {0xDB}.
    Schema bits =
        SchemaBuilder.bytes().name(DebeziumTypeConverter.BITS_TYPE)
            .parameter("length", "8").optional().build();
    Schema row =
        SchemaBuilder.struct().name("Row")
            .field("id", Schema.INT64_SCHEMA).field("flags", bits).optional().build();
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA).field("after", row).field("source", ENGINE_SOURCE)
            .build();
    byte[] db = new byte[] {(byte) 0xDB};

    Struct pgEnv =
        new Struct(env).put("op", "c")
            .put("after", new Struct(row).put("id", 1L).put("flags", db))
            .put("source", engineSource("postgresql", "t"));
    ChangeEvent pg = DebeziumEvents.parse(record(key(1L), pgEnv, "dsqlcdc.app.t"));
    assertEquals("11011011", pg.values().get(pg.columns().indexOf("flags")));

    Struct myEnv =
        new Struct(env).put("op", "c")
            .put("after", new Struct(row).put("id", 1L).put("flags", db))
            .put("source", engineSource("mysql", "t"));
    ChangeEvent my = DebeziumEvents.parse(record(key(1L), myEnv, "dsqlcdc.app.t"));
    assertEquals(219L, my.values().get(my.columns().indexOf("flags")));
  }

  @Test
  void absentConnectorRetainsSentinelColumn() {
    // No source.connector -> not a known-PostgreSQL source -> the TOAST omit is NOT applied
    // (safe default). A value equal to the sentinel is genuine data and MUST be retained.
    Schema envNoConn = SchemaBuilder.struct().name("Envelope")
        .field("op", Schema.STRING_SCHEMA).field("after", TOAST_ROW).field("source", SOURCE).build();
    Struct after = new Struct(TOAST_ROW).put("id", 3L)
        .put("name", DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER).put("body", "x").put("blob", null);
    Struct env = new Struct(envNoConn).put("op", "u").put("after", after).put("source", source("docs"));
    ChangeEvent event = DebeziumEvents.parse(record(key(3L), env, "dsqlcdc.app.docs"));
    assertTrue(event.columns().contains("name"));  // NOT dropped for a non-PG source
    assertEquals(
        DebeziumEvents.TOAST_UNAVAILABLE_PLACEHOLDER,
        event.values().get(event.columns().indexOf("name")));
  }

  private static final Schema COMPOSITE_KEY =
      SchemaBuilder.struct()
          .name("Key")
          .field("user_id", Schema.OPTIONAL_INT64_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .build();

  @Test
  void aDeleteWithANullKeyComponentIsRejectedRatherThanAppliedToZeroRows() {
    // A NULLABLE key component that arrives null (COMPOSITE_KEY makes user_id OPTIONAL). The
    // delete SQL renders "user_id" = ? AND "id" = ?, and a NULL makes that predicate UNKNOWN
    // -- the statement removes nothing, and the affected-row count is not inspected, so
    // without this guard the delete would be lost with no error, no log and no dead-letter.
    // NOT the shape a REPLICA IDENTITY DEFAULT before-image produces for a composite key's
    // NOT NULL leading column: Debezium puts a placeholder there instead (see
    // aRequiredRekeyColumnHoldingDebeziumsPlaceholderIsNotDetectedBySink).
    Struct k = new Struct(COMPOSITE_KEY).put("user_id", null).put("id", 510L);
    Struct env =
        new Struct(ENVELOPE)
            .put("op", "d")
            .put("before", row(510L, "Bob"))
            .put("source", source("orders"));
    DataException thrown =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(record(k, env, "dsqlcdc.app.orders")));
    // The message has to name the column AND the remedy: the cause is on the source, so an
    // operator reading only the dead-letter must still know what to change.
    assertTrue(thrown.getMessage().contains("user_id"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("0 rows"), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("REPLICA IDENTITY FULL"), thrown.getMessage());
  }

  @Test
  void aTombstoneWithANullKeyComponentIsAlsoRejected() {
    // Debezium sends a tombstone alongside the op=d envelope for one source DELETE, and it
    // applies like any delete -- so the same null key would be the same silent no-op.
    Struct k = new Struct(COMPOSITE_KEY).put("user_id", null).put("id", 510L);
    assertThrows(
        DataException.class, () -> DebeziumEvents.parse(record(k, null, "dsqlcdc.app.orders")));
  }

  @Test
  void aRequiredRekeyColumnHoldingDebeziumsPlaceholderIsNotDetectedBySink() {
    // CHARACTERIZATION (G2), pinning what the sink CANNOT see on a record keyed on the
    // COMPOSITE key -- infrastructure deployed before sink plugin v51, or an in-flight record
    // of it. The real shape of such a table's DELETE under PostgreSQL REPLICA IDENTITY DEFAULT,
    // live-confirmed on PostgreSQL 17 (2026-10-03): the composite key's leading column is NOT
    // NULL on the source, so its key field is REQUIRED, and Debezium's key generator converts
    // the absent before-image value with no null check -- the type's placeholder (0 here; ''
    // for text) or the column's DEFAULT (the live run carried 7, its DEFAULT, for a row whose
    // lead was 5). The key is therefore non-null, requireNoNullKeyComponent cannot fire, and
    // the sink builds DELETE ... WHERE user_id = 0 AND id = 510, which removes nothing --
    // silently, nothing dead-lettered. This legacy parse is unchanged on purpose: since G15 a
    // table listed in target.key.columns is keyed on the SOURCE primary key instead (see the
    // source-keyed tests below), and the control plane's Start prerequisite and runtime
    // REPLICA IDENTITY re-check still guard both shapes. If this test ever starts failing
    // because the legacy path changed, update that design (and the docs) deliberately.
    Schema requiredKey =
        SchemaBuilder.struct()
            .name("Key")
            .field("user_id", Schema.INT64_SCHEMA)
            .field("id", Schema.INT64_SCHEMA)
            .build();
    Struct k = new Struct(requiredKey).put("user_id", 0L).put("id", 510L);
    Struct env =
        new Struct(ENVELOPE)
            .put("op", "d")
            .put("before", row(510L, "Bob"))
            .put("source", source("orders"));
    ChangeEvent event = DebeziumEvents.parse(record(k, env, "dsqlcdc.app.orders"));
    assertTrue(event.isDelete());
    assertEquals(List.of("user_id", "id"), event.pkColumns());
    assertEquals(List.of(0L, 510L), event.pkValues());
    // ...and its tombstone the same way.
    ChangeEvent tombstone = DebeziumEvents.parse(record(k, null, "dsqlcdc.app.orders"));
    assertTrue(tombstone.isTombstone());
    assertEquals(List.of(0L, 510L), tombstone.pkValues());
  }

  @Test
  void aCompositeKeyWithEveryComponentPresentStillDeletes() {
    // The negative control: the guard must reject a NULL component, not composite keys.
    Struct k = new Struct(COMPOSITE_KEY).put("user_id", 42L).put("id", 510L);
    Struct env =
        new Struct(ENVELOPE)
            .put("op", "d")
            .put("before", row(510L, "Bob"))
            .put("source", source("orders"));
    ChangeEvent event = DebeziumEvents.parse(record(k, env, "dsqlcdc.app.orders"));
    assertTrue(event.isDelete());
    assertEquals(List.of("user_id", "id"), event.pkColumns());
    assertEquals(List.of(42L, 510L), event.pkValues());
  }

  @Test
  void pgArrayColumnBindsAsJsonbWhileMysqlIsUntouched() {
    // THE live defect: Debezium builds array schemas with SchemaBuilder.array(...) and never
    // NAMES them, so the name-keyed converter returned the List untouched and setObject handed
    // pgjdbc a type it cannot map -- SQLSTATE 07006, permanent, so the record dead-lettered
    // and the offset committed past it. Because an after-image carries every column, an UPDATE
    // touching only an unrelated column died with it: five products.price updates were lost on
    // a live Aurora PostgreSQL 17.7 run while the table still read
    // src=15 tgt=15 missing=0 extra=0 -- an update that never lands changes no row count.
    //
    // The BYTES matter as much as the binding. The Full Load converts on the SOURCE in SQL
    // (CAST(to_jsonb(col) AS text)), so DSQL already holds PostgreSQL's own to_jsonb output --
    // verified live: ["audio", "wireless", "anc"], with a space after each comma. Render it any
    // other way and Validation's checksum mismatches every CDC-written row.
    Schema tags = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    Schema row =
        SchemaBuilder.struct().name("Row")
            .field("id", Schema.INT64_SCHEMA).field("tags", tags).optional().build();
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA).field("after", row).field("source", ENGINE_SOURCE)
            .build();
    java.util.List<String> value = java.util.List.of("audio", "wireless", "anc");

    Struct pgEnv =
        new Struct(env).put("op", "u")
            .put("after", new Struct(row).put("id", 5L).put("tags", value))
            .put("source", engineSource("postgresql", "products"));
    ChangeEvent pg = DebeziumEvents.parse(record(key(5L), pgEnv, "dsqlcdc.app.products"));
    Object bound = pg.values().get(pg.columns().indexOf("tags"));
    PGobject asJsonb =
        assertInstanceOf(PGobject.class, bound, "a List cannot be bound by setObject");
    assertEquals("jsonb", asJsonb.getType(), "the target column is jsonb (DSQL has no arrays)");
    assertEquals("[\"audio\", \"wireless\", \"anc\"]", asJsonb.getValue());

    // MySQL is provably untouched: its value converters emit no array schema at all (SET
    // becomes a String, JSON becomes io.debezium.data.Json), so this branch is unreachable
    // there -- and it is gated on the source connector regardless.
    Struct myEnv =
        new Struct(env).put("op", "u")
            .put("after", new Struct(row).put("id", 5L).put("tags", value))
            .put("source", engineSource("mysql", "products"));
    ChangeEvent my = DebeziumEvents.parse(record(key(5L), myEnv, "dsqlcdc.app.products"));
    assertEquals(value, my.values().get(my.columns().indexOf("tags")));
  }

  @Test
  void pgArrayElementSchemaReachesTheJsonbRenderer() {
    // The plumbing that made the temporal fix possible: convertField must pass the ARRAY schema's
    // valueSchema() -- the ELEMENT schema, carrying the Debezium logical-type name -- to the
    // renderer. Without it the renderer saw only a raw Long and wrote a bare JSON number
    // ([1767323045678000]) where to_jsonb writes a quoted ISO string. That was SILENT: valid
    // jsonb, accepted by DSQL, and unequal to the Full Load bytes on every CDC-written row.
    Schema microTimestamp =
        new SchemaBuilder(Schema.Type.INT64)
            .name(DebeziumTypeConverter.MICRO_TIMESTAMP)
            .optional()
            .build();
    Schema seen = SchemaBuilder.array(microTimestamp).optional().build();
    Schema row =
        SchemaBuilder.struct().name("Row")
            .field("id", Schema.INT64_SCHEMA).field("seen_at", seen).optional().build();
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA).field("after", row).field("source", ENGINE_SOURCE)
            .build();
    Struct pgEnv =
        new Struct(env).put("op", "c")
            .put(
                "after",
                new Struct(row).put("id", 9L).put("seen_at", java.util.List.of(1767323045678000L)))
            .put("source", engineSource("postgresql", "visits"));
    ChangeEvent pg = DebeziumEvents.parse(record(key(9L), pgEnv, "dsqlcdc.app.visits"));
    PGobject bound =
        assertInstanceOf(PGobject.class, pg.values().get(pg.columns().indexOf("seen_at")));
    // Measured on live PostgreSQL 17.11:
    //   to_jsonb(ARRAY['2026-01-02 03:04:05.678'::timestamp]) -> ["2026-01-02T03:04:05.678"]
    assertEquals("[\"2026-01-02T03:04:05.678\"]", bound.getValue());
  }

  @Test
  void aSourceNullIsBoundAsNullEvenWhenTheSchemaCarriesAColumnDefault() {
    // Struct.get(Field) SUBSTITUTES the schema's default for a null slot (connect-api
    // 3.7.0), and Debezium sets that default from the SOURCE COLUMN's own DEFAULT clause.
    // So a genuine source NULL was written to the target as the default -- silently, with no
    // error, no dead letter and no log line. Live-reproduced on Aurora PostgreSQL 17.7: a
    // row whose column IS NULL landed on Aurora DSQL holding the default. BOTH engines.
    Schema defaulted =
        SchemaBuilder.struct()
            .name("Row")
            .field("id", Schema.INT64_SCHEMA)
            .field("n", SchemaBuilder.int32().optional().defaultValue(7).build())
            .optional()
            .build();
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", defaulted)
            .field("after", defaulted)
            .field("source", SOURCE)
            .build();
    // The slot is left unset: exactly what Debezium produces for a source NULL.
    Struct after = new Struct(defaulted).put("id", 1L);
    Struct value =
        new Struct(env).put("op", "c").put("after", after).put("source", source("t"));

    ChangeEvent event = DebeziumEvents.parse(record(key(1L), value, "srv.app.t"));

    int n = event.columns().indexOf("n");
    assertTrue(n >= 0, "the column must still be part of the upsert");
    org.junit.jupiter.api.Assertions.assertNull(
        event.values().get(n), "a source NULL must bind NULL, not the schema default");
    // Sanity: the sibling column is unaffected, so this is not a blanket null-out.
    assertEquals(1L, event.values().get(event.columns().indexOf("id")));
  }

  @Test
  void aDefaultedKeyColumnCannotFabricateADeleteKey() {
    // The other half of the same defect, on the KEY path. A re-keyed table's KEY schema
    // carries the source column's DEFAULT too, so Struct.get invented a non-NULL key
    // component from a before-image that never had one: requireNoNullKeyComponent could
    // NEVER fire, the DELETE went out with a fabricated key, applied to 0 ROWS, and was
    // acknowledged -- the source row gone and the target's copy kept, with nothing to see.
    Schema defaultedKey =
        SchemaBuilder.struct()
            .name("Key")
            .field("id", Schema.INT64_SCHEMA)
            .field("tenant", SchemaBuilder.string().optional().defaultValue("t0").build())
            .build();
    // `tenant` unset == the before-image did not carry it (REPLICA IDENTITY DEFAULT).
    Struct k = new Struct(defaultedKey).put("id", 1L);
    Struct value =
        new Struct(ENVELOPE).put("op", "d").put("before", row(1L, "x")).put("source", source("t"));

    DataException failure =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(record(k, value, "srv.app.t")));
    // The EXISTING guard now fires, with its already-actionable remedy.
    assertTrue(failure.getMessage().contains("tenant"), failure.getMessage());
    assertTrue(failure.getMessage().contains("REPLICA IDENTITY FULL"), failure.getMessage());
  }

  @Test
  void sourceTruncateIsDeadLetteredByNameNotMistakenForADelete() {
    // Measured live (PostgreSQL 17.7 -> MSK -> DSQL): a source TRUNCATE produced NO record at
    // all, because Debezium's `skipped.operations` defaults to "t". With the PostgreSQL source
    // connector set to `none` the record DOES arrive -- and it has no after-image, no
    // before-image and NO MESSAGE KEY (Envelope.truncate sets only op/source/ts_*;
    // PostgresChangeRecordEmitter.emitTruncateRecord passes a null key). It therefore fell
    // through the `after == null` branch into buildDelete and dead-lettered as
    // "no primary key in record key or before-image" -- loud, but naming the wrong problem.
    // The MySQL source now emits the same shape (see the MySQL test below).
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", ROW)
            .field("after", ROW)
            .field("source", ENGINE_SOURCE)
            .build();
    Struct truncate =
        new Struct(env).put("op", "t").put("source", engineSource("postgresql", "arr_dlq"));
    // No key and no key schema, exactly as the dispatcher builds it for a truncate.
    DataException failure =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(record(null, truncate, "dsqlcdc.app.arr_dlq")));
    String message = failure.getMessage();
    // The leading phrase is the control plane's classification anchor (cdc_dlq's
    // _SOURCE_TRUNCATE -> SOURCE_TRUNCATE_CODE -> the `source-truncate` drift banner), and the
    // table is SCHEMA-QUALIFIED from source.schema/source.db like every other event.
    assertTrue(message.startsWith("Cannot apply a source TRUNCATE of table app.arr_dlq"));
    // ...and it must name the four facts an operator cannot recover without.
    assertTrue(message.contains("no TRUNCATE statement"));
    assertTrue(message.contains("still holds the rows"));
    assertTrue(message.contains("Drop & reload"));
    // NOT the delete wording it used to produce.
    assertFalse(message.contains("no primary key"));
  }

  @Test
  void aMysqlSourceTruncateIsDeadLetteredByNameAndRowOpsAreUnchanged() {
    // The cdc-stack's MySQL source now sets `skipped.operations: none` too, so op="t" reaches
    // the sink from MySQL. Shape, verified offline against the shipped
    // debezium-connector-binlog 2.7.4 jar (BinlogStreamingChangeEventSource.handleQueryEvent ->
    // BinlogChangeRecordEmitter.emitTruncateRecord): null key and key schema, no before/after,
    // and a source block with connector=mysql and db/table -- MySQL's source block has NO
    // `schema` field, so the table must qualify from source.db.
    Schema env =
        SchemaBuilder.struct().name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", ROW)
            .field("after", ROW)
            .field("source", ENGINE_SOURCE)
            .build();
    Struct truncate =
        new Struct(env)
            .put("op", "t")
            .put(
                "source",
                new Struct(ENGINE_SOURCE)
                    .put("connector", "mysql")
                    .put("db", "migration_schema")
                    .put("table", "g3_trunc"));
    DataException failure =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(record(null, truncate, "dsqlcdc.migration_schema.g3_trunc")));
    String message = failure.getMessage();
    assertTrue(
        message.startsWith("Cannot apply a source TRUNCATE of table migration_schema.g3_trunc"),
        message);
    assertTrue(message.contains("no TRUNCATE statement"), message);
    assertFalse(message.contains("no primary key"), message);

    // Every row op MySQL emits is untouched -- asserted here rather than assumed, because the
    // truncate branch sits on the hot parse path.
    Struct ins =
        new Struct(ENVELOPE).put("op", "c").put("after", row(1L, "Alice")).put("source", source("users"));
    assertTrue(DebeziumEvents.parse(record(key(1L), ins, "dsqlcdc.app.users")).isInsert());
    Struct upd =
        new Struct(ENVELOPE).put("op", "u").put("after", row(1L, "Alicia")).put("source", source("users"));
    assertTrue(DebeziumEvents.parse(record(key(1L), upd, "dsqlcdc.app.users")).isUpdate());
    Struct del =
        new Struct(ENVELOPE).put("op", "d").put("before", row(1L, "Alice")).put("source", source("users"));
    assertTrue(DebeziumEvents.parse(record(key(1L), del, "dsqlcdc.app.users")).isDelete());
    assertTrue(DebeziumEvents.parse(record(key(1L), null, "dsqlcdc.app.users")).isTombstone());
  }

  // --- Source-keyed re-keyed tables (G15) -----------------------------------------------

  private static final java.util.Map<String, List<String>> REKEYED =
      java.util.Map.of("app.orders", List.of("lead", "id"));

  private static final Schema SRC_KEY =
      SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();

  private static final Schema LEAD_ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("lead", Schema.INT64_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();

  private static final Schema LEAD_ENVELOPE =
      SchemaBuilder.struct()
          .name("Envelope")
          .field("op", Schema.STRING_SCHEMA)
          .field("before", LEAD_ROW)
          .field("after", LEAD_ROW)
          .field("source", SOURCE)
          .build();

  private static Struct leadRow(long lead, long id, String name) {
    return new Struct(LEAD_ROW).put("lead", lead).put("id", id).put("name", name);
  }

  private static SinkRecord leadRecord(String op, Struct before, Struct after) {
    Struct env = new Struct(LEAD_ENVELOPE).put("op", op).put("source", source("orders"));
    if (before != null) {
      env.put("before", before);
    }
    if (after != null) {
      env.put("after", after);
    }
    return record(new Struct(SRC_KEY).put("id", 2L), env, "dsqlcdc.app.orders");
  }

  @Test
  void sourceKeyedInsertConflictsOnTheCompositeKey() {
    ChangeEvent event =
        DebeziumEvents.parse(leadRecord("c", null, leadRow(5L, 2L, "a")), REKEYED);
    assertTrue(event.isInsert());
    assertEquals(List.of("lead", "id"), event.pkColumns());
    assertEquals(List.of(5L, 2L), event.pkValues());
    assertEquals(List.of("lead", "id", "name"), event.columns());
    assertEquals(List.of("id"), event.sourceKeyColumns());
    assertEquals(List.of(2L), event.sourceKeyValues());
    assertFalse(event.isMove());
    assertEquals(
        "INSERT INTO \"app\".\"orders\" (\"lead\", \"id\", \"name\") VALUES (?, ?, ?) "
            + "ON CONFLICT (\"lead\", \"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\"",
        DsqlSinkTask.renderSql(event));
    // A snapshot read (op r) is an insert the same way.
    assertTrue(
        DebeziumEvents.parse(leadRecord("r", null, leadRow(5L, 2L, "a")), REKEYED).isInsert());
  }

  @Test
  void leadingColumnUpdateBecomesAMove() {
    ChangeEvent event =
        DebeziumEvents.parse(
            leadRecord("u", leadRow(5L, 2L, "a"), leadRow(9L, 2L, "b")), REKEYED);
    assertTrue(event.isUpdate());
    assertTrue(event.isMove());
    assertEquals(List.of(9L, 2L), event.pkValues());
    assertEquals(List.of(5L, 2L), event.movedFromPkValues());
    ChangeEvent delete = event.moveDelete();
    assertTrue(delete.isDelete());
    assertEquals(List.of("lead", "id"), delete.pkColumns());
    assertEquals(List.of(5L, 2L), delete.pkValues());
    // Keyed on the source key once the unique index on it is valid.
    ChangeEvent bySourceKey = event.withMoveDeleteBySourceKey().moveDelete();
    assertEquals(List.of("id"), bySourceKey.pkColumns());
    assertEquals(List.of(2L), bySourceKey.pkValues());
    assertEquals(
        "DELETE FROM \"app\".\"orders\" WHERE \"id\" = ?", DsqlSinkTask.renderSql(bySourceKey));
  }

  @Test
  void unchangedKeyUpdateIsAPlainUpsert() {
    ChangeEvent event =
        DebeziumEvents.parse(
            leadRecord("u", leadRow(5L, 2L, "a"), leadRow(5L, 2L, "b")), REKEYED);
    assertTrue(event.isUpdate());
    assertFalse(event.isMove());
    assertEquals(List.of(5L, 2L), event.pkValues());

    // Numbers are compared by value: a NUMERIC lead of 5.0 before and 5.00 after is the same
    // key, so it is never turned into a delete + insert.
    Schema decimalRow =
        SchemaBuilder.struct()
            .name("Row")
            .field("lead", org.apache.kafka.connect.data.Decimal.schema(2))
            .field("id", Schema.INT64_SCHEMA)
            .optional()
            .build();
    Schema env =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", decimalRow)
            .field("after", decimalRow)
            .field("source", SOURCE)
            .build();
    Struct value =
        new Struct(env)
            .put("op", "u")
            .put(
                "before",
                new Struct(decimalRow).put("lead", new java.math.BigDecimal("5.0")).put("id", 2L))
            .put(
                "after",
                new Struct(decimalRow).put("lead", new java.math.BigDecimal("5.00")).put("id", 2L))
            .put("source", source("orders"));
    ChangeEvent numeric =
        DebeziumEvents.parse(
            record(new Struct(SRC_KEY).put("id", 2L), value, "dsqlcdc.app.orders"), REKEYED);
    assertFalse(numeric.isMove());
    assertTrue(
        DebeziumEvents.sameKey(
            List.of(new java.math.BigDecimal("5.0"), 2L),
            List.of(new java.math.BigDecimal("5.00"), 2L)));
    assertTrue(
        DebeziumEvents.sameKey(
            List.of((Object) new byte[] {1, 2}), List.of((Object) new byte[] {1, 2})));
    assertFalse(DebeziumEvents.sameKey(List.of(5L, 2L), List.of(9L, 2L)));
  }

  @Test
  void updateWithoutABeforeImageHasAnUnknownOldKey() {
    // PostgreSQL REPLICA IDENTITY DEFAULT sends no old tuple for an UPDATE that leaves the
    // source key alone: whether lead changed cannot be told. It stays an upsert by the
    // after-image's key -- not a move -- until the task keys its delete on the source key.
    ChangeEvent event =
        DebeziumEvents.parse(leadRecord("u", null, leadRow(9L, 2L, "b")), REKEYED);
    assertTrue(event.isUpdate());
    assertFalse(event.isMove());
    assertTrue(event.oldKeyUnknown());
    assertEquals(List.of(9L, 2L), event.pkValues());
    assertEquals(List.of(2L), event.sourceKeyValues());

    // Once the unique index on the source key is valid: a move that replaces the row holding
    // id 2 by the new image -- its delete spares a row that already holds lead 9, which the
    // upsert then updates in place.
    ChangeEvent replace = event.withMoveDeleteBySourceKey();
    assertTrue(replace.isMove());
    assertTrue(replace.isUpdate());
    assertFalse(replace.oldKeyUnknown());
    assertNull(replace.movedFromPkValues());
    assertEquals(List.of(9L, 2L), replace.pkValues());
    ChangeEvent delete = replace.moveDelete();
    assertTrue(delete.isDelete());
    assertEquals(List.of("id"), delete.pkColumns());
    assertEquals(List.of(2L), delete.pkValues());
    assertEquals(List.of("lead"), delete.unlessColumns());
    assertEquals(List.of(9L), delete.unlessValues());
    assertEquals(List.of(2L, 9L), delete.deleteBindValues());
    assertEquals(
        "DELETE FROM \"app\".\"orders\" WHERE \"id\" = ? AND NOT (\"lead\" = ?)",
        DsqlSinkTask.renderSql(delete));
    // A move from a KNOWN old key deletes unconditionally (old != new by construction).
    ChangeEvent known =
        DebeziumEvents.parse(
                leadRecord("u", leadRow(5L, 2L, "a"), leadRow(9L, 2L, "b")), REKEYED)
            .withMoveDeleteBySourceKey()
            .moveDelete();
    assertNull(known.unlessColumns());
    assertEquals(List.of(2L), known.deleteBindValues());

    // Only an UPDATE of a source-keyed table: an insert, an UPDATE with a before-image (moved
    // or not) and an UPDATE of a table that is not re-keyed never have an unknown old key.
    assertFalse(
        DebeziumEvents.parse(leadRecord("c", null, leadRow(9L, 2L, "b")), REKEYED)
            .oldKeyUnknown());
    assertFalse(
        DebeziumEvents.parse(
                leadRecord("u", leadRow(5L, 2L, "a"), leadRow(9L, 2L, "b")), REKEYED)
            .oldKeyUnknown());
    assertFalse(
        DebeziumEvents.parse(
                leadRecord("u", leadRow(9L, 2L, "a"), leadRow(9L, 2L, "b")), REKEYED)
            .oldKeyUnknown());
    assertFalse(DebeziumEvents.parse(leadRecord("u", null, leadRow(9L, 2L, "b"))).oldKeyUnknown());
    assertThrows(
        IllegalStateException.class,
        () ->
            DebeziumEvents.parse(leadRecord("u", null, leadRow(9L, 2L, "b")))
                .withMoveDeleteBySourceKey());
  }

  @Test
  void placeholderDeleteCarriesTheSourceKey() {
    // The live REPLICA IDENTITY DEFAULT shape: the before-image's lead holds the column
    // DEFAULT (7). The delete is keyed on that exact composite (which matches nothing) and
    // carries the source key the task switches to once the unique index on it is valid.
    ChangeEvent event = DebeziumEvents.parse(leadRecord("d", leadRow(7L, 2L, null), null), REKEYED);
    assertTrue(event.isDelete());
    assertFalse(event.isTombstone());
    assertEquals(List.of("lead", "id"), event.pkColumns());
    assertEquals(List.of(7L, 2L), event.pkValues());
    assertEquals(List.of("id"), event.sourceKeyColumns());
    assertEquals(List.of(2L), event.sourceKeyValues());
    ChangeEvent bySourceKey = event.keyedBySourceKey();
    assertTrue(bySourceKey.isDelete());
    assertEquals(List.of("id"), bySourceKey.pkColumns());
    assertEquals(List.of(2L), bySourceKey.pkValues());
  }

  @Test
  void sourceKeyedDeleteWithoutABeforeImageIsRejected() {
    DataException thrown =
        assertThrows(
            DataException.class, () -> DebeziumEvents.parse(leadRecord("d", null, null), REKEYED));
    assertEquals(
        "Cannot build DELETE for table app.orders: the change record has no before-image, so the"
            + " row of its composite primary key (lead, id) cannot be located",
        thrown.getMessage());
  }

  @Test
  void afterImageMissingACompositeKeyColumnIsRejected() {
    Schema noLead =
        SchemaBuilder.struct()
            .name("Row")
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .optional()
            .build();
    Schema env =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", noLead)
            .field("source", SOURCE)
            .build();
    Struct value =
        new Struct(env)
            .put("op", "c")
            .put("after", new Struct(noLead).put("id", 2L).put("name", "a"))
            .put("source", source("orders"));
    DataException thrown =
        assertThrows(
            DataException.class,
            () ->
                DebeziumEvents.parse(
                    record(new Struct(SRC_KEY).put("id", 2L), value, "dsqlcdc.app.orders"),
                    REKEYED));
    assertEquals(
        "Cannot build upsert for table app.orders: the after-image has no value for composite"
            + " primary-key column 'lead'",
        thrown.getMessage());
  }

  // A NULLABLE leading column: only reachable after a source DDL relaxed it (Schema Conversion
  // only accepts a NOT NULL leading column), or -- with no lead field at all -- after the column
  // was dropped, since project() maps an absent field to null.
  private static final Schema NULLABLE_LEAD_ROW =
      SchemaBuilder.struct()
          .name("Row")
          .field("lead", Schema.OPTIONAL_INT64_SCHEMA)
          .field("id", Schema.INT64_SCHEMA)
          .field("name", Schema.OPTIONAL_STRING_SCHEMA)
          .optional()
          .build();

  private static SinkRecord nullableLeadRecord(String op, Struct before, Struct after) {
    Schema envelope =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("before", before != null ? before.schema() : NULLABLE_LEAD_ROW)
            .field("after", NULLABLE_LEAD_ROW)
            .field("source", SOURCE)
            .build();
    Struct env = new Struct(envelope).put("op", op).put("source", source("orders"));
    if (before != null) {
      env.put("before", before);
    }
    if (after != null) {
      env.put("after", after);
    }
    return record(new Struct(SRC_KEY).put("id", 2L), env, "dsqlcdc.app.orders");
  }

  @Test
  void sourceKeyedDeleteWithANullCompositeComponentIsRejected() {
    // Its exact composite DELETE would render "lead" = NULL and remove nothing, with no error:
    // it must be dead-lettered instead. Both shapes: a null lead, and no lead field at all.
    Struct nullLead =
        new Struct(NULLABLE_LEAD_ROW).put("lead", null).put("id", 2L).put("name", "a");
    Schema noLeadRow =
        SchemaBuilder.struct().name("Row").field("id", Schema.INT64_SCHEMA).optional().build();
    for (Struct before : List.of(nullLead, new Struct(noLeadRow).put("id", 2L))) {
      DataException thrown =
          assertThrows(
              DataException.class,
              () -> DebeziumEvents.parse(nullableLeadRecord("d", before, null), REKEYED));
      assertTrue(
          thrown.getMessage().startsWith(
              "Cannot build DELETE for table app.orders: key column 'lead' is null"),
          thrown.getMessage());
    }
  }

  @Test
  void aMoveFromANullCompositeComponentIsRejected() {
    // The move's delete half would be keyed "lead" = NULL: the old row would survive the move.
    Struct before =
        new Struct(NULLABLE_LEAD_ROW).put("lead", null).put("id", 2L).put("name", "a");
    Struct after =
        new Struct(NULLABLE_LEAD_ROW).put("lead", 9L).put("id", 2L).put("name", "b");
    DataException thrown =
        assertThrows(
            DataException.class,
            () -> DebeziumEvents.parse(nullableLeadRecord("u", before, after), REKEYED));
    assertTrue(
        thrown.getMessage().startsWith(
            "Cannot build UPDATE for table app.orders: key column 'lead' is null"),
        thrown.getMessage());
  }

  @Test
  void sourceKeyedTombstoneIsRecognisedAndALegacyKeyedOneIsNot() {
    SinkRecord sourceKeyed = record(new Struct(SRC_KEY).put("id", 2L), null, "dsqlcdc.app.orders");
    assertTrue(DebeziumEvents.isSourceKeyedTombstone(sourceKeyed, REKEYED));
    Schema compositeKey =
        SchemaBuilder.struct()
            .name("Key")
            .field("lead", Schema.INT64_SCHEMA)
            .field("id", Schema.INT64_SCHEMA)
            .build();
    SinkRecord legacy =
        record(new Struct(compositeKey).put("lead", 5L).put("id", 2L), null, "dsqlcdc.app.orders");
    assertFalse(DebeziumEvents.isSourceKeyedTombstone(legacy, REKEYED));
    // ...which still parses as today's tombstone delete.
    assertTrue(DebeziumEvents.parse(legacy, REKEYED).isTombstone());
    // Not a tombstone, an unlisted table, an empty map: never.
    assertFalse(
        DebeziumEvents.isSourceKeyedTombstone(
            leadRecord("d", leadRow(5L, 2L, "a"), null), REKEYED));
    assertFalse(
        DebeziumEvents.isSourceKeyedTombstone(
            record(new Struct(SRC_KEY).put("id", 2L), null, "dsqlcdc.app.items"), REKEYED));
    assertFalse(DebeziumEvents.isSourceKeyedTombstone(sourceKeyed, java.util.Map.of()));
  }

  @Test
  void aKeyThatIsNotAStrictSubsetIsNotSourceKeyed() {
    // The whole composite key in either order (an older pipeline's record), and a key with a
    // column outside the composite, all parse as today: keyed by the record key.
    assertNull(DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of("lead", "id")));
    assertNull(DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of("id", "lead")));
    assertNull(DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of("other")));
    assertNull(DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of("id", "x")));
    assertEquals(
        List.of("lead", "id"),
        DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of("id")));
    Schema otherKey =
        SchemaBuilder.struct().name("Key").field("other", Schema.INT64_SCHEMA).build();
    Struct env =
        new Struct(LEAD_ENVELOPE).put("op", "c").put("after", leadRow(5L, 2L, "a"))
            .put("source", source("orders"));
    ChangeEvent event =
        DebeziumEvents.parse(
            record(new Struct(otherKey).put("other", 1L), env, "dsqlcdc.app.orders"), REKEYED);
    assertEquals(List.of("other"), event.pkColumns());
    assertFalse(event.hasSourceKey());
  }

  @Test
  void emptyRecordKeyIsNotSourceKeyed() {
    assertNull(DebeziumEvents.sourceKeyedComposite(REKEYED, "app.orders", List.of()));
    // No key at all: today's path (a delete falls back to the before-image for its key).
    Struct env =
        new Struct(LEAD_ENVELOPE).put("op", "d").put("before", leadRow(5L, 2L, "a"))
            .put("source", source("orders"));
    ChangeEvent event = DebeziumEvents.parse(record(null, env, "dsqlcdc.app.orders"), REKEYED);
    assertTrue(event.isDelete());
    assertEquals(List.of("lead", "id", "name"), event.pkColumns());
    assertFalse(event.hasSourceKey());
  }

  @Test
  void unlistedTableParsesIdentically() {
    Struct env =
        new Struct(ENVELOPE).put("op", "u").put("before", row(1L, "A")).put("after", row(1L, "B"))
            .put("source", source("users"));
    SinkRecord rec = record(key(1L), env, "dsqlcdc.app.users");
    ChangeEvent plain = DebeziumEvents.parse(rec);
    ChangeEvent listed = DebeziumEvents.parse(rec, REKEYED);
    assertEquals(plain.pkColumns(), listed.pkColumns());
    assertEquals(plain.pkValues(), listed.pkValues());
    assertEquals(plain.columns(), listed.columns());
    assertEquals(plain.values(), listed.values());
    assertEquals(DsqlSinkTask.renderSql(plain), DsqlSinkTask.renderSql(listed));
    assertFalse(listed.hasSourceKey());
    assertFalse(listed.isMove());
  }

  @Test
  void tableLookupIsExact() {
    // A name that differs only in case is a DIFFERENT table (PostgreSQL; MySQL with
    // lower_case_table_names=0), never the listed one.
    java.util.Map<String, List<String>> keys = new java.util.LinkedHashMap<>();
    keys.put("app.orders", List.of("lead", "id"));
    assertEquals(
        List.of("lead", "id"),
        DebeziumEvents.sourceKeyedComposite(keys, "app.orders", List.of("id")));
    assertNull(DebeziumEvents.sourceKeyedComposite(keys, "app.Orders", List.of("id")));
    assertNull(DebeziumEvents.sourceKeyedComposite(keys, "APP.ORDERS", List.of("id")));
    // Both twins listed: each is matched to its own entry.
    keys.put("app.Orders", List.of("tenant", "id"));
    assertEquals(
        List.of("tenant", "id"),
        DebeziumEvents.sourceKeyedComposite(keys, "app.Orders", List.of("id")));
    assertEquals(
        List.of("lead", "id"),
        DebeziumEvents.sourceKeyedComposite(keys, "app.orders", List.of("id")));
    assertNull(DebeziumEvents.sourceKeyedComposite(keys, "app.orders2", List.of("id")));
  }

  @Test
  void anUnlistedCaseTwinOfAListedTableParsesIdentically() {
    // app.Orders (PK (id), no `lead`) next to the listed app.orders: keyed {id} as before G15,
    // so it must parse exactly as with no target.key.columns -- not by app.orders' key.
    for (SinkRecord rec :
        List.of(
            record(key(1L), new Struct(ENVELOPE).put("op", "c").put("after", row(1L, "a"))
                .put("source", source("Orders")), "dsqlcdc.app.Orders"),
            record(key(1L), new Struct(ENVELOPE).put("op", "u").put("before", row(1L, "a"))
                .put("after", row(1L, "b")).put("source", source("Orders")), "dsqlcdc.app.Orders"),
            record(key(1L), new Struct(ENVELOPE).put("op", "d").put("before", row(1L, "b"))
                .put("source", source("Orders")), "dsqlcdc.app.Orders"),
            record(key(1L), null, "dsqlcdc.app.Orders"))) {
      ChangeEvent plain = DebeziumEvents.parse(rec);
      ChangeEvent listed = DebeziumEvents.parse(rec, REKEYED);
      assertEquals(plain.pkColumns(), listed.pkColumns());
      assertEquals(plain.pkValues(), listed.pkValues());
      assertEquals(DsqlSinkTask.renderSql(plain), DsqlSinkTask.renderSql(listed));
      assertFalse(listed.hasSourceKey());
      assertFalse(listed.isMove());
      assertFalse(DebeziumEvents.isSourceKeyedTombstone(rec, REKEYED));
    }
  }
}
