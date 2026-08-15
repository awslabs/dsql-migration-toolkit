// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;

/**
 * DSQL write-contract parity tests — the Java (CDC sink) half.
 *
 * <p>Asserts {@link DebeziumTypeConverter} encodes each boundary MySQL type the
 * same way the Python bulk loader does, per the shared contract
 * (tests/fixtures/dsql_write_contract.json, also at
 * src/test/resources/dsql_write_contract.json). The Python half
 * (tests/test_dsql_write_contract.py) asserts the same cases for the bulk loader.
 */
class DebeziumTypeConverterTest {

  // 2024-01-01T00:00:00Z, matching the shared fixture's datetime cases.
  private static final long EPOCH_MS = 1_704_067_200_000L;

  @Test
  void datetimeMillisToTimestamp() {
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.TIMESTAMP_MS, EPOCH_MS);
    assertInstanceOf(Timestamp.class, r);
    assertEquals(Instant.ofEpochMilli(EPOCH_MS), ((Timestamp) r).toInstant());
  }

  @Test
  void datetime6MicrosToTimestamp() {
    // DATETIME(6): micros since epoch -> Timestamp with the fractional micros in nanos.
    long micros = EPOCH_MS * 1000L + 123; // 123 micros past the second boundary
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.MICRO_TIMESTAMP, micros);
    assertInstanceOf(Timestamp.class, r);
    Timestamp ts = (Timestamp) r;
    assertEquals(EPOCH_MS, ts.getTime()); // whole-millis part
    assertEquals(123 * 1000, ts.getNanos() % 1_000_000); // sub-milli micros -> nanos
  }

  @Test
  void datetime6FullMicrosecondPrecisionPreserved() {
    long micros = EPOCH_MS * 1000L + 123456; // 123456 micros = 0.123456 s
    Timestamp ts = (Timestamp) DebeziumTypeConverter.convert(
        DebeziumTypeConverter.MICRO_TIMESTAMP, micros);
    assertEquals(123_456_000, ts.getNanos()); // 123456 micros -> 123456000 nanos
  }

  @Test
  void zonedTimestampStringToTimestamp() {
    Object r = DebeziumTypeConverter.convert(
        DebeziumTypeConverter.ZONED_TIMESTAMP, "2024-01-01T00:00:00Z");
    assertInstanceOf(Timestamp.class, r);
    assertEquals(Instant.ofEpochMilli(EPOCH_MS), ((Timestamp) r).toInstant());
  }

  @Test
  void jsonStringWrappedInPgObject() throws Exception {
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.JSON_TYPE, "{\"k\": 1}");
    assertInstanceOf(PGobject.class, r);
    PGobject pg = (PGobject) r;
    assertEquals("json", pg.getType());
    assertEquals("{\"k\": 1}", pg.getValue());
  }

  @Test
  void decimalPassesThroughUnchanged() {
    BigDecimal d = new BigDecimal("1234.5678");
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.DECIMAL_TYPE, d);
    assertInstanceOf(BigDecimal.class, r);
    assertEquals(d, r);
  }

  @Test
  void bigintUnsignedAsBigDecimalPassesThrough() {
    // With bigint.unsigned.handling.mode=precise, Debezium sends a BigDecimal.
    BigDecimal max = new BigDecimal("18446744073709551615"); // 2^64 - 1
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.DECIMAL_TYPE, max);
    assertEquals(max, r);
  }

  @Test
  void geometryStructYieldsWkbBytes() {
    // Debezium delivers MySQL spatial as a Struct {wkb: byte[], srid: int}. DSQL
    // has no geometry type, so the sink stores the raw WKB bytes (-> bytea),
    // identical to Full Load's ST_AsBinary(col). SRID is dropped (plain WKB).
    Schema geom =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.GEOMETRY_TYPE)
            .field("wkb", Schema.BYTES_SCHEMA)
            .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
            .build();
    byte[] wkb = new byte[] {0x01, 0x01, 0x00, 0x00, 0x00};
    Struct value = new Struct(geom).put("wkb", wkb).put("srid", 4326);

    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.GEOMETRY_TYPE, value);

    assertInstanceOf(byte[].class, r);
    assertArrayEquals(wkb, (byte[]) r);
  }

  @Test
  void nullPassesThrough() {
    assertNull(DebeziumTypeConverter.convert(DebeziumTypeConverter.MICRO_TIMESTAMP, null));
  }

  @Test
  void plainPrimitiveWithoutSchemaNamePassesThrough() {
    assertEquals("hello", DebeziumTypeConverter.convert(null, "hello"));
    assertEquals(42L, DebeziumTypeConverter.convert(null, 42L));
    assertEquals(Boolean.TRUE, DebeziumTypeConverter.convert(null, Boolean.TRUE));
  }

  @Test
  void unknownSchemaNamePassesThrough() {
    assertEquals("x", DebeziumTypeConverter.convert("some.unknown.logical.type", "x"));
  }

  @Test
  void bitsLittleEndianBytesToLong() {
    // MySQL BIT(8) value 0xDB -> Debezium little-endian byte[] -> 219 (DSQL int).
    Object r = DebeziumTypeConverter.convert(
        DebeziumTypeConverter.BITS_TYPE, new byte[] {(byte) 0xDB});
    assertEquals(219L, r);
    // BIT(16) value 0x0102 little-endian {0x02,0x01} -> 0x0102 = 258.
    Object r2 = DebeziumTypeConverter.convert(
        DebeziumTypeConverter.BITS_TYPE, new byte[] {(byte) 0x02, (byte) 0x01});
    assertEquals(258L, r2);
  }

  @Test
  void bitsFull64BitStaysUnsigned() {
    // MySQL BIT(64) = 2^64-1 -> Debezium little-endian 8x 0xFF. A signed long would wrap
    // to -1; the DSQL target is numeric(20,0), so it must keep the UNSIGNED value.
    byte[] allOnes = new byte[] {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.BITS_TYPE, allOnes);
    assertEquals(new java.math.BigDecimal("18446744073709551615"), r);
    // BIT(63) max (0x7FFF...FFFF) still fits a signed long and stays a Long.
    byte[] max63 = new byte[] {
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F};
    assertEquals(Long.MAX_VALUE,
        DebeziumTypeConverter.convert(DebeziumTypeConverter.BITS_TYPE, max63));
  }

  @Test
  void microTimeToLocalTime() {
    // 05:24:39 = 19479 s past midnight -> micros; converts to a java.time.LocalTime
    // (NOT java.sql.Time, which would drop any sub-second component).
    long micros = 19_479L * 1_000_000L;
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.MICRO_TIME, micros);
    assertInstanceOf(java.time.LocalTime.class, r);
    assertEquals(java.time.LocalTime.of(5, 24, 39), r);
  }

  @Test
  void microTimeKeepsSubSecondMicroseconds() {
    // A MySQL TIME(6) value 12:34:56.789012: the micros MUST survive. java.sql.Time
    // .valueOf(LocalTime) would have truncated to 12:34:56, diverging from the Full
    // Load path (Python datetime.time(microsecond=…)) which keeps them -> Validation
    // mismatch. pgjdbc binds the LocalTime to time(n) with full micro precision.
    long micros = ((12L * 3600 + 34 * 60 + 56) * 1_000_000L) + 789_012L;
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.MICRO_TIME, micros);
    assertInstanceOf(java.time.LocalTime.class, r);
    assertEquals(java.time.LocalTime.of(12, 34, 56, 789_012_000), r); // 789012 micros
  }

  @Test
  void timeMillisToLocalTimeKeepsMilliseconds() {
    // io.debezium.time.Time (millis since midnight, MySQL TIME(1-3)) -> LocalTime with
    // the millis preserved (01:02:03.456), same as the micro-precision path.
    long millis = ((1L * 3600 + 2 * 60 + 3) * 1000L) + 456L;
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.TIME_MS, millis);
    assertInstanceOf(java.time.LocalTime.class, r);
    assertEquals(java.time.LocalTime.of(1, 2, 3, 456_000_000), r);
  }

  @Test
  void outOfRangeTimePassesThroughToFailLoudly() {
    // >= 24h has no `time` representation -> left as the raw long (DLQ'd downstream).
    long micros = 30L * 3600L * 1_000_000L; // 30h
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.MICRO_TIME, micros);
    assertEquals(micros, r);
  }

  // --- Integration: the converter is wired into DebeziumEvents.extractStruct ---

  @Test
  void extractStructConvertsTemporalFieldsInUpsert() {
    // Build a Debezium envelope whose after-image has a MicroTimestamp column, and
    // assert the parsed ChangeEvent carries a Timestamp (not the raw Long).
    Schema microTs =
        SchemaBuilder.int64().name(DebeziumTypeConverter.MICRO_TIMESTAMP).optional().build();
    Schema key = SchemaBuilder.struct().name("Key").field("id", Schema.INT64_SCHEMA).build();
    Schema row =
        SchemaBuilder.struct()
            .name("Row")
            .field("id", Schema.INT64_SCHEMA)
            .field("created_at", microTs)
            .optional()
            .build();
    Schema source =
        SchemaBuilder.struct()
            .name("Source")
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .optional()
            .build();
    Schema envelope =
        SchemaBuilder.struct()
            .name("Envelope")
            .field("op", Schema.STRING_SCHEMA)
            .field("after", row)
            .field("source", source)
            .build();

    long micros = EPOCH_MS * 1000L;
    Struct after = new Struct(row).put("id", 7L).put("created_at", micros);
    Struct src = new Struct(source).put("db", "app").put("table", "orders");
    Struct env = new Struct(envelope).put("op", "c").put("after", after).put("source", src);
    SinkRecord record =
        new SinkRecord("dsqlcdc.app.orders", 0, key, new Struct(key).put("id", 7L), envelope, env, 0L);

    ChangeEvent event = DebeziumEvents.parse(record);
    int idx = event.columns().indexOf("created_at");
    assertTrue(idx >= 0, "created_at column present");
    Object stored = event.values().get(idx);
    assertInstanceOf(Timestamp.class, stored);
    assertEquals(Instant.ofEpochMilli(EPOCH_MS), ((Timestamp) stored).toInstant());
  }
}
