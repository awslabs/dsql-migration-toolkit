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

  // --- PostgreSQL-source logical types (Phase D) ------------------------------

  @Test
  void pgUuidStringToJavaUuid() {
    // PostgreSQL uuid -> Debezium io.debezium.data.Uuid (a dashed string). The sink binds
    // a java.util.UUID so pgjdbc targets the uuid column (a plain String would be varchar).
    // Mirrors the Full Load path (psycopg uuid.UUID).
    String text = "0b6be8e2-2a11-4c3e-9d2e-1a2b3c4d5e6f";
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.UUID_TYPE, text);
    assertInstanceOf(java.util.UUID.class, r);
    assertEquals(java.util.UUID.fromString(text), r);
  }

  @Test
  void pgUuidMalformedPassesThroughToFailLoudly() {
    // A value that is not a UUID is left as the raw string so it DLQs, rather than crashing
    // the whole batch with an IllegalArgumentException.
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.UUID_TYPE, "not-a-uuid");
    assertEquals("not-a-uuid", r);
  }

  @Test
  void pgZonedTimeStringToOffsetTime() {
    // PostgreSQL timetz -> Debezium io.debezium.time.ZonedTime, an ISO-8601 offset time
    // that Debezium ALWAYS normalizes to UTC (a source 07:15:00-05:00 streams as
    // 12:15:00Z). The sink binds a java.time.OffsetTime, preserving the sub-second micros a
    // java.sql.Time would drop. (The UTC-vs-source-offset divergence from the Full Load
    // write is reconciled offset-insensitively in Validation -- see test_validation_sql.)
    Object r =
        DebeziumTypeConverter.convert(DebeziumTypeConverter.ZONED_TIME, "12:15:00.123456Z");
    assertInstanceOf(java.time.OffsetTime.class, r);
    assertEquals(java.time.OffsetTime.parse("12:15:00.123456Z"), r);
    // The converter binds whatever offset it is handed (it does not itself re-normalize);
    // a defensive non-UTC input still parses to the matching OffsetTime.
    Object r2 =
        DebeziumTypeConverter.convert(DebeziumTypeConverter.ZONED_TIME, "07:15:00-05:00");
    assertEquals(java.time.OffsetTime.parse("07:15:00-05:00"), r2);
  }

  @Test
  void pgZonedTimeUnparseablePassesThroughToFailLoudly() {
    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.ZONED_TIME, "nonsense");
    assertEquals("nonsense", r);
  }

  @Test
  void pgIntervalIsoStringWrappedInPgInterval() throws Exception {
    // PostgreSQL interval (interval.handling.mode=string) -> an ISO-8601 duration string.
    // Wrapped in a PGobject(type=interval) so pgjdbc binds it to the interval column;
    // PostgreSQL's interval input parses ISO-8601. Same stored value as the Full Load path.
    Object r =
        DebeziumTypeConverter.convert(DebeziumTypeConverter.INTERVAL_TYPE, "P1Y2M3DT4H5M6S");
    assertInstanceOf(PGobject.class, r);
    PGobject pg = (PGobject) r;
    assertEquals("interval", pg.getType());
    assertEquals("P1Y2M3DT4H5M6S", pg.getValue());
  }

  @Test
  void pgVariableScaleDecimalStructToBigDecimal() {
    // Unconstrained PostgreSQL numeric under decimal.handling.mode=precise -> a Struct
    // {scale INT32, value BYTES}. `value` is the two's-complement big-endian unscaled
    // integer; decode to a BigDecimal (mirrors the Full Load Decimal). 1234.5678 = unscaled
    // 12345678 with scale 4.
    java.math.BigInteger unscaled = new java.math.BigInteger("12345678");
    Schema vsd =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL)
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .build();
    Struct value = new Struct(vsd).put("scale", 4).put("value", unscaled.toByteArray());

    Object r = DebeziumTypeConverter.convert(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL, value);

    assertInstanceOf(BigDecimal.class, r);
    assertEquals(new BigDecimal("1234.5678"), r);
  }

  @Test
  void pgVariableScaleDecimalNegativeAndZeroScale() {
    // A negative value (two's-complement bytes) and a scale-0 integer both decode exactly.
    Schema vsd =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL)
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .build();
    Struct neg =
        new Struct(vsd).put("scale", 2).put("value", new java.math.BigInteger("-9999").toByteArray());
    assertEquals(
        new BigDecimal("-99.99"),
        DebeziumTypeConverter.convert(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL, neg));
    Struct whole =
        new Struct(vsd).put("scale", 0).put("value", new java.math.BigInteger("42").toByteArray());
    assertEquals(
        new BigDecimal("42"),
        DebeziumTypeConverter.convert(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL, whole));
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

  // --- Tier-3 regression guards ------------------------------------------------

  @Test
  void dateEpochDayToSqlDateIsTimezoneIndependent() {
    // io.debezium.time.Date is days-since-epoch. Converting via LocalDate.ofEpochDay is
    // calendar-based (TZ-independent); a `new java.sql.Date(epochDay*86400000L)` would shift
    // the day under a non-UTC JVM zone. Compare the LocalDate, not millis.
    Object r0 = DebeziumTypeConverter.convert(DebeziumTypeConverter.DATE, 0L);
    assertInstanceOf(java.sql.Date.class, r0);
    assertEquals(java.time.LocalDate.of(1970, 1, 1), ((java.sql.Date) r0).toLocalDate());
    long ed2024 = java.time.LocalDate.of(2024, 1, 1).toEpochDay();
    assertEquals(
        java.time.LocalDate.of(2024, 1, 1),
        ((java.sql.Date) DebeziumTypeConverter.convert(DebeziumTypeConverter.DATE, ed2024)).toLocalDate());
    long ed1900 = java.time.LocalDate.of(1900, 1, 1).toEpochDay(); // pre-epoch (negative)
    assertEquals(
        java.time.LocalDate.of(1900, 1, 1),
        ((java.sql.Date) DebeziumTypeConverter.convert(DebeziumTypeConverter.DATE, ed1900)).toLocalDate());
  }

  @Test
  void zonedTimestampKeepsMicroseconds() {
    // io.debezium.time.ZonedTimestamp with sub-millisecond micros must not truncate to millis.
    Timestamp ts = (Timestamp) DebeziumTypeConverter.convert(
        DebeziumTypeConverter.ZONED_TIMESTAMP, "2024-01-01T00:00:00.123456Z");
    assertEquals(123_456_000, ts.getNanos());
    // A non-UTC offset resolves to the same instant AND keeps the micros.
    Timestamp ts2 = (Timestamp) DebeziumTypeConverter.convert(
        DebeziumTypeConverter.ZONED_TIMESTAMP, "2024-01-01T05:00:00.123456+05:00");
    assertEquals(Instant.parse("2024-01-01T00:00:00.123456Z"), ts2.toInstant());
  }

  // --- PostgreSQL bit/varbit -> bit-string (live-test finding: CDC must match Full Load) ---

  private static Schema bitsSchema(int length) {
    return SchemaBuilder.bytes()
        .name(DebeziumTypeConverter.BITS_TYPE)
        .parameter("length", String.valueOf(length))
        .optional()
        .build();
  }

  @Test
  void pgBitFixedLengthRendersBitString() {
    // PG bit(8) B'11011011' -> Debezium little-endian bytes {0xDB} -> the bit-string
    // "11011011" (matches psycopg / the Full Load write), NOT the MySQL integer 219. The
    // live DLQ test proved the old integer path diverged the CDC write from Full Load.
    assertEquals("11011011", DebeziumTypeConverter.pgBitString(new byte[] {(byte) 0xDB}, bitsSchema(8)));
  }

  @Test
  void pgBitLeadingZerosPaddedToDeclaredLength() {
    // bit(8) value 15 = {0x0F} -> "00001111" (left-padded to the declared length 8) exactly
    // as psycopg returns; a bare binary of 15 would drop the leading zeros. All-zero -> all 0s.
    assertEquals("00001111", DebeziumTypeConverter.pgBitString(new byte[] {0x0F}, bitsSchema(8)));
    assertEquals("00000000", DebeziumTypeConverter.pgBitString(new byte[] {0x00}, bitsSchema(8)));
  }

  @Test
  void pgVarbitRendersBitStringAtDeclaredLength() {
    // varbit '1010101010' (10 bits) = 682, little-endian {0xAA,0x02}; with the declared
    // length 10 it reconstructs exactly. (An unbounded bit varying whose per-value length is
    // shorter than the declared max may carry extra leading zeros -- a documented best-effort
    // residual: Debezium's byte encoding does not preserve the per-value bit length.)
    assertEquals(
        "1010101010",
        DebeziumTypeConverter.pgBitString(new byte[] {(byte) 0xAA, 0x02}, bitsSchema(10)));
  }

  @Test
  void pgBitStringHandlesWideValuesAndNonBytes() {
    // > 64 bits must not overflow (BigInteger, not long): 9 bytes of 0xFF at length 72.
    byte[] wide = new byte[9];
    java.util.Arrays.fill(wide, (byte) 0xFF);
    assertEquals("1".repeat(72), DebeziumTypeConverter.pgBitString(wide, bitsSchema(72)));
    // a non-bytes value passes through unchanged (binds as-is; fails loudly if truly wrong).
    assertEquals("x", DebeziumTypeConverter.pgBitString("x", bitsSchema(8)));
  }

  @Test
  void arrayListPassesThroughUnchangedKnownGap() {
    // A PG array (a List, no logical schema name) hits convert()'s default branch and is
    // returned as-is. That is still the contract OF convert() -- but it is NOT what a real PG
    // array column does: DebeziumEvents.convertField intercepts an ARRAY schema first and routes
    // it to pgArrayAsJsonb (Schema Conversion maps <t>[] to jsonb, so these columns absolutely
    // DO reach the sink). This pins convert()'s pass-through only.
    java.util.List<Integer> arr = java.util.List.of(1, 2, 3);
    assertTrue(DebeziumTypeConverter.convert(null, arr) == arr, "List passes through unchanged");
  }

  // ---------------------------------------------------------------------------------------
  // PostgreSQL array -> jsonb parity.
  //
  // Schema Conversion maps a PostgreSQL <t>[] to jsonb (DSQL has no array type) and the Full
  // Load converts on the SOURCE in SQL -- CAST(to_jsonb(col) AS text) -- so the bytes already
  // in DSQL ARE PostgreSQL's own to_jsonb output. The sink must reproduce that text or the two
  // write paths disagree and Validation's CHECKSUM (which applies to_jsonb(col)::text to BOTH
  // ends, validation_sql kind array_json) mismatches every CDC-written row.
  //
  // EVERY expectedJson below is MEASURED output from a live PostgreSQL 17.11, not reasoning.
  // pgExpression is the exact expression it was measured with, so the whole table is
  // re-checkable against any live PostgreSQL: run
  //
  //     mvn -o test -Dtest=DebeziumTypeConverterTest
  //     PGPASSWORD=… psql -h … -U … -d … -f target/pg_array_jsonb_parity.sql
  //
  // and the generated script prints one row per DIVERGENCE (zero rows == parity). See
  // pgArrayJsonbExpectationsAreRecheckableAgainstLivePostgres below.
  // ---------------------------------------------------------------------------------------

  /** One measured parity case. */
  private record ArrayCase(
      String pgExpression, Schema elementSchema, java.util.List<?> value, String expectedJson) {}

  private static Schema named(Schema base, String logicalName) {
    return new SchemaBuilder(base.type()).name(logicalName).optional().build();
  }

  private static final Schema INT32 = Schema.OPTIONAL_INT32_SCHEMA;
  private static final Schema INT64 = Schema.OPTIONAL_INT64_SCHEMA;
  private static final Schema STRING = Schema.OPTIONAL_STRING_SCHEMA;
  private static final Schema BOOL = Schema.OPTIONAL_BOOLEAN_SCHEMA;
  private static final Schema FLOAT32 = Schema.OPTIONAL_FLOAT32_SCHEMA;
  private static final Schema FLOAT64 = Schema.OPTIONAL_FLOAT64_SCHEMA;

  private static java.util.List<ArrayCase> pgArrayJsonbCases() {
    Schema microTimestamp = named(INT64, DebeziumTypeConverter.MICRO_TIMESTAMP);
    Schema timestampMs = named(INT64, DebeziumTypeConverter.TIMESTAMP_MS);
    Schema zonedTimestamp = named(STRING, DebeziumTypeConverter.ZONED_TIMESTAMP);
    Schema date = named(INT32, DebeziumTypeConverter.DATE);
    Schema microTime = named(INT64, DebeziumTypeConverter.MICRO_TIME);
    Schema uuid = named(STRING, DebeziumTypeConverter.UUID_TYPE);
    Schema json = named(STRING, DebeziumTypeConverter.JSON_TYPE);
    Schema decimal = named(Schema.OPTIONAL_BYTES_SCHEMA, DebeziumTypeConverter.DECIMAL_TYPE);
    return java.util.List.of(
        // --- shapes that were already correct: pin them so the new dispatch cannot regress them
        new ArrayCase("ARRAY[1,2,3]::int[]", INT32, java.util.List.of(1, 2, 3), "[1, 2, 3]"),
        new ArrayCase("ARRAY[]::int[]", INT32, java.util.List.of(), "[]"),
        new ArrayCase(
            "ARRAY[1,NULL,3]::int[]", INT32, java.util.Arrays.asList(1, null, 3),
            "[1, null, 3]"),
        new ArrayCase(
            "ARRAY[true,false,NULL]::bool[]", BOOL, java.util.Arrays.asList(true, false, null),
            "[true, false, null]"),
        new ArrayCase(
            "ARRAY['a\"b', E'c\\\\d']::text[]", STRING, java.util.List.of("a\"b", "c\\d"),
            "[\"a\\\"b\", \"c\\\\d\"]"),
        new ArrayCase(
            "ARRAY[E'a\\tb', E'c\\nd']::text[]", STRING, java.util.List.of("a\tb", "c\nd"),
            "[\"a\\tb\", \"c\\nd\"]"),
        new ArrayCase( // char(n) keeps its blank padding on both paths
            "ARRAY['ab','cdefg']::char(5)[]", STRING, java.util.List.of("ab   ", "cdefg"),
            "[\"ab   \", \"cdefg\"]"),
        new ArrayCase(
            "ARRAY['0a8a1f6e-1111-4222-8333-444455556666'::uuid]", uuid,
            java.util.List.of("0a8a1f6e-1111-4222-8333-444455556666"),
            "[\"0a8a1f6e-1111-4222-8333-444455556666\"]"),
        new ArrayCase( // numeric(p,s): to_jsonb PRESERVES the declared scale
            "ARRAY[1.5,2,3.456]::numeric(10,2)[]", decimal,
            java.util.List.of(
                new BigDecimal("1.50"), new BigDecimal("2.00"), new BigDecimal("3.46")),
            "[1.50, 2.00, 3.46]"),

        // --- float8/float4: shortest round-tripping decimal, printed PLAIN, non-finite QUOTED
        new ArrayCase(
            "ARRAY[1.0,0.1,1e30,1e-9]::float8[]", FLOAT64,
            java.util.List.of(1.0d, 0.1d, 1e30d, 1e-9d),
            "[1, 0.1, 1000000000000000000000000000000, 0.000000001]"),
        new ArrayCase( // Double.toString gives 1.716943642359572E17 -> …200, PostgreSQL …180
            "ARRAY[1.7169436423595718e17::float8]", FLOAT64,
            java.util.List.of(1.7169436423595718e17d), "[171694364235957180]"),
        new ArrayCase(
            "ARRAY[0::float8,'-0'::float8]", FLOAT64, java.util.List.of(0.0d, -0.0d), "[0, 0]"),
        new ArrayCase(
            "ARRAY['NaN'::float8,'Infinity'::float8,'-Infinity'::float8]", FLOAT64,
            java.util.List.of(
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY),
            "[\"NaN\", \"Infinity\", \"-Infinity\"]"),
        new ArrayCase(
            "ARRAY[1.0,0.1,3.4028235e38]::float4[]", FLOAT32,
            java.util.List.of(1.0f, 0.1f, 3.4028235e38f),
            "[1, 0.1, 340282350000000000000000000000000000000]"),
        new ArrayCase( // Float.toString gives -1.2786838E8 -> …380, PostgreSQL …384
            "ARRAY['-1.2786838e8'::float4]", FLOAT32, java.util.List.of(-1.2786838e8f),
            "[-127868384]"),
        new ArrayCase( // Float.MIN_VALUE: Float.toString says 1.4E-45, PostgreSQL says 1e-45
            "ARRAY['1.4e-45'::float4]", FLOAT32, java.util.List.of(Float.MIN_VALUE),
            "[0.000000000000000000000000000000000000000000001]"),

        // --- temporal: to_jsonb writes a QUOTED ISO string, Debezium sends a raw Long/Integer
        new ArrayCase(
            "ARRAY['2026-01-02 03:04:05.678'::timestamp]", microTimestamp,
            java.util.List.of(1767323045678000L), "[\"2026-01-02T03:04:05.678\"]"),
        new ArrayCase( // a zero fraction is OMITTED, not printed as .000
            "ARRAY['2026-01-02 03:04:05'::timestamp]", microTimestamp,
            java.util.List.of(1767323045000000L), "[\"2026-01-02T03:04:05\"]"),
        new ArrayCase( // trailing zeros are TRIMMED: .100 prints as .1
            "ARRAY['2026-01-02 03:04:05.1'::timestamp]", microTimestamp,
            java.util.List.of(1767323045100000L), "[\"2026-01-02T03:04:05.1\"]"),
        new ArrayCase(
            "ARRAY['2026-01-02 03:04:05.678901'::timestamp]", microTimestamp,
            java.util.List.of(1767323045678901L), "[\"2026-01-02T03:04:05.678901\"]"),
        new ArrayCase( // pre-epoch: the fraction must stay positive (floorDiv/floorMod)
            "ARRAY['1969-12-31 23:59:59.5'::timestamp]", microTimestamp,
            java.util.List.of(-500000L), "[\"1969-12-31T23:59:59.5\"]"),
        new ArrayCase( // timestamp(0..3)[] arrives as io.debezium.time.Timestamp -> MILLIS
            "ARRAY['2026-01-02 03:04:05.678'::timestamp(3)]", timestampMs,
            java.util.List.of(1767323045678L), "[\"2026-01-02T03:04:05.678\"]"),
        new ArrayCase(
            "ARRAY['2026-01-02'::date]", date, java.util.List.of(20455), "[\"2026-01-02\"]"),
        new ArrayCase(
            "ARRAY['03:04:05.678'::time]", microTime, java.util.List.of(11045678000L),
            "[\"03:04:05.678\"]"),
        new ArrayCase(
            "ARRAY['03:04:05'::time]", microTime, java.util.List.of(11045000000L),
            "[\"03:04:05\"]"),
        new ArrayCase("ARRAY['00:00:00'::time]", microTime, java.util.List.of(0L),
            "[\"00:00:00\"]"),
        new ArrayCase( // PostgreSQL prints the offset WITH a colon; Debezium prints a bare Z
            "ARRAY['2026-01-02 03:04:05.678+00'::timestamptz]", zonedTimestamp,
            java.util.List.of("2026-01-02T03:04:05.678Z"),
            "[\"2026-01-02T03:04:05.678+00:00\"]"),
        new ArrayCase(
            "ARRAY['2026-01-02 03:04:05+00'::timestamptz]", zonedTimestamp,
            java.util.List.of("2026-01-02T03:04:05Z"), "[\"2026-01-02T03:04:05+00:00\"]"),

        // --- json[]/jsonb[]: to_jsonb EMBEDS the element JSON, it does not quote it.
        // jsonb_out already hands Debezium the canonical text, so the bytes match exactly;
        // for json[] the sink embeds the element's RAW text, which the target jsonb column
        // parses to the SAME stored value (this is why the re-check compares through ::jsonb).
        new ArrayCase(
            "ARRAY['{\"b\":1, \"a\":2}'::jsonb]", json,
            java.util.List.of("{\"a\": 2, \"b\": 1}"), "[{\"a\": 2, \"b\": 1}]"),
        new ArrayCase(
            "ARRAY['[1,2]'::jsonb,'{\"k\":\"v\"}'::jsonb]", json,
            java.util.List.of("[1, 2]", "{\"k\": \"v\"}"), "[[1, 2], {\"k\": \"v\"}]"),
        new ArrayCase( // a jsonb 'null' element is JSON null, not the string "null"
            "ARRAY['42'::jsonb,'\"str\"'::jsonb,'null'::jsonb]", json,
            java.util.List.of("42", "\"str\"", "null"), "[42, \"str\", null]"));
  }

  /** to_jsonb parity for every element shape the PostgreSQL source can deliver. */
  @Test
  void pgArrayJsonbRenderingMatchesPostgresForEveryElementShape() {
    for (ArrayCase c : pgArrayJsonbCases()) {
      assertEquals(
          c.expectedJson(),
          DebeziumTypeConverter.pgArrayAsJsonb(
                  c.value(), c.elementSchema())
              .getValue(),
          "to_jsonb(" + c.pgExpression() + ") measured on live PostgreSQL 17.11");
    }
  }

  /**
   * Emits the whole expectation table as a self-checking SQL script so the literals above can be
   * re-verified against a LIVE PostgreSQL instead of only against themselves.
   *
   * <p>The comparison is the one Validation actually performs: the sink's text is what gets
   * parsed into the jsonb column, so it is compared as {@code (rendered::jsonb)::text} against
   * {@code to_jsonb(<expr>)::text} — the STORED bytes. (That is scale-preserving:
   * {@code '[1.50]'::jsonb::text} is {@code [1.50]}, not {@code [1.5]}.) Running the script
   * prints one row per divergence; zero rows means parity.
   */
  @Test
  void pgArrayJsonbExpectationsAreRecheckableAgainstLivePostgres() throws java.io.IOException {
    StringBuilder sql =
        new StringBuilder(
            "-- GENERATED by DebeziumTypeConverterTest. Zero rows == the sink matches to_jsonb.\n"
                + "SET timezone='UTC'; SET datestyle='ISO'; SET intervalstyle='postgres';\n"
                + "SELECT * FROM (VALUES\n");
    java.util.List<ArrayCase> cases = pgArrayJsonbCases();
    for (int i = 0; i < cases.size(); i++) {
      ArrayCase c = cases.get(i);
      String rendered =
          DebeziumTypeConverter.pgArrayAsJsonb(c.value(), c.elementSchema()).getValue();
      sql.append("  (")
          .append(quote(c.pgExpression()))
          .append(", (")
          .append(quote(rendered))
          .append("::jsonb)::text, to_jsonb(")
          .append(c.pgExpression())
          .append(")::text)")
          .append(i == cases.size() - 1 ? "\n" : ",\n");
    }
    sql.append(") AS t(pg_expression, sink_stored, pg_to_jsonb)\n")
        .append("WHERE sink_stored IS DISTINCT FROM pg_to_jsonb;\n");
    java.nio.file.Path out = java.nio.file.Path.of("target", "pg_array_jsonb_parity.sql");
    java.nio.file.Files.createDirectories(out.getParent());
    java.nio.file.Files.writeString(out, sql.toString());
    assertTrue(java.nio.file.Files.size(out) > 0, "parity script written to " + out.toAbsolutePath());
  }

  /** SQL single-quoted literal (standard_conforming_strings: a backslash is literal). */
  private static String quote(String text) {
    return "'" + text.replace("'", "''") + "'";
  }

  /**
   * An element type whose to_jsonb text cannot be reproduced must THROW, not guess.
   *
   * <p>A best-effort rendering would write a value that silently differs from what the Full Load
   * loaded -- and pass every count-based check. A dead-letter NAMING the type is recoverable; a
   * wrong value that survives the checksum is not. Each case below is a shape the PostgreSQL
   * connector really produces, and each message must name the PostgreSQL type, not just the
   * Java class, so the DLQ entry is actionable.
   */
  @Test
  void anUnrenderableArrayElementDeadLettersInsteadOfGuessing() {
    // timetz[]: to_jsonb keeps the SOURCE offset (["03:04:05.678+09"]) and Debezium's ZonedTime
    // has already normalized to UTC and thrown that offset away -- unrecoverable, not a
    // formatting problem, so it must not be approximated as "+00:00".
    assertThrowsWith(
        "timetz",
        named(STRING, DebeziumTypeConverter.ZONED_TIME),
        java.util.List.of("03:04:05.678Z"));
    // numeric[] with no declared scale: Debezium stripTrailingZeros() the value before the sink
    // sees it, so to_jsonb's 1.50 can never be rebuilt from the event's 1.5.
    Schema variableScale =
        SchemaBuilder.struct()
            .name(DebeziumTypeConverter.VARIABLE_SCALE_DECIMAL)
            .field("scale", Schema.INT32_SCHEMA)
            .field("value", Schema.BYTES_SCHEMA)
            .optional()
            .build();
    assertThrowsWith(
        "numeric",
        variableScale,
        java.util.List.of(
            new Struct(variableScale).put("scale", 1).put("value", new byte[] {15})));
    // bytea[]: to_jsonb's spelling depends on the source's bytea_output GUC, absent from the event.
    assertThrowsWith("bytea", Schema.OPTIONAL_BYTES_SCHEMA, java.util.List.of(new byte[] {1, 2}));
    // infinity / BC: Debezium encodes both as ordinary epoch micros (infinity lands in 294247),
    // while to_jsonb writes "infinity" / "0044-03-15T10:00:00 BC".
    assertThrowsWith(
        "0001..9999",
        named(INT64, DebeziumTypeConverter.MICRO_TIMESTAMP),
        java.util.List.of(Long.MAX_VALUE));
    assertThrowsWith(
        "0001..9999",
        named(INT32, DebeziumTypeConverter.DATE),
        java.util.List.of(-720000)); // 0001-01-01 BC-ward
    // time '24:00:00' is a legal PostgreSQL value with no in-range micros-since-midnight form.
    assertThrowsWith(
        "24:00:00",
        named(INT64, DebeziumTypeConverter.MICRO_TIME),
        java.util.List.of(86_400L * 1_000_000L));
    // A NAMED numeric logical type this sink has not been taught must NOT fall through to a bare
    // JSON number -- that silent path is the whole defect. io.debezium.time.NanoTime is the real
    // one PostgresValueConverter can emit (timestamp precision > 6).
    assertThrowsWith(
        "io.debezium.time.NanoTime",
        named(INT64, "io.debezium.time.NanoTime"),
        java.util.List.of(1L));
    // A nested array still raises (Debezium's element converter nulls these out before the sink,
    // so this is a defensive guard rather than a reachable path).
    assertThrowsWith("array", INT32, java.util.List.of(java.util.List.of("nested")));
  }

  private static void assertThrowsWith(
      String expectedInMessage, Schema elementSchema, java.util.List<?> value) {
    org.apache.kafka.connect.errors.DataException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            org.apache.kafka.connect.errors.DataException.class,
            () -> DebeziumTypeConverter.pgArrayAsJsonb(value, elementSchema),
            "must dead-letter rather than render an approximation");
    assertTrue(
        thrown.getMessage().contains(expectedInMessage),
        "message must name " + expectedInMessage + " but was: " + thrown.getMessage());
  }

}
