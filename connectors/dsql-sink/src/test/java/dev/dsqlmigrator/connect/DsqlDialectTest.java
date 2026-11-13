// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class DsqlDialectTest {

  @Test
  void upsertWithSinglePkUpdatesNonPkColumns() {
    String sql = DsqlDialect.upsertSql("users", List.of("id", "name", "email"), List.of("id"));
    assertEquals(
        "INSERT INTO \"users\" (\"id\", \"name\", \"email\") VALUES (?, ?, ?) "
            + "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", "
            + "\"email\" = EXCLUDED.\"email\"",
        sql);
  }

  @Test
  void upsertWhereAllColumnsArePkEmitsDoNothing() {
    String sql = DsqlDialect.upsertSql("link", List.of("a", "b"), List.of("a", "b"));
    assertEquals(
        "INSERT INTO \"link\" (\"a\", \"b\") VALUES (?, ?) ON CONFLICT (\"a\", \"b\") DO NOTHING",
        sql);
  }

  @Test
  void deleteWithCompositePk() {
    String sql = DsqlDialect.deleteSql("orders", List.of("region", "id"));
    assertEquals("DELETE FROM \"orders\" WHERE \"region\" = ? AND \"id\" = ?", sql);
  }

  @Test
  void deleteUnlessSparesARowAlreadyHoldingTheValues() {
    assertEquals(
        "DELETE FROM \"app\".\"orders\" WHERE \"id\" = ? AND NOT (\"lead\" = ?)",
        DsqlDialect.deleteUnlessSql("app.orders", List.of("id"), List.of("lead")));
    assertEquals(
        "DELETE FROM \"t\" WHERE \"id\" = ? AND NOT (\"a\" = ? AND \"b\" = ?)",
        DsqlDialect.deleteUnlessSql("t", List.of("id"), List.of("a", "b")));
    assertThrows(
        IllegalArgumentException.class,
        () -> DsqlDialect.deleteUnlessSql("t", List.of("id"), List.of()));
  }

  @Test
  void quoteIdentEscapesEmbeddedQuotes() {
    assertEquals("\"we\"\"ird\"", DsqlDialect.quoteIdent("we\"ird"));
  }

  @Test
  void quoteQualifiedHandlesSchema() {
    assertEquals("\"public\".\"users\"", DsqlDialect.quoteQualified("public.users"));
    assertEquals("\"users\"", DsqlDialect.quoteQualified("users"));
  }

  @Test
  void quoteQualifiedSplitsAtTheFirstDotOnly() {
    // FAILS WITHOUT THE FIX (plugin v57): the name was split at EVERY '.', so table orders.v2 of
    // database shop became "shop"."orders"."v2" -- a three-part name every write rejected -- and
    // table app.orders of schema app became "app"."app"."orders". The part before the first '.'
    // is the schema and the rest is the table, as every other step of the tool reads it.
    assertEquals("\"shop\".\"a.b\"", DsqlDialect.quoteQualified("shop.a.b"));
    assertEquals("\"app\".\"app.orders\"", DsqlDialect.quoteQualified("app.app.orders"));
    assertEquals(
        "DELETE FROM \"shop\".\"orders.v2\" WHERE \"id\" = ?",
        DsqlDialect.deleteSql("shop.orders.v2", List.of("id")));
    assertEquals(
        "INSERT INTO \"shop\".\"orders.v2\" (\"id\") VALUES (?) ON CONFLICT (\"id\") DO NOTHING",
        DsqlDialect.upsertSql("shop.orders.v2", List.of("id"), List.of("id")));
  }

  @Test
  void upsertRejectsMissingPk() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DsqlDialect.upsertSql("t", List.of("a"), List.of()));
  }
}
