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
  void quoteIdentEscapesEmbeddedQuotes() {
    assertEquals("\"we\"\"ird\"", DsqlDialect.quoteIdent("we\"ird"));
  }

  @Test
  void quoteQualifiedHandlesSchema() {
    assertEquals("\"public\".\"users\"", DsqlDialect.quoteQualified("public.users"));
  }

  @Test
  void upsertRejectsMissingPk() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DsqlDialect.upsertSql("t", List.of("a"), List.of()));
  }
}
