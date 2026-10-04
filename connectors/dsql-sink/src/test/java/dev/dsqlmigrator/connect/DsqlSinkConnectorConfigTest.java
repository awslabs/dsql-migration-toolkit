// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DsqlSinkConnectorConfigTest {

  private static Map<String, String> props() {
    Map<String, String> props = new HashMap<>();
    props.put(DsqlSinkConnectorConfig.CLUSTER_ENDPOINT, "example.dsql.us-east-1.on.aws");
    props.put(DsqlSinkConnectorConfig.REGION, "us-east-1");
    return props;
  }

  @Test
  void targetKeyColumnsDefaultsToNoTable() {
    // Absent -- every stack that does not send it (MySQL/PostgreSQL with no re-keyed table,
    // an older control plane) -- and empty both mean no table: today's parse for every record.
    assertTrue(new DsqlSinkConnectorConfig(props()).targetKeyColumns().isEmpty());
    Map<String, String> empty = props();
    empty.put(DsqlSinkConnectorConfig.TARGET_KEY_COLUMNS, "");
    assertTrue(new DsqlSinkConnectorConfig(empty).targetKeyColumns().isEmpty());
  }

  @Test
  void targetKeyColumnsParsesTheControlPlaneFormat() {
    // core/cdc.format_message_key_columns: sorted entries joined by ';', the table's dots
    // escaped as '\.', columns joined by ','.
    Map<String, String> props = props();
    props.put(
        DsqlSinkConnectorConfig.TARGET_KEY_COLUMNS,
        "app\\.orders:lead,id;shop\\.line\\.items:tenant_id,order_id,line_no");
    Map<String, List<String>> keys = new DsqlSinkConnectorConfig(props).targetKeyColumns();
    assertEquals(
        Map.of(
            "app.orders", List.of("lead", "id"),
            "shop.line.items", List.of("tenant_id", "order_id", "line_no")),
        keys);
    assertEquals(List.of("app.orders", "shop.line.items"), List.copyOf(keys.keySet()));
    assertThrows(UnsupportedOperationException.class, () -> keys.put("x.y", List.of("a")));
  }

  @Test
  void malformedEntriesAreIgnored() {
    assertEquals(
        Map.of("app.t", List.of("lead", "id")),
        DsqlSinkConnectorConfig.parseTargetKeyColumns(
            "no_colon;:lead,id;app\\.empty:;app\\.t:lead,,id,;;"));
    // The FIRST ':' separates the table; a later one belongs to a column name.
    assertEquals(
        Map.of("app.t", List.of("a:b", "id")),
        DsqlSinkConnectorConfig.parseTargetKeyColumns("app\\.t:a:b,id"));
    // A table listed twice keeps its last entry.
    assertEquals(
        Map.of("app.t", List.of("c", "id")),
        DsqlSinkConnectorConfig.parseTargetKeyColumns("app\\.t:b,id;app\\.t:c,id"));
    assertTrue(DsqlSinkConnectorConfig.parseTargetKeyColumns(null).isEmpty());
  }

  @Test
  void theKeyIsDeclaredSoAnOlderPluginIsTheOnlyOneThatIgnoresIt() {
    assertTrue(DsqlSinkConnectorConfig.configDef().names().contains("target.key.columns"));
    assertEquals("target.key.columns", DsqlSinkConnectorConfig.TARGET_KEY_COLUMNS);
  }
}
