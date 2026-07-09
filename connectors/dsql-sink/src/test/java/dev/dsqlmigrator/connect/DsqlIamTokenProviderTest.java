// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package dev.dsqlmigrator.connect;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DsqlIamTokenProviderTest {

  @Test
  void cachesUntilRefreshBoundaryThenRegenerates() {
    AtomicInteger calls = new AtomicInteger();
    DsqlIamTokenProvider.TokenSource source = (host, region, user) -> "token-" + calls.incrementAndGet();
    AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2025-01-01T00:00:00Z"));

    DsqlIamTokenProvider provider =
        new DsqlIamTokenProvider("host", "us-east-1", "admin", source, now::get);

    assertEquals("token-1", provider.currentToken());

    // Still within TTL minus margin (refreshAt = +13min): cached, no new call.
    now.set(Instant.parse("2025-01-01T00:01:00Z"));
    assertEquals("token-1", provider.currentToken());
    assertEquals(1, calls.get());

    // Past the refresh boundary: regenerate.
    now.set(Instant.parse("2025-01-01T00:14:00Z"));
    assertEquals("token-2", provider.currentToken());
    assertEquals(2, calls.get());
  }

  @Test
  void passesHostRegionUsernameToSource() {
    AtomicReference<String> seen = new AtomicReference<>();
    DsqlIamTokenProvider.TokenSource source =
        (host, region, user) -> {
          seen.set(host + "|" + region + "|" + user);
          return "t";
        };
    DsqlIamTokenProvider provider =
        new DsqlIamTokenProvider("myhost", "eu-west-1", "app", source, Instant::now);
    provider.currentToken();
    assertEquals("myhost|eu-west-1|app", seen.get());
  }
}
