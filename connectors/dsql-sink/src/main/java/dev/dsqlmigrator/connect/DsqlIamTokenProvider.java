/*
 * Task 23.2 — custom Aurora DSQL Kafka Connect sink connector.
 * Implemented and offline unit-tested. Live MSK Connect + DSQL load validation
 * (cdc-connector-spike.md, H1) is still pending: see README.md.
 */
package dev.dsqlmigrator.connect;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

/**
 * Generates and caches Aurora DSQL IAM auth tokens for JDBC connections.
 *
 * <p>Mirrors the Python {@code core/target_connection.py} token logic: the IAM
 * token (used as the JDBC password) is short-lived (~15 min), so it is cached
 * and regenerated before expiry. Spike H1 validates auto-refresh across the
 * token-expiry and 1-hour connection-timeout boundaries inside the MSK Connect
 * worker. Uses the connector execution role's IAM identity (no stored secret —
 * Property 7).
 *
 * <p>The token-generation call and the clock are injected through {@link
 * TokenSource} and a {@link Supplier} of {@link Instant}, so the refresh policy
 * is unit-tested without touching AWS or the system clock. The production
 * default uses {@link AwsDsqlTokenSource}.
 */
public class DsqlIamTokenProvider {

  /** Seam for generating a DSQL IAM auth token. Injectable for tests. */
  @FunctionalInterface
  public interface TokenSource {
    String generate(String hostname, String region, String username);
  }

  /** DSQL IAM tokens are short-lived; 15 min is the DSQL default. */
  static final Duration TOKEN_TTL = Duration.ofMinutes(15);

  /** Regenerate this long before nominal expiry so an in-flight connect never
   * races a just-expired token (mirrors the Python 60s margin, scaled up). */
  static final Duration REFRESH_MARGIN = Duration.ofMinutes(2);

  private final String hostname;
  private final String region;
  private final String username;
  private final TokenSource source;
  private final Supplier<Instant> clock;

  private String cachedToken;
  private Instant refreshAt = Instant.EPOCH;

  /** Production constructor: generate tokens via the AWS SDK, real clock. */
  public DsqlIamTokenProvider(String hostname, String region, String username) {
    this(hostname, region, username, new AwsDsqlTokenSource(), Instant::now);
  }

  /** Test/seam constructor: injectable token source and clock. */
  DsqlIamTokenProvider(
      String hostname,
      String region,
      String username,
      TokenSource source,
      Supplier<Instant> clock) {
    this.hostname = hostname;
    this.region = region;
    this.username = username;
    this.source = source;
    this.clock = clock;
  }

  /** Return a valid token, regenerating it when expired/near expiry. */
  public synchronized String currentToken() {
    Instant now = clock.get();
    if (cachedToken != null && now.isBefore(refreshAt)) {
      return cachedToken;
    }
    String token = source.generate(hostname, region, username);
    this.cachedToken = token;
    this.refreshAt = now.plus(TOKEN_TTL).minus(REFRESH_MARGIN);
    return token;
  }
}
