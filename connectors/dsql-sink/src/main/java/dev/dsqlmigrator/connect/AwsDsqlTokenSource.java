// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/*
 * Custom Aurora DSQL Kafka Connect sink connector.
 * AWS SDK adapter for DSQL IAM auth-token generation. This is the only class
 * that touches the AWS SDK; the token policy/caching lives in
 * DsqlIamTokenProvider and is unit-tested without AWS.
 */
package dev.dsqlmigrator.connect;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dsql.DsqlUtilities;

/**
 * Default {@link DsqlIamTokenProvider.TokenSource} backed by the AWS SDK v2
 * {@link DsqlUtilities}. Mirrors the Python token logic in
 * {@code core/target_connection.py}: the {@code admin} role uses the admin
 * auth-token API, any other role uses the standard db-connect token API. The
 * default credentials chain (the connector execution role) is used — no secret
 * is stored (Property 7).
 */
final class AwsDsqlTokenSource implements DsqlIamTokenProvider.TokenSource {

  static final String ADMIN_USERNAME = "admin";

  // DSQL token generation requires an explicit credentials provider — the SDK
  // does NOT fall back to the default chain for DsqlUtilities. Without this the
  // MSK Connect worker fails with "CredentialsProvider must be provided in
  // GenerateAuthTokenRequest or DsqlUtilities". DefaultCredentialsProvider
  // resolves the connector execution role (no stored secret — Property 7).
  private static final AwsCredentialsProvider CREDENTIALS =
      DefaultCredentialsProvider.create();

  @Override
  public String generate(String hostname, String region, String username) {
    Region awsRegion = Region.of(region);
    DsqlUtilities utilities =
        DsqlUtilities.builder().region(awsRegion).credentialsProvider(CREDENTIALS).build();
    if (ADMIN_USERNAME.equals(username)) {
      return utilities.generateDbConnectAdminAuthToken(
          builder -> builder.hostname(hostname).region(awsRegion));
    }
    return utilities.generateDbConnectAuthToken(
        builder -> builder.hostname(hostname).region(awsRegion));
  }
}
