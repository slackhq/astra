package com.slack.astra.metadata.core;

import static com.slack.astra.util.ArgValidationUtils.ensureTrue;

import com.google.common.base.Strings;
import com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

// Note: DynamoDbStreamsAsyncClient ships inside the software.amazon.awssdk:dynamodb artifact.

/**
 * Builder for the DynamoDB clients used by the DynamoDB metadata store (POC).
 *
 * <p>Mirrors {@link EtcdClientBuilder} in structure and the credential/region/endpoint-override
 * idiom used by {@code S3AsyncUtil#initS3Client}: static credentials are used when both an access
 * key and secret key are configured, otherwise the {@link DefaultCredentialsProvider} chain is
 * used. An endpoint override lets tests point the client at DynamoDB Local.
 */
public class DynamoDbClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbClientBuilder.class);

  private DynamoDbClientBuilder() {}

  /** Builds the async DynamoDB client used for item operations. */
  public static DynamoDbAsyncClient build(DynamoDbConfig config) {
    ensureTrue(config.getEnabled(), "DynamoDB must be enabled to build a client");
    ensureTrue(!Strings.isNullOrEmpty(config.getRegion()), "A DynamoDB region must be provided");

    var builder =
        DynamoDbAsyncClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(credentialsProvider(config));

    applyEndpointOverride(config, builder::endpointOverride);
    applyOverrideConfiguration(config, builder::overrideConfiguration);

    LOG.info(
        "Started DynamoDB client with region: {}, table: {}, endpoint: {}",
        config.getRegion(),
        config.getTableName(),
        Strings.isNullOrEmpty(config.getEndpoint()) ? "<default>" : config.getEndpoint());

    return builder.build();
  }

  /** Builds the async DynamoDB Streams client used to drive watch/cache updates. */
  public static DynamoDbStreamsAsyncClient buildStreamsClient(DynamoDbConfig config) {
    ensureTrue(config.getEnabled(), "DynamoDB must be enabled to build a streams client");
    ensureTrue(!Strings.isNullOrEmpty(config.getRegion()), "A DynamoDB region must be provided");

    var builder =
        DynamoDbStreamsAsyncClient.builder()
            .region(Region.of(config.getRegion()))
            .credentialsProvider(credentialsProvider(config));

    applyEndpointOverride(config, builder::endpointOverride);
    applyOverrideConfiguration(config, builder::overrideConfiguration);

    return builder.build();
  }

  private static AwsCredentialsProvider credentialsProvider(DynamoDbConfig config) {
    if (!Strings.isNullOrEmpty(config.getAccessKey())
        && !Strings.isNullOrEmpty(config.getSecretKey())) {
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey()));
    }
    return DefaultCredentialsProvider.create();
  }

  private static void applyEndpointOverride(DynamoDbConfig config, EndpointConsumer consumer) {
    if (!Strings.isNullOrEmpty(config.getEndpoint())) {
      try {
        consumer.accept(new URI(config.getEndpoint()));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Invalid DynamoDB endpoint: " + config.getEndpoint(), e);
      }
    }
  }

  private static void applyOverrideConfiguration(DynamoDbConfig config, OverrideConsumer consumer) {
    // POC keeps overrides minimal; only the (stable) api-call timeout is applied. Retry tuning is
    // deferred to the follow-up production work.
    if (config.getOperationsTimeoutMs() > 0) {
      consumer.accept(
          ClientOverrideConfiguration.builder()
              .apiCallTimeout(Duration.ofMillis(config.getOperationsTimeoutMs()))
              .build());
    }
  }

  @FunctionalInterface
  private interface EndpointConsumer {
    void accept(URI uri);
  }

  @FunctionalInterface
  private interface OverrideConsumer {
    void accept(ClientOverrideConfiguration configuration);
  }
}
