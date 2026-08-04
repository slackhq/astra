package com.slack.astra.testlib;

import com.slack.astra.metadata.core.DynamoDbClientBuilder;
import com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

/**
 * Manages a singleton DynamoDB Local container for the metadata-store POC tests, mirroring {@link
 * TestEtcdClusterFactory}. The container is reused across test classes to avoid repeated startup
 * cost; each test gets a freshly-created table (via a unique name from {@link #createTable}).
 *
 * <p>Uses the {@code amazon/dynamodb-local} image with {@code -sharedDb} so the endpoint behaves
 * like a single shared account. Requires Docker to be available.
 */
public class TestDynamoDbFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestDynamoDbFactory.class);
  private static final int DYNAMODB_PORT = 8000;
  private static final long OP_TIMEOUT_MS = 30_000;

  private static GenericContainer<?> container;
  private static boolean initialized = false;

  public static synchronized void start() {
    if (initialized) {
      return;
    }
    LOG.info("Starting DynamoDB Local container");
    container =
        new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:2.5.2"))
            .withExposedPorts(DYNAMODB_PORT)
            .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb");
    container.start();
    initialized = true;
    Runtime.getRuntime().addShutdownHook(new Thread(TestDynamoDbFactory::close));
  }

  public static synchronized void close() {
    if (container != null) {
      container.stop();
      container = null;
    }
    initialized = false;
  }

  public static String endpoint() {
    if (!initialized) {
      throw new IllegalStateException("DynamoDB Local is not initialized");
    }
    return "http://" + container.getHost() + ":" + container.getMappedPort(DYNAMODB_PORT);
  }

  /**
   * Builds a config pointing at the running container, with a unique table name so each test gets
   * an isolated table.
   *
   * @param ephemeralTtlMs ttl for ephemeral nodes (ms); pass a small value to exercise expiry.
   */
  public static DynamoDbConfig config(int ephemeralTtlMs) {
    return DynamoDbConfig.newBuilder()
        .setEnabled(true)
        .setTableName("astra_metadata_" + UUID.randomUUID().toString().replace("-", ""))
        .setRegion("us-east-1")
        .setEndpoint(endpoint())
        .setAccessKey("fake")
        .setSecretKey("fake")
        .setEphemeralNodeTtlMs(ephemeralTtlMs)
        .setOperationsTimeoutMs(15_000)
        .build();
  }

  public static DynamoDbAsyncClient createClient(DynamoDbConfig config) {
    return DynamoDbClientBuilder.build(config);
  }

  public static DynamoDbStreamsAsyncClient createStreamsClient(DynamoDbConfig config) {
    return DynamoDbClientBuilder.buildStreamsClient(config);
  }

  /**
   * Creates the single metadata table for a test: {@code pk} (HASH) + {@code sk} (RANGE), on-demand
   * billing, with DynamoDB Streams ({@code NEW_AND_OLD_IMAGES}) enabled so the watch poller has a
   * stream to read. Blocks until the table is ACTIVE.
   */
  public static void createTable(DynamoDbAsyncClient client, String tableName) {
    try {
      client
          .createTable(
              b ->
                  b.tableName(tableName)
                      .billingMode(BillingMode.PAY_PER_REQUEST)
                      .attributeDefinitions(
                          AttributeDefinition.builder()
                              .attributeName("pk")
                              .attributeType(ScalarAttributeType.S)
                              .build(),
                          AttributeDefinition.builder()
                              .attributeName("sk")
                              .attributeType(ScalarAttributeType.S)
                              .build())
                      .keySchema(
                          KeySchemaElement.builder()
                              .attributeName("pk")
                              .keyType(KeyType.HASH)
                              .build(),
                          KeySchemaElement.builder()
                              .attributeName("sk")
                              .keyType(KeyType.RANGE)
                              .build())
                      .streamSpecification(
                          s ->
                              s.streamEnabled(true)
                                  .streamViewType(StreamViewType.NEW_AND_OLD_IMAGES)))
          .get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);

      client
          .waiter()
          .waitUntilTableExists(b -> b.tableName(tableName))
          .get(OP_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted creating table " + tableName, e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create table " + tableName, e);
    }
  }

  /** Convenience: build config, client, and create its table in one shot. */
  public static Handle newHandle(int ephemeralTtlMs) {
    start();
    DynamoDbConfig config = config(ephemeralTtlMs);
    DynamoDbAsyncClient client = createClient(config);
    DynamoDbStreamsAsyncClient streamsClient = createStreamsClient(config);
    createTable(client, config.getTableName());
    return new Handle(config, client, streamsClient);
  }

  /** Bundles a per-test config + its clients; close to release the SDK clients. */
  public record Handle(
      DynamoDbConfig config, DynamoDbAsyncClient client, DynamoDbStreamsAsyncClient streamsClient) {
    public void close() {
      client.close();
      streamsClient.close();
    }

    public URI endpointUri() {
      return URI.create(config.getEndpoint());
    }
  }
}
