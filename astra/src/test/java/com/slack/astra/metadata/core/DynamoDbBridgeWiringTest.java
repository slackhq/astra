package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.proto.config.AstraConfigs.MetadataStoreConfig;
import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import com.slack.astra.testlib.TestDynamoDbFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Proves the config-driven selection wiring end-to-end (without a kind cluster): when a store's
 * mode is {@code DYNAMODB_CREATES} and only a DynamoDB client is supplied (zk/etcd null), the
 * concrete {@link DatasetMetadataStore} / {@link SearchMetadataStore} route every operation through
 * the bridge's exclusive DynamoDB delegate.
 */
public class DynamoDbBridgeWiringTest {
  private TestDynamoDbFactory.Handle handle;
  private SimpleMeterRegistry meterRegistry;

  @BeforeAll
  static void beforeAll() {
    TestDynamoDbFactory.start();
  }

  @BeforeEach
  void setUp() {
    handle = TestDynamoDbFactory.newHandle(3_000);
    meterRegistry = new SimpleMeterRegistry();
  }

  @AfterEach
  void tearDown() {
    if (handle != null) {
      handle.close();
    }
  }

  /** Config selecting both sliced stores for DynamoDB, carrying the container-backed config. */
  private MetadataStoreConfig config() {
    return MetadataStoreConfig.newBuilder()
        .setDynamodbConfig(handle.config())
        .putStoreModes("DatasetMetadataStore", MetadataStoreMode.DYNAMODB_CREATES)
        .putStoreModes("SearchMetadataStore", MetadataStoreMode.DYNAMODB_CREATES)
        .build();
  }

  @Test
  void datasetStoreRoutesThroughDynamoBridge() {
    try (DatasetMetadataStore store =
        new DatasetMetadataStore(
            null, null, handle.client(), handle.streamsClient(), config(), meterRegistry, true)) {
      store.awaitCacheInitialized();

      DatasetMetadata dataset =
          new DatasetMetadata("wiring_ds", "owner", 100, List.of(), "wiring_ds");
      store.createSync(dataset);

      assertThat(store.getSync("wiring_ds")).isEqualTo(dataset);
      assertThat(store.listSync().stream().map(DatasetMetadata::getName).toList())
          .containsExactly("wiring_ds");

      store.deleteSync(dataset);
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(store.hasSync("wiring_ds")).isFalse());
    }
  }

  @Test
  void searchStoreRoutesThroughDynamoBridge() {
    try (SearchMetadataStore store =
        new SearchMetadataStore(
            null, null, handle.client(), handle.streamsClient(), config(), meterRegistry, true)) {
      // Ephemeral + partitioned: getPartition() is the url's last path segment.
      SearchMetadata node = new SearchMetadata("wiring_search", "snap", "http://host/p1");
      store.createSync(node);
      store.createPartitionSync("p1");

      assertThat(store.getSync("p1", "wiring_search")).isEqualTo(node);
      assertThat(store.hasSync("p1", "wiring_search")).isTrue();

      // Ephemeral liveness: stays visible while heartbeating, gone once the heartbeat stops.
      await()
          .during(Duration.ofSeconds(4))
          .atMost(Duration.ofSeconds(6))
          .untilAsserted(() -> assertThat(store.hasSync("p1", "wiring_search")).isTrue());
    }
  }
}
