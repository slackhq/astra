package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.dataset.DatasetMetadata;
import com.slack.astra.metadata.dataset.DatasetMetadataSerializer;
import com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig;
import com.slack.astra.testlib.TestDynamoDbFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

/**
 * POC proof for the persistent, non-partitioned case: baseline CRUD + list on the DynamoDB store,
 * plus the Streams-driven watch. Uses {@link DatasetMetadata} (a persistent, non-partitioned type)
 * and its existing serializer, unchanged.
 */
public class DynamoDbMetadataStoreTest {
  private static final String STORE_FOLDER = "/service";

  private TestDynamoDbFactory.Handle handle;
  private DynamoDbConfig config;
  private DynamoDbAsyncClient client;
  private DynamoDbStreamsAsyncClient streamsClient;
  private final DatasetMetadataSerializer serializer = new DatasetMetadataSerializer();
  private SimpleMeterRegistry meterRegistry;

  @BeforeAll
  static void beforeAll() {
    TestDynamoDbFactory.start();
  }

  @BeforeEach
  void setUp() {
    handle = TestDynamoDbFactory.newHandle(60_000);
    config = handle.config();
    client = handle.client();
    streamsClient = handle.streamsClient();
    meterRegistry = new SimpleMeterRegistry();
  }

  @AfterEach
  void tearDown() {
    if (handle != null) {
      handle.close();
    }
  }

  private DynamoDbMetadataStore<DatasetMetadata> store(boolean shouldCache) {
    return new DynamoDbMetadataStore<>(
        client,
        streamsClient,
        config,
        STORE_FOLDER,
        shouldCache,
        EtcdCreateMode.PERSISTENT,
        serializer,
        meterRegistry);
  }

  private static DatasetMetadata dataset(String name) {
    return new DatasetMetadata(name, "owner", 100, List.of(), name + "Service");
  }

  @Test
  void createGetUpdateDelete() {
    try (DynamoDbMetadataStore<DatasetMetadata> store = store(false)) {
      DatasetMetadata node = dataset("ds1");

      store.createSync(node);
      assertThat(store.getSync("ds1")).isEqualTo(node);
      assertThat(store.hasSync("ds1")).isTrue();

      DatasetMetadata updated = new DatasetMetadata("ds1", "owner2", 200, List.of(), "ds1Service");
      store.updateSync(updated);
      assertThat(store.getSync("ds1").getOwner()).isEqualTo("owner2");
      assertThat(store.getSync("ds1").getThroughputBytes()).isEqualTo(200);

      store.deleteSync("ds1");
      assertThat(store.hasSync("ds1")).isFalse();
    }
  }

  @Test
  void createExistingThrows() {
    try (DynamoDbMetadataStore<DatasetMetadata> store = store(false)) {
      store.createSync(dataset("dup"));
      assertThatThrownBy(() -> store.createSync(dataset("dup")))
          .isInstanceOf(InternalMetadataStoreException.class);
    }
  }

  @Test
  void getMissingThrows() {
    try (DynamoDbMetadataStore<DatasetMetadata> store = store(false)) {
      assertThatThrownBy(() -> store.getSync("nope"))
          .isInstanceOf(InternalMetadataStoreException.class);
      assertThat(store.hasSync("nope")).isFalse();
    }
  }

  @Test
  void listReturnsAllUnderPartition() {
    try (DynamoDbMetadataStore<DatasetMetadata> store = store(false)) {
      store.createSync(dataset("a"));
      store.createSync(dataset("b"));
      store.createSync(dataset("c"));

      List<String> names =
          store.listSync().stream().map(DatasetMetadata::getName).sorted().toList();
      assertThat(names).containsExactly("a", "b", "c");
    }
  }

  @Test
  void cachedListConvergesAfterInitialQuery() {
    // Seed via a non-cached store, then open a cached store and confirm it loads the seed.
    try (DynamoDbMetadataStore<DatasetMetadata> seed = store(false)) {
      seed.createSync(dataset("seed1"));
      seed.createSync(dataset("seed2"));
    }
    try (DynamoDbMetadataStore<DatasetMetadata> cached = store(true)) {
      cached.awaitCacheInitialized();
      assertThat(cached.listSync().stream().map(DatasetMetadata::getName).sorted().toList())
          .containsExactly("seed1", "seed2");
    }
  }

  @Test
  void watchFiresOnExternalMutations() {
    try (DynamoDbMetadataStore<DatasetMetadata> watcher = store(true);
        DynamoDbMetadataStore<DatasetMetadata> mutator = store(false)) {
      watcher.awaitCacheInitialized();

      CopyOnWriteArrayList<String> events = new CopyOnWriteArrayList<>();
      watcher.addListener(node -> events.add(node.getName()));

      // Create via a second store; the watcher's Streams poller should observe it.
      mutator.createSync(dataset("watched"));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertThat(watcher.hasSync("watched")).isTrue());
      assertThat(events).contains("watched");

      // Delete via the second store; the watcher should converge to absent.
      mutator.deleteSync("watched");
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertThat(watcher.hasSync("watched")).isFalse());
    }
  }
}
