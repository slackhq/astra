package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataSerializer;
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
 * The primary POC feasibility proof: the ephemeral + partitioned case, which is the hardest and
 * most operationally troublesome metadata pattern in Astra today. Uses {@link SearchMetadata} (an
 * {@code AstraPartitionedMetadata} whose {@code getPartition()} is derived from the url suffix)
 * with {@code EtcdCreateMode.EPHEMERAL} and its existing serializer, unchanged.
 *
 * <p>Covers: (1) partitioned CRUD isolated per {@code pk}; (2) the ephemeral-liveness crux — a node
 * stays visible while heartbeating and is treated as gone once the heartbeat stops, even though
 * DynamoDB Local never physically sweeps TTL; (3) partitioned watch scoped to one partition's
 * {@code pk}.
 */
public class DynamoDbPartitioningMetadataStoreTest {
  private static final String STORE_FOLDER = "/partitioned_search";

  private TestDynamoDbFactory.Handle handle;
  private DynamoDbConfig config;
  private DynamoDbAsyncClient client;
  private DynamoDbStreamsAsyncClient streamsClient;
  private final SearchMetadataSerializer serializer = new SearchMetadataSerializer();
  private SimpleMeterRegistry meterRegistry;

  @BeforeAll
  static void beforeAll() {
    TestDynamoDbFactory.start();
  }

  @BeforeEach
  void setUp() {
    // Short ttl so the ephemeral-expiry case runs quickly.
    handle = TestDynamoDbFactory.newHandle(3_000);
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

  private DynamoDbPartitioningMetadataStore<SearchMetadata> store(
      boolean shouldCache, EtcdCreateMode createMode) {
    return new DynamoDbPartitioningMetadataStore<>(
        client,
        streamsClient,
        config,
        STORE_FOLDER,
        shouldCache,
        createMode,
        serializer,
        meterRegistry);
  }

  /**
   * SearchMetadata.getPartition() returns the url's last path segment, so vary that to partition.
   */
  private static SearchMetadata search(String name, String snapshot, String partition) {
    return new SearchMetadata(name, snapshot, "http://host/" + partition);
  }

  @Test
  void partitionedCrudIsIsolatedPerPartition() {
    try (DynamoDbPartitioningMetadataStore<SearchMetadata> store =
        store(false, EtcdCreateMode.PERSISTENT)) {
      SearchMetadata p1a = search("s_p1_a", "snap1", "p1");
      SearchMetadata p1b = search("s_p1_b", "snap2", "p1");
      SearchMetadata p2a = search("s_p2_a", "snap3", "p2");

      store.createSync(p1a);
      store.createSync(p1b);
      store.createSync(p2a);

      assertThat(store.getSync("p1", "s_p1_a")).isEqualTo(p1a);
      assertThat(store.listSync("p1").stream().map(SearchMetadata::getName).sorted().toList())
          .containsExactly("s_p1_a", "s_p1_b");
      assertThat(store.listSync("p2").stream().map(SearchMetadata::getName).toList())
          .containsExactly("s_p2_a");

      // cross-partition find locates a node by name regardless of partition
      assertThat(store.findSync("s_p2_a")).isEqualTo(p2a);
      assertThat(store.listSync().stream().map(SearchMetadata::getName).sorted().toList())
          .containsExactly("s_p1_a", "s_p1_b", "s_p2_a");
    }
  }

  /**
   * The crux: an ephemeral node stays visible while the heartbeat runs, then is treated as gone
   * once the heartbeat stops — via the read-path/cache expiry filter, since DynamoDB Local won't
   * sweep.
   */
  @Test
  void ephemeralNodeVanishesWhenHeartbeatStops() {
    try (DynamoDbPartitioningMetadataStore<SearchMetadata> store =
        store(true, EtcdCreateMode.EPHEMERAL)) {
      SearchMetadata node = search("live", "snap", "p1");
      store.createSync(node);
      store.createPartitionSync("p1");

      // Stays visible across more than one ttl window while the heartbeat bumps it.
      assertThat(store.hasSync("p1", "live")).isTrue();
      await()
          .during(Duration.ofSeconds(5))
          .atMost(Duration.ofSeconds(6))
          .untilAsserted(() -> assertThat(store.hasSync("p1", "live")).isTrue());

      // Simulate node death: stop the heartbeat. The node should expire within the ttl window.
      store.stopHeartbeatForTest("p1");
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(store.hasSync("p1", "live")).isFalse());
    }
  }

  /** A registered listener sees the ephemeral node's removal fire when it expires. */
  @Test
  void listenerFiresOnEphemeralExpiry() {
    try (DynamoDbPartitioningMetadataStore<SearchMetadata> store =
        store(true, EtcdCreateMode.EPHEMERAL)) {
      CopyOnWriteArrayList<String> changed = new CopyOnWriteArrayList<>();
      store.addListener(node -> changed.add(node.getName()));

      SearchMetadata node = search("ephemeral1", "snap", "p1");
      store.createSync(node);
      store.createPartitionSync("p1");
      assertThat(store.hasSync("p1", "ephemeral1")).isTrue();
      // The create itself fires a listener event (the Streams poller sees the INSERT); wait for
      // that to land, then clear so we only observe the expiry event below.
      await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(changed).isNotEmpty());
      changed.clear();

      store.stopHeartbeatForTest("p1");
      // The node expires: the reaper fires the removal listener and the read path reports it gone.
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(() -> assertThat(changed).contains("ephemeral1"));
      assertThat(store.hasSync("p1", "ephemeral1")).isFalse();
    }
  }

  /** Partitioned watch: a create in a partition is observed by a cached store on that partition. */
  @Test
  void partitionedWatchObservesCreate() {
    try (DynamoDbPartitioningMetadataStore<SearchMetadata> watcher =
            store(true, EtcdCreateMode.PERSISTENT);
        DynamoDbPartitioningMetadataStore<SearchMetadata> mutator =
            store(false, EtcdCreateMode.PERSISTENT)) {
      watcher.createPartitionSync("p1");

      CopyOnWriteArrayList<String> changed = new CopyOnWriteArrayList<>();
      watcher.addListener(node -> changed.add(node.getName()));

      mutator.createSync(search("w1", "snap", "p1"));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(() -> assertThat(watcher.hasSync("p1", "w1")).isTrue());
      assertThat(changed).contains("w1");

      List<String> names = watcher.listSync("p1").stream().map(SearchMetadata::getName).toList();
      assertThat(names).contains("w1");
    }
  }
}
