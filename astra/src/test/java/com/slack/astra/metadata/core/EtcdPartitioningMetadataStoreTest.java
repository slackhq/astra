package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for EtcdPartitioningMetadataStore.
 *
 * <p>These tests use the jetcd-launcher to start a real etcd server in the test process.
 */
@Tag("integration")
public class EtcdPartitioningMetadataStoreTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(EtcdPartitioningMetadataStoreTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
  private EtcdPartitioningMetadataStore<TestPartitionedMetadata> store;
  private MetadataSerializer<TestPartitionedMetadata> serializer;

  /** Test metadata class for use in tests. */
  private static class TestPartitionedMetadata extends AstraPartitionedMetadata {
    private final String data;
    private final String partition;

    public TestPartitionedMetadata(String name, String partition, String data) {
      super(name);
      this.partition = partition;
      this.data = data;
    }

    public String getData() {
      return data;
    }

    @Override
    public String getPartition() {
      return partition;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestPartitionedMetadata)) return false;
      if (!super.equals(o)) return false;

      TestPartitionedMetadata metadata = (TestPartitionedMetadata) o;
      if (!data.equals(metadata.data)) return false;
      return partition.equals(metadata.partition);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + data.hashCode();
      result = 31 * result + partition.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "TestPartitionedMetadata{"
          + "name='"
          + name
          + '\''
          + ", partition='"
          + partition
          + '\''
          + ", data='"
          + data
          + '\''
          + '}';
    }
  }

  /** Serializer for TestPartitionedMetadata objects. */
  private static class TestMetadataSerializer
      implements MetadataSerializer<TestPartitionedMetadata> {
    @Override
    public String toJsonStr(TestPartitionedMetadata metadata) {
      return String.format(
          "{\"name\":\"%s\",\"partition\":\"%s\",\"data\":\"%s\"}",
          metadata.getName(), metadata.getPartition(), metadata.getData());
    }

    @Override
    public TestPartitionedMetadata fromJsonStr(String data) {
      // Very simple JSON parsing for test purposes
      String name = data.split("\"name\":\"")[1].split("\"")[0];
      String partition = data.split("\"partition\":\"")[1].split("\"")[0];
      String value = data.split("\"data\":\"")[1].split("\"")[0];
      return new TestPartitionedMetadata(name, partition, value);
    }
  }

  @BeforeAll
  public static void setUpClass() {
    // Start an embedded etcd server
    LOG.info("Starting embedded etcd cluster");
    etcdCluster = TestEtcdClusterFactory.start();
    LOG.info(
        "Embedded etcd cluster started with endpoints: {}",
        etcdCluster.clientEndpoints().stream().map(Object::toString).toList());
  }

  @AfterAll
  public static void tearDownClass() {
    if (etcdCluster != null) {
      LOG.info("Stopping embedded etcd cluster");

      LOG.info("Embedded etcd cluster stopped");
    }
  }

  @BeforeEach
  public void setUp() {
    // Set up a new meter registry and test metadata serializer for each test
    meterRegistry = new SimpleMeterRegistry();
    serializer = new TestMetadataSerializer();

    // Configure etcd
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .build();

    // Build the etcd client
    Client etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .connectTimeout(Duration.ofMillis(5000))
            .keepaliveTimeout(Duration.ofMillis(3000))
            .retryMaxAttempts(3)
            .retryDelay(100)
            .namespace(ByteSequence.from("test", StandardCharsets.UTF_8))
            .build();

    // Create store with cache enabled
    store =
        new EtcdPartitioningMetadataStore<>(
            etcdClient, etcdConfig, meterRegistry, EtcdCreateMode.PERSISTENT, serializer, "/test");
    store.listSyncUncached().forEach(s -> store.deleteSync(s));
  }

  @AfterEach
  public void tearDown() throws IOException {
    // Close the store and meter registry
    store.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateAndGet() throws ExecutionException, InterruptedException {
    // Create a test metadata object
    String partition = "partition1";
    TestPartitionedMetadata testData =
        new TestPartitionedMetadata("testCreateAndGet1", partition, "testData");

    // Create the node
    String path = store.createAsync(testData).toCompletableFuture().get();
    assertThat(path).isEqualTo("testCreateAndGet1");

    // Get the node
    TestPartitionedMetadata result =
        store.getAsync(partition, "testCreateAndGet1").toCompletableFuture().get();
    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo("testCreateAndGet1");
    assertThat(result.getPartition()).isEqualTo(partition);
    assertThat(result.getData()).isEqualTo("testData");
  }

  @Test
  public void testFindWithoutPartition() throws ExecutionException, InterruptedException {
    // Create test metadata objects with different partitions
    TestPartitionedMetadata testData1 =
        new TestPartitionedMetadata("testFindWithoutPartition1", "partition1", "data1");
    TestPartitionedMetadata testData2 =
        new TestPartitionedMetadata("testFindWithoutPartition2", "partition2", "data2");

    store.createSync(testData1);
    store.createSync(testData2);

    // Find the items without specifying the partition
    TestPartitionedMetadata result1 =
        store.findAsync("testFindWithoutPartition1").toCompletableFuture().get();
    assertThat(result1).isNotNull();
    assertThat(result1.getName()).isEqualTo("testFindWithoutPartition1");
    assertThat(result1.getPartition()).isEqualTo("partition1");

    TestPartitionedMetadata result2 = store.findSync("testFindWithoutPartition2");
    assertThat(result2).isNotNull();
    assertThat(result2.getName()).isEqualTo("testFindWithoutPartition2");
    assertThat(result2.getPartition()).isEqualTo("partition2");
  }

  @Test
  public void testUpdateNode() throws ExecutionException, InterruptedException {
    // Create a test metadata object
    String partition = "partition1";
    TestPartitionedMetadata testData =
        new TestPartitionedMetadata("testUpdateNode1", partition, "initialData");
    store.createSync(testData);

    // Create updated version with same name and partition but different data
    TestPartitionedMetadata updatedData =
        new TestPartitionedMetadata("testUpdateNode1", partition, "updatedData");

    // Update the node
    String nodeName = store.updateAsync(updatedData).toCompletableFuture().get();
    assertThat(nodeName).isEqualTo("testUpdateNode1");

    // Get the updated node
    TestPartitionedMetadata result = store.getSync(partition, "testUpdateNode1");
    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo("testUpdateNode1");
    assertThat(result.getPartition()).isEqualTo(partition);
    assertThat(result.getData()).isEqualTo("updatedData");
  }

  @Test
  public void testDeleteNode() throws ExecutionException, InterruptedException {
    // Create test metadata objects
    String partition = "partition1";
    TestPartitionedMetadata testData1 =
        new TestPartitionedMetadata("testDeleteNode1", partition, "data1");
    TestPartitionedMetadata testData2 =
        new TestPartitionedMetadata("testDeleteNode2", partition, "data2");

    store.createSync(testData1);
    store.createSync(testData2);

    // Delete first node by reference
    store.deleteAsync(testData1).toCompletableFuture().get();

    // Verify first is deleted but second still exists
    boolean firstNodeExists = false;
    try {
      store.getSync(partition, "testDeleteNode1");
      firstNodeExists = true;
    } catch (RuntimeException e) {
      // Expected exception
      assertThat(e.getMessage()).contains("Node not found");
    }
    assertThat(firstNodeExists).isFalse();

    // Second node should still exist
    TestPartitionedMetadata result = store.getSync(partition, "testDeleteNode2");
    assertThat(result).isNotNull();

    // Delete second node by reference
    store.deleteSync(testData2);

    // Verify second is now deleted
    boolean secondNodeExists = false;
    try {
      store.getSync(partition, "testDeleteNode2");
      secondNodeExists = true;
    } catch (RuntimeException e) {
      // Expected exception
      assertThat(e.getMessage()).contains("Node not found");
    }
    assertThat(secondNodeExists).isFalse();
  }

  @Test
  public void testListNodes() throws ExecutionException, InterruptedException {
    store.listSyncUncached().forEach(s -> store.deleteSync(s));

    // Create test metadata objects across different partitions
    TestPartitionedMetadata testData1 =
        new TestPartitionedMetadata("testListNodes1", "partition1", "data1");
    TestPartitionedMetadata testData2 =
        new TestPartitionedMetadata("testListNodes2", "partition1", "data2");
    TestPartitionedMetadata testData3 =
        new TestPartitionedMetadata("testListNodes3", "partition2", "data3");

    store.createSync(testData1);
    store.createSync(testData2);
    store.createSync(testData3);

    // List async
    await().until(() -> store.listSync().size() == 3);
    List<TestPartitionedMetadata> nodes = store.listAsync().toCompletableFuture().get();

    // Verify all nodes are in the list
    assertThat(nodes)
        .extracting(TestPartitionedMetadata::getName)
        .containsExactlyInAnyOrder("testListNodes1", "testListNodes2", "testListNodes3");

    // List sync
    nodes = store.listSync();
    assertThat(nodes).hasSize(3);

    // Extract items by partition to verify partitioning is working
    Map<String, List<TestPartitionedMetadata>> partitionMap =
        nodes.stream().collect(Collectors.groupingBy(TestPartitionedMetadata::getPartition));

    assertThat(partitionMap).hasSize(2);
    assertThat(partitionMap.get("partition1")).hasSize(2);
    assertThat(partitionMap.get("partition2")).hasSize(1);
  }

  @Test
  public void testListeners() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    AstraMetadataStoreChangeListener<TestPartitionedMetadata> listener =
        unused -> counter.incrementAndGet();

    // Add listener
    store.addListener(listener);

    // Wait for listener to be registered
    TimeUnit.MILLISECONDS.sleep(200);

    // Initial count should be 0
    assertThat(counter.get()).isEqualTo(0);

    // Create a node - should trigger listener
    String partition = "partition1";
    TestPartitionedMetadata testData =
        new TestPartitionedMetadata("testListeners1", partition, "data");
    store.createSync(testData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 1);

    // Update node - should trigger listener again
    TestPartitionedMetadata updatedData =
        new TestPartitionedMetadata("testListeners1", partition, "updated");
    store.updateSync(updatedData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 2);

    // Remove listener
    store.removeListener(listener);
    TimeUnit.MILLISECONDS.sleep(200);

    // Update node again - should not trigger listener
    TestPartitionedMetadata updatedData2 =
        new TestPartitionedMetadata("testListeners1", partition, "updated again");
    store.updateSync(updatedData2);

    // Wait a bit to see if counter changes
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(counter.get()).isEqualTo(2);
  }

  @Test
  public void testCacheInitialization()
      throws IOException, ExecutionException, InterruptedException {
    // First, make sure our own store is aware of the partitions
    store.awaitCacheInitialized();

    // Create test data across different partitions
    TestPartitionedMetadata testData1 =
        new TestPartitionedMetadata("testCacheInitialization1", "partition1", "data1");
    TestPartitionedMetadata testData2 =
        new TestPartitionedMetadata("testCacheInitialization2", "partition2", "data2");

    // Create the data in our store
    store.createSync(testData1);
    store.createSync(testData2);

    // Verify data was created
    TestPartitionedMetadata result1 = store.getSync("partition1", "testCacheInitialization1");
    TestPartitionedMetadata result2 = store.getSync("partition2", "testCacheInitialization2");
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();

    // Verify we can list the data
    List<TestPartitionedMetadata> items = store.listSync();
    List<TestPartitionedMetadata> filteredItems =
        items.stream()
            .filter(
                item ->
                    item.getName().equals("testCacheInitialization1")
                        || item.getName().equals("testCacheInitialization2"))
            .toList();
    assertThat(filteredItems).hasSize(2);

    // Now create a new store with the same parameters and verify it can see the data
    Client newEtcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .connectTimeout(Duration.ofMillis(5000))
            .keepaliveTimeout(Duration.ofMillis(3000))
            .retryMaxAttempts(3)
            .retryDelay(100)
            .namespace(ByteSequence.from("test", StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .build();

    // Use try-with-resources to ensure proper closing
    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> newStore =
        new EtcdPartitioningMetadataStore<>(
            newEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test")) {

      // Initialize the cache
      newStore.awaitCacheInitialized();

      // Get the node to verify it exists in the new store
      TestPartitionedMetadata verifyItem1 =
          newStore.getSync("partition1", "testCacheInitialization1");
      TestPartitionedMetadata verifyItem2 =
          newStore.getSync("partition2", "testCacheInitialization2");

      assertThat(verifyItem1).isNotNull();
      assertThat(verifyItem1.getName()).isEqualTo("testCacheInitialization1");
      assertThat(verifyItem1.getData()).isEqualTo("data1");

      assertThat(verifyItem2).isNotNull();
      assertThat(verifyItem2.getName()).isEqualTo("testCacheInitialization2");
      assertThat(verifyItem2.getData()).isEqualTo("data2");

      // Verify we can list all the data
      List<TestPartitionedMetadata> cachedItems = newStore.listSync();
      List<TestPartitionedMetadata> testItems =
          cachedItems.stream()
              .filter(
                  item ->
                      item.getName().equals("testCacheInitialization1")
                          || item.getName().equals("testCacheInitialization2"))
              .toList();

      assertThat(testItems).hasSize(2);
      assertThat(testItems)
          .extracting(TestPartitionedMetadata::getName)
          .containsExactlyInAnyOrder("testCacheInitialization1", "testCacheInitialization2");

      // Verify partitioning is preserved
      Map<String, List<TestPartitionedMetadata>> partitionMap =
          testItems.stream().collect(Collectors.groupingBy(TestPartitionedMetadata::getPartition));

      assertThat(partitionMap).hasSize(2);
      assertThat(partitionMap.get("partition1")).hasSize(1);
      assertThat(partitionMap.get("partition2")).hasSize(1);
    }
  }
}
