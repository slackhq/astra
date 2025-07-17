package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for AstraPartitioningMetadataStore that validate its behavior across the operational modes:
 * - ZOOKEEPER_CREATES - ETCD_CREATES
 */
@Tag("integration")
public class AstraPartitioningMetadataStoreTest {

  private static final Logger LOG =
      LoggerFactory.getLogger(AstraPartitioningMetadataStoreTest.class);

  private static TestingServer zkServer;
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;

  // ZK components
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.ZookeeperConfig zkConfig;

  // Etcd components
  private AstraConfigs.EtcdConfig etcdConfig;
  private Client etcdClient;

  /** Test metadata implementation for testing AstraPartitioningMetadataStore. */
  private static class TestPartitionedMetadata extends AstraPartitionedMetadata {
    private final String value;
    private final String partition;

    public TestPartitionedMetadata(String name, String partition, String value) {
      super(name);
      this.partition = partition;
      this.value = value;
    }

    public String getValue() {
      return value;
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
      if (!Objects.equals(value, metadata.value)) return false;
      return Objects.equals(partition, metadata.partition);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (partition != null ? partition.hashCode() : 0);
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
          + ", value='"
          + value
          + '\''
          + '}';
    }
  }

  /** Serializer for TestPartitionedMetadata objects. */
  static class TestMetadataSerializer
      implements MetadataSerializer<TestPartitionedMetadata>,
          ModelSerializer<TestPartitionedMetadata> {
    @Override
    public String toJsonStr(TestPartitionedMetadata metadata) {
      String json =
          String.format(
              "{\"name\":\"%s\",\"partition\":\"%s\",\"value\":\"%s\"}",
              metadata.getName(), metadata.getPartition(), metadata.getValue());
      LOG.info("Serializing to JSON: {} -> {}", metadata, json);
      return json;
    }

    @Override
    public TestPartitionedMetadata fromJsonStr(String data) {
      LOG.info("Deserializing JSON: {}", data);
      // Very simple JSON parsing for test purposes
      String name = data.split("\"name\":\"")[1].split("\"")[0];
      String partition = data.split("\"partition\":\"")[1].split("\"")[0];
      String value = data.split("\"value\":\"")[1].split("\"")[0];
      TestPartitionedMetadata metadata = new TestPartitionedMetadata(name, partition, value);
      LOG.info(
          "Deserialized metadata: {}, partition: {}", metadata.getName(), metadata.getPartition());
      return metadata;
    }

    @Override
    public byte[] serialize(TestPartitionedMetadata instance) {
      return toJsonStr(instance).getBytes();
    }

    @Override
    public TestPartitionedMetadata deserialize(byte[] bytes) {
      return fromJsonStr(new String(bytes));
    }
  }

  @BeforeAll
  public static void setUpClass() throws Exception {
    // Start ZooKeeper test server
    LOG.info("Starting ZooKeeper test server");
    zkServer = new TestingServer();
    LOG.info("ZooKeeper test server started at {}", zkServer.getConnectString());

    // Start embedded Etcd cluster
    LOG.info("Starting embedded Etcd cluster");
    etcdCluster = TestEtcdClusterFactory.start();
    LOG.info(
        "Embedded Etcd cluster started with endpoints: {}",
        String.join(", ", etcdCluster.clientEndpoints().stream().map(Object::toString).toList()));
  }

  @AfterAll
  public static void tearDownClass() throws Exception {
    // Stop ZooKeeper test server
    if (zkServer != null) {
      LOG.info("Stopping ZooKeeper test server");
      zkServer.close();
      LOG.info("ZooKeeper test server stopped");
    }

    // Stop embedded Etcd cluster
    if (etcdCluster != null) {
      LOG.info("Stopping embedded Etcd cluster");

      LOG.info("Embedded Etcd cluster stopped");
    }
  }

  @BeforeEach
  public void setUp() {
    // Set up a new meter registry
    meterRegistry = new SimpleMeterRegistry();

    // Configure ZooKeeper
    zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(zkServer.getConnectString())
            .setZkPathPrefix("AstraPartitioningMetadataStoreTest")
            .setZkSessionTimeoutMs(10000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .setZkCacheInitTimeoutMs(10000)
            .build();

    // Create curator framework
    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);

    // Configure Etcd
    etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("AstraPartitioningMetadataStoreTest")
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    // Create etcd client
    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .connectTimeout(Duration.ofMillis(5000))
            .keepaliveTimeout(Duration.ofMillis(3000))
            .retryMaxAttempts(3)
            .retryDelay(100)
            .namespace(
                ByteSequence.from("AstraPartitioningMetadataStoreTest", StandardCharsets.UTF_8))
            .build();
  }

  @AfterEach
  public void tearDown() {
    // Close etcd client
    if (etcdClient != null) {
      etcdClient.close();
    }

    // Close curator framework
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }

    // Close meter registry
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  /** Creates a ZookeeperPartitioningMetadataStore instance. */
  private ZookeeperPartitioningMetadataStore<TestPartitionedMetadata> createZkStore(
      String storeFolder) {
    return new ZookeeperPartitioningMetadataStore<>(
        curatorFramework,
        zkConfig,
        meterRegistry,
        CreateMode.PERSISTENT,
        new TestMetadataSerializer(),
        storeFolder);
  }

  /** Creates an EtcdPartitioningMetadataStore instance. */
  private EtcdPartitioningMetadataStore<TestPartitionedMetadata> createEtcdStore(
      String storeFolder) {
    MetadataSerializer<TestPartitionedMetadata> serializer = new TestMetadataSerializer();
    return new EtcdPartitioningMetadataStore<>(
        etcdClient, etcdConfig, meterRegistry, EtcdCreateMode.PERSISTENT, serializer, storeFolder);
  }

  /** Creates an AstraPartitioningMetadataStore instance in ZOOKEEPER_CREATES mode. */
  private AstraPartitioningMetadataStore<TestPartitionedMetadata> createZkCreatesStore(
      String storeFolder) {
    ZookeeperPartitioningMetadataStore<TestPartitionedMetadata> zkStore =
        createZkStore(storeFolder);
    EtcdPartitioningMetadataStore<TestPartitionedMetadata> etcdStore = createEtcdStore(storeFolder);

    return new AstraPartitioningMetadataStore<>(
        zkStore, etcdStore, MetadataStoreMode.ZOOKEEPER_CREATES, meterRegistry);
  }

  /** Creates an AstraPartitioningMetadataStore instance in ETCD_CREATES mode. */
  private AstraPartitioningMetadataStore<TestPartitionedMetadata> createEtcdCreatesStore(
      String storeFolder) {
    ZookeeperPartitioningMetadataStore<TestPartitionedMetadata> zkStore =
        createZkStore(storeFolder);
    EtcdPartitioningMetadataStore<TestPartitionedMetadata> etcdStore = createEtcdStore(storeFolder);

    return new AstraPartitioningMetadataStore<>(
        zkStore, etcdStore, MetadataStoreMode.ETCD_CREATES, meterRegistry);
  }

  /** Interface for running a test against all modes of AstraPartitioningMetadataStore. */
  private interface StoreTestRunner {
    void test(AstraPartitioningMetadataStore<TestPartitionedMetadata> store, String storeMode)
        throws Exception;
  }

  /** Runs a test against all modes of AstraPartitioningMetadataStore. */
  private void testAllStoreModes(String storeFolder, StoreTestRunner testRunner) throws Exception {
    // Test ZK creates mode
    try (AstraPartitioningMetadataStore<TestPartitionedMetadata> store =
        createZkCreatesStore(storeFolder)) {
      testRunner.test(store, "ZOOKEEPER_CREATES");
    }

    // Test Etcd creates mode
    try (AstraPartitioningMetadataStore<TestPartitionedMetadata> store =
        createEtcdCreatesStore(storeFolder)) {
      testRunner.test(store, "ETCD_CREATES");
    }
  }

  @Test
  public void testCreateAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_create_async",
        (store, storeMode) -> {
          LOG.info("Testing createAsync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition1";
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(
                  "test-create-" + storeMode, partition, "createAsyncValue");

          // Create the node asynchronously
          String path = store.createAsync(testData).toCompletableFuture().get();

          // Allow time for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // In ZK implementation, path will be returned as a full path:
          // "/storeFolder/partition/name"
          // In etcd implementation, we've modified it to also return the full path to match ZK
          LOG.info("Created node path: {}", path);
          assertThat(path).isNotNull();
          assertThat(path).isNotEmpty();

          // Path should be a full path containing the node name
          // We can't be more specific about the exact format since ZK and etcd formats differ
          // slightly
          assertThat(path).contains(testData.getName());

          // Verify the node exists
          boolean exists = store.hasSync(partition, testData.getName());
          assertThat(exists).isTrue();

          // Get the node and verify its contents
          TestPartitionedMetadata retrieved = store.getSync(partition, testData.getName());
          assertThat(retrieved).isNotNull();
          assertThat(retrieved.getName()).isEqualTo(testData.getName());
          assertThat(retrieved.getPartition()).isEqualTo(partition);
          assertThat(retrieved.getValue()).isEqualTo("createAsyncValue");

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testCreateSync() throws Exception {
    testAllStoreModes(
        "/partitioned_create_sync",
        (store, storeMode) -> {
          LOG.info("Testing createSync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition2";
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(
                  "test-create-sync-" + storeMode, partition, "createSyncValue");

          // Create the node synchronously
          store.createSync(testData);

          // Allow time for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          boolean exists = store.hasSync(partition, testData.getName());
          assertThat(exists).isTrue();

          // Get the node and verify its contents
          TestPartitionedMetadata retrieved = store.getSync(partition, testData.getName());
          assertThat(retrieved).isNotNull();
          assertThat(retrieved.getName()).isEqualTo(testData.getName());
          assertThat(retrieved.getPartition()).isEqualTo(partition);
          assertThat(retrieved.getValue()).isEqualTo("createSyncValue");

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testGetAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_get_async",
        (store, storeMode) -> {
          LOG.info("Testing getAsync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition3";
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(
                  "test-get-async-" + storeMode, partition, "getAsyncValue");
          store.createSync(testData);

          // Get the node asynchronously
          TestPartitionedMetadata result =
              store.getAsync(partition, testData.getName()).toCompletableFuture().get();

          // Verify the retrieved data
          assertThat(result).isNotNull();
          LOG.info(
              "Retrieved result: name={}, partition={}, value={}",
              result.getName(),
              result.getPartition(),
              result.getValue());
          assertThat(result.getName()).isEqualTo(testData.getName());
          assertThat(result.getPartition())
              .describedAs("Partition should match")
              .isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo(testData.getValue());

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testGetSync() throws Exception {
    testAllStoreModes(
        "/partitioned_get_sync",
        (store, storeMode) -> {
          LOG.info("Testing getSync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition4";
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata("test-get-sync-" + storeMode, partition, "getSyncValue");
          store.createSync(testData);

          // Get the node synchronously
          TestPartitionedMetadata result = store.getSync(partition, testData.getName());

          // Verify the retrieved data
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(testData.getName());
          assertThat(result.getPartition()).isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo(testData.getValue());

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testGetNotFound() throws Exception {
    testAllStoreModes(
        "/partitioned_get_not_found",
        (store, storeMode) -> {
          LOG.info("Testing get for nonexistent node in mode: {}", storeMode);

          // Try to get a node that doesn't exist
          String partition = "non-existent-partition";
          String nonExistentNodeName = "non-existent-node-" + storeMode;

          // First delete the node if it somehow exists from a previous test
          try {
            if (store.hasSync(partition, nonExistentNodeName)) {
              TestPartitionedMetadata metadata =
                  new TestPartitionedMetadata(nonExistentNodeName, partition, "");
              store.deleteSync(metadata);
            }
          } catch (Exception ignored) {
            // Ignore exceptions here - we're just making sure it doesn't exist
          }

          // Make sure the node really doesn't exist by waiting
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nonExistentNodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, the node probably doesn't exist
                    }
                  });

          try {
            // Attempt to get a non-existent node asynchronously
            store.getAsync(partition, nonExistentNodeName).toCompletableFuture().get();
            // If we got here, the get didn't throw an exception as expected
            assertThat(false).isTrue(); // This will fail the test
          } catch (InternalMetadataStoreException | ExecutionException e) {
            // Expected behavior - the node doesn't exist
            LOG.info("Got expected exception for getAsync: {}", e.getClass().getName());
          }

          try {
            // Attempt to get a non-existent node synchronously
            store.getSync(partition, nonExistentNodeName);
            // If we got here, the get didn't throw an exception as expected
            assertThat(false).isTrue(); // This will fail the test
          } catch (Exception e) {
            // Expected behavior - the node doesn't exist
            LOG.info("Got expected exception for getSync: {}", e.getClass().getName());
            // We just check that some exception was thrown
            assertThat(e).isNotNull();
          }
        });
  }

  @Test
  public void testFindAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_find_async",
        (store, storeMode) -> {
          LOG.info("Testing findAsync in mode: {}", storeMode);

          // Create a test metadata object in a specific partition
          String partition = "partition5";
          String nodeName = "test-find-async-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "findAsyncValue");
          store.createSync(testData);

          // Find the node asynchronously without knowing the partition
          TestPartitionedMetadata result = store.findAsync(nodeName).toCompletableFuture().get();

          // Verify the retrieved data
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(nodeName);
          assertThat(result.getPartition()).isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo("findAsyncValue");

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testFindSync() throws Exception {
    testAllStoreModes(
        "/partitioned_find_sync",
        (store, storeMode) -> {
          LOG.info("Testing findSync in mode: {}", storeMode);

          // Create a test metadata object in a specific partition
          String partition = "partition6";
          String nodeName = "test-find-sync-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "findSyncValue");
          store.createSync(testData);

          // Find the node synchronously without knowing the partition
          TestPartitionedMetadata result = store.findSync(nodeName);

          // Verify the retrieved data
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(nodeName);
          assertThat(result.getPartition()).isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo("findSyncValue");

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testFindNotFound() throws Exception {
    testAllStoreModes(
        "/partitioned_find_not_found",
        (store, storeMode) -> {
          LOG.info("Testing find for nonexistent node in mode: {}", storeMode);

          // Try to find a node that doesn't exist
          String nonExistentNodeName = "non-existent-find-node-" + storeMode;

          // Attempt to find a non-existent node asynchronously
          // We expect this to fail with an InternalMetadataStoreException
          // which could either be thrown immediately or wrapped in an ExecutionException
          try {
            store.findAsync(nonExistentNodeName).toCompletableFuture().get();
            // If we get here without an exception, the test should fail
            assertThat(false).describedAs("Expected exception not thrown").isTrue();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Test interrupted", e);
          } catch (ExecutionException e) {
            // Expected behavior - the node doesn't exist
            Throwable cause = e.getCause();
            assertThat(cause).isInstanceOf(InternalMetadataStoreException.class);
            LOG.info("Got expected exception for findAsync: {}", cause.getClass().getName());
            LOG.info("Exception message: {}", cause.getMessage());
          } catch (InternalMetadataStoreException e) {
            // This is also acceptable - direct exception without ExecutionException wrapper
            LOG.info(
                "Got direct InternalMetadataStoreException for findAsync: {}",
                e.getClass().getName());
            LOG.info("Exception message: {}", e.getMessage());
          }

          // Attempt to find a non-existent node synchronously
          // We expect this to fail with InternalMetadataStoreException
          try {
            store.findSync(nonExistentNodeName);
            // If we get here without an exception, the test should fail
            assertThat(false).describedAs("Expected exception not thrown").isTrue();
          } catch (InternalMetadataStoreException e) {
            // Expected behavior - the node doesn't exist
            LOG.info("Got expected exception for findSync: {}", e.getClass().getName());
            LOG.info("Exception message: {}", e.getMessage());
          }
        });
  }

  @Test
  public void testHasAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_has_async",
        (store, storeMode) -> {
          LOG.info("Testing hasAsync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition7";
          String nodeName = "test-has-async-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "hasAsyncValue");

          // Initially, the node should not exist
          Boolean exists = store.hasAsync(partition, nodeName).toCompletableFuture().get();
          assertThat(exists).isFalse();

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now, the node should exist
          exists = store.hasAsync(partition, nodeName).toCompletableFuture().get();
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // After deletion, the node should not exist
          exists = store.hasAsync(partition, nodeName).toCompletableFuture().get();
          assertThat(exists).isFalse();
        });
  }

  @Test
  public void testHasSync() throws Exception {
    testAllStoreModes(
        "/partitioned_has_sync",
        (store, storeMode) -> {
          LOG.info("Testing hasSync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition8";
          String nodeName = "test-has-sync-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "hasSyncValue");

          // Initially, the node should not exist
          boolean exists = store.hasSync(partition, nodeName);
          assertThat(exists).isFalse();

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now, the node should exist
          exists = store.hasSync(partition, nodeName);
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // After deletion, the node should not exist
          exists = store.hasSync(partition, nodeName);
          assertThat(exists).isFalse();
        });
  }

  @Test
  public void testUpdateAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_update_async",
        (store, storeMode) -> {
          LOG.info("Testing updateAsync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition9";
          String nodeName = "test-update-async-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "originalValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestPartitionedMetadata result = store.getSync(partition, nodeName);
                      return result != null && "originalValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Create an updated version of the metadata
          TestPartitionedMetadata updatedData =
              new TestPartitionedMetadata(nodeName, partition, "updatedValue");

          // Update the node asynchronously
          String version = store.updateAsync(updatedData).toCompletableFuture().get();

          // The version is implementation-specific, but shouldn't be null
          assertThat(version).isNotNull();

          // Wait for update to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestPartitionedMetadata result = store.getSync(partition, nodeName);
                      return result != null && "updatedValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now verify the update
          TestPartitionedMetadata result = store.getSync(partition, nodeName);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(nodeName);
          assertThat(result.getPartition()).isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo("updatedValue"); // Should have the updated value

          // Clean up
          store.deleteSync(updatedData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testUpdateSync() throws Exception {
    testAllStoreModes(
        "/partitioned_update_sync",
        (store, storeMode) -> {
          LOG.info("Testing updateSync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition10";
          String nodeName = "test-update-sync-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "originalValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestPartitionedMetadata result = store.getSync(partition, nodeName);
                      return result != null && "originalValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Create an updated version of the metadata
          TestPartitionedMetadata updatedData =
              new TestPartitionedMetadata(nodeName, partition, "updatedValue");

          // Update the node synchronously
          store.updateSync(updatedData);

          // Wait for update to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestPartitionedMetadata result = store.getSync(partition, nodeName);
                      return result != null && "updatedValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Get the node to verify the update
          TestPartitionedMetadata result = store.getSync(partition, nodeName);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(nodeName);
          assertThat(result.getPartition()).isEqualTo(partition);
          assertThat(result.getValue()).isEqualTo("updatedValue"); // Should have the updated value

          // Clean up
          store.deleteSync(updatedData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testDeleteAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_delete_async",
        (store, storeMode) -> {
          LOG.info("Testing deleteAsync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition11";
          String nodeName = "test-delete-async-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "deleteAsyncValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(partition, nodeName)).isTrue();

          // Delete the node asynchronously
          store.deleteAsync(testData).toCompletableFuture().get();

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(partition, nodeName)).isFalse();
        });
  }

  @Test
  public void testDeleteSync() throws Exception {
    testAllStoreModes(
        "/partitioned_delete_sync",
        (store, storeMode) -> {
          LOG.info("Testing deleteSync in mode: {}", storeMode);

          // Create a test metadata object
          String partition = "partition12";
          String nodeName = "test-delete-sync-" + storeMode;
          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "deleteSyncValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(partition, nodeName)).isTrue();

          // Delete the node synchronously
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(partition, nodeName)).isFalse();
        });
  }

  @Test
  public void testListAsync() throws Exception {
    testAllStoreModes(
        "/partitioned_list_async",
        (store, storeMode) -> {
          LOG.info("Testing listAsync in mode: {}", storeMode);

          // Create multiple test metadata objects in different partitions
          String prefix = "test-list-async-" + storeMode + "-";
          String partition1 = "partition13";
          String partition2 = "partition14";

          TestPartitionedMetadata testData1 =
              new TestPartitionedMetadata(prefix + "1", partition1, "value1");
          TestPartitionedMetadata testData2 =
              new TestPartitionedMetadata(prefix + "2", partition1, "value2");
          TestPartitionedMetadata testData3 =
              new TestPartitionedMetadata(prefix + "3", partition2, "value3");

          // Create the nodes
          store.createSync(testData1);
          store.createSync(testData2);
          store.createSync(testData3);

          // Wait for all creations to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition1, prefix + "1")
                          && store.hasSync(partition1, prefix + "2")
                          && store.hasSync(partition2, prefix + "3");
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Wait for the nodes to be available in listing
          await()
              .atMost(10, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      List<TestPartitionedMetadata> results =
                          store.listAsync().toCompletableFuture().get();
                      List<TestPartitionedMetadata> filtered =
                          results.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return filtered.size() >= 3;
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // List the nodes asynchronously
          List<TestPartitionedMetadata> results = store.listAsync().toCompletableFuture().get();

          // Filter the results to just include our test nodes for this test run
          List<TestPartitionedMetadata> filteredResults =
              results.stream().filter(item -> item.getName().startsWith(prefix)).toList();

          // Verify we have the expected number of nodes
          assertThat(filteredResults).hasSize(3);

          // Verify the nodes have the expected names, partitions and values
          assertThat(filteredResults)
              .extracting(TestPartitionedMetadata::getName)
              .containsExactlyInAnyOrder(prefix + "1", prefix + "2", prefix + "3");

          Map<String, List<TestPartitionedMetadata>> partitionMap =
              filteredResults.stream()
                  .collect(Collectors.groupingBy(TestPartitionedMetadata::getPartition));

          assertThat(partitionMap).hasSize(2);
          assertThat(partitionMap.get(partition1)).hasSize(2);
          assertThat(partitionMap.get(partition2)).hasSize(1);

          // Clean up
          store.deleteSync(testData1);
          store.deleteSync(testData2);
          store.deleteSync(testData3);

          // Wait for deletions to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !(store.hasSync(partition1, prefix + "1")
                          || store.hasSync(partition1, prefix + "2")
                          || store.hasSync(partition2, prefix + "3"));
                    } catch (Exception e) {
                      return true; // If we get an exception, assume nodes don't exist
                    }
                  });

          // Wait for deletions to be reflected in the listing
          await()
              .atMost(10, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      List<TestPartitionedMetadata> finalResults =
                          store.listAsync().toCompletableFuture().get();
                      List<TestPartitionedMetadata> filtered =
                          finalResults.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return filtered.isEmpty();
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testListSync() throws Exception {
    testAllStoreModes(
        "/partitioned_list_sync",
        (store, storeMode) -> {
          LOG.info("Testing listSync in mode: {}", storeMode);

          // Create multiple test metadata objects in different partitions
          String prefix = "test-list-sync-" + storeMode + "-";
          String partition1 = "partition15";
          String partition2 = "partition16";

          TestPartitionedMetadata testData1 =
              new TestPartitionedMetadata(prefix + "1", partition1, "value1");
          TestPartitionedMetadata testData2 =
              new TestPartitionedMetadata(prefix + "2", partition1, "value2");
          TestPartitionedMetadata testData3 =
              new TestPartitionedMetadata(prefix + "3", partition2, "value3");

          // Create the nodes
          store.createSync(testData1);
          store.createSync(testData2);
          store.createSync(testData3);

          // Wait for all creations to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition1, prefix + "1")
                          && store.hasSync(partition1, prefix + "2")
                          && store.hasSync(partition2, prefix + "3");
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Wait for the nodes to be available in listing
          await()
              .atMost(10, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      List<TestPartitionedMetadata> results = store.listSync();
                      List<TestPartitionedMetadata> filtered =
                          results.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return filtered.size() >= 3;
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // List the nodes synchronously
          List<TestPartitionedMetadata> results = store.listSync();

          // Filter the results to just include our test nodes for this test run
          List<TestPartitionedMetadata> filteredResults =
              results.stream().filter(item -> item.getName().startsWith(prefix)).toList();

          // Verify we have the expected number of nodes
          assertThat(filteredResults).hasSize(3);

          // Verify the nodes have the expected names, partitions and values
          assertThat(filteredResults)
              .extracting(TestPartitionedMetadata::getName)
              .containsExactlyInAnyOrder(prefix + "1", prefix + "2", prefix + "3");

          Map<String, List<TestPartitionedMetadata>> partitionMap =
              filteredResults.stream()
                  .collect(Collectors.groupingBy(TestPartitionedMetadata::getPartition));

          assertThat(partitionMap).hasSize(2);
          assertThat(partitionMap.get(partition1)).hasSize(2);
          assertThat(partitionMap.get(partition2)).hasSize(1);

          // Clean up
          store.deleteSync(testData1);
          store.deleteSync(testData2);
          store.deleteSync(testData3);

          // Wait for deletions to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !(store.hasSync(partition1, prefix + "1")
                          || store.hasSync(partition1, prefix + "2")
                          || store.hasSync(partition2, prefix + "3"));
                    } catch (Exception e) {
                      return true; // If we get an exception, assume nodes don't exist
                    }
                  });

          // Wait for deletions to be reflected in the listing
          await()
              .atMost(10, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      List<TestPartitionedMetadata> finalResults = store.listSync();
                      List<TestPartitionedMetadata> filtered =
                          finalResults.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return filtered.isEmpty();
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testAddRemoveListener() throws Exception {
    testAllStoreModes(
        "/partitioned_listener",
        (store, storeMode) -> {
          LOG.info("Testing addListener and removeListener in mode: {}", storeMode);

          // Create a counter to track listener notifications
          AtomicInteger counter = new AtomicInteger(0);

          // Create a listener that increments the counter when notified
          AstraMetadataStoreChangeListener<TestPartitionedMetadata> listener =
              ignoredMetadata -> counter.incrementAndGet();

          // Wait for cache initialization before adding listener
          await()
              .atMost(10, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      store.awaitCacheInitialized();
                      return true;
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Add the listener
          store.addListener(listener);

          // Initial count should be 0
          assertThat(counter.get()).isEqualTo(0);

          // Create a test metadata object
          String partition = "partition17";
          String nodeName = "test-listener-" + storeMode;

          // Make sure the test node doesn't exist from a previous test
          try {
            if (store.hasSync(partition, nodeName)) {
              TestPartitionedMetadata metadata =
                  new TestPartitionedMetadata(nodeName, partition, "");
              store.deleteSync(metadata);
              // Wait for deletion to complete
              await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync(partition, nodeName));
            }
          } catch (Exception ignored) {
            // Ignore exceptions here
          }

          TestPartitionedMetadata testData =
              new TestPartitionedMetadata(nodeName, partition, "listenerValue");

          // Reset counter in case cleanup triggered any events
          counter.set(0);

          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Wait for notification (should increment counter to 1)
          await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 1);

          // Update the node (should increment counter to 2)
          TestPartitionedMetadata updatedData =
              new TestPartitionedMetadata(nodeName, partition, "updatedListenerValue");
          store.updateSync(updatedData);

          // Wait for update to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestPartitionedMetadata result = store.getSync(partition, nodeName);
                      return result != null && "updatedListenerValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Wait for notification
          await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 2);

          // Remove the listener
          store.removeListener(listener);

          // Wait for listener removal to take effect
          await()
              .atMost(5, TimeUnit.SECONDS)
              .untilAsserted(
                  () -> {
                    // Update the node again to see if listener is removed
                    TestPartitionedMetadata updatedData2 =
                        new TestPartitionedMetadata(
                            nodeName, partition, "updatedAgainListenerValue");
                    store.updateSync(updatedData2);

                    // Wait for update to propagate
                    await()
                        .atMost(5, TimeUnit.SECONDS)
                        .until(
                            () -> {
                              try {
                                TestPartitionedMetadata result = store.getSync(partition, nodeName);
                                return result != null
                                    && "updatedAgainListenerValue".equals(result.getValue());
                              } catch (Exception e) {
                                return false;
                              }
                            });

                    // Even after update, counter should not change
                    assertThat(counter.get()).isEqualTo(2);
                  });

          // Clean up
          store.deleteSync(updatedData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(partition, nodeName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });
        });
  }

  @Test
  public void testAwaitCacheInitialized() throws Exception {
    testAllStoreModes(
        "/partitioned_cache_init",
        (store, storeMode) -> {
          LOG.info("Testing awaitCacheInitialized in mode: {}", storeMode);

          // Create some test data across different partitions
          String prefix = "test-cache-init-" + storeMode + "-";
          String partition1 = "partition18";
          String partition2 = "partition19";

          String key1 = prefix + "1";
          String key2 = prefix + "2";
          String key3 = prefix + "3";

          TestPartitionedMetadata testData1 =
              new TestPartitionedMetadata(key1, partition1, "value1");
          TestPartitionedMetadata testData2 =
              new TestPartitionedMetadata(key2, partition1, "value2");
          TestPartitionedMetadata testData3 =
              new TestPartitionedMetadata(key3, partition2, "value3");

          try {
            // Create the nodes
            store.createSync(testData1);
            store.createSync(testData2);
            store.createSync(testData3);

            // Wait for all creations to propagate
            await()
                .atMost(5, TimeUnit.SECONDS)
                .until(
                    () -> {
                      try {
                        return store.hasSync(partition1, key1)
                            && store.hasSync(partition1, key2)
                            && store.hasSync(partition2, key3);
                      } catch (Exception e) {
                        return false;
                      }
                    });

            // Call awaitCacheInitialized on the existing store - should not throw exceptions
            try {
              store.awaitCacheInitialized();
            } catch (Exception e) {
              fail("awaitCacheInitialized should not throw exceptions, but got: " + e.getMessage());
            }

            // Now verify we can list the items we created via the cache
            List<TestPartitionedMetadata> cachedItems = store.listSync();
            LOG.info("Found {} total items in the cache", cachedItems.size());

            // Filter the results to just include our test nodes
            List<TestPartitionedMetadata> filteredResults =
                cachedItems.stream().filter(item -> item.getName().startsWith(prefix)).toList();
            LOG.info("Found {} filtered items matching prefix {}", filteredResults.size(), prefix);

            // Should have 3 items
            assertThat(filteredResults).hasSize(3);

            // Verify the nodes have the expected names across different partitions
            assertThat(filteredResults)
                .extracting(TestPartitionedMetadata::getName)
                .containsExactlyInAnyOrder(key1, key2, key3);

            // Verify partitioning is preserved
            Map<String, List<TestPartitionedMetadata>> partitionMap =
                filteredResults.stream()
                    .collect(Collectors.groupingBy(TestPartitionedMetadata::getPartition));

            assertThat(partitionMap).hasSize(2);
            assertThat(partitionMap.get(partition1)).hasSize(2);
            assertThat(partitionMap.get(partition2)).hasSize(1);

            // Get nodes individually and verify contents
            TestPartitionedMetadata node1 = store.getSync(partition1, key1);
            TestPartitionedMetadata node2 = store.getSync(partition1, key2);
            TestPartitionedMetadata node3 = store.getSync(partition2, key3);

            assertThat(node1.getValue()).isEqualTo("value1");
            assertThat(node2.getValue()).isEqualTo("value2");
            assertThat(node3.getValue()).isEqualTo("value3");
          } finally {
            // Clean up the test data
            store.deleteSync(testData1);
            store.deleteSync(testData2);
            store.deleteSync(testData3);
          }
        });
  }

  @Test
  public void testListenerWithExistingZkDataInEtcdCreatesMode() throws Exception {
    // For this test, we'll focus only on ETCD_CREATES mode
    String storeFolder = "/partitioned_existing_zk_data_listener";
    LOG.info("Testing listener with existing ZK data in ETCD_CREATES mode");

    // Step 1: Create data in ZK directly using ZookeeperPartitioningMetadataStore
    ZookeeperPartitioningMetadataStore<TestPartitionedMetadata> zkStore =
        createZkStore(storeFolder);
    String partition = "partition-for-zk-existing";
    String nodeName = "test-existing-zk-data";
    TestPartitionedMetadata zkData =
        new TestPartitionedMetadata(nodeName, partition, "initialZkValue");
    zkStore.createSync(zkData);

    // Wait for creation to complete in ZK
    await().atMost(5, TimeUnit.SECONDS).until(() -> zkStore.hasSync(partition, nodeName));

    // Step 2: Now create the AstraPartitioningMetadataStore in ETCD_CREATES mode
    try (AstraPartitioningMetadataStore<TestPartitionedMetadata> store =
        createEtcdCreatesStore(storeFolder)) {
      // Step 3: Initialize the cache and wait for it to be ready
      store.awaitCacheInitialized();

      // Verify we can get the data that was originally stored in ZK via the bridge
      // This will use the fallback mechanism to ZK
      await().atMost(5, TimeUnit.SECONDS).until(() -> store.hasSync(partition, nodeName));

      TestPartitionedMetadata retrievedData = store.getSync(partition, nodeName);
      assertThat(retrievedData).isNotNull();
      assertThat(retrievedData.getName()).isEqualTo(nodeName);
      assertThat(retrievedData.getPartition()).isEqualTo(partition);
      assertThat(retrievedData.getValue()).isEqualTo("initialZkValue");

      // Step 4: Now add a listener to watch for changes to this data
      AtomicInteger callbackCount = new AtomicInteger(0);
      AtomicInteger updatesReceived = new AtomicInteger(0);

      AstraMetadataStoreChangeListener<TestPartitionedMetadata> listener =
          metadata -> {
            callbackCount.incrementAndGet();
            LOG.info("Listener received update for: {}", metadata);
            if (metadata.getName().equals(nodeName)
                && metadata.getPartition().equals(partition)
                && "updatedZkValue".equals(metadata.getValue())) {
              updatesReceived.incrementAndGet();
            }
          };

      store.addListener(listener);

      // Step 5: Update the ZK data directly and verify we get notifications
      TestPartitionedMetadata updatedZkData =
          new TestPartitionedMetadata(nodeName, partition, "updatedZkValue");
      zkStore.updateSync(updatedZkData);

      // Wait for the update to be detected
      await().atMost(10, TimeUnit.SECONDS).until(() -> updatesReceived.get() > 0);

      // Verify listener was called with updated data
      assertThat(callbackCount.get()).isGreaterThanOrEqualTo(1);
      assertThat(updatesReceived.get()).isEqualTo(1);

      // Clean up
      store.removeListener(listener);
      store.deleteSync(updatedZkData);

      // Wait for deletion to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync(partition, nodeName));
    } finally {
      zkStore.close();
    }
  }

  @Test
  public void testListenerWithZkUpdateInEtcdCreatesMode() throws Exception {
    // For this test, we'll create data in the bridge store first, then update it via ZK directly
    String storeFolder = "/partitioned_zk_update_listener";
    LOG.info("Testing listener with ZK update in ETCD_CREATES mode");

    // Step 1: Create AstraPartitioningMetadataStore in ETCD_CREATES mode
    try (AstraPartitioningMetadataStore<TestPartitionedMetadata> store =
        createEtcdCreatesStore(storeFolder)) {

      // Step 2: Create data through the bridge store
      String partition = "partition-for-zk-update";
      String nodeName = "test-zk-update-data";
      TestPartitionedMetadata initialData =
          new TestPartitionedMetadata(nodeName, partition, "initialValue");

      store.createSync(initialData);

      // Wait for creation to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> store.hasSync(partition, nodeName));

      // Step 3: Set up a listener to catch updates
      AtomicInteger listenerCallCount = new AtomicInteger(0);

      AstraMetadataStoreChangeListener<TestPartitionedMetadata> listener =
          metadata -> {
            LOG.info("Listener triggered with metadata: {}", metadata);
            listenerCallCount.incrementAndGet();
          };

      store.addListener(listener);

      // Step 4: Get a ZK store and update the data directly in ZK
      try (ZookeeperPartitioningMetadataStore<TestPartitionedMetadata> zkStore =
          createZkStore(storeFolder)) {
        // This requires that the data already exists in ZK, which it might not if using
        // ETCD_CREATES mode
        // So first check if it exists
        if (zkStore.hasSync(partition, nodeName)) {
          // Update directly in ZK
          TestPartitionedMetadata zkUpdateData =
              new TestPartitionedMetadata(nodeName, partition, "updatedByZkDirectly");
          zkStore.updateSync(zkUpdateData);

          // Wait for the update to be detected by the listener
          await().atMost(10, TimeUnit.SECONDS).until(() -> listenerCallCount.get() > 0);

          // Verify listener was called
          assertThat(listenerCallCount.get()).isGreaterThanOrEqualTo(1);

          // Get data through bridge and verify it reflects the ZK update
          TestPartitionedMetadata retrievedData = store.getSync(partition, nodeName);
          assertThat(retrievedData).isNotNull();
          assertThat(retrievedData.getValue()).isEqualTo("updatedByZkDirectly");
        } else {
          // Create the data in ZK (may happen in ETCD_CREATES mode where data doesn't automatically
          // go to ZK)
          TestPartitionedMetadata zkCreateData =
              new TestPartitionedMetadata(nodeName, partition, "createdInZkDirectly");
          zkStore.createSync(zkCreateData);

          // Wait for the create to be detected by the listener
          await().atMost(10, TimeUnit.SECONDS).until(() -> listenerCallCount.get() > 0);

          // Verify listener was called
          assertThat(listenerCallCount.get()).isGreaterThanOrEqualTo(1);
        }
      } finally {
        // Clean up
        store.removeListener(listener);
        store.deleteSync(initialData);

        // Wait for deletion to complete
        await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync(partition, nodeName));
      }
    }
  }
}
