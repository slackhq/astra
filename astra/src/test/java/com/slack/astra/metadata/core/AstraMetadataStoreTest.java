package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
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
 * Tests for AstraMetadataStore that validate its behavior across the two operational modes: -
 * ZOOKEEPER_CREATES - ETCD_CREATES
 */
@Tag("integration")
public class AstraMetadataStoreTest {

  private static final Logger LOG = LoggerFactory.getLogger(AstraMetadataStoreTest.class);

  private static TestingServer zkServer;
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;

  // ZK components
  private AsyncCuratorFramework curatorFramework;
  private AstraConfigs.ZookeeperConfig zkConfig;

  // Etcd components
  private AstraConfigs.EtcdConfig etcdConfig;
  private Client etcdClient;

  // Store folder for different test scenarios
  private static final String ZK_CREATES_STORE = "/zkCreates";
  private static final String ETCD_CREATES_STORE = "/etcdCreates";

  /** Test metadata implementation for testing AstraMetadataStore. */
  private static class TestMetadata extends AstraMetadata {
    private final String value;

    @JsonCreator
    public TestMetadata(@JsonProperty("name") String name, @JsonProperty("value") String value) {
      super(name);
      this.value = value;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestMetadata)) return false;
      if (!super.equals(o)) return false;

      TestMetadata metadata = (TestMetadata) o;
      return Objects.equals(value, metadata.value);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return "TestMetadata{" + "name='" + name + '\'' + ", value='" + value + '\'' + '}';
    }
  }

  /** Serializer for TestMetadata for use with Etcd. */
  private static class TestMetadataSerializer implements MetadataSerializer<TestMetadata> {
    @Override
    public String toJsonStr(TestMetadata metadata) {
      return String.format(
          "{\"name\":\"%s\",\"value\":\"%s\"}", metadata.getName(), metadata.getValue());
    }

    @Override
    public TestMetadata fromJsonStr(String data) {
      // Simple JSON parsing for testing purposes
      String name = data.split("\"name\":\"")[1].split("\"")[0];
      String value = data.split("\"value\":\"")[1].split("\"")[0];
      return new TestMetadata(name, value);
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
            .setZkPathPrefix("AstraMetadataStoreTest")
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
            .setNamespace("AstraMetadataStoreTest")
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    // Create etcd client
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    // Set namespace if provided
    if (!etcdConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    etcdClient = clientBuilder.build();
  }

  @AfterEach
  public void tearDown() {
    // Close curator framework
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }

    // Close etcd client
    if (etcdClient != null) {
      etcdClient.close();
    }

    // Close meter registry
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  /** Creates an AstraMetadataStore instance in ZOOKEEPER_CREATES mode. */
  private AstraMetadataStore<TestMetadata> createZkCreatesStore() {
    ZookeeperMetadataStore<TestMetadata> zkStore =
        new ZookeeperMetadataStore<>(
            curatorFramework,
            zkConfig,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            ZK_CREATES_STORE,
            meterRegistry);

    return new AstraMetadataStore<>(
        zkStore,
        null, // No Etcd store in ZK creates mode
        MetadataStoreMode.ZOOKEEPER_CREATES,
        meterRegistry);
  }

  /** Creates an AstraMetadataStore instance in ETCD_CREATES mode. */
  private AstraMetadataStore<TestMetadata> createEtcdCreatesStore() {
    MetadataSerializer<TestMetadata> serializer = new TestMetadataSerializer();
    EtcdMetadataStore<TestMetadata> etcdStore =
        new EtcdMetadataStore<>(
            ETCD_CREATES_STORE, etcdConfig, true, meterRegistry, serializer, etcdClient);

    return new AstraMetadataStore<>(
        null, // No ZK store in Etcd creates mode
        etcdStore,
        MetadataStoreMode.ETCD_CREATES,
        meterRegistry);
  }

  /** Interface for running a test against all modes of AstraMetadataStore. */
  private interface StoreTestRunner {
    void test(AstraMetadataStore<TestMetadata> store, String storeMode) throws Exception;
  }

  /** Runs a test against all modes of AstraMetadataStore. */
  private void testAllStoreModes(StoreTestRunner testRunner) throws Exception {
    // Test ZK creates mode
    try (AstraMetadataStore<TestMetadata> store = createZkCreatesStore()) {
      testRunner.test(store, "ZOOKEEPER_CREATES");
    }

    // Test Etcd creates mode
    try (AstraMetadataStore<TestMetadata> store = createEtcdCreatesStore()) {
      testRunner.test(store, "ETCD_CREATES");
    }
  }

  @Test
  public void testCreateAsync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing createAsync in mode: {}", storeMode);

          // Create a test metadata object
          TestMetadata testData = new TestMetadata("test-create-" + storeMode, "createAsyncValue");

          // Create the node asynchronously
          String path = store.createAsync(testData).toCompletableFuture().get();

          // Allow time for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the path is correct (note: ZK paths might have leading paths)
          assertThat(path.endsWith(testData.getName())).isTrue();

          // Verify the node exists
          boolean exists = store.hasSync(testData.getName());
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testCreateSync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing createSync in mode: {}", storeMode);

          // Create a test metadata object
          TestMetadata testData =
              new TestMetadata("test-create-sync-" + storeMode, "createSyncValue");

          // Create the node synchronously
          store.createSync(testData);

          // Allow time for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          boolean exists = store.hasSync(testData.getName());
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testData.getName());
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testGetAsync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing getAsync in mode: {}", storeMode);

          // Create a test metadata object
          TestMetadata testData = new TestMetadata("test-get-async-" + storeMode, "getAsyncValue");
          store.createSync(testData);

          // Get the node asynchronously
          TestMetadata result = store.getAsync(testData.getName()).toCompletableFuture().get();

          // Verify the retrieved data
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(testData.getName());
          assertThat(result.getValue()).isEqualTo(testData.getValue());

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testGetSync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing getSync in mode: {}", storeMode);

          // Create a test metadata object
          TestMetadata testData = new TestMetadata("test-get-sync-" + storeMode, "getSyncValue");
          store.createSync(testData);

          // Get the node synchronously
          TestMetadata result = store.getSync(testData.getName());

          // Verify the retrieved data
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(testData.getName());
          assertThat(result.getValue()).isEqualTo(testData.getValue());

          // Clean up
          store.deleteSync(testData);
        });
  }

  @Test
  public void testGetNotFound() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing get for nonexistent node in mode: {}", storeMode);

          // Try to get a node that doesn't exist
          String nonExistentPath = String.format("non-existent-node-%s", storeMode);

          // First delete the node if it somehow exists from a previous test
          try {
            if (store.hasSync(nonExistentPath)) {
              store.deleteSync(nonExistentPath);
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
                      return !store.hasSync(nonExistentPath);
                    } catch (Exception e) {
                      return true; // If we get an exception, the node probably doesn't exist
                    }
                  });

          try {
            // Attempt to get a non-existent node asynchronously
            store.getAsync(nonExistentPath).toCompletableFuture().get();
            // If we got here, the get didn't throw an exception as expected
            assertThat(false).isTrue(); // This will fail the test
          } catch (InternalMetadataStoreException | ExecutionException e) {
            // Expected behavior - the node doesn't exist
            assertThat(e.getMessage())
                .containsAnyOf("not found", "NoNode", "doesn't exist", "Error fetching node");
          }

          try {
            // Attempt to get a non-existent node synchronously
            store.getSync(nonExistentPath);
            // If we got here, the get didn't throw an exception as expected
            assertThat(false).isTrue(); // This will fail the test
          } catch (Exception e) {
            // Expected behavior - the node doesn't exist
            assertThat(e).isInstanceOf(InternalMetadataStoreException.class);
          }
        });
  }

  @Test
  public void testHasAsync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing hasAsync in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-has-async-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "hasAsyncValue");

          // Initially, the node should not exist
          // Use await to ensure the node definitely doesn't exist
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          Boolean exists = store.hasAsync(testName).toCompletableFuture().get();
          assertThat(exists).isFalse();

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now, the node should exist
          exists = store.hasAsync(testName).toCompletableFuture().get();
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // After deletion, the node should not exist
          exists = store.hasAsync(testName).toCompletableFuture().get();
          assertThat(exists).isFalse();
        });
  }

  @Test
  public void testHasSync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing hasSync in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-has-sync-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "hasSyncValue");

          // Initially, the node should not exist
          boolean exists = store.hasSync(testName);
          assertThat(exists).isFalse();

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now, the node should exist
          exists = store.hasSync(testName);
          assertThat(exists).isTrue();

          // Clean up
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // After deletion, the node should not exist
          exists = store.hasSync(testName);
          assertThat(exists).isFalse();
        });
  }

  @Test
  public void testUpdateAsync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing updateAsync in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-update-async-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "originalValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestMetadata result = store.getSync(testName);
                      return result != null && "originalValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Create an updated version of the metadata
          TestMetadata updatedData = new TestMetadata(testName, "updatedValue");

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
                      TestMetadata result = store.getSync(testName);
                      return result != null && "updatedValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Now verify the update
          TestMetadata result = store.getSync(testName);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(testName);
          assertThat(result.getValue()).isEqualTo("updatedValue"); // Should have the updated value

          // Clean up
          store.deleteSync(testName);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testUpdateSync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing updateSync in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-update-sync-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "originalValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestMetadata result = store.getSync(testName);
                      return result != null && "originalValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Create an updated version of the metadata
          TestMetadata updatedData = new TestMetadata(testName, "updatedValue");

          // Update the node synchronously
          store.updateSync(updatedData);

          // Wait for update to complete and be visible
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestMetadata result = store.getSync(testName);
                      return result != null && "updatedValue".equals(result.getValue());
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Get the node to verify the update
          TestMetadata result = store.getSync(testName);
          assertThat(result).isNotNull();
          assertThat(result.getName()).isEqualTo(testName);
          assertThat(result.getValue()).isEqualTo("updatedValue"); // Should have the updated value

          // Clean up
          store.deleteSync(testName);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });
        });
  }

  @Test
  public void testDeleteAsyncWithPath() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing deleteAsync with path in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-delete-async-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "deleteAsyncValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(testName)).isTrue();

          // Delete the node asynchronously by path
          store.deleteAsync(testName).toCompletableFuture().get();

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(testName)).isFalse();
        });
  }

  @Test
  public void testDeleteSyncWithPath() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing deleteSync with path in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-delete-sync-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "deleteSyncValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(testName)).isTrue();

          // Delete the node synchronously by path
          store.deleteSync(testName);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(testName)).isFalse();
        });
  }

  @Test
  public void testDeleteAsyncWithMetadata() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing deleteAsync with metadata in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-delete-async-md-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "deleteAsyncMdValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(testName)).isTrue();

          // Delete the node asynchronously by metadata object
          store.deleteAsync(testData).toCompletableFuture().get();

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(testName)).isFalse();
        });
  }

  @Test
  public void testDeleteSyncWithMetadata() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing deleteSync with metadata in mode: {}", storeMode);

          // Create a test metadata object
          String testName = "test-delete-sync-md-" + storeMode;
          TestMetadata testData = new TestMetadata(testName, "deleteSyncMdValue");

          // Create the node
          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(testName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Verify the node exists
          assertThat(store.hasSync(testName)).isTrue();

          // Delete the node synchronously by metadata object
          store.deleteSync(testData);

          // Wait for deletion to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(testName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });

          // Verify the node no longer exists
          assertThat(store.hasSync(testName)).isFalse();
        });
  }

  @Test
  public void testListAsync() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing listAsync in mode: {}", storeMode);

          // Create multiple test metadata objects
          String prefix = "test-list-async-" + storeMode + "-";
          TestMetadata testData1 = new TestMetadata(prefix + "1", "value1");
          TestMetadata testData2 = new TestMetadata(prefix + "2", "value2");
          TestMetadata testData3 = new TestMetadata(prefix + "3", "value3");

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
                      return store.hasSync(prefix + "1")
                          && store.hasSync(prefix + "2")
                          && store.hasSync(prefix + "3");
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
                      List<TestMetadata> results = store.listAsync().toCompletableFuture().get();
                      List<TestMetadata> filtered =
                          results.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return filtered.size() >= 3;
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // List the nodes asynchronously
          List<TestMetadata> results = store.listAsync().toCompletableFuture().get();

          // Filter the results to just include our test nodes for this test run
          List<TestMetadata> filteredResults =
              results.stream().filter(item -> item.getName().startsWith(prefix)).toList();

          // Verify we have the expected number of nodes
          assertThat(filteredResults).hasSize(3);

          // Verify the nodes have the expected names and values
          assertThat(filteredResults)
              .extracting(TestMetadata::getName)
              .containsExactlyInAnyOrder(prefix + "1", prefix + "2", prefix + "3");

          assertThat(filteredResults)
              .extracting(TestMetadata::getValue)
              .containsExactlyInAnyOrder("value1", "value2", "value3");

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
                      return !(store.hasSync(prefix + "1")
                          || store.hasSync(prefix + "2")
                          || store.hasSync(prefix + "3"));
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
                      List<TestMetadata> finalResults =
                          store.listAsync().toCompletableFuture().get();
                      List<TestMetadata> filtered =
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
        (store, storeMode) -> {
          LOG.info("Testing listSync in mode: {}", storeMode);

          // Create multiple test metadata objects
          String prefix = "test-list-sync-" + storeMode + "-";
          TestMetadata testData1 = new TestMetadata(prefix + "1", "value1");
          TestMetadata testData2 = new TestMetadata(prefix + "2", "value2");
          TestMetadata testData3 = new TestMetadata(prefix + "3", "value3");

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
                      return store.hasSync(prefix + "1")
                          && store.hasSync(prefix + "2")
                          && store.hasSync(prefix + "3");
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
                      List<TestMetadata> results = store.listSync();
                      List<TestMetadata> filtered =
                          results.stream()
                              .filter(item -> item.getName().startsWith(prefix))
                              .toList();
                      return !filtered.isEmpty();
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // List the nodes synchronously
          List<TestMetadata> results = store.listSync();

          // Filter the results to just include our test nodes for this test run
          List<TestMetadata> filteredResults =
              results.stream().filter(item -> item.getName().startsWith(prefix)).toList();

          // Verify we have some nodes
          assertThat(filteredResults).isNotEmpty();

          // Verify the nodes have the expected names and values
          assertThat(filteredResults)
              .extracting(TestMetadata::getName)
              .containsExactlyInAnyOrder(prefix + "1", prefix + "2", prefix + "3");

          assertThat(filteredResults)
              .extracting(TestMetadata::getValue)
              .containsExactlyInAnyOrder("value1", "value2", "value3");

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
                      return !(store.hasSync(prefix + "1")
                          || store.hasSync(prefix + "2")
                          || store.hasSync(prefix + "3"));
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
                      List<TestMetadata> finalResults = store.listSync();
                      List<TestMetadata> filtered =
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
        (store, storeMode) -> {
          LOG.info("Testing addListener and removeListener in mode: {}", storeMode);

          // Create a counter to track listener notifications
          AtomicInteger counter = new AtomicInteger(0);

          // Create a listener that increments the counter when notified
          AstraMetadataStoreChangeListener<TestMetadata> listener =
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
          String listenerName = String.format("test-listener-%s", storeMode);

          // Make sure the test node doesn't exist from a previous test
          try {
            if (store.hasSync(listenerName)) {
              store.deleteSync(listenerName);
              // Wait for deletion to complete
              await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync(listenerName));
            }
          } catch (Exception ignored) {
            // Ignore exceptions here
          }

          TestMetadata testData = new TestMetadata(listenerName, "listenerValue");

          // Reset counter in case cleanup triggered any events
          counter.set(0);

          store.createSync(testData);

          // Wait for creation to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return store.hasSync(listenerName);
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // Wait for notification (should increment counter to 1)
          await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 1);

          // Update the node (should increment counter to 2)
          TestMetadata updatedData = new TestMetadata(listenerName, "updatedListenerValue");
          store.updateSync(updatedData);

          // Wait for update to propagate
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      TestMetadata result = store.getSync(listenerName);
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
                    TestMetadata updatedData2 =
                        new TestMetadata(listenerName, "updatedAgainListenerValue");
                    store.updateSync(updatedData2);

                    // Wait for update to propagate
                    await()
                        .atMost(5, TimeUnit.SECONDS)
                        .until(
                            () -> {
                              try {
                                TestMetadata result = store.getSync(listenerName);
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
          store.deleteSync(listenerName);

          // Wait for deletion to complete
          await()
              .atMost(5, TimeUnit.SECONDS)
              .until(
                  () -> {
                    try {
                      return !store.hasSync(listenerName);
                    } catch (Exception e) {
                      return true; // If we get an exception, assume node doesn't exist
                    }
                  });
        });
  }

  /** Tests awaiting cache initialization in both modes. */
  @Test
  public void testAwaitCacheInitialized() throws Exception {
    testAllStoreModes(
        (store, storeMode) -> {
          LOG.info("Testing awaitCacheInitialized in mode: {}", storeMode);

          // Create some test data
          String prefix = String.format("test-cache-init-%s-", storeMode);
          String key1 = String.format("%s1", prefix);
          String key2 = String.format("%s2", prefix);
          String key3 = String.format("%s3", prefix);
          TestMetadata testData1 = new TestMetadata(key1, "value1");
          TestMetadata testData2 = new TestMetadata(key2, "value2");
          TestMetadata testData3 = new TestMetadata(key3, "value3");

          // Create the nodes
          store.createSync(testData1);
          store.createSync(testData2);
          store.createSync(testData3);

          // Create a new store of the same type with cache enabled
          AstraMetadataStore<TestMetadata> newStore = null;
          try {
            switch (storeMode) {
              case "ZOOKEEPER_CREATES" -> newStore = createZkCreatesStore();
              case "ETCD_CREATES" -> newStore = createEtcdCreatesStore();
              default ->
                  throw new IllegalArgumentException(
                      String.format("Unknown store mode: %s", storeMode));
            }

            // Initialize the cache
            newStore.awaitCacheInitialized();

            // Get the nodes from the cache
            List<TestMetadata> cachedItems = newStore.listSync();

            // Filter the results to just include our test nodes for this test
            List<TestMetadata> filteredResults =
                cachedItems.stream().filter(item -> item.getName().startsWith(prefix)).toList();

            // Should have 3 items
            assertThat(filteredResults).hasSize(3);

            // Verify the nodes have the expected names
            assertThat(filteredResults)
                .extracting(TestMetadata::getName)
                .containsExactlyInAnyOrder(key1, key2, key3);
          } finally {
            // Clean up the new store
            if (newStore != null) {
              newStore.close();
            }

            // Clean up the test data
            store.deleteSync(testData1);
            store.deleteSync(testData2);
            store.deleteSync(testData3);
          }
        });
  }
}
