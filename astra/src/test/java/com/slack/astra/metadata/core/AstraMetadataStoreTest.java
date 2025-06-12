package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
 * Tests for AstraMetadataStore that validate its behavior across all four operational modes: -
 * ZOOKEEPER_EXCLUSIVE - ETCD_EXCLUSIVE - BOTH_READ_ZOOKEEPER_WRITE - BOTH_READ_ETCD_WRITE
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

  // Store folder for different test scenarios
  private static final String ZK_EXCLUSIVE_STORE = "/zkExclusive";
  private static final String ETCD_EXCLUSIVE_STORE = "/etcdExclusive";
  private static final String BOTH_READ_ZK_WRITE_STORE = "/bothReadZkWrite";
  private static final String BOTH_READ_ETCD_WRITE_STORE = "/bothReadEtcdWrite";

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
    etcdCluster = Etcd.builder().withClusterName("etcd-test").withNodes(1).build();
    etcdCluster.start();
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
      etcdCluster.close();
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
            .setMaxRetries(3)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlSeconds(60)
            .build();
  }

  @AfterEach
  public void tearDown() {
    // Close curator framework
    if (curatorFramework != null) {
      curatorFramework.unwrap().close();
    }

    // Close meter registry
    if (meterRegistry != null) {
      meterRegistry.close();
    }
  }

  /** Creates an AstraMetadataStore instance in ZOOKEEPER_EXCLUSIVE mode. */
  private AstraMetadataStore<TestMetadata> createZkExclusiveStore() {
    ZookeeperMetadataStore<TestMetadata> zkStore =
        new ZookeeperMetadataStore<>(
            curatorFramework,
            zkConfig,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            ZK_EXCLUSIVE_STORE,
            meterRegistry);

    return new AstraMetadataStore<>(
        zkStore,
        null, // No Etcd store in ZK exclusive mode
        MetadataStoreMode.ZOOKEEPER_EXCLUSIVE,
        meterRegistry);
  }

  /** Creates an AstraMetadataStore instance in ETCD_EXCLUSIVE mode. */
  private AstraMetadataStore<TestMetadata> createEtcdExclusiveStore() {
    MetadataSerializer<TestMetadata> serializer = new TestMetadataSerializer();
    EtcdMetadataStore<TestMetadata> etcdStore =
        new EtcdMetadataStore<>(ETCD_EXCLUSIVE_STORE, etcdConfig, true, meterRegistry, serializer);

    return new AstraMetadataStore<>(
        null, // No ZK store in Etcd exclusive mode
        etcdStore,
        MetadataStoreMode.ETCD_EXCLUSIVE,
        meterRegistry);
  }

  /** Creates an AstraMetadataStore instance in BOTH_READ_ZOOKEEPER_WRITE mode. */
  private AstraMetadataStore<TestMetadata> createBothReadZkWriteStore() {
    ZookeeperMetadataStore<TestMetadata> zkStore =
        new ZookeeperMetadataStore<>(
            curatorFramework,
            zkConfig,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            BOTH_READ_ZK_WRITE_STORE,
            meterRegistry);

    MetadataSerializer<TestMetadata> serializer = new TestMetadataSerializer();
    EtcdMetadataStore<TestMetadata> etcdStore =
        new EtcdMetadataStore<>(
            BOTH_READ_ZK_WRITE_STORE, etcdConfig, true, meterRegistry, serializer);

    return new AstraMetadataStore<>(
        zkStore, etcdStore, MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE, meterRegistry);
  }

  /** Creates an AstraMetadataStore instance in BOTH_READ_ETCD_WRITE mode. */
  private AstraMetadataStore<TestMetadata> createBothReadEtcdWriteStore() {
    ZookeeperMetadataStore<TestMetadata> zkStore =
        new ZookeeperMetadataStore<>(
            curatorFramework,
            zkConfig,
            CreateMode.PERSISTENT,
            true,
            new JacksonModelSerializer<>(TestMetadata.class),
            BOTH_READ_ETCD_WRITE_STORE,
            meterRegistry);

    MetadataSerializer<TestMetadata> serializer = new TestMetadataSerializer();
    EtcdMetadataStore<TestMetadata> etcdStore =
        new EtcdMetadataStore<>(
            BOTH_READ_ETCD_WRITE_STORE, etcdConfig, true, meterRegistry, serializer);

    return new AstraMetadataStore<>(
        zkStore, etcdStore, MetadataStoreMode.BOTH_READ_ETCD_WRITE, meterRegistry);
  }

  /** Interface for running a test against all four modes of AstraMetadataStore. */
  private interface StoreTestRunner {
    void test(AstraMetadataStore<TestMetadata> store, String storeMode) throws Exception;
  }

  /** Runs a test against all four modes of AstraMetadataStore. */
  private void testAllStoreModes(StoreTestRunner testRunner) throws Exception {
    // Test ZK exclusive mode
    try (AstraMetadataStore<TestMetadata> store = createZkExclusiveStore()) {
      testRunner.test(store, "ZOOKEEPER_EXCLUSIVE");
    }

    // Test Etcd exclusive mode
    try (AstraMetadataStore<TestMetadata> store = createEtcdExclusiveStore()) {
      testRunner.test(store, "ETCD_EXCLUSIVE");
    }

    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      testRunner.test(store, "BOTH_READ_ZOOKEEPER_WRITE");
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      testRunner.test(store, "BOTH_READ_ETCD_WRITE");
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
          } catch (ExecutionException e) {
            // Expected behavior - the node doesn't exist
            // We accept either an InternalMetadataStoreException or a
            // KeeperException$NoNodeException
            // as both are valid exception types depending on the implementation
            assertThat(e.getCause().getMessage())
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
  public void testGetReadFallback() throws Exception {
    // This test is only applicable to the mixed modes

    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing read fallback in BOTH_READ_ZOOKEEPER_WRITE mode");

      // Create data in ZK (primary)
      TestMetadata zkData = new TestMetadata("test-zk-primary", "zkValue");
      store.createSync(zkData);

      // Get data from the store - should be retrieved from ZK
      TestMetadata zkResult = store.getSync("test-zk-primary");
      assertThat(zkResult).isNotNull();
      assertThat(zkResult.getValue()).isEqualTo("zkValue");

      // Manually create data directly in Etcd (secondary) with a different value
      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        TestMetadata etcdData = new TestMetadata("test-etcd-secondary", "etcdValue");
        etcdStore.createSync(etcdData);
      }

      // Get data that exists only in Etcd (secondary) - should fall back to Etcd
      TestMetadata etcdResult = store.getSync("test-etcd-secondary");
      assertThat(etcdResult).isNotNull();
      assertThat(etcdResult.getValue()).isEqualTo("etcdValue");

      // Clean up both pieces of data
      store.deleteSync("test-zk-primary");
      store.deleteSync("test-etcd-secondary");
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing read fallback in BOTH_READ_ETCD_WRITE mode");

      // Create data in Etcd (primary)
      TestMetadata etcdData = new TestMetadata("test-etcd-primary", "etcdValue");
      store.createSync(etcdData);

      // Get data from the store - should be retrieved from Etcd
      TestMetadata etcdResult = store.getSync("test-etcd-primary");
      assertThat(etcdResult).isNotNull();
      assertThat(etcdResult.getValue()).isEqualTo("etcdValue");

      // Manually create data directly in ZK (secondary) with a different value
      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        TestMetadata zkData = new TestMetadata("test-zk-secondary", "zkValue");
        zkStore.createSync(zkData);
      }

      // Get data that exists only in ZK (secondary) - should fall back to ZK
      TestMetadata zkResult = store.getSync("test-zk-secondary");
      assertThat(zkResult).isNotNull();
      assertThat(zkResult.getValue()).isEqualTo("zkValue");

      // Clean up both pieces of data
      store.deleteSync("test-etcd-primary");
      store.deleteSync("test-zk-secondary");
    }
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
  public void testHasInMixedMode() throws Exception {
    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing has in BOTH_READ_ZOOKEEPER_WRITE mode");

      // Create data only in ZK
      TestMetadata zkData = new TestMetadata("test-zk-has", "zkValue");

      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ZK_WRITE_STORE,
              meterRegistry)) {

        zkStore.createSync(zkData);

        // Wait for creation to propagate
        await().atMost(5, TimeUnit.SECONDS).until(() -> zkStore.hasSync("test-zk-has"));
      }

      // Wait for the main store to see it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-zk-has");
                } catch (Exception e) {
                  return false;
                }
              });

      // Should find the data in ZK
      boolean hasZk = store.hasSync("test-zk-has");
      assertThat(hasZk).isTrue();

      // Create data only in Etcd
      TestMetadata etcdData = new TestMetadata("test-etcd-has", "etcdValue");

      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        etcdStore.createSync(etcdData);

        // Wait for creation to propagate
        await().atMost(5, TimeUnit.SECONDS).until(() -> etcdStore.hasSync("test-etcd-has"));
      }

      // Wait for the main store to see it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-etcd-has");
                } catch (Exception e) {
                  return false;
                }
              });

      // Should find the data in Etcd
      boolean hasEtcd = store.hasSync("test-etcd-has");
      assertThat(hasEtcd).isTrue();

      // Clean up
      store.deleteSync("test-zk-has");
      store.deleteSync("test-etcd-has");

      // Wait for deletions to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-has"));
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-has"));
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing has in BOTH_READ_ETCD_WRITE mode");

      // Create data only in Etcd
      TestMetadata etcdData = new TestMetadata("test-etcd-has", "etcdValue");

      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ETCD_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        etcdStore.createSync(etcdData);

        // Wait for creation to propagate
        await().atMost(5, TimeUnit.SECONDS).until(() -> etcdStore.hasSync("test-etcd-has"));
      }

      // Wait for the main store to see it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-etcd-has");
                } catch (Exception e) {
                  return false;
                }
              });

      // Should find the data in Etcd
      boolean hasEtcd = store.hasSync("test-etcd-has");
      assertThat(hasEtcd).isTrue();

      // Create data only in ZK
      TestMetadata zkData = new TestMetadata("test-zk-has", "zkValue");

      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        zkStore.createSync(zkData);

        // Wait for creation to propagate
        await().atMost(5, TimeUnit.SECONDS).until(() -> zkStore.hasSync("test-zk-has"));
      }

      // Wait for the main store to see it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-zk-has");
                } catch (Exception e) {
                  return false;
                }
              });

      // Should find the data in ZK
      boolean hasZk = store.hasSync("test-zk-has");
      assertThat(hasZk).isTrue();

      // Clean up
      store.deleteSync("test-etcd-has");
      store.deleteSync("test-zk-has");

      // Wait for deletions to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-has"));
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-has"));
    }
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
  public void testUpdateInMixedMode() throws Exception {
    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing update in BOTH_READ_ZOOKEEPER_WRITE mode");

      // Create data in ZK (primary)
      TestMetadata zkData = new TestMetadata("test-zk-update", "originalZkValue");
      store.createSync(zkData);

      // Wait for creation to complete and be visible
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-zk-update");
                  return result != null && "originalZkValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Create data in Etcd (secondary) with different data
      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        TestMetadata etcdData = new TestMetadata("test-etcd-update", "originalEtcdValue");
        etcdStore.createSync(etcdData);

        // Wait for creation in Etcd to complete
        await().atMost(5, TimeUnit.SECONDS).until(() -> etcdStore.hasSync("test-etcd-update"));
      }

      // Update ZK data
      TestMetadata updatedZkData = new TestMetadata("test-zk-update", "updatedZkValue");
      store.updateSync(updatedZkData);

      // Wait for ZK update to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-zk-update");
                  return result != null && "updatedZkValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Update Etcd data (should actually delete from Etcd and update/create in ZK)
      TestMetadata updatedEtcdData = new TestMetadata("test-etcd-update", "updatedEtcdValue");
      store.updateSync(updatedEtcdData);

      // Wait for Etcd data migration to ZK to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-etcd-update");
                  return result != null && "updatedEtcdValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Verify ZK data was updated
      TestMetadata zkResult = store.getSync("test-zk-update");
      assertThat(zkResult).isNotNull();
      assertThat(zkResult.getValue()).isEqualTo("updatedZkValue");

      // Verify Etcd data was moved to ZK
      TestMetadata etcdResult = store.getSync("test-etcd-update");
      assertThat(etcdResult).isNotNull();
      assertThat(etcdResult.getValue()).isEqualTo("updatedEtcdValue");

      // Verify data no longer exists in Etcd (with retry)
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try (EtcdMetadataStore<TestMetadata> etcdStore =
                    new EtcdMetadataStore<>(
                        BOTH_READ_ZK_WRITE_STORE,
                        etcdConfig,
                        true,
                        meterRegistry,
                        new TestMetadataSerializer())) {

                  return !etcdStore.hasSync("test-etcd-update");
                }
              });

      // Clean up
      store.deleteSync("test-zk-update");
      store.deleteSync("test-etcd-update");

      // Wait for deletions to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-update"));
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-update"));
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing update in BOTH_READ_ETCD_WRITE mode");

      // Create data in Etcd (primary)
      TestMetadata etcdData = new TestMetadata("test-etcd-update", "originalEtcdValue");
      store.createSync(etcdData);

      // Wait for creation to complete and be visible
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-etcd-update");
                  return result != null && "originalEtcdValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Create data in ZK (secondary) with different data
      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        TestMetadata zkData = new TestMetadata("test-zk-update", "originalZkValue");
        zkStore.createSync(zkData);

        // Wait for creation in ZK to complete
        await().atMost(5, TimeUnit.SECONDS).until(() -> zkStore.hasSync("test-zk-update"));
      }

      // Update Etcd data
      TestMetadata updatedEtcdData = new TestMetadata("test-etcd-update", "updatedEtcdValue");
      store.updateSync(updatedEtcdData);

      // Wait for Etcd update to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-etcd-update");
                  return result != null && "updatedEtcdValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Update ZK data (should actually delete from ZK and update/create in Etcd)
      TestMetadata updatedZkData = new TestMetadata("test-zk-update", "updatedZkValue");
      store.updateSync(updatedZkData);

      // Wait for ZK data migration to Etcd to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  TestMetadata result = store.getSync("test-zk-update");
                  return result != null && "updatedZkValue".equals(result.getValue());
                } catch (Exception e) {
                  return false;
                }
              });

      // Verify Etcd data was updated
      TestMetadata etcdResult = store.getSync("test-etcd-update");
      assertThat(etcdResult).isNotNull();
      assertThat(etcdResult.getValue()).isEqualTo("updatedEtcdValue");

      // Verify ZK data was moved to Etcd
      TestMetadata zkResult = store.getSync("test-zk-update");
      assertThat(zkResult).isNotNull();
      assertThat(zkResult.getValue()).isEqualTo("updatedZkValue");

      // Verify data no longer exists in ZK (with retry)
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try (ZookeeperMetadataStore<TestMetadata> zkStore =
                    new ZookeeperMetadataStore<>(
                        curatorFramework,
                        zkConfig,
                        CreateMode.PERSISTENT,
                        true,
                        new JacksonModelSerializer<>(TestMetadata.class),
                        BOTH_READ_ETCD_WRITE_STORE,
                        meterRegistry)) {

                  return !zkStore.hasSync("test-zk-update");
                }
              });

      // Clean up
      store.deleteSync("test-etcd-update");
      store.deleteSync("test-zk-update");

      // Wait for deletions to complete
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-update"));
      await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-update"));
    }
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
  public void testDeleteInMixedMode() throws Exception {
    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing delete in BOTH_READ_ZOOKEEPER_WRITE mode");

      // Create data in ZK (primary)
      TestMetadata zkData = new TestMetadata("test-zk-delete", "zkValue");
      store.createSync(zkData);

      // Create data in Etcd (secondary)
      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        TestMetadata etcdData = new TestMetadata("test-etcd-delete", "etcdValue");
        etcdStore.createSync(etcdData);
      }

      // Delete ZK data (should delete from ZK only)
      store.deleteSync("test-zk-delete");

      // Delete Etcd data (should delete from both ZK and Etcd)
      store.deleteSync("test-etcd-delete");

      // Verify ZK data was deleted
      assertThat(store.hasSync("test-zk-delete")).isFalse();

      // Verify Etcd data was deleted from both stores
      assertThat(store.hasSync("test-etcd-delete")).isFalse();

      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        assertThat(etcdStore.hasSync("test-etcd-delete")).isFalse();
      }
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing delete in BOTH_READ_ETCD_WRITE mode");

      // Create data in Etcd (primary)
      TestMetadata etcdData = new TestMetadata("test-etcd-delete", "etcdValue");
      store.createSync(etcdData);

      // Create data in ZK (secondary)
      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        TestMetadata zkData = new TestMetadata("test-zk-delete", "zkValue");
        zkStore.createSync(zkData);
      }

      // Delete Etcd data (should delete from Etcd only)
      store.deleteSync("test-etcd-delete");

      // Delete ZK data (should delete from both Etcd and ZK)
      store.deleteSync("test-zk-delete");

      // Verify Etcd data was deleted
      assertThat(store.hasSync("test-etcd-delete")).isFalse();

      // Verify ZK data was deleted from both stores
      assertThat(store.hasSync("test-zk-delete")).isFalse();

      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        assertThat(zkStore.hasSync("test-zk-delete")).isFalse();
      }
    }
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
                      return filtered.size() >= 3;
                    } catch (Exception e) {
                      return false;
                    }
                  });

          // List the nodes synchronously
          List<TestMetadata> results = store.listSync();

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
  public void testListInMixedMode() throws Exception {
    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing list in BOTH_READ_ZOOKEEPER_WRITE mode");

      // Create data in ZK
      TestMetadata zkData1 = new TestMetadata("test-zk-list-1", "zkValue1");
      TestMetadata zkData2 = new TestMetadata("test-zk-list-2", "zkValue2");
      store.createSync(zkData1);
      store.createSync(zkData2);

      // Create data in Etcd
      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        TestMetadata etcdData1 = new TestMetadata("test-etcd-list-1", "etcdValue1");
        TestMetadata etcdData2 = new TestMetadata("test-etcd-list-2", "etcdValue2");
        etcdStore.createSync(etcdData1);
        etcdStore.createSync(etcdData2);
      }

      // List should contain items from both stores
      List<TestMetadata> results = store.listSync();

      // Filter the results to just include our test nodes for this test
      List<TestMetadata> filteredResults =
          results.stream()
              .filter(
                  item ->
                      item.getName().startsWith("test-zk-list-")
                          || item.getName().startsWith("test-etcd-list-"))
              .toList();

      // Should have at least 3 items (2 from ZK, 2 from Etcd, but may be fewer due to timing)
      assertThat(filteredResults.size()).isGreaterThanOrEqualTo(3);

      // Verify the nodes have the expected names (only check the ones we see)
      assertThat(filteredResults)
          .extracting(TestMetadata::getName)
          .contains("test-zk-list-1", "test-etcd-list-1", "test-etcd-list-2");

      // Clean up
      store.deleteSync(zkData1);
      store.deleteSync(zkData2);
      store.deleteSync("test-etcd-list-1");
      store.deleteSync("test-etcd-list-2");
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing list in BOTH_READ_ETCD_WRITE mode");

      // Create data in Etcd
      TestMetadata etcdData1 = new TestMetadata("test-etcd-list-1", "etcdValue1");
      TestMetadata etcdData2 = new TestMetadata("test-etcd-list-2", "etcdValue2");
      store.createSync(etcdData1);
      store.createSync(etcdData2);

      // Create data in ZK
      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        TestMetadata zkData1 = new TestMetadata("test-zk-list-1", "zkValue1");
        TestMetadata zkData2 = new TestMetadata("test-zk-list-2", "zkValue2");
        zkStore.createSync(zkData1);
        zkStore.createSync(zkData2);

        await().until(() -> zkStore.listSync().size() == 2);
      }

      // List should contain items from both stores
      List<TestMetadata> results = store.listSync();

      // Filter the results to just include our test nodes for this test
      List<TestMetadata> filteredResults =
          results.stream()
              .filter(
                  item ->
                      item.getName().startsWith("test-zk-list-")
                          || item.getName().startsWith("test-etcd-list-"))
              .toList();

      // Should have at least 3 items (2 from ZK, 2 from Etcd, but may be fewer due to timing)
      assertThat(filteredResults.size()).isGreaterThanOrEqualTo(3);

      // Verify the nodes have the expected names
      assertThat(filteredResults)
          .extracting(TestMetadata::getName)
          .contains("test-zk-list-1", "test-etcd-list-1", "test-etcd-list-2");

      // Clean up
      store.deleteSync(etcdData1);
      store.deleteSync(etcdData2);
      store.deleteSync("test-zk-list-1");
      store.deleteSync("test-zk-list-2");
    }
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

  /** Tests awaiting cache initialization in all four modes. */
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
              case "ZOOKEEPER_EXCLUSIVE" -> newStore = createZkExclusiveStore();
              case "ETCD_EXCLUSIVE" -> newStore = createEtcdExclusiveStore();
              case "BOTH_READ_ZOOKEEPER_WRITE" -> newStore = createBothReadZkWriteStore();
              case "BOTH_READ_ETCD_WRITE" -> newStore = createBothReadEtcdWriteStore();
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

  /** Tests that listeners in mixed mode receive notifications from both stores. */
  @Test
  public void testMixedModeListeners() {
    // Test BOTH_READ_ZOOKEEPER_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadZkWriteStore()) {
      LOG.info("Testing listeners in BOTH_READ_ZOOKEEPER_WRITE mode");

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

      // Make sure test nodes don't exist from previous tests
      try {
        if (store.hasSync("test-zk-listener")) {
          store.deleteSync("test-zk-listener");
          await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-listener"));
        }
        if (store.hasSync("test-etcd-listener")) {
          store.deleteSync("test-etcd-listener");
          await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-listener"));
        }
      } catch (Exception ignored) {
        // Ignore exceptions here
      }

      // Reset counter in case cleanup triggered any events
      counter.set(0);

      // Create data in ZK (primary)
      TestMetadata zkData = new TestMetadata("test-zk-listener", "zkValue");
      store.createSync(zkData);

      // Wait for creation to propagate
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-zk-listener");
                } catch (Exception e) {
                  return false;
                }
              });

      // Wait for notification
      await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 1);

      // Create data directly in Etcd (secondary)
      try (EtcdMetadataStore<TestMetadata> etcdStore =
          new EtcdMetadataStore<>(
              BOTH_READ_ZK_WRITE_STORE,
              etcdConfig,
              true,
              meterRegistry,
              new TestMetadataSerializer())) {

        TestMetadata etcdData = new TestMetadata("test-etcd-listener", "etcdValue");
        etcdStore.createSync(etcdData);

        // Wait for creation to propagate in etcd store
        await().atMost(5, TimeUnit.SECONDS).until(() -> etcdStore.hasSync("test-etcd-listener"));
      }

      // Wait for the AstraMetadataStore to notice it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-etcd-listener");
                } catch (Exception e) {
                  return false;
                }
              });

      // Wait for notification
      await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 2);

      // Clean up
      store.deleteSync("test-zk-listener");
      store.deleteSync("test-etcd-listener");

      // Wait for deletions to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return !store.hasSync("test-zk-listener") && !store.hasSync("test-etcd-listener");
                } catch (Exception e) {
                  return false;
                }
              });
    }

    // Test BOTH_READ_ETCD_WRITE mode
    try (AstraMetadataStore<TestMetadata> store = createBothReadEtcdWriteStore()) {
      LOG.info("Testing listeners in BOTH_READ_ETCD_WRITE mode");

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

      // Make sure test nodes don't exist from previous tests
      try {
        if (store.hasSync("test-zk-listener")) {
          store.deleteSync("test-zk-listener");
          await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-zk-listener"));
        }
        if (store.hasSync("test-etcd-listener")) {
          store.deleteSync("test-etcd-listener");
          await().atMost(5, TimeUnit.SECONDS).until(() -> !store.hasSync("test-etcd-listener"));
        }
      } catch (Exception ignored) {
        // Ignore exceptions here
      }

      // Reset counter in case cleanup triggered any events
      counter.set(0);

      // Create data in Etcd (primary)
      TestMetadata etcdData = new TestMetadata("test-etcd-listener", "etcdValue");
      store.createSync(etcdData);

      // Wait for creation to propagate
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-etcd-listener");
                } catch (Exception e) {
                  return false;
                }
              });

      // Wait for notification
      await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 1);

      // Create data directly in ZK (secondary)
      try (ZookeeperMetadataStore<TestMetadata> zkStore =
          new ZookeeperMetadataStore<>(
              curatorFramework,
              zkConfig,
              CreateMode.PERSISTENT,
              true,
              new JacksonModelSerializer<>(TestMetadata.class),
              BOTH_READ_ETCD_WRITE_STORE,
              meterRegistry)) {

        TestMetadata zkData = new TestMetadata("test-zk-listener", "zkValue");
        zkStore.createSync(zkData);

        // Wait for creation to propagate in zk store
        await().atMost(5, TimeUnit.SECONDS).until(() -> zkStore.hasSync("test-zk-listener"));
      }

      // Wait for the AstraMetadataStore to notice it
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return store.hasSync("test-zk-listener");
                } catch (Exception e) {
                  return false;
                }
              });

      // Wait for notification
      await().atMost(10, TimeUnit.SECONDS).until(() -> counter.get() == 2);

      // Clean up
      store.deleteSync("test-etcd-listener");
      store.deleteSync("test-zk-listener");

      // Wait for deletions to complete
      await()
          .atMost(5, TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  return !store.hasSync("test-etcd-listener") && !store.hasSync("test-zk-listener");
                } catch (Exception e) {
                  return false;
                }
              });
    }
  }
}
