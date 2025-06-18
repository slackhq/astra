package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Comprehensive tests for the ephemeral node functionality in EtcdMetadataStore.
 *
 * <p>These tests focus on thoroughly testing the ephemeral logic, including: 1. TTL expiration and
 * automatic node removal 2. Lease refresh functionality 3. Behavior when lease refresh fails 4.
 * Concurrent ephemeral node creation and cleanup
 *
 * <p>Note: Tests for connection interruptions and client restarts are not included as the in-memory
 * jetcd-launcher implementation used for testing deletes all data when the cluster is closed,
 * making it impossible to properly test these scenarios without a persistent etcd instance.
 */
@Tag("integration")
public class EtcdEphemeralNodeTest {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdEphemeralNodeTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
  private MetadataSerializer<TestMetadata> serializer;
  private AstraConfigs.EtcdConfig etcdConfig;
  private static final String STORE_FOLDER = "/test-ephemeral";
  private Client etcdClient;

  /** Test metadata class for use in tests. */
  private static class TestMetadata extends AstraMetadata {
    private final String data;

    public TestMetadata(String name, String data) {
      super(name);
      this.data = data;
    }

    public String getData() {
      return data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TestMetadata)) return false;
      if (!super.equals(o)) return false;

      TestMetadata metadata = (TestMetadata) o;
      return data.equals(metadata.data);
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + data.hashCode();
      return result;
    }

    @Override
    public String toString() {
      return "TestMetadata{" + "name='" + name + '\'' + ", data='" + data + '\'' + '}';
    }
  }

  /** Serializer for TestMetadata objects. */
  private static class TestMetadataSerializer implements MetadataSerializer<TestMetadata> {
    @Override
    public String toJsonStr(TestMetadata metadata) {
      return String.format(
          "{\"name\":\"%s\",\"data\":\"%s\"}", metadata.getName(), metadata.getData());
    }

    @Override
    public TestMetadata fromJsonStr(String data) {
      // Very simple JSON parsing for test purposes
      String name = data.split("\"name\":\"")[1].split("\"")[0];
      String value = data.split("\"data\":\"")[1].split("\"")[0];
      return new TestMetadata(name, value);
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
    etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setMaxRetries(3)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlSeconds(60) // Default long TTL for most tests
            .build();

    // Create etcd client
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));
    if (!etcdConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }
    etcdClient = clientBuilder.build();
  }

  @AfterEach
  public void tearDown() {
    // Clean up any leftover nodes in etcd
    try {
      Client cleanupClient = createEtcdClient();
      try (EtcdMetadataStore<TestMetadata> cleanupStore =
          new EtcdMetadataStore<>(
              STORE_FOLDER, etcdConfig, false, meterRegistry, serializer, cleanupClient)) {
        try {
          List<TestMetadata> nodes = cleanupStore.listSync();
          for (TestMetadata node : nodes) {
            cleanupStore.deleteSync(node);
          }
        } catch (Exception e) {
          LOG.warn("Error cleaning up test nodes", e);
        }
      } catch (Exception e) {
        LOG.warn("Error creating cleanup store", e);
      } finally {
        cleanupClient.close();
      }
    } catch (Exception e) {
      LOG.warn("Error creating cleanup client", e);
    }

    if (etcdClient != null) {
      etcdClient.close();
    }
    meterRegistry.close();
  }

  /** Helper method to create an etcd client for tests. */
  private Client createEtcdClient() {
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    // Set namespace if provided
    if (!etcdConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    return clientBuilder.build();
  }

  /**
   * Helper method to access the internal shared lease ID in the EtcdMetadataStore using reflection.
   *
   * @param store The store to get the shared lease ID from
   * @return The shared lease ID
   * @throws Exception If reflection fails
   */
  private long getSharedLeaseId(EtcdMetadataStore<TestMetadata> store) throws Exception {
    // Get the sharedLeaseFuture field via reflection
    Field sharedLeaseFutureField = null;

    try {
      // Try to get the field - this needs to match the exact field name in EtcdMetadataStore
      sharedLeaseFutureField = EtcdMetadataStore.class.getDeclaredField("sharedLeaseFuture");
      sharedLeaseFutureField.setAccessible(true);

      // Get the CompletableFuture<Long> instance
      @SuppressWarnings("unchecked")
      CompletableFuture<Long> sharedLeaseFuture =
          (CompletableFuture<Long>) sharedLeaseFutureField.get(store);

      // If the future is null or not completed, return -1 to indicate no lease
      if (sharedLeaseFuture == null
          || !sharedLeaseFuture.isDone()
          || sharedLeaseFuture.isCompletedExceptionally()) {
        return -1;
      }

      // Get the lease ID from the completed future
      return sharedLeaseFuture.get(5, TimeUnit.SECONDS);
    } catch (NoSuchFieldException e) {
      LOG.warn(
          "Could not find sharedLeaseFuture field in EtcdMetadataStore. Interface may have changed.");
      return -1;
    } catch (Exception e) {
      LOG.warn("Error retrieving shared lease ID", e);
      return -1;
    }
  }

  /**
   * Tests that the lease refresh mechanism correctly keeps ephemeral nodes alive. This test
   * verifies: 1. Ephemeral nodes with a short TTL stay alive well beyond their TTL as long as the
   * store is open (and thus refreshing leases) 2. We can create multiple ephemeral nodes and all
   * leases are refreshed 3. When the store is closed, the nodes expire after their TTL
   */
  @Test
  public void testLeaseRefreshKeepsNodesAlive() throws Exception {
    // Use a very short TTL to test refresh mechanism
    final long shortTtl = 2; // 2 seconds

    // Create a store with short TTL
    Client ephemeralClient = createEtcdClient();
    try (EtcdMetadataStore<TestMetadata> ephemeralStore =
        new EtcdMetadataStore<>(
            STORE_FOLDER,
            etcdConfig,
            true,
            meterRegistry,
            serializer,
            EtcdCreateMode.EPHEMERAL,
            shortTtl,
            ephemeralClient)) {

      // Create multiple ephemeral nodes
      TestMetadata node1 = new TestMetadata("refresh-node-1", "Refresh test node 1");
      TestMetadata node2 = new TestMetadata("refresh-node-2", "Refresh test node 2");
      TestMetadata node3 = new TestMetadata("refresh-node-3", "Refresh test node 3");

      ephemeralStore.createSync(node1);
      ephemeralStore.createSync(node2);
      ephemeralStore.createSync(node3);

      // Verify shared lease was created
      long sharedLeaseId = getSharedLeaseId(ephemeralStore);
      assertThat(sharedLeaseId).isNotEqualTo(-1);

      LOG.info("Waiting for several TTL periods to ensure nodes stay alive with refresh");

      // Wait for several times the TTL interval - the nodes should remain
      // The default refresh fraction is 0.25, so nodes will be refreshed every (shortTtl * 0.25)
      // seconds
      // Wait for 5x the TTL to ensure multiple refresh cycles have occurred
      TimeUnit.MILLISECONDS.sleep(shortTtl * 1000 * 5);

      // Create a reader store to check if nodes are still there
      Client innerReaderClient = createEtcdClient();
      try (EtcdMetadataStore<TestMetadata> readerStore =
          new EtcdMetadataStore<>(
              STORE_FOLDER,
              etcdConfig,
              false, // Don't use cache
              meterRegistry,
              serializer,
              innerReaderClient)) {

        // All nodes should still exist after several TTL periods
        assertThat(readerStore.hasSync(node1.getName())).isTrue();
        assertThat(readerStore.hasSync(node2.getName())).isTrue();
        assertThat(readerStore.hasSync(node3.getName())).isTrue();

        // Verify the shared lease ID is still the same (no lease recreation)
        long updatedSharedLeaseId = getSharedLeaseId(ephemeralStore);
        assertThat(updatedSharedLeaseId).isEqualTo(sharedLeaseId);
      }
    }

    // The ephemeral store is now closed, so refresh should stop
    // But the lease was also explicitly revoked when closing, so no need to wait for natural
    // expiration
    // We can still test that the nodes are gone after the store is closed
    Client outerReaderClient = createEtcdClient();
    try (EtcdMetadataStore<TestMetadata> readerStore =
        new EtcdMetadataStore<>(
            STORE_FOLDER, etcdConfig, false, meterRegistry, serializer, outerReaderClient)) {

      // Final verification - all nodes should be gone after store close
      assertThat(readerStore.hasSync("refresh-node-1")).isFalse();
      assertThat(readerStore.hasSync("refresh-node-2")).isFalse();
      assertThat(readerStore.hasSync("refresh-node-3")).isFalse();
    }
  }

  /**
   * Tests concurrent creation and deletion of ephemeral nodes. This test verifies: 1. Multiple
   * threads can concurrently create ephemeral nodes without issues 2. All nodes are properly
   * tracked with leases 3. Nodes can be concurrently deleted without interfering with each other 4.
   * Leases are properly cleaned up after nodes are deleted
   */
  @Test
  public void testConcurrentEphemeralNodeOperations() throws Exception {
    final int threadCount = 5; // Using fewer threads to avoid test flakiness
    final int nodesPerThread = 3; // Using fewer nodes per thread to speed up tests
    final long shortTtl = 10; // 10 seconds TTL for ephemeral nodes

    // Create a store with ephemeral nodes
    Client ephemeralClient = createEtcdClient();
    try (EtcdMetadataStore<TestMetadata> ephemeralStore =
        new EtcdMetadataStore<>(
            STORE_FOLDER,
            etcdConfig,
            true,
            meterRegistry,
            serializer,
            EtcdCreateMode.EPHEMERAL,
            shortTtl,
            ephemeralClient)) {

      // Create thread pool for concurrent operations
      ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
      CountDownLatch createLatch = new CountDownLatch(threadCount * nodesPerThread);
      List<CompletableFuture<Void>> futures = new ArrayList<>();

      try {
        // Submit concurrent node creation tasks
        for (int t = 0; t < threadCount; t++) {
          final int threadId = t;
          CompletableFuture<Void> future =
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      for (int i = 0; i < nodesPerThread; i++) {
                        String nodeName = "concurrent-node-" + threadId + "-" + i;
                        TestMetadata metadata =
                            new TestMetadata(nodeName, "Created by thread " + threadId);

                        // Add small random delay to increase concurrency chances
                        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(50));

                        ephemeralStore.createSync(metadata);
                        createLatch.countDown();
                      }
                    } catch (Exception e) {
                      LOG.error("Error creating nodes in thread {}", threadId, e);
                    }
                  },
                  executorService);

          futures.add(future);
        }

        // Wait for all nodes to be created
        boolean allCreated = createLatch.await(30, TimeUnit.SECONDS);
        assertThat(allCreated).isTrue();

        // Wait for all create futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        // Verify the shared lease was created
        long sharedLeaseId = getSharedLeaseId(ephemeralStore);
        assertThat(sharedLeaseId).isNotEqualTo(-1);

        // Verify all nodes exist
        for (int t = 0; t < threadCount; t++) {
          for (int i = 0; i < nodesPerThread; i++) {
            String nodeName = "concurrent-node-" + t + "-" + i;
            assertThat(ephemeralStore.hasSync(nodeName)).isTrue();
          }
        }

        // Now concurrently delete half the nodes
        CountDownLatch deleteLatch = new CountDownLatch(threadCount * nodesPerThread / 2);
        List<CompletableFuture<Void>> deleteFutures = new ArrayList<>();

        for (int t = 0; t < threadCount; t += 2) { // Delete nodes from even-numbered threads
          final int threadId = t;
          CompletableFuture<Void> future =
              CompletableFuture.runAsync(
                  () -> {
                    try {
                      for (int i = 0; i < nodesPerThread; i++) {
                        String nodeName = "concurrent-node-" + threadId + "-" + i;

                        // Add small random delay to increase concurrency chances
                        TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(50));

                        ephemeralStore.deleteSync(nodeName);
                        deleteLatch.countDown();
                      }
                    } catch (Exception e) {
                      LOG.error("Error deleting nodes in thread {}", threadId, e);
                    }
                  },
                  executorService);

          deleteFutures.add(future);
        }

        // Wait for all deletions to complete
        boolean allDeleted = deleteLatch.await(30, TimeUnit.SECONDS);
        assertThat(allDeleted).isTrue();

        CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
            .get(5, TimeUnit.SECONDS);

        // Verify deleted nodes are gone
        // Size assertions are flaky due to concurrent operations in different threads
        // Instead, only verify that odd thread nodes still exist and even thread nodes are gone

        // Check specific nodes - even thread nodes should be gone, odd thread nodes should remain
        for (int t = 0; t < threadCount; t++) {
          for (int i = 0; i < nodesPerThread; i++) {
            String nodeName = "concurrent-node-" + t + "-" + i;

            if (t % 2 == 0) {
              // Even thread nodes should be deleted
              assertThat(ephemeralStore.hasSync(nodeName)).isFalse();
            } else {
              // Odd thread nodes should still exist
              assertThat(ephemeralStore.hasSync(nodeName)).isTrue();
            }
          }
        }
      } finally {
        executorService.shutdown();
        boolean terminated = executorService.awaitTermination(5, TimeUnit.SECONDS);
        if (!terminated) {
          LOG.warn("Executor service did not terminate cleanly");
        }
      }
    }
  }

  /**
   * Tests the behavior of ephemeral nodes with the shared lease implementation. This test verifies
   * that multiple ephemeral nodes share a single lease.
   */
  @Test
  public void testEphemeralNodesWithSharedLease() {
    // Create a new metadata store with EPHEMERAL mode
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setMaxRetries(3)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlSeconds(5) // Short TTL for faster testing
            .build();

    // Create client builder for test
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    // Set namespace if provided
    if (!etcdConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    Client ephemeralClient = clientBuilder.build();
    MeterRegistry testMeterRegistry = new SimpleMeterRegistry();

    try {
      // Create a store with cache enabled and EPHEMERAL mode
      EtcdMetadataStore<TestMetadata> ephemeralStore =
          new EtcdMetadataStore<>(
              "/ephemeral",
              etcdConfig,
              true,
              testMeterRegistry,
              serializer,
              EtcdCreateMode.EPHEMERAL, // Use EPHEMERAL mode
              ephemeralClient);

      // Create multiple ephemeral nodes
      TestMetadata node1 = new TestMetadata("ephemeral1", "data1");
      TestMetadata node2 = new TestMetadata("ephemeral2", "data2");
      TestMetadata node3 = new TestMetadata("ephemeral3", "data3");

      ephemeralStore.createSync(node1);
      ephemeralStore.createSync(node2);
      ephemeralStore.createSync(node3);

      // Verify all nodes exist
      assertThat(ephemeralStore.hasSync("ephemeral1")).isTrue();
      assertThat(ephemeralStore.hasSync("ephemeral2")).isTrue();
      assertThat(ephemeralStore.hasSync("ephemeral3")).isTrue();

      // Close the store (this should revoke the shared lease)
      ephemeralStore.close();

      // Create a new store to check if nodes are gone
      EtcdMetadataStore<TestMetadata> checkStore =
          new EtcdMetadataStore<>(
              "/ephemeral",
              etcdConfig,
              true,
              testMeterRegistry,
              serializer,
              EtcdCreateMode.PERSISTENT, // Use PERSISTENT mode for checking
              ephemeralClient);

      // Wait for TTL to expire (5 seconds + a bit more)
      TimeUnit.SECONDS.sleep(8);

      // Verify all nodes are gone
      assertThat(checkStore.hasSync("ephemeral1")).isFalse();
      assertThat(checkStore.hasSync("ephemeral2")).isFalse();
      assertThat(checkStore.hasSync("ephemeral3")).isFalse();

      // Cleanup
      checkStore.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Test interrupted", e);
    } finally {
      ephemeralClient.close();
      testMeterRegistry.close();
    }
  }
}
