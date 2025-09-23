package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for EtcdMetadataStore.
 *
 * <p>These tests use the jetcd-launcher to start a real etcd server in the test process.
 */
@Tag("integration")
public class EtcdMetadataStoreTest {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdMetadataStoreTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
  private EtcdMetadataStore<TestMetadata> store;
  private MetadataSerializer<TestMetadata> serializer;
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
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
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
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    etcdClient = clientBuilder.build();

    // Create store with cache enabled
    store =
        new EtcdMetadataStore<>("/test", etcdConfig, true, meterRegistry, serializer, etcdClient);
  }

  @AfterEach
  public void tearDown() {
    // Close the store and meter registry
    store.close();
    etcdClient.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateAndGet() throws ExecutionException, InterruptedException {
    // Create a test metadata object
    TestMetadata testData = new TestMetadata("test1", "testData");

    // Create the node
    String path = store.createAsync(testData).toCompletableFuture().get();
    assertThat(path).isEqualTo("test1");

    // Get the node
    TestMetadata result = store.getAsync("test1").toCompletableFuture().get();
    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo("test1");
    assertThat(result.getData()).isEqualTo("testData");
  }

  @Test
  public void testHasNode() throws ExecutionException, InterruptedException {
    // Create a test metadata object
    TestMetadata testData = new TestMetadata("test2", "testData");

    // Check node does not exist
    boolean exists = store.hasSync("test2");
    assertThat(exists).isFalse();

    // Create the node
    store.createSync(testData);

    // Check node exists asynchronously
    Boolean hasNode = store.hasAsync("test2").toCompletableFuture().get();
    assertThat(hasNode).isTrue();

    // Check node exists synchronously
    boolean nodeExists = store.hasSync("test2");
    assertThat(nodeExists).isTrue();
  }

  @Test
  public void testUpdateNode() throws ExecutionException, InterruptedException {
    // Create a test metadata object
    TestMetadata testData = new TestMetadata("test3", "initialData");
    store.createSync(testData);

    // Create updated version with same name but different data
    TestMetadata updatedData = new TestMetadata("test3", "updatedData");

    // Update the node
    String nodeName = store.updateAsync(updatedData).toCompletableFuture().get();
    assertThat(nodeName).isEqualTo("test3");

    // Get the updated node
    TestMetadata result = store.getSync("test3");
    assertThat(result).isNotNull();
    assertThat(result.getName()).isEqualTo("test3");
    assertThat(result.getData()).isEqualTo("updatedData");
  }

  @Test
  public void testDeleteNode() throws ExecutionException, InterruptedException {
    // Create test metadata objects
    TestMetadata testData1 = new TestMetadata("delete1", "data1");
    TestMetadata testData2 = new TestMetadata("delete2", "data2");

    store.createSync(testData1);
    store.createSync(testData2);

    // Verify both exist
    assertThat(store.hasSync("delete1")).isTrue();
    assertThat(store.hasSync("delete2")).isTrue();

    // Delete first node by path
    store.deleteAsync("delete1").toCompletableFuture().get();

    assertThat(store.hasSync("delete1")).isFalse();
    assertThat(store.hasSync("delete2")).isTrue();

    // Delete second node by reference
    store.deleteSync(testData2);

    assertThat(store.hasSync("delete2")).isFalse();
  }

  @Test
  public void testListNodes() throws ExecutionException, InterruptedException {
    store.listSyncUncached().forEach(s -> store.deleteSync(s));

    // Create test metadata objects
    TestMetadata testData1 = new TestMetadata("list1", "data1");
    TestMetadata testData2 = new TestMetadata("list2", "data2");
    TestMetadata testData3 = new TestMetadata("list3", "data3");

    store.createSync(testData1);
    store.createSync(testData2);
    store.createSync(testData3);

    // List async
    List<TestMetadata> nodes = store.listAsync().toCompletableFuture().get();
    assertThat(nodes.size()).isGreaterThanOrEqualTo(3);

    // Verify all nodes are in the list
    assertThat(nodes)
        .extracting(TestMetadata::getName)
        .containsExactlyInAnyOrder("list1", "list2", "list3");

    // List sync
    nodes = store.listSync();
    assertThat(nodes).hasSize(3);
    assertThat(nodes)
        .extracting(TestMetadata::getName)
        .containsExactlyInAnyOrder("list1", "list2", "list3");
  }

  @Test
  public void testListeners() throws InterruptedException {
    AtomicInteger counter = new AtomicInteger(0);
    AstraMetadataStoreChangeListener<TestMetadata> listener = unused -> counter.incrementAndGet();

    // Add listener
    store.addListener(listener);

    // Wait for listener to be registered
    TimeUnit.MILLISECONDS.sleep(200);

    // Initial count should be 0
    assertThat(counter.get()).isEqualTo(1);

    // Create a node - should trigger listener
    TestMetadata testData = new TestMetadata("watch1", "data");
    store.createSync(testData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 2);

    // Update node - should trigger listener again
    TestMetadata updatedData = new TestMetadata("watch1", "updated");
    store.updateSync(updatedData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 3);

    // Remove listener
    store.removeListener(listener);
    TimeUnit.MILLISECONDS.sleep(200);

    // Update node again - should not trigger listener
    TestMetadata updatedData2 = new TestMetadata("watch1", "updated again");
    store.updateSync(updatedData2);

    // Wait a bit to see if counter changes
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(counter.get()).isEqualTo(3);
  }

  @Test
  public void testDeleteNodeWithFullPath() throws ExecutionException, InterruptedException {
    // Create test metadata object
    TestMetadata testData = new TestMetadata("deleteFull", "data");

    // Create the node
    store.createSync(testData);

    // Verify node exists
    assertThat(store.hasSync("deleteFull")).isTrue();

    // Delete by reference (which was using just the node name, not the full path)
    store.deleteAsync(testData).toCompletableFuture().get();

    // Verify node was properly deleted
    assertThat(store.hasSync("deleteFull")).isFalse();
  }

  @Test
  public void testCacheInitialization() {
    // Create test data
    TestMetadata testData1 = new TestMetadata("cache1", "data1");
    TestMetadata testData2 = new TestMetadata("cache2", "data2");

    store.createSync(testData1);
    store.createSync(testData2);

    // Create a new store with cache enabled
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    EtcdMetadataStore<TestMetadata> newStore = null;
    Client newEtcdClient = null;
    try {
      // Create a new client for the new store
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

      newEtcdClient = clientBuilder.build();
      newStore =
          new EtcdMetadataStore<>(
              "/test", etcdConfig, true, meterRegistry, serializer, newEtcdClient);

      // Initialize the cache
      newStore.awaitCacheInitialized();

      // Instead of asserting the total size, filter for just the items we created specifically for
      // this test
      List<TestMetadata> cachedItems = newStore.listSync();
      List<TestMetadata> testItems =
          cachedItems.stream()
              .filter(item -> item.getName().equals("cache1") || item.getName().equals("cache2"))
              .toList();

      assertThat(testItems).hasSize(2);
      assertThat(testItems)
          .extracting(TestMetadata::getName)
          .containsExactlyInAnyOrder("cache1", "cache2");
    } finally {
      if (newStore != null) {
        newStore.close();
      }
      if (newEtcdClient != null) {
        newEtcdClient.close();
      }
    }
  }

  @Test
  public void testDuplicateCreate() {
    // Create a test metadata object
    TestMetadata testData = new TestMetadata("duplicateTest", "testData");

    // Create the node
    store.createSync(testData);

    // Try to create the same node again - should throw exception
    assertThatExceptionOfType(InternalMetadataStoreException.class)
        .isThrownBy(() -> store.createSync(testData));
  }

  @Test
  public void testInvalidNodeNames() {
    // Test dot node name
    TestMetadata dotNameNode = new TestMetadata(".", "testData");
    assertThatExceptionOfType(InternalMetadataStoreException.class)
        .isThrownBy(() -> store.createSync(dotNameNode));

    // Test slash node name
    TestMetadata slashNameNode = new TestMetadata("/", "testData");
    assertThatExceptionOfType(InternalMetadataStoreException.class)
        .isThrownBy(() -> store.createSync(slashNameNode));

    // For completeness, test empty and null cases, but these are already
    // validated by AstraMetadata constructor
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new TestMetadata("", "testData"));

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new TestMetadata(null, "testData"));
  }

  @Test
  public void testEphemeralLeaseRenewalRetryLogic() throws Exception {

    // Create a separate etcd config for ephemeral nodes with specific retry settings
    AstraConfigs.EtcdConfig ephemeralConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(1000) // Short timeout to trigger retries
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlMs(2000) // Short TTL for faster testing
            .setEphemeralNodeMaxRetries(2) // Test retry logic with 2 retries
            .build();

    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    if (!ephemeralConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(ephemeralConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    Client ephemeralClient = clientBuilder.build();
    MeterRegistry ephemeralMeterRegistry = new SimpleMeterRegistry();

    EtcdMetadataStore<TestMetadata> ephemeralStore = null;
    try {
      ephemeralStore =
          new EtcdMetadataStore<>(
              "/ephemeral-test",
              ephemeralConfig,
              true,
              ephemeralMeterRegistry,
              serializer,
              EtcdCreateMode.EPHEMERAL,
              ephemeralClient);

      TestMetadata ephemeralNode = new TestMetadata("ephemeral1", "ephemeralData");
      ephemeralStore.createSync(ephemeralNode);

      // Verify the node exists
      assertThat(ephemeralStore.hasSync("ephemeral1")).isTrue();

      // Wait for at least one lease refresh cycle to ensure the retry logic is exercised
      TimeUnit.MILLISECONDS.sleep(1000);

      // Verify the node still exists after lease refresh
      assertThat(ephemeralStore.hasSync("ephemeral1")).isTrue();

      // Check that the lease refresh counter incremented (indicating successful refreshes)
      double leaseRefreshCount =
          ephemeralMeterRegistry.get("astra_etcd_lease_refresh_handler_fired").counter().count();

      // Should have fired at least once
      assertThat(leaseRefreshCount).isGreaterThanOrEqualTo(1.0);

    } finally {
      if (ephemeralStore != null) {
        ephemeralStore.close();
      }
      ephemeralClient.close();
      ephemeralMeterRegistry.close();
    }
  }

  @Test
  public void testEphemeralLeaseRenewalFailureHandling() throws Exception {
    // This test verifies that the retry logic is properly implemented
    // We can't easily test the fatal error case in a unit test since it would exit the JVM
    // But we can test the retry configuration and logic path

    // Create a config with minimal retries and timeouts for faster testing
    AstraConfigs.EtcdConfig failureConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(500) // Very short timeout to potentially trigger retries
            .setRetryDelayMs(50) // Short delay between retries
            .setNamespace("test")
            .setEphemeralNodeTtlMs(1000) // Short TTL
            .setEphemeralNodeMaxRetries(1) // Only 1 retry attempt
            .build();

    // Create client for failure test
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    if (!failureConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(failureConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    Client failureClient = clientBuilder.build();
    MeterRegistry failureMeterRegistry = new SimpleMeterRegistry();

    EtcdMetadataStore<TestMetadata> failureStore = null;
    try {
      // Create ephemeral store with failure-prone configuration
      failureStore =
          new EtcdMetadataStore<>(
              "/failure-test",
              failureConfig,
              true,
              failureMeterRegistry,
              serializer,
              EtcdCreateMode.EPHEMERAL,
              failureClient);

      // Create an ephemeral node
      TestMetadata ephemeralNode = new TestMetadata("failure1", "failureData");
      failureStore.createSync(ephemeralNode);

      // Verify the node exists initially
      assertThat(failureStore.hasSync("failure1")).isTrue();

      TimeUnit.MILLISECONDS.sleep(800);

    } finally {
      if (failureStore != null) {
        failureStore.close();
      }
      failureClient.close();
      failureMeterRegistry.close();
    }
  }

  @Test
  public void testEphemeralLeaseRenewalInterruption() throws Exception {
    // Test that lease renewal handles interruption properly

    // Create a config for interruption testing
    AstraConfigs.EtcdConfig interruptConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlMs(1500) // Short TTL for faster testing
            .setEphemeralNodeMaxRetries(3)
            .build();

    // Create client for interrupt test
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    if (!interruptConfig.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(interruptConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    Client interruptClient = clientBuilder.build();
    MeterRegistry interruptMeterRegistry = new SimpleMeterRegistry();

    EtcdMetadataStore<TestMetadata> interruptStore = null;
    try {
      // Create ephemeral store
      interruptStore =
          new EtcdMetadataStore<>(
              "/interrupt-test",
              interruptConfig,
              true,
              interruptMeterRegistry,
              serializer,
              EtcdCreateMode.EPHEMERAL,
              interruptClient);

      // Create an ephemeral node
      TestMetadata ephemeralNode = new TestMetadata("interrupt1", "interruptData");
      interruptStore.createSync(ephemeralNode);

      // Verify the node exists initially
      assertThat(interruptStore.hasSync("interrupt1")).isTrue();

      // Let it run for a short time to establish lease refresh
      TimeUnit.MILLISECONDS.sleep(200);

      // Close the store, which should trigger interruption of lease refresh thread
      interruptStore.close();
      interruptStore = null; // Set to null so finally block doesn't try to close again

      // Test passes if we don't hang or throw unexpected exceptions
      // The close() method should properly interrupt the lease refresh thread
      // and handle the InterruptedException in refreshAllLeases()

    } finally {
      if (interruptStore != null) {
        interruptStore.close();
      }
      interruptClient.close();
      interruptMeterRegistry.close();
    }
  }

  @Test
  public void testQuickClose() {
    // Create a new etcd client
    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
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

    Client testClient = clientBuilder.build();
    MeterRegistry testMeterRegistry = new SimpleMeterRegistry();

    try {
      // Create a store with cache enabled
      EtcdMetadataStore<TestMetadata> testStore =
          new EtcdMetadataStore<>(
              "/quickclose", etcdConfig, true, testMeterRegistry, serializer, testClient);

      // Immediately close it without waiting for cache initialization
      testStore.close();

      // The test passes if we don't get a RuntimeHalter error (which would exit the JVM)
      // Create and close another store with the same client to verify it still works
      EtcdMetadataStore<TestMetadata> secondStore =
          new EtcdMetadataStore<>(
              "/quickclose2", etcdConfig, true, testMeterRegistry, serializer, testClient);

      // Add a node to verify the client is still working
      TestMetadata testData = new TestMetadata("verify", "testData");
      secondStore.createSync(testData);

      // Check that the node was created successfully
      TestMetadata result = secondStore.getSync("verify");
      assertThat(result).isNotNull();
      assertThat(result.getData()).isEqualTo("testData");

      // Close the store
      secondStore.close();
    } finally {
      testClient.close();
      testMeterRegistry.close();
    }
  }
}
