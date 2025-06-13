package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.Etcd;
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
    etcdCluster = Etcd.builder().withClusterName("etcd-test").withNodes(1).build();
    etcdCluster.start();
    LOG.info(
        "Embedded etcd cluster started with endpoints: {}",
        etcdCluster.clientEndpoints().stream().map(Object::toString).toList());
  }

  @AfterAll
  public static void tearDownClass() {
    if (etcdCluster != null) {
      LOG.info("Stopping embedded etcd cluster");
      etcdCluster.close();
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
            .setMaxRetries(3)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlSeconds(60)
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
    // Create test metadata objects
    TestMetadata testData1 = new TestMetadata("list1", "data1");
    TestMetadata testData2 = new TestMetadata("list2", "data2");
    TestMetadata testData3 = new TestMetadata("list3", "data3");

    store.createSync(testData1);
    store.createSync(testData2);
    store.createSync(testData3);

    // List async
    List<TestMetadata> nodes = store.listAsync().toCompletableFuture().get();
    assertThat(nodes).hasSize(3);

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
    assertThat(counter.get()).isEqualTo(0);

    // Create a node - should trigger listener
    TestMetadata testData = new TestMetadata("watch1", "data");
    store.createSync(testData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 1);

    // Update node - should trigger listener again
    TestMetadata updatedData = new TestMetadata("watch1", "updated");
    store.updateSync(updatedData);

    // Wait for notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> counter.get() == 2);

    // Remove listener
    store.removeListener(listener);
    TimeUnit.MILLISECONDS.sleep(200);

    // Update node again - should not trigger listener
    TestMetadata updatedData2 = new TestMetadata("watch1", "updated again");
    store.updateSync(updatedData2);

    // Wait a bit to see if counter changes
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(counter.get()).isEqualTo(2);
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
            .setMaxRetries(3)
            .setRetryDelayMs(100)
            .setNamespace("test")
            .setEphemeralNodeTtlSeconds(60)
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
}
