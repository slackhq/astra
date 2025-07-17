package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
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
 * Integration tests for EtcdMetadataStore cache invalidation across JVMs.
 *
 * <p>These tests verify that the default cache watcher properly updates the cache when changes are
 * made by other instances, simulating multiple JVMs.
 */
@Tag("integration")
public class EtcdCacheInvalidationTest {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdCacheInvalidationTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
  private EtcdMetadataStore<TestMetadata> store1; // Simulates first JVM
  private EtcdMetadataStore<TestMetadata> store2; // Simulates second JVM
  private MetadataSerializer<TestMetadata> serializer;
  private Client etcdClient1;
  private Client etcdClient2;

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

    // Create etcd clients (one for each "JVM")
    ClientBuilder clientBuilder1 =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    ClientBuilder clientBuilder2 =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    // Set namespace if provided
    if (!etcdConfig.getNamespace().isEmpty()) {
      clientBuilder1.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
      clientBuilder2.namespace(
          io.etcd.jetcd.ByteSequence.from(etcdConfig.getNamespace(), StandardCharsets.UTF_8));
    }

    etcdClient1 = clientBuilder1.build();
    etcdClient2 = clientBuilder2.build();

    // Create stores with cache enabled (simulating two different JVM instances)
    store1 =
        new EtcdMetadataStore<>("/test", etcdConfig, true, meterRegistry, serializer, etcdClient1);
    store2 =
        new EtcdMetadataStore<>("/test", etcdConfig, true, meterRegistry, serializer, etcdClient2);

    // Initialize caches
    store1.awaitCacheInitialized();
    store2.awaitCacheInitialized();
  }

  @AfterEach
  public void tearDown() {
    // Close the stores and clients
    if (store1 != null) store1.close();
    if (store2 != null) store2.close();
    if (etcdClient1 != null) etcdClient1.close();
    if (etcdClient2 != null) etcdClient2.close();
    meterRegistry.close();
  }

  @Test
  public void testCreateInOneStoreUpdatesOtherStoreCache() {
    // Create a test metadata object in store1
    TestMetadata testData = new TestMetadata("cross-jvm-create", "testData");
    store1.createSync(testData);

    // Verify store2's cache is updated automatically through the default watcher
    await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(store2.hasSync("cross-jvm-create")).isTrue();

              TestMetadata result = store2.getSync("cross-jvm-create");
              assertThat(result).isNotNull();
              assertThat(result.getName()).isEqualTo("cross-jvm-create");
              assertThat(result.getData()).isEqualTo("testData");
            });
  }

  @Test
  public void testUpdateInOneStoreUpdatesOtherStoreCache() {
    // Create a test metadata object in store1
    TestMetadata testData = new TestMetadata("cross-jvm-update", "initialData");
    store1.createSync(testData);

    // Wait for store2 to get the create notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> store2.hasSync("cross-jvm-update"));

    // Update the node in store1
    TestMetadata updatedData = new TestMetadata("cross-jvm-update", "updatedData");
    store1.updateSync(updatedData);

    // Verify store2's cache is updated with the new data
    await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              TestMetadata result = store2.getSync("cross-jvm-update");
              assertThat(result).isNotNull();
              assertThat(result.getName()).isEqualTo("cross-jvm-update");
              assertThat(result.getData()).isEqualTo("updatedData");
            });
  }

  @Test
  public void testDeleteInOneStoreUpdatesOtherStoreCache() {
    // Create a test metadata object in store1
    TestMetadata testData = new TestMetadata("cross-jvm-delete", "testData");
    store1.createSync(testData);

    // Wait for store2 to get the create notification
    await().atMost(2, TimeUnit.SECONDS).until(() -> store2.hasSync("cross-jvm-delete"));

    // Delete the node in store1
    store1.deleteSync("cross-jvm-delete");

    // Verify store2's cache is updated (node is removed)
    await().atMost(2, TimeUnit.SECONDS).until(() -> !store2.hasSync("cross-jvm-delete"));
  }

  @Test
  public void testBidirectionalCacheUpdates() {
    // Create metadata in store1
    TestMetadata data1 = new TestMetadata("bidirectional-1", "from-store1");
    store1.createSync(data1);

    // Create metadata in store2
    TestMetadata data2 = new TestMetadata("bidirectional-2", "from-store2");
    store2.createSync(data2);

    // Verify store1 sees store2's changes
    await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(store1.hasSync("bidirectional-2")).isTrue();
              TestMetadata result = store1.getSync("bidirectional-2");
              assertThat(result.getData()).isEqualTo("from-store2");
            });

    // Verify store2 sees store1's changes
    await()
        .atMost(2, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(store2.hasSync("bidirectional-1")).isTrue();
              TestMetadata result = store2.getSync("bidirectional-1");
              assertThat(result.getData()).isEqualTo("from-store1");
            });
  }
}
