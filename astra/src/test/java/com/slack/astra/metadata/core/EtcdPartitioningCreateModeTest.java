package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.concurrent.ExecutionException;
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
 * Tests for the EtcdCreateMode functionality in EtcdPartitioningMetadataStore.
 *
 * <p>Tests both persistent and ephemeral modes in a partitioned context.
 */
@Tag("integration")
public class EtcdPartitioningCreateModeTest {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdPartitioningCreateModeTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
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
  }

  @AfterEach
  public void tearDown() {
    meterRegistry.close();
  }

  @Test
  public void testPersistentNode() throws ExecutionException, InterruptedException, IOException {
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

    String partition = "partition1";

    // Create a store with persistent nodes (default)
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> persistentStore =
        new EtcdPartitioningMetadataStore<>(
            etcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-persistent")) {

      // Create a node
      TestPartitionedMetadata testData =
          new TestPartitionedMetadata("persistent1", partition, "This node should persist");
      persistentStore.createSync(testData);

      // Verify it exists
      TestPartitionedMetadata result = persistentStore.getSync(partition, "persistent1");
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo("persistent1");
      assertThat(result.getPartition()).isEqualTo(partition);
      assertThat(result.getData()).isEqualTo("This node should persist");
    }

    // Create a new store instance and verify the node still exists
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> newStore =
        new EtcdPartitioningMetadataStore<>(
            newEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-persistent")) {

      TestPartitionedMetadata result = newStore.getSync(partition, "persistent1");
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo("persistent1");
      assertThat(result.getPartition()).isEqualTo(partition);
      assertThat(result.getData()).isEqualTo("This node should persist");
    }
  }

  @Test
  public void testEphemeralNodeWithShortTtl() throws Exception, IOException {
    // Use a short TTL for faster testing
    long shortTtlSeconds = 1;
    String partition = "partition1";

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

    // Create ephemeral node that should disappear after the store is closed
    String nodeName = "ephemeral1";

    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> ephemeralStore =
        new EtcdPartitioningMetadataStore<>(
            etcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.EPHEMERAL,
            serializer,
            "/test-ephemeral")) {

      // Create a node
      TestPartitionedMetadata testData =
          new TestPartitionedMetadata(nodeName, partition, "This node should disappear");
      ephemeralStore.createSync(testData);

      // Verify it exists
      TestPartitionedMetadata result = ephemeralStore.getSync(partition, nodeName);
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo(nodeName);
      assertThat(result.getPartition()).isEqualTo(partition);
      assertThat(result.getData()).isEqualTo("This node should disappear");
    }

    // Wait for the node to be cleared in etcd
    TimeUnit.SECONDS.sleep(shortTtlSeconds + 2);

    // Create a new store instance and verify the node no longer exists
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> newStore =
        new EtcdPartitioningMetadataStore<>(
            newEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-ephemeral")) {

      boolean existsAfterTTL = newStore.hasSync(partition, nodeName);
      assertThat(existsAfterTTL).isFalse();
    }
  }

  @Test
  public void testMultiplePartitionsWithEphemeralNodes() throws Exception {
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

    // Create ephemeral nodes in multiple partitions
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> ephemeralStore =
        new EtcdPartitioningMetadataStore<>(
            etcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.EPHEMERAL,
            serializer,
            "/test-multi-partition")) {

      // Create nodes in different partitions
      TestPartitionedMetadata node1 =
          new TestPartitionedMetadata("ephemeral1", "partition1", "Node in partition1");
      TestPartitionedMetadata node2 =
          new TestPartitionedMetadata("ephemeral2", "partition2", "Node in partition2");
      TestPartitionedMetadata node3 =
          new TestPartitionedMetadata("ephemeral3", "partition3", "Node in partition3");

      ephemeralStore.createSync(node1);
      ephemeralStore.createSync(node2);
      ephemeralStore.createSync(node3);

      // Verify all nodes exist
      assertThat(ephemeralStore.getSync("partition1", "ephemeral1")).isNotNull();
      assertThat(ephemeralStore.getSync("partition2", "ephemeral2")).isNotNull();
      assertThat(ephemeralStore.getSync("partition3", "ephemeral3")).isNotNull();

      // Close the store, which will stop the lease refresh
    }

    // Create a new store instance and verify the nodes no longer exist
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> newStore =
        new EtcdPartitioningMetadataStore<>(
            newEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-multi-partition")) {

      // The ephemeral nodes should still exist because of lease refresh
      // In a real system, ephemeral nodes persist until the store is closed or the node is deleted
      boolean exists1 = newStore.hasSync("partition1", "ephemeral1");
      boolean exists2 = newStore.hasSync("partition2", "ephemeral2");
      boolean exists3 = newStore.hasSync("partition3", "ephemeral3");

      assertThat(exists1).isFalse();
      assertThat(exists2).isFalse();
      assertThat(exists3).isFalse();
    }
  }

  @Test
  public void testMixedPersistentAndEphemeralNodes() throws Exception {
    // Use a short TTL for faster testing
    long shortTtlSeconds = 1;
    String partition = "partition1";

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

    // First create a persistent node
    // Build etcd client
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

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> persistentStore =
        new EtcdPartitioningMetadataStore<>(
            etcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-mixed")) {

      TestPartitionedMetadata persistentNode =
          new TestPartitionedMetadata("persistent1", partition, "This node should persist");
      persistentStore.createSync(persistentNode);
    }

    // Now create an ephemeral node in the same partition
    // Build etcd client
    Client ephemeralEtcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .connectTimeout(Duration.ofMillis(5000))
            .keepaliveTimeout(Duration.ofMillis(3000))
            .retryMaxAttempts(3)
            .retryDelay(100)
            .namespace(ByteSequence.from("test", StandardCharsets.UTF_8))
            .build();

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> ephemeralStore =
        new EtcdPartitioningMetadataStore<>(
            ephemeralEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.EPHEMERAL,
            serializer,
            "/test-mixed")) {

      TestPartitionedMetadata ephemeralNode =
          new TestPartitionedMetadata("ephemeral1", partition, "This node should disappear");
      ephemeralStore.createSync(ephemeralNode);

      // Verify both nodes exist
      assertThat(ephemeralStore.getSync(partition, "persistent1")).isNotNull();
      assertThat(ephemeralStore.getSync(partition, "ephemeral1")).isNotNull();
    }

    // Wait for the TTL to expire
    TimeUnit.SECONDS.sleep(shortTtlSeconds + 1);

    // Check that only the persistent node remains
    // Build etcd client
    Client finalEtcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .connectTimeout(Duration.ofMillis(5000))
            .keepaliveTimeout(Duration.ofMillis(3000))
            .retryMaxAttempts(3)
            .retryDelay(100)
            .namespace(ByteSequence.from("test", StandardCharsets.UTF_8))
            .build();

    try (EtcdPartitioningMetadataStore<TestPartitionedMetadata> newStore =
        new EtcdPartitioningMetadataStore<>(
            finalEtcdClient,
            etcdConfig,
            meterRegistry,
            EtcdCreateMode.PERSISTENT,
            serializer,
            "/test-mixed")) {

      // Persistent node should still exist
      TestPartitionedMetadata persistentResult = newStore.getSync(partition, "persistent1");
      assertThat(persistentResult).isNotNull();
      assertThat(persistentResult.getData()).isEqualTo("This node should persist");

      boolean ephemeralExists = newStore.hasSync(partition, "ephemeral1");
      assertThat(ephemeralExists).isFalse();
    }
  }
}
