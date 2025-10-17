package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.TestEtcdClusterFactory;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
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
 * Tests for the EtcdCreateMode functionality in EtcdMetadataStore.
 *
 * <p>Tests both persistent and ephemeral modes.
 */
@Tag("integration")
public class EtcdCreateModeTest {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdCreateModeTest.class);
  private static EtcdCluster etcdCluster;

  private MeterRegistry meterRegistry;
  private MetadataSerializer<TestMetadata> serializer;

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
  }

  @AfterEach
  public void tearDown() {
    meterRegistry.close();
  }

  /** Helper method to create an etcd client for tests. */
  private Client createEtcdClient(AstraConfigs.EtcdConfig config) {
    ClientBuilder clientBuilder =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream()
                    .map(Object::toString)
                    .toArray(String[]::new));

    // Set namespace if provided
    if (!config.getNamespace().isEmpty()) {
      clientBuilder.namespace(
          io.etcd.jetcd.ByteSequence.from(config.getNamespace(), StandardCharsets.UTF_8));
    }

    return clientBuilder.build();
  }

  @Test
  public void testPersistentNode() throws ExecutionException, InterruptedException {
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

    // Create a client for the store
    Client etcdClient = createEtcdClient(etcdConfig);

    // Create a store with persistent nodes (default)
    try (EtcdMetadataStore<TestMetadata> persistentStore =
        new EtcdMetadataStore<>(
            "/test-persistent", etcdConfig, true, meterRegistry, serializer, etcdClient)) {

      // Create a node
      TestMetadata testData = new TestMetadata("persistent1", "This node should persist");
      persistentStore.createSync(testData);

      // Verify it exists
      TestMetadata result = persistentStore.getSync("persistent1");
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo("persistent1");
      assertThat(result.getData()).isEqualTo("This node should persist");
    }
    etcdClient.close();

    // Create a new store instance and verify the node still exists
    Client newClient = createEtcdClient(etcdConfig);
    try (EtcdMetadataStore<TestMetadata> newStore =
        new EtcdMetadataStore<>(
            "/test-persistent", etcdConfig, true, meterRegistry, serializer, newClient)) {

      TestMetadata result = newStore.getSync("persistent1");
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo("persistent1");
      assertThat(result.getData()).isEqualTo("This node should persist");
    }
    newClient.close();
  }

  @Test
  public void testEphemeralNodeWithShortTtl() throws Exception {
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

    // Create a client for ephemeral store
    Client ephemeralClient = createEtcdClient(etcdConfig);

    try (EtcdMetadataStore<TestMetadata> ephemeralStore =
        new EtcdMetadataStore<>(
            "/test-ephemeral",
            etcdConfig,
            true,
            meterRegistry,
            serializer,
            EtcdCreateMode.EPHEMERAL,
            ephemeralClient)) {

      // Create a node
      TestMetadata testData = new TestMetadata(nodeName, "This node should disappear");
      ephemeralStore.createSync(testData);

      // Verify it exists
      TestMetadata result = ephemeralStore.getSync(nodeName);
      assertThat(result).isNotNull();
      assertThat(result.getName()).isEqualTo(nodeName);
      assertThat(result.getData()).isEqualTo("This node should disappear");

      // Close the store, which will stop the lease refresh
    }
    ephemeralClient.close();

    // Wait for the TTL to expire (plus a little extra time)
    TimeUnit.SECONDS.sleep(4);

    // Create a new store instance and verify the node no longer exists
    Client verifyClient = createEtcdClient(etcdConfig);
    try (EtcdMetadataStore<TestMetadata> newStore =
        new EtcdMetadataStore<>(
            "/test-ephemeral", etcdConfig, true, meterRegistry, serializer, verifyClient)) {

      // After TTL expiration, the node should no longer exist
      assertThat(newStore.hasSync(nodeName)).isFalse();
    }
    verifyClient.close();
  }
}
