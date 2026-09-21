package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
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
import java.util.concurrent.TimeUnit;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CacheNodeAssignmentStoreTest {
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;
  private MeterRegistry meterRegistry;
  private static EtcdCluster etcdCluster;
  private Client etcdClient;
  private AstraConfigs.MetadataStoreConfig metadataStoreConfig;

  private static final String NODE_A = "cache-node-a";
  private static final String NODE_B = "cache-node-b";

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
    etcdCluster = TestEtcdClusterFactory.start();

    etcdClient =
        Client.builder()
            .endpoints(
                etcdCluster.clientEndpoints().stream().map(Object::toString).toArray(String[]::new))
            .namespace(ByteSequence.from("Test", StandardCharsets.UTF_8))
            .build();

    AstraConfigs.EtcdConfig etcdConfig =
        AstraConfigs.EtcdConfig.newBuilder()
            .addAllEndpoints(etcdCluster.clientEndpoints().stream().map(Object::toString).toList())
            .setConnectionTimeoutMs(5000)
            .setKeepaliveTimeoutMs(3000)
            .setOperationsMaxRetries(3)
            .setOperationsTimeoutMs(3000)
            .setRetryDelayMs(100)
            .setNamespace("Test")
            .setEnabled(true)
            .setEphemeralNodeTtlMs(3000)
            .setEphemeralNodeMaxRetries(3)
            .build();

    metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .putStoreModes("CacheNodeAssignmentStore", AstraConfigs.MetadataStoreMode.ETCD_CREATES)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("Test")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(500)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .setEtcdConfig(etcdConfig)
            .build();
    this.curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
  }

  @AfterEach
  public void tearDown() throws IOException {
    curatorFramework.unwrap().close();
    if (etcdClient != null) etcdClient.close();
    testingServer.close();
    meterRegistry.close();
  }

  private CacheNodeAssignment assignmentFor(String cacheNodeId) {
    return new CacheNodeAssignment(
        "assignment-" + cacheNodeId,
        cacheNodeId,
        "snapshot-" + cacheNodeId,
        "replica-" + cacheNodeId,
        "rep1",
        1L,
        Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING);
  }

  /**
   * Regression test for the etcd O(n²) watch bug: the per-cache-node "restricted" constructor
   * filters the ZooKeeper store to the node's own partition but, prior to the fix, built the etcd
   * store with no filter, so in ETCD_CREATES mode every cache node materialized (and watched) every
   * other node's partition. This asserts the restricted store only ever sees its own cacheNodeId.
   */
  @Test
  public void restrictedStoreOnlySeesOwnPartition() throws Exception {
    // Writer store spanning all partitions, used only to seed assignments for two cache nodes.
    try (CacheNodeAssignmentStore writer =
        new CacheNodeAssignmentStore(
            curatorFramework, etcdClient, metadataStoreConfig, meterRegistry)) {
      writer.createSync(assignmentFor(NODE_A));
      writer.createSync(assignmentFor(NODE_B));
      await().atMost(Duration.ofSeconds(5)).until(() -> writer.listSync().size() == 2);

      // Restricted store for NODE_A must never materialize NODE_B's partition.
      try (CacheNodeAssignmentStore restricted =
          new CacheNodeAssignmentStore(
              curatorFramework, etcdClient, metadataStoreConfig, meterRegistry, NODE_A)) {
        restricted.awaitCacheInitialized();
        await().atMost(Duration.ofSeconds(5)).until(() -> restricted.listSync().size() == 1);

        List<CacheNodeAssignment> visible = restricted.listSync();
        assertThat(visible).extracting(a -> a.cacheNodeId).containsExactly(NODE_A);

        // A new assignment for NODE_B should never surface in the restricted store's cache.
        writer.createSync(assignmentFor("cache-node-c"));
        await().atMost(Duration.ofSeconds(3)).until(() -> writer.listSync().size() == 3);
        // Give any (erroneous) cross-partition watch time to fire before asserting it did not.
        TimeUnit.SECONDS.sleep(1);
        assertThat(restricted.listSync()).extracting(a -> a.cacheNodeId).containsExactly(NODE_A);
      }
    }
  }
}
