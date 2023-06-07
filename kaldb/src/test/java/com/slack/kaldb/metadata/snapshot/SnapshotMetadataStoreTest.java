package com.slack.kaldb.metadata.snapshot;

import static org.awaitility.Awaitility.await;

import com.slack.kaldb.metadata.core.CuratorBuilder;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnapshotMetadataStoreTest {
  private SimpleMeterRegistry meterRegistry;
  private TestingServer testingServer;
  private AsyncCuratorFramework curatorFramework;

  @BeforeEach
  public void setUp() throws Exception {
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zookeeperConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("Test")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(500)
            .build();
    this.curatorFramework = CuratorBuilder.build(meterRegistry, zookeeperConfig);
  }

  @AfterEach
  public void tearDown() throws IOException {
    curatorFramework.unwrap().close();
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  void createSync() throws Exception {
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(curatorFramework, true);

    snapshotMetadataStore.createSync(
        new SnapshotMetadata(
            "id",
            "path",
            Instant.now().toEpochMilli(),
            Instant.now().toEpochMilli(),
            100,
            "1",
            Metadata.IndexType.LOGS_LUCENE9));

    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
  }
}
