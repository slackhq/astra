package com.slack.kaldb.recovery;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.protobuf.TextFormat;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.TestKafkaServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class RecoveryServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

  private TestingServer testZkServer;
  private MeterRegistry meterRegistry;
  private BlobFs blobFs;
  private TestKafkaServer kafkaServer;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    kafkaServer = new TestKafkaServer();
    meterRegistry = new SimpleMeterRegistry();
    testZkServer = new TestingServer();
    blobFs = new S3BlobFs(S3_MOCK_RULE.createS3ClientV2());
    // TODO: Fix it.
    // KaldbConfigUtil.makeKaldbConfig(testZkServer.getConnectString(), )
  }

  @After
  public void shutdown() throws Exception {
    kafkaServer.close();
    testZkServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleRecoveryNodeLifecycle() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testZkServer.getConnectString())
            .setZkPathPrefix("shouldHandleRecoveryNodeLifecycle")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    KaldbConfigs.ServerConfig serverConfig =
        KaldbConfigs.ServerConfig.newBuilder()
            .setServerPort(1234)
            .setServerAddress("localhost")
            .build();

    SearchContext searchContext = SearchContext.fromConfig(serverConfig);
    MetadataStore metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);

    RecoveryService recoveryService =
        new RecoveryService(
            KaldbConfigs.KaldbConfig.newBuilder().build(), metadataStore, meterRegistry, blobFs);
    recoveryService.startAsync();
    recoveryService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStore, false);

    RecoveryNodeMetadata recoveryNodeMetadata =
        recoveryNodeMetadataStore.getNodeSync(searchContext.hostname);
    assertThat(recoveryNodeMetadata.recoveryNodeState)
        .isEqualTo(Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    RecoveryNodeMetadata recoveryNodeAssignment =
        new RecoveryNodeMetadata(
            recoveryNodeMetadata.name,
            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
            "recoveryTaskName",
            Instant.now().toEpochMilli());
    recoveryNodeMetadataStore.updateSync(recoveryNodeAssignment);

    // todo - verify it actually indexes here before it returns back to free.
    //  Recovery task should not exist at this point if it is complete

    await()
        .until(
            () ->
                recoveryNodeMetadataStore.getNodeSync(searchContext.hostname).recoveryNodeState
                    == Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE);

    recoveryService.stopAsync();
    recoveryService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    recoveryNodeMetadataStore.close();

    metadataStore.close();
  }

  @Test
  public void testShouldHandleRecoveryTask() {

  }
}
