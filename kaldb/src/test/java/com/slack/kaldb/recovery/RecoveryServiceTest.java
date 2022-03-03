package com.slack.kaldb.recovery;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.chunk.SearchContext;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
  }

  @After
  public void shutdown() throws IOException {
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleRecoveryNodeLifecycle() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
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

    KaldbConfigs.RecoveryConfig recoveryConfig =
        KaldbConfigs.RecoveryConfig.newBuilder().setServerConfig(serverConfig).build();

    SearchContext searchContext = SearchContext.fromConfig(serverConfig);
    MetadataStore metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);

    RecoveryService recoveryService =
        new RecoveryService(recoveryConfig, metadataStore, meterRegistry);
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
}
