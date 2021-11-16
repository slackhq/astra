package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.server.MetadataStoreService;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaCreatorServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStoreService metadataStoreService;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaCreatorServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    snapshotMetadataStore =
        new SnapshotMetadataStore(metadataStoreService.getMetadataStore(), true);
    replicaMetadataStore = new ReplicaMetadataStore(metadataStoreService.getMetadataStore(), true);
  }

  @After
  public void shutdown() throws IOException, TimeoutException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldDoNothingIfReplicasAlreadyExist() throws Exception {
    ReplicaMetadataStore replicaMetadataStore =
        new ReplicaMetadataStore(metadataStoreService.getMetadataStore(), true);
    SnapshotMetadataStore snapshotMetadataStore =
        new SnapshotMetadataStore(metadataStoreService.getMetadataStore(), true);

    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    SnapshotMetadata snapshotB =
        new SnapshotMetadata(
            "b", "b", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "b");

    snapshotMetadataStore.createSync(snapshotA);
    snapshotMetadataStore.createSync(snapshotB);

    // create one replica for A, two for B
    replicaMetadataStore.createSync(
        ReplicaCreatorService.replicaMetadataFromSnapshotId(snapshotA.snapshotId));
    replicaMetadataStore.createSync(
        ReplicaCreatorService.replicaMetadataFromSnapshotId(snapshotB.snapshotId));
    replicaMetadataStore.createSync(
        ReplicaCreatorService.replicaMetadataFromSnapshotId(snapshotB.snapshotId));

    KaldbConfigs.ManagerConfig.ReplicaServiceConfig replicaServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    ReplicaCreatorService replicaCreatorService =
        new ReplicaCreatorService(
            replicaMetadataStore, snapshotMetadataStore, replicaServiceConfig, meterRegistry);
    replicaCreatorService.startAsync();
    replicaCreatorService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreatorService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);
  }

  @Test
  public void shouldCreateReplicasIfNoneExist() throws Exception {
    SnapshotMetadata snapshotA =
        new SnapshotMetadata(
            "a", "a", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "a");
    SnapshotMetadata snapshotB =
        new SnapshotMetadata(
            "b", "b", Instant.now().toEpochMilli() - 1, Instant.now().toEpochMilli(), 0, "b");

    snapshotMetadataStore.createSync(snapshotA);
    snapshotMetadataStore.createSync(snapshotB);

    KaldbConfigs.ManagerConfig.ReplicaServiceConfig replicaServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    ReplicaCreatorService replicaCreatorService =
        new ReplicaCreatorService(
            replicaMetadataStore, snapshotMetadataStore, replicaServiceConfig, meterRegistry);
    replicaCreatorService.startAsync();
    replicaCreatorService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreatorService.REPLICAS_CREATED, meterRegistry)
                    == replicaServiceConfig.getReplicasPerSnapshot() * 2);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(4);
    await().until(() -> replicaMetadataStore.getCached().size() == 4);
    assertThat(
            (int)
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "a"))
                    .count())
        .isEqualTo(replicaServiceConfig.getReplicasPerSnapshot());
    assertThat(
            (int)
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "b"))
                    .count())
        .isEqualTo(replicaServiceConfig.getReplicasPerSnapshot());
  }
}
