package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaCreationServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
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

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);
    replicaMetadataStore = new ReplicaMetadataStore(metadataStore, true);
  }

  @After
  public void shutdown() throws IOException, TimeoutException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldDoNothingIfReplicasAlreadyExist() throws Exception {
    ReplicaMetadataStore replicaMetadataStore = new ReplicaMetadataStore(metadataStore, true);
    SnapshotMetadataStore snapshotMetadataStore = new SnapshotMetadataStore(metadataStore, true);

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
        ReplicaCreationService.replicaMetadataFromSnapshotId(snapshotA.snapshotId));
    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(snapshotB.snapshotId));
    replicaMetadataStore.createSync(
        ReplicaCreationService.replicaMetadataFromSnapshotId(snapshotB.snapshotId));

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    assertThat(MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry))
        .isEqualTo(0);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldCreateManyReplicas() throws Exception {
    int snapshotsToCreate = 100;
    IntStream.range(0, snapshotsToCreate)
        .parallel()
        .forEach(
            (i) -> {
              String snapshotId = UUID.randomUUID().toString();
              SnapshotMetadata snapshot =
                  new SnapshotMetadata(
                      snapshotId,
                      snapshotId,
                      Instant.now().toEpochMilli() - 1,
                      Instant.now().toEpochMilli(),
                      0,
                      snapshotId);
              snapshotMetadataStore.createSync(snapshot);
            });

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setReplicasPerSnapshot(4)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .atMost(30, TimeUnit.SECONDS)
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry)
                    == replicaCreationServiceConfig.getReplicasPerSnapshot() * snapshotsToCreate);

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
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

    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setReplicasPerSnapshot(2)
            .setScheduleInitialDelayMins(0)
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    ReplicaCreationService replicaCreationService =
        new ReplicaCreationService(
            replicaMetadataStore,
            snapshotMetadataStore,
            replicaCreationServiceConfig,
            replicaEvictionServiceConfig,
            meterRegistry);
    replicaCreationService.startAsync();
    replicaCreationService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .until(
            () ->
                MetricsUtil.getCount(ReplicaCreationService.REPLICAS_CREATED, meterRegistry)
                    == replicaCreationServiceConfig.getReplicasPerSnapshot() * 2);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(4);
    await().until(() -> replicaMetadataStore.getCached().size() == 4);
    assertThat(
            (int)
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "a"))
                    .count())
        .isEqualTo(replicaCreationServiceConfig.getReplicasPerSnapshot());
    assertThat(
            (int)
                replicaMetadataStore
                    .getCached()
                    .stream()
                    .filter(replicaMetadata -> Objects.equals(replicaMetadata.snapshotId, "b"))
                    .count())
        .isEqualTo(replicaCreationServiceConfig.getReplicasPerSnapshot());

    replicaCreationService.stopAsync();
    replicaCreationService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
