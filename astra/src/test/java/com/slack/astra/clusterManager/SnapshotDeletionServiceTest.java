package com.slack.astra.clusterManager;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import brave.Tracing;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.blobfs.S3TestUtils;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadata;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class SnapshotDeletionServiceTest {

  private static final String S3_TEST_BUCKET = "snapshot-deletion-service-bucket";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .withInitialBuckets(S3_TEST_BUCKET)
          .silent()
          .withSecureConnection(false)
          .build();

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private S3AsyncClient s3AsyncClient;
  private BlobStore blobStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("SnapshotDeletionServiceTest")
            .setZkSessionTimeoutMs(2500)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .setZkCacheInitTimeoutMs(1000)
            .build();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(zkConfig)
            .build();

    curatorFramework = CuratorBuilder.build(meterRegistry, zkConfig);
    snapshotMetadataStore =
        spy(new SnapshotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));

    s3AsyncClient = S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());
    blobStore = spy(new BlobStore(s3AsyncClient, S3_TEST_BUCKET));
  }

  @AfterEach
  public void shutdown() throws IOException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();
    s3AsyncClient.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldThrowOnInvalidSnapshotLifespan() {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new SnapshotDeletionService(
                    replicaMetadataStore,
                    snapshotMetadataStore,
                    blobStore,
                    managerConfig,
                    meterRegistry));
  }

  @Test
  public void shouldDeleteExpiredSnapshotNoReplicas() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.listSync().isEmpty());
    verify(blobStore, times(1)).delete(eq(chunkId));

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldNotDeleteExpiredSnapshotWithReplicas() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            0);
    snapshotMetadataStore.createAsync(snapshotMetadata);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            snapshotMetadata.name,
            "rep1",
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(500, ChronoUnit.MINUTES).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
    await().until(() -> replicaMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    List<String> s3CrtBlobFsFiles = blobStore.listFiles(chunkId);
    assertThat(s3CrtBlobFsFiles.size()).isNotEqualTo(0);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(snapshotMetadata);
    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrder(replicaMetadata);
    verify(blobStore, times(0)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEqualTo(s3CrtBlobFsFiles);

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoReplicasNoSnapshots() throws IOException {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    verify(blobStore, times(0)).delete(any());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoReplicasUnexpiredSnapshots() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(500, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            0);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
    List<String> s3CrtBlobFsFiles = blobStore.listFiles(chunkId);
    assertThat(s3CrtBlobFsFiles.size()).isNotEqualTo(0);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(blobStore, times(0)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEqualTo(s3CrtBlobFsFiles);

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleReplicasWithLongerLifespanThanSnapshots() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            0);
    snapshotMetadataStore.createAsync(snapshotMetadata);

    // replica is also expired
    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            snapshotMetadata.name,
            "rep1",
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    await().until(() -> snapshotMetadataStore.listSync().size() == 1);
    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    List<String> s3CrtBlobFsFiles = blobStore.listFiles(chunkId);
    assertThat(s3CrtBlobFsFiles.size()).isNotEqualTo(0);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(snapshotMetadata);
    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrder(replicaMetadata);
    verify(blobStore, times(0)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEqualTo(s3CrtBlobFsFiles);

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleExceptionalObjectStorageDelete() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);

    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
    doThrow(new RuntimeException()).when(blobStore).delete(any());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(1);

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleFailedZkDelete() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);

    // Mock the deleteSync method to throw an exception
    doAnswer(
            invocation -> {
              throw new RuntimeException("Test exception from ZK delete");
            })
        .when(snapshotMetadataStore)
        .deleteSync(any());
    assertThat(blobStore.listFiles(chunkId)).isNotEmpty();

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(blobStore, times(1)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEmpty();

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleFailedObjectDelete() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
    doThrow(new RuntimeException()).when(blobStore).delete(any());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(1);

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryTimedOutZkDeleteNextRun() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
    snapshotDeletionService.futuresListTimeoutSecs = 2;
    List<String> s3CrtBlobFsFiles = blobStore.listFiles(chunkId);
    assertThat(s3CrtBlobFsFiles.size()).isNotEqualTo(0);

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();

    // Create a mock that simulates a timeout by sleeping longer than the timeout setting
    doAnswer(
            invocation -> {
              Thread.sleep(3000); // Sleep 3 seconds (longer than the futuresListTimeoutSecs = 2)
              return null; // This won't be reached due to the timeout
            })
        .when(snapshotMetadataStore)
        .deleteSync(any());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(blobStore, times(1)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEmpty();

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    // Reset the mock to succeed on the retry attempt
    doCallRealMethod().when(snapshotMetadataStore).deleteSync(any());

    int deletesRetry = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletesRetry).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.listSync().isEmpty());
    verify(blobStore, times(2)).delete(any());
    assertThat(blobStore.listFiles(chunkId)).isEmpty();

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(2);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldRetryFailedObjectStorageDeleteNextRun() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);

    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
    doThrow(new RuntimeException()).when(blobStore).delete(any());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.listSync().size()).isEqualTo(1);
    verify(blobStore, times(1)).delete(any());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(blobStore).delete(any());
    int deleteRetry = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deleteRetry).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.listSync().size() == 0);
    verify(blobStore, times(2)).delete(any());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleSnapshotDeleteLifecycle() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        AstraConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path directory = Files.createTempDirectory("");
    Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directory);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            chunkId,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1",
            100);
    snapshotMetadataStore.createAsync(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.listSync().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, blobStore, managerConfig, meterRegistry);
    snapshotDeletionService.startAsync();
    snapshotDeletionService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> snapshotMetadataStore.listSync().size() == 0);
    verify(blobStore, times(1)).delete(eq(chunkId));

    await()
        .until(
            () ->
                MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry)
                    == 1);
    await()
        .until(
            () ->
                MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry)
                    == 0);
    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                        SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry)
                    == 1);

    snapshotDeletionService.stopAsync();
    snapshotDeletionService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
