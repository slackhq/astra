package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static com.slack.kaldb.logstore.BlobFsUtils.createURI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import brave.Tracing;
import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.google.common.util.concurrent.Futures;
import com.slack.kaldb.blobfs.s3.S3BlobFs;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class SnapshotDeletionServiceTest {

  @ClassRule public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();
  private static final String S3_TEST_BUCKET = "snapshot-deletion-service-bucket";

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
  private SnapshotMetadataStore snapshotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private S3Client s3Client;
  private S3BlobFs s3BlobFs;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("SnapshotDeletionServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    snapshotMetadataStore = spy(new SnapshotMetadataStore(metadataStore, true));
    replicaMetadataStore = spy(new ReplicaMetadataStore(metadataStore, true));

    s3Client = S3_MOCK_RULE.createS3ClientV2();
    s3Client.createBucket(CreateBucketRequest.builder().bucket(S3_TEST_BUCKET).build());

    s3BlobFs = spy(new S3BlobFs());
    s3BlobFs.init(s3Client);
  }

  @After
  public void shutdown() throws IOException {
    snapshotMetadataStore.close();
    replicaMetadataStore.close();
    metadataStore.close();
    s3Client.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnInvalidSnapshotLifespan() {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    new SnapshotDeletionService(
        replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
  }

  @Test
  public void shouldDeleteExpiredSnapshotNoReplicas() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(1);

    await().until(() -> snapshotMetadataStore.getCached().size() == 0);
    verify(s3BlobFs, times(1)).delete(eq(path), eq(true));

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            snapshotMetadata.name,
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(500, ChronoUnit.MINUTES).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    await().until(() -> snapshotMetadataStore.getCached().size() == 1);
    await().until(() -> replicaMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached()).containsExactlyInAnyOrder(snapshotMetadata);
    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrder(replicaMetadata);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.getCached().size()).isEqualTo(0);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(500, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);

    // replica is also expired
    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            snapshotMetadata.name,
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    await().until(() -> snapshotMetadataStore.getCached().size() == 1);
    await().until(() -> replicaMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached()).containsExactlyInAnyOrder(snapshotMetadata);
    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrder(replicaMetadata);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);

    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    doThrow(new IOException()).when(s3BlobFs).delete(any(), anyBoolean());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    await().until(() -> snapshotMetadataStore.getCached().size() == 0);
    verify(s3BlobFs, times(1)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    doReturn(Futures.immediateFailedFuture(new InternalMetadataStoreException("failed")))
        .when(snapshotMetadataStore)
        .delete(any(SnapshotMetadata.class));

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    doReturn(false).when(s3BlobFs).delete(any(), anyBoolean());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    await().until(() -> snapshotMetadataStore.getCached().size() == 0);
    verify(s3BlobFs, times(1)).delete(any(), anyBoolean());

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
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    snapshotDeletionService.futuresListTimeoutSecs = 2;

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(snapshotMetadataStore)
        .delete(any(SnapshotMetadata.class));

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    assertThat(snapshotMetadataStore.getCached()).containsExactlyInAnyOrder(snapshotMetadata);
    verify(s3BlobFs, times(0)).delete(any(), anyBoolean());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(snapshotMetadataStore).delete(any(SnapshotMetadata.class));

    int deletesRetry = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletesRetry).isEqualTo(1);

    assertThat(snapshotMetadataStore.getCached().size()).isEqualTo(0);
    verify(s3BlobFs, times(1)).delete(any(), anyBoolean());

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
  public void shouldNotRetryFailedObjectStorageDeleteNextRun() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    // snapshot is expired
    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);

    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    doThrow(new IOException()).when(s3BlobFs).delete(any(), anyBoolean());

    int deletes = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deletes).isEqualTo(0);

    await().until(() -> snapshotMetadataStore.getCached().size() == 0);
    verify(s3BlobFs, times(1)).delete(any(), anyBoolean());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(s3BlobFs).delete(any(), anyBoolean());
    int deleteRetry = snapshotDeletionService.deleteExpiredSnapshotsWithoutReplicas();
    assertThat(deleteRetry).isEqualTo(0);

    // should be the same count as before
    verify(s3BlobFs, times(1)).delete(any(), anyBoolean());

    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(SnapshotDeletionService.SNAPSHOT_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(SnapshotDeletionService.SNAPSHOT_DELETE_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleSnapshotDeleteLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();

    KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig snapshotDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.SnapshotDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .setSnapshotLifespanMins(10080)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaCreationServiceConfig(replicaCreationServiceConfig)
            .setSnapshotDeletionServiceConfig(snapshotDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    Path file = Files.createTempFile("", "");
    URI path = createURI(S3_TEST_BUCKET, "foo", "bar");
    s3BlobFs.copyFromLocalFile(file.toFile(), path);

    SnapshotMetadata snapshotMetadata =
        new SnapshotMetadata(
            UUID.randomUUID().toString(),
            path.toString(),
            Instant.now().minus(11000, ChronoUnit.MINUTES).toEpochMilli(),
            Instant.now().minus(10900, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            "1");
    snapshotMetadataStore.create(snapshotMetadata);
    await().until(() -> snapshotMetadataStore.getCached().size() == 1);

    SnapshotDeletionService snapshotDeletionService =
        new SnapshotDeletionService(
            replicaMetadataStore, snapshotMetadataStore, s3BlobFs, managerConfig, meterRegistry);
    snapshotDeletionService.startAsync();
    snapshotDeletionService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> snapshotMetadataStore.getCached().size() == 0);
    verify(s3BlobFs, times(1)).delete(eq(path), eq(true));

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
