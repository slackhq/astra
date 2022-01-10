package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import brave.Tracing;
import com.google.common.util.concurrent.Futures;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaDeletionServiceTest {
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
  private CacheSlotMetadataStore cacheSlotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaDeletionServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    cacheSlotMetadataStore = spy(new CacheSlotMetadataStore(metadataStore, true));
    replicaMetadataStore = spy(new ReplicaMetadataStore(metadataStore, true));
  }

  @After
  public void shutdown() throws IOException {
    cacheSlotMetadataStore.close();
    replicaMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnInvalidSchedulePeriodMins() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(-1)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
        .scheduler();
  }

  @Test
  public void shouldHandleNoReplicasOrAssignments() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldDeleteExpiredReplicaWithoutAssignment() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(1);

    await().until(() -> replicaMetadataStore.getCached().size() == 0);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactlyInAnyOrder(cacheSlotMetadata);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldNotDeleteExpiredReplicasWithAssignments() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().minusSeconds(30).toEpochMilli(),
              Instant.now().minusSeconds(10).toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    CacheSlotMetadata cacheSlotMetadataAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(0).name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadataAssigned);

    CacheSlotMetadata cacheSlotMetadataEvict =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadataList.get(1).name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadataEvict);

    CacheSlotMetadata cacheSlotMetadataEvicting =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
            replicaMetadataList.get(2).name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadataEvicting);

    await().until(() -> replicaMetadataStore.getCached().size() == 3);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached())
        .containsExactlyInAnyOrderElementsOf(replicaMetadataList);
    assertThat(cacheSlotMetadataStore.getCached())
        .containsExactlyInAnyOrder(
            cacheSlotMetadataAssigned, cacheSlotMetadataEvict, cacheSlotMetadataEvicting);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldNotDeleteUnexpiredReplicaWithoutAssignment() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().plusSeconds(30).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrder(replicaMetadata);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactlyInAnyOrder(cacheSlotMetadata);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedDeleteAttempt() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(replicaMetadataStore)
        .delete(any(ReplicaMetadata.class));

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrder(replicaMetadata);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactlyInAnyOrder(cacheSlotMetadata);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(replicaMetadataStore).delete(any(ReplicaMetadata.class));

    int replicasDeletedRetry = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeletedRetry).isEqualTo(1);

    await().until(() -> replicaMetadataStore.getCached().size() == 0);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactlyInAnyOrder(cacheSlotMetadata);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleMixOfZkSuccessFailure() {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().minusSeconds(30).toEpochMilli(),
              Instant.now().minusSeconds(10).toEpochMilli());
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 2);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
    replicaDeletionService.futuresListTimeoutSecs = 2;

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    doCallRealMethod()
        .doReturn(
            Futures.submit(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor))
        .when(replicaMetadataStore)
        .delete(any(ReplicaMetadata.class));

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeleted).isEqualTo(1);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    assertThat(cacheSlotMetadataStore.getCached())
        .containsExactlyInAnyOrderElementsOf(cacheSlotMetadataList);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(replicaMetadataStore).delete(any(ReplicaMetadata.class));

    int replicasDeletedRetry = replicaDeletionService.deleteExpiredUnassignedReplicas();
    assertThat(replicasDeletedRetry).isEqualTo(1);

    await().until(() -> replicaMetadataStore.getCached().size() == 0);
    assertThat(cacheSlotMetadataStore.getCached())
        .containsExactlyInAnyOrderElementsOf(cacheSlotMetadataList);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(2);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleDeletionLifecycle() throws TimeoutException {
    KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadataUnassigned =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataUnassigned);

    ReplicaMetadata replicaMetadataAssigned =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataAssigned);

    CacheSlotMetadata cacheSlotMetadataUnassigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadataUnassigned);

    CacheSlotMetadata cacheSlotMetadataAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataAssigned.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadataAssigned);

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 2);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    replicaDeletionService.startAsync();
    replicaDeletionService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrder(replicaMetadataAssigned);
    assertThat(cacheSlotMetadataStore.getCached())
        .containsExactlyInAnyOrder(cacheSlotMetadataUnassigned, cacheSlotMetadataAssigned);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    replicaDeletionService.stopAsync();
    replicaDeletionService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
