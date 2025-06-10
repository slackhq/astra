package com.slack.astra.clusterManager;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.CuratorBuilder;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.testlib.MetricsUtil;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplicaDeletionServiceTest {
  public static final String HOSTNAME = "hostname";
  private static final String REPLICA_SET = "rep1";
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private CacheSlotMetadataStore cacheSlotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;
  private CacheNodeAssignmentStore cacheNodeMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.MetadataStoreConfig metadataStoreConfig =
        AstraConfigs.MetadataStoreConfig.newBuilder()
            .setMode(AstraConfigs.MetadataStoreMode.ZOOKEEPER_EXCLUSIVE)
            .setZookeeperConfig(
                AstraConfigs.ZookeeperConfig.newBuilder()
                    .setZkConnectString(testingServer.getConnectString())
                    .setZkPathPrefix("ReplicaDeletionServiceTest")
                    .setZkSessionTimeoutMs(1000)
                    .setZkConnectionTimeoutMs(1000)
                    .setSleepBetweenRetriesMs(1000)
                    .setZkCacheInitTimeoutMs(1000)
                    .build())
            .build();

    curatorFramework =
        CuratorBuilder.build(meterRegistry, metadataStoreConfig.getZookeeperConfig());
    cacheSlotMetadataStore =
        spy(new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    cacheNodeMetadataStore =
        spy(new CacheNodeAssignmentStore(curatorFramework, metadataStoreConfig, meterRegistry));
  }

  @AfterEach
  public void shutdown() throws IOException {
    cacheSlotMetadataStore.close();
    replicaMetadataStore.close();
    curatorFramework.unwrap().close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldThrowOnInvalidSchedulePeriodMins() {
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(-1)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaDeletionService(
                        cacheSlotMetadataStore,
                        replicaMetadataStore,
                        cacheNodeMetadataStore,
                        managerConfig,
                        meterRegistry)
                    .scheduler());
  }

  @Test
  public void shouldHandleNoReplicasOrAssignments() {
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeleted).isEqualTo(1);

    await().until(() -> replicaMetadataStore.listSync().size() == 0);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactlyInAnyOrder(cacheSlotMetadata);

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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              REPLICA_SET,
              Instant.now().minusSeconds(30).toEpochMilli(),
              Instant.now().minusSeconds(10).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    CacheSlotMetadata cacheSlotMetadataAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(0).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadataAssigned);

    CacheSlotMetadata cacheSlotMetadataEvict =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadataList.get(1).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadataEvict);

    CacheSlotMetadata cacheSlotMetadataEvicting =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
            replicaMetadataList.get(2).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadataEvicting);

    await().until(() -> replicaMetadataStore.listSync().size() == 3);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(replicaMetadataList);
    assertThat(cacheSlotMetadataStore.listSync())
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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().plusSeconds(30).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrder(replicaMetadata);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactlyInAnyOrder(cacheSlotMetadata);

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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doReturn(asyncStage).when(replicaMetadataStore).deleteAsync(any(ReplicaMetadata.class));

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeleted).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrder(replicaMetadata);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactlyInAnyOrder(cacheSlotMetadata);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(replicaMetadataStore).deleteAsync(any(ReplicaMetadata.class));

    int replicasDeletedRetry =
        replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeletedRetry).isEqualTo(1);

    await().until(() -> replicaMetadataStore.listSync().size() == 0);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactlyInAnyOrder(cacheSlotMetadata);

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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              REPLICA_SET,
              Instant.now().minusSeconds(30).toEpochMilli(),
              Instant.now().minusSeconds(10).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 2);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);
    replicaDeletionService.futuresListTimeoutSecs = 2;

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(
            CompletableFuture.runAsync(
                () -> {
                  try {
                    Thread.sleep(30 * 1000);
                  } catch (InterruptedException ignored) {
                  }
                },
                timeoutServiceExecutor));

    doCallRealMethod()
        .doReturn(asyncStage)
        .when(replicaMetadataStore)
        .deleteAsync(any(ReplicaMetadata.class));

    int replicasDeleted = replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeleted).isEqualTo(1);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    assertThat(cacheSlotMetadataStore.listSync())
        .containsExactlyInAnyOrderElementsOf(cacheSlotMetadataList);

    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_SUCCESS, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getCount(ReplicaDeletionService.REPLICA_DELETE_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaDeletionService.REPLICA_DELETE_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(replicaMetadataStore).deleteAsync(any(ReplicaMetadata.class));

    int replicasDeletedRetry =
        replicaDeletionService.deleteExpiredUnassignedReplicas(Instant.now());
    assertThat(replicasDeletedRetry).isEqualTo(1);

    await().until(() -> replicaMetadataStore.listSync().size() == 0);
    assertThat(cacheSlotMetadataStore.listSync())
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
    AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig replicaDeletionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaDeletionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaDeletionServiceConfig(replicaDeletionServiceConfig)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadataUnassigned =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataUnassigned);

    ReplicaMetadata replicaMetadataAssigned =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minusSeconds(30).toEpochMilli(),
            Instant.now().minusSeconds(10).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataAssigned);

    CacheSlotMetadata cacheSlotMetadataUnassigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadataUnassigned);

    CacheSlotMetadata cacheSlotMetadataAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataAssigned.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadataAssigned);

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 2);

    ReplicaDeletionService replicaDeletionService =
        new ReplicaDeletionService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            cacheNodeMetadataStore,
            managerConfig,
            meterRegistry);

    replicaDeletionService.startAsync();
    replicaDeletionService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrder(replicaMetadataAssigned);
    assertThat(cacheSlotMetadataStore.listSync())
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
