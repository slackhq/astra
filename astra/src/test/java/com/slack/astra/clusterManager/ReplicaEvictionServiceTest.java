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
import java.util.Objects;
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

public class ReplicaEvictionServiceTest {
  public static final String HOSTNAME = "hostname";
  public static final String REPLICA_SET = "rep1";
  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private AsyncCuratorFramework curatorFramework;
  private CacheSlotMetadataStore cacheSlotMetadataStore;
  private ReplicaMetadataStore replicaMetadataStore;

  @BeforeEach
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    AstraConfigs.ZookeeperConfig zkConfig =
        AstraConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("ReplicaEvictionServiceTest")
            .setZkSessionTimeoutMs(1000)
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
    cacheSlotMetadataStore =
        spy(new CacheSlotMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
    replicaMetadataStore =
        spy(new ReplicaMetadataStore(curatorFramework, metadataStoreConfig, meterRegistry));
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
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(0)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(0)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaEvictionService(
                        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
                    .scheduler());
  }

  @Test
  public void shouldHandleNoReplicasOrAssignedSlots() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasMarked = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoExpiredCacheSlots() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicas = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicas.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlots = new ArrayList<>();
    CacheSlotMetadata cacheSlotAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicas.get(0).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlots.add(cacheSlotAssigned);
    cacheSlotMetadataStore.createAsync(cacheSlotAssigned);

    CacheSlotMetadata cacheSlotLive =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicas.get(1).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlots.add(cacheSlotLive);
    cacheSlotMetadataStore.createAsync(cacheSlotLive);

    CacheSlotMetadata cacheSlotLoading =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LOADING,
            replicas.get(2).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlots.add(cacheSlotLoading);
    cacheSlotMetadataStore.createAsync(cacheSlotLoading);

    await().until(() -> replicaMetadataStore.listSync().size() == 5);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);

    int replicasMarked = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrderElementsOf(replicas);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactlyInAnyOrderElementsOf(cacheSlots);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldEvictReplicaWithEmptyExpiration() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            0,
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadata.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarked).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync()).containsExactly(replicaMetadata);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                    .allMatch(
                        cacheSlot ->
                            cacheSlot.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.EVICT)));

    CacheSlotMetadata updatedCacheSlot = cacheSlotMetadataStore.listSync().get(0);
    assertThat(updatedCacheSlot.updatedTimeEpochMs)
        .isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(updatedCacheSlot.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(updatedCacheSlot.name).isEqualTo(cacheSlotMetadata.name);
    assertThat(updatedCacheSlot.replicaId).isEqualTo(cacheSlotMetadata.replicaId);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldNotMutateReplicaAlreadyMarkedForEviction() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadata.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactly(cacheSlotMetadata);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldNotMutateReplicaAlreadyEvicting() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
            replicaMetadata.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactly(cacheSlotMetadata);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedEvictionOnNextRun() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    ReplicaMetadata replicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadata.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 1);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 1);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doReturn(asyncStage).when(cacheSlotMetadataStore).updateAsync(any());

    int replicasMarkedFirstAttempt = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarkedFirstAttempt).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.listSync()).containsExactly(cacheSlotMetadata);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(cacheSlotMetadataStore).updateAsync(any());

    int replicasMarkedSecondAttempt = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarkedSecondAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync()).containsExactly(replicaMetadata);

    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                    .allMatch(
                        cacheSlot ->
                            cacheSlot.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.EVICT)));

    CacheSlotMetadata updatedCacheSlot = cacheSlotMetadataStore.listSync().get(0);
    assertThat(updatedCacheSlot.updatedTimeEpochMs)
        .isGreaterThan(cacheSlotMetadata.updatedTimeEpochMs);
    assertThat(updatedCacheSlot.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(updatedCacheSlot.name).isEqualTo(cacheSlotMetadata.name);
    assertThat(updatedCacheSlot.replicaId).isEqualTo(cacheSlotMetadata.replicaId);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleMixOfZkSuccessFailures() {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
    replicaEvictionService.futuresListTimeoutSecs = 2;

    List<ReplicaMetadata> replicas = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().minusSeconds(60).toEpochMilli(),
              false);
      replicas.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
              replicas.get(0).name,
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 2);

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

    // allow the first replica creation to work, and timeout the second one
    doCallRealMethod().doReturn(asyncStage).when(cacheSlotMetadataStore).updateAsync(any());

    int replicasMarkedFirstAttempt = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarkedFirstAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrderElementsOf(replicas);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.LIVE))
                        .count()
                    == 1);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 1);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(2);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(cacheSlotMetadataStore).updateAsync(any());

    int replicasMarkedSecondAttempt = replicaEvictionService.markReplicasForEviction(Instant.now());
    assertThat(replicasMarkedSecondAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync()).containsExactlyInAnyOrderElementsOf(replicas);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(2);

    assertThat(
            MetricsUtil.getCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getCount(ReplicaEvictionService.REPLICA_MARK_EVICT_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(
                ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry))
        .isEqualTo(2);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleMixOfExpiredAndUnexpiredLifecycle() throws TimeoutException {
    AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadataExpiredOne =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataExpiredOne);
    CacheSlotMetadata cacheSlotReplicaExpiredOne =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadataExpiredOne.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotReplicaExpiredOne);

    ReplicaMetadata replicaMetadataExpiredTwo =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataExpiredTwo);
    CacheSlotMetadata cacheSlotReplicaExpireTwo =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadataExpiredTwo.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotReplicaExpireTwo);

    ReplicaMetadata replicaMetadataUnexpiredOne =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(360).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataUnexpiredOne);
    CacheSlotMetadata cacheSlotReplicaUnexpiredOne =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadataUnexpiredOne.name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotReplicaUnexpiredOne);

    ReplicaMetadata replicaMetadataUnexpiredTwo =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(360).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(replicaMetadataUnexpiredTwo);
    CacheSlotMetadata cacheSlotFree =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotFree);

    await().until(() -> replicaMetadataStore.listSync().size() == 4);
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 4);

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
    replicaEvictionService.startAsync();
    replicaEvictionService.awaitRunning(DEFAULT_START_STOP_DURATION);

    await()
        .until(
            () ->
                MetricsUtil.getTimerCount(
                        ReplicaEvictionService.REPLICA_MARK_EVICT_TIMER, meterRegistry)
                    == 1);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 2);

    assertThat(replicaMetadataStore.listSync())
        .containsExactlyInAnyOrder(
            replicaMetadataExpiredOne,
            replicaMetadataExpiredTwo,
            replicaMetadataUnexpiredOne,
            replicaMetadataUnexpiredTwo);
    assertThat(cacheSlotMetadataStore.listSync())
        .contains(cacheSlotReplicaExpireTwo, cacheSlotReplicaUnexpiredOne, cacheSlotFree);
    assertThat(cacheSlotMetadataStore.listSync()).doesNotContain(cacheSlotReplicaExpiredOne);

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    CacheSlotMetadata updatedCacheSlot =
        cacheSlotMetadataStore.listSync().stream()
            .filter(
                cacheSlotMetadata ->
                    Objects.equals(cacheSlotMetadata.name, cacheSlotReplicaExpiredOne.name))
            .findFirst()
            .get();
    assertThat(updatedCacheSlot.replicaId).isEqualTo(cacheSlotReplicaExpiredOne.replicaId);
    assertThat(updatedCacheSlot.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(updatedCacheSlot.updatedTimeEpochMs)
        .isGreaterThan(cacheSlotReplicaExpiredOne.updatedTimeEpochMs);

    replicaEvictionService.stopAsync();
    replicaEvictionService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
