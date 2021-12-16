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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaEvictionServiceTest {

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
            .setZkPathPrefix("ReplicaEvictionServiceTest")
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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(0)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(0)
            .build();

    new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
        .scheduler();
  }

  @Test
  public void shouldHandleNoReplicasOrAssignedSlots() {
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(10)
            .build();

    ReplicaEvictionService replicaEvictionService =
        new ReplicaEvictionService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli());
      replicas.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlots = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicas.get(i).name,
              Instant.now().toEpochMilli());
      cacheSlots.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 5);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrderElementsOf(replicas);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactlyInAnyOrderElementsOf(cacheSlots);

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
  public void shouldEvictExpiredReplica() {
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadata.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarked).isEqualTo(1);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        cacheSlot ->
                            cacheSlot.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.EVICT)));

    CacheSlotMetadata updatedCacheSlot = cacheSlotMetadataStore.getCached().get(0);
    assertThat(updatedCacheSlot.updatedTimeUtc).isGreaterThan(cacheSlotMetadata.updatedTimeUtc);
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
  public void shouldEvictReplicaWithEmptyExpiration() {
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
            Instant.now().toEpochMilli(),
            0);
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadata.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarked).isEqualTo(1);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        cacheSlot ->
                            cacheSlot.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.EVICT)));

    CacheSlotMetadata updatedCacheSlot = cacheSlotMetadataStore.getCached().get(0);
    assertThat(updatedCacheSlot.updatedTimeUtc).isGreaterThan(cacheSlotMetadata.updatedTimeUtc);
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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadata.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactly(cacheSlotMetadata);

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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
            replicaMetadata.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    int replicasMarked = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarked).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactly(cacheSlotMetadata);

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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadata);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadata.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 1);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 1);

    doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(cacheSlotMetadataStore)
        .update(any());

    int replicasMarkedFirstAttempt = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarkedFirstAttempt).isEqualTo(0);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);
    assertThat(cacheSlotMetadataStore.getCached()).containsExactly(cacheSlotMetadata);

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

    doCallRealMethod().when(cacheSlotMetadataStore).update(any());

    int replicasMarkedSecondAttempt = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarkedSecondAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.getCached()).containsExactly(replicaMetadata);

    await()
        .until(
            () ->
                cacheSlotMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        cacheSlot ->
                            cacheSlot.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.EVICT)));

    CacheSlotMetadata updatedCacheSlot = cacheSlotMetadataStore.getCached().get(0);
    assertThat(updatedCacheSlot.updatedTimeUtc).isGreaterThan(cacheSlotMetadata.updatedTimeUtc);
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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
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
              Instant.now().toEpochMilli(),
              Instant.now().minusSeconds(60).toEpochMilli());
      replicas.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicas.get(0).name,
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 2);

    ExecutorService timeoutServiceExecutor = Executors.newSingleThreadExecutor();
    // allow the first replica creation to work, and timeout the second one
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
        .when(cacheSlotMetadataStore)
        .update(any());

    int replicasMarkedFirstAttempt = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarkedFirstAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrderElementsOf(replicas);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 1);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 1);
    assertThat(cacheSlotMetadataStore.getCached().size()).isEqualTo(2);

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

    doCallRealMethod().when(cacheSlotMetadataStore).update(any());

    int replicasMarkedSecondAttempt = replicaEvictionService.markReplicasForEviction();
    assertThat(replicasMarkedSecondAttempt).isEqualTo(1);

    assertThat(replicaMetadataStore.getCached()).containsExactlyInAnyOrderElementsOf(replicas);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 2);
    assertThat(cacheSlotMetadataStore.getCached().size()).isEqualTo(2);

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
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .build();

    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(0)
            .build();

    ReplicaMetadata replicaMetadataExpiredOne =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataExpiredOne);
    CacheSlotMetadata cacheSlotReplicaExpiredOne =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataExpiredOne.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotReplicaExpiredOne);

    ReplicaMetadata replicaMetadataExpiredTwo =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().toEpochMilli(),
            Instant.now().minusSeconds(60).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataExpiredTwo);
    CacheSlotMetadata cacheSlotReplicaExpireTwo =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaMetadataExpiredTwo.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotReplicaExpireTwo);

    ReplicaMetadata replicaMetadataUnexpiredOne =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(360).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataUnexpiredOne);
    CacheSlotMetadata cacheSlotReplicaUnexpiredOne =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataUnexpiredOne.name,
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotReplicaUnexpiredOne);

    ReplicaMetadata replicaMetadataUnexpiredTwo =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().toEpochMilli(),
            Instant.now().plusSeconds(360).toEpochMilli());
    replicaMetadataStore.create(replicaMetadataUnexpiredTwo);
    CacheSlotMetadata cacheSlotFree =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotFree);

    await().until(() -> replicaMetadataStore.getCached().size() == 4);
    await().until(() -> cacheSlotMetadataStore.getCached().size() == 4);

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
                cacheSlotMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT))
                        .count()
                    == 2);

    assertThat(replicaMetadataStore.getCached())
        .containsExactlyInAnyOrder(
            replicaMetadataExpiredOne,
            replicaMetadataExpiredTwo,
            replicaMetadataUnexpiredOne,
            replicaMetadataUnexpiredTwo);
    assertThat(cacheSlotMetadataStore.getCached())
        .contains(cacheSlotReplicaExpireTwo, cacheSlotReplicaUnexpiredOne, cacheSlotFree);
    assertThat(cacheSlotMetadataStore.getCached()).doesNotContain(cacheSlotReplicaExpiredOne);

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    CacheSlotMetadata updatedCacheSlot =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .filter(
                cacheSlotMetadata ->
                    Objects.equals(cacheSlotMetadata.name, cacheSlotReplicaExpiredOne.name))
            .findFirst()
            .get();
    assertThat(updatedCacheSlot.replicaId).isEqualTo(cacheSlotReplicaExpiredOne.replicaId);
    assertThat(updatedCacheSlot.cacheSlotState)
        .isEqualTo(Metadata.CacheSlotMetadata.CacheSlotState.EVICT);
    assertThat(updatedCacheSlot.updatedTimeUtc)
        .isGreaterThan(cacheSlotReplicaExpiredOne.updatedTimeUtc);

    replicaEvictionService.stopAsync();
    replicaEvictionService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
