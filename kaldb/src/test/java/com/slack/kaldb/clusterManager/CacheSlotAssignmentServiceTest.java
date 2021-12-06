package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.curator.test.TestingServer;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheSlotAssignmentServiceTest {

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
            .setZkPathPrefix("CacheSlotAssignmentServiceTest")
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
  public void shouldCheckInvalidEventAggregation() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(0)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    new CacheSlotAssignmentService(
        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldCheckInvalidReplicaLifespanMins() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(0)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    new CacheSlotAssignmentService(
        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldCheckInvalidScheduleInitialDelay() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(0)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(-1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
        .scheduler();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldCheckInvalidSchedulePeriod() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(-1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(0)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
        .scheduler();
  }

  @Test
  public void shouldHandleNoSlotsOrReplicas() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(0);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableSlots() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 3);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(0);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(3);

    assertTrue(replicaMetadataList.containsAll(replicaMetadataStore.listSync()));
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(3);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableReplicas() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);

    assertTrue(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync()));
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleAllReplicasAlreadyAssigned() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicaMetadataList.get(i).name,
              Instant.now().toEpochMilli());
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

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

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 5);
    await().until(() -> replicaMetadataStore.getCached().size() == 3);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(5);

    assertTrue(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync()));
    assertTrue(replicaMetadataList.containsAll(replicaMetadataStore.listSync()));
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleMixOfSlotStates() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> unmutatedSlots = new ArrayList<>();
    CacheSlotMetadata cacheSlotAssigned =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(0).name,
            Instant.now().toEpochMilli());
    unmutatedSlots.add(cacheSlotAssigned);
    cacheSlotMetadataStore.create(cacheSlotAssigned);

    CacheSlotMetadata cacheSlotLive =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadataList.get(1).name,
            Instant.now().toEpochMilli());
    unmutatedSlots.add(cacheSlotLive);
    cacheSlotMetadataStore.create(cacheSlotLive);

    CacheSlotMetadata cacheSlotEvicting =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(2).name,
            Instant.now().toEpochMilli());
    unmutatedSlots.add(cacheSlotEvicting);
    cacheSlotMetadataStore.create(cacheSlotEvicting);

    CacheSlotMetadata cacheSlotFree =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli());
    cacheSlotMetadataStore.create(cacheSlotFree);

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 4);
    await().until(() -> replicaMetadataStore.getCached().size() == 4);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(4);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(4);
    assertTrue(cacheSlotMetadataStore.getCached().containsAll(unmutatedSlots));

    List<CacheSlotMetadata> mutatedCacheSlots =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            .filter(cacheSlotMetadata -> !unmutatedSlots.contains(cacheSlotMetadata))
            .collect(Collectors.toList());
    assertThat(mutatedCacheSlots.size()).isEqualTo(1);
    assertThat(mutatedCacheSlots.get(0).name).isEqualTo(cacheSlotFree.name);
    assertThat(mutatedCacheSlots.get(0).replicaId).isEqualTo(replicaMetadataList.get(3).name);

    assertTrue(replicaMetadataList.containsAll(replicaMetadataStore.listSync()));

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleAllSlotsAlreadyAssigned() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicaMetadataList.get(i).name,
              Instant.now().toEpochMilli());
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 5);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(5);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);

    assertTrue(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync()));
    assertTrue(replicaMetadataList.containsAll(replicaMetadataStore.listSync()));
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleExpiredReplicas() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataExpiredList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().minus(1500, ChronoUnit.MINUTES).toEpochMilli());
      replicaMetadataExpiredList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<ReplicaMetadata> replicaMetadataNonExpiredList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataNonExpiredList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 4);
    await().until(() -> replicaMetadataStore.getCached().size() == 6);

    int assignments = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(assignments).isEqualTo(3);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(6);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(4);

    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(1);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(3);
    assertTrue(
        replicaMetadataExpiredList.containsAll(
            replicaMetadataStore
                .listSync()
                .stream()
                .filter(
                    replicaMetadata ->
                        replicaMetadata.createdTimeUtc
                            < Instant.now().minus(1440, ChronoUnit.MINUTES).toEpochMilli())
                .collect(Collectors.toList())));

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(3);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedAssignmentOnFollowingRun() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(cacheSlotMetadataStore)
        .update(any());

    int firstAssignment = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(cacheSlotMetadataStore).update(any());

    int secondAssignment = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(secondAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(1);

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleTimedOutFutures() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    cacheSlotAssignmentService.futuresListTimeoutSecs = 2;
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
        .when(cacheSlotMetadataStore)
        .update(any());

    int firstAssignment = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleExceptionalFutures() {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(cacheSlotMetadataStore)
        .update(any());

    int firstAssignment = cacheSlotAssignmentService.assignCacheSlotsToReplicas();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleSlotsAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(
            cacheSlotMetadataStore
                .listSync()
                .stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(3);

    cacheSlotAssignmentService.startAsync();
    cacheSlotAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
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
                    == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                        .count()
                    == 1);

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    cacheSlotAssignmentService.stopAsync();
    cacheSlotAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleReplicasAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig cacheSlotAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.CacheSlotAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig.newBuilder()
            .setReplicaLifespanMins(1440)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setCacheSlotAssignmentServiceConfig(cacheSlotAssignmentServiceConfig)
            .setReplicaEvictionServiceConfig(replicaEvictionServiceConfig)
            .build();

    CacheSlotAssignmentService cacheSlotAssignmentService =
        new CacheSlotAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli());
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 3);

    cacheSlotAssignmentService.startAsync();
    cacheSlotAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli());
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 2);
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
                    == 2);
    assertTrue(replicaMetadataList.containsAll(replicaMetadataStore.listSync()));

    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(CacheSlotAssignmentService.SLOT_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getCount(
                CacheSlotAssignmentService.SLOT_ASSIGN_INSUFFICIENT_CAPACITY, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(CacheSlotAssignmentService.SLOT_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    cacheSlotAssignmentService.stopAsync();
    cacheSlotAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
