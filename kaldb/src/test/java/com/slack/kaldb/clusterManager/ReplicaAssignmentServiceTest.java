package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.proto.metadata.Metadata.IndexType.LOGS_LUCENE9;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReplicaAssignmentServiceTest {

  private static final List<Metadata.IndexType> SUPPORTED_INDEX_TYPES = List.of(LOGS_LUCENE9);

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
            .setZkPathPrefix("ReplicaAssignmentServiceTest")
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
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(0)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    new ReplicaAssignmentService(
        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldCheckInvalidSchedulePeriod() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(-1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
        .scheduler();
  }

  @Test
  public void shouldHandleNoSlotsOrReplicas() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(0);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableSlots() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 3);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(0);

    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(3);

    assertThat(replicaMetadataList.containsAll(replicaMetadataStore.listSync())).isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(-3);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableReplicas() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);

    assertThat(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync())).isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(3);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleAllReplicasAlreadyAssigned() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
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
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 5);
    await().until(() -> replicaMetadataStore.getCached().size() == 3);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(5);

    assertThat(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync())).isTrue();
    assertThat(replicaMetadataList.containsAll(replicaMetadataStore.listSync())).isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleMixOfSlotStates() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    List<CacheSlotMetadata> unmutatedSlots = new ArrayList<>();
    CacheSlotMetadata cacheSlotWithAssignment =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(0).name,
            Instant.now().toEpochMilli(),
            SUPPORTED_INDEX_TYPES);
    unmutatedSlots.add(cacheSlotWithAssignment);
    cacheSlotMetadataStore.create(cacheSlotWithAssignment);

    CacheSlotMetadata cacheSlotLive =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadataList.get(1).name,
            Instant.now().toEpochMilli(),
            SUPPORTED_INDEX_TYPES);
    unmutatedSlots.add(cacheSlotLive);
    cacheSlotMetadataStore.create(cacheSlotLive);

    CacheSlotMetadata cacheSlotEvicting =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(2).name,
            Instant.now().toEpochMilli(),
            SUPPORTED_INDEX_TYPES);
    unmutatedSlots.add(cacheSlotEvicting);
    cacheSlotMetadataStore.create(cacheSlotEvicting);

    CacheSlotMetadata cacheSlotFree =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            SUPPORTED_INDEX_TYPES);
    cacheSlotMetadataStore.create(cacheSlotFree);

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 4);
    await().until(() -> replicaMetadataStore.getCached().size() == 4);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(4);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(4);
    assertThat(cacheSlotMetadataStore.getCached().containsAll(unmutatedSlots)).isTrue();

    List<CacheSlotMetadata> mutatedCacheSlots =
        cacheSlotMetadataStore.getCached().stream()
            .filter(cacheSlotMetadata -> !unmutatedSlots.contains(cacheSlotMetadata))
            .collect(Collectors.toList());
    assertThat(mutatedCacheSlots.size()).isEqualTo(1);
    assertThat(mutatedCacheSlots.get(0).name).isEqualTo(cacheSlotFree.name);
    assertThat(mutatedCacheSlots.get(0).replicaId).isEqualTo(replicaMetadataList.get(3).name);

    assertThat(replicaMetadataList.containsAll(replicaMetadataStore.listSync())).isTrue();

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleAllSlotsAlreadyAssigned() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
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
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 5);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(0);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(5);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);

    assertThat(cacheSlotMetadataList.containsAll(cacheSlotMetadataStore.listSync())).isTrue();
    assertThat(replicaMetadataList.containsAll(replicaMetadataStore.listSync())).isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(-2);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleExpiredReplicas() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataExpiredList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().minus(1500, ChronoUnit.MINUTES).toEpochMilli(),
              Instant.now().minusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataExpiredList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    // add an expired replica with a value of 0
    ReplicaMetadata replicaMetadataZero =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            Instant.now().minus(1500, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            false,
            LOGS_LUCENE9);
    replicaMetadataExpiredList.add(replicaMetadataZero);
    replicaMetadataStore.create(replicaMetadataZero);

    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 4; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 4);
    await().until(() -> replicaMetadataStore.getCached().size() == 6);

    int assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments).isEqualTo(3);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(6);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(4);

    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(1);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(3);
    assertThat(
            replicaMetadataExpiredList.containsAll(
                replicaMetadataStore.listSync().stream()
                    .filter(
                        replicaMetadata ->
                            replicaMetadata.createdTimeEpochMs
                                < Instant.now().minus(1440, ChronoUnit.MINUTES).toEpochMilli())
                    .collect(Collectors.toList())))
        .isTrue();

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(3);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedAssignmentOnFollowingRun() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(cacheSlotMetadataStore)
        .update(any());

    int firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    doCallRealMethod().when(cacheSlotMetadataStore).update(any());

    int secondAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(secondAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleTimedOutFutures() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    replicaAssignmentService.futuresListTimeoutSecs = 2;
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

    int firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleExceptionalFutures() {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.create(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(cacheSlotMetadataStore)
        .update(any());

    int firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment).isEqualTo(1);

    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(2);
    assertThat(cacheSlotMetadataStore.listSync().size()).isEqualTo(3);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleSlotsAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 3);
    assertThat(replicaMetadataStore.listSync().size()).isEqualTo(0);
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(3);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 2; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.getCached().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.getCached().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                        .count()
                    == 1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    replicaAssignmentService.stopAsync();
    replicaAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleReplicasAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    List<ReplicaMetadata> replicaMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false,
              LOGS_LUCENE9);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.create(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.getCached().size() == 3);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              SUPPORTED_INDEX_TYPES);
      cacheSlotMetadataStore.create(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.getCached().size() == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.getCached().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 2);
    assertThat(replicaMetadataList.containsAll(replicaMetadataStore.listSync())).isTrue();

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(-1);
    assertThat(
            MetricsUtil.getTimerCount(ReplicaAssignmentService.REPLICA_ASSIGN_TIMER, meterRegistry))
        .isEqualTo(1);

    replicaAssignmentService.stopAsync();
    replicaAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldPreferNewerReplicasIfLackingCapacity() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    Instant now = Instant.now();
    ReplicaMetadata olderReplicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false,
            LOGS_LUCENE9);
    replicaMetadataStore.create(olderReplicaMetadata);

    ReplicaMetadata newerReplicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            now.toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false,
            LOGS_LUCENE9);
    replicaMetadataStore.create(newerReplicaMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            SUPPORTED_INDEX_TYPES);

    cacheSlotMetadataStore.create(cacheSlotMetadata);
    await()
        .until(
            () -> {
              List<CacheSlotMetadata> cacheSlotMetadataList = cacheSlotMetadataStore.getCached();
              return cacheSlotMetadataList.size() == 1
                  && cacheSlotMetadataList.get(0).cacheSlotState
                      == Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
            });

    List<CacheSlotMetadata> assignedCacheSlot = cacheSlotMetadataStore.listSync();
    assertThat(assignedCacheSlot.get(0).replicaId).isEqualTo(newerReplicaMetadata.name);
    assertThat(assignedCacheSlot.get(0).supportedIndexTypes).containsAll(SUPPORTED_INDEX_TYPES);
  }

  @Test
  public void assignmentPreservesSupportedIndexTypes() throws Exception {
    KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        KaldbConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .build();
    KaldbConfigs.ManagerConfig managerConfig =
        KaldbConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    Instant now = Instant.now();
    ReplicaMetadata olderReplicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false,
            LOGS_LUCENE9);
    replicaMetadataStore.create(olderReplicaMetadata);

    ReplicaMetadata newerReplicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            now.toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false,
            LOGS_LUCENE9);
    replicaMetadataStore.create(newerReplicaMetadata);

    await().until(() -> replicaMetadataStore.getCached().size() == 2);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    final List<Metadata.IndexType> suppportedIndexTypes = List.of(LOGS_LUCENE9, LOGS_LUCENE9);
    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            suppportedIndexTypes);

    cacheSlotMetadataStore.create(cacheSlotMetadata);
    await()
        .until(
            () -> {
              List<CacheSlotMetadata> cacheSlotMetadataList = cacheSlotMetadataStore.getCached();
              return cacheSlotMetadataList.size() == 1
                  && cacheSlotMetadataList.get(0).cacheSlotState
                      == Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
            });

    List<CacheSlotMetadata> assignedCacheSlot = cacheSlotMetadataStore.listSync();
    assertThat(assignedCacheSlot.get(0).replicaId).isEqualTo(newerReplicaMetadata.name);
    assertThat(assignedCacheSlot.get(0).supportedIndexTypes)
        .containsExactlyInAnyOrderElementsOf(suppportedIndexTypes);
    assertThat(assignedCacheSlot.size()).isEqualTo(1);
  }
}
