package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.ReplicaAssignmentService.REPLICA_ASSIGN_PENDING;
import static com.slack.astra.clusterManager.ReplicaAssignmentService.REPLICA_ASSIGN_TIMER;
import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import brave.Tracing;
import com.slack.astra.metadata.cache.CacheSlotMetadata;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataTestUtils;
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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.curator.test.TestingServer;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReplicaAssignmentServiceTest {
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
            .setZkPathPrefix("ReplicaAssignmentServiceTest")
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
  public void shouldCheckInvalidEventAggregation() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .setSchedulePeriodMins(1)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(0)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaAssignmentService(
                    cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry));
  }

  @Test
  public void shouldCheckInvalidSchedulePeriod() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(-1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaAssignmentService(
                        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
                    .scheduler());
  }

  @Test
  public void shouldCheckInvalidMaxConcurrentAssignments() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(10)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(0)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(
            () ->
                new ReplicaAssignmentService(
                        cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry)
                    .scheduler());
  }

  @Test
  public void shouldHandleNoSlotsOrReplicas() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(10)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore, replicaMetadataStore, managerConfig, meterRegistry);

    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableSlots() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(0);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(3);

    assertThat(
            replicaMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore)))
        .isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(-3);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableReplicas() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);

    assertThat(
            cacheSlotMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore)))
        .isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(3);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleAllReplicasAlreadyAssigned() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicaMetadataList.get(i).name,
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

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

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 5);
    await().until(() -> replicaMetadataStore.listSync().size() == 3);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(5);

    assertThat(
            cacheSlotMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore)))
        .isTrue();
    assertThat(
            replicaMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore)))
        .isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(2);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleMixOfSlotStates() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    List<CacheSlotMetadata> unmutatedSlots = new ArrayList<>();
    CacheSlotMetadata cacheSlotWithAssignment =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
            replicaMetadataList.get(0).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    unmutatedSlots.add(cacheSlotWithAssignment);
    cacheSlotMetadataStore.createAsync(cacheSlotWithAssignment);

    CacheSlotMetadata cacheSlotLive =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
            replicaMetadataList.get(1).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    unmutatedSlots.add(cacheSlotLive);
    cacheSlotMetadataStore.createAsync(cacheSlotLive);

    CacheSlotMetadata cacheSlotEvicting =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
            replicaMetadataList.get(2).name,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    unmutatedSlots.add(cacheSlotEvicting);
    cacheSlotMetadataStore.createAsync(cacheSlotEvicting);

    CacheSlotMetadata cacheSlotFree =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotFree);

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 4);
    await().until(() -> replicaMetadataStore.listSync().size() == 4);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(1);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(4);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(4);
    assertThat(cacheSlotMetadataStore.listSync().containsAll(unmutatedSlots)).isTrue();

    List<CacheSlotMetadata> mutatedCacheSlots =
        AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
            .filter(cacheSlotMetadata -> !unmutatedSlots.contains(cacheSlotMetadata))
            .toList();
    assertThat(mutatedCacheSlots.size()).isEqualTo(1);
    assertThat(mutatedCacheSlots.get(0).name).isEqualTo(cacheSlotFree.name);
    assertThat(mutatedCacheSlots.get(0).replicaId).isEqualTo(replicaMetadataList.get(3).name);

    assertThat(
            replicaMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore)))
        .isTrue();

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(0);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleAllSlotsAlreadyAssigned() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    List<CacheSlotMetadata> cacheSlotMetadataList = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
              replicaMetadataList.get(i).name,
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataList.add(cacheSlotMetadata);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    await().until(() -> replicaMetadataStore.listSync().size() == 5);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(0);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(5);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);

    assertThat(
            cacheSlotMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore)))
        .isTrue();
    assertThat(
            replicaMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore)))
        .isTrue();
    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(-2);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleExpiredReplicas() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().minus(1500, ChronoUnit.MINUTES).toEpochMilli(),
              Instant.now().minusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataExpiredList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    // add an expired replica with a value of 0
    ReplicaMetadata replicaMetadataZero =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            Instant.now().minus(1500, ChronoUnit.MINUTES).toEpochMilli(),
            0,
            false);
    replicaMetadataExpiredList.add(replicaMetadataZero);
    replicaMetadataStore.createAsync(replicaMetadataZero);

    for (int i = 0; i < 3; i++) {
      ReplicaMetadata replicaMetadata =
          new ReplicaMetadata(
              UUID.randomUUID().toString(),
              UUID.randomUUID().toString(),
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    for (int i = 0; i < 4; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 4);
    await().until(() -> replicaMetadataStore.listSync().size() == 6);

    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    // this should be 2, even though 3 are eligible - due to MAX_ASSIGNMENTS_PER_CACHE_HOST
    assertThat(assignments.values().stream().mapToInt(i -> i).sum()).isEqualTo(2);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(6);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(4);

    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(2);
    assertThat(
            replicaMetadataExpiredList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).stream()
                    .filter(
                        replicaMetadata ->
                            replicaMetadata.createdTimeEpochMs
                                < Instant.now().minus(1440, ChronoUnit.MINUTES).toEpochMilli())
                    .toList()))
        .isTrue();

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(0);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(2);
    assertThat(
            MetricsUtil.getValue(
                ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedAssignmentOnFollowingRun() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod().doReturn(asyncStage).when(cacheSlotMetadataStore).updateAsync(any());

    Map<String, Integer> firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment.get(REPLICA_SET)).isEqualTo(1);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);

    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                        .count()
                    == 2);

    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 1);

    assertThat(MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry))
        .isEqualTo(1);
    assertThat(
            MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);

    doCallRealMethod().when(cacheSlotMetadataStore).updateAsync(any());

    Map<String, Integer> secondAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(secondAssignment.get(REPLICA_SET)).isEqualTo(1);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(2);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
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
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(2);
  }

  @Test
  public void shouldHandleTimedOutFutures() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    replicaAssignmentService.futuresListTimeoutSecs = 2;
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

    doCallRealMethod().doReturn(asyncStage).when(cacheSlotMetadataStore).updateAsync(any());

    Map<String, Integer> firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment.get(REPLICA_SET)).isEqualTo(1);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
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
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleExceptionalFutures() {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    for (int i = 0; i < 3; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    AsyncStage asyncStage = mock(AsyncStage.class);
    when(asyncStage.toCompletableFuture())
        .thenReturn(CompletableFuture.failedFuture(new Exception()));

    doCallRealMethod().doReturn(asyncStage).when(cacheSlotMetadataStore).updateAsync(any());

    Map<String, Integer> firstAssignment = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(firstAssignment.get(REPLICA_SET)).isEqualTo(1);

    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(2);
    assertThat(AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).size()).isEqualTo(3);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.FREE))
                .count())
        .isEqualTo(2);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
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
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);
  }

  @Test
  public void shouldHandleSlotsAvailableFirstLifecycle() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 3);
    assertThat(AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore).size()).isEqualTo(0);
    assertThat(
            AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore).stream()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
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
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry)).isEqualTo(1);

    replicaAssignmentService.stopAsync();
    replicaAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleReplicasAvailableFirstLifecycle() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
              REPLICA_SET,
              Instant.now().toEpochMilli(),
              Instant.now().plusSeconds(60).toEpochMilli(),
              false);
      replicaMetadataList.add(replicaMetadata);
      replicaMetadataStore.createAsync(replicaMetadata);
    }

    await().until(() -> replicaMetadataStore.listSync().size() == 3);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 2; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 2);
    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                        .filter(
                            cacheSlotMetadata ->
                                cacheSlotMetadata.cacheSlotState.equals(
                                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                        .count()
                    == 2);
    assertThat(
            replicaMetadataList.containsAll(
                AstraMetadataTestUtils.listSyncUncached(replicaMetadataStore)))
        .isTrue();

    await()
        .until(
            () ->
                MetricsUtil.getCount(ReplicaAssignmentService.REPLICA_ASSIGN_FAILED, meterRegistry),
            (value) -> value == 0);
    await()
        .until(
            () ->
                MetricsUtil.getCount(
                    ReplicaAssignmentService.REPLICA_ASSIGN_SUCCEEDED, meterRegistry),
            (value) -> value == 2);
    await()
        .until(
            () ->
                MetricsUtil.getValue(
                    ReplicaAssignmentService.REPLICA_ASSIGN_AVAILABLE_CAPACITY, meterRegistry),
            (value) -> value == -1);
    await()
        .until(
            () -> MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, meterRegistry),
            (value) -> value == 1);

    replicaAssignmentService.stopAsync();
    replicaAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldPreferNewerReplicasIfLackingCapacity() throws Exception {
    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
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
            REPLICA_SET,
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(olderReplicaMetadata);

    ReplicaMetadata newerReplicaMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(newerReplicaMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);

    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    await()
        .until(
            () -> {
              List<CacheSlotMetadata> cacheSlotMetadataList = cacheSlotMetadataStore.listSync();
              return cacheSlotMetadataList.size() == 1
                  && cacheSlotMetadataList.get(0).cacheSlotState
                      == Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
            });

    List<CacheSlotMetadata> assignedCacheSlot =
        AstraMetadataTestUtils.listSyncUncached(cacheSlotMetadataStore);
    assertThat(assignedCacheSlot.get(0).replicaId).isEqualTo(newerReplicaMetadata.name);
  }

  @Test
  public void shouldNotAssignIfAlreadyLoading() throws Exception {
    MeterRegistry concurrentAssignmentsRegistry = new SimpleMeterRegistry();

    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(1)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            managerConfig,
            concurrentAssignmentsRegistry);

    Instant now = Instant.now();
    ReplicaMetadata expectedUnassignedMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(expectedUnassignedMetadata);

    ReplicaMetadata loadingMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(loadingMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 2);

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.LOADING,
            loadingMetadata.snapshotId,
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(cacheSlotMetadata);

    CacheSlotMetadata freeCacheSlot =
        new CacheSlotMetadata(
            UUID.randomUUID().toString(),
            Metadata.CacheSlotMetadata.CacheSlotState.FREE,
            "",
            Instant.now().toEpochMilli(),
            HOSTNAME,
            REPLICA_SET);
    cacheSlotMetadataStore.createAsync(freeCacheSlot);

    await().until(() -> cacheSlotMetadataStore.listSync().size() == 2);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // immediately force a run
    Map<String, Integer> assignments = replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(assignments.get(REPLICA_SET)).isEqualTo(0);
  }

  @Test
  public void shouldPreventConcurrentAssignmentsExceedingLimit() throws Exception {
    MeterRegistry concurrentAssignmentsRegistry = new SimpleMeterRegistry();

    AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig replicaAssignmentServiceConfig =
        AstraConfigs.ManagerConfig.ReplicaAssignmentServiceConfig.newBuilder()
            .setSchedulePeriodMins(1)
            .addAllReplicaSets(List.of(REPLICA_SET))
            .setMaxConcurrentPerNode(2)
            .build();
    AstraConfigs.ManagerConfig managerConfig =
        AstraConfigs.ManagerConfig.newBuilder()
            .setEventAggregationSecs(2)
            .setScheduleInitialDelayMins(1)
            .setReplicaAssignmentServiceConfig(replicaAssignmentServiceConfig)
            .build();

    ReplicaAssignmentService replicaAssignmentService =
        new ReplicaAssignmentService(
            cacheSlotMetadataStore,
            replicaMetadataStore,
            managerConfig,
            concurrentAssignmentsRegistry);

    Instant now = Instant.now();
    ReplicaMetadata expectedAssignedMetadata1 =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(expectedAssignedMetadata1);

    ReplicaMetadata expectedAssignedMetadata2 =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.minus(1, ChronoUnit.HOURS).toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(expectedAssignedMetadata2);

    ReplicaMetadata expectedUnassignedMetadata =
        new ReplicaMetadata(
            UUID.randomUUID().toString(),
            UUID.randomUUID().toString(),
            REPLICA_SET,
            now.toEpochMilli(),
            now.plusSeconds(60).toEpochMilli(),
            false);
    replicaMetadataStore.createAsync(expectedUnassignedMetadata);

    await().until(() -> replicaMetadataStore.listSync().size() == 3);

    // create a large amount of free slots
    for (int i = 0; i < 10; i++) {
      CacheSlotMetadata cacheSlotMetadata =
          new CacheSlotMetadata(
              UUID.randomUUID().toString(),
              Metadata.CacheSlotMetadata.CacheSlotState.FREE,
              "",
              Instant.now().toEpochMilli(),
              HOSTNAME,
              REPLICA_SET);
      cacheSlotMetadataStore.createAsync(cacheSlotMetadata);
    }
    await().until(() -> cacheSlotMetadataStore.listSync().size() == 10);

    replicaAssignmentService.startAsync();
    replicaAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    // immediately force a run
    replicaAssignmentService.assignReplicasToCacheSlots();

    await()
        .until(
            () ->
                cacheSlotMetadataStore.listSync().stream()
                    .filter(
                        cacheSlotMetadata ->
                            cacheSlotMetadata.cacheSlotState.equals(
                                Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                    .count(),
            (count) -> {
              System.out.println(count);

              return count == 2;
            });
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, concurrentAssignmentsRegistry))
        .isEqualTo(1);
    assertThat(MetricsUtil.getValue(REPLICA_ASSIGN_PENDING, concurrentAssignmentsRegistry))
        .isEqualTo(1);

    // force another run
    replicaAssignmentService.assignReplicasToCacheSlots();
    assertThat(MetricsUtil.getTimerCount(REPLICA_ASSIGN_TIMER, concurrentAssignmentsRegistry))
        .isEqualTo(2);

    // verify that we still only have two assigned, and one is pending
    assertThat(
            cacheSlotMetadataStore.listSync().stream()
                .filter(
                    cacheSlotMetadata ->
                        cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED))
                .count())
        .isEqualTo(2);
    assertThat(MetricsUtil.getValue(REPLICA_ASSIGN_PENDING, concurrentAssignmentsRegistry))
        .isEqualTo(1);
  }
}
