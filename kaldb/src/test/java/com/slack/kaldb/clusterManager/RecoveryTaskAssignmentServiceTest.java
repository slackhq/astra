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
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
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
import org.apache.curator.test.TestingServer;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryTaskAssignmentServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  private MetadataStore metadataStore;
  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();

    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("RecoveryTaskAssignmentServiceTest")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry, zkConfig);
    recoveryTaskMetadataStore = spy(new RecoveryTaskMetadataStore(metadataStore, true));
    recoveryNodeMetadataStore = spy(new RecoveryNodeMetadataStore(metadataStore, true));
  }

  @After
  public void shutdown() throws IOException {
    recoveryNodeMetadataStore.close();
    recoveryTaskMetadataStore.close();
    metadataStore.close();

    testingServer.close();
    meterRegistry.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldCheckInvalidConfigs() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(-1)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    new RecoveryTaskAssignmentService(
        recoveryTaskMetadataStore,
        recoveryNodeMetadataStore,
        recoveryTaskAssignmentServiceConfig,
        meterRegistry);
  }

  @Test
  public void shouldHandleNoNodesOrTasks() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertTrue(recoveryTaskMetadataStore.listSync().isEmpty());
    assertTrue(recoveryNodeMetadataStore.listSync().isEmpty());

    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableNodes() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }

    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 3);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertThat(recoveryTaskMetadataStore.listSync().size()).isEqualTo(3);
    assertTrue(recoveryNodeMetadataStore.listSync().isEmpty());

    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(3);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNoAvailableTasks() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.create(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 3);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(0);
    assertTrue(recoveryTaskMetadataStore.listSync().isEmpty());
    assertThat(recoveryNodeMetadataStore.listSync().size()).isEqualTo(3);

    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(0);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);
  }

  @Test
  public void shouldRetryFailedAssignmentOnFollowingRun() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.create(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    for (int i = 0; i < 3; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }

    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 3);
    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 3);

    doCallRealMethod()
        .doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(recoveryNodeMetadataStore)
        .update(any());

    int firstAttemptAssignment = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();
    assertThat(firstAttemptAssignment).isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(2);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(1);

    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            recoveryNodeMetadata ->
                                recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                        .count()
                    == 2);

    doCallRealMethod().when(recoveryNodeMetadataStore).update(any());

    int secondAttemptAssignment = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();
    assertThat(secondAttemptAssignment).isEqualTo(1);

    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                        .getCached()
                        .stream()
                        .filter(
                            recoveryNodeMetadata ->
                                recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                        .count()
                    == 3);

    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(3);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(2);
  }

  @Test
  public void shouldHandleTimedOutFutures() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);
    recoveryTaskAssignmentService.futuresListTimeoutSecs = 2;

    List<RecoveryNodeMetadata> recoveryNodeMetadataList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      RecoveryNodeMetadata recoveryNodeMetadata =
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli());
      recoveryNodeMetadataStore.create(recoveryNodeMetadata);
      recoveryNodeMetadataList.add(recoveryNodeMetadata);
    }

    for (int i = 0; i < 2; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }

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
        .when(recoveryNodeMetadataStore)
        .update(any());

    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 2);
    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 2);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);

    assertThat(
            recoveryNodeMetadataStore
                .listSync()
                .stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                .count())
        .isEqualTo(1);
    assertThat(
            recoveryNodeMetadataStore
                .listSync()
                .stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
                .count())
        .isEqualTo(1);

    timeoutServiceExecutor.shutdown();
  }

  @Test
  public void shouldHandleExceptionalFutures() {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(5)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);

    List<RecoveryNodeMetadata> recoveryNodeMetadataList = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      RecoveryNodeMetadata recoveryNodeMetadata =
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli());
      recoveryNodeMetadataStore.create(recoveryNodeMetadata);
      recoveryNodeMetadataList.add(recoveryNodeMetadata);
    }

    for (int i = 0; i < 2; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }

    doCallRealMethod()
        .doReturn(Futures.immediateFailedFuture(new Exception()))
        .when(recoveryNodeMetadataStore)
        .update(any());

    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 2);
    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 2);

    int assignments = recoveryTaskAssignmentService.assignRecoveryTasksToNodes();

    assertThat(assignments).isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_CREATED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getCount(
                RecoveryTaskAssignmentService.RECOVERY_TASKS_FAILED, meterRegistry))
        .isEqualTo(1);
    Assertions.assertThat(
            MetricsUtil.getTimerCount(
                RecoveryTaskAssignmentService.RECOVERY_TASK_ASSIGNMENT_TIMER, meterRegistry))
        .isEqualTo(1);

    assertThat(
            recoveryNodeMetadataStore
                .listSync()
                .stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))
                .count())
        .isEqualTo(1);
    assertThat(
            recoveryNodeMetadataStore
                .listSync()
                .stream()
                .filter(
                    recoveryNodeMetadata ->
                        recoveryNodeMetadata.recoveryNodeState.equals(
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
                .count())
        .isEqualTo(1);
  }

  @Test
  public void shouldHandleNodesAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(2)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.create(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }

    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 3);
    // all nodes should be FREE, and have no assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)
                                && recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    for (int i = 0; i < 10; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }

    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 10);
    // all nodes should be ASSIGNED, and have an assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    // mark all as recovering
    recoveryNodeMetadataStore
        .getCached()
        .forEach(
            recoveryNodeMetadata -> {
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                      recoveryNodeMetadata.recoveryTaskName,
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.update(updatedRecoveryNode);
            });

    // all nodes should be recovering
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    Instant before = Instant.now();
    // next delete the task, and mark the node as free
    recoveryNodeMetadataStore
        .getCached()
        .forEach(
            recoveryNodeMetadata -> {
              // delete the task
              recoveryTaskMetadataStore.deleteSync(recoveryNodeMetadata.recoveryTaskName);
              // free this node
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                      "",
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.update(updatedRecoveryNode);
            });

    // wait until all nodes have been re-assigned to new tasks
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata ->
                            recoveryNodeMetadata.updatedTimeUtc > before.toEpochMilli()
                                && recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))));
    assertThat(recoveryTaskMetadataStore.getCached().size()).isEqualTo(7);

    recoveryTaskAssignmentService.stopAsync();
    recoveryTaskAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleTasksAvailableFirstLifecycle() throws Exception {
    KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
        recoveryTaskAssignmentServiceConfig =
            KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig.newBuilder()
                .setEventAggregationSecs(2)
                .setScheduleInitialDelayMins(1)
                .setSchedulePeriodMins(10)
                .build();

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(
            recoveryTaskMetadataStore,
            recoveryNodeMetadataStore,
            recoveryTaskAssignmentServiceConfig,
            meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    for (int i = 0; i < 10; i++) {
      recoveryTaskMetadataStore.create(
          new RecoveryTaskMetadata(UUID.randomUUID().toString(), "1", 0, 1));
    }
    await().until(() -> recoveryTaskMetadataStore.getCached().size() == 10);

    for (int i = 0; i < 3; i++) {
      recoveryNodeMetadataStore.create(
          new RecoveryNodeMetadata(
              UUID.randomUUID().toString(),
              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
              "",
              Instant.now().toEpochMilli()));
    }
    await().until(() -> recoveryNodeMetadataStore.getCached().size() == 3);

    // all nodes should immediately pickup tasks and have an assignment
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    // mark all as recovering
    recoveryNodeMetadataStore
        .getCached()
        .forEach(
            recoveryNodeMetadata -> {
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING,
                      recoveryNodeMetadata.recoveryTaskName,
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.update(updatedRecoveryNode);
            });

    // all nodes should be recovering
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata) ->
                            recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.RECOVERING)
                                && !recoveryNodeMetadata.recoveryTaskName.isEmpty()));

    Instant before = Instant.now();
    // next delete the task, and mark the node as free
    recoveryNodeMetadataStore
        .getCached()
        .forEach(
            recoveryNodeMetadata -> {
              // delete the task
              recoveryTaskMetadataStore.deleteSync(recoveryNodeMetadata.recoveryTaskName);
              // free this node
              RecoveryNodeMetadata updatedRecoveryNode =
                  new RecoveryNodeMetadata(
                      recoveryNodeMetadata.name,
                      Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE,
                      "",
                      Instant.now().toEpochMilli());
              recoveryNodeMetadataStore.update(updatedRecoveryNode);
            });

    // wait until all nodes have been re-assigned to new tasks
    await()
        .until(
            () ->
                recoveryNodeMetadataStore
                    .getCached()
                    .stream()
                    .allMatch(
                        (recoveryNodeMetadata ->
                            recoveryNodeMetadata.updatedTimeUtc > before.toEpochMilli()
                                && recoveryNodeMetadata.recoveryNodeState.equals(
                                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED))));
    assertThat(recoveryTaskMetadataStore.getCached().size()).isEqualTo(7);

    recoveryTaskAssignmentService.stopAsync();
    recoveryTaskAssignmentService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
