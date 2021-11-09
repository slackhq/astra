package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import brave.Tracing;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryTaskAssignmentServiceTest {

  private TestingServer testingServer;
  private MeterRegistry meterRegistry;

  @Before
  public void setup() throws Exception {
    Tracing.newBuilder().build();
    meterRegistry = new SimpleMeterRegistry();
    testingServer = new TestingServer();
  }

  @After
  public void shutdown() throws IOException {
    testingServer.close();
    meterRegistry.close();
  }

  @Test
  public void shouldHandleNodesAvailableFirst() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleNodesAvailableFirst")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(metadataStoreService, meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStoreService.getMetadataStore(), true);
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStoreService.getMetadataStore(), true);

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

    recoveryTaskMetadataStore.close();
    recoveryNodeMetadataStore.close();

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }

  @Test
  public void shouldHandleTasksAvailableFirst() throws Exception {
    KaldbConfigs.ZookeeperConfig zkConfig =
        KaldbConfigs.ZookeeperConfig.newBuilder()
            .setZkConnectString(testingServer.getConnectString())
            .setZkPathPrefix("shouldHandleTasksAvailableFirst")
            .setZkSessionTimeoutMs(1000)
            .setZkConnectionTimeoutMs(1000)
            .setSleepBetweenRetriesMs(1000)
            .build();

    MetadataStoreService metadataStoreService = new MetadataStoreService(meterRegistry, zkConfig);
    metadataStoreService.startAsync();
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryTaskAssignmentService recoveryTaskAssignmentService =
        new RecoveryTaskAssignmentService(metadataStoreService, meterRegistry);
    recoveryTaskAssignmentService.startAsync();
    recoveryTaskAssignmentService.awaitRunning(DEFAULT_START_STOP_DURATION);

    RecoveryNodeMetadataStore recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStoreService.getMetadataStore(), true);
    RecoveryTaskMetadataStore recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStoreService.getMetadataStore(), true);

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

    recoveryTaskMetadataStore.close();
    recoveryNodeMetadataStore.close();

    metadataStoreService.stopAsync();
    metadataStoreService.awaitTerminated(DEFAULT_START_STOP_DURATION);
  }
}
