package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import com.slack.kaldb.server.MetadataStoreService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryTaskAssignmentService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskAssignmentService.class);

  private final MetadataStoreService metadataStoreService;

  private RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private RecoveryNodeMetadataStore recoveryNodeMetadataStore;

  public static final String RECOVERY_TASK_ASSIGNED = "recovery_task_assigned";
  public static final String RECOVERY_TASK_NO_TASKS = "recovery_task_no_tasks";
  public static final String RECOVERY_TASK_NO_AVAILABLE_NODES = "recovery_task_no_available_nodes";

  protected final Counter recoveryTaskAssigned;
  protected final Counter recoveryTaskNoTasks;
  protected final Counter recoveryTaskNoAvailableNodes;

  public RecoveryTaskAssignmentService(
      MetadataStoreService metadataStoreService, MeterRegistry meterRegistry) {
    this.metadataStoreService = metadataStoreService;

    recoveryTaskAssigned = meterRegistry.counter(RECOVERY_TASK_ASSIGNED);
    recoveryTaskNoTasks = meterRegistry.counter(RECOVERY_TASK_NO_TASKS);
    recoveryTaskNoAvailableNodes = meterRegistry.counter(RECOVERY_TASK_NO_AVAILABLE_NODES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery task assignment service");
    metadataStoreService.awaitRunning(DEFAULT_START_STOP_DURATION);

    recoveryTaskMetadataStore =
        new RecoveryTaskMetadataStore(metadataStoreService.getMetadataStore(), true);
    recoveryTaskMetadataStore.addListener(this::assignRecoveryTasksToNodes);

    recoveryNodeMetadataStore =
        new RecoveryNodeMetadataStore(metadataStoreService.getMetadataStore(), true);
    recoveryNodeMetadataStore.addListener(this::assignRecoveryTasksToNodes);

    // do an initial assignment for anything that happened while this service was offline
    assignRecoveryTasksToNodes();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing recovery task assignment service");

    recoveryTaskMetadataStore.close();
    recoveryNodeMetadataStore.close();

    LOG.info("Closed recovery task assignment service");
  }

  private void assignRecoveryTasksToNodes() {
    List<RecoveryTaskMetadata> recoveryTaskMetadataList = recoveryTaskMetadataStore.getCached();
    List<RecoveryNodeMetadata> recoveryNodeMetadataList = recoveryNodeMetadataStore.getCached();

    List<RecoveryTaskMetadata> recoveryTasksThatNeedAssignment =
        recoveryTaskMetadataList
            .stream()
            .filter(
                recoveryTaskMetadata -> {
                  // does this individual task already have an assignment?
                  return recoveryNodeMetadataList
                      .stream()
                      .noneMatch(
                          (recoveryNode) ->
                              Objects.equals(
                                  recoveryNode.recoveryTaskName, recoveryTaskMetadata.name));
                })
            .collect(Collectors.toList());

    if (recoveryTasksThatNeedAssignment.isEmpty()) {
      LOG.info("No tasks require assignment");
      recoveryTaskNoTasks.increment();
      return;
    }

    recoveryTasksThatNeedAssignment.forEach(
        (recoveryTaskMetadata -> {
          Optional<RecoveryNodeMetadata> recoveryNodeToAssignOpt =
              recoveryNodeMetadataList
                  .stream()
                  .filter(
                      (recoveryNode) ->
                          recoveryNode.recoveryNodeState.equals(
                              Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
                  .findFirst();

          if (recoveryNodeToAssignOpt.isEmpty()) {
            LOG.warn(
                "No available recovery nodes to assign tasks, will try again later - {}",
                recoveryTaskMetadata.name);
            recoveryTaskNoAvailableNodes.increment();
          } else {
            RecoveryNodeMetadata recoveryNodeToAssign = recoveryNodeToAssignOpt.get();
            RecoveryNodeMetadata recoveryNodeAssigned =
                new RecoveryNodeMetadata(
                    recoveryNodeToAssign.name,
                    Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                    recoveryTaskMetadata.name,
                    Instant.now().toEpochMilli());
            recoveryNodeMetadataStore.update(recoveryNodeAssigned);

            LOG.info(
                "Assigning {} to recovery node {}",
                recoveryTaskMetadata.name,
                recoveryNodeToAssign.name);
            recoveryTaskAssigned.increment();
          }
        }));
  }
}
