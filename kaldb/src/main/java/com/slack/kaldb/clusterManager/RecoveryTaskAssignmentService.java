package com.slack.kaldb.clusterManager;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryTaskAssignmentService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskAssignmentService.class);

  private final RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private final RecoveryNodeMetadataStore recoveryNodeMetadataStore;

  private final MeterRegistry meterRegistry;

  public static final String RECOVERY_TASKS_CREATED = "recovery_tasks_created";
  public static final String RECOVERY_TASKS_FAILED = "recovery_tasks_failed";
  public static final String RECOVERY_TASK_ASSIGNMENT_TIMER = "recovery_task_assignment_timer";

  protected final Counter recoveryTasksCreated;
  protected final Counter recoveryTasksFailed;
  private final Timer recoveryAssignmentTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  public RecoveryTaskAssignmentService(
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      RecoveryNodeMetadataStore recoveryNodeMetadataStore,
      MeterRegistry meterRegistry) {
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.recoveryNodeMetadataStore = recoveryNodeMetadataStore;
    this.meterRegistry = meterRegistry;

    recoveryTasksCreated = meterRegistry.counter(RECOVERY_TASKS_CREATED);
    recoveryTasksFailed = meterRegistry.counter(RECOVERY_TASKS_FAILED);
    recoveryAssignmentTimer = meterRegistry.timer(RECOVERY_TASK_ASSIGNMENT_TIMER);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(this::assignRecoveryTasksToNodes, 10, TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Replica task already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery task assignment service");

    recoveryTaskMetadataStore.addListener(this::runOneIteration);
    recoveryNodeMetadataStore.addListener(this::runOneIteration);

    // do an initial assignment for anything that happened while this service was offline
    assignRecoveryTasksToNodes();
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdown();
    LOG.info("Closed recovery task assignment service");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 15, TimeUnit.MINUTES);
  }

  protected int assignRecoveryTasksToNodes() {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);

    List<String> recoveryTasksAlreadyAssigned =
        recoveryNodeMetadataStore
            .getCached()
            .stream()
            .map((recoveryNodeMetadata -> recoveryNodeMetadata.recoveryTaskName))
            .filter((recoveryTaskName) -> !recoveryTaskName.isEmpty())
            .collect(Collectors.toList());

    List<RecoveryTaskMetadata> recoveryTasksThatNeedAssignment =
        recoveryTaskMetadataStore
            .getCached()
            .stream()
            .filter(recoveryTask -> !recoveryTasksAlreadyAssigned.contains(recoveryTask.name))
            .collect(Collectors.toList());

    Stack<RecoveryNodeMetadata> availableRecoveryNodes =
        recoveryNodeMetadataStore
            .getCached()
            .stream()
            .filter(
                (recoveryNodeMetadata ->
                    recoveryNodeMetadata.recoveryNodeState.equals(
                        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)))
            .collect(Collectors.toCollection(Stack::new));

    List<ListenableFuture<?>> recoveryTaskAssignments =
        recoveryTasksThatNeedAssignment
            .stream()
            .map(
                (recoveryTaskMetadata -> {
                  if (!availableRecoveryNodes.empty()) {
                    RecoveryNodeMetadata recoveryNodeAssigned =
                        new RecoveryNodeMetadata(
                            availableRecoveryNodes.pop().name,
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                            recoveryTaskMetadata.name,
                            Instant.now().toEpochMilli());
                    return recoveryNodeMetadataStore.update(recoveryNodeAssigned);
                  } else {
                    LOG.warn(
                        "No available recovery nodes to assign task, will try again later - {}",
                        recoveryTaskMetadata.name);
                    return Futures.immediateCancelledFuture();
                  }
                }))
            .collect(Collectors.toList());

    ListenableFuture<List<Object>> futureList = Futures.allAsList(recoveryTaskAssignments);
    int completeFutures = 0;
    int incompleteFutures = 0;

    try {
      completeFutures = futureList.get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS).size();
    } catch (Exception futureListException) {
      for (ListenableFuture<?> future : recoveryTaskAssignments) {
        if (future.isDone()) {
          try {
            future.get();
            completeFutures++;
          } catch (Exception futureException) {
            incompleteFutures++;
          }
        } else {
          future.cancel(true);
          incompleteFutures++;
        }
      }
    }

    recoveryTasksCreated.increment(completeFutures);
    recoveryTasksFailed.increment(incompleteFutures);

    long assignmentDuration = assignmentTimer.stop(recoveryAssignmentTimer);
    LOG.info(
        "Completed recovery task assignment - successfully assigned {} tasks, failed to assign {} tasks in {} ms",
        completeFutures,
        incompleteFutures,
        TimeUnit.MILLISECONDS.convert(assignmentDuration, TimeUnit.NANOSECONDS));

    return completeFutures;
  }
}
