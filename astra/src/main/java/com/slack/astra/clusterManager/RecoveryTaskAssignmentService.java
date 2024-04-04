package com.slack.astra.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.astra.server.AstraConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.astra.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.recovery.RecoveryNodeMetadata;
import com.slack.astra.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadata;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import com.slack.astra.util.FutureUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The recovery task assignment service watches for changes in the available recovery executor nodes
 * or recovery tasks, and attempts to assign tasks to available executors. In the event there are no
 * available recovery executors for the tasks a failure to assign will be noted, and the assignment
 * will be retried on following run.
 */
public class RecoveryTaskAssignmentService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTaskAssignmentService.class);

  private final RecoveryTaskMetadataStore recoveryTaskMetadataStore;
  private final RecoveryNodeMetadataStore recoveryNodeMetadataStore;
  private final MeterRegistry meterRegistry;
  private final AstraConfigs.ManagerConfig managerConfig;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String RECOVERY_TASKS_ASSIGNED = "recovery_tasks_assigned";
  public static final String RECOVERY_TASKS_ASSIGNMENT_FAILURES =
      "recovery_tasks_assignment_failures";
  public static final String RECOVERY_TASKS_INSUFFICIENT_CAPACITY =
      "recovery_tasks_insufficient_capacity";
  public static final String RECOVERY_TASK_ASSIGNMENT_TIMER = "recovery_task_assignment_timer";

  protected final Counter recoveryTasksAssigned;
  protected final Counter recoveryTaskAssignmentFailures;
  protected final Counter recoveryTasksInsufficientCapacity;
  private final Timer recoveryAssignmentTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  private final AstraMetadataStoreChangeListener<RecoveryNodeMetadata> recoveryNodeListener =
      (recoveryNodeMetadata) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<RecoveryTaskMetadata> recoveryTaskListener =
      (recoveryTaskListener) -> runOneIteration();

  public RecoveryTaskAssignmentService(
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      RecoveryNodeMetadataStore recoveryNodeMetadataStore,
      AstraConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.recoveryNodeMetadataStore = recoveryNodeMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    checkArgument(managerConfig.getEventAggregationSecs() > 0, "eventAggregationSecs must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    recoveryTasksAssigned = meterRegistry.counter(RECOVERY_TASKS_ASSIGNED);
    recoveryTaskAssignmentFailures = meterRegistry.counter(RECOVERY_TASKS_ASSIGNMENT_FAILURES);
    recoveryTasksInsufficientCapacity = meterRegistry.counter(RECOVERY_TASKS_INSUFFICIENT_CAPACITY);
    recoveryAssignmentTimer = meterRegistry.timer(RECOVERY_TASK_ASSIGNMENT_TIMER);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignRecoveryTasksToNodes,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Recovery task already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting recovery task assignment service");
    recoveryTaskMetadataStore.addListener(recoveryTaskListener);
    recoveryNodeMetadataStore.addListener(recoveryNodeListener);
  }

  @Override
  protected void shutDown() throws Exception {
    recoveryTaskMetadataStore.removeListener(recoveryTaskListener);
    recoveryNodeMetadataStore.removeListener(recoveryNodeListener);
    executorService.shutdown();
    LOG.info("Closed recovery task assignment service");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getRecoveryTaskAssignmentServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  /**
   * Assigns recovery tasks needing assignment to the first available recovery node, starting with
   * the oldest created recovery task. No preference is given specific executor nodes, which may
   * result in over/under utilization of specific recovery nodes.
   *
   * <p>If this method fails to successfully assign all the required tasks, the following iteration
   * of this method would attempt to re-assign these until there are no more available tasks.
   *
   * @return The count of successfully assigned recovery tasks
   */
  @SuppressWarnings("UnstableApiUsage")
  protected int assignRecoveryTasksToNodes() {
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);

    Set<String> recoveryTasksAlreadyAssigned =
        recoveryNodeMetadataStore.listSync().stream()
            .map(recoveryNodeMetadata -> recoveryNodeMetadata.recoveryTaskName)
            .filter((recoveryTaskName) -> !recoveryTaskName.isEmpty())
            .collect(Collectors.toUnmodifiableSet());

    List<RecoveryTaskMetadata> recoveryTasksThatNeedAssignment =
        recoveryTaskMetadataStore.listSync().stream()
            .filter(recoveryTask -> !recoveryTasksAlreadyAssigned.contains(recoveryTask.name))
            // We are currently starting with the oldest tasks first in an effort to reduce the
            // possibility of data loss, but this is likely opposite of what most users will
            // want when running Astra as a logging solution. If newest recovery tasks were
            // preferred, under heavy lag you would have higher-value logs available sooner,
            // at the increased chance of losing old logs.
            .sorted(Comparator.comparingLong(RecoveryTaskMetadata::getCreatedTimeEpochMs))
            .collect(Collectors.toUnmodifiableList());

    List<RecoveryNodeMetadata> availableRecoveryNodes =
        recoveryNodeMetadataStore.listSync().stream()
            .filter(
                recoveryNodeMetadata ->
                    recoveryNodeMetadata.recoveryNodeState.equals(
                        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE))
            .collect(Collectors.toUnmodifiableList());

    if (recoveryTasksThatNeedAssignment.size() > availableRecoveryNodes.size()) {
      LOG.warn(
          "Insufficient recovery nodes to assign task, wanted {} nodes but had {} nodes",
          recoveryTasksThatNeedAssignment.size(),
          availableRecoveryNodes.size());
      recoveryTasksInsufficientCapacity.increment(
          recoveryTasksThatNeedAssignment.size() - availableRecoveryNodes.size());
    } else if (recoveryTasksThatNeedAssignment.size() == 0) {
      LOG.debug("No recovery tasks found requiring assignment");
      assignmentTimer.stop(recoveryAssignmentTimer);
      return 0;
    }

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> recoveryTaskAssignments =
        Streams.zip(
                recoveryTasksThatNeedAssignment.stream(),
                availableRecoveryNodes.stream(),
                (recoveryTask, recoveryNode) -> {
                  RecoveryNodeMetadata recoveryNodeAssigned =
                      new RecoveryNodeMetadata(
                          recoveryNode.name,
                          Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                          recoveryTask.name,
                          Instant.now().toEpochMilli());

                  ListenableFuture<?> future =
                      // todo - consider refactoring this to return a completable future instead
                      JdkFutureAdapters.listenInPoolThread(
                          recoveryNodeMetadataStore
                              .updateAsync(recoveryNodeAssigned)
                              .toCompletableFuture());
                  addCallback(
                      future,
                      FutureUtils.successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(recoveryTaskAssignments);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulAssignments = successCounter.get();
    int failedAssignments = recoveryTaskAssignments.size() - successfulAssignments;

    recoveryTasksAssigned.increment(successfulAssignments);
    recoveryTaskAssignmentFailures.increment(failedAssignments);

    long assignmentDuration = assignmentTimer.stop(recoveryAssignmentTimer);
    LOG.info(
        "Completed recovery task assignment - successfully assigned {} tasks, failed to assign {} tasks in {} ms",
        successfulAssignments,
        failedAssignments,
        nanosToMillis(assignmentDuration));

    return successfulAssignments;
  }
}
