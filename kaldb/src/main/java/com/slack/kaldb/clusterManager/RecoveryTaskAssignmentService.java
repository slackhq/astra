package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadata;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;
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
  private final KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
      recoveryTaskAssignmentServiceConfig;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

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
      KaldbConfigs.ManagerConfig.RecoveryTaskAssignmentServiceConfig
          recoveryTaskAssignmentServiceConfig,
      MeterRegistry meterRegistry) {
    this.recoveryTaskMetadataStore = recoveryTaskMetadataStore;
    this.recoveryNodeMetadataStore = recoveryNodeMetadataStore;
    this.recoveryTaskAssignmentServiceConfig = recoveryTaskAssignmentServiceConfig;
    this.meterRegistry = meterRegistry;

    checkArgument(
        recoveryTaskAssignmentServiceConfig.getEventAggregationSecs() > 0,
        "eventAggregationSecs must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    recoveryTasksCreated = meterRegistry.counter(RECOVERY_TASKS_CREATED);
    recoveryTasksFailed = meterRegistry.counter(RECOVERY_TASKS_FAILED);
    recoveryAssignmentTimer = meterRegistry.timer(RECOVERY_TASK_ASSIGNMENT_TIMER);
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::assignRecoveryTasksToNodes,
              recoveryTaskAssignmentServiceConfig.getEventAggregationSecs(),
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
    recoveryTaskMetadataStore.addListener(this::runOneIteration);
    recoveryNodeMetadataStore.addListener(this::runOneIteration);
  }

  @Override
  protected void shutDown() throws Exception {
    executorService.shutdown();
    LOG.info("Closed recovery task assignment service");
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        recoveryTaskAssignmentServiceConfig.getScheduleInitialDelayMins(),
        recoveryTaskAssignmentServiceConfig.getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  /**
   * Assigns the first recovery task needing assignment to the first available recovery node. No
   * preference is given to specific recovery tasks, nor to executor nodes. This may result in
   * over/under utilization of specific recovery nodes as well as indeterminate order of recovery
   * task fulfillment.
   *
   * <p>If this method fails to successfully assign all the required tasks, the following iteration
   * of this method would attempt to re-assign these until there are no more available tasks.
   *
   * @return The count of successfully assigned recovery tasks
   */
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

    ConcurrentLinkedDeque<RecoveryNodeMetadata> availableRecoveryNodes =
        recoveryNodeMetadataStore
            .getCached()
            .stream()
            .filter(
                (recoveryNodeMetadata ->
                    recoveryNodeMetadata.recoveryNodeState.equals(
                        Metadata.RecoveryNodeMetadata.RecoveryNodeState.FREE)))
            .collect(Collectors.toCollection(ConcurrentLinkedDeque::new));

    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> recoveryTaskAssignments =
        recoveryTasksThatNeedAssignment
            .stream()
            .map(
                (recoveryTaskMetadata -> {
                  if (!availableRecoveryNodes.isEmpty()) {
                    RecoveryNodeMetadata recoveryNodeAssigned =
                        new RecoveryNodeMetadata(
                            availableRecoveryNodes.pop().name,
                            Metadata.RecoveryNodeMetadata.RecoveryNodeState.ASSIGNED,
                            recoveryTaskMetadata.name,
                            Instant.now().toEpochMilli());

                    ListenableFuture<?> future =
                        recoveryNodeMetadataStore.update(recoveryNodeAssigned);
                    addCallback(
                        future,
                        successCountingCallback(successCounter),
                        MoreExecutors.directExecutor());
                    return future;
                  } else {
                    LOG.warn(
                        "No available recovery nodes to assign task, will try again later - {}",
                        recoveryTaskMetadata.name);
                    return Futures.immediateCancelledFuture();
                  }
                }))
            .collect(Collectors.toList());

    ListenableFuture<?> futureList = Futures.successfulAsList(recoveryTaskAssignments);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulAssignments = successCounter.get();
    int failedAssignments = recoveryTaskAssignments.size() - successfulAssignments;

    recoveryTasksCreated.increment(successfulAssignments);
    recoveryTasksFailed.increment(failedAssignments);

    long assignmentDuration = assignmentTimer.stop(recoveryAssignmentTimer);
    LOG.info(
        "Completed recovery task assignment - successfully assigned {} tasks, failed to assign {} tasks in {} ms",
        successfulAssignments,
        failedAssignments,
        TimeUnit.MILLISECONDS.convert(assignmentDuration, TimeUnit.NANOSECONDS));

    return successfulAssignments;
  }

  /** Uses the provided atomic integer to keep track of FutureCallbacks that are successful */
  private FutureCallback<Object> successCountingCallback(AtomicInteger counter) {
    return new FutureCallback<>() {
      @Override
      public void onSuccess(@Nullable Object result) {
        counter.incrementAndGet();
      }

      @Override
      public void onFailure(Throwable t) {
        // no-op
      }
    };
  }
}
