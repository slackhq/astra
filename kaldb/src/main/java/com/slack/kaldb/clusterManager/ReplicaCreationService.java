package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;
import static com.slack.kaldb.util.TimeUtils.nanosToMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.core.KaldbMetadataStoreChangeListener;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadata;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the lifecycle for the Replica metadata type. At least one Replica is expected to be
 * created once a snapshot has been published by an indexer node.
 *
 * <p>Each Replica is then expected to be assigned to a Cache node, depending on availability, by
 * the cache assignment service in the cluster manager
 */
public class ReplicaCreationService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaCreationService.class);
  private final KaldbConfigs.ManagerConfig managerConfig;

  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String REPLICAS_CREATED = "replicas_created";
  public static final String REPLICAS_FAILED = "replicas_failed";
  public static final String REPLICA_ASSIGNMENT_TIMER = "replica_assignment_timer";

  private final Counter.Builder replicasCreated;
  private final Counter.Builder replicasFailed;
  private final Timer.Builder replicaAssignmentTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  private final KaldbMetadataStoreChangeListener<SnapshotMetadata> snapshotListener =
      (snapshotMetadata) -> runOneIteration();

  public ReplicaCreationService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {

    checkArgument(
        managerConfig.getReplicaCreationServiceConfig().getReplicaLifespanMins() > 0,
        "replicaLifespanMins must be > 0");
    checkArgument(managerConfig.getEventAggregationSecs() > 0, "eventAggregationSecs must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.managerConfig = managerConfig;

    this.meterRegistry = meterRegistry;

    this.replicasCreated = Counter.builder(REPLICAS_CREATED);
    this.replicasFailed = Counter.builder(REPLICAS_FAILED);
    this.replicaAssignmentTimer = Timer.builder(REPLICA_ASSIGNMENT_TIMER);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica creator service");
    snapshotMetadataStore.addListener(snapshotListener);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica create service");
    snapshotMetadataStore.removeListener(snapshotListener);
    executorService.shutdownNow();
    LOG.info("Closed replica create service");
  }

  /**
   * Queues a task to be run in the future per a configurable value (getEventAggregationSecs). If a
   * task is pending but not yet started this is a no-op, and functions as a simple debounce to the
   * underlying createReplicasForUnassignedSnapshots method call. This method is synchronized as
   * this is invoked via the ZK event pool, in addition to directly via this service.
   */
  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::createReplicasForUnassignedSnapshots,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.debug(
          "Replica task already queued for execution, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected Scheduler scheduler() {
    // run one iteration getScheduleInitialDelayMins after startup, and then every
    // getSchedulePeriodMins mins after that
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getReplicaCreationServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  /**
   * Creates N replicas per the KalDb configuration for each snapshot. If a snapshot does not
   * contain at least the amount of replicas configured, this will attempt to create the missing
   * replicas to bring it into compliance. No attempt is made to reduce the replica count if for
   * some reason it exceeds the configured value.
   *
   * <p>If this method fails to successfully complete all the required replicas, the following
   * iteration of this method would attempt to re-create these until they are either brought into
   * compliance or the snapshot expiration is reached.
   *
   * @return The count of successful created replicas
   */
  protected Map<String, Integer> createReplicasForUnassignedSnapshots() {
    Map<String, Integer> assignments = new HashMap<>();

    for (String replicaSet : managerConfig.getReplicaCreationServiceConfig().getReplicaSetsList()) {
      LOG.info("Starting replica creation for unassigned snapshots for replicaSet {}", replicaSet);
      Timer.Sample assignmentTimer = Timer.start(meterRegistry);

      List<String> existingReplicas =
          replicaMetadataStore.listSync().stream()
              .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
              .map(replicaMetadata -> replicaMetadata.snapshotId)
              .toList();

      long snapshotExpiration =
          Instant.now()
              .minus(
                  managerConfig.getReplicaCreationServiceConfig().getReplicaLifespanMins(),
                  ChronoUnit.MINUTES)
              .toEpochMilli();

      AtomicInteger successCounter = new AtomicInteger(0);
      List<ListenableFuture<?>> createdReplicaMetadataList =
          snapshotMetadataStore.listSync().stream()
              // only attempt to create replicas for snapshots that have not expired, not live, and
              // do not already exist
              .filter(
                  snapshotMetadata ->
                      snapshotMetadata.endTimeEpochMs > snapshotExpiration
                          && !SnapshotMetadata.isLive(snapshotMetadata)
                          && !existingReplicas.contains(snapshotMetadata.snapshotId))
              .map(
                  (snapshotMetadata) -> {
                    // todo - consider refactoring this to return a completable future //
                    // instead
                    ListenableFuture<?> future =
                        JdkFutureAdapters.listenInPoolThread(
                            replicaMetadataStore
                                .createAsync(
                                    replicaMetadataFromSnapshotId(
                                        snapshotMetadata.snapshotId,
                                        replicaSet,
                                        Instant.ofEpochMilli(snapshotMetadata.endTimeEpochMs)
                                            .plus(
                                                managerConfig
                                                    .getReplicaCreationServiceConfig()
                                                    .getReplicaLifespanMins(),
                                                ChronoUnit.MINUTES),
                                        false))
                                .toCompletableFuture());
                    addCallback(
                        future,
                        successCountingCallback(successCounter),
                        MoreExecutors.directExecutor());
                    return future;
                  })
              .collect(Collectors.toUnmodifiableList());

      ListenableFuture<?> futureList = Futures.successfulAsList(createdReplicaMetadataList);
      try {
        futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
      } catch (Exception e) {
        futureList.cancel(true);
      }

      int createdReplicas = successCounter.get();
      int failedReplicas = createdReplicaMetadataList.size() - createdReplicas;

      replicasCreated
          .tag("replicaSet", replicaSet)
          .register(meterRegistry)
          .increment(createdReplicas);
      replicasFailed
          .tag("replicaSet", replicaSet)
          .register(meterRegistry)
          .increment(failedReplicas);

      long assignmentDuration =
          assignmentTimer.stop(
              replicaAssignmentTimer.tag("replicaSet", replicaSet).register(meterRegistry));
      LOG.info(
          "Completed replica creation for unassigned snapshots in replicaSet {} - successfully created {} replicas, failed {} replicas in {} ms",
          replicaSet,
          createdReplicas,
          failedReplicas,
          nanosToMillis(assignmentDuration));
      assignments.put(replicaSet, createdReplicas);
    }

    return assignments;
  }

  public static ReplicaMetadata replicaMetadataFromSnapshotId(
      String snapshotId, String replicaSet, Instant expireAfter, boolean isRestored) {
    return new ReplicaMetadata(
        String.format("%s-%s", snapshotId, UUID.randomUUID()),
        snapshotId,
        replicaSet,
        Instant.now().toEpochMilli(),
        expireAfter.toEpochMilli(),
        isRestored,
        Metadata.IndexType.LOGS_LUCENE9);
  }
}
