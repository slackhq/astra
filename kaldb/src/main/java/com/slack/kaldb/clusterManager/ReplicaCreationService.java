package com.slack.kaldb.clusterManager;

import static com.google.common.base.Preconditions.checkArgument;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
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
  private final KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig
      replicaCreationServiceConfig;
  private final KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig
      replicaEvictionServiceConfig;

  private final ReplicaMetadataStore replicaMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final MeterRegistry meterRegistry;

  public static final String REPLICAS_CREATED = "replicas_created";
  public static final String REPLICAS_FAILED = "replicas_failed";
  public static final String REPLICA_ASSIGNMENT_TIMER = "replica_assignment_timer";

  private final Counter replicasCreated;
  private final Counter replicasFailed;
  private final Timer replicaAssignmentTimer;

  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor();
  private ScheduledFuture<?> pendingTask;

  public ReplicaCreationService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      KaldbConfigs.ManagerConfig.ReplicaCreationServiceConfig replicaCreationServiceConfig,
      KaldbConfigs.ManagerConfig.ReplicaEvictionServiceConfig replicaEvictionServiceConfig,
      MeterRegistry meterRegistry) {

    checkArgument(
        replicaCreationServiceConfig.getReplicasPerSnapshot() >= 0,
        "replicasPerSnapshot must be >= 0");
    checkArgument(
        replicaCreationServiceConfig.getEventAggregationSecs() > 0,
        "eventAggregationSecs must be > 0");
    checkArgument(
        replicaEvictionServiceConfig.getReplicaLifespanMins() > 0,
        "replicaLifespanMins must be > 0");
    // schedule configs checked as part of the AbstractScheduledService

    this.replicaMetadataStore = replicaMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.replicaCreationServiceConfig = replicaCreationServiceConfig;
    this.replicaEvictionServiceConfig = replicaEvictionServiceConfig;

    this.meterRegistry = meterRegistry;
    this.replicasCreated = meterRegistry.counter(REPLICAS_CREATED);
    this.replicasFailed = meterRegistry.counter(REPLICAS_FAILED);
    this.replicaAssignmentTimer = meterRegistry.timer(REPLICA_ASSIGNMENT_TIMER);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica creator service");
    snapshotMetadataStore.addListener(this::runOneIteration);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing replica create service");
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
              replicaCreationServiceConfig.getEventAggregationSecs(),
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
        replicaCreationServiceConfig.getScheduleInitialDelayMins(),
        replicaCreationServiceConfig.getSchedulePeriodMins(),
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
  protected int createReplicasForUnassignedSnapshots() {
    LOG.info("Starting replica creation for unassigned snapshots");
    Timer.Sample assignmentTimer = Timer.start(meterRegistry);

    // build a map of snapshot ID to how many replicas currently exist for that snapshot ID
    Map<String, Long> snapshotToReplicas =
        replicaMetadataStore
            .getCached()
            .stream()
            .collect(
                Collectors.groupingBy(
                    (replicaMetadata) -> replicaMetadata.snapshotId, Collectors.counting()));

    long snapshotExpiration =
        Instant.now()
            .minus(replicaEvictionServiceConfig.getReplicaLifespanMins(), ChronoUnit.MINUTES)
            .toEpochMilli();
    List<ListenableFuture<?>> createdReplicaMetadataList =
        snapshotMetadataStore
            .getCached()
            .stream()
            // only attempt to create replicas for snapshots that have not expired
            .filter((snapshotMetadata -> snapshotMetadata.endTimeUtc > snapshotExpiration))
            .map(
                (snapshotMetadata) ->
                    LongStream.range(
                            snapshotToReplicas.getOrDefault(snapshotMetadata.snapshotId, 0L),
                            replicaCreationServiceConfig.getReplicasPerSnapshot())
                        .mapToObj(
                            (i) ->
                                replicaMetadataStore.create(
                                    replicaMetadataFromSnapshotId(snapshotMetadata.snapshotId)))
                        .collect(Collectors.toList()))
            .flatMap(List::stream)
            .collect(Collectors.toList());

    ListenableFuture<List<Object>> futureList = Futures.allAsList(createdReplicaMetadataList);
    int createdReplicas = 0;
    int failedReplicas = 0;

    try {
      createdReplicas = futureList.get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS).size();
    } catch (Exception futureListException) {
      for (ListenableFuture<?> future : createdReplicaMetadataList) {
        if (future.isDone()) {
          try {
            // this additional try/catch/get is used to determine if the future is complete with an
            // exception
            // if there is an exception we count this as a failed replica for monitoring purposes
            future.get();
            createdReplicas++;
          } catch (Exception futureException) {
            failedReplicas++;
          }
        } else {
          future.cancel(true);
          failedReplicas++;
        }
      }
    }

    replicasCreated.increment(createdReplicas);
    replicasFailed.increment(failedReplicas);

    long assignmentDuration = assignmentTimer.stop(replicaAssignmentTimer);
    LOG.info(
        "Completed replica creation for unassigned snapshots - successfully created {} replicas, failed {} replicas in {} ms",
        createdReplicas,
        failedReplicas,
        TimeUnit.MILLISECONDS.convert(assignmentDuration, TimeUnit.NANOSECONDS));

    return createdReplicas;
  }

  public static ReplicaMetadata replicaMetadataFromSnapshotId(String snapshotId) {
    return new ReplicaMetadata(
        String.format("%s-%s", snapshotId, UUID.randomUUID()),
        snapshotId,
        Instant.now().toEpochMilli());
  }
}
