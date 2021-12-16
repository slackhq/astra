package com.slack.kaldb.clusterManager;

import static com.google.common.util.concurrent.Futures.addCallback;
import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;
import static com.slack.kaldb.util.FutureUtils.successCountingCallback;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.slack.kaldb.metadata.cache.CacheSlotMetadata;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Marks expired replicas that are currently assigned to a cache slot for eviction. This service
 * does not delete or mutate replicas, but only marks the cache slot as pending eviction. Once the
 * cache node has evicted an expired replica, a separate service will delete expired, unassigned
 * replicas.
 */
public class ReplicaEvictionService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaEvictionService.class);

  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final KaldbConfigs.ManagerConfig managerConfig;
  private final MeterRegistry meterRegistry;

  @VisibleForTesting protected int futuresListTimeoutSecs = DEFAULT_ZK_TIMEOUT_SECS;

  public static final String REPLICA_MARK_EVICT_SUCCEEDED = "replica_mark_evict_succeeded";
  public static final String REPLICA_MARK_EVICT_FAILED = "replica_mark_evict_failed";
  public static final String REPLICA_MARK_EVICT_TIMER = "replica_mark_evict_timer";

  protected final Counter replicaMarkEvictSucceeded;
  protected final Counter replicaMarkEvictFailed;
  private final Timer replicaMarkEvictTimer;

  public ReplicaEvictionService(
      CacheSlotMetadataStore cacheSlotMetadataStore,
      ReplicaMetadataStore replicaMetadataStore,
      KaldbConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.managerConfig = managerConfig;
    this.meterRegistry = meterRegistry;

    // schedule configs checked as part of the AbstractScheduledService

    replicaMarkEvictSucceeded = meterRegistry.counter(REPLICA_MARK_EVICT_SUCCEEDED);
    replicaMarkEvictFailed = meterRegistry.counter(REPLICA_MARK_EVICT_FAILED);
    replicaMarkEvictTimer = meterRegistry.timer(REPLICA_MARK_EVICT_TIMER);
  }

  @Override
  protected void runOneIteration() {
    markReplicasForEviction();
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getReplicaEvictionServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting replica eviction service");
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closed replica eviction service");
  }

  /**
   * Marks cache slots that contain an expired replica as ready for eviction. Once eviction has
   * started the cache node will further update the state indicating it has started evicting the
   * replica from memory/disk.
   */
  protected int markReplicasForEviction() {
    Timer.Sample evictionTimer = Timer.start(meterRegistry);

    Map<String, ReplicaMetadata> replicaMetadataByReplicaId =
        replicaMetadataStore
            .getCached()
            .stream()
            .collect(Collectors.toUnmodifiableMap(ReplicaMetadata::getName, Function.identity()));

    long expireAfterNow = Instant.now().toEpochMilli();
    AtomicInteger successCounter = new AtomicInteger(0);
    List<ListenableFuture<?>> replicaEvictions =
        cacheSlotMetadataStore
            .getCached()
            .stream()
            // get all the slots that are currently assigned, but have an expiration in the past
            .filter(
                cacheSlotMetadata ->
                    cacheSlotMetadata.cacheSlotState.equals(
                            Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED)
                        && replicaMetadataByReplicaId.containsKey(cacheSlotMetadata.replicaId)
                        && replicaMetadataByReplicaId.get(cacheSlotMetadata.replicaId)
                                .expireAfterUtc
                            < expireAfterNow)
            .map(
                (cacheSlotMetadata) -> {
                  ListenableFuture<?> future =
                      cacheSlotMetadataStore.update(
                          new CacheSlotMetadata(
                              cacheSlotMetadata.name,
                              Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
                              cacheSlotMetadata.replicaId,
                              Instant.now().toEpochMilli()));

                  addCallback(
                      future,
                      successCountingCallback(successCounter),
                      MoreExecutors.directExecutor());
                  return future;
                })
            .collect(Collectors.toUnmodifiableList());

    ListenableFuture<?> futureList = Futures.successfulAsList(replicaEvictions);
    try {
      futureList.get(futuresListTimeoutSecs, TimeUnit.SECONDS);
    } catch (Exception e) {
      futureList.cancel(true);
    }

    int successfulEvictions = successCounter.get();
    int failedEvictions = replicaEvictions.size() - successfulEvictions;

    replicaMarkEvictSucceeded.increment(successfulEvictions);
    replicaMarkEvictFailed.increment(failedEvictions);

    long evictionDuration = evictionTimer.stop(replicaMarkEvictTimer);
    LOG.info(
        "Completed replica evictions - successfully marked {} slots for eviction, failed to mark {} slots for eviction in {} ms",
        successfulEvictions,
        failedEvictions,
        TimeUnit.MILLISECONDS.convert(evictionDuration, TimeUnit.NANOSECONDS));

    return successfulEvictions;
  }
}
