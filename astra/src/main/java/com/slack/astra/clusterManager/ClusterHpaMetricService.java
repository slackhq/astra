package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.CacheNodeAssignmentService.getSnapshotsFromIds;
import static com.slack.astra.clusterManager.CacheNodeAssignmentService.snapshotMetadataBySnapshotId;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.hpa.HpaMetricMetadata;
import com.slack.astra.metadata.hpa.HpaMetricMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadata;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.metadata.Metadata;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Cluster HPA metrics service is intended to be run on the manager node, and is used for making
 * centralized, application-aware decisions that can be used to inform a Kubernetes horizontal pod
 * autoscaler (HPA).
 *
 * @see <a
 *     href="https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/">Kubernetes
 *     HPA</a>
 */
public class ClusterHpaMetricService extends AbstractScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterHpaMetricService.class);

  // todo - consider making HPA_TOLERANCE and CACHE_SCALEDOWN_LOCK configurable
  // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#algorithm-details
  private static final double HPA_TOLERANCE = 0.1;
  private final SnapshotMetadataStore snapshotMetadataStore;
  protected Duration CACHE_SCALEDOWN_LOCK = Duration.of(15, ChronoUnit.MINUTES);

  private final ReplicaMetadataStore replicaMetadataStore;
  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final HpaMetricMetadataStore hpaMetricMetadataStore;
  private final CacheNodeMetadataStore cacheNodeMetadataStore;
  protected final Map<String, Instant> cacheScalingLock = new ConcurrentHashMap<>();
  protected static final String CACHE_HPA_METRIC_NAME = "hpa_cache_demand_factor_%s";

  public ClusterHpaMetricService(
      ReplicaMetadataStore replicaMetadataStore,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      HpaMetricMetadataStore hpaMetricMetadataStore,
      CacheNodeMetadataStore cacheNodeMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore) {
    this.replicaMetadataStore = replicaMetadataStore;
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.hpaMetricMetadataStore = hpaMetricMetadataStore;
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(Duration.ofSeconds(15), Duration.ofSeconds(30));
  }

  @Override
  protected synchronized void runOneIteration() {
    LOG.info("Running ClusterHpaMetricService");
    try {
      publishCacheHpaMetrics();
    } catch (Exception e) {
      LOG.error("Error running ClusterHpaMetricService", e);
    }
  }

  /**
   * Calculates and publishes HPA scaling metric(s) to Zookeeper for the Cache nodes. This makes use
   * of the Kubernetes HPA formula, which is:
   *
   * <p>desiredReplicas = ceil[currentReplicas * ( currentMetricValue / desiredMetricValue )]
   *
   * <p>Using this formula, if we inform the user what value to set for the desiredMetricValue, we
   * can vary the currentMetricValue to control the scale of the cluster. This is accomplished by
   * producing a metric (hpa_cache_demand_factor_REPLICASET) that targets a value of 1.0. As long as
   * the HPA is configured to target a value of 1.0 this will result in an optimized cluster size
   * based on cache slots vs replicas, plus a small buffer.
   *
   * <pre>
   * metrics:
   *   - type: Pods
   *     pods:
   *       metric:
   *         name: hpa_cache_demand_factor_REPLICASET
   *       target:
   *         type: AverageValue
   *         averageValue: 1.0
   * </pre>
   */
  private void publishCacheHpaMetrics() {
    List<String> replicaSets =
        replicaMetadataStore.listSync().stream()
            .map(ReplicaMetadata::getReplicaSet)
            .distinct()
            .collect(Collectors.toList());
    Collections.shuffle(replicaSets);

    for (String replicaSet : replicaSets) {
      long totalCacheSlotCapacity =
          cacheSlotMetadataStore.listSync().stream()
              .filter(cacheSlotMetadata -> cacheSlotMetadata.replicaSet.equals(replicaSet))
              .count();
      long totalReplicaDemand =
          replicaMetadataStore.listSync().stream()
              .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
              .count();

      long totalCacheNodeCapacityBytes =
          cacheNodeMetadataStore.listSync().stream()
              .filter(metadata -> metadata.getReplicaSet().equals(replicaSet))
              .mapToLong(node -> node.nodeCapacityBytes)
              .sum();
      long totalDemandBytes =
          getSnapshotsFromIds(
                  snapshotMetadataBySnapshotId(snapshotMetadataStore),
                  replicaMetadataStore.listSync().stream()
                      .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
                      .map(replica -> replica.snapshotId)
                      .collect(Collectors.toSet()))
              .stream()
              .mapToLong(snapshot -> snapshot.sizeInBytesOnDisk)
              .sum();

      double demandFactor =
          calculateDemandFactor(
              totalCacheSlotCapacity,
              totalReplicaDemand,
              totalCacheNodeCapacityBytes,
              totalDemandBytes);
      String action;
      if (demandFactor > 1) {
        // scale-up
        if (demandFactor < (1 + HPA_TOLERANCE)) {
          // scale-up required, but still within the HPA tolerance
          // we need to ensure the scale-up is at least triggering the HPA
          demandFactor = demandFactor + HPA_TOLERANCE;
        }
        action = "scale-up";
        persistCacheConfig(replicaSet, demandFactor);
      } else if (demandFactor < (1 - HPA_TOLERANCE)) {
        // scale-down required
        if (tryCacheReplicasetLock(replicaSet)) {
          action = "scale-down";
          persistCacheConfig(replicaSet, demandFactor);
        } else {
          // couldn't get exclusive lock, no-op
          action = "pending-scale-down";
          persistCacheConfig(replicaSet, 1.0);
        }
      } else {
        // over-provisioned, but within HPA tolerance
        action = "no-op";
        persistCacheConfig(replicaSet, demandFactor);
      }

      LOG.info(
          "Cache autoscaler for replicaset '{}' took action '{}', demandFactor: '{}', totalReplicaDemand: '{}', totalCacheSlotCapacity: '{}'",
          replicaSet,
          action,
          demandFactor,
          totalReplicaDemand,
          totalCacheSlotCapacity);
    }
  }

  private static double calculateDemandFactor(
      long totalCacheSlotCapacity,
      long totalReplicaDemand,
      long totalCacheNodeCapacityBytes,
      long totalAssignedBytes) {
    // Attempt to calculate hpa value from ng dynamic chunk cache nodes if no cache slot capacity
    if (totalCacheSlotCapacity == 0) {
      LOG.info(
          "Cache slot capacity is 0, attempting to calculate HPA value from dynamic chunk cache node capacities");
      return calculateDemandFactorFromCacheNodeCapacity(
          totalAssignedBytes, totalCacheNodeCapacityBytes);
    }

    // Fallback to old cache slot calculation
    return calculateDemandFactor(totalCacheSlotCapacity, totalReplicaDemand);
  }

  @VisibleForTesting
  protected static double calculateDemandFactor(
      long totalCacheSlotCapacity, long totalReplicaDemand) {
    if (totalCacheSlotCapacity == 0) {
      // we have no provisioned capacity, so cannot determine a value
      // this should never happen unless the user misconfigured the HPA with a minimum instance
      // count of 0
      LOG.error(
          "No cache slot capacity is detected, this indicates a misconfiguration of the HPA minimum instance count which must be at least 1");
      return 1;
    }
    // demand factor will be < 1 indicating a scale-down demand, and > 1 indicating a scale-up
    double rawDemandFactor = (double) totalReplicaDemand / totalCacheSlotCapacity;
    // round up to 2 decimals
    return Math.ceil(rawDemandFactor * 100) / 100;
  }

  private static double calculateDemandFactorFromCacheNodeCapacity(
      long totalBytesRequiringAssignment, long totalCacheNodeCapacityBytes) {
    if (totalCacheNodeCapacityBytes == 0) {
      LOG.error("No cache node capacity is detected");
      return 1;
    }

    double rawDemandFactor = (double) totalBytesRequiringAssignment / totalCacheNodeCapacityBytes;
    LOG.info(
        "Calculating demand factor from ng cache nodes: bytes needed: {}, capacity: {}, demandFactor: {}",
        totalBytesRequiringAssignment,
        totalCacheNodeCapacityBytes,
        Math.ceil(rawDemandFactor * 100) / 100);
    return Math.ceil(rawDemandFactor * 100) / 100;
  }

  /** Updates or inserts an (ephemeral) HPA metric for the cache nodes. This is NOT threadsafe. */
  private void persistCacheConfig(String replicaSet, Double demandFactor) {
    String key = String.format(CACHE_HPA_METRIC_NAME, replicaSet);
    try {
      if (hpaMetricMetadataStore.hasSync(key)) {
        hpaMetricMetadataStore.updateSync(
            new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
      } else {
        hpaMetricMetadataStore.createSync(
            new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
      }
    } catch (Exception e) {
      LOG.error("Failed to persist hpa metric metadata", e);
    }
  }

  /**
   * Either acquires or refreshes an existing time-based lock for the given replicaset. Used to
   * prevent scale-down operations from happening to quickly between replicasets, causing issues
   * with re-balancing.
   */
  protected boolean tryCacheReplicasetLock(String replicaset) {
    Optional<Instant> lastOtherScaleOperation =
        cacheScalingLock.entrySet().stream()
            .filter(entry -> !Objects.equals(entry.getKey(), replicaset))
            .map(Map.Entry::getValue)
            .max(Instant::compareTo);

    // if another replicaset was scaled down in the last CACHE_SCALEDOWN_LOCK mins, prevent this one
    // from scaling
    if (lastOtherScaleOperation.isPresent()) {
      if (!lastOtherScaleOperation.get().isBefore(Instant.now().minus(CACHE_SCALEDOWN_LOCK))) {
        return false;
      }
    }

    // only refresh the lock if it doesn't exist, or is expired
    if (cacheScalingLock.containsKey(replicaset)) {
      if (cacheScalingLock.get(replicaset).isBefore(Instant.now().minus(CACHE_SCALEDOWN_LOCK))) {
        // update the last-acquired lock time to now (ie, refresh the lock for another
        // CACHE_SCALEDOWN_LOCK mins
        cacheScalingLock.put(replicaset, Instant.now());
      }
    } else {
      // set the last-updated lock time to now
      cacheScalingLock.put(replicaset, Instant.now());
    }
    return true;
  }
}
