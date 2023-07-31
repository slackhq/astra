package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadata;
import com.slack.kaldb.metadata.hpa.HpaMetricMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadata;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

  // todo - consider making over-provision and lock duration configurable values
  private static final Double CACHE_OVER_PROVISION = 1.1;

  protected Duration CACHE_SCALEDOWN_LOCK = Duration.of(15, ChronoUnit.MINUTES);
  protected static final String CACHE_HPA_METRIC_NAME = "hpa_cache_demand_factor_%s";

  private final ReplicaMetadataStore replicaMetadataStore;
  private final CacheSlotMetadataStore cacheSlotMetadataStore;
  private final HpaMetricMetadataStore hpaMetricMetadataStore;
  protected final Map<String, Instant> cacheScalingLock = new ConcurrentHashMap<>();

  public ClusterHpaMetricService(
      ReplicaMetadataStore replicaMetadataStore,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      HpaMetricMetadataStore hpaMetricMetadataStore) {
    this.replicaMetadataStore = replicaMetadataStore;
    this.cacheSlotMetadataStore = cacheSlotMetadataStore;
    this.hpaMetricMetadataStore = hpaMetricMetadataStore;
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(Duration.ofSeconds(15), Duration.ofSeconds(30));
  }

  @Override
  protected void runOneIteration() {
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
    Set<String> replicaSets =
        replicaMetadataStore.listSync().stream()
            .map(ReplicaMetadata::getReplicaSet)
            .collect(Collectors.toSet());

    for (String replicaSet : replicaSets) {
      long totalCacheSlotCapacity =
          cacheSlotMetadataStore.listSync().stream()
              .filter(cacheSlotMetadata -> cacheSlotMetadata.replicaSet.equals(replicaSet))
              .count();
      long totalReplicaDemand =
          replicaMetadataStore.listSync().stream()
              .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
              .count();
      double rawDemandFactor =
          (totalReplicaDemand * CACHE_OVER_PROVISION + 1) / (totalCacheSlotCapacity + 1);
      double demandFactor = (double) Math.round(rawDemandFactor * 100) / 100;
      LOG.info(
          "Cache autoscaler for replicaSet '{}' calculated a demandFactor of '{}' - totalReplicaDemand: '{}', overProvision: '{}', totalCacheSlotCapacity: '{}'",
          replicaSet,
          demandFactor,
          totalReplicaDemand,
          CACHE_OVER_PROVISION,
          totalCacheSlotCapacity);

      if (demandFactor >= 1.0) {
        // Publish a scale-up metric
        persistCacheConfig(replicaSet, demandFactor);
        LOG.debug("Publishing scale-up request for replicaset '{}'", replicaSet);
      } else {
        if (tryCacheReplicasetLock(replicaSet)) {
          // Publish a scale-down metric
          persistCacheConfig(replicaSet, demandFactor);
          LOG.debug("Acquired scale-down lock for replicaSet '{}'", replicaSet);
        } else {
          // Publish a no-op scale metric (0) to disable the HPA from applying
          // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#implicit-maintenance-mode-deactivation
          persistCacheConfig(replicaSet, 0.0);
          LOG.debug("Unable to acquire scale-down lock for replicaSet '{}'", replicaSet);
        }
      }
    }
  }

  /** Updates or inserts an (ephemeral) HPA metric for the cache nodes. This is NOT threadsafe. */
  private void persistCacheConfig(String replicaSet, Double demandFactor) {
    String key = String.format(CACHE_HPA_METRIC_NAME, replicaSet);
    if (hpaMetricMetadataStore.hasSync(key)) {
      hpaMetricMetadataStore.updateSync(
          new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
    } else {
      hpaMetricMetadataStore.createSync(
          new HpaMetricMetadata(key, Metadata.HpaMetricMetadata.NodeRole.CACHE, demandFactor));
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

    // update the last-updated lock time to now
    cacheScalingLock.put(replicaset, Instant.now());
    return true;
  }
}
