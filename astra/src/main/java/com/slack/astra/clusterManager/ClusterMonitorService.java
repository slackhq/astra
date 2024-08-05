package com.slack.astra.clusterManager;

import static com.slack.astra.clusterManager.CacheNodeAssignmentService.getSnapshotsFromIds;
import static com.slack.astra.clusterManager.CacheNodeAssignmentService.snapshotMetadataBySnapshotId;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.cache.CacheSlotMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.dataset.DatasetMetadataStore;
import com.slack.astra.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.astra.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.astra.metadata.replica.ReplicaMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClusterMonitor runs as a service in the manager component and monitors the state of the Astra
 * cluster.
 */
public class ClusterMonitorService extends AbstractScheduledService {
  private final AstraConfigs.ManagerConfig managerConfig;
  private ScheduledFuture<?> pendingTask;
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setNameFormat("cluster-monitor-service-%d").build());

  private static final Logger LOG = LoggerFactory.getLogger(ClusterMonitorService.class);
  private final MultiGauge liveChunksPerPod;
  private final MultiGauge freeSpacePerPod;

  public static final String LIVE_CHUNKS_PER_POD = "live_chunks_per_pod";
  public static final String FREE_SPACE_PER_POD = "free_space_per_pod";

  private final CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private final CacheNodeMetadataStore cacheNodeMetadataStore;
  private final ReplicaMetadataStore replicaMetadataStore;
  private final AstraMetadataStoreChangeListener<CacheNodeMetadata> cacheNodeMetadataListener =
      (_) -> runOneIteration();
  private final AstraMetadataStoreChangeListener<CacheNodeAssignment> cacheNodeAssignmentListener =
      (_) -> runOneIteration();
  private final Map<String, AtomicInteger> cacheNodeIdToLiveChunksPerPod;
  private final Map<String, AtomicLong> cacheNodeIdToFreeSpaceBytes;

  public ClusterMonitorService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      RecoveryNodeMetadataStore recoveryNodeMetadataStore,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      CacheNodeMetadataStore cacheNodeMetadataStore,
      AstraConfigs.ManagerConfig managerConfig,
      MeterRegistry meterRegistry) {
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;
    this.replicaMetadataStore = replicaMetadataStore;
    this.cacheNodeIdToFreeSpaceBytes = new ConcurrentHashMap<>();
    this.cacheNodeIdToLiveChunksPerPod = new ConcurrentHashMap<>();
    this.managerConfig = managerConfig;

    this.liveChunksPerPod =
        MultiGauge.builder(LIVE_CHUNKS_PER_POD)
            .description("Number of live chunks per pod")
            .register(meterRegistry);

    this.freeSpacePerPod =
        MultiGauge.builder(FREE_SPACE_PER_POD)
            .description("Free space per pod in bytes")
            .register(meterRegistry);

    meterRegistry.gauge(
        "cached_replica_nodes_size", replicaMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_snapshots_size", snapshotMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_recovery_tasks_size", recoveryTaskMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_recovery_nodes_size", recoveryNodeMetadataStore, store -> store.listSync().size());

    for (String replicaSet :
        managerConfig.getCacheNodeAssignmentServiceConfig().getReplicaSetsList()) {
      meterRegistry.gauge(
          "total_live_bytes",
          List.of(Tag.of("replicaSet", replicaSet)),
          cacheNodeAssignmentStore,
          store ->
              store.listSync().stream()
                  .filter(
                      assignment ->
                          assignment.state
                                  == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE
                              && Objects.equals(assignment.replicaSet, replicaSet))
                  .mapToLong(assignment -> assignment.snapshotSize)
                  .sum());

      meterRegistry.gauge(
          "total_assigned_bytes",
          List.of(Tag.of("replicaSet", replicaSet)),
          snapshotMetadataStore,
          store -> calculateAssignedBytes(replicaSet, store));

      // # of assignments
      meterRegistry.gauge(
          "total_num_chunks_assigned",
          List.of(Tag.of("replicaSet", replicaSet)),
          replicaMetadataStore,
          store -> calculateAssignedChunks(replicaSet, store));

      // # of live assignments
      meterRegistry.gauge(
          "total_num_live_chunks",
          List.of(Tag.of("replicaSet", replicaSet)),
          cacheNodeAssignmentStore,
          store ->
              store.listSync().stream()
                  .filter(
                      assignment ->
                          assignment.state
                                  == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE
                              && Objects.equals(assignment.replicaSet, replicaSet))
                  .toList()
                  .size());

      // total capacity of all cache nodes
      meterRegistry.gauge(
          "total_capacity_cache_nodes",
          List.of(Tag.of("replicaSet", replicaSet)),
          cacheNodeMetadataStore,
          store ->
              store.listSync().stream()
                  .filter((node) -> Objects.equals(node.getReplicaSet(), replicaSet))
                  .mapToLong(node -> node.nodeCapacityBytes)
                  .sum());
    }

    updatePerPodMetrics();

    for (Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState :
        Metadata.CacheSlotMetadata.CacheSlotState.values()) {
      meterRegistry.gauge(
          "cached_cache_slots_size",
          List.of(Tag.of("cacheSlotState", cacheSlotState.toString())),
          cacheSlotMetadataStore,
          store ->
              store.listSync().stream()
                  .filter(
                      cacheSlotMetadata -> cacheSlotMetadata.cacheSlotState.equals(cacheSlotState))
                  .count());
    }

    meterRegistry.gauge(
        "cached_cache_slots_size", cacheSlotMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_service_nodes_size", datasetMetadataStore, store -> store.listSync().size());
  }

  private void updatePerPodMetrics() {
    liveChunksPerPod.register(
        cacheNodeMetadataStore.listSync().stream()
            .map(
                cacheNodeMetadata ->
                    MultiGauge.Row.of(
                        Tags.of(Tag.of("pod", cacheNodeMetadata.hostname)),
                        cacheNodeIdToLiveChunksPerPod.computeIfAbsent(
                            cacheNodeMetadata.hostname,
                            (_) -> new AtomicInteger(calculateLiveChunks(cacheNodeMetadata.id)))))
            .collect(Collectors.toUnmodifiableList()),
        true);

    freeSpacePerPod.register(
        cacheNodeMetadataStore.listSync().stream()
            .map(
                cacheNodeMetadata ->
                    MultiGauge.Row.of(
                        Tags.of(Tag.of("pod", cacheNodeMetadata.hostname)),
                        cacheNodeIdToFreeSpaceBytes.computeIfAbsent(
                            cacheNodeMetadata.hostname,
                            (_) -> new AtomicLong(calculateFreeSpaceForPod(cacheNodeMetadata.id)))))
            .collect(Collectors.toUnmodifiableList()),
        true);
  }

  private void cacheNodeAssignmentListener() {
    try {
      updateLiveChunksPerPod();
      updateFreeSpacePerPod();
      updatePerPodMetrics();
    } catch (Exception e) {
      LOG.error("Error updating per pod metrics", e);
    }
  }

  private static long getTotalLiveAssignmentSize(
      CacheNodeMetadata cacheNodeMetadata, CacheNodeAssignmentStore store) {
    return store.listSync().stream()
        .filter(
            assignment ->
                Objects.equals(assignment.cacheNodeId, cacheNodeMetadata.id)
                    && assignment.state
                        == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)
        .mapToLong(assignment -> assignment.snapshotSize)
        .sum();
  }

  @Override
  protected synchronized void runOneIteration() {
    if (pendingTask == null || pendingTask.getDelay(TimeUnit.SECONDS) <= 0) {
      pendingTask =
          executorService.schedule(
              this::cacheNodeAssignmentListener,
              managerConfig.getEventAggregationSecs(),
              TimeUnit.SECONDS);
    } else {
      LOG.info(
          "Cluster monitor task already scheduled, will run in {} ms",
          pendingTask.getDelay(TimeUnit.MILLISECONDS));
    }
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting cluster monitor service");
    cacheNodeMetadataStore.addListener(cacheNodeMetadataListener);
    cacheNodeAssignmentStore.addListener(cacheNodeAssignmentListener);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down cluster monitor service");
    cacheNodeMetadataStore.removeListener(cacheNodeMetadataListener);
    cacheNodeAssignmentStore.removeListener(cacheNodeAssignmentListener);
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        managerConfig.getScheduleInitialDelayMins(),
        managerConfig.getClusterMonitorServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }

  private long calculateAssignedBytes(String replicaSet, SnapshotMetadataStore store) {
    return getSnapshotsFromIds(
            snapshotMetadataBySnapshotId(store),
            replicaMetadataStore.listSync().stream()
                .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
                .map(replica -> replica.snapshotId)
                .collect(Collectors.toSet()))
        .stream()
        .mapToLong(snapshot -> snapshot.sizeInBytesOnDisk)
        .sum();
  }

  private long calculateAssignedChunks(String replicaSet, ReplicaMetadataStore store) {
    return store.listSync().stream()
        .filter(replicaMetadata -> replicaMetadata.getReplicaSet().equals(replicaSet))
        .map(replica -> replica.snapshotId)
        .collect(Collectors.toSet())
        .size();
  }

  private int calculateLiveChunks(String cacheNodeId) {
    return cacheNodeAssignmentStore.listSync().stream()
        .filter(
            assignment ->
                Objects.equals(assignment.cacheNodeId, cacheNodeId)
                    && assignment.state
                        == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE)
        .toList()
        .size();
  }

  private long calculateFreeSpaceForPod(String cacheNodeId) {
    CacheNodeMetadata cacheNodeMetadata = cacheNodeMetadataStore.getSync(cacheNodeId);
    return cacheNodeMetadata.nodeCapacityBytes
        - getTotalLiveAssignmentSize(cacheNodeMetadata, cacheNodeAssignmentStore);
  }

  private void updateLiveChunksPerPod() {
    for (CacheNodeMetadata cacheNodeMetadata : cacheNodeMetadataStore.listSync()) {
      if (!cacheNodeIdToLiveChunksPerPod.containsKey(cacheNodeMetadata.id)) {
        cacheNodeIdToLiveChunksPerPod.put(
            cacheNodeMetadata.id, new AtomicInteger(calculateLiveChunks(cacheNodeMetadata.id)));
        return;
      }

      cacheNodeIdToLiveChunksPerPod
          .get(cacheNodeMetadata.id)
          .set(calculateLiveChunks(cacheNodeMetadata.id));
    }
  }

  private void updateFreeSpacePerPod() {
    for (CacheNodeMetadata cacheNodeMetadata : cacheNodeMetadataStore.listSync()) {
      if (!cacheNodeIdToFreeSpaceBytes.containsKey(cacheNodeMetadata.id)) {
        cacheNodeIdToFreeSpaceBytes.put(
            cacheNodeMetadata.id, new AtomicLong(calculateFreeSpaceForPod(cacheNodeMetadata.id)));
        return;
      }

      cacheNodeIdToFreeSpaceBytes
          .get(cacheNodeMetadata.id)
          .set(calculateFreeSpaceForPod(cacheNodeMetadata.id));
    }
  }
}
