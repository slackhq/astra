package com.slack.kaldb.clusterManager;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.cache.CacheSlotMetadataStore;
import com.slack.kaldb.metadata.dataset.DatasetMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryNodeMetadataStore;
import com.slack.kaldb.metadata.recovery.RecoveryTaskMetadataStore;
import com.slack.kaldb.metadata.replica.ReplicaMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.proto.metadata.Metadata;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.List;

/**
 * ClusterMonitor runs as a service in the manager component and monitors the state of the KalDB
 * cluster.
 */
public class ClusterMonitorService extends AbstractIdleService {
  public ClusterMonitorService(
      ReplicaMetadataStore replicaMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore,
      RecoveryTaskMetadataStore recoveryTaskMetadataStore,
      RecoveryNodeMetadataStore recoveryNodeMetadataStore,
      CacheSlotMetadataStore cacheSlotMetadataStore,
      DatasetMetadataStore datasetMetadataStore,
      MeterRegistry meterRegistry) {

    meterRegistry.gauge(
        "cached_replica_nodes_size", replicaMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_snapshots_size", snapshotMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_recovery_tasks_size", recoveryTaskMetadataStore, store -> store.listSync().size());
    meterRegistry.gauge(
        "cached_recovery_nodes_size", recoveryNodeMetadataStore, store -> store.listSync().size());

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

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {}
}
