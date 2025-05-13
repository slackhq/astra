package com.slack.astra.clusterManager;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.slack.astra.metadata.cache.CacheNodeAssignment;
import com.slack.astra.metadata.cache.CacheNodeAssignmentStore;
import com.slack.astra.metadata.cache.CacheNodeMetadata;
import com.slack.astra.metadata.cache.CacheNodeMetadataStore;
import com.slack.astra.metadata.search.SearchMetadata;
import com.slack.astra.metadata.search.SearchMetadataStore;
import com.slack.astra.metadata.snapshot.SnapshotMetadataStore;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.proto.metadata.Metadata;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CacheNodeSearchabilityService extends AbstractScheduledService {

  protected static final Logger LOG = LoggerFactory.getLogger(CacheNodeSearchabilityService.class);

  public static final String UNSEARCHABLE_NODES = "unsearchable_nodes";

  private final CacheNodeMetadataStore cacheNodeMetadataStore;
  private final CacheNodeAssignmentStore cacheNodeAssignmentStore;
  private final SearchMetadataStore searchMetadataStore;
  private final SnapshotMetadataStore snapshotMetadataStore;
  private final AstraConfigs.ManagerConfig managerConfig;
  private final Counter numberOfUnsearchableNodes;

  public CacheNodeSearchabilityService(
      MeterRegistry meterRegistry,
      CacheNodeMetadataStore cacheNodeMetadataStore,
      AstraConfigs.ManagerConfig managerConfig,
      CacheNodeAssignmentStore cacheNodeAssignmentStore,
      SearchMetadataStore searchMetadataStore,
      SnapshotMetadataStore snapshotMetadataStore) {
    this.cacheNodeMetadataStore = cacheNodeMetadataStore;
    this.managerConfig = managerConfig;
    this.cacheNodeAssignmentStore = cacheNodeAssignmentStore;
    this.searchMetadataStore = searchMetadataStore;
    this.snapshotMetadataStore = snapshotMetadataStore;
    this.numberOfUnsearchableNodes = meterRegistry.counter(UNSEARCHABLE_NODES);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Closing cache node searchability service");
    if (cacheNodeMetadataStore != null) {
      cacheNodeMetadataStore.close();
    }
    if (cacheNodeAssignmentStore != null) {
      cacheNodeAssignmentStore.close();
    }
    if (searchMetadataStore != null) {
      searchMetadataStore.close();
    }
    if (snapshotMetadataStore != null) {
      snapshotMetadataStore.close();
    }
    LOG.info("Closed cache node assignment service");
  }

  @Override
  protected void runOneIteration() throws Exception {
    List<CacheNodeMetadata> unsearchableCacheNodes =
        cacheNodeMetadataStore.listSync().stream()
            .filter(cacheNodeMetadata -> !cacheNodeMetadata.searchable)
            .toList();

    for (CacheNodeMetadata cacheNodeMetadata : unsearchableCacheNodes) {
      List<CacheNodeAssignment> loadingCacheAssignments = new ArrayList<>();
      List<CacheNodeAssignment> liveCacheAssignments = new ArrayList<>();
      cacheNodeAssignmentStore
          .listSync()
          .forEach(
              cacheNodeAssignment -> {
                if (cacheNodeMetadata.id.equals(cacheNodeAssignment.cacheNodeId)) {
                  if (cacheNodeAssignment.state
                      == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LOADING) {
                    loadingCacheAssignments.add(cacheNodeAssignment);
                  } else if (cacheNodeAssignment.state
                      == Metadata.CacheNodeAssignment.CacheNodeAssignmentState.LIVE) {
                    liveCacheAssignments.add(cacheNodeAssignment);
                  }
                }
              });

      // This node is only searchable if it doesn't have more than 1 assignment that is
      // loading AND it has at least one assignment that is live
      boolean searchable = loadingCacheAssignments.size() <= 1 && !liveCacheAssignments.isEmpty();

      if (searchable) {
        LOG.info("Marking cache node {} as searchable", cacheNodeMetadata.hostname);
        cacheNodeMetadata.searchable = true;
        cacheNodeMetadataStore.updateSync(cacheNodeMetadata);

        List<SearchMetadata> cacheNodesSearchMetadata =
            searchMetadataStore.listSync().stream()
                .filter(searchMetadata -> searchMetadata.url.contains(cacheNodeMetadata.hostname))
                .toList();

        for (SearchMetadata searchMetadata : cacheNodesSearchMetadata) {
          if (!searchMetadata.isSearchable()) {
            searchMetadataStore.updateSearchability(searchMetadata, true);
          }
        }
      } else {
        LOG.info(
            "Cache node {} is NOT searchable. It has {} loading cache assignments and {} live cache assignments",
            cacheNodeMetadata.id,
            loadingCacheAssignments.size(),
            liveCacheAssignments.size());
        numberOfUnsearchableNodes.increment();
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(
        this.managerConfig.getScheduleInitialDelayMins(),
        this.managerConfig.getCacheNodeSearchabilityServiceConfig().getSchedulePeriodMins(),
        TimeUnit.MINUTES);
  }
}
