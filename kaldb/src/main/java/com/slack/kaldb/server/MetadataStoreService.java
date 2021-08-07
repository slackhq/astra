package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.search.SearchMetadataStore;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import com.slack.kaldb.metadata.zookeeper.ZookeeperMetadataStoreImpl;
import io.micrometer.core.instrument.MeterRegistry;

public class MetadataStoreService extends AbstractIdleService {
  private final MeterRegistry meterRegistry;
  private MetadataStore metadataStore;

  public static String SNAPSHOT_METADATA_PATH = "/snapshots";
  public static String SEARCH_METADATA_PATH = "/search";
  private SnapshotMetadataStore snapshotStore;
  private SearchMetadataStore searchStore;

  MetadataStoreService(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  public SnapshotMetadataStore getSnapshotStore(boolean shouldCache) throws Exception {
    if (!isRunning()) {
      throw new IllegalStateException("Can't create snapshot store before staring.");
    }
    if (snapshotStore == null) {
      snapshotStore = new SnapshotMetadataStore(metadataStore, SNAPSHOT_METADATA_PATH, shouldCache);
    }
    return snapshotStore;
  }

  public SearchMetadataStore getSearchStore(boolean shouldCache) throws Exception {
    if (!isRunning()) {
      throw new IllegalStateException("Can't create search store before staring.");
    }
    if (searchStore == null) {
      searchStore = new SearchMetadataStore(metadataStore, SEARCH_METADATA_PATH, shouldCache);
    }
    return searchStore;
  }

  @Override
  protected void startUp() throws Exception {
    metadataStore = ZookeeperMetadataStoreImpl.fromConfig(meterRegistry);
  }

  @Override
  protected void shutDown() throws Exception {
    metadataStore.close();
  }
}
