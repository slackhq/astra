package com.slack.kaldb.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.slack.kaldb.metadata.snapshot.SnapshotMetadataStore;
import java.util.concurrent.TimeUnit;

public class SnapshotMetadataStoreService extends AbstractIdleService {
  public static String SNAPSHOT_METADATA_PATH = "/snapshots";
  private final MetadataStoreService metadataStoreService;
  private final boolean shouldCache;
  private SnapshotMetadataStore snapshotStore;

  public SnapshotMetadataStoreService(
      MetadataStoreService metadataStoreService, boolean shouldCache) {
    this.metadataStoreService = metadataStoreService;
    this.shouldCache = shouldCache;
  }

  @Override
  protected void startUp() throws Exception {
    metadataStoreService.awaitRunning(15, TimeUnit.SECONDS);
    snapshotStore =
        new SnapshotMetadataStore(
            metadataStoreService.getMetadataStore(), SNAPSHOT_METADATA_PATH, shouldCache);
  }

  @Override
  protected void shutDown() throws Exception {
    snapshotStore.close();
  }
}
