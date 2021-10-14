package com.slack.kaldb.metadata.core;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;

/** A metadata store that supports updates. */
public abstract class UpdatableCacheableMetadataStore<T extends KaldbMetadata>
    extends CacheableMetadataStore<T> {

  public UpdatableCacheableMetadataStore(
      boolean shouldCache,
      String snapshotStoreFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {
    super(shouldCache, snapshotStoreFolder, metadataStore, metadataSerializer, logger);
  }

  public ListenableFuture<?> update(T metadataNode) {
    String path = getPath(metadataNode.name);
    try {
      return metadataStore.put(path, metadataSerializer.toJsonStr(metadataNode));
    } catch (InvalidProtocolBufferException e) {
      String msg = String.format("Error serializing node %s at path %s", metadataNode, path);
      logger.error(msg, e);
      return Futures.immediateFailedFuture(e);
    }
  }

  public void updateSync(T metadataNode) {
    try {
      update(metadataNode).get(KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new MetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }
}
