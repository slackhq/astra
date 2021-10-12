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

public abstract class EphemeralMutableMetadataStore<T extends KaldbMetadata>
    extends UpdatableCacheableMetadataStore<T> {

  private final boolean updatable;

  public EphemeralMutableMetadataStore(
      boolean shouldCache,
      boolean updatable,
      String storeFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {
    super(shouldCache, storeFolder, metadataStore, metadataSerializer, logger);
    this.updatable = updatable;
  }

  // TODO: Return ListenableFuture<bool>?
  public ListenableFuture<?> create(T metadataNode) {
    String path = getPath(metadataNode.name);
    try {
      return metadataStore.createEphemeralNode(path, metadataSerializer.toJsonStr(metadataNode));
    } catch (InvalidProtocolBufferException e) {
      String msg = String.format("Error serializing node %s at path %s", metadataNode, path);
      logger.error(msg, e);
      return Futures.immediateFailedFuture(e);
    }
  }

  public Object createSync(T metadataNode)
      throws ExecutionException, InterruptedException, TimeoutException {
    return create(metadataNode).get(KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
  }

  public ListenableFuture<?> update(T metadataNode) {
    if (!updatable) {
      throw new UnsupportedOperationException("Can't update store at path " + storeFolder);
    }

    return super.update(metadataNode);
  }
}
