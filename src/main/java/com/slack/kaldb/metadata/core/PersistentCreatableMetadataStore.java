package com.slack.kaldb.metadata.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;

public abstract class PersistentCreatableMetadataStore<T extends KaldbMetadata>
    extends CachableMetadataStore<T> {

  public PersistentCreatableMetadataStore(
      boolean shouldCache,
      String storeFolder,
      MetadataStore metadataStore,
      MetadataSerializer<T> metadataSerializer,
      Logger logger)
      throws Exception {
    super(shouldCache, storeFolder, metadataStore, metadataSerializer, logger);
  }

  // TODO: Return ListenableFuture<bool>?
  public ListenableFuture<?> create(T metadataNode) {
    String path = getPath(metadataNode.name);
    try {
      return metadataStore.create(path, metadataSerializer.toJsonStr(metadataNode), true);
    } catch (InvalidProtocolBufferException e) {
      String msg = String.format("Error serializing node %s at path %s", metadataNode, path);
      logger.error(msg, e);
      // TODO: Create a failed listentable future with exception?
      return SettableFuture.create();
    }
  }
}
