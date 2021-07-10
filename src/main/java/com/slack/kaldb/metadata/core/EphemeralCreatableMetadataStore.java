package com.slack.kaldb.metadata.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import org.slf4j.Logger;

public abstract class EphemeralCreatableMetadataStore<T extends KaldbMetadata>
    extends KaldbMetadataStore<T> {

  public EphemeralCreatableMetadataStore(
      MetadataStore metadataStore,
      String snapshotStoreFolder,
      MetadataSerializer<T> metadataSerializer,
      Logger logger) {
    super(metadataStore, snapshotStoreFolder, metadataSerializer, logger);
  }

  // TODO: Return ListenableFuture<bool>?
  public ListenableFuture<?> create(T metadataNode) {
    String path = getPath(metadataNode.name);
    try {
      return metadataStore.createEphemeralNode(path, metadataSerializer.toJsonStr(metadataNode));
    } catch (InvalidProtocolBufferException e) {
      String msg = String.format("Error serializing node %s at path %s", metadataNode, path);
      logger.error(msg, e);
      // TODO: Create a failed listentable future with exception?
      return SettableFuture.create();
    }
  }
}
