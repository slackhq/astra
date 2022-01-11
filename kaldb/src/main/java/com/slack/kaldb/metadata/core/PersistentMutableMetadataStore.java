package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.config.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.metadata.zookeeper.InternalMetadataStoreException;
import com.slack.kaldb.metadata.zookeeper.MetadataStore;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;

public abstract class PersistentMutableMetadataStore<T extends KaldbMetadata>
    extends UpdatableCacheableMetadataStore<T> {

  private final boolean updatable;

  public PersistentMutableMetadataStore(
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
      return metadataStore.create(path, metadataSerializer.toJsonStr(metadataNode), true);
    } catch (InvalidProtocolBufferException e) {
      String msg =
          String.format(Locale.ROOT, "Error serializing node %s at path %s", metadataNode, path);
      logger.error(msg, e);
      return Futures.immediateFailedFuture(e);
    }
  }

  public void createSync(T metadataNode) {
    try {
      create(metadataNode).get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | TimeoutException | InterruptedException e) {
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  public ListenableFuture<?> update(T metadataNode) {
    if (!updatable) {
      throw new UnsupportedOperationException("Can't update store at path " + storeFolder);
    }

    return super.update(metadataNode);
  }

  public void updateSync(T metadataNode) {
    try {
      update(metadataNode).get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error updating metadataNode " + metadataNode.toString(), e);
    }
  }

  public ListenableFuture<?> delete(T metadataNode) {
    String path = getPath(metadataNode.name);
    return metadataStore.delete(path);
  }

  public void deleteSync(T metadataNode) {
    try {
      delete(metadataNode).get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | TimeoutException | InterruptedException e) {
      throw new InternalMetadataStoreException(
          "Failed to delete node: " + metadataNode.toString(), e);
    }
  }
}
