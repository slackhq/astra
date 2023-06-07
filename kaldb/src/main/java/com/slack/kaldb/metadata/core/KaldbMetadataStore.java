package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.cached.ModeledCacheListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * KaldbMetadataStore is a class which provides consistent ZK apis for all the metadata store class.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 */
public class KaldbMetadataStore<T extends KaldbMetadata> implements Closeable {

  private final String storeFolder;

  private final ZPath zPath;

  private final ModeledFramework<T> modeledClient;

  private final CachedModeledFramework<T> cachedModeledFramework;

  private final Map<KaldbMetadataStoreChangeListener, ModeledCacheListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  public KaldbMetadataStore(
      AsyncCuratorFramework curator,
      CreateMode createMode,
      boolean shouldCache,
      ModelSerializer<T> modelSerializer,
      String storeFolder) {

    this.storeFolder = storeFolder;
    this.zPath = ZPath.parseWithIds(String.format("%s/{name}", storeFolder));

    ModelSpec<T> modelSpec =
        ModelSpec.builder(modelSerializer)
            .withPath(zPath)
            .withCreateOptions(Set.of(CreateOption.createParentsIfNeeded))
            .withCreateMode(createMode)
            .build();
    modeledClient = ModeledFramework.wrap(curator, modelSpec);

    if (shouldCache) {
      cachedModeledFramework = modeledClient.cached();
      cachedModeledFramework.start();
    } else {
      cachedModeledFramework = null;
    }
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    // by passing the version 0, this will throw if we attempt to create and it already exists
    return modeledClient.set(metadataNode, 0);
  }

  public void createSync(T metadataNode) {
    try {
      createAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  public CompletionStage<T> getAsync(String path) {
    if (cachedModeledFramework != null) {
      return cachedModeledFramework.withPath(zPath.resolved(path)).readThrough();
    }
    return modeledClient.withPath(zPath.resolved(path)).read();
  }

  public T getSync(String path) {
    try {
      return getAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<Stat> updateAsync(T metadataNode) {
    return modeledClient.update(metadataNode);
  }

  public void updateSync(T metadataNode) {
    try {
      updateAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  public CompletionStage<Void> deleteAsync(String path) {
    return modeledClient.withPath(zPath.resolved(path)).delete();
  }

  public void deleteSync(String path) {
    try {
      deleteAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error deleting node under at path: " + path, e);
    }
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return modeledClient.withPath(zPath.resolved(metadataNode)).delete();
  }

  public void deleteSync(T metadataNode) {
    try {
      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletionStage<List<T>> listAsync() {
    if (cachedModeledFramework == null)
      throw new UnsupportedOperationException("Caching is disabled");

    return cachedModeledFramework.list();
  }

  public List<T> listSync() {
    try {
      return listAsync().toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error getting cached nodes", e);
    }
  }

  private CompletionStage<List<T>> listAsyncUncached() {
    return modeledClient
        .withPath(ZPath.parse(storeFolder))
        .childrenAsZNodes()
        .thenApply(
            (zNodes) -> zNodes.stream().map(znode -> znode.model()).collect(Collectors.toList()));
  }

  /**
   * Listing an uncached directory is very expensive, and not recommended. For a directory
   * containing 100 znodes this results in 100 additional zookeeper queries. For any uses that need
   * listing they should be converted to use a cached implementation.
   */
  @Deprecated
  public List<T> listSyncUncached() {
    // todo - consider combining listSync and getCachedSync, forcing the caller to use the cache
    //  if it exists, as listing sync without a cache is a costly operation
    try {
      return listAsyncUncached()
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing node", e);
    }
  }

  public void addListener(KaldbMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null)
      throw new UnsupportedOperationException("Caching is disabled");

    // this mapping exists because the remove is by reference, and the listener is a different
    // object type
    ModeledCacheListener<T> modeledCacheListener =
        (type, path, stat, model) -> watcher.onMetadataStoreChanged(model);
    cachedModeledFramework.listenable().addListener(modeledCacheListener);
    listenerMap.put(watcher, modeledCacheListener);
  }

  public void removeListener(KaldbMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null)
      throw new UnsupportedOperationException("Caching is disabled");
    cachedModeledFramework.listenable().removeListener(listenerMap.remove(watcher));
  }

  @Override
  public void close() {
    if (cachedModeledFramework != null) {
      cachedModeledFramework.close();
    }
  }
}
