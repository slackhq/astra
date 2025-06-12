package com.slack.astra.metadata.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.util.RuntimeHalterImpl;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
 * ZookeeperMetadataStore is a class which provides consistent ZK apis for all the metadata store
 * class.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 */
public class ZookeeperMetadataStore<T extends AstraMetadata> implements Closeable {
  protected final String storeFolder;

  private final ZPath zPath;

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  protected final ModeledFramework<T> modeledClient;

  private final CachedModeledFramework<T> cachedModeledFramework;

  private final Map<AstraMetadataStoreChangeListener<T>, ModeledCacheListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  private final ExecutorService cacheInitializedService;
  private final ModeledCacheListener<T> initializedListener = getCacheInitializedListener();

  private final AstraConfigs.ZookeeperConfig zkConfig;

  private final MeterRegistry meterRegistry;

  private final String ASTRA_ZK_CREATE_CALL = "astra_zk_create_call";
  private final String ASTRA_ZK_HAS_CALL = "astra_zk_has_call";
  private final String ASTRA_ZK_DELETE_CALL = "astra_zk_delete_call";
  private final String ASTRA_ZK_LIST_CALL = "astra_zk_list_call";
  private final String ASTRA_ZK_GET_CALL = "astra_zk_get_call";
  private final String ASTRA_ZK_UPDATE_CALL = "astra_zk_update_call";
  private final String ASTRA_ZK_ADDED_LISTENER = "astra_zk_added_listener";
  private final String ASTRA_ZK_REMOVED_LISTENER = "astra_zk_removed_listener";
  private final String ASTRA_ZK_CACHE_INIT_HANDLER_FIRED = "astra_zk_cache_init_handler_fired";

  private final Counter createCall;
  private final Counter hasCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;
  private final Counter cacheInitializationHandlerFired;

  public ZookeeperMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.ZookeeperConfig zkConfig,
      CreateMode createMode,
      boolean shouldCache,
      ModelSerializer<T> modelSerializer,
      String storeFolder,
      MeterRegistry meterRegistry) {

    this.storeFolder = storeFolder;
    this.zPath = ZPath.parseWithIds(String.format("%s/{name}", storeFolder));
    this.zkConfig = zkConfig;
    this.meterRegistry = meterRegistry;
    String store = "/" + storeFolder.split("/")[1];

    this.createCall = this.meterRegistry.counter(ASTRA_ZK_CREATE_CALL, "store", store);
    this.deleteCall = this.meterRegistry.counter(ASTRA_ZK_DELETE_CALL, "store", store);
    this.listCall = this.meterRegistry.counter(ASTRA_ZK_LIST_CALL, "store", store);
    this.getCall = this.meterRegistry.counter(ASTRA_ZK_GET_CALL, "store", store);
    this.hasCall = this.meterRegistry.counter(ASTRA_ZK_HAS_CALL, "store", store);
    this.updateCall = this.meterRegistry.counter(ASTRA_ZK_UPDATE_CALL, "store", store);
    this.addedListener = this.meterRegistry.counter(ASTRA_ZK_ADDED_LISTENER, "store", store);
    this.removedListener = this.meterRegistry.counter(ASTRA_ZK_REMOVED_LISTENER, "store", store);
    this.cacheInitializationHandlerFired =
        this.meterRegistry.counter(ASTRA_ZK_CACHE_INIT_HANDLER_FIRED, "store", store);

    ModelSpec<T> modelSpec =
        ModelSpec.builder(modelSerializer)
            .withPath(zPath)
            .withCreateOptions(
                Set.of(CreateOption.createParentsIfNeeded, CreateOption.createParentsAsContainers))
            .withCreateMode(createMode)
            .build();
    modeledClient = ModeledFramework.wrap(curator, modelSpec);

    if (shouldCache) {
      cacheInitializedService =
          Executors.newSingleThreadExecutor(
              new ThreadFactoryBuilder().setNameFormat("cache-initialized-service-%d").build());
      cachedModeledFramework = modeledClient.cached();
      cachedModeledFramework.listenable().addListener(initializedListener, cacheInitializedService);
      cachedModeledFramework.start();
    } else {
      cachedModeledFramework = null;
      cacheInitializedService = null;
    }
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    // by passing the version 0, this will throw if we attempt to create and it already exists
    return modeledClient.set(metadataNode, 0);
  }

  public void createSync(T metadataNode) {
    try {
      this.createCall.increment();

      createAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
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
      this.getCall.increment();

      return getAsync(path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<Boolean> hasAsync(String path) {
    if (cachedModeledFramework != null) {
      awaitCacheInitialized();
      return cachedModeledFramework
          .withPath(zPath.resolved(path))
          .checkExists()
          .thenApply(stat -> stat != null);
    }
    return modeledClient
        .withPath(zPath.resolved(path))
        .checkExists()
        .thenApply(stat -> stat != null);
  }

  public boolean hasSync(String path) {
    try {
      this.hasCall.increment();

      return hasAsync(path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<String> updateAsync(T metadataNode) {
    return modeledClient
        .update(metadataNode)
        .thenApply(stat -> Integer.toString(stat != null ? stat.getVersion() : -1));
  }

  public void updateSync(T metadataNode) {
    try {

      this.updateCall.increment();

      updateAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  public CompletionStage<Void> deleteAsync(String path) {
    return modeledClient.withPath(zPath.resolved(path)).delete();
  }

  public void deleteSync(String path) {
    try {
      this.deleteCall.increment();

      deleteAsync(path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error deleting node under at path: " + path, e);
    }
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return modeledClient.withPath(zPath.resolved(metadataNode)).delete();
  }

  public void deleteSync(T metadataNode) {
    try {
      this.deleteCall.increment();

      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletionStage<List<T>> listAsync() {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    awaitCacheInitialized();
    return cachedModeledFramework.list();
  }

  public List<T> listSync() {
    try {
      this.listCall.increment();

      return listAsync()
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error getting cached nodes", e);
    }
  }

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    this.addedListener.increment();

    // this mapping exists because the remove is by reference, and the listener is a different
    // object type
    ModeledCacheListener<T> modeledCacheListener =
        (type, path, stat, model) -> {
          // We do not expect the model to ever be null for an event on a metadata node
          if (model != null) {
            watcher.onMetadataStoreChanged(model);
          }
        };
    cachedModeledFramework.listenable().addListener(modeledCacheListener);
    listenerMap.put(watcher, modeledCacheListener);
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    if (cachedModeledFramework == null) {
      throw new UnsupportedOperationException("Caching is disabled");
    }
    this.removedListener.increment();
    cachedModeledFramework.listenable().removeListener(listenerMap.remove(watcher));
  }

  public void awaitCacheInitialized() {
    try {
      if (!cacheInitialized.await(zkConfig.getZkCacheInitTimeoutMs(), TimeUnit.MILLISECONDS)) {
        // in the event we deadlock, go ahead and time this out at 30s and restart the pod
        new RuntimeHalterImpl()
            .handleFatal(
                new TimeoutException("Timed out waiting for Zookeeper cache to initialize"));
      }
    } catch (InterruptedException e) {
      new RuntimeHalterImpl().handleFatal(e);
    }
  }

  private ModeledCacheListener<T> getCacheInitializedListener() {
    return new ModeledCacheListener<T>() {
      @Override
      public void accept(Type type, ZPath path, Stat stat, T model) {
        // no-op
      }

      @Override
      public void initialized() {
        ModeledCacheListener.super.initialized();
        cacheInitialized.countDown();
        if (cacheInitializationHandlerFired != null) {
          cacheInitializationHandlerFired.increment();
        }

        // after it's initialized, we no longer need the listener or executor
        if (cachedModeledFramework != null) {
          cachedModeledFramework.listenable().removeListener(initializedListener);
        }
        if (cacheInitializedService != null) {
          cacheInitializedService.shutdown();
        }
      }
    };
  }

  @Override
  public void close() {
    if (cachedModeledFramework != null) {
      listenerMap.forEach(
          (_, tModeledCacheListener) ->
              cachedModeledFramework.listenable().removeListener(tModeledCacheListener));
      cachedModeledFramework.close();
    }
  }
}
