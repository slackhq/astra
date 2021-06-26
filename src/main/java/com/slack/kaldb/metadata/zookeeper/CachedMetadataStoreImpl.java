package com.slack.kaldb.metadata.zookeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;

public class CachedMetadataStoreImpl<T extends KaldbMetadata> implements CachedMetadataStore<T> {
  private final StandardListenerManager<CachedMetadataStoreListener> listenerContainer =
      StandardListenerManager.standard();
  private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
  private final CuratorCacheBridge cache;

  private final ConcurrentMap<String, T> instances = Maps.newConcurrentMap();

  // TODO: is ensureContainers needed?
  private final EnsureContainers ensureContainers;
  private final CountDownLatch initializedLatch = new CountDownLatch(1);
  private final MetadataSerializer<T> metadataSerde;

  private enum State {
    LATENT,
    STARTED,
    STOPPED
  }

  private static ExecutorService convertThreadFactory(ThreadFactory threadFactory) {
    Preconditions.checkNotNull(threadFactory, "threadFactory cannot be null");
    return Executors.newSingleThreadExecutor(threadFactory);
  }

  CachedMetadataStoreImpl(
      String path,
      MetadataSerializer<T> metadataSerde,
      CuratorFramework curator,
      ThreadFactory threadFactory) {
    this(path, metadataSerde, curator, convertThreadFactory(threadFactory));
  }

  CachedMetadataStoreImpl(
      String path,
      MetadataSerializer<T> metadataSerde,
      CuratorFramework curator,
      ExecutorService executorService) {
    Preconditions.checkNotNull(path, "name cannot be null");
    Preconditions.checkNotNull(metadataSerde, "metadata serializer cannot be null");
    Preconditions.checkNotNull(curator, "curator framework cannot be null");
    this.metadataSerde = metadataSerde;

    // Create a curator cache but don't store any data in it since CacheStorage only allows
    // storing data as a byte array. Instead use the curator cache implementation for
    // managing persistent watchers and other admin tasks. Instead add a listener which would
    // cache the data locally as a POJO using a serializer. In future, this also allows us to store
    // the data in a custom data structure other than a hash table.
    cache =
        CuratorCache.bridgeBuilder(curator, path)
            .withExecutorService(executorService)
            .withDataNotCached()
            .build();
    CuratorCacheListener listener =
        CuratorCacheListener.builder()
            .forPathChildrenCache(path, curator, this)
            .forInitialized(this::initialized)
            .build();
    cache.listenable().addListener(listener);
    ensureContainers = new EnsureContainers(curator, path);
  }

  @Override
  public List<T> getInstances() {
    return Lists.newArrayList(instances.values());
  }

  @Override
  public Optional<T> get(String path) {
    return Optional.ofNullable(instances.get(path));
  }

  // TODO: Need these latches?
  @VisibleForTesting volatile CountDownLatch debugStartLatch = null;
  volatile CountDownLatch debugStartWaitLatch = null;

  @Override
  public void start() throws Exception {
    startImmediate().await();
  }

  // @Override
  public CountDownLatch startImmediate() throws Exception {
    Preconditions.checkState(
        state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

    ensureContainers.ensure();
    cache.start();
    if (debugStartLatch != null) {
      initializedLatch.await();
      debugStartLatch.countDown();
      debugStartLatch = null;
    }
    if (debugStartWaitLatch != null) {
      debugStartWaitLatch.await();
      debugStartWaitLatch = null;
    }

    return initializedLatch;
  }

  @Override
  public void close() {
    Preconditions.checkState(
        state.compareAndSet(State.STARTED, State.STOPPED),
        "Already closed or has not been started");
    listenerContainer.clear();
    CloseableUtils.closeQuietly(cache);
  }

  @Override
  public void addListener(CachedMetadataStoreListener listener) {
    listenerContainer.addListener(listener);
  }

  @Override
  public void addListener(CachedMetadataStoreListener listener, Executor executor) {
    listenerContainer.addListener(listener, executor);
  }

  @Override
  public void removeListener(CachedMetadataStoreListener listener) {
    listenerContainer.removeListener(listener);
  }

  @Override
  public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
    boolean notifyListeners = false;
    switch (event.getType()) {
      case CHILD_ADDED:
      case CHILD_UPDATED:
        {
          addInstance(event.getData());
          notifyListeners = true;
          break;
        }

      case CHILD_REMOVED:
        {
          instances.remove(instanceIdFromData(event.getData()));
          notifyListeners = true;
          break;
        }
    }

    if (notifyListeners && (initializedLatch.getCount() == 0)) {
      listenerContainer.forEach(CachedMetadataStoreListener::cacheChanged);
    }
  }

  private String instanceIdFromData(ChildData childData) {
    return ZKPaths.getNodeFromPath(childData.getPath());
  }

  private void addInstance(ChildData childData) {
    try {
      String instanceId = instanceIdFromData(childData);
      // TODO: Use byte arrays here.
      T serviceInstance = metadataSerde.fromJsonStr(new String(childData.getData()));
      instances.put(instanceId, serviceInstance);
    } catch (Exception e) {
      throw new InternalMetadataStoreException(
          "Error adding node at path " + childData.getPath(), e);
    }
  }

  @VisibleForTesting
  public boolean isStarted() {
    return state.get().equals(State.STARTED);
  }

  private void initialized() {
    initializedLatch.countDown();
  }
}
