package com.slack.kaldb.metadata.zookeeper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.metadata.core.KaldbMetadata;
import com.slack.kaldb.metadata.core.MetadataSerializer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.listen.StandardListenerManager;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheBridge;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ZookeeperCachedMetadataStoreImpl uses a curator path cache to cache a node and all the child
 * nodes under it. In addition, this class also accepts a metadata serializer/de-serializer objects,
 * so we only serialize/de-serialize the objects only once.
 *
 * <p>This class also caches nested nodes. The key is the path of the node relative to the cache
 * root and the node value is the serialized metadata object.
 *
 * <p>NOTE: Unlike a regular file system, Since a directory is also a node in ZK, the directory node
 * should also have a metadata object in it's value.
 *
 * <p>Currently, the cache is not cleared when a ZK server starts and stops which could be a bug.
 * But it's fine for now, since we may terminate and restart the process when ZK is unavailable.
 *
 * <p>TODO: Cache is refreshed when a ZK server stops/restarts.
 */
public class ZookeeperCachedMetadataStoreImpl<T extends KaldbMetadata>
    implements ZookeeperCachedMetadataStore<T> {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCachedMetadataStoreImpl.class);

  public static final String CACHE_ERROR_COUNTER = "cache.error";

  private final StandardListenerManager<ZookeeperCachedMetadataStoreListener> listenerContainer =
      StandardListenerManager.standard();
  private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);
  private final CuratorCacheBridge cache;

  private final ConcurrentMap<String, T> instances = Maps.newConcurrentMap();

  private final EnsureContainers ensureContainers;
  private final CountDownLatch initializedLatch = new CountDownLatch(1);
  private final MetadataSerializer<T> metadataSerde;
  private final String pathPrefix;
  private final Counter errorCounter;

  private enum State {
    LATENT,
    STARTED,
    STOPPED
  }

  ZookeeperCachedMetadataStoreImpl(
      String path,
      MetadataSerializer<T> metadataSerde,
      CuratorFramework curator,
      MeterRegistry meterRegistry) {
    Preconditions.checkNotNull(path, "name cannot be null");
    Preconditions.checkNotNull(metadataSerde, "metadata serializer cannot be null");
    Preconditions.checkNotNull(curator, "curator framework cannot be null");
    this.metadataSerde = metadataSerde;
    this.pathPrefix = path.endsWith(ZKPaths.PATH_SEPARATOR) ? path : path + ZKPaths.PATH_SEPARATOR;
    /*
     * Create a curator cache but don't store any data in it since CacheStorage only allows storing
     * data as a byte array. Instead use the curator cache implementation for managing persistent
     * watchers and other admin tasks. Instead add a listener which would cache the data locally as
     * a POJO using a serializer. In future, this also allows us to store the data in a custom data
     * structure other than a hash table. Currently, if we lose a ZK connection the cache will grow
     * stale but this class is oblivious of it.
     *
     * NOTE: We need to pass in an executor service to the bridge builder if used with Zookeeper
     * versions older than 3.6. So, this code will may not be as performant when used with
     * Zookeeper 3.5 or less.
     *
     * <p>TODO: Add a mechanism to detect a stale cache indicate that a cache is stale.
     */
    cache = CuratorCache.bridgeBuilder(curator, path).withDataNotCached().build();
    // All changes to child nodes also fire a notification on root node. So, we handle all
    // callbacks on the parent node.
    CuratorCacheListener listener =
        CuratorCacheListener.builder()
            .forCreates(this::nodeCreated)
            .forChanges(this::nodeChanged)
            .forDeletes(this::nodeDeleted)
            .forInitialized(this::cachedNodeAndChildren)
            .build();
    cache.listenable().addListener(listener);
    ensureContainers = new EnsureContainers(curator, path);
    errorCounter = meterRegistry.counter(CACHE_ERROR_COUNTER);
  }

  private void nodeCreated(ChildData newData) {
    addInstance(newData);
    maybeNotify();
  }

  private void nodeDeleted(ChildData childData) {
    instances.remove(instanceIdFromData(childData));
    maybeNotify();
  }

  private void nodeChanged(ChildData oldData, ChildData currentData) {
    addInstance(currentData);
    maybeNotify();
  }

  @Override
  public List<T> getInstances() {
    return Lists.newArrayList(instances.values());
  }

  @Override
  public Optional<T> get(String path) {
    return Optional.ofNullable(instances.get(path));
  }

  @Override
  public void start() throws Exception {
    startImmediate().await();
    LOG.info("Started caching nodes at path {}.", pathPrefix);
  }

  private CountDownLatch startImmediate() throws Exception {
    Preconditions.checkState(
        state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

    ensureContainers.ensure();
    cache.start();

    return initializedLatch;
  }

  @Override
  public void close() {
    Preconditions.checkState(
        state.compareAndSet(State.STARTED, State.STOPPED),
        "Already closed or has not been started");
    listenerContainer.clear();
    CloseableUtils.closeQuietly(cache);
    LOG.info("Closing cache for path: {}", pathPrefix);
  }

  @Override
  public void addListener(ZookeeperCachedMetadataStoreListener listener) {
    listenerContainer.addListener(listener);
  }

  @Override
  public void addListener(ZookeeperCachedMetadataStoreListener listener, Executor executor) {
    listenerContainer.addListener(listener, executor);
  }

  @Override
  public void removeListener(ZookeeperCachedMetadataStoreListener listener) {
    listenerContainer.removeListener(listener);
  }

  private static String removeStart(final String str, final String remove) {
    if (str.isEmpty() || remove.isEmpty()) {
      return str;
    }
    if (str.startsWith(remove)) {
      return str.substring(remove.length());
    }
    return str;
  }

  // Use the path name relative to cache root as the instanceId to better support nested nodes.
  private String instanceIdFromData(ChildData childData) {
    return removeStart(childData.getPath(), pathPrefix);
  }

  private void addInstance(ChildData childData) {
    String instanceId = "";
    try {
      instanceId = instanceIdFromData(childData);
      T serviceInstance = metadataSerde.fromJsonStr(new String(childData.getData()));
      instances.put(instanceId, serviceInstance);
    } catch (InvalidProtocolBufferException e) {
      // If we are unable to add the updated value to the cache, invalidate the key so cache is
      // consistent even though it's incomplete. If the incomplete cache becomes an issue,
      // log a fatal.
      LOG.error("Invalidating key from cache: {}", instanceId);
      invalidateKey(instanceId);
      errorCounter.increment();
      throw new InternalMetadataStoreException(
          "Error adding node at path " + childData.getPath(), e);
    }
  }

  /**
   * If we are unable to get the value of the key, invalidate the cache by deleting the key for now.
   *
   * <p>TODO: Since deleting the key leaves the cache in an incomplete state, consider making the
   * value Optional, to better clarify the intent.
   */
  private void invalidateKey(String key) {
    if (!key.isEmpty()) instances.remove(key);
  }

  @VisibleForTesting
  public boolean isStarted() {
    return state.get().equals(State.STARTED);
  }

  @VisibleForTesting
  public boolean isStopped() {
    return state.get().equals(State.STOPPED);
  }

  private void maybeNotify() {
    // TODO: getCount when used this way could return a stale value and we may have a race
    //  condition here. Update this code to not use getCount.
    if (initializedLatch.getCount() == 0) {
      listenerContainer.forEach(
          listener -> {
            try {
              listener.cacheChanged();
            } catch (Exception e) {
              // If a listener throws an exception log it and ignore it.
              errorCounter.increment();
              LOG.error("Caught an exception notifying listener " + listener, e);
            }
          });
      LOG.debug("Notified {} listeners on node change at {}", listenerContainer.size(), pathPrefix);
    }
  }

  /** This function is called after both the path and it's children are cached. */
  public void cachedNodeAndChildren() {
    LOG.debug("initialized");
    initializedLatch.countDown();
  }
}
