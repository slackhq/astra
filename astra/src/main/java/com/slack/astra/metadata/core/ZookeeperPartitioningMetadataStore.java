package com.slack.astra.metadata.core;

import com.google.common.collect.Sets;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ZookeeperPartitioningMetadataStore is a variation of the ZookeeperMetadataStore that allows
 * for scaling a metadata store that exceeds Zookeepers ideal child node count. This is generally
 * encountered when attempting to list children or adding a listener encounters an issue exceeding
 * the jute.maxbuffer.
 *
 * <p>This partitioning store enables scaling by introducing an intermediate path to the existing
 * metadata stores, such that "foo/bar" becomes "/foo/{partitionIdentifier}/bar". For each
 * partitionIdentifier a separate instance of a ZookeeperMetadataStore is managed within a map. The
 * partitioning store transparently handles registration and discovery of these partitions, and
 * passes the various metadata store methods directly to the appropriate partition instance.
 *
 * <p>Switching to the partitioning store is not backward compatible with existing non-partitioned
 * metadata. This could potentially be addressed using a manager api to read and copy the metadata
 * to the new store path, using the non-partitioned and partitioning stores respectively.
 */
public class ZookeeperPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperPartitioningMetadataStore.class);
  private final Map<String, ZookeeperMetadataStore<T>> metadataStoreMap = new ConcurrentHashMap<>();
  private final List<AstraMetadataStoreChangeListener<T>> listeners = new CopyOnWriteArrayList<>();

  protected final AsyncCuratorFramework curator;
  protected final String storeFolder;
  private final CreateMode createMode;
  protected final ModelSerializer<T> modelSerializer;
  private final Watcher watcher;
  private final List<String> partitionFilters;
  private final AstraConfigs.ZookeeperConfig zkConfig;
  private final MeterRegistry meterRegistry;

  public ZookeeperPartitioningMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      CreateMode createMode,
      ModelSerializer<T> modelSerializer,
      String storeFolder) {
    this(curator, zkConfig, meterRegistry, createMode, modelSerializer, storeFolder, List.of());
  }

  public ZookeeperPartitioningMetadataStore(
      AsyncCuratorFramework curator,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      CreateMode createMode,
      ModelSerializer<T> modelSerializer,
      String storeFolder,
      List<String> partitionFilters) {
    this.curator = curator;
    this.storeFolder = storeFolder;
    this.createMode = createMode;
    this.modelSerializer = modelSerializer;
    this.watcher = buildWatcher();
    this.partitionFilters = partitionFilters;
    this.zkConfig = zkConfig;
    this.meterRegistry = meterRegistry;

    // register watchers for when partitions are added or removed
    curator
        .addWatch()
        .withMode(AddWatchMode.PERSISTENT) // intentionally NOT recursive
        .usingWatcher(watcher)
        .forPath(storeFolder);

    // init stores for each existing partition
    curator
        .getChildren()
        .forPath(storeFolder)
        .exceptionallyCompose(
            (throwable) -> {
              if (throwable instanceof KeeperException.NoNodeException) {
                // This is thrown because the storeFolder does not yet exist in ZK
                // This isn't a problem, as the node will be created once the first operation is
                // attempted
                return CompletableFuture.completedFuture(List.of());
              } else {
                return CompletableFuture.failedFuture(throwable);
              }
            })
        .thenAccept(
            (children) -> {
              if (partitionFilters.isEmpty()) {
                children.forEach(this::getOrCreateMetadataStore);
              } else {
                children.stream()
                    .filter(partitionFilters::contains)
                    .forEach(this::getOrCreateMetadataStore);
              }
            })
        .toCompletableFuture()
        // wait for all the stores to be initialized prior to exiting the constructor
        .join();

    if (partitionFilters.isEmpty()) {
      LOG.info(
          "The metadata store for folder '{}' was initialized with {} partitions",
          storeFolder,
          metadataStoreMap.size());
    } else {
      LOG.info(
          "The metadata store for folder '{}' was initialized with {} partitions (using partition filters: {})",
          storeFolder,
          metadataStoreMap.size(),
          String.join(",", partitionFilters));
    }
  }

  /**
   * Builds a watcher that is responsible for updating our internal metadata stores to match that is
   * stored in ZK. As we create parent nodes as containers, we do not need to be responsible for
   * deleting these intermediate nodes as this will be handled by ZK.
   *
   * <p>This method creates stores internally when they are detected in ZK storing them to the store
   * map, and removes stores that are in the map that no longer exist in ZK.
   *
   * @see ZookeeperMetadataStore#ZookeeperMetadataStore(AsyncCuratorFramework,
   *     AstraConfigs.ZookeeperConfig, CreateMode, boolean, ModelSerializer, String,
   *     io.micrometer.core.instrument.MeterRegistry)
   */
  private Watcher buildWatcher() {
    return event -> {
      if (event.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
        curator
            .getChildren()
            .forPath(storeFolder)
            .thenAcceptAsync(
                (partitions) -> {
                  if (partitionFilters.isEmpty()) {
                    // create internal stores foreach partition that do not already exist
                    partitions.forEach(this::getOrCreateMetadataStore);
                  } else {
                    partitions.stream()
                        .filter(partitionFilters::contains)
                        .forEach(this::getOrCreateMetadataStore);
                  }

                  // remove metadata stores that exist in memory but no longer exist on ZK
                  Set<String> partitionsToRemove =
                      Sets.difference(metadataStoreMap.keySet(), Sets.newHashSet(partitions));
                  partitionsToRemove.forEach(
                      partition -> {
                        int cachedSize = metadataStoreMap.get(partition).listSync().size();
                        if (cachedSize == 0) {
                          LOG.debug("Closing unused store for partition - {}", partition);
                          ZookeeperMetadataStore<T> store = metadataStoreMap.remove(partition);
                          store.close();
                        } else {
                          // This extra check is to prevent a race condition where multiple items
                          // are being quickly added. This can result in a scenario where the
                          // watcher is triggered, but we haven't persisted the items to ZK yet.
                          // When this happens it results in a premature close of the local cache.
                          LOG.warn(
                              "Skipping metadata store close for partition {}, still has {} cached elements",
                              partition,
                              cachedSize);
                        }
                      });
                });
      }
    };
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).createAsync(metadataNode);
  }

  public void createSync(T metadataNode) {
    try {
      createAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  public CompletionStage<T> getAsync(String partition, String path) {
    return getOrCreateMetadataStore(partition).getAsync(path);
  }

  public T getSync(String partition, String path) {
    try {
      return getAsync(partition, path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in. Use of this
   * should be avoided if possible, preferring the getAsync.
   *
   * @see ZookeeperPartitioningMetadataStore#getAsync(String, String)
   */
  public CompletionStage<T> findAsync(String path) {
    return getOrCreateMetadataStore(findPartition(path)).getAsync(path);
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in. Use of this
   * should be avoided if possible, preferring the getSync.
   *
   * @see ZookeeperPartitioningMetadataStore#getSync(String, String)
   */
  public T findSync(String path) {
    try {
      return findAsync(path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  /**
   * Checks if a node exists asynchronously in a specific partition.
   *
   * @param partition the partition to check in
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise
   */
  public CompletionStage<Boolean> hasAsync(String partition, String path) {
    return getOrCreateMetadataStore(partition).hasAsync(path);
  }

  /**
   * Checks if a node exists synchronously in a specific partition.
   *
   * @param partition the partition to check in
   * @param path the path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String partition, String path) {
    try {
      return hasAsync(partition, path)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error checking if node exists at path " + path, e);
    }
  }

  public CompletionStage<String> updateAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).updateAsync(metadataNode);
  }

  public void updateSync(T metadataNode) {
    try {
      updateAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).deleteAsync(metadataNode);
  }

  public void deleteSync(T metadataNode) {
    try {
      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletableFuture<List<T>> listAsync() {
    List<CompletableFuture<List<T>>> completionStages = new ArrayList<>();
    for (Map.Entry<String, ZookeeperMetadataStore<T>> metadataStoreEntry :
        metadataStoreMap.entrySet()) {
      completionStages.add(metadataStoreEntry.getValue().listAsync().toCompletableFuture());
    }

    return CompletableFuture.allOf(completionStages.toArray(new CompletableFuture[0]))
        .thenApply(
            (unused) ->
                completionStages.stream()
                    .map(f -> f.toCompletableFuture().join())
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
  }

  public List<T> listSync() {
    try {
      return listAsync()
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing nodes", e);
    }
  }

  private ZookeeperMetadataStore<T> getOrCreateMetadataStore(String partition) {
    if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
      LOG.error(
          "Partitioning metadata store attempted to use partition {}, filters restricted to {}",
          partition,
          String.join(",", partitionFilters));
      throw new InternalMetadataStoreException(
          "Partitioning metadata store using filters that does not include provided partition");
    }

    return metadataStoreMap.computeIfAbsent(
        partition,
        (p1) -> {
          String path = String.format("%s/%s", storeFolder, p1);
          LOG.debug(
              "Creating new metadata store for partition - {}, at path - {}", partition, path);

          ZookeeperMetadataStore<T> newStore =
              new ZookeeperMetadataStore<>(
                  curator, zkConfig, createMode, true, modelSerializer, path, meterRegistry);
          listeners.forEach(newStore::addListener);

          return newStore;
        });
  }

  /**
   * Attempts to locate the partition containing the sub-path. If no partition is found this will
   * throw an InternalMetadataStoreException. Use of this method should be carefully considered due
   * to performance implications of potentially invoking N hasSync calls.
   */
  private String findPartition(String path) {
    for (Map.Entry<String, ZookeeperMetadataStore<T>> metadataStoreEntry :
        metadataStoreMap.entrySet()) {
      // We may consider switching this to execute in parallel in the future. Even though this would
      // be faster, it would put quite a bit more load on ZK, and some of it unnecessary
      if (metadataStoreEntry.getValue().hasSync(path)) {
        return metadataStoreEntry.getKey();
      }
    }
    throw new InternalMetadataStoreException("Error finding node at path " + path);
  }

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // add this watcher to the list for new stores to add
    listeners.add(watcher);
    // add this watcher to existing stores
    metadataStoreMap.forEach((_, store) -> store.addListener(watcher));
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    listeners.remove(watcher);
    metadataStoreMap.forEach((_, store) -> store.removeListener(watcher));
  }

  @Override
  public void close() throws IOException {
    LOG.info(
        "Closing the partitioning metadata store, {} listeners to remove, {} partitions to close",
        listeners.size(),
        metadataStoreMap.size());

    // only remove the watcher we created, since this curator instance is a singleton
    curator.removeWatches().removing(watcher);
    listeners.forEach(this::removeListener);
    metadataStoreMap.forEach((partition, store) -> store.close());
  }
}
