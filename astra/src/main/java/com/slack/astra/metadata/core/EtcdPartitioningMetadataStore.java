package com.slack.astra.metadata.core;

import com.google.common.collect.Sets;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The EtcdPartitioningMetadataStore is a variation of the EtcdMetadataStore that allows for scaling
 * a metadata store that exceeds etcd's ideal key-value count. This is generally encountered when
 * attempting to list keys or adding a listener encounters performance issues with a large number of
 * keys.
 *
 * <p>This partitioning store enables scaling by introducing an intermediate path to the existing
 * metadata stores, such that "foo/bar" becomes "/foo/{partitionIdentifier}/bar". For each
 * partitionIdentifier a separate instance of an EtcdMetadataStore is managed within a map. The
 * partitioning store transparently handles registration and discovery of these partitions, and
 * passes the various metadata store methods directly to the appropriate partition instance.
 *
 * <p>Switching to the partitioning store is not backward compatible with existing non-partitioned
 * metadata. This could potentially be addressed using a manager api to read and copy the metadata
 * to the new store path, using the non-partitioned and partitioning stores respectively.
 */
public class EtcdPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(EtcdPartitioningMetadataStore.class);
  private final Map<String, EtcdMetadataStore<T>> metadataStoreMap = new ConcurrentHashMap<>();
  // Listeners are managed through the listenerMap instead of a list

  protected final String storeFolder;
  private final EtcdCreateMode createMode;
  protected final MetadataSerializer<T> serializer;
  private final List<String> partitionFilters;
  private final AstraConfigs.EtcdConfig etcdConfig;
  private final MeterRegistry meterRegistry;
  private final Client etcdClient;
  private final Watch watchClient;
  private final Map<String, AstraMetadataStoreChangeListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  /**
   * Constructor for EtcdPartitioningMetadataStore with default empty partition filters.
   *
   * @param etcdClient the etcd client
   * @param etcdConfig the etcd configuration
   * @param meterRegistry metrics registry
   * @param createMode whether to create persistent or ephemeral nodes
   * @param serializer serializer for the metadata type
   * @param storeFolder the folder to store data in
   */
  public EtcdPartitioningMetadataStore(
      Client etcdClient,
      AstraConfigs.EtcdConfig etcdConfig,
      MeterRegistry meterRegistry,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      String storeFolder) {
    this(etcdClient, etcdConfig, meterRegistry, createMode, serializer, storeFolder, List.of());
  }

  /**
   * Constructor for EtcdPartitioningMetadataStore.
   *
   * @param etcdClient the etcd client
   * @param etcdConfig the etcd configuration
   * @param meterRegistry metrics registry
   * @param createMode whether to create persistent or ephemeral nodes
   * @param serializer serializer for the metadata type
   * @param storeFolder the folder to store data in
   * @param partitionFilters optional list of partition IDs to filter on
   */
  public EtcdPartitioningMetadataStore(
      Client etcdClient,
      AstraConfigs.EtcdConfig etcdConfig,
      MeterRegistry meterRegistry,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      String storeFolder,
      List<String> partitionFilters) {
    this.etcdClient = etcdClient;
    this.watchClient = etcdClient.getWatchClient();
    this.storeFolder = storeFolder;
    this.createMode = createMode;
    this.serializer = serializer;
    this.partitionFilters = partitionFilters;
    this.etcdConfig = etcdConfig;
    this.meterRegistry = meterRegistry;

    Watch.Listener watcher = buildWatcher();

    // register watchers for when partitions are added or removed
    ByteSequence storeFolderKey = ByteSequence.from(storeFolder, StandardCharsets.UTF_8);
    watchClient.watch(storeFolderKey, watcher);

    // Init stores for each existing partition - similar to how ZookeeperPartitioningMetadataStore
    // works
    etcdClient
        .getKVClient()
        .get(storeFolderKey)
        .thenCompose(
            getResponse -> {
              if (getResponse.getKvs().isEmpty()) {
                // This is similar to KeeperException.NoNodeException in ZK
                // The storeFolder does not yet exist in etcd
                return CompletableFuture.completedFuture(List.<String>of());
              } else {
                // Get all direct children (partitions) of the storeFolder
                ByteSequence prefix =
                    ByteSequence.from(String.format("%s/", storeFolder), StandardCharsets.UTF_8);
                return etcdClient
                    .getKVClient()
                    .get(prefix, io.etcd.jetcd.options.GetOption.builder().isPrefix(true).build())
                    .thenApply(
                        childResponse -> {
                          List<String> children = new ArrayList<>();
                          childResponse
                              .getKvs()
                              .forEach(
                                  kv -> {
                                    String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
                                    if (keyStr.startsWith(String.format("%s/", storeFolder))) {
                                      // Extract partition name (the first segment after
                                      // storeFolder)
                                      String remaining =
                                          keyStr.substring(
                                              String.format("%s/", storeFolder).length());
                                      int slashIdx = remaining.indexOf('/');
                                      if (slashIdx > 0) {
                                        String partition = remaining.substring(0, slashIdx);
                                        if (!children.contains(partition)) {
                                          children.add(partition);
                                        }
                                      } else if (!remaining.isEmpty()) {
                                        // This is a partition with no children yet
                                        children.add(remaining);
                                      }
                                    }
                                  });
                          return children;
                        });
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
   * Builds a watcher that is responsible for updating our internal metadata stores to match what is
   * stored in etcd.
   *
   * <p>This method creates stores internally when they are detected in etcd storing them to the
   * store map, and removes stores that are in the map that no longer exist in etcd.
   */
  private Watch.Listener buildWatcher() {
    return new Watch.Listener() {
      @Override
      public void onNext(WatchResponse watchResponse) {
        // Check for any creation or deletion of partition directories
        for (WatchEvent event : watchResponse.getEvents()) {
          String keyStr = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);

          // Only process events relevant to our partitions
          if (keyStr.startsWith(String.format("%s/", storeFolder))) {
            // This is a change to a partition or its content
            String remaining = keyStr.substring(String.format("%s/", storeFolder).length());
            int slashIdx = remaining.indexOf('/');
            String partition = slashIdx > 0 ? remaining.substring(0, slashIdx) : remaining;

            // If we have partition filters, skip partitions not in our filter
            if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
              continue;
            }

            // Handle partition creation by creating metadata store if needed
            if (event.getEventType() == WatchEvent.EventType.PUT) {
              getOrCreateMetadataStore(partition);
            }
          }
        }

        // Refresh the partition list periodically to remove any defunct stores
        etcdClient
            .getKVClient()
            .get(
                ByteSequence.from(String.format("%s/", storeFolder), StandardCharsets.UTF_8),
                io.etcd.jetcd.options.GetOption.builder().isPrefix(true).build())
            .thenAcceptAsync(
                getResponse -> {
                  // Extract unique partition names from the key paths
                  Set<String> existingPartitions =
                      getResponse.getKvs().stream()
                          .map(
                              kv -> {
                                String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
                                if (keyStr.startsWith(String.format("%s/", storeFolder))) {
                                  String remaining =
                                      keyStr.substring(String.format("%s/", storeFolder).length());
                                  int slashIdx = remaining.indexOf('/');
                                  return slashIdx > 0
                                      ? remaining.substring(0, slashIdx)
                                      : remaining;
                                }
                                return null;
                              })
                          .filter(Objects::nonNull)
                          .collect(Collectors.toSet());

                  // Create metadata stores for any newly discovered partitions
                  if (partitionFilters.isEmpty()) {
                    existingPartitions.forEach(
                        EtcdPartitioningMetadataStore.this::getOrCreateMetadataStore);
                  } else {
                    existingPartitions.stream()
                        .filter(partitionFilters::contains)
                        .forEach(EtcdPartitioningMetadataStore.this::getOrCreateMetadataStore);
                  }

                  // Remove metadata stores that exist in memory but no longer exist in etcd
                  Set<String> partitionsToRemove =
                      Sets.difference(metadataStoreMap.keySet(), existingPartitions);
                  partitionsToRemove.forEach(
                      partition -> {
                        int cachedSize = metadataStoreMap.get(partition).listSync().size();
                        if (cachedSize == 0) {
                          LOG.debug("Closing unused store for partition - {}", partition);
                          EtcdMetadataStore<T> store = metadataStoreMap.remove(partition);
                          store.close();
                        } else {
                          // This extra check is to prevent a race condition where multiple items
                          // are being quickly added. This can result in a scenario where the
                          // watcher is triggered, but we haven't persisted the items to etcd yet.
                          // When this happens it results in a premature close of the local cache.
                          LOG.warn(
                              "Skipping metadata store close for partition {}, still has {} cached elements",
                              partition,
                              cachedSize);
                        }
                      });
                });
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.error("Error in etcd watcher for {}", storeFolder, throwable);
      }

      @Override
      public void onCompleted() {
        LOG.debug("Etcd watch completed for {}", storeFolder);
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
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
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
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in. Use of this
   * should be avoided if possible, preferring the getAsync.
   *
   * @see EtcdPartitioningMetadataStore#getAsync(String, String)
   */
  public CompletionStage<T> findAsync(String path) {
    return getOrCreateMetadataStore(findPartition(path)).getAsync(path);
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in. Use of this
   * should be avoided if possible, preferring the getSync.
   *
   * @see EtcdPartitioningMetadataStore#getSync(String, String)
   */
  public T findSync(String path) {
    try {
      return findAsync(path)
          .toCompletableFuture()
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
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
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
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
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
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
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletableFuture<List<T>> listAsync() {
    List<CompletableFuture<List<T>>> completionStages = new ArrayList<>();
    for (Map.Entry<String, EtcdMetadataStore<T>> metadataStoreEntry : metadataStoreMap.entrySet()) {
      completionStages.add(metadataStoreEntry.getValue().listAsync().toCompletableFuture());
    }

    return CompletableFuture.allOf(completionStages.toArray(new CompletableFuture[0]))
        .thenApply(
            _ ->
                completionStages.stream()
                    .map(f -> f.toCompletableFuture().join())
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
  }

  public List<T> listSync() {
    try {
      return listAsync()
          .toCompletableFuture()
          .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing nodes", e);
    }
  }

  private EtcdMetadataStore<T> getOrCreateMetadataStore(String partition) {
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

          EtcdMetadataStore<T> newStore =
              new EtcdMetadataStore<>(
                  path,
                  etcdConfig,
                  true,
                  meterRegistry,
                  serializer,
                  createMode,
                  etcdConfig.getEphemeralNodeTtlSeconds() > 0
                      ? etcdConfig.getEphemeralNodeTtlSeconds()
                      : EtcdCreateMode.DEFAULT_EPHEMERAL_TTL_SECONDS);
          listenerMap.forEach((_, listener) -> newStore.addListener(listener));

          return newStore;
        });
  }

  /**
   * Attempts to locate the partition containing the sub-path. If no partition is found this will
   * throw an InternalMetadataStoreException. Use of this method should be carefully considered due
   * to performance implications of potentially invoking N hasSync calls.
   */
  private String findPartition(String path) {
    for (Map.Entry<String, EtcdMetadataStore<T>> metadataStoreEntry : metadataStoreMap.entrySet()) {
      // We may consider switching this to execute in parallel in the future. Even though this would
      // be faster, it would put quite a bit more load on etcd, and some of it unnecessary
      if (metadataStoreEntry.getValue().hasSync(path)) {
        return metadataStoreEntry.getKey();
      }
    }
    throw new InternalMetadataStoreException("Error finding node at path " + path);
  }

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // add this watcher to the map for new stores to add
    listenerMap.put(System.identityHashCode(watcher) + "", watcher);
    // add this watcher to existing stores
    metadataStoreMap.forEach((_, store) -> store.addListener(watcher));
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    listenerMap.remove(System.identityHashCode(watcher) + "");
    metadataStoreMap.forEach((_, store) -> store.removeListener(watcher));
  }

  /**
   * Waits for the cache to be initialized.
   *
   * <p>This method ensures that all partitioned metadata stores have their caches initialized, if
   * caching is enabled. This is important for ensuring that data is available when queried,
   * especially after a fresh start.
   */
  public void awaitCacheInitialized() {
    LOG.info("Initializing caches for {} partitioned stores", metadataStoreMap.size());

    // Initialize cache for each metadata store
    metadataStoreMap.forEach(
        (partition, store) -> {
          LOG.debug("Initializing cache for partition {}", partition);
          store.awaitCacheInitialized();
        });

    LOG.info("Completed cache initialization for {} partitioned stores", metadataStoreMap.size());
  }

  @Override
  public void close() throws IOException {
    LOG.info(
        "Closing the partitioning metadata store, {} listeners to remove, {} partitions to close",
        listenerMap.size(),
        metadataStoreMap.size());

    // All watchers are automatically closed when the client is closed

    // Remove all listeners and close all stores
    new ArrayList<>(listenerMap.values()).forEach(this::removeListener);
    metadataStoreMap.forEach((ignored, store) -> store.close());

    // Clear the maps
    listenerMap.clear();
    metadataStoreMap.clear();
  }
}
