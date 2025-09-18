package com.slack.astra.metadata.core;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
  private final ExecutorService executorService;
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
    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("etcd-watcher-" + storeFolder + "-%d")
                .build());

    Watch.Listener watcher = buildWatcher();

    // register watchers for when partitions are added or removed
    ByteSequence storeFolderKey = ByteSequence.from(storeFolder, StandardCharsets.UTF_8);

    // Get the current revision before starting the watch to prevent race conditions
    // We start watching from the next revision to ensure we capture all events
    // that occur during and after watch setup
    WatchOption watchOption;
    try {
      long currentRevision =
          etcdClient
              .getKVClient()
              .get(
                  storeFolderKey,
                  GetOption.builder().withPrefix(storeFolderKey).withKeysOnly(true).build())
              .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS)
              .getHeader()
              .getRevision();

      // Create watch option starting from the current revision + 1
      // This ensures we don't miss events that occur during watch registration
      watchOption =
          WatchOption.builder()
              .withPrefix(storeFolderKey)
              .withRevision(currentRevision - 2)
              .build();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to get current revision for watch setup on store {}", storeFolder, e);
      // Fallback to basic watch without revision
      watchOption = WatchOption.builder().withPrefix(storeFolderKey).build();
    }

    LOG.debug("Adding watch client for folder: {} with prefix option", storeFolderKey);
    watchClient.watch(storeFolderKey, watchOption, watcher);

    // Init stores for each existing partition - similar to how ZookeeperPartitioningMetadataStore
    // works
    etcdClient
        .getKVClient()
        .get(storeFolderKey, GetOption.builder().isPrefix(true).build())
        .thenComposeAsync(
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
                    .get(prefix, GetOption.builder().isPrefix(true).build())
                    .orTimeout(etcdConfig.getOperationsTimeoutMs(), TimeUnit.MILLISECONDS)
                    .thenApplyAsync(
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
        .thenAcceptAsync(
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
   * store map, and handles partition deletions only when explicitly detected.
   */
  private Watch.Listener buildWatcher() {
    return new Watch.Listener() {
      @Override
      public void onNext(WatchResponse watchResponse) {
        if (!executorService.isShutdown()) {
          executorService.submit(
              () -> {
                LOG.debug(
                    "Watch event received: {} events for store {}",
                    watchResponse.getEvents().size(),
                    storeFolder);

                // Check for any creation or deletion of partition directories
                for (WatchEvent event : watchResponse.getEvents()) {
                  String keyStr = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
                  LOG.debug("Processing watch event: {} for key: {}", event.getEventType(), keyStr);

                  // Only process events relevant to our partitions
                  if (keyStr.startsWith(String.format("%s/", storeFolder))) {
                    // This is a change to a partition or its content
                    String remaining = keyStr.substring(String.format("%s/", storeFolder).length());
                    int slashIdx = remaining.indexOf('/');
                    String partition = slashIdx > 0 ? remaining.substring(0, slashIdx) : remaining;
                    LOG.debug("Identified partition '{}' from key: {}", partition, keyStr);

                    // If we have partition filters, skip partitions not in our filter
                    if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
                      continue;
                    }

                    // Handle PUT events - create metadata store if needed
                    if (event.getEventType() == WatchEvent.EventType.PUT) {
                      LOG.debug(
                          "PUT event detected, creating/updating metadata store for partition: {}",
                          partition);
                      getOrCreateMetadataStore(partition);
                    }
                    // Handle DELETE events
                    else if (event.getEventType() == WatchEvent.EventType.DELETE) {
                      LOG.debug("DELETE event detected for key: {}", keyStr);
                      handlePartitionDeletion(partition);
                    }
                  } else {
                    LOG.debug("Ignoring event for key outside our store folder: {}", keyStr);
                  }
                }
              });
        }
      }

      /**
       * Handles deletion of a partition root path by checking if the store is empty and then
       * removing it if appropriate.
       *
       * @param partition The partition identifier that was deleted
       */
      private void handlePartitionDeletion(String partition) {
        if (!metadataStoreMap.containsKey(partition)) {
          LOG.debug("Ignoring deletion of unknown partition: {}", partition);
          return;
        }

        // Get the store and check if it has any cached items
        EtcdMetadataStore<T> store = metadataStoreMap.get(partition);

        try {
          store
              .listAsync()
              .thenAcceptAsync(
                  (items) -> {
                    if (items.isEmpty()) {
                      LOG.info("Closing unused store for deleted partition: {}", partition);
                      metadataStoreMap.remove(partition);
                      store.close();
                    } else {
                      // This could indicate a race condition where we have items in memory
                      // that haven't been persisted to etcd yet, or the deletion was partial
                      LOG.warn(
                          "Detected deletion event on partition {}, but store still has {} cached elements. Keeping store active.",
                          partition,
                          items.size());
                    }
                  });
        } catch (Exception e) {
          // Safeguard against failures that could leave the store in a broken state
          LOG.error("Error checking partition store for deletion: {}", partition, e);
          // If we fail to check the store, it's likely in a bad state, so close it
          // This may be aggressive but prevents resource leaks
          metadataStoreMap.remove(partition);
          store.close();
        }
      }

      @Override
      public void onError(Throwable throwable) {
        LOG.error("Error in etcd watcher for {}", storeFolder, throwable);
      }

      @Override
      public void onCompleted() {
        LOG.warn(
            "Etcd watch completed for {} - object {}", storeFolder, System.identityHashCode(this));
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

  /**
   * Lists all nodes across all partitions synchronously, bypassing the cache. This is intended for
   * testing only and is not recommended for production use due to potentially high resource usage
   * when many partitions exist.
   *
   * @return The list of all nodes directly from etcd, bypassing cache
   * @throws InternalMetadataStoreException if there's an error fetching data from etcd
   */
  public List<T> listSyncUncached() {
    List<T> result = new ArrayList<>();

    // We need to get all the partition keys first
    Set<String> partitions = getPartitionsFromEtcd();

    // For each partition, get uncached data directly
    for (String partition : partitions) {
      try {
        // Get or create the store for this partition
        EtcdMetadataStore<T> store = getOrCreateMetadataStore(partition);

        // Get uncached data from this store
        List<T> partitionItems = store.listSyncUncached();

        // Add to combined result
        result.addAll(partitionItems);
      } catch (Exception e) {
        LOG.warn("Error listing uncached nodes from partition {}: {}", partition, e.getMessage());
      }
    }

    return result;
  }

  /**
   * Gets all available partitions directly from etcd. This method bypasses the cache and makes a
   * direct request to etcd to get partition information.
   *
   * @return A set of partition identifiers
   * @throws InternalMetadataStoreException if there's an error fetching data from etcd
   */
  private Set<String> getPartitionsFromEtcd() {
    try {
      // Get all keys with the store prefix
      ByteSequence prefix =
          ByteSequence.from(String.format("%s/", storeFolder), StandardCharsets.UTF_8);
      io.etcd.jetcd.options.GetOption getOption =
          io.etcd.jetcd.options.GetOption.builder().isPrefix(true).build();

      // Execute the query synchronously
      io.etcd.jetcd.kv.GetResponse getResponse =
          etcdClient
              .getKVClient()
              .get(prefix, getOption)
              .get(etcdConfig.getConnectionTimeoutMs(), TimeUnit.MILLISECONDS);

      // Extract partition names from the keys
      Set<String> partitions = new java.util.HashSet<>();
      for (io.etcd.jetcd.KeyValue kv : getResponse.getKvs()) {
        String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
        if (keyStr.startsWith(String.format("%s/", storeFolder))) {
          String remaining = keyStr.substring(String.format("%s/", storeFolder).length());
          int slashIdx = remaining.indexOf('/');
          String partition = slashIdx > 0 ? remaining.substring(0, slashIdx) : remaining;

          // Only add if it's not empty and matches our filters
          if (!partition.isEmpty()
              && (partitionFilters.isEmpty() || partitionFilters.contains(partition))) {
            partitions.add(partition);
          }
        }
      }

      return partitions;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching partitions from etcd", e);
    }
  }

  /**
   * Gets or creates a metadata store for a partition. If the partition exists in the store map, the
   * existing instance is returned. If not, a new instance is created and stored in the map.
   *
   * @param partition The partition identifier
   * @return The metadata store for the specified partition
   * @throws InternalMetadataStoreException if partition filters are used and the partition is not
   *     in the filter list
   */
  private EtcdMetadataStore<T> getOrCreateMetadataStore(String partition) {
    if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
      LOG.error(
          "Partitioning metadata store attempted to use partition {}, filters restricted to {}",
          partition,
          String.join(",", partitionFilters));
      throw new InternalMetadataStoreException(
          "Partitioning metadata store using filters that does not include provided partition");
    }

    // Create a new store for this partition or return existing one
    return metadataStoreMap.computeIfAbsent(
        partition,
        (p1) -> {
          String path = String.format("%s/%s", storeFolder, p1);
          LOG.info(
              "Creating new etcd metadata store for partition - {}, at path - {}", partition, path);

          EtcdMetadataStore<T> newStore =
              new EtcdMetadataStore<>(
                  path, etcdConfig, true, meterRegistry, serializer, createMode, etcdClient);
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

  /**
   * Checks if this store contains a metadata store for the specified partition.
   *
   * @param partition The partition identifier to check
   * @return true if the partition exists in this store, false otherwise
   */
  public boolean hasPartition(String partition) {
    // First check if partition filter allows this partition
    if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
      return false;
    }
    return metadataStoreMap.containsKey(partition);
  }

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    LOG.info("Adding listener {}", watcher);
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

    // Remove all listeners and close all stores
    new ArrayList<>(listenerMap.values()).forEach(this::removeListener);

    // All watchers are automatically closed when the client is closed
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(DEFAULT_START_STOP_DURATION.toSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for close", e);
    }

    metadataStoreMap.forEach((ignored, store) -> store.close());

    // Clear the maps
    listenerMap.clear();
    metadataStoreMap.clear();
  }
}
