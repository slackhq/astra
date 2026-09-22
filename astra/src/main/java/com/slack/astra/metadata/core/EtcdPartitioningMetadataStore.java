package com.slack.astra.metadata.core;

import static com.slack.astra.server.AstraConfig.DEFAULT_START_STOP_DURATION;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.slack.astra.proto.config.AstraConfigs;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

  protected final String storeFolder;
  private final String storeFolderPrefix;
  private final EtcdCreateMode createMode;
  protected final MetadataSerializer<T> serializer;
  private final List<String> partitionFilters;
  private final boolean shouldCache;
  private final AstraConfigs.EtcdConfig etcdConfig;
  private final MeterRegistry meterRegistry;
  private final Client etcdClient;
  private final Watch watchClient;
  private final Map<String, AstraMetadataStoreChangeListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  private final ScheduledExecutorService watchRetryExecutor;
  private final long listPageSize;
  private final long listPageTimeoutMs;
  private final long operationsTimeoutMs;

  /**
   * The open partition-discovery watch, swapped with {@code getAndSet} rather than under a lock
   * because closing a watcher calls into jetcd while it holds its own watcher lock.
   */
  private final AtomicReference<EtcdMetadataStore.WatchHandle> partitionWatcher =
      new AtomicReference<>();

  private volatile boolean closing = false;

  /** Pacing state for the single partition-discovery watch this store opens. */
  private final EtcdWatchRetry watchRetry;

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
    this(
        etcdClient,
        etcdConfig,
        meterRegistry,
        createMode,
        serializer,
        storeFolder,
        partitionFilters,
        true);
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
   * @param shouldCache when false, skip cluster-wide partition discovery and watching and do not
   *     cache per-partition data. Nodes that only register and remove their own entries (e.g.
   *     cache, index, recovery) never read the global set, so they can avoid mirroring every
   *     partition.
   */
  public EtcdPartitioningMetadataStore(
      Client etcdClient,
      AstraConfigs.EtcdConfig etcdConfig,
      MeterRegistry meterRegistry,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      String storeFolder,
      List<String> partitionFilters,
      boolean shouldCache) {
    this.etcdClient = etcdClient;
    this.watchClient = etcdClient.getWatchClient();
    this.storeFolder = storeFolder;
    this.storeFolderPrefix = storeFolder + "/";
    this.createMode = createMode;
    this.serializer = serializer;
    this.partitionFilters = partitionFilters;
    this.shouldCache = shouldCache;
    this.etcdConfig = etcdConfig;
    this.meterRegistry = meterRegistry;
    this.listPageSize = etcdConfig.getListPageSize();
    // The connection timeout sizes establishing a socket; a paginated range read needs longer.
    this.operationsTimeoutMs =
        EtcdMetadataStore.positiveOrDefault(
            etcdConfig.getOperationsTimeoutMs(), etcdConfig.getConnectionTimeoutMs());
    this.listPageTimeoutMs =
        EtcdMetadataStore.positiveOrDefault(etcdConfig.getListPageTimeoutMs(), operationsTimeoutMs);

    this.executorService =
        Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("etcd-watcher-" + storeFolder + "-%d")
                .build());
    this.watchRetryExecutor =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              t.setName("etcd-partition-watch-retry-" + storeFolder);
              return t;
            });

    this.watchRetry =
        new EtcdWatchRetry(
            etcdConfig,
            meterRegistry,
            EtcdMetadataStore.storeTag(storeFolder),
            watchRetryExecutor,
            () -> closing,
            (revision, unused) -> startPartitionWatch(revision),
            "partition watch for store " + storeFolder,
            "partition discovery is stale",
            System::currentTimeMillis);

    if (shouldCache) {
      startPartitionWatch(EtcdWatchRetry.REVISION_LATEST);

      // Initialize a store for each existing partition before exiting the constructor.
      // getPartitionsFromEtcd applies any partition filters and returns empty if the folder is
      // absent.
      getPartitionsFromEtcd().forEach(this::getOrCreateMetadataStore);
    }

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
   * Starts (or restarts) the partition-discovery watch, re-establishing it with unbounded backoff
   * on failure as {@link EtcdMetadataStore} does per partition.
   *
   * @param startRevision {@link EtcdWatchRetry#REVISION_LATEST} to fetch current revision, {@link
   *     EtcdWatchRetry#REVISION_RESYNC} to re-scan etcd first
   */
  private void startPartitionWatch(long startRevision) {
    if (closing) {
      return;
    }

    ByteSequence storeFolderKey = ByteSequence.from(storeFolder, StandardCharsets.UTF_8);

    long watchFromRevision;
    try {
      if (startRevision == EtcdWatchRetry.REVISION_RESYNC) {
        watchFromRevision = resyncPartitionsFromEtcd() + 1;
      } else if (startRevision == EtcdWatchRetry.REVISION_LATEST) {
        watchFromRevision =
            EtcdRangePaginator.currentRevision(
                    etcdClient.getKVClient(), storeFolderKey, operationsTimeoutMs)
                + 1;
      } else {
        // A reconnect: EtcdWatchRetry already stored a ready-to-use revision, so a second +1
        // would skip whatever happened at startRevision itself.
        watchFromRevision = startRevision;
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      if (closing || Thread.currentThread().isInterrupted()) {
        LOG.warn("Ignoring partition revision fetch error during shutdown for {}", storeFolder);
        return;
      }
      // No usable revision was learned, so the retry has nothing to resume from and must resync.
      watchRetry.requireResync();
      watchRetry.onFailure(
          e, "Failed to get current revision for partition watch on store " + storeFolder);
      return;
    }

    watchRetry.openedAtRevision(watchFromRevision);
    WatchOption watchOption =
        WatchOption.builder().withPrefix(storeFolderKey).withRevision(watchFromRevision).build();

    // Ensures a dead stream schedules exactly one retry, even if both onError and onCompleted fire.
    AtomicBoolean disposed = new AtomicBoolean(false);

    closePartitionWatcher();

    LOG.debug(
        "Starting partition watch for folder {} at revision {}", storeFolder, watchFromRevision);
    Watch.Watcher watcher =
        watchClient.watch(
            storeFolderKey,
            watchOption,
            watchResponse -> {
              if (watchResponse.getHeader() != null) {
                watchRetry.reachedRevision(watchResponse.getHeader().getRevision() + 1);
              }
              if (!executorService.isShutdown()) {
                // submit wraps the task in a Future that swallows any throwable it escapes with.
                executorService.execute(() -> processWatchEvents(watchResponse));
              }
            },
            throwable -> {
              if (!watchRetry.claimDisposal(disposed, "error")) {
                return;
              }
              watchRetry.onFailure(throwable, "Partition watch failed for store " + storeFolder);
            },
            () -> {
              if (!watchRetry.claimDisposal(disposed, "completion")) {
                return;
              }
              watchRetry.onFailure(
                  new IllegalStateException("etcd closed the partition watch stream"),
                  "Partition watch completed unexpectedly for store " + storeFolder);
            });

    partitionWatcher.set(new EtcdMetadataStore.WatchHandle(watcher, disposed));
    if (closing) {
      // close() ran while we were registering, so this handle is ours to dispose.
      closePartitionWatcher();
      return;
    }
    watchRetry.onEstablished();
  }

  /**
   * Disposes the open watcher, if any, claiming its latch before closing so the completion jetcd
   * synthesizes on close cannot schedule a retry.
   */
  private void closePartitionWatcher() {
    EtcdMetadataStore.WatchHandle old = partitionWatcher.getAndSet(null);
    if (old != null) {
      old.dispose("partition watcher");
    }
  }

  /**
   * Processes watch events for partition discovery. Creates stores for new partitions and handles
   * partition deletions.
   */
  private void processWatchEvents(WatchResponse watchResponse) {
    if (closing) {
      LOG.debug("Ignoring watch events during shutdown for store {}", storeFolder);
      return;
    }

    LOG.debug(
        "Watch event received: {} events for store {}",
        watchResponse.getEvents().size(),
        storeFolder);

    try {
      for (WatchEvent event : watchResponse.getEvents()) {
        String keyStr = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
        LOG.debug("Processing watch event: {} for key: {}", event.getEventType(), keyStr);

        String partition = extractPartition(keyStr);
        if (partition == null) {
          LOG.debug("Ignoring event for key outside our store folder: {}", keyStr);
          continue;
        }

        LOG.debug("Identified partition '{}' from key: {}", partition, keyStr);

        if (!partitionFilters.isEmpty() && !partitionFilters.contains(partition)) {
          continue;
        }

        if (event.getEventType() == WatchEvent.EventType.PUT) {
          LOG.debug(
              "PUT event detected, creating/updating metadata store for partition: {}", partition);
          getOrCreateMetadataStore(partition);
        } else if (event.getEventType() == WatchEvent.EventType.DELETE) {
          LOG.debug("DELETE event detected for key: {}", keyStr);
          handlePartitionDeletion(partition);
        }
      }
    } catch (InternalMetadataStoreException e) {
      // close() can flip `closing` mid-batch, making the remaining events expected failures; any
      // other cause is a real bug and still kills this thread loudly.
      if (!closing) {
        throw e;
      }
      LOG.debug("Abandoning remaining watch events during shutdown for store {}", storeFolder);
    }
  }

  /**
   * Extracts the partition identifier from a full etcd key under this store's folder. Returns null
   * if the key does not start with the store folder prefix or is empty after stripping.
   */
  private String extractPartition(String keyStr) {
    if (!keyStr.startsWith(storeFolderPrefix)) {
      return null;
    }
    String remaining = keyStr.substring(storeFolderPrefix.length());
    if (remaining.isEmpty()) {
      return null;
    }
    int slashIdx = remaining.indexOf('/');
    return slashIdx > 0 ? remaining.substring(0, slashIdx) : remaining;
  }

  private void handlePartitionDeletion(String partition) {
    EtcdMetadataStore<T> store = metadataStoreMap.get(partition);
    if (store == null) {
      LOG.debug("Ignoring deletion of unknown partition: {}", partition);
      return;
    }

    try {
      store
          .sizeAsync()
          .thenAcceptAsync(
              (remaining) -> {
                if (remaining == 0) {
                  LOG.info("Closing unused store for deleted partition: {}", partition);
                  closePartitionStore(partition, store);
                } else {
                  // The watch fires one DELETE event per key under the store folder, so this
                  // branch is hit on every single-node deletion from a partition that still has
                  // other members (the common case for busy partitions like LIVE). That is normal
                  // - only the emptied-partition case above is noteworthy - so keep this at DEBUG
                  // to avoid flooding logs on high-churn partitions.
                  LOG.debug(
                      "Deletion event on partition {}, but store still has {} cached elements. Keeping store active.",
                      partition,
                      remaining);
                }
              });
    } catch (Exception e) {
      LOG.error("Error checking partition store for deletion: {}", partition, e);
      closePartitionStore(partition, store);
    }
  }

  /** Removes a partition's store from the map and closes it. */
  private void closePartitionStore(String partition, EtcdMetadataStore<T> store) {
    metadataStoreMap.remove(partition, store);
    store.close();
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

  public EtcdMetadataStore<T> createPartitionSync(String partitionId) {
    return getOrCreateMetadataStore(partitionId);
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
      return readPartitions().partitions();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching partitions from etcd", e);
    }
  }

  /** The partitions that exist in etcd, with the revision the read observed them at. */
  private record PartitionsAt(Set<String> partitions, long revision) {}

  /**
   * Reads which partitions exist, probing only the filtered ones when filters are set since a
   * filtered store may not use anything a full folder scan would discover.
   */
  private PartitionsAt readPartitions()
      throws InterruptedException, ExecutionException, TimeoutException {
    if (!partitionFilters.isEmpty()) {
      return probeFilteredPartitions();
    }
    EtcdRangePaginator.PaginatedRange range =
        EtcdRangePaginator.listRange(
            etcdClient.getKVClient(),
            ByteSequence.from(storeFolderPrefix, StandardCharsets.UTF_8),
            true,
            operationsTimeoutMs,
            listPageTimeoutMs,
            listPageSize);
    return new PartitionsAt(extractPartitions(range.keyValues()), range.revision());
  }

  /**
   * One bounded keys-only probe per filtered partition, run concurrently, so discovery costs
   * O(filters) tiny reads instead of a scan of every key in the folder.
   */
  private PartitionsAt probeFilteredPartitions()
      throws InterruptedException, ExecutionException, TimeoutException {
    List<CompletableFuture<EtcdRangePaginator.PaginatedRange>> probes =
        partitionFilters.stream()
            .map(
                partition ->
                    EtcdRangePaginator.firstKeyAsync(
                        etcdClient.getKVClient(),
                        ByteSequence.from(storeFolderPrefix + partition, StandardCharsets.UTF_8),
                        operationsTimeoutMs))
            .toList();

    // Each probe carries its own timeout, so getting them in turn is already bounded.
    List<EtcdRangePaginator.PaginatedRange> ranges = new ArrayList<>();
    for (CompletableFuture<EtcdRangePaginator.PaginatedRange> probe : probes) {
      ranges.add(probe.get());
    }

    // extractPartitions re-checks the filter, since a prefix probe for "p1" also matches "p10".
    return new PartitionsAt(
        extractPartitions(ranges.stream().flatMap(r -> r.keyValues().stream()).toList()),
        // The oldest revision any probe saw, so a watch resuming from it replays rather than skips
        // events the later probes already reflect.
        ranges.stream().mapToLong(EtcdRangePaginator.PaginatedRange::revision).min().orElseThrow());
  }

  private Set<String> extractPartitions(List<io.etcd.jetcd.KeyValue> keyValues) {
    Set<String> partitions = new HashSet<>();
    for (io.etcd.jetcd.KeyValue kv : keyValues) {
      String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
      String partition = extractPartition(keyStr);
      if (partition != null
          && (partitionFilters.isEmpty() || partitionFilters.contains(partition))) {
        partitions.add(partition);
      }
    }
    return partitions;
  }

  /**
   * Re-scans etcd for partitions and ensures each one has a metadata store. Used on watch recovery
   * (compaction or gap) to discover partitions created while the watch was down.
   *
   * @return the etcd revision from the list response, suitable for starting a watch from revision+1
   */
  private long resyncPartitionsFromEtcd()
      throws InterruptedException, ExecutionException, TimeoutException {
    PartitionsAt read = readPartitions();
    long listRevision = read.revision();

    Set<String> discoveredPartitions = read.partitions();
    discoveredPartitions.forEach(this::getOrCreateMetadataStore);

    // Additive by design: an absent partition may just be initializing, so removal comes from
    // DELETE events only.
    LOG.info(
        "Partition resync for store {} complete: {} partitions from etcd at revision {}",
        storeFolder,
        discoveredPartitions.size(),
        listRevision);

    return listRevision;
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

    // Reject new partition stores once shutdown has begun. Watch events arriving on background
    // threads can otherwise create a store after close() has swept metadataStoreMap, orphaning its
    // lease and keepalive thread for the lifetime of the etcd client.
    if (closing) {
      throw new InternalMetadataStoreException(
          "Partitioning metadata store is closing, refusing to create partition " + partition);
    }

    AtomicBoolean created = new AtomicBoolean(false);

    EtcdMetadataStore<T> store =
        metadataStoreMap.computeIfAbsent(
            partition,
            (p1) -> {
              String path = String.format("%s/%s", storeFolder, p1);
              LOG.debug(
                  "Creating new etcd metadata store for partition - {}, at path - {}",
                  partition,
                  path);

              EtcdMetadataStore<T> newStore =
                  new EtcdMetadataStore<>(
                      path,
                      etcdConfig,
                      shouldCache,
                      meterRegistry,
                      serializer,
                      createMode,
                      etcdClient);
              listenerMap.forEach((_, listener) -> newStore.addListener(listener));

              created.set(true);
              return newStore;
            });

    if (created.get()) {
      List<T> cachedItems = store.listSync();
      if (!cachedItems.isEmpty()) {
        LOG.info(
            "Notifying {} listeners of {} pre-existing items in new partition {}",
            listenerMap.size(),
            cachedItems.size(),
            partition);
        for (T item : cachedItems) {
          for (AstraMetadataStoreChangeListener<T> listener : listenerMap.values()) {
            try {
              listener.onMetadataStoreChanged(item);
            } catch (Exception e) {
              LOG.warn("Listener notification failed for partition {}", partition, e);
            }
          }
        }
      }
    }

    return store;
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

    closing = true;
    closePartitionWatcher();

    // Remove all listeners and close all stores
    new ArrayList<>(listenerMap.values()).forEach(this::removeListener);

    executorService.shutdownNow();
    try {
      executorService.awaitTermination(DEFAULT_START_STOP_DURATION.toSeconds(), TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for close", e);
    }

    watchRetryExecutor.shutdownNow();
    try {
      watchRetryExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Close every partition store, re-sweeping until the map is stable. A watch callback that
    // passed the closing check before close() set the flag can still insert into the map after the
    // executors are shut down; draining here guarantees its lease and keepalive thread are revoked
    // rather than orphaned.
    Set<EtcdMetadataStore<T>> closed = new HashSet<>();
    while (!metadataStoreMap.isEmpty()) {
      List<String> partitions = new ArrayList<>(metadataStoreMap.keySet());
      for (String partition : partitions) {
        EtcdMetadataStore<T> store = metadataStoreMap.remove(partition);
        if (store != null && closed.add(store)) {
          store.close();
        }
      }
    }

    // Clear the maps
    listenerMap.clear();
    metadataStoreMap.clear();
  }
}
