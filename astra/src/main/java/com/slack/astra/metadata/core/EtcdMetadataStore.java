package com.slack.astra.metadata.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.config.AstraConfigs.EtcdConfig;
import com.slack.astra.util.ExponentialBackOff;
import com.slack.astra.util.FatalErrorHandler;
import com.slack.astra.util.RuntimeHalterImpl;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EtcdMetadataStore is a class which provides consistent Etcd apis for all the metadata store
 * classes.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 */
public class EtcdMetadataStore<T extends AstraMetadata> implements Closeable {
  /**
   * Single thread executor for handling watch events asynchronously to avoid deadlocks while
   * maintaining event ordering. Using a single thread ensures events are processed in the same
   * order they were received, which is important for consistency.
   */
  private static final ExecutorService WATCH_EVENT_EXECUTOR =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("etcd-watch-event-processor");
            return t;
          });

  /** Shared lease ID for all ephemeral nodes. Only valid if createMode is EPHEMERAL. */
  private volatile long sharedLeaseId = -1;

  /**
   * Handle to the active keep-alive stream, retained because {@link io.etcd.jetcd.Lease#keepAlive}
   * registers a new observer per call and discarding the handle leaks would leak one observer per
   * retry.
   */
  private volatile CloseableClient keepAliveClient;

  /**
   * Incremented per keep-alive restart/start, so callbacks from a superseded stream get discarded.
   */
  private final AtomicLong keepAliveGeneration = new AtomicLong();

  /** Flag to track if the store is being closed to prevent keepalive restarts during shutdown. */
  private volatile boolean isClosing = false;

  /** Used for watch retry operations with delays. */
  private final ScheduledExecutorService watchRetryExecutor;

  /** The create mode for this metadata store instance. */
  private final EtcdCreateMode createMode;

  /** TTL in milliseconds for ephemeral nodes. */
  private final long ephemeralTtlMs;

  private static final Logger LOG = LoggerFactory.getLogger(EtcdMetadataStore.class);

  /**
   * A watcher paired with a latch for callbacks to check before treating a completion or error as a
   * real failure.
   */
  record WatchHandle(Watcher watcher, AtomicBoolean disposed) {
    /** Marks the stream disposed so its pending callbacks go away, then closes it. */
    void dispose(String what) {
      disposed.set(true);
      closeQuietly(what, watcher);
    }
  }

  protected final String storeFolder;
  protected final String namespace;
  protected final Client etcdClient;
  private final ConcurrentHashMap<String, WatchHandle> watchers;
  protected final ConcurrentHashMap<String, T> cache = new ConcurrentHashMap<>();
  protected final boolean shouldCache;
  protected final MetadataSerializer<T> serializer;
  private final long etcdOperationTimeoutMs;
  private final long listPageSize;
  private final long listPageTimeoutMs;
  private final long retryTotalDurationMs;
  private final long maxRetryDelayMs;
  private final long initialRetryIntervalMs;
  private final ExponentialBackOff keepAliveBackoff;

  /** Retained so each listener's watch driver resolves its config from one place. */
  private final EtcdConfig etcdConfig;

  static final long DEFAULT_OPERATIONS_TIMEOUT_MS = 60_000;
  static final long DEFAULT_RETRY_TOTAL_DURATION_MS = 60_000;
  static final long DEFAULT_MAX_RETRY_DELAY_MS = 10_000;
  static final long DEFAULT_INITIAL_RETRY_INTERVAL_MS = 2_000;

  /** How long a watch may keep failing before the JVM halts. default of 5-minutes */
  static final long DEFAULT_WATCH_FATAL_AFTER_MS = 300_000;

  private static volatile FatalErrorHandler fatalErrorHandler = new RuntimeHalterImpl();

  static void setFatalErrorHandler(FatalErrorHandler handler) {
    fatalErrorHandler = handler;
  }

  static void resetFatalErrorHandler() {
    fatalErrorHandler = new RuntimeHalterImpl();
  }

  static long positiveOrDefault(long value, long defaultValue) {
    return value > 0 ? value : defaultValue;
  }

  /** The {@code store} metric tag for a store path: its top level */
  static String storeTag(String storeFolder) {
    int end = storeFolder.indexOf('/', 1);
    return end < 0 ? storeFolder : storeFolder.substring(0, end);
  }

  private final MeterRegistry meterRegistry;
  private final String storeTag;

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  private static final String ASTRA_ETCD_CREATE_CALL = "astra_etcd_create_call";
  private static final String ASTRA_ETCD_HAS_CALL = "astra_etcd_has_call";
  private static final String ASTRA_ETCD_DELETE_CALL = "astra_etcd_delete_call";
  private static final String ASTRA_ETCD_LIST_CALL = "astra_etcd_list_call";
  private static final String ASTRA_ETCD_GET_CALL = "astra_etcd_get_call";
  private static final String ASTRA_ETCD_UPDATE_CALL = "astra_etcd_update_call";
  private static final String ASTRA_ETCD_ADDED_LISTENER = "astra_etcd_added_listener";
  private static final String ASTRA_ETCD_REMOVED_LISTENER = "astra_etcd_removed_listener";
  private static final String ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED =
      "astra_etcd_cache_init_handler_fired";
  private static final String ASTRA_ETCD_LEASE_REFRESH_HANDLER_FIRED =
      "astra_etcd_lease_refresh_handler_fired";

  private static final String ASTRA_ETCD_RESYNC_SKIP = "astra_etcd_resync_skip";

  private final Counter createCall;
  private final Counter hasCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;
  private final Counter cacheInitHandlerFired;
  private final Counter leaseRefreshHandlerFired;

  private final Counter resyncSkip;

  /** Constructor that accepts an external etcd client instance with default persistent mode. */
  public EtcdMetadataStore(
      String storeFolder,
      EtcdConfig config,
      boolean shouldCache,
      MeterRegistry meterRegistry,
      MetadataSerializer<T> serializer,
      Client etcClient) {
    this(
        storeFolder,
        config,
        shouldCache,
        meterRegistry,
        serializer,
        EtcdCreateMode.PERSISTENT,
        etcClient);
  }

  /** Constructor that accepts an external etcd client instance with specified create mode. */
  public EtcdMetadataStore(
      String storeFolder,
      EtcdConfig config,
      boolean shouldCache,
      MeterRegistry meterRegistry,
      MetadataSerializer<T> serializer,
      EtcdCreateMode createMode,
      Client etcdClient) {
    this.storeFolder = storeFolder;
    this.namespace = config.getNamespace();
    this.etcdConfig = config;
    this.meterRegistry = meterRegistry;
    this.shouldCache = shouldCache;
    this.serializer = serializer;
    this.watchers = new ConcurrentHashMap<>();
    this.createMode = createMode;
    this.ephemeralTtlMs = config.getEphemeralNodeTtlMs();
    this.etcdOperationTimeoutMs =
        positiveOrDefault(config.getOperationsTimeoutMs(), DEFAULT_OPERATIONS_TIMEOUT_MS);
    this.listPageSize = config.getListPageSize();
    // Falls back to the operation timeout when unset, preserving prior behavior.
    this.listPageTimeoutMs =
        positiveOrDefault(config.getListPageTimeoutMs(), etcdOperationTimeoutMs);

    this.retryTotalDurationMs =
        positiveOrDefault(config.getRetryTotalDurationMs(), DEFAULT_RETRY_TOTAL_DURATION_MS);
    this.maxRetryDelayMs =
        positiveOrDefault(config.getMaxRetryDelayMs(), DEFAULT_MAX_RETRY_DELAY_MS);
    this.initialRetryIntervalMs =
        positiveOrDefault(config.getInitialRetryIntervalMs(), DEFAULT_INITIAL_RETRY_INTERVAL_MS);
    this.keepAliveBackoff = newRetryBackoff();

    String store = storeTag(storeFolder);
    this.storeTag = store;
    this.createCall = this.meterRegistry.counter(ASTRA_ETCD_CREATE_CALL, "store", store);
    this.deleteCall = this.meterRegistry.counter(ASTRA_ETCD_DELETE_CALL, "store", store);
    this.listCall = this.meterRegistry.counter(ASTRA_ETCD_LIST_CALL, "store", store);
    this.getCall = this.meterRegistry.counter(ASTRA_ETCD_GET_CALL, "store", store);
    this.hasCall = this.meterRegistry.counter(ASTRA_ETCD_HAS_CALL, "store", store);
    this.updateCall = this.meterRegistry.counter(ASTRA_ETCD_UPDATE_CALL, "store", store);
    this.addedListener = this.meterRegistry.counter(ASTRA_ETCD_ADDED_LISTENER, "store", store);
    this.removedListener = this.meterRegistry.counter(ASTRA_ETCD_REMOVED_LISTENER, "store", store);
    this.cacheInitHandlerFired =
        this.meterRegistry.counter(ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED, "store", store);
    this.leaseRefreshHandlerFired =
        this.meterRegistry.counter(ASTRA_ETCD_LEASE_REFRESH_HANDLER_FIRED, "store", store);
    this.resyncSkip = this.meterRegistry.counter(ASTRA_ETCD_RESYNC_SKIP, "store", store);

    if (etcdClient == null) {
      throw new IllegalArgumentException("External etcd client must be provided");
    }

    LOG.info(
        "Using provided external etcd client for store folder: {} with mode: {}",
        storeFolder,
        createMode);
    this.etcdClient = etcdClient;

    // One retry thread per store, for the store's lifetime: core threads are never reclaimed.
    this.watchRetryExecutor =
        Executors.newScheduledThreadPool(
            1,
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              t.setName("etcd-watch-retry-" + storeFolder);
              return t;
            });

    if (createMode == EtcdCreateMode.EPHEMERAL) {
      // Create a single shared lease for all ephemeral nodes synchronously
      try {
        sharedLeaseId =
            this.etcdClient
                .getLeaseClient()
                .grant(ephemeralTtlMs / 1000) // grant ttl is in seconds
                .get(ephemeralTtlMs, TimeUnit.MILLISECONDS)
                .getID();
        startKeepAlive();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }

      LOG.info(
          "Created shared lease {} (HEX: {}) with TTL {} milliseconds for all ephemeral nodes",
          sharedLeaseId,
          Long.toHexString(sharedLeaseId),
          ephemeralTtlMs);
    }

    // Initialize cache if needed
    if (shouldCache) {
      LOG.info("Cache enabled for etcd store: {}", storeFolder);

      // Create and register a default listener to keep the cache in sync with etcd changes
      // This ensures that even without explicit listeners, the cache stays updated across JVMs
      addListener(node -> LOG.trace("Default watcher updated cache for node: {}", node.getName()));

      // Populate cache synchronously during initialization
      populateInitialCache();

      LOG.info("Default cache watcher started for store: {}", storeFolder);
    }
  }

  /**
   * Closes a watcher or keep-alive handle, logging any failure: every caller is already tearing the
   * stream down.
   */
  static void closeQuietly(String what, AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (Exception e) {
        LOG.debug("Error closing {}", what, e);
      }
    }
  }

  static void handleFatalAsync(Throwable error, String threadNameSuffix) {
    LOG.error("Fatal error detected ({}), initiating shutdown", threadNameSuffix, error);
    // logic here to be able to swap the handler if we're in a test (to not fatal the tests)
    FatalErrorHandler handler = fatalErrorHandler;
    Thread t = new Thread(() -> handler.handleFatal(error));
    t.setName("etcd-fatal-" + threadNameSuffix);
    t.setDaemon(true);
    t.start();
  }

  /** Opens (or reopens) the lease keep-alive stream */
  private void startKeepAlive() {
    if (isClosing) {
      return;
    }
    long generation = keepAliveGeneration.incrementAndGet();
    closeKeepAliveClient();

    keepAliveClient =
        this.etcdClient
            .getLeaseClient()
            .keepAlive(
                sharedLeaseId,
                new StreamObserver<LeaseKeepAliveResponse>() {
                  /** True once a newer keep-alive stream has superseded this one. */
                  private boolean superseded() {
                    return keepAliveGeneration.get() != generation;
                  }

                  @Override
                  public void onNext(LeaseKeepAliveResponse response) {
                    if (superseded()) {
                      return;
                    }
                    LOG.debug(
                        "Received keepAlive response for lease {}, TTL: {}",
                        response.getID(),
                        response.getTTL());
                    keepAliveBackoff.reset();
                    leaseRefreshHandlerFired.increment();
                  }

                  @Override
                  public void onError(Throwable t) {
                    if (isClosing || superseded()) {
                      LOG.debug(
                          "Ignoring keepAlive error for superseded or closing lease {}",
                          sharedLeaseId);
                      return;
                    }
                    if (EtcdErrorClassifier.isLeaseGone(t)) {
                      handleLeaseLost(t);
                      return;
                    }
                    retryOrLoseLease(
                        t, "KeepAlive error for lease " + sharedLeaseId + ": " + t.getMessage());
                  }

                  @Override
                  public void onCompleted() {
                    if (isClosing || superseded()) {
                      LOG.debug(
                          "Ignoring keepAlive completion for superseded or closing lease {}",
                          sharedLeaseId);
                      return;
                    }
                    // A completion does not prove the lease is gone, since jetcd reports a broken
                    // connection the same way; if it really is gone, the reopen fails lease-gone.
                    retryOrLoseLease(
                        new IllegalStateException(
                            "etcd closed the keep-alive stream for lease " + sharedLeaseId),
                        "KeepAlive stream completed for lease " + sharedLeaseId);
                  }
                });
  }

  /**
   * Reopens the keep-alive after a backoff delay, reporting the lease lost once {@code
   * retryTotalDurationMs} is spent.
   */
  private void retryOrLoseLease(Throwable cause, String context) {
    if (EtcdErrorClassifier.classify(cause).isTerminal()) {
      // Shutdown rather than lease loss: halting here would call System.exit from the shutdown
      // hook.
      LOG.info("{} — the etcd client is closed, not renewing the lease", context);
      return;
    }

    long delayMs = keepAliveBackoff.nextBackOffMillis();
    if (delayMs != ExponentialBackOff.STOP) {
      LOG.warn(
          "{} — retrying in {} ms ({} ms elapsed)",
          context,
          delayMs,
          keepAliveBackoff.getElapsedTimeMs());
      try {
        watchRetryExecutor.schedule(this::reopenKeepAlive, delayMs, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        LOG.debug("Retry executor already shut down for {}, retry not scheduled", storeFolder);
      }
      return;
    }
    LOG.error(
        "KeepAlive retry budget exhausted for lease {} after {} ms",
        sharedLeaseId,
        keepAliveBackoff.getElapsedTimeMs());
    handleLeaseLost(cause);
  }

  /**
   * Halts on definitive loss of the shared lease, since etcd deleted every ephemeral key this store
   * registered and only startup registration can re-advertise them.
   */
  private void handleLeaseLost(Throwable cause) {
    LOG.error(
        "Shared lease {} for store {} is gone; ephemeral registrations were deleted by etcd",
        sharedLeaseId,
        storeFolder,
        cause);
    handleFatalAsync(cause, "keepalive-" + sharedLeaseId);
  }

  /**
   * {@link #startKeepAlive()} for the retry executor, because jetcd throws synchronously once the
   * lease client is closed and a scheduled task's throwable would go nowhere.
   */
  private void reopenKeepAlive() {
    try {
      startKeepAlive();
    } catch (RuntimeException e) {
      retryOrLoseLease(e, "Could not reopen keep-alive for lease " + sharedLeaseId);
    }
  }

  /** Closes the current keep-alive handle, if any. */
  private void closeKeepAliveClient() {
    CloseableClient previous = keepAliveClient;
    keepAliveClient = null;
    closeQuietly("keepAlive client for lease " + sharedLeaseId, previous);
  }

  /**
   * Converts a path string to an etcd ByteSequence key.
   *
   * @param path The path to convert
   * @return ByteSequence representation of the path
   */
  private ByteSequence pathToKey(String path) {
    String fullPath = storeFolder + "/" + path;
    return ByteSequence.from(fullPath, StandardCharsets.UTF_8);
  }

  /**
   * Extracts the name from a full etcd key.
   *
   * @param key The ByteSequence key from etcd
   * @return The name part of the key
   */
  private String keyToName(ByteSequence key) {
    String keyStr = key.toString(StandardCharsets.UTF_8);
    // Remove prefix if present
    if (keyStr.startsWith(storeFolder + "/")) {
      return keyStr.substring((storeFolder + "/").length());
    }
    return keyStr;
  }

  /** Returns true if the key is a direct child of storeFolder (not a nested descendant). */
  private boolean isDirectChild(ByteSequence key) {
    return !keyToName(key).contains("/");
  }

  /**
   * Creates a new metadata node asynchronously.
   *
   * @param metadataNode The node to create
   * @return A CompletionStage that completes with the path of the created node
   */
  public CompletionStage<String> createAsync(T metadataNode) {
    LOG.debug("Creating metadata node: {} in store: {}", metadataNode, storeFolder);
    this.createCall.increment();

    // Validate node name
    String nodeName = metadataNode.getName();
    if (nodeName == null || nodeName.isEmpty() || "/".equals(nodeName) || ".".equals(nodeName)) {
      CompletableFuture<String> future = new CompletableFuture<>();
      future.completeExceptionally(
          new InternalMetadataStoreException("Invalid node name: " + nodeName));
      return future;
    }

    try {
      ByteSequence key = pathToKey(nodeName);
      ByteSequence value =
          ByteSequence.from(serializer.toJsonStr(metadataNode), StandardCharsets.UTF_8);

      // First check if the node already exists
      return etcdClient
          .getKVClient()
          .get(key)
          .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
          .thenComposeAsync(
              getResponse -> {
                if (!getResponse.getKvs().isEmpty()) {
                  // Node exists, throw exception to match ZK behavior
                  CompletableFuture<String> future = new CompletableFuture<>();
                  future.completeExceptionally(
                      new InternalMetadataStoreException(
                          "Node already exists: " + metadataNode.getName()));
                  return future;
                }

                if (createMode == EtcdCreateMode.PERSISTENT) {
                  // For persistent nodes, just do a regular put
                  return etcdClient
                      .getKVClient()
                      .put(key, value)
                      .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
                      .thenApplyAsync(
                          putResponse -> {
                            // Always update the cache for consistency
                            if (shouldCache) {
                              cache.put(metadataNode.getName(), metadataNode);
                            }
                            // Return just the name (not the full path) to match
                            // ZookeeperMetadataStore
                            // behavior
                            return metadataNode.getName();
                          });
                } else {
                  // For ephemeral nodes, use the shared lease directly
                  // Create a put option that associates the key with the shared lease
                  PutOption putOption = PutOption.builder().withLeaseId(sharedLeaseId).build();

                  // Use the shared lease to put the key in etcd
                  return etcdClient
                      .getKVClient()
                      .put(key, value, putOption)
                      .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
                      .thenApplyAsync(
                          putResponse -> {
                            LOG.debug(
                                "Created ephemeral node {} with shared lease ID {}, TTL {} milliseconds",
                                metadataNode.getName(),
                                sharedLeaseId,
                                ephemeralTtlMs);

                            // Always update the cache for consistency
                            if (shouldCache) {
                              cache.put(metadataNode.getName(), metadataNode);
                            }
                            // Return just the name (not the full path) to match
                            // ZookeeperMetadataStore behavior
                            return metadataNode.getName();
                          });
                }
              });
    } catch (InvalidProtocolBufferException e) {
      CompletableFuture<String> future = new CompletableFuture<>();
      future.completeExceptionally(
          new InternalMetadataStoreException("Failed to serialize node", e));
      return future;
    }
  }

  /**
   * Creates a new metadata node synchronously.
   *
   * @param metadataNode The node to create
   */
  public void createSync(T metadataNode) {
    this.createCall.increment();

    try {
      createAsync(metadataNode)
          .toCompletableFuture()
          .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to create node: {}", metadataNode.getName(), e);
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  /**
   * Gets a metadata node asynchronously.
   *
   * @param path The path to the node
   * @return A CompletionStage that completes with the node, or null if not found
   */
  public CompletionStage<T> getAsync(String path) {
    this.getCall.increment();

    awaitCacheInitialized();

    // Check cache first if enabled
    if (shouldCache) {
      T result = cache.get(path);
      if (result != null) {
        return CompletableFuture.completedFuture(result);
      } else {
        throw new InternalMetadataStoreException("Node not found: " + path);
      }
    }

    ByteSequence key = pathToKey(path);
    return etcdClient
        .getKVClient()
        .get(key)
        .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
        .thenApplyAsync(
            getResponse -> {
              if (getResponse.getKvs().isEmpty()) {
                throw new InternalMetadataStoreException("Node not found: " + path);
              }

              KeyValue kv = getResponse.getKvs().getFirst();
              try {
                String json = kv.getValue().toString(StandardCharsets.UTF_8);
                return serializer.fromJsonStr(json);
              } catch (InvalidProtocolBufferException e) {
                throw new InternalMetadataStoreException("Failed to deserialize node", e);
              }
            });
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param path The path to the node
   * @return The node, or null if not found
   */
  public T getSync(String path) {
    this.getCall.increment();

    awaitCacheInitialized();

    // Check cache first if enabled
    if (shouldCache) {
      T result = cache.get(path);
      if (result != null) {
        return result;
      } else {
        throw new InternalMetadataStoreException("Node not found: " + path);
      }
    }

    try {
      return getAsync(path)
          .toCompletableFuture()
          .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException e) {
      LOG.error("Failed to get node: {}", path, e);
      throw new RuntimeException("Failed to get node", e);
    } catch (ExecutionException e) {
      // Handle exceptions from getAsync that are wrapped in ExecutionException
      if (e.getCause() instanceof RuntimeException) {
        // Rethrow all exceptions from getAsync
        throw (RuntimeException) e.getCause();
      }
      // For any other exception, log and throw
      LOG.error("Failed to get node: {}", path, e.getCause());
      throw new InternalMetadataStoreException("Failed to get node: " + path, e.getCause());
    }
  }

  /**
   * Checks if a node exists asynchronously.
   *
   * @param path The path to check
   * @return A CompletionStage that completes with a Boolean indicating if the node exists
   */
  public CompletionStage<Boolean> hasAsync(String path) {
    this.hasCall.increment();

    awaitCacheInitialized();

    // Check cache if enabled
    if (shouldCache) {
      return CompletableFuture.completedFuture(cache.containsKey(path));
    }

    ByteSequence key = pathToKey(path);
    return etcdClient
        .getKVClient()
        .get(key)
        .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
        .thenApplyAsync(getResponse -> !getResponse.getKvs().isEmpty());
  }

  /**
   * Checks if a node exists synchronously.
   *
   * @param path The path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String path) {
    this.hasCall.increment();

    awaitCacheInitialized();

    // Check cache if enabled
    if (shouldCache) {
      return cache.containsKey(path);
    }

    try {
      return hasAsync(path)
          .toCompletableFuture()
          .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to check if node exists: {}", path, e);
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  /**
   * Updates a metadata node asynchronously.
   *
   * @param metadataNode The node to update
   * @return A CompletionStage that completes with the node name when the update is done
   */
  public CompletionStage<String> updateAsync(T metadataNode) {
    this.updateCall.increment();

    try {
      ByteSequence key = pathToKey(metadataNode.getName());
      ByteSequence value =
          ByteSequence.from(serializer.toJsonStr(metadataNode), StandardCharsets.UTF_8);

      // First get the existing key to check if it has a lease
      return etcdClient
          .getKVClient()
          .get(key)
          .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
          .thenComposeAsync(
              getResponse -> {
                long existingLeaseId = 0;

                // Check if the key exists and has a lease
                if (!getResponse.getKvs().isEmpty()) {
                  KeyValue existingKv = getResponse.getKvs().getFirst();
                  existingLeaseId = existingKv.getLease();
                }

                // Determine which lease to use for the update
                PutOption putOption = null;
                if (existingLeaseId > 0) {
                  // Preserve existing lease
                  putOption = PutOption.builder().withLeaseId(existingLeaseId).build();
                } else if (createMode == EtcdCreateMode.EPHEMERAL && sharedLeaseId > 0) {
                  // Apply shared lease for ephemeral nodes that don't have a lease
                  putOption = PutOption.builder().withLeaseId(sharedLeaseId).build();
                }

                // Perform the put with appropriate lease option
                if (putOption != null) {
                  return etcdClient.getKVClient().put(key, value, putOption);
                } else {
                  return etcdClient.getKVClient().put(key, value);
                }
              })
          .thenApplyAsync(
              putResponse -> {
                // Always update the cache for consistency
                if (shouldCache) {
                  cache.put(metadataNode.getName(), metadataNode);
                }
                return metadataNode.getName();
              });
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Failed to update node (async): {}", metadataNode.getName(), e);
      CompletableFuture<String> future = new CompletableFuture<>();
      future.completeExceptionally(
          new InternalMetadataStoreException("Failed to serialize node", e));
      return future;
    }
  }

  /**
   * Updates a metadata node synchronously.
   *
   * @param metadataNode The node to update
   */
  public void updateSync(T metadataNode) {
    this.updateCall.increment();

    try {
      updateAsync(metadataNode)
          .exceptionally(
              throwable -> {
                throw new RuntimeException(throwable);
              })
          .toCompletableFuture()
          .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to update node: {} and took {} seconds", metadataNode.getName(), e);
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  /**
   * Deletes a node asynchronously.
   *
   * @param path The path to the node to delete
   * @return A CompletionStage that completes when the delete is done
   */
  public CompletionStage<Void> deleteAsync(String path) {
    this.deleteCall.increment();

    ByteSequence key = pathToKey(path);
    return etcdClient
        .getKVClient()
        .delete(key)
        .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
        .thenAcceptAsync(
            deleteResponse -> {
              // Evict before the getDeleted() check: a 0 response means the key is gone from etcd,
              // so a cached entry is stale and must be dropped rather than retried forever.
              if (shouldCache) {
                cache.remove(path);
              }

              // Note: deleteResponse.getDeleted() tells us how many keys were deleted
              if (deleteResponse.getDeleted() == 0) {
                throw new InternalMetadataStoreException("Failed to delete node: " + path);
              }

              // We don't need to take any special action for ephemeral nodes
              // since we're using a shared lease that will be revoked on close()
            });
  }

  /**
   * Deletes a node synchronously.
   *
   * @param path The path to the node to delete
   */
  public void deleteSync(String path) {
    this.deleteCall.increment();

    try {
      deleteAsync(path).toCompletableFuture().get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to delete node: {}", path, e);
      throw new InternalMetadataStoreException("Error deleting node under at path: " + path, e);
    }
  }

  /**
   * Deletes a node asynchronously.
   *
   * @param metadataNode The node to delete
   * @return A CompletionStage that completes when the delete is done
   */
  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return deleteAsync(metadataNode.getName());
  }

  /**
   * Deletes a node synchronously.
   *
   * @param metadataNode The node to delete
   */
  public void deleteSync(T metadataNode) {
    this.deleteCall.increment();

    try {
      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to delete node: {}", metadataNode.getName(), e);
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return A CompletionStage that completes with the list of all nodes
   */
  public CompletionStage<List<T>> listAsync() {
    LOG.debug("Listing async nodes under at path {}, shouldCache: {}", storeFolder, shouldCache);
    this.listCall.increment();

    awaitCacheInitialized();

    // First ensure the cache is initialized and then use it if enabled
    if (shouldCache) {
      List<T> cachedNodes = new ArrayList<>(cache.values());
      return CompletableFuture.completedFuture(cachedNodes);
    }

    // Add a trailing slash to the folder to make sure we only list entries directly under it.
    ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);

    return EtcdRangePaginator.listRangeAsync(
            etcdClient.getKVClient(),
            prefix,
            false,
            etcdOperationTimeoutMs,
            listPageTimeoutMs,
            listPageSize)
        .thenApplyAsync(
            range -> {
              List<T> nodes = new ArrayList<>();

              for (KeyValue kv : range.keyValues()) {
                try {
                  String json = kv.getValue().toString(StandardCharsets.UTF_8);
                  T node = serializer.fromJsonStr(json);
                  nodes.add(node);

                  // Always update the cache for consistency
                  if (shouldCache) {
                    cache.put(node.getName(), node);
                  }
                } catch (InvalidProtocolBufferException e) {
                  LOG.error("Failed to deserialize node from key: {}", kv.getKey(), e);
                }
              }

              return nodes;
            });
  }

  /**
   * How many nodes this store holds, without materializing them.
   *
   * @return A CompletionStage that completes with the node count
   */
  CompletionStage<Integer> sizeAsync() {
    if (shouldCache) {
      awaitCacheInitialized();
      return CompletableFuture.completedFuture(cache.size());
    }
    return listAsync().thenApply(List::size);
  }

  /**
   * Lists all nodes synchronously.
   *
   * @return The list of all nodes
   */
  public List<T> listSync() {
    LOG.debug("Listing sync nodes under at path {}, shouldCache: {}", storeFolder, shouldCache);
    this.listCall.increment();

    awaitCacheInitialized();

    // First ensure the cache is initialized and then use it if enabled
    if (shouldCache) {
      return new ArrayList<>(cache.values());
    }

    try {
      return listAsync().toCompletableFuture().get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to list nodes", e);
      throw new InternalMetadataStoreException("Error getting cached nodes", e);
    }
  }

  /**
   * Lists all nodes synchronously without relying on the cache. This is primarily for testing and
   * should not be used in production code.
   *
   * @return The list of all nodes directly from etcd
   * @throws InternalMetadataStoreException if there's an error fetching data from etcd
   */
  public List<T> listSyncUncached() {
    this.listCall.increment();

    try {
      // Add a trailing slash to the folder to make sure we only list entries directly under this
      // folder. Paginated so a large store cannot overflow the gRPC inbound message size limit.
      ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
      List<KeyValue> keyValues =
          EtcdRangePaginator.listRange(
                  etcdClient.getKVClient(),
                  prefix,
                  false,
                  etcdOperationTimeoutMs,
                  listPageTimeoutMs,
                  listPageSize)
              .keyValues();

      List<T> nodes = new ArrayList<>();

      for (KeyValue kv : keyValues) {
        try {
          String json = kv.getValue().toString(StandardCharsets.UTF_8);
          T node = serializer.fromJsonStr(json);
          nodes.add(node);
        } catch (InvalidProtocolBufferException e) {
          LOG.error("Failed to deserialize node from key: {}", kv.getKey(), e);
        }
      }

      return nodes;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to list nodes uncached", e);
      throw new InternalMetadataStoreException("Error listing nodes directly from etcd", e);
    }
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param listener The listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> listener) {
    // One retry driver per listener, because each listener owns its own watch stream.
    addListener(
        listener,
        EtcdWatchRetry.REVISION_LATEST,
        new EtcdWatchRetry(
            etcdConfig,
            meterRegistry,
            storeTag,
            watchRetryExecutor,
            () -> isClosing,
            (revision, retry) -> addListener(listener, revision, retry),
            "watch for store " + storeFolder,
            "cache is stale",
            System::currentTimeMillis));
  }

  /**
   * Internal method to add a listener, re-establishing the watch with unbounded backoff on failure.
   *
   * @param listener The listener to add
   * @param startRevision The ETCD revision to start at, or {@link EtcdWatchRetry#REVISION_LATEST}
   *     or {@link EtcdWatchRetry#REVISION_RESYNC}
   * @param retry Pacing state carried across retries of this listener's watch
   */
  private void addListener(
      AstraMetadataStoreChangeListener<T> listener, long startRevision, EtcdWatchRetry retry) {
    this.addedListener.increment();

    if (!shouldCache) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    if (listener == null) {
      LOG.warn("Attempted to add null listener, ignoring");
      return;
    }

    // Watch the exact node path itself as well as any children
    ByteSequence prefix = ByteSequence.from(storeFolder, StandardCharsets.UTF_8);

    // Get the current revision before starting the watch to prevent race conditions
    // We start watching from the next revision to ensure we capture all events
    // that occur during and after watch setup
    WatchOption watchOption;
    long watchFromRevision;
    try {
      if (startRevision == EtcdWatchRetry.REVISION_RESYNC) {
        watchFromRevision = resyncCacheFromEtcd(listener) + 1;
      } else if (startRevision == EtcdWatchRetry.REVISION_LATEST) {
        watchFromRevision =
            EtcdRangePaginator.currentRevision(
                    etcdClient.getKVClient(), prefix, etcdOperationTimeoutMs)
                + 1;
      } else {
        // A reconnect: startRevision came from EtcdWatchRetry.reachedRevision
        watchFromRevision = startRevision;
      }

      watchOption =
          WatchOption.builder().withPrefix(prefix).withRevision(watchFromRevision).build();
      LOG.debug(
          "adding listener {} for store {} at revision {}",
          listener,
          storeFolder,
          watchFromRevision);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      if (isClosing || Thread.currentThread().isInterrupted()) {
        LOG.warn(
            "Ignoring revision fetch error during shutdown for store {}: {}",
            storeFolder,
            e.getMessage());
        return;
      }
      // We didn't get a usable revision, retry has nothing to resume from and must resync.
      retry.requireResync();
      retry.onFailure(e, "Revision fetch failed for store " + storeFolder);
      return;
    }

    retry.openedAtRevision(watchFromRevision);
    AtomicReference<Watcher> watcherRef = new AtomicReference<>();
    // Whichever callback fires first claims this, so an error and a completion for the same dead
    // watcher cannot each schedule a retry.
    AtomicBoolean disposed = new AtomicBoolean(false);
    String listenerKey = listenerKey(listener);

    Watcher watcher =
        etcdClient
            .getWatchClient()
            .watch(
                prefix,
                watchOption,
                response -> {
                  // On the callback thread, so a failure racing this response still resumes from
                  // the newer revision.
                  if (response.getHeader() != null) {
                    retry.reachedRevision(response.getHeader().getRevision() + 1);
                  }
                  // Process watch events on a separate thread to avoid deadlocks
                  // This is critical when watch handlers need to make synchronous metadata
                  // operations
                  WATCH_EVENT_EXECUTOR.execute(
                      () -> {
                        for (WatchEvent event : response.getEvents()) {
                          try {
                            // Extract the path from the key
                            String path = keyToName(event.getKeyValue().getKey());

                            // Handle different event types
                            switch (event.getEventType()) {
                              case PUT:
                                // This could be a create or update
                                String json =
                                    event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                                T node = serializer.fromJsonStr(json);

                                // Update cache (we're already in a listener which means caching is
                                // enabled)
                                cache.put(path, node);

                                // Notify listener of changes only for create/update
                                listener.onMetadataStoreChanged(node);
                                break;

                              case DELETE:
                                // Remove from cache (we're already in a listener which means
                                // caching is enabled)
                                T deletedNode = cache.remove(path);
                                // We can only notify if we have the node in cache
                                if (deletedNode != null) {
                                  listener.onMetadataStoreChanged(deletedNode);
                                }
                                break;

                              default:
                                LOG.warn("Unknown event type: {}", event.getEventType());
                            }
                          } catch (Exception e) {
                            LOG.error("Error processing watch event", e);
                          }
                        }
                      });
                },
                error -> {
                  if (!retry.claimDisposal(disposed, "watch error")) {
                    return;
                  }
                  recoverOffCallbackThread(
                      listenerKey,
                      watcherRef,
                      retry,
                      error,
                      "Watch failed for store " + storeFolder);
                },
                () -> {
                  // The 4-argument watch overload supplies a no-op onCompleted, which leaves a
                  // cleanly completed stream permanently deaf.
                  if (!retry.claimDisposal(disposed, "watch completion")) {
                    return;
                  }
                  recoverOffCallbackThread(
                      listenerKey,
                      watcherRef,
                      retry,
                      new IllegalStateException("etcd closed the watch stream"),
                      "Watch completed unexpectedly for store " + storeFolder);
                });

    watcherRef.set(watcher);
    if (disposed.get()) {
      // Failed before registration finished; a retry is already scheduled, so this watcher must not
      // be left in the map as the live one.
      closeQuietly("watcher", watcher);
      return;
    }

    WatchHandle previous = watchers.put(listenerKey, new WatchHandle(watcher, disposed));
    if (previous != null) {
      previous.dispose("watcher");
    }
    retry.onEstablished();
  }

  /**
   * Closes the dead watcher and starts recovery off the thread jetcd called us on, since one
   * callback executor is shared by every watcher in the process.
   */
  private void recoverOffCallbackThread(
      String listenerKey,
      AtomicReference<Watcher> watcherRef,
      EtcdWatchRetry retry,
      Throwable error,
      String context) {
    try {
      watchRetryExecutor.execute(
          () -> {
            disposeWatcher(listenerKey, watcherRef.get());
            retry.onFailure(error, context);
          });
    } catch (RejectedExecutionException e) {
      // Only reachable once the store is closing, which closes every registered watcher itself.
      LOG.debug("Store {} is closing, not recovering watch", storeFolder);
      closeQuietly("watcher", watcherRef.get());
    }
  }

  /**
   * Removes a watcher from the registry and disposes it, claiming its latch first since {@link
   * Watcher#close()} fires the watcher's own onCompleted synchronously.
   *
   * @param fallback closed instead when nothing was registered under the key, so a watcher that
   *     failed before it was registered is still closed
   * @return whether a registered watcher was found
   */
  private boolean disposeWatcher(String listenerKey, Watcher fallback) {
    WatchHandle registered = watchers.remove(listenerKey);
    if (registered != null) {
      registered.dispose("watcher");
    } else {
      closeQuietly("watcher", fallback);
    }
    return registered != null;
  }

  /** Registry key for a listener's watcher. */
  private static String listenerKey(AstraMetadataStoreChangeListener<?> listener) {
    return String.valueOf(System.identityHashCode(listener));
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param listener The listener to remove
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> listener) {
    this.removedListener.increment();

    if (!shouldCache) {
      throw new UnsupportedOperationException("Caching is disabled");
    }

    if (listener == null) {
      LOG.warn("Attempted to remove null listener, ignoring");
      return;
    }

    if (!disposeWatcher(listenerKey(listener), null)) {
      LOG.warn("Attempted to remove unknown listener");
    }
  }

  /**
   * Waits for the cache to be initialized. This method only needs to be called once as the cache is
   * populated during construction.
   */
  public void awaitCacheInitialized() {
    try {
      if (shouldCache) {
        if (!cacheInitialized.await(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)) {
          // If we're not interrupted but timed out, this is a fatal condition
          // In the case where close() was called, it would interrupt the thread before this times
          // out
          if (!Thread.currentThread().isInterrupted()) {
            LOG.error("Timed out waiting for Etcd cache to initialize for store {}", storeFolder);
            fatalErrorHandler.handleFatal(
                new TimeoutException("Timed out waiting for Etcd cache to initialize"));
          } else {
            LOG.warn("Cache initialization wait interrupted for store {}", storeFolder);
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for cache to initialize for store {}", storeFolder, e);
      Thread.currentThread().interrupt(); // Preserve interrupt status
    }
  }

  /**
   * A fresh backoff over this store's configured retry values, one per retry loop since {@link
   * ExponentialBackOff} carries the elapsed time of the episode it paces.
   */
  private ExponentialBackOff newRetryBackoff() {
    return new ExponentialBackOff(initialRetryIntervalMs, maxRetryDelayMs, retryTotalDurationMs);
  }

  /**
   * Populates the cache with all nodes from etcd, retrying transient read failures within the
   * budget rather than halting on the first one.
   */
  private void populateInitialCache() {
    ExponentialBackOff backoff = newRetryBackoff();
    while (true) {
      try {
        loadInitialCache();
        return;
      } catch (ExecutionException | TimeoutException e) {
        long delayMs = backoff.nextBackOffMillis();
        if (delayMs == ExponentialBackOff.STOP) {
          LOG.error(
              "Failed to initialize cache for store {} after {} ms of retries",
              storeFolder,
              backoff.getElapsedTimeMs(),
              e);
          fatalErrorHandler.handleFatal(
              new TimeoutException("Timed out waiting for Etcd cache to initialize"));
          return;
        }
        LOG.warn(
            "Failed to initialize cache for store {}, retrying in {} ms: {}",
            storeFolder,
            delayMs,
            e.getMessage());
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException interrupted) {
          LOG.info("Cache initialization for store {} interrupted while retrying", storeFolder);
          cacheInitialized.countDown();
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  /** Single attempt to read the store's contents into the cache. */
  private void loadInitialCache() throws ExecutionException, TimeoutException {
    try {
      LOG.debug("Populating cache for store {}", storeFolder);
      // Get only nodes from this store folder.
      ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
      List<KeyValue> keyValues =
          EtcdRangePaginator.listRange(
                  etcdClient.getKVClient(),
                  prefix,
                  false,
                  etcdOperationTimeoutMs,
                  listPageTimeoutMs,
                  listPageSize)
              .keyValues();

      // Filter for only direct children of the store folder
      for (KeyValue kv : keyValues) {
        // Check for interruption on each iteration
        if (Thread.currentThread().isInterrupted()) {
          LOG.info(
              "Cache initialization for store {} was interrupted, exiting gracefully", storeFolder);
          // If interrupted during close, mark as initialized to avoid hanging
          cacheInitialized.countDown();
          return;
        }

        String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
        LOG.debug("Store {} had key {}", storeFolder, keyStr);

        // Only include direct children of the store folder
        if (isDirectChild(kv.getKey())) {
          try {
            String json = kv.getValue().toString(StandardCharsets.UTF_8);
            T node = serializer.fromJsonStr(json);
            cache.put(node.getName(), node);
          } catch (InvalidProtocolBufferException e) {
            LOG.error("Failed to deserialize node from key: {}", kv.getKey(), e);
            // Fail the whole system if we can't deserialize a node
            fatalErrorHandler.handleFatal(e);
          }
        }
      }

      LOG.info("Initialized cache for store {} with {} nodes", storeFolder, cache.size());
      // Successfully initialized the cache
      this.cacheInitHandlerFired.increment();
      cacheInitialized.countDown();

    } catch (InterruptedException e) {
      LOG.info(
          "Cache initialization for store {} was interrupted, exiting gracefully", storeFolder);
      // If interrupted during close(), mark as initialized to avoid hangs
      cacheInitialized.countDown();
      Thread.currentThread().interrupt(); // Preserve interrupt status
    } catch (ExecutionException | TimeoutException e) {
      // Propagated so the caller can retry; only a spent retry budget is fatal.
      throw e;
    } catch (Exception e) {
      LOG.error("Failed to initialize cache for store {}", storeFolder, e);
      fatalErrorHandler.handleFatal(e);
    }
    // Note: No finally block that always calls countDown - we only want to mark
    // as initialized on success or interruption, not on errors.
  }

  /**
   * Re-lists all keys under the given prefix from etcd and resyncs the in-memory cache. Used on
   * compaction recovery where the watcher's old revision has been compacted away, meaning events
   * were missed and the cache is stale.
   *
   * <p>Detects deletes (keys in old cache but absent from etcd), repopulates the cache, and
   * notifies the listener for every node so downstream consumers can react to any changes that
   * occurred during the gap.
   *
   * @return the etcd revision from the list response, suitable for starting a watch from revision+1
   */
  private long resyncCacheFromEtcd(AstraMetadataStoreChangeListener<T> listener)
      throws InterruptedException, ExecutionException, TimeoutException {
    ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
    EtcdRangePaginator.PaginatedRange range =
        EtcdRangePaginator.listRange(
            etcdClient.getKVClient(),
            prefix,
            false,
            etcdOperationTimeoutMs,
            listPageTimeoutMs,
            listPageSize);

    long listRevision = range.revision();

    // Build the new state from etcd and collect names for delete detection
    Set<String> newKeys = new HashSet<>();
    List<T> newNodes = new ArrayList<>();
    for (KeyValue kv : range.keyValues()) {
      if (isDirectChild(kv.getKey())) {
        try {
          String json = kv.getValue().toString(StandardCharsets.UTF_8);
          T node = serializer.fromJsonStr(json);
          newKeys.add(node.getName());
          newNodes.add(node);
        } catch (InvalidProtocolBufferException e) {
          LOG.error("Failed to deserialize node during compaction resync: {}", kv.getKey(), e);
          resyncSkip.increment();
        }
      }
    }

    // Update cache: remove deleted keys, repopulate current nodes
    List<T> deletedNodes = new ArrayList<>();
    for (String oldKey : cache.keySet()) {
      if (!newKeys.contains(oldKey)) {
        T deletedNode = cache.remove(oldKey);
        if (deletedNode != null) {
          deletedNodes.add(deletedNode);
        }
      }
    }
    for (T node : newNodes) {
      cache.put(node.getName(), node);
    }

    // Dispatch notifications through WATCH_EVENT_EXECUTOR for ordering consistency
    WATCH_EVENT_EXECUTOR.execute(
        () -> {
          for (T deleted : deletedNodes) {
            listener.onMetadataStoreChanged(deleted);
          }
          for (T node : newNodes) {
            listener.onMetadataStoreChanged(node);
          }
        });

    LOG.info(
        "Compaction resync for store {} complete: {} nodes from etcd at revision {}",
        storeFolder,
        newNodes.size(),
        listRevision);

    return listRevision;
  }

  @Override
  public void close() {
    LOG.info("Closing etcd clients and watchers");
    // Set closing flag to prevent keepalive restarts during shutdown
    isClosing = true;

    // Claim disposal first: the completion each close triggers is self-inflicted and must not
    // schedule a retry.
    watchers.values().forEach(handle -> handle.dispose("watcher"));
    watchers.clear();

    closeKeepAliveClient();

    // Mark cache as initialized to unblock any waiting calls to awaitCacheInitialized
    if (cacheInitialized.getCount() > 0) {
      cacheInitialized.countDown();
    }

    // Shut down watch retry executor
    if (watchRetryExecutor != null) {
      watchRetryExecutor.shutdownNow();
      try {
        watchRetryExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Revoke the shared lease if we have one
    if (sharedLeaseId != -1) {
      try {
        LOG.info("Revoking shared lease {}", sharedLeaseId);
        etcdClient.getLeaseClient().revoke(sharedLeaseId).get(5, TimeUnit.SECONDS);
      } catch (Exception e) {
        LOG.warn("Failed to revoke shared lease {}: {}", sharedLeaseId, e.getMessage());
      } finally {
        sharedLeaseId = -1;
      }
    }

    // Note: We intentionally don't shut down the WATCH_EVENT_EXECUTOR here as it's static and
    // shared
    // across all instances. If we shut it down for one instance, it would affect all other
    // instances.
    // The executor will be cleaned up by the JVM during shutdown.

    // DO NOT close the etcd clients, as they were passed in
  }
}
