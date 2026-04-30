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
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  /** Flag to track if the store is being closed to prevent keepalive restarts during shutdown. */
  private volatile boolean isClosing = false;

  /** Used for watch retry operations with delays. */
  private final ScheduledExecutorService watchRetryExecutor;

  /** The create mode for this metadata store instance. */
  private final EtcdCreateMode createMode;

  /** TTL in milliseconds for ephemeral nodes. */
  private final long ephemeralTtlMs;

  private static final Logger LOG = LoggerFactory.getLogger(EtcdMetadataStore.class);

  protected final String storeFolder;
  protected final String namespace;
  protected final Client etcdClient;
  protected final ConcurrentHashMap<String, Watcher> watchers;
  protected final ConcurrentHashMap<String, T> cache = new ConcurrentHashMap<>();
  protected final boolean shouldCache;
  protected final MetadataSerializer<T> serializer;
  private final long etcdOperationTimeoutMs;
  private final long retryTotalDurationMs;
  private final long maxRetryDelayMs;
  private final long initialRetryIntervalMs;
  private final ExponentialBackOff keepAliveBackoff;

  /** Fetch the current etcd revision (keysOnly) and watch from there. Does not touch the cache. */
  private static final long REVISION_LATEST = 0;

  /**
   * Re-list all keys from etcd, diff against the in-memory cache, and watch from the list revision.
   * Used on compaction recovery where the watcher's old revision has been compacted away.
   */
  private static final long REVISION_RESYNC = -1;

  static final long DEFAULT_RETRY_TOTAL_DURATION_MS = 60_000;
  static final long DEFAULT_MAX_RETRY_DELAY_MS = 10_000;
  static final long DEFAULT_INITIAL_RETRY_INTERVAL_MS = 2_000;

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

  private final MeterRegistry meterRegistry;

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  private final String ASTRA_ETCD_CREATE_CALL = "astra_etcd_create_call";
  private final String ASTRA_ETCD_HAS_CALL = "astra_etcd_has_call";
  private final String ASTRA_ETCD_DELETE_CALL = "astra_etcd_delete_call";
  private final String ASTRA_ETCD_LIST_CALL = "astra_etcd_list_call";
  private final String ASTRA_ETCD_GET_CALL = "astra_etcd_get_call";
  private final String ASTRA_ETCD_UPDATE_CALL = "astra_etcd_update_call";
  private final String ASTRA_ETCD_ADDED_LISTENER = "astra_etcd_added_listener";
  private final String ASTRA_ETCD_REMOVED_LISTENER = "astra_etcd_removed_listener";
  private final String ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED = "astra_etcd_cache_init_handler_fired";
  private final String ASTRA_ETCD_LEASE_REFRESH_HANDLER_FIRED =
      "astra_etcd_lease_refresh_handler_fired";

  private static final String ASTRA_ETCD_WATCH_RETRY = "astra_etcd_watch_retry";
  private static final String ASTRA_ETCD_WATCH_RETRY_DELAY = "astra_etcd_watch_retry_delay";
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
  private final Counter watchRetryError;
  private final Counter watchRetryCompaction;
  private final Counter watchRetryGracefulStop;
  private final Timer watchRetryDelay;
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
    this.meterRegistry = meterRegistry;
    this.shouldCache = shouldCache;
    this.serializer = serializer;
    this.watchers = new ConcurrentHashMap<>();
    this.createMode = createMode;
    this.ephemeralTtlMs = config.getEphemeralNodeTtlMs();
    this.etcdOperationTimeoutMs = config.getOperationsTimeoutMs();

    this.retryTotalDurationMs =
        positiveOrDefault(config.getRetryTotalDurationMs(), DEFAULT_RETRY_TOTAL_DURATION_MS);
    this.maxRetryDelayMs =
        positiveOrDefault(config.getMaxRetryDelayMs(), DEFAULT_MAX_RETRY_DELAY_MS);
    this.initialRetryIntervalMs =
        positiveOrDefault(config.getInitialRetryIntervalMs(), DEFAULT_INITIAL_RETRY_INTERVAL_MS);
    this.keepAliveBackoff =
        new ExponentialBackOff(initialRetryIntervalMs, maxRetryDelayMs, retryTotalDurationMs);

    String store = "/" + storeFolder.split("/")[1];
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
    this.watchRetryError =
        this.meterRegistry.counter(ASTRA_ETCD_WATCH_RETRY, "store", store, "reason", "error");
    this.watchRetryCompaction =
        this.meterRegistry.counter(ASTRA_ETCD_WATCH_RETRY, "store", store, "reason", "compaction");
    this.watchRetryGracefulStop =
        this.meterRegistry.counter(
            ASTRA_ETCD_WATCH_RETRY, "store", store, "reason", "graceful_stop");
    this.watchRetryDelay = this.meterRegistry.timer(ASTRA_ETCD_WATCH_RETRY_DELAY, "store", store);
    this.resyncSkip = this.meterRegistry.counter(ASTRA_ETCD_RESYNC_SKIP, "store", store);

    if (etcdClient == null) {
      throw new IllegalArgumentException("External etcd client must be provided");
    }

    LOG.info(
        "Using provided external etcd client for store folder: {} with mode: {}",
        storeFolder,
        createMode);
    this.etcdClient = etcdClient;

    // Initialize watch retry executor - scheduled cached thread pool that scales to 0
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

  /** Closes a watcher, suppressing any exception. */
  private static void closeQuietly(Watcher watcher) {
    if (watcher != null) {
      try {
        watcher.close();
      } catch (Exception e) {
        LOG.debug("Error closing watcher", e);
      }
    }
  }

  static void handleFatalAsync(Throwable error, String threadNameSuffix) {
    LOG.error("Fatal error detected ({}), initiating shutdown", threadNameSuffix, error);
    Thread t = new Thread(() -> fatalErrorHandler.handleFatal(error));
    t.setName("etcd-fatal-" + threadNameSuffix);
    t.setDaemon(true);
    t.start();
  }

  /**
   * Attempts to schedule a retry using the given backoff. Returns true if a retry was scheduled,
   * false if the backoff budget is exhausted (caller should handle escalation).
   */
  private boolean scheduleRetryWithBackoff(
      ExponentialBackOff backoff, Runnable retryAction, String context) {
    long delayMs = backoff.nextBackOffMillis();
    if (delayMs == ExponentialBackOff.STOP) {
      return false;
    }
    watchRetryDelay.record(delayMs, TimeUnit.MILLISECONDS);
    LOG.warn(
        "{} — retrying in {} ms ({} ms elapsed)", context, delayMs, backoff.getElapsedTimeMs());
    try {
      watchRetryExecutor.schedule(retryAction, delayMs, TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.RejectedExecutionException e) {
      LOG.warn("Retry executor already shut down for {}, retry not scheduled", storeFolder);
      return false;
    }
    return true;
  }

  /**
   * Detects whether a watch error is caused by etcd key-space compaction. When etcd compacts, any
   * watcher holding a revision older than the compacted revision gets an OUT_OF_RANGE error.
   */
  static boolean isCompactionError(Throwable error) {
    if (error instanceof StatusRuntimeException sre) {
      return sre.getStatus().getCode() == Status.Code.OUT_OF_RANGE;
    }
    String msg = error.getMessage();
    return msg != null && msg.contains("compacted");
  }

  /**
   * Detects whether a watch error is a graceful GOAWAY from an etcd node shutting down cleanly.
   * This is not a real error — it's HTTP/2's way of saying "I'm leaving, reconnect elsewhere." The
   * error message from jetcd looks like: "Connection closed after GOAWAY. HTTP/2 error code:
   * NO_ERROR, debug data: graceful_stop"
   */
  static boolean isGracefulStop(Throwable error) {
    String msg = error.getMessage();
    return msg != null && msg.contains("NO_ERROR") && msg.contains("graceful_stop");
  }

  /** Creates KeepAlive GRPC connection and handles error and completed cases */
  private void startKeepAlive() {
    this.etcdClient
        .getLeaseClient()
        .keepAlive(
            sharedLeaseId,
            new StreamObserver<LeaseKeepAliveResponse>() {
              @Override
              public void onNext(LeaseKeepAliveResponse response) {
                LOG.debug(
                    "Received keepAlive response for lease {}, TTL: {}",
                    response.getID(),
                    response.getTTL());
                keepAliveBackoff.reset();
                leaseRefreshHandlerFired.increment();
              }

              @Override
              public void onError(Throwable t) {
                if (isClosing) {
                  LOG.debug(
                      "KeepAlive stream error during shutdown for lease {}, not restarting",
                      sharedLeaseId);
                  return;
                }
                long delayMs = keepAliveBackoff.nextBackOffMillis();
                if (delayMs == ExponentialBackOff.STOP) {
                  LOG.error(
                      "KeepAlive retry budget exhausted for lease {} after {} ms — failing fatally",
                      sharedLeaseId,
                      keepAliveBackoff.getElapsedTimeMs());
                  handleFatalAsync(t, "keepalive-" + sharedLeaseId);
                  return;
                }
                LOG.warn(
                    "KeepAlive error for lease {}: {}. Retrying in {} ms ({} ms elapsed)",
                    sharedLeaseId,
                    t.getMessage(),
                    delayMs,
                    keepAliveBackoff.getElapsedTimeMs());
                try {
                  watchRetryExecutor.schedule(
                      EtcdMetadataStore.this::startKeepAlive, delayMs, TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.RejectedExecutionException e) {
                  LOG.warn(
                      "Retry executor shut down during keepalive retry for lease {}",
                      sharedLeaseId);
                }
              }

              @Override
              public void onCompleted() {
                if (isClosing) {
                  LOG.debug(
                      "KeepAlive stream completed during shutdown for lease {}, not restarting",
                      sharedLeaseId);
                  return;
                }
                LOG.warn("KeepAlive stream completed for shared lease {}", sharedLeaseId);
                if (!scheduleRetryWithBackoff(
                    keepAliveBackoff,
                    EtcdMetadataStore.this::startKeepAlive,
                    "keepalive-" + sharedLeaseId)) {
                  handleFatalAsync(
                      new RuntimeException("keepAlive stream completed, retry budget exhausted"),
                      "keepalive-" + sharedLeaseId);
                }
              }
            });
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
              // Note: deleteResponse.getDeleted() tells us how many keys were deleted
              if (deleteResponse.getDeleted() == 0) {
                throw new InternalMetadataStoreException("Failed to delete node: " + path);
              }

              // Remove from cache if enabled
              if (shouldCache) {
                cache.remove(path);
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

    // Add a trailing slash to the folder to make sure we only list entries directly under this
    // folder
    ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
    GetOption getOption = GetOption.builder().withPrefix(prefix).build();

    return etcdClient
        .getKVClient()
        .get(prefix, getOption)
        .orTimeout(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
        .thenApplyAsync(
            getResponse -> {
              List<T> nodes = new ArrayList<>();

              for (KeyValue kv : getResponse.getKvs()) {
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
      // folder
      ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
      GetOption getOption = GetOption.builder().withPrefix(prefix).build();

      GetResponse getResponse =
          etcdClient
              .getKVClient()
              .get(prefix, getOption)
              .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);

      List<T> nodes = new ArrayList<>();

      for (KeyValue kv : getResponse.getKvs()) {
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
   * Two-tier backoff state for watch retry logic. Transient errors (GOAWAY, compaction) share one
   * budget; when exhausted they escalate to the error tier. Error budget exhaustion is fatal.
   */
  static class WatchRetryState {
    final ExponentialBackOff transientBackoff;
    final ExponentialBackOff errorBackoff;
    private final long initialIntervalMs;

    WatchRetryState(long initialIntervalMs, long maxIntervalMs, long maxElapsedMs) {
      this.initialIntervalMs = initialIntervalMs;
      this.transientBackoff =
          new ExponentialBackOff(initialIntervalMs, maxIntervalMs, maxElapsedMs);
      this.errorBackoff = new ExponentialBackOff(initialIntervalMs, maxIntervalMs, maxElapsedMs);
    }

    /**
     * Resets both backoff tiers if the watcher was alive long enough to be considered healthy. A
     * watcher that survived more than 2x the initial retry interval is treated as a new disruption
     * episode.
     */
    void resetIfNewEpisode(long watcherCreatedTimeMs) {
      long timeSinceCreation = System.currentTimeMillis() - watcherCreatedTimeMs;
      if (timeSinceCreation > initialIntervalMs * 2) {
        transientBackoff.reset();
        errorBackoff.reset();
      }
    }
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param listener The listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> listener) {
    addListener(
        listener,
        REVISION_LATEST,
        new WatchRetryState(initialRetryIntervalMs, maxRetryDelayMs, retryTotalDurationMs));
  }

  /**
   * Internal method to add a listener with retry logic. Transient errors (GOAWAY, compaction) share
   * one backoff budget; when exhausted they escalate to the error tier. Error budget exhaustion is
   * fatal.
   *
   * @param listener The listener to add
   * @param startRevision The ETCD revision number to start at. {@link #REVISION_LATEST} fetches
   *     current revision (keysOnly). {@link #REVISION_RESYNC} triggers a full cache resync
   *     (compaction recovery).
   * @param retryState Two-tier backoff state carried across retries
   */
  private void addListener(
      AstraMetadataStoreChangeListener<T> listener,
      long startRevision,
      WatchRetryState retryState) {
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
    long currentRevision = startRevision;
    try {
      if (startRevision == REVISION_RESYNC) {
        currentRevision = resyncCacheFromEtcd(listener);
      } else if (startRevision == REVISION_LATEST) {
        currentRevision =
            etcdClient
                .getKVClient()
                .get(prefix, GetOption.builder().withPrefix(prefix).withKeysOnly(true).build())
                .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS)
                .getHeader()
                .getRevision();
      }

      // Create watch option starting from the current revision + 1
      // This ensures we don't miss events that occur during watch registration
      // and that we don't replay the last event
      watchOption =
          WatchOption.builder().withPrefix(prefix).withRevision(currentRevision + 1).build();
      LOG.debug(
          "adding listener {} for store {} at revision {}",
          listener,
          storeFolder,
          currentRevision + 1);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to get current revision for watch setup on store {}", storeFolder, e);
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      watchRetryError.increment();
      if (!scheduleRetryWithBackoff(
          retryState.errorBackoff,
          () -> addListener(listener, REVISION_RESYNC, retryState),
          "Revision fetch failed for store " + storeFolder + ": " + e.getMessage())) {
        LOG.error(
            "Watch retry budget exhausted for store {} after {} ms — failing fatally",
            storeFolder,
            retryState.errorBackoff.getElapsedTimeMs());
        handleFatalAsync(e, storeFolder);
      }
      return;
    }

    // Create a watcher for this listener
    long finalCurrentRevision = currentRevision + 1;
    // Records when this watcher was created. Used to distinguish new disruption episodes from
    // continuations of the same failure: if the watcher was alive longer than 2x the base retry
    // delay, a subsequent error is a new episode and the retry window resets.
    long watcherCreatedTimeMs = System.currentTimeMillis();
    AtomicReference<Watcher> watcherRef = new AtomicReference<>();
    Watcher watcher =
        etcdClient
            .getWatchClient()
            .watch(
                prefix,
                watchOption,
                response -> {
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
                  if (isClosing) {
                    LOG.debug("Ignoring watch error during shutdown for {}", storeFolder);
                    return;
                  }

                  String listenerKey = String.valueOf(System.identityHashCode(listener));
                  Watcher existingWatcher = watchers.remove(listenerKey);
                  closeQuietly(existingWatcher != null ? existingWatcher : watcherRef.get());

                  // GOAWAY: server is shutting down cleanly, reconnect with backoff.
                  if (isGracefulStop(error)) {
                    watchRetryGracefulStop.increment();
                    if (scheduleRetryWithBackoff(
                        retryState.transientBackoff,
                        () -> addListener(listener, finalCurrentRevision, retryState),
                        "Watch for store "
                            + storeFolder
                            + " received GOAWAY: "
                            + error.getMessage())) {
                      return;
                    }
                    LOG.error(
                        "Transient retry budget exhausted for store {} after {} ms"
                            + " — treating as real error",
                        storeFolder,
                        retryState.transientBackoff.getElapsedTimeMs());
                    retryState.transientBackoff.reset();
                  } else if (isCompactionError(error)) {
                    // Compaction: revision is behind, need full resync.
                    watchRetryCompaction.increment();
                    if (scheduleRetryWithBackoff(
                        retryState.transientBackoff,
                        () -> addListener(listener, REVISION_RESYNC, retryState),
                        "Watch for store "
                            + storeFolder
                            + " received compaction error: "
                            + error.getMessage())) {
                      return;
                    }
                    LOG.error(
                        "Transient retry budget exhausted for store {} after {} ms"
                            + " — treating as real error",
                        storeFolder,
                        retryState.transientBackoff.getElapsedTimeMs());
                    retryState.transientBackoff.reset();
                  }

                  retryState.resetIfNewEpisode(watcherCreatedTimeMs);

                  watchRetryError.increment();
                  if (scheduleRetryWithBackoff(
                      retryState.errorBackoff,
                      () -> addListener(listener, finalCurrentRevision, retryState),
                      "Watch failed for store " + storeFolder + ": " + error.getMessage())) {
                    return;
                  }

                  LOG.error(
                      "Watch retry budget exhausted for store {} after {} ms — failing fatally",
                      storeFolder,
                      retryState.errorBackoff.getElapsedTimeMs());
                  handleFatalAsync(error, storeFolder);
                });

    watcherRef.set(watcher);
    watchers.put(String.valueOf(System.identityHashCode(listener)), watcher);

    boolean isRetry =
        retryState.transientBackoff.getElapsedTimeMs() > 0
            || retryState.errorBackoff.getElapsedTimeMs() > 0;
    if (!isRetry) {
      LOG.info("Successfully established initial watch for store {}", storeFolder);
    } else {
      LOG.info("Successfully re-established watch for store {}", storeFolder);
    }
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

    String key = System.identityHashCode(listener) + "";
    Watcher watcher = watchers.remove(key);

    if (watcher != null) {
      watcher.close();
    } else {
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
            new RuntimeHalterImpl()
                .handleFatal(
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

  /** Populates the cache with all nodes from etcd. This is called once during initialization. */
  private void populateInitialCache() {
    try {
      LOG.debug("Populating cache for store {}", storeFolder);
      // Get only nodes from this store folder
      ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
      GetOption getOption = GetOption.builder().withPrefix(prefix).build();

      GetResponse getResponse =
          etcdClient
              .getKVClient()
              .get(prefix, getOption)
              .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);

      // Filter for only direct children of the store folder
      for (KeyValue kv : getResponse.getKvs()) {
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
            new RuntimeHalterImpl().handleFatal(e);
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
      LOG.error("Error initializing cache for store {}: {}", storeFolder, e.getMessage());
      new RuntimeHalterImpl()
          .handleFatal(new TimeoutException("Timed out waiting for Etcd cache to initialize"));
    } catch (Exception e) {
      LOG.error("Failed to initialize cache for store {}", storeFolder, e);
      new RuntimeHalterImpl().handleFatal(e);
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
    GetResponse getResponse =
        etcdClient
            .getKVClient()
            .get(prefix, GetOption.builder().withPrefix(prefix).build())
            .get(etcdOperationTimeoutMs, TimeUnit.MILLISECONDS);

    long listRevision = getResponse.getHeader().getRevision();

    // Build the new state from etcd and collect names for delete detection
    Set<String> newKeys = new HashSet<>();
    List<T> newNodes = new ArrayList<>();
    for (KeyValue kv : getResponse.getKvs()) {
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

    // Close all active watchers
    watchers.values().forEach(Watcher::close);
    watchers.clear();

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
