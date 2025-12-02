package com.slack.astra.metadata.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.config.AstraConfigs.EtcdConfig;
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
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

  private StreamObserver<LeaseKeepAliveResponse> leaseRenewalConnection = null;

  /** Used for watch retry operations with delays. */
  private final ScheduledExecutorService watchRetryExecutor;

  /** The create mode for this metadata store instance. */
  private final EtcdCreateMode createMode;

  /** TTL in milliseconds for ephemeral nodes. */
  private final long ephemeralTtlMs;

  private final int ephemeralMaxRetries;

  private static final Logger LOG = LoggerFactory.getLogger(EtcdMetadataStore.class);

  protected final String storeFolder;
  protected final String namespace;
  protected final Client etcdClient;
  protected final ConcurrentHashMap<String, Watcher> watchers;
  protected final ConcurrentHashMap<String, T> cache = new ConcurrentHashMap<>();
  protected final boolean shouldCache;
  protected final MetadataSerializer<T> serializer;
  private final long etcdOperationTimeoutMs;
  private final int etcdOperationsMaxRetries;
  private final long retryDelayMs;

  private final MeterRegistry meterRegistry;

  private final CountDownLatch cacheInitialized = new CountDownLatch(1);

  // These fields are no longer used but kept for compatibility
  private volatile Thread cacheInitThread = null;
  private volatile Thread leaseInitThread = null;

  private final String ASTRA_ETCD_CREATE_CALL = "astra_etcd_create_call";
  private final String ASTRA_ETCD_HAS_CALL = "astra_etcd_has_call";
  private final String ASTRA_ETCD_DELETE_CALL = "astra_etcd_delete_call";
  private final String ASTRA_ETCD_LIST_CALL = "astra_etcd_list_call";
  private final String ASTRA_ETCD_GET_CALL = "astra_etcd_get_call";
  private final String ASTRA_ETCD_UPDATE_CALL = "astra_etcd_update_call";
  private final String ASTRA_ETCD_ADDED_LISTENER = "astra_etcd_added_listener";
  private final String ASTRA_ETCD_REMOVED_LISTENER = "astra_etcd_removed_listener";
  private final String ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED = "astra_etcd_cache_init_handler_fired";
  private final String ASTRA_ETCD_LEASE_KEEPALIVE_RESPONSE_RECEIVED =
      "astra_etcd_lease_keepalive_response_received";

  private final Counter createCall;
  private final Counter hasCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;
  private final Counter cacheInitHandlerFired;
  private final Counter leaseKeepAliveResponseReceived;

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
      Client etcClient) {
    this.storeFolder = storeFolder;
    this.namespace = config.getNamespace();
    this.meterRegistry = meterRegistry;
    this.shouldCache = shouldCache;
    this.serializer = serializer;
    this.watchers = new ConcurrentHashMap<>();
    this.createMode = createMode;
    this.ephemeralTtlMs = config.getEphemeralNodeTtlMs();
    this.ephemeralMaxRetries = config.getEphemeralNodeMaxRetries();
    this.etcdOperationTimeoutMs = config.getOperationsTimeoutMs();

    // Store retry configuration for watch operations
    this.etcdOperationsMaxRetries = Math.max(0, config.getOperationsMaxRetries());
    this.retryDelayMs = Math.max(0, config.getRetryDelayMs());

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
    this.leaseKeepAliveResponseReceived =
        this.meterRegistry.counter(ASTRA_ETCD_LEASE_KEEPALIVE_RESPONSE_RECEIVED, "store", store);

    if (etcClient == null) {
      throw new IllegalArgumentException("External etcd client must be provided");
    }

    LOG.info(
        "Using provided external etcd client for store folder: {} with mode: {}",
        storeFolder,
        createMode);
    this.etcdClient = etcClient;

    // Initialize watch retry executor - scheduled cached thread pool that scales to 0
    this.watchRetryExecutor =
        Executors.newScheduledThreadPool(
            0, // Use 0 core threads so it can scale down completely
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              t.setName("etcd-watch-retry-" + storeFolder);
              return t;
            });

    // Initialize lease keepAlive if we're creating ephemeral nodes
    if (createMode == EtcdCreateMode.EPHEMERAL) {
      // Create a single shared lease for all ephemeral nodes synchronously
      try {
        sharedLeaseId =
            etcdClient
                .getLeaseClient()
                .grant(ephemeralTtlMs / 1000) // grant ttl is in seconds
                .get(ephemeralTtlMs, TimeUnit.MILLISECONDS)
                .getID();
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw new RuntimeException(e);
      }

      LOG.info(
          "Created shared lease {} (HEX: {}) with TTL {} milliseconds for all ephemeral nodes",
          sharedLeaseId,
          Long.toHexString(sharedLeaseId),
          ephemeralTtlMs);

      // Establish the keepAlive stream for the shared lease
      // This will automatically send keepAlive requests periodically
      establishLeaseKeepAlive();
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
   * Adds a listener for metadata changes.
   *
   * @param listener The listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> listener) {
    addListener(listener, 0, 0);
  }

  /**
   * Internal method to add a listener with retry logic.
   *
   * @param listener The listener to add
   * @param attemptNumber The current attempt number (0-based)
   * @param startRevision The ETCD revision number to start at (0 will get current revision of the
   *     db)
   */
  private void addListener(
      AstraMetadataStoreChangeListener<T> listener, int attemptNumber, long startRevision) {
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
      // only get currentRevision if a revision hasn't been specified
      if (startRevision == 0) {
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
      LOG.error("Failed to get current revision for watch setup on attempt {}", attemptNumber, e);
      // Fallback to basic watch without revision
      watchOption = WatchOption.builder().withPrefix(prefix).build();
    }

    // Create a watcher for this listener
    long finalCurrentRevision = currentRevision + 1;
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
                  // This is an enhancement to the retry logic for watchers, as there appears to be
                  // an issue in jetcd
                  // https://github.com/etcd-io/jetcd/issues/1352
                  LOG.error(
                      "Watch failed for store {} on attempt {}: {}",
                      storeFolder,
                      attemptNumber,
                      error.getMessage());

                  // Close the failed watcher
                  String listenerKey = String.valueOf(System.identityHashCode(listener));
                  Watcher existingWatcher = watchers.remove(listenerKey);
                  if (existingWatcher != null) {
                    try {
                      existingWatcher.close();
                    } catch (Exception e) {
                      LOG.debug("Error closing failed watcher", e);
                    }
                  }

                  // Retry if we haven't exceeded max attempts
                  if (attemptNumber < etcdOperationsMaxRetries) {
                    long delayMs = retryDelayMs > 0 ? retryDelayMs : 1000;
                    LOG.info(
                        "Retrying watch establishment for store {} in {} ms (attempt {} of {})",
                        storeFolder,
                        delayMs,
                        attemptNumber + 1,
                        etcdOperationsMaxRetries);

                    // Schedule retry using the dedicated watch retry executor with delay
                    // retry with the same revision number so we don't miss events on this async
                    // operation
                    watchRetryExecutor.schedule(
                        () -> addListener(listener, attemptNumber + 1, finalCurrentRevision),
                        delayMs,
                        TimeUnit.MILLISECONDS);
                  } else {
                    LOG.error(
                        "Failed to establish watch for store {} after {} attempts, failing fatally",
                        storeFolder,
                        etcdOperationsMaxRetries);
                    new RuntimeHalterImpl().handleFatal(error);
                  }
                });

    // Store the watcher so we can close it later
    watchers.put(String.valueOf(System.identityHashCode(listener)), watcher);

    if (attemptNumber == 0) {
      LOG.info("Successfully established initial watch for store {}", storeFolder);
    } else {
      LOG.info(
          "Successfully re-established watch for store {} after {} retries",
          storeFolder,
          attemptNumber);
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
        if (keyStr.startsWith(storeFolder + "/")
            && !keyStr.substring((storeFolder + "/").length()).contains("/")) {
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
   * Establishes a keepAlive stream for the shared lease. The etcd client library will automatically
   * send keepAlive requests periodically to keep the lease alive. If the stream fails, we mark it
   * as null so it can be re-established if needed.
   */
  private void establishLeaseKeepAlive() {
    if (sharedLeaseId == -1) {
      LOG.warn("Cannot establish keepAlive - lease not yet initialized");
      return;
    }

    if (leaseRenewalConnection != null) {
      LOG.debug("KeepAlive stream already established for lease {}", sharedLeaseId);
      return;
    }

    LOG.info("Establishing keepAlive stream for shared lease {}", sharedLeaseId);

    leaseRenewalConnection =
        new StreamObserver<LeaseKeepAliveResponse>() {
          @Override
          public void onNext(LeaseKeepAliveResponse response) {
            LOG.trace(
                "Received keepAlive response for lease {}, TTL: {}",
                response.getID(),
                response.getTTL());
            leaseKeepAliveResponseReceived.increment();
          }

          @Override
          public void onError(Throwable t) {
            LOG.error(
                "Error in keepAlive stream for shared lease {}: {}", sharedLeaseId, t.getMessage());
            // Reset the connection so it can be re-established
            leaseRenewalConnection = null;
            // Try to re-establish the connection
            establishLeaseKeepAlive();
          }

          @Override
          public void onCompleted() {
            LOG.warn("KeepAlive stream completed for shared lease {}", sharedLeaseId);
            // Reset the connection so it can be re-established
            leaseRenewalConnection = null;
            // Try to re-establish the connection
            establishLeaseKeepAlive();
          }
        };

    int retryCounter = 0;
    while (retryCounter <= ephemeralMaxRetries) {
      try {
        // Establish the keepAlive stream - this will continuously send keepAlive requests
        etcdClient.getLeaseClient().keepAlive(sharedLeaseId, leaseRenewalConnection);
        LOG.info("Successfully established keepAlive stream for shared lease {}", sharedLeaseId);
        break;
      } catch (Exception e) {
        retryCounter++;
        if (retryCounter >= ephemeralMaxRetries) {
          LOG.error(
              "Failed to establish keepAlive stream after {} attempts, fataling: {}",
              ephemeralMaxRetries,
              e.getMessage());
          // This is a critical error since it affects all ephemeral nodes
          new RuntimeHalterImpl().handleFatal(e);
        }
        LOG.warn(
            "Failed to establish keepAlive stream (attempt {}), retrying: {}",
            retryCounter,
            e.getMessage());
        try {
          Thread.sleep(1000); // Wait a bit before retrying
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  @Override
  public void close() {
    LOG.info("Closing etcd clients and watchers");
    // Close all active watchers
    watchers.values().forEach(Watcher::close);
    watchers.clear();

    // Mark cache as initialized to unblock any waiting calls to awaitCacheInitialized
    if (cacheInitialized.getCount() > 0) {
      cacheInitialized.countDown();
    }

    // Terminate the cache initialization thread if it's still running
    if (cacheInitThread != null && cacheInitThread.isAlive()) {
      LOG.info("Interrupting cache initialization thread");
      cacheInitThread.interrupt();
      // No need to wait - virtual threads are cheap to discard
    }

    // Terminate the init thread if it's sitll running
    if (leaseInitThread != null && leaseInitThread.isAlive()) {
      LOG.info("Interrupting lease initialization thread");
      leaseInitThread.interrupt();
      // No need to wait - virtual threads are cheap to discard
    }

    // Shut down watch retry executor
    if (watchRetryExecutor != null) {
      watchRetryExecutor.shutdownNow();
      try {
        watchRetryExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ignored) {
      }
    }

    // Clear the lease renewal connection reference and revoke the lease if we have one
    if (leaseRenewalConnection != null) {
      LOG.info("Clearing lease renewal connection for shared lease {}", sharedLeaseId);
      leaseRenewalConnection = null;
    }

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
