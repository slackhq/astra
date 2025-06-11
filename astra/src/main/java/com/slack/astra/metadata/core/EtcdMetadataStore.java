package com.slack.astra.metadata.core;

import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.config.AstraConfigs.EtcdConfig;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.Watch.Watcher;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
  /** Tracks leases used for ephemeral nodes. Maps key name to lease ID. */
  private final ConcurrentHashMap<String, Long> leases = new ConcurrentHashMap<>();

  /** Used for refreshing ephemeral node leases. */
  private final ScheduledExecutorService leaseRefreshExecutor;

  /** The create mode for this metadata store instance. */
  private final EtcdCreateMode createMode;

  /** TTL in seconds for ephemeral nodes. */
  private final long ephemeralTtlSeconds;

  /** Lease object for managing leases in etcd. */
  protected final Lease leaseClient;

  private static final Logger LOG = LoggerFactory.getLogger(EtcdMetadataStore.class);

  protected final String storeFolder;
  protected final String namespace;
  protected final Client etcdClient;
  protected final KV kvClient;
  protected final Watch watchClient;
  protected final ConcurrentHashMap<String, Watcher> watchers;
  protected final ConcurrentHashMap<String, T> cache;
  protected final boolean shouldCache;
  protected final MetadataSerializer<T> serializer;

  private static final int DEFAULT_TIMEOUT_SECONDS = 30;

  private final MeterRegistry meterRegistry;

  private final String ASTRA_ETCD_CREATE_CALL = "astra_etcd_create_call";
  private final String ASTRA_ETCD_HAS_CALL = "astra_etcd_has_call";
  private final String ASTRA_ETCD_DELETE_CALL = "astra_etcd_delete_call";
  private final String ASTRA_ETCD_LIST_CALL = "astra_etcd_list_call";
  private final String ASTRA_ETCD_GET_CALL = "astra_etcd_get_call";
  private final String ASTRA_ETCD_UPDATE_CALL = "astra_etcd_update_call";
  private final String ASTRA_ETCD_ADDED_LISTENER = "astra_etcd_added_listener";
  private final String ASTRA_ETCD_REMOVED_LISTENER = "astra_etcd_removed_listener";
  private final String ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED = "astra_etcd_cache_init_handler_fired";

  private final Counter createCall;
  private final Counter hasCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;

  public EtcdMetadataStore(
      String storeFolder,
      EtcdConfig config,
      boolean shouldCache,
      MeterRegistry meterRegistry,
      MetadataSerializer<T> serializer) {
    this(
        storeFolder,
        config,
        shouldCache,
        meterRegistry,
        serializer,
        EtcdCreateMode.PERSISTENT,
        config.getEphemeralNodeTtlSeconds() > 0
            ? config.getEphemeralNodeTtlSeconds()
            : EtcdCreateMode.DEFAULT_EPHEMERAL_TTL_SECONDS);
  }

  /**
   * Constructor with full parameters including create mode.
   *
   * @param storeFolder the folder to store data in
   * @param config the etcd configuration
   * @param shouldCache whether to cache data
   * @param meterRegistry metrics registry
   * @param serializer serializer for the metadata type
   * @param createMode whether to create persistent or ephemeral nodes
   * @param ephemeralTtlSeconds TTL in seconds for ephemeral nodes
   */
  public EtcdMetadataStore(
      String storeFolder,
      EtcdConfig config,
      boolean shouldCache,
      MeterRegistry meterRegistry,
      MetadataSerializer<T> serializer,
      EtcdCreateMode createMode,
      long ephemeralTtlSeconds) {
    this.storeFolder = storeFolder;
    this.namespace = config.getNamespace();
    this.meterRegistry = meterRegistry;
    this.shouldCache = shouldCache;
    this.serializer = serializer;
    this.cache = shouldCache ? new ConcurrentHashMap<>() : null;
    String store = storeFolder.replace('/', '_');

    // Initialize etcd client
    ClientBuilder clientBuilder = Client.builder();

    // Configure endpoints
    if (config.getEndpointsList() != null && !config.getEndpointsList().isEmpty()) {
      clientBuilder.endpoints(config.getEndpointsList().toArray(new String[0]));
    }

    // Configure timeouts
    if (config.getConnectionTimeoutMs() > 0) {
      clientBuilder.connectTimeout(Duration.ofMillis(config.getConnectionTimeoutMs()));
    }

    if (config.getKeepaliveTimeoutMs() > 0) {
      clientBuilder.keepaliveTimeout(Duration.ofMillis(config.getKeepaliveTimeoutMs()));
    }

    // Configure retries
    if (config.getMaxRetries() > 0) {
      clientBuilder.retryMaxAttempts(config.getMaxRetries());
    }

    if (config.getRetryDelayMs() > 0) {
      clientBuilder.retryDelay(config.getRetryDelayMs());
    }

    // Set namespace if provided
    if (!Strings.isNullOrEmpty(config.getNamespace())) {
      clientBuilder.namespace(ByteSequence.from(config.getNamespace(), StandardCharsets.UTF_8));
    }

    LOG.info(
        "Initializing etcd client with store folder: {} and namespace: {}, mode: {}",
        storeFolder,
        config.getNamespace(),
        createMode);
    this.etcdClient = clientBuilder.build();
    this.kvClient = this.etcdClient.getKVClient();
    this.watchClient = this.etcdClient.getWatchClient();
    this.leaseClient = this.etcdClient.getLeaseClient();
    this.watchers = new ConcurrentHashMap<>();

    this.createMode = createMode;
    this.ephemeralTtlSeconds = ephemeralTtlSeconds;

    // Initialize lease refresh executor if we're creating ephemeral nodes
    if (createMode == EtcdCreateMode.EPHEMERAL) {
      this.leaseRefreshExecutor =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("etcd-lease-refresh-" + storeFolder);
                return t;
              });

      // Calculate the refresh interval (default to 1/4 of the TTL)
      long refreshIntervalMs =
          (long) (ephemeralTtlSeconds * EtcdCreateMode.DEFAULT_REFRESH_INTERVAL_FRACTION * 1000);

      LOG.info("Starting lease refresh thread with interval: {} ms", refreshIntervalMs);

      // Start the refresh task
      leaseRefreshExecutor.scheduleAtFixedRate(
          this::refreshAllLeases, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
    } else {
      this.leaseRefreshExecutor = null;
    }

    // Initialize cache if needed
    if (shouldCache) {
      LOG.info("Cache enabled for etcd store: {}", storeFolder);
      // Initial population of cache will happen on first access or via awaitCacheInitialized
    }

    this.createCall = this.meterRegistry.counter(ASTRA_ETCD_CREATE_CALL, "store", store);
    this.deleteCall = this.meterRegistry.counter(ASTRA_ETCD_DELETE_CALL, "store", store);
    this.listCall = this.meterRegistry.counter(ASTRA_ETCD_LIST_CALL, "store", store);
    this.getCall = this.meterRegistry.counter(ASTRA_ETCD_GET_CALL, "store", store);
    this.hasCall = this.meterRegistry.counter(ASTRA_ETCD_HAS_CALL, "store", store);
    this.updateCall = this.meterRegistry.counter(ASTRA_ETCD_UPDATE_CALL, "store", store);
    this.addedListener = this.meterRegistry.counter(ASTRA_ETCD_ADDED_LISTENER, "store", store);
    this.removedListener = this.meterRegistry.counter(ASTRA_ETCD_REMOVED_LISTENER, "store", store);
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
    this.createCall.increment();

    try {
      ByteSequence key = pathToKey(metadataNode.getName());
      ByteSequence value =
          ByteSequence.from(serializer.toJsonStr(metadataNode), StandardCharsets.UTF_8);

      if (createMode == EtcdCreateMode.PERSISTENT) {
        // For persistent nodes, just do a regular put
        return kvClient
            .put(key, value)
            .thenApply(
                putResponse -> {
                  // Update cache if enabled
                  if (shouldCache) {
                    cache.put(metadataNode.getName(), metadataNode);
                  }
                  return metadataNode.getName();
                });
      } else {
        // For ephemeral nodes, create a lease and attach it to the key
        return leaseClient
            .grant(ephemeralTtlSeconds)
            .thenCompose(
                leaseGrantResponse -> {
                  long leaseId = leaseGrantResponse.getID();

                  // Store the lease ID for future refreshes
                  leases.put(metadataNode.getName(), leaseId);

                  // Create a put option that associates the key with the lease
                  PutOption putOption = PutOption.newBuilder().withLeaseId(leaseId).build();

                  return kvClient
                      .put(key, value, putOption)
                      .thenApply(
                          putResponse -> {
                            LOG.debug(
                                "Created ephemeral node {} with lease ID {}, TTL {} seconds",
                                metadataNode.getName(),
                                leaseId,
                                ephemeralTtlSeconds);

                            // Update cache if enabled
                            if (shouldCache) {
                              cache.put(metadataNode.getName(), metadataNode);
                            }
                            return metadataNode.getName();
                          });
                });
      }
    } catch (InvalidProtocolBufferException e) {
      CompletableFuture<String> future = new CompletableFuture<>();
      future.completeExceptionally(e);
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
          .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to create node: {}", metadataNode.getName(), e);
      throw new RuntimeException("Failed to create node", e);
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

    // Check cache first if enabled
    if (shouldCache && cache.containsKey(path)) {
      return CompletableFuture.completedFuture(cache.get(path));
    }

    ByteSequence key = pathToKey(path);
    return kvClient
        .get(key)
        .thenApply(
            getResponse -> {
              if (getResponse.getKvs().isEmpty()) {
                throw new RuntimeException("Node not found: " + path);
              }

              KeyValue kv = getResponse.getKvs().get(0);
              try {
                String json = kv.getValue().toString(StandardCharsets.UTF_8);
                T node = serializer.fromJsonStr(json);

                // Update cache if enabled
                if (shouldCache) {
                  cache.put(path, node);
                }

                return node;
              } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException("Failed to deserialize node", e);
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

    // Check cache first if enabled
    if (shouldCache && cache.containsKey(path)) {
      return cache.get(path);
    }

    try {
      return getAsync(path).toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      LOG.error("Failed to get node: {}", path, e);
      throw new RuntimeException("Failed to get node", e);
    } catch (ExecutionException e) {
      // This could be due to node not found (which is handled by returning null from getAsync)
      // or other exceptions from getAsync that are wrapped in ExecutionException
      if (e.getCause() instanceof RuntimeException
          && "Failed to deserialize node".equals(e.getCause().getMessage())) {
        // Rethrow deserialization failures
        throw (RuntimeException) e.getCause();
      }
      // For any other exception, log and return null
      LOG.error("Failed to get node: {}", path, e.getCause());
      return null;
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

    // Check cache first if enabled
    if (shouldCache && cache.containsKey(path)) {
      return CompletableFuture.completedFuture(true);
    }

    ByteSequence key = pathToKey(path);
    return kvClient
        .get(key)
        .thenApply(
            getResponse -> {
              return !getResponse.getKvs().isEmpty();
            });
  }

  /**
   * Checks if a node exists synchronously.
   *
   * @param path The path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String path) {
    this.hasCall.increment();

    // Check cache first if enabled
    if (shouldCache && cache.containsKey(path)) {
      return true;
    }

    try {
      return hasAsync(path).toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to check if node exists: {}", path, e);
      throw new RuntimeException("Error fetching node at path " + path, e);
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

      return kvClient
          .put(key, value)
          .thenApply(
              putResponse -> {
                // Update cache if enabled
                if (shouldCache) {
                  cache.put(metadataNode.getName(), metadataNode);
                }
                return metadataNode.getName();
              });
    } catch (InvalidProtocolBufferException e) {
      CompletableFuture<String> future = new CompletableFuture<>();
      future.completeExceptionally(e);
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
          .toCompletableFuture()
          .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to update node: {}", metadataNode.getName(), e);
      throw new RuntimeException("Failed to update node", e);
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
    return kvClient
        .delete(key)
        .thenAccept(
            deleteResponse -> {
              // Note: deleteResponse.getDeleted() tells us how many keys were deleted
              // We could log this, but we'll silently succeed if 0 keys were deleted

              // Remove from cache if enabled
              if (shouldCache && cache != null) {
                cache.remove(path);
              }

              // If this was an ephemeral node, revoke its lease
              Long leaseId = leases.remove(path);
              if (leaseId != null) {
                try {
                  leaseClient.revoke(leaseId);
                  LOG.debug("Revoked lease {} for deleted node {}", leaseId, path);
                } catch (Exception e) {
                  LOG.warn("Failed to revoke lease for node {}: {}", path, e.getMessage());
                }
              }
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
      deleteAsync(path).toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to delete node: {}", path, e);
      throw new RuntimeException("Failed to delete node", e);
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
          .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to delete node: {}", metadataNode.getName(), e);
      throw new RuntimeException("Failed to delete node", e);
    }
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return A CompletionStage that completes with the list of all nodes
   */
  public CompletionStage<List<T>> listAsync() {
    this.listCall.increment();

    // First check if everything is in cache
    if (shouldCache && cache != null && !cache.isEmpty()) {
      List<T> cachedNodes = new ArrayList<>(cache.values());
      return CompletableFuture.completedFuture(cachedNodes);
    }

    // Add a trailing slash to the folder to make sure we only list entries directly under this
    // folder
    ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
    GetOption getOption = GetOption.newBuilder().withPrefix(prefix).build();

    return kvClient
        .get(prefix, getOption)
        .thenApply(
            getResponse -> {
              List<T> nodes = new ArrayList<>();

              for (KeyValue kv : getResponse.getKvs()) {
                try {
                  String json = kv.getValue().toString(StandardCharsets.UTF_8);
                  T node = serializer.fromJsonStr(json);
                  nodes.add(node);

                  // Update cache if enabled
                  if (shouldCache && cache != null) {
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
    this.listCall.increment();

    // First check if everything is in cache
    if (shouldCache && cache != null && !cache.isEmpty()) {
      return new ArrayList<>(cache.values());
    }

    try {
      return listAsync().toCompletableFuture().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.error("Failed to list nodes", e);
      throw new RuntimeException("Failed to list nodes", e);
    }
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param listener The listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> listener) {
    this.addedListener.increment();

    if (listener == null) {
      LOG.warn("Attempted to add null listener, ignoring");
      return;
    }

    // Add a trailing slash to the folder to make sure we only watch entries directly under this
    // folder
    ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
    WatchOption watchOption = WatchOption.newBuilder().withPrefix(prefix).build();

    // Create a watcher for this listener
    Watcher watcher =
        watchClient.watch(
            prefix,
            watchOption,
            response -> {
              for (WatchEvent event : response.getEvents()) {
                try {
                  // Extract the path from the key
                  String path = keyToName(event.getKeyValue().getKey());

                  // Handle different event types
                  switch (event.getEventType()) {
                    case PUT:
                      // This could be a create or update
                      String json = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                      T node = serializer.fromJsonStr(json);

                      // Update cache if enabled
                      if (shouldCache && cache != null) {
                        cache.put(path, node);
                      }

                      // Notify listener of changes only for create/update
                      listener.onMetadataStoreChanged(node);
                      break;

                    case DELETE:
                      // Remove from cache if enabled
                      if (shouldCache && cache != null) {
                        T deletedNode = cache.remove(path);
                        // We can only notify if we have the node in cache
                        if (deletedNode != null) {
                          listener.onMetadataStoreChanged(deletedNode);
                        }
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

    // Store the watcher so we can close it later
    watchers.put(System.identityHashCode(listener) + "", watcher);
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param listener The listener to remove
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> listener) {
    this.removedListener.increment();

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
   * Waits for the cache to be initialized. This implementation populates the cache if it's enabled.
   */
  public void awaitCacheInitialized() {
    if (shouldCache && cache != null) {
      // Populate cache by listing all nodes directly under this store folder
      try {
        // Clear the cache first, to avoid accumulating data from previous tests
        cache.clear();

        // Get only nodes from this store folder
        ByteSequence prefix = ByteSequence.from(storeFolder + "/", StandardCharsets.UTF_8);
        GetOption getOption = GetOption.newBuilder().withPrefix(prefix).build();

        GetResponse getResponse =
            kvClient.get(prefix, getOption).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Filter for only direct children of the store folder
        for (KeyValue kv : getResponse.getKvs()) {
          String keyStr = kv.getKey().toString(StandardCharsets.UTF_8);
          // Only include direct children of the store folder
          if (keyStr.startsWith(storeFolder + "/")
              && !keyStr.substring((storeFolder + "/").length()).contains("/")) {
            try {
              String json = kv.getValue().toString(StandardCharsets.UTF_8);
              T node = serializer.fromJsonStr(json);
              cache.put(node.getName(), node);
            } catch (InvalidProtocolBufferException e) {
              LOG.error("Failed to deserialize node from key: {}", kv.getKey(), e);
            }
          }
        }

        LOG.info("Initialized cache with {} nodes", cache.size());
      } catch (Exception e) {
        LOG.error("Failed to initialize cache", e);
      }
    }
  }

  /** Refreshes all leases for ephemeral nodes to prevent them from expiring. */
  private void refreshAllLeases() {
    if (leases.isEmpty()) {
      return;
    }

    LOG.debug("Refreshing {} leases for ephemeral nodes", leases.size());

    // Keep track of leases that failed to refresh so we can remove them
    List<String> failedLeases = new ArrayList<>();

    for (Map.Entry<String, Long> entry : leases.entrySet()) {
      String key = entry.getKey();
      long leaseId = entry.getValue();

      try {
        // Keep alive once to extend the TTL
        leaseClient.keepAliveOnce(leaseId).get(5, TimeUnit.SECONDS);
        LOG.trace("Refreshed lease {} for key {}", leaseId, key);
      } catch (Exception e) {
        LOG.warn("Failed to refresh lease {} for key {}: {}", leaseId, key, e.getMessage());
        failedLeases.add(key);
      }
    }

    // Remove failed leases from our tracking
    for (String key : failedLeases) {
      leases.remove(key);
    }
  }

  @Override
  public void close() {
    LOG.info("Closing etcd clients and watchers");
    // Close all active watchers
    watchers.values().forEach(Watcher::close);
    watchers.clear();

    // Shut down lease refresh executor if it exists
    if (leaseRefreshExecutor != null) {
      leaseRefreshExecutor.shutdownNow();
      try {
        leaseRefreshExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    // Close etcd clients
    if (kvClient != null) {
      kvClient.close();
    }

    if (watchClient != null) {
      watchClient.close();
    }

    if (leaseClient != null) {
      leaseClient.close();
    }

    if (etcdClient != null) {
      etcdClient.close();
    }
  }
}
