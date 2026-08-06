package com.slack.astra.metadata.core;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

/**
 * A DynamoDB-backed metadata store (POC), providing the same public surface as {@link
 * EtcdMetadataStore} so it can be exercised in isolation by tests. This class is NOT wired into the
 * {@link AstraMetadataStore} ZK/etcd bridge; it is a standalone feasibility proof.
 *
 * <p>Data model (single table, single-table style):
 *
 * <ul>
 *   <li>partition key {@code pk} = this store's partition value (the store folder for a
 *       non-partitioned store; {@code storeFolder + "/" + partitionId} for a partitioned one)
 *   <li>sort key {@code sk} = the node name ({@link AstraMetadata#getName()})
 *   <li>{@code payload} = protobuf-as-JSON from the shared {@link MetadataSerializer}
 *   <li>{@code ttl} = epoch-seconds expiry, present only for ephemeral nodes
 * </ul>
 *
 * <p>Ephemeral nodes are emulated with a DynamoDB TTL attribute plus a background heartbeat that
 * bumps the {@code ttl}. DynamoDB TTL deletion is best-effort (and DynamoDB Local never sweeps at
 * all), so liveness does NOT rely on the physical delete: the cache is expiry-aware (a reaper drops
 * logically-expired entries and fires listener removals) and the non-cached read path filters
 * {@code ttl < now}. This reproduces ZK/etcd liveness semantics regardless of when — or whether —
 * DynamoDB physically removes the item.
 *
 * <p>Watch/subscribe is driven by a {@link DynamoDbStreamPoller} that keeps the in-memory cache in
 * sync and fans events out to registered listeners, mirroring {@link EtcdMetadataStore}'s design.
 */
public class DynamoDbMetadataStore<T extends AstraMetadata> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbMetadataStore.class);

  static final String PK = "pk";
  static final String SK = "sk";
  static final String PAYLOAD = "payload";
  static final String TTL = "ttl";

  /** A cached node plus its expiry (epoch seconds; 0 means persistent / never expires). */
  private record CacheEntry<T>(T node, long expiryEpochSeconds) {
    boolean isExpired(long nowSeconds) {
      return expiryEpochSeconds > 0 && expiryEpochSeconds < nowSeconds;
    }
  }

  private final DynamoDbAsyncClient client;
  private final DynamoDbStreamsAsyncClient streamsClient;
  private final String tableName;
  private final String partitionValue;
  private final boolean shouldCache;
  private final EtcdCreateMode createMode;
  private final long ephemeralTtlMs;
  private final long operationTimeoutMs;
  private final MetadataSerializer<T> serializer;

  private final ConcurrentHashMap<String, CacheEntry<T>> cache = new ConcurrentHashMap<>();
  private final CountDownLatch cacheInitialized = new CountDownLatch(1);
  private final List<AstraMetadataStoreChangeListener<T>> listeners = new CopyOnWriteArrayList<>();

  // Names of ephemeral nodes this store owns, re-heartbeated on an interval.
  private final ConcurrentHashMap<String, T> ephemeralNodes = new ConcurrentHashMap<>();
  private final ScheduledExecutorService maintenanceExecutor;
  private final DynamoDbStreamPoller streamPoller;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final Counter createCall;
  private final Counter getCall;
  private final Counter hasCall;
  private final Counter updateCall;
  private final Counter deleteCall;
  private final Counter listCall;

  public DynamoDbMetadataStore(
      DynamoDbAsyncClient client,
      DynamoDbStreamsAsyncClient streamsClient,
      DynamoDbConfig config,
      String storeFolder,
      boolean shouldCache,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      MeterRegistry meterRegistry) {
    this(
        client,
        streamsClient,
        config,
        storeFolder,
        storeFolder,
        shouldCache,
        createMode,
        serializer,
        meterRegistry);
  }

  /**
   * @param partitionValue the value to use for the {@code pk} attribute. For a non-partitioned
   *     store this equals {@code storeFolder}; the partitioning store passes {@code storeFolder +
   *     "/" + partitionId}.
   */
  DynamoDbMetadataStore(
      DynamoDbAsyncClient client,
      DynamoDbStreamsAsyncClient streamsClient,
      DynamoDbConfig config,
      String storeFolder,
      String partitionValue,
      boolean shouldCache,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      MeterRegistry meterRegistry) {
    this.client = client;
    this.streamsClient = streamsClient;
    this.tableName = config.getTableName();
    this.partitionValue = partitionValue;
    this.shouldCache = shouldCache;
    this.createMode = createMode;
    this.ephemeralTtlMs =
        EtcdMetadataStore.positiveOrDefault(config.getEphemeralNodeTtlMs(), 60_000);
    this.operationTimeoutMs =
        EtcdMetadataStore.positiveOrDefault(config.getOperationsTimeoutMs(), 60_000);
    this.serializer = serializer;

    String storeTag = storeFolder.startsWith("/") ? storeFolder : "/" + storeFolder;
    this.createCall = meterRegistry.counter("astra_dynamodb_create_call", "store", storeTag);
    this.getCall = meterRegistry.counter("astra_dynamodb_get_call", "store", storeTag);
    this.hasCall = meterRegistry.counter("astra_dynamodb_has_call", "store", storeTag);
    this.updateCall = meterRegistry.counter("astra_dynamodb_update_call", "store", storeTag);
    this.deleteCall = meterRegistry.counter("astra_dynamodb_delete_call", "store", storeTag);
    this.listCall = meterRegistry.counter("astra_dynamodb_list_call", "store", storeTag);

    boolean ephemeral = createMode == EtcdCreateMode.EPHEMERAL;
    if (ephemeral || shouldCache) {
      this.maintenanceExecutor =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("dynamodb-maintenance-%d")
                  .setDaemon(true)
                  .build());
    } else {
      this.maintenanceExecutor = null;
    }

    if (ephemeral) {
      long intervalMs = Math.max(ephemeralTtlMs / 3, 1_000);
      maintenanceExecutor.scheduleAtFixedRate(
          this::heartbeat, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    if (shouldCache) {
      populateInitialCache();
      // A local reaper stands in for DynamoDB's (best-effort, and locally absent) TTL sweeper so
      // ephemeral liveness is correct regardless of the backend's physical delete timing.
      maintenanceExecutor.scheduleWithFixedDelay(
          this::reapExpired, 1_000, 1_000, TimeUnit.MILLISECONDS);
      this.streamPoller =
          new DynamoDbStreamPoller(
              streamsClient,
              client,
              tableName,
              partitionValue::equals,
              this::onStreamChange,
              operationTimeoutMs);
      this.streamPoller.start();
    } else {
      this.streamPoller = null;
      cacheInitialized.countDown();
    }
  }

  // ---- key helpers -------------------------------------------------------

  private Map<String, AttributeValue> keyFor(String name) {
    return Map.of(
        PK, AttributeValue.fromS(partitionValue),
        SK, AttributeValue.fromS(name));
  }

  private Map<String, AttributeValue> itemFor(T node, String json) {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(PK, AttributeValue.fromS(partitionValue));
    item.put(SK, AttributeValue.fromS(node.getName()));
    item.put(PAYLOAD, AttributeValue.fromS(json));
    if (createMode == EtcdCreateMode.EPHEMERAL) {
      item.put(TTL, AttributeValue.fromN(Long.toString(expiryEpochSeconds())));
    }
    return item;
  }

  private long expiryEpochSeconds() {
    return (System.currentTimeMillis() + ephemeralTtlMs) / 1000;
  }

  static long nowSeconds() {
    return System.currentTimeMillis() / 1000;
  }

  /** Parses the ttl attribute (epoch seconds) from an item; 0 when absent. */
  private static long ttlOf(Map<String, AttributeValue> item) {
    AttributeValue ttl = item == null ? null : item.get(TTL);
    if (ttl == null || ttl.n() == null) {
      return 0;
    }
    return Long.parseLong(ttl.n());
  }

  /** True when an item carries a ttl attribute already in the past. */
  static boolean isExpiredItem(Map<String, AttributeValue> item) {
    long ttl = ttlOf(item);
    return ttl > 0 && ttl < nowSeconds();
  }

  private T deserialize(Map<String, AttributeValue> item) {
    try {
      return serializer.fromJsonStr(item.get(PAYLOAD).s());
    } catch (InvalidProtocolBufferException e) {
      throw new InternalMetadataStoreException("Failed to deserialize node", e);
    }
  }

  private CacheEntry<T> entryFor(Map<String, AttributeValue> item) {
    return new CacheEntry<>(deserialize(item), ttlOf(item));
  }

  // ---- create ------------------------------------------------------------

  public CompletionStage<String> createAsync(T metadataNode) {
    createCall.increment();
    String name = metadataNode.getName();
    if (name == null || name.isEmpty() || "/".equals(name) || ".".equals(name)) {
      return failed(new InternalMetadataStoreException("Invalid node name: " + name));
    }
    final String json;
    try {
      json = serializer.toJsonStr(metadataNode);
    } catch (InvalidProtocolBufferException e) {
      return failed(new InternalMetadataStoreException("Failed to serialize node", e));
    }

    return client
        .putItem(
            b ->
                b.tableName(tableName)
                    .item(itemFor(metadataNode, json))
                    // create-if-not-exists: matches etcd's pre-get "already exists" behavior
                    .conditionExpression("attribute_not_exists(" + SK + ")"))
        .handle(
            (resp, throwable) -> {
              if (throwable != null) {
                throw new InternalMetadataStoreException(
                    "Failed to create node (may already exist): " + name, unwrap(throwable));
              }
              if (createMode == EtcdCreateMode.EPHEMERAL) {
                ephemeralNodes.put(name, metadataNode);
              }
              if (shouldCache) {
                cache.put(
                    name,
                    new CacheEntry<>(
                        metadataNode,
                        createMode == EtcdCreateMode.EPHEMERAL ? expiryEpochSeconds() : 0));
              }
              return name;
            });
  }

  public void createSync(T metadataNode) {
    await(createAsync(metadataNode), "create " + metadataNode.getName());
  }

  // ---- get ---------------------------------------------------------------

  public CompletionStage<T> getAsync(String path) {
    getCall.increment();
    if (shouldCache) {
      awaitCacheInitialized();
      CacheEntry<T> cached = cache.get(path);
      if (cached != null && !cached.isExpired(nowSeconds())) {
        return CompletableFuture.completedFuture(cached.node());
      }
      return failed(new InternalMetadataStoreException("Node not found: " + path));
    }
    return client
        .getItem(b -> b.tableName(tableName).key(keyFor(path)))
        .thenApply(
            resp -> {
              if (!resp.hasItem() || resp.item().isEmpty() || isExpiredItem(resp.item())) {
                throw new InternalMetadataStoreException("Node not found: " + path);
              }
              return deserialize(resp.item());
            });
  }

  public T getSync(String path) {
    return await(getAsync(path), "get " + path);
  }

  // ---- has ---------------------------------------------------------------

  public CompletionStage<Boolean> hasAsync(String path) {
    hasCall.increment();
    if (shouldCache) {
      awaitCacheInitialized();
      CacheEntry<T> cached = cache.get(path);
      return CompletableFuture.completedFuture(cached != null && !cached.isExpired(nowSeconds()));
    }
    return client
        .getItem(b -> b.tableName(tableName).key(keyFor(path)))
        .thenApply(resp -> resp.hasItem() && !resp.item().isEmpty() && !isExpiredItem(resp.item()));
  }

  public boolean hasSync(String path) {
    return await(hasAsync(path), "has " + path);
  }

  // ---- update ------------------------------------------------------------

  public CompletionStage<String> updateAsync(T metadataNode) {
    updateCall.increment();
    String name = metadataNode.getName();
    final String json;
    try {
      json = serializer.toJsonStr(metadataNode);
    } catch (InvalidProtocolBufferException e) {
      return failed(new InternalMetadataStoreException("Failed to serialize node", e));
    }
    return client
        .putItem(b -> b.tableName(tableName).item(itemFor(metadataNode, json)))
        .thenApply(
            resp -> {
              if (createMode == EtcdCreateMode.EPHEMERAL && ephemeralNodes.containsKey(name)) {
                ephemeralNodes.put(name, metadataNode);
              }
              if (shouldCache) {
                cache.put(
                    name,
                    new CacheEntry<>(
                        metadataNode,
                        createMode == EtcdCreateMode.EPHEMERAL ? expiryEpochSeconds() : 0));
              }
              return name;
            });
  }

  public void updateSync(T metadataNode) {
    await(updateAsync(metadataNode), "update " + metadataNode.getName());
  }

  // ---- delete ------------------------------------------------------------

  public CompletionStage<Void> deleteAsync(String path) {
    deleteCall.increment();
    return client
        .deleteItem(b -> b.tableName(tableName).key(keyFor(path)))
        .thenAccept(
            resp -> {
              cache.remove(path);
              ephemeralNodes.remove(path);
            });
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return deleteAsync(metadataNode.getName());
  }

  public void deleteSync(String path) {
    await(deleteAsync(path), "delete " + path);
  }

  public void deleteSync(T metadataNode) {
    await(deleteAsync(metadataNode), "delete " + metadataNode.getName());
  }

  // ---- list --------------------------------------------------------------

  public CompletionStage<List<T>> listAsync() {
    listCall.increment();
    if (shouldCache) {
      awaitCacheInitialized();
      long now = nowSeconds();
      List<T> result = new ArrayList<>();
      for (CacheEntry<T> entry : cache.values()) {
        if (!entry.isExpired(now)) {
          result.add(entry.node());
        }
      }
      return CompletableFuture.completedFuture(result);
    }
    return client
        .query(
            b ->
                b.tableName(tableName)
                    .keyConditionExpression(PK + " = :pk")
                    .expressionAttributeValues(Map.of(":pk", AttributeValue.fromS(partitionValue))))
        .thenApply(
            resp -> {
              List<T> result = new ArrayList<>();
              for (Map<String, AttributeValue> item : resp.items()) {
                if (!isExpiredItem(item)) {
                  result.add(deserialize(item));
                }
              }
              return result;
            });
  }

  public List<T> listSync() {
    return await(listAsync(), "list");
  }

  // ---- listeners / cache -------------------------------------------------

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    if (!shouldCache) {
      throw new UnsupportedOperationException("Cannot add listener when caching is disabled");
    }
    listeners.add(watcher);
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    listeners.remove(watcher);
  }

  public void awaitCacheInitialized() {
    try {
      if (!cacheInitialized.await(operationTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new InternalMetadataStoreException("Timed out waiting for cache initialization");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted waiting for cache initialization", e);
    }
  }

  private void populateInitialCache() {
    try {
      var resp =
          client
              .query(
                  b ->
                      b.tableName(tableName)
                          .keyConditionExpression(PK + " = :pk")
                          .expressionAttributeValues(
                              Map.of(":pk", AttributeValue.fromS(partitionValue))))
              .get(operationTimeoutMs, TimeUnit.MILLISECONDS);
      for (Map<String, AttributeValue> item : resp.items()) {
        if (!isExpiredItem(item)) {
          cache.put(item.get(SK).s(), entryFor(item));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted populating initial cache", e);
    } catch (ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Failed to populate initial cache", e);
    } finally {
      cacheInitialized.countDown();
    }
  }

  /** Applies a stream change to the cache and fans it out to listeners. */
  private void onStreamChange(DynamoDbStreamPoller.Change change) {
    if (change.removed()) {
      CacheEntry<T> removed = cache.remove(change.sk());
      T node = removed != null ? removed.node() : imageNode(change.image());
      if (node != null) {
        notifyListeners(node);
      }
      return;
    }
    Map<String, AttributeValue> image = change.image();
    if (image == null || image.isEmpty()) {
      return;
    }
    if (isExpiredItem(image)) {
      // A write already past its ttl; treat as a removal so we don't resurrect a dead node.
      CacheEntry<T> removed = cache.remove(change.sk());
      if (removed != null) {
        notifyListeners(removed.node());
      }
      return;
    }
    CacheEntry<T> entry = entryFor(image);
    cache.put(change.sk(), entry);
    notifyListeners(entry.node());
  }

  private T imageNode(Map<String, AttributeValue> image) {
    if (image == null || image.get(PAYLOAD) == null) {
      return null;
    }
    return deserialize(image);
  }

  /**
   * Drops cache entries whose ttl has passed and fires their removal, standing in for the TTL
   * sweeper (best-effort on real DynamoDB, absent on DynamoDB Local).
   */
  private void reapExpired() {
    if (closed.get()) {
      return;
    }
    long now = nowSeconds();
    for (Map.Entry<String, CacheEntry<T>> e : cache.entrySet()) {
      if (!e.getValue().isExpired(now)) {
        continue;
      }
      // Plain remove + re-check rather than compare-and-remove: the stream poller runs on its own
      // thread and may replace the entry object between our read and remove. If what we pulled is
      // still expired, fire the removal; if a fresh heartbeat snuck a live entry back in, restore
      // it (the node is still alive) and let a later pass reap it.
      CacheEntry<T> prev = cache.remove(e.getKey());
      if (prev == null) {
        continue;
      }
      if (prev.isExpired(nowSeconds())) {
        notifyListeners(prev.node());
      } else {
        cache.putIfAbsent(e.getKey(), prev);
      }
    }
  }

  private void notifyListeners(T node) {
    for (AstraMetadataStoreChangeListener<T> listener : listeners) {
      try {
        listener.onMetadataStoreChanged(node);
      } catch (Exception e) {
        LOG.warn("Listener threw while handling change for {}", node.getName(), e);
      }
    }
  }

  // ---- ephemeral heartbeat ----------------------------------------------

  private void heartbeat() {
    if (closed.get()) {
      return;
    }
    for (Map.Entry<String, T> entry : ephemeralNodes.entrySet()) {
      try {
        String json = serializer.toJsonStr(entry.getValue());
        client
            .putItem(b -> b.tableName(tableName).item(itemFor(entry.getValue(), json)))
            .get(operationTimeoutMs, TimeUnit.MILLISECONDS);
        if (shouldCache) {
          cache.put(entry.getKey(), new CacheEntry<>(entry.getValue(), expiryEpochSeconds()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        LOG.warn("Failed to heartbeat ephemeral node {}", entry.getKey(), e);
      }
    }
  }

  /**
   * POC test helper: stop heartbeating (simulating node death) so ephemeral nodes are allowed to
   * expire. In production this happens implicitly when the JVM dies and the heartbeat stops.
   */
  void stopHeartbeatForTest() {
    ephemeralNodes.clear();
  }

  // ---- helpers -----------------------------------------------------------

  private static Throwable unwrap(Throwable t) {
    return (t instanceof java.util.concurrent.CompletionException && t.getCause() != null)
        ? t.getCause()
        : t;
  }

  private <R> CompletionStage<R> failed(Throwable t) {
    CompletableFuture<R> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  private <R> R await(CompletionStage<R> stage, String op) {
    try {
      return stage.toCompletableFuture().get(operationTimeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InternalMetadataStoreException ime) {
        throw ime;
      }
      throw new InternalMetadataStoreException("Failed to " + op, cause);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted during " + op, e);
    } catch (TimeoutException e) {
      throw new InternalMetadataStoreException("Timed out during " + op, e);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (maintenanceExecutor != null) {
      maintenanceExecutor.shutdownNow();
    }
    if (streamPoller != null) {
      streamPoller.close();
    }
  }
}
