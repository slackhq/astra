package com.slack.astra.metadata.core;

import com.slack.astra.proto.config.AstraConfigs.DynamoDbConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;

/**
 * A DynamoDB-backed partitioned metadata store (POC), mirroring the public surface of {@link
 * EtcdPartitioningMetadataStore}.
 *
 * <p>Unlike the etcd version — which keeps a {@code Map<partition, sub-store>} because etcd keys
 * are hierarchical strings — DynamoDB lets us fold the partition identifier directly into the
 * table's partition key ({@code pk = storeFolder + "/" + partitionId}). A per-partition {@code
 * list()} / watch is then a single DynamoDB {@code Query} on {@code pk}, which is exactly the
 * scaling problem partitioning exists to solve. Each partition gets its own lazily-created {@link
 * DynamoDbMetadataStore} sub-store (so each has its own cache + stream poller scoped to that {@code
 * pk}); a cross-partition {@code findAsync} / full {@code listSync} falls back to a table {@code
 * Scan} (POC-acceptable; documented as a cost to revisit with a GSI).
 */
public class DynamoDbPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {

  private final DynamoDbAsyncClient client;
  private final DynamoDbStreamsAsyncClient streamsClient;
  private final DynamoDbConfig config;
  private final String storeFolder;
  private final boolean shouldCache;
  private final EtcdCreateMode createMode;
  private final MetadataSerializer<T> serializer;
  private final MeterRegistry meterRegistry;
  private final String tableName;

  private final ConcurrentHashMap<String, DynamoDbMetadataStore<T>> partitionStores =
      new ConcurrentHashMap<>();
  private final List<AstraMetadataStoreChangeListener<T>> listeners = new java.util.ArrayList<>();

  public DynamoDbPartitioningMetadataStore(
      DynamoDbAsyncClient client,
      DynamoDbStreamsAsyncClient streamsClient,
      DynamoDbConfig config,
      String storeFolder,
      boolean shouldCache,
      EtcdCreateMode createMode,
      MetadataSerializer<T> serializer,
      MeterRegistry meterRegistry) {
    this.client = client;
    this.streamsClient = streamsClient;
    this.config = config;
    this.storeFolder = storeFolder;
    this.shouldCache = shouldCache;
    this.createMode = createMode;
    this.serializer = serializer;
    this.meterRegistry = meterRegistry;
    this.tableName = config.getTableName();
  }

  private String partitionValue(String partition) {
    return storeFolder + "/" + partition;
  }

  /** Returns (creating if needed) the sub-store scoped to a single partition's {@code pk}. */
  DynamoDbMetadataStore<T> storeFor(String partition) {
    return partitionStores.computeIfAbsent(
        partition,
        p -> {
          DynamoDbMetadataStore<T> store =
              new DynamoDbMetadataStore<>(
                  client,
                  streamsClient,
                  config,
                  storeFolder,
                  partitionValue(p),
                  shouldCache,
                  createMode,
                  serializer,
                  meterRegistry);
          if (shouldCache) {
            synchronized (listeners) {
              for (AstraMetadataStoreChangeListener<T> listener : listeners) {
                store.addListener(listener);
              }
            }
          }
          return store;
        });
  }

  // ---- writes (partition derived from the node) --------------------------

  public CompletionStage<String> createAsync(T metadataNode) {
    return storeFor(metadataNode.getPartition()).createAsync(metadataNode);
  }

  public void createSync(T metadataNode) {
    storeFor(metadataNode.getPartition()).createSync(metadataNode);
  }

  public CompletionStage<String> updateAsync(T metadataNode) {
    return storeFor(metadataNode.getPartition()).updateAsync(metadataNode);
  }

  public void updateSync(T metadataNode) {
    storeFor(metadataNode.getPartition()).updateSync(metadataNode);
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return storeFor(metadataNode.getPartition()).deleteAsync(metadataNode.getName());
  }

  public void deleteSync(T metadataNode) {
    storeFor(metadataNode.getPartition()).deleteSync(metadataNode.getName());
  }

  /**
   * Ensures a partition's sub-store exists (and, when caching, its cache + poller are running).
   * Mirrors {@code EtcdPartitioningMetadataStore#createPartitionSync}.
   */
  public void createPartitionSync(String partition) {
    DynamoDbMetadataStore<T> store = storeFor(partition);
    if (shouldCache) {
      store.awaitCacheInitialized();
    }
  }

  // ---- partition-scoped reads (single Query on pk) -----------------------

  public CompletionStage<T> getAsync(String partition, String path) {
    return storeFor(partition).getAsync(path);
  }

  public T getSync(String partition, String path) {
    return storeFor(partition).getSync(path);
  }

  public boolean hasSync(String partition, String path) {
    return storeFor(partition).hasSync(path);
  }

  public CompletionStage<List<T>> listAsync(String partition) {
    return storeFor(partition).listAsync();
  }

  public List<T> listSync(String partition) {
    return storeFor(partition).listSync();
  }

  // ---- cross-partition reads (Scan) --------------------------------------

  /**
   * Locates a node by name across all partitions with a table {@code Scan} on {@code sk}. This is
   * the cross-partition escape hatch; at scale it should become a GSI on {@code sk}. Returns a
   * failed stage if no live node matches.
   */
  public CompletionStage<T> findAsync(String path) {
    return client
        .scan(
            b ->
                b.tableName(tableName)
                    .filterExpression(DynamoDbMetadataStore.SK + " = :sk")
                    .expressionAttributeValues(java.util.Map.of(":sk", AttributeValue.fromS(path))))
        .thenApply(
            resp -> {
              long now = System.currentTimeMillis() / 1000;
              for (var item : resp.items()) {
                if (belongsToThisStore(item) && !expired(item, now)) {
                  try {
                    return serializer.fromJsonStr(item.get(DynamoDbMetadataStore.PAYLOAD).s());
                  } catch (Exception e) {
                    throw new InternalMetadataStoreException("Failed to deserialize node", e);
                  }
                }
              }
              throw new InternalMetadataStoreException("Node not found in any partition: " + path);
            });
  }

  public T findSync(String path) {
    try {
      return findAsync(path).toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted during find " + path, e);
    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof InternalMetadataStoreException ime) {
        throw ime;
      }
      throw new InternalMetadataStoreException("Failed to find " + path, cause);
    }
  }

  /** Full cross-partition list via {@code Scan}, scoped to this store's {@code pk} prefix. */
  public List<T> listSync() {
    try {
      var resp = client.scan(b -> b.tableName(tableName)).toCompletableFuture().get();
      long now = System.currentTimeMillis() / 1000;
      List<T> result = new ArrayList<>();
      for (var item : resp.items()) {
        if (belongsToThisStore(item) && !expired(item, now)) {
          result.add(serializer.fromJsonStr(item.get(DynamoDbMetadataStore.PAYLOAD).s()));
        }
      }
      return result;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InternalMetadataStoreException("Interrupted during list", e);
    } catch (Exception e) {
      throw new InternalMetadataStoreException("Failed to list", e);
    }
  }

  private boolean belongsToThisStore(java.util.Map<String, AttributeValue> item) {
    AttributeValue pk = item.get(DynamoDbMetadataStore.PK);
    return pk != null && pk.s() != null && pk.s().startsWith(storeFolder + "/");
  }

  private static boolean expired(java.util.Map<String, AttributeValue> item, long nowSeconds) {
    AttributeValue ttl = item.get(DynamoDbMetadataStore.TTL);
    if (ttl == null || ttl.n() == null) {
      return false;
    }
    return Long.parseLong(ttl.n()) < nowSeconds;
  }

  // ---- listeners ---------------------------------------------------------

  /**
   * Registers a listener across all partitions. It is applied to every existing sub-store and to
   * any created later. Mirrors {@code EtcdPartitioningMetadataStore#addListener}.
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    synchronized (listeners) {
      listeners.add(watcher);
      for (DynamoDbMetadataStore<T> store : partitionStores.values()) {
        store.addListener(watcher);
      }
    }
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    synchronized (listeners) {
      listeners.remove(watcher);
      for (DynamoDbMetadataStore<T> store : partitionStores.values()) {
        store.removeListener(watcher);
      }
    }
  }

  /** POC test helper: stop heartbeating a partition's ephemeral nodes to simulate node death. */
  void stopHeartbeatForTest(String partition) {
    DynamoDbMetadataStore<T> store = partitionStores.get(partition);
    if (store != null) {
      store.stopHeartbeatForTest();
    }
  }

  @Override
  public void close() {
    for (DynamoDbMetadataStore<T> store : partitionStores.values()) {
      store.close();
    }
    partitionStores.clear();
  }
}
