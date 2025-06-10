package com.slack.astra.metadata.core;

import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AstraPartitioningMetadataStore is a bridge implementation that takes both
 * ZookeeperPartitioningMetadataStore and EtcdPartitioningMetadataStore implementations. It will
 * route operations to one or both of these implementations depending on the configured mode in
 * MetadataStoreConfig.
 *
 * <p>This class is intended to be a bridge for migrating from Zookeeper to Etcd by supporting
 * different modes of operation: - ZookeeperExclusive: All operations go to Zookeeper only -
 * EtcdExclusive: All operations go to Etcd only - BothReadZookeeperWrite: In this migration mode: -
 * Creates go to ZK only - Updates delete from Etcd and create in ZK - Deletes apply to both stores
 * - Get tries ZK first, then falls back to Etcd - Has returns true if either store has the item -
 * List combines results from both stores - BothReadEtcdWrite: In this migration mode: - Creates go
 * to Etcd only - Updates delete from ZK and create in Etcd - Deletes apply to both stores - Get
 * tries Etcd first, then falls back to ZK - Has returns true if either store has the item - List
 * combines results from both stores
 */
public class AstraPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AstraPartitioningMetadataStore.class);

  protected final ZookeeperPartitioningMetadataStore<T> zkStore;
  private final EtcdPartitioningMetadataStore<T> etcdStore;
  private final MetadataStoreMode mode;
  private final MeterRegistry meterRegistry;

  private final String ASTRA_PARTITIONING_METADATA_STORE_INCONSISTENCY =
      "astra_partitioning_metadata_store_inconsistency";
  private final Counter inconsistencyCounter;

  // Tracks listeners registered, so we can properly register/unregister from both stores
  private final Map<AstraMetadataStoreChangeListener<T>, DualStoreChangeListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  /**
   * Constructor for AstraPartitioningMetadataStore.
   *
   * @param zkStore the ZookeeperPartitioningMetadataStore implementation
   * @param etcdStore the EtcdPartitioningMetadataStore implementation
   * @param mode the operation mode
   * @param meterRegistry the metrics registry
   */
  public AstraPartitioningMetadataStore(
      ZookeeperPartitioningMetadataStore<T> zkStore,
      EtcdPartitioningMetadataStore<T> etcdStore,
      MetadataStoreMode mode,
      MeterRegistry meterRegistry) {

    this.zkStore = zkStore;
    this.etcdStore = etcdStore;
    this.mode = mode;
    this.meterRegistry = meterRegistry;

    this.inconsistencyCounter =
        this.meterRegistry.counter(
            ASTRA_PARTITIONING_METADATA_STORE_INCONSISTENCY, "store", "composite");
  }

  /**
   * Creates a new metadata node.
   *
   * @param metadataNode the node to create
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<String> createAsync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.createAsync(metadataNode);
      case EtcdExclusive:
        return etcdStore.createAsync(metadataNode);
      case BothReadZookeeperWrite:
        // In migration to ZK mode, writes only go to ZK
        return zkStore.createAsync(metadataNode);
      case BothReadEtcdWrite:
        // In migration to Etcd mode, writes only go to Etcd
        return etcdStore.createAsync(metadataNode);
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Synchronously creates a new metadata node.
   *
   * @param metadataNode the node to create
   */
  public void createSync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        zkStore.createSync(metadataNode);
        break;
      case EtcdExclusive:
        etcdStore.createSync(metadataNode);
        break;
      case BothReadZookeeperWrite:
        // In migration to ZK mode, writes only go to ZK
        zkStore.createSync(metadataNode);
        break;
      case BothReadEtcdWrite:
        // In migration to Etcd mode, writes only go to Etcd
        etcdStore.createSync(metadataNode);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Gets a metadata node asynchronously.
   *
   * @param partition the partition to look in
   * @param path the path to the node
   * @return a CompletionStage that completes with the node
   */
  public CompletionStage<T> getAsync(String partition, String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.getAsync(partition, path);
      case EtcdExclusive:
        return etcdStore.getAsync(partition, path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .getAsync(partition, path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in ZK, try Etcd
                  return etcdStore.getAsync(partition, path);
                });
      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .getAsync(partition, path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in Etcd, try ZK
                  return zkStore.getAsync(partition, path);
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param partition the partition to look in
   * @param path the path to the node
   * @return the node
   */
  public T getSync(String partition, String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.getSync(partition, path);
      case EtcdExclusive:
        return etcdStore.getSync(partition, path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.getSync(partition, path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try Etcd
          inconsistencyCounter.increment();
        }
        // Not found in ZK or ZK threw exception, try Etcd
        return etcdStore.getSync(partition, path);

      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.getSync(partition, path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try ZK
          inconsistencyCounter.increment();
        }
        // Not found in Etcd or Etcd threw exception, try ZK
        return zkStore.getSync(partition, path);

      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return a CompletionStage that completes with the node
   */
  public CompletionStage<T> findAsync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.findAsync(path);
      case EtcdExclusive:
        return etcdStore.findAsync(path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .findAsync(path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in ZK, try Etcd
                  return etcdStore.findAsync(path);
                });
      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .findAsync(path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in Etcd, try ZK
                  return zkStore.findAsync(path);
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Attempts to find the metadata synchronously without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return the node
   */
  public T findSync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.findSync(path);
      case EtcdExclusive:
        return etcdStore.findSync(path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.findSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try Etcd
          inconsistencyCounter.increment();
        }
        // Not found in ZK or ZK threw exception, try Etcd
        return etcdStore.findSync(path);

      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.findSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try ZK
          inconsistencyCounter.increment();
        }
        // Not found in Etcd or Etcd threw exception, try ZK
        return zkStore.findSync(path);

      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Updates a node asynchronously.
   *
   * @param metadataNode the node to update
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<Stat> updateAsync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.updateAsync(metadataNode);
      case EtcdExclusive:
        return etcdStore.updateAsync(metadataNode);
      case BothReadZookeeperWrite:
        // Delete from Etcd and create in ZK
        // Try to delete from Etcd first (don't wait)
        etcdStore.deleteAsync(metadataNode);
        // Then update in ZK (this is the operation we wait for)
        return zkStore.updateAsync(metadataNode);
      case BothReadEtcdWrite:
        // Delete from ZK and create in Etcd
        // Try to delete from ZK first (don't wait)
        zkStore.deleteAsync(metadataNode);
        // Then update in Etcd (this is the operation we wait for)
        return etcdStore.updateAsync(metadataNode);
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   */
  public void updateSync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        zkStore.updateSync(metadataNode);
        break;
      case EtcdExclusive:
        etcdStore.updateSync(metadataNode);
        break;
      case BothReadZookeeperWrite:
        // Delete from Etcd and create in ZK
        try {
          // Try to delete from Etcd first
          etcdStore.deleteSync(metadataNode);
        } catch (Exception e) {
          // Log but continue with ZK update
          inconsistencyCounter.increment();
        }
        // Then update in ZK
        zkStore.updateSync(metadataNode);
        break;
      case BothReadEtcdWrite:
        // Delete from ZK and create in Etcd
        try {
          // Try to delete from ZK first
          zkStore.deleteSync(metadataNode);
        } catch (Exception e) {
          // Log but continue with Etcd update
          inconsistencyCounter.increment();
        }
        // Then update in Etcd
        etcdStore.updateSync(metadataNode);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node asynchronously.
   *
   * @param metadataNode the node to delete
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<Void> deleteAsync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.deleteAsync(metadataNode);
      case EtcdExclusive:
        return etcdStore.deleteAsync(metadataNode);
      case BothReadZookeeperWrite:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkResult = zkStore.deleteAsync(metadataNode);
        etcdStore.deleteAsync(metadataNode); // don't await this operation
        return zkResult;
      case BothReadEtcdWrite:
        // Delete from Etcd and also try to delete from ZK
        CompletionStage<Void> etcdResult = etcdStore.deleteAsync(metadataNode);
        zkStore.deleteAsync(metadataNode); // don't await this operation
        return etcdResult;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node synchronously.
   *
   * @param metadataNode the node to delete
   */
  public void deleteSync(T metadataNode) {
    switch (mode) {
      case ZookeeperExclusive:
        zkStore.deleteSync(metadataNode);
        break;
      case EtcdExclusive:
        etcdStore.deleteSync(metadataNode);
        break;
      case BothReadZookeeperWrite:
        zkStore.deleteSync(metadataNode);
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (Exception e) {
          // Log but don't fail the operation
          inconsistencyCounter.increment();
        }
        break;
      case BothReadEtcdWrite:
        etcdStore.deleteSync(metadataNode);
        try {
          zkStore.deleteSync(metadataNode);
        } catch (Exception e) {
          // Log but don't fail the operation
          inconsistencyCounter.increment();
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return a CompletionStage that completes with the list of nodes
   */
  public CompletionStage<List<T>> listAsync() {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.listAsync();
      case EtcdExclusive:
        return etcdStore.listAsync();
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // Combine results from both stores
        CompletionStage<List<T>> primaryList =
            mode == MetadataStoreMode.BothReadZookeeperWrite
                ? zkStore.listAsync()
                : etcdStore.listAsync();
        CompletionStage<List<T>> secondaryList =
            mode == MetadataStoreMode.BothReadZookeeperWrite
                ? etcdStore.listAsync()
                : zkStore.listAsync();

        return primaryList
            .exceptionally(ex -> List.of())
            .thenCombine(
                secondaryList.exceptionally(ex -> List.of()),
                (list1, list2) -> {
                  // Combine both lists, using name as identifier
                  Map<String, T> combinedMap = new ConcurrentHashMap<>();

                  // Add items from primary store first
                  for (T item : list1) {
                    combinedMap.put(item.name, item);
                  }

                  // Add items from secondary store if not already present
                  for (T item : list2) {
                    combinedMap.putIfAbsent(item.name, item);
                  }

                  return new ArrayList<>(combinedMap.values());
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Lists all nodes synchronously.
   *
   * @return the list of nodes
   */
  public List<T> listSync() {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.listSync();
      case EtcdExclusive:
        return etcdStore.listSync();
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // Combine results from both stores
        List<T> primaryList;
        List<T> secondaryList;

        try {
          primaryList =
              mode == MetadataStoreMode.BothReadZookeeperWrite
                  ? zkStore.listSync()
                  : etcdStore.listSync();
        } catch (Exception e) {
          inconsistencyCounter.increment();
          primaryList = List.of();
        }

        try {
          secondaryList =
              mode == MetadataStoreMode.BothReadZookeeperWrite
                  ? etcdStore.listSync()
                  : zkStore.listSync();
        } catch (Exception e) {
          inconsistencyCounter.increment();
          secondaryList = List.of();
        }

        // Combine both lists, using name as identifier
        Map<String, T> combinedMap = new ConcurrentHashMap<>();

        // Add items from primary store first
        for (T item : primaryList) {
          combinedMap.put(item.name, item);
        }

        // Add items from secondary store if not already present
        for (T item : secondaryList) {
          combinedMap.putIfAbsent(item.name, item);
        }

        return new ArrayList<>(combinedMap.values());

      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param watcher the listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // Create a wrapper that will forward events
    DualStoreChangeListener<T> dualListener = new DualStoreChangeListener<>(watcher);
    listenerMap.put(watcher, dualListener);

    switch (mode) {
      case ZookeeperExclusive:
        zkStore.addListener(dualListener);
        break;
      case EtcdExclusive:
        etcdStore.addListener(dualListener);
        break;
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // In dual modes, we need to listen to both stores
        zkStore.addListener(dualListener);
        etcdStore.addListener(dualListener);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param watcher the listener to remove
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    DualStoreChangeListener<T> dualListener = listenerMap.remove(watcher);
    if (dualListener == null) {
      return;
    }

    switch (mode) {
      case ZookeeperExclusive:
        zkStore.removeListener(dualListener);
        break;
      case EtcdExclusive:
        etcdStore.removeListener(dualListener);
        break;
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // In dual modes, we need to remove from both stores
        zkStore.removeListener(dualListener);
        etcdStore.removeListener(dualListener);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /** Waits for the cache to be initialized. */
  public void awaitCacheInitialized() {
    switch (mode) {
      case ZookeeperExclusive:
        // ZK partition store doesn't have this method directly
        break;
      case EtcdExclusive:
        // Etcd partition store doesn't have this method directly
        break;
      case BothReadZookeeperWrite:
        // We don't wait for Etcd in this mode since ZK is primary
        break;
      case BothReadEtcdWrite:
        // We don't wait for ZK in this mode since Etcd is primary
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  @Override
  public void close() {
    // Always try to close both stores regardless of mode
    try {
      if (zkStore != null) {
        zkStore.close();
      }
    } catch (Exception e) {
      // Log but continue to close the other store
    }

    try {
      if (etcdStore != null) {
        etcdStore.close();
      }
    } catch (Exception e) {
      // Log but continue
    }
  }

  /**
   * Helper class that wraps a user-provided listener and forwards events. This is used to track
   * listeners across both stores, and to ensure that each event is only delivered once to the user
   * listener.
   */
  private static class DualStoreChangeListener<T extends AstraPartitionedMetadata>
      implements AstraMetadataStoreChangeListener<T> {

    private final AstraMetadataStoreChangeListener<T> delegate;

    DualStoreChangeListener(AstraMetadataStoreChangeListener<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onMetadataStoreChanged(T model) {
      delegate.onMetadataStoreChanged(model);
    }
  }
}
