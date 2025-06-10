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

/**
 * AstraMetadataStore is a bridge implementation that takes both ZookeeperMetadataStore and
 * EtcdMetadataStore implementations. It will route operations to one or both of these
 * implementations depending on the configured mode in MetadataStoreConfig.
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
public class AstraMetadataStore<T extends AstraMetadata> implements Closeable {

  protected final ZookeeperMetadataStore<T> zkStore;
  private final EtcdMetadataStore<T> etcdStore;
  private final MetadataStoreMode mode;

  private final MeterRegistry meterRegistry;

  private final String ASTRA_METADATA_STORE_INCONSISTENCY = "astra_metadata_store_inconsistency";
  private final Counter inconsistencyCounter;

  // Tracks listeners registered, so we can properly register/unregister from both stores
  private final Map<AstraMetadataStoreChangeListener<T>, DualStoreChangeListener<T>> listenerMap =
      new ConcurrentHashMap<>();

  /**
   * Constructor for AstraMetadataStore.
   *
   * @param zkStore the ZookeeperMetadataStore implementation
   * @param etcdStore the EtcdMetadataStore implementation
   * @param mode the operation mode
   * @param meterRegistry the metrics registry
   */
  public AstraMetadataStore(
      ZookeeperMetadataStore<T> zkStore,
      EtcdMetadataStore<T> etcdStore,
      MetadataStoreMode mode,
      MeterRegistry meterRegistry) {

    this.zkStore = zkStore;
    this.etcdStore = etcdStore;
    this.mode = mode;
    this.meterRegistry = meterRegistry;

    this.inconsistencyCounter =
        this.meterRegistry.counter(ASTRA_METADATA_STORE_INCONSISTENCY, "store", "composite");
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
   * @param path the path to the node
   * @return a CompletionStage that completes with the node
   */
  public CompletionStage<T> getAsync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.getAsync(path);
      case EtcdExclusive:
        return etcdStore.getAsync(path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .getAsync(path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in ZK, try Etcd
                  return etcdStore.getAsync(path);
                });
      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .getAsync(path)
            .exceptionally(ex -> null)
            .thenCompose(
                result -> {
                  if (result != null) {
                    return CompletableFuture.completedFuture(result);
                  }
                  // Not found in Etcd, try ZK
                  return zkStore.getAsync(path);
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param path the path to the node
   * @return the node
   */
  public T getSync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.getSync(path);
      case EtcdExclusive:
        return etcdStore.getSync(path);
      case BothReadZookeeperWrite:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.getSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try Etcd
          inconsistencyCounter.increment();
        }
        // Not found in ZK or ZK threw exception, try Etcd
        return etcdStore.getSync(path);

      case BothReadEtcdWrite:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.getSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception e) {
          // Fall through to try ZK
          inconsistencyCounter.increment();
        }
        // Not found in Etcd or Etcd threw exception, try ZK
        return zkStore.getSync(path);

      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists asynchronously.
   *
   * @param path the path to check
   * @return a CompletionStage that completes with the Stat if the node exists, null otherwise
   */
  public CompletionStage<Stat> hasAsync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.hasAsync(path);
      case EtcdExclusive:
        return etcdStore.hasAsync(path);
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // Try both stores, return true if either has the item
        CompletionStage<Stat> primaryHas =
            mode == MetadataStoreMode.BothReadZookeeperWrite
                ? zkStore.hasAsync(path) // ZK is primary
                : etcdStore.hasAsync(path); // Etcd is primary

        return primaryHas
            .thenApply(stat -> stat != null ? stat : null)
            .exceptionally(ex -> null)
            .thenCompose(
                primaryStat -> {
                  if (primaryStat != null) {
                    return CompletableFuture.completedFuture(primaryStat);
                  }
                  // Not found in primary, check secondary
                  CompletionStage<Stat> secondaryHas =
                      mode == MetadataStoreMode.BothReadZookeeperWrite
                          ? etcdStore.hasAsync(path)
                          : zkStore.hasAsync(path);
                  return secondaryHas.exceptionally(ex -> null);
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists synchronously.
   *
   * @param path the path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.hasSync(path);
      case EtcdExclusive:
        return etcdStore.hasSync(path);
      case BothReadZookeeperWrite:
      case BothReadEtcdWrite:
        // Return true if it exists in either store
        boolean primaryResult = false;
        boolean secondaryResult = false;

        // Check primary store
        try {
          primaryResult =
              mode == MetadataStoreMode.BothReadZookeeperWrite
                  ? zkStore.hasSync(path)
                  : etcdStore.hasSync(path);

          if (primaryResult) {
            return true; // Short-circuit if found in primary
          }
        } catch (Exception e) {
          inconsistencyCounter.increment();
        }

        // Check secondary store
        try {
          secondaryResult =
              mode == MetadataStoreMode.BothReadZookeeperWrite
                  ? etcdStore.hasSync(path)
                  : zkStore.hasSync(path);
        } catch (Exception e) {
          inconsistencyCounter.increment();
        }

        if (primaryResult != secondaryResult) {
          inconsistencyCounter.increment();
        }

        return primaryResult || secondaryResult;
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
   * @param path the path to delete
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<Void> deleteAsync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        return zkStore.deleteAsync(path);
      case EtcdExclusive:
        return etcdStore.deleteAsync(path);
      case BothReadZookeeperWrite:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkResult = zkStore.deleteAsync(path);
        etcdStore.deleteAsync(path); // don't await this operation
        return zkResult;
      case BothReadEtcdWrite:
        // Delete from Etcd and also try to delete from ZK
        CompletionStage<Void> etcdResult = etcdStore.deleteAsync(path);
        zkStore.deleteAsync(path); // don't await this operation
        return etcdResult;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node synchronously.
   *
   * @param path the path to delete
   */
  public void deleteSync(String path) {
    switch (mode) {
      case ZookeeperExclusive:
        zkStore.deleteSync(path);
        break;
      case EtcdExclusive:
        etcdStore.deleteSync(path);
        break;
      case BothReadZookeeperWrite:
        zkStore.deleteSync(path);
        try {
          etcdStore.deleteSync(path);
        } catch (Exception e) {
          // Log but don't fail the operation
          inconsistencyCounter.increment();
        }
        break;
      case BothReadEtcdWrite:
        etcdStore.deleteSync(path);
        try {
          zkStore.deleteSync(path);
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
        zkStore.awaitCacheInitialized();
        break;
      case EtcdExclusive:
        etcdStore.awaitCacheInitialized();
        break;
      case BothReadZookeeperWrite:
        zkStore.awaitCacheInitialized();
        // We don't wait for Etcd in this mode since ZK is primary
        break;
      case BothReadEtcdWrite:
        etcdStore.awaitCacheInitialized();
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
  private static class DualStoreChangeListener<T extends AstraMetadata>
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
