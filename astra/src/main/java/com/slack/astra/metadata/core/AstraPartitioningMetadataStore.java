package com.slack.astra.metadata.core;

import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The AstraPartitioningMetadataStore is a bridge implementation that takes both
 * ZookeeperPartitioningMetadataStore and EtcdPartitioningMetadataStore implementations. It will
 * route operations to one or both of these implementations depending on the configured mode in
 * MetadataStoreConfig.
 *
 * <p>This class is intended to be a bridge for migrating from Zookeeper to Etcd by supporting
 * different modes of operation: - ZOOKEEPER_EXCLUSIVE: All operations go to Zookeeper only -
 * ETCD_EXCLUSIVE: All operations go to Etcd only - BOTH_READ_ZOOKEEPER_WRITE: In this migration
 * mode: - Creates go to ZK only - Updates delete from Etcd and create in ZK - Deletes apply to both
 * stores - Get tries ZK first, then falls back to Etcd - Has returns true if either store has the
 * item - List combines results from both stores - BOTH_READ_ETCD_WRITE: In this migration mode: -
 * Creates go to Etcd only - Updates delete from ZK and create in Etcd - Deletes apply to both
 * stores - Get tries Etcd first, then falls back to ZK - Has returns true if either store has the
 * item - List combines results from both stores
 */
public class AstraPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AstraPartitioningMetadataStore.class);

  protected final ZookeeperPartitioningMetadataStore<T> zkStore;
  private final EtcdPartitioningMetadataStore<T> etcdStore;
  private final MetadataStoreMode mode;
  private final MeterRegistry meterRegistry;

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
  }

  /**
   * Creates a new metadata node.
   *
   * @param metadataNode the node to create
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<String> createAsync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.createAsync(metadataNode);
      case ETCD_EXCLUSIVE:
        return etcdStore.createAsync(metadataNode);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // In migration to ZK mode, writes only go to ZK
        return zkStore.createAsync(metadataNode);
      case BOTH_READ_ETCD_WRITE:
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
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.createSync(metadataNode);
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.createSync(metadataNode);
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        // In migration to ZK mode, writes only go to ZK
        zkStore.createSync(metadataNode);
        break;
      case BOTH_READ_ETCD_WRITE:
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.getAsync(partition, path);
      case ETCD_EXCLUSIVE:
        return etcdStore.getAsync(partition, path);
      case BOTH_READ_ZOOKEEPER_WRITE:
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
      case BOTH_READ_ETCD_WRITE:
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.getSync(partition, path);
      case ETCD_EXCLUSIVE:
        return etcdStore.getSync(partition, path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.getSync(partition, path);
          if (result != null) {
            return result;
          }
        } catch (Exception ignored) {
        }
        // Not found in ZK or ZK threw exception, try Etcd
        return etcdStore.getSync(partition, path);

      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.getSync(partition, path);
          if (result != null) {
            return result;
          }
        } catch (Exception ignored) {
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.findAsync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.findAsync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
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
      case BOTH_READ_ETCD_WRITE:
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
   * Checks if a node exists asynchronously in the given partition.
   *
   * @param partition the partition to look in
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise
   */
  public CompletionStage<Boolean> hasAsync(String partition, String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.hasAsync(partition, path);
      case ETCD_EXCLUSIVE:
        return etcdStore.hasAsync(partition, path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .hasAsync(partition, path)
            .exceptionally(ex -> null)
            .thenCompose(
                stat -> {
                  if (stat != null) {
                    return CompletableFuture.completedFuture(stat);
                  }
                  // Not found in ZK, try Etcd
                  return etcdStore.hasAsync(partition, path);
                });
      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .hasAsync(partition, path)
            .exceptionally(ex -> null)
            .thenCompose(
                stat -> {
                  if (stat != null) {
                    return CompletableFuture.completedFuture(stat);
                  }
                  // Not found in Etcd, try ZK
                  return zkStore.hasAsync(partition, path);
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists synchronously in the given partition.
   *
   * @param partition the partition to look in
   * @param path the path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String partition, String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.hasSync(partition, path);
      case ETCD_EXCLUSIVE:
        return etcdStore.hasSync(partition, path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        boolean primaryResult = false;
        try {
          primaryResult = zkStore.hasSync(partition, path);
          if (primaryResult) {
            return true; // Short-circuit if found in primary
          }
        } catch (Exception ignored) {
        }
        // Try secondary store
        return etcdStore.hasSync(partition, path);
      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        try {
          boolean etcdResult = etcdStore.hasSync(partition, path);
          if (etcdResult) {
            return true; // Short-circuit if found in primary
          }
        } catch (Exception ignored) {
        }
        // Try secondary store
        return zkStore.hasSync(partition, path);
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.findSync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.findSync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.findSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception ignored) {
        }
        // Not found in ZK or ZK threw exception, try Etcd
        return etcdStore.findSync(path);

      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.findSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception ignored) {
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
   * @return a CompletionStage that completes with the node version when the operation is done
   */
  public CompletionStage<String> updateAsync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.updateAsync(metadataNode);
      case ETCD_EXCLUSIVE:
        return etcdStore.updateAsync(metadataNode);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Delete from Etcd and create in ZK
        // Try to delete from Etcd first (don't wait)
        etcdStore.deleteAsync(metadataNode);
        // Then check-and-update in ZK (this is the operation we wait for)
        return checkAndUpdateAsync(zkStore, metadataNode);
      case BOTH_READ_ETCD_WRITE:
        // Delete from ZK and create in Etcd
        // Try to delete from ZK first (don't wait)
        zkStore.deleteAsync(metadataNode);
        // Then check-and-update in Etcd (this is the operation we wait for)
        return checkAndUpdateAsync(etcdStore, metadataNode);
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists and then either updates or creates it in ZooKeeper.
   *
   * @param store the ZK store to use
   * @param metadataNode the node to update or create
   * @return a CompletionStage that completes with the version info when the operation is done
   */
  private CompletionStage<String> checkAndUpdateAsync(
      ZookeeperPartitioningMetadataStore<T> store, T metadataNode) {
    return store
        .hasAsync(metadataNode.getPartition(), metadataNode.name)
        .thenCompose(
            exists -> {
              if (exists) {
                // Node exists, update it
                return store.updateAsync(metadataNode);
              } else {
                // Node doesn't exist, create it
                return store
                    .createAsync(metadataNode)
                    .thenApply(
                        path -> {
                          // Return empty version since create doesn't return one
                          // but update interface requires one
                          return "";
                        });
              }
            });
  }

  /**
   * Checks if a node exists and then either updates or creates it in Etcd.
   *
   * @param store the Etcd store to use
   * @param metadataNode the node to update or create
   * @return a CompletionStage that completes with the version info when the operation is done
   */
  private CompletionStage<String> checkAndUpdateAsync(
      EtcdPartitioningMetadataStore<T> store, T metadataNode) {
    return store
        .hasAsync(metadataNode.getPartition(), metadataNode.name)
        .thenCompose(
            exists -> {
              if (exists) {
                // Node exists, update it
                return store.updateAsync(metadataNode);
              } else {
                // Node doesn't exist, create it
                return store
                    .createAsync(metadataNode)
                    .thenApply(
                        path -> {
                          // Return empty version since create doesn't return one
                          // but update interface requires one
                          return "";
                        });
              }
            });
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   */
  public void updateSync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.updateSync(metadataNode);
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.updateSync(metadataNode);
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Delete from Etcd and create in ZK
        try {
          // Try to delete from Etcd first
          etcdStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
        }
        // Then check-and-update in ZK
        checkAndUpdateSync(zkStore, metadataNode);
        break;
      case BOTH_READ_ETCD_WRITE:
        // Delete from ZK and create in Etcd
        try {
          // Try to delete from ZK first
          zkStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
        }
        // Then check-and-update in Etcd
        checkAndUpdateSync(etcdStore, metadataNode);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists and then either updates or creates it synchronously in ZooKeeper.
   *
   * @param store the ZK store to use
   * @param metadataNode the node to update or create
   */
  private void checkAndUpdateSync(ZookeeperPartitioningMetadataStore<T> store, T metadataNode) {
    boolean exists = store.hasSync(metadataNode.getPartition(), metadataNode.name);
    if (exists) {
      // Node exists, update it
      store.updateSync(metadataNode);
    } else {
      // Node doesn't exist, create it
      store.createSync(metadataNode);
    }
  }

  /**
   * Checks if a node exists and then either updates or creates it synchronously in Etcd.
   *
   * @param store the Etcd store to use
   * @param metadataNode the node to update or create
   */
  private void checkAndUpdateSync(EtcdPartitioningMetadataStore<T> store, T metadataNode) {
    boolean exists = store.hasSync(metadataNode.getPartition(), metadataNode.name);
    if (exists) {
      // Node exists, update it
      store.updateSync(metadataNode);
    } else {
      // Node doesn't exist, create it
      store.createSync(metadataNode);
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.deleteAsync(metadataNode);
      case ETCD_EXCLUSIVE:
        return etcdStore.deleteAsync(metadataNode);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkResult = zkStore.deleteAsync(metadataNode);
        etcdStore.deleteAsync(metadataNode); // don't await this operation
        return zkResult;
      case BOTH_READ_ETCD_WRITE:
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
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.deleteSync(metadataNode);
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.deleteSync(metadataNode);
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        zkStore.deleteSync(metadataNode);
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
        }
        break;
      case BOTH_READ_ETCD_WRITE:
        etcdStore.deleteSync(metadataNode);
        try {
          zkStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.listAsync();
      case ETCD_EXCLUSIVE:
        return etcdStore.listAsync();
      case BOTH_READ_ZOOKEEPER_WRITE:
      case BOTH_READ_ETCD_WRITE:
        // Combine results from both stores
        CompletionStage<List<T>> primaryList =
            mode == MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE
                ? zkStore.listAsync()
                : etcdStore.listAsync();
        CompletionStage<List<T>> secondaryList =
            mode == MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.listSync();
      case ETCD_EXCLUSIVE:
        return etcdStore.listSync();
      case BOTH_READ_ZOOKEEPER_WRITE:
      case BOTH_READ_ETCD_WRITE:
        // Combine results from both stores
        List<T> primaryList;
        List<T> secondaryList;

        try {
          primaryList =
              mode == MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE
                  ? zkStore.listSync()
                  : etcdStore.listSync();
        } catch (Exception e) {
          primaryList = List.of();
        }

        try {
          secondaryList =
              mode == MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE
                  ? etcdStore.listSync()
                  : zkStore.listSync();
        } catch (Exception e) {
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
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.addListener(dualListener);
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.addListener(dualListener);
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
      case BOTH_READ_ETCD_WRITE:
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
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.removeListener(dualListener);
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.removeListener(dualListener);
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
      case BOTH_READ_ETCD_WRITE:
        // In dual modes, we need to remove from both stores
        zkStore.removeListener(dualListener);
        etcdStore.removeListener(dualListener);
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Waits for the cache to be initialized.
   *
   * <p>Note: The ZookeeperPartitioningMetadataStore initializes cache during construction, while
   * EtcdPartitioningMetadataStore requires explicit initialization call.
   */
  public void awaitCacheInitialized() {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        // ZK partition store initializes cache during construction
        LOG.info(
            "ZookeeperPartitioningMetadataStore cache already initialized during construction");
        break;
      case ETCD_EXCLUSIVE:
        if (etcdStore != null) {
          LOG.info("Initializing EtcdPartitioningMetadataStore cache");
          etcdStore.awaitCacheInitialized();
        }
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        // ZK is primary and initializes during construction
        // We can also pre-populate the secondary store for faster fallback
        if (etcdStore != null) {
          LOG.info("Initializing secondary EtcdPartitioningMetadataStore cache");
          etcdStore.awaitCacheInitialized();
        }
        break;
      case BOTH_READ_ETCD_WRITE:
        // Initialize primary store first
        if (etcdStore != null) {
          LOG.info("Initializing primary EtcdPartitioningMetadataStore cache");
          etcdStore.awaitCacheInitialized();
        }
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
      LOG.warn("Failed to close ZK metadata store", e);
    }

    try {
      if (etcdStore != null) {
        etcdStore.close();
      }
    } catch (Exception e) {
      LOG.warn("Failed to close Etcd metadata store", e);
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
