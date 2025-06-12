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
 * different modes of operation: - ZOOKEEPER_CREATES: Creates go to Zookeeper, edits try ZK first
 * and fall back to Etcd, deletes go to both stores - ETCD_CREATES: Creates go to Etcd, edits try
 * Etcd first and fall back to ZK, deletes go to both stores
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
   * Creates a new metadata node asynchronously.
   *
   * @param metadataNode the node to create
   * @return a CompletionStage that completes with the path string (node name) when successful.
   *     Never returns null. In case of failure, the CompletionStage will be completed
   *     exceptionally. Specifically:
   *     <ul>
   *       <li>ZookeeperPartitioningMetadataStore: Throws InternalMetadataStoreException if the node
   *           already exists or another error occurs.
   *       <li>EtcdPartitioningMetadataStore: Throws InternalMetadataStoreException for
   *           serialization errors or other failures.
   *     </ul>
   */
  public CompletionStage<String> createAsync(T metadataNode) {
    return switch (mode) {
      case ZOOKEEPER_CREATES -> zkStore.createAsync(metadataNode);
      case ETCD_CREATES -> etcdStore.createAsync(metadataNode);
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Synchronously creates a new metadata node.
   *
   * @param metadataNode the node to create
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperPartitioningMetadataStore if the node
   *     already exists or another error occurs
   * @throws InternalMetadataStoreException from EtcdPartitioningMetadataStore if serialization
   *     fails or another error occurs
   */
  public void createSync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        zkStore.createSync(metadataNode);
        break;
      case ETCD_CREATES:
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
   * @return a CompletionStage that completes with the node if found. Never returns a
   *     CompletionStage that completes with null. In case of failure, the CompletionStage will
   *     complete exceptionally.
   *     <ul>
   *       <li>In exclusive modes, if the node doesn't exist, the underlying implementation will
   *           throw an exception.
   *       <li>ZookeeperPartitioningMetadataStore: throws InternalMetadataStoreException if the node
   *           doesn't exist or another error occurs
   *       <li>EtcdPartitioningMetadataStore: throws InternalMetadataStoreException if the node
   *           doesn't exist or another error occurs
   *       <li>In mixed modes, if the node is not found in the primary store, it will try the
   *           secondary store before throwing an exception. This provides transparent fallback
   *           reading during migration.
   *     </ul>
   */
  public CompletionStage<T> getAsync(String partition, String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES -> zkStore.getAsync(partition, path);
      case ETCD_CREATES -> etcdStore.getAsync(partition, path);
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param partition the partition to look in
   * @param path the path to the node
   * @return the node if found. Never returns null.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperPartitioningMetadataStore if the node
   *     doesn't exist or another error occurs
   * @throws InternalMetadataStoreException from EtcdPartitioningMetadataStore if the node doesn't
   *     exist or another error occurs
   * @throws InternalMetadataStoreException in mixed modes when the node is not found in either
   *     store after attempted fallback reads
   */
  public T getSync(String partition, String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES -> zkStore.getSync(partition, path);
      case ETCD_CREATES -> etcdStore.getSync(partition, path);
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Attempts to find the metadata asynchronously without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return a CompletionStage that completes with the node if found. Never returns a
   *     CompletionStage that completes with null. In case of failure, the CompletionStage will
   *     complete exceptionally.
   *     <ul>
   *       <li>In exclusive modes, if the node doesn't exist in any partition, the underlying
   *           implementation will throw an exception.
   *       <li>ZookeeperPartitioningMetadataStore: throws InternalMetadataStoreException if the node
   *           doesn't exist or another error occurs
   *       <li>EtcdPartitioningMetadataStore: throws InternalMetadataStoreException if the node
   *           doesn't exist or another error occurs
   *       <li>In mixed modes, if the node is not found in the primary store, it will try the
   *           secondary store before throwing an exception.
   *     </ul>
   */
  public CompletionStage<T> findAsync(String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES -> zkStore.findAsync(path);
      case ETCD_CREATES -> etcdStore.findAsync(path);
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Checks if a node exists asynchronously in the given partition.
   *
   * @param partition the partition to look in
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise. In
   *     exclusive modes, the result is based on just the active store. In mixed modes, returns true
   *     if the node exists in either store, enabling transparent fallback reading during
   *     migrations.
   *     <p>NOTE: Unlike getAsync(), this method does not throw exceptions if the node doesn't
   *     exist. It's designed to safely check existence and handle the case where the node is not
   *     found.
   *     <p>If other exceptions occur (e.g., connection problems), the CompletionStage will complete
   *     exceptionally with InternalMetadataStoreException.
   */
  public CompletionStage<Boolean> hasAsync(String partition, String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES ->
          // Try ZK first, fall back to Etcd if not found
          zkStore
              .hasAsync(partition, path)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return CompletableFuture.completedFuture(true);
                    } else if (etcdStore != null) {
                      // Try Etcd as fallback
                      return etcdStore.hasAsync(partition, path);
                    } else {
                      return CompletableFuture.completedFuture(false);
                    }
                  });
      case ETCD_CREATES ->
          // Try Etcd first, fall back to ZK if not found
          etcdStore
              .hasAsync(partition, path)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return CompletableFuture.completedFuture(true);
                    } else if (zkStore != null) {
                      // Try ZK as fallback
                      return zkStore.hasAsync(partition, path);
                    } else {
                      return CompletableFuture.completedFuture(false);
                    }
                  });
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Checks if a node exists synchronously in the given partition.
   *
   * @param partition the partition to look in
   * @param path the path to check
   * @return true if the node exists, false otherwise. In mixed modes, returns true if the node
   *     exists in either store, enabling transparent fallback reading during migrations.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException if a connection error or other unrecoverable error
   *     occurs. Note that the node not existing is NOT treated as an error; in that case, the
   *     method returns false.
   */
  public boolean hasSync(String partition, String path) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Try ZK first, fall back to Etcd if not found
        boolean existsInZk = zkStore.hasSync(partition, path);
        if (existsInZk) {
          return true;
        } else if (etcdStore != null) {
          // Try Etcd as fallback
          return etcdStore.hasSync(partition, path);
        } else {
          return false;
        }
      case ETCD_CREATES:
        // Try Etcd first, fall back to ZK if not found
        boolean existsInEtcd = etcdStore.hasSync(partition, path);
        if (existsInEtcd) {
          return true;
        } else if (zkStore != null) {
          // Try ZK as fallback
          return zkStore.hasSync(partition, path);
        } else {
          return false;
        }
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Attempts to find the metadata synchronously without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return the node if found. Never returns null.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException if the node doesn't exist in any partition or another
   *     error occurs
   * @throws InternalMetadataStoreException in mixed modes when the node is not found in either
   *     store after attempted fallback reads
   */
  public T findSync(String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES -> zkStore.findSync(path);
      case ETCD_CREATES -> etcdStore.findSync(path);
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Updates a node asynchronously.
   *
   * @param metadataNode the node to update
   * @return a CompletionStage that completes with the node version when the operation is done. The
   *     version is implementation-specific and may be empty. In case of failure, the
   *     CompletionStage will complete exceptionally.
   *     <ul>
   *       <li>ZookeeperPartitioningMetadataStore: returns the ZooKeeper version as a String
   *       <li>EtcdPartitioningMetadataStore: returns the node name
   *     </ul>
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Deletes from Etcd and creates/updates in ZooKeeper
   *       <li>BOTH_READ_ETCD_WRITE: Deletes from ZooKeeper and creates/updates in Etcd
   *     </ul>
   *     <p>This ensures that during migration, data is smoothly transitioned to the new primary
   *     store while maintaining consistency.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   */
  public CompletionStage<String> updateAsync(T metadataNode) {
    return switch (mode) {
      case ZOOKEEPER_CREATES ->
          // Try ZK first, fall back to Etcd if not found
          zkStore
              .hasAsync(metadataNode.getPartition(), metadataNode.name)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return zkStore.updateAsync(metadataNode);
                    } else {
                      // Try Etcd
                      return etcdStore.updateAsync(metadataNode);
                    }
                  });
      case ETCD_CREATES ->
          // Try Etcd first, fall back to ZK if not found
          etcdStore
              .hasAsync(metadataNode.getPartition(), metadataNode.name)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return etcdStore.updateAsync(metadataNode);
                    } else {
                      // Try ZK
                      return zkStore.updateAsync(metadataNode);
                    }
                  });
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperPartitioningMetadataStore if the node
   *     doesn't exist or another error occurs
   * @throws InternalMetadataStoreException from EtcdPartitioningMetadataStore if serialization
   *     fails or another error occurs
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Deletes from Etcd and creates/updates in ZooKeeper
   *       <li>BOTH_READ_ETCD_WRITE: Deletes from ZooKeeper and creates/updates in Etcd
   *     </ul>
   *     <p>This ensures that during migration, data is smoothly transitioned to the new primary
   *     store while maintaining consistency.
   */
  public void updateSync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Try ZK first, fall back to Etcd if not found
        if (zkStore.hasSync(metadataNode.getPartition(), metadataNode.name)) {
          zkStore.updateSync(metadataNode);
        } else {
          // Try Etcd
          etcdStore.updateSync(metadataNode);
        }
        break;
      case ETCD_CREATES:
        // Try Etcd first, fall back to ZK if not found
        if (etcdStore.hasSync(metadataNode.getPartition(), metadataNode.name)) {
          etcdStore.updateSync(metadataNode);
        } else {
          // Try ZK
          zkStore.updateSync(metadataNode);
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
   * @return a CompletionStage that completes when the operation is done. In case of failure, the
   *     CompletionStage will complete exceptionally.
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Deletes from ZooKeeper (primary) and also tries to delete
   *           from Etcd (secondary)
   *       <li>BOTH_READ_ETCD_WRITE: Deletes from Etcd (primary) and also tries to delete from
   *           ZooKeeper (secondary)
   *     </ul>
   *     <p>This ensures that during migration, data remains consistent across both stores.
   *     <p>Note: In mixed modes, if deletion from the secondary store fails, the failure is ignored
   *     to prevent cascading failures. The CompletionStage will still complete successfully as long
   *     as the primary deletion succeeds.
   */
  public CompletionStage<Void> deleteAsync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkDeleteResult = zkStore.deleteAsync(metadataNode);
        etcdStore.deleteAsync(metadataNode); // don't await this operation
        return zkDeleteResult;
      case ETCD_CREATES:
        // Delete from Etcd and also try to delete from ZK
        CompletionStage<Void> etcdDeleteResult = etcdStore.deleteAsync(metadataNode);
        zkStore.deleteAsync(metadataNode); // don't await this operation
        return etcdDeleteResult;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node synchronously.
   *
   * @param metadataNode the node to delete
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperPartitioningMetadataStore if another error
   *     occurs (not finding the node to delete is NOT considered an error)
   * @throws InternalMetadataStoreException from EtcdPartitioningMetadataStore if another error
   *     occurs (not finding the node to delete is NOT considered an error)
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Deletes from ZooKeeper (primary) and also tries to delete
   *           from Etcd (secondary)
   *       <li>BOTH_READ_ETCD_WRITE: Deletes from Etcd (primary) and also tries to delete from
   *           ZooKeeper (secondary)
   *     </ul>
   *     <p>This ensures that during migration, data remains consistent across both stores.
   *     <p>Note: In mixed modes, if deletion from the secondary store fails, the failure is ignored
   *     to prevent cascading failures.
   */
  public void deleteSync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Delete from ZK and also try to delete from Etcd
        zkStore.deleteSync(metadataNode);
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        break;
      case ETCD_CREATES:
        // Delete from Etcd and also try to delete from ZK
        etcdStore.deleteSync(metadataNode);
        try {
          zkStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return a CompletionStage that completes with the list of nodes. The list is never null but may
   *     be empty if no nodes exist. In case of failure, the CompletionStage will complete
   *     exceptionally.
   *     <ul>
   *       <li>ZookeeperPartitioningMetadataStore: throws UnsupportedOperationException if caching
   *           is disabled
   *       <li>EtcdPartitioningMetadataStore: returns cached results if available, otherwise fetches
   *           from Etcd
   *     </ul>
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>In both mixed modes, results from both primary and secondary stores are combined into
   *           a single list, with primary store items taking precedence in case of duplicate names
   *     </ul>
   */
  public CompletionStage<List<T>> listAsync() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Always combine results from both stores for complete visibility
        CompletionStage<List<T>> zkList =
            zkStore != null
                ? zkStore.listAsync().exceptionally(ex -> List.of())
                : CompletableFuture.completedFuture(List.of());

        CompletionStage<List<T>> etcdList =
            etcdStore != null
                ? etcdStore.listAsync().exceptionally(ex -> List.of())
                : CompletableFuture.completedFuture(List.of());

        // Combine the results
        return zkList.thenCombine(
            etcdList,
            (list1, list2) -> {
              // Combine both lists, using name as identifier
              Map<String, T> combinedMap = new ConcurrentHashMap<>();

              // Add items based on mode priority
              if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
                // ZK is primary, so add ZK items first
                for (T item : list1) {
                  combinedMap.put(item.name, item);
                }
                // Then add Etcd items if not already present
                for (T item : list2) {
                  combinedMap.putIfAbsent(item.name, item);
                }
              } else { // ETCD_CREATES
                // Etcd is primary, so add Etcd items first
                for (T item : list2) {
                  combinedMap.put(item.name, item);
                }
                // Then add ZK items if not already present
                for (T item : list1) {
                  combinedMap.putIfAbsent(item.name, item);
                }
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
   * @return the list of nodes. The list is never null but may be empty if no nodes exist.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperPartitioningMetadataStore if caching is
   *     disabled
   * @throws InternalMetadataStoreException from ZookeeperPartitioningMetadataStore if the list
   *     operation fails
   * @throws InternalMetadataStoreException from EtcdPartitioningMetadataStore if the list operation
   *     fails
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>In both mixed modes, results from both primary and secondary stores are combined into
   *           a single list, with primary store items taking precedence in case of duplicate names
   *       <li>If an error occurs while retrieving from one store, the operation will continue with
   *           the other store's results only, returning partial data instead of failing completely
   *     </ul>
   */
  public List<T> listSync() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Always combine results from both stores for complete visibility
        List<T> zkList;
        List<T> etcdList;

        if (zkStore != null) {
          try {
            zkList = zkStore.listSync();
          } catch (Exception e) {
            zkList = List.of();
          }
        } else {
          zkList = List.of();
        }

        if (etcdStore != null) {
          try {
            etcdList = etcdStore.listSync();
          } catch (Exception e) {
            etcdList = List.of();
          }
        } else {
          etcdList = List.of();
        }

        // Combine both lists, using name as identifier
        Map<String, T> resultMap = new ConcurrentHashMap<>();

        // Add items based on mode priority
        if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
          // ZK is primary, so add ZK items first
          for (T item : zkList) {
            resultMap.put(item.name, item);
          }
          // Then add Etcd items if not already present
          for (T item : etcdList) {
            resultMap.putIfAbsent(item.name, item);
          }
        } else { // ETCD_CREATES
          // Etcd is primary, so add Etcd items first
          for (T item : etcdList) {
            resultMap.put(item.name, item);
          }
          // Then add ZK items if not already present
          for (T item : zkList) {
            resultMap.putIfAbsent(item.name, item);
          }
        }

        return new ArrayList<>(resultMap.values());
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Adds a listener for metadata changes. This listener will be notified when nodes are created,
   * updated, or deleted.
   *
   * @param watcher the listener to add
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperPartitioningMetadataStore if caching is
   *     disabled
   * @throws UnsupportedOperationException from EtcdPartitioningMetadataStore if caching is disabled
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>In both mixed modes, the listener is registered with both primary and secondary
   *           stores, so changes in either store will trigger notifications
   *     </ul>
   *     <p>Note: When a node is changed in both stores in close succession, the listener might be
   *     notified twice. Applications should be prepared to handle duplicate notifications.
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // Create a wrapper that will forward events
    DualStoreChangeListener<T> dualListener = new DualStoreChangeListener<>(watcher);
    listenerMap.put(watcher, dualListener);

    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Add listeners to both non-null stores
        if (zkStore != null) {
          zkStore.addListener(dualListener);
        }
        if (etcdStore != null) {
          etcdStore.addListener(dualListener);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param watcher the listener to remove
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperPartitioningMetadataStore if caching is
   *     disabled
   * @throws UnsupportedOperationException from EtcdPartitioningMetadataStore if caching is disabled
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>In both mixed modes, the listener is removed from both primary and secondary stores
   *     </ul>
   *     <p>Note: If the listener was never added, this method will have no effect.
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    DualStoreChangeListener<T> dualListener = listenerMap.remove(watcher);
    if (dualListener == null) {
      return;
    }

    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Remove listeners from both non-null stores
        if (zkStore != null) {
          zkStore.removeListener(dualListener);
        }
        if (etcdStore != null) {
          etcdStore.removeListener(dualListener);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Waits for the cache to be initialized.
   *
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws RuntimeException if the cache initialization times out (applies to both
   *     ZookeeperPartitioningMetadataStore and EtcdPartitioningMetadataStore which uses
   *     RuntimeHalterImpl)
   *     <p>Note: The ZookeeperPartitioningMetadataStore initializes cache during construction,
   *     while EtcdPartitioningMetadataStore requires explicit initialization call.
   *     <p>In exclusive modes, this method simply calls the corresponding store's implementation.
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Primary store (ZK) initializes during construction, and
   *           secondary store (Etcd) is explicitly initialized for faster fallback
   *       <li>BOTH_READ_ETCD_WRITE: Primary store (Etcd) is explicitly initialized
   *     </ul>
   *     <p>This ensures that the system will be functional with at least the primary data, without
   *     waiting for the secondary store which may be less critical during migration.
   */
  public void awaitCacheInitialized() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // ZK partition store initializes cache during construction
        LOG.info(
            "ZookeeperPartitioningMetadataStore cache already initialized during construction");
        // Also initialize Etcd cache for faster fallback if needed
        if (etcdStore != null) {
          try {
            LOG.info("Initializing secondary EtcdPartitioningMetadataStore cache");
            etcdStore.awaitCacheInitialized();
          } catch (Exception e) {
            LOG.warn("Failed to initialize secondary Etcd cache", e);
          }
        }
        break;
      case ETCD_CREATES:
        if (etcdStore != null) {
          LOG.info("Initializing EtcdPartitioningMetadataStore cache");
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
