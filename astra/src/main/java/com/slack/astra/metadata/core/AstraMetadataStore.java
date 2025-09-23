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
 * AstraMetadataStore is a bridge implementation that takes both ZookeeperMetadataStore and
 * EtcdMetadataStore implementations. It will route operations to one or both of these
 * implementations depending on the configured mode in MetadataStoreConfig.
 *
 * <p>This class is intended to be a bridge for migrating between Zookeeper and Etcd by supporting
 * different modes of operation:
 *
 * <ul>
 *   <li>ZOOKEEPER_CREATES: Creates go to Zookeeper, edits try ZK first and fall back to Etcd,
 *       deletes go to both stores
 *   <li>ETCD_CREATES: Creates go to Etcd, edits try Etcd first and fall back to ZK, deletes go to
 *       both stores
 * </ul>
 *
 * <p>If either zkStore or etcdStore is null, the mode configuration will be overridden and
 * operations will be routed only to the non-null store, regardless of the specified mode.
 */
public class AstraMetadataStore<T extends AstraMetadata> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AstraMetadataStore.class);

  protected final ZookeeperMetadataStore<T> zkStore;
  protected final EtcdMetadataStore<T> etcdStore;
  protected final MetadataStoreMode mode;

  private final MeterRegistry meterRegistry;

  // Tracks listeners registered, so we can properly register/unregister from both stores
  private final List<AstraMetadataStoreChangeListener<T>> listeners = new ArrayList<>();

  /**
   * Constructor for AstraMetadataStore.
   *
   * @param zkStore the ZookeeperMetadataStore implementation, may be null
   * @param etcdStore the EtcdMetadataStore implementation, may be null
   * @param mode the operation mode (overridden if either store is null)
   * @param meterRegistry the metrics registry
   */
  public AstraMetadataStore(
      ZookeeperMetadataStore<T> zkStore,
      EtcdMetadataStore<T> etcdStore,
      MetadataStoreMode mode,
      MeterRegistry meterRegistry) {

    this.zkStore = zkStore;
    this.etcdStore = etcdStore;

    // Override mode if one of the stores is null
    if (zkStore == null && etcdStore != null) {
      this.mode = MetadataStoreMode.ETCD_CREATES;
      LOG.info("ZK store is null, overriding mode to ETCD_CREATES regardless of configured mode");
    } else if (etcdStore == null && zkStore != null) {
      this.mode = MetadataStoreMode.ZOOKEEPER_CREATES;
      LOG.info(
          "Etcd store is null, overriding mode to ZOOKEEPER_CREATES regardless of configured mode");
    } else if (etcdStore == null) {
      throw new IllegalArgumentException("Both zkStore and etcdStore cannot be null");
    } else {
      this.mode = mode;
      LOG.info("Using metadata store mode {}", mode);
    }

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
   *       <li>ZookeeperMetadataStore: Throws InternalMetadataStoreException if the node already
   *           exists or another error occurs.
   *       <li>EtcdMetadataStore: Throws InternalMetadataStoreException for serialization errors or
   *           other failures.
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
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the node already exists
   *     or another error occurs
   * @throws InternalMetadataStoreException from EtcdMetadataStore if serialization fails or another
   *     error occurs
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
   * Synchronously creates a new ETCD metadata node.
   *
   * @param metadataNode the node to create
   * @throws InternalMetadataStoreException from EtcdMetadataStore if serialization fails or another
   *     error occurs
   */
  public void createEtcdOnlySync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        throw new IllegalArgumentException(
            "Etcd metadata store mode is ZOOKEEPER_CREATES, can't create in etcd");
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
   * @param path the path to the node
   * @return a CompletionStage that completes with the node if found. Never returns a
   *     CompletionStage that completes with null. In case of failure, the CompletionStage will
   *     complete exceptionally.
   *     <ul>
   *       <li>In exclusive modes, if the node doesn't exist, the underlying implementation will
   *           throw an exception.
   *       <li>ZookeeperMetadataStore: throws InternalMetadataStoreException if the node doesn't
   *           exist or another error occurs
   *       <li>EtcdMetadataStore: throws InternalMetadataStoreException if the node doesn't exist or
   *           another error occurs
   *     </ul>
   */
  public CompletionStage<T> getAsync(String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES ->
          // Try ZK first, fall back to Etcd if not found
          zkStore
              .getAsync(path)
              .exceptionally(
                  throwable -> {
                    if (etcdStore != null) {
                      // If the node doesn't exist in ZK, try Etcd
                      if (throwable instanceof InternalMetadataStoreException) {
                        LOG.debug("Node not found in ZK, trying Etcd: {}", path);
                        try {
                          return etcdStore.getSync(path);
                        } catch (Exception e) {
                          // If it also fails in Etcd, throw the original exception
                          throw new RuntimeException(e);
                        }
                      } else {
                        // For other types of errors, rethrow
                        throw new RuntimeException(throwable);
                      }
                    } else {
                      // No etcd store, rethrow the original exception
                      throw new RuntimeException(throwable);
                    }
                  });
      case ETCD_CREATES ->
          // Try Etcd first, fall back to ZK if not found
          etcdStore
              .getAsync(path)
              .exceptionally(
                  throwable -> {
                    if (zkStore != null) {
                      // If the node doesn't exist in Etcd, try ZK
                      if (throwable instanceof InternalMetadataStoreException) {
                        LOG.debug("Node not found in Etcd, trying ZK: {}", path);
                        try {
                          return zkStore.getSync(path);
                        } catch (Exception e) {
                          // If it also fails in ZK, throw the original exception
                          throw new RuntimeException(e);
                        }
                      } else {
                        // For other types of errors, rethrow
                        throw new RuntimeException(throwable);
                      }
                    } else {
                      // No ZK store, rethrow the original exception
                      throw new RuntimeException(throwable);
                    }
                  });
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param path the path to the node
   * @return the node if found. Never returns null.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the node doesn't exist or
   *     another error occurs
   * @throws InternalMetadataStoreException from EtcdMetadataStore if the node doesn't exist or
   *     another error occurs
   */
  public T getSync(String path) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Try ZK first, fall back to Etcd if not found
        try {
          return zkStore.getSync(path);
        } catch (InternalMetadataStoreException e) {
          // If the node doesn't exist in ZK, try Etcd
          if (etcdStore != null) {
            LOG.debug("Node not found in ZK, trying Etcd: {}", path);
            return etcdStore.getSync(path);
          } else {
            // No etcd store, rethrow the original exception
            throw e;
          }
        }
      case ETCD_CREATES:
        // Try Etcd first, fall back to ZK if not found
        try {
          return etcdStore.getSync(path);
        } catch (InternalMetadataStoreException e) {
          // If the node doesn't exist in Etcd, try ZK
          if (zkStore != null) {
            LOG.debug("Node not found in Etcd, trying ZK: {}", path);
            return zkStore.getSync(path);
          } else {
            // No ZK store, rethrow the original exception
            throw e;
          }
        }
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists asynchronously.
   *
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise. In
   *     exclusive modes, the result is based on just the active store.
   *     <p>NOTE: Unlike getAsync(), this method does not throw exceptions if the node doesn't
   *     exist. It's designed to safely check existence and handle the case where the node is not
   *     found.
   *     <p>If other exceptions occur (e.g., connection problems), the CompletionStage will complete
   *     exceptionally.
   */
  public CompletionStage<Boolean> hasAsync(String path) {
    return switch (mode) {
      case ZOOKEEPER_CREATES ->
          // Try ZK first, fall back to Etcd if not found
          zkStore
              .hasAsync(path)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return CompletableFuture.completedFuture(true);
                    } else if (etcdStore != null) {
                      // Try Etcd as fallback
                      return etcdStore.hasAsync(path);
                    } else {
                      return CompletableFuture.completedFuture(false);
                    }
                  });
      case ETCD_CREATES ->
          // Try Etcd first, fall back to ZK if not found
          etcdStore
              .hasAsync(path)
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return CompletableFuture.completedFuture(true);
                    } else if (zkStore != null) {
                      // Try ZK as fallback
                      return zkStore.hasAsync(path);
                    } else {
                      return CompletableFuture.completedFuture(false);
                    }
                  });
      default -> throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    };
  }

  /**
   * Checks if a node exists synchronously.
   *
   * @param path the path to check
   * @return true if the node exists, false otherwise.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws RuntimeException from ZookeeperMetadataStore or EtcdMetadataStore if a connection error
   *     or other unrecoverable error occurs. Note that the node not existing is NOT treated as an
   *     error; in that case, the method returns false.
   */
  public boolean hasSync(String path) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Try ZK first, fall back to Etcd if not found
        boolean existsInZk = zkStore.hasSync(path);
        if (existsInZk) {
          return true;
        } else if (etcdStore != null) {
          // Try Etcd as fallback
          return etcdStore.hasSync(path);
        } else {
          return false;
        }
      case ETCD_CREATES:
        // Try Etcd first, fall back to ZK if not found
        boolean existsInEtcd = etcdStore.hasSync(path);
        if (existsInEtcd) {
          return true;
        } else if (zkStore != null) {
          // Try ZK as fallback
          return zkStore.hasSync(path);
        } else {
          return false;
        }
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if an ETCD node exists synchronously.
   *
   * @param path the path to check
   * @return true if the node exists, false otherwise.
   * @throws RuntimeException from EtcdMetadataStore if a connection error or other unrecoverable
   *     error occurs. Note that the node not existing is NOT treated as an error; in that case, the
   *     method returns false.
   */
  public boolean hasEtcdOnlySync(String path) {
    return etcdStore.hasSync(path);
  }

  /**
   * Updates a node asynchronously.
   *
   * @param metadataNode the node to update
   * @return a CompletionStage that completes with the node version when the operation is done. The
   *     version is implementation-specific and may be empty. In case of failure, the
   *     CompletionStage will complete exceptionally.
   *     <ul>
   *       <li>ZookeeperMetadataStore: returns the ZooKeeper version as a String
   *       <li>EtcdMetadataStore: returns the node name
   *     </ul>
   *
   * @throws IllegalArgumentException if the metadata store mode is invalid
   */
  public CompletionStage<String> updateAsync(T metadataNode) {
    return switch (mode) {
      case ZOOKEEPER_CREATES ->
          // Try ZK first, fall back to Etcd if not found
          zkStore
              .hasAsync(metadataNode.getName())
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return zkStore.updateAsync(metadataNode);
                    } else {
                      // Try Etcd
                      return etcdStore
                          .updateAsync(metadataNode)
                          .exceptionally(
                              throwable -> {
                                throw new RuntimeException(throwable);
                              });
                    }
                  });
      case ETCD_CREATES ->
          // Try Etcd first, fall back to ZK if not found
          etcdStore
              .hasAsync(metadataNode.getName())
              .thenCompose(
                  exists -> {
                    if (exists) {
                      return etcdStore
                          .updateAsync(metadataNode)
                          .exceptionally(
                              throwable -> {
                                throw new RuntimeException(throwable);
                              });
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
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the node doesn't exist or
   *     another error occurs
   * @throws RuntimeException from EtcdMetadataStore if serialization fails or another error occurs
   */
  public void updateSync(T metadataNode) {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        // Try ZK first, fall back to Etcd if not found
        try {
          if (zkStore.hasSync(metadataNode.getName())) {
            zkStore.updateSync(metadataNode);
          } else {
            // Try Etcd
            etcdStore.updateSync(metadataNode);
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
        break;
      case ETCD_CREATES:
        // Try Etcd first, fall back to ZK if not found
        try {
          if (etcdStore.hasSync(metadataNode.getName())) {
            etcdStore.updateSync(metadataNode);
          } else {
            // Try ZK
            zkStore.updateSync(metadataNode);
          }
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node asynchronously by path.
   *
   * @param path the path to delete
   * @return a CompletionStage that completes when the operation is done. In case of failure, the
   *     CompletionStage will complete exceptionally.
   * @throws IllegalStateException if the node does not exist in either store
   */
  public CompletionStage<Void> deleteAsync(String path) {
    return hasAsync(path)
        .thenCompose(
            exists -> {
              if (!exists) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(
                    new IllegalStateException("Node does not exist in either store: " + path));
                return future;
              }

              // We know it exists in at least one store, now check both
              CompletableFuture<Boolean> zkExistsFuture =
                  zkStore != null
                      ? zkStore.hasAsync(path).toCompletableFuture()
                      : CompletableFuture.completedFuture(false);

              CompletableFuture<Boolean> etcdExistsFuture =
                  etcdStore != null
                      ? etcdStore.hasAsync(path).toCompletableFuture()
                      : CompletableFuture.completedFuture(false);

              return CompletableFuture.allOf(zkExistsFuture, etcdExistsFuture)
                  .thenCompose(
                      unused -> {
                        boolean existsInZk = zkExistsFuture.join();
                        boolean existsInEtcd = etcdExistsFuture.join();

                        // Handle case where node exists in both stores
                        if (existsInZk && existsInEtcd) {
                          LOG.warn(
                              "Node exists in both ZK and Etcd stores, which should never happen: {}",
                              path);
                          // Delete from both stores based on current mode
                          if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
                            CompletionStage<Void> zkResult = zkStore.deleteAsync(path);
                            // Also delete from Etcd but don't wait for it
                            if (etcdStore != null) {
                              try {
                                etcdStore.deleteAsync(path);
                              } catch (Exception e) {
                                LOG.warn(
                                    "Failed to delete node from secondary Etcd store: {}", path, e);
                              }
                            }
                            return zkResult;
                          } else { // ETCD_CREATES
                            CompletionStage<Void> etcdResult = etcdStore.deleteAsync(path);
                            // Also delete from ZK but don't wait for it
                            if (zkStore != null) {
                              try {
                                zkStore.deleteAsync(path);
                              } catch (Exception e) {
                                LOG.warn(
                                    "Failed to delete node from secondary ZK store: {}", path, e);
                              }
                            }
                            return etcdResult;
                          }
                        }

                        // Handle case where node exists in only one store
                        if (existsInZk) {
                          return zkStore.deleteAsync(path);
                        } else { // existsInEtcd
                          return etcdStore.deleteAsync(path);
                        }
                      });
            });
  }

  /**
   * Deletes a node synchronously by path.
   *
   * @param path the path to delete
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore or EtcdMetadataStore if
   *     another error occurs
   * @throws IllegalStateException if the node does not exist in either store
   */
  public void deleteSync(String path) {
    boolean existsInZk = zkStore != null && zkStore.hasSync(path);
    boolean existsInEtcd = etcdStore != null && etcdStore.hasSync(path);

    // Check if node exists in either store
    if (!existsInZk && !existsInEtcd) {
      throw new IllegalStateException("Node does not exist in either store: " + path);
    }

    // Handle case where node exists in both stores
    if (existsInZk && existsInEtcd) {
      LOG.warn("Node exists in both ZK and Etcd stores, which should never happen: {}", path);
      // Delete from both stores based on current mode
      if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
        zkStore.deleteSync(path); // Primary store - let exceptions bubble up
        try {
          etcdStore.deleteSync(path); // Secondary store
        } catch (Exception e) {
          LOG.warn("Failed to delete node from secondary Etcd store: {}", path, e);
        }
      } else { // ETCD_CREATES
        etcdStore.deleteSync(path); // Primary store - let exceptions bubble up
        try {
          zkStore.deleteSync(path); // Secondary store
        } catch (Exception e) {
          LOG.warn("Failed to delete node from secondary ZK store: {}", path, e);
        }
      }
      return;
    }

    // Handle case where node exists in only one store
    if (existsInZk) {
      zkStore.deleteSync(path); // Let exceptions bubble up
    } else { // existsInEtcd
      etcdStore.deleteSync(path); // Let exceptions bubble up
    }
  }

  /**
   * Deletes a node asynchronously by metadata object reference.
   *
   * @param metadataNode the node to delete
   * @return a CompletionStage that completes when the operation is done. In case of failure, the
   *     CompletionStage will complete exceptionally.
   * @throws IllegalStateException if the node does not exist in either store
   */
  public CompletionStage<Void> deleteAsync(T metadataNode) {
    String path = metadataNode.getName();
    return hasAsync(path)
        .thenCompose(
            exists -> {
              if (!exists) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                future.completeExceptionally(
                    new IllegalStateException("Node does not exist in either store: " + path));
                return future;
              }

              // We know it exists in at least one store, now check both
              CompletableFuture<Boolean> zkExistsFuture =
                  zkStore != null
                      ? zkStore.hasAsync(path).toCompletableFuture()
                      : CompletableFuture.completedFuture(false);

              CompletableFuture<Boolean> etcdExistsFuture =
                  etcdStore != null
                      ? etcdStore.hasAsync(path).toCompletableFuture()
                      : CompletableFuture.completedFuture(false);

              return CompletableFuture.allOf(zkExistsFuture, etcdExistsFuture)
                  .thenCompose(
                      unused -> {
                        boolean existsInZk = zkExistsFuture.join();
                        boolean existsInEtcd = etcdExistsFuture.join();

                        // Handle case where node exists in both stores
                        if (existsInZk && existsInEtcd) {
                          LOG.warn(
                              "Node exists in both ZK and Etcd stores, which should never happen: {}",
                              path);
                          // Delete from both stores based on current mode
                          if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
                            CompletionStage<Void> zkResult = zkStore.deleteAsync(metadataNode);
                            // Also delete from Etcd but don't wait for it
                            if (etcdStore != null) {
                              try {
                                etcdStore.deleteAsync(metadataNode);
                              } catch (Exception e) {
                                LOG.warn(
                                    "Failed to delete node from secondary Etcd store: {}", path, e);
                              }
                            }
                            return zkResult;
                          } else { // ETCD_CREATES
                            CompletionStage<Void> etcdResult = etcdStore.deleteAsync(metadataNode);
                            // Also delete from ZK but don't wait for it
                            if (zkStore != null) {
                              try {
                                zkStore.deleteAsync(metadataNode);
                              } catch (Exception e) {
                                LOG.warn(
                                    "Failed to delete node from secondary ZK store: {}", path, e);
                              }
                            }
                            return etcdResult;
                          }
                        }

                        // Handle case where node exists in only one store
                        if (existsInZk) {
                          return zkStore.deleteAsync(metadataNode);
                        } else { // existsInEtcd
                          return etcdStore.deleteAsync(metadataNode);
                        }
                      });
            });
  }

  /**
   * Deletes a node synchronously by metadata object reference.
   *
   * @param metadataNode the node to delete
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore or EtcdMetadataStore if
   *     another error occurs
   * @throws IllegalStateException if the node does not exist in either store
   */
  public void deleteSync(T metadataNode) {
    String path = metadataNode.getName();
    boolean existsInZk = zkStore != null && zkStore.hasSync(path);
    boolean existsInEtcd = etcdStore != null && etcdStore.hasSync(path);

    // Check if node exists in either store
    if (!existsInZk && !existsInEtcd) {
      throw new IllegalStateException("Node does not exist in either store: " + path);
    }

    // Handle case where node exists in both stores
    if (existsInZk && existsInEtcd) {
      LOG.warn("Node exists in both ZK and Etcd stores, which should never happen: {}", path);
      // Delete from both stores based on current mode
      if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
        zkStore.deleteSync(metadataNode); // Primary store - let exceptions bubble up
        try {
          etcdStore.deleteSync(metadataNode); // Secondary store
        } catch (Exception e) {
          LOG.warn("Failed to delete node from secondary Etcd store: {}", path, e);
        }
      } else { // ETCD_CREATES
        etcdStore.deleteSync(metadataNode); // Primary store - let exceptions bubble up
        try {
          zkStore.deleteSync(metadataNode); // Secondary store
        } catch (Exception e) {
          LOG.warn("Failed to delete node from secondary ZK store: {}", path, e);
        }
      }
      return;
    }

    // Handle case where node exists in only one store
    if (existsInZk) {
      zkStore.deleteSync(metadataNode); // Let exceptions bubble up
    } else { // existsInEtcd
      etcdStore.deleteSync(metadataNode); // Let exceptions bubble up
    }
  }

  /**
   * Deletes a ZK node synchronously by metadata object reference.
   *
   * @param metadataNode the node to delete
   * @throws IllegalStateException if the node does not exist in either store
   */
  public void deleteZkOnlySync(T metadataNode) {
    String path = metadataNode.getName();
    boolean existsInZk = zkStore != null && zkStore.hasSync(path);

    // Handle case where node exists in both stores
    if (existsInZk) {
      zkStore.deleteAsync(metadataNode);
    } else {
      LOG.info("Node does not exist in zk store, not deleting: " + path);
    }
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return a CompletionStage that completes with the list of nodes. The list is never null but may
   *     be empty if no nodes exist. In case of failure, the CompletionStage will complete
   *     exceptionally.
   *     <ul>
   *       <li>ZookeeperMetadataStore: throws UnsupportedOperationException if caching is disabled
   *       <li>EtcdMetadataStore: returns cached results if available, otherwise fetches from Etcd
   *     </ul>
   */
  public CompletionStage<List<T>> listAsync() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Always combine results from both stores for list operations
        CompletionStage<List<T>> zkList =
            zkStore != null
                ? zkStore
                    .listAsync()
                    .exceptionally(
                        ex -> {
                          LOG.warn("Failed to list from ZooKeeper store: {}", ex.getMessage());
                          return List.of();
                        })
                : CompletableFuture.completedFuture(List.of());

        CompletionStage<List<T>> etcdList =
            etcdStore != null
                ? etcdStore
                    .listAsync()
                    .exceptionally(
                        ex -> {
                          LOG.warn("Failed to list from Etcd store: {}", ex.getMessage());
                          return List.of();
                        })
                : CompletableFuture.completedFuture(List.of());

        return zkList.thenCombine(
            etcdList,
            (list1, list2) -> {
              // Combine both lists, using name as identifier
              Map<String, T> combinedMap = new ConcurrentHashMap<>();

              // Add items from ZK store first if ZK is the primary store
              if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
                for (T item : list1) {
                  combinedMap.put(item.name, item);
                }
                for (T item : list2) {
                  combinedMap.putIfAbsent(item.name, item);
                }
              } else { // ETCD_CREATES
                for (T item : list2) {
                  combinedMap.put(item.name, item);
                }
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
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the list operation fails
   * @throws InternalMetadataStoreException from EtcdMetadataStore if the list operation fails
   */
  public List<T> listSync() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
      case ETCD_CREATES:
        // Combine results from both stores
        List<T> zkList = List.of();
        List<T> etcdList = List.of();

        if (zkStore != null) {
          try {
            zkList = zkStore.listSync();
          } catch (Exception e) {
            LOG.warn("Failed to list from ZooKeeper store: {}", e.getMessage());
          }
        }

        if (etcdStore != null) {
          try {
            etcdList = etcdStore.listSync();
          } catch (Exception e) {
            LOG.warn("Failed to list from Etcd store: {}", e.getMessage());
          }
        }

        // Combine both lists, using name as identifier
        Map<String, T> combinedMap = new ConcurrentHashMap<>();

        // Add items based on mode priority
        if (mode == MetadataStoreMode.ZOOKEEPER_CREATES) {
          // ZK is primary, so add ZK items first
          for (T item : zkList) {
            combinedMap.put(item.name, item);
          }
          // Then add Etcd items if not already present
          for (T item : etcdList) {
            combinedMap.putIfAbsent(item.name, item);
          }
        } else { // ETCD_CREATES
          // Etcd is primary, so add Etcd items first
          for (T item : etcdList) {
            combinedMap.put(item.name, item);
          }
          // Then add ZK items if not already present
          for (T item : zkList) {
            combinedMap.putIfAbsent(item.name, item);
          }
        }

        return new ArrayList<>(combinedMap.values());

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
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // Only add if not already present
    if (!listeners.contains(watcher)) {
      listeners.add(watcher);

      switch (mode) {
        case ZOOKEEPER_CREATES:
        case ETCD_CREATES:
          // In all modes, only add listeners to non-null stores
          if (zkStore != null) {
            zkStore.addListener(watcher);
          }
          if (etcdStore != null) {
            etcdStore.addListener(watcher);
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
      }
    }
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param watcher the listener to remove
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    if (listeners.remove(watcher)) {
      switch (mode) {
        case ZOOKEEPER_CREATES:
        case ETCD_CREATES:
          // In all modes, only remove listeners from non-null stores
          if (zkStore != null) {
            zkStore.removeListener(watcher);
          }
          if (etcdStore != null) {
            etcdStore.removeListener(watcher);
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
      }
    }
  }

  /**
   * Waits for the cache to be initialized.
   *
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws RuntimeException if the cache initialization times out (applies to
   *     ZookeeperMetadataStore and EtcdMetadataStore which uses RuntimeHalterImpl)
   */
  public void awaitCacheInitialized() {
    switch (mode) {
      case ZOOKEEPER_CREATES:
        zkStore.awaitCacheInitialized();
        // Also initialize Etcd cache for faster fallback if needed
        if (etcdStore != null) {
          try {
            etcdStore.awaitCacheInitialized();
          } catch (Exception e) {
            LOG.warn("Failed to initialize secondary Etcd cache", e);
          }
        }
        break;
      case ETCD_CREATES:
        etcdStore.awaitCacheInitialized();
        // Also initialize ZK cache for faster fallback if needed
        if (zkStore != null) {
          try {
            zkStore.awaitCacheInitialized();
          } catch (Exception e) {
            LOG.warn("Failed to initialize secondary ZK cache", e);
          }
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

  // End of class
}
