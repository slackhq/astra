package com.slack.astra.metadata.core;

import com.slack.astra.proto.config.AstraConfigs.MetadataStoreMode;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AstraMetadataStore is a bridge implementation that takes both ZookeeperMetadataStore and
 * EtcdMetadataStore implementations. It will route operations to one or both of these
 * implementations depending on the configured mode in MetadataStoreConfig.
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
 *
 * <p>If either zkStore or etcdStore is null, the mode configuration will be overridden and
 * operations will be routed only to the non-null store, regardless of the specified mode.
 */
public class AstraMetadataStore<T extends AstraMetadata> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AstraMetadataStore.class);

  protected final ZookeeperMetadataStore<T> zkStore;
  private final EtcdMetadataStore<T> etcdStore;
  private final MetadataStoreMode mode;

  private final MeterRegistry meterRegistry;

  // Tracks listeners registered, so we can properly register/unregister from both stores
  private final Map<AstraMetadataStoreChangeListener<T>, DualStoreChangeListener<T>> listenerMap =
      new ConcurrentHashMap<>();

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
      this.mode = MetadataStoreMode.ETCD_EXCLUSIVE;
      LOG.info("ZK store is null, overriding mode to ETCD_EXCLUSIVE regardless of configured mode");
    } else if (etcdStore == null && zkStore != null) {
      this.mode = MetadataStoreMode.ZOOKEEPER_EXCLUSIVE;
      LOG.info(
          "Etcd store is null, overriding mode to ZOOKEEPER_EXCLUSIVE regardless of configured mode");
    } else if (etcdStore == null && zkStore == null) {
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
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the node already exists
   *     or another error occurs
   * @throws InternalMetadataStoreException from EtcdMetadataStore if serialization fails or another
   *     error occurs
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
   *       <li>In mixed modes, if the node is not found in the primary store, it will try the
   *           secondary store before throwing an exception. This provides transparent fallback
   *           reading during migration.
   *     </ul>
   */
  public CompletionStage<T> getAsync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.getAsync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.getAsync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .getAsync(path)
            .exceptionally(
                primaryEx -> {
                  // Preserve primary exception for consistent behavior
                  try {
                    // Try fallback to Etcd
                    return etcdStore.getSync(path);
                  } catch (Exception secondaryEx) {
                    // Both failed, throw the primary exception
                    throw (primaryEx instanceof RuntimeException)
                        ? (RuntimeException) primaryEx
                        : new InternalMetadataStoreException("Error fetching node", primaryEx);
                  }
                });
      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .getAsync(path)
            .exceptionally(
                primaryEx -> {
                  // Preserve primary exception for consistent behavior
                  try {
                    // Try fallback to ZK
                    return zkStore.getSync(path);
                  } catch (Exception secondaryEx) {
                    // Both failed, throw the primary exception
                    throw (primaryEx instanceof RuntimeException)
                        ? (RuntimeException) primaryEx
                        : new InternalMetadataStoreException("Error fetching node", primaryEx);
                  }
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
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
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore in mixed modes when the node
   *     is not found in either store after attempted fallback reads
   */
  public T getSync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.getSync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.getSync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        try {
          T result = zkStore.getSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception primaryException) {
          try {
            // Fall through to try Etcd
            return etcdStore.getSync(path);
          } catch (Exception secondaryException) {
            // Both stores failed, throw the primary exception to maintain consistent behavior
            if (primaryException instanceof RuntimeException) {
              throw (RuntimeException) primaryException;
            } else {
              throw new InternalMetadataStoreException("Error fetching node", primaryException);
            }
          }
        }
        // Not found in ZK but didn't throw, try Etcd
        return etcdStore.getSync(path);

      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        try {
          T result = etcdStore.getSync(path);
          if (result != null) {
            return result;
          }
        } catch (Exception primaryException) {
          try {
            // Fall through to try ZK
            return zkStore.getSync(path);
          } catch (Exception secondaryException) {
            // Both stores failed, throw the primary exception to maintain consistent behavior
            if (primaryException instanceof RuntimeException) {
              throw (RuntimeException) primaryException;
            } else {
              throw new InternalMetadataStoreException("Error fetching node", primaryException);
            }
          }
        }
        // Not found in Etcd but didn't throw, try ZK
        return zkStore.getSync(path);

      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists asynchronously.
   *
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise. In
   *     exclusive modes, the result is based on just the active store. In mixed modes, returns true
   *     if the node exists in either store, enabling transparent fallback reading during
   *     migrations.
   *     <p>NOTE: Unlike getAsync(), this method does not throw exceptions if the node doesn't
   *     exist. It's designed to safely check existence and handle the case where the node is not
   *     found.
   *     <p>If other exceptions occur (e.g., connection problems), the CompletionStage will complete
   *     exceptionally.
   */
  public CompletionStage<Boolean> hasAsync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.hasAsync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.hasAsync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        return zkStore
            .hasAsync(path)
            .exceptionally(
                primaryEx -> {
                  // Preserve primary exception for consistent behavior
                  try {
                    // Try fallback to Etcd
                    return etcdStore.hasSync(path);
                  } catch (Exception secondaryEx) {
                    // Both failed, throw the primary exception
                    throw (primaryEx instanceof RuntimeException)
                        ? (RuntimeException) primaryEx
                        : new InternalMetadataStoreException(
                            "Error checking if node exists", primaryEx);
                  }
                });
      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        return etcdStore
            .hasAsync(path)
            .exceptionally(
                primaryEx -> {
                  // Preserve primary exception for consistent behavior
                  try {
                    // Try fallback to ZK
                    return zkStore.hasSync(path);
                  } catch (Exception secondaryEx) {
                    // Both failed, throw the primary exception
                    throw (primaryEx instanceof RuntimeException)
                        ? (RuntimeException) primaryEx
                        : new InternalMetadataStoreException(
                            "Error checking if node exists", primaryEx);
                  }
                });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Checks if a node exists synchronously.
   *
   * @param path the path to check
   * @return true if the node exists, false otherwise. In mixed modes, returns true if the node
   *     exists in either store, enabling transparent fallback reading during migrations.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws RuntimeException from ZookeeperMetadataStore or EtcdMetadataStore if a connection error
   *     or other unrecoverable error occurs. Note that the node not existing is NOT treated as an
   *     error; in that case, the method returns false.
   */
  public boolean hasSync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.hasSync(path);
      case ETCD_EXCLUSIVE:
        return etcdStore.hasSync(path);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Try ZK first, fall back to Etcd if not found
        try {
          boolean primaryResult = zkStore.hasSync(path);
          if (primaryResult) {
            return true; // Short-circuit if found in primary
          }
        } catch (Exception primaryException) {
          try {
            // Fall back to Etcd
            return etcdStore.hasSync(path);
          } catch (Exception secondaryException) {
            // Both failed, throw the primary exception to maintain consistent behavior
            if (primaryException instanceof RuntimeException) {
              throw (RuntimeException) primaryException;
            } else {
              throw new InternalMetadataStoreException(
                  "Error checking if node exists", primaryException);
            }
          }
        }
        // Not found in ZK but didn't throw, try Etcd
        return etcdStore.hasSync(path);
      case BOTH_READ_ETCD_WRITE:
        // Try Etcd first, fall back to ZK if not found
        try {
          boolean primaryResult = etcdStore.hasSync(path);
          if (primaryResult) {
            return true; // Short-circuit if found in primary
          }
        } catch (Exception primaryException) {
          try {
            // Fall back to ZK
            return zkStore.hasSync(path);
          } catch (Exception secondaryException) {
            // Both failed, throw the primary exception to maintain consistent behavior
            if (primaryException instanceof RuntimeException) {
              throw (RuntimeException) primaryException;
            } else {
              throw new InternalMetadataStoreException(
                  "Error checking if node exists", primaryException);
            }
          }
        }
        // Not found in Etcd but didn't throw, try ZK
        return zkStore.hasSync(path);
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
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
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore.updateAsync(metadataNode);
      case ETCD_EXCLUSIVE:
        return etcdStore.updateAsync(metadataNode);
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Write to ZK and clean up Etcd
        // First update directly in ZK
        CompletionStage<String> zkResult =
            zkStore
                .updateAsync(metadataNode)
                .exceptionally(
                    updateEx -> {
                      // Update failed, try to create instead
                      try {
                        // Try to create synchronously as a fallback
                        zkStore.createSync(metadataNode);
                        LOG.debug("Created node in ZK instead of update: {}", metadataNode.name);
                        return ""; // Return empty version since we created rather than updated
                      } catch (Exception createEx) {
                        throw new InternalMetadataStoreException(
                            "Failed to update or create node in ZK: " + metadataNode.name,
                            updateEx);
                      }
                    });

        // After updating ZK, clean up Etcd (don't wait for this or let failures affect the result)
        try {
          etcdStore.deleteAsync(metadataNode);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }

        return zkResult.exceptionally(
            ex -> {
              throw (ex instanceof RuntimeException)
                  ? (RuntimeException) ex
                  : new InternalMetadataStoreException("Error updating node: " + metadataNode, ex);
            });
      case BOTH_READ_ETCD_WRITE:
        // Write to Etcd and clean up ZK
        // First update directly in Etcd
        CompletionStage<String> etcdResult =
            etcdStore
                .updateAsync(metadataNode)
                .exceptionally(
                    updateEx -> {
                      // Update failed, try to create instead
                      try {
                        // Try to create synchronously as a fallback
                        etcdStore.createSync(metadataNode);
                        LOG.debug("Created node in Etcd instead of update: {}", metadataNode.name);
                        return metadataNode.name; // Return name as that's what Etcd returns
                      } catch (Exception createEx) {
                        throw new InternalMetadataStoreException(
                            "Failed to update or create node in Etcd: " + metadataNode.name,
                            updateEx);
                      }
                    });

        // After updating Etcd, clean up ZK (don't wait for this or let failures affect the result)
        try {
          zkStore.deleteAsync(metadataNode);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }

        return etcdResult.exceptionally(
            ex -> {
              throw (ex instanceof RuntimeException)
                  ? (RuntimeException) ex
                  : new InternalMetadataStoreException("Error updating node: " + metadataNode, ex);
            });
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the node doesn't exist or
   *     another error occurs
   * @throws RuntimeException from EtcdMetadataStore if serialization fails or another error occurs
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
      case ZOOKEEPER_EXCLUSIVE:
        try {
          zkStore.updateSync(metadataNode);
        } catch (Exception ex) {
          throw (ex instanceof RuntimeException)
              ? (RuntimeException) ex
              : new InternalMetadataStoreException("Error updating node: " + metadataNode, ex);
        }
        break;
      case ETCD_EXCLUSIVE:
        try {
          etcdStore.updateSync(metadataNode);
        } catch (Exception ex) {
          throw (ex instanceof RuntimeException)
              ? (RuntimeException) ex
              : new InternalMetadataStoreException("Error updating node: " + metadataNode, ex);
        }
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Write to ZK (primary store) first
        try {
          // First attempt direct update in ZK
          try {
            zkStore.updateSync(metadataNode);
            LOG.debug("Updated node in ZK: {}", metadataNode.name);
          } catch (Exception updateEx) {
            // If update fails, try creating instead
            LOG.debug("Update failed, attempting create for node: {}", metadataNode.name);
            zkStore.createSync(metadataNode);
            LOG.debug("Created node in ZK: {}", metadataNode.name);
          }

          // After successful update/create in ZK, try to delete from Etcd (secondary)
          try {
            etcdStore.deleteSync(metadataNode);
            LOG.debug("Deleted node from Etcd after ZK update: {}", metadataNode.name);
          } catch (Exception ignored) {
            // Ignore errors from secondary store deletion
            LOG.debug("Failed to delete from Etcd after ZK update: {}", metadataNode.name);
          }
        } catch (Exception ex) {
          throw new InternalMetadataStoreException(
              "Error updating node in primary store: " + metadataNode.name, ex);
        }
        break;
      case BOTH_READ_ETCD_WRITE:
        // Write to Etcd (primary store) first
        try {
          // First attempt direct update in Etcd
          try {
            etcdStore.updateSync(metadataNode);
            LOG.debug("Updated node in Etcd: {}", metadataNode.name);
          } catch (Exception updateEx) {
            // If update fails, try creating instead
            LOG.debug("Update failed, attempting create for node: {}", metadataNode.name);
            etcdStore.createSync(metadataNode);
            LOG.debug("Created node in Etcd: {}", metadataNode.name);
          }

          // After successful update/create in Etcd, try to delete from ZK (secondary)
          try {
            zkStore.deleteSync(metadataNode);
            LOG.debug("Deleted node from ZK after Etcd update: {}", metadataNode.name);
          } catch (Exception ignored) {
            // Ignore errors from secondary store deletion
            LOG.debug("Failed to delete from ZK after Etcd update: {}", metadataNode.name);
          }
        } catch (Exception ex) {
          throw new InternalMetadataStoreException(
              "Error updating node in primary store: " + metadataNode.name, ex);
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
  public CompletionStage<Void> deleteAsync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore
            .deleteAsync(path)
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException(
                          "Error deleting node at path: " + path, ex);
                });
      case ETCD_EXCLUSIVE:
        return etcdStore
            .deleteAsync(path)
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException(
                          "Error deleting node at path: " + path, ex);
                });
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkResult =
            zkStore
                .deleteAsync(path)
                .exceptionally(
                    ex -> {
                      throw (ex instanceof RuntimeException)
                          ? (RuntimeException) ex
                          : new InternalMetadataStoreException(
                              "Error deleting node from primary store at path: " + path, ex);
                    });
        try {
          etcdStore.deleteAsync(path); // don't await this operation
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        return zkResult;
      case BOTH_READ_ETCD_WRITE:
        // Delete from Etcd and also try to delete from ZK
        CompletionStage<Void> etcdResult =
            etcdStore
                .deleteAsync(path)
                .exceptionally(
                    ex -> {
                      throw (ex instanceof RuntimeException)
                          ? (RuntimeException) ex
                          : new InternalMetadataStoreException(
                              "Error deleting node from primary store at path: " + path, ex);
                    });
        try {
          zkStore.deleteAsync(path); // don't await this operation
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        return etcdResult;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node synchronously by path.
   *
   * @param path the path to delete
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if another error occurs (not
   *     finding the node to delete is NOT considered an error)
   * @throws InternalMetadataStoreException from EtcdMetadataStore if another error occurs (not
   *     finding the node to delete is NOT considered an error)
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
  public void deleteSync(String path) {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        try {
          zkStore.deleteSync(path);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug("Ignoring exception when deleting path {}: {}", path, e.getMessage());
        }
        break;
      case ETCD_EXCLUSIVE:
        try {
          etcdStore.deleteSync(path);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug("Ignoring exception when deleting path {}: {}", path, e.getMessage());
        }
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        try {
          zkStore.deleteSync(path);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug("Ignoring exception when deleting from ZK path {}: {}", path, e.getMessage());
        }
        try {
          etcdStore.deleteSync(path);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        break;
      case BOTH_READ_ETCD_WRITE:
        try {
          etcdStore.deleteSync(path);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug("Ignoring exception when deleting from Etcd path {}: {}", path, e.getMessage());
        }
        try {
          zkStore.deleteSync(path);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node asynchronously by metadata object reference.
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
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore
            .deleteAsync(metadataNode)
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException(
                          "Error deleting node: " + metadataNode.name, ex);
                });
      case ETCD_EXCLUSIVE:
        return etcdStore
            .deleteAsync(metadataNode)
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException(
                          "Error deleting node: " + metadataNode.name, ex);
                });
      case BOTH_READ_ZOOKEEPER_WRITE:
        // Delete from ZK and also try to delete from Etcd
        CompletionStage<Void> zkResult =
            zkStore
                .deleteAsync(metadataNode)
                .exceptionally(
                    ex -> {
                      throw (ex instanceof RuntimeException)
                          ? (RuntimeException) ex
                          : new InternalMetadataStoreException(
                              "Error deleting node from primary store: " + metadataNode.name, ex);
                    });
        try {
          etcdStore.deleteAsync(metadataNode); // don't await this operation
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        return zkResult;
      case BOTH_READ_ETCD_WRITE:
        // Delete from Etcd and also try to delete from ZK
        CompletionStage<Void> etcdResult =
            etcdStore
                .deleteAsync(metadataNode)
                .exceptionally(
                    ex -> {
                      throw (ex instanceof RuntimeException)
                          ? (RuntimeException) ex
                          : new InternalMetadataStoreException(
                              "Error deleting node from primary store: " + metadataNode.name, ex);
                    });
        try {
          zkStore.deleteAsync(metadataNode); // don't await this operation
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        return etcdResult;
      default:
        throw new IllegalArgumentException("Unknown metadata store mode: " + mode);
    }
  }

  /**
   * Deletes a node synchronously by metadata object reference.
   *
   * @param metadataNode the node to delete
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if another error occurs (not
   *     finding the node to delete is NOT considered an error)
   * @throws InternalMetadataStoreException from EtcdMetadataStore if another error occurs (not
   *     finding the node to delete is NOT considered an error)
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
      case ZOOKEEPER_EXCLUSIVE:
        try {
          zkStore.deleteSync(metadataNode);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug(
              "Ignoring exception when deleting node {}: {}", metadataNode.name, e.getMessage());
        }
        break;
      case ETCD_EXCLUSIVE:
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug(
              "Ignoring exception when deleting node {}: {}", metadataNode.name, e.getMessage());
        }
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        try {
          zkStore.deleteSync(metadataNode);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug(
              "Ignoring exception when deleting from ZK node {}: {}",
              metadataNode.name,
              e.getMessage());
        }
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (Exception ignored) {
          // Ignore errors from secondary store
        }
        break;
      case BOTH_READ_ETCD_WRITE:
        try {
          etcdStore.deleteSync(metadataNode);
        } catch (InternalMetadataStoreException e) {
          // Ignore if the node doesn't exist - it might be referencing a never-created node
          // or a node that was already deleted by a concurrent operation
          LOG.debug(
              "Ignoring exception when deleting from Etcd node {}: {}",
              metadataNode.name,
              e.getMessage());
        }
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
   *       <li>ZookeeperMetadataStore: throws UnsupportedOperationException if caching is disabled
   *       <li>EtcdMetadataStore: returns cached results if available, otherwise fetches from Etcd
   *     </ul>
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>In both mixed modes, results from both primary and secondary stores are combined into
   *           a single list, with primary store items taking precedence in case of duplicate names
   *     </ul>
   */
  public CompletionStage<List<T>> listAsync() {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        return zkStore
            .listAsync()
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException(
                          "Error listing items from ZooKeeper", ex);
                });
      case ETCD_EXCLUSIVE:
        return etcdStore
            .listAsync()
            .exceptionally(
                ex -> {
                  throw (ex instanceof RuntimeException)
                      ? (RuntimeException) ex
                      : new InternalMetadataStoreException("Error listing items from Etcd", ex);
                });
      case BOTH_READ_ZOOKEEPER_WRITE:
      case BOTH_READ_ETCD_WRITE:
        boolean isZkPrimary = mode == MetadataStoreMode.BOTH_READ_ZOOKEEPER_WRITE;
        String primaryType = isZkPrimary ? "ZooKeeper" : "Etcd";
        String secondaryType = isZkPrimary ? "Etcd" : "ZooKeeper";

        // Get primary and secondary stores
        CompletionStage<List<T>> primaryList =
            isZkPrimary ? zkStore.listAsync() : etcdStore.listAsync();
        CompletionStage<List<T>> secondaryList =
            isZkPrimary ? etcdStore.listAsync() : zkStore.listAsync();

        // Handle exceptions for primary with appropriate messaging
        primaryList =
            primaryList.exceptionally(
                ex -> {
                  LOG.warn(
                      "Failed to list from primary ({}) store: {}", primaryType, ex.getMessage());
                  return List.of();
                });

        // Handle exceptions for secondary with appropriate messaging
        secondaryList =
            secondaryList.exceptionally(
                ex -> {
                  LOG.warn(
                      "Failed to list from secondary ({}) store: {}",
                      secondaryType,
                      ex.getMessage());
                  return List.of();
                });

        return primaryList.thenCombine(
            secondaryList,
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
   * @return the list of nodes. The list is never null but may be empty if no nodes exist.
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
   * @throws InternalMetadataStoreException from ZookeeperMetadataStore if the list operation fails
   * @throws InternalMetadataStoreException from EtcdMetadataStore if the list operation fails
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
   * Adds a listener for metadata changes. This listener will be notified when nodes are created,
   * updated, or deleted.
   *
   * @param watcher the listener to add
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
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
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws UnsupportedOperationException from ZookeeperMetadataStore if caching is disabled
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
   * @throws IllegalArgumentException if the metadata store mode is invalid
   * @throws RuntimeException if the cache initialization times out (applies to
   *     ZookeeperMetadataStore and EtcdMetadataStore which uses RuntimeHalterImpl)
   *     <p>In exclusive modes, this method simply calls the corresponding store's implementation.
   *     <p>In mixed modes, this method has special behavior:
   *     <ul>
   *       <li>BOTH_READ_ZOOKEEPER_WRITE: Waits only for the ZooKeeper cache to initialize (primary)
   *       <li>BOTH_READ_ETCD_WRITE: Waits only for the Etcd cache to initialize (primary)
   *     </ul>
   *     <p>This ensures that the system will be functional with at least the primary data, without
   *     waiting for the secondary store which may be less critical during migration.
   */
  public void awaitCacheInitialized() {
    switch (mode) {
      case ZOOKEEPER_EXCLUSIVE:
        zkStore.awaitCacheInitialized();
        break;
      case ETCD_EXCLUSIVE:
        etcdStore.awaitCacheInitialized();
        break;
      case BOTH_READ_ZOOKEEPER_WRITE:
        zkStore.awaitCacheInitialized();
        // We don't wait for Etcd in this mode since ZK is primary
        break;
      case BOTH_READ_ETCD_WRITE:
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
