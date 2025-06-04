package com.slack.astra.metadata.search;

import com.slack.astra.metadata.core.AstraMetadataStore;
import com.slack.astra.metadata.core.AstraMetadataStoreChangeListener;
import com.slack.astra.metadata.core.InternalMetadataStoreException;
import com.slack.astra.proto.config.AstraConfigs;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class that manages both legacy and partitioned SearchMetadataStore instances. This
 * provides backward compatibility with the old /search path while enabling scaled operations
 * through the partitioning store.
 */
public class SearchMetadataStoreManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStoreManager.class);

  public static final String LEGACY_SEARCH_METADATA_STORE_ZK_PATH = "/search";

  // Made package-private for testing
  final AstraMetadataStore<SearchMetadata> legacyStore;
  final SearchMetadataStore partitionedStore;
  private final AstraConfigs.ZookeeperConfig zkConfig;
  private final SearchMetadataStoreType preferredStoreType;

  /**
   * Creates a new SearchMetadataStoreManager that manages both legacy and partitioned stores.
   *
   * @param curatorFramework The async curator framework
   * @param zkConfig The ZooKeeper config
   * @param meterRegistry The meter registry
   * @param shouldCache Whether to cache metadata
   * @throws Exception If an error occurs initializing the stores
   */
  public SearchMetadataStoreManager(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache)
      throws Exception {
    this(curatorFramework, zkConfig, meterRegistry, shouldCache, SearchMetadataStoreType.LEGACY);
  }

  /**
   * Creates a new SearchMetadataStoreManager that manages both legacy and partitioned stores.
   *
   * @param curatorFramework The async curator framework
   * @param zkConfig The ZooKeeper config
   * @param meterRegistry The meter registry
   * @param shouldCache Whether to cache metadata
   * @param preferredStoreType The preferred store type for read operations
   * @throws Exception If an error occurs initializing the stores
   */
  public SearchMetadataStoreManager(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache,
      SearchMetadataStoreType preferredStoreType)
      throws Exception {
    this.zkConfig = zkConfig;
    this.preferredStoreType = preferredStoreType;

    // Initialize both stores
    this.legacyStore =
        new AstraMetadataStore<>(
            curatorFramework,
            zkConfig,
            CreateMode.EPHEMERAL,
            shouldCache,
            new SearchMetadataSerializer().toModelSerializer(),
            LEGACY_SEARCH_METADATA_STORE_ZK_PATH,
            meterRegistry);

    this.partitionedStore =
        new SearchMetadataStore(curatorFramework, zkConfig, meterRegistry, shouldCache);

    LOG.info("Initialized SearchMetadataStoreManager with legacy and partitioned stores");
  }

  /**
   * Creates a new search metadata entry in the partitioned store only. Legacy store entries will be
   * left unchanged, as we're migrating to the partitioned store.
   */
  public void createSync(SearchMetadata metadata) {
    try {
      // Only create in the partitioned store
      partitionedStore
          .createAsync(metadata)
          .toCompletableFuture()
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating search metadata: " + metadata, e);
    }
  }

  /**
   * Updates the searchability of a metadata node. Updates in both stores if it exists in both,
   * otherwise just in the store where it exists.
   */
  public void updateSearchability(String name, boolean searchable) {
    try {
      boolean existsInLegacy = false;
      boolean existsInPartitioned = false;
      SearchMetadata legacyMetadata = null;
      SearchMetadata partitionedMetadata = null;

      // Try to get from both stores
      try {
        legacyMetadata = legacyStore.getSync(name);
        legacyMetadata.setSearchable(searchable);
        existsInLegacy = true;
      } catch (InternalMetadataStoreException e) {
        // Node might not exist in the legacy store
      }

      try {
        partitionedMetadata = partitionedStore.findSync(name);
        partitionedMetadata.setSearchable(searchable);
        existsInPartitioned = true;
      } catch (InternalMetadataStoreException e) {
        // Node might not exist in the partitioned store
      }

      // Make sure it exists in at least one store
      if (!existsInLegacy && !existsInPartitioned) {
        throw new InternalMetadataStoreException("Could not find metadata for node: " + name);
      }

      // Update both stores where the node exists
      CompletableFuture<Stat> legacyFuture =
          existsInLegacy
              ? legacyStore.updateAsync(legacyMetadata).toCompletableFuture()
              : CompletableFuture.completedFuture(null);

      CompletableFuture<Stat> partitionedFuture =
          existsInPartitioned
              ? partitionedStore.updateAsync(partitionedMetadata).toCompletableFuture()
              : CompletableFuture.completedFuture(null);

      // Wait for all operations to complete
      CompletableFuture.allOf(legacyFuture, partitionedFuture)
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating searchability for node: " + name, e);
    }
  }

  /**
   * Gets a metadata node by name, trying the preferred store first based on configuration.
   *
   * @param name The name of the metadata node to retrieve
   * @return The SearchMetadata if found
   * @throws InternalMetadataStoreException if the node can't be found in either store
   */
  public SearchMetadata getSync(String name) {
    if (preferredStoreType == SearchMetadataStoreType.LEGACY) {
      try {
        return legacyStore.getSync(name);
      } catch (InternalMetadataStoreException e) {
        // If not found in legacy store, try partitioned store
        return partitionedStore.findSync(name);
      }
    } else {
      try {
        return partitionedStore.findSync(name);
      } catch (InternalMetadataStoreException e) {
        // If not found in partitioned store, try legacy store
        return legacyStore.getSync(name);
      }
    }
  }

  /**
   * Lists all metadata nodes from both stores, deduplicating by name. With LEGACY preferred type,
   * legacy nodes take precedence. With PARTITIONED preferred type, partitioned nodes take
   * precedence.
   *
   * @return A combined list of metadata from both stores
   */
  public List<SearchMetadata> listSync() {
    List<SearchMetadata> legacyNodes = legacyStore.listSync();
    List<SearchMetadata> partitionedNodes = partitionedStore.listSync();

    // Combine and deduplicate based on name and preferred store type
    if (preferredStoreType == SearchMetadataStoreType.LEGACY) {
      // Add partitioned nodes that don't exist in legacy store
      return Stream.concat(
              legacyNodes.stream(),
              partitionedNodes.stream()
                  .filter(
                      node ->
                          !legacyNodes.stream()
                              .anyMatch(legacyNode -> legacyNode.name.equals(node.name))))
          .collect(Collectors.toList());
    } else {
      // Add legacy nodes that don't exist in partitioned store
      return Stream.concat(
              partitionedNodes.stream(),
              legacyNodes.stream()
                  .filter(
                      node ->
                          !partitionedNodes.stream()
                              .anyMatch(partitionedNode -> partitionedNode.name.equals(node.name))))
          .collect(Collectors.toList());
    }
  }

  /**
   * Deletes a metadata node from both stores if it exists. Ensures we clean up legacy data while
   * migrating to the partitioned store.
   */
  public void deleteSync(String name) {
    try {
      // Try to get from legacy store first
      SearchMetadata legacyMetadata = null;
      SearchMetadata partitionedMetadata = null;

      try {
        legacyMetadata = legacyStore.getSync(name);
      } catch (InternalMetadataStoreException e) {
        // Node might not exist in the legacy store
      }

      try {
        partitionedMetadata = partitionedStore.findSync(name);
      } catch (InternalMetadataStoreException e) {
        // Node might not exist in the partitioned store
      }

      if (legacyMetadata == null && partitionedMetadata == null) {
        throw new InternalMetadataStoreException(
            "Could not find metadata for node: " + name + " in either store");
      }

      // Delete from both stores if they exist
      CompletableFuture<Void> legacyFuture =
          legacyMetadata != null
              ? legacyStore.deleteAsync(legacyMetadata).toCompletableFuture()
              : CompletableFuture.completedFuture(null);

      CompletableFuture<Void> partitionedFuture =
          partitionedMetadata != null
              ? partitionedStore.deleteAsync(partitionedMetadata).toCompletableFuture()
              : CompletableFuture.completedFuture(null);

      // Wait for both operations to complete
      CompletableFuture.allOf(legacyFuture, partitionedFuture)
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);

    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error deleting node: " + name, e);
    }
  }

  /** Adds a listener to both stores. */
  public void addListener(AstraMetadataStoreChangeListener<SearchMetadata> listener) {
    legacyStore.addListener(listener);
    partitionedStore.addListener(listener);
  }

  /** Removes a listener from both stores. */
  public void removeListener(AstraMetadataStoreChangeListener<SearchMetadata> listener) {
    legacyStore.removeListener(listener);
    partitionedStore.removeListener(listener);
  }

  @Override
  public void close() throws IOException {
    try {
      legacyStore.close();
    } catch (Exception e) {
      LOG.error("Error closing legacy search metadata store", e);
    }

    try {
      partitionedStore.close();
    } catch (Exception e) {
      LOG.error("Error closing partitioned search metadata store", e);
    }
  }
}
