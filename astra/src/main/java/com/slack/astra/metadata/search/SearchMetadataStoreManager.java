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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.AsyncStage;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class that manages both legacy and partitioned SearchMetadataStore instances.
 * This provides backward compatibility with the old /search path while enabling
 * scaled operations through the partitioning store.
 */
public class SearchMetadataStoreManager implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SearchMetadataStoreManager.class);
  
  public static final String LEGACY_SEARCH_METADATA_STORE_ZK_PATH = "/search";
  
  private final AstraMetadataStore<SearchMetadata> legacyStore;
  private final SearchMetadataStore partitionedStore;
  private final AstraConfigs.ZookeeperConfig zkConfig;
  
  /**
   * Creates a new SearchMetadataStoreManager that manages both legacy and partitioned stores.
   */
  public SearchMetadataStoreManager(
      AsyncCuratorFramework curatorFramework,
      AstraConfigs.ZookeeperConfig zkConfig,
      MeterRegistry meterRegistry,
      boolean shouldCache) 
      throws Exception {
    this.zkConfig = zkConfig;
    
    // Initialize both stores
    this.legacyStore = new AstraMetadataStore<>(
        curatorFramework,
        zkConfig,
        CreateMode.EPHEMERAL,
        shouldCache,
        new SearchMetadataSerializer().toModelSerializer(),
        LEGACY_SEARCH_METADATA_STORE_ZK_PATH,
        meterRegistry);
        
    this.partitionedStore = new SearchMetadataStore(
        curatorFramework, 
        zkConfig, 
        meterRegistry,
        shouldCache);
        
    LOG.info("Initialized SearchMetadataStoreManager with legacy and partitioned stores");
  }
  
  /**
   * Creates a new search metadata entry in both the legacy and partitioned stores.
   */
  public void createSync(SearchMetadata metadata) {
    try {
      // Try to create in the legacy store first
      CompletableFuture<String> legacyFuture = legacyStore.createAsync(metadata)
          .toCompletableFuture();
          
      // Then try to create in the partitioned store
      CompletableFuture<String> partitionedFuture = partitionedStore.createAsync(metadata)
          .toCompletableFuture();
          
      // Wait for both operations to complete
      CompletableFuture.allOf(legacyFuture, partitionedFuture)
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
          
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating search metadata: " + metadata, e);
    }
  }
  
  /**
   * Updates the searchability of a metadata node in both stores.
   */
  public void updateSearchability(String name, boolean searchable) {
    try {
      // Try to get from legacy store first
      SearchMetadata legacyMetadata = legacyStore.getSync(name);
      legacyMetadata.setSearchable(searchable);
      
      // Try to get from partitioned store
      SearchMetadata partitionedMetadata = partitionedStore.findSync(name);
      partitionedMetadata.setSearchable(searchable);
      
      // Update both stores
      CompletableFuture<Stat> legacyFuture = 
          legacyStore.updateAsync(legacyMetadata).toCompletableFuture();
      CompletableFuture<Stat> partitionedFuture = 
          partitionedStore.updateAsync(partitionedMetadata).toCompletableFuture();
      
      // Wait for both operations to complete
      CompletableFuture.allOf(legacyFuture, partitionedFuture)
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
          
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating searchability for node: " + name, e);
    }
  }
  
  /**
   * Gets a metadata node by name, trying the legacy store first.
   */
  public SearchMetadata getSync(String name) {
    try {
      return legacyStore.getSync(name);
    } catch (InternalMetadataStoreException e) {
      // If not found in legacy store, try partitioned store
      return partitionedStore.findSync(name);
    }
  }
  
  /**
   * Lists all metadata nodes from both stores, deduplicating by name.
   */
  public List<SearchMetadata> listSync() {
    List<SearchMetadata> legacyNodes = legacyStore.listSync();
    List<SearchMetadata> partitionedNodes = partitionedStore.listSync();
    
    // Combine and deduplicate based on name
    return partitionedNodes.stream()
        .filter(node -> !legacyNodes.stream()
            .anyMatch(legacyNode -> legacyNode.name.equals(node.name)))
        .collect(Collectors.toList());
  }
  
  /**
   * Deletes a metadata node from both stores.
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
      
      // Delete from both stores if they exist
      CompletableFuture<Void> legacyFuture = legacyMetadata != null ?
          legacyStore.deleteAsync(legacyMetadata).toCompletableFuture() :
          CompletableFuture.completedFuture(null);
          
      CompletableFuture<Void> partitionedFuture = partitionedMetadata != null ?
          partitionedStore.deleteAsync(partitionedMetadata).toCompletableFuture() :
          CompletableFuture.completedFuture(null);
          
      // Wait for both operations to complete
      CompletableFuture.allOf(legacyFuture, partitionedFuture)
          .get(zkConfig.getZkConnectionTimeoutMs(), TimeUnit.MILLISECONDS);
          
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error deleting node: " + name, e);
    }
  }
  
  /**
   * Adds a listener to both stores.
   */
  public void addListener(AstraMetadataStoreChangeListener<SearchMetadata> listener) {
    legacyStore.addListener(listener);
    partitionedStore.addListener(listener);
  }
  
  /**
   * Removes a listener from both stores.
   */
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