package com.slack.astra.metadata.core;

import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EtcdPartitioningMetadataStore is a class which provides consistent Etcd apis for partitioned
 * metadata store operations.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 *
 * <p>This class is the Etcd counterpart to ZookeeperPartitioningMetadataStore. It is designed to be
 * used with AstraPartitioningMetadataStore for migrating between Zookeeper and Etcd.
 */
public class EtcdPartitioningMetadataStore<T extends AstraPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(EtcdPartitioningMetadataStore.class);
  protected final String storeFolder;

  private final MeterRegistry meterRegistry;

  public EtcdPartitioningMetadataStore(
      boolean shouldCache, MeterRegistry meterRegistry, String storeFolder) {
    this.storeFolder = storeFolder;
    this.meterRegistry = meterRegistry;
  }

  /**
   * Creates a new metadata node asynchronously.
   *
   * @param metadataNode the node to create
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<String> createAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Creates a new metadata node synchronously.
   *
   * @param metadataNode the node to create
   */
  public void createSync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Gets a metadata node asynchronously.
   *
   * @param partition the partition to look in
   * @param path the path to the node
   * @return a CompletionStage that completes with the node
   */
  public CompletionStage<T> getAsync(String partition, String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Gets a metadata node synchronously.
   *
   * @param partition the partition to look in
   * @param path the path to the node
   * @return the node
   */
  public T getSync(String partition, String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Attempts to find the metadata without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return a CompletionStage that completes with the node
   */
  public CompletionStage<T> findAsync(String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Attempts to find the metadata synchronously without knowledge of the partition it exists in.
   *
   * @param path the path to the node
   * @return the node
   */
  public T findSync(String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Updates a node asynchronously.
   *
   * @param metadataNode the node to update
   * @return a CompletionStage that completes when the operation is done
   */
  /**
   * Checks if a node exists asynchronously in a specific partition.
   *
   * @param partition the partition to check in
   * @param path the path to check
   * @return a CompletionStage that completes with true if the node exists, false otherwise
   */
  public CompletionStage<Boolean> hasAsync(String partition, String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Checks if a node exists synchronously in a specific partition.
   *
   * @param partition the partition to check in
   * @param path the path to check
   * @return true if the node exists, false otherwise
   */
  public boolean hasSync(String partition, String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<String> updateAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   */
  public void updateSync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Deletes a node asynchronously.
   *
   * @param metadataNode the node to delete
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<Void> deleteAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Deletes a node synchronously.
   *
   * @param metadataNode the node to delete
   */
  public void deleteSync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Lists all nodes asynchronously.
   *
   * @return a CompletionStage that completes with the list of nodes
   */
  public CompletionStage<List<T>> listAsync() {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Lists all nodes synchronously.
   *
   * @return the list of nodes
   */
  public List<T> listSync() {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param watcher the listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param watcher the listener to remove
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /** Waits for the cache to be initialized. */
  public void awaitCacheInitialized() {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() {
    // To be implemented
  }
}
