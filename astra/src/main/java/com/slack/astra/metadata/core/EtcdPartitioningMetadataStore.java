package com.slack.astra.metadata.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.zookeeper.data.Stat;
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

  private final String ASTRA_ETCD_PARTITIONING_CREATE_CALL = "astra_etcd_partitioning_create_call";
  private final String ASTRA_ETCD_PARTITIONING_DELETE_CALL = "astra_etcd_partitioning_delete_call";
  private final String ASTRA_ETCD_PARTITIONING_LIST_CALL = "astra_etcd_partitioning_list_call";
  private final String ASTRA_ETCD_PARTITIONING_GET_CALL = "astra_etcd_partitioning_get_call";
  private final String ASTRA_ETCD_PARTITIONING_UPDATE_CALL = "astra_etcd_partitioning_update_call";
  private final String ASTRA_ETCD_PARTITIONING_ADDED_LISTENER =
      "astra_etcd_partitioning_added_listener";
  private final String ASTRA_ETCD_PARTITIONING_REMOVED_LISTENER =
      "astra_etcd_partitioning_removed_listener";
  private final String ASTRA_ETCD_PARTITIONING_CACHE_INIT_HANDLER_FIRED =
      "astra_etcd_partitioning_cache_init_handler_fired";

  private final Counter createCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;

  public EtcdPartitioningMetadataStore(
      boolean shouldCache, MeterRegistry meterRegistry, String storeFolder) {
    this.storeFolder = storeFolder;
    this.meterRegistry = meterRegistry;
    String store = "etcd_partitioning";

    this.createCall =
        this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_CREATE_CALL, "store", store);
    this.deleteCall =
        this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_DELETE_CALL, "store", store);
    this.listCall = this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_LIST_CALL, "store", store);
    this.getCall = this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_GET_CALL, "store", store);
    this.updateCall =
        this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_UPDATE_CALL, "store", store);
    this.addedListener =
        this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_ADDED_LISTENER, "store", store);
    this.removedListener =
        this.meterRegistry.counter(ASTRA_ETCD_PARTITIONING_REMOVED_LISTENER, "store", store);
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
    this.createCall.increment();
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
    this.getCall.increment();
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
    this.getCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Updates a node asynchronously.
   *
   * @param metadataNode the node to update
   * @return a CompletionStage that completes when the operation is done
   */
  public CompletionStage<Stat> updateAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Updates a node synchronously.
   *
   * @param metadataNode the node to update
   */
  public void updateSync(T metadataNode) {
    this.updateCall.increment();
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
    this.deleteCall.increment();
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
    this.listCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Adds a listener for metadata changes.
   *
   * @param watcher the listener to add
   */
  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    this.addedListener.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  /**
   * Removes a listener for metadata changes.
   *
   * @param watcher the listener to remove
   */
  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    this.removedListener.increment();
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
