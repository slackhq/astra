package com.slack.astra.metadata.core;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * EtcdMetadataStore is a class which provides consistent Etcd apis for all the metadata store
 * classes.
 *
 * <p>Every method provides an async and a sync API. In general, use the async API you are
 * performing batch operations and a sync if you are performing a synchronous operation on a node.
 */
public class EtcdMetadataStore<T extends AstraMetadata> implements Closeable {
  protected final String storeFolder;

  private final MeterRegistry meterRegistry;

  private final String ASTRA_ETCD_CREATE_CALL = "astra_etcd_create_call";
  private final String ASTRA_ETCD_HAS_CALL = "astra_etcd_has_call";
  private final String ASTRA_ETCD_DELETE_CALL = "astra_etcd_delete_call";
  private final String ASTRA_ETCD_LIST_CALL = "astra_etcd_list_call";
  private final String ASTRA_ETCD_GET_CALL = "astra_etcd_get_call";
  private final String ASTRA_ETCD_UPDATE_CALL = "astra_etcd_update_call";
  private final String ASTRA_ETCD_ADDED_LISTENER = "astra_etcd_added_listener";
  private final String ASTRA_ETCD_REMOVED_LISTENER = "astra_etcd_removed_listener";
  private final String ASTRA_ETCD_CACHE_INIT_HANDLER_FIRED = "astra_etcd_cache_init_handler_fired";

  private final Counter createCall;
  private final Counter hasCall;
  private final Counter deleteCall;
  private final Counter listCall;
  private final Counter getCall;
  private final Counter updateCall;
  private final Counter addedListener;
  private final Counter removedListener;

  public EtcdMetadataStore(boolean shouldCache, MeterRegistry meterRegistry) {

    this.storeFolder = ""; // This will be set by the specific Etcd implementation
    this.meterRegistry = meterRegistry;
    String store = "etcd"; // This will be based on the actual store folder

    this.createCall = this.meterRegistry.counter(ASTRA_ETCD_CREATE_CALL, "store", store);
    this.deleteCall = this.meterRegistry.counter(ASTRA_ETCD_DELETE_CALL, "store", store);
    this.listCall = this.meterRegistry.counter(ASTRA_ETCD_LIST_CALL, "store", store);
    this.getCall = this.meterRegistry.counter(ASTRA_ETCD_GET_CALL, "store", store);
    this.hasCall = this.meterRegistry.counter(ASTRA_ETCD_HAS_CALL, "store", store);
    this.updateCall = this.meterRegistry.counter(ASTRA_ETCD_UPDATE_CALL, "store", store);
    this.addedListener = this.meterRegistry.counter(ASTRA_ETCD_ADDED_LISTENER, "store", store);
    this.removedListener = this.meterRegistry.counter(ASTRA_ETCD_REMOVED_LISTENER, "store", store);
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void createSync(T metadataNode) {
    this.createCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<T> getAsync(String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public T getSync(String path) {
    this.getCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<Boolean> hasAsync(String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public boolean hasSync(String path) {
    this.hasCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<String> updateAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void updateSync(T metadataNode) {
    this.updateCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<Void> deleteAsync(String path) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void deleteSync(String path) {
    this.deleteCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void deleteSync(T metadataNode) {
    this.deleteCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public CompletionStage<List<T>> listAsync() {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public List<T> listSync() {
    this.listCall.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void addListener(AstraMetadataStoreChangeListener<T> watcher) {
    this.addedListener.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void removeListener(AstraMetadataStoreChangeListener<T> watcher) {
    this.removedListener.increment();
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  public void awaitCacheInitialized() {
    // To be implemented
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() {
    // To be implemented
  }
}
