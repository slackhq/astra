package com.slack.kaldb.metadata;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;

/**
 * An interface for the physical metadata store that abstracts the details of the underlying
 * implementation.
 */
public interface MetadataStore {
  void close();

  ListenableFuture<?> createEphemeralNode(String path, String data);

  ListenableFuture<?> create(String path, String data, boolean createMissingParents);

  ListenableFuture<Boolean> exists(String path);

  ListenableFuture<?> put(String path, String data);

  ListenableFuture<String> get(String path);

  ListenableFuture<?> delete(String path);

  ListenableFuture<List<String>> getChildren(String path);
}
