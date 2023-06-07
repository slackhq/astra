package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModelSerializer;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbPartitioningMetadataStore<T extends KaldbPartitionedMetadata>
    implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbPartitioningMetadataStore.class);

  private final Map<String, KaldbMetadataStore<T>> metadataStoreMap = new ConcurrentHashMap<>();

  private final List<KaldbMetadataStoreChangeListener<T>> listeners = new CopyOnWriteArrayList<>();

  private final AsyncCuratorFramework curator;

  private final String storeFolder;

  private final CreateMode createMode;
  private final ModelSerializer<T> modelSerializer;

  private final Watcher watcher;

  public KaldbPartitioningMetadataStore(
      AsyncCuratorFramework curator,
      CreateMode createMode,
      ModelSerializer<T> modelSerializer,
      String storeFolder) {
    this.curator = curator;
    this.storeFolder = storeFolder;
    this.createMode = createMode;
    this.modelSerializer = modelSerializer;
    this.watcher = buildWatcher();

    curator
        .addWatch()
        .withMode(AddWatchMode.PERSISTENT) // NOT recursive
        .usingWatcher(watcher)
        .forPath(storeFolder);

    // init stores for each existing path
    curator
        .getChildren()
        .forPath(storeFolder)
        .exceptionallyCompose(
            (throwable) -> {
              if (throwable instanceof KeeperException.NoNodeException) {
                // this isn't a problem, as the node will be created once the first one is attempted
                return CompletableFuture.completedFuture(List.of());
              } else {
                return CompletableFuture.failedFuture(throwable);
              }
            })
        .thenAccept((children) -> children.forEach(this::getOrCreateMetadataStore))
        .toCompletableFuture()
        .join();
  }

  private Watcher buildWatcher() {
    return event -> {
      if (event.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
        curator
            .getChildren()
            .forPath(storeFolder)
            .thenAcceptAsync(
                (partitions) -> {
                  // create internal stores foreach partition that do not already exist
                  partitions.forEach(this::getOrCreateMetadataStore);

                  // remove metadata stores that exist in memory but no longer exist on ZK
                  Set<String> partitionsToRemove =
                      Sets.difference(metadataStoreMap.keySet(), Sets.newHashSet(partitions));
                  partitionsToRemove.forEach(
                      partition -> {
                        LOG.info("Closing unused store for partition - {}", partition);
                        KaldbMetadataStore<T> store = metadataStoreMap.remove(partition);
                        store.close();
                      });
                });
      }
    };
  }

  public CompletionStage<String> createAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).createAsync(metadataNode);
  }

  public void createSync(T metadataNode) {
    try {
      createAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error creating node " + metadataNode, e);
    }
  }

  public CompletionStage<T> getAsync(String partition, String path) {
    return getOrCreateMetadataStore(partition).getAsync(path);
  }

  public T getSync(String partition, String path) {
    try {
      return getAsync(partition, path)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  @Deprecated
  public CompletionStage<T> findAsync(String path) {
    return getOrCreateMetadataStore(findPartition(path)).getAsync(path);
  }

  @Deprecated
  public T findSync(String path) {
    try {
      return findAsync(path).toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error fetching node at path " + path, e);
    }
  }

  public CompletionStage<Stat> updateAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).updateAsync(metadataNode);
  }

  public void updateSync(T metadataNode) {
    try {
      updateAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error updating node: " + metadataNode, e);
    }
  }

  public CompletionStage<Void> deleteAsync(T metadataNode) {
    return getOrCreateMetadataStore(metadataNode.getPartition()).deleteAsync(metadataNode);
  }

  public void deleteSync(T metadataNode) {
    try {
      deleteAsync(metadataNode)
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException(
          "Error deleting node under at path: " + metadataNode.name, e);
    }
  }

  public CompletableFuture<List<T>> listAsync() {
    List<CompletableFuture<List<T>>> completionStages = new ArrayList<>();
    for (Map.Entry<String, KaldbMetadataStore<T>> metadataStoreEntry :
        metadataStoreMap.entrySet()) {
      completionStages.add(metadataStoreEntry.getValue().listAsync().toCompletableFuture());
    }

    return CompletableFuture.allOf(completionStages.toArray(new CompletableFuture[0]))
        .thenApply(
            (unused) ->
                completionStages.stream()
                    .map(f -> f.toCompletableFuture().join())
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
  }

  public List<T> listSync() {
    try {
      return listAsync().toCompletableFuture().get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing nodes", e);
    }
  }

  /**
   * WARNING: DO NOT USE THIS IN PRODUCTION CODE // todo - consider making this visible only for
   * tests
   */
  @VisibleForTesting
  @Deprecated
  public List<T> listSyncUncached() {
    try {
      List<String> children;
      try {
        children =
            curator
                .getChildren()
                .forPath(storeFolder)
                .toCompletableFuture()
                .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      } catch (ExecutionException executionException) {
        if (executionException.getCause() instanceof KeeperException.NoNodeException) {
          return new ArrayList<>();
        } else {
          throw executionException;
        }
      }

      List<T> results = new ArrayList<>();
      for (String child : children) {
        String path = String.format("%s/%s", storeFolder, child);
        List<String> grandchildren =
            curator
                .getChildren()
                .forPath(path)
                .toCompletableFuture()
                .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);

        for (String grandchild : grandchildren) {
          String grandchildPath = String.format("%s/%s/%s", storeFolder, child, grandchild);
          results.add(
              modelSerializer.deserialize(
                  curator
                      .getData()
                      .forPath(grandchildPath)
                      .toCompletableFuture()
                      .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS)));
        }
      }
      return results;
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing nodes", e);
    }
  }

  private KaldbMetadataStore<T> getOrCreateMetadataStore(String partition) {
    return metadataStoreMap.computeIfAbsent(
        partition,
        (p1) -> {
          String path = String.format("%s/%s", storeFolder, p1);
          LOG.info("Creating new metadata store for partition - {}, at path - {}", partition, path);

          KaldbMetadataStore<T> newStore =
              new KaldbMetadataStore<>(curator, createMode, true, modelSerializer, path);
          listeners.forEach(newStore::addListener);

          return newStore;
        });
  }

  private String findPartition(String path) {
    for (Map.Entry<String, KaldbMetadataStore<T>> metadataStoreEntry :
        metadataStoreMap.entrySet()) {
      if (metadataStoreEntry.getValue().hasSync(path)) {
        return metadataStoreEntry.getKey();
      }
    }
    throw new IllegalArgumentException();
  }

  public void addListener(KaldbMetadataStoreChangeListener<T> watcher) {
    listeners.add(watcher);
    metadataStoreMap.forEach((partition, store) -> listeners.forEach(store::addListener));
  }

  public void removeListener(KaldbMetadataStoreChangeListener<T> watcher) {
    listeners.remove(watcher);
    metadataStoreMap.forEach((partition, store) -> store.removeListener(watcher));
  }

  @Override
  public void close() throws IOException {
    // only remove the watcher we created, since this curator instance is a singleton
    curator.removeWatches().removing(watcher);
    metadataStoreMap.forEach((partition, store) -> store.close());
  }
}
