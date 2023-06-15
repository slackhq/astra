package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.ZPath;
import org.apache.zookeeper.KeeperException;

/**
 * This is a collection of helpful methods for writing Kaldb tests that use the KaldbMetadataStores
 * that cannot or should not exist in the production client.
 */
public class KaldbMetadataTestUtils {

  /**
   * Listing an uncached directory is very expensive, and NOT recommended for production code. For a
   * directory containing 100 znodes this would result in 100 additional zookeeper queries after the
   * initial listing.
   *
   * <p>To prevent production access, all existing use of this method has been deprecated and
   * removed. However, the ability to perform asserts without dealing with cache timing is useful
   * for testing, so this method still has visibility for all test suites.
   */
  public static <T extends KaldbMetadata> List<T> listSyncUncached(KaldbMetadataStore<T> store) {
    try {
      return store
          .modeledClient
          .withPath(ZPath.parse(store.storeFolder))
          .childrenAsZNodes()
          .thenApply(
              (zNodes) -> zNodes.stream().map(znode -> znode.model()).collect(Collectors.toList()))
          .toCompletableFuture()
          .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new InternalMetadataStoreException("Error listing node", e);
    }
  }

  /**
   * Variation of the listSyncUncached method allowing for a partitioning metadata store to be used.
   * This is known to be very slow, as each call is done synchronously to build the resulting data.
   * As this is a test-only method and operates with an in-memory ZK instance, performance here is
   * not a significant concern. This implementation may be revisited in the future if this method
   * becomes a bottleneck to test performance.
   *
   * @see KaldbMetadataTestUtils#listSyncUncached(KaldbMetadataStore store)
   */
  public static <T extends KaldbPartitionedMetadata> List<T> listSyncUncached(
      KaldbPartitioningMetadataStore<T> store) {
    try {
      List<String> children;
      try {
        children =
            store
                .curator
                .getChildren()
                .forPath(store.storeFolder)
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
        String path = String.format("%s/%s", store.storeFolder, child);
        List<String> grandchildren =
            store
                .curator
                .getChildren()
                .forPath(path)
                .toCompletableFuture()
                .get(DEFAULT_ZK_TIMEOUT_SECS, TimeUnit.SECONDS);

        for (String grandchild : grandchildren) {
          String grandchildPath = String.format("%s/%s/%s", store.storeFolder, child, grandchild);
          results.add(
              store.modelSerializer.deserialize(
                  store
                      .curator
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
}
