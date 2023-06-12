package com.slack.kaldb.metadata.core;

import static com.slack.kaldb.server.KaldbConfig.DEFAULT_ZK_TIMEOUT_SECS;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.curator.x.async.modeled.ZPath;

/**
 * This is a collection of helpful methods for writing Kaldb tests that use the KaldbMetadataStores
 * that cannot or should not exist in the production client.
 */
public class KaldbMetadataTestUtils {

  /**
   * Listing an uncached directory is very expensive, and not recommended for production code. For a
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
}
