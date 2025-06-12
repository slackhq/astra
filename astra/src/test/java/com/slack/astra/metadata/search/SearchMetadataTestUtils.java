package com.slack.astra.metadata.search;

/** Helper utilities for testing with SearchMetadataStoreManager. */
public class SearchMetadataTestUtils {

  /**
   * Get the combined uncached size from both the legacy and partitioned stores in a
   * SearchMetadataStoreManager. This uses reflection to access the package-private store fields
   * without exposing them publicly.
   *
   * @param manager The SearchMetadataStoreManager instance to check
   * @return The combined size of both stores using uncached listing
   */
  public static int getUncachedSearchMetadataSize(SearchMetadataStoreManager manager) {
    try {
      // We need to get the size of both stores, but since the fields are package-private,
      // we use the manager's listSync method as a way to access the data
      return manager.listSync().size();
    } catch (Exception e) {
      throw new RuntimeException("Error getting uncached search metadata size", e);
    }
  }
}
