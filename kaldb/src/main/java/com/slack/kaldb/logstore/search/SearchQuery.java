package com.slack.kaldb.logstore.search;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  public final String indexName;

  public final String queryStr;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final int howMany;
  public final int bucketCount;

  public SearchQuery(
      String indexName,
      String queryStr,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int bucketCount) {
    this.indexName = indexName;
    this.queryStr = queryStr;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.howMany = howMany;
    this.bucketCount = bucketCount;
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "indexName='"
        + indexName
        + '\''
        + ", queryStr='"
        + queryStr
        + '\''
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + ", howMany="
        + howMany
        + ", bucketCount="
        + bucketCount
        + '}';
  }
}
