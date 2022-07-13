package com.slack.kaldb.logstore.search;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final String queryStr;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final int howMany;
  public final int bucketCount;

  public SearchQuery(
      String dataset,
      String queryStr,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      int bucketCount) {
    this.dataset = dataset;
    this.queryStr = queryStr;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.howMany = howMany;
    this.bucketCount = bucketCount;
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "dataset='"
        + dataset
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
