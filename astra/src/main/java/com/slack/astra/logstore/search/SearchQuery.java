package com.slack.astra.logstore.search;

import com.slack.astra.logstore.search.aggregations.AggBuilder;
import java.util.List;
import org.opensearch.index.query.QueryBuilder;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final String queryStr;
  public final QueryBuilder queryBuilder;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final int howMany;
  public final AggBuilder aggBuilder;
  public final List<String> chunkIds;

  public SearchQuery(
      String dataset,
      String queryStr,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      AggBuilder aggBuilder,
      List<String> chunkIds,
      QueryBuilder queryBuilder) {
    this.dataset = dataset;
    this.queryStr = queryStr;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.howMany = howMany;
    this.aggBuilder = aggBuilder;
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
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
        + ", chunkIds="
        + chunkIds
        + ", aggBuilder="
        + aggBuilder
        + ", queryBuilder="
        + queryBuilder
        + '}';
  }
}
