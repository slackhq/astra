package com.slack.astra.logstore.search;

import com.slack.astra.logstore.search.aggregations.AggBuilder;
import java.util.List;
import org.opensearch.index.query.QueryBuilder;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final QueryBuilder queryBuilder;
  public final int howMany;
  public final AggBuilder aggBuilder;
  public final List<String> chunkIds;
  public final SourceFieldFilter sourceFieldFilter;

  public SearchQuery(
      String dataset,
      int howMany,
      AggBuilder aggBuilder,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter) {
    this.dataset = dataset;
    this.howMany = howMany;
    this.aggBuilder = aggBuilder;
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
    this.sourceFieldFilter = sourceFieldFilter;
  }

  @Override
  public String toString() {
    return "SearchQuery{"
        + "dataset='"
        + dataset
        + '\''
        + ", howMany="
        + howMany
        + ", chunkIds="
        + chunkIds
        + ", aggBuilder="
        + aggBuilder
        + ", queryBuilder="
        + queryBuilder
        + ", sourceFieldFilter="
        + sourceFieldFilter
        + '}';
  }
}
