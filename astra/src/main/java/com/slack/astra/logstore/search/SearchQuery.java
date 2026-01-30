package com.slack.astra.logstore.search;

import java.util.List;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;

/** A class that represents a search query internally to LogStore. */
public class SearchQuery {
  // TODO: Remove the dataset field from this class since it is not a lucene level concept.
  @Deprecated public final String dataset;

  public final AggregatorFactories.Builder aggregatorFactoriesBuilder;
  public final QueryBuilder queryBuilder;
  public final int howMany;
  public final List<String> chunkIds;
  public final SourceFieldFilter sourceFieldFilter;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;
  public final List<SortSpec> sortFields;

  public static class SortSpec {
    public final String fieldName;
    public final boolean isDescending;
    public final String unmappedType; // Optional Elasticsearch type hint for missing fields

    public SortSpec(String fieldName, boolean isDescending, String unmappedType) {
      this.fieldName = fieldName;
      this.isDescending = isDescending;
      this.unmappedType = unmappedType;
    }

    @Override
    public String toString() {
      return "SortSpec{"
          + "fieldName='"
          + fieldName
          + '\''
          + ", isDescending="
          + isDescending
          + ", unmappedType='"
          + unmappedType
          + '\''
          + '}';
    }
  }

  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder,
      List<SortSpec> sortFields) {
    this.dataset = dataset;
    this.howMany = howMany;
    this.chunkIds = chunkIds;
    this.queryBuilder = queryBuilder;
    this.sourceFieldFilter = sourceFieldFilter;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
    this.aggregatorFactoriesBuilder = aggregatorFactoriesBuilder;
    this.sortFields = sortFields;
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
        + ", queryBuilder="
        + queryBuilder
        + ", sourceFieldFilter="
        + sourceFieldFilter
        + ", aggregatorFactoriesBuilder="
        + aggregatorFactoriesBuilder
        + ", sortFields="
        + sortFields
        + '}';
  }
}
