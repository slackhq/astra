package com.slack.astra.logstore.search;

import java.util.List;
import org.apache.lucene.search.SortField;
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

  /**
   * Represents a single sort field specification. Contains the field name, sort order
   * (ascending/descending), Lucene sort field type, and optional unmapped type hint from
   * Elasticsearch.
   */
  public static class SortSpec {
    public final String fieldName;
    public final boolean isDescending;
    public final SortField.Type luceneType;
    public final String unmappedType; // Optional Elasticsearch type hint for missing fields

    public SortSpec(
        String fieldName, boolean isDescending, SortField.Type luceneType, String unmappedType) {
      this.fieldName = fieldName;
      this.isDescending = isDescending;
      this.luceneType = luceneType;
      this.unmappedType = unmappedType;
    }

    // Backward-compatible constructor without unmappedType
    public SortSpec(String fieldName, boolean isDescending, SortField.Type luceneType) {
      this(fieldName, isDescending, luceneType, null);
    }

    @Override
    public String toString() {
      return "SortSpec{"
          + "fieldName='"
          + fieldName
          + '\''
          + ", isDescending="
          + isDescending
          + ", luceneType="
          + luceneType
          + ", unmappedType='"
          + unmappedType
          + '\''
          + '}';
    }
  }

  /** Constructor with custom sort fields. */
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

  /**
   * Backward-compatible constructor that defaults to empty sort fields (will use default timestamp
   * descending sort).
   */
  public SearchQuery(
      String dataset,
      long startTimeEpochMs,
      long endTimeEpochMs,
      int howMany,
      List<String> chunkIds,
      QueryBuilder queryBuilder,
      SourceFieldFilter sourceFieldFilter,
      AggregatorFactories.Builder aggregatorFactoriesBuilder) {
    this(
        dataset,
        startTimeEpochMs,
        endTimeEpochMs,
        howMany,
        chunkIds,
        queryBuilder,
        sourceFieldFilter,
        aggregatorFactoriesBuilder,
        List.of()); // Default to empty list for backward compatibility
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
