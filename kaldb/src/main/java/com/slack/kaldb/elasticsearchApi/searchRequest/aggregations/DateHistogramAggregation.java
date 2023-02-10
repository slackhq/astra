package com.slack.kaldb.elasticsearchApi.searchRequest.aggregations;

public class DateHistogramAggregation extends SearchRequestAggregation {

  private final String interval;
  private final int minDocCount;
  private final String fieldName;

  public DateHistogramAggregation(
      String aggregationKey, String interval, int minDocCount, String fieldName) {
    super(aggregationKey);

    this.interval = interval;
    this.minDocCount = minDocCount;
    this.fieldName = fieldName;
  }

  public String getInterval() {
    return interval;
  }

  public int getMinDocCount() {
    return minDocCount;
  }

  public String getFieldName() {
    return fieldName;
  }
}
