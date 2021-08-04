package com.slack.kaldb.elasticsearchApi.searchRequest.aggregations;

public class DateHistogramAggregation extends SearchRequestAggregation {

  private final String interval;
  private final int minDocCount;

  public DateHistogramAggregation(String aggregationKey, String interval, int minDocCount) {
    super(aggregationKey);

    this.interval = interval;
    this.minDocCount = minDocCount;
  }

  public String getInterval() {
    return interval;
  }

  public int getMinDocCount() {
    return minDocCount;
  }
}
