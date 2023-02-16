package com.slack.kaldb.elasticsearchApi.searchRequest.aggregations;

import java.util.Map;

@Deprecated
public class DateHistogramAggregation extends SearchRequestAggregation {

  private final String interval;
  private final long minDocCount;
  private final String offset;
  private final String fieldName;

  private final String format;

  private final Map<String, Long> extendedBounds;

  public DateHistogramAggregation(
      String aggregationKey,
      String interval,
      long minDocCount,
      String offset,
      String fieldName,
      String format,
      Map<String, Long> extendedBounds) {
    super(aggregationKey);

    this.interval = interval;
    this.minDocCount = minDocCount;
    this.offset = offset;
    this.fieldName = fieldName;
    this.format = format;
    this.extendedBounds = extendedBounds;
  }

  public String getInterval() {
    return interval;
  }

  public long getMinDocCount() {
    return minDocCount;
  }

  public String getOffset() {
    return offset;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getFormat() {
    return format;
  }

  public Map<String, Long> getExtendedBounds() {
    return extendedBounds;
  }
}
