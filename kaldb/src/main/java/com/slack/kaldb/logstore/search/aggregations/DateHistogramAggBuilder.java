package com.slack.kaldb.logstore.search.aggregations;

public class DateHistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "date_histogram";
  private final String interval;
  private final String offset;
  private final long minDocCount;

  public DateHistogramAggBuilder(String name, String fieldName, String interval) {
    super(name, fieldName);

    this.interval = interval;
    this.offset = "";
    this.minDocCount = 0;
  }

  public DateHistogramAggBuilder(
      String name, String fieldName, String interval, String offset, long minDocCount) {
    super(name, fieldName);

    this.interval = interval;
    this.offset = offset;
    this.minDocCount = minDocCount;
  }

  public String getInterval() {
    return interval;
  }

  public String getOffset() {
    return offset;
  }

  public long getMinDocCount() {
    return minDocCount;
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
