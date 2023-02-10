package com.slack.kaldb.logstore.search.aggregations;

public class DateHistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "date_histogram";
  private final String interval;
  private final long offset = 0;
  private final long minDocCount = 0;

  public DateHistogramAggBuilder(String name, String fieldName, String interval) {
    super(name, fieldName);

    this.interval = interval;
  }

  public String getInterval() {
    return interval;
  }

  public long getOffset() {
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
