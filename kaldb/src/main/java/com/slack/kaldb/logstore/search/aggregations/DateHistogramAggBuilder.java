package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public class DateHistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "date_histogram";
  private final String interval;
  private final String offset;
  private final long minDocCount;

  private final String format;

  private final Map<String, Long> extendedBounds;

  public DateHistogramAggBuilder(String name, String fieldName, String interval) {
    super(name, fieldName);

    this.interval = interval;
    this.offset = "";
    this.minDocCount = 1;
    this.format = null;
    this.extendedBounds = Map.of();
  }

  public DateHistogramAggBuilder(
      String name,
      String fieldName,
      String interval,
      String offset,
      long minDocCount,
      String format,
      Map<String, Long> extendedBounds,
      List<AggBuilder> subAggregations) {
    // todo - metadata?
    super(name, Map.of(), subAggregations, fieldName);

    this.interval = interval;
    this.offset = offset;
    this.minDocCount = minDocCount;
    this.format = format;
    this.extendedBounds = extendedBounds;
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

  public String getFormat() {
    return format;
  }

  public Map<String, Long> getExtendedBounds() {
    return extendedBounds;
  }

  @Override
  public String getType() {
    return TYPE;
  }
}
