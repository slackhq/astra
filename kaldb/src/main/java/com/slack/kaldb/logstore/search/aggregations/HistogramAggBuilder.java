package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/** Aggregation request type to form a histogram bucketed by a timestamp */
public class HistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "histogram";
  private final String interval;
  private final long minDocCount;

  public HistogramAggBuilder(String name, String fieldName, String interval) {
    super(name, fieldName);
    this.interval = interval;
    this.minDocCount = 1;
  }

  public HistogramAggBuilder(
      String name,
      String fieldName,
      String interval,
      long minDocCount,
      List<AggBuilder> subAggregations) {
    // todo - metadata?
    super(name, Map.of(), subAggregations, fieldName);

    this.interval = interval;
    this.minDocCount = minDocCount;
  }

  public String getInterval() {
    return interval;
  }

  // Unlike DateHistogramAggregationBuilder, HistogramAggregationBuilder takes a double for the
  // interval
  public double getIntervalDouble() {
    return Double.parseDouble(interval);
  }

  public long getMinDocCount() {
    return minDocCount;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HistogramAggBuilder)) return false;
    if (!super.equals(o)) return false;

    HistogramAggBuilder that = (HistogramAggBuilder) o;

    if (minDocCount != that.minDocCount) return false;
    return interval.equals(that.interval);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + interval.hashCode();
    result = 31 * result + (int) (minDocCount ^ (minDocCount >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "HistogramAggBuilder{"
        + "name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + ", interval='"
        + interval
        + '\''
        + ", minDocCount="
        + minDocCount
        + ", field='"
        + field
        + '\''
        + ", missing="
        + missing
        + '}';
  }
}
