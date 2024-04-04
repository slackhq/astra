package com.slack.astra.logstore.search.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Aggregation request type to form an automatic histogram bucketed by a timestamp */
public class AutoDateHistogramAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "auto_date_histogram";
  private final String minInterval;
  private final Integer numBuckets;

  public AutoDateHistogramAggBuilder(
      String name,
      String fieldName,
      String minInterval,
      Integer numBuckets,
      List<AggBuilder> subAggregations) {
    // todo - metadata?
    super(name, Map.of(), subAggregations, fieldName);

    this.minInterval = minInterval;
    this.numBuckets = numBuckets;
  }

  public String getMinInterval() {
    return minInterval;
  }

  public Integer getNumBuckets() {
    return numBuckets;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AutoDateHistogramAggBuilder that)) return false;
    if (!super.equals(o)) return false;

    if (!Objects.equals(minInterval, that.minInterval)) return false;
    return Objects.equals(numBuckets, that.numBuckets);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (minInterval != null ? minInterval.hashCode() : 0);
    result = 31 * result + (numBuckets != null ? numBuckets.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AutoDateHistogramAggBuilder{"
        + "minInterval='"
        + minInterval
        + '\''
        + ", numBuckets="
        + numBuckets
        + ", field='"
        + field
        + '\''
        + ", missing="
        + missing
        + ", script='"
        + script
        + '\''
        + ", name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + '}';
  }
}
