package com.slack.kaldb.logstore.search.aggregations;

import java.util.Objects;

public class UniqueCountAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "cardinality";
  private final Long precisionThreshold;

  public UniqueCountAggBuilder(String name, String field, Object missing, Long precisionThreshold) {
    super(name, field, missing);

    this.precisionThreshold = precisionThreshold;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public Long getPrecisionThreshold() {
    return precisionThreshold;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UniqueCountAggBuilder)) return false;
    if (!super.equals(o)) return false;

    UniqueCountAggBuilder that = (UniqueCountAggBuilder) o;

    return Objects.equals(precisionThreshold, that.precisionThreshold);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (precisionThreshold != null ? precisionThreshold.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "UniqueCountAggBuilder{"
        + "precisionThreshold="
        + precisionThreshold
        + ", field='"
        + field
        + '\''
        + ", missing="
        + missing
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
