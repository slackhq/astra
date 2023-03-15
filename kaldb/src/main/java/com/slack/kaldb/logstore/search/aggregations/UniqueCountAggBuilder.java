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
    if (o == null || getClass() != o.getClass()) return false;

    UniqueCountAggBuilder that = (UniqueCountAggBuilder) o;

    // AggBuilderBase
    if (!Objects.equals(super.name, that.name)) return false;
    if (!Objects.equals(super.metadata, that.metadata)) return false;
    if (!Objects.equals(super.subAggregations, that.subAggregations)) return false;

    // ValueSourceAggBuilder
    if (!Objects.equals(super.field, that.field)) return false;
    if (!Objects.equals(super.missing, that.missing)) return false;

    // UniqueCountAggBuilder
    return Objects.equals(precisionThreshold, that.precisionThreshold);
  }

  @Override
  public int hashCode() {
    // UniqueCountAggBuilder
    int result = precisionThreshold != null ? precisionThreshold.hashCode() : 0;

    // ValueSourceAggBuilder
    result = 31 * result + (field != null ? field.hashCode() : 0);
    result = 31 * result + (missing != null ? missing.hashCode() : 0);

    // AggBuilderBase
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
    result = 31 * result + (subAggregations != null ? subAggregations.hashCode() : 0);

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
