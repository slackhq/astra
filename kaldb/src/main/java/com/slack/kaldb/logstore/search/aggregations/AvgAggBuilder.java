package com.slack.kaldb.logstore.search.aggregations;

import java.util.Objects;

/** Aggregation request type to calculate the average value */
public class AvgAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "avg";

  public AvgAggBuilder(String name, String field) {
    super(name, field);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    AvgAggBuilder that = (AvgAggBuilder) o;

    // AggBuilderBase
    if (!Objects.equals(super.name, that.name)) return false;
    if (!Objects.equals(super.metadata, that.metadata)) return false;
    if (!Objects.equals(super.subAggregations, that.subAggregations)) return false;

    // ValueSourceAggBuilder
    if (!Objects.equals(super.field, that.field)) return false;

    // AvgAggBuilder
    return true;
  }

  @Override
  public int hashCode() {
    // AvgAggBuilder
    int result = 0;

    // ValueSourceAggBuilder
    result = 31 * result + (field != null ? field.hashCode() : 0);

    // AggBuilderBase
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
    result = 31 * result + (subAggregations != null ? subAggregations.hashCode() : 0);

    return result;
  }

  @Override
  public String toString() {
    return "AvgAggBuilder{"
        + "field='"
        + field
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
