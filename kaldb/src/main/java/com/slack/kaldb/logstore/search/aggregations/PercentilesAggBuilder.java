package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Objects;

public class PercentilesAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "percentiles";
  private final List<Double> percentiles;

  public PercentilesAggBuilder(
      String name, String field, Object missing, List<Double> percentiles) {
    super(name, field, missing);

    this.percentiles = percentiles;
  }

  public List<Double> getPercentiles() {
    return percentiles;
  }

  public double[] getPercentilesArray() {
    return percentiles.stream().mapToDouble(Double::doubleValue).toArray();
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PercentilesAggBuilder that = (PercentilesAggBuilder) o;

    // AggBuilderBase
    if (!Objects.equals(super.name, that.name)) return false;
    if (!Objects.equals(super.metadata, that.metadata)) return false;
    if (!Objects.equals(super.subAggregations, that.subAggregations)) return false;

    // ValueSourceAggBuilder
    if (!Objects.equals(super.field, that.field)) return false;
    if (!Objects.equals(super.missing, that.missing)) return false;

    // PercentilesAggBuilder
    return Objects.equals(percentiles, that.percentiles);
  }

  @Override
  public int hashCode() {
    // PercentilesAggBuilder
    int result = percentiles != null ? percentiles.hashCode() : 0;

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
    return "PercentilesAggBuilder{"
        + "percentiles="
        + percentiles
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
