package com.slack.kaldb.logstore.search.aggregations;

import java.util.Objects;

public class DerivativeAggBuilder extends PipelineAggBuilder {
  public static final String TYPE = "derivative";

  private final String unit;

  public DerivativeAggBuilder(String name, String bucketsPath, String unit) {
    super(name, bucketsPath);

    this.unit = unit;
  }

  public String getUnit() {
    return unit;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DerivativeAggBuilder)) return false;
    if (!super.equals(o)) return false;

    DerivativeAggBuilder that = (DerivativeAggBuilder) o;

    return Objects.equals(unit, that.unit);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (unit != null ? unit.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DerivativeAggBuilder{"
        + "unit='"
        + unit
        + '\''
        + ", bucketsPath='"
        + bucketsPath
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
