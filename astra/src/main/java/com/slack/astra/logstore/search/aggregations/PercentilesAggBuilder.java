package com.slack.astra.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public class PercentilesAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "percentiles";
  private final List<Double> percentiles;

  public PercentilesAggBuilder(
      String name, String field, Object missing, List<Double> percentiles, String script) {
    super(name, Map.of(), List.of(), field, missing, script);

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
  public String toString() {
    return "PercentilesAggBuilder{"
        + "percentiles="
        + percentiles
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof PercentilesAggBuilder)) return false;
    if (!super.equals(o)) return false;

    PercentilesAggBuilder that = (PercentilesAggBuilder) o;

    return percentiles.equals(that.percentiles);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + percentiles.hashCode();
    return result;
  }
}
