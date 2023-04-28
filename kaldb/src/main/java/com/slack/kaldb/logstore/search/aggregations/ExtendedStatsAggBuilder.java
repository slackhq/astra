package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ExtendedStatsAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "extended_stats";

  private final Double sigma;

  public ExtendedStatsAggBuilder(
      String name, String field, Object missing, String script, Double sigma) {
    super(name, Map.of(), List.of(), field, missing, script);

    this.sigma = sigma;
  }

  public Double getSigma() {
    return sigma;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ExtendedStatsAggBuilder)) return false;
    if (!super.equals(o)) return false;

    ExtendedStatsAggBuilder that = (ExtendedStatsAggBuilder) o;

    return Objects.equals(sigma, that.sigma);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (sigma != null ? sigma.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ExtendedStatsAggBuilder{"
        + "sigma="
        + sigma
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
