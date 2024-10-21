package com.slack.astra.logstore.search.aggregations;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provides common functionality for all aggregations. All aggregations are expected to extend this
 * class, or another base class that extends this one (ie, ValueSourceAggBuilder).
 */
public abstract class AggBuilderBase implements AggBuilder {
  // This is the name of the aggregation, as provided by the user. In most cases this will be a
  // unique string ID to correlate the output values - Grafana generally uses incrementing
  // integers as a string (ie "1", "2").
  protected final String name;
  protected final Map<String, Object> metadata;
  protected final List<AggBuilder> subAggregations;

  public AggBuilderBase(String name) {
    this.name = name;
    this.metadata = Map.of();
    this.subAggregations = List.of();
  }

  public AggBuilderBase(
      String name, Map<String, Object> metadata, List<AggBuilder> subAggregations) {
    this.name = name;
    this.metadata = metadata;
    this.subAggregations = subAggregations;
  }

  public List<AggBuilder> getSubAggregations() {
    return subAggregations;
  }

  public String getName() {
    return name;
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof AggBuilderBase)) return false;

    AggBuilderBase that = (AggBuilderBase) o;

    if (!name.equals(that.name)) return false;
    if (!Objects.equals(metadata, that.metadata)) return false;
    return Objects.equals(subAggregations, that.subAggregations);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
    result = 31 * result + (subAggregations != null ? subAggregations.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AggBuilderBase{"
        + "name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + '}';
  }
}
