package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

/**
 * Provides common functionality for pipeline aggregations. This would include things that reference
 * other buckets, like moving avg, or derivatives.
 */
public abstract class PipelineAggBuilder extends AggBuilderBase {
  protected final String bucketsPath;

  public PipelineAggBuilder(String name, String bucketsPath) {
    super(name);
    this.bucketsPath = bucketsPath;
  }

  public PipelineAggBuilder(
      String name,
      Map<String, Object> metadata,
      List<AggBuilder> subAggregations,
      String bucketsPath) {
    super(name, metadata, subAggregations);
    this.bucketsPath = bucketsPath;
  }

  public String getBucketsPath() {
    return bucketsPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    PipelineAggBuilder that = (PipelineAggBuilder) o;

    return bucketsPath.equals(that.bucketsPath);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (bucketsPath.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return "PipelineAggBuilder{"
        + "bucketsPath='"
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
