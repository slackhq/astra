package com.slack.kaldb.logstore.search.aggregations;

import java.util.Objects;

public class CumulativeSumAggBuilder extends PipelineAggBuilder {
  public static final String TYPE = "cumulative_sum";

  private final String format;

  public CumulativeSumAggBuilder(String name, String bucketsPath, String format) {
    super(name, bucketsPath);
    this.format = format;
  }

  public String getFormat() {
    return format;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CumulativeSumAggBuilder)) return false;
    if (!super.equals(o)) return false;

    CumulativeSumAggBuilder that = (CumulativeSumAggBuilder) o;

    return Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (format != null ? format.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "CumulativeSumAggBuilder{"
        + "format='"
        + format
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
