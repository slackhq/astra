package com.slack.kaldb.logstore.search.aggregations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TermsAggBuilder extends ValueSourceAggBuilder {
  public static final String TYPE = "terms";

  private final int size;

  private final long minDocCount;

  private Map<String, String> order;

  public TermsAggBuilder(
      String name,
      List<AggBuilder> subAggregations,
      String field,
      Object missing,
      int size,
      long minDocCount,
      Map<String, String> order) {
    // todo - metadata?
    super(name, Map.of(), subAggregations, field, missing);

    this.size = size;
    this.minDocCount = minDocCount;
    this.order = Collections.unmodifiableMap(order);
  }

  @Override
  public String getType() {
    return TYPE;
  }

  public int getSize() {
    return size;
  }

  public long getMinDocCount() {
    return minDocCount;
  }

  public Map<String, String> getOrder() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TermsAggBuilder that = (TermsAggBuilder) o;

    // AggBuilderBase
    if (!Objects.equals(super.name, that.name)) return false;
    if (!Objects.equals(super.metadata, that.metadata)) return false;
    if (!Objects.equals(super.subAggregations, that.subAggregations)) return false;

    // ValueSourceAggBuilder
    if (!Objects.equals(super.field, that.field)) return false;
    if (!Objects.equals(super.missing, that.missing)) return false;

    // TermsAggBuilder
    if (size != that.size) return false;
    if (minDocCount != that.minDocCount) return false;
    return Objects.equals(order, that.order);
  }

  @Override
  public int hashCode() {
    // TermsAggBuilder
    int result = size;
    result = 31 * result + (int) (minDocCount ^ (minDocCount >>> 32));
    result = 31 * result + (order != null ? order.hashCode() : 0);

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
    return "TermsAggBuilder{"
        + "size="
        + size
        + ", minDocCount="
        + minDocCount
        + ", order="
        + order
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
