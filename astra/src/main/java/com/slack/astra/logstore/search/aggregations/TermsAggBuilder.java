package com.slack.astra.logstore.search.aggregations;

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
    super(name, Map.of(), subAggregations, field, missing, null);

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
    if (!(o instanceof TermsAggBuilder)) return false;
    if (!super.equals(o)) return false;

    TermsAggBuilder that = (TermsAggBuilder) o;

    if (size != that.size) return false;
    if (minDocCount != that.minDocCount) return false;
    return Objects.equals(order, that.order);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + size;
    result = 31 * result + (int) (minDocCount ^ (minDocCount >>> 32));
    result = 31 * result + (order != null ? order.hashCode() : 0);
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
