package com.slack.kaldb.logstore.search.aggregations;

import java.util.List;
import java.util.Map;

public class FiltersAggBuilder extends AggBuilderBase {
  public static final String TYPE = "filters";

  private final Map<String, FilterAgg> filterAggMap;

  public FiltersAggBuilder(
      String name, List<AggBuilder> subAggregations, Map<String, FilterAgg> filterAggMap) {
    super(name, Map.of(), subAggregations);
    this.filterAggMap = filterAggMap;
  }

  public Map<String, FilterAgg> getFilterAggMap() {
    return filterAggMap;
  }

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FiltersAggBuilder)) return false;
    if (!super.equals(o)) return false;

    FiltersAggBuilder that = (FiltersAggBuilder) o;

    return filterAggMap.equals(that.filterAggMap);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + filterAggMap.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "FiltersAggBuilder{"
        + "filterAggMap="
        + filterAggMap
        + ", name='"
        + name
        + '\''
        + ", metadata="
        + metadata
        + ", subAggregations="
        + subAggregations
        + '}';
  }

  public static class FilterAgg {

    private final String queryString;

    private final boolean analyzeWildcard;

    public FilterAgg(String queryString, boolean analyzeWildcard) {
      this.queryString = queryString;
      this.analyzeWildcard = analyzeWildcard;
    }

    public String getQueryString() {
      return queryString;
    }

    public boolean isAnalyzeWildcard() {
      return analyzeWildcard;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof FilterAgg)) return false;

      FilterAgg filterAgg = (FilterAgg) o;

      if (analyzeWildcard != filterAgg.analyzeWildcard) return false;
      return queryString.equals(filterAgg.queryString);
    }

    @Override
    public int hashCode() {
      int result = queryString.hashCode();
      result = 31 * result + (analyzeWildcard ? 1 : 0);
      return result;
    }

    @Override
    public String toString() {
      return "FilterAgg{"
          + "queryString='"
          + queryString
          + '\''
          + ", analyzeWildcard="
          + analyzeWildcard
          + '}';
    }
  }
}
