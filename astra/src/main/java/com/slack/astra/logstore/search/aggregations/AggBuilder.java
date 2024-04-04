package com.slack.astra.logstore.search.aggregations;

/** Interface for all aggregation requests, as provided from the original search request. */
public interface AggBuilder {
  /**
   * The aggregation type. This is expected to be a static final string on the aggregation that
   * indicates what type it is (ie, "avg", "date_histogram"), and allows for safe type casting. This
   * should directly map to the requested aggregation string in most cases.
   */
  String getType();
}
