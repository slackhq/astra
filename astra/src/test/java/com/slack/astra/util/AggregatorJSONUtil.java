package com.slack.astra.util;

public class AggregatorJSONUtil {
  public static String createGenericDateHistogramJSONBlob(
      String name, String field, String interval, int minDocCount) {
    return """
        {"%s":{"date_histogram":{"interval":"%s","field":"%s","min_doc_count":"%d","extended_bounds":{"min":1676498801027,"max":1676500240688},"format":"epoch_millis","offset":"5s"},"aggs":{}}}}
    """
        .formatted(name, interval, field, minDocCount);
  }
}
