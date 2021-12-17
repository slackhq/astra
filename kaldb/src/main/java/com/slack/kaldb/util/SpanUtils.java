package com.slack.kaldb.util;

import brave.ScopedSpan;
import com.slack.kaldb.logstore.search.SearchResult;

public class SpanUtils {

  public static void addSearchResultMeta(ScopedSpan span, SearchResult searchResult) {
    span.tag("totalCount", String.valueOf(searchResult.totalCount));
    span.tag("tookMicros", String.valueOf(searchResult.tookMicros));
    span.tag("failedNodes", String.valueOf(searchResult.failedNodes));
    span.tag("totalNodes", String.valueOf(searchResult.totalNodes));
    span.tag("totalSnapshots", String.valueOf(searchResult.totalSnapshots));
    span.tag("snapshotsWithReplicas", String.valueOf(searchResult.snapshotsWithReplicas));
    span.tag("hits", String.valueOf(searchResult.hits.size()));
    span.tag("buckets", String.valueOf(searchResult.buckets.size()));
  }
}
