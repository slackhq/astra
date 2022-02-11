package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbLocalQueryService<T> extends KaldbQueryServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbLocalQueryService.class);

  private final ChunkManager<T> chunkManager;

  public KaldbLocalQueryService(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request) {
    LOG.info("Received search request: {}", request);
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    span.tag("query", query.toString());
    SearchResult<T> searchResult = chunkManager.query(query);
    KaldbSearch.SearchResult result = SearchResultUtils.toSearchResultProto(searchResult);
    span.tag("totalNodes", String.valueOf(result.getTotalNodes()));
    span.tag("failedNodes", String.valueOf(result.getFailedNodes()));
    span.finish();
    LOG.info("Finished search request: {}", request);
    return result;
  }
}
