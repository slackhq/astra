package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;

public class KaldbLocalQueryService<T> extends KaldbQueryServiceBase {

  private final ChunkManager<T> chunkManager;

  public KaldbLocalQueryService(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request) {
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    SearchResult<T> searchResult;
    searchResult = chunkManager.query(query);
    KaldbSearch.SearchResult result = SearchResultUtils.toSearchResultProto(searchResult);
    span.finish();
    return result;
  }
}
