package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import java.util.concurrent.CompletableFuture;

public class KaldbLocalQueryService<T> extends KaldbQueryServiceBase {

  private final ChunkManager<T> chunkManager;

  public KaldbLocalQueryService(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public CompletableFuture<KaldbSearch.SearchResult> doSearch(KaldbSearch.SearchRequest request) {
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    CompletableFuture<SearchResult<T>> searchResult = chunkManager.query(query);
    CompletableFuture<KaldbSearch.SearchResult> result =
        SearchResultUtils.toSearchResultProto(searchResult);
    result.thenRun(span::finish);
    return result;
  }
}
