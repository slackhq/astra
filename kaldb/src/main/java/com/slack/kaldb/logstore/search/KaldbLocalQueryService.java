package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.SpanUtils;
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
    searchResult.whenComplete(
        (res, ex) -> {
          SpanUtils.addSearchResultMeta(span, res);
        });
    CompletableFuture<KaldbSearch.SearchResult> result =
        SearchResultUtils.toSearchResultProto(searchResult);
    result.whenComplete(
        (res, ex) -> {
          if (ex != null) {
            span.error(ex);
          }
          span.finish();
        });
    return result;
  }
}
