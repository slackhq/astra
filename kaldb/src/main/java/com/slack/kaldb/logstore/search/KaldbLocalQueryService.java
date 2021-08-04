package com.slack.kaldb.logstore.search;

import com.slack.kaldb.chunk.ChunkManager;
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
    System.out.println("Request received q=" + request.getQueryString());
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    CompletableFuture<SearchResult<T>> searchResult = chunkManager.query(query);
    return SearchResultUtils.toSearchResultProto(searchResult);
  }
}
