package com.slack.kaldb.logstore.search;

import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbLocalQueryService<T> extends KaldbQueryServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbLocalQueryService.class);

  private final ChunkManager<T> chunkManager;

  public KaldbLocalQueryService(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  @Override
  public CompletableFuture<KaldbSearch.SearchResult> doSearch(KaldbSearch.SearchRequest request) {
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    CompletableFuture<SearchResult<T>> searchResult = chunkManager.query(query);
    return SearchResultUtils.toSearchResultProto(searchResult);
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {
    LOG.warn("Beginning local query");
    super.search(request, responseObserver);
  }
}
