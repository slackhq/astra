package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbLocalQueryService<T> extends KaldbQueryServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbLocalQueryService.class);

  private final ChunkManager<T> chunkManager;
  private final Duration defaultQueryTimeout;

  public KaldbLocalQueryService(ChunkManager<T> chunkManager, Duration defaultQueryTimeout) {
    this.chunkManager = chunkManager;
    this.defaultQueryTimeout = defaultQueryTimeout;
  }

  @Override
  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request) {
    LOG.info("Received search request: {}", request);
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    span.tag("query", query.toString());
    // TODO: In the future we will also accept query timeouts from the search request. If provided
    // we'll use that over defaultQueryTimeout
    SearchResult<T> searchResult = chunkManager.query(query, defaultQueryTimeout);
    KaldbSearch.SearchResult result = SearchResultUtils.toSearchResultProto(searchResult);
    span.tag("totalSnapshots", String.valueOf(result.getTotalSnapshots()));
    span.tag("failedSnapshots", String.valueOf(result.getFailedSnapshots()));
    span.tag("successfulSnapshots", String.valueOf(result.getSuccessfulSnapshots()));
    span.tag("hitCount", String.valueOf(result.getHitsCount()));
    span.finish();
    LOG.info("Finished search request: {}", request);
    return result;
  }
}
