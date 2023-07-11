package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.kaldb.chunkManager.ChunkManager;
import com.slack.kaldb.metadata.schema.FieldType;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import java.time.Duration;
import java.util.Map;
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
    LOG.debug("Received search request: {}", request);
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    span.tag("query", query.toString());
    // TODO: In the future we will also accept query timeouts from the search request. If provided
    // we'll use that over defaultQueryTimeout
    SearchResult<T> searchResult = chunkManager.query(query, defaultQueryTimeout);
    KaldbSearch.SearchResult result = SearchResultUtils.toSearchResultProto(searchResult);
    span.tag("totalNodes", String.valueOf(result.getTotalNodes()));
    span.tag("failedNodes", String.valueOf(result.getFailedNodes()));
    span.tag("hitCount", String.valueOf(result.getHitsCount()));
    span.finish();
    LOG.debug("Finished search request: {}", request);
    return result;
  }

  @Override
  public KaldbSearch.SchemaResult getSchema(KaldbSearch.SchemaRequest request) {
    LOG.debug("Received schema request: {}", request);
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("KaldbLocalQueryService.getSchema");
    Map<String, FieldType> schema = chunkManager.getSchema();
    KaldbSearch.SchemaResult schemaResult = SearchResultUtils.toSchemaResultProto(schema);
    span.tag("fieldDefinitionCount", String.valueOf(schemaResult.getFieldDefinitionCount()));
    span.finish();
    LOG.debug("Finished schema request: {}", request);
    return schemaResult;
  }
}
