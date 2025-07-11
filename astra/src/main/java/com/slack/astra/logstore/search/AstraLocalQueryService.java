package com.slack.astra.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.slack.astra.chunkManager.ChunkManager;
import com.slack.astra.metadata.schema.FieldType;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraLocalQueryService<T> extends AstraQueryServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(AstraLocalQueryService.class);

  private final ChunkManager<T> chunkManager;
  private final Duration defaultQueryTimeout;

  public AstraLocalQueryService(ChunkManager<T> chunkManager, Duration defaultQueryTimeout) {
    this.chunkManager = chunkManager;
    this.defaultQueryTimeout = defaultQueryTimeout;
  }

  @Override
  public AstraSearch.SearchResult doSearch(AstraSearch.SearchRequest request) {
    LOG.debug("Received search request: {}", request);

    ScopedSpan span = Tracing.currentTracer().startScopedSpan("AstraLocalQueryService.doSearch");
    SearchQuery query = SearchResultUtils.fromSearchRequest(request);
    // TODO: In the future we will also accept query timeouts from the search request. If provided
    // we'll use that over defaultQueryTimeout
    SearchResult<T> searchResult = chunkManager.query(query, defaultQueryTimeout);
    AstraSearch.SearchResult result = SearchResultUtils.toSearchResultProto(searchResult);
    span.tag("totalNodes", String.valueOf(result.getTotalNodes()));
    span.tag("failedNodes", String.valueOf(result.getFailedNodes()));
    span.tag("hitCount", String.valueOf(result.getHitsCount()));
    span.finish();
    LOG.debug("Finished search request: {}", request);
    return result;
  }

  @Override
  public AstraSearch.SchemaResult getSchema(AstraSearch.SchemaRequest request) {
    LOG.debug("Received schema request: {}", request);
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("AstraLocalQueryService.getSchema");
    Map<String, FieldType> schema = chunkManager.getSchema();
    AstraSearch.SchemaResult schemaResult = SearchResultUtils.toSchemaResultProto(schema);
    span.tag("fieldDefinitionCount", String.valueOf(schemaResult.getFieldDefinitionCount()));
    span.finish();
    LOG.debug("Finished schema request: {}", request);
    return schemaResult;
  }
}
