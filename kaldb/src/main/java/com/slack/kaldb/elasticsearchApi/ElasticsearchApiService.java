package com.slack.kaldb.elasticsearchApi;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.TraceContext;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.elasticsearchApi.searchRequest.EsSearchRequest;
import com.slack.kaldb.elasticsearchApi.searchRequest.aggregations.SearchRequestAggregation;
import com.slack.kaldb.elasticsearchApi.searchResponse.AggregationBucketResponse;
import com.slack.kaldb.elasticsearchApi.searchResponse.AggregationResponse;
import com.slack.kaldb.elasticsearchApi.searchResponse.EsSearchResponse;
import com.slack.kaldb.elasticsearchApi.searchResponse.HitsMetadata;
import com.slack.kaldb.elasticsearchApi.searchResponse.SearchResponseHit;
import com.slack.kaldb.elasticsearchApi.searchResponse.SearchResponseMetadata;
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.opensearch.search.aggregations.bucket.histogram.InternalAutoDateHistogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Elasticsearch compatible API service, for use in Grafana
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/elasticsearch/datasource.ts">Grafana
 *     ES API</a>
 */
@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ElasticsearchApiService {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchApiService.class);
  private final KaldbQueryServiceBase searcher;

  // This uses a separate cached threadpool for multisearch queries so that we can run these in
  // parallel. A cached threadpool was chosen over something like forkjoin, as it's easier to
  // propagate the trace instrumentation, and has better visibility using a custom threadfactory.
  private final ExecutorService multisearchExecutor =
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder()
              .setUncaughtExceptionHandler(
                  (t, e) -> LOG.error("Exception on thread {}: {}", t.getName(), e))
              .setNameFormat("elasticsearch-multisearch-api-%d")
              .build());

  public ElasticsearchApiService(KaldbQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  /**
   * Multisearch API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html">API
   *     doc</a>
   */
  @Post
  @Blocking
  @Path("/_msearch")
  public HttpResponse multiSearch(String postBody) throws Exception {
    LOG.debug("Search request: {}", postBody);

    List<EsSearchRequest> requests = EsSearchRequest.parse(postBody);

    List<ListenableFuture<EsSearchResponse>> responseFutures =
        requests
            .stream()
            .map(
                (request) ->
                    Futures.submit(
                        () -> this.doSearch(request),
                        Tracing.current().currentTraceContext().executor(multisearchExecutor)))
            .collect(Collectors.toList());

    SearchResponseMetadata responseMetadata =
        new SearchResponseMetadata(
            0, Futures.allAsList(responseFutures).get(), Map.of("traceId", getTraceId()));
    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(responseMetadata));
  }

  private EsSearchResponse doSearch(EsSearchRequest request) {
    ScopedSpan span = Tracing.currentTracer().startScopedSpan("ElasticsearchApiService.doSearch");
    KaldbSearch.SearchRequest searchRequest = request.toKaldbSearchRequest();
    KaldbSearch.SearchResult searchResult = searcher.doSearch(searchRequest);

    span.tag("requestDataset", searchRequest.getDataset());
    span.tag("requestQueryString", searchRequest.getQueryString());
    span.tag("requestQueryStartTimeEpochMs", String.valueOf(searchRequest.getStartTimeEpochMs()));
    span.tag("requestQueryEndTimeEpochMs", String.valueOf(searchRequest.getEndTimeEpochMs()));
    span.tag("requestHowMany", String.valueOf(searchRequest.getHowMany()));
    span.tag("resultTotalCount", String.valueOf(searchResult.getTotalCount()));
    span.tag("resultHitsCount", String.valueOf(searchResult.getHitsCount()));
    span.tag("resultTookMicros", String.valueOf(searchResult.getTookMicros()));
    span.tag("resultFailedNodes", String.valueOf(searchResult.getFailedNodes()));
    span.tag("resultTotalNodes", String.valueOf(searchResult.getTotalNodes()));
    span.tag("resultTotalSnapshots", String.valueOf(searchResult.getTotalNodes()));
    span.tag(
        "resultSnapshotsWithReplicas", String.valueOf(searchResult.getSnapshotsWithReplicas()));

    try {
      HitsMetadata hits = getHits(searchResult);
      Map<String, AggregationResponse> aggregations =
          buildLegacyAggregationResponse(
              request.getAggregations(), searchResult.getInternalAggregations());

      return new EsSearchResponse.Builder()
          .hits(hits)
          .aggregations(aggregations)
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .debugMetadata(getDebugAggregations(searchResult.getInternalAggregations()))
          .status(200)
          .build();
    } catch (Exception e) {
      LOG.error("Error fulfilling request for multisearch query", e);
      span.error(e);
      return new EsSearchResponse.Builder()
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .status(500)
          .build();
    } finally {
      span.finish();
    }
  }

  /**
   * Temporary method to serialize the internal aggregations into the debug metadata for eaiser
   * troubleshooting
   */
  @Deprecated
  private Map<String, String> getDebugAggregations(ByteString internalAggregations)
      throws IOException {
    Map<String, String> debugAggs = new HashMap<>();
    if (internalAggregations.size() > 0) {
      debugAggs.put(
          "internalAggs",
          OpenSearchAggregationAdapter.fromByteArray(internalAggregations.toByteArray())
              .toString());
    }
    return debugAggs;
  }

  /**
   * Converts an InternalAggregation response type into one that maps into the EsSearchResponse.
   * This is a temporary method and will be removed after implementing another aggregation type, as
   * the existing AggregationResponse will only work with DateHistogram count responses.
   */
  @Deprecated
  private Map<String, AggregationResponse> buildLegacyAggregationResponse(
      List<SearchRequestAggregation> searchRequestAggregations, ByteString internalAggregations) {
    InternalAutoDateHistogram internalAggregation;
    Map<String, AggregationResponse> aggregations = new HashMap<>();
    if (internalAggregations.size() > 0) {
      try {
        internalAggregation =
            OpenSearchAggregationAdapter.fromByteArray(internalAggregations.toByteArray());
        List<AggregationBucketResponse> aggregationBucketResponses = new ArrayList<>();
        internalAggregation
            .getBuckets()
            .forEach(
                bucket ->
                    aggregationBucketResponses.add(
                        new AggregationBucketResponse(
                            Double.parseDouble(bucket.getKeyAsString()), bucket.getDocCount())));
        aggregations.put(
            searchRequestAggregations.stream().findFirst().get().getAggregationKey(),
            new AggregationResponse(aggregationBucketResponses));
      } catch (Exception e) {
        LOG.error("Error converting internal aggregations to Elasticsearch response", e);
      }
    }

    return aggregations;
  }

  private String getTraceId() {
    TraceContext traceContext = Tracing.current().currentTraceContext().get();
    if (traceContext != null) {
      return traceContext.traceIdString();
    }
    return "";
  }

  private HitsMetadata getHits(KaldbSearch.SearchResult searchResult) throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>(hitsByteList.size());
    for (ByteString bytes : hitsByteList) {
      responseHits.add(SearchResponseHit.fromByteString(bytes));
    }

    return new HitsMetadata.Builder()
        .hitsTotal(ImmutableMap.of("value", responseHits.size(), "relation", "eq"))
        .hits(responseHits)
        .build();
  }

  /**
   * Mapping API
   *
   * @see <a
   *     href="https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html">API
   *     doc</a>
   */
  @Get
  @Path("/:indexName/_mapping")
  public HttpResponse mapping(@Param("indexName") Optional<String> indexName) throws IOException {
    return HttpResponse.of(
        HttpStatus.OK,
        MediaType.JSON,
        JsonUtil.writeAsString(
            ImmutableMap.of(
                indexName.orElseThrow(),
                ImmutableMap.of(
                    "mappings",
                    ImmutableMap.of(
                        "properties",
                        ImmutableMap.of("@timestamp", ImmutableMap.of("type", "date")))))));
  }
}
