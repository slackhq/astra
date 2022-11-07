package com.slack.kaldb.elasticsearchApi;

import brave.ScopedSpan;
import brave.Tracing;
import com.google.common.collect.ImmutableMap;
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
import java.util.stream.Collectors;
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
  public HttpResponse multiSearch(String postBody) throws IOException {
    LOG.debug("Search request: {}", postBody);

    List<EsSearchRequest> requests = EsSearchRequest.parse(postBody);
    List<EsSearchResponse> responses =
        requests.parallelStream().map(this::doSearch).collect(Collectors.toList());

    SearchResponseMetadata responseMetadata = new SearchResponseMetadata(0, responses);
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
    span.tag("resultBucketCount", String.valueOf(searchResult.getBucketsCount()));
    span.tag("resultTookMicros", String.valueOf(searchResult.getTookMicros()));
    span.tag("resultFailedNodes", String.valueOf(searchResult.getFailedNodes()));
    span.tag("resultTotalNodes", String.valueOf(searchResult.getTotalNodes()));
    span.tag("resultTotalSnapshots", String.valueOf(searchResult.getTotalNodes()));
    span.tag(
        "resultSnapshotsWithReplicas", String.valueOf(searchResult.getSnapshotsWithReplicas()));

    HitsMetadata hits = getHits(searchResult);
    Map<String, AggregationResponse> aggregations =
        getAggregations(request.getAggregations(), searchResult);

    try {
      return new EsSearchResponse.Builder()
          .hits(hits)
          .aggregations(aggregations)
          .debugMetadata(
              Map.of("traceId", Tracing.current().currentTraceContext().get().traceIdString()))
          .took(Duration.of(searchResult.getTookMicros(), ChronoUnit.MICROS).toMillis())
          .shardsMetadata(searchResult.getTotalNodes(), searchResult.getFailedNodes())
          .status(200)
          .build();
    } finally {
      span.finish();
    }
  }

  private HitsMetadata getHits(KaldbSearch.SearchResult searchResult) {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>(hitsByteList.size());
    for (ByteString bytes : hitsByteList) {
      try {
        responseHits.add(SearchResponseHit.fromByteString(bytes));
      } catch (IOException e) {
        LOG.error("Error processing search hits bytes", e);
      }
    }

    return new HitsMetadata.Builder()
        .hitsTotal(ImmutableMap.of("value", responseHits.size(), "relation", "eq"))
        .hits(responseHits)
        .build();
  }

  private Map<String, AggregationResponse> getAggregations(
      List<SearchRequestAggregation> aggregations, KaldbSearch.SearchResult searchResult) {
    // todo - we currently are only supporting a single aggregation of type `date_histogram` and
    //  assume it is the always the first aggregation requested
    //  this will need to be refactored when we support more aggregation types
    Optional<SearchRequestAggregation> aggregationRequest = aggregations.stream().findFirst();

    Map<String, AggregationResponse> aggregationResponseMap = new HashMap<>();
    if (aggregationRequest.isPresent()) {
      List<AggregationBucketResponse> buckets =
          new ArrayList<>(searchResult.getBucketsList().size());
      searchResult
          .getBucketsList()
          .forEach(
              histogramBucket -> {
                // our response from kaldb has the start and end of the bucket, but we only need the
                // midpoint for the response object
                double getKey =
                    histogramBucket.getLow()
                        + ((histogramBucket.getHigh() - histogramBucket.getLow()) / 2);
                buckets.add(new AggregationBucketResponse(getKey, histogramBucket.getCount()));
              });
      aggregationResponseMap.put(
          aggregationRequest.get().getAggregationKey(), new AggregationResponse(buckets));
    }

    return aggregationResponseMap;
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
