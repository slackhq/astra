package com.slack.kaldb.elasticsearchApi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.linecorp.armeria.server.annotation.Post;
import com.slack.kaldb.elasticsearchApi.searchRequest.EsSearchRequest;
import com.slack.kaldb.elasticsearchApi.searchResponse.EsSearchResponse;
import com.slack.kaldb.elasticsearchApi.searchResponse.HitsMetadata;
import com.slack.kaldb.elasticsearchApi.searchResponse.SearchResponseHit;
import com.slack.kaldb.elasticsearchApi.searchResponse.SearchResponseMetadata;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbLocalSearcher;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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

  private final KaldbLocalSearcher<LogMessage> searcher;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public ElasticsearchApiService(KaldbLocalSearcher<LogMessage> searcher) {
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
  @Path("/_msearch")
  public HttpResponse multiSearch(String postBody) throws IOException {
    List<EsSearchRequest> requests = EsSearchRequest.parse(postBody);
    List<EsSearchResponse> responses = new ArrayList<>();

    for (EsSearchRequest request : requests) {
      responses.add(doSearch(request));
    }

    SearchResponseMetadata responseMetadata = new SearchResponseMetadata(0, responses);
    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON, objectMapper.writeValueAsString(responseMetadata));
  }

  private EsSearchResponse doSearch(EsSearchRequest request) throws IOException {
    KaldbSearch.SearchRequest searchRequest = request.toKaldbSearchRequest();
    KaldbSearch.SearchResult searchResult = searcher.doSearch(searchRequest);

    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<SearchResponseHit> responseHits = new ArrayList<>();
    for (ByteString bytes : hitsByteList) {
      responseHits.add(SearchResponseHit.fromByteString(bytes));
    }

    HitsMetadata hitsMetadata =
        new HitsMetadata.Builder()
            .hitsTotal(ImmutableMap.of("value", responseHits.size(), "relation", "eq"))
            .hits(responseHits)
            .build();

    return new EsSearchResponse.Builder().hits(hitsMetadata).status(200).build();
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
        objectMapper.writeValueAsString(
            ImmutableMap.of(
                indexName.orElseThrow(),
                ImmutableMap.of(
                    "mappings",
                    ImmutableMap.of(
                        "properties",
                        ImmutableMap.of("@timestamp", ImmutableMap.of("type", "date")))))));
  }
}
