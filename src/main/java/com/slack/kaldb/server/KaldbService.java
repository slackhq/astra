package com.slack.kaldb.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.slack.kaldb.chunk.ChunkManager;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.search.SearchQuery;
import com.slack.kaldb.logstore.search.SearchResult;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.util.JsonUtil;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbService<T> extends KaldbServiceGrpc.KaldbServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(KaldbService.class);

  private final ChunkManager<T> chunkManager;

  public KaldbService(ChunkManager<T> chunkManager) {
    this.chunkManager = chunkManager;
  }

  public static SearchQuery fromSearchRequest(KaldbSearch.SearchRequest searchRequest) {
    return new SearchQuery(
        searchRequest.getIndexName(),
        searchRequest.getQueryString(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        searchRequest.getBucketCount());
  }

  public static <T> KaldbSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult)
      throws JsonProcessingException {

    KaldbSearch.SearchResult.Builder searchResultBuilder = KaldbSearch.SearchResult.newBuilder();
    searchResultBuilder.setCount(searchResult.count);
    searchResultBuilder.setTotalCount(searchResult.totalCount);
    searchResultBuilder.setTookMicros(searchResult.tookMicros);
    searchResultBuilder.setFailedNodes(searchResult.failedNodes);
    searchResultBuilder.setTotalNodes(searchResult.totalNodes);
    searchResultBuilder.setTotalSnapshots(searchResult.totalSnapshots);

    // Set hits
    ArrayList<String> protoHits = new ArrayList<>(searchResult.hits.size());
    for (T hit : searchResult.hits) {
      protoHits.add(JsonUtil.writeAsString(hit));
    }
    searchResultBuilder.addAllHits(protoHits);

    // Set buckets
    List<KaldbSearch.HistogramBucket> protoBuckets = new ArrayList<>(searchResult.buckets.size());
    for (HistogramBucket bucket : searchResult.buckets) {
      KaldbSearch.HistogramBucket.Builder builder = KaldbSearch.HistogramBucket.newBuilder();
      protoBuckets.add(
          builder
              .setCount(bucket.getCount())
              .setHigh(bucket.getHigh())
              .setLow(bucket.getLow())
              .build());
    }
    searchResultBuilder.addAllBuckets(protoBuckets);

    return searchResultBuilder.build();
  }

  public KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request)
      throws JsonProcessingException {
    SearchQuery query = fromSearchRequest(request);
    SearchResult<T> searchResult = chunkManager.query(query);
    return toSearchResultProto(searchResult);
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    try {
      KaldbSearch.SearchResult protoSearchResult = doSearch(request);
      responseObserver.onNext(protoSearchResult);
    } catch (JsonProcessingException e) {
      LOG.error("Error formatting search result into protobuf.", e);
      // TODO: Ensure the exception is thrown correctly.
      responseObserver.onError(
          new RuntimeException("Encountered exception formatting search result"));
    }
    responseObserver.onCompleted();
  }
}
