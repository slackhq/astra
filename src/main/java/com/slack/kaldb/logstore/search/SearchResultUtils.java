package com.slack.kaldb.logstore.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.histogram.HistogramBucket;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SearchResultUtils {

  public static SearchQuery fromSearchRequest(KaldbSearch.SearchRequest searchRequest) {
    return new SearchQuery(
        searchRequest.getIndexName(),
        searchRequest.getQueryString(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        searchRequest.getBucketCount());
  }

  public static SearchResult<LogMessage> fromSearchResultProtoOrEmpty(
      KaldbSearch.SearchResult protoSearchResult) {
    try {
      return fromSearchResultProto(protoSearchResult);
    } catch (IOException e) {
      return SearchResult.empty();
    }
  }

  public static SearchResult<LogMessage> fromSearchResultProto(
      KaldbSearch.SearchResult protoSearchResult) throws IOException {
    List<LogMessage> hits = new ArrayList<>(protoSearchResult.getHitsCount());

    for (ByteString bytes : protoSearchResult.getHitsList().asByteStringList()) {
      LogWireMessage hit = JsonUtil.read(bytes.toStringUtf8(), LogWireMessage.class);
      LogMessage message = LogMessage.fromWireMessage(hit);
      hits.add(message);
    }
    List<HistogramBucket> histogramBuckets = new ArrayList<>();
    for (KaldbSearch.HistogramBucket protoBucket : protoSearchResult.getBucketsList()) {
      histogramBuckets.add(new HistogramBucket(protoBucket.getLow(), protoBucket.getHigh()));
    }

    return new SearchResult<>(
        hits,
        protoSearchResult.getTookMicros(),
        protoSearchResult.getTotalCount(),
        histogramBuckets,
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getTotalSnapshots(),
        protoSearchResult.getSnapshotsWithReplicas());
  }

  public static <T> CompletableFuture<KaldbSearch.SearchResult> toSearchResultProto(
      CompletableFuture<SearchResult<T>> searchResult) {
    return searchResult.thenApply(SearchResultUtils::toSearchResultProto);
  }

  public static <T> KaldbSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    KaldbSearch.SearchResult.Builder searchResultBuilder = KaldbSearch.SearchResult.newBuilder();
    searchResultBuilder.setTotalCount(searchResult.totalCount);
    searchResultBuilder.setTookMicros(searchResult.tookMicros);
    searchResultBuilder.setFailedNodes(searchResult.failedNodes);
    searchResultBuilder.setTotalNodes(searchResult.totalNodes);
    searchResultBuilder.setTotalSnapshots(searchResult.totalSnapshots);
    searchResultBuilder.setSnapshotsWithReplicas(searchResult.snapshotsWithReplicas);

    // Set hits
    ArrayList<String> protoHits = new ArrayList<>(searchResult.hits.size());
    for (T hit : searchResult.hits) {
      try {
        protoHits.add(JsonUtil.writeAsString(hit));
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
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
}
