package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
<<<<<<< bburkholder/opensearch-serialize
=======
import com.slack.kaldb.logstore.OpensearchShim;
>>>>>>> Test aggs all the way out
=======
import com.slack.kaldb.logstore.opensearch.OpensearchShim;
>>>>>>> Initial cleanup
=======
import com.slack.kaldb.logstore.opensearch.OpenSearchAdapter;
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
=======
import com.slack.kaldb.logstore.opensearch.OpenSearchAggregationAdapter;
>>>>>>> Cleanup OpenSearchAggregationAdapter
=======
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
>>>>>>> Initial aggs request POC
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.NotImplementedException;

public class SearchResultUtils {

  public static AggBuilder fromSearchAggregations(
      KaldbSearch.SearchRequest.SearchAggregation searchAggregation) {
    if (searchAggregation.getType().isEmpty()) {
      return null;
    } else if (searchAggregation.getType().equals(AvgAggBuilder.TYPE)) {
      return new AvgAggBuilder(
          searchAggregation.getName(), searchAggregation.getValueSource().getField());
    } else if (searchAggregation.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return new DateHistogramAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          searchAggregation.getValueSource().getDateHistogram().getInterval());
    }

    throw new NotImplementedException();
  }

  public static KaldbSearch.SearchRequest.SearchAggregation toSearchAggregationProto(
      AggBuilder aggBuilder) {
    if (aggBuilder instanceof AvgAggBuilder) {
      AvgAggBuilder avgAggregation = (AvgAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(AvgAggBuilder.TYPE)
          .setName(avgAggregation.getName())
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(avgAggregation.getField())
                  .build())
          .build();

    } else if (aggBuilder instanceof DateHistogramAggBuilder) {
      DateHistogramAggBuilder dateHistogramAggBuilder = (DateHistogramAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(DateHistogramAggBuilder.TYPE)
          .setName(dateHistogramAggBuilder.getName())
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(dateHistogramAggBuilder.getField())
                  .setDateHistogram(
                      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                          .DateHistogramAggregation.newBuilder()
                          .setInterval(dateHistogramAggBuilder.getInterval())
                          .setMinDocCount(dateHistogramAggBuilder.getMinDocCount())
                          .setOffset(dateHistogramAggBuilder.getOffset())
                          .build())
                  .build())
          .build();
    } else {
      throw new NotImplementedException();
    }
  }

  public static SearchQuery fromSearchRequest(KaldbSearch.SearchRequest searchRequest) {
    return new SearchQuery(
        searchRequest.getDataset(),
        searchRequest.getQueryString(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        fromSearchAggregations(searchRequest.getAggregations()),
        searchRequest.getChunkIdsList());
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

    return new SearchResult<>(
        hits,
        protoSearchResult.getTookMicros(),
        protoSearchResult.getTotalCount(),
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getTotalSnapshots(),
        protoSearchResult.getSnapshotsWithReplicas(),
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
        OpenSearchAggregationAdapter.fromByteArray(
            protoSearchResult.getInternalAggregations().toByteArray()));
=======
        OpensearchShim.fromByteArray(protoSearchResult.getInternalAggregations().toByteArray()));
>>>>>>> Test aggs all the way out
=======
        OpenSearchAdapter.fromByteArray(protoSearchResult.getInternalAggregations().toByteArray()));
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
=======
        OpenSearchAggregationAdapter.fromByteArray(
            protoSearchResult.getInternalAggregations().toByteArray()));
>>>>>>> Cleanup OpenSearchAggregationAdapter
  }

  public static <T> KaldbSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultUtils.toSearchResultProto");
    span.tag("totalCount", String.valueOf(searchResult.totalCount));
    span.tag("tookMicros", String.valueOf(searchResult.tookMicros));
    span.tag("failedNodes", String.valueOf(searchResult.failedNodes));
    span.tag("totalNodes", String.valueOf(searchResult.totalNodes));
    span.tag("totalSnapshots", String.valueOf(searchResult.totalSnapshots));
    span.tag("snapshotsWithReplicas", String.valueOf(searchResult.snapshotsWithReplicas));
    span.tag("hits", String.valueOf(searchResult.hits.size()));
<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
=======
>>>>>>> Fix tests
    span.tag(
        "internalAggregation",
        searchResult.internalAggregation != null
            ? searchResult.internalAggregation.toString()
            : "");
<<<<<<< bburkholder/opensearch-serialize
=======
    span.tag("internalAggregation", searchResult.internalAggregation.toString());
>>>>>>> Initial cleanup
=======
>>>>>>> Fix tests

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

<<<<<<< bburkholder/opensearch-serialize
<<<<<<< bburkholder/opensearch-serialize
    ByteString bytes =
        ByteString.copyFrom(
            OpenSearchAggregationAdapter.toByteArray(searchResult.internalAggregation));
<<<<<<< bburkholder/opensearch-serialize
=======
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
=======
>>>>>>> Initial cleanup
    ByteString bytes =
<<<<<<< bburkholder/opensearch-serialize
        ByteString.copyFrom(OpensearchShim.toByteArray(searchResult.internalAggregation));
>>>>>>> Test aggs all the way out
=======
        ByteString.copyFrom(OpenSearchAdapter.toByteArray(searchResult.internalAggregation));
>>>>>>> More cleanup (KaldbSearchContext docs, OpenSearchAdapter rename)
=======
>>>>>>> Cleanup OpenSearchAggregationAdapter
    searchResultBuilder.setInternalAggregations(bytes);
    span.finish();
    return searchResultBuilder.build();
  }
}
