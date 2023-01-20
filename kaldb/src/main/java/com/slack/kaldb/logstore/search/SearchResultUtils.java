package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SearchResultUtils {

  public static SearchQuery fromSearchRequest(KaldbSearch.SearchRequest searchRequest) {
    return new SearchQuery(
        searchRequest.getDataset(),
        searchRequest.getQueryString(),
        searchRequest.getStartTimeEpochMs(),
        searchRequest.getEndTimeEpochMs(),
        searchRequest.getHowMany(),
        fromSearchAgg(List.of(searchRequest.getAggs())).get(0),
        searchRequest.getChunkIdsList());
  }

  public static List<SearchAggregation> fromSearchAgg(
      List<KaldbSearch.SearchAggregation> searchAggregation) {
    List<SearchAggregation> searchAggregations = new ArrayList<>();
    for (KaldbSearch.SearchAggregation aggregation : searchAggregation) {
      searchAggregations.add(
          new SearchAggregation(
              aggregation.getName(),
              aggregation.getType(),
              fromSearchStruct(aggregation.getMetadata()),
              fromSearchAgg(aggregation.getSubAggregatorsList())));
    }
    return searchAggregations;
  }

  public static Map<String, Object> fromSearchStruct(KaldbSearch.Struct struct) {
    Map<String, Object> resultMap = new HashMap<>();
    for (Map.Entry<String, KaldbSearch.Value> structEntry : struct.getFieldsMap().entrySet()) {
      resultMap.put(structEntry.getKey(), fromValue(structEntry.getValue()));
    }

    return resultMap;
  }

  public static List<Object> fromSearchListValue(KaldbSearch.ListValue listValue) {
    List<Object> resultList = new ArrayList<>();
    for (KaldbSearch.Value value : listValue.getValuesList()) {
      resultList.add(fromValue(value));
    }
    return resultList;
  }

  public static Object fromValue(KaldbSearch.Value value) {
    if (value.hasIntValue()) {
      return value.getIntValue();
    } else if (value.hasDoubleValue()) {
      return value.getDoubleValue();
    } else if (value.hasStringValue()) {
      return value.getStringValue();
    } else if (value.hasBoolValue()) {
      return value.getBoolValue();
    } else if (value.hasStructValue()) {
      return fromSearchStruct(value.getStructValue());
    } else if (value.hasListValue()) {
      return fromSearchListValue(value.getListValue());
    } else {
      throw new IllegalArgumentException();
    }
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

    List<ResponseAggregation> aggregations =
        fromProtoResponseAggregations(protoSearchResult.getAggregationsList());
    return new SearchResult<>(
        hits,
        protoSearchResult.getTookMicros(),
        protoSearchResult.getTotalCount(),
        aggregations,
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getTotalSnapshots(),
        protoSearchResult.getSnapshotsWithReplicas());
  }

  public static Object fromProtoResponseBucketResult(
      KaldbSearch.ResponseBucketResult responseBucketResult) {
    if (responseBucketResult.hasAggregation()) {
      return fromProtoResponseAggregations(List.of(responseBucketResult.getAggregation()));
    } else if (responseBucketResult.hasValue()) {
      return responseBucketResult.getValue().getValue();
    } else {
      throw new IllegalArgumentException();
    }
  }

  public static Map<String, Object> fromProtoResponseBucketResultMap(
      Map<String, KaldbSearch.ResponseBucketResult> protoResponseBucketResults) {
    Map<String, Object> returnMap = new HashMap<>();

    for (Map.Entry<String, KaldbSearch.ResponseBucketResult> stringResponseBucketResultEntry :
        protoResponseBucketResults.entrySet()) {
      returnMap.put(
          stringResponseBucketResultEntry.getKey(),
          fromProtoResponseBucketResult(stringResponseBucketResultEntry.getValue()));
    }

    return returnMap;
  }

  public static List<ResponseBucket> fromProtoResponseBuckets(
      List<KaldbSearch.ResponseBuckets> protoResponseBuckets) {
    List<ResponseBucket> responseBuckets = new ArrayList<>();

    for (KaldbSearch.ResponseBuckets protoResponseBucket : protoResponseBuckets) {
      responseBuckets.add(
          new ResponseBucket(
              List.of(protoResponseBucket.getKey(0)), // todo
              protoResponseBucket.getDocCount(),
              fromProtoResponseBucketResultMap(protoResponseBucket.getValuesMap())));
    }

    return responseBuckets;
  }

  public static List<ResponseAggregation> fromProtoResponseAggregations(
      List<KaldbSearch.ResponseAggregation> protoResponseAggregations) {
    List<ResponseAggregation> returnList = new ArrayList<>();

    protoResponseAggregations.forEach(
        protoResponseAggregation -> {
          returnList.add(
              new ResponseAggregation(
                  protoResponseAggregation.getName(),
                  protoResponseAggregation.getDocCountErrorUpperBound(),
                  protoResponseAggregation.getSumOtherDocCount(),
                  fromProtoResponseBuckets(protoResponseAggregation.getBucketsList())));
        });

    return returnList;
  }

  public static Map<String, KaldbSearch.ResponseBucketResult> toResponseBucketResultMap(
      Map<String, Object> responseBucketValues) {
    Map<String, KaldbSearch.ResponseBucketResult> resultMap = new HashMap<>();
    responseBucketValues
        .entrySet()
        .stream()
        .forEach(
            responseBucketValue -> {
              KaldbSearch.ResponseBucketResult.Builder responseBucketResult =
                  KaldbSearch.ResponseBucketResult.newBuilder();
              // todo
              //      if (responseBucketValue.getValue() instanceof ResponseBucket) {
              //
              //      }
              resultMap.put(responseBucketValue.getKey(), responseBucketResult.build());
              throw new IllegalArgumentException("Not implemented yet");
            });

    return resultMap;
  }

  public static List<KaldbSearch.ResponseBuckets> toResponseBucketsProto(
      List<ResponseBucket> responseBuckets) {
    List<KaldbSearch.ResponseBuckets> responseBucketsList = new ArrayList<>();

    for (ResponseBucket responseBucket : responseBuckets) {
      responseBucketsList.add(
          KaldbSearch.ResponseBuckets.newBuilder()
              .addAllKey(
                  responseBucket
                      .getKey()
                      .stream()
                      .map(keyObj -> String.valueOf(keyObj))
                      .collect(Collectors.toList()))
              .setDocCount(responseBucket.getDocCount())
              .putAllValues(toResponseBucketResultMap(responseBucket.getValues()))
              .build());
    }
    return responseBucketsList;
  }

  public static List<KaldbSearch.ResponseAggregation> toResponseAggregationProto(
      List<ResponseAggregation> responseAggregations) {

    List<KaldbSearch.ResponseAggregation> responseList = new ArrayList<>();

    for (ResponseAggregation responseAggregation : responseAggregations) {
      responseList.add(
          KaldbSearch.ResponseAggregation.newBuilder()
              .setName(responseAggregation.getName())
              .setDocCountErrorUpperBound(responseAggregation.getDocCountErrorUpperBound())
              .setSumOtherDocCount(responseAggregation.getSumOtherDocCount())
              .addAllBuckets(
                  (responseAggregation.getResponseBuckets() != null
                      ? toResponseBucketsProto(responseAggregation.getResponseBuckets())
                      : List.of()))
              .build());
    }

    return responseList;
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

    // Set aggs
    searchResultBuilder.addAllAggregations(toResponseAggregationProto(searchResult.aggregations));

    span.finish();
    return searchResultBuilder.build();
  }
}
