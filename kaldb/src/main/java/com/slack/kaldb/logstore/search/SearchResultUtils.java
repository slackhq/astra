package com.slack.kaldb.logstore.search;

import brave.ScopedSpan;
import brave.Tracing;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.logstore.opensearch.OpenSearchInternalAggregation;
import com.slack.kaldb.logstore.search.aggregations.AggBuilder;
import com.slack.kaldb.logstore.search.aggregations.AvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.HistogramAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MinAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.MovingAvgAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.PercentilesAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.TermsAggBuilder;
import com.slack.kaldb.logstore.search.aggregations.UniqueCountAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;

public class SearchResultUtils {
  public static Map<String, Object> fromValueStruct(KaldbSearch.Struct struct) {
    Map<String, Object> returnMap = new HashMap<>();
    struct.getFieldsMap().forEach((key, value) -> returnMap.put(key, fromValueProto(value)));
    return returnMap;
  }

  public static KaldbSearch.Struct toStructProto(Map<String, Object> map) {
    Map<String, KaldbSearch.Value> valueMap = new HashMap<>();
    map.forEach((key, value) -> valueMap.put(key, toValueProto(value)));
    return KaldbSearch.Struct.newBuilder().putAllFields(valueMap).build();
  }

  public static Object fromValueProto(KaldbSearch.Value value) {
    if (value.hasNullValue()) {
      return null;
    } else if (value.hasIntValue()) {
      return value.getIntValue();
    } else if (value.hasLongValue()) {
      return value.getLongValue();
    } else if (value.hasDoubleValue()) {
      return value.getDoubleValue();
    } else if (value.hasStringValue()) {
      return value.getStringValue();
    } else if (value.hasBoolValue()) {
      return value.getBoolValue();
    } else if (value.hasStructValue()) {
      return fromValueStruct(value.getStructValue());
    } else if (value.hasListValue()) {
      return value
          .getListValue()
          .getValuesList()
          .stream()
          .map(SearchResultUtils::fromValueProto)
          .collect(Collectors.toList());
    } else {
      return null;
    }
  }

  public static KaldbSearch.Value toValueProto(Object object) {
    KaldbSearch.Value.Builder valueBuilder = KaldbSearch.Value.newBuilder();

    if (object == null) {
      valueBuilder.setNullValue(KaldbSearch.NullValue.NULL_VALUE);
    } else if (object instanceof Integer) {
      valueBuilder.setIntValue((Integer) object);
    } else if (object instanceof Long) {
      valueBuilder.setLongValue((Long) object);
    } else if (object instanceof Double) {
      valueBuilder.setDoubleValue((Double) object);
    } else if (object instanceof String) {
      valueBuilder.setStringValue((String) object);
    } else if (object instanceof Boolean) {
      valueBuilder.setBoolValue((Boolean) object);
    } else if (object instanceof Map) {
      valueBuilder.setStructValue(toStructProto((Map<String, Object>) object));
    } else if (object instanceof List) {
      valueBuilder.setListValue(
          KaldbSearch.ListValue.newBuilder()
              .addAllValues(
                  ((List<?>) object)
                      .stream()
                      .map(SearchResultUtils::toValueProto)
                      .collect(Collectors.toList()))
              .build());
    } else {
      throw new IllegalArgumentException();
    }

    return valueBuilder.build();
  }

  public static AggBuilder fromSearchAggregations(
      KaldbSearch.SearchRequest.SearchAggregation searchAggregation) {
    if (searchAggregation.getType().isEmpty()) {
      return null;
    } else if (searchAggregation.getType().equals(AvgAggBuilder.TYPE)) {
      return new AvgAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          fromValueProto(searchAggregation.getValueSource().getMissing()));
    } else if (searchAggregation.getType().equals(MinAggBuilder.TYPE)) {
      return new MinAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          fromValueProto((searchAggregation.getValueSource().getMissing())));
    } else if (searchAggregation.getType().equals(UniqueCountAggBuilder.TYPE)) {
      return new UniqueCountAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          fromValueProto(searchAggregation.getValueSource().getMissing()),
          (Long)
              fromValueProto(
                  searchAggregation.getValueSource().getUniqueCount().getPrecisionThreshold()));
    } else if (searchAggregation.getType().equals(PercentilesAggBuilder.TYPE)) {
      return new PercentilesAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          fromValueProto(searchAggregation.getValueSource().getMissing()),
          searchAggregation.getValueSource().getPercentiles().getPercentilesList());
    } else if (searchAggregation.getType().equals(MovingAvgAggBuilder.TYPE)) {
      return new MovingAvgAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getPipeline().getBucketsPath(),
          searchAggregation.getPipeline().getMovingAverage().getModel(),
          searchAggregation.getPipeline().getMovingAverage().getWindow(),
          searchAggregation.getPipeline().getMovingAverage().getPredict(),
          searchAggregation.getPipeline().getMovingAverage().getAlpha(),
          searchAggregation.getPipeline().getMovingAverage().getBeta(),
          searchAggregation.getPipeline().getMovingAverage().getGamma(),
          searchAggregation.getPipeline().getMovingAverage().getPeriod(),
          searchAggregation.getPipeline().getMovingAverage().getPad(),
          searchAggregation.getPipeline().getMovingAverage().getMinimize());
    } else if (searchAggregation.getType().equals(TermsAggBuilder.TYPE)) {
      return new TermsAggBuilder(
          searchAggregation.getName(),
          searchAggregation
              .getSubAggregationsList()
              .stream()
              .map(SearchResultUtils::fromSearchAggregations)
              .collect(Collectors.toList()),
          searchAggregation.getValueSource().getField(),
          fromValueProto(searchAggregation.getValueSource().getMissing()),
          searchAggregation.getValueSource().getTerms().getSize(),
          searchAggregation.getValueSource().getTerms().getMinDocCount(),
          searchAggregation.getValueSource().getTerms().getOrderMap());
    } else if (searchAggregation.getType().equals(DateHistogramAggBuilder.TYPE)) {
      return new DateHistogramAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          searchAggregation.getValueSource().getDateHistogram().getInterval(),
          searchAggregation.getValueSource().getDateHistogram().getOffset(),
          searchAggregation.getValueSource().getDateHistogram().getMinDocCount(),
          searchAggregation.getValueSource().getDateHistogram().getFormat(),
          searchAggregation.getValueSource().getDateHistogram().getExtendedBoundsMap(),
          searchAggregation
              .getSubAggregationsList()
              .stream()
              .map(SearchResultUtils::fromSearchAggregations)
              .collect(Collectors.toList()));
    } else if (searchAggregation.getType().equals(HistogramAggBuilder.TYPE)) {
      return new HistogramAggBuilder(
          searchAggregation.getName(),
          searchAggregation.getValueSource().getField(),
          searchAggregation.getValueSource().getHistogram().getInterval(),
          searchAggregation.getValueSource().getHistogram().getMinDocCount(),
          searchAggregation
              .getSubAggregationsList()
              .stream()
              .map(SearchResultUtils::fromSearchAggregations)
              .collect(Collectors.toList()));
    }

    throw new NotImplementedException(
        String.format("Search aggregation %s is not yet supported", searchAggregation.getType()));
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
                  .setMissing(toValueProto(avgAggregation.getMissing()))
                  .build())
          .build();

    } else if (aggBuilder instanceof MinAggBuilder) {
      MinAggBuilder minAggBuilder = (MinAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(MinAggBuilder.TYPE)
          .setName(minAggBuilder.getName())
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(minAggBuilder.getField())
                  .setMissing(toValueProto(minAggBuilder.getMissing()))
                  .build())
          .build();

    } else if (aggBuilder instanceof UniqueCountAggBuilder) {
      UniqueCountAggBuilder uniqueCountAggBuilder = (UniqueCountAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(UniqueCountAggBuilder.TYPE)
          .setName(uniqueCountAggBuilder.getName())
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(uniqueCountAggBuilder.getField())
                  .setMissing(toValueProto(uniqueCountAggBuilder.getMissing()))
                  .setUniqueCount(
                      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                          .UniqueCountAggregation.newBuilder()
                          .setPrecisionThreshold(
                              toValueProto(uniqueCountAggBuilder.getPrecisionThreshold()))
                          .build())
                  .build())
          .build();
    } else if (aggBuilder instanceof PercentilesAggBuilder) {
      PercentilesAggBuilder percentilesAggBuilder = (PercentilesAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(PercentilesAggBuilder.TYPE)
          .setName(percentilesAggBuilder.getName())
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(percentilesAggBuilder.getField())
                  .setMissing(toValueProto(percentilesAggBuilder.getMissing()))
                  .setPercentiles(
                      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                          .PercentilesAggregation.newBuilder()
                          .addAllPercentiles(percentilesAggBuilder.getPercentiles())
                          .build())
                  .build())
          .build();
    } else if (aggBuilder instanceof MovingAvgAggBuilder) {
      MovingAvgAggBuilder movingAvgAggBuilder = (MovingAvgAggBuilder) aggBuilder;

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(MovingAvgAggBuilder.TYPE)
          .setName(movingAvgAggBuilder.getName())
          .setPipeline(
              KaldbSearch.SearchRequest.SearchAggregation.PipelineAggregation.newBuilder()
                  .setBucketsPath(movingAvgAggBuilder.getBucketsPath())
                  .setMovingAverage(
                      KaldbSearch.SearchRequest.SearchAggregation.PipelineAggregation
                          .MovingAverageAggregation.newBuilder()
                          .setModel(movingAvgAggBuilder.getModel())
                          .setWindow(movingAvgAggBuilder.getWindow())
                          .setPredict(movingAvgAggBuilder.getPredict())
                          .setAlpha(movingAvgAggBuilder.getAlpha())
                          .setBeta(movingAvgAggBuilder.getBeta())
                          .setGamma(movingAvgAggBuilder.getGamma())
                          .setPeriod(movingAvgAggBuilder.getPeriod())
                          .setPad(movingAvgAggBuilder.isPad())
                          .setMinimize(movingAvgAggBuilder.isMinimize())
                          .build())
                  .build())
          .build();
    } else if (aggBuilder instanceof TermsAggBuilder) {
      TermsAggBuilder termsAggBuilder = (TermsAggBuilder) aggBuilder;

      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.Builder
          valueSourceAggregationBuilder =
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(termsAggBuilder.getField())
                  .setTerms(
                      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                          .TermsAggregation.newBuilder()
                          .setSize(termsAggBuilder.getSize())
                          .setMinDocCount(termsAggBuilder.getMinDocCount())
                          .putAllOrder(termsAggBuilder.getOrder())
                          .build());
      if (termsAggBuilder.getMissing() != null) {
        valueSourceAggregationBuilder.setMissing(toValueProto(termsAggBuilder.getMissing()));
      }

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(TermsAggBuilder.TYPE)
          .setName(termsAggBuilder.getName())
          .addAllSubAggregations(
              termsAggBuilder
                  .getSubAggregations()
                  .stream()
                  .map(SearchResultUtils::toSearchAggregationProto)
                  .collect(Collectors.toList()))
          .setValueSource(valueSourceAggregationBuilder.build())
          .build();
    } else if (aggBuilder instanceof DateHistogramAggBuilder) {
      DateHistogramAggBuilder dateHistogramAggBuilder = (DateHistogramAggBuilder) aggBuilder;

      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.DateHistogramAggregation
              .Builder
          dateHistogramAggregationBuilder =
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                  .DateHistogramAggregation.newBuilder()
                  .setInterval(dateHistogramAggBuilder.getInterval())
                  .setMinDocCount(dateHistogramAggBuilder.getMinDocCount())
                  .putAllExtendedBounds(dateHistogramAggBuilder.getExtendedBounds());

      if (dateHistogramAggBuilder.getOffset() != null
          && !dateHistogramAggBuilder.getOffset().isEmpty()) {
        dateHistogramAggregationBuilder.setOffset(dateHistogramAggBuilder.getOffset());
      }

      if (dateHistogramAggBuilder.getFormat() != null
          && !dateHistogramAggBuilder.getFormat().isEmpty()) {
        dateHistogramAggregationBuilder.setFormat(dateHistogramAggBuilder.getFormat());
      }

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(DateHistogramAggBuilder.TYPE)
          .setName(dateHistogramAggBuilder.getName())
          .addAllSubAggregations(
              dateHistogramAggBuilder
                  .getSubAggregations()
                  .stream()
                  .map(SearchResultUtils::toSearchAggregationProto)
                  .collect(Collectors.toList()))
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(dateHistogramAggBuilder.getField())
                  .setDateHistogram(dateHistogramAggregationBuilder.build())
                  .build())
          .build();
    } else if (aggBuilder instanceof HistogramAggBuilder) {
      HistogramAggBuilder histogramAggBuilder = (HistogramAggBuilder) aggBuilder;

      KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.HistogramAggregation
              .Builder
          histogramAggregationBuilder =
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                  .HistogramAggregation.newBuilder()
                  .setInterval(histogramAggBuilder.getInterval())
                  .setMinDocCount(histogramAggBuilder.getMinDocCount());

      return KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
          .setType(HistogramAggBuilder.TYPE)
          .setName(histogramAggBuilder.getName())
          .addAllSubAggregations(
              histogramAggBuilder
                  .getSubAggregations()
                  .stream()
                  .map(SearchResultUtils::toSearchAggregationProto)
                  .collect(Collectors.toList()))
          .setValueSource(
              KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation.newBuilder()
                  .setField(histogramAggBuilder.getField())
                  .setHistogram(histogramAggregationBuilder.build())
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
        protoSearchResult.getFailedNodes(),
        protoSearchResult.getTotalNodes(),
        protoSearchResult.getTotalSnapshots(),
        protoSearchResult.getSnapshotsWithReplicas(),
        OpenSearchInternalAggregation.fromByteArray(
            protoSearchResult.getInternalAggregations().toByteArray()));
  }

  public static <T> KaldbSearch.SearchResult toSearchResultProto(SearchResult<T> searchResult) {
    ScopedSpan span =
        Tracing.currentTracer().startScopedSpan("SearchResultUtils.toSearchResultProto");
    span.tag("tookMicros", String.valueOf(searchResult.tookMicros));
    span.tag("failedNodes", String.valueOf(searchResult.failedNodes));
    span.tag("totalNodes", String.valueOf(searchResult.totalNodes));
    span.tag("totalSnapshots", String.valueOf(searchResult.totalSnapshots));
    span.tag("snapshotsWithReplicas", String.valueOf(searchResult.snapshotsWithReplicas));
    span.tag("hits", String.valueOf(searchResult.hits.size()));

    KaldbSearch.SearchResult.Builder searchResultBuilder = KaldbSearch.SearchResult.newBuilder();
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

    ByteString bytes =
        ByteString.copyFrom(
            OpenSearchInternalAggregation.toByteArray(searchResult.internalAggregation));
    searchResultBuilder.setInternalAggregations(bytes);
    span.finish();
    return searchResultBuilder.build();
  }
}
