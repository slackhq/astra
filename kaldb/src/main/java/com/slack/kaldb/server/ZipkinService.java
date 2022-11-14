package com.slack.kaldb.server;

import static com.slack.kaldb.metadata.dataset.DatasetPartitionMetadata.MATCH_ALL_DATASET;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.proto3.Endpoint;
import zipkin2.proto3.Span;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/**
 * Zipkin compatible API service
 *
 * @see <a
 *     href="https://github.com/grafana/grafana/blob/main/public/app/plugins/datasource/zipkin/datasource.ts">Grafana
 *     Zipkin API</a> <a
 *     href="https://github.com/openzipkin/zipkin-api/blob/master/zipkin.proto">Trace proto
 *     compatible with Zipkin</a> <a href="https://zipkin.io/zipkin-api/#/">Trace API Swagger
 *     Hub</a>
 */
public class ZipkinService {

  protected static String convertLogWireMessageToZipkinSpan(List<LogWireMessage> messages)
      throws InvalidProtocolBufferException {
    List<String> traces = new ArrayList<>(messages.size());
    for (LogWireMessage message : messages) {
      if (message.id == null) {
        LOG.warn("Document={} cannot have missing id ", message);
        continue;
      }

      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = null;
      long duration = Long.MIN_VALUE;
      Map<String, String> messageTags = new HashMap<>();

      for (String k : message.source.keySet()) {
        Object value = message.source.get(k);
        if (LogMessage.ReservedField.TRACE_ID.fieldName.equals(k)) {
          messageTraceId = (String) value;
        } else if (LogMessage.ReservedField.PARENT_ID.fieldName.equals(k)) {
          parentId = (String) value;
        } else if (LogMessage.ReservedField.NAME.fieldName.equals(k)) {
          name = (String) value;
        } else if (LogMessage.ReservedField.SERVICE_NAME.fieldName.equals(k)) {
          serviceName = (String) value;
        } else if (LogMessage.ReservedField.TIMESTAMP.fieldName.equals(k)) {
          timestamp = (String) value;
        } else if (LogMessage.ReservedField.DURATION_MS.fieldName.equals(k)) {
          duration = ((Number) value).longValue();
        } else {
          messageTags.put(k, String.valueOf(value));
        }
      }
      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.id;
      }
      if (timestamp == null) {
        LOG.warn("Document id={} missing @timestamp", message);
        continue;
      }

      final long messageConvertedTimestamp = convertToMicroSeconds(Instant.parse(timestamp));

      final Span span =
          makeSpan(
              messageTraceId,
              Optional.ofNullable(parentId),
              message.id,
              Optional.ofNullable(name),
              Optional.ofNullable(serviceName),
              messageConvertedTimestamp,
              duration,
              messageTags);
      String spanJson = printer.print(span);
      traces.add(spanJson);
    }
    StringJoiner outputJsonArray = new StringJoiner(",", "[", "]");
    traces.forEach(outputJsonArray::add);
    return outputJsonArray.toString();
  }

  // returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object. To return LogWireMessage we do a JSON parse
  private static List<LogWireMessage> searchResultToLogWireMessage(
      KaldbSearch.SearchResult searchResult) throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogWireMessage> messages = new ArrayList<>(hitsByteList.size());
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      // LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(hit);
    }
    return messages;
  }

  private static Span makeSpan(
      String traceId,
      Optional<String> parentId,
      String id,
      Optional<String> name,
      Optional<String> serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    Span.Builder spanBuilder = Span.newBuilder();

    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()).toStringUtf8());
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()).toStringUtf8());
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);

    parentId.ifPresent(
        s -> spanBuilder.setParentId(ByteString.copyFrom(s.getBytes()).toStringUtf8()));
    name.ifPresent(spanBuilder::setName);
    serviceName.ifPresent(
        s -> spanBuilder.setRemoteEndpoint(Endpoint.newBuilder().setServiceName(s)));
    spanBuilder.putAllTags(tags);
    return spanBuilder.build();
  }

  @VisibleForTesting
  protected static long convertToMicroSeconds(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);
  private static long LOOKBACK_MINS = 60 * 2;

  private static final int MAX_SPANS = 20_000;

  private final KaldbQueryServiceBase searcher;
  private static final JsonFormat.Printer printer =
      JsonFormat.printer().includingDefaultValueFields();

  public ZipkinService(KaldbQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  @Get
  @Path("/api/v2/services")
  public HttpResponse getServices() throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/spans")
  public HttpResponse getSpans(@Param("serviceName") Optional<String> serviceName)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/traces")
  public HttpResponse getTraces(
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Long endTs,
      @Param("lookback") Long lookback,
      @Param("limit") @Default("10") Integer limit)
      throws IOException {
    String output = "[]";
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("maxSpans") Optional<Integer> maxSpans)
      throws IOException {

    String queryString = "trace_id:" + traceId;

    long startTime =
        startTimeEpochMs.orElseGet(
            () -> Instant.now().minus(LOOKBACK_MINS, ChronoUnit.MINUTES).toEpochMilli());
    // we are adding a buffer to end time also because some machines clock may be ahead of current
    // system clock and those spans would be stored but can't be queried
    long endTime =
        endTimeEpochMs.orElseGet(
            () -> Instant.now().plus(LOOKBACK_MINS, ChronoUnit.MINUTES).toEpochMilli());

    // TODO: when MAX_SPANS is hit the results will look weird because the index is sorted in
    // reverse timestamp and the spans returned will be the tail. We should support sort in the
    // search request
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset(MATCH_ALL_DATASET)
                .setQueryString(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(endTime)
                .setLimit(maxSpans.orElse(MAX_SPANS))
                .setBucketCount(0)
                .build());
    // we don't account for any failed nodes in the searchResult today
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    String output = convertLogWireMessageToZipkinSpan(messages);

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }
}
