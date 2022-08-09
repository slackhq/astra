package com.slack.kaldb.zipkinApi;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

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

  private final KaldbQueryServiceBase searcher;

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
  public HttpResponse getTraceByTraceId(@Param("traceId") String traceId) throws IOException {

    String queryString = "trace_id:" + traceId;
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset("_all")
                .setQueryString(queryString)
                .setStartTimeEpochMs(0)
                .setEndTimeEpochMs(System.currentTimeMillis())
                .setHowMany(10)
                .setBucketCount(0)
                .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();

    for (LogMessage message : messages) {
      Map<String, Object> source = message.getSource();
      String messageTraceId = null;
      String messageParentId = null;
      final String messageId = message.id;
      String messageName = null;
      String messageServiceName = null;
      String messageTimestamp = null;
      long messageDuration = Long.MIN_VALUE;

      final Map<String, String> messageTags = new HashMap<>();

      for (String k : source.keySet()) {
        Object value = source.get(k);

        switch (k) {
          case "trace_id":
            messageTraceId = (String) value;
            break;
          case "parent_id":
            messageParentId = (String) value;
            break;
          case "name":
            messageName = (String) value;
            break;
          case "service_name":
            messageServiceName = (String) value;
            break;
          case "@timestamp":
            messageTimestamp = (String) value;
            break;
          case "duration_ms":
            messageDuration = ((Number) value).longValue();
            break;
          default:
            messageTags.put(k, String.valueOf(value));
        }
      }
      Instant instant = Instant.parse(messageTimestamp);
      final long messageConvertedTimestamp =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      if (messageTraceId == null
          || messageParentId == null
          || messageName == null
          || messageServiceName == null
          || messageTimestamp == null
          || messageDuration == Long.MIN_VALUE) {
        continue;
      }
      final Trace.Span messageSpan =
          makeSpan(
              messageTraceId,
              messageParentId,
              messageId,
              messageName,
              messageServiceName,
              messageConvertedTimestamp,
              messageDuration,
              messageTags);
      String messageString = JsonFormat.printer().print(messageSpan);
      messageStrings.add(messageString);
    }

    String output = String.valueOf(messageStrings);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  public static Trace.Span makeSpan(
      String traceId,
      String parentId,
      String id,
      String name,
      String serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    Trace.Span.Builder spanBuilder = Trace.Span.newBuilder();

    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));
    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));
    spanBuilder.setName(name);
    // TODO: vthacker - should this be a separate named field?
    // spanBuilder.setServiceName(serviceName);
    spanBuilder.setStartTimestampMicros(timestamp);
    spanBuilder.setDurationMicros(duration);
    // TODO: currently tags is <String, KeyValue> since we want to preserve typing from the incoming
    // murron message
    // the zipkin proto however expects it to be <String, String>
    // spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }

  public static List<LogMessage> searchResultToLogMessage(KaldbSearch.SearchResult searchResult)
      throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogMessage> messages = new ArrayList<>();
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(message);
    }
    return messages;
  }
}
