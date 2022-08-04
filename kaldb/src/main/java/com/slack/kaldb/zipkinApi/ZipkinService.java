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
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
/**
 * Zipkin compatible API service, for use in Grafana
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
    long defaultLookback = 86400000L;

    String queryString = "trace_id:" + traceId;
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                .setQueryString(queryString) // query everything
                // startTime: endTs - lookback (conversion)
                .setStartTimeEpochMs(
                    defaultLookback) // [Q] double check that these correspond to lookback and endTs
                // [Q] what to set for this
                .setEndTimeEpochMs(System.currentTimeMillis())
                // [Q] difference between howmany and bucketcount?
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
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";

      // should I change the makeSpan method
      final com.slack.service.murron.trace.Trace.ZipkinSpan messageSpan =
          makeSpan(
              messageTraceId,
              messageParentId,
              messageId,
              messageName,
              messageServiceName,
              messageConvertedTimestamp,
              messageDuration,
              messageTags);
      // NEED TO CONVERT A LIST OF TRACES
      String messageString = JsonFormat.printer().print(messageSpan);
      // String messageToAdd = "{" + messageString + "}";
      messageStrings.add(messageString);
    }

    String output = String.valueOf(messageStrings);
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  public static com.slack.service.murron.trace.Trace.ZipkinSpan makeSpan(
      String traceId,
      String parentId,
      String id,
      String name,
      String serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    com.slack.service.murron.trace.Trace.ZipkinSpan.Builder spanBuilder =
        makeSpanBuilder(traceId, parentId, id, name, serviceName, timestamp, duration, tags);
    return spanBuilder.build();
  }

  public static com.slack.service.murron.trace.Trace.ZipkinSpan.Builder makeSpanBuilder(
      String traceId,
      String parentId,
      String id,
      String name,
      String serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    com.slack.service.murron.trace.Trace.ZipkinSpan.Builder spanBuilder =
        com.slack.service.murron.trace.Trace.ZipkinSpan.newBuilder();
    spanBuilder.setTraceId(traceId);

    spanBuilder.setParentId(parentId);
    spanBuilder.setId(id);
    spanBuilder.setName(name);
    spanBuilder.setServiceName(serviceName);
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);
    spanBuilder.putAllTags(tags);
    return spanBuilder;
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
