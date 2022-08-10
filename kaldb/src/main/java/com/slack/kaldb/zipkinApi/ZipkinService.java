package com.slack.kaldb.zipkinApi;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.logstore.LogWireMessage;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.server.KaldbQueryServiceBase;
import com.slack.kaldb.util.JsonUtil;
import com.slack.service.murron.trace.Trace;
import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);

  private final KaldbQueryServiceBase searcher;
  JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();

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
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    List<String> traces = new ArrayList<>();

    for (LogWireMessage message : messages) {
      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = null;
      long duration = Long.MIN_VALUE;

      final Map<String, String> messageTags = new HashMap<>();

      for (String k : message.source.keySet()) {
        Object value = message.source.get(k);

        switch (k) {
          case "trace_id":
            messageTraceId = (String) value;
            break;
          case "parent_id":
            parentId = (String) value;
            break;
          case "name":
            name = (String) value;
            break;
          case "service_name":
            serviceName = (String) value;
            break;
          case "@timestamp":
            timestamp = (String) value;
            break;
          case "duration_ms":
            duration = ((Number) value).longValue();
            break;
          default:
            messageTags.put(k, String.valueOf(value));
        }
      }
      if (messageTraceId == null) {
        LOG.trace("Document id={} missing trace_id", message.id);
        continue;
      } else if (parentId == null) {
        LOG.trace("Document id={} missing parent_id", parentId);
        continue;
      } else if (name == null) {
        LOG.trace("Document id={} missing name", name);
        continue;
      } else if (serviceName == null) {
        LOG.trace("Document id={} missing parent_id", serviceName);
        continue;
      } else if (timestamp == null) {
        LOG.trace("Document id={} missing @timestamp", timestamp);
        continue;
      } else if (duration == Long.MIN_VALUE) {
        LOG.trace("Document id={} duration_ms cannot be set to ", message.id);
        continue;
      }

      Instant instant = Instant.parse(timestamp);
      final long messageConvertedTimestamp =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());

      final Trace.Span span =
          makeSpan(
              messageTraceId,
              parentId,
              message.id,
              name,
              serviceName,
              messageConvertedTimestamp,
              duration,
              messageTags);
      String spanJson = printer.print(span);
      traces.add(spanJson);
    }
    String output = String.valueOf(traces);
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
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);
    // TODO: currently tags is <String, KeyValue> since we want to preserve typing from the incoming
    // murron message
    // the zipkin proto however expects it to be <String, String>
    // spanBuilder.addAllTags(tags);
    return spanBuilder.build();
  }

  // intentionally returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object
  public static List<LogWireMessage> searchResultToLogWireMessage(
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
}
