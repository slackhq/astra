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
import java.time.LocalDate;
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
    // since the traceById doesn't support a start date param max out to 1 days for now
    // todo: read this value from the replica config?
    long startTime = TimeUnit.NANOSECONDS.toMillis(LocalDate.now().minusDays(1).atStartOfDay().getNano());
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset("_all")
                .setQueryString(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(System.currentTimeMillis())
                .setHowMany(10)
                .setBucketCount(0)
                .build());
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    List<String> traces = new ArrayList<>();

    for (LogWireMessage message : messages) {
      if (message.id == null) {
        LOG.warn("Document cannot have missing id");
        continue;
      }

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
      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.id;
      }
      if (timestamp == null) {
        LOG.warn("Document id={} missing @timestamp", message.id);
        continue;
      }

      Instant instant = Instant.parse(timestamp);
      final long messageConvertedTimestamp =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());

      final Trace.ZipkinSpan span =
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
    String output = outputJsonArray.toString();

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  public static Trace.ZipkinSpan makeSpan(
      String traceId,
      Optional<String> parentId,
      String id,
      Optional<String> name,
      Optional<String> serviceName,
      long timestamp,
      long duration,
      Map<String, String> tags) {
    Trace.ZipkinSpan.Builder spanBuilder = Trace.ZipkinSpan.newBuilder();

    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()).toStringUtf8());
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()).toStringUtf8());
    spanBuilder.setTimestamp(timestamp);
    spanBuilder.setDuration(duration);

    parentId.ifPresent(
        s -> spanBuilder.setParentId(ByteString.copyFrom(s.getBytes()).toStringUtf8()));
    name.ifPresent(spanBuilder::setName);
    serviceName.ifPresent(
        s -> spanBuilder.setRemoteEndpoint(Trace.Endpoint.newBuilder().setServiceName(s)));
    spanBuilder.putAllTags(tags);
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
