package com.slack.astra.zipkinApi;

import static com.slack.astra.metadata.dataset.DatasetPartitionMetadata.MATCH_ALL_DATASET;

import brave.Tracing;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Default;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Header;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Path;
import com.slack.astra.blobfs.BlobStore;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.logstore.LogWireMessage;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.server.AstraQueryServiceBase;
import com.slack.astra.util.JsonUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
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

  protected static String convertLogWireMessageToZipkinSpan(List<LogWireMessage> messages)
      throws JsonProcessingException {
    List<ZipkinSpanResponse> traces = new ArrayList<>(messages.size());
    for (LogWireMessage message : messages) {
      if (message.getId() == null) {
        LOG.warn("Document={} cannot have missing id ", message);
        continue;
      }

      String id = message.getId();
      String messageTraceId = null;
      String parentId = null;
      String name = null;
      String serviceName = null;
      String timestamp = String.valueOf(message.getTimestamp().toEpochMilli());
      long duration = 0L;
      Map<String, String> messageTags = new HashMap<>();

      for (String k : message.getSource().keySet()) {
        Object value = message.getSource().get(k);
        if (LogMessage.ReservedField.TRACE_ID.fieldName.equals(k)) {
          messageTraceId = (String) value;
        } else if (LogMessage.ReservedField.PARENT_ID.fieldName.equals(k)) {
          parentId = (String) value;
        } else if (LogMessage.ReservedField.NAME.fieldName.equals(k)) {
          name = (String) value;
        } else if (LogMessage.ReservedField.SERVICE_NAME.fieldName.equals(k)) {
          serviceName = (String) value;
        } else if (LogMessage.ReservedField.DURATION.fieldName.equals(k)) {
          duration = ((Number) value).longValue();
        } else if (LogMessage.ReservedField.ID.fieldName.equals(k)) {
          id = (String) value;
        } else {
          messageTags.put(k, String.valueOf(value));
        }
      }

      // TODO: today at Slack the duration is sent as "duration_ms"
      // We we have this special handling which should be addressed upstream
      // and then removed from here
      if (duration == 0) {
        Object value =
            message.getSource().getOrDefault(LogMessage.ReservedField.DURATION_MS.fieldName, 0);
        duration = TimeUnit.MICROSECONDS.convert(Duration.ofMillis(((Number) value).intValue()));
      }

      // these are some mandatory fields without which the grafana zipkin plugin fails to display
      // the span
      if (messageTraceId == null) {
        messageTraceId = message.getId();
      }
      if (timestamp == null) {
        LOG.warn(
            "Document id={} missing {}",
            message,
            LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName);
        continue;
      }

      final ZipkinSpanResponse span = new ZipkinSpanResponse(id, messageTraceId);
      span.setParentId(parentId);
      span.setName(name);
      if (serviceName != null) {
        ZipkinEndpointResponse remoteEndpoint = new ZipkinEndpointResponse();
        remoteEndpoint.setServiceName(serviceName);
        span.setRemoteEndpoint(remoteEndpoint);
      }
      span.setTimestamp(convertToMicroSeconds(message.getTimestamp()));
      span.setDuration(duration);
      span.setTags(messageTags);
      traces.add(span);
    }
    return objectMapper.writeValueAsString(traces);
  }

  // returning LogWireMessage instead of LogMessage
  // If we return LogMessage the caller then needs to call getSource which is a deep copy of the
  // object. To return LogWireMessage we do a JSON parse
  static List<LogWireMessage> searchResultToLogWireMessage(AstraSearch.SearchResult searchResult)
      throws IOException {
    List<ByteString> hitsByteList = searchResult.getHitsList().asByteStringList();
    List<LogWireMessage> messages = new ArrayList<>(hitsByteList.size());
    for (ByteString byteString : hitsByteList) {
      LogWireMessage hit = JsonUtil.read(byteString.toStringUtf8(), LogWireMessage.class);
      // LogMessage message = LogMessage.fromWireMessage(hit);
      messages.add(hit);
    }
    return messages;
  }

  @VisibleForTesting
  protected static long convertToMicroSeconds(Instant instant) {
    return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
  }

  private static final Logger LOG = LoggerFactory.getLogger(ZipkinService.class);
  private final int defaultMaxSpans;
  private final int defaultLookbackMins;
  private final long defaultDataFreshnessInSeconds;

  private final AstraQueryServiceBase searcher;

  private final BlobStore blobStore;

  public static final String TRACE_CACHE_PREFIX = "traceCacheData";

  private static final ObjectMapper objectMapper =
      JsonMapper.builder()
          // sort alphabetically for easier test asserts
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          // don't serialize null values or empty maps
          .serializationInclusion(JsonInclude.Include.NON_EMPTY)
          .build();

  public ZipkinService(
      AstraQueryServiceBase searcher,
      BlobStore blobStore,
      int defaultMaxSpans,
      int defaultLookbackMins,
      long defaultDataFreshnessInSeconds) {
    this.searcher = searcher;
    this.blobStore = blobStore;
    this.defaultMaxSpans = defaultMaxSpans;
    this.defaultLookbackMins = defaultLookbackMins;
    this.defaultDataFreshnessInSeconds = defaultDataFreshnessInSeconds;
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

  @Blocking
  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(
      @Param("traceId") String traceId,
      @Param("startTimeEpochMs") Optional<Long> startTimeEpochMs,
      @Param("endTimeEpochMs") Optional<Long> endTimeEpochMs,
      @Param("maxSpans") Optional<Integer> maxSpans,
      @Header("X-User-Request") Optional<Boolean> userRequest,
      @Header("X-Data-Freshness-In-Seconds") Optional<Long> dataFreshnessInSeconds)
      throws IOException {

    // Log the custom header userRequest value if present
    if (userRequest.isPresent()) {
      LOG.info("Received custom header X-User-Request: {}", userRequest.get());
      // try to retrieve trace data from blob store cache; check timestamp before using blob store
      // cache for data freshness

      String traceData = retrieveDataFromBlobStoreCache(traceId);
      // if found, return the data
      if (traceData != null) {
        LOG.info("Trace data retrieved from blob store cache for traceId={}", traceId);
        return HttpResponse.of(HttpStatus.OK, MediaType.ANY_APPLICATION_TYPE, traceData);
      }
    }
    JSONObject traceObject = new JSONObject();
    traceObject.put("trace_id", traceId);
    JSONObject queryJson = new JSONObject();
    queryJson.put("term", traceObject);
    String queryString = queryJson.toString();

    long startTime =
        startTimeEpochMs.orElseGet(
            () -> Instant.now().minus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    // we are adding a buffer to end time also because some machines clock may be ahead of current
    // system clock and those spans would be stored but can't be queried

    long endTime =
        endTimeEpochMs.orElseGet(
            () -> Instant.now().plus(this.defaultLookbackMins, ChronoUnit.MINUTES).toEpochMilli());
    int howMany = maxSpans.orElse(this.defaultMaxSpans);

    brave.Span span = Tracing.currentTracer().currentSpan();
    span.tag("startTimeEpochMs", String.valueOf(startTime));
    span.tag("endTimeEpochMs", String.valueOf(endTime));
    span.tag("howMany", String.valueOf(howMany));
    // Add custom header to span tags if present
    userRequest.ifPresent(headerValue -> span.tag("userRequest", headerValue.toString()));

    // TODO: when MAX_SPANS is hit the results will look weird because the index is sorted in
    // reverse timestamp and the spans returned will be the tail. We should support sort in the
    // search request
    AstraSearch.SearchRequest.Builder searchRequestBuilder = AstraSearch.SearchRequest.newBuilder();
    AstraSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setDataset(MATCH_ALL_DATASET)
                .setQuery(queryString)
                .setStartTimeEpochMs(startTime)
                .setEndTimeEpochMs(endTime)
                .setHowMany(howMany)
                .build());
    // we don't account for any failed nodes in the searchResult today
    List<LogWireMessage> messages = searchResultToLogWireMessage(searchResult);
    String output = convertLogWireMessageToZipkinSpan(messages);

    if (userRequest.isPresent() && userRequest.get() && !messages.isEmpty()) {
      long dataFreshnessInSecondsValue =
          dataFreshnessInSeconds.orElse(
              this.defaultDataFreshnessInSeconds); // default to 15 minutes if not
      // Check if no new spans in trace data, it can be saved
      Instant latestSpanTimestamp = getLatestSpanTimestamp(messages);

      if (shouldSaveToBlobStoreCache(latestSpanTimestamp, dataFreshnessInSecondsValue)) {
        LOG.info(
            "Data freshness check done, can be saved to blob store cache for traceId={}", traceId);
        // Save the trace data to blob store cache
        saveDataToBlobStoreCache(traceId, output);
      }
    }
    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);
  }

  protected static boolean shouldSaveToBlobStoreCache(
      Instant latestSpanTimestamp, long dataFreshnessInSeconds) {
    Instant currentTime = Instant.now();
    return latestSpanTimestamp.isBefore(
        currentTime.minus(dataFreshnessInSeconds, ChronoUnit.SECONDS));
  }

  @VisibleForTesting
  protected Instant getLatestSpanTimestamp(List<LogWireMessage> spanList) {
    return spanList.stream()
        .map(LogWireMessage::getTimestamp)
        .max(Comparator.naturalOrder())
        .orElse(null);
  }

  protected String retrieveDataFromBlobStoreCache(String traceId) {
    assert traceId != null && !traceId.isEmpty();

    try {
      // Retrieve the compressed trace data from blob store cache
      String jsonData =
          blobStore.readFileData(
              String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId), true);

      if (jsonData == null || jsonData.isEmpty()) {
        LOG.warn("No trace data found in blob store cache for traceId={}", traceId);
        return null;
      }
      LOG.info(
          "Retrieved and decompressed trace data from blob store cache for traceId={}", traceId);
      return jsonData;

    } catch (Exception e) {
      // Log other exceptions as errors
      LOG.error("Error retrieving trace data from blob store cache for traceId={}", traceId, e);
      return null;
    }
  }

  protected void saveDataToBlobStoreCache(String traceId, String output) {
    assert traceId != null && !traceId.isEmpty();
    assert output != null && !output.isEmpty();

    try {
      // Upload the compressed trace data to blob store cache
      String srcLocation =
          String.format("%s/tmp-%s/%s.json.gz", TRACE_CACHE_PREFIX, traceId, UUID.randomUUID());
      String dstLocation = String.format("%s/%s/traceData.json.gz", TRACE_CACHE_PREFIX, traceId);

      if (blobStore.pathExists("%s/tmp-%s".formatted(TRACE_CACHE_PREFIX, traceId))) {
        LOG.info("Temporary location found in blob store cache for traceId={}", traceId);
        return;
      }

      blobStore.uploadData(srcLocation, output, true);
      blobStore.copyFile(srcLocation, dstLocation);
      blobStore.delete(String.format("%s/tmp-%s", TRACE_CACHE_PREFIX, traceId));

      LOG.info("Compressed trace data saved to blob store cache for traceId={}", traceId);

    } catch (Exception e) {
      LOG.error("Error saving trace data to blob store cache for traceId={}", traceId, e);
      throw new RuntimeException("Failed to save trace data to blob store cache", e);
    }
  }
}
