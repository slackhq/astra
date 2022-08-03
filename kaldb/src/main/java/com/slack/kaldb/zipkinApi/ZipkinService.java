package com.slack.kaldb.zipkinApi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
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

@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ZipkinService {
  private final KaldbQueryServiceBase searcher;

  public ZipkinService(KaldbQueryServiceBase searcher) {
    this.searcher = searcher;
  }

  /**
   * Get Services API
   *
   * @return a list of all service names associated with span endpoints.
   * @see <a href="https://zipkin.io/zipkin-api/#/default/get_services">API doc</a>
   */
  @Get
  @Path("/api/v2/services")
  public HttpResponse getServices() throws IOException {

    GetServiceResult results = new GetServiceResult(ImmutableList.of("s1", "s2"));
    String output = JsonUtil.writeJsonArray(JsonUtil.writeAsString(results.getServices()));

    return HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, output);

    /*

    String queryString =
            "*:*";
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
            searcher.doSearch(
                    searchRequestBuilder
                            .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                            .setQueryString("*:*") // query everything
                            // startTime: endTs - lookback (conversion)
                            .setStartTimeEpochMs(
                                    lookback) // [Q] double check that these correspond to lookback and endTs not
                            // min and max Duration
                            .setEndTimeEpochMs(endTs)
                            // [Q] difference between howmany and bucketcount?
                            .setHowMany(limit)
                            .setBucketCount(0)
                            .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();

    for (LogMessage message : messages) {
      Map<String, Object> source = message.getSource();
      final String messageTraceId = (String) source.get("trace_id");
      final String messageId = message.id;
      final String messageParentId = (String) source.get("parent_id");
      // [Q]timestamp not completely correct
      final String messageTimestamp = (String) source.get("@timestamp");
      Instant instant = Instant.parse(messageTimestamp);
      final long messageTimestampMicros =
              TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
                      + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      final long messageDurationMicros = ((Number) source.get("duration_ms")).longValue();
      final String messageServiceName = (String) source.get("service_name");
      final String messageName = (String) source.get("name");
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";
      final com.slack.service.murron.trace.Trace.Span messageSpan =
              makeSpan(
                      messageTraceId,
                      messageId,
                      messageParentId,
                      messageTimestampMicros,
                      messageDurationMicros,
                      messageName,
                      messageServiceName,
                      messageMsgType);
      // NEED TO CONVERT A LIST OF TRACES
      messageStrings.add(JsonUtil.writeJsonArray(JsonUtil.writeAsString(messageSpan)));
    }

    return String.valueOf(messageStrings);

     */

  }

  /**
   * Get Span Names API
   *
   * @return all the span names recorded by a particular service
   * @see <a href="https://zipkin.io/zipkin-api/#/default/get_spans">API doc</a>
   */
  @Get("/api/v2/spans")
  public String getSpans(@Param("serviceName") Optional<String> serviceName) throws IOException {
    /*
    String id = "352bff9a74ca9ad2";
    String traceId = "5af7183fb1d4cf5f";
    String parentId = "6b221d5bc9e6496c";
    String name = "get /api";
    long timestamp = 1556604172355737L;
    long duration = 1431;
    String kind = "SERVER";
    HashMap<String, Object> localEndpoint = new HashMap<>();
    localEndpoint.put("serviceName", "backend");
    localEndpoint.put("ipv4", "192.168.99.1");
    localEndpoint.put("port", 3306);
    HashMap<String, Object> remoteEndpoint = new HashMap<>();
    remoteEndpoint.put("ipv4", "172.19.0.2");
    remoteEndpoint.put("port", 58648);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("http.method", "GET");
    tags.put("http.path", "/api");
    Span span1 =
        new Span(
            id,
            traceId,
            parentId,
            name,
            timestamp,
            duration,
            kind,
            localEndpoint,
            remoteEndpoint,
            null,
            tags);
    //TODO: output List<Span> as JSON instead of Service of List<Span>
    Service service = new Service(ImmutableList.of(span1));
    return JsonUtil.writeJsonArray(JsonUtil.writeAsString(service));
    */

    // TODO: consider how to qualify the query variables
    String queryString = "serviceName:" + serviceName;
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                .setQueryString(queryString) // query everything
                // startTime: endTs - lookback (conversion)
                .setStartTimeEpochMs(
                    1234567) // [Q] double check that these correspond to lookback and
                // endTs not
                // min and max Duration
                .setEndTimeEpochMs(1234567)
                // [Q] difference between howmany and bucketcount?
                .setHowMany(10)
                .setBucketCount(0)
                .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();
    for (LogMessage message : messages) {

      /*
      Map<String, Object> source = message.getSource();
      final String messageTraceId = (String) source.get("trace_id");
      final String messageId = message.id;
      final String messageParentId = (String) source.get("parent_id");
      // [Q]timestamp not completely correct
      final String messageTimestamp = (String) source.get("@timestamp");
      Instant instant = Instant.parse(messageTimestamp);
      final long messageTimestampMicros =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      final long messageDurationMicros = ((Number) source.get("duration_ms")).longValue();
      final String messageServiceName = (String) source.get("service_name");
      final String messageName = (String) source.get("name");
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";
      final com.slack.service.murron.trace.Trace.Span messageSpan =
          makeSpan(
              messageTraceId,
              messageId,
              messageParentId,
              messageTimestampMicros,
              messageDurationMicros,
              messageName,
              messageServiceName,
              messageMsgType);
      messageStrings.add(JsonUtil.writeAsString(messageSpan));

       */
      Map<String, Object> source = message.getSource();
      final String messageName = (String) source.get("name");
      messageStrings.add(messageName);
    }
    return String.valueOf(messageStrings);
  }

  public static void main(String[] args) throws JsonProcessingException {
    System.out.println(ByteString.copyFrom("12345".getBytes()));
  }

  /*
  @Get("/api/v2/traces")
  public String getTraces(
      // [Q]should be optional string change later!!
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Long endTs,
      @Param("lookback") Long lookback,
      @Param("limit") @Default("10") Integer limit)
      throws IOException {
    /*
      String id = "352bff9a74ca9ad2";
      String traceId = "5af7183fb1d4cf5f";
      String parentId = "6b221d5bc9e6496c";
      String name = "get /api";
      long timestamp = 1556604172355737L;
      long duration = 1431;
      String kind = "SERVER";
      HashMap<String, Object> localEndpoint = new HashMap<>();
      localEndpoint.put("serviceName", "backend");
      localEndpoint.put("ipv4", "192.168.99.1");
      localEndpoint.put("port", 3306);
      HashMap<String, Object> remoteEndpoint = new HashMap<>();
      remoteEndpoint.put("ipv4", "172.19.0.2");
      remoteEndpoint.put("port", 58648);
      HashMap<String, String> tags = new HashMap<>();
      tags.put("http.method", "GET");
      tags.put("http.path", "/api");
      HashMap<String, Object> annotation1 = new HashMap<>();
      annotation1.put("timestamp", 1654801067552016L);
      annotation1.put("value", "wr");
      HashMap<String, Object> annotation2 = new HashMap<>();
      annotation2.put("timestamp", 1654801067558147L);
      annotation2.put("value", "ws");
      ImmutableList<HashMap<String, Object>> annotations = ImmutableList.of(annotation1, annotation2);
      Span span1 =
          new Span(
              id,
              traceId,
              parentId,
              name,
              timestamp,
              duration,
              kind,
              localEndpoint,
              remoteEndpoint,
              annotations,
              tags);
      Trace trace1 = new Trace(ImmutableList.of(span1, span1), traceId);
      String id2 = "7b6a379f25526305";
      String traceId2 = "7b6a379f25526305";
      String parentId2 = "6b221d5bc9e6496c";
      String name2 = "get";
      long timestamp2 = 1654812644350000L;
      long duration2 = 5233;
      String kind2 = "SERVER";
      HashMap<String, Object> localEndpoint2 = new HashMap<>();
      localEndpoint2.put("serviceName", "kaldbquery");
      HashMap<String, Object> remoteEndpoint2 = new HashMap<>();
      remoteEndpoint2.put("ipv6", "::1");
      remoteEndpoint2.put("port", 58264);
      HashMap<String, String> tags2 = new HashMap<>();
      tags2.put("address.local", "/0:0:0:0:0:0:0:1:8081");
      tags2.put("address.remote", "/0:0:0:0:0:0:0:1:58264");
      HashMap<String, Object> annotation3 = new HashMap<>();
      annotation3.put("timestamp", 1654812644350021L);
      annotation3.put("value", "wr");
      HashMap<String, Object> annotation4 = new HashMap<>();
      annotation4.put("timestamp", 1654812644354773L);
      annotation4.put("value", "ws");
      ImmutableList<HashMap<String, Object>> annotations2 =
          ImmutableList.of(annotation3, annotation4);
      Span span2 =
          new Span(
              id2,
              traceId2,
              parentId2,
              name2,
              timestamp2,
              duration2,
              kind2,
              localEndpoint2,
              remoteEndpoint2,
              annotations2,
              tags2);
      Trace trace2 = new Trace(ImmutableList.of(span2, span2), traceId2);
      List<String> traces =
          ImmutableList.of(
              JsonUtil.writeJsonArray(JsonUtil.writeAsString(trace1)),
              JsonUtil.writeJsonArray(JsonUtil.writeAsString(trace2)));
      return String.valueOf(traces);
    */
  /*
      String serviceName = "";
      String spanName = "";
      String annotationQuery = "first and h=/ and retried and hi and fh=wef";
      int minDuration = 0;
      int maxDuration = 0;

      // [Q] what format is endTs?
      long endTs = 1612550512340953L;
      // [Q] double check that lookback <= endTs?
      long lookback = 1612550512340953L;
      int limit = 10;
  */

  /*
  String s = "";
  Map<String, String> kv = new HashMap<>();
  List<String> words = new ArrayList<>();
  String k = "";
  String v = "";
  int equal = 2;
  int equalIndex = 0;
  int cIndex = 0;
  int startIndex = 0;
  int endIndex = 0;

  while (cIndex < annotationQuery.length()) {
    char c = annotationQuery.charAt(cIndex);
    if (cIndex == annotationQuery.length() - 1) {
      if (equal == 0) {
        words.add(annotationQuery.substring(startIndex, cIndex + 1));
      } else if (equal == 1) {
        k = annotationQuery.substring(startIndex, equalIndex);
        v = annotationQuery.substring(equalIndex + 1, cIndex + 1);
        kv.put(k, v);
      }
    }
    if (c != 32) { // checks if it's space
      s = s + c;
      if (c == 61) { // checks if contains =
        equal = 1;
        equalIndex = cIndex;
      }
      cIndex++;
    } else { // it is space but could be before or after "and"
      if (equal == 2) {
        equal = 0;
      }
      endIndex = cIndex;
      if (equal == 1) {
        k = annotationQuery.substring(startIndex, equalIndex);
        v = annotationQuery.substring(equalIndex + 1, endIndex);
        kv.put(k, v);

      } else if (equal == 0) {
        words.add(s);
      }
      equal = 2;
      cIndex += 5;
      startIndex = cIndex;
      s = "";
    }
  }

   */

  /*
    String queryString =
        "serviceName:"
            + serviceName
            + " and spanName:"
            + spanName
            + " and duration <= "
            + maxDuration
            + " and duration >= "
            + minDuration;
    KaldbSearch.SearchRequest.Builder searchRequestBuilder = KaldbSearch.SearchRequest.newBuilder();
    KaldbSearch.SearchResult searchResult =
        searcher.doSearch(
            searchRequestBuilder
                .setIndexName("testindex") // [Q] as of now we don't need to worry about index?
                .setQueryString(queryString) // query everything
                // startTime: endTs - lookback (conversion)
                .setStartTimeEpochMs(
                    lookback) // [Q] double check that these correspond to lookback and endTs not
                // min and max Duration
                .setEndTimeEpochMs(endTs)
                // [Q] difference between howmany and bucketcount?
                .setHowMany(limit)
                .setBucketCount(0)
                .build());
    List<LogMessage> messages = searchResultToLogMessage(searchResult);
    List<String> messageStrings = new ArrayList<>();

    for (LogMessage message : messages) {
      Map<String, Object> source = message.getSource();
      final String messageTraceId = (String) source.get("trace_id");
      final String messageId = message.id;
      final String messageParentId = (String) source.get("parent_id");
      // [Q]timestamp not completely correct
      final String messageTimestamp = (String) source.get("@timestamp");
      Instant instant = Instant.parse(messageTimestamp);
      final long messageTimestampMicros =
          TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
              + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
      final long messageDurationMicros = ((Number) source.get("duration_ms")).longValue();
      final int messageDuration = ((Number) source.get("duration_ms")).intValue();
      final String messageServiceName = (String) source.get("service_name");
      final String messageName = (String) source.get("name");
      // [Q]what to put for msgtype
      final String messageMsgType = "test message type";
      final com.slack.service.murron.trace.Trace.Span messageSpan =
          makeSpan(
              messageTraceId,
              messageId,
              messageParentId,
              messageTimestampMicros,
              messageDurationMicros,
              messageDuration,
              messageName,
              messageServiceName,
              messageMsgType);
      // NEED TO CONVERT A LIST OF TRACES
      messageStrings.add(JsonUtil.writeJsonArray(JsonUtil.writeAsString(messageSpan)));
    }

    return String.valueOf(messageStrings);
  }

  */

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

  // [Q] COPIED THESE TWO FUNCTIONS FROM SPANUTIL BECAUSE THERE WAS AN IMPORT ISSUE BUT WILL FIX
  // LATER
  public static final String BINARY_TAG_VALUE = "binaryTagValue";

  @Get("/api/v2/trace/{traceId}")
  public HttpResponse getTraceByTraceId(@Param("traceId") String traceId) throws IOException {

    /* ONE SPAN VERSION

    String id = "352bff9a74ca9ad2";
    //String traceId = "5af7183fb1d4cf5f";
    String parentId = "6b221d5bc9e6496c";
    String name = "get /api";
    long timestamp = 1556604172355737L;
    long duration = 1431;
    String kind = "SERVER";
    HashMap<String, Object> localEndpoint = new HashMap<>();
    localEndpoint.put("serviceName", "backend");
    localEndpoint.put("ipv4", "192.168.99.1");
    localEndpoint.put("port", 3306);
    HashMap<String, Object> remoteEndpoint = new HashMap<>();
    remoteEndpoint.put("ipv4", "172.19.0.2");
    remoteEndpoint.put("port", 58648);
    HashMap<String, String> tags = new HashMap<>();
    tags.put("http.method", "GET");
    tags.put("http.path", "/api");

    HashMap<String, Object> annotation1 = new HashMap<>();
    annotation1.put("timestamp", 1654801067552016L);
    annotation1.put("value", "wr");
    HashMap<String, Object> annotation2 = new HashMap<>();
    annotation2.put("timestamp", 1654801067558147L);
    annotation2.put("value", "ws");
    ImmutableList<HashMap<String, Object>> annotations = ImmutableList.of(annotation1, annotation2);
    Span span1 =
        new Span(
            id,
            traceId,
            parentId,
            name,
            timestamp,
            duration,
            kind,
            localEndpoint,
            remoteEndpoint,
            annotations,
            tags);
    Trace trace = new Trace(ImmutableList.of(span1), traceId);
    String output = JsonUtil.writeAsString(trace);
    ArrayList<Span> spanOutput = new ArrayList<>();
    spanOutput.add(span1);
    return HttpResponse.of(
        HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(spanOutput));




    */

    // TWO
    //
//        String id = "0e43015b91a2f78e";
//        String parentId = "3a77ad082938539e";
//        String name = "logindexsearcherimpl.search";
//        long timestamp = 1658448809744354L;
//        long duration = 1296;
//        String kind = "SERVER";
//        HashMap<String, Object> localEndpoint = new HashMap<>();
//        localEndpoint.put("serviceName", "kaldbindex");
//        localEndpoint.put("ipv4", "192.168.99.1");
//        localEndpoint.put("port", 3306);
//        HashMap<String, Object> remoteEndpoint = new HashMap<>();
//        remoteEndpoint.put("ipv4", "172.19.0.2");
//        remoteEndpoint.put("port", 58648);
//        HashMap<String, String> tags = new HashMap<>();
//        tags.put("http.method", "GET");
//        tags.put("http.path", "/api");
//        HashMap<String, Object> annotation1 = new HashMap<>();
//        annotation1.put("timestamp", 1654801067552016L);
//        annotation1.put("value", "wr");
//        HashMap<String, Object> annotation2 = new HashMap<>();
//        annotation2.put("timestamp", 1654801067558147L);
//        annotation2.put("value", "ws");
//        ImmutableList<HashMap<String, Object>> annotations = ImmutableList.of(annotation1,
//     annotation2);
//        Span span1 =
//                new Span(
//                        id,
//                        traceId,
//                        parentId,
//                        name,
//                        timestamp,
//                        duration,
//                        kind,
//                        localEndpoint,
//                        remoteEndpoint,
//                        annotations,
//                        tags);
//
//
//
//        String id2 = "9bd9a9a81d2760b5";
//        String parentId2 = "3a77ad082938539e";
//        String name2 = "searchresultutils.tosearchresultproto";
//        long timestamp2 = 1658448809746405L;
//        long duration2 = 542;
//        String kind2 = "SERVER";
//        HashMap<String, Object> localEndpoint2 = new HashMap<>();
//        localEndpoint2.put("serviceName", "kaldbindex");
//        HashMap<String, Object> remoteEndpoint2 = new HashMap<>();
//        remoteEndpoint.put("ipv4", "172.19.0.2");
//        remoteEndpoint.put("port", 58648);
//        HashMap<String, String> tags2 = new HashMap<>();
//        tags.put("http.method", "GET");
//        tags.put("http.path", "/api");
//        HashMap<String, Object> annotation3 = new HashMap<>();
//        annotation3.put("timestamp", 1654801067552016L);
//        annotation3.put("value", "wr");
//        HashMap<String, Object> annotation4 = new HashMap<>();
//        annotation4.put("timestamp", 1654801067558147L);
//        annotation4.put("value", "ws");
//        ImmutableList<HashMap<String, Object>> annotations2 = ImmutableList.of(annotation3,
//     annotation4);
//        Span span2 =
//                new Span(
//                        id2,
//                        traceId,
//                        parentId2,
//                        name2,
//                        timestamp2,
//                        duration2,
//                        kind2,
//                        localEndpoint2,
//                        remoteEndpoint2,
//                        annotations2,
//                        tags2);
//        Trace trace = new Trace(ImmutableList.of(span1, span2), traceId);
//        String output = JsonUtil.writeAsString(trace);
//        ArrayList<Span> spanOutput = new ArrayList<>();
//        spanOutput.add(span1);
//        spanOutput.add(span2);
//        return HttpResponse.of(
//                HttpStatus.OK, MediaType.JSON_UTF_8, JsonUtil.writeAsString(spanOutput));
//




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
    // TODO: figure out why this is not giving back "12345"
    /*
    spanBuilder.setTraceId(ByteString.copyFrom(traceId.getBytes()));

    spanBuilder.setParentId(ByteString.copyFrom(parentId.getBytes()));
    spanBuilder.setId(ByteString.copyFrom(id.getBytes()));

     */
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
}
