package com.slack.kaldb.zipkinApi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.elasticsearchApi.searchResponse.*;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.*;

@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ZipkinService {
  private final KaldbServiceGrpc.KaldbServiceImplBase searcher;

  public ZipkinService(KaldbServiceGrpc.KaldbServiceImplBase searcher) {
    this.searcher = searcher;
  }

  /**
   * Get Services API
   *
   * @return a list of all service names associated with span endpoints.
   * @see <a href="https://zipkin.io/zipkin-api/#/default/get_services">API doc</a>
   */
  @Get
  @Path("/services")
  public String getServices() throws IOException {
    GetServiceResult results = new GetServiceResult(ImmutableList.of("s1", "s2"));
    return JsonUtil.writeJsonArray(JsonUtil.writeAsString(results.getServices()));
  }

  /**
   * Get Span Names API
   *
   * @return all the span names recorded by a particular service
   * @see <a href="https://zipkin.io/zipkin-api/#/default/get_spans">API doc</a>
   */
  @Get("/spans")
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
    return null;
  }

  public static void main(String[] args) throws JsonProcessingException {
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
    System.out.println(traces);
  }

  @Get("/traces")
  public String getTraces(
      @Param("serviceName") Optional<String> serviceName,
      @Param("spanName") Optional<String> spanName,
      @Param("annotationQuery") Optional<String> annotationQuery,
      @Param("minDuration") Optional<Integer> minDuration,
      @Param("maxDuration") Optional<Integer> maxDuration,
      @Param("endTs") Optional<Long> endTs,
      @Param("lookback") Optional<Long> lookback,
      @Param("limit") @Default("10") Optional<Integer> limit)
      throws JsonProcessingException {
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
  }

  @Get("/trace/{traceId}")
  public String getTraceByTraceId(@Param("traceId") String traceId) throws JsonProcessingException {
    String id = "352bff9a74ca9ad2";
    // String traceId = "5af7183fb1d4cf5f";
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
    return JsonUtil.writeJsonArray(JsonUtil.writeAsString(trace));
  }
}
