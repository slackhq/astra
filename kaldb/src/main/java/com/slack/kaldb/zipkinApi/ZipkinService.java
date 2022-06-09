package com.slack.kaldb.zipkinApi;


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
  @Get("/spans/{serviceName}")
  public String getSpans(@Param("serviceName") String serviceName) throws IOException {
    String id = "352bff9a74ca9ad2";
    String traceId = "5af7183fb1d4cf5f";
    String parentId = "6b221d5bc9e6496c";
    String name = "get /api";
    long timestamp = 1556604172355737L;
    int duration = 1431;
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
            tags);
    Service service = new Service(ImmutableList.of(span1));
    return JsonUtil.writeJsonArray(JsonUtil.writeAsString(service));
  }
}
