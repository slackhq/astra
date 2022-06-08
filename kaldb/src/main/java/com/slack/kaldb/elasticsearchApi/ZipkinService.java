package com.slack.kaldb.elasticsearchApi;

import static com.slack.kaldb.util.JsonUtil.writeJsonValues;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.linecorp.armeria.server.annotation.*;
import com.slack.kaldb.elasticsearchApi.searchResponse.*;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import com.slack.kaldb.util.JsonUtil;
import java.io.IOException;
import java.util.*;

/**
 * Elasticsearch compatible API service, for use in Grafana
 *
 * @see <a href="https://zipkin.io/zipkin-api/#/">Zipkin API</a>
 */
@SuppressWarnings(
    "OptionalUsedAsFieldOrParameterType") // Per https://armeria.dev/docs/server-annotated-service/
public class ZipkinService {
  private final KaldbServiceGrpc.KaldbServiceImplBase searcher;

  public ZipkinService(KaldbServiceGrpc.KaldbServiceImplBase searcher) {
    this.searcher = searcher;
  }

  @JsonFormat(shape = JsonFormat.Shape.ARRAY)
  public static final class GetServicesResult {

    private final List<String> services;

    public GetServicesResult(List<String> services) {
      this.services = services;
    }

    @JsonProperty("services")
    public List<String> services() {
      return services;
    }
  }

  public static final class Service {
    private final List<Span> spanNames;

    public Service(List<Span> spanNames) {
      this.spanNames = spanNames;
    }

    public List<Span> getSpanNames() {
      return spanNames;
    }
  }

  public static final class Span {
    private final String id;
    private final String traceId;
    private final String parentId;
    private final String name;
    private final long timestamp;
    private final int duration;
    private final String kind;
    private final HashMap<String, Object> localEndpoint;
    private final HashMap<String, Object> remoteEndpoint;
    private final HashMap<String, String> tags;

    public Span(
        String id,
        String traceId,
        String parentId,
        String name,
        long timestamp,
        int duration,
        String kind,
        HashMap<String, Object> localEndpoint,
        HashMap<String, Object> remoteEndpoint,
        HashMap<String, String> tags) {
      this.id = id;
      this.traceId = traceId;
      this.parentId = parentId;
      this.name = name;
      this.timestamp = timestamp;
      this.duration = duration;
      this.kind = kind;
      this.localEndpoint = localEndpoint;
      this.remoteEndpoint = remoteEndpoint;
      this.tags = tags;
    }

    public String getId() {
      return this.id;
    }

    public String getTraceId() {
      return this.traceId;
    }

    public String getParentId() {
      return this.parentId;
    }

    public String getName() {
      return this.name;
    }

    public long getTimestamp() {
      return this.timestamp;
    }

    public int getDuration() {
      return this.duration;
    }

    public String getKind() {
      return this.kind;
    }

    public HashMap<String, Object> getLocalEndpoint() {
      return this.localEndpoint;
    }

    public HashMap<String, Object> getRemoteEndpoint() {
      return this.remoteEndpoint;
    }

    public HashMap<String, String> getTags() {
      return this.tags;
    }
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
    GetServicesResult results = new GetServicesResult(ImmutableList.of("s1", "s2"));
    return JsonUtil.writeJsonValues(JsonUtil.writeAsString(results.services));
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
    return JsonUtil.writeJsonValues(JsonUtil.writeAsString(service));
  }
}
