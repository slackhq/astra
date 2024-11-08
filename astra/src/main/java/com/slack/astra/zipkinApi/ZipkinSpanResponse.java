package com.slack.astra.zipkinApi;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

/**
 * Span response object for Zipkin API
 *
 * @see <a href="https://zipkin.io/zipkin-api/#/">Zipkin API Spec</a>
 */
@SuppressWarnings("unused")
public class ZipkinSpanResponse {
  private final String id;
  private final String traceId;
  private String parentId = null;
  private String name = null;

  @JsonProperty("timestamp")
  private Long timestampMicros = null;

  private ZipkinEndpointResponse localEndpoint = null;

  private ZipkinEndpointResponse remoteEndpoint = null;

  @JsonProperty("duration")
  // Zipkin spec defines this is integer64, so long seems to be more appropriate
  private long durationMicros;

  private String kind;

  private Map<String, String> tags;

  public ZipkinSpanResponse(String id, String traceId) {
    // id and traceId are only required fields
    this.id = id;
    this.traceId = traceId;
  }

  public void setParentId(String parentId) {
    this.parentId = parentId;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setTimestamp(long timestampMicros) {
    this.timestampMicros = timestampMicros;
  }

  public void setDuration(long durationMicros) {
    this.durationMicros = durationMicros;
  }

  public void setKind(String kind) {
    this.kind = kind;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public ZipkinEndpointResponse getLocalEndpoint() {
    return localEndpoint;
  }

  public void setLocalEndpoint(ZipkinEndpointResponse localEndpoint) {
    this.localEndpoint = localEndpoint;
  }

  public ZipkinEndpointResponse getRemoteEndpoint() {
    return remoteEndpoint;
  }

  public void setRemoteEndpoint(ZipkinEndpointResponse remoteEndpoint) {
    this.remoteEndpoint = remoteEndpoint;
  }

  public String getId() {
    return id;
  }

  public String getTraceId() {
    return traceId;
  }

  public String getParentId() {
    return parentId;
  }

  public String getName() {
    return name;
  }

  public long getTimestamp() {
    return timestampMicros;
  }

  public long getDuration() {
    return durationMicros;
  }

  public String getKind() {
    return kind;
  }

  public Map<String, String> getTags() {
    return tags;
  }
}
