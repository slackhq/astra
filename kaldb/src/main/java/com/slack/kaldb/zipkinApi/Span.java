package com.slack.kaldb.zipkinApi;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.HashMap;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Span {

  private final String id;
  private final String traceId;
  private final String parentId;
  private final String name;
  private final long timestamp;
  private final long duration;
  private final String kind;
  private final HashMap<String, Object> localEndpoint;
  private final HashMap<String, Object> remoteEndpoint;

  private final List<HashMap<String, Object>> annotations;
  private final HashMap<String, String> tags;

  public Span(
      String id,
      String traceId,
      String parentId,
      String name,
      long timestamp,
      long duration,
      String kind,
      HashMap<String, Object> localEndpoint,
      HashMap<String, Object> remoteEndpoint,
      List<HashMap<String, Object>> annotations,
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
    this.annotations = annotations;
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

  public long getDuration() {
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

  public List<HashMap<String, Object>> getAnnotations() {
    return this.annotations;
  }

  public HashMap<String, String> getTags() {
    return this.tags;
  }
}
