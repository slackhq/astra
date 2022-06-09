package com.slack.kaldb.zipkinApi;

import java.util.HashMap;

public class Span {

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
