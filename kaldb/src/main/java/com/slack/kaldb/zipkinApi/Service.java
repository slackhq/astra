package com.slack.kaldb.zipkinApi;

import java.util.List;

public class Service {
  private final List<Span> spanNames;

  public Service(List<Span> spanNames) {
    this.spanNames = spanNames;
  }

  public List<Span> getSpanNames() {
    return spanNames;
  }
}
