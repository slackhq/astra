package com.slack.kaldb.zipkinApi;

import java.util.List;

public class Trace {

  private final List<Span> trace;
  private final String traceId;

  public Trace(List<Span> trace, String traceId) {
    this.trace = trace;
    this.traceId = traceId;
  }

  public List<Span> getTrace() {
    return trace;
  }
}
