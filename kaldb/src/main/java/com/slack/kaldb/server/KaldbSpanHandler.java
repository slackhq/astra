package com.slack.kaldb.server;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;

/** Add common tags that we want to annotate on every request */
public class KaldbSpanHandler extends SpanHandler {

  @Override
  public boolean begin(TraceContext context, MutableSpan span, TraceContext parent) {
    span.tag("threadName", Thread.currentThread().getName());
    return true;
  }
}
