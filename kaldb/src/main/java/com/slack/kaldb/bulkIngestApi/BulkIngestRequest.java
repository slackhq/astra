package com.slack.kaldb.bulkIngestApi;

import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

/**
 * Wrapper object to enable building a bulk request and awaiting on an asynchronous response to be
 * populated. As this uses a synchronous queue internally, it expects a thread to already be waiting
 * on getResponse when setResponse is invoked with the result data.
 */
public class BulkIngestRequest {
  private final Map<String, List<Trace.Span>> inputDocs;
  private final SynchronousQueue<BulkIngestResponse> internalResponse = new SynchronousQueue<>();

  protected BulkIngestRequest(Map<String, List<Trace.Span>> inputDocs) {
    this.inputDocs = inputDocs;
  }

  Map<String, List<Trace.Span>> getInputDocs() {
    return inputDocs;
  }

  boolean setResponse(BulkIngestResponse response) {
    return internalResponse.offer(response);
  }

  public BulkIngestResponse getResponse() throws InterruptedException {
    return internalResponse.take();
  }
}
