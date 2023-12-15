package com.slack.kaldb.bulkIngestApi;

import com.slack.service.murron.trace.Trace;
import java.util.List;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;

public class BatchRequest {
  private final Map<String, List<Trace.Span>> inputDocs;
  private final SynchronousQueue<BulkIngestResponse> internalResponse = new SynchronousQueue<>();

  protected BatchRequest(Map<String, List<Trace.Span>> inputDocs) {
    this.inputDocs = inputDocs;
  }

  Map<String, List<Trace.Span>> getInputDocs() {
    return inputDocs;
  }

  void setResponse(BulkIngestResponse response) {
    internalResponse.add(response);
  }

  public BulkIngestResponse getResponse() throws InterruptedException {
    return internalResponse.take();
  }
}
