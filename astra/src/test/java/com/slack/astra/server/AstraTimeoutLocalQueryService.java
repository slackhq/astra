package com.slack.astra.server;

import com.slack.astra.logstore.search.AstraLocalQueryService;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AstraTimeoutLocalQueryService extends AstraServiceGrpc.AstraServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(AstraTimeoutLocalQueryService.class);

  private final AstraLocalQueryService astraLocalQueryService;
  private final int waitMS;

  public AstraTimeoutLocalQueryService(AstraLocalQueryService astraLocalQueryService, int waitMS) {
    this.astraLocalQueryService = astraLocalQueryService;
    this.waitMS = waitMS;
  }

  @Override
  public void search(
      AstraSearch.SearchRequest request,
      StreamObserver<AstraSearch.SearchResult> responseObserver) {
    try {
      Thread.sleep(waitMS);
    } catch (InterruptedException e) {
      LOG.warn("Pause interrupted" + e);
    }
    astraLocalQueryService.search(request, responseObserver);
  }
}
