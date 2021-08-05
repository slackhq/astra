package com.slack.kaldb.server;

import com.slack.kaldb.logstore.search.KaldbLocalQueryService;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbTimeoutLocalQueryService extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbTimeoutLocalQueryService.class);

  private final KaldbLocalQueryService kaldbLocalQueryService;
  private final int waitMS;

  public KaldbTimeoutLocalQueryService(KaldbLocalQueryService kaldbLocalQueryService, int waitMS) {
    this.kaldbLocalQueryService = kaldbLocalQueryService;
    this.waitMS = waitMS;
  }

  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {
    try {
      LOG.warn(String.format("Delaying search by %s ms", waitMS));
      Thread.sleep(waitMS);
    } catch (InterruptedException e) {
      LOG.warn("Pause interrupted" + e);
    }
    kaldbLocalQueryService.search(request, responseObserver);
  }
}
