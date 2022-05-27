package com.slack.kaldb.server;

import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KaldbQueryServiceBase extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbQueryServiceBase.class);

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    LOG.info(
        String.format("Search request received: '%s'", request.toString().replace("\n", ", ")));

    try {
      responseObserver.onNext(doSearch(request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error completing search request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  public abstract KaldbSearch.SearchResult doSearch(KaldbSearch.SearchRequest request);
}
