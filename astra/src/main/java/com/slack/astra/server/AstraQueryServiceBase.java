package com.slack.astra.server;

import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AstraQueryServiceBase extends AstraServiceGrpc.AstraServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(AstraQueryServiceBase.class);

  @Override
  public void search(
      AstraSearch.SearchRequest request,
      StreamObserver<AstraSearch.SearchResult> responseObserver) {
    try {
      responseObserver.onNext(doSearch(request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error completing search request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  @Override
  public void schema(
      AstraSearch.SchemaRequest request,
      StreamObserver<AstraSearch.SchemaResult> responseObserver) {
    try {
      responseObserver.onNext(getSchema(request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      LOG.error("Error completing schema request", e);
      responseObserver.onError(Status.UNKNOWN.withDescription(e.getMessage()).asException());
    }
  }

  public abstract AstraSearch.SearchResult doSearch(AstraSearch.SearchRequest request) throws InterruptedException;

  public abstract AstraSearch.SchemaResult getSchema(AstraSearch.SchemaRequest request);
}
