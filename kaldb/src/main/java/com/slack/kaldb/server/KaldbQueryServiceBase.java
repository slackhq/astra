package com.slack.kaldb.server;

import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KaldbQueryServiceBase extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbQueryServiceBase.class);

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    // There is a nuance between handle vs handleAsync/whenCompleteAsync
    // handleAsync/whenCompleteAsync will cause the callback to be invoked from Java's default
    // fork-join pool
    doSearch(request)
        .handle(
            (result, t) -> {
              if (t != null) {
                LOG.error("Error completing the future", t);
                responseObserver.onError(t);
              } else {
                responseObserver.onNext(result);
                responseObserver.onCompleted();
              }
              return null;
            });
  }

  public abstract CompletableFuture<KaldbSearch.SearchResult> doSearch(
      KaldbSearch.SearchRequest request);
}
