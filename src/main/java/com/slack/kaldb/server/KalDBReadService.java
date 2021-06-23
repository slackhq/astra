package com.slack.kaldb.server;

import com.linecorp.armeria.client.Clients;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KalDBReadService extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KalDBReadService.class);

  public static String servers = "gproto+http://127.0.0.1:8080/";

  /**
   * TODO once we have ZK integration we would have the servers variable be 1. a list 2. update when
   * the ZK list entry changes 3. Figure out how in SearchRequest can we pass in the list of servers
   * that we actually need to query on 4. Cache the stub as it creates a gRPC channel which is
   * supposed to be reused
   */
  public KaldbSearch.SearchResult distribSearch(KaldbSearch.SearchRequest request) {
    KaldbServiceGrpc.KaldbServiceBlockingStub stub =
        Clients.newClient(servers, KaldbServiceGrpc.KaldbServiceBlockingStub.class);
    KaldbSearch.SearchResult result = stub.search(request);
    return result;
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    KaldbSearch.SearchResult protoSearchResult = distribSearch(request);
    responseObserver.onNext(protoSearchResult);
    responseObserver.onCompleted();
  }
}
