package com.slack.kaldb.server;

import com.linecorp.armeria.client.Clients;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KaldbQueryService extends KaldbServiceGrpc.KaldbServiceImplBase {

  private static final Logger LOG = LoggerFactory.getLogger(KaldbQueryService.class);

  public static List<String> servers = new ArrayList<>();

  /**
   * TODO once we have ZK integration we would have the servers variable be 1. a list 2. update when
   * the ZK list entry changes 3. Figure out how in SearchRequest can we pass in the list of servers
   * that we actually need to query on 4. Cache the stub as it creates a gRPC channel which is
   * supposed to be reused
   */
  public KaldbSearch.SearchResult distributedSearch(KaldbSearch.SearchRequest request) {
    for (String server : servers) {
      KaldbServiceGrpc.KaldbServiceBlockingStub stub =
          Clients.newClient(server, KaldbServiceGrpc.KaldbServiceBlockingStub.class);
      return stub.search(request);
    }
    throw new RuntimeException("no live servers");
  }

  @Override
  public void search(
      KaldbSearch.SearchRequest request,
      StreamObserver<KaldbSearch.SearchResult> responseObserver) {

    KaldbSearch.SearchResult protoSearchResult = distributedSearch(request);
    responseObserver.onNext(protoSearchResult);
    responseObserver.onCompleted();
  }
}
