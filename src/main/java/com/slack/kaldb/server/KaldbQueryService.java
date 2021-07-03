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
   * TODO 1. Cache the stub 2. Run in parallel 3. Integrate with ZK to update list 4. Aggregate the
   * responses instead of only returning the result from the first server
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
