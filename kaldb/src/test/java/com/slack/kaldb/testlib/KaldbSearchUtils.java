package com.slack.kaldb.testlib;

import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;

public class KaldbSearchUtils {

  public static KaldbSearch.SearchResult searchUsingGrpcApi(
      String queryString, int port, long startTime, long endTime) {
    KaldbServiceGrpc.KaldbServiceBlockingStub kaldbService =
        GrpcClients.builder(uri(port))
            .build(KaldbServiceGrpc.KaldbServiceBlockingStub.class)
            .withCompression("gzip");

    return kaldbService.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setQueryString(queryString)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setLimit(100)
            .setBucketCount(2)
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }
}
