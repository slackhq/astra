package com.slack.kaldb.testlib;

import com.linecorp.armeria.client.Clients;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;

public class KaldbGrpcQueryUtil {
  public static KaldbSearch.SearchResult searchUsingGrpcApi(
      String queryString,
      long startTimeMs,
      long endTimeMs,
      String indexName,
      int howMany,
      int bucketCount,
      int port) {
    KaldbServiceGrpc.KaldbServiceBlockingStub kaldbService =
        Clients.newClient(uri(port), KaldbServiceGrpc.KaldbServiceBlockingStub.class);

    return kaldbService.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setIndexName(indexName)
            .setQueryString(queryString)
            .setStartTimeEpochMs(startTimeMs)
            .setEndTimeEpochMs(endTimeMs)
            .setHowMany(howMany)
            .setBucketCount(bucketCount)
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }
}
