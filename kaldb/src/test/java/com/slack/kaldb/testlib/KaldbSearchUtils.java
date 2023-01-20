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
            .setHowMany(100)
            .setAggs(setBucketCount(2, startTime, endTime))
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }

  public static KaldbSearch.SearchAggregation setBucketCount(
      int bucketCount, long startTimeEpochMs, long endTimeEpochMs) {
    if (bucketCount == 0) {
      return KaldbSearch.SearchAggregation.newBuilder().build();
    }

    return KaldbSearch.SearchAggregation.newBuilder()
        .setName("1")
        .setType("date_histogram")
        .setMetadata(
            KaldbSearch.Struct.newBuilder()
                .putFields(
                    "interval",
                    KaldbSearch.Value.newBuilder()
                        .setStringValue(
                            (endTimeEpochMs - startTimeEpochMs) / (bucketCount * 1000L) + "S")
                        .build())
                .putFields(
                    "extended_bounds",
                    KaldbSearch.Value.newBuilder()
                        .setStructValue(
                            KaldbSearch.Struct.newBuilder()
                                .putFields(
                                    "min",
                                    KaldbSearch.Value.newBuilder()
                                        .setLongValue(startTimeEpochMs)
                                        .build())
                                .putFields(
                                    "max",
                                    KaldbSearch.Value.newBuilder()
                                        .setLongValue(endTimeEpochMs)
                                        .build())
                                .build())
                        .build())
                .build())
        .build();
  }
}
