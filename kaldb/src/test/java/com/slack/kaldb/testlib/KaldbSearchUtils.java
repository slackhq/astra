package com.slack.kaldb.testlib;

import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.kaldb.logstore.LogMessage;
import com.slack.kaldb.logstore.search.aggregations.DateHistogramAggBuilder;
import com.slack.kaldb.proto.service.KaldbSearch;
import com.slack.kaldb.proto.service.KaldbServiceGrpc;
import java.util.concurrent.Executors;

public class KaldbSearchUtils {

  public static KaldbSearch.SearchResult searchUsingGrpcApi(
      String queryString, int port, long startTime, long endTime, String interval) {
    KaldbServiceGrpc.KaldbServiceBlockingStub kaldbService =
        GrpcClients.builder(uri(port))
            .build(KaldbServiceGrpc.KaldbServiceBlockingStub.class)
            .withExecutor(Executors.newVirtualThreadPerTaskExecutor())
            .withCompression("gzip");

    return kaldbService.search(
        KaldbSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setQueryString(queryString)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setHowMany(100)
            .setAggregations(
                KaldbSearch.SearchRequest.SearchAggregation.newBuilder()
                    .setType(DateHistogramAggBuilder.TYPE)
                    .setName("1")
                    .setValueSource(
                        KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                            .newBuilder()
                            .setField(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
                            .setDateHistogram(
                                KaldbSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
                                    .DateHistogramAggregation.newBuilder()
                                    .setMinDocCount(1)
                                    .setInterval(interval)
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }
}
