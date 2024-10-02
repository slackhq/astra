package com.slack.astra.testlib;

import static com.slack.astra.util.AggregatorJSONUtil.createGenericDateHistogramJSONBlob;

import com.linecorp.armeria.client.grpc.GrpcClients;
import com.slack.astra.logstore.LogMessage;
import com.slack.astra.proto.service.AstraSearch;
import com.slack.astra.proto.service.AstraServiceGrpc;

public class AstraSearchUtils {

  public static AstraSearch.SearchResult searchUsingGrpcApi(
      String queryString, int port, long startTime, long endTime, String interval) {
    AstraServiceGrpc.AstraServiceBlockingStub astraService =
        GrpcClients.builder(uri(port))
            .build(AstraServiceGrpc.AstraServiceBlockingStub.class)
            .withCompression("gzip");

    String query =
        "{\"bool\":{\"filter\":[{\"range\":{\"_timesinceepoch\":{\"gte\":%d,\"lte\":%d,\"format\":\"epoch_millis\"}}},{\"query_string\":{\"analyze_wildcard\":true,\"query\":\"%s\"}}]}}"
            .formatted(startTime, endTime, queryString);

    return astraService.search(
        AstraSearch.SearchRequest.newBuilder()
            .setDataset(MessageUtil.TEST_DATASET_NAME)
            .setQuery(query)
            .setStartTimeEpochMs(startTime)
            .setEndTimeEpochMs(endTime)
            .setHowMany(100)
            .setAggregationJson(
                createGenericDateHistogramJSONBlob(
                    "1", LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName, interval, 1))
            .build());
  }

  private static String uri(int port) {
    return "gproto+http://127.0.0.1:" + port + '/';
  }
}
