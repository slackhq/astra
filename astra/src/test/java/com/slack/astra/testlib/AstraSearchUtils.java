// TODO FOR KYLE: FIX
//package com.slack.astra.testlib;
//
//import com.linecorp.armeria.client.grpc.GrpcClients;
//import com.slack.astra.logstore.LogMessage;
//import com.slack.astra.logstore.search.aggregations.DateHistogramAggBuilder;
//import com.slack.astra.proto.service.AstraSearch;
//import com.slack.astra.proto.service.AstraServiceGrpc;
//
//public class AstraSearchUtils {
//
//  public static AstraSearch.SearchResult searchUsingGrpcApi(
//      String queryString, int port, long startTime, long endTime, String interval) {
//    AstraServiceGrpc.AstraServiceBlockingStub astraService =
//        GrpcClients.builder(uri(port))
//            .build(AstraServiceGrpc.AstraServiceBlockingStub.class)
//            .withCompression("gzip");
//
//    return astraService.search(
//        AstraSearch.SearchRequest.newBuilder()
//            .setDataset(MessageUtil.TEST_DATASET_NAME)
//            .setQueryString(queryString)
//            .setStartTimeEpochMs(startTime)
//            .setEndTimeEpochMs(endTime)
//            .setHowMany(100)
//            .setAggregations(
//                AstraSearch.SearchRequest.SearchAggregation.newBuilder()
//                    .setType(DateHistogramAggBuilder.TYPE)
//                    .setName("1")
//                    .setValueSource(
//                        AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
//                            .newBuilder()
//                            .setField(LogMessage.SystemField.TIME_SINCE_EPOCH.fieldName)
//                            .setDateHistogram(
//                                AstraSearch.SearchRequest.SearchAggregation.ValueSourceAggregation
//                                    .DateHistogramAggregation.newBuilder()
//                                    .setMinDocCount(1)
//                                    .setInterval(interval)
//                                    .build())
//                            .build())
//                    .build())
//            .build());
//  }
//
//  private static String uri(int port) {
//    return "gproto+http://127.0.0.1:" + port + '/';
//  }
//}
