package com.slack.kaldb.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.proto.service.KaldbSearch;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class SearchResultUtilsTest {

  @Test
  public void testComplexResponseAggregationConversion() {

    KaldbSearch.ResponseBucketValue responseBucketValue1 =
        KaldbSearch.ResponseBucketValue.newBuilder().setValue(1001L).build();
    KaldbSearch.ResponseBucketResult responseBucketInner1 =
        KaldbSearch.ResponseBucketResult.newBuilder().setValue(responseBucketValue1).build();

    KaldbSearch.ResponseBucketValue responseBucketValue2 =
        KaldbSearch.ResponseBucketValue.newBuilder().setValue(1002L).build();

    KaldbSearch.ResponseBucketResult responseBucketInner2 =
        KaldbSearch.ResponseBucketResult.newBuilder().setValue(responseBucketValue2).build();

    KaldbSearch.Value key2 = KaldbSearch.Value.newBuilder().setStringValue("key2").build();
    List<KaldbSearch.Value> keys2 = new ArrayList<>();
    keys2.add(key2);
    KaldbSearch.ResponseBuckets responseBucket2 =
        KaldbSearch.ResponseBuckets.newBuilder()
            .setDocCount(3)
            .addAllKey(keys2)
            .putAllValues(Map.of("foo1", responseBucketInner2))
            .build();

    KaldbSearch.ResponseAggregation innerResponseAggregation =
        KaldbSearch.ResponseAggregation.newBuilder()
            .setName("innerName")
            .setSumOtherDocCount(4)
            .setDocCountErrorUpperBound(5)
            .addAllBuckets(List.of(responseBucket2))
            .build();

    KaldbSearch.ResponseBucketResult responseBucketInner3 =
        KaldbSearch.ResponseBucketResult.newBuilder()
            .setAggregation(innerResponseAggregation)
            .build();
    Map<String, KaldbSearch.ResponseBucketResult> responseBucketValues =
        Map.of("example1", responseBucketInner1, "example3", responseBucketInner3);

    KaldbSearch.Value key = KaldbSearch.Value.newBuilder().setStringValue("key").build();
    List<KaldbSearch.Value> keys = new ArrayList<>();
    keys.add(key);
    KaldbSearch.ResponseBuckets responseBucket =
        KaldbSearch.ResponseBuckets.newBuilder()
            .setDocCount(3)
            .addAllKey(keys)
            .putAllValues(responseBucketValues)
            .build();

    List<KaldbSearch.ResponseBuckets> responseBucketsList = new ArrayList<>();
    responseBucketsList.add(responseBucket);

    KaldbSearch.ResponseAggregation responseAggregation =
        KaldbSearch.ResponseAggregation.newBuilder()
            .setName("name")
            .setSumOtherDocCount(1)
            .setDocCountErrorUpperBound(2)
            .addAllBuckets(responseBucketsList)
            .build();

    ResponseAggregation responseAggregationFromProto =
        SearchResultUtils.fromProtoResponseAggregations(List.of(responseAggregation)).get(0);
    KaldbSearch.ResponseAggregation responseAggregationBackToProto =
        SearchResultUtils.toResponseAggregationProto(List.of(responseAggregationFromProto)).get(0);

    assertThat(responseAggregation).isEqualTo(responseAggregationBackToProto);
  }
}
