package com.slack.kaldb.logstore.search;

import java.util.List;

public class ResponseAggregation {

  private String name;
  private long docCountErrorUpperBound;
  private long sumOtherDocCount;

  private List<ResponseBucket> responseBuckets;

  public ResponseAggregation(
      String name,
      long docCountErrorUpperBound,
      long sumOtherDocCount,
      List<ResponseBucket> responseBuckets) {
    this.name = name;
    this.docCountErrorUpperBound = docCountErrorUpperBound;
    this.sumOtherDocCount = sumOtherDocCount;
    this.responseBuckets = responseBuckets;
  }

  public String getName() {
    return name;
  }

  public long getDocCountErrorUpperBound() {
    return docCountErrorUpperBound;
  }

  public long getSumOtherDocCount() {
    return sumOtherDocCount;
  }

  public List<ResponseBucket> getResponseBuckets() {
    return responseBuckets;
  }
}
