package com.slack.kaldb.logstore.search;

import java.util.ArrayList;
import java.util.List;

public class ResponseAggregation {

  private String name;
  private long docCountErrorUpperBound;
  private long sumOtherDocCount;

  private List<ResponseBucket> responseBuckets = new ArrayList<>();

  public ResponseAggregation() {
    this.name = "";
    this.docCountErrorUpperBound = 0;
    this.sumOtherDocCount = 0;
  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ResponseAggregation that = (ResponseAggregation) o;

    if (docCountErrorUpperBound != that.docCountErrorUpperBound) return false;
    if (sumOtherDocCount != that.sumOtherDocCount) return false;
    if (!name.equals(that.name)) return false;
    return responseBuckets.equals(that.responseBuckets);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (int) (docCountErrorUpperBound ^ (docCountErrorUpperBound >>> 32));
    result = 31 * result + (int) (sumOtherDocCount ^ (sumOtherDocCount >>> 32));
    result = 31 * result + responseBuckets.hashCode();
    return result;
  }
}
