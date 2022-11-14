package com.slack.kaldb.histogram;

import com.slack.kaldb.logstore.search.AggregationDefinition;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.NumericDocValues;

/** Represents the degenerate case for a histogram in which there is only a single bucket. */
public class SingleBucketHistogram implements Histogram {

  private final HistogramBucket singleBucket;
  private final List<AggregationDefinition> aggs;

  public SingleBucketHistogram() {
    singleBucket = new HistogramBucket(0, 1, 0);
    this.aggs = List.of();
  }

  public SingleBucketHistogram(List<AggregationDefinition> aggs) {
    singleBucket = new HistogramBucket(0, 1, 0, aggs);
    this.aggs = aggs;
  }

  @Override
  public void addTimestamp(long value) {
    singleBucket.increment(1);
  }

  @Override
  public void addDocument(int doc, NumericDocValues docValues, NumericDocValues[] docValuesForAggs)
      throws IOException {
    addTimestamp(0);
    for (int k = 0; k < docValuesForAggs.length; k++) {
      singleBucket.getaggregationCollectors().get(k).collect(docValuesForAggs[k], doc);
    }
  }

  @Override
  public void mergeHistogram(List<HistogramBucket> mergeBuckets) {
    for (HistogramBucket bucket : mergeBuckets) {
      singleBucket.increment(bucket.getCount());
    }
  }

  @Override
  public List<HistogramBucket> getBuckets() {
    return Collections.emptyList();
  }

  @Override
  public long count() {
    return (long) singleBucket.getCount();
  }

  @Override
  public List<AggregationDefinition> getAggregations() {
    return aggs;
  }
}
