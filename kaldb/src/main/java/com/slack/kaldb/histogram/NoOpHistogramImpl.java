package com.slack.kaldb.histogram;

import java.util.Collections;
import java.util.List;

public class NoOpHistogramImpl implements Histogram {
  private double count;

  public NoOpHistogramImpl() {
    count = 0;
  }

  @Override
  public void add(long value) {
    count++;
  }

  @Override
  public void mergeHistogram(List<HistogramBucket> mergeBuckets) {
    for (HistogramBucket bucket : mergeBuckets) {
      count += bucket.getCount();
    }
  }

  @Override
  public List<HistogramBucket> getBuckets() {
    return Collections.emptyList();
  }

  @Override
  public double count() {
    return count;
  }
}
