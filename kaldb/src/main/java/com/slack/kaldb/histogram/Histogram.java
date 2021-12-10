package com.slack.kaldb.histogram;

import java.util.List;

/**
 * An interface that implements a histogram in kaldb. The histogram returns the results in kaldb.
 */
public interface Histogram {
  // Add a value to the histogram?
  void add(long value);

  void mergeHistogram(List<HistogramBucket> mergeBuckets);

  /** Get the histogram distribution. */
  List<HistogramBucket> getBuckets();

  // Return count of number of values in histogram.
  double count();
}
