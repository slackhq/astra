package com.slack.kaldb.histogram;

import com.slack.kaldb.logstore.search.AggregationDefinition;
import java.io.IOException;
import java.util.List;
import org.apache.lucene.index.NumericDocValues;

/**
 * An interface that implements a histogram in kaldb. The histogram returns the results in kaldb.
 */
public interface Histogram {

  /** Adds a timestamp value to the histogram. */
  void addTimestamp(long value);

  void addDocument(int doc, NumericDocValues docValues, NumericDocValues[] docValuesForAggs)
      throws IOException;

  void mergeHistogram(List<HistogramBucket> mergeBuckets);

  /** Get the histogram distribution. */
  List<HistogramBucket> getBuckets();

  // Return count of number of values in histogram.
  long count();

  List<AggregationDefinition> getAggregations();
}
