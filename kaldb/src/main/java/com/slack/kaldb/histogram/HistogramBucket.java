package com.slack.kaldb.histogram;

import com.google.common.base.Objects;
import com.slack.kaldb.logstore.search.AggregationDefinition;
import com.slack.kaldb.logstore.search.ComposableCollector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * A HistogramBucket represents one bucket in the histogram. The range for the bucket is [low,
 * high). The bucket keeps a count of the objects in histogram as well as any aggregations performed
 * on the bucket.
 */
public class HistogramBucket implements Comparable<HistogramBucket> {
  private final double low;
  private final double high;

  private double count;

  private final List<ComposableCollector> aggregationCollectors;

  public HistogramBucket(double low, double high, double count) {
    this(low, high, count, List.of());
  }

  public HistogramBucket(double low, double high, double count, List<AggregationDefinition> aggs) {
    if (low >= high) {
      throw new IllegalArgumentException(
          String.format("The low %s should be higher than high %s", low, high));
    }
    this.low = low;
    this.high = high;
    this.count = count;
    this.aggregationCollectors = new ArrayList<>(aggs.size());
    try {
      for (AggregationDefinition agg : aggs) {
        aggregationCollectors.add((ComposableCollector) agg.getCollectorManager().newCollector());
      }
    } catch (IOException e) {
      // todo: handle exception
    }
  }

  public HistogramBucket(double low, double high) {
    this(low, high, 0);
  }

  public void increment(double incr) {
    this.count += incr;
  }

  public boolean hasOverlap(HistogramBucket bucket) {
    return bucket.high > low && bucket.low < high;
  }

  public boolean contains(double value) {
    return value >= low && value < high;
  }

  public double getLow() {
    return low;
  }

  public double getHigh() {
    return high;
  }

  public double getCount() {
    return count;
  }

  public List<ComposableCollector> getaggregationCollectors() {
    return aggregationCollectors;
  }

  public void mergeAggregations(HistogramBucket otherBucket) {
    for (int k = 0; k < aggregationCollectors.size(); k++) {
      aggregationCollectors.get(k).merge(otherBucket.aggregationCollectors.get(k));
    }
  }

  @Override
  public int compareTo(@Nonnull HistogramBucket bucket) {
    if (hasOverlap(bucket)) return 0;
    else if (bucket.high <= low) return 1;
    else return -1;
  }

  public int compareTo(double value) {
    if (value < low) return 1;
    else if (value >= high) return -1;
    else return 0;
  }

  public String toString() {
    return String.format("HistogramBucket low:%f, high:%f, count:%f", low, high, count);
  }
  // TODO: Consider adding "overlap" projection for merge?

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HistogramBucket that = (HistogramBucket) o;
    return Double.compare(that.low, low) == 0
        && Double.compare(that.high, high) == 0
        && Double.compare(that.count, count) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(low, high, count);
  }
}
