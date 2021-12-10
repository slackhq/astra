package com.slack.kaldb.histogram;

import static com.slack.kaldb.util.ArgValidationUtils.ensureTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** This class contains an implementation of the histogram with fixed interval buckets. */
public class FixedIntervalHistogramImpl implements Histogram {

  private final double low;
  private final double high;
  private final int bucketCount;
  /**
   * Once the histogram is initialized, the buckets can't be changed since we rely on insertion
   * order of buckets for the findBucket function.
   */
  // TODO: Make the list immutable?
  private final ArrayList<HistogramBucket> buckets;
  // Count the number of elements in the histogram.
  private double count;

  public FixedIntervalHistogramImpl(double low, double high, int bucketCount) {
    ensureTrue(bucketCount > 0, "Bucket count should be a positive number");
    ensureTrue(low >= 0, "Low value should at least be zero.");
    ensureTrue(high > low, "High value should be larger than low value.");
    this.low = low;
    this.high = high;
    this.bucketCount = bucketCount;
    this.count = 0;
    this.buckets = makeHistogram(low, high, bucketCount);
  }

  /** Make a histogram where the width of each bucket is (last-first)/bucketCount */
  public static ArrayList<HistogramBucket> makeHistogram(
      double first, double last, int bucketCount) {
    ensureTrue(last > first, "last value is greater than first value.");

    if (bucketCount == 0) return new ArrayList<>();

    double width = (last - first) / bucketCount;
    ArrayList<HistogramBucket> buckets = new ArrayList<>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      buckets.add(
          new HistogramBucket(
              first + (width * i), (i == bucketCount - 1) ? last : first + (width * (i + 1))));
    }
    return buckets;
  }

  @Override
  public void add(long value) {
    // The histogram contains inclusive ranges but the buckets don't. So, make an exception for
    // high value and count it towards the last bucket.
    if (value == high) {
      buckets.get(bucketCount - 1).increment(1);
    } else {
      int bucketIdx = findMatchingBucket(buckets, 0, bucketCount, (double) value);
      buckets.get(bucketIdx).increment(1);
    }
    count++;
  }

  @Override
  public void mergeHistogram(List<HistogramBucket> mergeBuckets) {
    // In the current use case, all histograms are of the same size, so this case shouldn't happen
    // outside of tests.
    if (mergeBuckets.size() > buckets.size()) {
      throw new IllegalArgumentException(
          "The histogram being merged should be smaller than this histogram");
    }

    for (HistogramBucket mergeBucket : mergeBuckets) {
      Optional<HistogramBucket> localBucket = findMatchingBucket(mergeBucket);
      if (localBucket.isPresent()) {
        double additionalCount = mergeBucket.getCount();
        localBucket.get().increment(additionalCount);
        count += additionalCount;
      } else {
        throw new IllegalArgumentException(
            "The input histogram buckets should match. No matching bucket found for: "
                + mergeBucket.toString());
      }
    }
  }

  private Optional<HistogramBucket> findMatchingBucket(HistogramBucket matchingBucket) {
    for (HistogramBucket bucket : buckets) {
      if (bucket.getHigh() == matchingBucket.getHigh()
          && bucket.getLow() == matchingBucket.getLow()) {
        return Optional.of(bucket);
      }
    }
    return Optional.empty();
  }

  /**
   * Runs a binary search to find the correct bucket for the value being inserted and return it's
   * index. Binary search works since ArrayList preserves the order of the elements inserted.
   */
  private int findMatchingBucket(
      ArrayList<HistogramBucket> buckets, int start, int end, double value) {
    if (start == end) {
      throw new IllegalStateException(
          String.format("Value out of bounds for histogram (%f %f %f)", value, low, high));
    } else {
      int mid = start + ((end - start - 1) / 2);
      int valueComparator = buckets.get(mid).compareTo(value);
      if (valueComparator < 0) return findMatchingBucket(buckets, mid + 1, end, value);
      if (valueComparator > 0) return findMatchingBucket(buckets, start, mid, value);
      return mid;
    }
  }

  @Override
  public List<HistogramBucket> getBuckets() {
    return buckets;
  }

  @Override
  public double count() {
    return count;
  }
}
