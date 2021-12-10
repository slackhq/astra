package com.slack.kaldb.histogram;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class HistogramBucketTest {

  @Test
  public void testInit() {
    HistogramBucket bucket = new HistogramBucket(0d, 1.1d);
    assertThat(bucket.getLow()).isEqualTo(0d);
    assertThat(bucket.getHigh()).isEqualTo(1.1d);
    assertThat(bucket.getCount()).isEqualTo(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadInit() {
    new HistogramBucket(2.0d, 1.1d);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadInitOnEqualValues() {
    new HistogramBucket(2.0d, 2.0d);
  }

  @Test
  public void testSimpleIncrementAndBoundaryTest() {
    HistogramBucket bucket = new HistogramBucket(0d, 1d);
    assertThat(bucket.getLow()).isEqualTo(0d);
    assertThat(bucket.getHigh()).isEqualTo(1d);
    bucket.increment(5);
    assertThat(bucket.getCount()).isEqualTo(5);
    bucket.increment(1);
    assertThat(bucket.getCount()).isEqualTo(6);
    assertThat(bucket.getLow()).isEqualTo(0d);
    assertThat(bucket.getHigh()).isEqualTo(1d);
  }

  @Test
  public void testContains() {
    HistogramBucket b = new HistogramBucket(1.0d, 3.0d);
    assertThat(b.contains(1.0d)).isTrue();
    assertThat(b.contains(3.0d)).isFalse();
    assertThat(b.contains(3.1d)).isFalse();
    assertThat(b.contains(0.1d)).isFalse();
    assertThat(b.contains(2.0d)).isTrue();
  }

  @Test
  public void testOverlappingBuckets() {
    HistogramBucket b1 = new HistogramBucket(0d, 0.1d);
    HistogramBucket b2 = new HistogramBucket(0.1d, 0.2d);
    HistogramBucket b3 = new HistogramBucket(0.15d, 0.25d);
    HistogramBucket b4 = new HistogramBucket(0.1d, 0.11d);
    HistogramBucket b5 = new HistogramBucket(0.19d, 0.2d);

    // Compare same bucket
    assertThat(b1).isEqualByComparingTo(b1);
    assertThat(b2).isEqualByComparingTo(b2);
    assertThat(b3).isEqualByComparingTo(b3);

    // Adjacent buckets
    assertThat(b1).isLessThan(b2);
    assertThat(b2).isGreaterThan(b1);

    // Non-overlapping buckets.
    assertThat(b1).isLessThan(b3);
    assertThat(b3).isGreaterThan(b1);

    // Overlapping buckets.
    assertThat(b2).isEqualByComparingTo(b3);
    assertThat(b2).isGreaterThanOrEqualTo(b3);
    assertThat(b3).isEqualByComparingTo(b2);
    assertThat(b3).isLessThanOrEqualTo(b2);

    // Overlapping interval start
    assertThat(b2).isEqualByComparingTo(b4);
    assertThat(b2).isGreaterThanOrEqualTo(b4);
    assertThat(b4).isEqualByComparingTo(b2);
    assertThat(b4).isLessThanOrEqualTo(b2);

    // Overlapping interval ends
    assertThat(b2).isEqualByComparingTo(b5);
    assertThat(b2).isGreaterThanOrEqualTo(b5);
    assertThat(b5).isEqualByComparingTo(b2);
    assertThat(b5).isLessThanOrEqualTo(b2);
  }
}
