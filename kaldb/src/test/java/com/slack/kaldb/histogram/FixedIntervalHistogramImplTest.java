package com.slack.kaldb.histogram;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class FixedIntervalHistogramImplTest {

  @Test
  public void testSimpleHistogramOperations() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 5, 5);
    for (int i = 0; i < 5; i++) {
      h.addTimestamp(i);
    }

    assertThat(h.count()).isEqualTo(5);
    assertThat(h.getBuckets().size()).isEqualTo(5);

    // Buckets are stored in increasing order.
    int bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(1);
      bucketLow++;
    }

    for (int i = 0; i < 5; i++) {
      h.addTimestamp(i);
    }
    assertThat(h.count()).isEqualTo(10);
    assertThat(h.getBuckets().size()).isEqualTo(5);

    // Adding elements doesn't change buckets and their ordering.
    bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(2);
      bucketLow++;
    }
  }

  @Test
  public void testInsertionInFirstBucket() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 15, 15);
    for (int i = 0; i < 15; i++) {
      h.addTimestamp(0);
    }

    assertThat(h.count()).isEqualTo(15);
    assertThat(h.getBuckets().size()).isEqualTo(15);

    HistogramBucket firstBucket = h.getBuckets().get(0);
    assertThat(firstBucket.getCount()).isEqualTo(15);

    // Buckets are stored in increasing order.
    int bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(bucketLow == 0 ? 15 : 0);
      bucketLow++;
    }
  }

  @Test
  public void testSimpleHistogramMerge() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 10, 1);
    for (int i = 0; i < 10; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(10);
    assertThat(h1.getBuckets().size()).isEqualTo(1);

    HistogramBucket b1 = new HistogramBucket(0, 10);
    b1.increment(10);

    h1.mergeHistogram(List.of(b1));
    assertThat(h1.count()).isEqualTo(20);
    assertThat(h1.getBuckets().size()).isEqualTo(1);
  }

  @Test
  public void testLargeSmallHistogramMerge() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 10, 2);
    for (int i = 0; i < 10; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(10);
    assertThat(h1.getBuckets().size()).isEqualTo(2);

    HistogramBucket b1 = new HistogramBucket(0, 5);
    b1.increment(10);

    h1.mergeHistogram(List.of(b1));
    assertThat(h1.count()).isEqualTo(20);
    assertThat(h1.getBuckets().size()).isEqualTo(2);
    for (HistogramBucket b : h1.getBuckets()) {
      if (b.getLow() == 0) {
        assertThat(b.getCount()).isEqualTo(15);
      } else {
        assertThat(b.getCount()).isEqualTo(5);
      }
    }
  }

  @Test
  public void testSelfHistogramMerge() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 10, 1);
    for (int i = 0; i < 10; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(10);
    assertThat(h1.getBuckets().size()).isEqualTo(1);

    h1.mergeHistogram(h1.getBuckets());
    assertThat(h1.count()).isEqualTo(20);
    assertThat(h1.getBuckets().size()).isEqualTo(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailedSimpleHistogramMerge() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 10, 1);
    for (int i = 0; i < 10; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(10);
    assertThat(h1.getBuckets().size()).isEqualTo(1);

    HistogramBucket b1 = new HistogramBucket(0, 15);
    b1.increment(10);

    h1.mergeHistogram(List.of(b1));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeLargeIntoSmallHistogram() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 10, 1);
    for (int i = 0; i < 10; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(10);
    assertThat(h1.getBuckets().size()).isEqualTo(1);

    HistogramBucket b1 = new HistogramBucket(0, 10);
    b1.increment(10);
    HistogramBucket b2 = new HistogramBucket(10, 20);
    b2.increment(10);

    h1.mergeHistogram(List.of(b1, b2));
  }

  @Test
  public void testHistogramMerge() {
    FixedIntervalHistogramImpl h1 = new FixedIntervalHistogramImpl(0, 15, 15);
    for (int i = 0; i < 15; i++) {
      h1.addTimestamp(i);
    }
    assertThat(h1.count()).isEqualTo(15);
    assertThat(h1.getBuckets().size()).isEqualTo(15);
    for (HistogramBucket b : h1.getBuckets()) {
      assertThat(b.getCount()).isEqualTo(1);
    }

    FixedIntervalHistogramImpl h2 = new FixedIntervalHistogramImpl(0, 15, 15);
    for (int i = 0; i < 15; i++) {
      h2.addTimestamp(i);
    }
    assertThat(h2.count()).isEqualTo(15);
    assertThat(h2.getBuckets().size()).isEqualTo(15);
    for (HistogramBucket b : h2.getBuckets()) {
      assertThat(b.getCount()).isEqualTo(1);
    }

    h1.mergeHistogram(h2.getBuckets());
    assertThat(h1.count()).isEqualTo(30);
    assertThat(h1.getBuckets().size()).isEqualTo(15);
    for (HistogramBucket b : h1.getBuckets()) {
      assertThat(b.getCount()).isEqualTo(2);
    }
  }

  @Test
  public void testInsertionInLastBucket() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 15, 15);
    for (int i = 0; i < 15; i++) {
      h.addTimestamp(14);
    }

    assertThat(h.count()).isEqualTo(15);
    assertThat(h.getBuckets().size()).isEqualTo(15);

    HistogramBucket lastBucket = h.getBuckets().get(h.getBuckets().size() - 1);
    assertThat(lastBucket.getCount()).isEqualTo(15);

    // Buckets are stored in increasing order.
    int bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(bucketLow == 14 ? 15 : 0);
      bucketLow++;
    }
  }

  @Test
  public void testRandomInsertion() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 15, 15);
    List<Integer> nums = IntStream.range(0, 15).boxed().collect(Collectors.toList());
    Collections.shuffle(nums);
    for (int i : nums) {
      h.addTimestamp(i);
    }

    assertThat(h.count()).isEqualTo(15);
    assertThat(h.getBuckets().size()).isEqualTo(15);

    // Buckets are stored in increasing order.
    int bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(1);
      bucketLow++;
    }

    // Insert random values again.
    nums = IntStream.range(0, 15).boxed().collect(Collectors.toList());
    Collections.shuffle(nums);
    for (int i : nums) {
      h.addTimestamp(i);
    }

    bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(2);
      bucketLow++;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testZeroBuckets() {
    new FixedIntervalHistogramImpl(1, 100, 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeLow() {
    new FixedIntervalHistogramImpl(-1, 100, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadRange() {
    new FixedIntervalHistogramImpl(100, 99, 0);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testAddingElementOutOfRange() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 5, 5);
    for (int i = 0; i < 5; i++) {
      h.addTimestamp(i);
    }

    assertThat(h.count()).isEqualTo(5);
    assertThat(h.getBuckets().size()).isEqualTo(5);

    // Throws exception since 6 is out of histogram range.
    h.addTimestamp(6);
  }

  @Test
  public void testAddingHistogramHigh() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 5, 5);
    for (int i = 0; i < 5; i++) {
      h.addTimestamp(i);
    }

    assertThat(h.count()).isEqualTo(5);
    assertThat(h.getBuckets().size()).isEqualTo(5);

    h.addTimestamp(5);
    assertThat(h.count()).isEqualTo(6);
    assertThat(h.getBuckets().size()).isEqualTo(5);

    int bucketLow = 0;
    for (HistogramBucket bucket : h.getBuckets()) {
      assertThat(bucket.getLow()).isEqualTo(bucketLow);
      assertThat(bucket.getHigh()).isEqualTo(bucketLow + 1);
      assertThat(bucket.getCount()).isEqualTo(bucketLow == 4 ? 2 : 1);
      bucketLow++;
    }
  }

  @Test
  public void testSmallBucketSizes() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(0, 2, 3);
    h.addTimestamp(1);

    assertThat(h.count()).isEqualTo(1);
    assertThat(h.getBuckets().size()).isEqualTo(3);
    assertThat(h.getBuckets().get(1).getCount()).isEqualTo(1);

    FixedIntervalHistogramImpl h2 = new FixedIntervalHistogramImpl(9, 10, 10);
    h2.addTimestamp(9);

    assertThat(h2.count()).isEqualTo(1);
    assertThat(h2.getBuckets().size()).isEqualTo(10);
    assertThat(h2.getBuckets().get(0).getCount()).isEqualTo(1);

    FixedIntervalHistogramImpl h3 = new FixedIntervalHistogramImpl(9, 11, 10);
    h3.addTimestamp(10);

    assertThat(h3.count()).isEqualTo(1);
    assertThat(h3.getBuckets().size()).isEqualTo(10);
    assertThat(h3.getBuckets().get(5).getCount()).isEqualTo(1);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testOutOfBoundsHigh() {
    FixedIntervalHistogramImpl h = new FixedIntervalHistogramImpl(9, 10, 10);
    h.addTimestamp(15);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testOutOfBoundsLow() {
    FixedIntervalHistogramImpl h2 = new FixedIntervalHistogramImpl(9, 10, 10);
    h2.addTimestamp(1);
  }
}
