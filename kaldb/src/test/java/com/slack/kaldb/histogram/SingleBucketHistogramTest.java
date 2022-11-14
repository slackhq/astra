package com.slack.kaldb.histogram;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SingleBucketHistogramTest {

  @Test
  public void testNoOpHistogram() {
    SingleBucketHistogram h = new SingleBucketHistogram();
    assertThat(h.count()).isEqualTo(0);
    h.addTimestamp(100L);
    assertThat(h.count()).isEqualTo(1);
    h.addTimestamp(100L);
    assertThat(h.count()).isEqualTo(2);
  }
}
