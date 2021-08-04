package com.slack.kaldb.histogram;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class NoOpHistogramImplTest {

  @Test
  public void testNoOpHistogram() {
    NoOpHistogramImpl h = new NoOpHistogramImpl();
    assertThat(h.count()).isEqualTo(0);
    h.add(100L);
    assertThat(h.count()).isEqualTo(1);
    h.add(100L);
    assertThat(h.count()).isEqualTo(2);
  }
}
