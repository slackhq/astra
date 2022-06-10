package com.slack.kaldb.chunkManager;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class NeverRolloverChunkStrategyImplTest {
  @Test
  public void testShouldRolloverIsAlwaysFalse() {
    assertThat(new NeverRolloverChunkStrategyImpl().shouldRollOver(Long.MAX_VALUE, Long.MAX_VALUE))
        .isFalse();
  }
}
