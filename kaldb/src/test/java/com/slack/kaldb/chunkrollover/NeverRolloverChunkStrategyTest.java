package com.slack.kaldb.chunkrollover;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.chunkrollover.NeverRolloverChunkStrategy;
import org.junit.Test;

public class NeverRolloverChunkStrategyTest {
  @Test
  public void testShouldRolloverIsAlwaysFalse() {
    assertThat(new NeverRolloverChunkStrategy().shouldRollOver(Long.MAX_VALUE, Long.MAX_VALUE))
        .isFalse();
  }
}
