package com.slack.kaldb.chunkManager;

import static org.assertj.core.api.Assertions.assertThat;

import com.slack.kaldb.proto.config.KaldbConfigs;
import com.slack.kaldb.testlib.KaldbConfigUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SizeOrMessageBasedRolloverStrategyTest {

  private SimpleMeterRegistry metricsRegistry;

  @Before
  public void setUp() throws Exception {
    metricsRegistry = new SimpleMeterRegistry();
  }

  @After
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
  }

  @Test
  public void testInitViaConfig() {
    KaldbConfigs.IndexerConfig indexerCfg = KaldbConfigUtil.makeIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(100);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    SizeOrMessageBasedRolloverStrategy chunkRollOverStrategy =
        SizeOrMessageBasedRolloverStrategy.fromConfig(metricsRegistry, indexerCfg);
    assertThat(chunkRollOverStrategy.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    assertThat(chunkRollOverStrategy.getMaxMessagesPerChunk()).isEqualTo(100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxMessagesPerChunk() {
    new SizeOrMessageBasedRolloverStrategy(metricsRegistry, 100, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxBytesPerChunk() {
    new SizeOrMessageBasedRolloverStrategy(metricsRegistry, -100, 1);
  }

  @Test
  public void testChunkRollOver() {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new SizeOrMessageBasedRolloverStrategy(metricsRegistry, 1000, 2000);

    assertThat(chunkRollOverStrategy.shouldRollOver(1, 1)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(-1, -1)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(0, 0)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(100, 100)).isFalse();
    assertThat(chunkRollOverStrategy.shouldRollOver(1000, 1)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(1001, 1)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(100, 2000)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(100, 2001)).isTrue();
    assertThat(chunkRollOverStrategy.shouldRollOver(1001, 2001)).isTrue();
  }
}
