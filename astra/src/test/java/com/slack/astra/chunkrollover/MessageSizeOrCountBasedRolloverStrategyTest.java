package com.slack.astra.chunkrollover;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import com.slack.astra.proto.config.AstraConfigs;
import com.slack.astra.testlib.AstraConfigUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MessageSizeOrCountBasedRolloverStrategyTest {

  private SimpleMeterRegistry metricsRegistry;

  @BeforeEach
  public void setUp() throws Exception {
    metricsRegistry = new SimpleMeterRegistry();
  }

  @AfterEach
  public void tearDown() throws TimeoutException, IOException {
    metricsRegistry.close();
  }

  @Test
  public void testInitViaConfig() {
    AstraConfigs.IndexerConfig indexerCfg = AstraConfigUtil.makeIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(100);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    MessageSizeOrCountBasedRolloverStrategy chunkRollOverStrategy =
        MessageSizeOrCountBasedRolloverStrategy.fromConfig(metricsRegistry, indexerCfg);
    assertThat(chunkRollOverStrategy.getMaxBytesPerChunk()).isEqualTo(10737418240L);
    assertThat(chunkRollOverStrategy.getMaxMessagesPerChunk()).isEqualTo(100);
  }

  @Test
  public void testNegativeMaxMessagesPerChunk() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 100, -1));
  }

  @Test
  public void testNegativeMaxBytesPerChunk() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, -100, 1));
  }

  @Test
  public void testChunkRollOver() {
    ChunkRollOverStrategy chunkRollOverStrategy =
        new MessageSizeOrCountBasedRolloverStrategy(metricsRegistry, 1000, 2000);

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
