package com.slack.kaldb.chunk;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.InvalidProtocolBufferException;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategy;
import com.slack.kaldb.chunkManager.ChunkRollOverStrategyImpl;
import com.slack.kaldb.config.KaldbConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import org.junit.Test;

public class ChunkRollOverStrategyImplTest {

  @Test
  public void testInitViaConfig() throws InvalidProtocolBufferException {
    /*
     {
       "indexerConfig": {
         "maxMessagesPerChunk": 1,
         "maxBytesPerChunk": 100
       },
       nodeRoles: [INDEX]
     }
    */
    // Need https://openjdk.java.net/jeps/355
    final String testIndexerCfg =
        "{\n"
            + "  \"indexerConfig\": {\n"
            + "    \"maxMessagesPerChunk\": 1,\n"
            + "    \"maxBytesPerChunk\": 100\n"
            + "  },\n"
            + "  nodeRoles: [INDEX]\n"
            + "}";

    KaldbConfig.initFromJsonStr(testIndexerCfg);

    final KaldbConfigs.IndexerConfig indexerCfg = KaldbConfig.get().getIndexerConfig();
    assertThat(indexerCfg.getMaxMessagesPerChunk()).isEqualTo(1);
    assertThat(indexerCfg.getMaxBytesPerChunk()).isEqualTo(100);

    ChunkRollOverStrategyImpl chunkRollOverStrategy = ChunkRollOverStrategyImpl.fromConfig();
    assertThat(chunkRollOverStrategy.getMaxBytesPerChunk()).isEqualTo(100);
    assertThat(chunkRollOverStrategy.getMaxMessagesPerChunk()).isEqualTo(1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxMessagesPerChunk() {
    new ChunkRollOverStrategyImpl(100, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeMaxBytesPerChunk() {
    new ChunkRollOverStrategyImpl(-100, 1);
  }

  @Test
  public void testChunkRollOver() {
    ChunkRollOverStrategy chunkRollOverStrategy = new ChunkRollOverStrategyImpl(1000, 2000);

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
