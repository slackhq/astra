package com.slack.kaldb.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.slack.kaldb.testlib.TestKafkaServer;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Ignore;
import org.junit.Test;

public class LocalKafkaSeed {

  /**
   * Initializes local kafka broker with sample messages occurring in the immediate future.
   *
   * <p>This test is intended for local debugging purposes, and will be executed manually as needed.
   */
  @Ignore
  @Test
  public void seedLocalBrokerWithSampleData()
      throws ExecutionException, InterruptedException, JsonProcessingException, TimeoutException {
    EphemeralKafkaBroker broker = EphemeralKafkaBroker.create(9092, 2181);
    final Instant startTime = Instant.now();
    TestKafkaServer.produceMessagesToKafka(broker, startTime);
  }
}
