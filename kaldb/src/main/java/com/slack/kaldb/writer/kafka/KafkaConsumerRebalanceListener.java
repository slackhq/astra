package com.slack.kaldb.writer.kafka;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class logs the consumer re-balance events. */
public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerRebalanceListener.class);

  public KafkaConsumerRebalanceListener() {
    LOG.info("Created consumer re-balance listener");
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
}
