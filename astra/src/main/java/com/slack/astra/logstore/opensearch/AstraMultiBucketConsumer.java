package com.slack.astra.logstore.opensearch;

import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom MultiBucketConsumer that enforces the max_buckets limit during the collection phase.
 *
 * <p>OpenSearch's default MultiBucketConsumer only checks the bucket count limit when accept() is
 * called with a non-zero value, which happens during the reduce phase on the coordinating node.
 * During collection, BucketsAggregator calls accept(0) for each new bucket, which skips the count
 * check entirely.
 *
 * <p>In Astra, aggregations run per-chunk without a coordinating-node-style reduce, so the bucket
 * limit is never enforced. This allows terms aggregations on high-cardinality fields to create
 * unlimited buckets during collection, leading to OOM.
 *
 * <p>This subclass counts every accept(0) call (each representing a new bucket) and throws
 * MultiBucketConsumerService.TooManyBucketsException when the limit is exceeded.
 */
class AstraMultiBucketConsumer extends MultiBucketConsumerService.MultiBucketConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(AstraMultiBucketConsumer.class);

  private int newBucketCount = 0;
  private final int maxBuckets;

  AstraMultiBucketConsumer(int limit, CircuitBreaker breaker) {
    super(limit, breaker);
    this.maxBuckets = limit;
  }

  @Override
  public void accept(int value) {
    if (value == 0) {
      newBucketCount++;
      if (newBucketCount > maxBuckets) {
        LOG.warn(
            "Too many buckets created during collection: {} exceeds limit of {}",
            newBucketCount,
            maxBuckets);
        throw new MultiBucketConsumerService.TooManyBucketsException(
            "Trying to create too many buckets. Must be less than or equal to: ["
                + maxBuckets
                + "] but was ["
                + newBucketCount
                + "]",
            maxBuckets);
      }
    }
    super.accept(value);
  }
}
