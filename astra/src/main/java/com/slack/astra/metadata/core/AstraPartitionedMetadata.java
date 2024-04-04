package com.slack.astra.metadata.core;

public abstract class AstraPartitionedMetadata extends AstraMetadata {
  public AstraPartitionedMetadata(String name) {
    super(name);
  }

  /**
   * Returns a string identifying the partition to store this metadata under.
   *
   * <p>The deciding on a partition identifier for a metadata care should taken to ensure it is a
   * stable identifier, and cannot change as the metadata is mutated. If the partition identifier
   * were to be based off a mutable field this would cause problems updating the znode, as this
   * would require a different znode path.
   *
   * <p>Changing the logic for a partition identifier should always be backward compatible, to
   * ensure previously stored data can still be located for operations.
   *
   * <p>The amount of partitions directly maps to the number of potential metadata stores that must
   * be maintained in memory. The amount of partitions should be targeted such that each partition
   * comes close to the configured jute.maxbuffer, but is not at risk of exceeding this threshold.
   */
  public abstract String getPartition();
}
