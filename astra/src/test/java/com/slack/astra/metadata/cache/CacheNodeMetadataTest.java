package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class CacheNodeMetadataTest {
  @Test
  public void testCacheNodeMetadata() {
    String id = "abcd";
    String hostname = "host";
    String replicaSet = "rep1";
    long nodeCapacityBytes = 4096;

    CacheNodeMetadata cacheNodeMetadata =
        new CacheNodeMetadata(id, hostname, nodeCapacityBytes, replicaSet);
    assertThat(cacheNodeMetadata.name).isEqualTo(id);
    assertThat(cacheNodeMetadata.hostname).isEqualTo(hostname);
    assertThat(cacheNodeMetadata.replicaSet).isEqualTo(replicaSet);
    assertThat(cacheNodeMetadata.nodeCapacityBytes).isEqualTo(nodeCapacityBytes);
  }

  @Test
  public void testCacheNodeEqualsHashcode() {
    String id = "abcd";
    String hostname = "host";
    String replicaSet = "rep1";
    long nodeCapacityBytes = 4096;

    CacheNodeMetadata cacheNode =
        new CacheNodeMetadata(id, hostname, nodeCapacityBytes, replicaSet);
    CacheNodeMetadata cacheNodeDuplicate =
        new CacheNodeMetadata(id, hostname, nodeCapacityBytes, replicaSet);
    CacheNodeMetadata cacheNodeDifferentHostname =
        new CacheNodeMetadata(id, "localhost", nodeCapacityBytes, replicaSet);
    CacheNodeMetadata cacheNodeDifferentId =
        new CacheNodeMetadata("dog", hostname, nodeCapacityBytes, replicaSet);
    CacheNodeMetadata cacheNodeDifferentCapacity =
        new CacheNodeMetadata(id, hostname, 1024, replicaSet);
    CacheNodeMetadata cacheNodeDifferentReplica =
        new CacheNodeMetadata(id, hostname, nodeCapacityBytes, "rep2");

    assertThat(cacheNode.hashCode()).isEqualTo(cacheNodeDuplicate.hashCode());
    assertThat(cacheNode).isEqualTo(cacheNodeDuplicate);

    assertThat(cacheNode).isNotEqualTo(cacheNodeDifferentHostname);
    assertThat(cacheNode.hashCode()).isNotEqualTo(cacheNodeDifferentHostname.hashCode());
    assertThat(cacheNode).isNotEqualTo(cacheNodeDifferentId);
    assertThat(cacheNode.hashCode()).isNotEqualTo(cacheNodeDifferentId.hashCode());
    assertThat(cacheNode).isNotEqualTo(cacheNodeDifferentCapacity);
    assertThat(cacheNode.hashCode()).isNotEqualTo(cacheNodeDifferentCapacity.hashCode());
    assertThat(cacheNode).isNotEqualTo(cacheNodeDifferentReplica);
    assertThat(cacheNode.hashCode()).isNotEqualTo(cacheNodeDifferentReplica.hashCode());
  }
}
