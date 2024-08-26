package com.slack.astra.metadata.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.slack.astra.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class CacheSlotMetadataTest {

  @Test
  public void testCacheSlotMetadata() {
    String hostname = "hostname";
    String name = "name";
    String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    String replicaId = "";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, hostname, replicaSet);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.hostname).isEqualTo(hostname);
    assertThat(cacheSlotMetadata.replicaSet).isEqualTo(replicaSet);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeEpochMs);
  }

  @Test
  public void testCacheSlotEqualsHashcode() {
    String hostname = "hostname";
    String name = "name";
    String replicaSet = "rep1";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
    String replicaId = "123";
    long updatedTimeEpochMs = Instant.now().toEpochMilli();

    CacheSlotMetadata cacheSlot =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, hostname, replicaSet);
    CacheSlotMetadata cacheSlotDuplicate =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, hostname, replicaSet);
    CacheSlotMetadata cacheSlotDifferentState =
        new CacheSlotMetadata(
            name,
            Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
            replicaId,
            updatedTimeEpochMs,
            hostname,
            replicaSet);
    CacheSlotMetadata cacheSlotDifferentReplicaId =
        new CacheSlotMetadata(
            name, cacheSlotState, "321", updatedTimeEpochMs, hostname, replicaSet);
    CacheSlotMetadata cacheSlotDifferentUpdatedTime =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs + 1, hostname, replicaSet);
    CacheSlotMetadata cacheSlotDifferentSupportedIndexType =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs + 1, hostname, replicaSet);
    CacheSlotMetadata cacheSlotDifferentHostname =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, "hostname2", replicaSet);

    CacheSlotMetadata cacheSlotDifferentReplicaPartition =
        new CacheSlotMetadata(
            name, cacheSlotState, replicaId, updatedTimeEpochMs, hostname, "rep2");

    assertThat(cacheSlot.hashCode()).isEqualTo(cacheSlotDuplicate.hashCode());
    assertThat(cacheSlot).isEqualTo(cacheSlotDuplicate);

    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentState);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentState.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentReplicaId);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentReplicaId.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentUpdatedTime);
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentSupportedIndexType);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentUpdatedTime.hashCode());
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentSupportedIndexType.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentHostname);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentHostname.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentReplicaPartition);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentReplicaPartition.hashCode());
  }

  @Test
  public void invalidArgumentsShouldThrow() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "123",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
                    "",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
                    "",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
                    "",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.LOADING,
                    "",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
                    "",
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    "",
                    0,
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    null,
                    Instant.now().toEpochMilli(),
                    "hostname",
                    "rep1"));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              new CacheSlotMetadata(
                  "name",
                  Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                  "123",
                  100000,
                  "hostname",
                  "rep1");
            });
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> {
              new CacheSlotMetadata(
                  "name",
                  Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                  "123",
                  100000,
                  "hostname",
                  "rep1");
            });
  }
}
