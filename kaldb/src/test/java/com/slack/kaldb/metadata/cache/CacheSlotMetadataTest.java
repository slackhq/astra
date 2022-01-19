package com.slack.kaldb.metadata.cache;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.slack.kaldb.proto.metadata.Metadata;
import java.time.Instant;
import org.junit.Test;

public class CacheSlotMetadataTest {

  @Test
  public void testCacheSlotMetadata() {
    String name = "name";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.FREE;
    String replicaId = "";
    long updatedTimeUtc = Instant.now().toEpochMilli();

    CacheSlotMetadata cacheSlotMetadata =
        new CacheSlotMetadata(name, cacheSlotState, replicaId, updatedTimeUtc);
    assertThat(cacheSlotMetadata.name).isEqualTo(name);
    assertThat(cacheSlotMetadata.cacheSlotState).isEqualTo(cacheSlotState);
    assertThat(cacheSlotMetadata.replicaId).isEqualTo(replicaId);
    assertThat(cacheSlotMetadata.updatedTimeEpochMs).isEqualTo(updatedTimeUtc);
  }

  @Test
  public void testCacheSlotEqualsHashcode() {
    String name = "name";
    Metadata.CacheSlotMetadata.CacheSlotState cacheSlotState =
        Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED;
    String replicaId = "123";
    long updatedTimeUtc = Instant.now().toEpochMilli();

    CacheSlotMetadata cacheSlot =
        new CacheSlotMetadata(name, cacheSlotState, replicaId, updatedTimeUtc);
    CacheSlotMetadata cacheSlotDuplicate =
        new CacheSlotMetadata(name, cacheSlotState, replicaId, updatedTimeUtc);
    CacheSlotMetadata cacheSlotDifferentState =
        new CacheSlotMetadata(
            name, Metadata.CacheSlotMetadata.CacheSlotState.EVICT, replicaId, updatedTimeUtc);
    CacheSlotMetadata cacheSlotDifferentReplicaId =
        new CacheSlotMetadata(name, cacheSlotState, "321", updatedTimeUtc);
    CacheSlotMetadata cacheSlotDifferentUpdatedTime =
        new CacheSlotMetadata(name, cacheSlotState, replicaId, updatedTimeUtc + 1);

    assertThat(cacheSlot.hashCode()).isEqualTo(cacheSlotDuplicate.hashCode());
    assertThat(cacheSlot).isEqualTo(cacheSlotDuplicate);

    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentState);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentState.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentReplicaId);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentReplicaId.hashCode());
    assertThat(cacheSlot).isNotEqualTo(cacheSlotDifferentUpdatedTime);
    assertThat(cacheSlot.hashCode()).isNotEqualTo(cacheSlotDifferentUpdatedTime.hashCode());
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
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.ASSIGNED,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.EVICT,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.EVICTING,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.LOADING,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.LIVE,
                    "",
                    Instant.now().toEpochMilli()));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name", Metadata.CacheSlotMetadata.CacheSlotState.FREE, "", 0));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new CacheSlotMetadata(
                    "name",
                    Metadata.CacheSlotMetadata.CacheSlotState.FREE,
                    null,
                    Instant.now().toEpochMilli()));
  }
}
