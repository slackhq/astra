package com.slack.astra.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

import java.util.List;
import org.junit.jupiter.api.Test;

public class AstraMetadataTest {
  private static class DummyMetadata extends AstraMetadata {
    public DummyMetadata(String name) {
      super(name);
    }
  }

  @Test
  public void testAstraMetadata() {
    final String name = "dummy";
    assertThat(new DummyMetadata(name).name).isEqualTo(name);
    assertThat(List.of(new DummyMetadata("test1"), new DummyMetadata("test2")).size()).isEqualTo(2);
  }

  @Test
  public void testEmptyName() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new DummyMetadata(""));
  }
}
