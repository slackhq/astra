package com.slack.kaldb.metadata.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.Test;

public class KaldbMetadataTest {
  private static class DummyMetadata extends KaldbMetadata {
    public DummyMetadata(String name) {
      super(name);
    }
  }

  @Test
  public void testKaldbMetadata() {
    final String name = "dummy";
    assertThat(new DummyMetadata(name).name).isEqualTo(name);
    assertThat(List.of(new DummyMetadata("test1"), new DummyMetadata("test2")).size()).isEqualTo(2);
  }

  @Test(expected = IllegalStateException.class)
  public void testEmptyName() {
    new DummyMetadata("");
  }
}
