package com.slack.astra.chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;

public class SearchContextTest {

  public static final String HOSTNAME = "localhost";
  public static final int PORT = 10000;

  @Test
  public void testSearchContextInit() {
    SearchContext searchContext = new SearchContext(HOSTNAME, PORT);
    assertThat(searchContext.hostname).isEqualTo(HOSTNAME);
    assertThat(searchContext.port).isEqualTo(PORT);
  }

  @Test
  public void testNegativePort() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new SearchContext(HOSTNAME, -1));
  }

  @Test
  public void testEmptyHostname() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new SearchContext("", 1000));
  }

  @Test
  public void testNullHostname() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> new SearchContext(null, 1000));
  }

  @Test
  public void testUrl() {
    SearchContext searchContext = new SearchContext(HOSTNAME, PORT);
    final String url = searchContext.toUrl();
    assertThat(url).isNotEmpty();
    assertThat(url).contains(HOSTNAME);
    assertThat(url).contains(String.valueOf(PORT));
  }
}
