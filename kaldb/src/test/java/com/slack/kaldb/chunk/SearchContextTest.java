package com.slack.kaldb.chunk;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SearchContextTest {

  public static final String HOSTNAME = "localhost";
  public static final int PORT = 10000;

  @Test
  public void testSearchContextInit() {
    SearchContext searchContext = new SearchContext(HOSTNAME, PORT);
    assertThat(searchContext.hostname).isEqualTo(HOSTNAME);
    assertThat(searchContext.port).isEqualTo(PORT);
  }

  @Test(expected = IllegalStateException.class)
  public void testNegativePort() {
    new SearchContext(HOSTNAME, -1);
  }

  @Test(expected = IllegalStateException.class)
  public void testEmptyHostname() {
    new SearchContext("", 1000);
  }

  @Test(expected = IllegalStateException.class)
  public void testNullHostname() {
    new SearchContext(null, 1000);
  }
}
