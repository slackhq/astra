package com.slack.astra.logstore.search;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

public class AstraQueryFlagsTest {

  @Test
  public void testQueryFlagEnabledWithExclamationPrefix() {
    String query = "message:error !astra.testFlag";
    String queryFlag = "!astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isTrue();
    assertThat(result.getRight()).isEqualTo("message:error ");
  }

  @Test
  public void testQueryFlagEnabledWithoutExclamationPrefix() {
    String query = "message:error !astra.testFlag";
    String queryFlag = "astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isTrue();
    assertThat(result.getRight()).isEqualTo("message:error ");
  }

  @Test
  public void testQueryFlagNotPresent() {
    String query = "message:error status:500";
    String queryFlag = "astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isFalse();
    assertThat(result.getRight()).isEqualTo("message:error status:500");
  }

  @Test
  public void testQueryFlagAtBeginningOfQuery() {
    String query = "!astra.testFlag message:error";
    String queryFlag = "astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isTrue();
    assertThat(result.getRight()).isEqualTo(" message:error");
  }

  @Test
  public void testMultipleQueryFlagsOnlySpecifiedFlagRemoved() {
    String query = "message:error !astra.flag1 !astra.flag2";
    String queryFlag = "astra.flag1";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isTrue();
    assertThat(result.getRight()).isEqualTo("message:error  !astra.flag2");
  }

  @Test
  public void testEmptyQuery() {
    String query = "";
    String queryFlag = "astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isFalse();
    assertThat(result.getRight()).isEqualTo("");
  }

  @Test
  public void testNullQuery() {
    String query = null;
    String queryFlag = "astra.testFlag";

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isFalse();
    assertThat(result.getRight()).isEqualTo(null);
  }

  @Test
  public void testNullQueryFlag() {
    String query = "message:error";
    String queryFlag = null;

    Pair<Boolean, String> result = AstraQueryFlags.isQueryFlagEnabled(query, queryFlag);

    assertThat(result.getLeft()).isFalse();
    assertThat(result.getRight()).isEqualTo("message:error");
  }
}
