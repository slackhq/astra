package com.slack.kaldb.logstore;

import java.time.Instant;
import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;

public class OpensearchShimTest {

  @Test
  public void test() throws Exception {

    Aggregator aggregator = OpensearchShim.test(20, 0, Instant.now().toEpochMilli());

    System.out.println(aggregator.name());
  }
}
