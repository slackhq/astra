package com.slack.kaldb.logstore;

import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;

import java.time.Instant;


public class OpensearchShimTest {


  @Test
  public void test() throws Exception{

    Aggregator aggregator = OpensearchShim.test(20, 0, Instant.now().toEpochMilli());

    System.out.println(aggregator.name());
  }
}
