package com.slack.kaldb.logstore;

import org.junit.Test;
import org.opensearch.search.aggregations.Aggregator;


public class OpensearchShimTest {


  @Test
  public void test() throws Exception{

    Aggregator aggregator = OpensearchShim.test(20);

    System.out.println(aggregator.name());
  }
}
