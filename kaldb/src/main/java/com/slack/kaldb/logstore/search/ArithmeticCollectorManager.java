package com.slack.kaldb.logstore.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.search.CollectorManager;

public class ArithmeticCollectorManager
    implements CollectorManager<ArithmeticCollector, ArithmeticResult> {

  private final String field;

  public ArithmeticCollectorManager(String field) {
    this.field = field;
  }

  @Override
  public ArithmeticCollector newCollector() throws IOException {
    return new ArithmeticCollector(field);
  }

  @Override
  public ArithmeticResult reduce(Collection<ArithmeticCollector> collectors) throws IOException {
    ArithmeticResult result = null;
    for (ArithmeticCollector collector : collectors) {
      if (result == null) {
        result = collector.getResult();
      } else {
        result.merge(collector.getResult());
      }
    }
    return result;
  }
}
