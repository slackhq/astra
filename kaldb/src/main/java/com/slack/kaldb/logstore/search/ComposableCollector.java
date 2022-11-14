package com.slack.kaldb.logstore.search;

import java.io.IOException;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.SimpleCollector;

public abstract class ComposableCollector extends SimpleCollector {
  public abstract void collect(NumericDocValues docValues, int i) throws IOException;

  public abstract void merge(ComposableCollector c);
}
