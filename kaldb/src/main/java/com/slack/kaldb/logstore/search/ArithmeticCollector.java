package com.slack.kaldb.logstore.search;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.ScoreMode;

public class ArithmeticCollector extends ComposableCollector {

  private final String field;
  private long sum;
  private long count;
  private long min = Long.MAX_VALUE;
  private long max = Long.MIN_VALUE;
  private long errorCount;
  private NumericDocValues docValues;

  public ArithmeticCollector(String field) {
    this.field = field;
  }

  @Override
  public void collect(int i) throws IOException {
    collect(docValues, i);
  }

  @Override
  public void collect(NumericDocValues docValues, int i) throws IOException {
    if (docValues != null && docValues.advanceExact(i)) {
      long val = docValues.longValue();
      sum += val;
      count++;
      if (val > max) {
        max = val;
      }
      if (val < min) {
        min = val;
      }
    } else {
      errorCount++;
    }
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    this.docValues = context.reader().getNumericDocValues(field);
  }

  @Override
  public void merge(ComposableCollector c) {
    if (c instanceof ArithmeticCollector) {
      ArithmeticCollector ac = (ArithmeticCollector) c;
      sum += ac.sum;
      count += ac.count;
      errorCount += ac.errorCount;
      if (ac.max > max) {
        max = ac.max;
      }
      if (ac.min < min) {
        min = ac.min;
      }
    } else {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  public long getSum() {
    return sum;
  }

  public long getCount() {
    return count;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public long getErrorCount() {
    return errorCount;
  }

  public ArithmeticResult getResult() {
    return new ArithmeticResult(sum, count, min, max, errorCount);
  }
}
