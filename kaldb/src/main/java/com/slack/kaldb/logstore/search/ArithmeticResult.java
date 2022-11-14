package com.slack.kaldb.logstore.search;

public class ArithmeticResult {
  public long sum;
  public long count;
  public long min = Long.MAX_VALUE;
  public long max = Long.MIN_VALUE;
  public long errorCount;

  public ArithmeticResult(long sum, long count, long min, long max, long errorCount) {
    this.sum = sum;
    this.count = count;
    this.min = min;
    this.max = max;
    this.errorCount = errorCount;
  }

  public void merge(ArithmeticResult result) {
    sum += result.sum;
    count += result.count;
    if (result.min < min) {
      min = result.min;
    }
    if (result.max > max) {
      max = result.max;
    }
    errorCount += result.errorCount;
  }
}
