package com.slack.kaldb.testlib;

import com.slack.kaldb.histogram.FixedIntervalHistogramImpl;
import com.slack.kaldb.histogram.Histogram;
import com.slack.kaldb.logstore.LogMessage;
import java.util.List;

public class HistogramUtil {
  public static Histogram makeHistogram(
      long histogramStartMs, long histogramEndMs, int bucketCount, List<LogMessage> messages) {
    Histogram histogram =
        new FixedIntervalHistogramImpl(histogramStartMs, histogramEndMs, bucketCount);
    for (LogMessage m : messages) {
      histogram.addTimestamp(m.timeSinceEpochMilli);
    }
    return histogram;
  }
}
