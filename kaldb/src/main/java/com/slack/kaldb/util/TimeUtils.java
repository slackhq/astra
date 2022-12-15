package com.slack.kaldb.util;

import java.util.concurrent.TimeUnit;

public class TimeUtils {

  public static long nanosToMillis(long timeInNanos) {
    return TimeUnit.MILLISECONDS.convert(timeInNanos, TimeUnit.NANOSECONDS);
  }
}
