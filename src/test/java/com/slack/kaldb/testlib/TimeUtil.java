package com.slack.kaldb.testlib;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TimeUtil {
  public static long timeEpochMs(LocalDateTime time) {
    return time.toEpochSecond(ZoneOffset.UTC) * 1000;
  }
}
