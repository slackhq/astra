package com.slack.astra.logstore.search;

import java.time.Instant;

public class MaskedField {
  private final String fieldName;
  private final long startTimeEpochMs;
  private final long endTimeEpochMs;

  public MaskedField(String field, long startTimeEpochMs, long endTimeEpochMs) {
    this.fieldName = field;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
  }

  public String getFieldName() {
    return fieldName;
  }

  public long getStartTimeEpochMs() {
    return startTimeEpochMs;
  }

  public long getEndTimeEpochMs() {
    return endTimeEpochMs;
  }

  public boolean inMaskedTimerange(long logTimestamp) {
    return startTimeEpochMs <= logTimestamp && endTimeEpochMs >= logTimestamp;
  }
}
