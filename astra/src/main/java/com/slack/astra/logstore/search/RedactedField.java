package com.slack.astra.logstore.search;

public class RedactedField {
  private final String fieldName;
  private final long startTimeEpochMs;
  private final long endTimeEpochMs;

  public RedactedField(String field, long startTimeEpochMs, long endTimeEpochMs) {
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
