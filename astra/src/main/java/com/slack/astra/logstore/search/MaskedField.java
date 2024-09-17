package com.slack.astra.logstore.search;

import java.time.Instant;

public class MaskedField {
  private final String fieldName;
  private final Instant startTime;
  private final Instant endTime;

  public MaskedField(String field, Instant startTimeEpochMs, Instant endTimeEpochMs) {
    this.fieldName = field;
    this.startTime = startTimeEpochMs;
    this.endTime = endTimeEpochMs;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Instant getStartTime() {
    return startTime;
  }

  public Instant getEndTime() {
    return endTime;
  }

  public boolean inMaskedTimerange(Instant logTimestamp) {
    return logTimestamp.isAfter(startTime) && logTimestamp.isBefore(endTime);
  }
}
