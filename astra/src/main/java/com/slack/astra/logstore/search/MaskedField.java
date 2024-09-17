package com.slack.astra.logstore.search;

import java.time.Instant;

public class MaskedField {
  private final String fieldName;
  private final Instant startTimeEpochMs;
  private final Instant endTimeEpochMs;

  public MaskedField(String field, Instant startTimeEpochMs, Instant endTimeEpochMs) {
    this.fieldName = field;
    this.startTimeEpochMs = startTimeEpochMs;
    this.endTimeEpochMs = endTimeEpochMs;
  }

  public String getFieldName() {
    return fieldName;
  }

  public Instant getStartTimeEpochMs() {
    return startTimeEpochMs;
  }

  public Instant getEndTimeEpochMs() {
    return endTimeEpochMs;
  }

  public boolean inMaskedTimerange(Instant logTimestamp) {
    return logTimestamp.isAfter(startTimeEpochMs) && logTimestamp.isBefore(endTimeEpochMs);
  }
}
