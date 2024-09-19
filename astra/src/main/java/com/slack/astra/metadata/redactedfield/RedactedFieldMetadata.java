package com.slack.astra.metadata.redactedfield;

import com.slack.astra.proto.metadata.Metadata;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Metadata for a redacted field with a timerange for redaction
 */
public class RedactedFieldMetadata {

  private final String fieldName;
  private final long startTimeEpochMs;
  private final long endTimeEpochMs;

  public RedactedFieldMetadata(String field, long startTimeEpochMs, long endTimeEpochMs) {
    checkArgument(field != null, "field name cannot be empty");
    checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be greater than 0");
    checkArgument(
            endTimeEpochMs > startTimeEpochMs,
            "endTimeEpochMs must be greater than the startTimeEpochMs");
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

  @Override
  public String toString() {
    return "RedactedFieldMetadata{"
            + "fieldName='"
            + fieldName
            + ", startTimeEpochMs="
            + startTimeEpochMs
            + ", endTimeEpochMs="
            + endTimeEpochMs
            + "}";
  }

}
