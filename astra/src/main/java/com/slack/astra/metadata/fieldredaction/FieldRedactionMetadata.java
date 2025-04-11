package com.slack.astra.metadata.fieldredaction;

import static com.google.common.base.Preconditions.checkArgument;

import com.slack.astra.metadata.core.AstraMetadata;

/** Metadata for a field redaction with a timerange for redaction */
public class FieldRedactionMetadata extends AstraMetadata {

  public final String fieldName;
  public final long startTimeEpochMs;
  public final long endTimeEpochMs;

  public FieldRedactionMetadata(
      String name, String fieldName, long startTimeEpochMs, long endTimeEpochMs) {
    super(name);
    checkArgument(fieldName != null && !fieldName.isEmpty(), "field name cannot be null");
    checkArgument(startTimeEpochMs > 0, "startTimeEpochMs must be greater than 0");
    checkArgument(
        endTimeEpochMs > startTimeEpochMs,
        "endTimeEpochMs must be greater than the startTimeEpochMs");
    this.fieldName = fieldName;
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

  public boolean inRedactionTimerange(long logTimestamp) {
    return startTimeEpochMs <= logTimestamp && endTimeEpochMs >= logTimestamp;
  }

  @Override
  public String toString() {
    return "RedactedFieldMetadata{"
        + "name='"
        + name
        + '\''
        + ", fieldName='"
        + fieldName
        + '\''
        + ", startTimeEpochMs="
        + startTimeEpochMs
        + ", endTimeEpochMs="
        + endTimeEpochMs
        + "}";
  }
}
