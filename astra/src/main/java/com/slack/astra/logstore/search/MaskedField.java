package com.slack.astra.logstore.search;

public class MaskedField {
    public final String field;
    public final long startTimeEpochMs;
    public final long endTimeEpochMs;

    public MaskedField(String field, long startTimeEpochMs, long endTimeEpochMs) {
        this.field = field;
        this.startTimeEpochMs = startTimeEpochMs;
        this.endTimeEpochMs = endTimeEpochMs;
    }

    public String getField() {
        return field;
    }

    public long getStartTimeEpochMs() {
        return startTimeEpochMs;
    }

    public long getEndTimeEpochMs() {
        return endTimeEpochMs;
    }

    public boolean inMaskedTimerange(long logTimestamp) {
       return logTimestamp >= startTimeEpochMs && logTimestamp <= endTimeEpochMs;
    }
}
