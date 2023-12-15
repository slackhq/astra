package com.slack.kaldb.bulkIngestApi;

public record BulkIngestResponse(int totalDocs, long failedDocs, String errorMsg) {}
