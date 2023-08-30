package com.slack.kaldb.elasticsearchApi;

public record BulkIngestResponse(int totalDocs, long failedDocs, String errorMsg) {}
