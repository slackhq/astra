package com.slack.kaldb.bulkIngestApi;

/** Metadata object for the result of a bulk ingest request */
public record BulkIngestResponse(int totalDocs, long failedDocs, String errorMsg) {}
