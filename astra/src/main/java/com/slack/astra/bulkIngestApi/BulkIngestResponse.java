package com.slack.astra.bulkIngestApi;

/** Metadata object for the result of a bulk ingest request */
public record BulkIngestResponse(int totalDocs, long failedDocs, String errorMsg) {}
