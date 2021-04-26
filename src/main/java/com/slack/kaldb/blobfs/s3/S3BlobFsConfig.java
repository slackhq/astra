package com.slack.kaldb.blobfs.s3;

public class S3BlobFsConfig {
  public final String accessKey;
  public final String secretKey;
  public final String region;
  public final String endpoint;

  public S3BlobFsConfig(String accessKey, String secretKey, String region, String endpoint) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
    this.endpoint = endpoint;
  }
}
