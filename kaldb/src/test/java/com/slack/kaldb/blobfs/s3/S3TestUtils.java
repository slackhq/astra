package com.slack.kaldb.blobfs.s3;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3TestUtils {

  public static PutObjectRequest getPutObjectRequest(String bucket, String key) {
    return PutObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static HeadObjectRequest getHeadObjectRequest(String bucket, String key) {
    return HeadObjectRequest.builder().bucket(bucket).key(key).build();
  }

  public static ListObjectsV2Request getListObjectRequest(
      String bucket, String key, boolean isInBucket) {
    ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
        ListObjectsV2Request.builder().bucket(bucket);

    if (!isInBucket) {
      listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(key);
    }

    return listObjectsV2RequestBuilder.build();
  }

  /**
   * Based off of S3_MOCK_EXTENSION.createS3ClientV2();
   *
   * @see com.adobe.testing.s3mock.testsupport.common.S3MockStarter
   */
  public static S3AsyncClient createS3CrtClient(String serviceEndpoint) {
    return S3AsyncClient.crtBuilder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .forcePathStyle(true)
        .checksumValidationEnabled(false) // https://github.com/adobe/S3Mock/issues/1123
        .endpointOverride(URI.create(serviceEndpoint))
        .httpConfiguration(
            S3CrtHttpConfiguration.builder().trustAllCertificatesEnabled(true).build())
        .build();
  }
}
