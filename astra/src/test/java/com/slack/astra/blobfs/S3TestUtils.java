package com.slack.astra.blobfs;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;

public class S3TestUtils {
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
