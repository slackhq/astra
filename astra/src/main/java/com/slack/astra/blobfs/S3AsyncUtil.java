package com.slack.astra.blobfs;

import com.google.common.base.Preconditions;
import com.slack.astra.proto.config.AstraConfigs;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.crt.S3CrtConnectionHealthConfiguration;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;
import software.amazon.awssdk.services.s3.crt.S3CrtProxyConfiguration;
import software.amazon.awssdk.services.s3.crt.S3CrtRetryConfiguration;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3AsyncUtil {
  private static final Logger LOG = LoggerFactory.getLogger(S3AsyncUtil.class);

  private S3AsyncUtil() {}

  public static S3AsyncClient initS3Client(AstraConfigs.S3Config config) {
    Preconditions.checkArgument(notNullOrEmpty(config.getS3Region()));
    String region = config.getS3Region();

    AwsCredentialsProvider awsCredentialsProvider;
    try {

      if (notNullOrEmpty(config.getS3AccessKey()) && notNullOrEmpty(config.getS3SecretKey())) {
        String accessKey = config.getS3AccessKey();
        String secretKey = config.getS3SecretKey();
        AwsBasicCredentials awsBasicCredentials = AwsBasicCredentials.create(accessKey, secretKey);
        awsCredentialsProvider = StaticCredentialsProvider.create(awsBasicCredentials);
      } else {
        awsCredentialsProvider = DefaultCredentialsProvider.create();
      }

      // default to 5% of the heap size for the max crt off-heap or 1GiB (min for client)
      long jvmMaxHeapSizeBytes = Runtime.getRuntime().maxMemory();
      long defaultCrtMemoryLimit = Math.max(Math.round(jvmMaxHeapSizeBytes * 0.05), 1073741824);
      long maxNativeMemoryLimitBytes =
          Long.parseLong(
              System.getProperty(
                  "astra.s3CrtBlobFs.maxNativeMemoryLimitBytes",
                  String.valueOf(defaultCrtMemoryLimit)));
      LOG.info(
          "Using a maxNativeMemoryLimitInBytes for the S3AsyncClient of '{}' bytes",
          maxNativeMemoryLimitBytes);
      S3CrtAsyncClientBuilder s3AsyncClient =
          S3AsyncClient.crtBuilder()
              .retryConfiguration(S3CrtRetryConfiguration.builder().numRetries(3).build())
              .targetThroughputInGbps(config.getS3TargetThroughputGbps())
              .region(Region.of(region))
              .maxNativeMemoryLimitInBytes(maxNativeMemoryLimitBytes)
              .credentialsProvider(awsCredentialsProvider);

      // We add a healthcheck to prevent an error with the CRT client, where it will
      // continue to attempt to read data from a socket that is no longer returning data
      S3CrtHttpConfiguration.Builder httpConfigurationBuilder =
          S3CrtHttpConfiguration.builder()
              .proxyConfiguration(
                  S3CrtProxyConfiguration.builder().useEnvironmentVariableValues(false).build())
              .connectionTimeout(Duration.ofSeconds(5))
              .connectionHealthConfiguration(
                  S3CrtConnectionHealthConfiguration.builder()
                      .minimumThroughputTimeout(Duration.ofSeconds(3))
                      .minimumThroughputInBps(32000L)
                      .build());
      s3AsyncClient.httpConfiguration(httpConfigurationBuilder.build());


      // You cannot set the endpoint if AWS_USE_FIPS_ENDPOINT is set to `true`
      if (!System.getenv().getOrDefault("AWS_USE_FIPS_ENDPOINT", "false").equals("true")) {
          if (notNullOrEmpty(config.getS3EndPoint())) {
              String endpoint = config.getS3EndPoint();
              try {
                  s3AsyncClient.endpointOverride(new URI(endpoint));
              } catch (URISyntaxException e) {
                  throw new RuntimeException(e);
              }
          }
      }


      return s3AsyncClient.build();
    } catch (S3Exception e) {
      throw new RuntimeException("Could not initialize S3blobFs", e);
    }
  }

  static boolean notNullOrEmpty(String target) {
    return target != null && !target.isEmpty();
  }
}
