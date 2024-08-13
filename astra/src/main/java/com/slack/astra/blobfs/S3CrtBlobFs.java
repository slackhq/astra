package com.slack.astra.blobfs;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.slack.astra.proto.config.AstraConfigs;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * This class is a duplicate of the original S3BlobFs, but modified to support the new S3 CRT client
 * and S3 transfer manager. As part of this all internal api calls to S3 were moved to async, as
 * this is the only client type supported by the new CRT code.
 *
 * <p>Todo - this class would hugely benefit from a clean sheet rewrite, as a lot of the original
 * assumptions this was based on no longer apply. Additionally, several retrofits have been made to
 * support new API approaches which has left this overly complex.
 */
@Deprecated
public class S3CrtBlobFs extends BlobFs {
  public static final String S3_SCHEME = "s3://";
  private static final Logger LOG = LoggerFactory.getLogger(S3CrtBlobFs.class);
  private static final String DELIMITER = "/";
  private static final int LIST_MAX_KEYS = 2500;

  private final S3AsyncClient s3AsyncClient;
  private final S3TransferManager transferManager;

  public S3CrtBlobFs(S3AsyncClient s3AsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
    this.transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
  }

  static boolean isNullOrEmpty(String target) {
    return target == null || "".equals(target);
  }

  public static S3AsyncClient initS3Client(AstraConfigs.S3Config config) {
    Preconditions.checkArgument(!isNullOrEmpty(config.getS3Region()));
    String region = config.getS3Region();

    AwsCredentialsProvider awsCredentialsProvider;
    try {

      if (!isNullOrEmpty(config.getS3AccessKey()) && !isNullOrEmpty(config.getS3SecretKey())) {
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

      if (!isNullOrEmpty(config.getS3EndPoint())) {
        String endpoint = config.getS3EndPoint();
        try {
          s3AsyncClient.endpointOverride(new URI(endpoint));
        } catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
      return s3AsyncClient.build();
    } catch (S3Exception e) {
      throw new RuntimeException("Could not initialize S3blobFs", e);
    }
  }

  private boolean isPathTerminatedByDelimiter(URI uri) {
    return uri.getPath().endsWith(DELIMITER);
  }

  private String normalizeToDirectoryPrefix(URI uri) throws IOException {
    Preconditions.checkNotNull(uri, "uri is null");
    URI strippedUri = getBase(uri).relativize(uri);
    if (isPathTerminatedByDelimiter(strippedUri)) {
      return sanitizePath(strippedUri.getPath());
    }
    return sanitizePath(strippedUri.getPath() + DELIMITER);
  }

  private String sanitizePath(String path) {
    path = path.replaceAll(DELIMITER + "+", DELIMITER);
    if (path.startsWith(DELIMITER) && !path.equals(DELIMITER)) {
      path = path.substring(1);
    }
    return path;
  }

  private URI getBase(URI uri) throws IOException {
    try {
      return new URI(uri.getScheme(), uri.getHost(), null, null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  private boolean existsFile(URI uri) throws IOException {
    try {
      URI base = getBase(uri);
      String path = sanitizePath(base.relativize(uri).getPath());
      HeadObjectRequest headObjectRequest =
          HeadObjectRequest.builder().bucket(uri.getHost()).key(path).build();

      s3AsyncClient.headObject(headObjectRequest).get();
      return true;
    } catch (Exception e) {
      if (e instanceof ExecutionException && e.getCause() instanceof NoSuchKeyException) {
        return false;
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public boolean exists(URI fileUri) throws IOException {
    try {
      if (isDirectory(fileUri)) {
        return true;
      }
      if (isPathTerminatedByDelimiter(fileUri)) {
        return false;
      }
      return existsFile(fileUri);
    } catch (NoSuchKeyException e) {
      return false;
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    try {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      String continuationToken = null;
      boolean isDone = false;
      String prefix = normalizeToDirectoryPrefix(fileUri);
      int fileCount = 0;
      while (!isDone) {
        ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
            ListObjectsV2Request.builder().maxKeys(LIST_MAX_KEYS).bucket(fileUri.getHost());
        if (!prefix.equals(DELIMITER)) {
          listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix);
        }
        if (!recursive) {
          listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.delimiter(DELIMITER);
        }
        if (continuationToken != null) {
          listObjectsV2RequestBuilder.continuationToken(continuationToken);
        }
        ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
        LOG.debug("Trying to send ListObjectsV2Request {}", listObjectsV2Request);
        ListObjectsV2Response listObjectsV2Response =
            s3AsyncClient.listObjectsV2(listObjectsV2Request).get();
        LOG.debug("Getting ListObjectsV2Response: {}", listObjectsV2Response);
        List<S3Object> filesReturned = listObjectsV2Response.contents();
        fileCount += filesReturned.size();
        filesReturned.stream()
            .forEach(
                object -> {
                  // Only add files and not directories
                  if (!object.key().equals(fileUri.getPath())
                      && !object.key().endsWith(DELIMITER)) {
                    String fileKey = object.key();
                    if (fileKey.startsWith(DELIMITER)) {
                      fileKey = fileKey.substring(1);
                    }
                    builder.add(S3_SCHEME + fileUri.getHost() + DELIMITER + fileKey);
                  }
                });
        if (fileCount == LIST_MAX_KEYS) {
          // check if we reached the max keys returned, if so abort and throw an error message
          LOG.error(
              "Too many files ({}) returned from S3 when attempting to list object prefixes",
              LIST_MAX_KEYS);
          throw new IllegalStateException(
              String.format(
                  "Max keys (%s) reached when attempting to list S3 objects", LIST_MAX_KEYS));
        }
        isDone = !listObjectsV2Response.isTruncated();
        continuationToken = listObjectsV2Response.nextContinuationToken();
      }
      String[] listedFiles = builder.build().toArray(new String[0]);
      LOG.debug(
          "Listed {} files from URI: {}, is recursive: {}", listedFiles.length, fileUri, recursive);
      return listedFiles;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile) throws Exception {
    LOG.debug("Copy {} to local {}", srcUri, dstFile.getAbsolutePath());
    URI base = getBase(srcUri);
    FileUtils.forceMkdir(dstFile.getParentFile());
    String prefix = sanitizePath(base.relativize(srcUri).getPath());

    if (isDirectory(srcUri)) {
      CompletedDirectoryDownload completedDirectoryDownload =
          transferManager
              .downloadDirectory(
                  DownloadDirectoryRequest.builder()
                      .destination(dstFile.toPath())
                      .bucket(srcUri.getHost())
                      .listObjectsV2RequestTransformer(
                          builder -> {
                            builder.maxKeys(LIST_MAX_KEYS);
                            builder.prefix(prefix);
                          })
                      .build())
              .completionFuture()
              .get();
      if (!completedDirectoryDownload.failedTransfers().isEmpty()) {
        completedDirectoryDownload
            .failedTransfers()
            .forEach(
                failedFileDownload -> LOG.warn("Failed to download file '{}'", failedFileDownload));
        throw new IllegalStateException(
            String.format(
                "Was unable to download all files - failed %s",
                completedDirectoryDownload.failedTransfers().size()));
      }
    } else {
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(srcUri.getHost()).key(prefix).build();
      transferManager
          .downloadFile(
              DownloadFileRequest.builder()
                  .getObjectRequest(getObjectRequest)
                  .destination(dstFile)
                  .build())
          .completionFuture()
          .get();
    }
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    LOG.debug("Copy {} from local to {}", srcFile.getAbsolutePath(), dstUri);
    URI base = getBase(dstUri);
    String prefix = sanitizePath(base.relativize(dstUri).getPath());

    if (srcFile.isDirectory()) {
      CompletedDirectoryUpload completedDirectoryUpload =
          transferManager
              .uploadDirectory(
                  UploadDirectoryRequest.builder()
                      .source(srcFile.toPath())
                      .bucket(dstUri.getHost())
                      .build())
              .completionFuture()
              .get();

      if (!completedDirectoryUpload.failedTransfers().isEmpty()) {
        completedDirectoryUpload
            .failedTransfers()
            .forEach(failedFileUpload -> LOG.warn("Failed to upload file '{}'", failedFileUpload));
        throw new IllegalStateException(
            String.format(
                "Was unable to upload all files - failed %s",
                completedDirectoryUpload.failedTransfers().size()));
      }
    } else {
      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(dstUri.getHost()).key(prefix).build();
      transferManager
          .uploadFile(
              UploadFileRequest.builder()
                  .putObjectRequest(putObjectRequest)
                  .source(srcFile)
                  .build())
          .completionFuture()
          .get();
    }
  }

  @Override
  public boolean isDirectory(URI uri) throws IOException {
    try {
      String prefix = normalizeToDirectoryPrefix(uri);
      if (prefix.equals(DELIMITER)) {
        return true;
      }

      ListObjectsV2Request listObjectsV2Request =
          ListObjectsV2Request.builder().bucket(uri.getHost()).prefix(prefix).maxKeys(2).build();
      ListObjectsV2Response listObjectsV2Response =
          s3AsyncClient.listObjectsV2(listObjectsV2Request).get();
      return listObjectsV2Response.hasContents();
    } catch (NoSuchKeyException e) {
      LOG.error("Could not get directory entry for {}", uri);
      return false;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
