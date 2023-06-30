package com.slack.kaldb.blobfs.s3;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.slack.kaldb.blobfs.BlobFs;
import com.slack.kaldb.blobfs.BlobFsConfig;
import com.slack.kaldb.proto.config.KaldbConfigs;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MetadataDirective;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3CrtBlobFs extends BlobFs {
  public static final String S3_SCHEME = "s3://";
  private static final Logger LOG = LoggerFactory.getLogger(S3CrtBlobFs.class);
  private static final String DELIMITER = "/";

  private final S3AsyncClient s3AsyncClient;

  public S3CrtBlobFs(S3AsyncClient s3AsyncClient) {
    this.s3AsyncClient = s3AsyncClient;
  }

  static boolean isNullOrEmpty(String target) {
    return target == null || "".equals(target);
  }

  public static S3AsyncClient initS3Client(KaldbConfigs.S3Config config) {
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

      // TODO: Remove hard coded HTTP IMPL property setting by only having 1 http client on the
      // classpath.
      System.setProperty(
          SdkSystemSetting.SYNC_HTTP_SERVICE_IMPL.property(),
          "software.amazon.awssdk.http.apache.ApacheSdkHttpService");
      S3CrtAsyncClientBuilder s3AsyncClient =
          S3AsyncClient.crtBuilder()
              .region(Region.of(region))
              .credentialsProvider(awsCredentialsProvider);

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

  @Override
  public void init(BlobFsConfig config) {
    // Not sure if this interface works for a library. So on ice for now.
    throw new UnsupportedOperationException(
        "This class doesn't support initialization via blobfsconfig.");
  }

  private HeadObjectResponse getS3ObjectMetadata(URI uri) throws IOException {
    URI base = getBase(uri);
    String path = sanitizePath(base.relativize(uri).getPath());
    HeadObjectRequest headObjectRequest =
        HeadObjectRequest.builder().bucket(uri.getHost()).key(path).build();

    try {
      return s3AsyncClient.headObject(headObjectRequest).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
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

  private URI normalizeToDirectoryUri(URI uri) throws IOException {
    if (isPathTerminatedByDelimiter(uri)) {
      return uri;
    }
    try {
      return new URI(uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER), null);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
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

  private boolean isEmptyDirectory(URI uri) throws IOException {
    if (!isDirectory(uri)) {
      return false;
    }
    String prefix = normalizeToDirectoryPrefix(uri);
    boolean isEmpty = true;
    ListObjectsV2Response listObjectsV2Response;
    ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
        ListObjectsV2Request.builder().bucket(uri.getHost());

    if (!prefix.equals(DELIMITER)) {
      listObjectsV2RequestBuilder = listObjectsV2RequestBuilder.prefix(prefix);
    }

    ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
    try {
      listObjectsV2Response = s3AsyncClient.listObjectsV2(listObjectsV2Request).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    for (S3Object s3Object : listObjectsV2Response.contents()) {
      if (s3Object.key().equals(prefix)) {
        continue;
      } else {
        isEmpty = false;
        break;
      }
    }
    return isEmpty;
  }

  private boolean copyFile(URI srcUri, URI dstUri) throws IOException {
    try {
      String encodedUrl = null;
      try {
        encodedUrl =
            URLEncoder.encode(
                srcUri.getHost() + srcUri.getPath(), StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }

      String dstPath = sanitizePath(dstUri.getPath());
      CopyObjectRequest copyReq =
          CopyObjectRequest.builder()
              .copySource(encodedUrl)
              .destinationBucket(dstUri.getHost())
              .destinationKey(dstPath)
              .build();

      CopyObjectResponse copyObjectResponse = s3AsyncClient.copyObject(copyReq).get();
      return copyObjectResponse.sdkHttpResponse().isSuccessful();
    } catch (S3Exception | ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean mkdir(URI uri) throws IOException {
    LOG.info("mkdir {}", uri);
    try {
      Preconditions.checkNotNull(uri, "uri is null");
      String path = normalizeToDirectoryPrefix(uri);
      // Bucket root directory already exists and cannot be created
      if (path.equals(DELIMITER)) {
        return true;
      }

      PutObjectRequest putObjectRequest =
          PutObjectRequest.builder().bucket(uri.getHost()).key(path).build();

      PutObjectResponse putObjectResponse =
          s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromBytes(new byte[0])).get();

      return putObjectResponse.sdkHttpResponse().isSuccessful();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete) throws IOException {
    LOG.info("Deleting uri {} force {}", segmentUri, forceDelete);
    try {
      if (isDirectory(segmentUri)) {
        if (!forceDelete) {
          Preconditions.checkState(
              isEmptyDirectory(segmentUri),
              "ForceDelete flag is not set and directory '%s' is not empty",
              segmentUri);
        }
        String prefix = normalizeToDirectoryPrefix(segmentUri);
        ListObjectsV2Response listObjectsV2Response;
        ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
            ListObjectsV2Request.builder().bucket(segmentUri.getHost());

        if (prefix.equals(DELIMITER)) {
          ListObjectsV2Request listObjectsV2Request = listObjectsV2RequestBuilder.build();
          listObjectsV2Response = s3AsyncClient.listObjectsV2(listObjectsV2Request).get();
        } else {
          ListObjectsV2Request listObjectsV2Request =
              listObjectsV2RequestBuilder.prefix(prefix).build();
          listObjectsV2Response = s3AsyncClient.listObjectsV2(listObjectsV2Request).get();
        }
        boolean deleteSucceeded = true;
        for (S3Object s3Object : listObjectsV2Response.contents()) {
          DeleteObjectRequest deleteObjectRequest =
              DeleteObjectRequest.builder()
                  .bucket(segmentUri.getHost())
                  .key(s3Object.key())
                  .build();

          DeleteObjectResponse deleteObjectResponse =
              s3AsyncClient.deleteObject(deleteObjectRequest).get();

          deleteSucceeded &= deleteObjectResponse.sdkHttpResponse().isSuccessful();
        }
        return deleteSucceeded;
      } else {
        String prefix = DELIMITER + sanitizePath(segmentUri.getPath());
        DeleteObjectRequest deleteObjectRequest =
            DeleteObjectRequest.builder().bucket(segmentUri.getHost()).key(prefix).build();

        DeleteObjectResponse deleteObjectResponse =
            s3AsyncClient.deleteObject(deleteObjectRequest).get();

        return deleteObjectResponse.sdkHttpResponse().isSuccessful();
      }
    } catch (NoSuchKeyException e) {
      return false;
    } catch (S3Exception e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean doMove(URI srcUri, URI dstUri) throws IOException {
    if (copy(srcUri, dstUri)) {
      return delete(srcUri, true);
    }
    return false;
  }

  @Override
  public boolean copy(URI srcUri, URI dstUri) throws IOException {
    LOG.info("Copying uri {} to uri {}", srcUri, dstUri);
    Preconditions.checkState(exists(srcUri), "Source URI '%s' does not exist", srcUri);
    if (srcUri.equals(dstUri)) {
      return true;
    }
    if (!isDirectory(srcUri)) {
      delete(dstUri, true);
      return copyFile(srcUri, dstUri);
    }
    dstUri = normalizeToDirectoryUri(dstUri);
    Path srcPath = Paths.get(srcUri.getPath());
    try {
      boolean copySucceeded = true;
      for (String filePath : listFiles(srcUri, true)) {
        URI srcFileURI = URI.create(filePath);
        String directoryEntryPrefix = srcFileURI.getPath();
        URI src = new URI(srcUri.getScheme(), srcUri.getHost(), directoryEntryPrefix, null);
        String relativeSrcPath = srcPath.relativize(Paths.get(directoryEntryPrefix)).toString();
        String dstPath = dstUri.resolve(relativeSrcPath).getPath();
        URI dst = new URI(dstUri.getScheme(), dstUri.getHost(), dstPath, null);
        copySucceeded &= copyFile(src, dst);
      }
      return copySucceeded;
    } catch (URISyntaxException e) {
      throw new IOException(e);
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
  public long length(URI fileUri) throws IOException {
    try {
      Preconditions.checkState(!isPathTerminatedByDelimiter(fileUri), "URI is a directory");
      HeadObjectResponse s3ObjectMetadata = getS3ObjectMetadata(fileUri);
      Preconditions.checkState((s3ObjectMetadata != null), "File '%s' does not exist", fileUri);
      if (s3ObjectMetadata.contentLength() == null) {
        return 0;
      }
      return s3ObjectMetadata.contentLength();
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive) throws IOException {
    try {
      ImmutableList.Builder<String> builder = ImmutableList.builder();
      String continuationToken = null;
      boolean isDone = false;
      String prefix = normalizeToDirectoryPrefix(fileUri);
      while (!isDone) {
        ListObjectsV2Request.Builder listObjectsV2RequestBuilder =
            ListObjectsV2Request.builder().bucket(fileUri.getHost());
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
        isDone = !listObjectsV2Response.isTruncated();
        continuationToken = listObjectsV2Response.nextContinuationToken();
      }
      String[] listedFiles = builder.build().toArray(new String[0]);
      LOG.info(
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
    GetObjectRequest getObjectRequest =
        GetObjectRequest.builder().bucket(srcUri.getHost()).key(prefix).build();

    s3AsyncClient.getObject(getObjectRequest, AsyncResponseTransformer.toFile(dstFile)).get();
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri) throws Exception {
    LOG.debug("Copy {} from local to {}", srcFile.getAbsolutePath(), dstUri);
    URI base = getBase(dstUri);
    String prefix = sanitizePath(base.relativize(dstUri).getPath());
    PutObjectRequest putObjectRequest =
        PutObjectRequest.builder().bucket(dstUri.getHost()).key(prefix).build();

    s3AsyncClient.putObject(putObjectRequest, AsyncRequestBody.fromFile(srcFile)).get();
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
  public long lastModified(URI uri) throws IOException {
    return getS3ObjectMetadata(uri).lastModified().toEpochMilli();
  }

  @Override
  public boolean touch(URI uri) throws IOException {
    try {
      HeadObjectResponse s3ObjectMetadata = getS3ObjectMetadata(uri);
      String encodedUrl = null;
      try {
        encodedUrl =
            URLEncoder.encode(uri.getHost() + uri.getPath(), StandardCharsets.UTF_8.toString());
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }

      String path = sanitizePath(uri.getPath());
      Map<String, String> mp = new HashMap<>();
      mp.put("lastModified", String.valueOf(System.currentTimeMillis()));
      CopyObjectRequest request =
          CopyObjectRequest.builder()
              .copySource(encodedUrl)
              .destinationBucket(uri.getHost())
              .destinationKey(path)
              .metadata(mp)
              .metadataDirective(MetadataDirective.REPLACE)
              .build();

      s3AsyncClient.copyObject(request).get();
      long newUpdateTime = getS3ObjectMetadata(uri).lastModified().toEpochMilli();
      return newUpdateTime > s3ObjectMetadata.lastModified().toEpochMilli();
    } catch (NoSuchKeyException e) {
      String path = sanitizePath(uri.getPath());
      try {
        s3AsyncClient
            .putObject(
                PutObjectRequest.builder().bucket(uri.getHost()).key(path).build(),
                AsyncRequestBody.fromBytes(new byte[0]))
            .get();
      } catch (InterruptedException | ExecutionException ex) {
        throw new IOException(ex);
      }
      return true;
    } catch (S3Exception | ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(URI uri) throws IOException {
    try {
      String path = sanitizePath(uri.getPath());
      GetObjectRequest getObjectRequest =
          GetObjectRequest.builder().bucket(uri.getHost()).key(path).build();
      return s3AsyncClient
          .getObject(getObjectRequest, AsyncResponseTransformer.toBlockingInputStream())
          .get();
    } catch (S3Exception e) {
      throw e;
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
