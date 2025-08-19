package com.slack.astra.blobfs;

import static software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder;

import com.slack.astra.chunk.ReadWriteChunk;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

/**
 * Blob store abstraction for basic operations on chunk/snapshots on remote storage. All operations
 * are invoked with a prefix, which can be considered analogous to the "folder," and all objects are
 * stored to a consistent location of "{prefix}/{filename}".
 */
public class BlobStore {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStore.class);

  protected final String bucketName;
  protected final S3AsyncClient s3AsyncClient;
  private final S3TransferManager transferManager;

  public BlobStore(S3AsyncClient s3AsyncClient, String bucketName) {
    this.bucketName = bucketName;
    this.s3AsyncClient = s3AsyncClient;
    this.transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
  }

  /**
   * Uploads a directory to the object store by prefix
   *
   * @param prefix Prefix to store (ie, chunk id)
   * @param directoryToUpload Directory to upload
   * @throws IllegalStateException Thrown if any files fail to upload
   * @throws RuntimeException Thrown when error is considered generally non-retryable
   */
  public void upload(String prefix, Path directoryToUpload) {
    assert prefix != null && !prefix.isEmpty();
    assert directoryToUpload.toFile().isDirectory();
    assert Objects.requireNonNull(directoryToUpload.toFile().listFiles()).length > 0;

    try {
      CompletedDirectoryUpload upload =
          transferManager
              .uploadDirectory(
                  UploadDirectoryRequest.builder()
                      .source(directoryToUpload)
                      .s3Prefix(prefix)
                      .bucket(bucketName)
                      .build())
              .completionFuture()
              .get();
      if (!upload.failedTransfers().isEmpty()) {
        // Log each failed transfer with its exception
        upload
            .failedTransfers()
            .forEach(
                failedFileUpload ->
                    LOG.error(
                        "Error attempting to upload file to S3", failedFileUpload.exception()));

        throw new IllegalStateException(
            String.format(
                "Some files failed to upload - attempted to upload %s files, failed %s.",
                Objects.requireNonNull(directoryToUpload.toFile().listFiles()).length,
                upload.failedTransfers().size()));
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Downloads a directory from the object store by prefix
   *
   * @param prefix Prefix to store (ie, chunk id)
   * @param destinationDirectory Directory to download to
   * @throws RuntimeException Thrown when error is considered generally non-retryable
   */
  public void download(String prefix, Path destinationDirectory) {
    assert prefix != null && !prefix.isEmpty();
    assert !destinationDirectory.toFile().exists() || destinationDirectory.toFile().isDirectory();

    try {
      CompletedDirectoryDownload download =
          transferManager
              .downloadDirectory(
                  DownloadDirectoryRequest.builder()
                      .bucket(bucketName)
                      .destination(destinationDirectory)
                      .listObjectsV2RequestTransformer(l -> l.prefix(prefix))
                      .build())
              .completionFuture()
              .get();

      LOG.info(
          "Downloaded directory from S3: bucket={}, prefix={}, destination={}, status={}",
          bucketName,
          prefix,
          destinationDirectory,
          download.failedTransfers());

      if (!download.failedTransfers().isEmpty()) {
        // Log each failed transfer with its exception
        LOG.error(
            "Error attempting to download directory from S3: bucket={}, prefix={}, destination={}, failed transfers={}",
            bucketName,
            prefix,
            destinationDirectory,
            download.failedTransfers());
        download
            .failedTransfers()
            .forEach(
                failedFileDownload -> {
                  Throwable cause = failedFileDownload.exception();
                  while (cause.getCause() != null && cause != cause.getCause()) {
                    cause = cause.getCause();
                  }
                  if (cause instanceof S3Exception s3ex) {
                    LOG.error(
                        "Error attempting to download file from S3: key={}, requestId={}, extendedRequestId={},",
                        failedFileDownload.request().getObjectRequest().key(),
                        s3ex.requestId(),
                        s3ex.extendedRequestId(),
                        s3ex);

                  } else {
                    LOG.error(
                        "Error attempting to download file from S3",
                        failedFileDownload.exception());
                  }
                });

        throw new IllegalStateException(
            String.format(
                "Some files failed to download - failed to download %s files.",
                download.failedTransfers().size()));
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getSchema(String chunkId) {
    try {
      return transferManager
          .download(
              DownloadRequest.builder()
                  .getObjectRequest(
                      GetObjectRequest.builder()
                          .bucket(bucketName)
                          .key(String.format("%s/%s", chunkId, ReadWriteChunk.SCHEMA_FILE_NAME))
                          .build())
                  .responseTransformer(new ByteArrayAsyncResponseTransformer<>())
                  .build())
          .completionFuture()
          .get()
          .result()
          .asByteArray();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Lists all files found on object store by the complete object key (including prefix). This would
   * included what is generally considered the directory (ie foo/bar.example)
   *
   * @param prefix Prefix to list (ie, chunk id)
   * @return List of file names by complete object key
   * @throws RuntimeException Thrown when error is considered generally non-retryable
   */
  public List<String> listFiles(String prefix) {
    assert prefix != null && !prefix.isEmpty();

    ListObjectsV2Request listRequest = builder().bucket(bucketName).prefix(prefix).build();
    ListObjectsV2Publisher asyncPaginatedListResponse =
        s3AsyncClient.listObjectsV2Paginator(listRequest);

    List<String> filesList = new CopyOnWriteArrayList<>();
    try {
      asyncPaginatedListResponse
          .subscribe(
              listResponse ->
                  listResponse.contents().forEach(s3Object -> filesList.add(s3Object.key())))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return filesList;
  }

  public Map<String, Long> listFilesWithSize(String prefix) {
    assert prefix != null && !prefix.isEmpty();

    ListObjectsV2Request listRequest = builder().bucket(bucketName).prefix(prefix).build();
    ListObjectsV2Publisher asyncPaginatedListResponse =
        s3AsyncClient.listObjectsV2Paginator(listRequest);

    Map<String, Long> filesListWithSize = new HashMap<>();
    try {
      asyncPaginatedListResponse
          .subscribe(
              listResponse ->
                  listResponse
                      .contents()
                      .forEach(s3Object -> filesListWithSize.put(s3Object.key(), s3Object.size())))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return filesListWithSize;
  }

  /**
   * Deletes a chunk off of object storage by chunk id. If object was not found returns false.
   *
   * @param prefix Prefix to delete (ie, chunk id)
   * @return boolean if any files at the prefix/directory were deleted
   * @throws RuntimeException Thrown when error is considered generally non-retryable
   */
  public boolean delete(String prefix) {
    assert prefix != null && !prefix.isEmpty();

    ListObjectsV2Request listRequest = builder().bucket(bucketName).prefix(prefix).build();
    ListObjectsV2Publisher asyncPaginatedListResponse =
        s3AsyncClient.listObjectsV2Paginator(listRequest);

    AtomicBoolean deleted = new AtomicBoolean(false);
    try {
      asyncPaginatedListResponse
          .subscribe(
              listResponse -> {
                List<ObjectIdentifier> objects =
                    listResponse.contents().stream()
                        .map(s3Object -> ObjectIdentifier.builder().key(s3Object.key()).build())
                        .toList();
                if (objects.isEmpty()) {
                  return;
                }
                DeleteObjectsRequest deleteRequest =
                    DeleteObjectsRequest.builder()
                        .bucket(bucketName)
                        .delete(Delete.builder().objects(objects).build())
                        .build();
                try {
                  s3AsyncClient.deleteObjects(deleteRequest).get();
                  deleted.set(true);
                } catch (InterruptedException | ExecutionException e) {
                  throw new RuntimeException(e);
                }
              })
          .get();
      return deleted.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Compresses JSON data using GZIP.
   *
   * @param data The JSON data to compress
   * @return The compressed byte array
   * @throws IOException if compression fails
   */
  public static byte[] compressData(String data) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
      gzipOutputStream.write(data.getBytes(StandardCharsets.UTF_8));
    }
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * Uploads JSON data to the object store by S3 key (full path in the bucket).
   *
   * @param key The S3 key (full path in the bucket)
   * @param jsonData The JSON data to upload
   * @throws RuntimeException if compression fails or upload fails
   */
  public void uploadData(String key, String jsonData, boolean gzip) throws RuntimeException {
    assert key != null && !key.isEmpty();
    assert jsonData != null && !jsonData.isEmpty();

    PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
    try {
      if (gzip) {
        byte[] compressedData = compressData(jsonData);
        s3AsyncClient.putObject(request, AsyncRequestBody.fromBytes(compressedData)).get();
      } else {
        s3AsyncClient
            .putObject(request, AsyncRequestBody.fromString(jsonData, StandardCharsets.UTF_8))
            .get();
      }
    } catch (IOException | InterruptedException | ExecutionException e) {
      throw new RuntimeException("Failed to upload JSON data", e);
    }
  }

  /**
   * Decompresses JSON data that has been compressed using GZIP.
   *
   * @param compressedData The compressed byte array
   * @return The decompressed JSON data as a String
   * @throws RuntimeException if decompression fails
   */
  public static String decompressData(byte[] compressedData) {
    assert compressedData != null && compressedData.length > 0;

    try (GZIPInputStream gzipInputStream =
        new GZIPInputStream(new ByteArrayInputStream(compressedData))) {
      return new String(gzipInputStream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("Error decompressing JSON data", e);
      throw new RuntimeException("Failed to decompress JSON data", e);
    }
  }

  /**
   * Reads the contents of a gzip file from the object store by S3 key (full path in the bucket).
   *
   * @param key full S3 path
   * @param gzip whether the file is gzipped
   * @return File content in string format
   */
  public String readFileData(String key, boolean gzip) throws RuntimeException {
    assert key != null && !key.isEmpty();

    try {
      ResponseInputStream<GetObjectResponse> futureStream =
          s3AsyncClient
              .getObject(
                  GetObjectRequest.builder().bucket(bucketName).key(key).build(),
                  AsyncResponseTransformer.toBlockingInputStream())
              .get();
      if (gzip) {
        // TODO: Handle below operation in more efficient way via streaming
        return decompressData(futureStream.readAllBytes());
      }
      return new String(futureStream.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException | ExecutionException | InterruptedException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchKeyException
          || (cause instanceof S3Exception s3ex && s3ex.statusCode() == 404)) {
        LOG.warn("File not found in S3: {}", key);
        return null; // File not found
      }
      throw new RuntimeException(
          String.format("Failed to read file data from S3 for key: %s", key), e);
    }
  }

  public void copyFile(String sourceKey, String destinationKey) throws RuntimeException {
    assert sourceKey != null && !sourceKey.isEmpty();
    assert destinationKey != null && !destinationKey.isEmpty();

    if (!fileExists(destinationKey)) {
      CopyObjectRequest copyRequest =
          CopyObjectRequest.builder()
              .sourceBucket(bucketName)
              .sourceKey(sourceKey)
              .destinationBucket(bucketName)
              .destinationKey(destinationKey)
              .build();

      try {
        s3AsyncClient.copyObject(copyRequest).get();
        LOG.info("Copied {} to {} successfully", sourceKey, destinationKey);
      } catch (ExecutionException | InterruptedException e) {
        LOG.error("Failed to copy file from {} to {}", sourceKey, destinationKey, e);
        throw new RuntimeException(
            String.format("Failed to copy file from %s to %s", sourceKey, destinationKey), e);
      }
    } else {
      LOG.info("File {} already exists, skipping copy", destinationKey);
    }
  }

  /**
   * Checks if a file exists in the object store by S3 key (full path in the bucket).
   *
   * @param key The S3 key (full path in the bucket)
   * @return true if the file exists, false otherwise
   * @throws RuntimeException if checking fails
   */
  public boolean fileExists(String key) {
    assert key != null && !key.isEmpty();

    HeadObjectRequest headRequest = HeadObjectRequest.builder().bucket(bucketName).key(key).build();

    try {
      s3AsyncClient.headObject(headRequest).get();
      return true;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NoSuchKeyException
          || (cause instanceof S3Exception s3ex && s3ex.statusCode() == 404)) {
        return false; // Not found
      }
      LOG.error("Error checking if file exists in S3: {}", key, e);
      throw new RuntimeException("Failed to check if S3 file exists", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while checking if S3 file exists", e);
    }
  }

  /**
   * Checks if a path exists in the object store by prefix.
   *
   * @param prefix The S3 prefix (considered as a "directory")
   * @return true if the path exists, false otherwise
   * @throws RuntimeException if checking fails
   */
  public boolean pathExists(String prefix) {
    ListObjectsV2Request listReq =
        ListObjectsV2Request.builder()
            .bucket(bucketName)
            .prefix(prefix.endsWith("/") ? prefix : prefix + "/")
            .maxKeys(1)
            .build();

    try {
      ListObjectsV2Response listRes = s3AsyncClient.listObjectsV2(listReq).get();
      return !listRes.contents().isEmpty();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to check if S3 path exists", e);
    }
  }

  /*
   * Fetch the crc32 checksum of a file in the object store by S3 key (full path in the bucket).
   */
  public String getFileCRC32(String key) {
    CompletableFuture<HeadObjectResponse> head =
        s3AsyncClient
            .headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build())
            .toCompletableFuture();
    try {
      HeadObjectResponse response = head.get();
      return response.checksumCRC32();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException("Failed to get CRC32 checksum for file: " + key, e);
    }
  }
}
