package com.slack.astra.blobfs;

import static software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder;

import com.slack.astra.chunk.ReadWriteChunk;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.internal.async.ByteArrayAsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadRequest;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

/**
 * Blob store abstraction for basic operations on chunk/snapshots on remote storage. All operations
 * are invoked with a prefix, which can be considered analogous to the "folder," and all objects are
 * stored to a consistent location of "{prefix}/{filename}".
 */
public class BlobStore {
  private static final Logger LOG = LoggerFactory.getLogger(BlobStore.class);
  private static final int MAX_CONCURRENT_UPLOADS = 100;

  protected final String bucketName;
  protected final S3AsyncClient s3AsyncClient;
  private final S3TransferManager transferManager;

  public BlobStore(S3AsyncClient s3AsyncClient, String bucketName) {
    this.bucketName = bucketName;
    this.s3AsyncClient = s3AsyncClient;
    this.transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
  }

  /**
   * Uploads all files in a directory to the object store by prefix. Primarily used by tests to
   * stage data in S3. Production callers should prefer the 3-arg overload with an explicit file
   * list to avoid race conditions with concurrent file deletions.
   */
  public void upload(String prefix, Path directoryToUpload) {
    assert prefix != null && !prefix.isEmpty();
    assert directoryToUpload.toFile().isDirectory();

    File[] entries = directoryToUpload.toFile().listFiles();
    assert entries != null && entries.length > 0;

    List<String> fileNames = new ArrayList<>();
    for (File file : entries) {
      if (file.isFile()) {
        fileNames.add(file.getName());
      }
    }
    upload(prefix, directoryToUpload, fileNames);
  }

  /**
   * Uploads only the specified files from a directory to the object store by prefix. This prevents
   * race conditions where files in the directory may be deleted by concurrent processes (e.g.,
   * Lucene segment merges) between directory enumeration and file read.
   *
   * @param prefix Prefix to store (ie, chunk id)
   * @param directoryToUpload Directory containing the files
   * @param fileNames Collection of file names to upload (only these will be included)
   * @throws IllegalStateException Thrown if any files fail to upload
   * @throws RuntimeException Thrown when error is considered generally non-retryable
   */
  public void upload(String prefix, Path directoryToUpload, Collection<String> fileNames) {
    assert prefix != null && !prefix.isEmpty();
    assert directoryToUpload.toFile().isDirectory();
    assert fileNames != null && !fileNames.isEmpty();

    List<Exception> failures = new ArrayList<>();
    List<String> keysBatch = new ArrayList<>(MAX_CONCURRENT_UPLOADS);
    List<FileUpload> uploadsBatch = new ArrayList<>(MAX_CONCURRENT_UPLOADS);

    for (String fileName : fileNames) {
      Path filePath = directoryToUpload.resolve(fileName);
      String key = prefix + "/" + fileName;
      FileUpload fileUpload =
          transferManager.uploadFile(
              UploadFileRequest.builder()
                  .putObjectRequest(PutObjectRequest.builder().bucket(bucketName).key(key).build())
                  .source(filePath)
                  .build());
      keysBatch.add(key);
      uploadsBatch.add(fileUpload);

      if (uploadsBatch.size() >= MAX_CONCURRENT_UPLOADS) {
        awaitUploads(uploadsBatch, keysBatch, failures);
        uploadsBatch.clear();
        keysBatch.clear();
      }
    }
    if (!uploadsBatch.isEmpty()) {
      awaitUploads(uploadsBatch, keysBatch, failures);
    }

    if (!failures.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Some files failed to upload - attempted to upload %s files, failed %s.",
              fileNames.size(), failures.size()),
          failures.get(0));
    }
  }

  private void awaitUploads(List<FileUpload> uploads, List<String> keys, List<Exception> failures) {
    for (int i = 0; i < uploads.size(); i++) {
      try {
        uploads.get(i).completionFuture().get();
      } catch (ExecutionException e) {
        LOG.error("Error attempting to upload file '{}' to S3", keys.get(i), e.getCause());
        failures.add(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
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

      if (!download.failedTransfers().isEmpty()) {
        // Log each failed transfer with its exception
        download
            .failedTransfers()
            .forEach(
                failedFileUpload ->
                    LOG.error(
                        "Error attempting to download file from S3", failedFileUpload.exception()));

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
}
