package com.slack.astra.blobfs;

import static software.amazon.awssdk.services.s3.model.ListObjectsV2Request.builder;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedDirectoryUpload;
import software.amazon.awssdk.transfer.s3.model.DownloadDirectoryRequest;
import software.amazon.awssdk.transfer.s3.model.UploadDirectoryRequest;

public class ChunkStore {

  private final String bucketName;
  private final S3AsyncClient s3AsyncClient;
  private final S3TransferManager transferManager;

  public ChunkStore(S3AsyncClient s3AsyncClient, String bucketName) {
    this.bucketName = bucketName;
    this.s3AsyncClient = s3AsyncClient;
    this.transferManager = S3TransferManager.builder().s3Client(s3AsyncClient).build();
  }

  public void upload(String chunkId, Path directoryToUpload) {
    try {
      CompletedDirectoryUpload upload =
          transferManager
              .uploadDirectory(
                  UploadDirectoryRequest.builder()
                      .source(directoryToUpload)
                      .s3Prefix(chunkId)
                      .bucket(bucketName)
                      .build())
              .completionFuture()
              .get();
      if (!upload.failedTransfers().isEmpty()) {
        throw new IllegalStateException("Some or all files failed to upload");
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  // todo - make destination optional
  public Path download(String chunkId, Path destination) {
    try {
      transferManager
          .downloadDirectory(
              DownloadDirectoryRequest.builder()
                  .bucket(bucketName)
                  .destination(destination)
                  .listObjectsV2RequestTransformer(l -> l.prefix(chunkId))
                  .build())
          .completionFuture()
          .get();
      return destination;
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> listFiles(String chunkId) {
    ListObjectsV2Request listRequest = builder().bucket(bucketName).prefix(chunkId).build();
    ListObjectsV2Publisher asyncPaginatedListResponse =
        s3AsyncClient.listObjectsV2Paginator(listRequest);

    List<String> filesList = new ArrayList<>();
    try {
      asyncPaginatedListResponse
          .subscribe(
              listResponse -> {
                listResponse
                    .contents()
                    .forEach(
                        s3Object -> {
                          filesList.add(String.format("s3://%s/%s", bucketName, s3Object.key()));
                        });
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    return filesList;
  }

  public String getRemotePath(String chunkId) {
    return String.format("s3://%s/%s", bucketName, chunkId);
  }

  public boolean delete(String chunkId) {
    ListObjectsV2Request listRequest = builder().bucket(bucketName).prefix(chunkId).build();
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
