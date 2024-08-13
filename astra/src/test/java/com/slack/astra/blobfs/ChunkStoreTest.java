package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;

class ChunkStoreTest {
  private static final String TEST_BUCKET = "chunkStoreTest";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .silent()
          .withInitialBuckets(TEST_BUCKET)
          .withSecureConnection(false)
          .build();

  private final S3AsyncClient s3Client =
      S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint());

  @Test
  void testUploadDownload() throws IOException {
    ChunkStore chunkStore = new ChunkStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test");
    }
    String chunkId = UUID.randomUUID().toString();
    chunkStore.upload(chunkId, directoryUpload);

    // what goes up, must come down
    Path directoryDownloaded = Files.createTempDirectory("");
    chunkStore.download(chunkId, directoryDownloaded);

    File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
    assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);

    // contents of the file we uploaded should match
    assertThat(Files.readAllBytes(filesDownloaded[0].toPath())).isEqualTo(Files.readAllBytes(foo));
  }

  @Test
  void testDownloadDoesNotExist() throws IOException {
    ChunkStore chunkStore = new ChunkStore(s3Client, TEST_BUCKET);
    Path directoryDownloaded = Files.createTempDirectory("");
    chunkStore.download(UUID.randomUUID().toString(), directoryDownloaded);
  }

  @Test
  void testDeleteMultipleFiles() throws IOException, ExecutionException, InterruptedException {
    ChunkStore chunkStore = new ChunkStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test 1");
    }
    Path bar = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(bar.toFile())) {
      fileWriter.write("Example test 2");
    }
    String chunkId = UUID.randomUUID().toString();
    chunkStore.upload(chunkId, directoryUpload);
    assertThat(
            s3Client
                .listObjects(
                    ListObjectsRequest.builder().bucket(TEST_BUCKET).prefix(chunkId).build())
                .get()
                .contents()
                .size())
        .isEqualTo(2);

    boolean deleted = chunkStore.delete(chunkId);
    assertThat(deleted).isTrue();
    assertThat(
            s3Client
                .listObjects(
                    ListObjectsRequest.builder().bucket(TEST_BUCKET).prefix(chunkId).build())
                .get()
                .contents()
                .size())
        .isEqualTo(0);
  }

  @Test
  void testDeleteDoesNotExist() {
    ChunkStore chunkStore = new ChunkStore(s3Client, TEST_BUCKET);
    boolean deleted = chunkStore.delete(UUID.randomUUID().toString());
    assertThat(deleted).isFalse();
  }

  @Test
  void testListFiles() throws IOException {
    ChunkStore chunkStore = new ChunkStore(s3Client, TEST_BUCKET);
    String chunkId = UUID.randomUUID().toString();

    assertThat(chunkStore.listFiles(chunkId).size()).isEqualTo(0);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test 1");
    }
    Path bar = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(bar.toFile())) {
      fileWriter.write("Example test 2");
    }
    chunkStore.upload(chunkId, directoryUpload);

    assertThat(chunkStore.listFiles(chunkId).size()).isEqualTo(2);
    assertThat(chunkStore.listFiles(chunkId))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                String.format(
                    "%s/%s", chunkStore.getRemotePath(chunkId), foo.getFileName().toString()),
                String.format(
                    "%s/%s", chunkStore.getRemotePath(chunkId), bar.getFileName().toString())));
  }
}
