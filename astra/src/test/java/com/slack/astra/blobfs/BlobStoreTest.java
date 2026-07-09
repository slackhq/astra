package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

class BlobStoreTest {
  private static final String TEST_BUCKET = "blobStoreTest";

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
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test");
    }
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directoryUpload);

    // what goes up, must come down
    Path directoryDownloaded = Files.createTempDirectory("");
    blobStore.download(chunkId, directoryDownloaded);

    File[] filesDownloaded = directoryDownloaded.toFile().listFiles();
    assertThat(Objects.requireNonNull(filesDownloaded).length).isEqualTo(1);

    // contents of the file we uploaded should match
    assertThat(Files.readAllBytes(filesDownloaded[0].toPath())).isEqualTo(Files.readAllBytes(foo));
  }

  @Test
  void testUploadOnlyListedFiles() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path included = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(included.toFile())) {
      fileWriter.write("Example test");
    }
    // A second file exists in the directory but is intentionally not in the upload list.
    Path excluded = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(excluded.toFile())) {
      fileWriter.write("Should not be uploaded");
    }
    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directoryUpload, List.of(included.getFileName().toString()));

    assertThat(blobStore.listFiles(chunkId))
        .containsExactly(String.format("%s/%s", chunkId, included.getFileName().toString()));
  }

  @Test
  void testUploadIgnoresUnlistedFileDeletedFromDirectory() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path listed = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(listed.toFile())) {
      fileWriter.write("committed segment");
    }
    // previously, the S3 upload logic took a whole directory as input
    // and some files could get deleted before being uploaded. This would
    // result in a FileNotFoundException of a file we didn't actually need.
    // this simple unit test ensures that even though we pass the dir as
    // an arg, only what was provided in the list is what we upload.
    Path orphan = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(orphan.toFile())) {
      fileWriter.write("pre-merge orphan");
    }
    Files.delete(orphan);

    String chunkId = UUID.randomUUID().toString();
    blobStore.upload(chunkId, directoryUpload, List.of(listed.getFileName().toString()));

    assertThat(blobStore.listFiles(chunkId))
        .containsExactly(String.format("%s/%s", chunkId, listed.getFileName().toString()));
  }

  @Test
  void testUploadMissingListedFileThrows() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test");
    }
    String chunkId = UUID.randomUUID().toString();
    // Listing a file that does not exist on disk must fail loudly rather than silently skipping it.
    // The transfer manager rejects the missing source synchronously while the batch is being built,
    // so this surfaces as an UncheckedIOException rather than the aggregated IllegalStateException
    // used for async transfer failures. Production never hits this path because the IndexCommit
    // snapshot guarantees every listed file exists for the duration of the upload.
    assertThatThrownBy(
            () ->
                blobStore.upload(
                    chunkId,
                    directoryUpload,
                    List.of(foo.getFileName().toString(), "does-not-exist")))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  void testUploadEmptyPrefix() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");

    assertThatThrownBy(() -> blobStore.upload("", directoryUpload))
        .isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> blobStore.upload(null, directoryUpload))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void testUploadEmpty() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryUpload = Files.createTempDirectory("");
    String chunkId = UUID.randomUUID().toString();

    assertThatThrownBy(() -> blobStore.upload(chunkId, directoryUpload))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void testUploadNotADirectory() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directory = Files.createTempDirectory("");
    Path fileUpload = Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();

    assertThatThrownBy(() -> blobStore.upload(chunkId, fileUpload))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void testDownloadDoesNotExist() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    Path directoryDownloaded = Files.createTempDirectory("");
    blobStore.download(UUID.randomUUID().toString(), directoryDownloaded);
    assertThat(Objects.requireNonNull(directoryDownloaded.toFile().listFiles()).length)
        .isEqualTo(0);
  }

  @Test
  void testDownloadNotADirectory() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directory = Files.createTempDirectory("");
    Path fileLocation = Files.createTempFile(directory, "", "");
    String chunkId = UUID.randomUUID().toString();

    assertThatThrownBy(() -> blobStore.download(chunkId, fileLocation))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void testDownloadEmptyPrefix() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    Path directoryDownload = Files.createTempDirectory("");

    assertThatThrownBy(() -> blobStore.download("", directoryDownload))
        .isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> blobStore.download(null, directoryDownload))
        .isInstanceOf(AssertionError.class);
  }

  @Test
  void testDeleteMultipleFiles() throws IOException, ExecutionException, InterruptedException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

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
    blobStore.upload(chunkId, directoryUpload);
    assertThat(
            s3Client
                .listObjects(
                    ListObjectsRequest.builder().bucket(TEST_BUCKET).prefix(chunkId).build())
                .get()
                .contents()
                .size())
        .isEqualTo(2);

    boolean deleted = blobStore.delete(chunkId);
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
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    boolean deleted = blobStore.delete(UUID.randomUUID().toString());
    assertThat(deleted).isFalse();
  }

  @Test
  void testDeleteBadPrefix() {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    assertThatThrownBy(() -> blobStore.delete("")).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> blobStore.delete(null)).isInstanceOf(AssertionError.class);
  }

  @Test
  void testListFiles() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    String chunkId = UUID.randomUUID().toString();

    assertThat(blobStore.listFiles(chunkId).size()).isEqualTo(0);

    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test 1");
    }
    Path bar = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(bar.toFile())) {
      fileWriter.write("Example test 2");
    }
    blobStore.upload(chunkId, directoryUpload);

    assertThat(blobStore.listFiles(chunkId).size()).isEqualTo(2);
    assertThat(blobStore.listFiles(chunkId))
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                String.format("%s/%s", chunkId, foo.getFileName().toString()),
                String.format("%s/%s", chunkId, bar.getFileName().toString())));
  }

  @Test
  void testListBadPrefix() {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);

    assertThatThrownBy(() -> blobStore.listFiles("")).isInstanceOf(AssertionError.class);
    assertThatThrownBy(() -> blobStore.listFiles(null)).isInstanceOf(AssertionError.class);
  }

  @Test
  void testListFilesNonExistingPrefix() {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    String chunkId = UUID.randomUUID().toString();

    assertThat(blobStore.listFiles(chunkId).size()).isEqualTo(0);
  }
}
