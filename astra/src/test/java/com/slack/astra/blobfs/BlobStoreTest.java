package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

  @Test
  void testListFilesWithSize() throws IOException {
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

    Map<String, Long> filesWithSize = blobStore.listFilesWithSize(chunkId);
    assertThat(filesWithSize.size()).isEqualTo(2);
    assertThat(filesWithSize)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                String.format("%s/%s", chunkId, foo.getFileName().toString()),
                Files.size(foo),
                String.format("%s/%s", chunkId, bar.getFileName().toString()),
                Files.size(bar)));
  }

  @Test
  public void testCompressDecompressJsonData() throws Exception {
    // Arrange
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_789\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}]";
    byte[] compressedData = BlobStore.compressData(jsonData);

    // Act
    String decompressedData = BlobStore.decompressData(compressedData);

    // Assert
    assertNotNull(decompressedData, "Decompressed data should not be null");
    assertEquals(jsonData, decompressedData, "Decompressed data should match original");
  }

  @Test
  public void testCompressDecompressJsonData_emptyString() throws Exception {
    // Arrange
    String jsonData = "";
    byte[] compressedData = BlobStore.compressData(jsonData);

    // Act
    String decompressedData = BlobStore.decompressData(compressedData);

    // Assert
    assertNotNull(decompressedData, "Decompressed data should not be null");
    assertEquals(
        jsonData, decompressedData, "Decompressed data should match original empty string");
  }

  @Test
  public void testUploadDownloadJsonData() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    String chunkId = UUID.randomUUID().toString();
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_789\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}]";

    // Upload JSON data
    blobStore.uploadData(chunkId, jsonData, false);

    // Download and decompress the JSON data
    String downloadedJsonData = blobStore.readFileData(chunkId, false);

    // Assert that the downloaded data matches the original
    assertEquals(jsonData, downloadedJsonData, "Downloaded JSON data should match original");
  }

  @Test
  public void testUploadDownloadJsonData_gzip() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    String chunkId = UUID.randomUUID().toString();
    String jsonData =
        "[{\"id\":\"101\",\"traceId\":\"test_trace_789\",\"name\":\"test-span\",\"tags\":{\"key1\":\"value1\",\"key2\":\"value2\",\"key3\":\"value3\"}}]";

    // Upload empty JSON data
    blobStore.uploadData(chunkId, jsonData, true);

    // Download and decompress the JSON data
    String downloadedJsonData = blobStore.readFileData(chunkId, true);

    // Assert that the downloaded data matches the original empty string
    assertEquals(
        jsonData, downloadedJsonData, "Downloaded JSON data should match original empty string");
  }

  @Test
  public void testCopyFile() throws IOException {
    BlobStore blobStore = new BlobStore(s3Client, TEST_BUCKET);
    String sourceChunkId = UUID.randomUUID().toString();
    String destinationChunkId = UUID.randomUUID().toString();

    // Upload a file
    Path directoryUpload = Files.createTempDirectory("");
    Path foo = Files.createTempFile(directoryUpload, "", "");
    try (FileWriter fileWriter = new FileWriter(foo.toFile())) {
      fileWriter.write("Example test");
    }
    blobStore.upload(sourceChunkId, directoryUpload);

    // Copy the file to a new chunk ID
    blobStore.copyFile(
        String.format("%s/%s", sourceChunkId, foo.getFileName()),
        String.format("%s/%s", destinationChunkId, foo.getFileName()));

    // Verify the file is copied
    List<String> filesInDestination = blobStore.listFiles(destinationChunkId);
    assertThat(filesInDestination)
        .contains(String.format("%s/%s", destinationChunkId, foo.getFileName()));
  }
}
