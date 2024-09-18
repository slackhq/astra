package com.slack.astra.blobfs;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

class S3IndexInputTest {
  private static final String TEST_BUCKET = "testBucket";

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
  public void shouldPageInContents() throws IOException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String resourceDescription = "resource";
    String chunkId = UUID.randomUUID().toString();

    // for test purposes we'll write two pages worth of data
    // first page is all "1", second is all "2"
    Path directoryUpload = Files.createTempDirectory("");
    Path tempFile = Files.createTempFile(directoryUpload, "example", "");
    byte[] testData = new byte[2 * Math.toIntExact(S3IndexInput.PAGE_SIZE)];
    for (int i = 0; i < S3IndexInput.PAGE_SIZE; i++) {
      testData[i] = 1;
    }
    for (int i = 0; i < S3IndexInput.PAGE_SIZE; i++) {
      testData[Math.toIntExact(i + S3IndexInput.PAGE_SIZE)] = 2;
    }
    Files.write(tempFile, testData);
    blobStore.upload(chunkId, directoryUpload);

    try (S3IndexInput s3IndexInput =
        new S3IndexInput(
            blobStore, resourceDescription, chunkId, tempFile.getFileName().toString())) {
      assertThat(s3IndexInput.getCachedData().size()).isZero();

      // read in a single byte, and ensure that it paged in a single page worth of contents
      byte readByte = s3IndexInput.readByte();
      assertThat(readByte).isEqualTo((byte) 1);
      assertThat(s3IndexInput.getFilePointer()).isEqualTo(1);

      // bulk read in the remainder of the page, ensure that is still is using the cached data
      byte[] bulkRead = new byte[Math.toIntExact(S3IndexInput.PAGE_SIZE - 1)];
      s3IndexInput.readBytes(bulkRead, 0, Math.toIntExact(S3IndexInput.PAGE_SIZE - 1));
      assertThat(bulkRead[bulkRead.length - 1]).isEqualTo((byte) 1);
      assertTrue(s3IndexInput.getCachedData().containsKey(0L));

      // read in a single byte more, which will trigger another page load
      // ensure that the page was loaded as expected
      byte readByteNextPage = s3IndexInput.readByte();
      assertThat(readByteNextPage).isEqualTo((byte) 2);
      assertTrue(s3IndexInput.getCachedData().containsKey(1L));
    }
  }
}
