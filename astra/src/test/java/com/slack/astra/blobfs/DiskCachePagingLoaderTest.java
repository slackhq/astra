package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

class DiskCachePagingLoaderTest {

  private static final String TEST_BUCKET = "testBucket";

  @RegisterExtension
  public static final S3MockExtension S3_MOCK_EXTENSION =
      S3MockExtension.builder()
          .silent()
          .withInitialBuckets(TEST_BUCKET)
          .withSecureConnection(false)
          .build();

  private final S3AsyncClient s3Client =
      spy(S3TestUtils.createS3CrtClient(S3_MOCK_EXTENSION.getServiceEndpoint()));

  @Test
  public void testCacheKeyCalculations() {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader =
        new DiskCachePagingLoader(blobStore, s3Client, 10);

    List<LoadingCacheKey> oneKey = diskCachePagingLoader.getCacheKeys(chunkId, filename, 5, 4);
    assertThat(oneKey).containsExactlyInAnyOrder(new LoadingCacheKey(chunkId, filename, 0, 9));

    List<LoadingCacheKey> multipleKeys =
        diskCachePagingLoader.getCacheKeys(chunkId, filename, 8, 18);
    assertThat(multipleKeys)
        .containsExactlyInAnyOrder(
            new LoadingCacheKey(chunkId, filename, 0, 9),
            new LoadingCacheKey(chunkId, filename, 10, 19),
            new LoadingCacheKey(chunkId, filename, 20, 29));

    List<LoadingCacheKey> exactMatch = diskCachePagingLoader.getCacheKeys(chunkId, filename, 0, 10);
    assertThat(exactMatch).containsExactlyInAnyOrder(new LoadingCacheKey(chunkId, filename, 0, 9));
    List<LoadingCacheKey> matchStartingTwoChunks =
        diskCachePagingLoader.getCacheKeys(chunkId, filename, 0, 11);
    assertThat(matchStartingTwoChunks)
        .containsExactlyInAnyOrder(
            new LoadingCacheKey(chunkId, filename, 0, 9),
            new LoadingCacheKey(chunkId, filename, 10, 19));
  }

  @Test
  public void testDiskPaging() throws IOException, ExecutionException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader =
        new DiskCachePagingLoader(blobStore, s3Client, 10);

    int bytesToRead = 14;
    byte[] readBytes = new byte[bytesToRead];

    Path directory = Files.createTempDirectory(chunkId);
    Path exampleFile = Files.createFile(Path.of(directory.toString(), filename));
    Files.writeString(exampleFile, "12345678qwertyuiopasdfghjkl;", Charset.defaultCharset());
    blobStore.upload(chunkId, directory);

    Path download = Files.createTempDirectory(UUID.randomUUID().toString());
    blobStore.download(chunkId, download);

    // this should read in bytes 12-28 from the original file, stored in readBytes 0-14
    diskCachePagingLoader.readBytes(chunkId, filename, readBytes, 0, 12, 14);
    System.out.println(new String(readBytes));
    assertThat(readBytes).isEqualTo("tyuiopasdfghjk".getBytes(Charset.defaultCharset()));
  }

  @Test
  public void testDiskPagingWholeFileOneChunk() throws IOException, ExecutionException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file2.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader =
        new DiskCachePagingLoader(blobStore, s3Client, 2000);

    String contents = "1827367483821827237sjzuj7au378wjs";
    byte[] readBytes = new byte[contents.getBytes().length];

    Path directory = Files.createTempDirectory(chunkId);
    Path exampleFile = Files.createFile(Path.of(directory.toString(), filename));
    Files.writeString(exampleFile, contents, Charset.defaultCharset());
    blobStore.upload(chunkId, directory);

    diskCachePagingLoader.readBytes(chunkId, filename, readBytes, 0, 0, contents.getBytes().length);
    assertThat(contents).isEqualTo(new String(readBytes));
  }

  @Test
  public void testDiskPagingWholeFileSmallChunks() throws IOException, ExecutionException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file3.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader = new DiskCachePagingLoader(blobStore, s3Client, 3);

    String contents = "alksjdsfuwjwui387kja83kjw8i";
    byte[] readBytes = new byte[contents.getBytes().length];

    Path directory = Files.createTempDirectory(chunkId);
    Path exampleFile = Files.createFile(Path.of(directory.toString(), filename));
    Files.writeString(exampleFile, contents, Charset.defaultCharset());
    blobStore.upload(chunkId, directory);

    diskCachePagingLoader.readBytes(chunkId, filename, readBytes, 0, 0, contents.getBytes().length);
    assertThat(contents).isEqualTo(new String(readBytes));
  }
}
