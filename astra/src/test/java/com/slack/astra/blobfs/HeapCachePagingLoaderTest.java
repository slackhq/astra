package com.slack.astra.blobfs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.services.s3.S3AsyncClient;

class HeapCachePagingLoaderTest {

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
  public void testDiskPagingWholeFileOneChunk() throws IOException, ExecutionException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file2.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader =
        new DiskCachePagingLoader(blobStore, s3Client, 2000);
    HeapCachePagingLoader heapCachePagingLoader =
        new HeapCachePagingLoader(blobStore, s3Client, diskCachePagingLoader, 1000);

    String contents = "1827367483821827237sjzuj7au378wjs";
    byte[] readBytes = new byte[contents.getBytes().length];

    Path directory = Files.createTempDirectory(chunkId);
    Path exampleFile = Files.createFile(Path.of(directory.toString(), filename));
    Files.writeString(exampleFile, contents, Charset.defaultCharset());
    blobStore.upload(chunkId, directory);

    heapCachePagingLoader.readBytes(chunkId, filename, readBytes, 0, 0, contents.getBytes().length);
    assertThat(contents).isEqualTo(new String(readBytes));
  }

  @Test
  @Disabled
  public void testDiskPagingWholeFileSmallChunks() throws IOException, ExecutionException {
    BlobStore blobStore = spy(new BlobStore(s3Client, TEST_BUCKET));
    String filename = "file3.example";
    String chunkId = UUID.randomUUID().toString();

    DiskCachePagingLoader diskCachePagingLoader = new DiskCachePagingLoader(blobStore, s3Client, 4);
    HeapCachePagingLoader heapCachePagingLoader =
        new HeapCachePagingLoader(blobStore, s3Client, diskCachePagingLoader, 2);

    String contents = "alksjdsfuwjwui387kja83kjw8i";
    byte[] readBytes = new byte[contents.getBytes().length];

    Path directory = Files.createTempDirectory(chunkId);
    Path exampleFile = Files.createFile(Path.of(directory.toString(), filename));
    Files.writeString(exampleFile, contents, Charset.defaultCharset());
    blobStore.upload(chunkId, directory);

    heapCachePagingLoader.readBytes(chunkId, filename, readBytes, 0, 0, contents.getBytes().length);
    assertThat(contents).isEqualTo(new String(readBytes));
  }
}
