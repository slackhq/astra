package com.slack.astra.blobfs;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;

class S3RemoteDirectoryTest {

  @Test
  public void shouldListFileNamesOnly() throws IOException {
    String chunkId = "chunkId";
    String bucketName = "bucketName";
    S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
    BlobStore blobStore = spy(new BlobStore(s3AsyncClient, bucketName));

    doReturn(List.of("foo/bar.example")).when(blobStore).listFiles(any());

    try (S3RemoteDirectory s3RemoteDirectory = new S3RemoteDirectory(chunkId, blobStore)) {
      String[] filesArray = s3RemoteDirectory.listAll();
      assertThat(filesArray.length).isEqualTo(1);
      assertThat(filesArray[0]).isEqualTo("bar.example");
    }
  }
}
